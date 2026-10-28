/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_cleaner.c
 *    Real shared CTRC selection and reply-progress regression tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ctrc_cleaner.c
 *
 * NOTES
 *    This is a pgrac-original file. No CLUSTER_CTRC_UNIT_TEST: the actual
 *    shared selectors, reservations and reply consumers are compiled.
 *    Native transaction state, allocation, locks, error reporting and wakeup
 *    are process-boundary fixtures. This is not a disk or network end-to-end
 *    test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include "access/xact.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "storage/procarray.h"
#include "utils/wait_event.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "test_cluster_pgproc_active_types.inc"

static unsigned spin_acquisitions;
static unsigned sleepable_acquisitions;
static unsigned held_count;
static unsigned conditional_calls, conditional_refuse_call;
static const void *held_locks[10];
static const void *last_participant_lock;
static LWLockMode last_pool_mode;
static const void *paused_actor_locks[10];
static unsigned paused_actor_count;
static void test_lock_enter(const void *lock, bool sleepable);
static void test_lock_leave(const void *lock);

static void
pg_attribute_unused() test_spin_acquire(volatile slock_t *lock)
{
	S_LOCK(lock);
	test_lock_enter((const void *)lock, false);
}

static void
pg_attribute_unused() test_spin_release(volatile slock_t *lock)
{
	test_lock_leave((const void *)lock);
	S_UNLOCK(lock);
}

/* Observe the real shared bodies, including the old spinlock baseline.
 * LWLock scheduling itself is PostgreSQL, not implemented by this fixture. */
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) test_spin_acquire(lock)
#define SpinLockRelease(lock) test_spin_release(lock)

static int test_clock_gettime(clockid_t clock_id, struct timespec *ts);
static Size table_visits[6];
static Size shared_validation_writer_visits;
static void test_table_visit(unsigned kind);
static void test_pool_upgrade(void);
#define CLUSTER_CTRC_TEST_TABLE_VISIT(kind) test_table_visit(kind)
#define CLUSTER_CTRC_TEST_POOL_UPGRADE() test_pool_upgrade()
#define clock_gettime test_clock_gettime
#include "../../backend/cluster/cluster_terminal_ref_census.c"
#undef clock_gettime

#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#include "cluster/cluster_undo_cleaner.h"

int NBuffers = 16;
int MaxBackends = 8;
int cluster_node_id = 0;
BackendType MyBackendType = B_BACKEND;
AuxProcType MyAuxProcType = NotAnAuxProcess;
int MyProcPid = 2901;
static PGPROC native_proc_slots[2];
#define test_my_proc native_proc_slots[0]
PGPROC *MyProc = &test_my_proc;
static FullTransactionId test_top_full;
static unsigned top_full_reads;
/* Only the new composition groups run the actual native source exit. */
static bool native_source_exit;
static bool native_grouped_source_exit;
static bool native_prepared_source_exit;
static unsigned native_prepared_retirements;
static bool native_reused_source_exit;
static unsigned native_reused_source_publications;
static bool native_multi_member_exit;
static unsigned native_group_wakeups;
static FullTransactionId native_group_wake_minimum, native_group_wake_expected;
static ClusterSemanticActiveSampleFence native_group_wake_source;
static bool native_publish_source;
static unsigned native_published_samples;
static bool native_worker_respawn;
static unsigned native_worker_respawns;
static unsigned native_group_refusals;
static unsigned native_group_wait_observations;
static FullTransactionId native_group_wait_minimum;
static VariableCacheData native_variables;
static VariableCache native_variable_cache = &native_variables;
static PROC_HDR native_procs;
static PROC_HDR *native_proc_global = &native_procs;
static PGPROC *allProcs = native_proc_slots;
static TransactionId native_xids[2];
static XidCacheStatus native_subxids[2];
static uint8 native_status_flags[2];
static char native_array_storage[offsetof(ProcArrayStruct, pgprocnos) + 2 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static ProcArrayStruct *procArray = (ProcArrayStruct *) native_array_storage;
static LWLockPadded native_locks[NUM_INDIVIDUAL_LWLOCKS];
/* Resolve original native lock macros against this private fixture array. */
#define MainLWLockArray native_locks
static LWLock *const native_xidgen_lock = XidGenLock;
static LWLock *const native_procarray_lock = ProcArrayLock;
static LWLock *const native_twophase_lock = TwoPhaseStateLock;
#undef MainLWLockArray
static bool native_lock_held;
static LWLock *native_held_lock;
static LWLockMode native_lock_mode;
static unsigned native_lock_acquires, native_lock_releases, native_future_reads;
static FullTransactionId native_future;
static TransactionId native_completed_xid;
static GlobalTransaction native_locked_gxact;
static BackendId native_backend_id = 1;
static bool native_postmaster_environment = true;
static LWLock *native_prepared_locks[2];
static LWLockMode native_prepared_modes[2];
static unsigned native_prepared_depth;
static void ProcArrayEndTransactionInternal(PGPROC *, TransactionId);
static void native_procarray_end_transaction(PGPROC *, TransactionId);
static void native_procarray_add(PGPROC *);
static void native_procarray_remove(PGPROC *, TransactionId);
static void native_procarray_clear(PGPROC *);
static bool native_procarray_sample(FullTransactionId *, FullTransactionId *,
								   FullTransactionId *, ClusterXidStripeSampleFence *);

static void
MaintainLatestCompletedXid(TransactionId latest_xid)
{
	if (!native_lock_held || native_lock_mode != LW_EXCLUSIVE)
		abort();
	native_completed_xid = latest_xid;
}
static void ProcArrayGroupClearXid(PGPROC *, TransactionId);
static void native_group_wakeup(PGSemaphore);

static void
native_unexpected_semaphore(PGSemaphore sema pg_attribute_unused())
{
	/* These groups execute the real group leader, never a semaphore waiter. */
	abort();
}

static void
native_unexpected_wait_start(uint32 event pg_attribute_unused())
{
	abort();
}

static void
native_unexpected_wait_end(void)
{
	abort();
}

static bool
native_recovery_in_progress(void)
{
	return false;
}
static bool
native_lock_held_in_mode(LWLock *lock, LWLockMode mode)
{
	if (native_prepared_source_exit || native_reused_source_exit)
		for (unsigned i = 0; i < native_prepared_depth; i++)
			if (native_prepared_locks[i] == lock && native_prepared_modes[i] == mode)
				return true;
	return native_lock_held && lock == native_held_lock && native_lock_mode == mode;
}
static bool
native_lock_is_held(LWLock *lock)
{
	return native_lock_held && lock == native_held_lock;
}
static bool
native_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	if (!native_source_exit || held_count != 0 || !native_lock_held
		|| native_held_lock != native_xidgen_lock || native_lock_mode != LW_SHARED)
		abort();
	native_future_reads++;
	*future = native_future;
	MemSet(fence, 0, sizeof(*fence));
	return true;
}
/* Alias only the extracted native bodies, leaving all importing fixtures'
 * original global native owners and helper implementations intact. */
#define ShmemVariableCache native_variable_cache
#define ProcGlobal native_proc_global
#define MainLWLockArray native_locks
#define RecoveryInProgress native_recovery_in_progress
#define LWLockHeldByMeInMode native_lock_held_in_mode
#define LWLockHeldByMe native_lock_is_held
#define cluster_xid_stripe_sample_future native_sample_future
#define ProcArrayGetActiveFullXidSample native_procarray_sample
#define ProcArrayEndTransaction native_procarray_end_transaction
#include "test_cluster_ctrc_native_exit.inc"
#define allProcs (native_procs.allProcs)
#define PGSemaphoreLock native_unexpected_semaphore
#define PGSemaphoreUnlock native_group_wakeup
#define pgstat_report_wait_start native_unexpected_wait_start
#define pgstat_report_wait_end native_unexpected_wait_end
#include "test_cluster_ctrc_native_group_exit.inc"
#undef allProcs
#define MyBackendId native_backend_id
#define IsPostmasterEnvironment native_postmaster_environment
#define MyLockedGxact native_locked_gxact
#define ProcArrayAdd native_procarray_add
#define ProcArrayRemove native_procarray_remove
#define ProcArrayClearTransaction native_procarray_clear
#include "test_cluster_ctrc_native_prepare.inc"
#undef ProcArrayClearTransaction
#undef ProcArrayRemove
#undef ProcArrayAdd
#undef MyLockedGxact
#undef IsPostmasterEnvironment
#undef MyBackendId
#undef pgstat_report_wait_end
#undef pgstat_report_wait_start
#undef PGSemaphoreUnlock
#undef PGSemaphoreLock
#undef allProcs
#undef ProcArrayEndTransaction
#undef ProcArrayGetActiveFullXidSample
#undef cluster_xid_stripe_sample_future
#undef LWLockHeldByMe
#undef LWLockHeldByMeInMode
#undef RecoveryInProgress
#undef MainLWLockArray
#undef ProcGlobal
#undef ShmemVariableCache
/* Original worker0 publication, using the public original state layout.
 * Lease/admission/configuration are explicit current-identity leaves here;
 * the shared census registration check below remains the real engine. */
static UndoCleanerSharedState native_cleaner_state;
static bool native_cleaner_lock_held;
static unsigned native_cleaner_lock_acquires, native_cleaner_lock_releases;
static UndoCleanerSharedState *undo_cleaner_state = &native_cleaner_state;
static int undo_cleaner_worker;
static ClusterCtrcActiveScan undo_cleaner_active_scan;
static ClusterXidStripeSampleFence undo_cleaner_active_lease_fence;
static ClusterSemanticActiveSampleFence undo_cleaner_active_admission_fence;
static bool native_cleaner_sample(ClusterUndoCleanerActiveSample *);
static bool native_cleaner_sample_current(const ClusterUndoCleanerActiveSample *);
static bool undo_cleaner_active_scan_pending;
static TimestampTz native_status_timestamp;
static unsigned native_status_broadcasts;
/* Controlled source/current leaves for the actual worker0 scan coordinator. */
static bool native_pass_mode;
static bool native_pass_capture_allowed = true;
static bool native_pass_lease_valid = true;
static bool native_pass_admission_valid = true;
static unsigned native_pass_captures;
static ClusterSemanticActiveSampleFence native_pass_source;

static bool
native_cleaner_admission_capture(ClusterSemanticActiveSampleFence *out)
{
	if (!native_pass_mode || held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	native_pass_captures++;
	MemSet(out, 0, sizeof(*out));
	if (!native_pass_capture_allowed)
		return false;
	*out = native_pass_source;
	return true;
}



static TimestampTz
native_status_time(void)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	return ++native_status_timestamp;
}

static void
native_status_broadcast(ConditionVariable *cv)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held
		|| cv != &native_cleaner_state.capacity_cv)
		abort();
	native_status_broadcasts++;
}

static bool
native_cleaner_storage(void)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	return native_publish_source;
}

static bool
native_cleaner_stop(void)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	return false;
}

static bool
native_cleaner_lease_current(const ClusterXidStripeSampleFence *expected)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	return expected != NULL && native_publish_source
		&& (!native_pass_mode || native_pass_lease_valid)
		&& memcmp(expected, &undo_cleaner_active_lease_fence, sizeof(*expected)) == 0;
}

static bool
native_cleaner_admission_current(const ClusterSemanticActiveSampleFence *expected)
{
	if (held_count != 0 || native_lock_held || native_cleaner_lock_held)
		abort();
	return expected != NULL && native_publish_source
		&& (!native_pass_mode || native_pass_admission_valid)
		&& memcmp(expected, &undo_cleaner_active_admission_fence, sizeof(*expected)) == 0;
}

#define cluster_undo_cleaner_enabled native_publish_source
#define cluster_storage_mode_enabled native_cleaner_storage
#define cluster_normal_stop_maintenance_cut native_cleaner_stop
#define cluster_xid_stripe_sample_fence_current native_cleaner_lease_current
#define cluster_semantic_activation_active_sample_current native_cleaner_admission_current
#define cluster_undo_cleaner_active_sample native_cleaner_sample
#define cluster_undo_cleaner_active_sample_current native_cleaner_sample_current
#define cluster_semantic_activation_active_sample_capture native_cleaner_admission_capture
#define ProcArrayGetActiveFullXidSample native_procarray_sample
#define GetCurrentTimestamp native_status_time
#define ConditionVariableBroadcast native_status_broadcast
#include "test_cluster_ctrc_worker_sample.inc"
#undef ConditionVariableBroadcast
#undef GetCurrentTimestamp
#undef ProcArrayGetActiveFullXidSample
#undef cluster_semantic_activation_active_sample_capture
#undef cluster_undo_cleaner_active_sample_current
#undef cluster_undo_cleaner_active_sample
#undef cluster_semantic_activation_active_sample_current
#undef cluster_xid_stripe_sample_fence_current
#undef cluster_normal_stop_maintenance_cut
#undef cluster_storage_mode_enabled
#undef cluster_undo_cleaner_enabled

static bool allow_new_service_participant = true;
static unsigned new_service_participant_calls;
static unsigned wake_count;
static uint64 test_now_us = UINT64_C(1000000);
static uint32 test_capability_generation = 19;
static uint64 test_cluster_epoch = 13;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
static unsigned allocated;
static bool allocation_failure;
static unsigned allocation_attempts, fail_allocation_attempt;
static unsigned flush_calls, durability_calls, barrier_calls;
static XLogRecPtr test_flush_lsn;
static void (*durability_hook)(unsigned call);
static XLogRecPtr (*origin_durability_hook)(int origin);
static void (*barrier_hook)(void);
static void (*pool_upgrade_hook)(void);
static void (*wakeup_hook)(void);
/* Optional boundary for a caller's native/truncation guards; shared CTRC
 * lock ordering is still checked by the original fixture below. */
static bool (*external_lock_hook)(LWLock *lock, LWLockMode mode, bool acquire);
static bool read_scope_enter_refused;
static unsigned read_scope_entered, read_scope_left;
bool cluster_shared_config;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
static bool publication_fenced, fence_at_receipt_lock;
static uint64 publication_revision = 1;
static int publication_smgr = 1;

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	UT_ASSERT_EQ(held_count, 0);
	top_full_reads++;
	return test_top_full;
}

int
cluster_smgr_which_for(RelFileLocator locator pg_attribute_unused(),
					   BackendId backend pg_attribute_unused())
{
	return publication_smgr;
}

/* KO is a boundary here: the actual shared PREPARE and its lock order,
 * participant, receipt, locator and handle publication are under test. */
bool
cluster_ko_peer_admission_begin_v3(RelFileLocator locator, ClusterKoPeerAdmissionV3 *out)
{
	UT_ASSERT_EQ(held_count, 0);
	MemSet(out, 0, sizeof(*out));
	if (publication_fenced && locator.relNumber == 9001)
		return false;
	out->locator = locator;
	out->revision = publication_revision;
	return true;
}

bool
cluster_ko_peer_admission_current_v3(const ClusterKoPeerAdmissionV3 *sample, RelFileLocator locator)
{
	return sample != NULL && sample->revision != 0 && sample->revision == publication_revision
		   && sample->cleaner.serial == 0 && sample->extension_serial == 0
		   && RelFileLocatorEquals(sample->locator, locator);
}

bool
cluster_ko_peer_read_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber block,
							  ClusterKoPeerAdmissionV3 *out)
{
	if (cluster_ko_peer_admission_begin_v3(locator, out))
		return true;
	if (!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, fork, block, &out->cleaner))
		return false;
	out->locator = locator;
	out->revision = publication_revision;
	out->fork = fork;
	out->block = block;
	return true;
}

bool
cluster_ko_peer_read_current_v3(const ClusterKoPeerAdmissionV3 *sample, RelFileLocator locator,
								ForkNumber fork, BlockNumber block)
{
	if (sample->cleaner.serial == 0)
		return cluster_ko_peer_admission_current_v3(sample, locator);
	return sample->revision == publication_revision && sample->extension_serial == 0
		   && sample->fork == fork && sample->block == block
		   && RelFileLocatorEquals(sample->locator, locator)
		   && cluster_ctrc_cleaner_read_scope_current_v3(&sample->cleaner, locator, fork, block);
}

bool
cluster_ko_cleaner_enter_v3(RelFileLocator locator, const ClusterCtrcCleanerReadScopeV3 *scope)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_current_v3(scope, locator, SPACE_FORKNUM, 0));
	if (read_scope_enter_refused)
		return false;
	read_scope_entered++;
	return true;
}

void
cluster_ko_cleaner_leave_v3(RelFileLocator locator pg_attribute_unused(),
							const ClusterCtrcCleanerReadScopeV3 *scope pg_attribute_unused())
{
	UT_ASSERT_EQ(held_count, 0);
	read_scope_left++;
}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	UT_ASSERT(modifies_data);
	new_service_participant_calls++;
	return allow_new_service_participant;
}

void *
palloc_extended(Size size, int flags)
{
	void *memory;
	if (held_count != 0)
		abort();
	allocation_attempts++;
	if (allocation_failure || allocation_attempts == fail_allocation_attempt)
		return NULL;
	memory = (flags & MCXT_ALLOC_ZERO) != 0 ? calloc(1, size) : malloc(size);
	if (memory != NULL)
		allocated++;
	return memory;
}

void
pfree(void *memory)
{
	if (held_count != 0 || allocated == 0 || memory == NULL)
		abort();
	allocated--;
	free(memory);
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ProcessInterrupts(void)
{
	if (held_count != 0)
		abort();
	pg_re_throw();
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (held_count != 0)
		abort();
	flush_calls++;
	if (lsn > test_flush_lsn)
		test_flush_lsn = lsn;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *tli)
{
	if (held_count != 0 || tli != NULL)
		abort();
	durability_calls++;
	if (durability_hook != NULL)
		durability_hook(durability_calls);
	return test_flush_lsn;
}

XLogRecPtr
cluster_sf_observed_origin_durable_lsn(int origin)
{
	if (held_count != 0)
		abort();
	return origin_durability_hook != NULL ? origin_durability_hook(origin) : test_flush_lsn;
}

void
pg_usleep(long microsec)
{
	if (held_count != 0 || microsec != 1000L)
		abort();
	barrier_calls++;
	if (barrier_hook != NULL)
		barrier_hook();
	(void)cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, false);
}

static void
test_lock_enter(const void *lock, bool sleepable)
{
	int shard = -1;
	int previous_shard = -1;

	if (CtrcShared == NULL || held_count >= lengthof(held_locks))
		abort();
	for (unsigned i = 0; i < paused_actor_count; i++)
		if (lock == paused_actor_locks[i])
			abort();
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
		if (lock == &CtrcShared->participant_locks[i].lock)
			shard = i;
		if (held_count != 0 && held_locks[held_count - 1] == &CtrcShared->participant_locks[i].lock)
			previous_shard = i;
	}
	if ((shard < 0 && lock != &CtrcShared->origin_lock && lock != &CtrcShared->receipt_lock)
		|| (held_count != 0
			&& (held_locks[held_count - 1] == &CtrcShared->receipt_lock
				|| (lock != &CtrcShared->receipt_lock
					&& (shard < 0
						|| (held_locks[held_count - 1] != &CtrcShared->origin_lock
							&& (previous_shard < 0 || shard <= previous_shard)))))))
		abort();
	held_locks[held_count++] = lock;
	if (sleepable)
		sleepable_acquisitions++;
	else
		spin_acquisitions++;
}

static void
test_lock_leave(const void *lock)
{
	if (held_count == 0 || held_locks[held_count - 1] != lock)
		abort();
	held_locks[--held_count] = NULL;
}

static void
test_table_visit(unsigned kind)
{
	table_visits[kind]++;
	if (kind == 0 && held_count != 0 && held_locks[held_count - 1] == &CtrcShared->receipt_lock
		&& last_pool_mode == LW_EXCLUSIVE)
		shared_validation_writer_visits++;
}

static void
test_pool_upgrade(void)
{
	if (pool_upgrade_hook != NULL)
		pool_upgrade_hook();
}

void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	MemSet(lock, 0, sizeof(*lock));
	lock->tranche = tranche_id;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_EXCLUSIVE && mode != LW_SHARED)
		abort();
	if (external_lock_hook != NULL && external_lock_hook(lock, mode, true))
		return true;
	if (lock->tranche == LWTRANCHE_CLUSTER_CTRC_PARTICIPANT)
		last_participant_lock = lock;
	if (lock == &CtrcShared->receipt_lock)
		last_pool_mode = mode;
	test_lock_enter(lock, true);
	if (lock == &CtrcShared->receipt_lock && fence_at_receipt_lock) {
		fence_at_receipt_lock = false;
		publication_fenced = true;
		publication_revision++;
	}
	return true;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	conditional_calls++;
	if (native_grouped_source_exit && native_source_exit
		&& lock == native_procarray_lock && mode == LW_EXCLUSIVE)
	{
		if (native_lock_held || held_count != 0)
			abort();
		native_group_refusals++;
		return false;
	}
	if (conditional_refuse_call != 0 && conditional_calls == conditional_refuse_call)
		return false;
	return LWLockAcquire(lock, mode);
}

void
LWLockRelease(LWLock *lock)
{
	if (external_lock_hook != NULL && external_lock_hook(lock, LW_SHARED, false))
		return;
	test_lock_leave(lock);
}

static int
test_clock_gettime(clockid_t clock_id pg_attribute_unused(), struct timespec *ts)
{
	ts->tv_sec = test_now_us / UINT64_C(1000000);
	ts->tv_nsec = (test_now_us % UINT64_C(1000000)) * 1000;
	return 0;
}

int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	abort();
}

int
cluster_conf_declared_node_count_early(void)
{
	return 4;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	*found = false;
	return calloc(1, size);
}

void
cluster_undo_cleaner_wakeup(void)
{
	wake_count++;
	if (wakeup_hook != NULL)
		wakeup_hook();
}

bool
errstart(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errcode(int code)
{
	return code;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
reset_fixture(void)
{
	MyBackendType = B_BACKEND;
	MemSet(&test_my_proc, 0, sizeof(test_my_proc));
	MyProc = &test_my_proc;
	test_top_full = InvalidFullTransactionId;
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	top_full_reads = 0;
	allow_new_service_participant = true;
	new_service_participant_calls = 0;
	external_lock_hook = NULL;
	if (held_count != 0 || allocated != 0)
		abort();
	free(CtrcShared);
	CtrcShared = NULL;
	cluster_ctrc_shmem_init();
	wake_count = 0;
	conditional_calls = conditional_refuse_call = 0;
	test_now_us = UINT64_C(1000000);
	spin_acquisitions = 0;
	sleepable_acquisitions = 0;
	allocation_failure = false;
	allocation_attempts = fail_allocation_attempt = 0;
	flush_calls = durability_calls = barrier_calls = 0;
	test_flush_lsn = 0;
	durability_hook = NULL;
	origin_durability_hook = NULL;
	barrier_hook = NULL;
	pool_upgrade_hook = NULL;
	wakeup_hook = NULL;
	paused_actor_count = 0;
	InterruptPending = 0;
	MyAuxProcType = NotAnAuxProcess;
	memset(&CtrcReadScope, 0, sizeof(CtrcReadScope));
	CtrcReadSerial = 0;
	read_scope_enter_refused = false;
	read_scope_entered = read_scope_left = 0;
	cluster_shared_config = false;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	publication_fenced = fence_at_receipt_lock = false;
	publication_revision = 1;
	publication_smgr = 1;
}

static ClusterCtrcOriginEntry *
seed_origin(unsigned index, unsigned participants)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	ClusterCtrcParticipantIdentity identity = { 0 };
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint32 grant = 0;
	unsigned node;

	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = 1;
	key.segment_id = index / TT_SLOTS_PER_SEGMENT + 1;
	key.segment_generation = 1;
	key.slot_offset = index % TT_SLOTS_PER_SEGMENT;
	key.slot_wrap = 1;
	key.xid = 7001 + index;
	key.cluster_epoch = test_cluster_epoch;
	key.system_identifier = 42;
	key.origin_boot_incarnation = 1101;
	key.formation_epoch = 23;
	key.admission_record_generation = 29;
	key.root_descriptor_incarnation = 43;
	key.root_id = 37;
	key.root_generation = 43;
	if (cluster_ctrc_origin_open_active(origin, &key, 7) != CLUSTER_CTRC_ORIGIN_OPENED)
		abort();
	/* Explicit fixture provenance, independent of the later scan reference. */
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(7, 7001 + index);
	for (node = 0; node < participants; node++) {
		identity.node_id = node;
		identity.boot_incarnation = 1101 + node;
		identity.capability_record_generation = test_capability_generation;
		identity.formation_epoch = 23;
		identity.admission_record_generation = 29;
		if (cluster_ctrc_origin_record_touched(origin, &identity, CTRC_PROOF_ACTIVE, &grant)
			!= CLUSTER_CTRC_TOUCH_RECORDED)
			abort();
	}
	if (!cluster_ctrc_origin_begin_seal_entry(origin, 41 + index))
		abort();
	return origin;
}

static ClusterCtrcTxnKeyV1
fresh_origin_key(void)
{
	ClusterCtrcOriginEntry *origin = seed_origin(0, 0);
	ClusterCtrcTxnKeyV1 key = origin->key;

	MemSet(origin, 0, sizeof(*origin));
	return key;
}

UT_TEST(test_origin_reserve_requires_published_local_top_protection)
{
	unsigned fault;

	for (fault = 0; fault < 8; fault++) {
		ClusterCtrcTxnKeyV1 key;
		ClusterCtrcOriginReservation reservation;
		ClusterCtrcSharedHeader before;

		reset_fixture();
		key = fresh_origin_key();
		test_my_proc.xid = key.xid;
		test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
		pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
								U64FromFullTransactionId(test_top_full));
		switch (fault) {
		case 0:
			MyProc = NULL;
			break;
		case 1:
			test_my_proc.xid = InvalidTransactionId;
			break;
		case 2:
			test_top_full = InvalidFullTransactionId;
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			break;
		case 3:
			test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid + 1);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			break;
		case 4:
			test_my_proc.xid = key.xid + 1;
			break;
		case 5:
			key.xid = FrozenTransactionId;
			test_my_proc.xid = key.xid;
			test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			break;
		case 6:
			key.xid = BootstrapTransactionId;
			test_my_proc.xid = key.xid;
			test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			break;
		case 7:
			key.origin_node_id = 1;
			key.owner_instance = 2;
			key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			break;
		}
		before = *CtrcShared;
		MemSet(&reservation, 0xa5, sizeof(reservation));
		UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
					 CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&reservation, sizeof(reservation)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
		UT_ASSERT(memcmp(CtrcShared, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(sleepable_acquisitions, 0);
		UT_ASSERT_EQ(spin_acquisitions, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_origin_reserve_requires_same_published_original_fullxid)
{
	for (unsigned fault = 0; fault < 5; fault++)
	{
		ClusterCtrcTxnKeyV1 key;
		ClusterCtrcOriginReservation reservation;
		ClusterCtrcOriginEntry origin_before;
		ClusterCtrcSharedHeader header_before;
		uint64 published;

		reset_fixture();
		key = fresh_origin_key();
		test_my_proc.xid = key.xid;
		test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
		published = U64FromFullTransactionId(test_top_full);
		switch (fault)
		{
		case 0: published = 0; break;
		case 1: published = (UINT64CONST(6) << 32) + key.xid; break;
		case 2: published++; break;
		case 3: published = UINT64_MAX; break;
		case 4:
			key.xid = test_my_proc.xid = UINT32_MAX;
			test_top_full = FullTransactionIdFromU64(UINT64_MAX);
			published = UINT64_MAX;
			break;
		}
		pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic, published);
		origin_before = ctrc_origin_entries()[0];
		header_before = *CtrcShared;
		MemSet(&reservation, 0xa5, sizeof(reservation));
		UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
					 CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&reservation, sizeof(reservation)));
		UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &origin_before, sizeof(origin_before)) == 0);
		UT_ASSERT(memcmp(CtrcShared, &header_before, sizeof(header_before)) == 0);
		UT_ASSERT_EQ(top_full_reads, 1);
		UT_ASSERT_EQ(sleepable_acquisitions, 0);
		UT_ASSERT_EQ(spin_acquisitions, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_origin_reserve_protected_pending_can_cancel)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcOriginEntry *origin;

	reset_fixture();
	key = fresh_origin_key();
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	origin = &ctrc_origin_entries()[0];
	UT_ASSERT_EQ(top_full_reads, 1);
	UT_ASSERT_EQ(sleepable_acquisitions, 1);
	UT_ASSERT_EQ(spin_acquisitions, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(reservation.valid);
	UT_ASSERT_EQ(reservation.kind, CTRC_ORIGIN_RESERVATION_PENDING_OWNED);
	UT_ASSERT(FullTransactionIdEquals(reservation.original_full_xid, test_top_full));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT(origin->reservation_generation != 0);
	UT_ASSERT(memcmp(&origin->key, &key, sizeof(key)) == 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(origin->original_full_xid),
				 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 7001)));
	UT_ASSERT(cluster_ctrc_origin_cancel_pre_bind(&reservation));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
}

UT_TEST(test_origin_reserve_open_replay_requires_live_original_protection)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation, duplicate;
	ClusterCtrcOriginEntry before;
	ClusterCtrcSharedHeader header_before;
	uint32 grant;

	reset_fixture();
	key = fresh_origin_key();
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
	UT_ASSERT(grant != 0);
	before = ctrc_origin_entries()[0];
	header_before = *CtrcShared;
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &duplicate),
				 CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN);
	UT_ASSERT_EQ(duplicate.reservation_generation, reservation.reservation_generation);
	UT_ASSERT(FullTransactionIdEquals(duplicate.original_full_xid, reservation.original_full_xid));
	UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
	UT_ASSERT(memcmp(CtrcShared, &header_before, sizeof(header_before)) == 0);
	test_my_proc.xid = InvalidTransactionId;
	MemSet(&duplicate, 0xa5, sizeof(duplicate));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &duplicate),
				 CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&duplicate, sizeof(duplicate)));
	UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
	UT_ASSERT(memcmp(CtrcShared, &header_before, sizeof(header_before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_origin_open_replay_refuses_missing_or_conflicting_original_fullxid)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcTxnKeyV1 key;
		ClusterCtrcOriginReservation reservation, replay;
		ClusterCtrcOriginEntry before;
		uint64 refusals, generation;
		uint32 grant;

		reset_fixture();
		key = fresh_origin_key();
		test_my_proc.xid = key.xid;
		test_top_full = FullTransactionIdFromEpochAndXid(7, 7001);
		pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
								U64FromFullTransactionId(test_top_full));
		UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
					 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
		switch (fault) {
		case 0:
			test_top_full = FullTransactionIdFromEpochAndXid(8, 7001);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			break;
		case 1:
			ctrc_origin_entries()[0].original_full_xid = InvalidFullTransactionId;
			break;
		case 2:
			ctrc_origin_entries()[0].original_full_xid =
				FullTransactionIdFromEpochAndXid(7, FrozenTransactionId);
			break;
		case 3:
			ctrc_origin_entries()[0].original_full_xid =
				FullTransactionIdFromEpochAndXid(7, 7002);
			break;
		}
		before = ctrc_origin_entries()[0];
		refusals = CtrcShared->full_refusal_count;
		generation = CtrcShared->global_reservation_generation;
		MemSet(&replay, 0xa5, sizeof(replay));
		UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &replay),
					 CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&replay, sizeof(replay)));
		UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(CtrcShared->full_refusal_count, refusals + 1);
		UT_ASSERT_EQ(CtrcShared->global_reservation_generation, generation);
		UT_ASSERT_EQ(held_count, 0);
	}
}

/* The producer's original identity must survive reserve -> durable BIND ->
 * OPEN unchanged, even when the raw XID and physical key still match. */
UT_TEST(test_origin_open_reserved_refuses_original_identity_drift)
{
	for (unsigned opened = 0; opened < 2; opened++) {
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterCtrcTxnKeyV1 key;
			ClusterCtrcOriginReservation reservation;
			ClusterCtrcOriginEntry before;
			uint32 grant, next_grant;

			reset_fixture();
			key = fresh_origin_key();
			test_my_proc.xid = key.xid;
			test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
						 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
			if (opened)
				UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
			switch (fault) {
			case 0:
				ctrc_origin_entries()[0].original_full_xid = InvalidFullTransactionId;
				break;
			case 1:
				ctrc_origin_entries()[0].original_full_xid =
					FullTransactionIdFromEpochAndXid(7, FrozenTransactionId);
				break;
			case 2:
				ctrc_origin_entries()[0].original_full_xid =
					FullTransactionIdFromEpochAndXid(7, key.xid + 1);
				break;
			case 3:
				ctrc_origin_entries()[0].original_full_xid =
					FullTransactionIdFromEpochAndXid(8, key.xid);
				break;
			}
			before = ctrc_origin_entries()[0];
			next_grant = CtrcShared->global_grant_generation;
			grant = UINT32_MAX;
			UT_ASSERT(!cluster_ctrc_origin_open_reserved(&reservation, &grant));
			UT_ASSERT_EQ(grant, 0);
			UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
			UT_ASSERT_EQ(CtrcShared->global_grant_generation, next_grant);
			UT_ASSERT_EQ(top_full_reads, 1);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

UT_TEST(test_origin_open_reserved_refuses_invalid_handle_identity)
{
	for (unsigned opened = 0; opened < 2; opened++) {
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterCtrcTxnKeyV1 key;
			ClusterCtrcOriginReservation reservation;
			ClusterCtrcOriginEntry before;
			uint32 grant, next_grant;

			reset_fixture();
			key = fresh_origin_key();
			test_my_proc.xid = key.xid;
			test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
			pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
									U64FromFullTransactionId(test_top_full));
			UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
						 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
			if (opened)
				UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
			before = ctrc_origin_entries()[0];
			next_grant = CtrcShared->global_grant_generation;
			switch (fault) {
			case 0:
				reservation.original_full_xid = InvalidFullTransactionId;
				break;
			case 1:
				reservation.original_full_xid =
					FullTransactionIdFromEpochAndXid(7, FrozenTransactionId);
				break;
			case 2:
				reservation.original_full_xid =
					FullTransactionIdFromEpochAndXid(7, key.xid + 1);
				break;
			case 3:
				reservation.original_full_xid =
					FullTransactionIdFromEpochAndXid(8, key.xid);
				break;
			}
			grant = UINT32_MAX;
			UT_ASSERT(!cluster_ctrc_origin_open_reserved(&reservation, &grant));
			UT_ASSERT_EQ(grant, 0);
			UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
			UT_ASSERT_EQ(CtrcShared->global_grant_generation, next_grant);
			UT_ASSERT_EQ(top_full_reads, 1);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

UT_TEST(test_origin_reserved_cleanup_refuses_original_identity_drift)
{
	for (unsigned post_bind = 0; post_bind < 2; post_bind++) {
		ClusterCtrcTxnKeyV1 key;
		ClusterCtrcOriginReservation reservation;
		ClusterCtrcOriginEntry before;

		reset_fixture();
		key = fresh_origin_key();
		test_my_proc.xid = key.xid;
		test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
		pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
								U64FromFullTransactionId(test_top_full));
		UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
					 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		ctrc_origin_entries()[0].original_full_xid =
			FullTransactionIdFromEpochAndXid(8, key.xid);
		before = ctrc_origin_entries()[0];
		if (post_bind)
			UT_ASSERT(!cluster_ctrc_origin_block_post_bind(&reservation));
		else
			UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind(&reservation));
		UT_ASSERT(memcmp(&ctrc_origin_entries()[0], &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(top_full_reads, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_origin_open_reserved_keeps_protected_original_identity)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation, duplicate;
	FullTransactionId original;
	uint32 grant, replay_grant;

	reset_fixture();
	key = fresh_origin_key();
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	original = test_top_full;
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	/* OPEN consumes the retained producer identity, never a fresh raw-XID
	 * widening or a second PGPROC sample. */
	test_top_full = FullTransactionIdFromEpochAndXid(8, key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(top_full_reads, 1);
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[0].original_full_xid, original));
	test_top_full = original;
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &duplicate),
				 CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN);
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&duplicate, &replay_grant));
	UT_ASSERT_EQ(replay_grant, grant);
	UT_ASSERT_EQ(top_full_reads, 2);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_new_seals_do_not_starve_old_pending)
{
	ClusterCtrcCloseDispatch dispatch;
	unsigned i;
	unsigned revisited = 0;

	reset_fixture();
	for (i = 0; i < 256; i++) {
		ClusterCtrcOriginEntry *origin;

		seed_origin(i, 1);
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
		if (i != 0 && dispatch.key.xid == 7001)
			revisited++;
		origin = &ctrc_origin_entries()[dispatch.key.xid - 7001];
		UT_ASSERT(cluster_ctrc_origin_note_close_reply_entry(origin, 0, dispatch.request_id,
															 CTRC_SEAL_REPLY_PENDING_DRAIN));
	}
	UT_ASSERT(revisited > 0);
}

UT_TEST(test_pending_participant_does_not_starve_peer)
{
	ClusterCtrcCloseDispatch dispatch;
	unsigned seen = 0;
	unsigned i;

	reset_fixture();
	seed_origin(0, 2);
	for (i = 0; i < 4; i++) {
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
		seen |= 1U << dispatch.participant.node_id;
		UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(
			dispatch.request_id, dispatch.participant.node_id, CTRC_SEAL_REPLY_PENDING_DRAIN));
	}
	UT_ASSERT_EQ(seen, 3);
}

UT_TEST(test_duplicate_pending_is_accepted_without_wakeup)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;
	unsigned wakes;
	uint64 progress;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 99));
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(99, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	before = *origin;
	wakes = wake_count;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(wakes > 0);
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(99, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_DUPLICATE), 1);
}

UT_TEST(test_uncorrelated_reply_cannot_progress)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 99));
	before = *origin;
	UT_ASSERT(!cluster_ctrc_origin_note_close_reply_shared(100, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(wake_count, 0);
}

UT_TEST(test_batch_is_bounded_oldest_first_and_fair)
{
	bool seen[160] = { false };
	ClusterCtrcCloseDispatch dispatch;
	unsigned pass;
	unsigned i;

	reset_fixture();
	for (i = 0; i < lengthof(seen); i++)
		seed_origin(i, 1);
	CtrcBatch.active = true;
	for (pass = 0; pass < 5; pass++) {
		bool in_batch[160] = { false };
		unsigned count = 0;

		ctrc_dispatch_batch_collect();
		UT_ASSERT_EQ(CtrcBatch.dispatch_count, 64);
		for (i = 0; i < 32; i++)
			UT_ASSERT_EQ(CtrcBatch.dispatch_index[i], i);
		while (cluster_ctrc_origin_next_close_dispatch_shared(&dispatch)) {
			unsigned index = dispatch.key.xid - 7001;

			UT_ASSERT(index < lengthof(seen));
			UT_ASSERT(!in_batch[index]);
			in_batch[index] = true;
			seen[index] = true;
			count++;
			/* Deliberately never ACK the oldest or any peer. */
			UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(dispatch.request_id, 0,
																  CTRC_SEAL_REPLY_PENDING_DRAIN));
		}
		UT_ASSERT_EQ(count, 64);
		UT_ASSERT(!cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	}
	for (i = 0; i < lengthof(seen); i++)
		UT_ASSERT(seen[i]);
	CtrcBatch.active = false;
}

UT_TEST(test_batch_hint_rechecks_current_identity)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcTxnKeyV1 previous;

	reset_fixture();
	origin = seed_origin(0, 1);
	previous = origin->key;
	CtrcBatch.active = true;
	ctrc_dispatch_batch_collect();
	/* Controlled slot replacement between selection and arm. No proof is
	 * carried by the batch; arm must build only the new current identity. */
	MemSet(origin, 0, sizeof(*origin));
	origin = seed_origin(0, 1);
	origin->key.slot_wrap++;
	origin->key.xid++;
	origin->seal_generation++;
	UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	UT_ASSERT_EQ(dispatch.key.xid, origin->key.xid);
	UT_ASSERT_EQ(dispatch.key.slot_wrap, origin->key.slot_wrap);
	UT_ASSERT(memcmp(&dispatch.key, &previous, sizeof(previous)) != 0);
	UT_ASSERT(!cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	CtrcBatch.active = false;
}

UT_TEST(test_scan_budget_prevents_same_pending_item_revisit)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry *origin;
	unsigned count = 0;

	reset_fixture();
	origin = seed_origin(0, 0);
	/* Real OPEN entry: the terminal sample may retain it. The selector
	 * must not rescan the same entry during this bounded pass. */
	origin->state = CTRC_ORIGIN_OPEN;
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_OPEN] = CtrcShared->origin_key_entries;
	while (cluster_ctrc_origin_next_open_shared(&key)) {
		count++;
		UT_ASSERT(count <= 1);
		if (count > 1)
			break;
	}
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_OPEN], 0);
	UT_ASSERT(!cluster_ctrc_origin_next_open_shared(&key));
	CtrcBatch.active = false;
}

UT_TEST(test_observed_age_resets_without_releasing_authority)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;

	reset_fixture();
	origin = seed_origin(0, 1);
	before = *origin;
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
	test_now_us += UINT64_C(9000000);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_OBSERVATION_AGE_MS), 9000);
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 9000);
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), 0);
	origin->seal_generation++;
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
	MemSet(origin, 0, sizeof(*origin));
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
}

UT_TEST(test_participant_duplicate_pending_does_not_wake)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 index;
	uint64 progress;
	unsigned wakes;
	uint16 reason;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
	participant = &ctrc_participant_entries()[index];
	UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &origin->key, origin->grant_generation,
											   &origin->touched[0]),
				 CLUSTER_CTRC_PARTICIPANT_OPENED);
	/* A single prepared, not-yet-applied publication holds the close. This
	 * fixture exercises the reply contract, not receipt/disk durability. */
	participant->receipt_count = 1;
	participant->prepared_count = 1;
	participant->last_key_sequence = 1;
	participant->next_key_sequence = 2;
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &origin->key, &origin->touched[0], origin->grant_generation,
					 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	wakes = wake_count;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(wakes > 0);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &origin->key, &origin->touched[0], origin->grant_generation,
					 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(participant->prepared_count, 1);
	UT_ASSERT_EQ(participant->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
}

UT_TEST(test_real_ack_and_certificate_consumers_are_idempotent)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginCertificateSnapshot snapshot;
	uint64 progress;
	unsigned wakes;
	unsigned node;

	reset_fixture();
	origin = seed_origin(0, 2);
	for (node = 0; node < 2; node++) {
		ClusterCtrcParticipantEntry participant = { 0 };
		ClusterCtrcLocalReleaseAckV1 summary = { 0 };
		ClusterCtrcLocalReleaseAckV1 ack;
		uint16 reason;

		UT_ASSERT_EQ(cluster_ctrc_participant_open(&participant, &origin->key,
												   origin->grant_generation,
												   &origin->touched[node]),
					 CLUSTER_CTRC_PARTICIPANT_OPENED);
		UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, node, 100 + node));
		/* Actual zero-range ACK encoder, not an ACK-shaped literal. */
		UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
						 &participant, &summary, &origin->key, &origin->touched[node],
						 origin->grant_generation, origin->seal_generation,
						 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
		UT_ASSERT(cluster_ctrc_origin_ack_land_shared(100 + node, &ack));
		progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
		wakes = wake_count;
		UT_ASSERT(cluster_ctrc_origin_ack_land_shared(100 + node, &ack));
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
		UT_ASSERT_EQ(wake_count, wakes);
		if (node == 0)
			UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	}
	UT_ASSERT(!ctrc_origin_dispatchable_locked(origin));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	/* This call tests the in-memory edge AFTER the separately tested durable
	 * publication point. It does not simulate a successful disk write. */
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(origin, &snapshot));
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 0, 200));
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	wakes = wake_count;
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 1, 201));
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		201, 1, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
	UT_ASSERT(!cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
}

UT_TEST(test_receipt_and_ack_scans_do_not_retry_retained_entries)
{
	ClusterCtrcParticipantEntry participant_copy;
	ClusterCtrcReceipt receipt_copy;
	uint64 participant_index;
	uint64 receipt_index;
	bool seen[2] = { false };
	unsigned count = 0;
	unsigned i;

	reset_fixture();
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = seed_origin(i, 1);
		ClusterCtrcParticipantEntry *participant;
		ClusterCtrcReceipt *receipt = &ctrc_receipt_entries()[i];

		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
		participant = &ctrc_participant_entries()[participant_index];
		UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &origin->key,
												   origin->grant_generation, &origin->touched[0]),
					 CLUSTER_CTRC_PARTICIPANT_OPENED);
		participant->receipt_count = 1;
		participant->applied_count = 1;
		participant->last_key_sequence = 1;
		participant->next_key_sequence = 2;
		UT_ASSERT_EQ(cluster_ctrc_participant_close(participant, &origin->touched[0],
													origin->grant_generation,
													origin->seal_generation),
					 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
		receipt->key = origin->key;
		receipt->publication.grant_generation = origin->grant_generation;
		pg_atomic_write_u32((pg_atomic_uint32 *)&receipt->state, CTRC_RECEIPT_APPLIED);
	}
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_RECEIPT] = CtrcShared->receipt_entries;
	while (ctrc_cleaner_next_applied_receipt(&participant_copy, &receipt_copy, &participant_index,
											 &receipt_index)) {
		UT_ASSERT(receipt_index < 2);
		UT_ASSERT(!seen[receipt_index]);
		seen[receipt_index] = true;
		if (++count > 2)
			break;
		/* Leave APPLIED unchanged to model a failed physical clean. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_RECEIPT], 0);
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[i];
		ClusterCtrcParticipantEntry *participant;

		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
		participant = &ctrc_participant_entries()[participant_index];
		participant->applied_count = 0;
		participant->cleaned_count = 1;
	}
	count = 0;
	MemSet(seen, 0, sizeof(seen));
	CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
	while (ctrc_participant_find_ack_ready(&participant_copy, &participant_index)) {
		unsigned index = participant_copy.key.xid - 7001;

		UT_ASSERT(index < 2);
		UT_ASSERT(!seen[index]);
		seen[index] = true;
		if (++count > 2)
			break;
		/* The ACK provider may retain this snapshot on durability failure. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_ACK], 0);
	CtrcBatch.active = false;
}

UT_TEST(test_certificate_scan_bounds_unpublished_candidates)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	bool seen[2] = { false };
	unsigned count = 0;
	unsigned i;

	reset_fixture();
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = seed_origin(i, 0);

		UT_ASSERT(!ctrc_origin_dispatchable_locked(origin));
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	}
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE] = CtrcShared->origin_key_entries;
	while (ctrc_origin_next_certificate_snapshot_shared(&snapshot)) {
		UT_ASSERT(snapshot.origin_index < 2);
		UT_ASSERT(!seen[snapshot.origin_index]);
		seen[snapshot.origin_index] = true;
		if (++count > 2)
			break;
		/* No certificate is published; repeated selection is still bounded. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE], 0);
	UT_ASSERT_EQ(ctrc_origin_entries()[0].state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT_EQ(ctrc_origin_entries()[1].state, CTRC_ORIGIN_CERTIFYING);
	CtrcBatch.active = false;
}

/* Build one real pending receipt, cancel it without publishing a page
 * reference, then close and encode its actual nonempty ACK. The fixture
 * owns no disk or page I/O and must not claim durability of an applied row. */
static void
fixture_receipt_inputs(ClusterCtrcOriginEntry *origin, uint64 operation_id,
	ClusterCtrcPublicationIdV1 *publication_out, ClusterCtrcTargetV1 *target_out)
{
	ClusterCtrcPublicationIdV1 publication = { 0 };
	ClusterCtrcTargetV1 target = { 0 };

	publication.requester_node_id = 0;
	publication.requester_boot_incarnation = origin->touched[0].boot_incarnation;
	publication.capability_record_generation = origin->touched[0].capability_record_generation;
	publication.requester_backend_id = 11;
	publication.wire_request_id = 101;
	publication.operation_id = operation_id;
	publication.attempt_generation = 1;
	publication.member_ordinal = UINT16_MAX;
	publication.reference_kind = CTRC_REF_HEAP_ITL_UBA;
	publication.target_kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	publication.grant_generation = origin->grant_generation;
	target.kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	target.spc_oid = 1663;
	target.db_oid = 5;
	target.rel_number = 9001;
	target.block_number = 44;
	target.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	target.publication_own_generation = 17;
	target.publication_acquisition_epoch = 19;
	target.relation_persistence = 'p';
	target.needs_wal = true;
	target.page_operation_kind = 1;
	*publication_out = publication;
	*target_out = target;
}

static ClusterCtrcPrepareResult
prepare_fixture_receipt(ClusterCtrcOriginEntry *origin, uint64 operation_id,
						ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;

	fixture_receipt_inputs(origin, operation_id, &publication, &target);
	return cluster_ctrc_receipt_prepare_shared(
		&origin->key, &origin->touched[0], origin->grant_generation, &publication, &target, handle);
}

static ClusterCtrcPrepareResult
prepare_fixture_receipt_observed(ClusterCtrcOriginEntry *origin, uint64 operation_id,
	ClusterCtrcReceiptHandle *handle, ClusterCtrcPreparePath *path)
{
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;

	fixture_receipt_inputs(origin, operation_id, &publication, &target);
	return cluster_ctrc_receipt_prepare_shared_observed(&origin->key, &origin->touched[0],
		origin->grant_generation, &publication, &target, handle, path);
}

UT_TEST(test_prepare_path_namespace_precedes_input_and_resets_output)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcPreparePath path = CLUSTER_CTRC_PREPARE_PATH_TABLE;
	uint64 journal;

	reset_fixture(); origin = seed_origin(0, 1);
	fixture_receipt_inputs(origin, 201, &publication, &target);
	journal = CtrcShared->global_journal_generation;
	target.rel_number = InvalidRelFileNumber;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared_observed(&origin->key, NULL,
		origin->grant_generation, &publication, &target, &handle, &path),
		CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_NAMESPACE);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared_observed(&origin->key, &origin->touched[0],
		origin->grant_generation, &publication, NULL, &handle, &path),
		CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_INPUT);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, journal);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_prepare_path_input_precedes_journal_exhaustion)
{
	const uint64 unavailable[] = {0, UINT64_MAX};
	const ClusterCtrcPreparePath expected[] = {
		CLUSTER_CTRC_PREPARE_PATH_INPUT, CLUSTER_CTRC_PREPARE_PATH_JOURNAL
	};
	for (unsigned i = 0; i < lengthof(unavailable); i++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcPreparePath path = CLUSTER_CTRC_PREPARE_PATH_TABLE;
		ClusterCtrcParticipantEntry before;
		uint64 pi;

		reset_fixture(); origin = seed_origin(0, 1);
		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &pi));
		before = ctrc_participant_entries()[pi];
		CtrcShared->global_journal_generation = unavailable[i];
		/* Zero invalidates the shared header before journal allocation;
		 * UINT64_MAX is a valid header with an exhausted journal. */
		UT_ASSERT_EQ(cluster_ctrc_shmem_ready(), unavailable[i] != 0);
		UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 201, &handle, &path),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(path, expected[i]);
		UT_ASSERT(!handle.valid);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_participant_entries()[pi], sizeof(before)), 0);
		UT_ASSERT_EQ(ctrc_participant_receipt_heads()[pi], 0);
		UT_ASSERT_EQ(CtrcShared->global_journal_generation, unavailable[i]);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_prepare_path_chain_precedes_locator_and_keeps_blocked_state)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcPreparePath path;
		uint64 pi;

		reset_fixture(); origin = seed_origin(0, 1);
		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &pi));
		if (kind != 1)
			ctrc_participant_receipt_heads()[pi] = 1;
		if (kind != 0)
			for (uint64 i = 0; i < CtrcShared->receipt_entries; i++)
				ctrc_receipt_links()[i].next_plus_one = 1;
		UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 201, &handle, &path),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(path, kind == 1 ? CLUSTER_CTRC_PREPARE_PATH_LOCATOR
			: CLUSTER_CTRC_PREPARE_PATH_CHAIN);
		UT_ASSERT(!handle.valid);
		UT_ASSERT_EQ(ctrc_participant_entries()[pi].state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT(ctrc_bytes_zero(ctrc_receipt_entries(),
			CtrcShared->receipt_entries * sizeof(ClusterCtrcReceipt)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_prepare_path_object_existing_keeps_refusal_race_and_duplicate)
{
	for (unsigned race = 0; race < 2; race++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcPreparePath path;
		ClusterCtrcParticipantEntry before;
		uint64 pi;

		reset_fixture(); origin = seed_origin(0, 1);
		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &pi));
		before = ctrc_participant_entries()[pi];
		cluster_shared_config = true;
		publication_fenced = race == 0;
		fence_at_receipt_lock = race != 0;
		UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 201, &handle, &path),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_OBJECT_EXISTING);
		UT_ASSERT(!handle.valid);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_participant_entries()[pi], sizeof(before)), 0);
		UT_ASSERT_EQ(held_count, 0);
	}
	{
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle first, duplicate;
		ClusterCtrcPreparePath path;
		ClusterCtrcReceipt before;

		reset_fixture(); origin = seed_origin(0, 1); cluster_shared_config = true;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &first), CLUSTER_CTRC_PREPARE_READY);
		before = *first.receipt; publication_fenced = true; publication_revision++;
		UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 201, &duplicate, &path),
			CLUSTER_CTRC_PREPARE_DUPLICATE);
		UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_OBJECT_EXISTING);
		UT_ASSERT_EQ(duplicate.receipt_index, first.receipt_index);
		UT_ASSERT_EQ(memcmp(&before, first.receipt, sizeof(before)), 0);
		UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&first));
	}
}

UT_TEST(test_prepare_path_table_ready_duplicate_and_refusals)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, handle;
	ClusterCtrcPreparePath path;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;

	reset_fixture(); origin = seed_origin(0, 1);
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 1, &first, &path), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_TABLE);
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 1, &handle, &path), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_TABLE);
	UT_ASSERT_EQ(handle.receipt_index, first.receipt_index);
	for (uint64 i = 1; i < CtrcShared->receipt_entries; i++) {
		UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, i + 1, &handle, &path), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_TABLE);
		UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&handle));
	}
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, CtrcShared->receipt_entries + 1,
		&handle, &path), CLUSTER_CTRC_PREPARE_CAPACITY);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_TABLE_CAPACITY);
	UT_ASSERT(!handle.valid);
	fixture_receipt_inputs(origin, 1, &publication, &target);
	publication.attempt_generation = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared_observed(&origin->key, &origin->touched[0],
		origin->grant_generation, &publication, &target, &handle, &path), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_TABLE_PUBLICATION);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(held_count, 0);
}

typedef struct TablePrepareFixture {
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceipt receipts[2];
	uint8 probe_states[2];
	uint64 index;
	Size probes;
	ClusterCtrcPreparePath path;
} TablePrepareFixture;

static void
fixture_table_prepare(TablePrepareFixture *f)
{
	reset_fixture();
	MemSet(f, 0, sizeof(*f));
	f->origin = seed_origin(0, 1);
	fixture_receipt_inputs(f->origin, 201, &f->publication, &f->target);
	f->index = 42;
	f->probes = 42;
	f->path = CLUSTER_CTRC_PREPARE_PATH_UNKNOWN;
}

static ClusterCtrcPrepareResult
prepare_table_fixture(TablePrepareFixture *f, Size count)
{
	return ctrc_receipt_prepare_table_observed(&f->participant, &f->origin->key,
		&f->origin->touched[0], f->origin->grant_generation, &f->publication, &f->target,
		f->receipts, f->probe_states, count, 500, &f->index, &f->probes, &f->path);
}

UT_TEST(test_table_path_input_short_circuits_participant_and_publication)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		TablePrepareFixture f;
		ClusterCtrcParticipantEntry before;

		fixture_table_prepare(&f);
		f.participant.state = CTRC_PARTICIPANT_CLOSED_DRAINING;
		f.publication.attempt_generation = 0;
		before = f.participant;
		UT_ASSERT_EQ(ctrc_receipt_prepare_table_observed(kind == 0 ? NULL : &f.participant,
			&f.origin->key, &f.origin->touched[0], f.origin->grant_generation,
			&f.publication, &f.target, f.receipts, f.probe_states, kind == 1 ? 0 : 1,
			kind == 2 ? 0 : 500, kind == 3 ? NULL : &f.index, &f.probes, &f.path),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE_INPUT);
		UT_ASSERT_EQ(memcmp(&before, &f.participant, sizeof(before)), 0);
		if (kind != 3)
			UT_ASSERT_EQ(f.index, UINT64_MAX);
		UT_ASSERT_EQ(f.probes, 0);
		UT_ASSERT(ctrc_bytes_zero(f.receipts, sizeof(f.receipts)));
	}
}

UT_TEST(test_table_path_participant_refusal_precedes_publication)
{
	TablePrepareFixture f;

	fixture_table_prepare(&f);
	f.participant.state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	f.publication.attempt_generation = 0;
	UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE_PARTICIPANT);
	UT_ASSERT_EQ(f.participant.state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(f.participant.next_key_sequence, 0);
	UT_ASSERT_EQ(f.index, UINT64_MAX);
	UT_ASSERT_EQ(f.probes, 0);
	UT_ASSERT(ctrc_bytes_zero(f.receipts, sizeof(f.receipts)));
}

UT_TEST(test_table_path_publication_refusal_keeps_original_participant_open)
{
	TablePrepareFixture f;

	fixture_table_prepare(&f);
	f.publication.attempt_generation = 0;
	UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE_PUBLICATION);
	UT_ASSERT_EQ(f.participant.state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT_EQ(f.participant.next_key_sequence, 1);
	UT_ASSERT_EQ(f.participant.receipt_count, 0);
	UT_ASSERT_EQ(f.index, UINT64_MAX);
	UT_ASSERT_EQ(f.probes, 0);
	UT_ASSERT(ctrc_bytes_zero(f.receipts, sizeof(f.receipts)));
}

UT_TEST(test_table_path_dirty_or_malformed_probe_keeps_bytes_and_blocks)
{
	const uint8 states[] = {CTRC_RECEIPT_PROBE_EMPTY, CTRC_RECEIPT_PROBE_TOMBSTONE,
		255, CTRC_RECEIPT_PROBE_OCCUPIED, CTRC_RECEIPT_PROBE_OCCUPIED};
	const ClusterCtrcPreparePath expected[] = {CLUSTER_CTRC_PREPARE_PATH_TABLE_EMPTY_BYTES,
		CLUSTER_CTRC_PREPARE_PATH_TABLE_TOMBSTONE_BYTES,
		CLUSTER_CTRC_PREPARE_PATH_TABLE_OCCUPIED, CLUSTER_CTRC_PREPARE_PATH_TABLE_OCCUPIED,
		CLUSTER_CTRC_PREPARE_PATH_TABLE_OCCUPIED};

	for (unsigned kind = 0; kind < lengthof(states); kind++) {
		TablePrepareFixture f;
		ClusterCtrcReceipt before[2];

		fixture_table_prepare(&f);
		f.probe_states[0] = states[kind];
		if (kind != 4)
			f.receipts[0].publication.operation_id = 99;
		memcpy(before, f.receipts, sizeof(before));
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(f.path, expected[kind]);
		UT_ASSERT_EQ(f.participant.state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(f.participant.next_key_sequence, 1);
		UT_ASSERT_EQ(f.participant.receipt_count, 0);
		UT_ASSERT_EQ(f.participant.prepared_count, 0);
		UT_ASSERT_EQ(f.index, UINT64_MAX);
		UT_ASSERT_EQ(f.probes, 1);
		UT_ASSERT_EQ(memcmp(before, f.receipts, sizeof(before)), 0);
		UT_ASSERT_EQ(f.probe_states[0], states[kind]);
	}
}

UT_TEST(test_table_path_duplicate_conflict_does_not_publish_again)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		TablePrepareFixture f;
		ClusterCtrcReceipt before;

		fixture_table_prepare(&f);
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE);
		if (kind == 1)
			f.target.block_number++;
		if (kind == 2)
			f.receipts[0].publication.journal_sequence = 0;
		if (kind == 3)
			f.receipts[0].publication.key_sequence = 0;
		before = f.receipts[0];
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), kind == 0 ? CLUSTER_CTRC_PREPARE_DUPLICATE
			: CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(f.path, kind == 0 ? CLUSTER_CTRC_PREPARE_PATH_TABLE
			: CLUSTER_CTRC_PREPARE_PATH_TABLE_DUPLICATE_CONFLICT);
		UT_ASSERT_EQ(f.participant.state, kind == 0 ? CTRC_PARTICIPANT_OPEN : CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(f.participant.next_key_sequence, 2);
		UT_ASSERT_EQ(f.participant.receipt_count, 1);
		UT_ASSERT_EQ(f.participant.prepared_count, 1);
		UT_ASSERT_EQ(f.index, kind == 0 ? 0 : UINT64_MAX);
		UT_ASSERT_EQ(f.probes, 1);
		UT_ASSERT_EQ(memcmp(&before, &f.receipts[0], sizeof(before)), 0);
	}
}

UT_TEST(test_table_path_sequence_capacity_and_tombstone_probe_order)
{
	const uint64 next[] = {0, UINT64_MAX, 2};
	for (unsigned kind = 0; kind < lengthof(next); kind++) {
		TablePrepareFixture f;

		fixture_table_prepare(&f);
		UT_ASSERT_EQ(cluster_ctrc_participant_open(&f.participant, &f.origin->key,
			f.origin->grant_generation, &f.origin->touched[0]), CLUSTER_CTRC_PARTICIPANT_OPENED);
		f.participant.next_key_sequence = next[kind];
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE_SLOT_PREPARE);
		UT_ASSERT_EQ(f.participant.state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(f.participant.next_key_sequence, next[kind]);
		UT_ASSERT_EQ(f.participant.receipt_count, 0);
		UT_ASSERT_EQ(f.probes, 1);
		UT_ASSERT_EQ(f.index, UINT64_MAX);
		UT_ASSERT_EQ(f.probe_states[0], CTRC_RECEIPT_PROBE_EMPTY);
		UT_ASSERT(ctrc_bytes_zero(f.receipts, sizeof(f.receipts)));
	}
	{
		TablePrepareFixture f;
		ClusterCtrcReceipt before;

		fixture_table_prepare(&f);
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_READY);
		before = f.receipts[0];
		f.publication.operation_id++;
		UT_ASSERT_EQ(prepare_table_fixture(&f, 1), CLUSTER_CTRC_PREPARE_CAPACITY);
		UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE_CAPACITY);
		UT_ASSERT_EQ(f.participant.state, CTRC_PARTICIPANT_OPEN);
		UT_ASSERT_EQ(f.participant.receipt_count, 1);
		UT_ASSERT_EQ(f.probes, 1);
		UT_ASSERT_EQ(f.index, UINT64_MAX);
		UT_ASSERT_EQ(memcmp(&before, &f.receipts[0], sizeof(before)), 0);
	}
	{
		TablePrepareFixture f;
		uint64 first;

		fixture_table_prepare(&f);
		first = ctrc_receipt_probe_hash(&f.origin->key, &f.publication) % 2;
		f.probe_states[first] = CTRC_RECEIPT_PROBE_TOMBSTONE;
		UT_ASSERT_EQ(prepare_table_fixture(&f, 2), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT_EQ(f.path, CLUSTER_CTRC_PREPARE_PATH_TABLE);
		UT_ASSERT_EQ(f.index, first);
		UT_ASSERT_EQ(f.probes, 2);
		UT_ASSERT_EQ(f.probe_states[first], CTRC_RECEIPT_PROBE_OCCUPIED);
		UT_ASSERT_EQ(f.probe_states[1 - first], CTRC_RECEIPT_PROBE_EMPTY);
		UT_ASSERT_EQ(f.receipts[first].publication.journal_sequence, 500);
	}
}

UT_TEST(test_table_path_null_observer_preserves_original_table_effects)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		TablePrepareFixture f, g;
		ClusterCtrcPrepareResult old_result, observed_result;
		uint64 journal;

		fixture_table_prepare(&f);
		if (kind == 1)
			f.publication.attempt_generation = 0;
		if (kind == 2)
			f.receipts[0].publication.operation_id = 99;
		g = f;
		journal = CtrcShared->global_journal_generation;
		old_result = cluster_ctrc_receipt_prepare_table_locked(&f.participant, &f.origin->key,
			&f.origin->touched[0], f.origin->grant_generation, &f.publication, &f.target,
			f.receipts, f.probe_states, 1, 500, &f.index, &f.probes);
		observed_result = ctrc_receipt_prepare_table_observed(&g.participant, &g.origin->key,
			&g.origin->touched[0], g.origin->grant_generation, &g.publication, &g.target,
			g.receipts, g.probe_states, 1, 500, &g.index, &g.probes, NULL);
		UT_ASSERT_EQ(old_result, kind == 0 ? CLUSTER_CTRC_PREPARE_READY : CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(observed_result, old_result);
		UT_ASSERT_EQ(memcmp(&f.participant, &g.participant, sizeof(f.participant)), 0);
		UT_ASSERT_EQ(memcmp(f.receipts, g.receipts, sizeof(f.receipts)), 0);
		UT_ASSERT_EQ(memcmp(f.probe_states, g.probe_states, sizeof(f.probe_states)), 0);
		UT_ASSERT_EQ(f.index, g.index);
		UT_ASSERT_EQ(f.probes, g.probes);
		UT_ASSERT_EQ(CtrcShared->global_journal_generation, journal);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_prepare_path_null_observer_preserves_exact_shared_effects)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, duplicate;
	ClusterCtrcReceipt before;
	uint64 journal;

	reset_fixture(); origin = seed_origin(0, 1);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &first), CLUSTER_CTRC_PREPARE_READY);
	before = *first.receipt; journal = CtrcShared->global_journal_generation;
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 201, &duplicate, NULL), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(duplicate.receipt_index, first.receipt_index);
	UT_ASSERT_EQ(memcmp(&before, first.receipt, sizeof(before)), 0);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, journal + 1);
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&first));
	UT_ASSERT_EQ(held_count, 0);
}


UT_TEST(test_local_receipt_refuses_missing_or_aliased_origin_protection)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcOriginEntry request, before;
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcReceiptHandle handle;
		uint64 pi;

		reset_fixture();
		origin = seed_origin(0, 1);
		request = *origin;
		UT_ASSERT(ctrc_participant_index(&request.key, 0, &pi));
		participant = ctrc_participant_entries()[pi];
		switch (fault) {
		case 0:
			MemSet(origin, 0, sizeof(*origin));
			break;
		case 1:
			origin->state = CTRC_ORIGIN_RELEASE_PROVEN;
			break;
		case 2:
			origin->state = CTRC_ORIGIN_BLOCKED;
			break;
		case 3:
			origin->state = UINT8_MAX;
			break;
		case 4:
			origin->key.xid++;
			break;
		case 5:
			origin->key.slot_wrap++;
			break;
		case 6:
			origin->key.segment_generation++;
			break;
		case 7:
			origin->key.origin_boot_incarnation++;
			break;
		case 8:
			origin->key.root_generation++;
			break;
		case 9:
			origin->grant_generation++;
			break;
		case 10:
			origin->touched_bitmap = 0;
			break;
		case 11:
			origin->touched[0].boot_incarnation++;
			break;
		case 12:
			origin->touched[0].capability_record_generation++;
			break;
		case 13:
			origin->touched[0].formation_epoch++;
			break;
		case 14:
			origin->touched[0].admission_record_generation++;
			break;
		case 15:
			origin->key.system_identifier++;
			break;
		case 16:
			origin->key.cluster_epoch++;
			break;
		}
		before = *origin;
		MemSet(&handle, 0xa5, sizeof(handle));
		UT_ASSERT_EQ(prepare_fixture_receipt(&request, 201, &handle), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&handle, sizeof(handle)));
		UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&participant, &ctrc_participant_entries()[pi], sizeof(participant)), 0);
		UT_ASSERT_EQ(ctrc_participant_receipt_heads()[pi], 0);
		for (uint64 i = 0; i < CtrcShared->receipt_entries; i++)
			UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[i], sizeof(ClusterCtrcReceipt)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

static ClusterCtrcReceiptHandle *watched_receipt;
static unsigned receipt_origin_guards, guarded_receipt_locks, guarded_receipt_publications;
static unsigned guarded_original_identity_publications;
static bool retire_before_origin_guard;

/* The lock fixture observes production acquisition/publication boundaries;
 * it does not implement the identity check or keep the origin alive. */
static bool
observe_receipt_origin_guard(LWLock *lock, LWLockMode mode, bool acquiring)
{
	if (lock == &CtrcShared->origin_lock && acquiring && mode == LW_SHARED) {
		receipt_origin_guards++;
		if (retire_before_origin_guard)
			ctrc_origin_entries()[0].state = CTRC_ORIGIN_RELEASE_PROVEN;
	}
	if (lock == &CtrcShared->receipt_lock && acquiring && held_count != 0
		&& held_locks[0] == &CtrcShared->origin_lock)
		guarded_receipt_locks++;
	if (lock == &CtrcShared->origin_lock && !acquiring && watched_receipt != NULL
		&& watched_receipt->valid && held_count == 1
		&& watched_receipt->participant->receipt_count == 1
		&& watched_receipt->receipt->state == CTRC_RECEIPT_PREPARED
		&& ctrc_participant_receipt_heads()[watched_receipt->participant_index]
			   == watched_receipt->receipt_index + 1) {
		guarded_receipt_publications++;
		if (FullTransactionIdIsValid(watched_receipt->participant->original_full_xid)
			&& FullTransactionIdEquals(watched_receipt->participant->original_full_xid,
									   ctrc_origin_entries()[0].original_full_xid)
			&& FullTransactionIdEquals(watched_receipt->original_full_xid,
									   ctrc_origin_entries()[0].original_full_xid))
			guarded_original_identity_publications++;
	}
	return false;
}

UT_TEST(test_local_origin_protection_spans_receipt_and_handle_publication)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;

	reset_fixture();
	origin = seed_origin(0, 1);
	watched_receipt = &handle;
	receipt_origin_guards = guarded_receipt_locks = guarded_receipt_publications = 0;
	retire_before_origin_guard = false;
	external_lock_hook = observe_receipt_origin_guard;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(receipt_origin_guards, 1);
	UT_ASSERT_EQ(guarded_receipt_locks, 1);
	UT_ASSERT_EQ(guarded_receipt_publications, 1);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(guarded_receipt_publications, 2);
	UT_ASSERT_EQ(held_count, 0);
	cluster_shared_config = true;
	publication_fenced = true;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 202, &handle), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(receipt_origin_guards, 3);
	UT_ASSERT_EQ(guarded_receipt_locks, 3);
	UT_ASSERT_EQ(guarded_receipt_publications, 2);
	UT_ASSERT_EQ(held_count, 0);
	external_lock_hook = NULL;
	watched_receipt = NULL;
}

UT_TEST(test_local_receipt_rechecks_origin_after_journal_allocation)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	uint64 pi, journal;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &pi));
	journal = CtrcShared->global_journal_generation;
	watched_receipt = NULL;
	receipt_origin_guards = guarded_receipt_locks = 0;
	retire_before_origin_guard = true;
	external_lock_hook = observe_receipt_origin_guard;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, journal + 1);
	UT_ASSERT_EQ(receipt_origin_guards, 1);
	UT_ASSERT_EQ(guarded_receipt_locks, 0);
	UT_ASSERT(
		ctrc_bytes_zero(&ctrc_participant_entries()[pi], sizeof(ClusterCtrcParticipantEntry)));
	UT_ASSERT_EQ(ctrc_participant_receipt_heads()[pi], 0);
	UT_ASSERT_EQ(held_count, 0);
	external_lock_hook = NULL;
	retire_before_origin_guard = false;
}

/* Sealing stops issuing new grants. The participant CLOSE, rather than the
 * origin's SEALING transition, rejects an already issued in-flight grant. */
UT_TEST(test_local_receipt_keeps_inflight_grant_until_participant_close)
{
	for (unsigned sealing = 0; sealing < 2; sealing++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle, late;
		ClusterCtrcReceipt before;

		reset_fixture();
		origin = seed_origin(0, 1);
		if (!sealing) {
			origin->state = CTRC_ORIGIN_OPEN;
			origin->seal_generation = 0;
		}
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT_EQ(cluster_ctrc_receipt_cancel_shared(&handle), CLUSTER_CTRC_APPLY_APPLIED);
		if (!sealing)
			UT_ASSERT(cluster_ctrc_origin_begin_seal_entry(origin, 41));
		UT_ASSERT_EQ(cluster_ctrc_participant_close(handle.participant, &origin->touched[0],
													origin->grant_generation,
													origin->seal_generation),
					 CLUSTER_CTRC_CLOSE_ACK_READY);
		before = *handle.receipt;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 202, &late), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!late.valid);
		UT_ASSERT_EQ(memcmp(handle.receipt, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(handle.participant->receipt_count, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_remote_origin_receipt_does_not_require_local_origin_row)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcReceiptHandle handle;

	reset_fixture();
	request = *seed_origin(0, 1);
	MemSet(&ctrc_origin_entries()[0], 0, sizeof(ClusterCtrcOriginEntry));
	request.key.owner_instance = 2;
	request.key.origin_node_id = 1;
	request.key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	request.key.origin_boot_incarnation = 1102;
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 201, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(handle.valid);
	UT_ASSERT_EQ(memcmp(&handle.receipt->key, &request.key, sizeof(request.key)), 0);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
	UT_ASSERT_EQ(held_count, 0);
}

/* A closed object cannot acquire a new CTRC producer, including a caller
 * whose admission sample became stale while taking the original locks. */
UT_TEST(test_object_fence_refuses_new_receipt_without_publishing_participant)
{
	for (unsigned race = 0; race < 2; race++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry before;
		uint64 pi, journal;

		reset_fixture();
		origin = seed_origin(0, 1);
		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &pi));
		before = ctrc_participant_entries()[pi];
		journal = CtrcShared->global_journal_generation;
		cluster_shared_config = true;
		publication_fenced = race == 0;
		fence_at_receipt_lock = race != 0;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!handle.valid);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_participant_entries()[pi], sizeof(before)), 0);
		UT_ASSERT_EQ(ctrc_participant_receipt_heads()[pi], 0);
		for (uint64 i = 0; i < CtrcShared->receipt_entries; i++)
			UT_ASSERT_EQ(ctrc_receipt_entries()[i].state, CTRC_RECEIPT_FREE);
		if (!race)
			UT_ASSERT_EQ(CtrcShared->global_journal_generation, journal);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_object_fence_retains_exact_duplicate_and_original_cancel)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, duplicate, refused;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt;

	reset_fixture();
	origin = seed_origin(0, 1);
	cluster_shared_config = true;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &first), CLUSTER_CTRC_PREPARE_READY);
	before = *first.participant;
	receipt = *first.receipt;
	publication_fenced = true;
	publication_revision++;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &duplicate), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(duplicate.receipt_index, first.receipt_index);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 202, &refused), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(!refused.valid);
	UT_ASSERT_EQ(memcmp(&before, first.participant, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&receipt, first.receipt, sizeof(receipt)), 0);
	for (unsigned kind = 0; kind < 4; kind++) {
		ClusterCtrcParticipantIdentity identity = origin->touched[0];
		ClusterCtrcPublicationIdV1 publication = receipt.publication;
		ClusterCtrcTargetV1 target = receipt.target;
		uint32 grant = origin->grant_generation;

		publication.journal_sequence = publication.key_sequence
			= publication.journal_slot_generation = 0;
		if (kind == 0)
			identity.boot_incarnation++;
		if (kind == 1)
			grant++;
		if (kind == 2)
			target.block_number++;
		if (kind == 3)
			publication.descriptor_hash++;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared(&origin->key, &identity, grant,
														 &publication, &target, &refused),
					 CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!refused.valid);
		UT_ASSERT_EQ(memcmp(&before, first.participant, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&receipt, first.receipt, sizeof(receipt)), 0);
	}
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&first));
	UT_ASSERT_EQ(first.receipt->state, CTRC_RECEIPT_CANCELLED);
}

UT_TEST(test_object_receipt_gate_does_not_change_private_or_other_tier)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;

		reset_fixture();
		origin = seed_origin(0, 1);
		publication_fenced = true;
		cluster_shared_config = kind != 0;
		publication_smgr = kind == 1 ? 0 : 1;
		cluster_interconnect_tier = kind == 2 ? CLUSTER_IC_TIER_2 : CLUSTER_IC_TIER_1;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 201, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(handle.valid);
	}
}

static bool
seed_applied_read_scope(ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
						uint64 *participant_index, uint64 *receipt_index)
{
	ClusterCtrcOriginEntry *origin = seed_origin(0, 1);
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target;

	if (prepare_fixture_receipt(origin, 81, &handle) != CLUSTER_CTRC_PREPARE_READY)
		return false;
	target = handle.receipt->target;
	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_xid = origin->key.xid;
	target.itl_class = 1;
	target.itl_slot_wrap = 1;
	target.uba[0] = 1;
	target.planned_predecessor_sha256[0] = 1;
	target.planned_successor_sha256[0] = 2;
	if (cluster_ctrc_receipt_apply_shared(&handle, &target, &token) != CLUSTER_CTRC_APPLY_APPLIED
		|| cluster_ctrc_participant_close(handle.participant, &origin->touched[0],
										  origin->grant_generation, origin->seal_generation)
			   != CLUSTER_CTRC_CLOSE_PENDING_DRAIN)
		return false;
	*participant = *handle.participant;
	*receipt = *handle.receipt;
	*participant_index = handle.participant_index;
	*receipt_index = handle.receipt_index;
	CtrcWorkerId = 0;
	MyBackendType = B_UNDO_CLEANER;
	MyAuxProcType = UndoCleanerProcess;
	return true;
}

UT_TEST(test_original_cleaner_scope_binds_one_receipt_and_its_space_read)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	ClusterCtrcCleanerReadScopeV3 proof;
	uint64 pi, ri, serial = 0;
	RelFileLocator locator;

	reset_fixture();
	UT_ASSERT(seed_applied_read_scope(&participant, &receipt, &pi, &ri));
	locator = (RelFileLocator){ receipt.target.spc_oid, receipt.target.db_oid,
								receipt.target.rel_number };
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 44, &proof));
	UT_ASSERT(ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &serial));
	UT_ASSERT(serial > 0);
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 44, &proof));
	UT_ASSERT_EQ(proof.pid, MyProcPid);
	UT_ASSERT_EQ(proof.serial, serial);
	UT_ASSERT_EQ(proof.formation_epoch, participant.identity.formation_epoch);
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, MAIN_FORKNUM, 44));
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, SPACE_FORKNUM, 0, &proof));
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, SPACE_FORKNUM, 1, &proof));
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 45, &proof));
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, FSM_FORKNUM, 44, &proof));
	locator.relNumber++;
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 44, &proof));
	locator.relNumber--;
	UT_ASSERT(!ctrc_cleaner_read_scope_end(serial + 1));
	UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, SPACE_FORKNUM, 0));
	UT_ASSERT_EQ(ctrc_receipt_entries()[ri].state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_cleaner_scope_rejects_wrong_owner_and_receipt_drift)
{
	for (unsigned kind = 0; kind < 12; kind++) {
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcReceipt receipt;
		uint64 pi, ri, serial = 0;
		reset_fixture();
		UT_ASSERT(seed_applied_read_scope(&participant, &receipt, &pi, &ri));
		switch (kind) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			MyAuxProcType = CheckpointerProcess;
			break;
		case 2:
			CtrcWorkerId = 1;
			break;
		case 3:
			participant.seal_generation++;
			break;
		case 4:
			participant.identity.boot_incarnation++;
			break;
		case 5:
			receipt.publication.journal_slot_generation++;
			break;
		case 6:
			receipt.target.block_number++;
			break;
		case 7:
			ctrc_receipt_entries()[ri].state = CTRC_RECEIPT_CLEANED;
			break;
		case 8:
			ctrc_participant_entries()[pi].state = CTRC_PARTICIPANT_OPEN;
			break;
		case 9:
			ctrc_receipt_links()[ri].participant_plus_one = pi + 2;
			break;
		case 10:
			CtrcReadSerial = UINT64_MAX;
			break;
		case 11:
			MyProcPid = 0;
			break;
		}
		UT_ASSERT(!ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &serial));
		UT_ASSERT_EQ(serial, 0);
		UT_ASSERT(!CtrcReadScope.active);
		UT_ASSERT_EQ(held_count, 0);
		MyProcPid = 2901;
		CtrcReadSerial = 0;
	}
}

UT_TEST(test_cleaner_scope_current_is_local_and_expires_with_original_attempt)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	ClusterCtrcCleanerReadScopeV3 proof;
	uint64 pi, ri, serial = 0, second = 0;
	RelFileLocator locator;
	unsigned before;

	reset_fixture();
	UT_ASSERT(seed_applied_read_scope(&participant, &receipt, &pi, &ri));
	locator = (RelFileLocator){ receipt.target.spc_oid, receipt.target.db_oid,
								receipt.target.rel_number };
	UT_ASSERT(ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &serial));
	UT_ASSERT(!ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &second));
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 44, &proof));
	before = sleepable_acquisitions;
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, MAIN_FORKNUM, 44));
	UT_ASSERT_EQ(sleepable_acquisitions, before);
	MyProcPid++;
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, MAIN_FORKNUM, 44));
	MyProcPid--;
	UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	UT_ASSERT(ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &second));
	UT_ASSERT(second > serial);
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, MAIN_FORKNUM, 44));
	UT_ASSERT(ctrc_cleaner_read_scope_end(second));
}

UT_TEST(test_concurrent_original_discharge_does_not_end_running_cleaner_attempt)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	ClusterCtrcCleanerReadScopeV3 proof;
	ClusterCtrcDurability durability = { 0 };
	uint64 pi, ri, serial = 0;
	RelFileLocator locator;

	reset_fixture();
	UT_ASSERT(seed_applied_read_scope(&participant, &receipt, &pi, &ri));
	locator = (RelFileLocator){ receipt.target.spc_oid, receipt.target.db_oid,
								receipt.target.rel_number };
	UT_ASSERT(ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &serial));
	durability.highest_local_lsn = durability.local_flush_lsn = 100;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&ctrc_participant_entries()[pi],
													&ctrc_receipt_entries()[ri],
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(ctrc_receipt_entries()[ri].state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, MAIN_FORKNUM, 44, &proof));
	UT_ASSERT_EQ(read_scope_entered, 1);
	UT_ASSERT_EQ(read_scope_left, 0);
	UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	UT_ASSERT_EQ(read_scope_left, 1);
	UT_ASSERT(!cluster_ctrc_cleaner_read_scope_current_v3(&proof, locator, MAIN_FORKNUM, 44));
	UT_ASSERT(!ctrc_cleaner_read_scope_begin(&participant, &receipt, pi, ri, &serial));
}

static unsigned read_driver_mode, read_driver_calls;

static bool
read_driver_physical(const ClusterCtrcParticipantEntry *participant pg_attribute_unused(),
					 const ClusterCtrcReceipt *receipt, uint64 pi pg_attribute_unused(),
					 uint64 ri pg_attribute_unused())
{
	ClusterCtrcCleanerReadScopeV3 proof;
	RelFileLocator locator
		= { receipt->target.spc_oid, receipt->target.db_oid, receipt->target.rel_number };

	read_driver_calls++;
	UT_ASSERT_EQ(read_scope_entered, 1);
	UT_ASSERT(cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, receipt->target.fork_number,
														  receipt->target.block_number, &proof));
	if (read_driver_mode == 2)
		pg_re_throw();
	return read_driver_mode == 1;
}

#define ctrc_cleaner_clean_next_receipt read_driver_next
#define ctrc_cleaner_clean_itl_receipt read_driver_physical
#define ctrc_cleaner_clean_current_mx_receipt read_driver_physical
#include "test_cluster_ctrc_read_scope.inc"
#undef ctrc_cleaner_clean_next_receipt
#undef ctrc_cleaner_clean_itl_receipt
#undef ctrc_cleaner_clean_current_mx_receipt

UT_TEST(test_original_cleaner_driver_expires_read_scope_on_return_error_and_refusal)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcReceipt receipt;
		uint64 pi, ri;
		volatile bool caught = false;
		volatile bool result = false;

		reset_fixture();
		UT_ASSERT(seed_applied_read_scope(&participant, &receipt, &pi, &ri));
		read_driver_mode = mode;
		read_driver_calls = 0;
		read_scope_enter_refused = mode == 3;
		PG_TRY();
		{
			result = read_driver_next();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, mode == 2);
		UT_ASSERT_EQ(result, mode == 1);
		UT_ASSERT_EQ(read_driver_calls, mode == 3 ? 0 : 1);
		UT_ASSERT_EQ(read_scope_entered, mode == 3 ? 0 : 1);
		UT_ASSERT_EQ(read_scope_left, read_scope_entered);
		UT_ASSERT(!CtrcReadScope.active);
		UT_ASSERT_EQ(ctrc_receipt_entries()[ri].state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static bool
seed_cancelled_ack_at(unsigned origin_index, ClusterCtrcParticipantEntry *snapshot,
					  ClusterCtrcReceipt *receipt, ClusterCtrcLocalReleaseAckV1 *ack,
					  uint64 *participant_index, uint64 *receipt_index)
{
	ClusterCtrcOriginEntry *origin = seed_origin(origin_index, 1);
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcDurability durability = { 0 };

	if (prepare_fixture_receipt(origin, 81, &handle) != CLUSTER_CTRC_PREPARE_READY || !handle.valid
		|| !cluster_ctrc_receipt_cancel_shared(&handle)
		|| !ctrc_participant_index(&origin->key, 0, participant_index))
		return false;
	*receipt_index = handle.receipt_index;
	participant = &ctrc_participant_entries()[*participant_index];
	if (cluster_ctrc_participant_close(participant, &origin->touched[0], origin->grant_generation,
									   origin->seal_generation)
		!= CLUSTER_CTRC_CLOSE_ACK_READY)
		return false;
	*snapshot = *participant;
	*receipt = ctrc_receipt_entries()[*receipt_index];
	return cluster_ctrc_participant_ack_from_snapshot(snapshot, receipt, 1, &durability, ack)
		   == CLUSTER_CTRC_ACK_RELEASED;
}

static bool
seed_cancelled_ack(ClusterCtrcParticipantEntry *snapshot, ClusterCtrcReceipt *receipt,
				   ClusterCtrcLocalReleaseAckV1 *ack, uint64 *participant_index,
				   uint64 *receipt_index)
{
	return seed_cancelled_ack_at(0, snapshot, receipt, ack, participant_index, receipt_index);
}

UT_TEST(test_shared_prepare_journal_uses_sleepable_exclusion)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT_EQ(snapshot.receipt_count, 1);
	UT_ASSERT_EQ(spin_acquisitions, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, 4);
}

/* The real shared PREPARE must not serialize different canonical segments
 * on one participant mutex, while a segment in the same shard still must. */
UT_TEST(test_shared_prepare_isolates_participant_shards)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index, receipt_index;
	const void *first;
	const void *second;

	reset_fixture();
	UT_ASSERT(
		seed_cancelled_ack_at(0, &snapshot, &receipt, &ack, &participant_index, &receipt_index));
	first = last_participant_lock;
	UT_ASSERT(seed_cancelled_ack_at(TT_SLOTS_PER_SEGMENT, &snapshot, &receipt, &ack,
									&participant_index, &receipt_index));
	second = last_participant_lock;
	UT_ASSERT(first != NULL && second != NULL && first != second);
	UT_ASSERT(seed_cancelled_ack_at(8 * TT_SLOTS_PER_SEGMENT, &snapshot, &receipt, &ack,
									&participant_index, &receipt_index));
	UT_ASSERT(last_participant_lock == first);
	UT_ASSERT_EQ(held_count, 0);
}

/* An ACK copy must visit its actual receipt, not every unrelated empty slot,
 * and it must leave the pool readable by another shard's independent copy. */
UT_TEST(test_ack_copy_is_indexed_and_pool_read_only)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt, copy;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index, receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	MemSet(table_visits, 0, sizeof(table_visits));
	UT_ASSERT(ctrc_participant_copy_receipts(participant_index, &snapshot, &copy, 1));
	UT_ASSERT_EQ(memcmp(&copy, &receipt, sizeof(copy)), 0);
	UT_ASSERT_EQ(table_visits[1], 1);
	UT_ASSERT_EQ(last_pool_mode, LW_SHARED);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_shared_pool_capacity_is_not_a_per_shard_quota)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle, duplicate;
	uint64 participant_index;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
	for (uint64 i = 0; i < CtrcShared->receipt_entries; i++) {
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, i + 1, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, i + 1, &duplicate),
					 CLUSTER_CTRC_PREPARE_DUPLICATE);
		UT_ASSERT_EQ(duplicate.receipt_index, handle.receipt_index);
		UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&handle));
	}
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_index].receipt_count,
				 CtrcShared->receipt_entries);
	UT_ASSERT(ctrc_receipt_chain_exact_locked(participant_index, false));
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, CtrcShared->receipt_entries + 1, &handle),
				 CLUSTER_CTRC_PREPARE_CAPACITY);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_prepare_does_not_overwrite_a_corrupt_empty_locator)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	CtrcReceiptLink *links;

	reset_fixture();
	origin = seed_origin(0, 1);
	links = ctrc_receipt_links();
	/* Corrupt all empty locator rows so this does not depend on the product
	 * hash function choosing a particular slot. No receipt exists yet. */
	for (uint64 i = 0; i < CtrcShared->receipt_entries; i++)
		links[i].next_plus_one = 1;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 1, &handle), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(!handle.valid);
	UT_ASSERT(ctrc_bytes_zero(ctrc_receipt_entries(),
							  CtrcShared->receipt_entries * sizeof(ClusterCtrcReceipt)));
}

UT_TEST(test_nonempty_ack_copy_and_freeze_use_sleepable_exclusion)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt copied;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	spin_acquisitions = sleepable_acquisitions = 0;
	UT_ASSERT(ctrc_participant_copy_receipts(participant_index, &snapshot, &copied, 1));
	UT_ASSERT_EQ(memcmp(&copied, &receipt, sizeof(receipt)), 0);
	UT_ASSERT(ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &copied, 1, &ack));
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_index].state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_index].state, CTRC_PARTICIPANT_ACK_FROZEN);
	UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[participant_index], &ack, sizeof(ack)), 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(spin_acquisitions, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, 4);
}

UT_TEST(test_stale_ack_snapshot_releases_without_publication)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	before = snapshot;
	snapshot.grant_generation++;
	UT_ASSERT(!ctrc_participant_copy_receipts(participant_index, &snapshot, &receipt, 1));
	UT_ASSERT(!ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &receipt, 1, &ack));
	UT_ASSERT_EQ(memcmp(&ctrc_participant_entries()[participant_index], &before, sizeof(before)),
				 0);
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_index].state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[participant_index], sizeof(ack)));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_receipt_drift_retains_bytes_and_blocks_ack)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt changed;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	ctrc_receipt_entries()[receipt_index].target.predecessor_page_scn++;
	changed = ctrc_receipt_entries()[receipt_index];
	UT_ASSERT(!ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &receipt, 1, &ack));
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_index].state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(memcmp(&ctrc_receipt_entries()[receipt_index], &changed, sizeof(changed)), 0);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[participant_index], sizeof(ack)));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_sleepable_header_requires_exact_layout_and_tranches)
{
	ClusterCtrcCapacity capacity;

	reset_fixture();
	UT_ASSERT(cluster_ctrc_capacity_compute(NBuffers, MaxBackends, 4, &capacity));
	UT_ASSERT(cluster_ctrc_shmem_ready());
	UT_ASSERT_EQ(CtrcShared->total_bytes, capacity.total_bytes);
	UT_ASSERT_EQ(CtrcShared->origin_offset, MAXALIGN(sizeof(*CtrcShared)));
	UT_ASSERT_EQ(CtrcShared->origin_lock.tranche, LWTRANCHE_CLUSTER_CTRC_ORIGIN);
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++)
		UT_ASSERT_EQ(CtrcShared->participant_locks[i].lock.tranche,
					 LWTRANCHE_CLUSTER_CTRC_PARTICIPANT);
	UT_ASSERT_EQ(CtrcShared->receipt_lock.tranche, LWTRANCHE_CLUSTER_CTRC_RECEIPT);
	CtrcShared->version--;
	UT_ASSERT(!cluster_ctrc_shmem_ready());
	CtrcShared->version++;
	CtrcShared->total_bytes--;
	UT_ASSERT(!cluster_ctrc_shmem_ready());
	CtrcShared->total_bytes++;
	UT_ASSERT(cluster_ctrc_shmem_ready());
}

static void
seed_reclaim_batch(ClusterCtrcReclaimWork *work, ClusterCtrcReceipt *rows, uint8 *states,
				   Size capacity, Size count)
{
	MemSet(work, 0, count * sizeof(*work));
	MemSet(rows, 0, capacity * sizeof(*rows));
	MemSet(states, 0, capacity);
	for (Size i = 0; i < count; i++) {
		work[i].key = seed_origin(i, 0)->key;
		work[i].expected_count = 2;
		for (Size j = 0; j < 2; j++) {
			Size slot = 2 * i + j;
			rows[slot].key = work[i].key;
			rows[slot].state = CTRC_RECEIPT_ACK_FROZEN;
			states[slot] = CTRC_RECEIPT_PROBE_OCCUPIED;
		}
	}
}

UT_TEST(test_reclaim_batch_validates_table_once_for_eight_keys)
{
	ClusterCtrcReclaimWork work[8];
	ClusterCtrcReceipt rows[32];
	uint8 states[32];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	MemSet(table_visits, 0, sizeof(table_visits));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	for (Size i = 0; i < lengthof(work); i++) {
		UT_ASSERT(work[i].reclaimed);
		UT_ASSERT_EQ(work[i].found, 2);
	}
	UT_ASSERT(ctrc_bytes_zero(rows, sizeof(rows)));
	UT_ASSERT(ctrc_bytes_zero(states, sizeof(states)));
	UT_ASSERT_EQ(table_visits[0], lengthof(rows));
}

UT_TEST(test_reclaim_batch_bad_key_retains_its_whole_set_only)
{
	ClusterCtrcReclaimWork work[3];
	ClusterCtrcReceipt rows[16];
	ClusterCtrcReceipt retained[2];
	uint8 states[16];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	rows[3].state = CTRC_RECEIPT_APPLIED;
	memcpy(retained, &rows[2], sizeof(retained));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	UT_ASSERT(work[0].reclaimed && work[2].reclaimed);
	UT_ASSERT(!work[1].reclaimed);
	UT_ASSERT_EQ(memcmp(retained, &rows[2], sizeof(retained)), 0);
	UT_ASSERT_EQ(states[2], CTRC_RECEIPT_PROBE_OCCUPIED);
	UT_ASSERT_EQ(states[3], CTRC_RECEIPT_PROBE_OCCUPIED);
}

UT_TEST(test_reclaim_batch_all_empty_bytes_guard_before_any_delete)
{
	ClusterCtrcReclaimWork work[2];
	ClusterCtrcReceipt rows[8];
	ClusterCtrcReceipt before[8];
	uint8 states[8];
	uint8 states_before[8];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	for (Size byte = 0; byte < sizeof(rows[7]); byte++) {
		((uint8 *)&rows[7])[byte] = 0x80;
		memcpy(before, rows, sizeof(rows));
		memcpy(states_before, states, sizeof(states));
		(void)cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows));
		UT_ASSERT(!work[0].reclaimed && !work[1].reclaimed);
		UT_ASSERT_EQ(memcmp(before, rows, sizeof(rows)), 0);
		UT_ASSERT_EQ(memcmp(states_before, states, sizeof(states)), 0);
		((uint8 *)&rows[7])[byte] = 0;
	}
}

UT_TEST(test_reclaim_batch_duplicate_and_missing_member_retain_exactly)
{
	ClusterCtrcReclaimWork work[2];
	ClusterCtrcReceipt rows[8];
	ClusterCtrcReceipt before[8];
	uint8 states[8];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	memcpy(before, rows, sizeof(rows));
	work[1].key = work[0].key;
	UT_ASSERT(!cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														  lengthof(rows)));
	UT_ASSERT_EQ(memcmp(before, rows, sizeof(rows)), 0);
	work[1].key = rows[2].key;
	work[0].expected_count++;
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	UT_ASSERT(!work[0].reclaimed && work[1].reclaimed);
	UT_ASSERT_EQ(memcmp(before, rows, 2 * sizeof(rows[0])), 0);
}

UT_TEST(test_zero_bytes_exact_at_every_offset_and_protected_end)
{
	uint8 bytes[608];
	long page_size = sysconf(_SC_PAGESIZE);
	uint8 *guard;

	UT_ASSERT(!ctrc_bytes_zero(NULL, 0));
	UT_ASSERT(!ctrc_bytes_zero(NULL, 1));
	for (Size offset = 0; offset < 8; offset++)
		for (Size length = 0; length <= 592; length++) {
			MemSet(bytes, 0, sizeof(bytes));
			bytes[offset + length] = 0x80;
			UT_ASSERT(ctrc_bytes_zero(bytes + offset, length));
			for (Size changed = 0; changed < length; changed++) {
				bytes[offset + changed] = 1;
				UT_ASSERT(!ctrc_bytes_zero(bytes + offset, length));
				bytes[offset + changed] = 0;
			}
		}
	UT_ASSERT(page_size > 592);
	guard = mmap(NULL, 2 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	UT_ASSERT(guard != MAP_FAILED);
	UT_ASSERT_EQ(mprotect(guard + page_size, page_size, PROT_NONE), 0);
	for (Size length = 0; length <= 592; length++)
		UT_ASSERT(ctrc_bytes_zero(guard + page_size - length, length));
	UT_ASSERT_EQ(munmap(guard, 2 * page_size), 0);
}

UT_TEST(test_reclaim_empty_range_preserves_failed_and_collapses_success)
{
	ClusterCtrcReclaimWork work = { 0 };
	ClusterCtrcReceipt rows[8] = { { 0 } };
	uint8 states[8] = { 0 };
	uint8 before[8];

	reset_fixture();
	work.key = seed_origin(0, 0)->key;
	work.expected_count = 1;
	states[6] = CTRC_RECEIPT_PROBE_TOMBSTONE;
	memcpy(before, states, sizeof(states));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(&work, 1, rows, states, lengthof(rows)));
	UT_ASSERT(!work.reclaimed);
	UT_ASSERT_EQ(memcmp(before, states, sizeof(states)), 0);
	work.expected_count = 0;
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(&work, 1, rows, states, lengthof(rows)));
	UT_ASSERT(work.reclaimed);
	UT_ASSERT(ctrc_bytes_zero(states, sizeof(states)));
}

static bool
seed_frozen_certificate(unsigned index, ClusterCtrcCloseDispatch *dispatch,
						uint64 *participant_index, uint64 *receipt_index)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;

	if (!seed_cancelled_ack_at(index, &snapshot, &receipt, &ack, participant_index, receipt_index)
		|| !ctrc_participant_freeze_ack_exact(*participant_index, &snapshot, &receipt, 1, &ack))
		return false;
	MemSet(dispatch, 0, sizeof(*dispatch));
	dispatch->key = snapshot.key;
	dispatch->participant = snapshot.identity;
	dispatch->grant_generation = snapshot.grant_generation;
	dispatch->seal_generation = snapshot.seal_generation;
	dispatch->suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	dispatch->request_id = 1234 + index;
	return true;
}

UT_TEST(test_shared_certificate_batch_uses_one_real_validation)
{
	ClusterCtrcCloseDispatch dispatches[8];
	ClusterCtrcSealReplyResult results[8];
	uint64 participant_indices[8];
	uint64 receipt_indices[8];
	uint64 progress;

	reset_fixture();
	for (Size i = 0; i < lengthof(dispatches); i++)
		UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
										  &receipt_indices[i]));
	MemSet(table_visits, 0, sizeof(table_visits));
	sleepable_acquisitions = 0;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, lengthof(dispatches),
																results));
	for (Size i = 0; i < lengthof(dispatches); i++) {
		UT_ASSERT_EQ(results[i], CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[participant_indices[i]],
								  sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt_indices[i]],
								  sizeof(ClusterCtrcReceipt)));
	}
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS) - progress, 8);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(table_visits[0], CtrcShared->receipt_entries);
	/* One key-shard hold plus pool S validation and pool X publication. */
	UT_ASSERT_EQ(sleepable_acquisitions, 3);
}

UT_TEST(test_shared_certificate_batch_identity_and_receipt_faults_are_isolated)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcSealReplyResult results[2];
		uint64 participant_indices[2];
		uint64 receipt_indices[2];
		ClusterCtrcReceipt retained;
		ClusterCtrcLocalReleaseAckV1 ack_retained;

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
											  &receipt_indices[i]));
		if (fault == 0)
			dispatches[0].participant.boot_incarnation++;
		else if (fault == 1)
			dispatches[0].grant_generation++;
		else if (fault == 2)
			dispatches[0].seal_generation++;
		else if (fault == 3)
			dispatches[0].key.xid++;
		else if (fault == 4)
			ctrc_receipt_entries()[receipt_indices[0]].state = CTRC_RECEIPT_APPLIED;
		else
			ctrc_participant_entries()[participant_indices[0]].receipt_count++;
		retained = ctrc_receipt_entries()[receipt_indices[0]];
		ack_retained = ctrc_participant_ack_entries()[participant_indices[0]];
		UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
		UT_ASSERT_EQ(results[0], CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(results[1], CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT_EQ(
			memcmp(&retained, &ctrc_receipt_entries()[receipt_indices[0]], sizeof(retained)), 0);
		UT_ASSERT_EQ(memcmp(&ack_retained, &ctrc_participant_ack_entries()[participant_indices[0]],
							sizeof(ack_retained)),
					 0);
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[0]].state,
					 fault < 4 ? CTRC_PARTICIPANT_ACK_FROZEN : CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt_indices[1]], sizeof(retained)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_shared_certificate_global_corruption_and_alias_never_partially_clear)
{
	ClusterCtrcCloseDispatch dispatches[2];
	ClusterCtrcSealReplyResult results[2];
	uint64 participant_indices[2];
	uint64 receipt_indices[2];
	ClusterCtrcReceipt retained[2];
	Size empty = 0;
	uint64 progress;

	reset_fixture();
	for (Size i = 0; i < 2; i++) {
		UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
										  &receipt_indices[i]));
		retained[i] = ctrc_receipt_entries()[receipt_indices[i]];
	}
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	dispatches[1] = dispatches[0];
	UT_ASSERT(!cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[0]].state,
				 CTRC_PARTICIPANT_ACK_FROZEN);
	dispatches[1].key = ctrc_participant_entries()[participant_indices[1]].key;
	dispatches[1].seal_generation
		= ctrc_participant_entries()[participant_indices[1]].seal_generation;
	while (ctrc_receipt_probe_states()[empty] != CTRC_RECEIPT_PROBE_EMPTY)
		empty++;
	ctrc_receipt_entries()[empty].reserved8[2] = 1;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
	for (Size i = 0; i < 2; i++) {
		UT_ASSERT_EQ(results[i], CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(
			memcmp(&retained[i], &ctrc_receipt_entries()[receipt_indices[i]], sizeof(retained[i])),
			0);
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[i]].state,
					 CTRC_PARTICIPANT_BLOCKED);
	}
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_shared_reclaim_validates_read_only_and_clears_locators)
{
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcSealReplyResult result;
	uint64 participant_index, receipt_index;

	reset_fixture();
	UT_ASSERT(seed_frozen_certificate(0, &dispatch, &participant_index, &receipt_index));
	MemSet(table_visits, 0, sizeof(table_visits));
	shared_validation_writer_visits = 0;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
	UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT_EQ(table_visits[0], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(shared_validation_writer_visits, 0);
	UT_ASSERT_EQ(ctrc_participant_receipt_heads()[participant_index], 0);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_links()[receipt_index], sizeof(CtrcReceiptLink)));
}

/* Fill the original pool, so deleting the last occupied row leaves no EMPTY
 * probe as a shortcut. Reuse every physical address with another transaction
 * and replay an old handle against an actually occupied replacement row. */
UT_TEST(test_shared_whole_pool_reclaim_and_old_handle_replay)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceiptHandle handle, old_handle;
	ClusterCtrcReceipt *copies;
	ClusterCtrcReceipt replacement;
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcDurability durability = { 0 };
	ClusterCtrcCloseDispatch dispatch = { 0 };
	ClusterCtrcSealReplyResult result;
	uint64 participant_index;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
	for (uint64 i = 0; i < CtrcShared->receipt_entries; i++) {
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, i + 1, &handle), CLUSTER_CTRC_PREPARE_READY);
		if (i == 0)
			old_handle = handle;
		UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&handle));
	}
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&ctrc_participant_entries()[participant_index],
												&origin->touched[0], origin->grant_generation,
												origin->seal_generation),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	snapshot = ctrc_participant_entries()[participant_index];
	copies = palloc_extended(CtrcShared->receipt_entries * sizeof(*copies), 0);
	UT_ASSERT(copies != NULL);
	UT_ASSERT(ctrc_participant_copy_receipts(participant_index, &snapshot, copies,
											 CtrcShared->receipt_entries));
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(
					 &snapshot, copies, CtrcShared->receipt_entries, &durability, &ack),
				 CLUSTER_CTRC_ACK_RELEASED);
	UT_ASSERT(ctrc_participant_freeze_ack_exact(participant_index, &snapshot, copies,
												CtrcShared->receipt_entries, &ack));
	pfree(copies);
	dispatch.key = snapshot.key;
	dispatch.participant = snapshot.identity;
	dispatch.grant_generation = snapshot.grant_generation;
	dispatch.seal_generation = snapshot.seal_generation;
	dispatch.suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	dispatch.request_id = 8123;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
	UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT(
		ctrc_bytes_zero(ctrc_receipt_entries(), CtrcShared->receipt_entries * sizeof(*copies)));
	UT_ASSERT(ctrc_bytes_zero(ctrc_receipt_links(),
							  CtrcShared->receipt_entries * sizeof(CtrcReceiptLink)));
	UT_ASSERT(ctrc_bytes_zero(ctrc_receipt_probe_states(), CtrcShared->receipt_entries));
	UT_ASSERT(!cluster_ctrc_receipt_cancel_shared(&old_handle));

	origin = seed_origin(TT_SLOTS_PER_SEGMENT, 1);
	for (uint64 i = 0; i < CtrcShared->receipt_entries; i++)
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, i + 1, &handle), CLUSTER_CTRC_PREPARE_READY);
	replacement = *old_handle.receipt;
	UT_ASSERT_EQ(replacement.state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT(replacement.publication.journal_slot_generation
			  != old_handle.journal_slot_generation);
	UT_ASSERT(!cluster_ctrc_receipt_cancel_shared(&old_handle));
	UT_ASSERT_EQ(memcmp(&replacement, old_handle.receipt, sizeof(replacement)), 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_shared_reclaim_rejects_incomplete_cyclic_and_stale_locators)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcSealReplyResult results[2];
		uint64 participant_indices[2], receipt_indices[2];
		ClusterCtrcReceipt before;

		reset_fixture();
		for (unsigned i = 0; i < 2; i++)
			UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
											  &receipt_indices[i]));
		if (fault == 0)
			ctrc_participant_receipt_heads()[participant_indices[0]] = 0;
		else if (fault == 1)
			ctrc_receipt_links()[receipt_indices[0]].next_plus_one = receipt_indices[0] + 1;
		else if (fault == 2)
			ctrc_receipt_links()[receipt_indices[0]].journal_generation++;
		else
			ctrc_receipt_links()[receipt_indices[0]].participant_plus_one++;
		before = ctrc_receipt_entries()[receipt_indices[0]];
		UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
		UT_ASSERT_EQ(results[0], CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(results[1], CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_receipt_entries()[receipt_indices[0]], sizeof(before)),
					 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static ClusterCtrcOriginEntry *interleave_origin;
static ClusterCtrcCloseDispatch interleave_dispatch;
static ClusterCtrcReceiptHandle interleave_handle;
static unsigned interleave_mode;
static unsigned interleave_completed;

/* Deterministic scheduling boundary, not a substitute for PostgreSQL's lock
 * implementation: the suspended actor's actual held locks remain forbidden
 * to the second actor while it executes the real shared producer/consumer. */
static void
run_other_key_during_pool_upgrade(void)
{
	ClusterCtrcSealReplyResult result;

	if (held_count != 1 || held_locks[0] != &CtrcShared->participant_locks[0].lock)
		abort();
	pool_upgrade_hook = NULL;
	paused_actor_count = held_count;
	memcpy(paused_actor_locks, held_locks, sizeof(held_locks));
	MemSet(held_locks, 0, sizeof(held_locks));
	held_count = 0;
	if (interleave_mode == 0) {
		if (prepare_fixture_receipt(interleave_origin, 99, &interleave_handle)
				!= CLUSTER_CTRC_PREPARE_READY
			|| !cluster_ctrc_receipt_cancel_shared(&interleave_handle))
			abort();
	} else if (!cluster_ctrc_participant_certificate_batch_shared(&interleave_dispatch, 1, &result)
			   || result != CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED)
		abort();
	if (held_count != 0)
		abort();
	memcpy(held_locks, paused_actor_locks, sizeof(held_locks));
	held_count = paused_actor_count;
	paused_actor_count = 0;
	interleave_completed++;
}

UT_TEST(test_reclaim_upgrade_preserves_other_key_prepare_and_reclaim)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterCtrcCloseDispatch dispatch;
		ClusterCtrcSealReplyResult result;
		uint64 participant_index, receipt_index, other_participant, other_receipt;

		reset_fixture();
		UT_ASSERT(seed_frozen_certificate(0, &dispatch, &participant_index, &receipt_index));
		if (mode == 0)
			interleave_origin = seed_origin(TT_SLOTS_PER_SEGMENT, 1);
		else
			UT_ASSERT(seed_frozen_certificate(TT_SLOTS_PER_SEGMENT, &interleave_dispatch,
											  &other_participant, &other_receipt));
		interleave_mode = mode;
		interleave_completed = 0;
		pool_upgrade_hook = run_other_key_during_pool_upgrade;
		UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
		UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT_EQ(interleave_completed, 1);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT(
			ctrc_bytes_zero(&ctrc_receipt_entries()[receipt_index], sizeof(ClusterCtrcReceipt)));
		if (mode == 0) {
			UT_ASSERT_EQ(interleave_handle.receipt->state, CTRC_RECEIPT_CANCELLED);
			UT_ASSERT(ctrc_receipt_chain_exact_locked(interleave_handle.participant_index, false));
		} else
			UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[other_receipt],
									  sizeof(ClusterCtrcReceipt)));
	}
}

static void
throw_before_pool_write(void)
{
	if (held_count != 1 || held_locks[0] != &CtrcShared->participant_locks[0].lock)
		abort();
	pg_re_throw();
}

UT_TEST(test_reclaim_error_before_write_keeps_exact_set)
{
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcSealReplyResult result;
	uint64 participant_index, receipt_index;
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	volatile bool caught = false;

	reset_fixture();
	UT_ASSERT(seed_frozen_certificate(0, &dispatch, &participant_index, &receipt_index));
	participant = ctrc_participant_entries()[participant_index];
	receipt = ctrc_receipt_entries()[receipt_index];
	ack = ctrc_participant_ack_entries()[participant_index];
	pool_upgrade_hook = throw_before_pool_write;
	PG_TRY();
	{
		(void)cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result);
	}
	PG_CATCH();
	{
		caught = true;
		/* Model PostgreSQL's error cleanup of the still-held LWLocks. */
		while (held_count != 0)
			LWLockRelease((LWLock *)held_locks[held_count - 1]);
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(
		memcmp(&participant, &ctrc_participant_entries()[participant_index], sizeof(participant)),
		0);
	UT_ASSERT_EQ(memcmp(&receipt, &ctrc_receipt_entries()[receipt_index], sizeof(receipt)), 0);
	UT_ASSERT_EQ(memcmp(&ack, &ctrc_participant_ack_entries()[participant_index], sizeof(ack)), 0);
	UT_ASSERT(ctrc_receipt_chain_exact_locked(participant_index, true));
	pool_upgrade_hook = NULL;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
	UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
}

UT_TEST(test_ack_batch_keeps_independent_copies_and_complete_final_census)
{
	CtrcAckWork work[8] = { { 0 } };
	ClusterCtrcReceipt copies[8];
	ClusterCtrcReceipt originals[8];
	ClusterCtrcLocalReleaseAckV1 initial_ack;
	ClusterCtrcDurability durability = { 0 };
	uint64 receipt_indices[8];

	reset_fixture();
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(seed_cancelled_ack_at(i, &work[i].participant, &originals[i], &initial_ack,
										&work[i].participant_index, &receipt_indices[i]));
		work[i].eligible = true;
		work[i].receipts = &copies[i];
		work[i].sorted_receipts = &copies[i];
		work[i].receipt_count = 1;
	}
	MemSet(table_visits, 0, sizeof(table_visits));
	ctrc_participant_copy_ack_batch(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].eligible);
		UT_ASSERT_EQ(memcmp(work[i].receipts, &originals[i], sizeof(originals[i])), 0);
		UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&work[i].participant,
																work[i].receipts, 1, &durability,
																&work[i].first_ack),
					 CLUSTER_CTRC_ACK_RELEASED);
	}
	ctrc_participant_copy_ack_batch(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].eligible);
		UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&work[i].participant,
																work[i].receipts, 1, &durability,
																&work[i].second_ack),
					 CLUSTER_CTRC_ACK_RELEASED);
		UT_ASSERT_EQ(memcmp(&work[i].first_ack, &work[i].second_ack, sizeof(initial_ack)), 0);
	}
	ctrc_participant_freeze_ack_batch_exact(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].frozen);
		UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_indices[i]].state, CTRC_RECEIPT_ACK_FROZEN);
		UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[work[i].participant_index],
							&work[i].second_ack, sizeof(initial_ack)),
					 0);
	}
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(table_visits[1], 16);
	UT_ASSERT_EQ(table_visits[2], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[3], 8);
}

UT_TEST(test_real_ack_driver_shares_scans_and_keeps_durability_and_barrier)
{
	ClusterCtrcParticipantEntry participants[8];
	ClusterCtrcReceipt receipts[8];
	ClusterCtrcLocalReleaseAckV1 acks[8];
	uint64 participant_indices[8], receipt_indices[8];

	reset_fixture();
	for (Size i = 0; i < 8; i++)
		UT_ASSERT(seed_cancelled_ack_at(i, &participants[i], &receipts[i], &acks[i],
										&participant_indices[i], &receipt_indices[i]));
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
	MemSet(table_visits, 0, sizeof(table_visits));
	UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
	wake_count = 0;
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(8), 8);
	CtrcBatch.active = false;
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[i]].state,
					 CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[participant_indices[i]], &acks[i],
							sizeof(acks[i])),
					 0);
	}
	UT_ASSERT_EQ(durability_calls, 16);
	UT_ASSERT_EQ(wake_count, 8);
	UT_ASSERT_EQ(barrier_calls, 1);
	UT_ASSERT_EQ(allocated, 0);
	UT_ASSERT_EQ(table_visits[1], 16);
	UT_ASSERT_EQ(table_visits[2], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[3], 8);
}

static uint64 drift_receipt_index;
static unsigned drift_at_capture;

static void
drift_during_capture(unsigned call)
{
	if (call == drift_at_capture)
		ctrc_receipt_entries()[drift_receipt_index].target.block_number++;
}

static void
drift_during_barrier(void)
{
	ctrc_receipt_entries()[drift_receipt_index].target.block_number++;
}

static void
throw_during_capture(unsigned call)
{
	if (call == 2)
		pg_re_throw();
}

static void
throw_during_barrier(void)
{
	pg_re_throw();
}

UT_TEST(test_real_ack_driver_independent_capture_and_final_drift_retain_one_key)
{
	for (int phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry snapshots[2];
		ClusterCtrcReceipt receipts[2];
		ClusterCtrcLocalReleaseAckV1 acks[2];
		uint64 participants[2], indices[2];

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_cancelled_ack_at(i, &snapshots[i], &receipts[i], &acks[i],
											&participants[i], &indices[i]));
		drift_receipt_index = indices[0];
		drift_at_capture = phase == 0 ? 2 : 4;
		if (phase < 2)
			durability_hook = drift_during_capture;
		else {
			barrier_hook = drift_during_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		}
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(2), 1);
		CtrcBatch.active = false;
		UT_ASSERT_EQ(ctrc_participant_entries()[participants[0]].state,
					 phase == 0 ? CTRC_PARTICIPANT_ACK_READY : CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(ctrc_receipt_entries()[indices[0]].state, CTRC_RECEIPT_CANCELLED);
		UT_ASSERT(
			ctrc_bytes_zero(&ctrc_participant_ack_entries()[participants[0]], sizeof(acks[0])));
		UT_ASSERT_EQ(ctrc_participant_entries()[participants[1]].state,
					 CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT_EQ(ctrc_receipt_entries()[indices[1]].state, CTRC_RECEIPT_ACK_FROZEN);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_real_ack_driver_exception_and_allocation_failure_never_publish)
{
	for (int phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry snapshots[2];
		ClusterCtrcReceipt receipts[2];
		ClusterCtrcLocalReleaseAckV1 acks[2];
		uint64 participants[2], indices[2];
		volatile bool caught = false;

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_cancelled_ack_at(i, &snapshots[i], &receipts[i], &acks[i],
											&participants[i], &indices[i]));
		if (phase == 0)
			durability_hook = throw_during_capture;
		else if (phase == 1) {
			barrier_hook = throw_during_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		} else
			allocation_failure = true;
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
		PG_TRY();
		{
			UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(2), 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		CtrcBatch.active = false;
		UT_ASSERT_EQ(caught, phase != 2);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT_EQ(held_count, 0);
		for (Size i = 0; i < 2; i++) {
			UT_ASSERT_EQ(memcmp(&ctrc_participant_entries()[participants[i]], &snapshots[i],
								sizeof(snapshots[i])),
						 0);
			UT_ASSERT_EQ(
				memcmp(&ctrc_receipt_entries()[indices[i]], &receipts[i], sizeof(receipts[i])), 0);
			UT_ASSERT(
				ctrc_bytes_zero(&ctrc_participant_ack_entries()[participants[i]], sizeof(acks[i])));
		}
	}
}

UT_TEST(test_ack_batch_final_count_summary_and_identity_faults_are_isolated)
{
	for (int fault = 0; fault < 7; fault++) {
		CtrcAckWork work[2] = { { 0 } };
		ClusterCtrcReceipt copies[2], originals[2];
		ClusterCtrcLocalReleaseAckV1 ack;
		uint64 indices[2];
		Size unused = 0;

		reset_fixture();
		for (Size i = 0; i < 2; i++) {
			UT_ASSERT(seed_cancelled_ack_at(i, &work[i].participant, &originals[i], &ack,
											&work[i].participant_index, &indices[i]));
			work[i].receipts = &copies[i];
			work[i].sorted_receipts = &copies[i];
			work[i].receipt_count = 1;
			work[i].eligible = true;
			work[i].second_ack = ack;
		}
		ctrc_participant_copy_ack_batch(work, 2);
		if (fault == 0)
			ctrc_receipt_entries()[indices[0]].state = CTRC_RECEIPT_APPLIED;
		if (fault == 1)
			ctrc_participant_entries()[work[0].participant_index].seal_generation++;
		if (fault == 2)
			ctrc_participant_ack_entries()[work[0].participant_index].crc32c = 1;
		if (fault == 3)
			ctrc_participant_entries()[work[0].participant_index].receipt_count++;
		if (fault == 4)
			MemSet(&ctrc_receipt_entries()[indices[0]], 0, sizeof(ClusterCtrcReceipt));
		if (fault == 5) {
			while (unused == indices[0] || unused == indices[1])
				unused++;
			ctrc_receipt_entries()[unused] = originals[0];
		}
		if (fault == 6)
			ctrc_receipt_entries()[indices[0]].publication.key_sequence = 2;
		originals[0] = ctrc_receipt_entries()[indices[0]];
		ack = ctrc_participant_ack_entries()[work[0].participant_index];
		ctrc_participant_freeze_ack_batch_exact(work, 2);
		UT_ASSERT(!work[0].frozen);
		UT_ASSERT(work[1].frozen);
		UT_ASSERT_EQ(
			memcmp(&ctrc_receipt_entries()[indices[0]], &originals[0], sizeof(originals[0])), 0);
		UT_ASSERT_EQ(
			memcmp(&ctrc_participant_ack_entries()[work[0].participant_index], &ack, sizeof(ack)),
			0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

/* Compile the exact top-level batch body, keeping its real shared selection
 * and finally cleanup. Only physical I/O/network leaves are fixture inputs. */
static bool driver_other_progress;
static bool driver_interrupt;

static bool
driver_no_receipt(void)
{
	return false;
}

static bool
driver_no_certificate(const ClusterCtrcOriginCertificateSnapshot *snapshot pg_attribute_unused())
{
	return false;
}

static bool
driver_no_terminal(const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
				   ClusterCtrcTerminalStatus *status pg_attribute_unused(),
				   SCN *scn pg_attribute_unused())
{
	return false;
}

static void
driver_dispatch_boundary(const ClusterCtrcCloseDispatch *dispatches pg_attribute_unused(),
						 Size count pg_attribute_unused())
{
	if (driver_other_progress)
		cluster_ctrc_stat_bump(CTRC_STAT_SEMANTIC_PROGRESS);
	if (driver_interrupt)
		pg_re_throw();
}

bool ctrc_fixture_run_pass(void);
#define cluster_ctrc_cleaner_run_pass ctrc_fixture_run_pass
#define ctrc_cleaner_clean_next_receipt driver_no_receipt
#define ctrc_cleaner_publish_certificate driver_no_certificate
#define ctrc_cleaner_terminal_sample_exact driver_no_terminal
#define cluster_gcs_ctrc_dispatch_batch driver_dispatch_boundary
#ifndef CLUSTER_CTRC_DRIVER_BODY
#define CLUSTER_CTRC_DRIVER_BODY "test_cluster_ctrc_run_pass.inc"
#endif
#include CLUSTER_CTRC_DRIVER_BODY
#undef cluster_ctrc_cleaner_run_pass
#undef ctrc_cleaner_clean_next_receipt
#undef ctrc_cleaner_publish_certificate
#undef ctrc_cleaner_terminal_sample_exact
#undef cluster_gcs_ctrc_dispatch_batch

static bool
bind_test_worker(unsigned worker)
{
	return cluster_ctrc_cleaner_bind_worker(worker);
}

UT_TEST(test_cleaner_dispatch_has_one_canonical_segment_owner)
{
	static const unsigned segments[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 256 };
	static const unsigned owners[] = { 0, 1, 2, 3, 4, 5, 6, 7, 0, 7 };

	for (unsigned worker = 0; worker < 8; worker++) {
		unsigned selected = 0;
		unsigned expected = 0;
		ClusterCtrcCloseDispatch dispatch;

		reset_fixture();
		for (unsigned i = 0; i < lengthof(segments); i++) {
			seed_origin((segments[i] - 1) * TT_SLOTS_PER_SEGMENT, 1);
			if (owners[i] == worker)
				expected++;
		}
		UT_ASSERT(bind_test_worker(worker));
		ctrc_dispatch_batch_collect();
		CtrcBatch.active = true;
		while (cluster_ctrc_origin_next_close_dispatch_shared(&dispatch)) {
			unsigned i;

			for (i = 0; i < lengthof(segments); i++)
				if (segments[i] == dispatch.key.segment_id)
					break;
			UT_ASSERT(i < lengthof(segments));
			UT_ASSERT_EQ(owners[i], worker);
			selected++;
		}
		CtrcBatch.active = false;
		UT_ASSERT_EQ(selected, expected);
		for (unsigned i = 0; i < lengthof(segments); i++) {
			ClusterCtrcOriginEntry *origin
				= &ctrc_origin_entries()[(segments[i] - 1) * TT_SLOTS_PER_SEGMENT];

			if (owners[i] != worker)
				UT_ASSERT_EQ(origin->close_dispatched_bitmap, 0);
		}
	}
}

UT_TEST(test_ack_scan_does_not_advance_foreign_shard)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index, receipt_index, selected_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack_at(TT_SLOTS_PER_SEGMENT, &snapshot, &receipt, &ack,
									&participant_index, &receipt_index));
	ctrc_participant_entries()[participant_index].state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	before = ctrc_participant_entries()[participant_index];
	UT_ASSERT(bind_test_worker(0));
	UT_ASSERT(!ctrc_participant_find_ack_ready(&snapshot, &selected_index));
	UT_ASSERT_EQ(memcmp(&before, &ctrc_participant_entries()[participant_index], sizeof(before)),
				 0);
}

UT_TEST(test_worker_binding_and_bad_keys_never_change_ownership)
{
	ClusterCtrcTxnKeyV1 key;

	reset_fixture();
	key = seed_origin(0, 0)->key;
	UT_ASSERT(!bind_test_worker(8));
	UT_ASSERT_EQ(CtrcWorkerId, -1);
	UT_ASSERT(bind_test_worker(0));
	UT_ASSERT(bind_test_worker(0));
	UT_ASSERT(!bind_test_worker(1));
	UT_ASSERT(!bind_test_worker(UINT_MAX));
	UT_ASSERT(ctrc_cleaner_owns_key(&key));
	key.owner_instance = 0;
	UT_ASSERT(!ctrc_cleaner_owns_key(&key));
	key.owner_instance = 1;
	key.segment_id = 0;
	UT_ASSERT(!ctrc_cleaner_owns_key(&key));
	key.segment_id = 257;
	UT_ASSERT(!ctrc_cleaner_owns_key(&key));
	key.segment_id = 1;
	key.reserved[0] = 1;
	UT_ASSERT(!ctrc_cleaner_owns_key(&key));
	key.reserved[0] = 0;
	/* A foreign origin's participant uses its canonical segment ordinal,
	 * not this node's segment range or its slot number. */
	key.owner_instance = 2;
	key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	key.origin_node_id = 1;
	key.segment_id = 257;
	UT_ASSERT(ctrc_cleaner_owns_key(&key));
	key.segment_id = 258;
	UT_ASSERT(!ctrc_cleaner_owns_key(&key));
	UT_ASSERT_EQ(CtrcWorkerId, 0);
}

UT_TEST(test_open_and_certificate_scans_preserve_foreign_bytes)
{
	for (unsigned worker = 0; worker < 8; worker++) {
		ClusterCtrcTxnKeyV1 selected;
		ClusterCtrcOriginCertificateSnapshot certificate;
		ClusterCtrcOriginEntry foreign[8];

		reset_fixture();
		for (unsigned i = 0; i < 8; i++) {
			ClusterCtrcOriginEntry *origin = seed_origin(i * TT_SLOTS_PER_SEGMENT, 0);

			origin->state = CTRC_ORIGIN_OPEN;
			origin->seal_generation = 0;
			foreign[i] = *origin;
		}
		UT_ASSERT(bind_test_worker(worker));
		UT_ASSERT(cluster_ctrc_origin_next_open_shared(&selected));
		UT_ASSERT_EQ(selected.segment_id, worker + 1);
		for (unsigned i = 0; i < 8; i++)
			UT_ASSERT_EQ(memcmp(&foreign[i], &ctrc_origin_entries()[i * TT_SLOTS_PER_SEGMENT],
								sizeof(foreign[i])),
						 0);
		for (unsigned i = 0; i < 8; i++) {
			ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[i * TT_SLOTS_PER_SEGMENT];

			UT_ASSERT(cluster_ctrc_origin_begin_seal_entry(origin, 800 + i));
			origin->state = CTRC_ORIGIN_CERTIFYING;
			foreign[i] = *origin;
		}
		UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&certificate));
		UT_ASSERT_EQ(certificate.origin_index, worker * TT_SLOTS_PER_SEGMENT);
		for (unsigned i = 0; i < 8; i++)
			UT_ASSERT_EQ(memcmp(&foreign[i], &ctrc_origin_entries()[i * TT_SLOTS_PER_SEGMENT],
								sizeof(foreign[i])),
						 0);
		/* An old-generation selected snapshot cannot certify its replacement. */
		ctrc_origin_entries()[certificate.origin_index].key.segment_generation++;
		UT_ASSERT(!ctrc_origin_certificate_snapshot_matches_shared(&certificate));
	}
}

UT_TEST(test_receipt_and_ack_select_only_canonical_worker)
{
	for (unsigned worker = 0; worker < 8; worker++) {
		ClusterCtrcParticipantEntry selected;
		ClusterCtrcReceipt receipt;
		uint64 participant_index, receipt_index;

		reset_fixture();
		for (unsigned i = 0; i < 8; i++) {
			ClusterCtrcOriginEntry *origin = seed_origin(i * TT_SLOTS_PER_SEGMENT, 1);
			ClusterCtrcParticipantEntry *participant;

			UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
			participant = &ctrc_participant_entries()[participant_index];
			UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &origin->key,
													   origin->grant_generation,
													   &origin->touched[0]),
						 CLUSTER_CTRC_PARTICIPANT_OPENED);
			participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING;
			participant->applied_count = participant->receipt_count = 1;
			participant->seal_generation = origin->seal_generation;
			ctrc_receipt_entries()[i].key = origin->key;
			ctrc_receipt_entries()[i].publication.grant_generation = origin->grant_generation;
			ctrc_receipt_entries()[i].state = CTRC_RECEIPT_APPLIED;
		}
		UT_ASSERT(bind_test_worker(worker));
		UT_ASSERT(ctrc_cleaner_next_applied_receipt(&selected, &receipt, &participant_index,
													&receipt_index));
		UT_ASSERT_EQ(receipt.key.segment_id, worker + 1);
		for (unsigned i = 0; i < 8; i++) {
			ClusterCtrcParticipantEntry *participant;

			UT_ASSERT(
				ctrc_participant_index(&ctrc_receipt_entries()[i].key, 0, &participant_index));
			participant = &ctrc_participant_entries()[participant_index];
			participant->state = CTRC_PARTICIPANT_ACK_READY;
			participant->applied_count = 0;
			participant->cleaned_count = 1;
		}
		UT_ASSERT(ctrc_participant_find_ack_ready(&selected, &participant_index));
		UT_ASSERT_EQ(selected.key.segment_id, worker + 1);
	}
}

UT_TEST(test_foreign_progress_and_observations_cannot_drive_local_work)
{
	ClusterCtrcCleanerWorkerObservation row;
	ClusterCtrcOriginEntry before;
	uint64 global;

	reset_fixture();
	seed_origin(0, 1);
	seed_origin(TT_SLOTS_PER_SEGMENT, 1);
	UT_ASSERT(bind_test_worker(0));
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 2);
	UT_ASSERT(cluster_ctrc_cleaner_worker_observation(0, &row));
	UT_ASSERT_EQ(row.dispatch_backlog, 1);
	/* Model the private address space of the second process, preserving all
	 * shared rows. This is not a product rebind or a shared-owner handoff. */
	CtrcWorkerId = -1;
	CtrcLocalProgress = CtrcLocalPasses = 0;
	UT_ASSERT(bind_test_worker(1));
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 2);
	UT_ASSERT(cluster_ctrc_cleaner_worker_observation(1, &row));
	UT_ASSERT_EQ(row.dispatch_backlog, 1);
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_reason_get(), CTRC_CLEANER_REASON_NONE);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_worker_reason(1), CTRC_CLEANER_REASON_RESOURCE_X);
	global = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	cluster_ctrc_stat_bump(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), global + 1);
	ctrc_origin_entries()[0].state = CTRC_ORIGIN_SEALED;
	before = ctrc_origin_entries()[0];
	UT_ASSERT(!ctrc_origin_dispatchable_locked(&ctrc_origin_entries()[0]));
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 0);
	ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT].state = CTRC_ORIGIN_SEALED;
	ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT].close_confirmed_bitmap = 1;
	ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT].close_dispatched_bitmap = 1;
	UT_ASSERT(ctrc_origin_dispatchable_locked(&ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT]));
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), global + 2);
	UT_ASSERT(!cluster_ctrc_cleaner_worker_observation(8, &row));
}

UT_TEST(test_actual_driver_ignores_other_progress_and_clears_error_batch)
{
	volatile bool caught = false;

	reset_fixture();
	driver_other_progress = true;
	driver_interrupt = false;
	UT_ASSERT(!ctrc_fixture_run_pass());
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_passes(), 0);
	UT_ASSERT(bind_test_worker(0));
	UT_ASSERT(!ctrc_fixture_run_pass());
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_passes(), 1);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), 1);
	seed_origin(0, 0);
	UT_ASSERT(ctrc_fixture_run_pass());
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 1);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_passes(), 2);
	seed_origin(1, 0);
	driver_interrupt = true;
	PG_TRY();
	{
		(void)ctrc_fixture_run_pass();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_progress(), 2);
	UT_ASSERT_EQ(cluster_ctrc_cleaner_local_passes(), 2);
	UT_ASSERT(ctrc_bytes_zero(&CtrcBatch, sizeof(CtrcBatch)));
	UT_ASSERT_EQ(held_count, 0);
	driver_interrupt = driver_other_progress = false;
}

UT_TEST(test_normal_stop_uninitialized_and_empty_observation)
{
	ClusterCtrcNormalStopObservation observed;
	ClusterCtrcSharedHeader *saved;

	reset_fixture();
	saved = CtrcShared;
	CtrcShared = NULL;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(observed.reason, CTRC_STOP_REASON_UNINITIALIZED);
	CtrcShared = saved;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(NULL), CLUSTER_NORMAL_STOP_INVALID);
	/* Generations, scheduling hints and allocator tombstones are not debt. */
	CtrcShared->global_journal_generation = 456;
	CtrcShared->global_seal_generation = 789;
	ctrc_receipt_probe_states()[3] = CTRC_RECEIPT_PROBE_TOMBSTONE;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(observed.result, CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(observed.reason, CTRC_STOP_REASON_NONE);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(flush_calls + durability_calls + wake_count, 0);
}

UT_TEST(test_normal_stop_reserved_origin_is_not_empty)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcNormalStopObservation observed;

	reset_fixture();
	key = seed_origin(3, 0)->key;
	reset_fixture();
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
		CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(ctrc_origin_entries()[3].state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(observed.domain, CTRC_STOP_DOMAIN_ORIGIN);
	UT_ASSERT_EQ(observed.reason, CTRC_STOP_REASON_RESERVED);
	UT_ASSERT_EQ(observed.object_index, 3);
	UT_ASSERT_EQ(memcmp(&observed.key, &key, sizeof(key)), 0);
	UT_ASSERT(cluster_ctrc_origin_cancel_pre_bind(&reservation));
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_normal_stop_origin_phases_and_invalid_override_pending)
{
	ClusterCtrcNormalStopObservation observed;
	ClusterCtrcOriginEntry *origin;

	for (unsigned state = CTRC_ORIGIN_OPEN; state <= CTRC_ORIGIN_RELEASE_PROVEN; state++) {
		reset_fixture();
		origin = seed_origin(0, 0);
		origin->state = state;
		if (state == CTRC_ORIGIN_OPEN)
			origin->seal_generation = 0;
		UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(observed.state, state);
		/* A later malformed/blocked object cannot hide behind the first debt. */
		seed_origin(1, 0)->state = CTRC_ORIGIN_BLOCKED;
		UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(observed.object_index, 1);
		UT_ASSERT_EQ(observed.reason, CTRC_STOP_REASON_BLOCKED);
	}
	reset_fixture();
	seed_origin(0, 0)->key.segment_id++;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(observed.reason, CTRC_STOP_REASON_MALFORMED);
	reset_fixture();
	ctrc_origin_ack_entries()[4].grant_generation = 1;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(observed.domain, CTRC_STOP_DOMAIN_ORIGIN_ACK);
}

UT_TEST(test_normal_stop_live_receipts_and_corrupt_links_are_never_ready)
{
	ClusterCtrcNormalStopObservation observed;
	ClusterCtrcReceiptHandle handle;

	for (unsigned state = CTRC_RECEIPT_PREPARED; state <= CTRC_RECEIPT_RETARGETING; state++) {
		reset_fixture();
		UT_ASSERT_EQ(prepare_fixture_receipt(seed_origin(0, 1), 123, &handle),
					 CLUSTER_CTRC_PREPARE_READY);
		/* Exercise the poll's phase classification, not a release transition. */
		handle.receipt->state = state;
		UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), state == CTRC_RECEIPT_BLOCKED
																   ? CLUSTER_NORMAL_STOP_INVALID
																   : CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(held_count, 0);
	}
	for (unsigned fault = 0; fault < 8; fault++) {
		reset_fixture();
		UT_ASSERT_EQ(prepare_fixture_receipt(seed_origin(0, 1), 123, &handle),
					 CLUSTER_CTRC_PREPARE_READY);
		switch (fault) {
		case 0:
			handle.receipt->state = CTRC_RECEIPT_FREE;
			break;
		case 1:
			ctrc_receipt_links()[handle.receipt_index].next_plus_one = handle.receipt_index + 1;
			break;
		case 2:
			ctrc_receipt_links()[handle.receipt_index].journal_generation++;
			break;
		case 3:
			ctrc_participant_receipt_heads()[handle.participant_index] = 0;
			break;
		case 4:
			handle.participant->identity.boot_incarnation = 0;
			break;
		case 5:
			handle.participant->state = CTRC_PARTICIPANT_BLOCKED;
			break;
		case 6:
			ctrc_participant_ack_entries()[handle.participant_index].grant_generation = 7;
			break;
		case 7:
			ctrc_receipt_probe_states()[handle.receipt_index] = CTRC_RECEIPT_PROBE_EMPTY;
			break;
		}
		UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(held_count, 0);
	}
	reset_fixture();
	ctrc_receipt_links()[2].journal_generation = 10;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(observed.domain, CTRC_STOP_DOMAIN_RECEIPT);
	reset_fixture();
	ctrc_receipt_entries()[2].publication.journal_sequence = 9;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_normal_stop_requires_actual_certificate_notification_reclaim)
{
	ClusterCtrcNormalStopObservation observed;
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcSealReplyResult result;
	ClusterCtrcOriginCertificateSnapshot certificate;
	ClusterCtrcOriginEntry *origin;
	uint64 participant_index, receipt_index;

	reset_fixture();
	UT_ASSERT(seed_frozen_certificate(0, &dispatch, &participant_index, &receipt_index));
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	origin = &ctrc_origin_entries()[0];
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 501));
	UT_ASSERT(cluster_ctrc_origin_ack_land_shared(
		501, &ctrc_participant_ack_entries()[participant_index]));
	LWLockAcquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	(void)ctrc_origin_dispatchable_locked(origin);
	LWLockRelease(&CtrcShared->origin_lock);
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &certificate));
	/* Exercise the actual in-memory edge AFTER the independently verified
	 * block-0 durable boundary; this fixture does not prove disk durability. */
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(origin, &certificate));
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(observed.state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 0, dispatch.request_id));
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
	UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(dispatch.request_id, 0, result));
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(held_count, 0);
	/* READY is not cached: a later legal producer revokes the observation. */
	seed_origin(1, 0);
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_normal_stop_cleaner_idle_is_local_ownership_not_progress)
{
	reset_fixture();
	UT_ASSERT(!cluster_ctrc_cleaner_local_idle());
	UT_ASSERT(cluster_ctrc_cleaner_bind_worker(7));
	UT_ASSERT(cluster_ctrc_cleaner_local_idle());
	CtrcBatch.active = true;
	UT_ASSERT(!cluster_ctrc_cleaner_local_idle());
	CtrcBatch.active = false;
	CtrcBatch.dispatch_count = 2;
	CtrcBatch.dispatch_next = 1;
	UT_ASSERT(!cluster_ctrc_cleaner_local_idle());
	CtrcBatch.dispatch_next = 2;
	UT_ASSERT(cluster_ctrc_cleaner_local_idle());
	CtrcBatch.dispatch_next = 3;
	UT_ASSERT(!cluster_ctrc_cleaner_local_idle());
	CtrcBatch.dispatch_next = 2;
	UT_ASSERT(!ctrc_fixture_run_pass());
	UT_ASSERT(cluster_ctrc_cleaner_local_idle());
}

UT_TEST(test_normal_stop_new_close_tombstone_is_not_retained_completion)
{
	const BackendType services[] = { B_LMON, B_LMS, B_LMS_WORKER, B_SINVAL_BCAST };
	for (unsigned i = 0; i < lengthof(services); i++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcParticipantEntry *participant;
		ClusterCtrcLocalReleaseAckV1 *summary, ack, frozen;
		uint64 index;
		uint16 reason;
		reset_fixture();
		origin = seed_origin(0, 1);
		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
		participant = &ctrc_participant_entries()[index];
		summary = &ctrc_participant_ack_entries()[index];
		MyBackendType = services[i];
		allow_new_service_participant = false;
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &origin->key, &origin->touched[0], origin->grant_generation,
						 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_DENIED);
		UT_ASSERT(ctrc_bytes_zero(participant, sizeof(*participant)));
		UT_ASSERT(ctrc_bytes_zero(summary, sizeof(*summary)));
		UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
		UT_ASSERT_EQ(new_service_participant_calls, 1);
		UT_ASSERT_EQ(wake_count, 0);
		UT_ASSERT_EQ(held_count, 0);
		allow_new_service_participant = true;
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &origin->key, &origin->touched[0], origin->grant_generation,
						 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
		frozen = ack;
		allow_new_service_participant = false;
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &origin->key, &origin->touched[0], origin->grant_generation,
						 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
		UT_ASSERT(memcmp(&ack, &frozen, sizeof(ack)) == 0);
		UT_ASSERT_EQ(new_service_participant_calls, 2);
		/* Actual shared certificate census retires the owned zero-range ACK.
		 * A later CLOSE cannot recreate that responsibility after sealing. */
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &origin->key, &origin->touched[0], origin->grant_generation,
						 origin->seal_generation, CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &ack),
					 CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT_EQ(new_service_participant_calls, 2);
		UT_ASSERT(ctrc_bytes_zero(participant, sizeof(*participant)));
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &origin->key, &origin->touched[0], origin->grant_generation,
						 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_DENIED);
		UT_ASSERT_EQ(new_service_participant_calls, 3);
		UT_ASSERT(ctrc_bytes_zero(participant, sizeof(*participant)));
		UT_ASSERT(ctrc_bytes_zero(summary, sizeof(*summary)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_normal_stop_close_validation_and_local_caller_are_not_service_admission)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;
	reset_fixture();
	origin = seed_origin(0, 1);
	allow_new_service_participant = false;
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&origin->key, &origin->touched[0], 0,
														 origin->seal_generation,
														 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_BLOCKED_RETAIN);
	UT_ASSERT_EQ(new_service_participant_calls, 0);
	MyBackendType = B_BACKEND;
	/* Foreground/cleaner local calls belong to the native exit/park barrier.
	 * Do not forge a service bracket while Smart shutdown waits for them. */
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &origin->key, &origin->touched[0], origin->grant_generation,
					 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	UT_ASSERT_EQ(new_service_participant_calls, 0);
	UT_ASSERT_EQ(held_count, 0);
}

/* Exercise the original fixed tables and real scanner, not a copied reducer. */
static void
finish_active_scan(ClusterCtrcActiveScan *scan)
{
	unsigned slices = 0;

	while (scan->status == CTRC_ACTIVE_SCAN_INCOMPLETE) {
		cluster_ctrc_active_scan_step(scan);
		if (++slices > 1000)
			abort();
	}
}

UT_TEST(test_active_scan_covers_fixed_arrays_in_bounded_slices)
{
	ClusterCtrcActiveScan scan;
	uint64 before, slices = 0;
	FullTransactionId reference = FullTransactionIdFromEpochAndXid(7, 10000);

	reset_fixture();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, reference));
	UT_ASSERT_EQ(scan.total_entries, UINT64_C(61440));
	do {
		before = scan.next_index;
		cluster_ctrc_active_scan_step(&scan);
		UT_ASSERT_EQ(scan.next_index - before, 64);
		UT_ASSERT_EQ(held_count, 0);
		slices++;
	} while (scan.status == CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(slices, 960);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(scan.min_full), U64FromFullTransactionId(reference));
	before = conditional_calls;
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(conditional_calls, before);
	UT_ASSERT_EQ(wake_count, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT_EQ(durability_calls, 0);
}

UT_TEST(test_active_scan_keeps_reserved_and_all_unretired_origin_states)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry *origin;

	reset_fixture();
	origin = seed_origin(0, 0);
	key = origin->key;
	MemSet(origin, 0, sizeof(*origin));
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, 7001);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(scan.min_full),
				 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, key.xid)));
	for (unsigned state = CTRC_ORIGIN_OPEN; state <= CTRC_ORIGIN_BLOCKED; state++) {
		origin->state = state;
		UT_ASSERT(
			cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
		finish_active_scan(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), key.xid);
	}
}

UT_TEST(test_active_scan_keeps_participants_outside_worker_zero_shard)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *last;

	reset_fixture();
	UT_ASSERT(cluster_ctrc_cleaner_bind_worker(0));
	origin = seed_origin(0, 0);
	last = &ctrc_participant_entries()[CtrcShared->participant_key_entries - 1];
	last->key = origin->key;
	last->key.xid = 6017;
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT_NE(ctrc_participant_lock_mask(CtrcShared->participant_key_entries - 1), 1);
	for (unsigned state = CTRC_PARTICIPANT_OPEN; state <= CTRC_PARTICIPANT_BLOCKED; state++) {
		last->state = state;
		UT_ASSERT(
			cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
		finish_active_scan(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), 6017);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_active_scan_busy_lock_retains_exact_position_and_minimum)
{
	ClusterCtrcActiveScan scan;

	reset_fixture();
	seed_origin(0, 0);
	seed_origin(1, 0)->key.xid = 6001;
	ctrc_origin_entries()[1].original_full_xid = FullTransactionIdFromEpochAndXid(7, 6001);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
	conditional_refuse_call = 2;
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT(scan.lock_busy);
	UT_ASSERT_EQ(scan.next_index, 1);
	UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), 7001);
	UT_ASSERT_EQ(held_count, 0);
	conditional_refuse_call = 0;
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT(!scan.lock_busy);
	UT_ASSERT_EQ(scan.next_index, 65);
	UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), 6001);
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
}

UT_TEST(test_active_scan_corrupt_empty_entry_invalidates_sample)
{
	ClusterCtrcActiveScan scan;

	reset_fixture();
	ctrc_origin_entries()[1].reservation_generation = 9;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT_EQ(scan.next_index, 1);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_active_scan_fresh_begin_discards_previous_partial_sample)
{
	ClusterCtrcActiveScan scan;

	reset_fixture();
	seed_origin(0, 0);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 10000)));
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(scan.next_index, 64);
	MemSet(&ctrc_origin_entries()[0], 0, sizeof(ClusterCtrcOriginEntry));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 11000)));
	UT_ASSERT_EQ(scan.next_index, 0);
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), 11000);
}

UT_TEST(test_active_scan_uses_original_origin_identity_after_pgproc_exit)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcActiveScan scan;
	uint32 grant;

	reset_fixture();
	key = fresh_origin_key();
	test_my_proc.xid = key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(3, 7001);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
	test_my_proc.xid = InvalidTransactionId;
	test_top_full = InvalidFullTransactionId;
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
							U64FromFullTransactionId(test_top_full));
	for (unsigned state = CTRC_ORIGIN_OPEN; state <= CTRC_ORIGIN_BLOCKED; state++) {
		ctrc_origin_entries()[0].state = state;
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		finish_active_scan(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(U64FromFullTransactionId(scan.min_full),
					 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(3, 7001)));
		UT_ASSERT_EQ(held_count, 0);
	}
	UT_ASSERT_EQ(top_full_reads, 1);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT_EQ(durability_calls, 0);
}

/* A later exact local origin does not retroactively identify receipts that
 * were registered without original FullXid provenance.  Admission is kept,
 * but that older participant must remain unqualified for the census. */
UT_TEST(test_local_receipt_does_not_backfill_older_unknown_receipts)
{
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle first, later;
		ClusterCtrcActiveScan scan;
		FullTransactionId original;

		reset_fixture();
		origin = seed_origin(0, 1);
		original = origin->original_full_xid;
		/* Controlled legacy/unknown ingress. The real original protection
		 * predicate still owns the exact raw key/touch, without an epoch. */
		origin->original_full_xid = InvalidFullTransactionId;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 701, &first), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(!FullTransactionIdIsValid(first.participant->original_full_xid));
		UT_ASSERT(!FullTransactionIdIsValid(first.original_full_xid));
		origin->original_full_xid = original;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, duplicate ? 701 : 702, &later),
					 duplicate ? CLUSTER_CTRC_PREPARE_DUPLICATE : CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(later.valid);
		UT_ASSERT_EQ(later.participant, first.participant);
		UT_ASSERT(!FullTransactionIdIsValid(later.participant->original_full_xid));
		UT_ASSERT(!FullTransactionIdIsValid(later.original_full_xid));
		UT_ASSERT_EQ(later.participant->receipt_count, duplicate ? 1 : 2);
		UT_ASSERT_EQ(later.participant->prepared_count, duplicate ? 1 : 2);
		UT_ASSERT_EQ(first.receipt->state, CTRC_RECEIPT_PREPARED);
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		finish_active_scan(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(scan.unqualified_entries, 1);
		UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_local_receipt_hands_original_identity_to_retained_participant)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;
	FullTransactionId original;

	reset_fixture();
	origin = seed_origin(0, 1);
	original = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	origin->original_full_xid = original;
	watched_receipt = &handle;
	guarded_original_identity_publications = 0;
	external_lock_hook = observe_receipt_origin_guard;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, original));
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	UT_ASSERT_EQ(guarded_original_identity_publications, 1);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &handle), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(guarded_original_identity_publications, 2);
	external_lock_hook = NULL;
	watched_receipt = NULL;
	/* Source retirement is a fixture interleaving, not a release certificate.
	 * The receiver must retain the original epoch after that source is gone. */
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
	UT_ASSERT_EQ(top_full_reads, 0);
	UT_ASSERT_EQ(held_count, 0);
}

static void
receipt_handle_identity_fault(ClusterCtrcReceiptHandle *handle, unsigned fault)
{
	/* Isolate consumer sensitivity from the separate producer-copy test. */
	handle->original_full_xid = FullTransactionIdFromEpochAndXid(3, handle->key.xid);
	switch (fault) {
		case 0:
			handle->original_full_xid = FullTransactionIdFromEpochAndXid(4, handle->key.xid);
			break;
		case 1:
			handle->original_full_xid = FullTransactionIdFromEpochAndXid(3, handle->key.xid + 1);
			break;
		case 2:
			handle->original_full_xid = FullTransactionIdFromU64(1);
			break;
		case 3:
			handle->participant->original_full_xid = FullTransactionIdFromEpochAndXid(4, handle->key.xid);
			break;
		case 4:
			handle->participant->original_full_xid = InvalidFullTransactionId;
			break;
	}
}

UT_TEST(test_receipt_known_handle_identity_conflict_cannot_cancel)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry participant_before;
		ClusterCtrcReceipt receipt_before;

		reset_fixture();
		origin = seed_origin(0, 1);
		origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &handle), CLUSTER_CTRC_PREPARE_READY);
		receipt_handle_identity_fault(&handle, fault);
		participant_before = *handle.participant;
		receipt_before = *handle.receipt;
		UT_ASSERT(!cluster_ctrc_receipt_cancel_shared(&handle));
		UT_ASSERT(memcmp(&participant_before, handle.participant, sizeof(participant_before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_receipt_known_handle_identity_conflict_cannot_apply)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry participant_before;
		ClusterCtrcReceipt receipt_before;
		ClusterCtrcApplyToken token, zero = { 0 };
		ClusterCtrcTargetV1 target;

		reset_fixture();
		origin = seed_origin(0, 1);
		origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &handle), CLUSTER_CTRC_PREPARE_READY);
		target = handle.receipt->target;
		target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
		target.itl_xid = origin->key.xid;
		target.itl_class = 1;
		target.itl_slot_wrap = 1;
		target.uba[0] = 1;
		target.planned_predecessor_sha256[0] = 1;
		target.planned_successor_sha256[0] = 2;
		receipt_handle_identity_fault(&handle, fault);
		participant_before = *handle.participant;
		receipt_before = *handle.receipt;
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(cluster_ctrc_receipt_apply_shared(&handle, &target, &token),
					 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
		UT_ASSERT(memcmp(&token, &zero, sizeof(token)) == 0);
		UT_ASSERT(memcmp(&participant_before, handle.participant, sizeof(participant_before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_receipt_unknown_handle_remains_unknown_after_later_exact_local_source)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle unknown, duplicate;
	FullTransactionId original;

	reset_fixture();
	origin = seed_origin(0, 1);
	original = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	origin->original_full_xid = InvalidFullTransactionId;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &unknown), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(!FullTransactionIdIsValid(unknown.original_full_xid));
	origin->original_full_xid = original;
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &duplicate), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT(!FullTransactionIdIsValid(duplicate.original_full_xid));
	UT_ASSERT(!FullTransactionIdIsValid(duplicate.participant->original_full_xid));
	UT_ASSERT(!FullTransactionIdIsValid(unknown.original_full_xid));
	/* The original pending publisher can still finish/cancel. Unknown is
	 * never retrospectively promoted to qualified FullXid provenance. */
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&unknown));
	UT_ASSERT(!FullTransactionIdIsValid(unknown.original_full_xid));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_local_receipt_refuses_conflicting_retained_original_identity)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, refused;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt_before;

	reset_fixture();
	origin = seed_origin(0, 1);
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &first), CLUSTER_CTRC_PREPARE_READY);
	first.participant->original_full_xid = origin->original_full_xid;
	before = *first.participant;
	receipt_before = *first.receipt;
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(4, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 302, &refused), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
	UT_ASSERT(memcmp(first.participant, &before, sizeof(before)) == 0);
	UT_ASSERT(memcmp(first.receipt, &receipt_before, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_receipt_original_identity_refusal_precedes_chain_blocking)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, refused;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt_before;
	uint64 participant_index;

	reset_fixture();
	origin = seed_origin(0, 1);
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &first), CLUSTER_CTRC_PREPARE_READY);
	participant_index = first.participant - ctrc_participant_entries();
	before = *first.participant;
	receipt_before = *first.receipt;
	/* A later chain check would BLOCK this owner. Identity must refuse first. */
	ctrc_participant_receipt_heads()[participant_index] = UINT64_MAX;
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(4, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 302, &refused), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
	UT_ASSERT_EQ(memcmp(first.participant, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(first.receipt, &receipt_before, sizeof(receipt_before)), 0);
	UT_ASSERT_EQ(ctrc_participant_receipt_heads()[participant_index], UINT64_MAX);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_receipt_observed_identity_refusal_precedes_chain_blocking)
{
	ClusterCtrcPreparePath path = CLUSTER_CTRC_PREPARE_PATH_UNKNOWN;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle first, refused;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt_before;
	uint64 participant_index;

	reset_fixture();
	origin = seed_origin(0, 1);
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 301, &first, &path), CLUSTER_CTRC_PREPARE_READY);
	participant_index = first.participant - ctrc_participant_entries();
	before = *first.participant;
	receipt_before = *first.receipt;
	/* A later chain check would BLOCK this owner. Identity must refuse first. */
	ctrc_participant_receipt_heads()[participant_index] = UINT64_MAX;
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(4, origin->key.xid);
	UT_ASSERT_EQ(prepare_fixture_receipt_observed(origin, 302, &refused, &path), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
	UT_ASSERT_EQ(memcmp(first.participant, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(first.receipt, &receipt_before, sizeof(receipt_before)), 0);
	UT_ASSERT_EQ(ctrc_participant_receipt_heads()[participant_index], UINT64_MAX);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(path, CLUSTER_CTRC_PREPARE_PATH_ORIGINAL_FULL_XID);
}

UT_TEST(test_receipt_never_guesses_missing_or_foreign_original_identity)
{
	for (unsigned foreign = 0; foreign < 2; foreign++) {
		ClusterCtrcOriginEntry *origin, request;
		ClusterCtrcReceiptHandle handle;

		reset_fixture();
		origin = seed_origin(0, 1);
		request = *origin;
		if (foreign) {
			MemSet(origin, 0, sizeof(*origin));
			request.key.owner_instance = 2;
			request.key.origin_node_id = 1;
			request.key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			request.key.origin_boot_incarnation = 1102;
			origin = &request;
		} else
			origin->original_full_xid = InvalidFullTransactionId;
		UT_ASSERT_EQ(prepare_fixture_receipt(origin, 301, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(!FullTransactionIdIsValid(handle.participant->original_full_xid));
		UT_ASSERT(!FullTransactionIdIsValid(handle.original_full_xid));
		UT_ASSERT_EQ(top_full_reads, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_active_scan_refuses_malformed_known_participant_identity)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcParticipantEntry *participant;
		ClusterCtrcActiveScan scan;

		reset_fixture();
		origin = seed_origin(0, 0);
		participant = &ctrc_participant_entries()[0];
		participant->key = origin->key;
		participant->state = CTRC_PARTICIPANT_OPEN;
		participant->original_full_xid = FullTransactionIdFromEpochAndXid(
			3, fault == 0 ? FrozenTransactionId : origin->key.xid + 1);
		MemSet(origin, 0, sizeof(*origin));
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		finish_active_scan(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_active_scan_refuses_unknown_or_mismatched_origin_fullxid)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterCtrcActiveScan scan;
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcOriginEntry before;

		reset_fixture();
		origin = seed_origin(0, 0);
		origin->original_full_xid = fault == 0 ? InvalidFullTransactionId
			: FullTransactionIdFromEpochAndXid(7, fault == 1 ? FrozenTransactionId : 7002);
		before = *origin;
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		cluster_ctrc_active_scan_step(&scan);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
		UT_ASSERT_EQ(scan.next_index, 0);
		UT_ASSERT(memcmp(origin, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_active_scan_participant_widens_old_epoch_and_rejects_unrepresentable_xids)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *participant;

	reset_fixture();
	origin = seed_origin(0, 0);
	participant = &ctrc_participant_entries()[0];
	participant->key = origin->key;
	participant->state = CTRC_PARTICIPANT_OPEN;
	MemSet(origin, 0, sizeof(*origin));
	participant->key.xid = UINT32_MAX - 5;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 100)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(scan.min_full),
				 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(6, UINT32_MAX - 5)));
	participant->key.xid = UINT32_C(0x80000000) + 100;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(7, 100)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
	participant->key.xid = 5;
	UT_ASSERT(cluster_ctrc_active_scan_begin(
		&scan, FullTransactionIdFromEpochAndXid(UINT32_MAX, UINT32_MAX - 10)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_active_scan_counts_raw_only_identity_coverage)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *known, *unknown;
	uint64 index;

	reset_fixture();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	origin = seed_origin(0, 0);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
	known = &ctrc_participant_entries()[index];
	known->key = origin->key;
	known->state = CTRC_PARTICIPANT_OPEN;
	known->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	MemSet(origin, 0, sizeof(*origin));
	origin = seed_origin(1, 0);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
	unknown = &ctrc_participant_entries()[index];
	unknown->key = origin->key;
	unknown->state = CTRC_PARTICIPANT_OPEN;
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, known->original_full_xid));
	unknown->original_full_xid = FullTransactionIdFromEpochAndXid(3, unknown->key.xid);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT_EQ(held_count, 0);
}


/* Exercise the shared scanner with the worker's original semantic source cut.
 * The lease first-owner incarnation is deliberately absent from this input. */
static ClusterSemanticActiveSampleFence
active_source_for_key(const ClusterCtrcTxnKeyV1 *key)
{
	ClusterSemanticActiveSampleFence source;

	MemSet(&source, 0, sizeof(source));
	source.system_identifier = key->system_identifier;
	source.record_generation = key->admission_record_generation;
	source.formation_epoch = key->formation_epoch;
	source.local_boot_incarnation = key->origin_boot_incarnation;
	source.local_node_id = key->origin_node_id;
	source.admitted_members_lo = UINT64_C(1) << key->origin_node_id;
	source.admitted_incarnation[key->origin_node_id] = key->origin_boot_incarnation;
	return source;
}

static void
finish_source_active_scan(ClusterCtrcActiveScan *scan,
						  const ClusterSemanticActiveSampleFence *source)
{
	unsigned slices = 0;

	while (scan->status == CTRC_ACTIVE_SCAN_INCOMPLETE) {
		cluster_ctrc_active_scan_step_source(scan, source);
		if (++slices > 1000)
			abort();
	}
}

/* Compose the original registration and release owners, rather than seeding
 * an OPEN row or clearing it to simulate source retirement.  PGPROC removal
 * and block-0 certificate durability remain controlled fixture boundaries. */
/* Native PGPROC locks are observed separately and may never nest with any
 * shared census lock. Scheduling and compact membership remain test leaves. */
static bool
observe_native_source_lock(LWLock *lock, LWLockMode mode, bool acquire)
{
	/* The original CTRC checker continues to reject every unknown lock.
	 * Observe the separate cleaner owner here without admitting lock nesting. */
	if (native_publish_source && lock == &native_cleaner_state.lwlock) {
		if (held_count != 0 || native_lock_held)
			abort();
		if (acquire) {
			if (native_cleaner_lock_held)
				abort();
			native_cleaner_lock_held = true;
			native_cleaner_lock_acquires++;
		} else {
			if (!native_cleaner_lock_held)
				abort();
			native_cleaner_lock_held = false;
			native_cleaner_lock_releases++;
		}
		return true;
	}
	if (native_cleaner_lock_held)
		abort();
	/* Only the opt-in source-handoff compositions admit the original native
	 * ProcArray-X -> XidGen-X pair. No shared census lock may be nested. */
	if ((native_prepared_source_exit || native_reused_source_exit) && (lock == native_xidgen_lock
		|| lock == native_procarray_lock || lock == native_twophase_lock)) {
		if (!native_source_exit || held_count != 0)
			abort();
		if (acquire) {
			if (native_prepared_depth >= lengthof(native_prepared_locks)
				|| (native_prepared_depth == 0
					? (lock == native_xidgen_lock ? mode != LW_SHARED
						: lock == native_twophase_lock && mode != LW_EXCLUSIVE)
					: native_prepared_depth != 1
					  || native_prepared_locks[0] != native_procarray_lock
					  || native_prepared_modes[0] != LW_EXCLUSIVE
					  || lock != native_xidgen_lock || mode != LW_EXCLUSIVE))
				abort();
			native_prepared_locks[native_prepared_depth] = lock;
			native_prepared_modes[native_prepared_depth++] = mode;
			native_lock_acquires++;
		} else {
			if (native_prepared_depth == 0
				|| native_prepared_locks[native_prepared_depth - 1] != lock)
				abort();
			native_prepared_depth--;
			native_lock_releases++;
		}
		native_lock_held = native_prepared_depth != 0;
		native_held_lock = native_lock_held
			? native_prepared_locks[native_prepared_depth - 1] : NULL;
		native_lock_mode = native_lock_held
			? native_prepared_modes[native_prepared_depth - 1] : LW_SHARED;
		return true;
	}
	if (lock != native_xidgen_lock && lock != native_procarray_lock) {
		if (native_lock_held)
			abort();
		return false;
	}
	if (!native_source_exit || held_count != 0)
		abort();
	if (acquire) {
		if (native_grouped_source_exit && lock == native_procarray_lock
			&& mode == LW_EXCLUSIVE) {
			FullTransactionId reference;
			FullTransactionId future;
			ClusterXidStripeSampleFence fence;

			/* Queue placement is a controlled scheduling leaf; the original
			 * leader below must clear and wake both actual PGPROC records. */
			if (native_multi_member_exit) {
				PGPROC *member = &native_proc_slots[1];
				uint32 first = 0;

				if (member->procArrayGroupMember || !TransactionIdIsValid(member->xid))
					abort();
				member->procArrayGroupMember = true;
				member->procArrayGroupMemberXid = member->xid + 2;
				pg_atomic_write_u32(&member->procArrayGroupNext, 0);
				if (!pg_atomic_compare_exchange_u32(&native_procs.procArrayGroupFirst, &first, 1))
					abort();
			}
			/* Before content ownership, the real sampler must still see
			 * the original oldest native identity in this complete queue. */
			if (native_lock_held || !MyProc->procArrayGroupMember
				|| pg_atomic_read_u32(&native_procs.procArrayGroupFirst) != (native_multi_member_exit ? 1 : 0)
				|| !native_procarray_sample(&reference, &future,
					&native_group_wait_minimum, &fence))
				abort();
			native_group_wait_observations++;
		}
		if (native_lock_held || (lock == native_xidgen_lock && mode != LW_SHARED))
			abort();
		native_lock_held = true;
		native_held_lock = lock;
		native_lock_mode = mode;
		native_lock_acquires++;
	} else {
		if (!native_lock_held || lock != native_held_lock)
			abort();
		native_lock_held = false;
		native_held_lock = NULL;
		native_lock_releases++;
	}
	return true;
}

static void
initialize_native_source(FullTransactionId original, FullTransactionId reference)
{
	MemSet(&native_variables, 0, sizeof(native_variables));
	MemSet(&native_procs, 0, sizeof(native_procs));
	MemSet(&native_proc_slots[1], 0, sizeof(native_proc_slots[1]));
	MemSet(native_xids, 0, sizeof(native_xids));
	MemSet(native_array_storage, 0, sizeof(native_array_storage));
	MemSet(native_subxids, 0, sizeof(native_subxids));
	MemSet(native_status_flags, 0, sizeof(native_status_flags));
	native_variables.nextXid = reference;
	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(reference) + 16);
	native_xids[0] = XidFromFullTransactionId(original);
	native_procs.xids = native_xids;
	native_procs.allProcs = &test_my_proc;
	native_procs.subxidStates = native_subxids;
	native_procs.statusFlags = native_status_flags;
	procArray->numProcs = procArray->maxProcs = 1;
	procArray->pgprocnos[0] = 0;
	test_my_proc.pgxactoff = 0;
	test_my_proc.lxid = 23;
	test_my_proc.xmin = XidFromFullTransactionId(original);
	pg_atomic_write_u64(&test_my_proc.cluster_read_scn_atomic, 17);
	test_my_proc.subxidStatus.count = native_subxids[0].count = 2;
	test_my_proc.subxidStatus.overflowed = native_subxids[0].overflowed = true;
	test_my_proc.subxids.xids[0] = native_xids[0] + 1;
	test_my_proc.subxids.xids[1] = native_xids[0] + 2;
	native_lock_held = false;
	native_held_lock = NULL;
	native_lock_acquires = native_lock_releases = native_future_reads = 0;
	native_group_refusals = 0;
	native_group_wakeups = 0;
	native_group_wait_observations = 0;
	native_group_wait_minimum = InvalidFullTransactionId;
	test_my_proc.pgprocno = 0;
	test_my_proc.procArrayGroupMember = false;
	pg_atomic_init_u32(&native_procs.procArrayGroupFirst, INVALID_PGPROCNO);
	pg_atomic_init_u32(&test_my_proc.procArrayGroupNext, INVALID_PGPROCNO);
	native_completed_xid = InvalidTransactionId;
	native_prepared_depth = 0;
	native_locked_gxact = NULL;
	if (native_prepared_source_exit)
		procArray->maxProcs = 2;
	native_group_wake_minimum = InvalidFullTransactionId;
	if (native_multi_member_exit) {
		PGPROC *member = &native_proc_slots[1];
		FullTransactionId other = FullTransactionIdFromU64(U64FromFullTransactionId(original) + 64);

		*member = test_my_proc;
		member->pgprocno = member->pgxactoff = 1;
		member->pid = MyProcPid + 1;
		member->xid = member->xmin = native_xids[1] = XidFromFullTransactionId(other);
		member->lxid = 24;
		member->sem = (PGSemaphore)member;
		member->subxids.xids[0] = member->xid + 1;
		member->subxids.xids[1] = member->xid + 2;
		native_subxids[1] = member->subxidStatus;
		native_status_flags[1] = member->statusFlags;
		pg_atomic_write_u64(&member->cluster_top_full_xid_atomic, U64FromFullTransactionId(other));
		procArray->numProcs = procArray->maxProcs = 2;
		procArray->pgprocnos[1] = 1;
	}
	external_lock_hook = observe_native_source_lock;
}

static FullTransactionId
assert_native_source_minimum(FullTransactionId expected)
{
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence;
	VariableCacheData before = native_variables;
	unsigned acquires = native_lock_acquires;
	unsigned reads = native_future_reads;

	UT_ASSERT(native_procarray_sample(&reference, &future, &minimum, &fence));
	UT_ASSERT(FullTransactionIdEquals(reference, native_variables.nextXid));
	UT_ASSERT(FullTransactionIdEquals(future, native_future));
	UT_ASSERT(FullTransactionIdEquals(minimum, expected));
	UT_ASSERT_EQ(memcmp(&native_variables, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(native_future_reads, reads + 1);
	UT_ASSERT_EQ(native_lock_acquires, acquires + 2);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT(!native_lock_held);
	UT_ASSERT_EQ(held_count, 0);
	return minimum;
}

/* Execute the original native PREPARE/retirement owners. The terminal
 * caller's WAL/CLOG and completed-XID value remain controlled boundaries. */
static void
exercise_native_prepared_retirement(FullTransactionId original, TransactionId latest)
{
	GlobalTransactionData gxact = { 0 };
	PGPROC before;
	PGPROC *dummy = &native_proc_slots[1];
	TransactionId children[2] = { MyProc->xid + 1, MyProc->xid + 2 };

	UT_ASSERT(native_prepared_source_exit && !native_grouped_source_exit);
	gxact.pgprocno = 1;
	LWLockAcquire(native_twophase_lock, LW_EXCLUSIVE);
	MarkAsPreparingGuts(&gxact, MyProc->xid, "original-retained", 123, 456, 789);
	GXactLoadSubxactData(&gxact, lengthof(children), children);
	LWLockRelease(native_twophase_lock);
	UT_ASSERT(native_locked_gxact == &gxact && !gxact.valid);
	UT_ASSERT_EQ(procArray->numProcs, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&dummy->cluster_top_full_xid_atomic),
		U64FromFullTransactionId(original));
	assert_native_source_minimum(original);
	MarkAsPrepared(&gxact, false);
	UT_ASSERT(gxact.valid);
	UT_ASSERT_EQ(procArray->numProcs, 2);
	UT_ASSERT_EQ(procArray->pgprocnos[1], 1);
	UT_ASSERT_EQ(native_subxids[1].count, 2);
	UT_ASSERT_EQ(dummy->subxids.xids[1], latest);
	assert_native_source_minimum(original);
	native_procarray_clear(MyProc);
	UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
	UT_ASSERT_EQ(native_xids[1], XidFromFullTransactionId(original));
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(native_variables.xactCompletionCount, 1);
	assert_native_source_minimum(original);
	memcpy(&before, dummy, sizeof(before));
	native_procarray_remove(dummy, latest);
	UT_ASSERT_EQ(procArray->numProcs, 1);
	UT_ASSERT_EQ(procArray->pgprocnos[0], 0);
	UT_ASSERT_EQ(procArray->pgprocnos[1], -1);
	UT_ASSERT_EQ(memcmp(dummy, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(native_variables.xactCompletionCount, 2);
	UT_ASSERT_EQ(native_prepared_depth, 0);
	UT_ASSERT(!native_lock_held);
	/* The complete FinishPreparedTransaction caller is outside this test. */
	native_locked_gxact = NULL;
	native_prepared_retirements++;
}

/* The allocator identity assignment and timing are controlled; both native
 * membership transitions and the shared retention fold are original bodies. */
static void
exercise_native_slot_reuse(FullTransactionId original, FullTransactionId reference,
	const ClusterSemanticActiveSampleFence *source)
{
	FullTransactionId replacement = FullTransactionIdFromEpochAndXid(
		EpochFromFullTransactionId(reference), XidFromFullTransactionId(original));
	unsigned acquires = native_lock_acquires;

	UT_ASSERT(native_reused_source_exit && !native_prepared_source_exit
		&& !native_grouped_source_exit);
	UT_ASSERT(FullTransactionIdPrecedes(original, replacement));
	UT_ASSERT(FullTransactionIdPrecedes(replacement, reference));
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic), 0);
	native_procarray_remove(MyProc, InvalidTransactionId);
	UT_ASSERT_EQ(procArray->numProcs, 0);
	UT_ASSERT_EQ(procArray->pgprocnos[0], -1);
	UT_ASSERT_EQ(native_variables.xactCompletionCount, 1);
	for (unsigned stage = 0; stage < 2; stage++) {
		ClusterCtrcActiveScan scan;
		FullTransactionId minimum;

		if (stage == 1) {
			/* Reuse this exact PGPROC and raw xid with a different original
			 * epoch, before any old shared debt is discharged. */
			MyProc->xid = MyProc->xmin = XidFromFullTransactionId(replacement);
			MyProc->lxid = 24;
			pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic,
				U64FromFullTransactionId(replacement));
			test_top_full = replacement;
			native_procarray_add(MyProc);
			UT_ASSERT_EQ(procArray->numProcs, 1);
			UT_ASSERT_EQ(procArray->pgprocnos[0], 0);
			UT_ASSERT_EQ(native_xids[0], XidFromFullTransactionId(original));
		}
		minimum = assert_native_source_minimum(stage == 0 ? native_future : replacement);
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, reference));
		scan.future_full = native_future;
		scan.min_full = minimum;
		finish_source_active_scan(&scan, source);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(scan.unqualified_entries, 0);
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&scan));
		UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
	}
	/* Retire only the new native producer. The old shared receipt/origin
	 * then continue through their original independent cleanup below. */
	native_procarray_end_transaction(MyProc, MyProc->xid + 2);
	native_procarray_remove(MyProc, InvalidTransactionId);
	test_top_full = InvalidFullTransactionId;
	UT_ASSERT_EQ(procArray->numProcs, 0);
	UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(native_variables.xactCompletionCount, 2);
	assert_native_source_minimum(native_future);
	UT_ASSERT_EQ(native_lock_acquires, acquires + 13);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT_EQ(native_prepared_depth, 0);
	native_reused_source_publications++;
}

/* The semaphore is a leaf. Its caller is the real post-unlock leader tail. */
static void
native_group_wakeup(PGSemaphore sema)
{
	PGPROC *member = &native_proc_slots[1];
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence;
	ClusterCtrcActiveScan scan;

	if (!native_multi_member_exit || !native_grouped_source_exit
		|| native_lock_held || native_cleaner_lock_held || held_count != 0
		|| sema != member->sem || native_group_wakeups != 0
		|| member->procArrayGroupMember
		|| pg_atomic_read_u32(&member->procArrayGroupNext) != INVALID_PGPROCNO
		|| TransactionIdIsValid(member->xid) || TransactionIdIsValid(MyProc->xid)
		|| TransactionIdIsValid(native_xids[0]) || TransactionIdIsValid(native_xids[1])
		|| pg_atomic_read_u64(&member->cluster_top_full_xid_atomic) != 0
		|| pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic) != 0)
		abort();
	if (!native_procarray_sample(&reference, &future, &minimum, &fence)
		|| !FullTransactionIdEquals(minimum, native_future)
		|| !cluster_ctrc_active_scan_begin(&scan, reference))
		abort();
	scan.future_full = future;
	scan.min_full = minimum;
	finish_source_active_scan(&scan, &native_group_wake_source);
	if (scan.status != CTRC_ACTIVE_SCAN_COMPLETE || scan.unqualified_entries != 0
		|| !FullTransactionIdEquals(scan.min_full, native_group_wake_expected))
		abort();
	native_group_wake_minimum = scan.min_full;
	native_group_wakeups++;
}

/* Reenter the original worker0 lifecycle with the same numeric PID. Time,
 * broadcast and admission capture are leaves; native/census scans are real. */
static void
exercise_same_pid_worker_respawn(const ClusterUndoCleanerActiveSample *old,
	const ClusterSemanticActiveSampleFence *source)
{
	ClusterUndoCleanerActiveSample sample, zero = { 0 };
	ClusterCtrcActiveScan abandoned;
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence lease;
	unsigned broadcasts = native_status_broadcasts;
	unsigned reads = native_future_reads;

	UT_ASSERT(native_worker_respawn && native_worker_respawns == 0);
	UT_ASSERT_EQ(old->worker_pid, MyProcPid);
	UT_ASSERT_EQ(native_cleaner_state.active_sample_sequence, old->sample_id);
	/* Keep an actual next-round partial scan beside the last complete image,
	 * exactly the state that must not survive a worker0 incarnation. */
	UT_ASSERT(cluster_ctrc_active_scan_begin(&abandoned, old->scan.reference_full));
	abandoned.future_full = old->scan.future_full;
	cluster_ctrc_active_scan_step_source(&abandoned, source);
	UT_ASSERT_EQ(abandoned.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT(abandoned.next_index != 0);
	undo_cleaner_active_scan = abandoned;
	undo_cleaner_active_scan_pending = true;
	undo_cleaner_publish_active_sample();
	UT_ASSERT_EQ(memcmp(&native_cleaner_state.active_sample, old, sizeof(*old)), 0);
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	UT_ASSERT_EQ(native_cleaner_state.workers[0].status, UNDO_CLEANER_SPAWNING);
	UT_ASSERT_EQ(native_cleaner_state.workers[0].pid, old->worker_pid);
	UT_ASSERT_EQ(memcmp(&native_cleaner_state.active_sample, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(native_cleaner_state.active_sample_sequence, old->sample_id);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 0);
	UT_ASSERT_EQ(undo_cleaner_active_scan.total_entries, 0);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.reference_full));
	UT_ASSERT(!native_cleaner_sample_current(old));
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!native_cleaner_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
	undo_cleaner_publish_status(UNDO_CLEANER_READY);
	/* Restore the same admitted source to show rejection is not merely a
	 * missing leaf fence. Numeric PID reuse cannot restore the old image. */
	undo_cleaner_active_admission_fence = *source;
	undo_cleaner_active_lease_fence = old->lease_fence;
	UT_ASSERT(!native_cleaner_sample_current(old));
	UT_ASSERT(!native_cleaner_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(native_status_broadcasts, broadcasts + 2);
	UT_ASSERT(native_procarray_sample(&reference, &future, &minimum, &lease));
	UT_ASSERT_EQ(native_future_reads, reads + 1);
	UT_ASSERT(FullTransactionIdEquals(minimum, native_future));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&undo_cleaner_active_scan, reference));
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 0);
	undo_cleaner_active_scan.future_full = future;
	undo_cleaner_active_scan.min_full = minimum;
	undo_cleaner_active_lease_fence = lease;
	finish_source_active_scan(&undo_cleaner_active_scan, source);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, undo_cleaner_active_scan.total_entries);
	UT_ASSERT(FullTransactionIdEquals(undo_cleaner_active_scan.min_full, old->scan.min_full));
	undo_cleaner_publish_active_sample();
	UT_ASSERT(native_cleaner_sample(&sample));
	UT_ASSERT(native_cleaner_sample_current(&sample));
	UT_ASSERT_EQ(sample.worker_pid, old->worker_pid);
	UT_ASSERT_EQ(sample.sample_id, old->sample_id + 1);
	UT_ASSERT(!native_cleaner_sample_current(old));
	UT_ASSERT(!native_cleaner_lock_held && !native_lock_held && held_count == 0);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	native_worker_respawns++;
	native_published_samples++;
}

/* Optional observer used only by the original lifecycle-to-wire composition.
 * All existing component cases leave it NULL. It runs after the completed
 * publication and its locks/current assertions, never in a shared lock. */
static void (*original_worker_publication_observer)(const ClusterUndoCleanerActiveSample *);
static bool original_retention_report_peer;
static void (*original_worker_pre_exit_observer)(void);

static void
assert_original_worker_publication(const ClusterCtrcActiveScan *scan,
								  const ClusterSemanticActiveSampleFence *source)
{
	ClusterUndoCleanerActiveSample before = native_cleaner_state.active_sample;
	ClusterUndoCleanerActiveSample sample;

	undo_cleaner_active_scan = *scan;
	undo_cleaner_active_admission_fence = *source;
	undo_cleaner_publish_active_sample();
	if (scan->status == CTRC_ACTIVE_SCAN_INCOMPLETE) {
		UT_ASSERT_EQ(memcmp(&before, &native_cleaner_state.active_sample, sizeof(before)), 0);
		MemSet(&sample, 0xa5, sizeof(sample));
		if (before.sample_id == 0) {
			UT_ASSERT(!native_cleaner_sample(&sample));
			UT_ASSERT_EQ(sample.sample_id, 0);
			UT_ASSERT(!FullTransactionIdIsValid(sample.scan.min_full));
		} else {
			UT_ASSERT(native_cleaner_sample(&sample));
			UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
		}
		UT_ASSERT(!native_cleaner_lock_held);
		UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
		return;
	}
	UT_ASSERT_EQ(scan->status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(native_cleaner_sample(&sample));
	UT_ASSERT(native_cleaner_sample_current(&sample));
	UT_ASSERT_EQ(sample.sample_id, before.sample_id + 1);
	UT_ASSERT_EQ(sample.worker_pid, MyProcPid);
	UT_ASSERT_EQ(memcmp(&sample.scan, scan, sizeof(*scan)), 0);
	if (before.sample_id != 0)
		UT_ASSERT(!native_cleaner_sample_current(&before));
	UT_ASSERT(!native_cleaner_lock_held);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	native_published_samples++;
	if (original_worker_publication_observer != NULL)
		original_worker_publication_observer(&sample);
	if (native_worker_respawn && native_worker_respawns == 0)
		exercise_same_pid_worker_respawn(&sample, source);
}

static void
exercise_original_producer_retention_lifecycle(bool applied)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity identity = { 0 };
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt copied;
	ClusterCtrcDurability durability = { 0 };
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcOriginCertificateSnapshot certificate = { 0 };
	ClusterCtrcCloseDispatch dispatch = { 0 };
	ClusterCtrcSealReplyResult reply;
	ClusterCtrcActiveScan retained, fresh, registration_cut, retirement_cut;
	ClusterSemanticActiveSampleFence source;
	FullTransactionId original, reference = FullTransactionIdFromEpochAndXid(9, 10000);
	FullTransactionId native_minimum = InvalidFullTransactionId;
	uint32 grant;
	uint16 reason;

	reset_fixture();
	key = fresh_origin_key();
	key.formation_epoch = key.cluster_epoch;
	source = active_source_for_key(&key);
	/* The lifecycle-to-wire composition fixes its required peer before any
	 * native/shared sampling or publication, not on an already complete cut. */
	if (original_retention_report_peer) {
		source.admitted_members_lo |= UINT64CONST(1) << 1;
		source.admitted_incarnation[1] = 1102;
	}
	original = FullTransactionIdFromEpochAndXid(7, key.xid);
	test_my_proc.xid = key.xid;
	test_top_full = original;
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
						U64FromFullTransactionId(original));
	if (native_source_exit) {
		initialize_native_source(original, reference);
		native_minimum = assert_native_source_minimum(original);
	}
	if (native_publish_source) {
		MemSet(&native_cleaner_state, 0, sizeof(native_cleaner_state));
		native_cleaner_lock_held = false;
		native_cleaner_lock_acquires = native_cleaner_lock_releases = 0;
		MemSet(&undo_cleaner_active_lease_fence, 0, sizeof(undo_cleaner_active_lease_fence));
		undo_cleaner_worker = 0;
		native_cleaner_state.workers[0].status = UNDO_CLEANER_READY;
		native_cleaner_state.workers[0].pid = MyProcPid;
		LWLockInitialize(&native_cleaner_state.lwlock, 0);
	}
	/* Pause the real cursor before any entry, then publish through the
	 * original reservation/receipt owners before retiring PGPROC. */
	UT_ASSERT(cluster_ctrc_active_scan_begin(&registration_cut, reference));
	if (native_source_exit) {
		/* Keep the native pre-census cut, as the original worker does. */
		registration_cut.future_full = native_future;
		registration_cut.min_full = native_minimum;
	}
	conditional_refuse_call = conditional_calls + 1;
	cluster_ctrc_active_scan_step_source(&registration_cut, &source);
	UT_ASSERT_EQ(registration_cut.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT(registration_cut.lock_busy);
	UT_ASSERT_EQ(registration_cut.next_index, 0);
	conditional_refuse_call = 0;
	if (native_publish_source)
		assert_original_worker_publication(&registration_cut, &source);
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	origin = &ctrc_origin_entries()[reservation.origin_index];
	UT_ASSERT(cluster_ctrc_active_scan_begin(&retained, reference));
	finish_source_active_scan(&retained, &source);
	UT_ASSERT_EQ(retained.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(retained.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(retained.min_full, original));
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
	identity.node_id = key.origin_node_id;
	identity.boot_incarnation = key.origin_boot_incarnation;
	identity.capability_record_generation = test_capability_generation;
	identity.formation_epoch = key.formation_epoch;
	identity.admission_record_generation = key.admission_record_generation;
	UT_ASSERT_EQ(cluster_ctrc_origin_touch_exact(&key, &identity, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 701, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, original));
	/* Only after both actual shared owners retain the source may this
	 * controlled backend retire its original transaction protection. */
	if (native_source_exit) {
		ClusterCtrcOriginEntry before_origin = *origin;
		ClusterCtrcParticipantEntry before_participant = *handle.participant;
		TransactionId latest = key.xid + 2;
		unsigned acquires = native_lock_acquires;

		native_group_wake_source = source;
		native_group_wake_expected = original;
		/* Both original ordinary native end branches run only after the
		 * actual shared owners already retain this same FullXid. */
		if (native_prepared_source_exit)
			exercise_native_prepared_retirement(original, latest);
		else
			native_procarray_end_transaction(MyProc, latest);
		UT_ASSERT_EQ(native_group_refusals, native_grouped_source_exit ? 1 : 0);
		UT_ASSERT_EQ(native_group_wait_observations, native_grouped_source_exit ? 1 : 0);
		if (native_grouped_source_exit) {
			UT_ASSERT(FullTransactionIdEquals(native_group_wait_minimum, original));
			UT_ASSERT(!MyProc->procArrayGroupMember);
			UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->procArrayGroupNext), INVALID_PGPROCNO);
			UT_ASSERT_EQ(pg_atomic_read_u32(&native_procs.procArrayGroupFirst), INVALID_PGPROCNO);
		}
		UT_ASSERT_EQ(native_completed_xid, latest);
		UT_ASSERT_EQ(native_variables.xactCompletionCount,
			(native_multi_member_exit || native_prepared_source_exit
			 || (native_reused_source_exit && native_reused_source_publications != 0)) ? 2 : 1);
		UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
		UT_ASSERT_EQ(test_my_proc.xid, InvalidTransactionId);
		UT_ASSERT_EQ(test_my_proc.xmin, InvalidTransactionId);
		UT_ASSERT_EQ(test_my_proc.lxid, InvalidLocalTransactionId);
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_my_proc.cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_my_proc.cluster_read_scn_atomic), 0);
		UT_ASSERT_EQ(test_my_proc.subxidStatus.count, 0);
		UT_ASSERT(!test_my_proc.subxidStatus.overflowed);
		UT_ASSERT_EQ(native_subxids[0].count, 0);
		UT_ASSERT(!native_subxids[0].overflowed);
		if (native_multi_member_exit) {
			PGPROC *member = &native_proc_slots[1];

			UT_ASSERT_EQ(native_group_wakeups, 1);
			UT_ASSERT(FullTransactionIdEquals(native_group_wake_minimum, original));
			UT_ASSERT_EQ(member->xid, InvalidTransactionId);
			UT_ASSERT_EQ(member->xmin, InvalidTransactionId);
			UT_ASSERT_EQ(member->lxid, InvalidLocalTransactionId);
			UT_ASSERT_EQ(native_xids[1], InvalidTransactionId);
			UT_ASSERT_EQ(pg_atomic_read_u64(&member->cluster_read_scn_atomic), 0);
			UT_ASSERT_EQ(member->subxidStatus.count, 0);
			UT_ASSERT(!member->subxidStatus.overflowed);
			UT_ASSERT_EQ(native_subxids[1].count, 0);
			UT_ASSERT(!native_subxids[1].overflowed);
			UT_ASSERT(!member->procArrayGroupMember);
			UT_ASSERT_EQ(pg_atomic_read_u32(&member->procArrayGroupNext), INVALID_PGPROCNO);
		}
		UT_ASSERT_EQ(native_lock_acquires,
			acquires + (native_prepared_source_exit ? 13
				: native_grouped_source_exit ? (native_multi_member_exit ? 5 : 3) : 1));
		UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
		UT_ASSERT(!native_lock_held);
		UT_ASSERT_EQ(memcmp(origin, &before_origin, sizeof(before_origin)), 0);
		UT_ASSERT_EQ(memcmp(handle.participant, &before_participant,
						  sizeof(before_participant)), 0);
		test_top_full = InvalidFullTransactionId;
		assert_native_source_minimum(native_future);
		if (native_reused_source_exit)
			exercise_native_slot_reuse(original, reference, &source);
	} else {
		test_my_proc.xid = InvalidTransactionId;
		test_top_full = InvalidFullTransactionId;
		pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic, 0);
	}
	finish_source_active_scan(&registration_cut, &source);
	UT_ASSERT_EQ(registration_cut.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(registration_cut.unqualified_entries, 0);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&registration_cut));
	UT_ASSERT(FullTransactionIdEquals(registration_cut.min_full, original));
	if (native_publish_source)
		assert_original_worker_publication(&registration_cut, &source);
	/* Pause immediately after this origin entry was sampled.  Its original
	 * minimum must survive receipt cleanup and final source retirement. */
	UT_ASSERT(cluster_ctrc_active_scan_begin(&retirement_cut, reference));
	while (reservation.origin_index - retirement_cut.next_index
		   >= CLUSTER_CTRC_ACTIVE_SCAN_SLICE)
		cluster_ctrc_active_scan_step_source(&retirement_cut, &source);
	conditional_refuse_call = conditional_calls
		+ (reservation.origin_index - retirement_cut.next_index) + 2;
	do {
		cluster_ctrc_active_scan_step_source(&retirement_cut, &source);
	} while (retirement_cut.status == CTRC_ACTIVE_SCAN_INCOMPLETE
			 && !retirement_cut.lock_busy);
	UT_ASSERT_EQ(retirement_cut.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT(retirement_cut.lock_busy);
	UT_ASSERT_EQ(retirement_cut.next_index, reservation.origin_index + 1);
	UT_ASSERT(FullTransactionIdEquals(retirement_cut.min_full, original));
	conditional_refuse_call = 0;
	UT_ASSERT(cluster_ctrc_origin_begin_seal_shared(&key));
	if (applied) {
		ClusterCtrcTargetV1 target = handle.receipt->target;
		ClusterCtrcItlTargetIdentity expected = { 0 };
		ClusterCtrcApplyToken token;

		target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
		target.itl_xid = key.xid;
		target.itl_class = 1;
		target.itl_slot_wrap = 1;
		target.uba[0] = 1;
		target.planned_predecessor_sha256[0] = 1;
		target.planned_successor_sha256[0] = 2;
		UT_ASSERT_EQ(cluster_ctrc_receipt_apply_shared(&handle, &target, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
						 &key, &identity, grant, origin->seal_generation,
						 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_PENDING_DRAIN);
		expected.spc_oid = target.spc_oid;
		expected.db_oid = target.db_oid;
		expected.rel_number = target.rel_number;
		expected.fork_number = target.fork_number;
		expected.block_number = target.block_number;
		expected.itl_xid = target.itl_xid;
		expected.itl_slot_index = target.itl_slot_index;
		expected.itl_slot_wrap = target.itl_slot_wrap;
		expected.itl_class = target.itl_class;
		expected.needs_wal = target.needs_wal;
		memcpy(expected.uba, target.uba, sizeof(expected.uba));
		/* No durable rewrite witness: the actual shared discharge must
		 * retain APPLIED debt and the original source minimum. */
		UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl_shared(
						 &handle, &expected, CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
					 CLUSTER_CTRC_DISCHARGE_RETAIN);
		UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT(cluster_ctrc_active_scan_begin(&fresh, reference));
	if (native_publish_source) {
		fresh.future_full = native_future;
		fresh.min_full = assert_native_source_minimum(native_future);
		assert_original_worker_publication(&fresh, &source);
	}
		finish_source_active_scan(&fresh, &source);
		UT_ASSERT_EQ(fresh.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(fresh.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(fresh.min_full, original));
		/* Controlled durability leaf, not a real page/WAL rewrite. */
		durability.highest_local_lsn = durability.local_flush_lsn = 100;
		UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl_shared(
						 &handle, &expected, CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
					 CLUSTER_CTRC_DISCHARGE_CLEANED);
	} else
		UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&handle));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &key, &identity, grant, origin->seal_generation,
					 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	/* Nonempty ranges require the original cleaner copy/hash/double-sample
	 * ACK producer; CLOSE alone cannot skip it. */
	snapshot = *handle.participant;
	UT_ASSERT(ctrc_participant_copy_receipts(handle.participant_index, &snapshot, &copied, 1));
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&snapshot, &copied, 1, &durability, &ack),
				 CLUSTER_CTRC_ACK_RELEASED);
	UT_ASSERT(ctrc_participant_freeze_ack_exact(handle.participant_index, &snapshot, &copied, 1, &ack));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &key, &identity, grant, origin->seal_generation,
					 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, identity.node_id, 501));
	UT_ASSERT(cluster_ctrc_origin_ack_land_shared(501, &ack));
	LWLockAcquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	(void)ctrc_origin_dispatchable_locked(origin);
	LWLockRelease(&CtrcShared->origin_lock);
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(reservation.origin_index, &certificate));

	/* Neither a closed receipt nor the local ACK is final reclamation. */
	UT_ASSERT(cluster_ctrc_active_scan_begin(&fresh, reference));
	if (native_publish_source) {
		fresh.future_full = native_future;
		fresh.min_full = assert_native_source_minimum(native_future);
		assert_original_worker_publication(&fresh, &source);
	}
	finish_source_active_scan(&fresh, &source);
	UT_ASSERT_EQ(fresh.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(fresh.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(fresh.min_full, original));
	/* Actual volatile commit edge after the fixture's durable boundary;
	 * this does not establish a real block-0 write or flush. */
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(origin, &certificate));
	dispatch.key = key;
	dispatch.participant = identity;
	dispatch.grant_generation = grant;
	dispatch.seal_generation = origin->seal_generation;
	dispatch.suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	dispatch.request_id = 1234;
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, identity.node_id, dispatch.request_id));
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &reply));
	UT_ASSERT_EQ(reply, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT(ctrc_bytes_zero(handle.participant, sizeof(*handle.participant)));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&fresh, reference));
	if (native_publish_source) {
		fresh.future_full = native_future;
		fresh.min_full = assert_native_source_minimum(native_future);
		assert_original_worker_publication(&fresh, &source);
	}
	finish_source_active_scan(&fresh, &source);
	UT_ASSERT_EQ(fresh.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(fresh.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(fresh.min_full, original));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
	if (native_publish_source)
		assert_original_worker_publication(&fresh, &source);

	{
		ClusterCtrcOriginEntry before = *origin;

		UT_ASSERT(!cluster_ctrc_origin_note_certificate_reply_shared(dispatch.request_id + 1,
																	  identity.node_id, reply));
		UT_ASSERT(memcmp(origin, &before, sizeof(before)) == 0);
	}
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(dispatch.request_id,
																 identity.node_id, reply));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
	finish_source_active_scan(&retirement_cut, &source);
	UT_ASSERT_EQ(retirement_cut.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(retirement_cut.unqualified_entries, 0);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&retirement_cut));
	UT_ASSERT(FullTransactionIdEquals(retirement_cut.min_full, original));
	/* A completed observation stays conservative after retirement; only a
	 * fresh full cover can advance to the original reference. */
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&retained));
	UT_ASSERT(FullTransactionIdEquals(retained.min_full, original));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&fresh, reference));
	if (native_publish_source) {
		fresh.future_full = native_future;
		fresh.min_full = assert_native_source_minimum(native_future);
		assert_original_worker_publication(&fresh, &source);
	}
	finish_source_active_scan(&fresh, &source);
	UT_ASSERT_EQ(fresh.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(fresh.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(fresh.min_full,
									 native_publish_source ? native_future : reference));
	if (native_publish_source)
		assert_original_worker_publication(&fresh, &source);
	UT_ASSERT_EQ(top_full_reads, 1);
	UT_ASSERT_EQ(held_count, 0);
	if (native_source_exit) {
		assert_native_source_minimum(native_future);
		UT_ASSERT_EQ(native_variables.xactCompletionCount,
			(native_multi_member_exit || native_prepared_source_exit
			 || (native_reused_source_exit && native_reused_source_publications != 0)) ? 2 : 1);
		external_lock_hook = NULL;
	}
}

UT_TEST(test_original_cancelled_producer_retains_fullxid_until_final_certificate_reply)
{
	exercise_original_producer_retention_lifecycle(false);
}

UT_TEST(test_original_applied_producer_retains_fullxid_until_final_certificate_reply)
{
	exercise_original_producer_retention_lifecycle(true);
}

UT_TEST(test_active_scan_source_matches_original_not_reference_epoch)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterSemanticActiveSampleFence source;

	reset_fixture();
	origin = seed_origin(0, 0);
	origin->key.cluster_epoch = (uint32)origin->key.formation_epoch;
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	source = active_source_for_key(&origin->key);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, origin->original_full_xid));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_active_scan_source_mismatch_retains_original_minimum)
{
	unsigned mismatch;

	for (mismatch = 0; mismatch < 6; mismatch++) {
		ClusterCtrcActiveScan scan;
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcTxnKeyV1 before;
		ClusterSemanticActiveSampleFence source;

		reset_fixture();
		origin = seed_origin(0, 0);
		origin->key.cluster_epoch = (uint32)origin->key.formation_epoch;
		origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
		before = origin->key;
		source = active_source_for_key(&origin->key);
		switch (mismatch) {
		case 0: source.system_identifier++; break;
		case 1: source.record_generation++; break;
		case 2: source.formation_epoch++; break;
		case 3:
			source.local_boot_incarnation++;
			source.admitted_incarnation[source.local_node_id]++;
			break;
		case 4:
			source.admitted_members_lo = 0;
			source.local_node_id = (source.local_node_id + 1) % 4;
			source.admitted_members_lo = UINT64_C(1) << source.local_node_id;
			source.admitted_incarnation[source.local_node_id] = source.local_boot_incarnation;
			break;
		case 5:
			/* A valid key can retain an older cluster epoch. It still constrains F. */
			origin->key.cluster_epoch++;
			before = origin->key;
			break;
		}
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		finish_source_active_scan(&scan, &source);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(scan.unqualified_entries, 1);
		UT_ASSERT(FullTransactionIdEquals(scan.min_full, origin->original_full_xid));
		UT_ASSERT_EQ(memcmp(&before, &origin->key, sizeof(before)), 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_active_scan_unknown_and_foreign_source_count_once)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *entry;
	ClusterSemanticActiveSampleFence source;
	uint64 index;

	reset_fixture();
	origin = seed_origin(0, 0);
	origin->key.cluster_epoch = (uint32)origin->key.formation_epoch;
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
	entry = &ctrc_participant_entries()[index];
	entry->key = origin->key;
	entry->state = CTRC_PARTICIPANT_OPEN;
	source = active_source_for_key(&origin->key);
	source.local_boot_incarnation++;
	source.admitted_incarnation[source.local_node_id]++;
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT(!FullTransactionIdIsValid(entry->original_full_xid));
	UT_ASSERT_EQ(XidFromFullTransactionId(scan.min_full), entry->key.xid);
	entry->original_full_xid = FullTransactionIdFromEpochAndXid(3, entry->key.xid);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, entry->original_full_xid));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_active_scan_foreign_origin_uses_original_cohort_member)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *entry;
	ClusterCtrcTxnKeyV1 key;
	ClusterSemanticActiveSampleFence source;
	uint64 index;

	reset_fixture();
	origin = seed_origin(0, 0);
	origin->key.cluster_epoch = (uint32)origin->key.formation_epoch;
	source = active_source_for_key(&origin->key);
	key = origin->key;
	key.owner_instance = 2;
	key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	key.origin_node_id = 1;
	key.origin_boot_incarnation = 5100;
	UT_ASSERT(ctrc_participant_index(&key, 0, &index));
	entry = &ctrc_participant_entries()[index];
	entry->key = key;
	entry->state = CTRC_PARTICIPANT_OPEN;
	entry->original_full_xid = FullTransactionIdFromEpochAndXid(3, key.xid);
	MemSet(origin, 0, sizeof(*origin));
	source.admitted_members_lo |= UINT64_C(2);
	source.admitted_incarnation[1] = 5100;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, entry->original_full_xid));
	source.admitted_incarnation[1]++;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, entry->original_full_xid));
	source.admitted_members_lo &= ~UINT64_C(2);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_source_active_scan(&scan, &source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_active_scan_missing_source_refuses_before_shared_locks)
{
	unsigned missing;

	for (missing = 0; missing < 8; missing++) {
		ClusterCtrcActiveScan scan;
		ClusterSemanticActiveSampleFence source;
		ClusterCtrcOriginEntry *origin;
		unsigned before;

		reset_fixture();
		origin = seed_origin(0, 0);
		origin->key.cluster_epoch = (uint32)origin->key.formation_epoch;
		origin->original_full_xid = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
		source = active_source_for_key(&origin->key);
		switch (missing) {
		case 0: source.system_identifier = 0; break;
		case 1: source.record_generation = 0; break;
		case 2: source.formation_epoch = 0; break;
		case 3: source.local_boot_incarnation = 0; break;
		case 4: source.local_node_id = -1; break;
		case 5: source.local_node_id = CLUSTER_MAX_NODES; break;
		case 6: source.formation_epoch = (uint64)UINT32_MAX + 1; break;
		case 7: break;
		}
		UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
		before = conditional_calls;
		cluster_ctrc_active_scan_step_source(&scan, missing == 7 ? NULL : &source);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT_EQ(scan.next_index, 0);
		UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
		UT_ASSERT_EQ(conditional_calls, before);
		UT_ASSERT_EQ(held_count, 0);
	}
}


/* Real shared producers may install a foreign key behind a sliced cursor.
 * The fixture supplies the remote grant identity, never its original epoch. */
static ClusterCtrcOriginEntry
late_foreign_request(void)
{
	ClusterCtrcOriginEntry *origin = seed_origin(0, 1);
	ClusterCtrcOriginEntry request = *origin;

	MemSet(origin, 0, sizeof(*origin));
	request.key.owner_instance = 2;
	request.key.origin_node_id = 1;
	request.key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	request.key.origin_boot_incarnation = 1102;
	return request;
}

static void
scan_past_participant(ClusterCtrcActiveScan *scan, const ClusterCtrcTxnKeyV1 *key)
{
	uint64 index;

	if (!ctrc_participant_index(key, 0, &index))
		abort();
	index += CtrcShared->origin_key_entries;
	while (scan->next_index <= index && scan->status == CTRC_ACTIVE_SCAN_INCOMPLETE)
		cluster_ctrc_active_scan_step(scan);
	if (scan->status != CTRC_ACTIVE_SCAN_INCOMPLETE)
		abort();
}

UT_TEST(test_late_unknown_receipt_invalidates_scanned_identity_coverage)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;
	uint64 cursor;

	reset_fixture();
	request = late_foreign_request();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	scan_past_participant(&scan, &request.key);
	cursor = scan.next_index;
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 401, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(!FullTransactionIdIsValid(handle.original_full_xid));
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT_EQ(scan.next_index, cursor);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_late_unknown_close_tombstone_invalidates_scanned_identity_coverage)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcActiveScan scan;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;

	reset_fixture();
	request = late_foreign_request();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	scan_past_participant(&scan, &request.key);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
		&request.key, &request.touched[0], request.grant_generation, 1,
		CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack), CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_exact_local_receipt_handoff_keeps_original_scan_progress)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;
	FullTransactionId original;

	reset_fixture();
	origin = seed_origin(0, 1);
	original = FullTransactionIdFromEpochAndXid(3, origin->key.xid);
	origin->original_full_xid = original;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	scan_past_participant(&scan, &origin->key);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 401, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_existing_unknown_receipt_duplicate_does_not_restart_scan)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;

	reset_fixture();
	request = late_foreign_request();
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 401, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	scan_past_participant(&scan, &request.key);
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 401, &handle), CLUSTER_CTRC_PREPARE_DUPLICATE);
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 1);
	UT_ASSERT_EQ(held_count, 0);
}


UT_TEST(test_complete_scan_registration_cut_revoked_and_recaptured)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan, fresh;

	reset_fixture();
	request = late_foreign_request();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&scan));
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 401, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&scan));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&fresh, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&fresh);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&fresh));
	UT_ASSERT_EQ(fresh.unqualified_entries, 1);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_unknown_registration_sequence_saturates_without_changing_prepare)
{
	ClusterCtrcOriginEntry request;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;

	reset_fixture();
	request = late_foreign_request();
	pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation, UINT64_MAX - 1);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 401, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), UINT64_MAX);
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&scan));
	request.key.slot_offset++;
	request.key.xid++;
	UT_ASSERT_EQ(prepare_fixture_receipt(&request, 402, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), UINT64_MAX);
	UT_ASSERT(!cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_unknown_registration_zero_and_absent_source_never_current)
{
	ClusterCtrcActiveScan scan;
	ClusterCtrcSharedHeader *saved;

	reset_fixture();
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(NULL));
	saved = CtrcShared;
	CtrcShared = NULL;
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&scan));
	CtrcShared = saved;
	pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation, 0);
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&scan));
	UT_ASSERT(!cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}


static ClusterCtrcOriginEntry final_slice_request;
static unsigned final_slice_registrations;

static bool
register_unknown_on_scan_unlock(LWLock *lock, LWLockMode mode, bool acquire)
{
	ClusterCtrcReceiptHandle handle;

	if (acquire || lock->tranche != LWTRANCHE_CLUSTER_CTRC_PARTICIPANT)
		return false;
	(void)mode;
	external_lock_hook = NULL;
	/* Schedule the producer after releasing the scanner's original lock.
	 * The original caller then skips this already modelled release. */
	test_lock_leave(lock);
	if (prepare_fixture_receipt(&final_slice_request, 401, &handle) != CLUSTER_CTRC_PREPARE_READY)
		abort();
	final_slice_registrations++;
	return true;
}

UT_TEST(test_last_slice_registration_cannot_publish_complete_old_cut)
{
	ClusterCtrcActiveScan scan;

	reset_fixture();
	final_slice_request = late_foreign_request();
	final_slice_registrations = 0;
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	while (scan.next_index < scan.total_entries - CLUSTER_CTRC_ACTIVE_SCAN_SLICE)
		cluster_ctrc_active_scan_step(&scan);
	external_lock_hook = register_unknown_on_scan_unlock;
	cluster_ctrc_active_scan_step(&scan);
	UT_ASSERT_EQ(final_slice_registrations, 1);
	UT_ASSERT_EQ(scan.next_index, scan.total_entries);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&scan));
	UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
	UT_ASSERT_EQ(held_count, 0);
}


UT_TEST(test_native_source_exit_keeps_cancelled_receipt_and_certificate_retention)
{
	native_source_exit = true;
	exercise_original_producer_retention_lifecycle(false);
	native_source_exit = false;
}

UT_TEST(test_native_source_exit_keeps_applied_receipt_and_certificate_retention)
{
	native_source_exit = true;
	exercise_original_producer_retention_lifecycle(true);
	native_source_exit = false;
}

UT_TEST(test_native_group_exit_keeps_cancelled_receipt_and_certificate_retention)
{
	native_source_exit = native_grouped_source_exit = true;
	exercise_original_producer_retention_lifecycle(false);
	native_source_exit = native_grouped_source_exit = false;
}

UT_TEST(test_native_group_exit_keeps_applied_receipt_and_certificate_retention)
{
	native_source_exit = native_grouped_source_exit = true;
	exercise_original_producer_retention_lifecycle(true);
	native_source_exit = native_grouped_source_exit = false;
}

UT_TEST(test_original_worker_publication_keeps_cancelled_native_source_retention)
{
	native_source_exit = native_publish_source = true;
	native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(false);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_publish_source = false;
}

UT_TEST(test_original_worker_publication_keeps_applied_group_source_retention)
{
	native_source_exit = native_grouped_source_exit = native_publish_source = true;
	native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(true);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_grouped_source_exit = native_publish_source = false;
}

UT_TEST(test_multi_member_group_cancel_keeps_original_retention_through_wakeup)
{
	native_source_exit = native_grouped_source_exit = native_publish_source = true;
	native_multi_member_exit = true;
	native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(false);
	UT_ASSERT_EQ(native_group_wakeups, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_multi_member_exit = false;
	native_source_exit = native_grouped_source_exit = native_publish_source = false;
}

UT_TEST(test_multi_member_group_applied_keeps_original_retention_through_wakeup)
{
	native_source_exit = native_grouped_source_exit = native_publish_source = true;
	native_multi_member_exit = true;
	native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(true);
	UT_ASSERT_EQ(native_group_wakeups, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_multi_member_exit = false;
	native_source_exit = native_grouped_source_exit = native_publish_source = false;
}

UT_TEST(test_prepared_cancel_keeps_original_shared_retention_after_native_removal)
{
	native_source_exit = native_publish_source = native_prepared_source_exit = true;
	native_prepared_retirements = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(false);
	UT_ASSERT_EQ(native_prepared_retirements, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_publish_source = native_prepared_source_exit = false;
}

UT_TEST(test_prepared_applied_keeps_original_shared_retention_after_native_removal)
{
	native_source_exit = native_publish_source = native_prepared_source_exit = true;
	native_prepared_retirements = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(true);
	UT_ASSERT_EQ(native_prepared_retirements, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_publish_source = native_prepared_source_exit = false;
}

UT_TEST(test_reused_native_slot_cancel_retains_old_fullxid_after_same_raw_publication)
{
	native_source_exit = native_publish_source = native_reused_source_exit = true;
	native_reused_source_publications = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(false);
	UT_ASSERT_EQ(native_reused_source_publications, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_publish_source = native_reused_source_exit = false;
}

UT_TEST(test_reused_native_slot_applied_retains_old_fullxid_after_same_raw_publication)
{
	native_source_exit = native_publish_source = native_reused_source_exit = true;
	native_reused_source_publications = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(true);
	UT_ASSERT_EQ(native_reused_source_publications, 1);
	UT_ASSERT_EQ(native_published_samples, 3);
	native_source_exit = native_publish_source = native_reused_source_exit = false;
}

UT_TEST(test_same_pid_worker_respawn_cancel_restarts_scan_with_shared_retention)
{
	native_source_exit = native_publish_source = native_worker_respawn = true;
	native_worker_respawns = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(false);
	UT_ASSERT_EQ(native_worker_respawns, 1);
	UT_ASSERT_EQ(native_published_samples, 4);
	native_source_exit = native_publish_source = native_worker_respawn = false;
}

UT_TEST(test_same_pid_worker_respawn_applied_restarts_scan_with_shared_retention)
{
	native_source_exit = native_publish_source = native_worker_respawn = true;
	native_worker_respawns = native_published_samples = 0;
	exercise_original_producer_retention_lifecycle(true);
	UT_ASSERT_EQ(native_worker_respawns, 1);
	UT_ASSERT_EQ(native_published_samples, 4);
	native_source_exit = native_publish_source = native_worker_respawn = false;
}

/* Execute the original worker0 sampler/continuation/publisher sequence with
 * actual native PGPROC and census owners. Scheduling and source-current
 * refusal injection remain explicit leaves, not a backend worker run. */
static void
exercise_worker_active_pass(unsigned fault)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcParticipantIdentity identity = { 0 };
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle receipt;
	ClusterUndoCleanerActiveSample first, second, zero = { 0 };
	FullTransactionId original, reference = FullTransactionIdFromEpochAndXid(9, 10000);
	uint32 grant;
	unsigned captures, reads, slices = 0;

	reset_fixture();
	native_source_exit = native_publish_source = native_pass_mode = true;
	native_pass_capture_allowed = native_pass_lease_valid = native_pass_admission_valid = true;
	native_pass_captures = 0;
	key = fresh_origin_key();
	key.formation_epoch = key.cluster_epoch;
	original = FullTransactionIdFromEpochAndXid(7, key.xid);
	test_my_proc.xid = key.xid;
	test_top_full = original;
	initialize_native_source(original, reference);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
						U64FromFullTransactionId(original));
	native_pass_source = active_source_for_key(&key);
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_active(&key, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	origin = &ctrc_origin_entries()[reservation.origin_index];
	UT_ASSERT(cluster_ctrc_origin_open_reserved(&reservation, &grant));
	identity.node_id = key.origin_node_id;
	identity.boot_incarnation = key.origin_boot_incarnation;
	identity.capability_record_generation = test_capability_generation;
	identity.formation_epoch = key.formation_epoch;
	identity.admission_record_generation = key.admission_record_generation;
	UT_ASSERT_EQ(cluster_ctrc_origin_touch_exact(&key, &identity, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 701, &receipt), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(receipt.original_full_xid, original));
	if (original_worker_pre_exit_observer != NULL)
		original_worker_pre_exit_observer();
	/* Real native protection retires only after the original shared owners
	 * retain this exact FullXid. The coordinator must find it in the census. */
	native_procarray_end_transaction(MyProc, key.xid + 2);
	UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
	MemSet(&native_cleaner_state, 0, sizeof(native_cleaner_state));
	MemSet(&undo_cleaner_active_scan, 0, sizeof(undo_cleaner_active_scan));
	MemSet(&undo_cleaner_active_lease_fence, 0, sizeof(undo_cleaner_active_lease_fence));
	MemSet(&undo_cleaner_active_admission_fence, 0, sizeof(undo_cleaner_active_admission_fence));
	undo_cleaner_active_scan_pending = native_cleaner_lock_held = false;
	native_cleaner_lock_acquires = native_cleaner_lock_releases = 0;
	undo_cleaner_worker = 0;
	native_cleaner_state.workers[0].status = UNDO_CLEANER_READY;
	native_cleaner_state.workers[0].pid = MyProcPid;
	LWLockInitialize(&native_cleaner_state.lwlock, 0);

	if (fault == 3) {
		native_pass_capture_allowed = false;
		undo_cleaner_sample_active_pass();
		UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!undo_cleaner_active_scan_pending);
		UT_ASSERT_EQ(native_future_reads, 0);
		UT_ASSERT(!native_cleaner_sample(&first));
		UT_ASSERT_EQ(memcmp(&first, &zero, sizeof(first)), 0);
		native_pass_capture_allowed = true;
	}
	conditional_refuse_call = conditional_calls + 1;
	undo_cleaner_sample_active_pass();
	conditional_refuse_call = 0;
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT(undo_cleaner_active_scan_pending && undo_cleaner_active_scan.lock_busy);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 0);
	UT_ASSERT(FullTransactionIdEquals(undo_cleaner_active_scan.min_full, native_future));
	UT_ASSERT_EQ(native_future_reads, 1);
	UT_ASSERT_EQ(native_pass_captures, fault == 3 ? 2 : 1);
	UT_ASSERT(!native_cleaner_sample(&first));
	UT_ASSERT_EQ(memcmp(&first, &zero, sizeof(first)), 0);

	if (fault == 1 || fault == 2) {
		native_pass_lease_valid = fault != 1;
		native_pass_admission_valid = fault != 2;
		undo_cleaner_sample_active_pass();
		UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!undo_cleaner_active_scan_pending);
		UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 0);
		UT_ASSERT_EQ(native_future_reads, 1);
		UT_ASSERT(!native_cleaner_sample(&first));
		UT_ASSERT_EQ(memcmp(&first, &zero, sizeof(first)), 0);
		native_pass_lease_valid = native_pass_admission_valid = true;
		undo_cleaner_sample_active_pass();
		UT_ASSERT_EQ(native_future_reads, 2);
		UT_ASSERT_EQ(native_pass_captures, 2);
	}
	captures = native_pass_captures;
	reads = native_future_reads;
	while (undo_cleaner_active_scan.status == CTRC_ACTIVE_SCAN_INCOMPLETE) {
		undo_cleaner_sample_active_pass();
		UT_ASSERT(++slices <= 1000);
		UT_ASSERT_EQ(native_pass_captures, captures);
		UT_ASSERT_EQ(native_future_reads, reads);
	}
	UT_ASSERT(slices > 1);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, undo_cleaner_active_scan.total_entries);
	UT_ASSERT(native_cleaner_sample(&first));
	UT_ASSERT(native_cleaner_sample_current(&first));
	UT_ASSERT(FullTransactionIdEquals(first.scan.min_full, original));
	UT_ASSERT_EQ(first.scan.unqualified_entries, 0);
	UT_ASSERT_EQ(first.sample_id, 1);

	/* A completed round starts with a fresh native observation; an incomplete
	 * successor retains the old complete publication until full coverage. */
	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(native_future) + 16);
	undo_cleaner_sample_active_pass();
	UT_ASSERT_EQ(native_pass_captures, captures + 1);
	UT_ASSERT_EQ(native_future_reads, reads + 1);
	UT_ASSERT(undo_cleaner_active_scan_pending);
	UT_ASSERT(native_cleaner_sample(&second));
	UT_ASSERT_EQ(memcmp(&first, &second, sizeof(first)), 0);
	while (undo_cleaner_active_scan.status == CTRC_ACTIVE_SCAN_INCOMPLETE) {
		undo_cleaner_sample_active_pass();
		UT_ASSERT(++slices <= 2000);
	}
	UT_ASSERT(native_cleaner_sample(&second));
	UT_ASSERT_EQ(second.sample_id, first.sample_id + 1);
	UT_ASSERT(FullTransactionIdEquals(second.scan.min_full, original));
	UT_ASSERT(FullTransactionIdEquals(second.scan.future_full, native_future));
	UT_ASSERT(!native_cleaner_sample_current(&first));
	UT_ASSERT(native_cleaner_sample_current(&second));
	UT_ASSERT_EQ(native_pass_captures, captures + 1);
	UT_ASSERT_EQ(native_future_reads, reads + 1);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT(!native_cleaner_lock_held && !native_lock_held);
	UT_ASSERT_EQ(held_count, 0);
	native_source_exit = native_publish_source = native_pass_mode = false;
}

UT_TEST(test_actual_worker_active_pass_keeps_old_shared_fullxid_across_slices)
{
	exercise_worker_active_pass(0);
}

UT_TEST(test_actual_worker_active_pass_restarts_after_original_lease_refusal)
{
	exercise_worker_active_pass(1);
}

UT_TEST(test_actual_worker_active_pass_restarts_after_original_source_refusal)
{
	exercise_worker_active_pass(2);
}

UT_TEST(test_actual_worker_active_pass_does_not_sample_native_without_source)
{
	exercise_worker_active_pass(3);
}

/* Capture and receiver publication use the real source and shared engine.
 * The origin transport authentication and native execution are boundaries. */
UT_TEST(test_origin_fullxid_v2_foreign_receiver_retains_source)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry saved_participant, *participant;
	ClusterCtrcReceipt saved_receipt, *receipt;
	ClusterCtrcActiveScan scan;
	FullTransactionId original;
	uint8 bytes[144];
	uint32 grant;

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 2);
	key = origin->key;
	receiver = origin->touched[1];
	grant = origin->grant_generation;
	original = origin->original_full_xid;
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&key, &receiver, grant, bytes));
	fixture_receipt_inputs(origin, 901, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	cluster_node_id = receiver.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&key, &receiver, grant,
		&publication, &target, bytes, sizeof(bytes), &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, original));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&key, &receiver, grant,
		&publication, &target, bytes, sizeof(bytes), &handle), CLUSTER_CTRC_PREPARE_DUPLICATE);
	participant = handle.participant;
	receipt = handle.receipt;
	saved_participant = *participant;
	saved_receipt = *receipt;
	/* Equal raw XID cannot replace this already retained original epoch. */
	bytes[12] ^= 1;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&key, &receiver, grant,
		&publication, &target, bytes, sizeof(bytes), &handle), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(memcmp(&saved_participant, participant, sizeof(saved_participant)) == 0);
	UT_ASSERT(memcmp(&saved_receipt, receipt, sizeof(saved_receipt)) == 0);
	UT_ASSERT(!handle.valid);
	/* This source-retirement interleaving is not a release certificate. */
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_origin_grant_v2_registers_and_captures_in_one_original_lock)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcTxnKeyV1 key;
	FullTransactionId original, decoded;
	uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];
	uint8 duplicate[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];
	unsigned acquisitions;
	uint32 grant;

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 1);
	origin->state = CTRC_ORIGIN_OPEN;
	key = origin->key;
	original = origin->original_full_xid;
	receiver = origin->touched[0];
	receiver.node_id = 1;
	receiver.boot_incarnation = 1102;
	acquisitions = sleepable_acquisitions;
	UT_ASSERT_EQ(cluster_ctrc_origin_grant_fullxid_v2(&key, &receiver,
		CTRC_PROOF_ACTIVE, &grant, bytes), CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(sleepable_acquisitions - acquisitions, 1);
	UT_ASSERT(cluster_ctrc_origin_has_exact_touch(origin, &receiver));
	UT_ASSERT(cluster_ctrc_origin_fullxid_match_v2(bytes, sizeof(bytes),
		&key, &receiver, grant, &decoded));
	UT_ASSERT(FullTransactionIdEquals(decoded, original));
	UT_ASSERT_EQ(cluster_ctrc_origin_grant_fullxid_v2(&key, &receiver,
		CTRC_PROOF_ACTIVE, &grant, duplicate), CLUSTER_CTRC_TOUCH_DUPLICATE);
	UT_ASSERT(memcmp(bytes, duplicate, sizeof(bytes)) == 0);
	fixture_receipt_inputs(origin, 908, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	cluster_node_id = receiver.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&key, &receiver, grant,
		&publication, &target, bytes, sizeof(bytes), &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, original));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_origin_grant_v2_refuses_unknown_identity_before_registration)
{
	unsigned scenario;

	for (scenario = 0; scenario < 4; scenario++) {
		ClusterCtrcOriginEntry *origin, saved;
		ClusterCtrcParticipantIdentity receiver;
		uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];
		uint32 grant = 99;

		reset_fixture();
		cluster_node_id = 0;
		origin = seed_origin(0, 1);
		origin->state = CTRC_ORIGIN_OPEN;
		receiver = origin->touched[0];
		receiver.node_id = 1;
		receiver.boot_incarnation = 1102;
		switch (scenario) {
			case 0: origin->original_full_xid = InvalidFullTransactionId; break;
			case 1: origin->original_full_xid = FullTransactionIdFromU64(UINT64_MAX); break;
			case 2: origin->original_full_xid = FullTransactionIdFromEpochAndXid(7, 2); break;
			case 3: origin->original_full_xid = FullTransactionIdFromEpochAndXid(7, origin->key.xid + 1); break;
		}
		saved = *origin;
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT_EQ(cluster_ctrc_origin_grant_fullxid_v2(&origin->key, &receiver,
			CTRC_PROOF_ACTIVE, &grant, bytes), CLUSTER_CTRC_TOUCH_REFUSED);
		UT_ASSERT_EQ(grant, 0);
		UT_ASSERT(ctrc_bytes_zero(bytes, sizeof(bytes)));
		UT_ASSERT(memcmp(origin, &saved, sizeof(saved)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_receiver_retains_original_v2_bytes_for_delegation)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];
	uint8 saved[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 2);
	receiver = origin->touched[1];
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
		origin->grant_generation, bytes));
	memcpy(saved, bytes, sizeof(saved));
	fixture_receipt_inputs(origin, 909, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	cluster_node_id = receiver.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
		CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(memcmp(handle.participant->original_identity_v2, saved, sizeof(saved)) == 0);
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(memcmp(handle.participant->original_identity_v2, saved, sizeof(saved)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, saved, sizeof(saved), &handle),
		CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT(memcmp(handle.participant->original_identity_v2, saved, sizeof(saved)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_local_first_registration_captures_protected_original_v2)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	FullTransactionId full;
	uint8 expected[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 1);
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &origin->touched[0],
		origin->grant_generation, expected));
	UT_ASSERT_EQ(prepare_fixture_receipt(origin, 910, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(memcmp(handle.participant->original_identity_v2, expected, sizeof(expected)) == 0);
	UT_ASSERT(cluster_ctrc_origin_fullxid_match_v2(handle.participant->original_identity_v2,
		sizeof(handle.participant->original_identity_v2), &origin->key, &origin->touched[0],
		origin->grant_generation, &full));
	UT_ASSERT(FullTransactionIdEquals(full, origin->original_full_xid));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_v2_registration_never_backfills_missing_original_bytes)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry *participant, saved;
	ClusterCtrcReceipt *receipt, saved_receipt;
	uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 2);
	receiver = origin->touched[1];
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
		origin->grant_generation, bytes));
	fixture_receipt_inputs(origin, 911, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	cluster_node_id = receiver.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, &handle), CLUSTER_CTRC_PREPARE_READY);
	participant = handle.participant;
	receipt = handle.receipt;
	UT_ASSERT(ctrc_bytes_zero(participant->original_identity_v2,
		sizeof(participant->original_identity_v2)));
	/* Numeric FullXid alone must not retroactively qualify missing custody. */
	participant->original_full_xid = origin->original_full_xid;
	saved = *participant;
	saved_receipt = *receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
		CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(memcmp(participant, &saved, sizeof(saved)) == 0);
	UT_ASSERT(memcmp(receipt, &saved_receipt, sizeof(saved_receipt)) == 0);
	UT_ASSERT(!handle.valid);
	UT_ASSERT_EQ(held_count, 0);
}

/* Local producers use the same explicit envelope, but the origin remains
 * locked through the original receiver publication and revalidates its value. */
UT_TEST(test_origin_fullxid_v2_local_producer_retains_source)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcActiveScan scan;
	FullTransactionId original;
	uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 1);
	receiver = origin->touched[0];
	original = origin->original_full_xid;
	fixture_receipt_inputs(origin, 950, &publication, &target);
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
		origin->grant_generation, bytes));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
		CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(handle.valid);
	UT_ASSERT(FullTransactionIdEquals(handle.original_full_xid, original));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
		CLUSTER_CTRC_PREPARE_DUPLICATE);
	/* This controlled retirement is not a certificate. The real participant
	 * alone must continue to constrain the original completed census. */
	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, original));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_origin_fullxid_v2_local_producer_rechecks_locked_source)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcParticipantIdentity receiver;
		ClusterCtrcPublicationIdV1 publication;
		ClusterCtrcTargetV1 target;
		ClusterCtrcReceiptHandle handle;
		uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

		reset_fixture();
		cluster_node_id = 0;
		origin = seed_origin(0, 1);
		receiver = origin->touched[0];
		fixture_receipt_inputs(origin, 951, &publication, &target);
		UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
			origin->grant_generation, bytes));
		if (fault == 0)
			origin->original_full_xid = FullTransactionIdFromEpochAndXid(
				EpochFromFullTransactionId(origin->original_full_xid) + 1, origin->key.xid);
		else if (fault == 1)
			origin->original_full_xid = InvalidFullTransactionId;
		else if (fault == 2)
			origin->grant_generation++;
		else
			origin->touched[0].boot_incarnation++;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
			publication.grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!handle.valid);
		UT_ASSERT(ctrc_bytes_zero(ctrc_participant_entries(),
			CtrcShared->participant_key_entries * sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT(ctrc_bytes_zero(ctrc_receipt_entries(),
			CtrcShared->receipt_entries * sizeof(ClusterCtrcReceipt)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_origin_fullxid_v2_rejects_missing_or_mismatched_identity)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcParticipantIdentity receiver;
		ClusterCtrcPublicationIdV1 publication;
		ClusterCtrcTargetV1 target;
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcSharedHeader before;
		uint8 bytes[144];
		Size length = sizeof(bytes);

		reset_fixture();
		cluster_node_id = 0;
		origin = seed_origin(0, 2);
		receiver = origin->touched[1];
		UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
			origin->grant_generation, bytes));
		fixture_receipt_inputs(origin, 902, &publication, &target);
		publication.requester_node_id = receiver.node_id;
		publication.requester_boot_incarnation = receiver.boot_incarnation;
		switch (fault) {
		case 0: bytes[0] = 1; break;
		case 1: bytes[0] = 3; break;
		case 2: bytes[2]--; break;
		case 3: length--; break;
		case 4: bytes[4] ^= 1; break;
		case 5: MemSet(bytes + 8, 0, 8); break;
		case 6: MemSet(bytes + 8, 0xff, 8); break;
		case 7: bytes[8] ^= 1; break;
		case 8: bytes[48] ^= 1; break;
		case 9: bytes[120] ^= 1; break;
		case 10: MemSet(bytes, 0, sizeof(bytes)); break;
		}
		cluster_node_id = receiver.node_id;
		before = *CtrcShared;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
			origin->grant_generation, &publication, &target, bytes, length, &handle),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(memcmp(&before, CtrcShared, sizeof(before)) == 0);
		UT_ASSERT(!handle.valid);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_origin_fullxid_v2_refuses_raw_only_receiver_backfill)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantIdentity receiver;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry before, *saved;
	uint8 bytes[144];

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 2);
	receiver = origin->touched[1];
	UT_ASSERT(cluster_ctrc_origin_fullxid_capture_v2(&origin->key, &receiver,
		origin->grant_generation, bytes));
	fixture_receipt_inputs(origin, 903, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	cluster_node_id = receiver.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(!FullTransactionIdIsValid(handle.participant->original_full_xid));
	saved = handle.participant;
	before = *saved;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_origin_v2(&origin->key, &receiver,
		origin->grant_generation, &publication, &target, bytes, sizeof(bytes), &handle),
		CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(memcmp(&before, saved, sizeof(before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

/* The real original producer and first foreign registration establish B's
 * immutable origin binding. The following API prepares only outgoing custody. */
static bool
seed_delegation_sender(ClusterCtrcReceiptHandle *handle,
	ClusterCtrcParticipantIdentity *destination, uint8 original[144])
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity sender;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	uint32 grant;

	reset_fixture();
	cluster_node_id = 0;
	origin = seed_origin(0, 2);
	key = origin->key;
	sender = origin->touched[1];
	grant = origin->grant_generation;
	if (!cluster_ctrc_origin_fullxid_capture_v2(&key, &sender, grant, original))
		return false;
	fixture_receipt_inputs(origin, 1001, &publication, &target);
	publication.requester_node_id = sender.node_id;
	publication.requester_boot_incarnation = sender.boot_incarnation;
	cluster_node_id = sender.node_id;
	*destination = sender;
	destination->node_id = 2;
	destination->boot_incarnation = 1103;
	return cluster_ctrc_receipt_prepare_origin_v2(&key, &sender, grant,
		&publication, &target, original, 144, handle) == CLUSTER_CTRC_PREPARE_READY;
}

static uint64
delegation_test_u64_be(const uint8 *bytes)
{
	uint64 result = 0;

	for (unsigned i = 0; i < 8; i++)
		result = (result << 8) | bytes[i];
	return result;
}

UT_TEST(test_delegation_custody_precedes_immutable_record_and_retry)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcParticipantEntry *participant, saved;
	uint8 original[144], bytes[224], retry[224];
	unsigned acquired;

	UT_ASSERT(seed_delegation_sender(&handle, &destination, original));
	participant = handle.participant;
	acquired = sleepable_acquisitions;
	UT_ASSERT(cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, bytes, sizeof(bytes)));
	UT_ASSERT_EQ(sleepable_acquisitions, acquired + 1);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(bytes[0], 0);
	UT_ASSERT_EQ(bytes[1], 1);
	UT_ASSERT_EQ(bytes[2], 0);
	UT_ASSERT_EQ(bytes[3], sizeof(bytes));
	UT_ASSERT_EQ(bytes[4], 0);
	UT_ASSERT_EQ(bytes[5], 1);
	UT_ASSERT_EQ(bytes[6] | bytes[7], 0);
	UT_ASSERT(memcmp(bytes + 8, original, sizeof(original)) == 0);
	UT_ASSERT_EQ(bytes[152], 0);
	UT_ASSERT_EQ(bytes[153], participant->identity.node_id);
	UT_ASSERT_EQ(bytes[184], 0);
	UT_ASSERT_EQ(bytes[185], destination.node_id);
	UT_ASSERT_EQ(delegation_test_u64_be(bytes + 160), participant->identity.boot_incarnation);
	UT_ASSERT_EQ(delegation_test_u64_be(bytes + 192), destination.boot_incarnation);
	UT_ASSERT_EQ(delegation_test_u64_be(bytes + 216), 1);
	UT_ASSERT_EQ(participant->delegation_count, 1);
	UT_ASSERT_EQ(participant->last_delegation_sequence, 1);
	UT_ASSERT(memcmp(&participant->delegations[destination.node_id].receiver,
		&destination, sizeof(destination)) == 0);
	UT_ASSERT_EQ(participant->delegations[destination.node_id].sequence, 1);
	saved = *participant;
	MemSet(original, 0x5a, sizeof(original));
	UT_ASSERT(cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, retry, sizeof(retry)));
	UT_ASSERT(memcmp(bytes, retry, sizeof(bytes)) == 0);
	UT_ASSERT(memcmp(&saved, participant, sizeof(saved)) == 0);
	/* Another destination gets a distinct sequence without replacing the first. */
	destination.node_id = 3;
	destination.boot_incarnation = 1104;
	UT_ASSERT(cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, retry, sizeof(retry)));
	UT_ASSERT_EQ(participant->delegation_count, 2);
	UT_ASSERT_EQ(delegation_test_u64_be(retry + 216), 2);
	UT_ASSERT(memcmp(participant->original_identity_v2, bytes + 8, 144) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_custody_refusal_preserves_row_and_clears_output)
{
	for (unsigned fault = 0; fault < 20; fault++) {
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantIdentity destination, sender;
		ClusterCtrcParticipantEntry saved, *participant;
		ClusterCtrcTxnKeyV1 key;
		uint8 original[144], bytes[224];
		uint32 grant;
		Size length = sizeof(bytes);

		UT_ASSERT(seed_delegation_sender(&handle, &destination, original));
		participant = handle.participant;
		key = participant->key;
		sender = participant->identity;
		grant = participant->grant_generation;
		switch (fault) {
			case 0: sender.boot_incarnation++; break;
			case 1: sender.capability_record_generation++; break;
			case 2: sender.admission_record_generation++; break;
			case 3: grant++; break;
			case 4: key.root_generation++; break;
			case 5: participant->original_identity_v2[0] = 1; break;
			case 6: MemSet(participant->original_identity_v2, 0, 144); break;
			case 7: participant->original_identity_v2[12] ^= 1; break;
			case 8: participant->original_identity_v2[120] ^= 1; break;
			case 9: participant->original_full_xid = InvalidFullTransactionId; break;
			case 10: cluster_node_id = 2; break;
			case 11: destination.node_id = sender.node_id; break;
			case 12: destination.node_id = key.origin_node_id; break;
			case 13: destination.capability_record_generation = 0; break;
			case 14: destination.formation_epoch++; break;
			case 15: destination.reserved16 = 1; break;
			case 16: participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING; break;
			case 17: participant->seal_generation = 1; break;
			case 18: participant->delegation_count = CLUSTER_CTRC_MAX_PARTICIPANTS; break;
			case 19: length--; break;
		}
		saved = *participant;
		MemSet(bytes, 0x5a, sizeof(bytes));
		UT_ASSERT(!cluster_ctrc_participant_delegate_prepare_v1(&key, &sender,
			grant, &destination, bytes, length));
		UT_ASSERT(ctrc_bytes_zero(bytes, length));
		UT_ASSERT(memcmp(&saved, participant, sizeof(saved)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_delegation_custody_destination_restart_does_not_close_old_obligation)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcParticipantEntry saved, *participant;
	uint8 original[144], bytes[224];

	UT_ASSERT(seed_delegation_sender(&handle, &destination, original));
	participant = handle.participant;
	UT_ASSERT(cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, bytes, sizeof(bytes)));
	saved = *participant;
	destination.boot_incarnation++;
	UT_ASSERT(!cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(&saved, participant, sizeof(saved)) == 0);
	UT_ASSERT(ctrc_bytes_zero(bytes, sizeof(bytes)));
	destination.boot_incarnation--;
	destination.capability_record_generation++;
	UT_ASSERT(!cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(&saved, participant, sizeof(saved)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_custody_prevents_drained_close_and_ack)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcParticipantEntry *participant, forged;
	ClusterCtrcReceipt copy;
	ClusterCtrcDurability durability = { 0 };
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;
	ClusterCtrcActiveScan scan;
	uint8 original[144], bytes[224];

	UT_ASSERT(seed_delegation_sender(&handle, &destination, original));
	participant = handle.participant;
	/* A controlled pending entry exercises old CLOSE/ACK even on the new-API
	 * RED scaffold. Production creates this entry only under the shared lock. */
	participant->delegation_count = 1;
	participant->last_delegation_sequence = 1;
	participant->delegations[destination.node_id].receiver = destination;
	participant->delegations[destination.node_id].sequence = 1;
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&handle));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&participant->key,
		&participant->identity, participant->grant_generation, 41,
		CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack), CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(participant->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(participant->receipt_count, 1);
	UT_ASSERT_EQ(participant->cancelled_count, 1);
	UT_ASSERT_EQ(participant->delegation_count, 1);
	forged = *participant;
	forged.state = CTRC_PARTICIPANT_ACK_READY;
	copy = *handle.receipt;
	MemSet(&ack, 0x5a, sizeof(ack));
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&forged, &copy, 1,
		&durability, &ack), CLUSTER_CTRC_ACK_DENIED);
	UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	UT_ASSERT(!cluster_ctrc_participant_delegate_prepare_v1(&participant->key,
		&participant->identity, participant->grant_generation, &destination, bytes, sizeof(bytes)));
	/* No source origin row is needed to retain B's already-owned responsibility. */
	MemSet(ctrc_origin_entries(), 0, sizeof(ClusterCtrcOriginEntry));
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, participant->original_full_xid));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_custody_blocks_frozen_certificate_reclamation)
{
	ClusterCtrcParticipantEntry snapshot, *participant, saved;
	ClusterCtrcReceipt copy;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;
	ClusterCtrcParticipantIdentity destination;
	uint64 participant_index, receipt_index;

	reset_fixture();
	cluster_node_id = 0;
	UT_ASSERT(seed_cancelled_ack(&snapshot, &copy, &ack, &participant_index, &receipt_index));
	UT_ASSERT(ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &copy, 1, &ack));
	participant = &ctrc_participant_entries()[participant_index];
	destination = participant->identity;
	destination.node_id = 1;
	destination.boot_incarnation++;
	/* Fault injection: even a pre-existing valid frozen summary cannot hide
	 * an outstanding downstream obligation from the certificate consumer. */
	participant->delegation_count = 1;
	participant->last_delegation_sequence = 1;
	participant->delegations[1].receiver = destination;
	participant->delegations[1].sequence = 1;
	saved = *participant;
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&participant->key,
		&participant->identity, participant->grant_generation, participant->seal_generation,
		CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &ack), CTRC_SEAL_REPLY_BLOCKED_RETAIN);
	/* Preserve the original certificate refusal: no row or receipt mutation. */
	UT_ASSERT(memcmp(&saved, participant, sizeof(saved)) == 0);
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_index].state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(held_count, 0);
}

static bool
seed_delegation_record(ClusterCtrcReceiptHandle *sender_handle,
	ClusterCtrcParticipantIdentity *destination, ClusterCtrcPublicationIdV1 *publication,
	ClusterCtrcTargetV1 *target, uint8 record[224])
{
	uint8 original[144];

	if (!seed_delegation_sender(sender_handle, destination, original)
		|| !cluster_ctrc_participant_delegate_prepare_v1(&sender_handle->key,
			&sender_handle->participant->identity, sender_handle->participant->grant_generation,
			destination, record, 224))
		return false;
	*publication = sender_handle->receipt->publication;
	publication->requester_node_id = destination->node_id;
	publication->requester_boot_incarnation = destination->boot_incarnation;
	publication->capability_record_generation = destination->capability_record_generation;
	publication->operation_id++;
	publication->journal_sequence = 0;
	publication->key_sequence = 0;
	publication->journal_slot_generation = 0;
	*target = sender_handle->receipt->target;
	cluster_node_id = destination->node_id;
	return true;
}

UT_TEST(test_delegation_receiver_registers_original_and_parent_before_handle)
{
	ClusterCtrcReceiptHandle sender_handle, received, duplicate;
	ClusterCtrcParticipantIdentity destination, sender;
	ClusterCtrcParticipantEntry sender_before, receiver_before;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceipt receipt_before;
	ClusterCtrcActiveScan scan;
	uint8 record[224];
	ClusterCtrcPrepareResult result;

	UT_ASSERT(seed_delegation_record(&sender_handle, &destination, &publication, &target, record));
	sender = sender_handle.participant->identity;
	sender_before = *sender_handle.participant;
	result = cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &received);
	UT_ASSERT_EQ(result, CLUSTER_CTRC_PREPARE_READY);
	if (result != CLUSTER_CTRC_PREPARE_READY)
		return;
	UT_ASSERT(received.valid);
	UT_ASSERT_EQ(received.participant->identity.node_id, destination.node_id);
	UT_ASSERT(FullTransactionIdEquals(received.original_full_xid, sender_handle.original_full_xid));
	UT_ASSERT(memcmp(received.participant->original_identity_v2, record + 8, 144) == 0);
	UT_ASSERT(memcmp(&received.participant->delegation_parent.receiver, &sender, sizeof(sender)) == 0);
	UT_ASSERT_EQ(received.participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(received.participant->receipt_count, 1);
	UT_ASSERT_EQ(received.participant->prepared_count, 1);
	UT_ASSERT_EQ(received.receipt->state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT(ctrc_receipt_chain_exact_locked(received.participant_index, false));
	UT_ASSERT(memcmp(&sender_before, sender_handle.participant, sizeof(sender_before)) == 0);
	receiver_before = *received.participant;
	receipt_before = *received.receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &duplicate), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT(memcmp(&receiver_before, received.participant, sizeof(receiver_before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, received.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT(duplicate.participant == received.participant);
	UT_ASSERT(cluster_ctrc_active_scan_begin(&scan, FullTransactionIdFromEpochAndXid(9, 10000)));
	finish_active_scan(&scan);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, received.original_full_xid));
	/* The origin issued B's original, not a new original naming C. */
	{
		FullTransactionId full;
		UT_ASSERT(!cluster_ctrc_origin_fullxid_match_v2(record + 8, 144,
			&sender_handle.key, &destination, sender_handle.participant->grant_generation, &full));
	}
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_receiver_rejects_unbound_or_malformed_carrier_without_owner_change)
{
	for (unsigned fault = 0; fault < 34; fault++) {
		ClusterCtrcReceiptHandle sender_handle, received;
		ClusterCtrcParticipantIdentity destination, sender;
		ClusterCtrcParticipantEntry sender_before;
		ClusterCtrcPublicationIdV1 publication;
		ClusterCtrcTargetV1 target;
		ClusterCtrcReceipt receipt_before;
		ClusterCtrcTxnKeyV1 key;
		uint8 record[224];
		uint32 grant;
		uint64 index;
		Size length = sizeof(record);

		UT_ASSERT(seed_delegation_record(&sender_handle, &destination, &publication, &target, record));
		sender = sender_handle.participant->identity;
		key = sender_handle.key;
		grant = sender_handle.participant->grant_generation;
		UT_ASSERT(ctrc_participant_index(&key, destination.node_id, &index));
		sender_before = *sender_handle.participant;
		receipt_before = *sender_handle.receipt;
		switch (fault) {
			case 0: record[1] = 2; break;
			case 1: record[3]--; break;
			case 2: record[5] = 0; break;
			case 3: record[5] = 2; break;
			case 4: record[7] = 1; break;
			case 5: record[8] = 1; break;
			case 6: record[10]--; break;
			case 7: record[12]++; break;
			case 8: MemSet(record + 16, 0xff, 8); break;
			case 9: MemSet(record + 16, 0, 8); break;
			case 10: record[16]++; break;
			case 11: record[136]++; break;
			case 12: record[153]++; break;
			case 13: record[155] = 1; break;
			case 14: record[159]++; break;
			case 15: record[167]++; break;
			case 16: record[175]++; break;
			case 17: record[183]++; break;
			case 18: record[185]++; break;
			case 19: record[187] = 1; break;
			case 20: record[191]++; break;
			case 21: record[199]++; break;
			case 22: record[207]++; break;
			case 23: record[215]++; break;
			case 24: MemSet(record + 216, 0, 8); break;
			case 25: MemSet(record + 216, 0xff, 8); break;
			case 26: sender.boot_incarnation++; break;
			case 27: sender.capability_record_generation++; break;
			case 28: destination.boot_incarnation++; break;
			case 29: destination.admission_record_generation++; break;
			case 30: key.root_generation++; break;
			case 31: grant++; break;
			case 32: cluster_node_id = sender.node_id; break;
			case 33: length--; break;
		}
		MemSet(&received, 0x5a, sizeof(received));
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&key, &sender,
			&destination, grant, &publication, &target, record, length, &received),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&received, sizeof(received)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[index], sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT(memcmp(&sender_before, sender_handle.participant, sizeof(sender_before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, sender_handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_delegation_receiver_never_backfills_existing_raw_only_registration)
{
	ClusterCtrcReceiptHandle sender_handle, raw, received;
	ClusterCtrcParticipantIdentity destination, sender;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceipt receipt_before;
	uint8 record[224];

	UT_ASSERT(seed_delegation_record(&sender_handle, &destination, &publication, &target, record));
	sender = sender_handle.participant->identity;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_shared(&sender_handle.key, &destination,
		sender_handle.participant->grant_generation, &publication, &target, &raw), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(!FullTransactionIdIsValid(raw.participant->original_full_xid));
	before = *raw.participant;
	receipt_before = *raw.receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &received), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&received, sizeof(received)));
	UT_ASSERT(memcmp(&before, raw.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, raw.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_receiver_parent_or_sequence_cannot_be_replaced)
{
	ClusterCtrcReceiptHandle sender_handle, received, refused;
	ClusterCtrcParticipantIdentity destination, sender;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcReceipt receipt_before;
	uint8 record[224];
	ClusterCtrcPrepareResult result;

	UT_ASSERT(seed_delegation_record(&sender_handle, &destination, &publication, &target, record));
	sender = sender_handle.participant->identity;
	result = cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &received);
	UT_ASSERT_EQ(result, CLUSTER_CTRC_PREPARE_READY);
	if (result != CLUSTER_CTRC_PREPARE_READY)
		return;
	before = *received.participant;
	receipt_before = *received.receipt;
	record[223] = 2;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &refused), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(memcmp(&before, received.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, received.receipt, sizeof(receipt_before)) == 0);
	/* Restoring the sequence permits only the original exact duplicate. */
	record[223] = 1;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &refused), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegation_receiver_local_drain_cannot_retire_parent)
{
	ClusterCtrcReceiptHandle sender_handle, received;
	ClusterCtrcParticipantIdentity destination, sender;
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	ClusterCtrcDurability durability = { 0 };
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcReceipt copy;
	uint8 record[224];
	uint16 reason;
	ClusterCtrcPrepareResult result;

	UT_ASSERT(seed_delegation_record(&sender_handle, &destination, &publication, &target, record));
	sender = sender_handle.participant->identity;
	result = cluster_ctrc_receipt_prepare_delegation_v1(&sender_handle.key, &sender,
		&destination, sender_handle.participant->grant_generation, &publication, &target, record,
		sizeof(record), &received);
	UT_ASSERT_EQ(result, CLUSTER_CTRC_PREPARE_READY);
	if (result != CLUSTER_CTRC_PREPARE_READY)
		return;
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&received));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&received.key, &destination,
		received.participant->grant_generation, 41, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
		CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(received.participant->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(received.participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(received.participant->delegation_count, 0);
	snapshot = *received.participant;
	snapshot.state = CTRC_PARTICIPANT_ACK_READY;
	copy = *received.receipt;
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&snapshot, &copy, 1,
		&durability, &ack), CLUSTER_CTRC_ACK_DENIED);
	UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	UT_ASSERT_EQ(sender_handle.participant->delegation_count, 1);
	UT_ASSERT_EQ(sender_handle.participant->delegations[destination.node_id].sequence, 1);
	UT_ASSERT_EQ(held_count, 0);
}

/* Source-side page bytes are a protected copy; receipt creation/application
 * and both delegation owners below are the actual production functions. */
static bool
seed_delegation_itl_source(ClusterCtrcReceiptHandle *handle,
	ClusterCtrcParticipantIdentity *destination, ClusterCtrcItlTargetIdentity *expected,
	ClusterItlSlotData *slot)
{
	ClusterCtrcTargetV1 target;
	ClusterCtrcApplyToken token;
	uint8 original[144];

	if (!seed_delegation_sender(handle, destination, original))
		return false;
	MemSet(slot, 0, sizeof(*slot));
	slot->xid = handle->key.xid;
	slot->wrap = 3;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->write_scn = 91;
	slot->first_change_lsn = 0x100;
	slot->undo_segment_head = uba_encode(handle->key.segment_id, 9,
		handle->key.slot_offset, 8);
	target = handle->receipt->target;
	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = 2;
	target.itl_slot_wrap = slot->wrap;
	target.itl_xid = slot->xid;
	target.itl_class = slot->flags;
	memcpy(target.uba, &slot->undo_segment_head, sizeof(target.uba));
	target.planned_predecessor_sha256[0] = 1;
	if (!cluster_ctrc_sha256_exact(slot, sizeof(*slot), target.planned_successor_sha256)
		|| cluster_ctrc_receipt_apply_shared(handle, &target, &token)
			!= CLUSTER_CTRC_APPLY_APPLIED)
		return false;
	MemSet(expected, 0, sizeof(*expected));
	expected->spc_oid = target.spc_oid;
	expected->db_oid = target.db_oid;
	expected->rel_number = target.rel_number;
	expected->fork_number = target.fork_number;
	expected->block_number = target.block_number;
	expected->itl_xid = slot->xid;
	expected->itl_slot_index = target.itl_slot_index;
	expected->itl_slot_wrap = slot->wrap;
	expected->itl_class = slot->flags;
	expected->needs_wal = target.needs_wal;
	memcpy(expected->uba, target.uba, sizeof(expected->uba));
	return true;
}

UT_TEST(test_itl_delegation_source_binds_applied_slot_before_outgoing_custody)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcItlTargetIdentity expected;
	ClusterItlSlotData slot;
	ClusterCtrcReceipt receipt_before;
	uint8 record[224];
	bool prepared;

	UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &slot));
	receipt_before = *handle.receipt;
	prepared = cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
		&handle.participant->identity, &destination, record, sizeof(record));
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	UT_ASSERT_EQ(handle.participant->delegation_count, 1);
	UT_ASSERT_EQ(handle.participant->delegations[destination.node_id].sequence, 1);
	UT_ASSERT_EQ(delegation_test_u64_be(record + 216), 1);
	UT_ASSERT(memcmp(record + 8, handle.participant->original_identity_v2, 144) == 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(handle.participant->original_full_xid),
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 7001)));
	UT_ASSERT(memcmp(handle.receipt, &receipt_before, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_itl_delegation_source_rejects_every_changed_slot_byte)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcItlTargetIdentity expected;
	ClusterItlSlotData original, changed;
	ClusterCtrcParticipantEntry saved;
	uint8 record[224];

	UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &original));
	saved = *handle.participant;
	for (unsigned i = 0; i < sizeof(original); i++) {
		changed = original;
		((uint8 *)&changed)[i] ^= 1;
		MemSet(record, 0xa5, sizeof(record));
		UT_ASSERT(!cluster_ctrc_itl_delegate_prepare_v1(&expected, &changed,
			&saved.identity, &destination, record, sizeof(record)));
		UT_ASSERT(ctrc_bytes_zero(record, sizeof(record)));
		UT_ASSERT(memcmp(handle.participant, &saved, sizeof(saved)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_itl_delegation_source_requires_exact_page_and_itl_target)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcItlTargetIdentity expected, changed;
	ClusterItlSlotData slot;
	ClusterCtrcParticipantEntry saved;
	uint8 record[224];

	UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &slot));
	saved = *handle.participant;
	for (unsigned i = 0; i < 13; i++) {
		changed = expected;
		switch (i) {
		case 0: changed.spc_oid++; break;
		case 1: changed.db_oid++; break;
		case 2: changed.rel_number++; break;
		case 3: changed.fork_number++; break;
		case 4: changed.block_number++; break;
		case 5: changed.itl_xid++; break;
		case 6: changed.itl_slot_index++; break;
		case 7: changed.itl_slot_wrap++; break;
		case 8: changed.itl_class++; break;
		case 9: changed.needs_wal = false; break;
		case 10: changed.uba[8]++; break;
		case 11: changed.reserved8[0] = 1; break;
		case 12: MemSet(changed.uba, 0, sizeof(changed.uba)); break;
		}
		MemSet(record, 0xa5, sizeof(record));
		UT_ASSERT(!cluster_ctrc_itl_delegate_prepare_v1(&changed, &slot,
			&saved.identity, &destination, record, sizeof(record)));
		UT_ASSERT(ctrc_bytes_zero(record, sizeof(record)));
		UT_ASSERT(memcmp(handle.participant, &saved, sizeof(saved)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_itl_delegation_source_rejects_nonapplied_or_corrupt_owner)
{
	for (unsigned i = 0; i < 10; i++) {
		ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantIdentity destination;
		ClusterCtrcItlTargetIdentity expected;
		ClusterItlSlotData slot;
		ClusterCtrcParticipantEntry saved;
		ClusterCtrcReceipt receipt_before;
		uint8 record[224];

		UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &slot));
		switch (i) {
		case 0: handle.receipt->state = CTRC_RECEIPT_PREPARED; break;
		case 1: handle.receipt->state = CTRC_RECEIPT_CLEANED; break;
		case 2: handle.receipt->state = CTRC_RECEIPT_CANCELLED; break;
		case 3: handle.receipt->state = CTRC_RECEIPT_BLOCKED; break;
		case 4: handle.participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING; break;
		case 5: handle.receipt->publication.grant_generation++; break;
		case 6: handle.participant->key.origin_node_id = 1; break;
		case 7: handle.participant->original_full_xid = InvalidFullTransactionId; break;
		case 8: handle.participant->original_identity_v2[8] ^= 1; break;
		case 9: ctrc_receipt_links()[handle.receipt_index].next_plus_one
			= handle.receipt_index + 1; break;
		}
		saved = *handle.participant;
		receipt_before = *handle.receipt;
		MemSet(record, 0xa5, sizeof(record));
		UT_ASSERT(!cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
			&saved.identity, &destination, record, sizeof(record)));
		UT_ASSERT(ctrc_bytes_zero(record, sizeof(record)));
		UT_ASSERT(memcmp(handle.participant, &saved, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(handle.receipt, &receipt_before, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_itl_delegation_source_exact_retry_reuses_original_obligation)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcItlTargetIdentity expected;
	ClusterItlSlotData slot;
	ClusterCtrcParticipantEntry saved;
	uint8 record[224], retry[224];
	bool prepared;

	UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &slot));
	prepared = cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
		&handle.participant->identity, &destination, record, sizeof(record));
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	saved = *handle.participant;
	UT_ASSERT(cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
		&saved.identity, &destination, retry, sizeof(retry)));
	UT_ASSERT(memcmp(record, retry, sizeof(record)) == 0);
	UT_ASSERT(memcmp(handle.participant, &saved, sizeof(saved)) == 0);
	destination.boot_incarnation++;
	UT_ASSERT(!cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
		&saved.identity, &destination, retry, sizeof(retry)));
	UT_ASSERT(ctrc_bytes_zero(retry, sizeof(retry)));
	UT_ASSERT(memcmp(handle.participant, &saved, sizeof(saved)) == 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_itl_delegation_source_record_reaches_original_receiver_engine)
{
	ClusterCtrcReceiptHandle handle, received;
	ClusterCtrcParticipantIdentity destination, sender;
	ClusterCtrcItlTargetIdentity expected;
	ClusterItlSlotData slot;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	uint8 record[224];
	bool prepared;

	UT_ASSERT(seed_delegation_itl_source(&handle, &destination, &expected, &slot));
	sender = handle.participant->identity;
	prepared = cluster_ctrc_itl_delegate_prepare_v1(&expected, &slot,
		&sender, &destination, record, sizeof(record));
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	fixture_receipt_inputs(&ctrc_origin_entries()[0], 1002, &publication, &target);
	publication.requester_node_id = destination.node_id;
	publication.requester_boot_incarnation = destination.boot_incarnation;
	publication.capability_record_generation = destination.capability_record_generation;
	cluster_node_id = destination.node_id;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&handle.key, &sender,
		&destination, handle.participant->grant_generation, &publication, &target,
		record, sizeof(record), &received), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(received.valid);
	if (!received.valid)
		return;
	UT_ASSERT_EQ(U64FromFullTransactionId(received.original_full_xid),
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 7001)));
	UT_ASSERT_EQ(received.participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(handle.participant->delegation_count, 1);
	UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(held_count, 0);
}

static bool
seed_delegation_receive(ClusterCtrcReceiptHandle *source,
	ClusterCtrcParticipantIdentity *receiver, ClusterItlSlotData *slot,
	uint8 record[CLUSTER_CTRC_DELEGATION_V1_BYTES], uint64 *index)
{
	ClusterCtrcItlTargetIdentity target;

	if (!seed_delegation_itl_source(source, receiver, &target, slot)
		|| !cluster_ctrc_itl_delegate_prepare_v1(&target, slot,
			&source->participant->identity, receiver, record, CLUSTER_CTRC_DELEGATION_V1_BYTES)
		|| !ctrc_participant_index(&source->key, receiver->node_id, index))
		return false;
	cluster_node_id = receiver->node_id;
	return true;
}

UT_TEST(test_image_receiver_retains_original_parent_before_any_local_receipt)
{
	ClusterCtrcReceiptHandle source;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	ClusterCtrcParticipantEntry *accepted, saved;
	uint8 record[224];
	uint64 index;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;

	UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
	UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record)));
	accepted = &ctrc_participant_entries()[index];
	UT_ASSERT_EQ(accepted->state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT_EQ(accepted->receipt_count, 0);
	UT_ASSERT(FullTransactionIdEquals(accepted->original_full_xid, source.original_full_xid));
	UT_ASSERT(memcmp(accepted->original_identity_v2, record + 8, 144) == 0);
	UT_ASSERT_EQ(accepted->delegation_parent.sequence, 1);
	UT_ASSERT(ctrc_participant_delegations_valid(accepted));
	UT_ASSERT(!ctrc_participant_delegations_empty(accepted));
	saved = *accepted;
	UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record)));
	UT_ASSERT(memcmp(accepted, &saved, sizeof(saved)) == 0);
	UT_ASSERT_EQ(source.participant->delegation_count, 1);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&source.key, &receiver,
		source.participant->grant_generation, 41, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
		CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(accepted->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(accepted->receipt_count, 0);
	UT_ASSERT_EQ(accepted->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_image_receiver_rejects_physical_or_authenticated_identity_mismatch)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterCtrcReceiptHandle source;
		ClusterCtrcParticipantIdentity receiver, sender;
		ClusterItlSlotData slot;
		uint8 record[224];
		uint64 index;
		Size length = sizeof(record);

		UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
		sender = source.participant->identity;
		switch (fault) {
		case 0: slot.xid++; break;
		case 1: slot.flags = ITL_FLAG_FREE; break;
		case 2: slot.flags = ITL_FLAG_LOCK_ONLY_ACTIVE; break;
		case 3: slot.undo_segment_head = uba_encode(source.key.segment_id + 1, 9,
			source.key.slot_offset, 8); break;
		case 4: slot.undo_segment_head = uba_encode(source.key.segment_id, 9,
			source.key.slot_offset + 1, 8); break;
		case 5: MemSet(&slot.undo_segment_head, 0, sizeof(slot.undo_segment_head)); break;
		case 6: sender.boot_incarnation++; break;
		case 7: receiver.capability_record_generation++; break;
		case 8: record[1]++; break;
		case 9: record[6] = 1; break;
		case 10: record[24] = record[25] = 0; break;
		case 11: length--; break;
		case 12: receiver.node_id = sender.node_id; break;
		case 13: cluster_node_id = sender.node_id; break;
		}
		UT_ASSERT(!cluster_ctrc_itl_delegate_accept_v1(&slot, &sender, &receiver, record, length));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[index],
			sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT_EQ(source.participant->delegation_count, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_image_receiver_does_not_qualify_old_rows_or_replace_parent)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterCtrcReceiptHandle source;
		ClusterCtrcParticipantIdentity receiver;
		ClusterItlSlotData slot;
		ClusterCtrcParticipantEntry *accepted, saved;
		uint8 record[224];
		uint64 index;

		UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
		accepted = &ctrc_participant_entries()[index];
		if (fault == 0) {
			UT_ASSERT_EQ(cluster_ctrc_participant_open(accepted, &source.key,
				source.participant->grant_generation, &receiver), CLUSTER_CTRC_PARTICIPANT_OPENED);
		} else {
			UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
				&receiver, record, sizeof(record)));
			switch (fault) {
			case 1: record[223]++; break;
			case 2: accepted->state = CTRC_PARTICIPANT_CLOSED_DRAINING; break;
			case 3: accepted->original_full_xid = FullTransactionIdFromEpochAndXid(8, slot.xid); break;
			case 4: accepted->identity.boot_incarnation++; break;
			}
		}
		saved = *accepted;
		UT_ASSERT(!cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record)));
		UT_ASSERT(memcmp(accepted, &saved, sizeof(saved)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_image_receiver_custody_composes_with_original_receipt_prepare)
{
	ClusterCtrcReceiptHandle source, received;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	uint8 record[224];
	uint64 index;

	UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
	UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record)));
	fixture_receipt_inputs(&ctrc_origin_entries()[0], 1003, &publication, &target);
	publication.requester_node_id = receiver.node_id;
	publication.requester_boot_incarnation = receiver.boot_incarnation;
	publication.capability_record_generation = receiver.capability_record_generation;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&source.key,
		&source.participant->identity, &receiver, source.participant->grant_generation,
		&publication, &target, record, sizeof(record), &received), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(received.valid);
	UT_ASSERT_EQ(received.participant, &ctrc_participant_entries()[index]);
	UT_ASSERT(FullTransactionIdEquals(received.original_full_xid, source.original_full_xid));
	UT_ASSERT_EQ(received.participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(received.participant->prepared_count, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_image_receiver_first_registration_revokes_prior_scan_cut)
{
	for (unsigned complete = 0; complete < 2; complete++) {
		ClusterCtrcReceiptHandle source;
		ClusterCtrcParticipantIdentity receiver;
		ClusterItlSlotData slot;
		ClusterCtrcActiveScan old, current;
		uint8 record[224];
		uint64 index, generation;

		UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
		UT_ASSERT(cluster_ctrc_active_scan_begin(&old,
			FullTransactionIdFromEpochAndXid(9, 10000)));
		while (old.next_index <= CtrcShared->origin_key_entries + index)
			cluster_ctrc_active_scan_step(&old);
		if (complete)
			finish_active_scan(&old);
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&old));
		generation = old.unqualified_registration_generation;
		UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record)));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation),
			generation + 1);
		UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&old));
		if (!complete) {
			finish_active_scan(&old);
			UT_ASSERT_EQ(old.status, CTRC_ACTIVE_SCAN_INVALID);
			UT_ASSERT(!FullTransactionIdIsValid(old.min_full));
		}
		UT_ASSERT(cluster_ctrc_active_scan_begin(&current,
			FullTransactionIdFromEpochAndXid(9, 10000)));
		UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record)));
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&current));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation),
			generation + 1);
		finish_active_scan(&current);
		UT_ASSERT_EQ(current.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT(FullTransactionIdEquals(current.min_full, source.original_full_xid));
		UT_ASSERT_EQ(held_count, 0);
	}
}


/* IMAGE operations have no ordinary backend ID. They must receive their
 * publication identity from the original shared journal, never from a PID,
 * caller-selected sequence or a guessed backend slot. */
static void
image_pending_target(ClusterCtrcTargetV1 *target)
{
	ClusterCtrcPublicationIdV1 unused;

	fixture_receipt_inputs(&ctrc_origin_entries()[0], 0, &unused, target);
	target->page_operation_kind = CLUSTER_CTRC_IMAGE_INSTALL_V1;
}

UT_TEST(test_image_publication_allocates_original_journal_identity_without_backend)
{
	ClusterCtrcReceiptHandle source, image;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	ClusterCtrcTargetV1 pending;
	uint8 record[224];
	uint64 index, next;

	UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
	image_pending_target(&pending);
	next = CtrcShared->global_journal_generation;
	UT_ASSERT_EQ(cluster_ctrc_itl_image_prepare_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record), &pending, &image), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(image.valid);
	if (!image.valid)
		return;
	UT_ASSERT_EQ(image.receipt->publication.reference_kind, CTRC_REF_IMAGE_ITL_V1);
	UT_ASSERT_EQ(image.receipt->publication.requester_backend_id, InvalidBackendId);
	UT_ASSERT_EQ(image.receipt->publication.operation_id, next);
	UT_ASSERT_EQ(image.receipt->publication.wire_request_id, next);
	UT_ASSERT_EQ(image.receipt->publication.journal_sequence, next);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, next + 1);
	UT_ASSERT_EQ(image.participant_index, index);
	UT_ASSERT_EQ(image.participant->delegation_parent.sequence, 1);
	UT_ASSERT(FullTransactionIdEquals(image.original_full_xid, source.original_full_xid));
	UT_ASSERT_EQ(image.participant->prepared_count, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_image_publication_uses_exact_original_apply_and_premutation_cancel)
{
	ClusterCtrcReceiptHandle source, image, cancelled;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	ClusterCtrcTargetV1 pending, exact, wrong;
	ClusterCtrcApplyToken token;
	uint8 record[224];
	uint64 index;

	UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
	image_pending_target(&pending);
	UT_ASSERT_EQ(cluster_ctrc_itl_image_prepare_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record), &pending, &image), CLUSTER_CTRC_PREPARE_READY);
	if (!image.valid)
		return;
	exact = pending;
	exact.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	exact.itl_xid = slot.xid;
	exact.itl_slot_wrap = slot.wrap;
	exact.itl_class = slot.flags;
	memcpy(exact.uba, &slot.undo_segment_head, sizeof(exact.uba));
	exact.planned_predecessor_sha256[0] = 1;
	UT_ASSERT(cluster_ctrc_sha256_exact(&slot, sizeof(slot), exact.planned_successor_sha256));
	wrong = exact;
	wrong.block_number++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_shared(&image, &wrong, &token),
		CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT_EQ(image.receipt->state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT(!token.valid);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_shared(&image, &exact, &token),
		CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT(!cluster_ctrc_receipt_cancel_shared(&image));
	UT_ASSERT_EQ(image.receipt->state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(cluster_ctrc_itl_image_prepare_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record), &pending, &cancelled), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(cancelled.valid);
	if (!cancelled.valid)
		return;
	UT_ASSERT(image.receipt->publication.operation_id != cancelled.receipt->publication.operation_id);
	UT_ASSERT(cluster_ctrc_receipt_cancel_shared(&cancelled));
	UT_ASSERT(ctrc_receipt_snapshot_valid(cancelled.participant, cancelled.receipt));
	UT_ASSERT_EQ(image.participant->prepared_count, 0);
	UT_ASSERT_EQ(image.participant->applied_count, 1);
	UT_ASSERT_EQ(image.participant->cancelled_count, 1);
	UT_ASSERT_EQ(image.participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(source.participant->delegation_count, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_image_publication_refuses_invalid_target_before_custody_or_sequence)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterCtrcReceiptHandle source, image;
		ClusterCtrcParticipantIdentity receiver;
		ClusterItlSlotData slot;
		ClusterCtrcTargetV1 pending;
		ClusterCtrcParticipantEntry saved;
		uint8 record[224];
		uint64 index, next;

		UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
		image_pending_target(&pending);
		switch (fault) {
		case 0: pending.kind = CTRC_TARGET_EXACT_ITL_SLOT; break;
		case 1: pending.page_operation_kind = 1; break;
		case 2: pending.publication_acquisition_epoch = 0; break;
		case 3: pending.publication_own_generation = 0; break;
		case 4: pending.relation_persistence = 't'; break;
		case 5: pending.needs_wal = false; break;
		case 6: pending.block_number = InvalidBlockNumber; break;
		case 7: pending.itl_xid = slot.xid; break;
		case 8: pending.fork_number = FSM_FORKNUM; break;
		}
		saved = ctrc_participant_entries()[index];
		next = CtrcShared->global_journal_generation;
		MemSet(&image, 0xa5, sizeof(image));
		UT_ASSERT_EQ(cluster_ctrc_itl_image_prepare_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record), &pending, &image), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&image, sizeof(image)));
		UT_ASSERT(memcmp(&saved, &ctrc_participant_entries()[index], sizeof(saved)) == 0);
		UT_ASSERT_EQ(CtrcShared->global_journal_generation, next);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_image_publication_keeps_parent_when_journal_is_exhausted)
{
	ClusterCtrcReceiptHandle source, image;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	ClusterCtrcTargetV1 pending;
	ClusterCtrcParticipantEntry saved;
	uint8 record[224];
	uint64 index;

	UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
	UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record)));
	saved = ctrc_participant_entries()[index];
	image_pending_target(&pending);
	CtrcShared->global_journal_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_ctrc_itl_image_prepare_v1(&slot, &source.participant->identity,
		&receiver, record, sizeof(record), &pending, &image), CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&image, sizeof(image)));
	UT_ASSERT(memcmp(&saved, &ctrc_participant_entries()[index], sizeof(saved)) == 0);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, UINT64_MAX);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_image_publication_cannot_be_forged_through_ordinary_publication_api)
{
	for (unsigned fake_backend = 0; fake_backend < 2; fake_backend++) {
		ClusterCtrcReceiptHandle source, image;
		ClusterCtrcParticipantIdentity receiver;
		ClusterItlSlotData slot;
		ClusterCtrcTargetV1 pending;
		ClusterCtrcPublicationIdV1 forged;
		ClusterCtrcParticipantEntry saved;
		uint8 record[224];
		uint64 index, next;

		UT_ASSERT(seed_delegation_receive(&source, &receiver, &slot, record, &index));
		UT_ASSERT(cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record)));
		saved = ctrc_participant_entries()[index];
		fixture_receipt_inputs(&ctrc_origin_entries()[0], 9999, &forged, &pending);
		image_pending_target(&pending);
		forged.requester_node_id = receiver.node_id;
		forged.requester_boot_incarnation = receiver.boot_incarnation;
		forged.capability_record_generation = receiver.capability_record_generation;
		forged.requester_backend_id = fake_backend ? 11 : InvalidBackendId;
		forged.wire_request_id = forged.operation_id;
		forged.reference_kind = CTRC_REF_IMAGE_ITL_V1;
		next = CtrcShared->global_journal_generation;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_delegation_v1(&source.key,
			&source.participant->identity, &receiver, source.participant->grant_generation,
			&forged, &pending, record, sizeof(record), &image), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!image.valid);
		UT_ASSERT(memcmp(&saved, &ctrc_participant_entries()[index], sizeof(saved)) == 0);
		UT_ASSERT_EQ(CtrcShared->global_journal_generation, next);
		UT_ASSERT_EQ(held_count, 0);
	}
}

/* Actual GCS receipt preparation/apply/cancel uses the complete shared engine
 * above. Only original HELLO/admission sampling and physical copy scheduling
 * remain controlled here; these cases do not run native transport or PCM. */
static ClusterCtrcParticipantIdentity install_test_sender, install_test_receiver;
static bool install_test_peer_current;
static unsigned install_test_fail_closed;
static uint64
install_test_self_boot(void)
{
	return install_test_receiver.boot_incarnation;
}
static bool
gcs_block_pcm_x_resource_x_peer_ready_exact(int32 source, uint32 *connection)
{
	*connection = 11;
	return source == install_test_sender.node_id && install_test_peer_current;
}
static bool
gcs_block_resource_x_ctrc_source_identities(const ClusterSemanticAdmissionToken *admission,
	int32 source, uint32 connection, uint64 boot, ClusterCtrcParticipantIdentity *receiver,
	ClusterCtrcParticipantIdentity *sender)
{
	*sender = install_test_sender;
	*receiver = install_test_receiver;
	return install_test_peer_current && admission != NULL && admission->entered
		&& source == sender->node_id && connection == 11 && boot == receiver->boot_incarnation;
}
#define cluster_qvotec_get_self_incarnation install_test_self_boot
#include "test_cluster_gcs_image_receive.inc"
#define gcs_block_resource_x_fail_closed_current() (install_test_fail_closed++)
#include "test_cluster_gcs_image_receipts.inc"
#undef gcs_block_resource_x_fail_closed_current
#undef cluster_qvotec_get_self_incarnation

static bool
install_test_setup(ResourceXDecodedFrame *frame, ResourceXAcquisitionRef *ref,
	ClusterPcmOwnSnapshot *base, ClusterSemanticAdmissionToken *admission,
	ClusterCtrcReceiptHandle *source)
{
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	uint8 record[224];
	uint64 index;
	Page page;

	if (!seed_delegation_receive(source, &receiver, &slot, record, &index))
		return false;
	install_test_sender = source->participant->identity;
	install_test_receiver = receiver;
	install_test_peer_current = true;
	install_test_fail_closed = 0;
	memset(frame, 0, sizeof(*frame));
	memset(ref, 0, sizeof(*ref));
	memset(base, 0, sizeof(*base));
	memset(admission, 0, sizeof(*admission));
	admission->entered = true;
	base->tag.spcOid = 1663;
	base->tag.dbOid = 5;
	base->tag.relNumber = 9001;
	base->tag.forkNum = MAIN_FORKNUM;
	base->tag.blockNum = 44;
	base->pcm_state = PCM_STATE_N;
	base->generation = 16;
	ref->assertion.resource = base->tag;
	ref->assertion.requester_node = receiver.node_id;
	ref->formation = 7;
	ref->acquisition_generation = 19;
	frame->kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	frame->common.logical_assertion = ref->assertion;
	frame->common.resource_formation = ref->formation;
	frame->common.assertion_sequence = ref->acquisition_generation;
	frame->common.sender_connection_generation = 12345;
	frame->body.image_envelope.image_length = BLCKSZ;
	gcs_block_resource_x_put_u32(frame->body.image_envelope.source_fence, install_test_sender.node_id);
	gcs_block_resource_x_put_u64(frame->body.image_envelope.source_fence + 4,
		install_test_sender.boot_incarnation);
	page = (Page)frame->body.image_envelope.page_bytes;
	((PageHeader)page)->pd_flags = PD_HAS_ITL;
	((PageHeader)page)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)page)->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	((PageHeader)page)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	PageSetLSNPreserveOrigin(page, 100);
	PageSetLSNOrigin(page, 1);
	((PageHeader)page)->pd_block_scn = 80;
	ClusterPageGetItlSlots(page)[0] = slot;
	frame->body.image_envelope.ctrc_delegation_bytes = 8 + 224;
	frame->body.image_envelope.ctrc_delegation[1] = RESOURCE_X_CTRC_BUNDLE_VERSION;
	frame->body.image_envelope.ctrc_delegation[3] = 8 + 224;
	frame->body.image_envelope.ctrc_delegation[5] = 1;
	memcpy(frame->body.image_envelope.ctrc_delegation + 8, record, sizeof(record));
	return true;
}

UT_TEST(test_actual_gcs_install_receipt_binds_original_fullxid_and_cancels_only_before_copy)
{
	for (unsigned applied = 0; applied < 2; applied++) {
		ResourceXDecodedFrame frame;
		ResourceXAcquisitionRef ref;
		ClusterPcmOwnSnapshot base;
		ClusterSemanticAdmissionToken admission;
		ClusterCtrcReceiptHandle source;
		GcsBlockCtrcImageInstall install = { 0 };
		PGAlignedBlock resident;
		ClusterCtrcReceiptHandle *image = &install.receipts[0];

		UT_ASSERT(install_test_setup(&frame, &ref, &base, &admission, &source));
		UT_ASSERT(gcs_block_resource_x_ctrc_image_prepare(&install, &frame, &admission, &ref, &base, 9));
		UT_ASSERT(install.ready && image->valid);
		if (!image->valid)
			return;
		UT_ASSERT_EQ(image->receipt->state, CTRC_RECEIPT_PREPARED);
		UT_ASSERT(FullTransactionIdEquals(source.original_full_xid, image->original_full_xid));
		UT_ASSERT_EQ(image->participant->delegation_parent.sequence, 1);
		UT_ASSERT_EQ(source.participant->delegation_count, 1);
		if (applied) {
			install.copy_started = true; /* Same edge as original physical-copy owner. */
			memcpy(resident.data, frame.body.image_envelope.page_bytes, BLCKSZ);
			UT_ASSERT(gcs_block_resource_x_ctrc_image_apply(&install, resident.data));
			UT_ASSERT_EQ(image->receipt->state, CTRC_RECEIPT_APPLIED);
			UT_ASSERT_EQ(image->receipt->target.itl_slot_index, 0);
			UT_ASSERT_EQ(image->receipt->target.publication_own_generation, 17);
		}
		gcs_block_resource_x_ctrc_image_cancel(&install);
		UT_ASSERT_EQ(image->receipt->state, applied ? CTRC_RECEIPT_APPLIED : CTRC_RECEIPT_CANCELLED);
		UT_ASSERT_EQ(image->participant->delegation_parent.sequence, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_actual_gcs_install_refused_after_copy_keeps_prepared_receipt_and_parent)
{
	ResourceXDecodedFrame frame;
	ResourceXAcquisitionRef ref;
	ClusterPcmOwnSnapshot base;
	ClusterSemanticAdmissionToken admission;
	ClusterCtrcReceiptHandle source;
	GcsBlockCtrcImageInstall install = { 0 };
	PGAlignedBlock resident;
	ClusterCtrcReceiptHandle *image = &install.receipts[0];

	UT_ASSERT(install_test_setup(&frame, &ref, &base, &admission, &source));
	UT_ASSERT(gcs_block_resource_x_ctrc_image_prepare(&install, &frame, &admission, &ref, &base, 9));
	if (!image->valid)
		return;
	install.copy_started = true;
	memcpy(resident.data, frame.body.image_envelope.page_bytes, BLCKSZ);
	ClusterPageGetItlSlots(resident.data)[0].wrap++;
	UT_ASSERT(!gcs_block_resource_x_ctrc_image_apply(&install, resident.data));
	gcs_block_resource_x_ctrc_image_cancel(&install);
	UT_ASSERT_EQ(image->receipt->state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT_EQ(image->participant->prepared_count, 1);
	UT_ASSERT_EQ(image->participant->delegation_parent.sequence, 1);
	UT_ASSERT_EQ(source.participant->delegation_count, 1);
	UT_ASSERT_EQ(held_count, 0);
}

int
main(void)
{
	ClusterCtrcCapacity capacity;

	if (!cluster_ctrc_capacity_compute(NBuffers, MaxBackends, 4, &capacity))
		abort();
	UT_PLAN(181);
	UT_RUN(test_prepare_path_namespace_precedes_input_and_resets_output);
	UT_RUN(test_prepare_path_input_precedes_journal_exhaustion);
	UT_RUN(test_prepare_path_chain_precedes_locator_and_keeps_blocked_state);
	UT_RUN(test_prepare_path_object_existing_keeps_refusal_race_and_duplicate);
	UT_RUN(test_prepare_path_table_ready_duplicate_and_refusals);
	UT_RUN(test_table_path_input_short_circuits_participant_and_publication);
	UT_RUN(test_table_path_participant_refusal_precedes_publication);
	UT_RUN(test_table_path_publication_refusal_keeps_original_participant_open);
	UT_RUN(test_table_path_dirty_or_malformed_probe_keeps_bytes_and_blocks);
	UT_RUN(test_table_path_duplicate_conflict_does_not_publish_again);
	UT_RUN(test_table_path_sequence_capacity_and_tombstone_probe_order);
	UT_RUN(test_table_path_null_observer_preserves_original_table_effects);
	UT_RUN(test_prepare_path_null_observer_preserves_exact_shared_effects);

	printf("# CTRC header_bytes=%zu total_bytes=%zu\n", sizeof(ClusterCtrcSharedHeader),
		   capacity.total_bytes);
	UT_RUN(test_origin_reserve_requires_published_local_top_protection);
	UT_RUN(test_origin_reserve_requires_same_published_original_fullxid);
	UT_RUN(test_origin_reserve_protected_pending_can_cancel);
	UT_RUN(test_origin_reserve_open_replay_requires_live_original_protection);
	UT_RUN(test_origin_open_replay_refuses_missing_or_conflicting_original_fullxid);
	UT_RUN(test_origin_open_reserved_refuses_original_identity_drift);
	UT_RUN(test_origin_open_reserved_refuses_invalid_handle_identity);
	UT_RUN(test_origin_reserved_cleanup_refuses_original_identity_drift);
	UT_RUN(test_origin_open_reserved_keeps_protected_original_identity);
	UT_RUN(test_new_seals_do_not_starve_old_pending);
	UT_RUN(test_pending_participant_does_not_starve_peer);
	UT_RUN(test_duplicate_pending_is_accepted_without_wakeup);
	UT_RUN(test_uncorrelated_reply_cannot_progress);
	UT_RUN(test_batch_is_bounded_oldest_first_and_fair);
	UT_RUN(test_batch_hint_rechecks_current_identity);
	UT_RUN(test_scan_budget_prevents_same_pending_item_revisit);
	UT_RUN(test_observed_age_resets_without_releasing_authority);
	UT_RUN(test_participant_duplicate_pending_does_not_wake);
	UT_RUN(test_real_ack_and_certificate_consumers_are_idempotent);
	UT_RUN(test_receipt_and_ack_scans_do_not_retry_retained_entries);
	UT_RUN(test_certificate_scan_bounds_unpublished_candidates);
	UT_RUN(test_shared_prepare_journal_uses_sleepable_exclusion);
	UT_RUN(test_shared_prepare_isolates_participant_shards);
	UT_RUN(test_ack_copy_is_indexed_and_pool_read_only);
	UT_RUN(test_shared_pool_capacity_is_not_a_per_shard_quota);
	UT_RUN(test_prepare_does_not_overwrite_a_corrupt_empty_locator);
	UT_RUN(test_nonempty_ack_copy_and_freeze_use_sleepable_exclusion);
	UT_RUN(test_stale_ack_snapshot_releases_without_publication);
	UT_RUN(test_receipt_drift_retains_bytes_and_blocks_ack);
	UT_RUN(test_sleepable_header_requires_exact_layout_and_tranches);
	UT_RUN(test_reclaim_batch_validates_table_once_for_eight_keys);
	UT_RUN(test_reclaim_batch_bad_key_retains_its_whole_set_only);
	UT_RUN(test_reclaim_batch_all_empty_bytes_guard_before_any_delete);
	UT_RUN(test_reclaim_batch_duplicate_and_missing_member_retain_exactly);
	UT_RUN(test_zero_bytes_exact_at_every_offset_and_protected_end);
	UT_RUN(test_reclaim_empty_range_preserves_failed_and_collapses_success);
	UT_RUN(test_shared_certificate_batch_uses_one_real_validation);
	UT_RUN(test_shared_certificate_batch_identity_and_receipt_faults_are_isolated);
	UT_RUN(test_shared_certificate_global_corruption_and_alias_never_partially_clear);
	UT_RUN(test_shared_reclaim_validates_read_only_and_clears_locators);
	UT_RUN(test_shared_whole_pool_reclaim_and_old_handle_replay);
	UT_RUN(test_shared_reclaim_rejects_incomplete_cyclic_and_stale_locators);
	UT_RUN(test_reclaim_upgrade_preserves_other_key_prepare_and_reclaim);
	UT_RUN(test_reclaim_error_before_write_keeps_exact_set);
	UT_RUN(test_ack_batch_keeps_independent_copies_and_complete_final_census);
	UT_RUN(test_real_ack_driver_shares_scans_and_keeps_durability_and_barrier);
	UT_RUN(test_real_ack_driver_independent_capture_and_final_drift_retain_one_key);
	UT_RUN(test_real_ack_driver_exception_and_allocation_failure_never_publish);
	UT_RUN(test_ack_batch_final_count_summary_and_identity_faults_are_isolated);
	UT_RUN(test_cleaner_dispatch_has_one_canonical_segment_owner);
	UT_RUN(test_ack_scan_does_not_advance_foreign_shard);
	UT_RUN(test_worker_binding_and_bad_keys_never_change_ownership);
	UT_RUN(test_open_and_certificate_scans_preserve_foreign_bytes);
	UT_RUN(test_receipt_and_ack_select_only_canonical_worker);
	UT_RUN(test_foreign_progress_and_observations_cannot_drive_local_work);
	UT_RUN(test_actual_driver_ignores_other_progress_and_clears_error_batch);
	UT_RUN(test_normal_stop_uninitialized_and_empty_observation);
	UT_RUN(test_normal_stop_reserved_origin_is_not_empty);
	UT_RUN(test_normal_stop_origin_phases_and_invalid_override_pending);
	UT_RUN(test_normal_stop_live_receipts_and_corrupt_links_are_never_ready);
	UT_RUN(test_normal_stop_requires_actual_certificate_notification_reclaim);
	UT_RUN(test_normal_stop_cleaner_idle_is_local_ownership_not_progress);
	UT_RUN(test_normal_stop_new_close_tombstone_is_not_retained_completion);
	UT_RUN(test_normal_stop_close_validation_and_local_caller_are_not_service_admission);
	UT_RUN(test_original_cleaner_scope_binds_one_receipt_and_its_space_read);
	UT_RUN(test_cleaner_scope_rejects_wrong_owner_and_receipt_drift);
	UT_RUN(test_cleaner_scope_current_is_local_and_expires_with_original_attempt);
	UT_RUN(test_original_cleaner_driver_expires_read_scope_on_return_error_and_refusal);
	UT_RUN(test_concurrent_original_discharge_does_not_end_running_cleaner_attempt);
	UT_RUN(test_local_receipt_refuses_missing_or_aliased_origin_protection);
	UT_RUN(test_local_origin_protection_spans_receipt_and_handle_publication);
	UT_RUN(test_local_receipt_rechecks_origin_after_journal_allocation);
	UT_RUN(test_local_receipt_keeps_inflight_grant_until_participant_close);
	UT_RUN(test_remote_origin_receipt_does_not_require_local_origin_row);
	UT_RUN(test_object_fence_refuses_new_receipt_without_publishing_participant);
	UT_RUN(test_object_fence_retains_exact_duplicate_and_original_cancel);
	UT_RUN(test_object_receipt_gate_does_not_change_private_or_other_tier);
	UT_RUN(test_active_scan_covers_fixed_arrays_in_bounded_slices);
	UT_RUN(test_active_scan_keeps_reserved_and_all_unretired_origin_states);
	UT_RUN(test_active_scan_keeps_participants_outside_worker_zero_shard);
	UT_RUN(test_active_scan_busy_lock_retains_exact_position_and_minimum);
	UT_RUN(test_active_scan_corrupt_empty_entry_invalidates_sample);
	UT_RUN(test_active_scan_fresh_begin_discards_previous_partial_sample);
	UT_RUN(test_active_scan_uses_original_origin_identity_after_pgproc_exit);
	UT_RUN(test_local_receipt_does_not_backfill_older_unknown_receipts);
	UT_RUN(test_local_receipt_hands_original_identity_to_retained_participant);
	UT_RUN(test_receipt_known_handle_identity_conflict_cannot_cancel);
	UT_RUN(test_receipt_known_handle_identity_conflict_cannot_apply);
	UT_RUN(test_receipt_unknown_handle_remains_unknown_after_later_exact_local_source);
	UT_RUN(test_local_receipt_refuses_conflicting_retained_original_identity);
	UT_RUN(test_receipt_original_identity_refusal_precedes_chain_blocking);
	UT_RUN(test_receipt_observed_identity_refusal_precedes_chain_blocking);
	UT_RUN(test_receipt_never_guesses_missing_or_foreign_original_identity);
	UT_RUN(test_active_scan_refuses_malformed_known_participant_identity);
	UT_RUN(test_active_scan_refuses_unknown_or_mismatched_origin_fullxid);
	UT_RUN(test_active_scan_participant_widens_old_epoch_and_rejects_unrepresentable_xids);
	UT_RUN(test_active_scan_counts_raw_only_identity_coverage);
	UT_RUN(test_original_cancelled_producer_retains_fullxid_until_final_certificate_reply);
	UT_RUN(test_original_applied_producer_retains_fullxid_until_final_certificate_reply);
	UT_RUN(test_active_scan_source_matches_original_not_reference_epoch);
	UT_RUN(test_active_scan_source_mismatch_retains_original_minimum);
	UT_RUN(test_active_scan_unknown_and_foreign_source_count_once);
	UT_RUN(test_active_scan_foreign_origin_uses_original_cohort_member);
	UT_RUN(test_active_scan_missing_source_refuses_before_shared_locks);
	UT_RUN(test_late_unknown_receipt_invalidates_scanned_identity_coverage);
	UT_RUN(test_late_unknown_close_tombstone_invalidates_scanned_identity_coverage);
	UT_RUN(test_exact_local_receipt_handoff_keeps_original_scan_progress);
	UT_RUN(test_existing_unknown_receipt_duplicate_does_not_restart_scan);
	UT_RUN(test_complete_scan_registration_cut_revoked_and_recaptured);
	UT_RUN(test_unknown_registration_sequence_saturates_without_changing_prepare);
	UT_RUN(test_unknown_registration_zero_and_absent_source_never_current);
	UT_RUN(test_last_slice_registration_cannot_publish_complete_old_cut);
	UT_RUN(test_native_source_exit_keeps_cancelled_receipt_and_certificate_retention);
	UT_RUN(test_native_source_exit_keeps_applied_receipt_and_certificate_retention);
	UT_RUN(test_native_group_exit_keeps_cancelled_receipt_and_certificate_retention);
	UT_RUN(test_native_group_exit_keeps_applied_receipt_and_certificate_retention);
	UT_RUN(test_original_worker_publication_keeps_cancelled_native_source_retention);
	UT_RUN(test_original_worker_publication_keeps_applied_group_source_retention);
	UT_RUN(test_multi_member_group_cancel_keeps_original_retention_through_wakeup);
	UT_RUN(test_multi_member_group_applied_keeps_original_retention_through_wakeup);
	UT_RUN(test_prepared_cancel_keeps_original_shared_retention_after_native_removal);
	UT_RUN(test_prepared_applied_keeps_original_shared_retention_after_native_removal);
	UT_RUN(test_reused_native_slot_cancel_retains_old_fullxid_after_same_raw_publication);
	UT_RUN(test_reused_native_slot_applied_retains_old_fullxid_after_same_raw_publication);
	UT_RUN(test_same_pid_worker_respawn_cancel_restarts_scan_with_shared_retention);
	UT_RUN(test_same_pid_worker_respawn_applied_restarts_scan_with_shared_retention);
	UT_RUN(test_actual_worker_active_pass_keeps_old_shared_fullxid_across_slices);
	UT_RUN(test_actual_worker_active_pass_restarts_after_original_lease_refusal);
	UT_RUN(test_actual_worker_active_pass_restarts_after_original_source_refusal);
	UT_RUN(test_actual_worker_active_pass_does_not_sample_native_without_source);
	UT_RUN(test_origin_fullxid_v2_foreign_receiver_retains_source);
	UT_RUN(test_origin_grant_v2_registers_and_captures_in_one_original_lock);
	UT_RUN(test_origin_grant_v2_refuses_unknown_identity_before_registration);
	UT_RUN(test_receiver_retains_original_v2_bytes_for_delegation);
	UT_RUN(test_local_first_registration_captures_protected_original_v2);
	UT_RUN(test_v2_registration_never_backfills_missing_original_bytes);
	UT_RUN(test_origin_fullxid_v2_local_producer_retains_source);
	UT_RUN(test_origin_fullxid_v2_local_producer_rechecks_locked_source);
	UT_RUN(test_origin_fullxid_v2_rejects_missing_or_mismatched_identity);
	UT_RUN(test_origin_fullxid_v2_refuses_raw_only_receiver_backfill);
	UT_RUN(test_delegation_custody_precedes_immutable_record_and_retry);
	UT_RUN(test_delegation_custody_refusal_preserves_row_and_clears_output);
	UT_RUN(test_delegation_custody_destination_restart_does_not_close_old_obligation);
	UT_RUN(test_delegation_custody_prevents_drained_close_and_ack);
	UT_RUN(test_delegation_custody_blocks_frozen_certificate_reclamation);
	UT_RUN(test_delegation_receiver_registers_original_and_parent_before_handle);
	UT_RUN(test_delegation_receiver_rejects_unbound_or_malformed_carrier_without_owner_change);
	UT_RUN(test_delegation_receiver_never_backfills_existing_raw_only_registration);
	UT_RUN(test_delegation_receiver_parent_or_sequence_cannot_be_replaced);
	UT_RUN(test_delegation_receiver_local_drain_cannot_retire_parent);
	UT_RUN(test_itl_delegation_source_binds_applied_slot_before_outgoing_custody);
	UT_RUN(test_itl_delegation_source_rejects_every_changed_slot_byte);
	UT_RUN(test_itl_delegation_source_requires_exact_page_and_itl_target);
	UT_RUN(test_itl_delegation_source_rejects_nonapplied_or_corrupt_owner);
	UT_RUN(test_itl_delegation_source_exact_retry_reuses_original_obligation);
	UT_RUN(test_itl_delegation_source_record_reaches_original_receiver_engine);
	UT_RUN(test_image_receiver_retains_original_parent_before_any_local_receipt);
	UT_RUN(test_image_receiver_rejects_physical_or_authenticated_identity_mismatch);
	UT_RUN(test_image_receiver_does_not_qualify_old_rows_or_replace_parent);
	UT_RUN(test_image_receiver_custody_composes_with_original_receipt_prepare);
	UT_RUN(test_image_receiver_first_registration_revokes_prior_scan_cut);
	UT_RUN(test_image_publication_allocates_original_journal_identity_without_backend);
	UT_RUN(test_image_publication_uses_exact_original_apply_and_premutation_cancel);
	UT_RUN(test_image_publication_refuses_invalid_target_before_custody_or_sequence);
	UT_RUN(test_image_publication_keeps_parent_when_journal_is_exhausted);
	UT_RUN(test_image_publication_cannot_be_forged_through_ordinary_publication_api);
	UT_RUN(test_actual_gcs_install_receipt_binds_original_fullxid_and_cancels_only_before_copy);
	UT_RUN(test_actual_gcs_install_refused_after_copy_keeps_prepared_receipt_and_parent);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
