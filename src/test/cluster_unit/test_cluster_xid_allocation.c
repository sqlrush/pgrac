/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_allocation.c
 *    Exercise the actual native allocator across stripe reservation waits.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_allocation.c
 *
 * NOTES
 *    PGRAC-original tests. Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_qvotec.h"
#include "storage/condition_variable.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "test_cluster_pgproc_active_types.inc"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

/* This allocator fixture never reads terminal CLOG or enters ERROR cleanup.
 * Keep such calls fatal; the integrated producer adds only native link leaves. */
volatile uint32 InterruptHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	abort();
}
XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	(void)xid;
	(void)lsn;
	abort();
}

bool
cluster_clog_native_terminal_sync_v1(TransactionId xid, ClusterClogNativeTerminalV1 *out)
{
	(void)xid;
	(void)out;
	abort();
}

bool cluster_enabled, cluster_shared_config, cluster_shared_catalog, cluster_xid_striping;
int cluster_node_id = 4, cluster_xid_herding_slack = 4194304;
bool IsUnderPostmaster;
bool IsPostmasterEnvironment;
AuxProcType MyAuxProcType = NotAnAuxProcess;
ProcessingMode Mode = NormalProcessing;
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static PGPROC entries[2];
PGPROC *MyProc = &entries[0];
static PROC_HDR procs;
PROC_HDR *ProcGlobal = &procs;
static TransactionId visible_xids[2];
static XidCacheStatus subxids[2];
static uint8 status_flags[2];
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool held, reservation, jump, advance_while_unlocked;
static unsigned waits, extensions, early_extensions, lock_errors;
static bool sampling, lifecycle;
static LWLock *held_lock;
static LWLockMode held_mode;
static unsigned sample_lock_phase;
static char array_storage[offsetof(ProcArrayStruct, pgprocnos) + 2 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static ProcArrayStruct *procArray = (ProcArrayStruct *) array_storage;
static uint64 floor_full, limit_full;
static unsigned native_publish_count, native_publish_order_errors;
int cluster_injection_armed_count;
BackendId MyBackendId;
static GlobalTransaction MyLockedGxact;
static TransactionId completed_xid;
static sigjmp_buf allocation_error_env;
static bool allocation_error_expected;
static int allocation_error_code;
static unsigned allocation_errors;
static bool real_stripe, stripe_lock_held, stripe_quorum;
static LWLock *stripe_lock;
static uint64 stripe_incarnation;
static FullTransactionId real_reserved_candidate;

static bool allocation_real_future(FullTransactionId, FullTransactionId *, ClusterXidStripeSampleFence *);
static bool allocation_real_fence_current(const ClusterXidStripeSampleFence *);
static bool allocation_real_lease_ready(FullTransactionId);
static bool allocation_real_window_exceeded(FullTransactionId);
static bool allocation_real_wrap_passed(void);
static FullTransactionId allocation_real_slot_floor(void);
static FullTransactionId allocation_real_herding_floor(void);

/* Aliases only isolate these actual bodies from the original cases' leaves. */
#define cluster_xid_stripe_sample_future allocation_real_future
#define cluster_xid_stripe_sample_fence_current allocation_real_fence_current
#define cluster_xid_stripe_lease_ready allocation_real_lease_ready
#define cluster_xid_stripe_window_exceeded allocation_real_window_exceeded
#define cluster_xid_wrap_barrier_passed allocation_real_wrap_passed
#define cluster_xid_stripe_my_slot_floor allocation_real_slot_floor
#define cluster_xid_stripe_herding_floor allocation_real_herding_floor
#include "test_cluster_xid_allocation_stripe.inc"
#undef cluster_xid_stripe_sample_future
#undef cluster_xid_stripe_sample_fence_current
#undef cluster_xid_stripe_lease_ready
#undef cluster_xid_stripe_window_exceeded
#undef cluster_xid_wrap_barrier_passed
#undef cluster_xid_stripe_my_slot_floor
#undef cluster_xid_stripe_herding_floor
static ClusterXidStripeBootShmem allocation_boot;

void
cluster_injection_run(const char *name)
{
	/* No injection is armed; an unexpected dispatch must fail this fixture. */
	abort();
}

bool
cluster_qvotec_in_quorum(void)
{
	return stripe_quorum;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return stripe_incarnation;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return (held && held_lock == lock) || (stripe_lock_held && stripe_lock == lock);
}

static void
MaintainLatestCompletedXid(TransactionId xid)
{
	completed_xid = xid;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return held && held_lock == lock && held_mode == mode;
}

bool
IsInParallelMode(void)
{
	return false;
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (real_stripe && lock == stripe_lock) {
		if (stripe_lock_held || mode != LW_SHARED || (held && held_lock != XidGenLock))
			lock_errors++;
		stripe_lock_held = true;
		return false;
	}
	if (real_stripe && lock == XidGenLock && mode == LW_EXCLUSIVE)
		real_reserved_candidate = InvalidFullTransactionId;
	if (held || (lifecycle && (mode != LW_EXCLUSIVE
			|| (lock != ProcArrayLock && lock != TwoPhaseStateLock)))
		|| (!lifecycle && !sampling && (lock != XidGenLock || mode != LW_EXCLUSIVE))
		|| (sampling && (mode != LW_SHARED
			|| (sample_lock_phase == 0 ? lock != XidGenLock
				: sample_lock_phase != 2 || lock != ProcArrayLock))))
		lock_errors++;
	held = true;
	held_lock = lock;
	held_mode = mode;
	if (sampling)
		sample_lock_phase++;
	return false;
}
void
LWLockRelease(LWLock *lock)
{
	FullTransactionId published;

	if (real_stripe && lock == stripe_lock) {
		if (!stripe_lock_held)
			lock_errors++;
		stripe_lock_held = false;
		return;
	}
	published = FullTransactionIdFromU64(
		pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic));

	if (!held || lock != held_lock)
		lock_errors++;
	/* Observe the publication itself, not an epoch guessed from the already
	 * advanced nextXid. Tests below compare the exact allocator return too. */
	if (TransactionIdIsNormal(entries[0].xid) &&
		(!FullTransactionIdIsNormal(published) || XidFromFullTransactionId(published) != entries[0].xid))
		lock_errors++;
	if (sampling)
		sample_lock_phase++;
	held = false;
	held_lock = NULL;
}
bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(sampling && held && held_lock == XidGenLock);
	if (real_stripe)
		return allocation_real_future(reference, future, fence);
	*future = reference;
	MemSet(fence, 0, sizeof(*fence));
	return true;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	return variables.nextXid;
}
void
cluster_xid_stripe_lazy_latch(void)
{}
FullTransactionId
cluster_xid_stripe_my_slot_floor(void)
{
	if (real_stripe)
		return allocation_real_slot_floor();
	return FullTransactionIdFromU64(1024);
}
FullTransactionId
cluster_xid_stripe_herding_floor(void)
{
	if (real_stripe)
		return allocation_real_herding_floor();
	return FullTransactionIdFromU64(floor_full);
}
bool
cluster_xid_stripe_window_exceeded(FullTransactionId candidate)
{
	if (real_stripe)
		return allocation_real_window_exceeded(candidate);
	return false;
}
bool
cluster_xid_wrap_barrier_passed(void)
{
	if (real_stripe)
		return allocation_real_wrap_passed();
	return true;
}
bool
cluster_xid_stripe_lease_ready(FullTransactionId candidate)
{
	if (real_stripe) {
		bool ready = allocation_real_lease_ready(candidate);

		if (ready && held && held_lock == XidGenLock && held_mode == LW_EXCLUSIVE)
			real_reserved_candidate = candidate;
		return ready;
	}
	return !cluster_shared_catalog
		   || (reservation && U64FromFullTransactionId(candidate) >= floor_full
			   && U64FromFullTransactionId(candidate) < limit_full);
}
void
cluster_xid_stripe_wait_lease(FullTransactionId candidate)
{
	waits++;
	/* A refusal test must never reach this boundary. Stop a parent's mock
	 * wait with a distinct marker, not a synthetic product ERROR/success. */
	if (allocation_error_expected)
		siglongjmp(allocation_error_env, 2);
	if (held)
		lock_errors++;
	reservation = true;
	floor_full = jump ? 4100 : 2052;
	limit_full = 8196;
	if (advance_while_unlocked)
		variables.nextXid = FullTransactionIdFromU64(2084);
}
static void
extended(void)
{
	extensions++;
	if (cluster_shared_catalog && (real_stripe
		? !FullTransactionIdIsValid(real_reserved_candidate)
			|| !allocation_real_lease_ready(real_reserved_candidate)
		: !reservation))
		early_extensions++;
	if (!held)
		lock_errors++;
}
void
ExtendCLOG(TransactionId xid)
{
	extended();
}
void
ExtendCommitTs(TransactionId xid)
{
	extended();
}
void
ExtendSUBTRANS(TransactionId xid)
{
	extended();
}
void
SendPostmasterSignal(PMSignalReason reason)
{}
char *
get_database_name(Oid databaseid)
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	if (!allocation_error_expected || level != ERROR)
		abort();
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int sqlerrcode)
{
	allocation_error_code = sqlerrcode;
	return 0;
}
int errmsg(const char *fmt, ...) { return 0; }
int errdetail(const char *fmt, ...) { return 0; }
int errhint(const char *fmt, ...) { return 0; }
int errmsg_plural(const char *singular, const char *plural, unsigned long n, ...) { return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	if (!allocation_error_expected)
		abort();
	allocation_errors++;
	siglongjmp(allocation_error_env, 1);
}

bool
cluster_relmap_owner_terminal_matches_v2(const ClusterRelmapOwnerSampleV2 *owner)
{
	(void)owner;
	return false;
} /* No original-owner authority in this fixture. */
#define LWLockHeldByMe c_allocator_relmap_LWLockHeldByMe
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == XidGenLock && held;
}
#include "../../backend/cluster/cluster_relmap_recovery.c"
#undef LWLockHeldByMe

/* The real allocator calls through this test-only observation, which checks
 * original ProcArray publication before delegating to the product tracker. */
static void
native_publication_checked(FullTransactionId issued)
{
	TransactionId xid = XidFromFullTransactionId(issued);
	bool top = MyProc->xid == xid && visible_xids[0] == xid;
	bool child = MyProc->subxidStatus.count > 0
				 && MyProc->subxids.xids[MyProc->subxidStatus.count - 1] == xid
				 && subxids[0].count == MyProc->subxidStatus.count;
	if (!held || (!top && !child && !MyProc->subxidStatus.overflowed))
		native_publish_order_errors++;
	native_publish_count++;
	cluster_relmap_recovery_issued_publish(issued);
}
#define cluster_relmap_recovery_issued_publish native_publication_checked
#ifndef CLUSTER_XID_ALLOCATOR_BODY
#define CLUSTER_XID_ALLOCATOR_BODY "test_cluster_xid_allocation.inc"
#endif
#include CLUSTER_XID_ALLOCATOR_BODY
#undef cluster_relmap_recovery_issued_publish

static void
reset_allocator(void)
{
	real_stripe = stripe_lock_held = false;
	real_reserved_candidate = InvalidFullTransactionId;
	stripe_lock = NULL;
	memset(&variables, 0, sizeof(variables));
	memset(entries, 0, sizeof(entries));
	memset(&procs, 0, sizeof(procs));
	memset(subxids, 0, sizeof(subxids));
	memset(visible_xids, 0, sizeof(visible_xids));
	memset(status_flags, 0, sizeof(status_flags));
	memset(array_storage, 0, sizeof(array_storage));
	procArray->numProcs = procArray->maxProcs = 1;
	procArray->pgprocnos[0] = 0;
	variables.nextXid = FullTransactionIdFromU64(2052);
	variables.xidVacLimit = UINT32_MAX / 4;
	procs.xids = visible_xids;
	procs.allProcs = &entries[0];
	procs.subxidStates = subxids;
	procs.statusFlags = status_flags;
	held = reservation = jump = advance_while_unlocked = false;
	cluster_enabled = cluster_shared_config = cluster_shared_catalog = cluster_xid_striping = true;
	cluster_node_id = 4;
	waits = extensions = early_extensions = lock_errors = 0;
	floor_full = limit_full = 0;
	native_publish_count = native_publish_order_errors = 0;
	sampling = lifecycle = false;
	held_lock = NULL;
	sample_lock_phase = 0;
	MyBackendId = 19;
	MyLockedGxact = NULL;
	completed_xid = InvalidTransactionId;
	allocation_error_expected = false;
	allocation_error_code = allocation_errors = 0;
}

UT_TEST(no_storage_or_procarray_mutation_before_reservation)
{
	reset_allocator();
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
	UT_ASSERT_EQ(entries[0].xid, 2052);
	UT_ASSERT_EQ(visible_xids[0], 2052);
}
UT_TEST(wait_rederives_native_counter_and_new_boot_floor)
{
	reset_allocator();
	advance_while_unlocked = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(true)), 2084);
	UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(entries[0].subxids.xids[0], 2084);
	UT_ASSERT_EQ(lock_errors, 0);
	reset_allocator();
	jump = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 4100);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(lock_errors, 0);
}
UT_TEST(actual_issuance_is_recorded_after_native_publication)
{
	FullTransactionId child;
	reset_allocator();
	jump = true;
	UT_ASSERT_EQ(variables.clusterIssuedState, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 4100);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), 4100);
	UT_ASSERT_EQ(variables.clusterIssuedState, 1);
	UT_ASSERT(U64FromFullTransactionId(variables.clusterLastIssuedXid) != limit_full);
	child = GetNewTransactionId(true);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid),
				 U64FromFullTransactionId(child));
	UT_ASSERT_EQ(native_publish_count, 2);
	UT_ASSERT_EQ(native_publish_order_errors, 0);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}
UT_TEST(native_allocation_preserves_ordinary_counter)
{
	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromU64(2053);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2053);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 3);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT_EQ(variables.clusterIssuedState, 0);
	UT_ASSERT_EQ(native_publish_count, 0);
}
UT_TEST(original_top_fullxid_is_published_and_subxid_does_not_replace_it)
{
	FullTransactionId top, child;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 2053);
	top = GetNewTransactionId(false);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	child = GetNewTransactionId(true);
	UT_ASSERT(FullTransactionIdFollows(child, top));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	UT_ASSERT_EQ(entries[0].xid, XidFromFullTransactionId(top));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(raw_wrap_preserves_the_allocated_top_epoch)
{
	FullTransactionId top, child;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, UINT32_MAX);
	top = GetNewTransactionId(false);
	child = GetNewTransactionId(true);
	UT_ASSERT_EQ(U64FromFullTransactionId(top), (UINT64CONST(7) << 32) + UINT32_MAX);
	UT_ASSERT_EQ(EpochFromFullTransactionId(child), 8);
	UT_ASSERT_EQ(XidFromFullTransactionId(child), FirstNormalTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	UT_ASSERT_EQ(entries[0].xid, UINT32_MAX);
	{
		FullTransactionId n, future, minimum;
		ClusterXidStripeSampleFence fence;

		sampling = true;
		UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
		UT_ASSERT_EQ(EpochFromFullTransactionId(n), 8);
		UT_ASSERT(FullTransactionIdEquals(minimum, top));
		UT_ASSERT_EQ(sample_lock_phase, 4);
	}
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(real_sample_keeps_original_top_when_child_cache_overflows)
{
	FullTransactionId top, n, future, minimum;
	ClusterXidStripeSampleFence fence;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 2053);
	top = GetNewTransactionId(false);
	for (unsigned i = 0; i < PGPROC_MAX_CACHED_SUBXIDS + 2; i++)
		(void) GetNewTransactionId(true);
	UT_ASSERT(entries[0].subxidStatus.overflowed);
	UT_ASSERT_EQ(entries[0].subxidStatus.count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	sampling = true;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
	UT_ASSERT(FullTransactionIdEquals(minimum, top));
	UT_ASSERT(FullTransactionIdFollows(n, top));
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(real_sample_refuses_raw_publication_that_does_not_match_allocation)
{
	FullTransactionId top, n, future, minimum;
	ClusterXidStripeSampleFence fence;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	top = GetNewTransactionId(false);
	/* Mutate only the visible compact raw word, never the actual allocation. */
	visible_xids[0] = XidFromFullTransactionId(top) + 1;
	sampling = true;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

static void
assert_sample_minimum(FullTransactionId expected)
{
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence;

	sampling = true;
	sample_lock_phase = 0;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
	sampling = false;
	UT_ASSERT(FullTransactionIdEquals(minimum, expected));
	UT_ASSERT(FullTransactionIdEquals(reference, variables.nextXid));
	UT_ASSERT(FullTransactionIdEquals(future, reference));
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(real_prepare_retains_the_allocated_identity_until_its_own_retirement)
{
	GlobalTransactionData gxact = {0};
	FullTransactionId top;
	PGPROC *prepared = &entries[1];

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, UINT32_MAX);
	top = GetNewTransactionId(false);
	for (unsigned i = 0; i <= PGPROC_MAX_CACHED_SUBXIDS; i++)
		(void) GetNewTransactionId(true);
	UT_ASSERT_EQ(U64FromFullTransactionId(top), (UINT64CONST(7) << 32) | UINT32_MAX);
	UT_ASSERT(entries[0].subxidStatus.overflowed);
	assert_sample_minimum(top);

	lifecycle = true;
	entries[0].lxid = 8;
	gxact.pgprocno = 1;
	LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
	MarkAsPreparingGuts(&gxact, XidFromFullTransactionId(top), "actual", 123, 456, 789);
	LWLockRelease(TwoPhaseStateLock);
	lifecycle = false;
	UT_ASSERT(MyLockedGxact == &gxact);
	UT_ASSERT_EQ(pg_atomic_read_u64(&prepared->cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	/* ProcArray membership is a fixture boundary. Neither original identity
	 * is supplied here: the dummy's raw/full values came from the real body. */
	procArray->numProcs = procArray->maxProcs = 2;
	procArray->pgprocnos[1] = 1;
	prepared->pgxactoff = 1;
	visible_xids[1] = prepared->xid;
	assert_sample_minimum(top);

	lifecycle = true;
	ProcArrayClearTransaction(MyProc);
	lifecycle = false;
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(visible_xids[0], InvalidTransactionId);
	UT_ASSERT_EQ(entries[0].subxidStatus.count, 0);
	UT_ASSERT(!entries[0].subxidStatus.overflowed);
	UT_ASSERT_EQ(variables.xactCompletionCount, 1);
	assert_sample_minimum(top);

	lifecycle = true;
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	ProcArrayEndTransactionInternal(prepared, XidFromFullTransactionId(top));
	LWLockRelease(ProcArrayLock);
	lifecycle = false;
	UT_ASSERT_EQ(pg_atomic_read_u64(&prepared->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(visible_xids[1], InvalidTransactionId);
	UT_ASSERT_EQ(completed_xid, XidFromFullTransactionId(top));
	assert_sample_minimum(variables.nextXid);
}

/* The real ERROR is raised before any mutation. Only the outer resource-owner
 * lock release is simulated here; no live backend ERROR recovery is claimed. */
static void
expect_unqualified_allocation_refusal(uint64 reference, int slot, bool child)
{
	VariableCacheData before_cache;
	PGPROC before_procs[lengthof(entries)];
	TransactionId before_xids[lengthof(visible_xids)];
	XidCacheStatus before_subxids[lengthof(subxids)];
	volatile bool caught = false;
	int jump_reason;

	reset_allocator();
	cluster_xid_striping = slot >= 0;
	if (slot >= 0)
		cluster_node_id = slot;
	variables.nextXid = FullTransactionIdFromU64(reference);
	if (child) {
		entries[0].xid = visible_xids[0] = 90;
		pg_atomic_write_u64(&entries[0].cluster_top_full_xid_atomic,
			U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 90)));
	}
	memcpy(&before_cache, &variables, sizeof(variables));
	memcpy(before_procs, entries, sizeof(entries));
	memcpy(before_xids, visible_xids, sizeof(visible_xids));
	memcpy(before_subxids, subxids, sizeof(subxids));
	allocation_error_expected = true;
	jump_reason = sigsetjmp(allocation_error_env, 0);
	if (jump_reason == 0)
		(void)GetNewTransactionId(child);
	else
		caught = jump_reason == 1;
	allocation_error_expected = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(allocation_errors, 1);
	UT_ASSERT_EQ(allocation_error_code, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT(held && held_lock == XidGenLock && held_mode == LW_EXCLUSIVE);
	UT_ASSERT(memcmp(&before_cache, &variables, sizeof(variables)) == 0);
	UT_ASSERT(memcmp(before_procs, entries, sizeof(entries)) == 0);
	UT_ASSERT(memcmp(before_xids, visible_xids, sizeof(visible_xids)) == 0);
	UT_ASSERT(memcmp(before_subxids, subxids, sizeof(subxids)) == 0);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 0);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(lock_errors, 0);
	LWLockRelease(XidGenLock);
	UT_ASSERT(!held);
}

UT_TEST(shared_allocator_rejects_unknown_or_special_original_identity)
{
	const uint64 invalid[] = { 0, 1, 2, UINT64_MAX,
		UINT64CONST(0x500000000), UINT64CONST(0x500000001), UINT64CONST(0x500000002) };
	const int slots[] = { -1, 4 };

	for (unsigned i = 0; i < lengthof(invalid); i++)
		for (unsigned s = 0; s < lengthof(slots); s++)
			for (unsigned child = 0; child < 2; child++)
				expect_unqualified_allocation_refusal(invalid[i], slots[s], child != 0);
}

UT_TEST(shared_stripe_projection_cannot_issue_below_original_fullxid)
{
	for (unsigned child = 0; child < 2; child++)
	{
		expect_unqualified_allocation_refusal(UINT64_MAX - 1, 4, child != 0);
		expect_unqualified_allocation_refusal(UINT64_MAX - 2, 15, child != 0);
	}
}

UT_TEST(shared_finite_identity_keeps_original_native_publication)
{
	FullTransactionId issued;
	uint64 reference = UINT64_MAX - 1;

	reset_allocator();
	cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromU64(reference);
	issued = GetNewTransactionId(false);
	UT_ASSERT_EQ(U64FromFullTransactionId(issued), reference);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), reference);
	UT_ASSERT_EQ(entries[0].xid, XidFromFullTransactionId(issued));
	UT_ASSERT_EQ(visible_xids[0], XidFromFullTransactionId(issued));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_MAX);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 3);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

/* Use the actual stripe readers with the actual allocator and PGPROC sampler.
 * Lease/claim bytes, native SLRU extensions and scheduling remain test inputs. */
static void
prepare_real_stripe(uint64 reference, int slot)
{
	reset_allocator();
	real_stripe = stripe_quorum = true;
	stripe_incarnation = 31;
	cluster_node_id = slot;
	variables.nextXid = FullTransactionIdFromU64(reference);
	MemSet(&allocation_boot, 0, sizeof(allocation_boot));
	StripeBootShmem = &allocation_boot;
	StripeLease = &allocation_boot.lease;
	stripe_lock = &allocation_boot.lock;
	allocation_boot.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	allocation_boot.floor_full = allocation_boot.my_slot_floor_full = 1024;
	allocation_boot.epoch = allocation_boot.generation = 1;
	allocation_boot.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
	allocation_boot.my_slot_claimed = true;
	allocation_boot.my_owner_incarnation = 17;
	pg_atomic_init_u64(&allocation_boot.herding_floor_full, 0);
	pg_atomic_init_u64(&allocation_boot.cluster_min_active_hwm, 0);
	pg_atomic_init_u64(&allocation_boot.cluster_max_active_hwm, 0);
	pg_atomic_init_u32(&allocation_boot.wrap_barrier_done, 1);
	pg_atomic_init_u64(&StripeLease->floor_full, reference);
	pg_atomic_init_u64(&StripeLease->limit_full, reference + 4096);
	pg_atomic_init_u64(&StripeLease->published_incarnation, stripe_incarnation);
	pg_atomic_init_u32(&StripeLease->failed, 0);
}

static void
assert_real_stripe_sample(FullTransactionId expected_minimum, uint64 expected_future)
{
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence;
	VariableCacheData before = variables;
	unsigned extensions_before = extensions;

	sampling = true;
	sample_lock_phase = 0;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
	sampling = false;
	UT_ASSERT(FullTransactionIdEquals(reference, variables.nextXid));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), expected_future);
	UT_ASSERT(FullTransactionIdEquals(minimum, expected_minimum));
	UT_ASSERT_EQ(fence.activation_floor_full, allocation_boot.floor_full);
	UT_ASSERT_EQ(fence.activation_generation, allocation_boot.generation);
	UT_ASSERT_EQ(fence.slot_floor_full, allocation_boot.my_slot_floor_full);
	UT_ASSERT_EQ(fence.slot_owner_incarnation, allocation_boot.my_owner_incarnation);
	UT_ASSERT_EQ(fence.incarnation, stripe_incarnation);
	UT_ASSERT_EQ(fence.slot, cluster_node_id);
	UT_ASSERT_EQ(fence.lease_floor_full, pg_atomic_read_u64(&StripeLease->floor_full));
	UT_ASSERT_EQ(fence.lease_limit_full, pg_atomic_read_u64(&StripeLease->limit_full));
	UT_ASSERT(allocation_real_fence_current(&fence));
	UT_ASSERT_EQ(memcmp(&variables, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(extensions, extensions_before);
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held && !stripe_lock_held);
}

UT_TEST(actual_stripe_sample_keeps_assigned_top_through_children_and_raw_wrap)
{
	const struct {
		uint64 reference, top, child, future;
		int slot;
		unsigned extension_probes;
	} inputs[] = {
		{2052, 2052, 2068, 2084, 4, 51},
		{(UINT64CONST(7) << 32) | (UINT32_MAX - 2),
		 (UINT64CONST(8) << 32) | 4, (UINT64CONST(8) << 32) | 20,
		 (UINT64CONST(8) << 32) | 36, 4, 63},
		{(UINT64CONST(7) << 32) | (UINT32_MAX - 2),
		 (UINT64CONST(7) << 32) | UINT32_MAX, (UINT64CONST(8) << 32) | 15,
		 (UINT64CONST(8) << 32) | 31, 15, 48}
	};

	for (unsigned i = 0; i < lengthof(inputs); i++) {
		FullTransactionId top, child;

		prepare_real_stripe(inputs[i].reference, inputs[i].slot);
		top = GetNewTransactionId(false);
		child = GetNewTransactionId(true);
		UT_ASSERT_EQ(U64FromFullTransactionId(top), inputs[i].top);
		UT_ASSERT_EQ(U64FromFullTransactionId(child), inputs[i].child);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), inputs[i].top);
		UT_ASSERT_EQ(entries[0].xid, XidFromFullTransactionId(top));
		UT_ASSERT_EQ(visible_xids[0], XidFromFullTransactionId(top));
		UT_ASSERT_EQ(entries[0].subxids.xids[0], XidFromFullTransactionId(child));
		UT_ASSERT_EQ(waits, 0);
		/* Three original Extend* probes per normal position, including the
		 * original allocator's skipped stripe positions; raw 0/1/2 are skipped. */
		UT_ASSERT_EQ(extensions, inputs[i].extension_probes);
		UT_ASSERT_EQ(early_extensions, 0);
		assert_real_stripe_sample(top, inputs[i].future);
	}
}

UT_TEST(actual_stripe_source_refusal_cannot_erase_the_original_allocation)
{
	for (unsigned scenario = 0; scenario < 5; scenario++) {
		FullTransactionId top, reference = FullTransactionIdFromU64(99);
		FullTransactionId future = reference, minimum = reference;
		ClusterXidStripeSampleFence fence, empty = {0};
		PGPROC before[2];
		VariableCacheData before_cache;
		unsigned extensions_before;

		prepare_real_stripe((UINT64CONST(7) << 32) | 2052, 4);
		top = GetNewTransactionId(false);
		UT_ASSERT_EQ(U64FromFullTransactionId(top), (UINT64CONST(7) << 32) | 2052);
		switch (scenario) {
		case 0: pg_atomic_write_u32(&StripeLease->failed, 1); break;
		case 1: stripe_quorum = false; break;
		case 2: pg_atomic_write_u64(&StripeLease->published_incarnation, stripe_incarnation + 1); break;
		case 3: pg_atomic_write_u32(&allocation_boot.wrap_barrier_done, 0); break;
		case 4: allocation_boot.my_slot_claimed = false; break;
		}
		memcpy(before, entries, sizeof(before));
		before_cache = variables;
		extensions_before = extensions;
		MemSet(&fence, 0x5a, sizeof(fence));
		sampling = true;
		sample_lock_phase = 0;
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
		sampling = false;
		UT_ASSERT(!FullTransactionIdIsValid(reference));
		UT_ASSERT(!FullTransactionIdIsValid(future));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(memcmp(&fence, &empty, sizeof(empty)), 0);
		UT_ASSERT_EQ(memcmp(entries, before, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&variables, &before_cache, sizeof(before_cache)), 0);
		UT_ASSERT_EQ(visible_xids[0], XidFromFullTransactionId(top));
		UT_ASSERT_EQ(extensions, extensions_before);
		UT_ASSERT_EQ(early_extensions, 0);
		UT_ASSERT_EQ(waits, 0);
		UT_ASSERT_EQ(sample_lock_phase, 2);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held && !stripe_lock_held);
	}
}

UT_TEST(actual_stripe_future_keeps_prepared_fullxid_until_original_retirement)
{
	GlobalTransactionData gxact = {0};
	FullTransactionId top;
	PGPROC *prepared = &entries[1];

	prepare_real_stripe(2052, 4);
	top = GetNewTransactionId(false);
	(void)GetNewTransactionId(true);
	assert_real_stripe_sample(top, 2084);
	lifecycle = true;
	entries[0].lxid = 8;
	gxact.pgprocno = 1;
	LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
	MarkAsPreparingGuts(&gxact, XidFromFullTransactionId(top), "stripe", 123, 456, 789);
	LWLockRelease(TwoPhaseStateLock);
	lifecycle = false;
	UT_ASSERT_EQ(pg_atomic_read_u64(&prepared->cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	/* Original body supplied both identities; only membership is controlled. */
	procArray->numProcs = procArray->maxProcs = 2;
	procArray->pgprocnos[1] = 1;
	prepared->pgxactoff = 1;
	visible_xids[1] = prepared->xid;
	assert_real_stripe_sample(top, 2084);
	lifecycle = true;
	ProcArrayClearTransaction(MyProc);
	lifecycle = false;
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(visible_xids[0], InvalidTransactionId);
	assert_real_stripe_sample(top, 2084);
	lifecycle = true;
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	ProcArrayEndTransactionInternal(prepared, XidFromFullTransactionId(top));
	LWLockRelease(ProcArrayLock);
	lifecycle = false;
	UT_ASSERT_EQ(pg_atomic_read_u64(&prepared->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(visible_xids[1], InvalidTransactionId);
	UT_ASSERT_EQ(completed_xid, XidFromFullTransactionId(top));
	UT_ASSERT_EQ(early_extensions, 0);
	assert_real_stripe_sample(FullTransactionIdFromU64(2084), 2084);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(no_storage_or_procarray_mutation_before_reservation);
	UT_RUN(wait_rederives_native_counter_and_new_boot_floor);
	UT_RUN(native_allocation_preserves_ordinary_counter);
	UT_RUN(actual_issuance_is_recorded_after_native_publication);
	UT_RUN(original_top_fullxid_is_published_and_subxid_does_not_replace_it);
	UT_RUN(raw_wrap_preserves_the_allocated_top_epoch);
	UT_RUN(real_sample_keeps_original_top_when_child_cache_overflows);
	UT_RUN(real_sample_refuses_raw_publication_that_does_not_match_allocation);
	UT_RUN(real_prepare_retains_the_allocated_identity_until_its_own_retirement);
	UT_RUN(shared_allocator_rejects_unknown_or_special_original_identity);
	UT_RUN(shared_stripe_projection_cannot_issue_below_original_fullxid);
	UT_RUN(shared_finite_identity_keeps_original_native_publication);
	UT_RUN(actual_stripe_sample_keeps_assigned_top_through_children_and_raw_wrap);
	UT_RUN(actual_stripe_source_refusal_cannot_erase_the_original_allocation);
	UT_RUN(actual_stripe_future_keeps_prepared_fullxid_until_original_retirement);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
