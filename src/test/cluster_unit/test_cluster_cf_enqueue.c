/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_enqueue.c
 *	  Unit tests for the CF enqueue layer (spec-5.6 Db1/Db2).
 *
 *	  U1 covers the pure resid encoder.  The Db2 tests stub the GES
 *	  seven-step / S5 promote / S6 release entry points so the
 *	  correctness-critical result-to-action mapping in cluster_cf_lock /
 *	  cluster_cf_unlock can be exercised deterministically: a grant must
 *	  register a holder and the matching release must drain exactly that CF
 *	  holder; an OK_NATIVE (cluster layer inactive) must NOT register a
 *	  holder, so release is a no-op; and any failure must fail closed
 *	  without claiming the lock.  The real cross-node grant/release is the
 *	  2-node TAP (t/288).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Db1/Db2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xact.h"
#include "cluster/cluster_cancel_token.h"

#include <setjmp.h>
#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_lock_owner.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_sequence.h"
#include "cluster/cluster_control_retire.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "utils/memutils.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

AuxProcType MyAuxProcType = NotAnAuxProcess;
BackendType MyBackendType = B_INVALID;
static ClusterNormalStopPollResult g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
ClusterNormalStopPollResult
cluster_cf_normal_stop_shared_poll(bool post_checkpoint, const char **reason)
{
	(void)post_checkpoint;
	if (reason)
		*reason = "FIXTURE_CF_SHARED";
	return g_shared_stop_result;
}
bool cluster_controlfile_shared_authority = false;
bool cluster_shared_config = false;
int cluster_node_id = 0;
char *cluster_config_file = NULL;
static int g_node_count = 1;

int
cluster_conf_node_count(void)
{
	return g_node_count;
}

FILE *
AllocateFile(const char *name, const char *mode)
{
	return fopen(name, mode);
}

int
FreeFile(FILE *file)
{
	return fclose(file);
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* RF-ROOT P6 (L4/L5 diag refs): cluster_cf_enqueue.o samples the phase word,
 * the clean-leave write gate, the GRD shard/master surface and the process
 * context; the pure unit pins them inert so the binary stays standalone. */
bool IsUnderPostmaster = false;
int MyProcPid = 0;

static bool g_capture_log;
static char g_log_detail[1024];
static ClusterGesTimeoutDetail g_timeout_detail;

const ClusterGesTimeoutDetail *
cluster_ges_timeout_detail_get(void)
{
	return &g_timeout_detail;
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return g_capture_log;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(g_log_detail, sizeof(g_log_detail), fmt, args);
	va_end(args);
	return 0;
}

bool
cluster_clean_leave_node_refuses_writes(void)
{
	return false;
}

int
cluster_current_phase(void)
{
	return 0;
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

int32
cluster_grd_lookup_master(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

/* ---- GES substrate stubs (settable outcomes) ---- */
static ClusterLockAcquireResult g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static int g_s6_count = 0;
static uint8 g_s6_last_resid_type = 0;
static int g_seven_count;
static uint64 g_next_request_id = 7;
static ClusterLockAcquireRequest g_promoted_request;
static ClusterLockAcquireRequest g_released_request;
static bool g_s6_observe;
static bool g_s6_throw;
static PGPROC owner_proc;
PGPROC *MyProc = &owner_proc;
static uint64 owner_epoch = 9;
static uint64 owner_generation = 1;
static uint64 owner_routing_generation = 1;
static unsigned owner_wait_deleted;
static bool observe_installing;
static bool drift_during_install;
static bool observe_releasing;
static bool no_route;
static bool throw_during_install;
static bool observe_acquiring;
static bool throw_during_acquire;
static bool fail_after_publication;
static bool throw_before_publication;
static ClusterLockAcquireRequest observed_acquire_request;
static ClusterGesRedeclareResult owner_reply = CLUSTER_GES_REDECLARE_PENDING;
static uint64 owner_next_id = 100;
static ClusterGrdHolderId observed_target;
static unsigned owner_poll_count;
static void *control_memory;
static LWLock *control_locked;
static uint64 retire_driver;
static pg_on_exit_callback recorded_exit;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext TopMemoryContext = (MemoryContext)1;
Latch *MyLatch = &owner_proc.procLatch;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
static TimestampTz owner_clock;
static long owner_clock_step = 60000000;
static bool owner_ack_on_wait;
static bool owner_rebuild_frozen;
static unsigned owner_wait_calls;
static bool acknowledge_retirements(void);
#include "test_cluster_startup_interrupt_fixture.h"
static unsigned owner_shutdown_on_wait;

/* The common gate itself is exercised against the real GRD FSM separately. */
bool
cluster_grd_control_recovery_ready(const ClusterResId *resid pg_attribute_unused(),
								   LOCKMODE mode pg_attribute_unused())
{
	return false;
}
bool
cluster_grd_control_rebuild_frozen(uint64 epoch, uint64 generation)
{
	return owner_rebuild_frozen && epoch == cluster_epoch_get_current()
		   && generation == cluster_grd_redeclare_generation();
}

void
cluster_lmon_wakeup(void)
{}
void
ResetLatch(Latch *latch pg_attribute_unused())
{}
int
WaitLatch(Latch *latch pg_attribute_unused(), int wake_events pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 event pg_attribute_unused())
{
	owner_wait_calls++;
	if (owner_shutdown_on_wait != 0) {
		StartupProcShutdownHandler(SIGTERM);
		/* Bound RED without changing the first missing-ACK observation. */
		if (--owner_shutdown_on_wait == 0)
			UT_ASSERT(acknowledge_retirements());
	}
	if (owner_ack_on_wait)
		UT_ASSERT(acknowledge_retirements());
	return WL_LATCH_SET;
}
void
ProcessInterrupts(void)
{
	InterruptPending = false;
}
void
cluster_grd_redeclare_all_registered(void)
{
	uint64 enumerated;

	(void)cluster_lock_owners_redeclare(&enumerated);
}
int cluster_ges_request_timeout_ms = 60000;
static ClusterGesAcquireResult cf_poll_result = CLUSTER_GES_ACQUIRE_PENDING;
static unsigned cf_acquire_polls;
static bool cf_cancel;

TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}
bool
cluster_cancel_token_consume(void)
{
	bool result = cf_cancel;
	cf_cancel = false;
	return result;
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	UT_ASSERT(context == TopMemoryContext);
	return calloc(1, size);
}
void
pfree(void *memory)
{
	free(memory);
}

void
before_shmem_exit(pg_on_exit_callback callback, Datum argument pg_attribute_unused())
{
	recorded_exit = callback;
}

int
cluster_grd_retire_request_and_drain(const ClusterResId *resid pg_attribute_unused(),
									 const ClusterGrdHolderId *holder pg_attribute_unused(),
									 uint64 previous pg_attribute_unused(),
									 LOCKMODE mode pg_attribute_unused(),
									 ClusterGrdGrantIdentity *grants, int count)
{
	UT_ASSERT(grants == NULL && count == 0);
	return CLUSTER_GRD_RELEASE_NOT_FOUND;
}

/* Allocation/transport are fixture boundaries; the request registry and
 * stable CF owner are production objects, not verdict stubs. */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	*found = control_memory != NULL;
	if (control_memory == NULL)
		control_memory = calloc(1, size);
	return control_memory;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}
int
LWLockNewTrancheId(void)
{
	return 200;
}
void
LWLockRegisterTranche(int id pg_attribute_unused(), const char *name pg_attribute_unused())
{}
void
LWLockInitialize(LWLock *lock, int id)
{
	lock->tranche = id;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	Assert(control_locked == NULL);
	control_locked = lock;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	Assert(control_locked == lock);
	control_locked = NULL;
}

static void
control_cut(ClusterControlRequestCut *out)
{
	memset(out, 0, sizeof(*out));
	out->epoch = owner_epoch;
	out->generation = owner_routing_generation;
	out->master = 0;
}

static bool
acknowledge_retirements(void)
{
	ClusterControlRequestView view;
	ClusterControlRequestCut current;
	ClusterControlRetireMessage message;
	uint32 cursor = 0;
	bool observed = false;

	control_cut(&current);
	while (cluster_control_request_next(&cursor, &view)) {
		if (view.state != CLUSTER_CONTROL_REQUEST_ABANDONED)
			continue;
		UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &current, &message));
		UT_ASSERT(!cluster_control_request_send_allowed(&message.key));
		message.verb = CLUSTER_CONTROL_RETIRED;
		UT_ASSERT(cluster_control_request_ack(&message, 0, retire_driver, &current));
		observed = true;
	}
	return observed;
}

void
FlushErrorState(void)
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

uint64
cluster_epoch_get_current(void)
{
	return owner_epoch;
}

uint64
cluster_grd_redeclare_generation(void)
{
	return owner_generation;
}

uint64
cluster_ges_reply_wait_next_request_id(void)
{
	return ++owner_next_id;
}

ClusterGesRedeclareResult
cluster_ges_redeclare_poll(ClusterGesRedeclareAttempt *attempt, const ClusterResId *resid,
						   uint32 mode, const ClusterGrdHolderId *holder)
{
	UT_ASSERT(cluster_control_request_resid_valid(resid));
	UT_ASSERT(mode == ShareLock || mode == ExclusiveLock);
	observed_target = *holder;
	owner_poll_count++;
	if (holder->cluster_epoch != owner_epoch)
		return CLUSTER_GES_REDECLARE_CUT_CHANGED;
	if (no_route)
		return CLUSTER_GES_REDECLARE_PENDING;
	if (!attempt->initialized) {
		attempt->initialized = true;
		attempt->key.cluster_epoch = holder->cluster_epoch;
		attempt->key.request_id = holder->request_id;
		attempt->master = 0;
		attempt->master_generation = owner_routing_generation;
		attempt->wait_registered = true;
	}
	if (owner_reply == CLUSTER_GES_REDECLARE_CONFIRMED)
		attempt->wait_registered = false;
	return owner_reply;
}

int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(), uint64 *generation)
{
	*generation = owner_routing_generation;
	return 0;
}

void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *key pg_attribute_unused())
{
	owner_wait_deleted++;
}
/* spec-5.6 Dc4b: capture what cluster_cf_lock threaded into the request. */
static int g_last_timeout_ms = -999;
static uint32 g_last_wait_event = 0xFFFFFFFFu;

ClusterLockAcquireResult
cluster_lock_acquire_s1_entry(const ClusterLockAcquireRequest *req pg_attribute_unused())
{
	return g_seven_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	ClusterLockAcquireRequest *mut = (ClusterLockAcquireRequest *)req;

	g_seven_count++;
	/* spec-5.6 Dc4b: record the CF acquire's timeout + wait-event override. */
	g_last_timeout_ms = req->timeout_ms;
	g_last_wait_event = req->wait_event;
	if (throw_before_publication)
		pg_re_throw();
	/* Default refusal fixture represents the pre-publication S1/S3 path.
	 * Explicit late-failure tests allocate identity and transport state. */
	if (!throw_during_acquire && !fail_after_publication
		&& g_seven_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		&& g_seven_result != CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK)
		return g_seven_result;

	/* simulate S3 fill_request_holder filling the holder + request id */
	mut->holder.node_id = cluster_node_id;
	mut->holder.procno = 42;
	mut->holder.cluster_epoch = owner_epoch;
	if (mut->request_id == 0)
		mut->request_id = g_next_request_id;
	mut->holder.request_id = mut->request_id;
	if (!cluster_lock_owner_request_prepare(mut))
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	if (observe_acquiring) {
		uint64 enumerated = 0;

		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
		UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 1);
		UT_ASSERT(cluster_cf_held(req->lockmode));
		UT_ASSERT(!cluster_cf_held_is_usable(req->lockmode));
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(req->lockmode), CLUSTER_CF_RELEASE_UNCONFIRMED);
	}
	/* Real S3 reservation metadata written before the controlled S4 ERROR.
	 * The CF transport's complete retained grant is a separate pending edge. */
	if (throw_during_acquire || fail_after_publication) {
		mut->master_gen_snapshot = 6622;
		observed_acquire_request = *req;
	}
	if (throw_during_acquire)
		pg_re_throw();
	return g_seven_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{
	if (observe_installing) {
		uint64 enumerated = 0;

		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
		UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 1);
		UT_ASSERT(!cluster_cf_held_is_usable(req->lockmode));
	}
	g_promoted_request = *req;
	if (throw_during_install)
		pg_re_throw();
	if (drift_during_install)
		owner_epoch++;
	return g_s5_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s2_identity(const ClusterLockAcquireRequest *req pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

ClusterLockAcquireResult
cluster_lock_acquire_s3_partition_reservation(const ClusterLockAcquireRequest *req)
{
	return cluster_lock_acquire_seven_step(req); /* Explicit S3 boundary fixture. */
}

ClusterGesAcquireResult
cluster_ges_cf_request_poll(ClusterGesAcquireAttempt *attempt, const ClusterResId *resid,
							uint32 mode, const ClusterGrdHolderId *holder, ClusterGesHwGrant *grant)
{
	(void)resid;
	(void)mode;
	(void)grant;
	if (attempt->exchange.initialized)
		UT_ASSERT_EQ(attempt->exchange.key.request_id, holder->request_id);
	attempt->exchange.initialized = true;
	attempt->exchange.key.request_id = holder->request_id;
	attempt->exchange.key.cluster_epoch = holder->cluster_epoch;
	cf_acquire_polls++;
	return cf_poll_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	g_s6_count++;
	g_s6_last_resid_type = req->resid.type;
	g_released_request = *req;
	if (g_s6_observe) {
		/* These are observations of the real CF owner while S6 is in flight. */
		UT_ASSERT(cluster_cf_held(req->lockmode));
		UT_ASSERT(!cluster_cf_held_is_clusterwide(req->lockmode));
		UT_ASSERT(!cluster_cf_write_permitted());
	}
	if (g_s6_throw)
		pg_re_throw();
	if (observe_releasing) {
		uint64 enumerated = 0;
		uint64 id = req->request_id;
		unsigned polls = owner_poll_count;

		owner_epoch++;
		owner_generation++;
		UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 1);
		UT_ASSERT_EQ(owner_poll_count, polls);
		UT_ASSERT_EQ(req->request_id, id);
	}
	return g_s6_result;
}

/* spec-5.6 Dc4: cluster_cf_lock bumps CF acquire/fail-closed counters;
 * cluster_cf_stats.o is not linked here, so a no-op stub satisfies the link
 * (the counter mechanism is covered by test_cluster_cf_stats). */
void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

/* spec-5.6 Dc4b: cluster_cf_lock reads this GUC into req.timeout_ms; cluster_
 * guc.o is not linked here, so define it locally. */
int cluster_cf_enqueue_timeout_ms = 30000;

/* spec-5.6 increment (iii) follow-up: join-readonly is now a cross-process CF
 * shmem flag (cluster_cf_stats.o, not linked here).  A stateful stub keeps the
 * set->get behaviour the write-permission test exercises. */
static bool g_join_ro = false;
void
cluster_cf_stats_set_join_readonly(bool on)
{
	g_join_ro = on;
}
bool
cluster_cf_stats_get_join_readonly(void)
{
	return g_join_ro;
}

/* R18: model the real actor-bound shared phase while testing the enqueue-local
 * permission and eligibility wrapper without linking cluster_cf_stats.o. */
static ClusterCfOwnerEorPhase g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;

ClusterCfOwnerEorPhase
cluster_cf_owner_eor_phase_read(void)
{
	return g_owner_eor_phase;
}

bool
cluster_cf_owner_eor_phase_install(void)
{
	if (!AmStartupProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_EMPTY)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_INSTALLED;
	return true;
}

bool
cluster_cf_owner_eor_phase_activate(void)
{
	if (!AmCheckpointerProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_INSTALLED)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_ACTIVE;
	return true;
}

bool
cluster_cf_owner_eor_phase_done(void)
{
	if (!AmCheckpointerProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_ACTIVE)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_DONE;
	return true;
}

bool
cluster_cf_owner_eor_phase_clear(void)
{
	if (!AmStartupProcess()
		|| (g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_INSTALLED
			&& g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_DONE))
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	owner_clock += owner_clock_step;
	return owner_clock;
}

/* ======================================================================
 * U1 -- CF resid encoding
 * ====================================================================== */
UT_TEST(test_cf_resid_encode)
{
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	cluster_cf_resid_encode(&r);

	UT_ASSERT_EQ(r.field1, 0);
	UT_ASSERT_EQ(r.field2, 0);
	UT_ASSERT_EQ(r.field3, 0);
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_CF_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF1);
	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* ======================================================================
 * Db2 -- a cluster grant registers a holder; release drains the CF holder
 * ====================================================================== */
UT_TEST(test_lock_grant_then_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;
	g_s6_last_resid_type = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held(ExclusiveLock)); /* held while locked */
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock)); /* released */

	UT_ASSERT_EQ(g_s6_count, 1);			  /* exactly one release */
	UT_ASSERT_EQ(g_s6_last_resid_type, 0xF1); /* of the CF resid, not a locktag */
}

/* ======================================================================
 * Db3 -- write permission: held CF X, or the bootstrap authority window
 * ====================================================================== */
UT_TEST(test_held_and_write_permitted)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;

	/* nothing held -> no write permitted */
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_write_permitted());

	/* holding CF X permits a write */
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_write_permitted());

	/* the bootstrap window permits a write without a held CF X */
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_set_bootstrap_authority(false);
	UT_ASSERT(!cluster_cf_write_permitted());

	/*
	 * Join read-only (increment ii) is an orthogonal signal: it marks an
	 * attaching node whose recovery writes are skipped, and it must NEVER by
	 * itself grant write permission (a join node is a reader, not a writer).
	 */
	UT_ASSERT(!cluster_cf_join_readonly());
	cluster_cf_set_join_readonly(true);
	UT_ASSERT(cluster_cf_join_readonly());
	UT_ASSERT(!cluster_cf_write_permitted()); /* join != write permission */
	cluster_cf_set_join_readonly(false);
	UT_ASSERT(!cluster_cf_join_readonly());

	/*
	 * The process-local bring-up write-skip is what the chokepoint consults; it
	 * is orthogonal to write permission (skipping a write is not permission to
	 * write) and independent of the node-wide join flag.
	 */
	UT_ASSERT(!cluster_cf_write_skip());
	cluster_cf_set_write_skip(true);
	UT_ASSERT(cluster_cf_write_skip());
	UT_ASSERT(!cluster_cf_write_permitted()); /* write-skip != write permission */
	cluster_cf_set_write_skip(false);
	UT_ASSERT(!cluster_cf_write_skip());
}

/* ======================================================================
 * R18 -- exact OWNER->EOR eligibility, local authority and abort boundary
 * ====================================================================== */
UT_TEST(test_owner_eor_handoff_gates_and_lifecycle)
{
	cluster_controlfile_shared_authority = true;
	g_node_count = 1;
	g_join_ro = false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	cluster_cf_set_bootstrap_authority(false);
	cluster_cf_owner_eor_abort();

	/* INSTALL is Startup-only and exact-one-node only. */
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	MyAuxProcType = StartupProcess;
	g_node_count = 2;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	g_node_count = 1;
	g_join_ro = true;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	g_join_ro = false;
	cluster_controlfile_shared_authority = false;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	cluster_controlfile_shared_authority = true;
	UT_ASSERT(cluster_cf_owner_eor_install());
	UT_ASSERT(cluster_cf_in_bootstrap_window());
	UT_ASSERT(cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_INSTALLED);

	/* Simulate the separate checkpointer process: phase alone grants no write. */
	cluster_cf_set_bootstrap_authority(false);
	UT_ASSERT(!cluster_cf_write_permitted());

	/* CONSUME requires actor, EOR, authority, exact role, JOIN=false and identity. */
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true)); /* still Startup */
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_consume(false, true));
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, false));
	cluster_controlfile_shared_authority = false;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	cluster_controlfile_shared_authority = true;
	g_node_count = 2;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	g_node_count = 1;
	g_join_ro = true;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	g_join_ro = false;
	UT_ASSERT(cluster_cf_owner_eor_consume(true, true));
	UT_ASSERT(cluster_cf_owner_eor_local_active());
	UT_ASSERT(cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_ACTIVE);

	UT_ASSERT(cluster_cf_owner_eor_complete());
	UT_ASSERT(!cluster_cf_owner_eor_local_active());
	UT_ASSERT(!cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_DONE);

	/* Original Startup permission remains local there until the final close. */
	MyAuxProcType = StartupProcess;
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT(cluster_cf_owner_eor_close());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_EMPTY);
	UT_ASSERT(!cluster_cf_in_bootstrap_window());
	UT_ASSERT(!cluster_cf_write_permitted());

	cluster_controlfile_shared_authority = false;
	MyAuxProcType = NotAnAuxProcess;
}

UT_TEST(test_owner_eor_disabled_seed_uses_exact_declared_node)
{
	char path[] = "/tmp/pgrac-cf-seed-XXXXXX";
	int fd;
	FILE *f;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	f = fdopen(fd, "w");
	UT_ASSERT(f != NULL);
	UT_ASSERT(fprintf(f, "[cluster]\nname = pgrac\n\n[node.0]\n"
						 "interconnect_addr = 127.0.0.1:6433\n")
			  > 0);
	UT_ASSERT_EQ(fclose(f), 0);

	g_node_count = 0;
	cluster_node_id = 0;
	cluster_config_file = path;
	UT_ASSERT(cluster_cf_exactly_one_declared_node());

	cluster_node_id = 1;
	UT_ASSERT(!cluster_cf_exactly_one_declared_node());

	UT_ASSERT_EQ(unlink(path), 0);
	cluster_config_file = NULL;
	cluster_node_id = 0;
	g_node_count = 1;
}

UT_TEST(test_owner_eor_abort_retains_active_and_blocks_retry)
{
	cluster_controlfile_shared_authority = true;
	g_node_count = 1;
	g_join_ro = false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_INSTALLED;
	cluster_cf_set_bootstrap_authority(false);
	MyAuxProcType = CheckpointerProcess;

	UT_ASSERT(cluster_cf_owner_eor_consume(true, true));
	UT_ASSERT(cluster_cf_owner_eor_local_active());
	cluster_cf_owner_eor_abort();
	UT_ASSERT(!cluster_cf_owner_eor_local_active());
	UT_ASSERT(!cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_ACTIVE);
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));

	/* Test-process cleanup only; product abort/close never performs this edge. */
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	cluster_controlfile_shared_authority = false;
	MyAuxProcType = NotAnAuxProcess;
}

/* ======================================================================
 * Db2 -- OK_NATIVE (cluster layer inactive) registers no holder
 * ====================================================================== */
UT_TEST(test_lock_native_no_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	cluster_cf_unlock(ShareLock);

	UT_ASSERT_EQ(g_s6_count, 0); /* uncoordinated -> no S6 */
}

UT_TEST(test_confirmed_release_requires_clusterwide_s6_success)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT(!cluster_cf_write_permitted());

	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
}

UT_TEST(test_void_release_retains_exact_request_without_authority)
{
	static const ClusterLockAcquireResult failures[]
		= { CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT, CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE,
			CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL };
	int i, j;

	cluster_cf_set_bootstrap_authority(false);
	cluster_cf_owner_eor_abort();
	g_seven_result = g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	for (i = 0; i < 2; i++) {
		LOCKMODE mode = i ? ExclusiveLock : ShareLock;
		for (j = 0; j < lengthof(failures); j++) {
			ClusterLockAcquireRequest acquired;
			UT_ASSERT(cluster_cf_lock(mode));
			acquired = g_promoted_request;
			g_s6_result = failures[j];
			g_s6_observe = true;
			cluster_cf_unlock(mode);
			UT_ASSERT(cluster_cf_held(mode));
			UT_ASSERT(!cluster_cf_held_is_clusterwide(mode));
			UT_ASSERT(!cluster_cf_write_permitted());
			UT_ASSERT(memcmp(&g_released_request, &acquired, sizeof(acquired)) == 0);
			g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
			UT_ASSERT(memcmp(&g_released_request, &acquired, sizeof(acquired)) == 0);
			UT_ASSERT(!cluster_cf_held(mode));
			g_s6_observe = false;
		}
	}
}

static void
check_release_interrupt(LOCKMODE mode)
{
	volatile bool caught = false;

	g_seven_result = g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(mode));
	g_s6_throw = g_s6_observe = true;
	PG_TRY();
	{
		cluster_cf_unlock(mode);
		UT_ASSERT(false); /* The controlled substrate must unwind this call. */
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	g_s6_throw = false;
	UT_ASSERT(cluster_cf_held(mode));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(mode));
	UT_ASSERT(!cluster_cf_write_permitted());
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(memcmp(&g_released_request, &g_promoted_request, sizeof(g_released_request)) == 0);
	UT_ASSERT(!cluster_cf_held(mode));
	g_s6_observe = false;
}

UT_TEST(test_release_interrupt_keeps_cleanup_but_no_authority)
{
	check_release_interrupt(ShareLock);
	check_release_interrupt(ExclusiveLock);
}

UT_TEST(test_reacquire_drains_old_identity_before_creating_new)
{
	ClusterLockAcquireRequest original;
	int acquires;

	g_seven_result = g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id = 41;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	original = g_promoted_request;
	acquires = g_seven_count;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	g_next_request_id = 42;
	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, acquires);
	UT_ASSERT(memcmp(&g_released_request, &original, sizeof(original)) == 0);
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_write_permitted());
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, acquires + 1);
	UT_ASSERT(memcmp(&g_released_request, &original, sizeof(original)) == 0);
	UT_ASSERT_EQ(g_promoted_request.request_id, 42);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT_EQ(g_released_request.request_id, 42);
	g_next_request_id = 7;
}

UT_TEST(test_native_hold_never_becomes_clusterwide_authority)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_NOT_HELD);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(g_s6_count, 0);
}

/* ======================================================================
 * Db2 -- a GES failure fails closed and registers nothing
 * ====================================================================== */
UT_TEST(test_lock_failclosed_timeout)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	g_s6_count = 0;
	g_timeout_detail
		= (ClusterGesTimeoutDetail){ CLUSTER_GES_TSRC_MASTER_REJECT_TIMEOUT, 2, 137, 3, 1, 30000 };
	g_log_detail[0] = '\0';
	g_capture_log = true;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	g_capture_log = false;
	UT_ASSERT(strstr(g_log_detail,
					 "timeout_master=2 elapsed_ms=137 attempts=3 conflicts=1 timeout_ms=30000")
			  != NULL);
	cluster_cf_unlock(ExclusiveLock); /* not held -> no-op */

	UT_ASSERT_EQ(g_s6_count, 0);
}

/* ======================================================================
 * Db2 -- a granted reservation that fails the S5 promote fails closed
 * ====================================================================== */
UT_TEST(test_lock_s5_fail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	g_s6_count = 0;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	cluster_cf_unlock(ExclusiveLock); /* S5 failure still owns exact cleanup. */
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);

	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* reset */
}

/* ======================================================================
 * Db2 -- a try-conflict (NOT_AVAIL) does not claim the lock
 * ====================================================================== */
UT_TEST(test_lock_notavail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;

	UT_ASSERT(!cluster_cf_lock(ShareLock));
}

/* ======================================================================
 * Dc4b -- the CF acquire carries cluster.cf_enqueue_timeout_ms +
 * WAIT_EVENT_CLUSTER_CF_ENQUEUE so the GES wait is bounded + observable
 * ====================================================================== */
UT_TEST(test_lock_timeout_and_wait_event)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_last_timeout_ms = -999;
	g_last_wait_event = 0xFFFFFFFFu;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock);

	/* CF threads the GUC timeout + its own wait-event label into the request */
	UT_ASSERT_EQ(g_last_timeout_ms, cluster_cf_enqueue_timeout_ms);
	UT_ASSERT_EQ(g_last_wait_event, (uint32)WAIT_EVENT_CLUSTER_CF_ENQUEUE);
}

UT_TEST(test_stop_cf_original_holds_and_confirmed_retirement)
{
	const char *reason;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	cluster_cf_set_bootstrap_authority(false);
	cluster_cf_set_write_skip(false);
	cluster_cf_owner_eor_abort();
	g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
	g_seven_result = g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	for (int i = 0; i < 2; i++) {
		LOCKMODE mode = i ? ExclusiveLock : ShareLock;
		int releases;
		UT_ASSERT(cluster_cf_lock(mode));
		releases = g_s6_count;
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(g_s6_count, releases);
		UT_ASSERT(cluster_cf_held(mode));
		g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_UNCONFIRMED);
		UT_ASSERT(!cluster_cf_held_is_clusterwide(mode));
		UT_ASSERT(!cluster_cf_write_permitted());
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_PENDING);
		g_shared_stop_result = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
		g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
		g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	}
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_BACKEND;
	MyAuxProcType = NotAnAuxProcess;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_stop_cf_separates_old_skip_from_post_checkpoint_permission)
{
	const char *reason;
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
	cluster_cf_set_write_skip(true); /* previous EOR's scoped value */
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(false, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(reason, "CF_POST_CHECKPOINT_WRITE_SKIP") == 0);
	UT_ASSERT(cluster_cf_write_skip());
	/* The original next normal checkpoint, not the observer, clears it. */
	cluster_cf_set_write_skip(false);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(false, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(cluster_cf_in_bootstrap_window());
	cluster_cf_set_bootstrap_authority(false);
}

UT_TEST(test_private_cf_is_enumerated_before_s5_and_until_release)
{
	uint64 enumerated = 0;

	g_seven_result = g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	observe_installing = true;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	observe_installing = false;
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_pending_redeclare_owns_release_identity)
{
	uint64 enumerated = 0;
	ClusterGrdHolderId target;
	int releases;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_PENDING;
	owner_poll_count = 0;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT_EQ(owner_poll_count, 1);
	target = observed_target;
	releases = g_s6_count;
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(g_s6_count, releases);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT_EQ(observed_target.request_id, target.request_id);
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	/* Still no authority: this hold was already requested to retire. */
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(g_released_request.holder.cluster_epoch, target.cluster_epoch);
	UT_ASSERT_EQ(g_released_request.request_id, target.request_id);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_install_cut_drift_cannot_grant_authority)
{
	drift_during_install = true;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	drift_during_install = false;
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_release_in_flight_cannot_rebind)
{
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	observe_releasing = true;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	observe_releasing = false;
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_cut_before_route_does_not_strand_old_target)
{
	uint64 enumerated = 0;
	uint64 first_id;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	owner_epoch++;
	owner_generation++;
	no_route = true;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	first_id = observed_target.request_id;
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	owner_epoch++;
	owner_generation++;
	no_route = false;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated)); /* Retire obsolete target. */
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(observed_target.cluster_epoch, owner_epoch);
	UT_ASSERT(observed_target.request_id > first_id);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_s5_error_keeps_exact_cleanup_owner)
{
	volatile bool caught = false;

	throw_during_install = true;
	PG_TRY();
	{
		(void)cluster_cf_lock(ExclusiveLock);
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	throw_during_install = false;
	UT_ASSERT(caught);
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_failed_s5_is_not_reconstruction_proof)
{
	uint64 epoch = owner_epoch;
	uint64 generation = owner_generation;
	uint64 enumerated = 0;
	unsigned polls;

	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	polls = owner_poll_count;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT_EQ(owner_poll_count, polls);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	/* Fixture teardown restores the original cut to exercise exact S6;
	 * this is not evidence of post-reconfiguration uncertain cleanup. */
	owner_epoch = epoch;
	owner_generation = generation;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_same_epoch_cut_keeps_exact_exchange)
{
	uint64 enumerated = 0;
	uint64 request_id;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_PENDING;
	owner_wait_deleted = 0;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	request_id = observed_target.request_id;
	owner_routing_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CUT_CHANGED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(owner_wait_deleted, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	/* Only the exact eventual completion permits release of that identity. */
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(observed_target.request_id, request_id);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(g_released_request.request_id, request_id);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	owner_routing_generation = 1;
}

/* Break caught: S4 currently publishes from a stack request before the
 * stable owner exists, so ERROR loses both the census and its exact identity.
 * S4/S6 here are controlled; remote cancellation is a separate composition. */
UT_TEST(test_private_cf_acquisition_is_owned_before_return)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	observe_acquiring = true;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	observe_acquiring = false;
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(test_private_cf_acquire_error_retains_exact_cleanup)
{
	for (int i = 0; i < 2; i++) {
		LOCKMODE mode = i ? ExclusiveLock : ShareLock;
		volatile bool caught = false;

		g_next_request_id = 701 + i;
		throw_during_acquire = true;
		PG_TRY();
		{
			(void)cluster_cf_lock(mode);
			UT_ASSERT(false);
		}
		PG_CATCH();
		{
			caught = true;
			FlushErrorState();
		}
		PG_END_TRY();
		throw_during_acquire = false;
		UT_ASSERT(caught);
		UT_ASSERT(cluster_cf_held(mode));
		UT_ASSERT(!cluster_cf_held_is_usable(mode));
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
		UT_ASSERT(
			memcmp(&g_released_request, &observed_acquire_request, sizeof(observed_acquire_request))
			== 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	}
	g_next_request_id = 7;
}

UT_TEST(test_private_cf_published_refusal_retains_cleanup)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	fail_after_publication = true;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	fail_after_publication = false;
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(
		memcmp(&g_released_request, &observed_acquire_request, sizeof(observed_acquire_request))
		== 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

UT_TEST(test_private_cf_prepublication_error_has_no_phantom_owner)
{
	volatile bool caught = false;
	int releases = g_s6_count;

	throw_before_publication = true;
	PG_TRY();
	{
		(void)cluster_cf_lock(ShareLock);
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	throw_before_publication = false;
	UT_ASSERT(caught);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT_EQ(g_s6_count, releases);
}

UT_TEST(test_private_cf_without_pgproc_cannot_publish_cluster_request)
{
	int acquires = g_seven_count;

	MyProc = NULL;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, acquires);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(g_seven_count, acquires);
	MyProc = &owner_proc;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(shared_cf_failed_s5_retires_original_identity_without_redeclare)
{
	ClusterControlRequestView view = { 0 };
	uint32 cursor = 0;
	uint64 enumerated;
	unsigned polls = owner_poll_count;
	uint64 original_epoch = owner_epoch;

	cluster_shared_config = true;
	MyProcPid = 4002;
	cluster_control_request_shmem_init();
	retire_driver = cluster_control_request_driver_start();
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_control_request_next(&cursor, &view));
	UT_ASSERT_EQ(view.message.key.holder.cluster_epoch, original_epoch);
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	owner_epoch++;
	owner_generation++;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(owner_poll_count, polls);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	cursor = 0;
	UT_ASSERT(!cluster_control_request_next(&cursor, &view));
	cluster_shared_config = false;
}

UT_TEST(shared_cf_redeclare_keeps_both_attempts_until_old_terminal)
{
	ClusterControlRequestView view;
	uint32 cursor = 0;
	unsigned records = 0;
	uint64 enumerated;

	cluster_shared_config = true;
	g_next_request_id++;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	while (cluster_control_request_next(&cursor, &view))
		records++;
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	cluster_shared_config = false;
}

UT_TEST(shared_cf_pending_redeclare_is_retired_even_at_same_epoch)
{
	ClusterControlRequestView view;
	uint32 cursor = 0;
	unsigned records = 0, abandoned = 0;
	uint64 enumerated;
	unsigned polls;

	cluster_shared_config = true;
	g_next_request_id++;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_PENDING;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	owner_routing_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CUT_CHANGED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	polls = owner_poll_count;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(owner_poll_count, polls); /* No new target before terminal. */
	while (cluster_control_request_next(&cursor, &view)) {
		records++;
		if (view.state == CLUSTER_CONTROL_REQUEST_ABANDONED)
			abandoned++;
	}
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(abandoned, 1);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	owner_routing_generation++; /* The old exact ACK is no longer this cut. */
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	/* The real LMON reclaims terminal records at a changed cut before ACK. */
	cursor = 0;
	while (cluster_control_request_next(&cursor, &view)) {
		ClusterControlRequestCut current;
		ClusterControlRetireMessage message;

		control_cut(&current);
		UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &current, &message));
	}
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(owner_poll_count, polls);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	cluster_shared_config = false;
}

UT_TEST(copyable_control_guard_tracks_stable_rebound_owner)
{
	ClusterLockAcquireRequest request = { 0 }, copy;
	uint64 enumerated, cookie, old_request;

	cluster_shared_config = true;
	g_next_request_id++;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	request.resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	request.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	request.resid.field1 = 2;
	request.lockmode = ShareLock;
	request.op = CLUSTER_LOCK_OP_REQUEST;
	UT_ASSERT_EQ(cluster_lock_owner_request_acquire(&request, NULL, NULL),
				 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	copy = request;
	cookie = copy.control_owner_id;
	old_request = copy.request_id;
	memset(&request, 0, sizeof(request)); /* The acquisition stack goes away. */
	UT_ASSERT(cookie != 0);
	UT_ASSERT(cluster_lock_owner_request_usable(&copy));
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(cluster_lock_owner_request_refresh(&copy));
	UT_ASSERT_EQ(copy.control_owner_id, cookie);
	UT_ASSERT(copy.request_id != old_request);
	UT_ASSERT_EQ(copy.holder.cluster_epoch, owner_epoch);
	UT_ASSERT_EQ(cluster_lock_owner_request_release(&copy), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(!cluster_lock_owner_request_usable(&copy));
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(cluster_lock_owner_request_release(&copy), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT(!cluster_lock_owner_request_usable(&copy));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	cluster_shared_config = false;
}

UT_TEST(auxiliary_poll_retires_without_sleeping_or_dropping_live_cf)
{
	BackendType saved_type = MyBackendType;
	unsigned waits = owner_wait_calls;

	cluster_shared_config = true;
	MyBackendType = B_LMS;
	g_next_request_id++;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	cluster_lock_owners_service_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	MyBackendType = saved_type;
	cluster_shared_config = false;
}

#ifndef PGRAC_CONTROL_CF_EMBEDDED
UT_TEST(shared_cf_service_acquisition_keeps_one_identity_across_ticks)
{
	unsigned prepares = g_seven_count, polls = cf_acquire_polls;
	uint64 waits = owner_wait_calls;
	uint64 cookie;

	cluster_shared_config = true;
	MyBackendType = B_LMON;
	g_next_request_id++;
	g_seven_result = g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	cf_poll_result = CLUSTER_GES_ACQUIRE_PENDING;
	UT_ASSERT(!cluster_cf_lock_poll(ShareLock));
	UT_ASSERT(cluster_cf_acquire_pending(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(!cluster_cf_lock_poll(ShareLock));
	UT_ASSERT_EQ(g_seven_count, prepares + 1);
	UT_ASSERT_EQ(cf_acquire_polls, polls + 2);
	cf_poll_result = CLUSTER_GES_ACQUIRE_GRANTED;
	UT_ASSERT(cluster_cf_lock_poll(ShareLock));
	UT_ASSERT(!cluster_cf_acquire_pending(ShareLock));
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(g_seven_count, prepares + 1);
	cookie = cluster_cf_owner_cookie(ShareLock);
	UT_ASSERT(cookie != 0);
	UT_ASSERT(!cluster_cf_release_completed(ShareLock, cookie));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(cluster_cf_release_completed(ShareLock, cookie));
	UT_ASSERT(!cluster_cf_release_completed(ShareLock, cookie + 1));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(shared_cf_service_cut_change_retires_without_producing_holder)
{
	uint64 enumerated;
	unsigned redeclares = owner_poll_count;

	cluster_shared_config = true;
	MyBackendType = B_LMON;
	g_next_request_id++;
	cf_poll_result = CLUSTER_GES_ACQUIRE_PENDING;
	UT_ASSERT(!cluster_cf_lock_poll(ShareLock));
	owner_epoch++;
	owner_generation++;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(!cluster_cf_acquire_pending(ShareLock));
	UT_ASSERT_EQ(owner_poll_count, redeclares);
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(cf_local_caller_cannot_steal_pending_or_acquired_request)
{
	static const char configuration, observer;
	unsigned polls, prepares, waits = owner_wait_calls;
	uint64 cookie;

	cluster_shared_config = true;
	MyBackendType = B_LMON;
	g_next_request_id++;
	cf_poll_result = CLUSTER_GES_ACQUIRE_PENDING;
	UT_ASSERT(!cluster_cf_lock_poll_owned(ShareLock, &configuration));
	UT_ASSERT(cluster_cf_acquire_pending_owned(ShareLock, &configuration));
	UT_ASSERT(!cluster_cf_acquire_pending_owned(ShareLock, &observer));
	UT_ASSERT(!cluster_cf_acquire_pending(ShareLock));
	UT_ASSERT(cluster_cf_held_by(ShareLock, &configuration));
	UT_ASSERT(!cluster_cf_held_by(ShareLock, &observer));
	polls = cf_acquire_polls;
	prepares = g_seven_count;
	UT_ASSERT(!cluster_cf_lock_poll_owned(ShareLock, &observer));
	UT_ASSERT(!cluster_cf_lock_poll(ShareLock));
	UT_ASSERT_EQ(cf_acquire_polls, polls);
	UT_ASSERT_EQ(g_seven_count, prepares);
	cf_poll_result = CLUSTER_GES_ACQUIRE_GRANTED;
	UT_ASSERT(cluster_cf_lock_poll_owned(ShareLock, &configuration));
	cookie = cluster_cf_owner_cookie(ShareLock);
	UT_ASSERT(cookie != 0);
	UT_ASSERT_EQ(cluster_cf_unlock_owned(ShareLock, &observer), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT_EQ(cluster_cf_unlock_owned(ShareLock, &configuration),
				 CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_by(ShareLock, &configuration));
	UT_ASSERT(cluster_cf_release_completed(ShareLock, cookie));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(cf_tagged_caller_cannot_take_legacy_request_and_cut_retires_owner)
{
	static const char configuration;
	uint64 enumerated;
	unsigned polls;

	cluster_shared_config = true;
	MyBackendType = B_LMON;
	g_next_request_id++;
	cf_poll_result = CLUSTER_GES_ACQUIRE_PENDING;
	UT_ASSERT(!cluster_cf_lock_poll(ShareLock));
	polls = cf_acquire_polls;
	UT_ASSERT(!cluster_cf_lock_poll_owned(ShareLock, &configuration));
	UT_ASSERT(!cluster_cf_held_by(ShareLock, &configuration));
	UT_ASSERT(!cluster_cf_acquire_pending_owned(ShareLock, &configuration));
	UT_ASSERT_EQ(cluster_cf_unlock_owned(ShareLock, &configuration),
				 CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(cluster_cf_acquire_pending(ShareLock));
	UT_ASSERT_EQ(cf_acquire_polls, polls);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	g_next_request_id++;
	UT_ASSERT(!cluster_cf_lock_poll_owned(ShareLock, &configuration));
	owner_epoch++;
	owner_generation++;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(!cluster_cf_acquire_pending_owned(ShareLock, &configuration));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_by(ShareLock, &configuration));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

int
main(void)
{
	owner_proc.pgprocno = 42;
	pg_atomic_init_u32(&owner_proc.cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked_epoch, 0);
	UT_PLAN(40);
	UT_RUN(test_cf_resid_encode);
	UT_RUN(test_lock_grant_then_release);
	UT_RUN(test_held_and_write_permitted);
	UT_RUN(test_owner_eor_handoff_gates_and_lifecycle);
	UT_RUN(test_owner_eor_disabled_seed_uses_exact_declared_node);
	UT_RUN(test_owner_eor_abort_retains_active_and_blocks_retry);
	UT_RUN(test_lock_native_no_release);
	UT_RUN(test_confirmed_release_requires_clusterwide_s6_success);
	UT_RUN(test_void_release_retains_exact_request_without_authority);
	UT_RUN(test_release_interrupt_keeps_cleanup_but_no_authority);
	UT_RUN(test_reacquire_drains_old_identity_before_creating_new);
	UT_RUN(test_native_hold_never_becomes_clusterwide_authority);
	UT_RUN(test_lock_failclosed_timeout);
	UT_RUN(test_lock_s5_fail);
	UT_RUN(test_lock_notavail);
	UT_RUN(test_lock_timeout_and_wait_event);
	UT_RUN(test_stop_cf_original_holds_and_confirmed_retirement);
	UT_RUN(test_stop_cf_separates_old_skip_from_post_checkpoint_permission);
	UT_RUN(test_private_cf_is_enumerated_before_s5_and_until_release);
	UT_RUN(test_private_cf_pending_redeclare_owns_release_identity);
	UT_RUN(test_private_cf_install_cut_drift_cannot_grant_authority);
	UT_RUN(test_private_cf_release_in_flight_cannot_rebind);
	UT_RUN(test_private_cf_cut_before_route_does_not_strand_old_target);
	UT_RUN(test_private_cf_s5_error_keeps_exact_cleanup_owner);
	UT_RUN(test_private_cf_failed_s5_is_not_reconstruction_proof);
	UT_RUN(test_private_cf_same_epoch_cut_keeps_exact_exchange);
	UT_RUN(test_private_cf_acquisition_is_owned_before_return);
	UT_RUN(test_private_cf_acquire_error_retains_exact_cleanup);
	UT_RUN(test_private_cf_published_refusal_retains_cleanup);
	UT_RUN(test_private_cf_prepublication_error_has_no_phantom_owner);
	UT_RUN(test_private_cf_without_pgproc_cannot_publish_cluster_request);
	UT_RUN(shared_cf_failed_s5_retires_original_identity_without_redeclare);
	UT_RUN(shared_cf_redeclare_keeps_both_attempts_until_old_terminal);
	UT_RUN(shared_cf_pending_redeclare_is_retired_even_at_same_epoch);
	UT_RUN(copyable_control_guard_tracks_stable_rebound_owner);
	UT_RUN(auxiliary_poll_retires_without_sleeping_or_dropping_live_cf);
	UT_RUN(shared_cf_service_acquisition_keeps_one_identity_across_ticks);
	UT_RUN(shared_cf_service_cut_change_retires_without_producing_holder);
	UT_RUN(cf_local_caller_cannot_steal_pending_or_acquired_request);
	UT_RUN(cf_tagged_caller_cannot_take_legacy_request_and_cut_retires_owner);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
#endif
