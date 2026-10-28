/* Actual CF failure/retirement producer and native Startup entry.
 * S4, ROOT I/O, scheduling and authority freshness are fixture boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_wal_cut.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_write_fence.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/resowner.h"
#include "../../backend/cluster/cluster_control_root_private.h"

/* Reuse leaf fixtures without replacing the CF owner or shared registry. */
bool entry_inherited_errstart(int elevel, const char *domain);
void entry_inherited_errfinish(const char *file, int line, const char *function);
int entry_inherited_errmsg(const char *format, ...) pg_attribute_printf(1, 2);
int entry_inherited_errdetail(const char *format, ...) pg_attribute_printf(1, 2);
int entry_inherited_wait(Latch *latch, int events, long timeout, uint32 event);
void entry_inherited_reset(Latch *latch);
bool entry_inherited_lock(LWLock *lock, LWLockMode mode);
#undef UT_TEST
#define UT_TEST(name) static void pg_attribute_unused() name(void)
#define PGRAC_CONTROL_CF_EMBEDDED
#define errstart entry_inherited_errstart
#define errfinish entry_inherited_errfinish
#define errmsg entry_inherited_errmsg
#define errdetail entry_inherited_errdetail
#define WaitLatch entry_inherited_wait
#define ResetLatch entry_inherited_reset
#define WakeupRecovery entry_inherited_wakeup
#define LWLockAcquire entry_inherited_lock
#include "test_cluster_cf_enqueue.c"
#undef LWLockAcquire
#undef WakeupRecovery
#undef ResetLatch
#undef WaitLatch
#undef errdetail
#undef errmsg
#undef errfinish
#undef errstart
#undef UT_TEST
#define UT_TEST(name) static void name(void)

/* Include exact production storage too: fault injection never adds a product
 * setter or a shared test flag. */
#include "../../backend/cluster/cluster_control_request.c"
void entry_actual_owner_service_poll(void);
#define cluster_lock_owners_service_poll entry_actual_owner_service_poll
#include "../../backend/cluster/cluster_lock_owner.c"
#undef cluster_lock_owners_service_poll
#include "../../backend/cluster/cluster_cf_enqueue.c"

bool cluster_enabled = true;
bool enableFsync = true;
bool InRecovery, ArchiveRecoveryRequested;
volatile sig_atomic_t ShutdownRequestPending;
static ControlFileData control;
static ControlFileData *ControlFile = &control;
static LWLockPadded entry_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = entry_locks;
static bool entry_native_lock, entry_authority;
static ClusterWalStartupImage clusterStartupWriter, entry_selected;
static bool clusterStartupWriterSelected, clusterStartupWriterBound, clusterStartupWriterInstalled;
static EndOfWalRecoveryInfo input;
static unsigned entry_waits, entry_resets, entry_ack_at, entry_root_calls;
static unsigned entry_config_calls;
static unsigned entry_registry_reads, entry_select_calls;
static bool entry_ack_on_reset, entry_lost_latch;
static bool entry_ack_on_service, entry_service_consumed;
static int entry_level, entry_code;
static XLogRecPtr entry_output;
static void (*entry_on_wait)(void);
static bool entry_clean_consumer;
static unsigned entry_read_timeouts, entry_read_result;
static uint64 entry_read_cookie;
static bool claim_teardown;
static void (*claim_on_reset)(void);
static bool cold_consumer;
static unsigned cold_release_waits, cold_reads;
static uint64 entry_timeout(LOCKMODE mode);

void
cluster_lock_owners_service_poll(void)
{
	if (entry_ack_on_service) {
		entry_ack_on_service = false;
		UT_ASSERT(acknowledge_retirements());
		entry_actual_owner_service_poll();
		UT_ASSERT(cluster_cf_held(ShareLock));
		UT_ASSERT_EQ(cf_hold_s.owner.attempt.generation, 0);
		UT_ASSERT(!cluster_cf_release_completed(ShareLock, cluster_cf_owner_cookie(ShareLock)));
		entry_service_consumed = true;
		return;
	}
	entry_actual_owner_service_poll();
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(!entry_native_lock && CritSectionCount == 0);
	++entry_registry_reads;
	return entry_inherited_lock(lock, mode);
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	entry_level = level;
	return level >= ERROR;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	UT_ASSERT(control_locked == NULL);
	if (entry_level >= ERROR)
		pg_re_throw();
}
int
errcode(int code)
{
	entry_code = code;
	return 0;
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == ControlFileLock);
	return entry_native_lock;
}
void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
	++entry_resets;
	if (claim_on_reset != NULL)
		claim_on_reset();
	if (entry_ack_on_reset) {
		entry_ack_on_reset = false;
		UT_ASSERT(acknowledge_retirements());
	}
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	if (claim_teardown)
		return entry_inherited_wait(latch, events, timeout, event);
	if (cold_consumer && event != WAIT_EVENT_CLUSTER_STARTUP_PHASE_3) {
		/* Original blocking release must reach its own one-second limit;
		 * this boundary deliberately withholds its ACK for the RED. */
		UT_ASSERT_EQ(timeout, 100);
		owner_clock += INT64CONST(1000000);
		++cold_release_waits;
		if (g_seven_result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			UT_ASSERT(acknowledge_retirements());
		return WL_TIMEOUT;
	}
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
	UT_ASSERT(timeout == 20 || (cold_consumer && timeout == 100));
	UT_ASSERT_EQ(event, WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
	UT_ASSERT(!entry_native_lock && CritSectionCount == 0 && control_locked == NULL);
	if (entry_clean_consumer)
		UT_ASSERT_EQ(entry_root_calls, 1);
	else
		UT_ASSERT_EQ(entry_root_calls | entry_config_calls | entry_select_calls, 0);
	UT_ASSERT(entry_resets > entry_waits);
	if (++entry_waits > 8)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("fixture exceeded eight wakes")));
	owner_clock += INT64CONST(1000000); /* Time alone must not end retirement. */
	if (entry_on_wait != NULL)
		entry_on_wait();
	if (entry_ack_at != 0 && entry_waits == entry_ack_at)
		UT_ASSERT(acknowledge_retirements());
	return !entry_lost_latch && (entry_waits & 1) ? WL_LATCH_SET : WL_TIMEOUT;
}

bool
cluster_recovery_transport_components_current(void)
{
	return entry_authority;
}
static uint64
cluster_checkpoint_wait_budget_remaining_us(uint64 started pg_attribute_unused())
{
	UT_ASSERT(false);
	return 0;
}
static void
ClusterStartupWriterSelect(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(entry_root_calls, 0);
	++entry_select_calls;
	clusterStartupWriter = entry_selected;
	clusterStartupWriterSelected = true;
}
static void
ClusterStartupConfigurationRequire(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++entry_config_calls;
	if (!entry_authority)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("fixture authority is no longer current")));
}
static void
entry_assert_root_context(const ClusterControlRootIdentity *identity, const uint8 uuid[16])
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!entry_native_lock && CritSectionCount == 0);
	UT_ASSERT_EQ(memcmp(identity, &entry_selected.claim.identity, sizeof(*identity)), 0);
	UT_ASSERT_EQ(memcmp(uuid, entry_selected.operation_uuid, 16), 0);
	++entry_root_calls;
}
ClusterControlRootResult
cluster_control_root_v3_startup_read_writer(const ClusterControlRootIdentity *identity,
											const uint8 uuid[16], ClusterWalStartupImage *out)
{
	entry_assert_root_context(identity, uuid);
	if (entry_read_timeouts != 0) {
		--entry_read_timeouts;
		entry_read_cookie = entry_timeout(ExclusiveLock);
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	}
	if (entry_read_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return entry_read_result;
	*out = entry_selected;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_control_root_v3_startup_route_writer(const ClusterControlRootIdentity *identity,
											 const uint8 uuid[16], ClusterWalStartupImage *out)
{
	entry_assert_root_context(identity, uuid);
	*out = entry_selected;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_wal_writer_startup_prepare(const ClusterControlRootIdentity *identity, const uint8 uuid[16],
								   XLogRecPtr *first)
{
	entry_assert_root_context(identity, uuid);
	*first = entry_selected.first_segment_lsn;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_wal_writer_startup_binding_present(void)
{
	return false;
}

#include "test_cluster_startup_cf_entry.inc"

static void
entry_actual_callsite(void)
{
	EndOfWalRecoveryInfo *endOfRecoveryInfo = &input;
	XLogRecPtr EndOfLog = InvalidXLogRecPtr;

#include "test_cluster_startup_cf_callsite.inc"
	entry_output = EndOfLog;
}
static bool
entry_call(void)
{
	volatile bool completed = false;

	PG_TRY();
	{
		entry_actual_callsite();
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

static void
entry_fixture(bool recovery)
{
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	cluster_shared_config = cluster_enabled = cluster_controlfile_shared_authority = true;
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	owner_clock_step = 1;
	cluster_cf_enqueue_timeout_ms = 1000;
	g_timeout_detail.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	fail_after_publication = true;
	entry_waits = entry_resets = entry_root_calls = entry_config_calls = 0;
	entry_registry_reads = entry_select_calls = 0;
	entry_ack_on_reset = entry_lost_latch = false;
	entry_ack_on_service = entry_service_consumed = false;
	entry_level = entry_code = 0;
	entry_output = InvalidXLogRecPtr;
	entry_native_lock = false;
	entry_authority = true;
	entry_ack_at = 3;
	entry_on_wait = NULL;
	entry_clean_consumer = false;
	entry_read_timeouts = 0;
	entry_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	entry_read_cookie = 0;
	InRecovery = recovery;
	ArchiveRecoveryRequested = false;
	ShutdownRequestPending = false;
	CritSectionCount = 0;
	clusterStartupWriterSelected = true;
	clusterStartupWriterBound = clusterStartupWriterInstalled = false;
	memset(&control, 0, sizeof(control));
	control.state = recovery ? DB_IN_CRASH_RECOVERY : DB_SHUTDOWNED;
	control.checkPoint = 100;
	control.checkPointCopy.ThisTimeLineID = 1;
	memset(&entry_selected, 0, sizeof(entry_selected));
	entry_selected.formation_epoch = owner_epoch;
	entry_selected.input_kind
		= recovery ? CLUSTER_WAL_STARTUP_RECOVERED : CLUSTER_WAL_STARTUP_CLEAN;
	entry_selected.timeline = entry_selected.input_timeline = 1;
	entry_selected.first_segment_lsn = 16777216;
	entry_selected.input_record_start = 100;
	entry_selected.input_record_end = 220;
	entry_selected.predecessor.snapshot.checkpoint_lower_lsn = 80;
	entry_selected.predecessor.snapshot.tail_last_record_lsn = recovery ? 300 : 100;
	entry_selected.sealed_input_end = recovery ? 340 : 220;
	entry_selected.operation_uuid[0] = 5;
	entry_selected.claim.identity.system_identifier = 1234;
	entry_selected.claim.identity.storage_uuid[0] = 31;
	entry_selected.claim.identity.authority_uuid[0] = 41;
	entry_selected.claim.identity.origin_thread_id = 1;
	entry_selected.claim.identity.origin_owner_incarnation = 99;
	clusterStartupWriter = entry_selected;
	memset(&input, 0, sizeof(input));
	input.endOfLog = entry_selected.sealed_input_end;
	input.lastRec = entry_selected.predecessor.snapshot.tail_last_record_lsn;
	input.lastRecTLI = input.endOfLogTLI = 1;
}
static uint64
entry_timeout(LOCKMODE mode)
{
	ClusterControlRequestView view;
	ClusterCfAttemptObservation observed;
	uint64 before = cluster_cf_attempt_sequence();

	g_next_request_id++;
	UT_ASSERT(!cluster_cf_lock(mode));
	UT_ASSERT_EQ(g_last_timeout_ms, 1000);
	UT_ASSERT(cluster_cf_last_attempt_since(before, &observed));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&observed));
	UT_ASSERT(cluster_cf_held(mode) && !cluster_cf_held_is_usable(mode));
	UT_ASSERT_EQ(cf_slot(mode)->owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	UT_ASSERT(cluster_control_request_snapshot(&cf_slot(mode)->owner.attempt, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT_EQ(view.message.key.holder.request_id, g_next_request_id);
	UT_ASSERT_EQ(view.owner.incarnation, control_creator.incarnation);
	UT_ASSERT_EQ(view.owner.pid, MyProcPid);
	return cluster_cf_owner_cookie(mode);
}
static void
entry_cleanup(LOCKMODE mode, uint64 cookie)
{
	if (cluster_cf_held(mode)) {
		(void)acknowledge_retirements();
		cluster_cf_retirement_poll();
	}
	UT_ASSERT(!cluster_cf_held(mode));
	UT_ASSERT(cluster_cf_release_completed(mode, cookie));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(PG_exception_stack == NULL && control_locked == NULL);
}

/* Break caught: a real failed CF attempt reaches Begin's retained-slot
 * guard before the existing retirement producer can publish completion. */
UT_TEST(startup_cf_timeout_waits_for_exact_completion_before_begin)
{
	for (int recovery = 0; recovery < 2; recovery++)
		for (int x = 0; x < 2; x++) {
			LOCKMODE mode = x ? ExclusiveLock : ShareLock;
			uint64 cookie;

			entry_fixture(recovery);
			cookie = entry_timeout(mode);
			UT_ASSERT(entry_call());
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(entry_output, 16777216);
			UT_ASSERT_EQ(entry_root_calls, 3);
			UT_ASSERT(clusterStartupWriterBound);
			UT_ASSERT_EQ(InRecovery, recovery);
			entry_cleanup(mode, cookie);
		}
}

static void
entry_change_input(void)
{
	input.lastRec++;
}
UT_TEST(startup_cf_completion_does_not_preserve_changed_native_input)
{
	uint64 cookie;

	entry_fixture(false);
	cookie = entry_timeout(ShareLock);
	entry_on_wait = entry_change_input;
	UT_ASSERT(!entry_call());
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(entry_root_calls | entry_output, 0);
	entry_on_wait = NULL;
	entry_cleanup(ShareLock, cookie);
}

UT_TEST(startup_cf_unsafe_native_context_never_takes_a_registry_lock)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		uint64 cookie;

		entry_fixture(false);
		cookie = entry_timeout(ShareLock);
		switch (fault) {
		case 0:
			entry_native_lock = true;
			break;
		case 1:
			CritSectionCount = 1;
			break;
		case 2:
			MyBackendType = B_LMON;
			break;
		case 3:
			cluster_enabled = false;
			break;
		case 4:
			cluster_controlfile_shared_authority = false;
			break;
		case 5:
			clusterStartupWriterBound = true;
			break;
		case 6:
			clusterStartupWriterInstalled = true;
			break;
		case 7:
			ArchiveRecoveryRequested = true;
			break;
		case 8:
			input.standby_signal_file_found = true;
			break;
		case 9:
			input.abortedRecPtr = 1;
			break;
		case 10:
			ShutdownRequestPending = true;
			break;
		}
		entry_registry_reads = 0;
		UT_ASSERT(!entry_call());
		UT_ASSERT_EQ(entry_registry_reads, 0);
		UT_ASSERT_EQ(entry_waits | entry_root_calls | entry_output, 0);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		entry_native_lock = false;
		CritSectionCount = 0;
		MyBackendType = B_STARTUP;
		cluster_enabled = cluster_controlfile_shared_authority = true;
		ShutdownRequestPending = false;
		entry_cleanup(ShareLock, cookie);
	}
}

UT_TEST(startup_cf_exact_ack_survives_reset_race_or_lost_latch)
{
	for (int reset_race = 0; reset_race < 2; reset_race++) {
		uint64 cookie;

		entry_fixture(false);
		cookie = entry_timeout(ShareLock);
		entry_ack_on_reset = reset_race;
		entry_lost_latch = true;
		UT_ASSERT(entry_call());
		UT_ASSERT_EQ(entry_waits, reset_race ? 0 : 3);
		UT_ASSERT_EQ(entry_root_calls, 3);
		entry_cleanup(ShareLock, cookie);
	}
}

UT_TEST(startup_cf_both_original_modes_retire_before_any_root_operation)
{
	uint64 s, x;

	entry_fixture(false);
	s = entry_timeout(ShareLock);
	x = entry_timeout(ExclusiveLock);
	UT_ASSERT(s != x);
	UT_ASSERT(entry_call());
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(entry_root_calls, 3);
	entry_cleanup(ShareLock, s);
	entry_cleanup(ExclusiveLock, x);
}

UT_TEST(startup_cf_crash_selection_remains_after_exact_retirement)
{
	uint64 cookie;

	entry_fixture(true);
	clusterStartupWriterSelected = false;
	cookie = entry_timeout(ShareLock);
	UT_ASSERT(entry_call());
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(entry_select_calls, 1);
	UT_ASSERT_EQ(entry_root_calls, 3);
	UT_ASSERT(InRecovery && control.state == DB_IN_CRASH_RECOVERY);
	entry_cleanup(ShareLock, cookie);
}

UT_TEST(startup_cf_rejects_foreign_or_unproven_cleanup_without_polling)
{
	for (unsigned fault = 0; fault < 23; fault++) {
		CfHoldState saved;
		ClusterControlRequestView record;
		uint64 cookie;
		uint32 slot;

		entry_fixture(false);
		cookie = entry_timeout(ShareLock);
		saved = cf_hold_s;
		slot = saved.owner.attempt.slot;
		record = control_requests->records[slot];
		switch (fault) {
		case 0:
			cf_hold_s.caller = &entry_authority;
			break;
		case 1:
			cf_hold_s.release_pending = false;
			break;
		case 2:
			cf_hold_s.coordinated = false;
			break;
		case 3:
			cf_hold_s.owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
			break;
		case 4:
			cf_hold_s.owner.shared = false;
			break;
		case 5:
			cf_hold_s.owner.request.control_owner_id = 0;
			break;
		case 6:
			cf_hold_s.owner.attempt.generation = 0;
			break;
		case 7:
			cf_hold_s.owner.request.holder.node_id++;
			break;
		case 8:
			cf_hold_s.owner.request.holder.procno++;
			break;
		case 9:
			cf_hold_s.owner.request.holder.cluster_epoch++;
			break;
		case 10:
			cf_hold_s.owner.request.request_id++;
			break;
		case 11:
			cf_hold_s.owner.request.holder.request_id++;
			break;
		case 12:
			cf_hold_s.owner.request.resid.field1++;
			break;
		case 13:
			cf_hold_s.owner.request.lockmode = ExclusiveLock;
			break;
		case 14:
			control_requests->records[slot].owner.pid++;
			break;
		case 15:
			control_requests->records[slot].owner.procno++;
			break;
		case 16:
			control_requests->records[slot].owner_exited = true;
			break;
		case 17:
			control_requests->records[slot].state = CLUSTER_CONTROL_REQUEST_INVALID;
			break;
		case 18:
			control_requests->records[slot].mode = ExclusiveLock;
			break;
		case 19:
			control_requests->records[slot].message.key.holder.request_id++;
			break;
		case 20:
			cf_hold_s.owner.secondary.generation = 1;
			break;
		case 21:
			cf_hold_s.owner.cooperative_acquire = true;
			break;
		case 22:
			cf_hold_s.release_pending = false;
			cf_hold_s.owner.state = CLUSTER_LOCK_OWNER_HELD;
			control_requests->records[slot].state = CLUSTER_CONTROL_REQUEST_HELD;
			UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
			break;
		}
		UT_ASSERT(!entry_call());
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(entry_waits | entry_root_calls | entry_output, 0);
		UT_ASSERT(cluster_cf_held(ShareLock));
		/* Restore only test-injected corruption before real teardown. */
		cf_hold_s = saved;
		control_requests->records[slot] = record;
		entry_cleanup(ShareLock, cookie);
	}
}

static unsigned entry_drift;
static void
entry_change_context(void)
{
	switch (entry_drift) {
	case 0:
		input.lastRec++;
		break;
	case 1:
		input.endOfLog++;
		break;
	case 2:
		input.lastRecTLI++;
		break;
	case 3:
		clusterStartupWriter.operation_uuid[0]++;
		break;
	case 4:
		clusterStartupWriter.claim.identity.origin_owner_incarnation++;
		break;
	case 5:
		owner_epoch++;
		break;
	case 6:
		owner_generation++;
		break;
	case 7:
		entry_authority = false;
		break;
	case 8:
		control.checkPoint++;
		break;
	case 9:
		control.state = DB_SHUTDOWNING;
		break;
	case 10:
		clusterStartupWriterSelected = false;
		break;
	case 11:
		clusterStartupWriterBound = true;
		break;
	case 12:
		clusterStartupWriterInstalled = true;
		break;
	case 13:
		InRecovery = !InRecovery;
		break;
	case 14:
		ShutdownRequestPending = true;
		break;
	case 15:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 16:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 17:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 18:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	}
}
UT_TEST(startup_cf_wait_rechecks_native_owner_and_exact_cleanup_identity)
{
	for (entry_drift = 0; entry_drift < 19; entry_drift++) {
		CfHoldState saved;
		ClusterControlRequestView record;
		uint64 cookie, old_epoch = owner_epoch, old_generation = owner_generation;
		uint32 slot;

		entry_fixture(false);
		cookie = entry_timeout(ShareLock);
		saved = cf_hold_s;
		slot = saved.owner.attempt.slot;
		record = control_requests->records[slot];
		entry_on_wait = entry_change_context;
		UT_ASSERT(!entry_call());
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(entry_root_calls | entry_output, 0);
		UT_ASSERT(cluster_cf_held(ShareLock));
		entry_on_wait = NULL;
		owner_epoch = old_epoch;
		owner_generation = old_generation;
		ShutdownRequestPending = false;
		cf_hold_s = saved;
		control_requests->records[slot] = record;
		entry_cleanup(ShareLock, cookie);
	}
}

static unsigned entry_receipt_fault;
static void
entry_remove_receipt(void)
{
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	cf_retired_s = entry_receipt_fault == 0 ? 0 : cf_retired_s + 1;
}
UT_TEST(startup_cf_slot_disappearance_without_exact_receipt_is_not_completion)
{
	for (entry_receipt_fault = 0; entry_receipt_fault < 2; entry_receipt_fault++) {
		uint64 cookie;

		entry_fixture(false);
		cookie = entry_timeout(ShareLock);
		entry_on_wait = entry_remove_receipt;
		entry_ack_at = 0;
		UT_ASSERT(!entry_call());
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(entry_root_calls | entry_output, 0);
		cf_retired_s = cookie; /* Restore only the deliberately damaged receipt. */
		entry_cleanup(ShareLock, cookie);
	}
}

static void
entry_hard_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("fixture cleanup I/O error")));
}
UT_TEST(startup_cf_hard_error_keeps_original_cleanup_and_error)
{
	uint64 cookie;

	entry_fixture(false);
	cookie = entry_timeout(ExclusiveLock);
	entry_on_wait = entry_hard_error;
	UT_ASSERT(!entry_call());
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(entry_level, ERROR);
	UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
	UT_ASSERT_EQ(entry_root_calls | entry_output, 0);
	entry_on_wait = NULL;
	entry_cleanup(ExclusiveLock, cookie);
}

static void
entry_shutdown(void)
{
	StartupProcShutdownHandler(SIGTERM);
}
static void
entry_owner_exit(void)
{
	UT_ASSERT(recorded_exit != NULL);
	recorded_exit(1, (Datum)0);
}
UT_TEST(startup_cf_original_startup_termination_retains_shared_responsibility)
{
	uint64 cookie;
	volatile bool exited = false;

	entry_fixture(false);
	cookie = entry_timeout(ShareLock);
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!entry_call());
	else
		exited = true;
	UT_ASSERT(exited);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(entry_root_calls | entry_output, 0);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
	/* A real proc_exit never resumes; reset its private harness boundary. */
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	entry_on_wait = NULL;
	entry_cleanup(ShareLock, cookie);
}

UT_TEST(startup_cf_owner_service_ack_finishes_original_cf_receipt)
{
	uint64 cookie;

	entry_fixture(false);
	cookie = entry_timeout(ShareLock);
	entry_ack_on_service = true;
	entry_ack_at = 0;
	UT_ASSERT(entry_call());
	UT_ASSERT(entry_service_consumed);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(entry_root_calls, 3);
	entry_cleanup(ShareLock, cookie);
}

/* The semantic CLEAN reader is a separate pre-bind consumer. Its actual
 * ROOT read boundary produces the original failed CF request and registry. */
UT_TEST(startup_cf_clean_input_waits_for_its_original_request_before_reread)
{
	ClusterWalStartupCleanInputV1 observed;
	ClusterControlRootResult result;
	unsigned waits, reads;

	entry_fixture(false);
	clusterStartupWriter.phase = entry_selected.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
	entry_clean_consumer = true;
	entry_read_timeouts = 1;
	result = cluster_wal_startup_clean_input_v1(&observed);
	waits = entry_waits;
	reads = entry_root_calls;
	entry_cleanup(ExclusiveLock, entry_read_cookie);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(waits, 3);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(observed.successor.identity.system_identifier, 1234);
	UT_ASSERT_EQ(observed.checkpoint_lsn, 100);
	UT_ASSERT(!clusterStartupWriterBound && !clusterStartupWriterInstalled);
}

static void
entry_change_clean_context(void)
{
	if (entry_drift < 15)
		entry_change_context();
	else if (entry_drift == 15)
		cf_hold_x.owner.request.control_owner_id++;
	else if (entry_drift == 16)
		cf_hold_x.owner.attempt.generation++;
	else if (entry_drift == 17)
		control_requests->records[cf_hold_x.owner.attempt.slot].owner.incarnation++;
	else
		control_requests->records[cf_hold_x.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
}

static bool
entry_clean_call(ClusterWalStartupCleanInputV1 *out, ClusterControlRootResult *result)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result = cluster_wal_startup_clean_input_v1(out);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_cf_clean_wait_rejects_changed_owner_or_retirement_identity)
{
	for (entry_drift = 3; entry_drift < 19; entry_drift++) {
		ClusterWalStartupCleanInputV1 observed = { 0 }, zero = { 0 };
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_ABSENT;
		uint64 old_epoch = owner_epoch, old_generation = owner_generation;
		bool completed;

		entry_fixture(false);
		clusterStartupWriter.phase = entry_selected.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
		entry_clean_consumer = true;
		entry_read_timeouts = 1;
		entry_on_wait = entry_change_clean_context;
		/* The second real request is the consumer's, not the saved setup. */
		completed = entry_clean_call(&observed, &result);
		UT_ASSERT(!completed);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(entry_root_calls, 1);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&observed, &zero, sizeof(zero)), 0);
		entry_on_wait = NULL;
		owner_epoch = old_epoch;
		owner_generation = old_generation;
		ShutdownRequestPending = false;
		/* Restore only the corruption introduced by the test. */
		if (entry_drift == 15)
			cf_hold_x.owner.request.control_owner_id--;
		if (entry_drift == 16)
			cf_hold_x.owner.attempt.generation--;
		if (entry_drift == 17)
			control_requests->records[cf_hold_x.owner.attempt.slot].owner.incarnation--;
		if (entry_drift == 18)
			control_requests->records[cf_hold_x.owner.attempt.slot].state
				= CLUSTER_CONTROL_REQUEST_ABANDONED;
		entry_cleanup(ExclusiveLock, entry_read_cookie);
	}
}

UT_TEST(startup_cf_clean_input_does_not_retry_real_root_refusal)
{
	const ClusterControlRootResult failures[]
		= { CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_STALE_TOKEN,
			CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE };

	for (unsigned i = 0; i < lengthof(failures); i++) {
		ClusterWalStartupCleanInputV1 observed;
		ClusterWalStartupCleanInputV1 zero = { 0 };
		ClusterControlRootResult result;

		entry_fixture(false);
		clusterStartupWriter.phase = entry_selected.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
		entry_clean_consumer = true;
		entry_read_result = failures[i];
		memset(&observed, 0xA5, sizeof(observed));
		result = cluster_wal_startup_clean_input_v1(&observed);
		UT_ASSERT_EQ(result, failures[i]);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(entry_root_calls, 1);
		UT_ASSERT_EQ(memcmp(&observed, &zero, sizeof(zero)), 0);
		UT_ASSERT(!clusterStartupWriterBound && !clusterStartupWriterInstalled);
	}
}

/* The CF owner, registry, consumer and custody declaration are production
 * bodies. File/claim validation is the leaf here; test_cluster_merge_claim
 * separately exercises the original validators against real private files. */
ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
static bool claim_files_current = true;
static bool pg_attribute_unused() merge_claim_waiter_current(void);
static bool pg_attribute_unused() merge_claim_custody_current(void);
static void (*claim_after_acquire)(void);
static bool
claim_actual_cf_lock(LOCKMODE mode)
{
	bool granted = cluster_cf_lock(mode);

	if (granted && claim_after_acquire != NULL)
		claim_after_acquire();
	return granted;
}
#define cluster_cf_lock claim_actual_cf_lock
#include "test_cluster_merge_cf_entry.inc"
#undef cluster_cf_lock
static bool
merge_claim_waiter_current(void)
{
	return claim_files_current && merge_claim_custody.owner == CurrentResourceOwner
		   && merge_claim_custody.pid == getpid()
		   && merge_claim_custody.boot == control_creator.incarnation
		   && merge_claim_custody.serial == merge_claim_serial;
}
static bool
merge_claim_custody_current(void)
{
	return merge_claim_waiter_current();
}

static unsigned claim_acquires;
static uint64 claim_retiring_cookie;
static void
claim_ready_after_original_ack(void)
{
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cf_hold_x.owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ExclusiveLock), claim_retiring_cookie);
	UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, claim_retiring_cookie));
	if (entry_waits == entry_ack_at) {
		g_next_request_id++;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	}
}
static void
claim_fixture(bool held)
{
	entry_fixture(true);
	claim_on_reset = NULL;
	claim_after_acquire = NULL;
	merge_claim_held = held;
	memset(&merge_claim_custody, 0, sizeof(merge_claim_custody));
	merge_claim_custody.owner = CurrentResourceOwner;
	merge_claim_custody.pid = getpid();
	merge_claim_custody.boot = control_creator.incarnation;
	merge_claim_custody.serial = ++merge_claim_serial;
	merge_claim_custody.fd = 100;
	merge_claim_custody.directory_fd = 101;
	claim_files_current = true;
	claim_acquires = g_seven_count;
	claim_retiring_cookie = 0;
	g_next_request_id++;
	entry_on_wait = claim_ready_after_original_ack;
}
static bool
claim_call(uint64 *cookie)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*cookie = merge_claim_cf_enter();
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
static void
claim_cleanup(uint64 cookie)
{
	bool ack_on_wait = owner_ack_on_wait;

	if (cookie == 0)
		cookie = cluster_cf_owner_cookie(ExclusiveLock);
	entry_on_wait = NULL;
	claim_on_reset = NULL;
	if (cookie == 0) {
		UT_ASSERT(!cluster_cf_held(ExclusiveLock) && cluster_control_request_empty());
		return;
	}
	claim_teardown = owner_ack_on_wait = true;
	if (cluster_cf_held_is_usable(ExclusiveLock))
		(void)cluster_cf_unlock_confirmed(ExclusiveLock);
	entry_cleanup(ExclusiveLock, cookie);
	claim_teardown = false;
	owner_ack_on_wait = ack_on_wait;
}
UT_TEST(startup_claim_cf_wait_retires_before_reacquiring)
{
	for (unsigned held = 0; held < 2; held++) {
		uint64 cookie = 0;
		bool completed;

		claim_fixture(held);
		completed = claim_call(&cookie);
		UT_ASSERT(completed);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
		UT_ASSERT_EQ(merge_claim_held, held);
		if (completed) {
			UT_ASSERT(cookie != 0 && cookie != claim_retiring_cookie);
			UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
			UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, claim_retiring_cookie));
		}
		claim_cleanup(cookie);
	}
}

static void
claim_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ExclusiveLock) && cluster_control_request_empty());
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id++;
}
UT_TEST(startup_claim_cf_prepublication_wait_has_no_cleanup_authority)
{
	uint64 cookie = 0;

	claim_fixture(false);
	fail_after_publication = false;
	entry_on_wait = claim_empty_grant;
	UT_ASSERT(claim_call(&cookie));
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
	UT_ASSERT(cookie != 0 && cluster_cf_held_is_clusterwide(ExclusiveLock));
	claim_cleanup(cookie);
}

UT_TEST(startup_claim_cf_hard_native_and_stale_observations_do_not_retry)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		uint64 cookie = 0;

		claim_fixture(false);
		switch (fault) {
		case 0:
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			break;
		case 1:
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
			break;
		case 2:
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
			cluster_shared_config = false;
			break;
		case 3: {
			uint64 old = entry_timeout(ExclusiveLock);
			entry_cleanup(ExclusiveLock, old);
			claim_acquires = g_seven_count;
			g_next_request_id++;
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			break;
		}
		case 4:
			fail_after_publication = false;
			g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
			break;
		}
		UT_ASSERT(!claim_call(&cookie));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		UT_ASSERT_EQ(cookie, 0);
		claim_cleanup(cookie);
	}
}

UT_TEST(startup_claim_cf_preexisting_slot_is_never_adopted)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		uint64 cookie = 0, retiring;
		CfHoldState saved;

		claim_fixture(false);
		retiring = entry_timeout(ExclusiveLock);
		saved = cf_hold_x;
		switch (fault) {
		case 0: /* Even a valid old cleanup slot is not this acquire's wait. */
			break;
		case 1:
			cf_hold_x.release_pending = false;
			cf_hold_x.owner.state = CLUSTER_LOCK_OWNER_HELD;
			break;
		case 2:
			cf_hold_x.owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
			break;
		case 3:
			cf_hold_x.caller = &cookie;
			break;
		case 4:
			cf_hold_x.owner.request.control_owner_id = 0;
			break;
		}
		UT_ASSERT(!claim_call(&cookie));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits | cookie, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		cf_hold_x = saved; /* Undo fixture corruption, not product retirement. */
		claim_cleanup(retiring);
	}
}

static unsigned claim_fault;
static CfHoldState claim_original_slot;
static ClusterControlRequestView claim_original_record;
static void
claim_change_context(void)
{
	claim_ready_after_original_ack();
	claim_original_slot = cf_hold_x;
	claim_original_record = control_requests->records[cf_hold_x.owner.attempt.slot];
	switch (claim_fault) {
	case 0:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 1:
		merge_claim_custody.pid++;
		break;
	case 2:
		merge_claim_custody.boot++;
		break;
	case 3:
		merge_claim_custody.serial++;
		break;
	case 4:
		merge_claim_serial++;
		break;
	case 5:
		merge_claim_custody.fd++;
		break;
	case 6:
		merge_claim_custody.directory_fd++;
		break;
	case 7:
		merge_claim_custody.image.node++;
		break;
	case 8:
		merge_claim_custody.path[0]++;
		break;
	case 9:
		claim_files_current = false;
		break;
	case 10:
		merge_claim_custody.revoked = true;
		break;
	case 11:
		merge_claim_held = !merge_claim_held;
		break;
	case 12:
		owner_epoch++;
		break;
	case 13:
		owner_generation++;
		break;
	case 14:
		entry_authority = false;
		break;
	case 15:
		InRecovery = !InRecovery;
		break;
	case 16:
		ShutdownRequestPending = true;
		break;
	case 17:
		entry_native_lock = true;
		break;
	case 18:
		CritSectionCount = 1;
		break;
	case 19:
		cf_hold_x.owner.request.control_owner_id++;
		break;
	case 20:
		cf_hold_x.owner.attempt.generation++;
		break;
	case 21:
		control_requests->records[cf_hold_x.owner.attempt.slot].owner.incarnation++;
		break;
	case 22:
		control_requests->records[cf_hold_x.owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 23:
		control_requests->records[cf_hold_x.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 24:
		MyProc = NULL;
		break;
	case 25:
		MyLatch = NULL;
		break;
	case 26:
		MyBackendType = B_LMON;
		break;
	case 27:
		MyAuxProcType = CheckpointerProcess;
		break;
	case 28:
		cf_hold_x.owner.cooperative_acquire = true;
		break;
	case 29:
		cf_hold_x.release_pending = false;
		break;
	case 30:
		cf_hold_x.owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
		break;
	}
}
UT_TEST(startup_claim_cf_wait_requalifies_actor_custody_and_exact_request)
{
	for (unsigned held = 0; held < 2; held++)
		for (claim_fault = 0; claim_fault < 31; claim_fault++) {
			uint64 cookie = 0, epoch = owner_epoch, generation = owner_generation;

			claim_fixture(held);
			entry_on_wait = claim_change_context;
			UT_ASSERT(!claim_call(&cookie));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
			UT_ASSERT_EQ(cookie, 0);
			/* Restore only injected state before retiring the actual original. */
			MyProc = &owner_proc;
			MyLatch = &owner_proc.procLatch;
			MyBackendType = B_STARTUP;
			MyAuxProcType = StartupProcess;
			CurrentResourceOwner = (ResourceOwner)1;
			CritSectionCount = 0;
			entry_native_lock = false;
			owner_epoch = epoch;
			owner_generation = generation;
			cf_hold_x = claim_original_slot;
			control_requests->records[cf_hold_x.owner.attempt.slot] = claim_original_record;
			claim_cleanup(claim_retiring_cookie);
		}
}

static void
claim_remove_completion(void)
{
	claim_ready_after_original_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, claim_retiring_cookie));
	cf_retired_x = 0; /* Fault injection: slot disappearance is not a receipt. */
}
UT_TEST(startup_claim_cf_missing_receipt_and_hard_cleanup_remain_errors)
{
	for (unsigned hard = 0; hard < 2; hard++) {
		uint64 cookie = 0;

		claim_fixture(false);
		entry_on_wait = hard ? entry_hard_error : claim_remove_completion;
		UT_ASSERT(!claim_call(&cookie));
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		UT_ASSERT_EQ(entry_code, hard ? ERRCODE_IO_ERROR : ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_level, hard ? ERROR : FATAL);
		UT_ASSERT_EQ(cookie, 0);
		if (!hard)
			cf_retired_x = claim_retiring_cookie;
		claim_cleanup(hard ? cluster_cf_owner_cookie(ExclusiveLock) : claim_retiring_cookie);
	}
}

static void
claim_ack_during_reset(void)
{
	claim_retiring_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT(acknowledge_retirements());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id++;
	claim_on_reset = NULL;
}
UT_TEST(startup_claim_cf_ack_race_and_lost_latch_still_make_progress)
{
	for (unsigned race = 0; race < 2; race++) {
		uint64 cookie = 0;

		claim_fixture(false);
		entry_lost_latch = true;
		if (race)
			claim_on_reset = claim_ack_during_reset;
		UT_ASSERT(claim_call(&cookie));
		UT_ASSERT_EQ(entry_waits, race ? 0 : 3);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
		UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
		UT_ASSERT(cookie != 0 && cookie != claim_retiring_cookie);
		claim_cleanup(cookie);
	}
}

static void
claim_change_during_reacquire(void)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	merge_claim_custody.fd++;
}
UT_TEST(startup_claim_cf_reacquire_cannot_return_changed_custody)
{
	uint64 cookie = 0;

	claim_fixture(false);
	claim_after_acquire = claim_change_during_reacquire;
	UT_ASSERT(!claim_call(&cookie));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
	UT_ASSERT_EQ(cookie, 0);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	claim_cleanup(0);
}

UT_TEST(startup_claim_cf_original_startup_cancel_retains_cleanup)
{
	uint64 cookie = 0;
	volatile bool exited = false;

	claim_fixture(false);
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!claim_call(&cookie));
	else
		exited = true;
	UT_ASSERT(exited);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT_EQ(cookie, 0);
	UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT(control_requests->records[cf_hold_x.owner.attempt.slot].owner_exited);
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	claim_cleanup(0);
}

static ClusterFormationSnapshotV1 cold_cohort;
static ClusterWalSourceRef cold_restart;
void *
palloc(Size size)
{
	void *out = malloc(size);

	UT_ASSERT(out != NULL);
	return out;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return control_creator.incarnation;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	*out = cold_restart;
	return true;
}
bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 thread, ClusterFormationSnapshotV1 *out)
{
	UT_ASSERT_EQ(thread, 1);
	*out = cold_cohort;
	return true;
}
ClusterFormationWitnessResult
cluster_authority_startup_refresh_recovery(int budget)
{
	UT_ASSERT_EQ(budget, 100);
	return CLUSTER_FORMATION_WITNESS_READY;
}
static ClusterControlRootResult
recovery_merge_cold_inputs_observe(const ClusterWalSourceRef *restart,
								   const ClusterFormationSnapshotV1 *cohort, ControlRootImage *root,
								   ClusterWalOriginInputs *inputs, bool *pending)
{
	UT_ASSERT(restart != NULL && cohort != NULL && root != NULL && inputs != NULL);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(cluster_formation_snapshot_matches_v1(cohort, &cold_cohort));
	++cold_reads;
	*pending = false;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
#define cluster_cf_lock claim_actual_cf_lock
#include "test_cluster_merge_cold_cf.inc"
#undef cluster_cf_lock

static void
cold_ready_after_ack(void)
{
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT_EQ(cold_reads, 0);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		g_next_request_id++;
	}
}
static void
cold_fixture(void)
{
	claim_fixture(false);
	cold_consumer = true;
	cold_reads = cold_release_waits = 0;
	memset(&cold_cohort, 0, sizeof(cold_cohort));
	cold_cohort.local_epoch = owner_epoch;
	cold_cohort.startup_formation_generation = 9;
	memset(&cold_restart, 0, sizeof(cold_restart));
	cold_restart.timeline = 1;
	entry_on_wait = cold_ready_after_ack;
}
static bool
cold_call(bool *ready)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*ready = recovery_merge_wait_cold_inputs(1);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
static void
cold_cleanup(void)
{
	uint64 cookie = cluster_cf_owner_cookie(ShareLock);

	cold_consumer = false;
	entry_on_wait = NULL;
	if (cluster_cf_held(ShareLock))
		entry_cleanup(ShareLock, cookie);
	UT_ASSERT(cluster_control_request_empty());
}
UT_TEST(startup_cold_cohort_cf_wait_does_not_become_release_failure)
{
	bool ready = false;

	cold_fixture();
	UT_ASSERT(cold_call(&ready));
	UT_ASSERT(ready);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
	UT_ASSERT_EQ(cold_reads, 1);
	cold_cleanup();
}

static void
cold_change_restart_on_grant(void)
{
	cold_restart.timeline++;
}
UT_TEST(startup_cold_cf_reacquire_rechecks_original_input_before_observation)
{
	bool ready = false;

	cold_fixture();
	claim_after_acquire = cold_change_restart_on_grant;
	UT_ASSERT(cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(cold_reads, 0);
	cold_cleanup();
}

static unsigned cold_fault;
static CfHoldState cold_saved_slot;
static ClusterControlRequestView cold_saved_record;
static void
cold_change_context(void)
{
	cold_ready_after_ack();
	cold_saved_slot = cf_hold_s;
	cold_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	switch (cold_fault) {
	case 0:
		cold_cohort.startup_formation_generation++;
		break;
	case 1:
		cold_restart.timeline++;
		break;
	case 2:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 3:
		owner_epoch++;
		break;
	case 4:
		owner_generation++;
		break;
	case 5:
		control_creator.incarnation++;
		break;
	case 6:
		entry_authority = false;
		break;
	case 7:
		ShutdownRequestPending = true;
		break;
	case 8:
		InRecovery = !InRecovery;
		break;
	case 9:
		entry_native_lock = true;
		break;
	case 10:
		CritSectionCount = 1;
		break;
	case 11:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 12:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 13:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 14:
		cf_hold_s.release_pending = false;
		break;
	case 15:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 16:
		cf_hold_x.held = true;
		break;
	case 17:
		MyProc = NULL;
		break;
	case 18:
		MyLatch = NULL;
		break;
	case 19:
		MyBackendType = B_LMON;
		break;
	case 20:
		cluster_enabled = false;
		break;
	case 21:
		cluster_shared_config = false;
		break;
	case 22:
		cluster_controlfile_shared_authority = false;
		break;
	}
}
UT_TEST(startup_cold_cf_wait_rejects_context_and_exact_identity_changes)
{
	for (cold_fault = 0; cold_fault < 23; cold_fault++) {
		uint64 epoch = owner_epoch, generation = owner_generation;
		uint64 boot = control_creator.incarnation;
		bool ready = false;

		cold_fixture();
		entry_on_wait = cold_change_context;
		UT_ASSERT(!cold_call(&ready));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(cold_reads, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		UT_ASSERT(!ready);
		/* Restore injected corruption only; use actual ACK/poll to retire. */
		MyProc = &owner_proc;
		MyLatch = &owner_proc.procLatch;
		CurrentResourceOwner = (ResourceOwner)1;
		MyBackendType = B_STARTUP;
		CritSectionCount = 0;
		entry_native_lock = false;
		cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
		owner_epoch = epoch;
		owner_generation = generation;
		control_creator.incarnation = boot;
		cf_hold_s = cold_saved_slot;
		memset(&cf_hold_x, 0, sizeof(cf_hold_x));
		control_requests->records[cf_hold_s.owner.attempt.slot] = cold_saved_record;
		cold_cleanup();
	}
}

static void
cold_missing_receipt(void)
{
	cold_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ShareLock, claim_retiring_cookie));
	cf_retired_s = 0;
}
static void
cold_ack_during_reset(void)
{
	claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	UT_ASSERT(acknowledge_retirements());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id++;
	claim_on_reset = NULL;
}
UT_TEST(startup_cold_cf_receipt_is_required_and_wakes_are_only_scheduling)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		bool ready = false;

		cold_fixture();
		entry_lost_latch = true;
		if (fault == 0)
			entry_on_wait = cold_missing_receipt;
		else if (fault == 1)
			claim_on_reset = cold_ack_during_reset;
		else if (fault == 2) {
			fail_after_publication = false;
			entry_on_wait = claim_empty_grant;
		}
		UT_ASSERT_EQ(cold_call(&ready), fault != 0);
		UT_ASSERT_EQ(ready, fault != 0);
		UT_ASSERT_EQ(cold_reads, fault != 0);
		UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
		if (fault == 0) {
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT(!cluster_cf_held(ShareLock));
			cf_retired_s = claim_retiring_cookie;
		}
		cold_cleanup();
	}
}

UT_TEST(startup_cold_cf_untyped_and_hard_cleanup_are_not_wait_slices)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		bool ready = false;

		cold_fixture();
		if (fault == 0)
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		else if (fault == 1)
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		else
			entry_on_wait = entry_hard_error;
		UT_ASSERT(!cold_call(&ready));
		UT_ASSERT_EQ(cold_reads, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(entry_code,
					 fault == 2 ? ERRCODE_IO_ERROR : ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		cold_cleanup();
	}
}

UT_TEST(startup_cold_cf_original_cancel_leaves_the_original_cleanup_owner)
{
	bool ready = false;
	volatile bool exited = false;

	cold_fixture();
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!cold_call(&ready));
	else
		exited = true;
	UT_ASSERT(exited && !ready);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(cold_reads, 0);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	cold_cleanup();
}

/* Exact input consumer/cleanup and scope predicates join the actual CF owner.
 * Native Start/work and WALR qualification plus ROOT bytes are leaf boundaries;
 * no root observation can run until a fresh usable CF grant is established. */
#define cluster_cf_lock claim_actual_cf_lock
#include "test_cluster_inputs_cf.inc"
#undef cluster_cf_lock

static ClusterWalInputsV1 input_scope;
static WalInputsWork input_work;
static ClusterColdTypedV1 input_typed;
static ClusterColdTopTxCensusEntryV1 input_subject;
static ClusterRecoveryCtrcSourcePlanV1 input_sources;
static struct {
	uint64 align;
	char bytes[offsetof(ClusterColdCtrcWorkV1, entries)];
} input_ctrc_storage;
#define INPUT_CTRC_WORK ((ClusterColdCtrcWorkV1 *)input_ctrc_storage.bytes)
static char input_start_object, input_pin_object;
#define INPUT_START ((const struct ClusterRecoveryStartV1 *)&input_start_object)
#define INPUT_PIN ((ClusterWalReadPinV1 *)&input_pin_object)
static ClusterControlRootFileToken input_token;
static ClusterRecoveryCheckpointOwnerV1 input_checkpoint_owner;
static uint64 input_generation, input_serial, input_pin_generation;
static unsigned input_reads;
static bool input_start_current, input_pin_current, input_first;

bool
RecoveryInProgress(void)
{
	return InRecovery;
}
const struct ClusterColdTypedV1 *
ClusterColdRecoveryTxInputs(const struct ClusterRecoveryStartV1 *start)
{
	return input_start_current && start == INPUT_START ? &input_typed : NULL;
}
bool
ClusterColdRecoveryTxSubjectCurrent(const struct ClusterRecoveryStartV1 *start,
									const struct ClusterColdTopTxCensusEntryV1 *subject,
									uint64 *generation, uint64 *serial)
{
	*generation = *serial = 0;
	if (!input_start_current || start != INPUT_START || subject != &input_subject)
		return false;
	*generation = input_generation;
	*serial = input_serial;
	return true;
}
bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
								   const struct ClusterColdCtrcWorkV1 *work)
{
	return input_start_current && start == INPUT_START && work == INPUT_CTRC_WORK
		   && input_typed.ctrc_work == work && work->generation != 0
		   && work->generation == input_typed.ctrc_work_generation;
}
bool
ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
									   struct ClusterRecoveryCheckpointOwnerV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!input_start_current || start != INPUT_START)
		return false;
	*out = input_checkpoint_owner;
	return true;
}
bool
cluster_wal_read_pin_covers_v1(ClusterWalReadPinV1 *pin, uint16 thread)
{
	return input_pin_current && pin == INPUT_PIN && (thread == 1 || thread == 3);
}
bool
cluster_wal_read_pin_owner_v1(ClusterWalReadPinV1 *pin, ClusterWalReadPinOwnerV1 *out)
{
	if (!cluster_wal_read_pin_covers_v1(pin, 1))
		return false;
	memset(out, 0, sizeof(*out));
	out->pin = pin;
	out->generation = input_pin_generation;
	return true;
}
bool
cluster_wal_read_pin_owner_current_v1(const ClusterWalReadPinOwnerV1 *owner)
{
	return owner->generation == input_pin_generation
		   && cluster_wal_read_pin_covers_v1(owner->pin, 1);
}
ClusterControlRootResult
cluster_control_root_v3_read_control_locked(const uint8 uuid[16], uint64 system_id,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(system_id, 1234);
	UT_ASSERT_EQ(uuid[0], 31);
	++input_reads;
	g_next_request_id++;
	memset(root, 0, sizeof(*root));
	memset(common, 0, sizeof(*common));
	root->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	root->header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	*token = input_token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static void
input_ready_after_ack(void)
{
	UT_ASSERT_EQ(input_reads, 0);
	cold_ready_after_ack();
}
static void
input_fixture(unsigned phase)
{
	cold_fixture();
	memset(&input_scope, 0, sizeof(input_scope));
	memset(&input_work, 0, sizeof(input_work));
	memset(&input_typed, 0, sizeof(input_typed));
	memset(&input_subject, 0, sizeof(input_subject));
	memset(&input_sources, 0, sizeof(input_sources));
	memset(&input_ctrc_storage, 0, sizeof(input_ctrc_storage));
	memset(&input_checkpoint_owner, 0, sizeof(input_checkpoint_owner));
	memset(&input_token, 0, sizeof(input_token));
	input_token.file_txn_seq = 7;
	input_generation = 17;
	input_serial = 19;
	input_pin_generation = 23;
	input_reads = 0;
	input_start_current = input_pin_current = true;
	input_scope.system_identifier = 1234;
	input_scope.storage_uuid[0] = 31;
	input_scope.pid = getpid();
	input_scope.owner = CurrentResourceOwner;
	input_first = phase == 0 || phase == 3 || phase == 5;
	if (!input_first)
		input_scope.token = input_token;
	if (phase == 2 || phase == 4 || phase == 6) {
		input_scope.pin = INPUT_PIN;
		input_scope.thread_count = 2;
		input_scope.threads[0] = 1;
		input_scope.threads[1] = 3;
	}
	input_scope.purpose = phase < 3	  ? WAL_INPUTS_COLD_REPLAY
						  : phase < 5 ? WAL_INPUTS_TX_CLOSURE
									  : WAL_INPUTS_CTRC_CENSUS;
	InRecovery = phase < 3;
	input_typed.census_root_captured = true;
	input_typed.census_root = input_token;
	if (phase == 3 || phase == 4) {
		input_scope.tx.start = INPUT_START;
		input_scope.tx.inputs = &input_typed;
		input_scope.tx.subject = &input_subject;
		input_scope.tx.generation = input_generation;
		input_scope.tx.serial = input_serial;
		input_scope.tx.version = input_subject;
		input_scope.tx.root = input_token;
	} else if (phase >= 5) {
		INPUT_CTRC_WORK->generation = 29;
		input_typed.ctrc_work = INPUT_CTRC_WORK;
		input_typed.ctrc_work_generation = 29;
		input_checkpoint_owner.start_generation = input_generation;
		input_checkpoint_owner.start_serial = input_serial;
		input_checkpoint_owner.owner_boot = control_creator.incarnation;
		input_scope.ctrc.start = INPUT_START;
		input_scope.ctrc.inputs = &input_typed;
		input_scope.ctrc.work = INPUT_CTRC_WORK;
		input_scope.ctrc.work_generation = 29;
		input_scope.ctrc.owner = input_checkpoint_owner;
		input_scope.ctrc.root = input_token;
		input_scope.ctrc.source_plan = &input_sources;
		input_typed.participant_count = input_sources.participant_count = 1;
		input_typed.replay_count = input_sources.replay_count = 1;
	}
	entry_on_wait = input_ready_after_ack;
}
static bool
input_call(ClusterControlRootResult *result)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result = inputs_root(&input_scope, &input_work, input_first);
		inputs_unlock(&input_work);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
UT_TEST(startup_input_cf_wait_keeps_initial_scratch_and_retained_scopes)
{
	for (unsigned phase = 0; phase < 7; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		bool completed;

		input_fixture(phase);
		completed = input_call(&result);
		UT_ASSERT(completed);
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(input_reads, 1);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
		UT_ASSERT(!input_work.cf_attempted);
		cold_cleanup();
	}
}

static unsigned input_fault;
static CfHoldState input_saved_slot;
static ClusterControlRequestView input_saved_record;
static void
input_change_context(void)
{
	input_ready_after_ack();
	input_saved_slot = cf_hold_s;
	input_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	switch (input_fault) {
	case 0:
		MyProc = NULL;
		break;
	case 1:
		MyLatch = NULL;
		break;
	case 2:
		MyProcPid++;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		owner_epoch++;
		break;
	case 5:
		owner_generation++;
		break;
	case 6:
		control_creator.incarnation++;
		break;
	case 7:
		entry_authority = false;
		break;
	case 8:
		ShutdownRequestPending = true;
		break;
	case 9:
		CritSectionCount = 1;
		break;
	case 10:
		entry_native_lock = true;
		break;
	case 11:
		MyBackendType = B_LMON;
		break;
	case 12:
		InRecovery = !InRecovery;
		break;
	case 13:
		cluster_enabled = false;
		break;
	case 14:
		cluster_shared_config = false;
		break;
	case 15:
		cluster_controlfile_shared_authority = false;
		break;
	case 16:
		input_scope.system_identifier++;
		break;
	case 17:
		input_scope.storage_uuid[0]++;
		break;
	case 18:
		input_scope.token.file_txn_seq++;
		break;
	case 19:
		input_scope.pid++;
		break;
	case 20:
		input_scope.owner = (ResourceOwner)2;
		break;
	case 21:
		input_scope.purpose = WAL_INPUTS_ONLINE;
		break;
	case 22:
		input_scope.stale = true;
		break;
	case 23:
		input_scope.suspended = true;
		break;
	case 24:
		input_scope.thread_count = CLUSTER_WAL_RETENTION_MAX_THREADS + 1;
		break;
	case 25:
		input_scope.threads[0]++;
		break;
	case 26:
		input_scope.pin = (ClusterWalReadPinV1 *)2;
		break;
	case 27:
		input_pin_generation++;
		break;
	case 28:
		input_pin_current = false;
		break;
	case 29:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 30:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 31:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 32:
		cf_hold_s.release_pending = false;
		break;
	case 33:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 34:
		cf_hold_x.held = true;
		break;
	}
}
UT_TEST(startup_input_cf_wait_rejects_changed_actor_scope_pin_and_request)
{
	for (unsigned phase = 0; phase < 7; phase++) {
		for (input_fault = 0; input_fault < 35; input_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			uint64 epoch = owner_epoch, generation = owner_generation;
			uint64 boot = control_creator.incarnation;
			int pid = MyProcPid;

			input_fixture(phase);
			if ((input_fault == 25 || input_fault == 27 || input_fault == 28)
				&& input_scope.pin == NULL) {
				cold_cleanup();
				continue;
			}
			entry_on_wait = input_change_context;
			UT_ASSERT(!input_call(&result));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(input_reads, 0);
			UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
			UT_ASSERT(!input_work.cf_attempted);
			/* Undo only the injected corruption, then use real ACK/poll. */
			MyProc = &owner_proc;
			MyLatch = &owner_proc.procLatch;
			MyProcPid = pid;
			CurrentResourceOwner = (ResourceOwner)1;
			MyBackendType = B_STARTUP;
			CritSectionCount = 0;
			entry_native_lock = false;
			cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
			owner_epoch = epoch;
			owner_generation = generation;
			control_creator.incarnation = boot;
			cf_hold_s = input_saved_slot;
			memset(&cf_hold_x, 0, sizeof(cf_hold_x));
			control_requests->records[cf_hold_s.owner.attempt.slot] = input_saved_record;
			cold_cleanup();
		}
	}
}

static void
input_change_original_start(void)
{
	input_ready_after_ack();
	switch (input_fault) {
	case 0:
		input_start_current = false;
		break;
	case 1:
		input_typed.census_root_captured = false;
		break;
	case 2:
		input_typed.census_root.file_txn_seq++;
		break;
	case 3:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE)
			input_scope.tx.inputs = NULL;
		else
			input_scope.ctrc.inputs = NULL;
		break;
	case 4:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE)
			input_subject.input.key.slot_offset++;
		else
			INPUT_CTRC_WORK->generation++;
		break;
	case 5:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE) {
			input_generation++;
			input_scope.tx.generation = input_generation; /* cannot adopt a new Start */
		} else
			input_checkpoint_owner.start_serial++;
		break;
	case 6:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE) {
			input_serial++;
			input_scope.tx.serial = input_serial;
		} else
			input_typed.participant_count++;
		break;
	case 7:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE)
			input_scope.tx.start = NULL;
		else
			input_typed.sources[0].timeline++;
		break;
	case 8:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE)
			input_scope.tx.version.input.key.slot_offset++;
		else
			input_scope.ctrc.source_plan = NULL;
		break;
	case 9:
		if (input_scope.purpose == WAL_INPUTS_TX_CLOSURE)
			input_scope.tx.ir_revoked = true;
		else
			input_scope.ctrc.source_plan->sources[0].timeline++;
		break;
	}
}
UT_TEST(startup_input_cf_wait_cannot_adopt_changed_tx_or_ctrc_start)
{
	for (unsigned phase = 3; phase < 7; phase++) {
		for (input_fault = 0; input_fault < 10; input_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			input_fixture(phase);
			entry_on_wait = input_change_original_start;
			UT_ASSERT(!input_call(&result));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(input_reads, 0);
			UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
			cold_cleanup();
		}
	}
}

static void
input_change_after_grant(void)
{
	input_scope.storage_uuid[0]++;
}
UT_TEST(startup_input_cf_new_grant_rechecks_scope_before_root_io)
{
	for (unsigned phase = 0; phase < 7; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		input_fixture(phase);
		claim_after_acquire = input_change_after_grant;
		UT_ASSERT(input_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(input_reads, 0);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
		UT_ASSERT(!cluster_cf_held(ShareLock));
		cold_cleanup();
	}
}

UT_TEST(startup_input_cf_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned phase = 0; phase < 7; phase++) {
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			input_fixture(phase);
			entry_lost_latch = true;
			if (fault == 0)
				entry_on_wait = cold_missing_receipt;
			else if (fault == 1)
				claim_on_reset = cold_ack_during_reset;
			else if (fault == 2) {
				fail_after_publication = false;
				entry_on_wait = claim_empty_grant;
			}
			UT_ASSERT_EQ(input_call(&result), fault != 0);
			UT_ASSERT_EQ(input_reads, fault != 0);
			UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
			if (fault == 0) {
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT(!cluster_cf_held(ShareLock));
				cf_retired_s = claim_retiring_cookie;
			} else
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			cold_cleanup();
		}
	}
}

UT_TEST(startup_input_cf_untyped_hard_and_preexisting_owners_are_not_retried)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		input_fixture(2);
		if (fault == 0)
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		else if (fault == 1)
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		else if (fault == 2)
			entry_on_wait = entry_hard_error;
		else {
			(void)entry_timeout(ShareLock);
			claim_acquires = g_seven_count;
		}
		if (fault == 3) {
			UT_ASSERT(input_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(g_seven_count, claim_acquires);
		} else {
			UT_ASSERT(!input_call(&result));
			UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		}
		UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(input_reads, 0);
		if (fault == 2)
			UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
		cold_cleanup();
	}
}

UT_TEST(startup_input_cf_original_cancel_keeps_the_original_cleanup_owner)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	volatile bool exited = false;

	input_fixture(6);
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!input_call(&result));
	else
		exited = true;
	UT_ASSERT(exited);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(input_reads, 0);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	cold_cleanup();
}

UT_TEST(ordinary_input_cf_timeout_keeps_its_original_release_behavior)
{
	const BackendType roles[] = { B_STARTUP, B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER };

	for (unsigned role = 0; role < lengthof(roles); role++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		input_fixture(2);
		input_scope.purpose = WAL_INPUTS_ONLINE;
		MyBackendType = roles[role];
		UT_ASSERT(!input_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(entry_level, FATAL);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(input_reads, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
		UT_ASSERT(input_work.cf_attempted);
		MyBackendType = B_STARTUP;
		cold_cleanup();
	}
}

static ClusterControlRootSnapshot input_expected, input_expected_original;
static unsigned input_selections, input_claim_reads;
char *cluster_wal_threads_dir = "startup-input-fixture";
void *
palloc0(Size size)
{
	void *out = calloc(1, size);

	UT_ASSERT(out != NULL);
	return out;
}
uint64
GetSystemIdentifier(void)
{
	return 1234;
}
static ClusterControlRootResult
inputs_cold_checkpoint_select(ClusterWalInputsV1 *scratch, WalInputsWork *work,
							  const ClusterControlRootSnapshot *expected)
{
	UT_ASSERT(work != NULL && cluster_cf_held_is_clusterwide(ShareLock));
	++input_selections;
	if (memcmp(expected, &input_expected_original, sizeof(*expected)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	scratch->items[0].checkpoint_start = 100;
	scratch->items[0].source.timeline = 1;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_wal_claim_v2_read(const char *dir, const ClusterWalThreadClaimRefV2 *expected,
						  ClusterWalThreadClaimV2 *out)
{
	UT_ASSERT(dir != NULL && expected != NULL && out != NULL);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	++input_claim_reads;
	memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
#include "test_cluster_inputs_cf_checkpoint.inc"

static void
input_checkpoint_change_external_argument(void)
{
	input_expected.root_publish_seq++;
}
UT_TEST(startup_checkpoint_scratch_cf_wait_keeps_its_original_selection)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalRecoveryCheckpointV1 out;
		volatile bool completed = false;
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		input_fixture(1);
		memset(&input_expected, 0, sizeof(input_expected));
		input_expected.identity.system_identifier = 1234;
		input_expected.identity.storage_uuid[0] = 31;
		input_expected.identity.origin_node_id = 0;
		input_expected.identity.origin_thread_id = 1;
		input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		input_expected_original = input_expected;
		input_claim_reads = input_selections = 0;
		if (fault)
			claim_after_acquire = input_checkpoint_change_external_argument;
		PG_TRY();
		{
			result = cluster_wal_inputs_cold_checkpoint_v1(&input_expected, &input_token, &out);
			completed = true;
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
		UT_ASSERT(completed);
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(input_reads, 2);
		UT_ASSERT_EQ(g_seven_count, claim_acquires + 3);
		UT_ASSERT_EQ(input_selections, 1);
		UT_ASSERT_EQ(input_claim_reads, 1);
		UT_ASSERT_EQ(out.checkpoint_start, 100);
		cold_cleanup();
	}
}


/* Runtime identity facts and physical ROOT I/O are leaves. The owner check,
 * canonical reader and both retention consumers below are production bodies. */
#define CONTROL_ROOT_SOURCE_PRIMARY UINT8_C(1)
#define CONTROL_ROOT_HEADER_VERSION_V2 UINT16_C(2)
#define CONTROL_ROOT_HEADER_VERSION_V3 UINT16_C(3)
#include "test_cluster_retention_cf_types.inc"
static ClusterControlRootIdentity retention_expected;
static ControlRootImage retention_image;
static unsigned retention_phase, retention_attempts, retention_threads, retention_controls;
static unsigned retention_histories;
static bool retention_member, retention_serving, retention_pending, retention_fence;
static bool retention_directory, retention_prebump;
static uint64 retention_admitted;
static uint16 retention_thread_id;
static ClusterControlRootResult retention_root_result;
static ClusterLockAcquireResult retention_second_result;

ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	return retention_member ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_DEAD;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	return retention_admitted;
}
bool
cluster_serving_ready_check(bool *pending, const char **failed)
{
	*pending = retention_pending;
	UT_ASSERT(failed == NULL);
	return retention_serving;
}
bool
cluster_write_fence_allowed(void)
{
	return retention_fence;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return retention_directory;
}
uint16
cluster_wal_thread_id(void)
{
	return retention_thread_id;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return retention_prebump;
}
static bool
current_storage_uuid(uint8 uuid[16])
{
	memset(uuid, 0, 16);
	uuid[0] = 31;
	return true;
}
static ClusterControlRootResult
storage_contract_check(const uint8 *uuid, bool probe)
{
	UT_ASSERT(uuid != NULL && uuid[0] == 31 && probe);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static ClusterControlRootResult
read_thread_version(const ClusterControlRootIdentity *expected, ControlRootImage *root,
					ControlFileData *native, ClusterControlRootFileToken *token, uint16 version)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(version == 2 || version == 3);
	UT_ASSERT_EQ(memcmp(expected, &retention_image.records[0].identity, sizeof(*expected)), 0);
	++retention_threads;
	g_next_request_id++;
	*root = retention_image;
	memset(native, 0, sizeof(*native));
	memset(token, 0, sizeof(*token));
	token->file_txn_seq = 7;
	return retention_root_result;
}
static ClusterControlRootResult
read_control_version(const uint8 uuid[16], uint64 sysid, ControlRootImage *root,
					 ControlFileData *native, ClusterControlRootFileToken *token, uint16 version)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(uuid[0] == 31 && sysid == 1234 && version == 3);
	++retention_controls;
	g_next_request_id++;
	*root = retention_image;
	memset(native, 0, sizeof(*native));
	memset(token, 0, sizeof(*token));
	token->file_txn_seq = 7;
	return retention_root_result;
}
static ClusterControlRootResult
read_recovery_subject_locked(uint16 thread, const uint8 uuid[16], uint64 sysid,
							 const ClusterControlRootIdentity *expected, ControlRootImage *root,
							 ClusterControlRecoverySubject *out)
{
	(void)thread;
	(void)uuid;
	(void)sysid;
	(void)expected;
	(void)root;
	(void)out;
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
static ClusterControlRootResult
stop_phase_v2_locked(const ControlRootImage *root, uint32 index, const ControlFileData *view,
					 uint64 admitted, ClusterControlRootStopPhase *phase)
{
	(void)root;
	(void)index;
	(void)view;
	(void)admitted;
	(void)phase;
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
ClusterControlRootResult
cluster_wal_history_read_locked(const ControlRootImage *root, uint32 node,
								ClusterWalHistoryImage *history)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(root != NULL && node == 0);
	++retention_histories;
	memset(history, 0, sizeof(*history));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static bool
retention_actual_cf_lock(LOCKMODE mode)
{
	if (retention_phase == 2 && retention_attempts == 1)
		g_seven_result = retention_second_result;
	++retention_attempts;
	return claim_actual_cf_lock(mode);
}
#define cluster_cf_lock retention_actual_cf_lock
#include "test_cluster_retention_cf.inc"
#undef cluster_cf_lock

static void
retention_ready_after_ack(void)
{
	UT_ASSERT_EQ(g_seven_count, claim_acquires + (retention_phase == 2 ? 2 : 1));
	UT_ASSERT_EQ(retention_threads + retention_controls, retention_phase == 2 ? 1 : 0);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		g_next_request_id++;
	}
}
static void
retention_fixture(unsigned phase)
{
	cold_fixture();
	retention_phase = phase;
	retention_attempts = retention_threads = retention_controls = retention_histories = 0;
	retention_member = retention_serving = retention_fence = retention_directory = true;
	retention_pending = retention_prebump = false;
	retention_admitted = control_creator.incarnation;
	retention_thread_id = 1;
	retention_root_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	retention_second_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	memset(&canonical_aux_read, 0, sizeof(canonical_aux_read));
	memset(&retention_expected, 0, sizeof(retention_expected));
	retention_expected.system_identifier = 1234;
	retention_expected.storage_uuid[0] = 31;
	retention_expected.authority_uuid[0] = 41;
	retention_expected.origin_node_id = 0;
	retention_expected.origin_thread_id = 1;
	retention_expected.origin_owner_incarnation = control_creator.incarnation;
	retention_expected.thread_claim_created_at = 1001;
	retention_expected.thread_claim_crc32c = 4321;
	retention_expected.root_lineage_seq = 5;
	memset(&retention_image, 0, sizeof(retention_image));
	retention_image.header.format_version = 3;
	retention_image.header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	retention_image.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_OPEN;
	retention_image.header.v2.serving[0] = 1;
	retention_image.present[0] = true;
	retention_image.records[0].identity = retention_expected;
	retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	retention_image.records[0].root_publish_seq = 13;
	retention_image.record_crc32c[0] = 17;
	if (phase == 2)
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	entry_on_wait = retention_ready_after_ack;
}
static bool
retention_call(ClusterControlRootResult *result, bool *blocked,
			   ClusterControlRootSnapshot *snapshot, ClusterControlRootReadToken *token)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result
			= retention_phase == 0
				  ? read_retention_version(&retention_expected, snapshot, token, 3)
				  : cluster_control_root_v3_terminal_history_blocked(&retention_expected, blocked);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
UT_TEST(startup_retention_cf_slice_waits_for_original_completion)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootReadToken token;
		bool blocked = true;

		retention_fixture(phase);
		UT_ASSERT(retention_call(&result, &blocked, &snapshot, &token));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(retention_attempts, phase == 0 ? 2 : 3);
		UT_ASSERT_EQ(retention_threads, 1);
		UT_ASSERT_EQ(retention_controls, phase == 0 ? 0 : 1);
		UT_ASSERT_EQ(retention_histories, phase == 0 ? 0 : 1);
		if (phase == 0) {
			UT_ASSERT_EQ(
				memcmp(&snapshot.identity, &retention_expected, sizeof(retention_expected)), 0);
			UT_ASSERT_EQ(token.root_publish_seq, 13);
		} else
			UT_ASSERT(!blocked);
		cold_cleanup();
	}
}


static unsigned retention_fault;
static CfHoldState retention_saved_slot;
static ClusterControlRequestView retention_saved_record;
static void
retention_change_context(void)
{
	retention_ready_after_ack();
	retention_saved_slot = cf_hold_s;
	retention_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	switch (retention_fault) {
	case 0:
		MyProc = NULL;
		break;
	case 1:
		MyLatch = NULL;
		break;
	case 2:
		MyProcPid++;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		MyBackendType = B_BG_WRITER;
		break;
	case 5:
		owner_epoch++;
		break;
	case 6:
		owner_generation++;
		break;
	case 7:
		control_creator.incarnation++;
		break;
	case 8:
		entry_authority = false;
		break;
	case 9:
		ShutdownRequestPending = true;
		break;
	case 10:
		CritSectionCount = 1;
		break;
	case 11:
		entry_native_lock = true;
		break;
	case 12:
		InRecovery = !InRecovery;
		break;
	case 13:
		cluster_enabled = false;
		break;
	case 14:
		cluster_shared_config = false;
		break;
	case 15:
		cluster_controlfile_shared_authority = false;
		break;
	case 16:
		retention_member = false;
		break;
	case 17:
		retention_admitted++;
		break;
	case 18:
		retention_serving = false;
		break;
	case 19:
		retention_pending = true;
		break;
	case 20:
		retention_fence = false;
		break;
	case 21:
		retention_directory = false;
		break;
	case 22:
		retention_thread_id++;
		break;
	case 23:
		retention_prebump = true;
		break;
	case 24:
		cluster_node_id++;
		break;
	case 25:
		retention_expected.system_identifier++;
		break;
	case 26:
		retention_expected.storage_uuid[0]++;
		break;
	case 27:
		retention_expected.authority_uuid[0]++;
		break;
	case 28:
		retention_expected.origin_node_id++;
		break;
	case 29:
		retention_expected.origin_thread_id++;
		break;
	case 30:
		retention_expected.origin_owner_incarnation++;
		break;
	case 31:
		retention_expected.thread_claim_created_at++;
		break;
	case 32:
		retention_expected.thread_claim_crc32c++;
		break;
	case 33:
		retention_expected.root_lineage_seq++;
		break;
	case 34:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 35:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 36:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 37:
		cf_hold_s.release_pending = false;
		break;
	case 38:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 39:
		cf_hold_x.held = true;
		break;
	}
}
UT_TEST(startup_retention_wait_rejects_changed_actor_input_and_exact_request)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		for (retention_fault = 0; retention_fault < 40; retention_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			uint64 epoch = owner_epoch, generation = owner_generation;
			uint64 boot = control_creator.incarnation;
			int pid = MyProcPid;
			bool blocked = true;

			retention_fixture(phase);
			entry_on_wait = retention_change_context;
			UT_ASSERT(!retention_call(&result, &blocked, &snapshot, &token));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(retention_threads, phase == 2);
			UT_ASSERT_EQ(retention_controls | retention_histories, 0);
			UT_ASSERT_EQ(g_seven_count, claim_acquires + (phase == 2 ? 2 : 1));
			UT_ASSERT(blocked && token.root_publish_seq == 0);
			/* Restore injected corruption, never replace the actual ACK/poll. */
			MyProc = &owner_proc;
			MyLatch = &owner_proc.procLatch;
			MyProcPid = pid;
			CurrentResourceOwner = (ResourceOwner)1;
			MyBackendType = B_STARTUP;
			CritSectionCount = 0;
			entry_native_lock = false;
			cluster_node_id = 0;
			cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
			owner_epoch = epoch;
			owner_generation = generation;
			control_creator.incarnation = boot;
			cf_hold_s = retention_saved_slot;
			memset(&cf_hold_x, 0, sizeof(cf_hold_x));
			control_requests->records[cf_hold_s.owner.attempt.slot] = retention_saved_record;
			cold_cleanup();
		}
	}
}
static void
retention_change_after_grant(void)
{
	if (entry_waits != 0) {
		if (retention_fault == 0)
			retention_expected.thread_claim_crc32c++;
		else if (retention_fault == 1)
			retention_fence = false;
		else
			retention_pending = true;
	}
}
UT_TEST(startup_retention_new_grant_requalifies_before_root_io)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		for (retention_fault = 0; retention_fault < 3; retention_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			bool blocked = true;

			retention_fixture(phase);
			claim_after_acquire = retention_change_after_grant;
			UT_ASSERT(retention_call(&result, &blocked, &snapshot, &token));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(retention_threads, phase == 2);
			UT_ASSERT_EQ(retention_controls | retention_histories, 0);
			UT_ASSERT(blocked && token.root_publish_seq == 0);
			UT_ASSERT(!cluster_cf_held(ShareLock));
			cold_cleanup();
		}
	}
}
static void
retention_remove_receipt(void)
{
	retention_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ShareLock, claim_retiring_cookie));
	cf_retired_s = 0;
}
static void
retention_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
	UT_ASSERT_EQ(retention_threads, retention_phase == 2);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id++;
}
static void
retention_ack_during_reset(void)
{
	if (retention_phase != 2 || retention_attempts >= 2)
		cold_ack_during_reset();
}
UT_TEST(startup_retention_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			bool blocked = true;

			retention_fixture(phase);
			entry_lost_latch = true;
			if (fault == 0)
				entry_on_wait = retention_remove_receipt;
			else if (fault == 1)
				claim_on_reset = retention_ack_during_reset;
			else if (fault == 2) {
				fail_after_publication = false;
				entry_on_wait = retention_empty_grant;
			}
			UT_ASSERT_EQ(retention_call(&result, &blocked, &snapshot, &token), fault != 0);
			UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
			if (fault == 0) {
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(retention_threads, phase == 2);
				UT_ASSERT_EQ(retention_controls | retention_histories, 0);
				UT_ASSERT(blocked);
				cf_retired_s = claim_retiring_cookie;
			} else {
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
				UT_ASSERT_EQ(retention_threads, 1);
				UT_ASSERT_EQ(retention_histories, phase != 0);
			}
			cold_cleanup();
		}
	}
}
UT_TEST(startup_retention_hard_untyped_and_preexisting_requests_are_not_retried)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		for (unsigned fault = 0; fault < 6; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			bool blocked = true;
			unsigned before;

			retention_fixture(phase);
			if (fault == 0) {
				retention_second_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
				if (phase != 2)
					g_seven_result = retention_second_result;
			} else if (fault == 1)
				g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
			else if (fault == 2)
				entry_on_wait = entry_hard_error;
			else {
				if (fault == 4)
					g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
				else
					g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
				UT_ASSERT_EQ(cluster_cf_lock(fault == 5 ? ExclusiveLock : ShareLock), fault == 4);
			}
			before = g_seven_count;
			if (fault == 2) {
				UT_ASSERT(!retention_call(&result, &blocked, &snapshot, &token));
				UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
			} else {
				(void)retention_call(&result, &blocked, &snapshot, &token);
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			}
			UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
			UT_ASSERT_EQ(g_seven_count, before + (fault < 3 ? (phase == 2 ? 2 : 1) : 0));
			UT_ASSERT_EQ(retention_controls | retention_histories, 0);
			UT_ASSERT(blocked);
			if (fault == 4) {
				bool saved = owner_ack_on_wait;

				claim_teardown = owner_ack_on_wait = true;
				UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
				claim_teardown = false;
				owner_ack_on_wait = saved;
			}
			if (fault == 5) {
				cold_consumer = false;
				entry_cleanup(ExclusiveLock, cluster_cf_owner_cookie(ExclusiveLock));
			}
			cold_cleanup();
		}
	}
}
static void
retention_change_root(void)
{
	retention_ready_after_ack();
	if (retention_phase == 2)
		retention_image.records[0].root_publish_seq++;
	else if (retention_fault == 0)
		retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	else
		retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
UT_TEST(startup_retention_preserves_real_root_and_token_refusals)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		for (retention_fault = 0; retention_fault < 2; retention_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			bool blocked = true;

			retention_fixture(phase);
			entry_on_wait = retention_change_root;
			UT_ASSERT(retention_call(&result, &blocked, &snapshot, &token));
			/* A canonical observation accepts CLOSED; its history predicate
			 * is independently checked below. No authority is invented. */
			if (phase == 1 && retention_fault == 0) {
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
				UT_ASSERT(!blocked);
			} else {
				UT_ASSERT_EQ(result, phase == 0 && retention_fault == 0
										 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID
										 : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
				UT_ASSERT(blocked && token.root_publish_seq == 0);
				UT_ASSERT_EQ(retention_histories, 0);
			}
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(retention_attempts,
						 phase == 0 || (phase == 1 && retention_fault != 0) ? 2 : 3);
			cold_cleanup();
		}
	}
}
UT_TEST(startup_retention_original_cancel_keeps_shared_cleanup_responsibility)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot snapshot = { 0 };
		ClusterControlRootReadToken token = { 0 };
		bool blocked = true;
		volatile bool exited = false;

		retention_fixture(phase);
		entry_on_wait = entry_shutdown;
		startup_fixture_exit_hook = entry_owner_exit;
		startup_fixture_armed = true;
		if (sigsetjmp(startup_fixture_exit, 1) == 0)
			UT_ASSERT(!retention_call(&result, &blocked, &snapshot, &token));
		else
			exited = true;
		UT_ASSERT(exited && blocked);
		UT_ASSERT_EQ(startup_fixture_exit_code, 1);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(retention_threads, phase == 2);
		UT_ASSERT_EQ(retention_controls | retention_histories, 0);
		UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
		PG_exception_stack = NULL;
		error_context_stack = NULL;
		proc_exit_inprogress = shutdown_requested = false;
		startup_fixture_armed = false;
		startup_fixture_exit_hook = NULL;
		cold_cleanup();
	}
}
UT_TEST(ordinary_retention_and_observational_canonical_keep_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER };

	for (unsigned phase = 0; phase < 3; phase++) {
		for (unsigned role = 0; role < lengthof(roles); role++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot snapshot = { 0 };
			ClusterControlRootReadToken token = { 0 };
			bool blocked = true;

			retention_fixture(phase);
			MyBackendType = roles[role];
			UT_ASSERT(retention_call(&result, &blocked, &snapshot, &token));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(retention_attempts, phase == 2 ? 2 : 1);
			UT_ASSERT(blocked);
			MyBackendType = B_STARTUP;
			cold_cleanup();
		}
	}
	retention_fixture(1);
	{
		ClusterControlRootSnapshot snapshot = { 0 };
		ClusterControlRootReadToken token = { 0 };

		UT_ASSERT_EQ(
			cluster_control_root_v3_read_canonical(1, &retention_expected, &snapshot, &token),
			CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, 1);
		UT_ASSERT_EQ(snapshot.identity.system_identifier | token.root_publish_seq, 0);
	}
	cold_cleanup();
}

/* ROOT, claim and physical WAL are explicit leaf boundaries. The original
 * four recovery callers share the actual CF/CONTROL retirement producer. */
static unsigned recovery_phase, recovery_attempts, recovery_reads, recovery_wal_reads;
static ClusterLockAcquireResult recovery_acquire_result;
static ClusterControlRootResult recovery_root_result, recovery_wal_result;
static bool recovery_claim_valid;
static bool recovery_throw_read;
static void *recovery_work;
static void (*recovery_wal_hook)(void);
static ClusterControlRootSnapshot recovery_expected;
static ClusterControlRootSnapshot recovery_selected;
static ClusterControlRootReadToken recovery_token;
int wal_segment_size = 16777216;

static bool
recovery_second_read(void)
{
	return recovery_phase == 3 || recovery_phase == 5;
}
static unsigned
recovery_api(void)
{
	return recovery_phase < 2 ? recovery_phase : recovery_phase < 4 ? 2 : 3;
}
static bool
recovery_actual_cf_lock(LOCKMODE mode)
{
	++recovery_attempts;
	++g_next_request_id;
	if (recovery_attempts == (recovery_second_read() ? 2 : 1))
		g_seven_result = recovery_acquire_result;
	return claim_actual_cf_lock(mode);
}
static void *
recovery_alloc(Size size)
{
	recovery_work = palloc0(size);
	return recovery_work;
}
static ClusterControlRootResult
recovery_storage_check(const uint8 *uuid, bool probe)
{
	UT_ASSERT(uuid != NULL && uuid[0] == 31 && !probe);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static ClusterControlRootResult
read_thread_input_window(const ClusterControlRootIdentity *expected, ControlRootImage *root,
						 ControlFileData *native, ClusterControlRootFileToken *file, uint16 version,
						 bool native_checkpoint, uint8 *window)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(version, 3);
	UT_ASSERT_EQ(memcmp(expected, &recovery_selected.identity, sizeof(*expected)), 0);
	UT_ASSERT_EQ(native_checkpoint, recovery_api() == 1);
	++recovery_reads;
	if (recovery_throw_read)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("recovery ROOT read failed")));
	*root = retention_image;
	memset(native, 0, sizeof(*native));
	native->checkPoint = 100;
	native->checkPointCopy.redo = 80;
	native->checkPointCopy.ThisTimeLineID = 1;
	native->checkPointCopy.nextXid = FullTransactionIdFromU64(103);
	memset(file, 0, sizeof(*file));
	if (native_checkpoint) {
		UT_ASSERT(window != NULL);
		memset(window, 0x5a, 160);
	} else
		UT_ASSERT(window == NULL);
	return recovery_root_result;
}
bool
cluster_wal_claim_v2_ref_valid(const ClusterWalThreadClaimRefV2 *ref)
{
	UT_ASSERT_EQ(ref->database_incarnation, 7);
	UT_ASSERT_EQ(ref->max_config_generation, 9);
	UT_ASSERT_EQ(ref->claim_sha256[0], 3);
	return recovery_claim_valid;
}
static ClusterControlRootResult
recovery_physical_wal(const char *dir, const ClusterWalSourceRef *ref, int segment_size,
					  const ClusterControlRootSnapshot *sealed, XLogRecPtr checkpoint,
					  ClusterWalTailObservation *out)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(dir == cluster_wal_threads_dir && segment_size == wal_segment_size);
	UT_ASSERT_EQ(ref->timeline, 1);
	UT_ASSERT_EQ(checkpoint, 100);
	UT_ASSERT_EQ(memcmp(sealed, &recovery_selected, sizeof(*sealed)), 0);
	++recovery_wal_reads;
	memset(out, 0, sizeof(*out));
	out->complete_end = sealed->validated_tail_lsn_exclusive;
	out->last_record_start = sealed->tail_last_record_lsn;
	out->last_record_crc = sealed->tail_last_record_crc32c;
	out->records = 2;
	out->database_incarnation = 7;
	if (recovery_wal_hook != NULL)
		recovery_wal_hook();
	return recovery_wal_result;
}
ClusterControlRootResult
cluster_wal_tail_visit_sealed(const char *dir, const ClusterWalSourceRef *ref, int segment_size,
							  const ClusterControlRootSnapshot *sealed, XLogRecPtr checkpoint,
							  ClusterWalRecordVisitor visitor, void *arg,
							  ClusterWalTailObservation *out)
{
	UT_ASSERT(recovery_api() == 2 && visitor == NULL && arg == NULL);
	return recovery_physical_wal(dir, ref, segment_size, sealed, checkpoint, out);
}
ClusterControlRootResult
cluster_wal_tail_sync_sealed_v1(const char *dir, const ClusterWalSourceRef *ref, int segment_size,
								const ClusterControlRootSnapshot *sealed, XLogRecPtr checkpoint,
								ClusterWalTailObservation *out)
{
	UT_ASSERT_EQ(recovery_api(), 3);
	return recovery_physical_wal(dir, ref, segment_size, sealed, checkpoint, out);
}
#define cluster_cf_lock recovery_actual_cf_lock
#define palloc0 recovery_alloc
#define storage_contract_check recovery_storage_check
#include "test_cluster_recovery_wal_cf.inc"
#undef storage_contract_check
#undef palloc0
#undef cluster_cf_lock

typedef struct RecoveryTestOutput {
	ClusterWalSourceRef source;
	XLogRecPtr redo;
	ClusterWalRecoveryCheckpointV1 checkpoint;
	ClusterWalTailObservation tail;
} RecoveryTestOutput;

static void
recovery_ready_after_ack(void)
{
	UT_ASSERT_EQ(recovery_attempts, recovery_second_read() ? 2 : 1);
	UT_ASSERT_EQ(recovery_reads, recovery_second_read());
	UT_ASSERT_EQ(recovery_wal_reads, recovery_second_read());
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}
static void
recovery_fixture(unsigned phase)
{
	retention_fixture(0);
	recovery_phase = phase;
	recovery_attempts = recovery_reads = recovery_wal_reads = 0;
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	recovery_root_result = recovery_wal_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	recovery_claim_valid = true;
	recovery_throw_read = false;
	recovery_work = NULL;
	recovery_wal_hook = NULL;
	retention_image.header.v2.database_incarnation = 7;
	retention_image.header.v2.config_generation = 9;
	retention_image.refs[0].claim_sha256[0] = 3;
	retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	retention_image.records[0].checkpoint_tli = 1;
	retention_image.records[0].checkpoint_lower_lsn = 80;
	retention_image.records[0].validated_tail_lsn_exclusive = 340;
	retention_image.records[0].tail_last_record_lsn = 300;
	retention_image.records[0].tail_last_record_crc32c = 21;
	recovery_expected = retention_image.records[0];
	recovery_selected = recovery_expected;
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY, &recovery_token);
	if (recovery_second_read())
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	entry_on_wait = recovery_ready_after_ack;
}
static bool
recovery_call(ClusterControlRootResult *result, RecoveryTestOutput *out)
{
	volatile bool completed = false;

	memset(out, 0xa5, sizeof(*out));
	PG_TRY();
	{
		switch (recovery_api()) {
		case 0:
			*result = cluster_control_root_recovery_source_v1(&recovery_expected, &recovery_token,
															  &out->source, &out->redo);
			break;
		case 1:
			*result = cluster_control_root_recovery_checkpoint_v1(
				&recovery_expected, &recovery_token, &out->checkpoint);
			break;
		case 2:
			*result = cluster_control_root_recovery_visit(&recovery_expected, &recovery_token, NULL,
														  NULL, &out->tail);
			break;
		default:
			*result = cluster_control_root_recovery_sync_v1(&recovery_expected, &recovery_token,
															&out->tail);
		}
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
static void
recovery_no_output(const RecoveryTestOutput *out)
{
	RecoveryTestOutput zero = { 0 };

	if (recovery_api() == 0) {
		UT_ASSERT_EQ(memcmp(&out->source, &zero.source, sizeof(out->source)), 0);
		UT_ASSERT_EQ(out->redo, InvalidXLogRecPtr);
	} else if (recovery_api() == 1)
		UT_ASSERT_EQ(memcmp(&out->checkpoint, &zero.checkpoint, sizeof(out->checkpoint)), 0);
	else
		UT_ASSERT_EQ(memcmp(&out->tail, &zero.tail, sizeof(out->tail)), 0);
}
UT_TEST(startup_recovery_wal_cf_slice_waits_without_exporting_provisional_input)
{
	for (unsigned phase = 0; phase < 6; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		RecoveryTestOutput out;

		recovery_fixture(phase);
		UT_ASSERT(recovery_call(&result, &out));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(recovery_attempts, recovery_api() < 2 ? 2 : 3);
		UT_ASSERT_EQ(recovery_reads, recovery_api() < 2 ? 1 : 2);
		UT_ASSERT_EQ(recovery_wal_reads, recovery_api() < 2 ? 0 : 1);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (recovery_api() == 0) {
				UT_ASSERT_EQ(out.redo, 80);
				UT_ASSERT_EQ(out.source.claim.database_incarnation, 7);
			} else if (recovery_api() == 1) {
				UT_ASSERT_EQ(out.checkpoint.checkpoint_start, 100);
				UT_ASSERT_EQ(U64FromFullTransactionId(out.checkpoint.checkpoint.nextXid), 103);
				UT_ASSERT_EQ(out.checkpoint.native_window[0], 0x5a);
				UT_ASSERT_EQ(out.checkpoint.native_window[159], 0x5a);
			} else {
				UT_ASSERT_EQ(out.tail.complete_end, 340);
				UT_ASSERT_EQ(out.tail.database_incarnation, 7);
			}
		} else
			recovery_no_output(&out);
		cold_cleanup();
	}
}

static unsigned recovery_fault;
static CfHoldState recovery_saved_slot;
static ClusterControlRequestView recovery_saved_record;
static void
recovery_change_context(void)
{
	RecoveryWalVisitWork *work = recovery_work;

	switch (recovery_fault) {
	case 0:
		MyProc = NULL;
		break;
	case 1:
		MyLatch = NULL;
		break;
	case 2:
		MyProcPid++;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		MyBackendType = B_BG_WRITER;
		break;
	case 5:
		MyAuxProcType = CheckpointerProcess;
		break;
	case 6:
		cluster_node_id++;
		break;
	case 7:
		owner_epoch++;
		break;
	case 8:
		owner_generation++;
		break;
	case 9:
		control_creator.incarnation++;
		break;
	case 10:
		entry_authority = false;
		break;
	case 11:
		InRecovery = !InRecovery;
		break;
	case 12:
		ShutdownRequestPending = true;
		break;
	case 13:
		CritSectionCount = 1;
		break;
	case 14:
		entry_native_lock = true;
		break;
	case 15:
		cluster_enabled = false;
		break;
	case 16:
		cluster_shared_config = false;
		break;
	case 17:
		cluster_controlfile_shared_authority = false;
		break;
	case 18:
		work->expected.identity.system_identifier++;
		break;
	case 19:
		work->expected.identity.storage_uuid[0]++;
		break;
	case 20:
		work->expected.identity.authority_uuid[0]++;
		break;
	case 21:
		work->expected.identity.origin_node_id++;
		break;
	case 22:
		work->expected.identity.origin_thread_id++;
		break;
	case 23:
		work->expected.identity.origin_owner_incarnation++;
		break;
	case 24:
		work->expected.identity.thread_claim_created_at++;
		break;
	case 25:
		work->expected.identity.thread_claim_crc32c++;
		break;
	case 26:
		work->expected.identity.root_lineage_seq++;
		break;
	case 27:
		work->expected.root_publish_seq++;
		break;
	case 28:
		work->expected.checkpoint_tli++;
		break;
	case 29:
		work->expected.checkpoint_lower_lsn++;
		break;
	case 30:
		work->expected.validated_tail_lsn_exclusive++;
		break;
	case 31:
		work->expected.lifecycle++;
		break;
	case 32:
		work->token.file_txn_seq++;
		break;
	case 33:
		work->token.record_crc32c++;
		break;
	case 34:
		work->native_checkpoint = !work->native_checkpoint;
		break;
	case 35:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 36:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 37:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 38:
		control_requests->records[cf_hold_s.owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 39:
		control_requests->records[cf_hold_s.owner.attempt.slot].message.key.holder.cluster_epoch++;
		break;
	case 40:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 41:
		cf_hold_s.release_pending = false;
		break;
	case 42:
		cf_hold_s.owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
		break;
	case 43:
		cf_hold_s.owner.cooperative_acquire = true;
		break;
	case 44:
		cf_hold_x.held = true;
		break;
	}
}
static void
recovery_change_while_waiting(void)
{
	recovery_ready_after_ack();
	recovery_saved_slot = cf_hold_s;
	recovery_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	recovery_change_context();
}
static void
recovery_restore_actor(int pid, uint64 epoch, uint64 generation, uint64 boot)
{
	MyProc = &owner_proc;
	MyLatch = &owner_proc.procLatch;
	MyProcPid = pid;
	CurrentResourceOwner = (ResourceOwner)1;
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	cluster_node_id = 0;
	owner_epoch = epoch;
	owner_generation = generation;
	control_creator.incarnation = boot;
	entry_native_lock = false;
	CritSectionCount = 0;
	ShutdownRequestPending = false;
	cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
}
UT_TEST(startup_recovery_wal_wait_rechecks_original_actor_input_and_request)
{
	for (unsigned phase = 0; phase < 6; phase++)
		for (recovery_fault = 0; recovery_fault < 45; recovery_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;
			int pid = MyProcPid;
			uint64 epoch = owner_epoch, generation = owner_generation;
			uint64 boot = control_creator.incarnation;

			recovery_fixture(phase);
			entry_on_wait = recovery_change_while_waiting;
			UT_ASSERT(!recovery_call(&result, &out));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_level, FATAL);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(recovery_attempts, recovery_second_read() ? 2 : 1);
			UT_ASSERT_EQ(recovery_reads, recovery_second_read());
			UT_ASSERT_EQ(recovery_wal_reads, recovery_second_read());
			recovery_no_output(&out);
			recovery_restore_actor(pid, epoch, generation, boot);
			/* Restore the injected corruption; only the real ACK/poll retires. */
			cf_hold_s = recovery_saved_slot;
			control_requests->records[cf_hold_s.owner.attempt.slot] = recovery_saved_record;
			memset(&cf_hold_x, 0, sizeof(cf_hold_x));
			cold_cleanup();
		}
}
static void
recovery_change_after_grant(void)
{
	if (entry_waits != 0)
		recovery_change_context();
}
UT_TEST(startup_recovery_wal_new_grant_rechecks_before_root_io)
{
	const unsigned faults[] = { 7, 27, 32, 34 };

	for (unsigned phase = 0; phase < 6; phase++)
		for (unsigned i = 0; i < lengthof(faults); i++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;
			uint64 epoch = owner_epoch;

			recovery_fixture(phase);
			recovery_fault = faults[i];
			claim_after_acquire = recovery_change_after_grant;
			UT_ASSERT(recovery_call(&result, &out));
			/* CF itself rejects a grant from an already changed epoch. */
			UT_ASSERT_EQ(result, recovery_fault == 7 ? CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
													 : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(recovery_reads, recovery_second_read());
			UT_ASSERT_EQ(recovery_wal_reads, recovery_second_read());
			recovery_no_output(&out);
			owner_epoch = epoch;
			cold_cleanup();
		}
}
static void
recovery_remove_receipt(void)
{
	recovery_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ShareLock, claim_retiring_cookie));
	cf_retired_s = 0;
}
static void
recovery_ack_during_reset(void)
{
	if (recovery_attempts >= (recovery_second_read() ? 2 : 1))
		cold_ack_during_reset();
}
static void
recovery_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
	UT_ASSERT_EQ(recovery_reads, recovery_second_read());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
UT_TEST(startup_recovery_wal_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned phase = 0; phase < 6; phase++)
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;

			recovery_fixture(phase);
			entry_lost_latch = true;
			if (fault == 0)
				entry_on_wait = recovery_remove_receipt;
			else if (fault == 1)
				claim_on_reset = recovery_ack_during_reset;
			else if (fault == 2) {
				fail_after_publication = false;
				entry_on_wait = recovery_empty_grant;
			}
			UT_ASSERT_EQ(recovery_call(&result, &out), fault != 0);
			UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
			if (fault == 0) {
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(recovery_reads, recovery_second_read());
				recovery_no_output(&out);
				cf_retired_s = claim_retiring_cookie;
			} else {
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
				UT_ASSERT_EQ(recovery_reads, recovery_api() < 2 ? 1 : 2);
			}
			cold_cleanup();
		}
}
UT_TEST(startup_recovery_wal_hard_untyped_and_preexisting_requests_do_not_retry)
{
	for (unsigned phase = 0; phase < 6; phase++)
		for (unsigned fault = 0; fault < 6; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;
			unsigned before;

			recovery_fixture(phase);
			if (fault == 0)
				recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			else if (fault == 1)
				g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
			else if (fault == 2)
				entry_on_wait = entry_hard_error;
			else {
				g_seven_result = fault == 4 ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
											: CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
				UT_ASSERT_EQ(cluster_cf_lock(fault == 5 ? ExclusiveLock : ShareLock), fault == 4);
			}
			before = g_seven_count;
			if (fault == 2) {
				UT_ASSERT(!recovery_call(&result, &out));
				UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
			} else {
				UT_ASSERT(recovery_call(&result, &out));
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			}
			UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
			UT_ASSERT_EQ(g_seven_count,
						 before + (fault < 3 ? (recovery_second_read() ? 2 : 1) : 0));
			UT_ASSERT_EQ(recovery_reads, fault < 3 && recovery_second_read());
			recovery_no_output(&out);
			if (fault == 4) {
				bool saved = owner_ack_on_wait;

				claim_teardown = owner_ack_on_wait = true;
				UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
				claim_teardown = false;
				owner_ack_on_wait = saved;
			}
			if (fault == 5) {
				cold_consumer = false;
				entry_cleanup(ExclusiveLock, cluster_cf_owner_cookie(ExclusiveLock));
			}
			cold_cleanup();
		}
}
static void
recovery_change_root(void)
{
	recovery_ready_after_ack();
	switch (recovery_fault) {
	case 0:
		retention_image.records[0].root_publish_seq++;
		break;
	case 1:
		retention_image.records[0].validated_tail_lsn_exclusive++;
		break;
	case 2:
		retention_image.startup[0].generation = 1;
		break;
	case 3:
		recovery_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 4:
		recovery_claim_valid = false;
		break;
	case 5:
		recovery_throw_read = true;
		break;
	}
}
UT_TEST(startup_recovery_wal_real_root_refusals_and_errors_never_export_input)
{
	for (unsigned phase = 0; phase < 6; phase++)
		for (recovery_fault = 0; recovery_fault < 6; recovery_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;

			recovery_fixture(phase);
			entry_on_wait = recovery_change_root;
			UT_ASSERT_EQ(recovery_call(&result, &out), recovery_fault != 5);
			if (recovery_fault == 5)
				UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
			else
				UT_ASSERT_EQ(result, recovery_fault == 3   ? CLUSTER_CONTROL_ROOT_IO_ERROR
									 : recovery_fault == 4 ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
														   : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(recovery_reads, recovery_second_read() ? 2 : 1);
			UT_ASSERT_EQ(recovery_wal_reads, recovery_second_read());
			UT_ASSERT(!cluster_cf_held(ShareLock));
			recovery_no_output(&out);
			cold_cleanup();
		}
}
static void
recovery_change_wal_owner(void)
{
	recovery_change_context();
}
UT_TEST(startup_recovery_wal_second_interval_keeps_original_owner_and_failure)
{
	const unsigned faults[] = { 3, 7, 8 };

	for (unsigned phase = 2; phase < 6; phase++) {
		for (unsigned i = 0; i < lengthof(faults); i++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;
			uint64 epoch = owner_epoch, generation = owner_generation;

			recovery_fixture(phase);
			recovery_fault = faults[i];
			recovery_wal_hook = recovery_change_wal_owner;
			UT_ASSERT_EQ(recovery_call(&result, &out), !recovery_second_read());
			if (recovery_second_read()) {
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(entry_waits, 0);
			} else {
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
				UT_ASSERT_EQ(entry_waits, 3);
			}
			UT_ASSERT_EQ(recovery_attempts, 2);
			UT_ASSERT_EQ(recovery_reads, 1);
			UT_ASSERT_EQ(recovery_wal_reads, 1);
			recovery_no_output(&out);
			CurrentResourceOwner = (ResourceOwner)1;
			owner_generation = generation;
			/* The second request may have been created in the injected epoch.
			 * Retire it under its actual cut before restoring the fixture. */
			cold_cleanup();
			owner_epoch = epoch;
		}
		{
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;

			recovery_fixture(phase);
			recovery_wal_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			UT_ASSERT(recovery_call(&result, &out));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_IO_ERROR);
			UT_ASSERT_EQ(recovery_reads, 1);
			UT_ASSERT_EQ(recovery_attempts, recovery_second_read() ? 1 : 2);
			UT_ASSERT_EQ(entry_waits, recovery_second_read() ? 0 : 3);
			recovery_no_output(&out);
			cold_cleanup();
		}
	}
}
UT_TEST(startup_recovery_wal_original_cancel_keeps_shared_cleanup_responsibility)
{
	for (unsigned phase = 0; phase < 6; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		RecoveryTestOutput out;
		volatile bool exited = false;

		recovery_fixture(phase);
		entry_on_wait = entry_shutdown;
		startup_fixture_exit_hook = entry_owner_exit;
		startup_fixture_armed = true;
		if (sigsetjmp(startup_fixture_exit, 1) == 0)
			UT_ASSERT(!recovery_call(&result, &out));
		else
			exited = true;
		UT_ASSERT(exited);
		UT_ASSERT_EQ(startup_fixture_exit_code, 1);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(recovery_reads, recovery_second_read());
		UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
		recovery_no_output(&out);
		PG_exception_stack = NULL;
		error_context_stack = NULL;
		proc_exit_inprogress = shutdown_requested = false;
		startup_fixture_armed = false;
		startup_fixture_exit_hook = NULL;
		cold_cleanup();
	}
}
UT_TEST(ordinary_recovery_wal_roles_keep_the_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };

	for (unsigned phase = 0; phase < 6; phase++)
		for (unsigned role = 0; role < lengthof(roles); role++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			RecoveryTestOutput out;
			bool release_refused = roles[role] == B_LMON && (phase == 3 || phase == 5);

			recovery_fixture(phase);
			MyBackendType = roles[role];
			UT_ASSERT(recovery_call(&result, &out));
			/* LMON cannot block even for the original confirmed release. */
			UT_ASSERT_EQ(result, release_refused ? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN
												 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(recovery_attempts, recovery_second_read() && !release_refused ? 2 : 1);
			UT_ASSERT_EQ(recovery_reads, recovery_second_read());
			recovery_no_output(&out);
			MyBackendType = B_STARTUP;
			cold_cleanup();
		}
}


/* Actual CF/CONTROL plus the original self-seal S/X caller and authorizer.
 * Quorum/death, physical ROOT/WAL and WALR are explicit leaf facts. */
static unsigned self_phase, self_attempts, self_reads, self_tail_reads, self_publications;
static unsigned self_walr_begins, self_walr_ends, self_death_calls;
static bool self_quorum, self_death, self_throw_read, self_direct, self_tail_divergent;
static bool self_receipt_before_reapply;
static ClusterControlRootResult self_root_result, self_tail_result;
static ClusterLockAcquireResult self_acquire_result;
static ClusterRecoverySerialRequest self_request;
static void (*self_wal_hook)(void);
static int self_walr_identity;
static LOCKMODE
self_wait_mode(void)
{
	return self_phase == 0 ? ShareLock : ExclusiveLock;
}
bool
cluster_qvotec_in_quorum(void)
{
	return self_quorum;
}
bool
cluster_qvotec_prior_death_observe(uint32 node, uint64 prior, uint64 observing, uint64 now,
								   uint64 min_dead, ClusterQvotecPriorExitObservation *out)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(prior, self_request.duty.origin_owner_incarnation);
	UT_ASSERT_EQ(observing, control_creator.incarnation);
	UT_ASSERT(now != 0);
	UT_ASSERT(min_dead != 0 && out != NULL);
	memset(out, 0, sizeof(*out));
	++self_death_calls;
	return self_death;
}
static bool
self_unreachable_authority(void)
{
	/* No foreign/IR authority is available in this self-only fixture. */
	UT_ASSERT(false);
	return false;
}
ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *root,
											   bool require_sealed,
											   ClusterWalRootPublishGuard **out)
{
	UT_ASSERT_EQ(memcmp(root, &self_request.expected_root_token, sizeof(*root)), 0);
	UT_ASSERT(!require_sealed && *out == NULL);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++self_walr_begins;
	*out = (ClusterWalRootPublishGuard *)&self_walr_identity;
	return CLUSTER_WAL_PIN_OK;
}
ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT(*guard == (ClusterWalRootPublishGuard *)&self_walr_identity);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++self_walr_ends;
	*guard = NULL;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}
static ClusterControlRootResult
self_read_thread(const ClusterControlRootIdentity *expected, ControlRootImage *root,
				 ControlFileData *native, ClusterControlRootFileToken *file, uint16 version)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(self_direct	   ? self_wait_mode()
											 : self_reads == 0 ? ShareLock
															   : ExclusiveLock));
	UT_ASSERT_EQ(version, 3);
	UT_ASSERT_EQ(memcmp(expected, &self_request.duty, sizeof(*expected)), 0);
	++self_reads;
	if (self_throw_read)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("self-seal ROOT read failed")));
	*root = retention_image;
	memset(native, 0, sizeof(*native));
	native->checkPoint = 100;
	native->checkPointCopy.redo = 80;
	memset(file, 0, sizeof(*file));
	return self_root_result;
}
ClusterControlRootResult
cluster_wal_tail_observe_checkpoint(const char *dir, const ClusterWalSourceRef *ref,
									int segment_size, XLogRecPtr lower, XLogRecPtr upper,
									XLogRecPtr checkpoint, pg_crc32c crc,
									ClusterWalTailObservation *out)
{
	UT_ASSERT(dir == cluster_wal_threads_dir && segment_size == wal_segment_size);
	UT_ASSERT_EQ(memcmp(ref, &cold_restart, sizeof(*ref)), 0);
	UT_ASSERT_EQ(checkpoint, 100);
	UT_ASSERT_EQ(lower, 80);
	UT_ASSERT_EQ(upper, self_tail_reads == 0 ? 101 : 340);
	UT_ASSERT_EQ(crc, 21);
	UT_ASSERT_EQ(cluster_cf_held(ExclusiveLock), self_tail_reads != 0);
	if (self_tail_reads == 0)
		UT_ASSERT(!cluster_cf_held(ShareLock));
	++self_tail_reads;
	memset(out, 0, sizeof(*out));
	out->complete_end = 340;
	out->last_record_start = 300;
	out->last_record_crc = 23 + (self_tail_divergent && self_tail_reads == 2);
	if (self_wal_hook != NULL && self_tail_reads == 1)
		self_wal_hook();
	return self_tail_result;
}
static ClusterControlRootResult
self_encode(ControlRootImage *root, uint16 version)
{
	UT_ASSERT_EQ(version, 3);
	memset(root->bytes, (uint8)root->header.file_txn_seq, sizeof(root->bytes));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static bool
self_publish(const ControlRootImage *base, const ControlRootImage *next)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(base->header.file_txn_seq, retention_image.header.file_txn_seq);
	++self_publications;
	retention_image = *next;
	return true;
}
static bool
self_actual_cf_lock(LOCKMODE mode)
{
	++self_attempts;
	if (claim_retiring_cookie != 0 && mode == self_wait_mode()) {
		UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
		UT_ASSERT(cluster_cf_release_completed(mode, claim_retiring_cookie));
		UT_ASSERT(cluster_control_request_empty());
		self_receipt_before_reapply = true;
	}
	++g_next_request_id;
	if (self_attempts == (self_direct || self_phase == 0 ? 1 : 2))
		g_seven_result = self_acquire_result;
	return claim_actual_cf_lock(mode);
}
/* The foreign/fenced branch remains the exact product body but has no
 * authority fixture. Reaching it is a test failure, never fabricated proof. */
static bool
self_unavailable_digest(const ClusterRecoveryDutyKey *key, bool v2, ClusterRecoveryDutyDigest *out)
{
	(void)key;
	(void)v2;
	memset(out, 0, sizeof(*out));
	return self_unreachable_authority();
}
#define cluster_recovery_duty_digest_for_claim self_unavailable_digest
#define cluster_formation_witness_revalidate_nowait(...) self_unreachable_authority()
#define cluster_external_fence_need_set_revalidate_nowait(needs, formation, reason)                \
	((void)(reason), self_unreachable_authority())
#define cluster_external_fence_revalidate_set_nowait(...) self_unreachable_authority()
#define cluster_external_fence_need_set_count(...) ((uint32)self_unreachable_authority())
#define cluster_external_fence_need_set_at(...) ((const PgracExternalFenceNeedV1 *)NULL)
#define cluster_recovery_serial_input_revalidate(...) self_unreachable_authority()
#define cluster_recovery_serial_release(...) self_unreachable_authority()
#define cluster_cf_lock self_actual_cf_lock
#define read_thread_version self_read_thread
#define encode_extended_image self_encode
#define publish_updated_image self_publish
#include "test_cluster_self_seal_cf.inc"
#undef publish_updated_image
#undef encode_extended_image
#undef read_thread_version
#undef cluster_cf_lock
#undef cluster_recovery_serial_release
#undef cluster_recovery_serial_input_revalidate
#undef cluster_external_fence_need_set_at
#undef cluster_external_fence_need_set_count
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_recovery_duty_digest_for_claim

static FailureInputV2Work self_work;
static void
self_ready_after_ack(void)
{
	LOCKMODE mode = self_wait_mode();

	UT_ASSERT_EQ(self_attempts, self_phase == 0 ? 1 : 2);
	UT_ASSERT_EQ(self_reads, self_phase);
	UT_ASSERT_EQ(self_tail_reads, self_phase);
	UT_ASSERT(cluster_cf_held(mode) && !cluster_cf_held_is_usable(mode));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(mode);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}
static void
self_fixture(unsigned phase)
{
	retention_fixture(0);
	self_phase = phase;
	self_attempts = self_reads = self_tail_reads = self_publications = 0;
	self_walr_begins = self_walr_ends = self_death_calls = 0;
	self_quorum = self_death = enableFsync = true;
	self_throw_read = self_direct = self_tail_divergent = false;
	self_receipt_before_reapply = false;
	self_root_result = self_tail_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	self_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	self_wal_hook = NULL;
	retention_image.header.file_txn_seq = 7;
	retention_image.header.v2.database_incarnation = 7;
	retention_image.header.v2.config_generation = 9;
	retention_image.refs[0].claim_sha256[0] = 3;
	retention_image.records[0].identity.origin_owner_incarnation--;
	retention_image.records[0].checkpoint_tli = 1;
	retention_image.records[0].checkpoint_lower_lsn = 80;
	retention_image.records[0].checkpoint_record_crc32c = 21;
	retention_image.records[0].root_flags = CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
	memset(&cold_restart, 0, sizeof(cold_restart));
	cold_restart.claim.identity = retention_image.records[0].identity;
	cold_restart.claim.database_incarnation = 7;
	cold_restart.claim.max_config_generation = 9;
	cold_restart.claim.claim_sha256[0] = 3;
	cold_restart.timeline = 1;
	memset(&self_request, 0, sizeof(self_request));
	self_request.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	self_request.duty = cold_restart.claim.identity;
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY,
					&self_request.expected_root_token);
	memset(&self_work, 0, sizeof(self_work));
	self_work.format_version = 3;
	self_work.publisher_node = cluster_node_id;
	self_work.publisher_incarnation = control_creator.incarnation;
	self_work.self = true;
	self_work.self_min_dead_us = 3000000;
	self_work.self_restart = cold_restart;
	if (phase == 1)
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	entry_on_wait = self_ready_after_ack;
}
static bool
self_call(ClusterControlRootResult *result)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result = self_direct
					  ? failure_v2_self_open_read(&self_request, &self_work, self_wait_mode())
					  : failure_v2_self_seal_work(&self_request, &self_work);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
static void
self_cleanup(void)
{
	entry_on_wait = NULL;
	claim_after_acquire = NULL;
	claim_on_reset = NULL;
	claim_teardown = owner_ack_on_wait = true;
	for (unsigned i = 0; i < 2; i++) {
		LOCKMODE mode = i == 0 ? ShareLock : ExclusiveLock;

		if (cluster_cf_held_is_usable(mode))
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
	}
	(void)acknowledge_retirements();
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	self_work.cf_mode = NoLock;
	UT_ASSERT_EQ(failure_v2_cleanup(&self_work, CLUSTER_CONTROL_ROOT_OK_PRIMARY),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	claim_teardown = owner_ack_on_wait = false;
	cold_consumer = false;
	UT_ASSERT(cluster_control_request_empty());
}
UT_TEST(startup_self_seal_cf_slice_waits_for_exact_completion_in_both_intervals)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		self_fixture(phase);
		UT_ASSERT(self_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(self_attempts, 3);
		UT_ASSERT_EQ(self_reads, 3);
		UT_ASSERT_EQ(self_tail_reads, 2);
		UT_ASSERT_EQ(self_publications, 1);
		UT_ASSERT(self_death_calls > 0);
		UT_ASSERT(self_receipt_before_reapply);
		UT_ASSERT_EQ(retention_image.records[0].lifecycle,
					 CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
		UT_ASSERT(retention_image.records[0].root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF);
		UT_ASSERT_EQ(retention_image.header.v2.serving[0], 0);
		self_cleanup();
		UT_ASSERT_EQ(self_walr_begins, 1);
		UT_ASSERT_EQ(self_walr_ends, 1);
	}
}


static unsigned self_fault;
static CfHoldState self_saved_slot;
static ClusterControlRequestView self_saved_record;
static void
self_change_context(void)
{
	CfHoldState *slot = cf_slot(self_wait_mode());

	switch (self_fault) {
	case 0:
		MyProcPid++;
		break;
	case 1:
		MyProc = NULL;
		break;
	case 2:
		MyLatch = NULL;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		MyBackendType = B_BG_WORKER;
		break;
	case 5:
		MyAuxProcType = BgWriterProcess;
		break;
	case 6:
		cluster_node_id = 1;
		break;
	case 7:
		owner_epoch++;
		break;
	case 8:
		owner_generation++;
		break;
	case 9:
		control_creator.incarnation++;
		break;
	case 10:
		InRecovery = !InRecovery;
		break;
	case 11:
		entry_authority = false;
		break;
	case 12:
		cluster_shared_config = false;
		break;
	case 13:
		cluster_controlfile_shared_authority = false;
		break;
	case 14:
		cluster_enabled = false;
		break;
	case 15:
		ShutdownRequestPending = true;
		break;
	case 16:
		entry_native_lock = true;
		break;
	case 17:
		CritSectionCount = 1;
		break;
	case 18:
		self_work.self = false;
		break;
	case 19:
		self_request.mode++;
		break;
	case 20:
		self_request.duty.thread_claim_crc32c++;
		break;
	case 21:
		self_request.expected_root_token.file_txn_seq++;
		break;
	case 22:
		self_work.self_min_dead_us++;
		break;
	case 23:
		self_work.publisher_node++;
		break;
	case 24:
		self_work.publisher_incarnation++;
		break;
	case 25:
		self_work.self_restart.claim.claim_sha256[0]++;
		break;
	case 26:
		cold_restart.claim.claim_sha256[0]++;
		break;
	case 27:
		self_work.walr = NULL;
		break;
	case 28:
		self_work.format_version++;
		break;
	case 29:
		self_quorum = false;
		break;
	case 30:
		retention_member = false;
		break;
	case 31:
		retention_admitted++;
		break;
	case 32:
		self_death = false;
		break;
	case 33:
		self_work.self_restart.timeline++;
		break;
	case 34:
		slot->owner.request.control_owner_id++;
		break;
	case 35:
		slot->owner.attempt.generation++;
		break;
	case 36:
		control_requests->records[slot->owner.attempt.slot].owner.incarnation++;
		break;
	case 37:
		control_requests->records[slot->owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 38:
		control_requests->records[slot->owner.attempt.slot].message.key.holder.cluster_epoch++;
		break;
	case 39:
		control_requests->records[slot->owner.attempt.slot].state = CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 40:
		slot->release_pending = false;
		break;
	case 41:
		slot->owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
		break;
	case 42:
		slot->owner.cooperative_acquire = true;
		break;
	case 43:
		cf_slot(self_wait_mode() == ShareLock ? ExclusiveLock : ShareLock)->held = true;
		break;
	case 44:
		enableFsync = false;
		break;
	}
}
static void
self_change_while_waiting(void)
{
	self_ready_after_ack();
	self_saved_slot = *cf_slot(self_wait_mode());
	self_saved_record = control_requests->records[self_saved_slot.owner.attempt.slot];
	self_change_context();
}
UT_TEST(startup_self_seal_wait_rechecks_original_actor_purpose_and_request)
{
	for (unsigned phase = 0; phase < 2; phase++)
		for (self_fault = 0; self_fault < 45; self_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			int pid = MyProcPid;
			uint64 epoch = owner_epoch, generation = owner_generation;
			uint64 boot = control_creator.incarnation;

			self_fixture(phase);
			entry_on_wait = self_change_while_waiting;
			UT_ASSERT(!self_call(&result));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_level, FATAL);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(self_reads, phase);
			UT_ASSERT_EQ(self_tail_reads, phase);
			UT_ASSERT_EQ(self_publications, 0);
			recovery_restore_actor(pid, epoch, generation, boot);
			*cf_slot(self_wait_mode()) = self_saved_slot;
			control_requests->records[self_saved_slot.owner.attempt.slot] = self_saved_record;
			memset(cf_slot(self_wait_mode() == ShareLock ? ExclusiveLock : ShareLock), 0,
				   sizeof(CfHoldState));
			self_work.walr = (ClusterWalRootPublishGuard *)&self_walr_identity;
			self_cleanup();
		}
}
static void
self_change_after_grant(void)
{
	if (entry_waits != 0)
		self_change_context();
}
UT_TEST(startup_self_seal_new_grant_requalifies_before_root_io)
{
	const unsigned faults[] = { 3, 7, 21, 25, 27, 29, 32 };

	for (unsigned phase = 0; phase < 2; phase++)
		for (unsigned i = 0; i < lengthof(faults); i++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			uint64 epoch = owner_epoch;

			self_fixture(phase);
			self_fault = faults[i];
			claim_after_acquire = self_change_after_grant;
			UT_ASSERT(self_call(&result));
			UT_ASSERT_EQ(result, self_fault == 7 ? CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
												 : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(self_reads, phase);
			UT_ASSERT_EQ(self_tail_reads, phase);
			UT_ASSERT_EQ(self_publications, 0);
			CurrentResourceOwner = (ResourceOwner)1;
			owner_epoch = epoch;
			self_work.walr = (ClusterWalRootPublishGuard *)&self_walr_identity;
			self_cleanup();
		}
}
static void
self_remove_receipt(void)
{
	self_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(self_wait_mode(), claim_retiring_cookie));
	if (self_wait_mode() == ShareLock)
		cf_retired_s = 0;
	else
		cf_retired_x = 0;
}
static void
self_ack_during_reset(void)
{
	if (self_attempts != (self_phase == 0 ? 1 : 2) || !cluster_cf_held(self_wait_mode()))
		return;
	claim_retiring_cookie = cluster_cf_owner_cookie(self_wait_mode());
	UT_ASSERT(acknowledge_retirements());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
static void
self_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(self_reads, self_phase);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
UT_TEST(startup_self_seal_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned phase = 0; phase < 2; phase++)
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			self_fixture(phase);
			entry_lost_latch = true;
			if (fault == 0)
				entry_on_wait = self_remove_receipt;
			else if (fault == 1)
				claim_on_reset = self_ack_during_reset;
			else if (fault == 2) {
				fail_after_publication = false;
				entry_on_wait = self_empty_grant;
			}
			UT_ASSERT_EQ(self_call(&result), fault != 0);
			UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
			if (fault == 0) {
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(self_reads, phase);
				UT_ASSERT_EQ(self_publications, 0);
			} else {
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
				UT_ASSERT_EQ(self_publications, 1);
			}
			self_cleanup();
		}
}
UT_TEST(startup_self_seal_hard_untyped_and_preexisting_requests_do_not_retry)
{
	for (unsigned phase = 0; phase < 2; phase++)
		for (unsigned fault = 0; fault < 7; fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			unsigned before;
			LOCKMODE preexisting = fault == 3 || fault == 5 ? ShareLock : ExclusiveLock;
			bool foreign = fault >= 3 && preexisting != (phase == 0 ? ShareLock : ExclusiveLock);

			self_fixture(phase);
			if (fault == 0)
				self_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			else if (fault == 1)
				g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
			else if (fault == 2)
				entry_on_wait = entry_hard_error;
			else {
				g_seven_result = fault >= 5 ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
											: CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
				UT_ASSERT_EQ(cluster_cf_lock(preexisting), fault >= 5);
				self_direct = true;
				self_work.walr = (ClusterWalRootPublishGuard *)&self_walr_identity;
			}
			before = g_seven_count;
			if (fault == 2 || foreign) {
				UT_ASSERT(!self_call(&result));
				UT_ASSERT_EQ(entry_code,
							 foreign ? ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED : ERRCODE_IO_ERROR);
			} else {
				UT_ASSERT(self_call(&result));
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			}
			UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
			UT_ASSERT_EQ(g_seven_count, before
											+ (foreign	   ? 1
											   : fault < 3 ? (phase == 0 ? 1 : 2)
														   : 0));
			UT_ASSERT_EQ(self_reads, fault < 3 ? phase : 0);
			UT_ASSERT_EQ(self_publications, 0);
			self_cleanup();
		}
}
static void
self_change_root(void)
{
	self_ready_after_ack();
	if (self_fault == 0)
		retention_image.records[0].root_publish_seq++;
	else if (self_fault == 1)
		self_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	else if (self_fault == 2)
		self_throw_read = true;
	else if (self_fault == 3)
		self_tail_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	else
		self_tail_divergent = true;
}
UT_TEST(startup_self_seal_real_root_and_wal_refusals_remain_errors)
{
	for (unsigned phase = 0; phase < 2; phase++)
		for (self_fault = 0; self_fault < 5; self_fault++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			const ClusterControlRootResult expected[]
				= { CLUSTER_CONTROL_ROOT_CAS_CONFLICT, CLUSTER_CONTROL_ROOT_IO_ERROR,
					CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_IO_ERROR,
					CLUSTER_CONTROL_ROOT_COPY_DIVERGENT };

			self_fixture(phase);
			entry_on_wait = self_change_root;
			UT_ASSERT_EQ(self_call(&result), self_fault != 2);
			if (self_fault != 2)
				UT_ASSERT_EQ(result, expected[self_fault]);
			else
				UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(self_publications, 0);
			self_cleanup();
		}
}
static void
self_change_owner_outside_cf(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	CurrentResourceOwner = (ResourceOwner)2;
}
UT_TEST(startup_self_seal_second_interval_keeps_the_original_owner)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

	self_fixture(1);
	self_wal_hook = self_change_owner_outside_cf;
	UT_ASSERT(!self_call(&result));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(self_reads, 1);
	UT_ASSERT_EQ(self_tail_reads, 1);
	UT_ASSERT_EQ(self_publications, 0);
	CurrentResourceOwner = (ResourceOwner)1;
	self_cleanup();
}
UT_TEST(startup_self_seal_original_cancel_keeps_shared_cleanup_responsibility)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		volatile bool exited = false;

		self_fixture(phase);
		entry_on_wait = entry_shutdown;
		startup_fixture_exit_hook = entry_owner_exit;
		startup_fixture_armed = true;
		if (sigsetjmp(startup_fixture_exit, 1) == 0)
			UT_ASSERT(!self_call(&result));
		else
			exited = true;
		UT_ASSERT(exited);
		UT_ASSERT_EQ(startup_fixture_exit_code, 1);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(self_reads, phase);
		UT_ASSERT_EQ(self_publications, 0);
		UT_ASSERT(
			control_requests->records[cf_slot(self_wait_mode())->owner.attempt.slot].owner_exited);
		PG_exception_stack = NULL;
		error_context_stack = NULL;
		proc_exit_inprogress = shutdown_requested = false;
		startup_fixture_armed = false;
		startup_fixture_exit_hook = NULL;
		self_cleanup();
	}
}
UT_TEST(ordinary_self_open_read_keeps_the_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };

	for (unsigned phase = 0; phase < 2; phase++)
		for (unsigned role = 0; role < lengthof(roles); role++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			self_fixture(phase);
			self_direct = true;
			self_work.walr = (ClusterWalRootPublishGuard *)&self_walr_identity;
			MyBackendType = roles[role];
			UT_ASSERT(self_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(self_reads | self_publications, 0);
			MyBackendType = B_STARTUP;
			self_cleanup();
		}
}


/* The actual CF/owner/CONTROL producer joins the original failure publishers.
 * Formation/fence/IR/WALR and physical ROOT/WAL are explicit fixture leaves. */
static unsigned failure_kind, failure_phase, failure_attempts, failure_reads;
static unsigned failure_wal_reads, failure_publishes, failure_ir_acquires, failure_ir_releases;
static unsigned failure_walr_begins, failure_walr_ends, failure_need_count;
static bool failure_formation, failure_needs, failure_admissions, failure_ir_current;
static bool failure_digest_valid, failure_null_need, failure_direct, failure_throw_read;
static bool failure_tail_divergent, failure_receipt_before_reapply;
static ClusterRecoverySerialRequest failure_request;
static ClusterRecoverySerialGuard failure_serial;
static PgracExternalFenceNeedV1 failure_need;
static ClusterControlRootResult failure_root_result, failure_wal_result;
static ClusterLockAcquireResult failure_acquire_result;
static ClusterRecoverySerialAcquireResult failure_ir_result;
static void (*failure_wal_hook)(void);
static int failure_walr_identity;
static LOCKMODE
failure_wait_mode(void)
{
	return failure_kind == 0 || failure_phase == 1 ? ExclusiveLock : ShareLock;
}
static unsigned
failure_first_attempt(void)
{
	return failure_direct || failure_kind == 0 || failure_phase == 0 ? 1 : 2;
}
static bool
failure_digest(const ClusterRecoveryDutyKey *key, bool v2, ClusterRecoveryDutyDigest *out)
{
	UT_ASSERT(key != NULL && v2 && out != NULL);
	memset(out, 0x2a, sizeof(*out));
	return failure_digest_valid;
}
static ClusterFormationWitnessResult
failure_formation_current(const ClusterFormationWitnessV1 *witness)
{
	UT_ASSERT(witness == (const ClusterFormationWitnessV1 *)1);
	return failure_formation ? CLUSTER_FORMATION_WITNESS_READY : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}
static bool
failure_needs_current(const PgracExternalFenceNeedSetV1 *needs,
					  const ClusterFormationWitnessV1 *formation,
					  PgracExternalFenceDenyReason *reason)
{
	UT_ASSERT(needs == (const PgracExternalFenceNeedSetV1 *)2);
	UT_ASSERT(formation == (const ClusterFormationWitnessV1 *)1);
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return failure_needs;
}
static bool
failure_admissions_current(const PgracExternalFenceAdmissionSetV1 *admissions,
						   const PgracExternalFenceNeedSetV1 *needs,
						   const ClusterFormationWitnessV1 *formation,
						   PgracExternalFenceDenyReason *reason)
{
	UT_ASSERT(admissions == (const PgracExternalFenceAdmissionSetV1 *)3);
	UT_ASSERT(needs == (const PgracExternalFenceNeedSetV1 *)2);
	UT_ASSERT(formation == (const ClusterFormationWitnessV1 *)1);
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return failure_admissions;
}
static uint32
failure_count(const PgracExternalFenceNeedSetV1 *needs)
{
	UT_ASSERT(needs == (const PgracExternalFenceNeedSetV1 *)2);
	return failure_need_count;
}
static const PgracExternalFenceNeedV1 *
failure_at(const PgracExternalFenceNeedSetV1 *needs, uint32 index)
{
	UT_ASSERT(needs == (const PgracExternalFenceNeedSetV1 *)2 && index == 0);
	return failure_null_need ? NULL : &failure_need;
}
static ClusterRecoverySerialAcquireResult
failure_ir_acquire(const ClusterRecoverySerialRequest *request, ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT_EQ(failure_kind, 1);
	UT_ASSERT_EQ(memcmp(request, &failure_request, sizeof(*request)), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++failure_ir_acquires;
	if (failure_ir_result == CLUSTER_RECOVERY_SERIAL_GRANTED)
		*guard = failure_serial;
	return failure_ir_result;
}
static ClusterRecoverySerialRevalidateResult
failure_ir_revalidate(ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT_EQ(failure_kind, 1);
	return failure_ir_current && guard->held && !guard->release_uncertain
				   && memcmp(guard, &failure_serial, sizeof(*guard)) == 0
			   ? CLUSTER_RECOVERY_SERIAL_CURRENT
			   : CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
}
static ClusterRecoverySerialReleaseResult
failure_ir_release(ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT_EQ(failure_kind, 1);
	UT_ASSERT_EQ(memcmp(guard, &failure_serial, sizeof(*guard)), 0);
	++failure_ir_releases;
	memset(guard, 0, sizeof(*guard));
	return CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
}
static ClusterWalPinResult
failure_walr_begin(const ClusterControlRootReadToken *token, bool sealed,
				   ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT_EQ(memcmp(token, &failure_request.expected_root_token, sizeof(*token)), 0);
	UT_ASSERT(!sealed && *guard == NULL);
	++failure_walr_begins;
	*guard = (ClusterWalRootPublishGuard *)&failure_walr_identity;
	return CLUSTER_WAL_PIN_OK;
}
static ClusterWalrReleaseResult
failure_walr_end(ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT(*guard == (ClusterWalRootPublishGuard *)&failure_walr_identity);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	++failure_walr_ends;
	*guard = NULL;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}
static ClusterControlRootResult
failure_read(const ClusterControlRootIdentity *expected, ControlRootImage *root,
			 ControlFileData *native, ClusterControlRootFileToken *file, uint16 version)
{
	LOCKMODE mode = failure_direct							  ? failure_wait_mode()
					: failure_kind == 0 || failure_reads != 0 ? ExclusiveLock
															  : ShareLock;

	UT_ASSERT(cluster_cf_held_is_clusterwide(mode));
	UT_ASSERT_EQ(version, 3);
	UT_ASSERT_EQ(memcmp(expected, &failure_request.duty, sizeof(*expected)), 0);
	++failure_reads;
	if (failure_throw_read)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("failure input ROOT read failed")));
	*root = retention_image;
	memset(native, 0, sizeof(*native));
	native->checkPoint = 100;
	memset(file, 0, sizeof(*file));
	return failure_root_result;
}
static ClusterControlRootResult
failure_tail(const char *dir, const ClusterWalSourceRef *ref, int segment_size, XLogRecPtr lower,
			 XLogRecPtr upper, XLogRecPtr checkpoint, pg_crc32c crc, ClusterWalTailObservation *out)
{
	UT_ASSERT(failure_kind != 0);
	UT_ASSERT(dir == cluster_wal_threads_dir && segment_size == wal_segment_size);
	UT_ASSERT_EQ(memcmp(ref, &cold_restart, sizeof(*ref)), 0);
	UT_ASSERT_EQ(lower, 80);
	UT_ASSERT_EQ(upper, failure_wal_reads == 0 ? 101 : 340);
	UT_ASSERT_EQ(checkpoint, 100);
	UT_ASSERT_EQ(crc, 21);
	UT_ASSERT_EQ(cluster_cf_held(ExclusiveLock), failure_wal_reads != 0);
	if (failure_wal_reads == 0)
		UT_ASSERT(!cluster_cf_held(ShareLock));
	++failure_wal_reads;
	memset(out, 0, sizeof(*out));
	out->complete_end = 340;
	out->last_record_start = 300;
	out->last_record_crc = 23 + (failure_tail_divergent && failure_wal_reads == 2);
	if (failure_wal_hook != NULL && failure_wal_reads == 1)
		failure_wal_hook();
	return failure_wal_result;
}
static bool
failure_publish(const ControlRootImage *base, const ControlRootImage *next)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(base->header.file_txn_seq, retention_image.header.file_txn_seq);
	++failure_publishes;
	retention_image = *next;
	return true;
}
static bool
failure_actual_cf_lock(LOCKMODE mode)
{
	++failure_attempts;
	if (claim_retiring_cookie != 0 && mode == failure_wait_mode()) {
		UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
		UT_ASSERT(cluster_cf_release_completed(mode, claim_retiring_cookie));
		UT_ASSERT(cluster_control_request_empty());
		failure_receipt_before_reapply = true;
	}
	++g_next_request_id;
	if (failure_attempts == failure_first_attempt())
		g_seven_result = failure_acquire_result;
	return claim_actual_cf_lock(mode);
}
/* Compile the original authorizer again against explicit fenced/IR leaves;
 * the earlier self-only copy still has unavailable foreign/IR authority. */
#define failure_v2_authorized failure_authorized
#define failure_v2_current failure_current
#define failure_v2_cleanup failure_cleanup
#define cluster_recovery_duty_digest_for_claim failure_digest
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#define cluster_external_fence_need_set_count failure_count
#define cluster_external_fence_need_set_at failure_at
#define cluster_recovery_serial_acquire failure_ir_acquire
#define cluster_recovery_serial_input_revalidate failure_ir_revalidate
#define cluster_recovery_serial_release failure_ir_release
#define cluster_wal_retention_root_publish_begin_exact failure_walr_begin
#define cluster_wal_retention_root_publish_end failure_walr_end
#define cluster_cf_lock failure_actual_cf_lock
#define read_thread_version failure_read
#define encode_extended_image self_encode
#define publish_updated_image failure_publish
#define cluster_wal_tail_observe_checkpoint failure_tail
#include "test_cluster_failure_input_cf.inc"
#undef cluster_wal_tail_observe_checkpoint
#undef publish_updated_image
#undef encode_extended_image
#undef read_thread_version
#undef cluster_cf_lock
#undef cluster_wal_retention_root_publish_end
#undef cluster_wal_retention_root_publish_begin_exact
#undef cluster_recovery_serial_release
#undef cluster_recovery_serial_input_revalidate
#undef cluster_recovery_serial_acquire
#undef cluster_external_fence_need_set_at
#undef cluster_external_fence_need_set_count
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_recovery_duty_digest_for_claim
#undef failure_v2_cleanup
#undef failure_v2_current
#undef failure_v2_authorized
static FailureInputV2Work failure_work;
static void
failure_ready_after_ack(void)
{
	UT_ASSERT_EQ(failure_attempts, failure_first_attempt());
	UT_ASSERT_EQ(failure_reads, failure_kind != 0 ? failure_phase : 0);
	UT_ASSERT_EQ(failure_wal_reads, failure_kind != 0 ? failure_phase : 0);
	UT_ASSERT(cluster_cf_held(failure_wait_mode())
			  && !cluster_cf_held_is_usable(failure_wait_mode()));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(failure_wait_mode());
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}
static void
failure_fixture(unsigned kind, unsigned phase)
{
	self_fixture(phase);
	failure_kind = kind;
	failure_phase = phase;
	failure_attempts = failure_reads = failure_wal_reads = failure_publishes = 0;
	failure_ir_acquires = failure_ir_releases = failure_walr_begins = failure_walr_ends = 0;
	failure_formation = failure_needs = failure_admissions = failure_ir_current = true;
	failure_digest_valid = true;
	failure_null_need = failure_direct = failure_throw_read = false;
	failure_tail_divergent = failure_receipt_before_reapply = false;
	failure_need_count = 1;
	failure_root_result = failure_wal_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	failure_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	failure_ir_result = CLUSTER_RECOVERY_SERIAL_GRANTED;
	failure_wal_hook = NULL;
	retention_image.records[0].root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	if (kind != 0) {
		retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		retention_image.header.v2.serving[0] = 0;
	}
	failure_request = self_request;
	if (kind != 2) {
		failure_request.formation = (const ClusterFormationWitnessV1 *)1;
		failure_request.fence_need_set = (const PgracExternalFenceNeedSetV1 *)2;
		failure_request.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)3;
		failure_request.acquire_timeout_ms = failure_request.release_timeout_ms = 1000;
	}
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY,
					&failure_request.expected_root_token);
	memset(&failure_need, 0, sizeof(failure_need));
	failure_need.system_identifier = failure_request.duty.system_identifier;
	failure_need.victim_node_id = failure_request.duty.origin_node_id;
	failure_need.victim_incarnation = failure_request.duty.origin_owner_incarnation;
	memset(&failure_need.canonical_duty_digest, 0x2a, sizeof(failure_need.canonical_duty_digest));
	memset(&failure_serial, 0, sizeof(failure_serial));
	failure_serial.held = true;
	failure_serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	failure_serial.duty = failure_request.duty;
	failure_serial.root_read_token = failure_request.expected_root_token;
	failure_serial.formation = failure_request.formation;
	failure_serial.fence_need_set = failure_request.fence_need_set;
	failure_serial.fence_admission_set = failure_request.fence_admission_set;
	failure_serial.release_timeout_ms = 1000;
	failure_serial.lock_request.request_id = 123456;
	memset(&failure_work, 0, sizeof(failure_work));
	failure_work.format_version = 3;
	failure_work.publisher_node = cluster_node_id;
	failure_work.publisher_incarnation = control_creator.incarnation;
	failure_work.self = kind == 2;
	failure_work.self_restart = cold_restart;
	failure_work.self_min_dead_us = 3000000;
	if (kind == 0)
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	entry_on_wait = failure_ready_after_ack;
}
static bool
failure_call(ClusterControlRootResult *result)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result = failure_kind == 0 ? failure_v2_open_work(&failure_request, &failure_work)
				  : failure_direct
					  ? failure_v2_tail_read(&failure_request, &failure_work, failure_wait_mode())
					  : failure_v2_tail_work(&failure_request, &failure_work);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
static void
failure_finish(void)
{
	entry_on_wait = NULL;
	claim_after_acquire = claim_on_reset = NULL;
	claim_teardown = owner_ack_on_wait = true;
	/* Test-only foreign holds are not owned by the publisher's cleanup. */
	for (unsigned i = 0; i < 2; i++) {
		LOCKMODE mode = i == 0 ? ShareLock : ExclusiveLock;

		if (mode != failure_work.cf_mode && cluster_cf_held_is_usable(mode))
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
	}
	UT_ASSERT_EQ(failure_cleanup(&failure_work, CLUSTER_CONTROL_ROOT_OK_PRIMARY),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	for (unsigned i = 0; i < 2; i++) {
		LOCKMODE mode = i == 0 ? ShareLock : ExclusiveLock;

		if (cluster_cf_held_is_usable(mode))
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
	}
	(void)acknowledge_retirements();
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	claim_teardown = owner_ack_on_wait = false;
	cold_consumer = false;
}
UT_TEST(startup_failure_input_cf_slice_waits_with_original_authority)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			failure_fixture(kind, phase);
			UT_ASSERT(failure_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(failure_attempts, kind == 0 ? 2 : 3);
			UT_ASSERT_EQ(failure_reads, kind == 0 ? 2 : 3);
			UT_ASSERT_EQ(failure_wal_reads, kind == 0 ? 0 : 2);
			UT_ASSERT_EQ(failure_publishes, 1);
			UT_ASSERT(failure_receipt_before_reapply);
			UT_ASSERT_EQ(retention_image.records[0].lifecycle,
						 CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
			UT_ASSERT(retention_image.records[0].root_flags
					  & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF);
			UT_ASSERT_EQ(retention_image.header.v2.serving[0], 0);
			failure_finish();
			UT_ASSERT_EQ(failure_ir_acquires, kind == 1);
			UT_ASSERT_EQ(failure_ir_releases, kind == 1);
			UT_ASSERT_EQ(failure_walr_begins, 1);
			UT_ASSERT_EQ(failure_walr_ends, 1);
		}
}


static unsigned failure_fault;
static CfHoldState failure_saved_slot;
static ClusterControlRequestView failure_saved_record;
static bool
failure_fault_applies(unsigned fault)
{
	if (fault == 26 || (fault >= 45 && fault <= 48))
		return failure_kind == 2;
	if ((fault >= 29 && fault <= 31) || (fault >= 39 && fault <= 44))
		return failure_kind != 2;
	if (fault == 32 || fault == 33)
		return failure_kind == 1;
	return true;
}
static void
failure_change_context(void)
{
	CfHoldState *slot = cf_slot(failure_wait_mode());

	switch (failure_fault) {
	case 0:
		MyProcPid++;
		break;
	case 1:
		MyProc = NULL;
		break;
	case 2:
		MyLatch = NULL;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		MyBackendType = B_BG_WORKER;
		break;
	case 5:
		MyAuxProcType = BgWriterProcess;
		break;
	case 6:
		cluster_node_id = 1;
		break;
	case 7:
		owner_epoch++;
		break;
	case 8:
		owner_generation++;
		break;
	case 9:
		control_creator.incarnation++;
		break;
	case 10:
		InRecovery = !InRecovery;
		break;
	case 11:
		entry_authority = false;
		break;
	case 12:
		cluster_shared_config = false;
		break;
	case 13:
		cluster_controlfile_shared_authority = false;
		break;
	case 14:
		cluster_enabled = false;
		break;
	case 15:
		ShutdownRequestPending = true;
		break;
	case 16:
		entry_native_lock = true;
		break;
	case 17:
		CritSectionCount = 1;
		break;
	case 18:
		failure_work.self = !failure_work.self;
		break;
	case 19:
		failure_request.mode++;
		break;
	case 20:
		failure_request.duty.thread_claim_crc32c++;
		break;
	case 21:
		failure_request.expected_root_token.file_txn_seq++;
		break;
	case 22:
		failure_work.self_min_dead_us++;
		break;
	case 23:
		failure_work.publisher_node++;
		break;
	case 24:
		failure_work.publisher_incarnation++;
		break;
	case 25:
		failure_work.self_restart.claim.claim_sha256[0]++;
		break;
	case 26:
		cold_restart.claim.claim_sha256[0]++;
		break;
	case 27:
		failure_work.walr = NULL;
		break;
	case 28:
		failure_work.format_version++;
		break;
	case 29:
		failure_formation = false;
		break;
	case 30:
		failure_needs = false;
		break;
	case 31:
		failure_admissions = false;
		break;
	case 32:
		failure_ir_current = false;
		break;
	case 33:
		failure_work.serial.held = false;
		break;
	case 34:
		failure_work.serial.release_uncertain = true;
		break;
	case 35:
		failure_work.serial.lock_request.request_id++;
		break;
	case 36:
		failure_work.serial.duty.thread_claim_crc32c++;
		break;
	case 37:
		failure_work.serial.root_read_token.file_txn_seq++;
		break;
	case 38:
		failure_work.serial.mode++;
		break;
	case 39:
		failure_digest_valid = false;
		break;
	case 40:
		failure_need.victim_incarnation++;
		break;
	case 41:
		failure_need.canonical_duty_digest.bytes[0] ^= 1;
		break;
	case 42:
		failure_need.system_identifier++;
		break;
	case 43:
		failure_need_count = 0;
		break;
	case 44:
		failure_null_need = true;
		break;
	case 45:
		self_quorum = false;
		break;
	case 46:
		retention_member = false;
		break;
	case 47:
		retention_admitted++;
		break;
	case 48:
		self_death = false;
		break;
	case 49:
		enableFsync = false;
		break;
	case 50:
		slot->owner.request.control_owner_id++;
		break;
	case 51:
		slot->owner.attempt.generation++;
		break;
	case 52:
		control_requests->records[slot->owner.attempt.slot].owner.incarnation++;
		break;
	case 53:
		control_requests->records[slot->owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 54:
		control_requests->records[slot->owner.attempt.slot].message.key.holder.cluster_epoch++;
		break;
	case 55:
		control_requests->records[slot->owner.attempt.slot].state = CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 56:
		slot->release_pending = false;
		break;
	case 57:
		slot->owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
		break;
	case 58:
		slot->owner.cooperative_acquire = true;
		break;
	case 59:
		cf_slot(failure_wait_mode() == ShareLock ? ExclusiveLock : ShareLock)->held = true;
		break;
	}
}
static void
failure_change_while_waiting(void)
{
	failure_ready_after_ack();
	failure_saved_slot = *cf_slot(failure_wait_mode());
	failure_saved_record = control_requests->records[failure_saved_slot.owner.attempt.slot];
	failure_change_context();
}
static void
failure_restore_guards(void)
{
	if (failure_kind == 1)
		failure_work.serial = failure_serial;
	else
		memset(&failure_work.serial, 0, sizeof(failure_work.serial));
	failure_work.walr = (ClusterWalRootPublishGuard *)&failure_walr_identity;
}
UT_TEST(startup_failure_input_wait_rechecks_original_actor_purpose_guard_and_request)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (failure_fault = 0; failure_fault < 60; failure_fault++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
				int pid = MyProcPid;
				uint64 epoch = owner_epoch, generation = owner_generation;
				uint64 boot = control_creator.incarnation;

				failure_fixture(kind, phase);
				if (!failure_fault_applies(failure_fault))
					continue;
				entry_on_wait = failure_change_while_waiting;
				UT_ASSERT(!failure_call(&result));
				UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(entry_level, FATAL);
				UT_ASSERT_EQ(entry_waits, 1);
				UT_ASSERT_EQ(failure_reads, kind == 0 ? 0 : phase);
				UT_ASSERT_EQ(failure_wal_reads, kind == 0 ? 0 : phase);
				UT_ASSERT_EQ(failure_publishes, 0);
				recovery_restore_actor(pid, epoch, generation, boot);
				*cf_slot(failure_wait_mode()) = failure_saved_slot;
				control_requests->records[failure_saved_slot.owner.attempt.slot]
					= failure_saved_record;
				memset(cf_slot(failure_wait_mode() == ShareLock ? ExclusiveLock : ShareLock), 0,
					   sizeof(CfHoldState));
				failure_restore_guards();
				failure_finish();
			}
}
static void
failure_change_after_grant(void)
{
	if (entry_waits != 0)
		failure_change_context();
}
UT_TEST(startup_failure_input_new_grant_requalifies_before_root_io)
{
	const unsigned faults[] = { 3, 7, 18, 21, 27, 29, 31, 32, 35, 45 };

	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (unsigned i = 0; i < lengthof(faults); i++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
				uint64 epoch = owner_epoch;

				failure_fixture(kind, phase);
				failure_fault = faults[i];
				if (!failure_fault_applies(failure_fault))
					continue;
				claim_after_acquire = failure_change_after_grant;
				UT_ASSERT(failure_call(&result));
				UT_ASSERT_EQ(result, failure_fault == 7 ? CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
														: CLUSTER_CONTROL_ROOT_STALE_TOKEN);
				UT_ASSERT_EQ(entry_waits, 3);
				UT_ASSERT_EQ(failure_reads, kind == 0 ? 0 : phase);
				UT_ASSERT_EQ(failure_wal_reads, kind == 0 ? 0 : phase);
				UT_ASSERT_EQ(failure_publishes, 0);
				CurrentResourceOwner = (ResourceOwner)1;
				owner_epoch = epoch;
				failure_restore_guards();
				failure_finish();
			}
}
static void
failure_remove_receipt(void)
{
	failure_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(failure_wait_mode(), claim_retiring_cookie));
	if (failure_wait_mode() == ShareLock)
		cf_retired_s = 0;
	else
		cf_retired_x = 0;
}
static void
failure_ack_during_reset(void)
{
	if (failure_attempts != failure_first_attempt() || !cluster_cf_held(failure_wait_mode()))
		return;
	claim_retiring_cookie = cluster_cf_owner_cookie(failure_wait_mode());
	UT_ASSERT(acknowledge_retirements());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
static void
failure_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(failure_reads, failure_kind == 0 ? 0 : failure_phase);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
UT_TEST(startup_failure_input_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (unsigned fault = 0; fault < 4; fault++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

				failure_fixture(kind, phase);
				entry_lost_latch = true;
				if (fault == 0)
					entry_on_wait = failure_remove_receipt;
				else if (fault == 1)
					claim_on_reset = failure_ack_during_reset;
				else if (fault == 2) {
					fail_after_publication = false;
					entry_on_wait = failure_empty_grant;
				}
				UT_ASSERT_EQ(failure_call(&result), fault != 0);
				UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
				if (fault == 0) {
					UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
					UT_ASSERT_EQ(failure_publishes, 0);
				} else {
					UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
					UT_ASSERT_EQ(failure_publishes, 1);
				}
				failure_finish();
			}
}
UT_TEST(startup_failure_input_hard_untyped_and_preexisting_requests_do_not_retry)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (unsigned fault = 0; fault < 7; fault++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
				LOCKMODE preexisting = fault == 3 || fault == 5 ? ShareLock : ExclusiveLock;
				bool foreign;
				unsigned before;

				failure_fixture(kind, phase);
				foreign = fault >= 3 && preexisting != failure_wait_mode();
				if (fault == 0)
					failure_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
				else if (fault == 1)
					g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
				else if (fault == 2)
					entry_on_wait = entry_hard_error;
				else {
					g_seven_result = fault >= 5 ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
												: CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
					UT_ASSERT_EQ(cluster_cf_lock(preexisting), fault >= 5);
					failure_direct = true;
					if (kind != 0)
						failure_work.walr = (ClusterWalRootPublishGuard *)&failure_walr_identity;
					if (kind == 1)
						failure_work.serial = failure_serial;
				}
				before = g_seven_count;
				if (fault == 2 || foreign) {
					UT_ASSERT(!failure_call(&result));
					UT_ASSERT_EQ(entry_code, foreign ? ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED
													 : ERRCODE_IO_ERROR);
				} else {
					UT_ASSERT(failure_call(&result));
					UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
				}
				UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
				UT_ASSERT_EQ(g_seven_count, before
												+ (foreign	   ? 1
												   : fault < 3 ? failure_first_attempt()
															   : 0));
				UT_ASSERT_EQ(failure_reads, fault < 3 && kind != 0 ? phase : 0);
				UT_ASSERT_EQ(failure_publishes, 0);
				failure_finish();
			}
}
static void
failure_change_root(void)
{
	failure_ready_after_ack();
	switch (failure_fault) {
	case 0:
		retention_image.records[0].root_publish_seq++;
		break;
	case 1:
		failure_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 2:
		failure_throw_read = true;
		break;
	case 3:
		failure_wal_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 4:
		failure_tail_divergent = true;
		break;
	}
}
UT_TEST(startup_failure_input_real_root_and_wal_refusals_remain_errors)
{
	const ClusterControlRootResult expected[]
		= { CLUSTER_CONTROL_ROOT_CAS_CONFLICT, CLUSTER_CONTROL_ROOT_IO_ERROR,
			CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_IO_ERROR,
			CLUSTER_CONTROL_ROOT_COPY_DIVERGENT };

	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (failure_fault = 0; failure_fault < (kind == 0 ? 3 : 5); failure_fault++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

				failure_fixture(kind, phase);
				entry_on_wait = failure_change_root;
				UT_ASSERT_EQ(failure_call(&result), failure_fault != 2);
				if (failure_fault == 2)
					UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
				else
					UT_ASSERT_EQ(result, expected[failure_fault]);
				UT_ASSERT_EQ(entry_waits, 3);
				UT_ASSERT_EQ(failure_publishes, 0);
				failure_finish();
			}
}
static void
failure_change_owner_outside_cf(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	CurrentResourceOwner = (ResourceOwner)2;
}
UT_TEST(startup_failure_tail_second_interval_keeps_the_original_owner)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

		failure_fixture(kind, 1);
		failure_wal_hook = failure_change_owner_outside_cf;
		UT_ASSERT(!failure_call(&result));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(failure_reads, 1);
		UT_ASSERT_EQ(failure_wal_reads, 1);
		UT_ASSERT_EQ(failure_publishes, 0);
		CurrentResourceOwner = (ResourceOwner)1;
		failure_finish();
	}
}
UT_TEST(startup_failure_input_original_cancel_keeps_shared_cleanup_responsibility)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			volatile bool exited = false;

			failure_fixture(kind, phase);
			entry_on_wait = entry_shutdown;
			startup_fixture_exit_hook = entry_owner_exit;
			startup_fixture_armed = true;
			if (sigsetjmp(startup_fixture_exit, 1) == 0)
				UT_ASSERT(!failure_call(&result));
			else
				exited = true;
			UT_ASSERT(exited);
			UT_ASSERT_EQ(startup_fixture_exit_code, 1);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(failure_reads, kind == 0 ? 0 : phase);
			UT_ASSERT_EQ(failure_publishes, 0);
			UT_ASSERT(control_requests->records[cf_slot(failure_wait_mode())->owner.attempt.slot]
						  .owner_exited);
			PG_exception_stack = NULL;
			error_context_stack = NULL;
			proc_exit_inprogress = shutdown_requested = false;
			startup_fixture_armed = false;
			startup_fixture_exit_hook = NULL;
			failure_finish();
		}
}
UT_TEST(ordinary_failure_input_roles_keep_the_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };

	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned phase = 0; phase < (kind == 0 ? 1 : 2); phase++)
			for (unsigned role = 0; role < lengthof(roles); role++) {
				ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

				failure_fixture(kind, phase);
				failure_direct = true;
				if (kind != 0)
					failure_work.walr = (ClusterWalRootPublishGuard *)&failure_walr_identity;
				if (kind == 1)
					failure_work.serial = failure_serial;
				MyBackendType = roles[role];
				UT_ASSERT(failure_call(&result));
				UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
				UT_ASSERT_EQ(entry_waits, 0);
				UT_ASSERT_EQ(failure_reads | failure_publishes, 0);
				MyBackendType = B_STARTUP;
				failure_finish();
			}
}

/* Actual CF/owner/CONTROL and original post-IR completion call/cleanup.
 * Converted WALR, physical ROOT/WAL and upstream proof are explicit leaves. */
static unsigned complete_attempts, complete_reads, complete_wal_reads, complete_publications;
static unsigned complete_pin_begins, complete_pin_ends, complete_dispatches, complete_consumptions;
static unsigned complete_pin_checks;
static bool complete_receipt_before_reapply, complete_pin_active, complete_throw_read;
static bool complete_pin_flags[9];
static bool complete_publish_ok, complete_postread_bad, complete_uuid_ok;
static ClusterWalPinResult complete_pin_result;
static ClusterWalrReleaseResult complete_end_result;
static ClusterControlRootResult complete_read_result, complete_wal_result, complete_storage_result;
static ClusterLockAcquireResult complete_acquire_result;
static ClusterControlRootReadToken complete_expected, complete_pin_token;
static ClusterControlRootPatch complete_patch;
static ClusterControlRootSnapshot complete_original;
static ResourceOwner complete_pin_owner;
static int complete_pin_pid, complete_pin_identity;
static void *complete_work;
static void (*complete_after_publish)(void);
static ClusterControlRootResult complete_dispatch(const ClusterControlRootReadToken *expected,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootPublishReason reason,
												  ClusterControlRootSnapshot *out,
												  ClusterControlRootReadToken *token);

/* The bounded dispatch below consumes the original authority once. The
 * generic ROOT API's alias/reason branches remain separate full-ROOT tests. */
#define cluster_control_root_compare_and_publish complete_dispatch
#include "test_cluster_complete_authority.inc"
#undef cluster_control_root_compare_and_publish

static void *
complete_alloc(Size size)
{
	UT_ASSERT(complete_work == NULL);
	complete_work = palloc0(size);
	return complete_work;
}
static void
complete_free(void *ptr)
{
	UT_ASSERT(ptr == complete_work);
	complete_work = NULL;
	pfree(ptr);
}
static ClusterWalPinResult
complete_pin_begin(const ClusterControlRootReadToken *expected, bool sealed,
				   ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT(sealed && *guard == NULL && expected != NULL);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(memcmp(expected, &complete_pin_token, sizeof(*expected)), 0);
	++complete_pin_begins;
	if (complete_pin_result == CLUSTER_WAL_PIN_OK) {
		complete_pin_active = true;
		*guard = (ClusterWalRootPublishGuard *)&complete_pin_identity;
	}
	return complete_pin_result;
}
static bool
complete_pin_current(const ClusterWalRootPublishGuard *guard,
					 const ClusterControlRootReadToken *expected)
{
	++complete_pin_checks;
	if (guard != (ClusterWalRootPublishGuard *)&complete_pin_identity || !complete_pin_active
		|| MyProcPid != complete_pin_pid || CurrentResourceOwner != complete_pin_owner
		|| memcmp(expected, &complete_pin_token, sizeof(*expected)) != 0)
		return false;
	for (unsigned i = 0; i < lengthof(complete_pin_flags); i++)
		if (!complete_pin_flags[i])
			return false;
	return true;
}
static ClusterWalrReleaseResult
complete_pin_end(ClusterWalRootPublishGuard **guard)
{
	if (cluster_cf_held_is_usable(ShareLock) || cluster_cf_held_is_usable(ExclusiveLock))
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	++complete_pin_ends;
	if (*guard != (ClusterWalRootPublishGuard *)&complete_pin_identity
		|| MyProcPid != complete_pin_pid || CurrentResourceOwner != complete_pin_owner)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	if (complete_end_result == CLUSTER_WALR_RELEASE_CONFIRMED) {
		complete_pin_active = false;
		*guard = NULL;
	}
	return complete_end_result;
}
static ClusterControlRootResult
complete_storage(const uint8 *uuid, bool probe)
{
	UT_ASSERT(uuid == NULL && probe);
	return complete_storage_result;
}
static bool
complete_uuid(uint8 uuid[16])
{
	memset(uuid, 0, 16);
	uuid[0] = 31;
	return complete_uuid_ok;
}
static ClusterControlRootResult
complete_read(ControlRootImage *root, ControlFileData *native, ClusterControlRootFileToken *file)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	++complete_reads;
	if (complete_throw_read)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("completion ROOT read failed")));
	*root = retention_image;
	if (complete_postread_bad && complete_reads == 3)
		root->bytes[0] ^= 1;
	memset(native, 0, sizeof(*native));
	native->checkPoint = 100;
	memset(file, 0, sizeof(*file));
	return complete_read_result;
}
static ClusterControlRootResult
complete_read_control(const uint8 uuid[16], uint64 sysid, ControlRootImage *root,
					  ControlFileData *native, ClusterControlRootFileToken *file, uint16 version)
{
	UT_ASSERT(uuid[0] == 31 && sysid == 1234 && version == 3);
	return complete_read(root, native, file);
}
static ClusterControlRootResult
complete_read_thread(const ClusterControlRootIdentity *expected, ControlRootImage *root,
					 ControlFileData *native, ClusterControlRootFileToken *file, uint16 version)
{
	UT_ASSERT_EQ(version, 3);
	UT_ASSERT_EQ(memcmp(expected, &complete_original.identity, sizeof(*expected)), 0);
	return complete_read(root, native, file);
}
static ClusterControlRootResult
complete_wal(const char *dir, const ClusterWalSourceRef *source, int segment_size,
			 const ClusterControlRootSnapshot *record, XLogRecPtr checkpoint,
			 ClusterWalRecordVisitor visitor, void *arg, ClusterWalTailObservation *out)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT(dir == cluster_wal_threads_dir && segment_size == wal_segment_size);
	UT_ASSERT(visitor == NULL && arg == NULL);
	UT_ASSERT_EQ(checkpoint, 100);
	UT_ASSERT_EQ(memcmp(record, &complete_original, sizeof(*record)), 0);
	UT_ASSERT_EQ(memcmp(&source->claim.identity, &record->identity, sizeof(record->identity)), 0);
	UT_ASSERT_EQ(source->claim.database_incarnation, 7);
	UT_ASSERT_EQ(source->claim.max_config_generation, 9);
	UT_ASSERT_EQ(source->claim.claim_sha256[0], 3);
	UT_ASSERT_EQ(source->timeline, 1);
	++complete_wal_reads;
	memset(out, 0, sizeof(*out));
	out->complete_end = 340;
	out->last_record_start = 300;
	out->last_record_crc = 23;
	return complete_wal_result;
}
static bool
complete_publish(const ControlRootImage *base, const ControlRootImage *next)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(base->header.file_txn_seq, 7);
	UT_ASSERT_EQ(next->header.file_txn_seq, 8);
	UT_ASSERT_EQ(memcmp(&base->startup[127], &next->startup[127], sizeof(base->startup[127])), 0);
	++complete_publications;
	if (!complete_publish_ok)
		return false;
	retention_image = *next;
	if (complete_after_publish != NULL)
		complete_after_publish();
	return true;
}
static bool
complete_actual_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	++complete_attempts;
	UT_ASSERT(!root_publish_authority.active);
	UT_ASSERT_EQ(complete_consumptions, 1);
	if (claim_retiring_cookie != 0) {
		UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
		UT_ASSERT(cluster_cf_release_completed(mode, claim_retiring_cookie));
		UT_ASSERT(cluster_control_request_empty());
		complete_receipt_before_reapply = true;
	}
	++g_next_request_id;
	if (complete_attempts == 1)
		g_seven_result = complete_acquire_result;
	return claim_actual_cf_lock(mode);
}
#define palloc0 complete_alloc
#define pfree complete_free
#define acquire_clusterwide_cf complete_acquire
#define cluster_cf_lock complete_actual_cf_lock
#define cluster_wal_retention_root_publish_begin_exact complete_pin_begin
#define cluster_wal_retention_root_publish_sealed_current complete_pin_current
#define cluster_wal_retention_root_publish_end complete_pin_end
#define storage_contract_check complete_storage
#define current_storage_uuid complete_uuid
#define read_control_version complete_read_control
#define read_thread_version complete_read_thread
#define cluster_wal_tail_visit_sealed complete_wal
#define cluster_control_root_v3_encode(root) self_encode((root), 3)
#define publish_updated_image complete_publish
#include "test_cluster_recovery_complete_cf.inc"
#undef publish_updated_image
#undef cluster_control_root_v3_encode
#undef cluster_wal_tail_visit_sealed
#undef read_thread_version
#undef read_control_version
#undef current_storage_uuid
#undef storage_contract_check
#undef cluster_wal_retention_root_publish_end
#undef cluster_wal_retention_root_publish_sealed_current
#undef cluster_wal_retention_root_publish_begin_exact
#undef cluster_cf_lock
#undef acquire_clusterwide_cf
#undef pfree
#undef palloc0

static ClusterControlRootResult
complete_dispatch(const ClusterControlRootReadToken *expected, const ClusterControlRootPatch *patch,
				  ClusterControlRootPublishReason reason, ClusterControlRootSnapshot *out,
				  ClusterControlRootReadToken *token)
{
	++complete_dispatches;
	UT_ASSERT_EQ(reason, CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE);
	if (!cluster_control_root_publish_authority_current_v1(expected, patch, reason))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	++complete_consumptions;
	return recovery_complete_v3_publish(expected, patch, out, token);
}
static void
complete_ready_after_ack(void)
{
	UT_ASSERT_EQ(complete_attempts, 1);
	UT_ASSERT_EQ(complete_reads | complete_wal_reads | complete_publications, 0);
	UT_ASSERT(!root_publish_authority.active);
	UT_ASSERT_EQ(complete_consumptions, 1);
	UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}
static void
complete_fixture(void)
{
	/* Use the real creator initialization before reading its fixture boot. */
	UT_ASSERT(lock_owner_creator());
	self_fixture(0);
	complete_attempts = complete_reads = complete_wal_reads = complete_publications = 0;
	complete_pin_begins = complete_pin_ends = complete_pin_checks = 0;
	complete_dispatches = complete_consumptions = 0;
	complete_work = NULL;
	complete_pin_owner = CurrentResourceOwner;
	complete_pin_pid = MyProcPid;
	complete_receipt_before_reapply = complete_pin_active = complete_throw_read = false;
	complete_publish_ok = complete_uuid_ok = true;
	complete_postread_bad = false;
	for (unsigned i = 0; i < lengthof(complete_pin_flags); i++)
		complete_pin_flags[i] = true;
	complete_pin_result = CLUSTER_WAL_PIN_OK;
	complete_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
	complete_read_result = complete_wal_result = complete_storage_result
		= CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	complete_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	complete_after_publish = NULL;
	retention_image.header.v2.serving[0] = 0;
	retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	retention_image.records[0].root_flags |= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
											 | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
											 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
											 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	retention_image.records[0].tail_tli = 1;
	retention_image.records[0].validated_tail_lsn_exclusive = 340;
	retention_image.records[0].tail_last_record_lsn = 300;
	retention_image.records[0].tail_last_record_crc32c = 23;
	retention_image.startup[127].generation = 99;
	complete_original = retention_image.records[0];
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY, &complete_expected);
	complete_pin_token = complete_expected;
	memset(&complete_patch, 0, sizeof(complete_patch));
	complete_patch.mask
		= CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	complete_patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	complete_patch.expected_flags_mask = complete_patch.expected_flags_value
		= complete_original.root_flags;
	complete_patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	complete_patch.desired.root_flags = complete_original.root_flags
										| CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
										| CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;
	complete_patch.desired.recovered_tli = 1;
	complete_patch.desired.recovered_through_lsn_exclusive = 340;
	complete_patch.desired.recovered_last_record_lsn = 300;
	complete_patch.desired.recovered_last_record_crc32c = 23;
	cluster_control_root_publish_authority_clear_v1();
	entry_on_wait = complete_ready_after_ack;
}
static bool
complete_call(ClusterControlRootResult *result, ClusterControlRootSnapshot *out,
			  ClusterControlRootReadToken *token)
{
	volatile bool returned = false;
	memset(out, 0xa5, sizeof(*out));
	memset(token, 0xa5, sizeof(*token));
	PG_TRY();
	{
		*result = cluster_control_root_recovery_complete_publish_v1(&complete_expected,
																	&complete_patch, out, token);
		returned = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return returned;
}
static void
complete_finish(void)
{
	entry_on_wait = NULL;
	claim_on_reset = claim_after_acquire = NULL;
	claim_teardown = owner_ack_on_wait = true;
	for (unsigned i = 0; i < 2; i++) {
		LOCKMODE mode = i == 0 ? ShareLock : ExclusiveLock;
		if (cluster_cf_held_is_usable(mode))
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
	}
	(void)acknowledge_retirements();
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(complete_work == NULL && !root_publish_authority.active);
	/* Retained pin cleanup is an explicit leaf, not a native exit proof. */
	complete_pin_active = false;
	claim_teardown = owner_ack_on_wait = cold_consumer = false;
}
static void
complete_no_output(const ClusterControlRootSnapshot *out, const ClusterControlRootReadToken *token)
{
	const ClusterControlRootSnapshot empty = { 0 };
	const ClusterControlRootReadToken empty_token = { 0 };
	UT_ASSERT_EQ(memcmp(out, &empty, sizeof(*out)), 0);
	UT_ASSERT_EQ(memcmp(token, &empty_token, sizeof(*token)), 0);
}
UT_TEST(startup_recovery_complete_cf_wait_stays_with_one_consumed_publication)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	complete_fixture();
	UT_ASSERT(complete_call(&result, &out, &token));
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		complete_no_output(&out, &token);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(complete_attempts, 2);
	UT_ASSERT_EQ(complete_reads, 3);
	UT_ASSERT_EQ(complete_wal_reads, 1);
	UT_ASSERT_EQ(complete_publications, 1);
	UT_ASSERT(complete_receipt_before_reapply);
	UT_ASSERT_EQ(complete_dispatches, 1);
	UT_ASSERT_EQ(complete_consumptions, 1);
	UT_ASSERT_EQ(complete_pin_begins, 1);
	UT_ASSERT_EQ(complete_pin_ends, 1);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(out.root_publish_seq, 14);
	UT_ASSERT_EQ(out.recovered_through_lsn_exclusive, 340);
	UT_ASSERT_EQ(out.recovered_last_record_crc32c, 23);
	UT_ASSERT_EQ(memcmp(&out.identity, &complete_original.identity, sizeof(out.identity)), 0);
	UT_ASSERT_EQ(token.file_txn_seq, 8);
	UT_ASSERT_EQ(retention_image.header.v2.serving[0], 0);
	UT_ASSERT_EQ(retention_image.startup[127].generation, 99);
	UT_ASSERT(out.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF);
	complete_finish();
}

static unsigned complete_fault;
static CfHoldState complete_saved_slot;
static ClusterControlRequestView complete_saved_record;
static void
complete_change_context(void)
{
	RecoveryCompleteV3Work *work = complete_work;
	switch (complete_fault) {
	case 0:
		MyProc = NULL;
		break;
	case 1:
		MyLatch = NULL;
		break;
	case 2:
		MyProcPid++;
		break;
	case 3:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 4:
		MyBackendType = B_BG_WORKER;
		break;
	case 5:
		MyAuxProcType = BgWriterProcess;
		break;
	case 6:
		cluster_node_id = 1;
		break;
	case 7:
		owner_epoch++;
		break;
	case 8:
		owner_generation++;
		break;
	case 9:
		control_creator.incarnation++;
		break;
	case 10:
		InRecovery = !InRecovery;
		break;
	case 11:
		entry_authority = false;
		break;
	case 12:
		cluster_shared_config = false;
		break;
	case 13:
		cluster_controlfile_shared_authority = false;
		break;
	case 14:
		cluster_enabled = false;
		break;
	case 15:
		ShutdownRequestPending = true;
		break;
	case 16:
		CritSectionCount = 1;
		break;
	case 17:
		entry_native_lock = true;
		break;
	case 18:
		complete_expected.file_txn_seq++;
		break;
	case 19:
		complete_patch.mask ^= CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY;
		break;
	case 20:
		complete_patch.desired.recovered_through_lsn_exclusive--;
		break;
	case 21:
		complete_patch.expected_flags_value ^= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
		break;
	case 22:
		work->publisher_node++;
		break;
	case 23:
		work->publisher_incarnation++;
		break;
	case 24:
		work->walr = NULL;
		break;
	case 25:
		enableFsync = false;
		break;
	case 35:
		work->walr = (ClusterWalRootPublishGuard *)&failure_walr_identity;
		break;
	case 36:
		cf_hold_x.owner.request.control_owner_id++;
		break;
	case 37:
		cf_hold_x.owner.attempt.generation++;
		break;
	case 38:
		control_requests->records[cf_hold_x.owner.attempt.slot].owner.incarnation++;
		break;
	case 39:
		control_requests->records[cf_hold_x.owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 40:
		control_requests->records[cf_hold_x.owner.attempt.slot].message.key.holder.cluster_epoch++;
		break;
	case 41:
		control_requests->records[cf_hold_x.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 42:
		cf_hold_x.release_pending = false;
		break;
	case 43:
		cf_hold_x.owner.state = CLUSTER_LOCK_OWNER_ACQUIRING;
		break;
	case 44:
		cf_hold_x.owner.cooperative_acquire = true;
		break;
	case 45:
		cf_hold_s.held = true;
		break;
	case 46:
		complete_patch.reserved24++;
		break;
	case 47:
		complete_expected.authority_uuid[0] ^= 1;
		break;
	default:
		UT_ASSERT(complete_fault >= 26 && complete_fault <= 34);
		/* Native-held/converted/sealed/IR-released/token/formation/needs/
		 * admissions/pin-valid facts are explicit getter leaf boundaries. */
		complete_pin_flags[complete_fault - 26] = false;
		break;
	}
}
static void
complete_change_while_waiting(void)
{
	complete_ready_after_ack();
	complete_saved_slot = cf_hold_x;
	complete_saved_record = control_requests->records[cf_hold_x.owner.attempt.slot];
	complete_change_context();
}
UT_TEST(startup_recovery_complete_wait_rechecks_actor_patch_sealed_guard_and_exact_request)
{
	for (complete_fault = 0; complete_fault < 48; complete_fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		int pid = MyProcPid;
		uint64 epoch = owner_epoch, generation = owner_generation,
			   boot = control_creator.incarnation;
		complete_fixture();
		entry_on_wait = complete_change_while_waiting;
		UT_ASSERT(!complete_call(&result, &out, &token));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_level, FATAL);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(complete_attempts, 1);
		UT_ASSERT_EQ(complete_reads | complete_wal_reads | complete_publications, 0);
		UT_ASSERT_EQ(complete_consumptions, 1);
		complete_no_output(&out, &token);
		recovery_restore_actor(pid, epoch, generation, boot);
		cf_hold_x = complete_saved_slot;
		control_requests->records[cf_hold_x.owner.attempt.slot] = complete_saved_record;
		memset(&cf_hold_s, 0, sizeof(cf_hold_s));
		complete_finish();
	}
}
static void
complete_change_after_grant(void)
{
	if (entry_waits != 0)
		complete_change_context();
}
UT_TEST(startup_recovery_complete_new_grant_requalifies_before_root_io)
{
	const unsigned faults[] = { 3, 7, 18, 20, 22, 24, 26, 34, 35 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		uint64 epoch = owner_epoch;
		complete_fixture();
		complete_fault = faults[i];
		claim_after_acquire = complete_change_after_grant;
		UT_ASSERT(complete_call(&result, &out, &token));
		UT_ASSERT_EQ(result, complete_fault == 7 ? CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
							 : complete_fault == 3 || complete_fault == 35
								 ? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN
								 : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(complete_attempts, 2);
		UT_ASSERT_EQ(complete_reads | complete_wal_reads | complete_publications, 0);
		complete_no_output(&out, &token);
		CurrentResourceOwner = (ResourceOwner)1;
		owner_epoch = epoch;
		complete_finish();
	}
}
static void
complete_missing_receipt(void)
{
	complete_ready_after_ack();
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, claim_retiring_cookie));
	cf_retired_x = 0;
}
static void
complete_ack_during_reset(void)
{
	if (complete_attempts != 1 || !cluster_cf_held(ExclusiveLock))
		return;
	claim_retiring_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT(acknowledge_retirements());
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
static void
complete_empty_grant(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(complete_reads | complete_publications, 0);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}
UT_TEST(startup_recovery_complete_receipt_wake_and_empty_preserve_progress)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		complete_fixture();
		entry_lost_latch = true;
		if (fault == 0)
			entry_on_wait = complete_missing_receipt;
		else if (fault == 1)
			claim_on_reset = complete_ack_during_reset;
		else if (fault == 2) {
			fail_after_publication = false;
			entry_on_wait = complete_empty_grant;
		}
		UT_ASSERT_EQ(complete_call(&result, &out, &token), fault != 0);
		UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
		if (fault == 0) {
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(complete_publications, 0);
			complete_no_output(&out, &token);
		} else {
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(complete_publications, 1);
		}
		UT_ASSERT_EQ(complete_consumptions, 1);
		UT_ASSERT_EQ(complete_pin_begins, 1);
		complete_finish();
	}
}
UT_TEST(startup_recovery_complete_hard_untyped_and_preexisting_are_not_retried)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		unsigned before;
		complete_fixture();
		if (fault == 0)
			complete_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		else if (fault == 1)
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		else if (fault == 2)
			entry_on_wait = entry_hard_error;
		else {
			LOCKMODE mode = fault == 3 || fault == 5 ? ShareLock : ExclusiveLock;
			g_seven_result
				= fault >= 5 ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
			UT_ASSERT_EQ(cluster_cf_lock(mode), fault >= 5);
		}
		before = g_seven_count;
		if (fault == 2 || fault == 3) {
			UT_ASSERT(!complete_call(&result, &out, &token));
			UT_ASSERT_EQ(entry_code,
						 fault == 2 ? ERRCODE_IO_ERROR : ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		} else {
			UT_ASSERT(complete_call(&result, &out, &token));
			UT_ASSERT_EQ(result, fault >= 5 ? CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT
											: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		}
		UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(g_seven_count, before + (fault < 4 ? 1 : 0));
		UT_ASSERT_EQ(complete_reads | complete_wal_reads | complete_publications, 0);
		UT_ASSERT_EQ(complete_consumptions, 1);
		complete_no_output(&out, &token);
		complete_finish();
	}
}
static void
complete_lose_pin_after_publish(void)
{
	complete_pin_flags[0] = false;
}
static void
complete_change_owner_after_publish(void)
{
	CurrentResourceOwner = (ResourceOwner)2;
}
static void
complete_change_root_or_io(void)
{
	complete_ready_after_ack();
	switch (complete_fault) {
	case 0:
		retention_image.header.file_txn_seq++;
		break;
	case 1:
		retention_image.startup[0].generation = 1;
		break;
	case 2:
		complete_read_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 3:
		complete_throw_read = true;
		break;
	case 4:
		complete_wal_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 5:
		complete_publish_ok = false;
		break;
	case 6:
		complete_postread_bad = true;
		break;
	case 7:
		complete_after_publish = complete_lose_pin_after_publish;
		break;
	case 8:
		complete_after_publish = complete_change_owner_after_publish;
		break;
	}
}
UT_TEST(startup_recovery_complete_real_errors_never_export_provisional_or_persisted_permission)
{
	const ClusterControlRootResult expected[]
		= { CLUSTER_CONTROL_ROOT_STALE_TOKEN,	   CLUSTER_CONTROL_ROOT_CAS_CONFLICT,
			CLUSTER_CONTROL_ROOT_IO_ERROR,		   CLUSTER_CONTROL_ROOT_IO_ERROR,
			CLUSTER_CONTROL_ROOT_IO_ERROR,		   CLUSTER_CONTROL_ROOT_IO_ERROR,
			CLUSTER_CONTROL_ROOT_POSTREAD_FAILED,  CLUSTER_CONTROL_ROOT_STALE_TOKEN,
			CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN };
	for (complete_fault = 0; complete_fault < lengthof(expected); complete_fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		complete_fixture();
		entry_on_wait = complete_change_root_or_io;
		UT_ASSERT_EQ(complete_call(&result, &out, &token), complete_fault != 3);
		if (complete_fault == 3)
			UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
		else
			UT_ASSERT_EQ(result, expected[complete_fault]);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(complete_publications, complete_fault >= 5);
		UT_ASSERT_EQ(retention_image.records[0].lifecycle,
					 complete_fault >= 6 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
										 : CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
		UT_ASSERT_EQ(complete_consumptions, 1);
		complete_no_output(&out, &token);
		CurrentResourceOwner = (ResourceOwner)1;
		complete_finish();
	}
}
UT_TEST(startup_recovery_complete_original_preconditions_do_not_become_cf_waits)
{
	const ClusterWalPinResult pins[] = { CLUSTER_WAL_PIN_INVALID, CLUSTER_WAL_PIN_STALE,
										 CLUSTER_WAL_PIN_CAPACITY, CLUSTER_WAL_PIN_OK };
	const ClusterControlRootResult expected[]
		= { CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT, CLUSTER_CONTROL_ROOT_STALE_TOKEN,
			CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE, CLUSTER_CONTROL_ROOT_IO_ERROR };
	for (unsigned fault = 0; fault < lengthof(pins); fault++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		complete_fixture();
		complete_pin_result = pins[fault];
		if (fault == 3)
			complete_storage_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		UT_ASSERT(complete_call(&result, &out, &token));
		UT_ASSERT_EQ(result, expected[fault]);
		UT_ASSERT_EQ(entry_waits | complete_attempts | complete_reads | complete_publications, 0);
		UT_ASSERT_EQ(complete_consumptions, 1);
		UT_ASSERT_EQ(complete_pin_begins, 1);
		complete_no_output(&out, &token);
		complete_finish();
	}
}
UT_TEST(startup_recovery_complete_original_cancel_keeps_shared_cleanup_responsibility)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	volatile bool exited = false;
	complete_fixture();
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!complete_call(&result, &out, &token));
	else
		exited = true;
	UT_ASSERT(exited);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(complete_reads | complete_wal_reads | complete_publications, 0);
	UT_ASSERT(control_requests->records[cf_hold_x.owner.attempt.slot].owner_exited);
	UT_ASSERT_EQ(complete_consumptions, 1);
	complete_no_output(&out, &token);
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	/* Simulated process exit bypasses C cleanup frames. Only this leaf pin
	 * is unwound here; the real owner/registry has already recorded exit. */
	UT_ASSERT(complete_work != NULL);
	UT_ASSERT_EQ(recovery_complete_v3_cleanup(complete_work, CLUSTER_CONTROL_ROOT_IO_ERROR),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	complete_free(complete_work);
	complete_finish();
}
UT_TEST(ordinary_recovery_complete_roles_keep_the_original_cf_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };
	for (unsigned role = 0; role < lengthof(roles); role++) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		complete_fixture();
		MyBackendType = roles[role];
		UT_ASSERT(complete_call(&result, &out, &token));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(complete_attempts, 1);
		UT_ASSERT_EQ(complete_reads | complete_publications, 0);
		UT_ASSERT_EQ(complete_consumptions, 1);
		complete_no_output(&out, &token);
		MyBackendType = B_STARTUP;
		complete_finish();
	}
}

#include "test_cluster_prepared_cf.h"
#include "test_cluster_generic_cf.h"
#include "test_cluster_rejoin_cf.h"
#include "test_cluster_cold_own_root_cf.h"

int
main(void)
{
	MyProcPid = 4002;
	owner_proc.pgprocno = 42;
	pg_atomic_init_u32(&owner_proc.cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked_epoch, 0);
	cluster_control_request_shmem_init();
	retire_driver = cluster_control_request_driver_start();
	if (getenv("PGRAC_PRE2_TEST_COLD_OWN_ROOT_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(cold_own_root_wait_retires_before_accepting_fresh_canonical_input);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_COLD_OWN_ROOT_CF") != NULL) {
		UT_PLAN(8);
		UT_RUN(cold_own_root_wait_retires_before_accepting_fresh_canonical_input);
		UT_RUN(cold_own_root_wait_rechecks_context_and_exact_request);
		UT_RUN(cold_own_root_fresh_read_cannot_escape_a_changed_context);
		UT_RUN(cold_own_root_receipt_and_wake_rules_preserve_progress);
		UT_RUN(cold_own_root_hard_untyped_and_preexisting_requests_are_not_retried);
		UT_RUN(cold_own_root_real_root_refusals_preserve_original_diagnostics);
		UT_RUN(cold_own_root_original_cancel_keeps_shared_cleanup_responsibility);
		UT_RUN(ordinary_cold_and_observational_canonical_keep_their_wait_policy);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_REJOIN_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(rejoin_cf_wait_keeps_original_staged_publication);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_GENERIC_CF_AUTHORITY") != NULL) {
		UT_PLAN(3);
		UT_RUN(generic_cf_refusal_consumes_authority_before_any_retry);
		UT_RUN(generic_cf_drift_never_consumes_a_different_publication);
		UT_RUN(generic_cf_unconfirmed_pin_release_never_restores_authority);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_PREPARED_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(lmon_prepared_cf_successful_publication_must_finish_original_round);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_SELF_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(startup_self_seal_cf_slice_waits_for_exact_completion_in_both_intervals);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_FAILURE_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(startup_failure_input_cf_slice_waits_with_original_authority);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_COMPLETE_CF_LIVENESS") != NULL) {
		UT_PLAN(1);
		UT_RUN(startup_recovery_complete_cf_wait_stays_with_one_consumed_publication);
		UT_DONE();
		return ut_failed_count != 0;
	}
	UT_PLAN(105);
	UT_RUN(startup_cf_timeout_waits_for_exact_completion_before_begin);
	UT_RUN(startup_cf_completion_does_not_preserve_changed_native_input);
	UT_RUN(startup_cf_unsafe_native_context_never_takes_a_registry_lock);
	UT_RUN(startup_cf_exact_ack_survives_reset_race_or_lost_latch);
	UT_RUN(startup_cf_both_original_modes_retire_before_any_root_operation);
	UT_RUN(startup_cf_crash_selection_remains_after_exact_retirement);
	UT_RUN(startup_cf_rejects_foreign_or_unproven_cleanup_without_polling);
	UT_RUN(startup_cf_wait_rechecks_native_owner_and_exact_cleanup_identity);
	UT_RUN(startup_cf_slot_disappearance_without_exact_receipt_is_not_completion);
	UT_RUN(startup_cf_hard_error_keeps_original_cleanup_and_error);
	UT_RUN(startup_cf_original_startup_termination_retains_shared_responsibility);
	UT_RUN(startup_cf_owner_service_ack_finishes_original_cf_receipt);
	UT_RUN(startup_cf_clean_input_waits_for_its_original_request_before_reread);
	UT_RUN(startup_cf_clean_input_does_not_retry_real_root_refusal);
	UT_RUN(startup_cf_clean_wait_rejects_changed_owner_or_retirement_identity);
	UT_RUN(startup_claim_cf_wait_retires_before_reacquiring);
	UT_RUN(startup_claim_cf_prepublication_wait_has_no_cleanup_authority);
	UT_RUN(startup_claim_cf_hard_native_and_stale_observations_do_not_retry);
	UT_RUN(startup_claim_cf_preexisting_slot_is_never_adopted);
	UT_RUN(startup_claim_cf_wait_requalifies_actor_custody_and_exact_request);
	UT_RUN(startup_claim_cf_missing_receipt_and_hard_cleanup_remain_errors);
	UT_RUN(startup_claim_cf_ack_race_and_lost_latch_still_make_progress);
	UT_RUN(startup_claim_cf_reacquire_cannot_return_changed_custody);
	UT_RUN(startup_claim_cf_original_startup_cancel_retains_cleanup);
	UT_RUN(startup_cold_cohort_cf_wait_does_not_become_release_failure);
	UT_RUN(startup_cold_cf_reacquire_rechecks_original_input_before_observation);
	UT_RUN(startup_cold_cf_wait_rejects_context_and_exact_identity_changes);
	UT_RUN(startup_cold_cf_receipt_is_required_and_wakes_are_only_scheduling);
	UT_RUN(startup_cold_cf_untyped_and_hard_cleanup_are_not_wait_slices);
	UT_RUN(startup_cold_cf_original_cancel_leaves_the_original_cleanup_owner);
	UT_RUN(startup_input_cf_wait_keeps_initial_scratch_and_retained_scopes);
	UT_RUN(startup_input_cf_wait_rejects_changed_actor_scope_pin_and_request);
	UT_RUN(startup_input_cf_wait_cannot_adopt_changed_tx_or_ctrc_start);
	UT_RUN(startup_input_cf_new_grant_rechecks_scope_before_root_io);
	UT_RUN(startup_input_cf_receipt_and_wake_rules_preserve_progress);
	UT_RUN(startup_input_cf_untyped_hard_and_preexisting_owners_are_not_retried);
	UT_RUN(startup_input_cf_original_cancel_keeps_the_original_cleanup_owner);
	UT_RUN(ordinary_input_cf_timeout_keeps_its_original_release_behavior);
	UT_RUN(startup_checkpoint_scratch_cf_wait_keeps_its_original_selection);
	UT_RUN(startup_retention_cf_slice_waits_for_original_completion);
	UT_RUN(startup_retention_wait_rejects_changed_actor_input_and_exact_request);
	UT_RUN(startup_retention_new_grant_requalifies_before_root_io);
	UT_RUN(startup_retention_receipt_and_wake_rules_preserve_progress);
	UT_RUN(startup_retention_hard_untyped_and_preexisting_requests_are_not_retried);
	UT_RUN(startup_retention_preserves_real_root_and_token_refusals);
	UT_RUN(startup_retention_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_retention_and_observational_canonical_keep_original_wait_policy);
	UT_RUN(startup_recovery_wal_cf_slice_waits_without_exporting_provisional_input);
	UT_RUN(startup_recovery_wal_wait_rechecks_original_actor_input_and_request);
	UT_RUN(startup_recovery_wal_new_grant_rechecks_before_root_io);
	UT_RUN(startup_recovery_wal_receipt_and_wake_rules_preserve_progress);
	UT_RUN(startup_recovery_wal_hard_untyped_and_preexisting_requests_do_not_retry);
	UT_RUN(startup_recovery_wal_real_root_refusals_and_errors_never_export_input);
	UT_RUN(startup_recovery_wal_second_interval_keeps_original_owner_and_failure);
	UT_RUN(startup_recovery_wal_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_recovery_wal_roles_keep_the_original_wait_policy);
	UT_RUN(startup_self_seal_cf_slice_waits_for_exact_completion_in_both_intervals);
	UT_RUN(startup_self_seal_wait_rechecks_original_actor_purpose_and_request);
	UT_RUN(startup_self_seal_new_grant_requalifies_before_root_io);
	UT_RUN(startup_self_seal_receipt_and_wake_rules_preserve_progress);
	UT_RUN(startup_self_seal_hard_untyped_and_preexisting_requests_do_not_retry);
	UT_RUN(startup_self_seal_real_root_and_wal_refusals_remain_errors);
	UT_RUN(startup_self_seal_second_interval_keeps_the_original_owner);
	UT_RUN(startup_self_seal_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_self_open_read_keeps_the_original_wait_policy);
	UT_RUN(startup_failure_input_cf_slice_waits_with_original_authority);
	UT_RUN(startup_failure_input_wait_rechecks_original_actor_purpose_guard_and_request);
	UT_RUN(startup_failure_input_new_grant_requalifies_before_root_io);
	UT_RUN(startup_failure_input_receipt_and_wake_rules_preserve_progress);
	UT_RUN(startup_failure_input_hard_untyped_and_preexisting_requests_do_not_retry);
	UT_RUN(startup_failure_input_real_root_and_wal_refusals_remain_errors);
	UT_RUN(startup_failure_tail_second_interval_keeps_the_original_owner);
	UT_RUN(startup_failure_input_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_failure_input_roles_keep_the_original_wait_policy);
	UT_RUN(startup_recovery_complete_cf_wait_stays_with_one_consumed_publication);
	UT_RUN(startup_recovery_complete_wait_rechecks_actor_patch_sealed_guard_and_exact_request);
	UT_RUN(startup_recovery_complete_new_grant_requalifies_before_root_io);
	UT_RUN(startup_recovery_complete_receipt_wake_and_empty_preserve_progress);
	UT_RUN(startup_recovery_complete_hard_untyped_and_preexisting_are_not_retried);
	UT_RUN(startup_recovery_complete_real_errors_never_export_provisional_or_persisted_permission);
	UT_RUN(startup_recovery_complete_original_preconditions_do_not_become_cf_waits);
	UT_RUN(startup_recovery_complete_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_recovery_complete_roles_keep_the_original_cf_wait_policy);
	UT_RUN(lmon_prepared_cf_requires_exact_retirement_before_reapply);
	UT_RUN(lmon_prepared_cf_wrong_ack_cannot_release_the_original_request);
	UT_RUN(lmon_prepared_cf_service_consumed_ack_still_finishes_own_receipt);
	UT_RUN(lmon_prepared_cf_retry_rechecks_coordinator_and_prepared_ack);
	UT_RUN(lmon_prepared_cf_real_token_and_round_failure_still_refuse);
	UT_RUN(lmon_prepared_cf_hard_failure_never_publishes_root);
	UT_RUN(generic_cf_refusal_consumes_authority_before_any_retry);
	UT_RUN(generic_cf_drift_never_consumes_a_different_publication);
	UT_RUN(generic_cf_unconfirmed_pin_release_never_restores_authority);
	UT_RUN(rejoin_cf_slice_releases_stage_but_keeps_original_cleanup);
	UT_RUN(rejoin_fresh_attempt_rechecks_original_root_and_jcmk);
	UT_RUN(rejoin_fresh_proof_drift_never_reuses_consumed_publication);
	UT_RUN(rejoin_staged_marker_wait_is_not_a_root_wait);
	UT_RUN(rejoin_open_exact_owner_still_requires_fresh_import);
	UT_RUN(cold_own_root_wait_retires_before_accepting_fresh_canonical_input);
	UT_RUN(cold_own_root_wait_rechecks_context_and_exact_request);
	UT_RUN(cold_own_root_fresh_read_cannot_escape_a_changed_context);
	UT_RUN(cold_own_root_receipt_and_wake_rules_preserve_progress);
	UT_RUN(cold_own_root_hard_untyped_and_preexisting_requests_are_not_retried);
	UT_RUN(cold_own_root_real_root_refusals_preserve_original_diagnostics);
	UT_RUN(cold_own_root_original_cancel_keeps_shared_cleanup_responsibility);
	UT_RUN(ordinary_cold_and_observational_canonical_keep_their_wait_policy);
	UT_DONE();
	return ut_failed_count != 0;
}
