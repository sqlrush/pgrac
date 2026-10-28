/* Actual CF failure/retirement producer and native Startup entry.
 * S4, ROOT I/O, scheduling and authority freshness are fixture boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_qvotec.h"
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
errdetail(const char *format pg_attribute_unused(), ...)
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
	UT_PLAN(30);
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
	UT_DONE();
	return ut_failed_count != 0;
}
