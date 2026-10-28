/* Actual CF failure/retirement producer and native Startup entry.
 * S4, ROOT I/O, scheduling and authority freshness are fixture boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "unit_test.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_config_members.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
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
	if (entry_ack_on_reset) {
		entry_ack_on_reset = false;
		UT_ASSERT(acknowledge_retirements());
	}
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
	UT_ASSERT_EQ(timeout, 20);
	UT_ASSERT_EQ(event, WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
	UT_ASSERT(!entry_native_lock && CritSectionCount == 0 && control_locked == NULL);
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
	UT_PLAN(12);
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
	UT_DONE();
	return ut_failed_count != 0;
}
