/* Unbound pin and ROOT consumers joined to the actual CF/owner/CONTROL.
 * WALR grants, formation/fence and physical ROOT/subject I/O are explicit
 * leaves; no IR, native recovery or isolation authority is minted here.
 * Author: SqlRush <sqlrush@gmail.com> */
#define CLUSTER_WAL_PIN_MAGIC UINT32_C(0x57414c50)
#include "test_cluster_wal_pin_cf_types.inc"

static ClusterWalRetentionPin *active_pin;
static ClusterWalReadPinV1 *active_read_pin;
static ClusterWalRootPublishGuard *active_root_publish_guard;
static ClusterWalReuseActionGuard *active_reuse_guard;
static unsigned pin_cf_walr_acquires, pin_cf_walr_releases;
static bool pin_cf_walr_live, pin_cf_release_confirmed;
static ClusterControlRecoverySubject pin_cf_subject;
static ClusterWalRetentionPinThreadRequest pin_cf_request;
static ClusterWalRetentionPin *pin_cf_out;
static ClusterWalPinThread pin_cf_thread;
static ClusterWalPinLock pin_cf_reader;
static ClusterWalRetentionPin pin_cf_existing;
static bool pin_cf_preflight;
static int pin_cf_walr_budgets[8], pin_cf_release_budget;
static uint64 pin_cf_after_walr_ns;
static bool pin_cf_walr_error, pin_cf_walr_refuse;

static void
walr_native_locktag_init(uint16 thread pg_attribute_unused(), LOCKTAG *tag pg_attribute_unused())
{}
static void
walr_native_lock_release_or_fatal(const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	UT_ASSERT(false);
}
static ClusterLockAcquireResult
walr_request_finish_native(ClusterLockAcquireRequest *request pg_attribute_unused(),
						   void *argument pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

/* These leaves only stand in for the pre-existing WALR/input setup. */
static bool
walr_share_request_init(uint16 thread, ClusterLockAcquireRequest *out)
{
	memset(out, 0, sizeof(*out));
	out->lockmode = ShareLock;
	out->resid.field1 = thread;
	out->request_id = out->holder.request_id = 500 + thread;
	out->control_owner_id = 900 + thread;
	return true;
}
static void
walr_resource_ensure_callback(void)
{}
static bool
pin_request_valid(const ClusterWalRetentionPinThreadRequest *request, int segsize, Size *bytes)
{
	UT_ASSERT(request == &pin_cf_request && segsize == wal_segment_size && bytes != NULL);
	return true;
}
static ClusterLockAcquireResult
pin_cf_owner_acquire(ClusterLockAcquireRequest *request,
					 ClusterControlNativeFinish finish pg_attribute_unused(),
					 void *argument pg_attribute_unused())
{
	UT_ASSERT_EQ(request->lockmode, ShareLock);
	UT_ASSERT(pin_cf_walr_acquires < lengthof(pin_cf_walr_budgets));
	pin_cf_walr_budgets[pin_cf_walr_acquires++] = request->timeout_ms;
	set_cf_clock_ns += pin_cf_after_walr_ns;
	if (pin_cf_walr_error)
		ereport(ERROR, (errmsg("modeled WALR transport error")));
	return pin_cf_walr_refuse ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}
static ClusterLockAcquireResult
walr_request_release_actual(const ClusterLockAcquireRequest *request)
{
	UT_ASSERT_EQ(request->lockmode, ShareLock);
	++pin_cf_walr_releases;
	pin_cf_release_budget = request->timeout_ms;
	return pin_cf_release_confirmed ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
									: CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
}
static bool
pin_cf_walr_current(const ClusterLockAcquireRequest *request)
{
	return pin_cf_walr_live && request->control_owner_id == 900 + request->resid.field1
		   && request->lockmode == ShareLock;
}
static bool
pin_cf_nonshared_identity(const ClusterControlRootIdentity *a, const ClusterControlRootIdentity *b)
{
	(void)a;
	(void)b;
	UT_ASSERT(false);
	return false;
}
static bool
pin_cf_nonshared_verify(void)
{
	UT_ASSERT(false);
	return false;
}
static char pin_cf_detail[512];
static bool
pin_cf_errstart(int level, const char *domain)
{
	bool enabled = errstart(level, domain);

	return enabled || level == LOG;
}
static int
pin_cf_errdetail(const char *format, ...)
{
	va_list args;

	va_start(args, format);
	vsnprintf(pin_cf_detail, sizeof(pin_cf_detail), format, args);
	va_end(args);
	return 0;
}
#define clock_gettime cf_budget_clock_gettime
#define cluster_lock_owner_request_acquire pin_cf_owner_acquire
#define errstart pin_cf_errstart
#define errdetail pin_cf_errdetail
#define acquire_clusterwide_cf prepared_acquire
#define release_cf prepared_release
#define read_canonical_pair prepared_read_pair
#define cluster_control_root_identity_equal pin_cf_nonshared_identity
#define cluster_r4_bit22_cutover_latch_verify pin_cf_nonshared_verify
#define cluster_lock_owner_request_usable pin_cf_walr_current
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#include "test_cluster_wal_pin_cf.inc"
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_lock_owner_request_usable
#undef cluster_r4_bit22_cutover_latch_verify
#undef cluster_control_root_identity_equal
#undef read_canonical_pair
#undef release_cf
#undef acquire_clusterwide_cf
#undef errdetail
#undef errstart
#undef cluster_lock_owner_request_acquire
#undef clock_gettime

static void
pin_cf_fixture(bool preflight, bool pending)
{
	pin_cf_detail[0] = 0;
	UT_ASSERT(lock_owner_creator());
	retention_fixture(1);
	retention_image.header.file_txn_seq = 7;
	retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	retention_image.records[0].root_flags = COLD_REQUIRED_ROOT_FLAGS;
	memset(&pin_cf_subject, 0, sizeof(pin_cf_subject));
	pin_cf_subject.kind = CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER;
	pin_cf_subject.duty = retention_expected;
	pin_cf_subject.pending.generation = 19;
	if (pending)
		retention_subject = &pin_cf_subject;
	memset(&pin_cf_request, 0, sizeof(pin_cf_request));
	pin_cf_request.duty = retention_expected;
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY, &pin_cf_request.root_read);
	if (pending)
		pin_cf_request.pending = pin_cf_subject.pending;
	pin_cf_request.formation = (const ClusterFormationWitnessV1 *)1;
	pin_cf_request.needs = (const PgracExternalFenceNeedSetV1 *)2;
	pin_cf_request.admissions = (const PgracExternalFenceAdmissionSetV1 *)3;
	failure_formation = failure_needs = failure_admissions = true;
	pin_cf_walr_live = pin_cf_release_confirmed = true;
	pin_cf_walr_acquires = pin_cf_walr_releases = 0;
	pin_cf_after_walr_ns = 0;
	cf_budget_clock_fail = false;
	pin_cf_walr_error = pin_cf_walr_refuse = false;
	pin_cf_release_budget = -1;
	memset(pin_cf_walr_budgets, 0, sizeof(pin_cf_walr_budgets));
	pin_cf_preflight = preflight;
	pin_cf_out = NULL;
	active_pin = NULL;
	active_read_pin = NULL;
	active_root_publish_guard = NULL;
	active_reuse_guard = NULL;
	if (preflight) {
		memset(&pin_cf_thread, 0, sizeof(pin_cf_thread));
		memset(&pin_cf_reader, 0, sizeof(pin_cf_reader));
		memset(&pin_cf_existing, 0, sizeof(pin_cf_existing));
		pin_cf_thread.duty = pin_cf_request.duty;
		pin_cf_thread.root_read = pin_cf_request.root_read;
		pin_cf_thread.pending = pin_cf_request.pending;
		pin_cf_thread.formation = pin_cf_request.formation;
		pin_cf_thread.needs = pin_cf_request.needs;
		pin_cf_thread.admissions = pin_cf_request.admissions;
		walr_share_request_init(1, &pin_cf_thread.walr.request);
		pin_cf_thread.walr.held = true;
		walr_share_request_init(2, &pin_cf_reader.request);
		pin_cf_reader.held = true;
		pin_cf_existing.magic = CLUSTER_WAL_PIN_MAGIC;
		pin_cf_existing.owner_pid = MyProcPid;
		pin_cf_existing.owner = CurrentResourceOwner;
		pin_cf_existing.nthreads = 1;
		pin_cf_existing.state = CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND;
		pin_cf_existing.threads = &pin_cf_thread;
		pin_cf_existing.nreaders = 1;
		pin_cf_existing.readers = &pin_cf_reader;
		active_pin = &pin_cf_existing;
	}
}
static bool
pin_cf_call(ClusterWalPinResult *result)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*result = pin_cf_preflight
					  ? cluster_wal_retention_pin_preflight_revalidate_wait_v1(&pin_cf_existing)
					  : cluster_wal_retention_pin_acquire(&pin_cf_request, 1, &pin_cf_out);
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
pin_cf_cleanup(void)
{
	if (!pin_cf_preflight && active_pin != NULL) {
		UT_ASSERT_EQ(pin_release_locks(active_pin), CLUSTER_WALR_RELEASE_CONFIRMED);
		pin_free(active_pin);
	}
	active_pin = NULL;
	retention_subject = NULL;
	cold_cleanup();
}
UT_TEST(startup_unbound_pin_retires_cf_before_revalidating_original_input)
{
	for (unsigned preflight = 0; preflight < 2; preflight++) {
		for (unsigned pending = 0; pending < 2; pending++) {
			ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

			pin_cf_fixture(preflight, pending);
			UT_ASSERT(pin_cf_call(&result));
			printf("# unbound pin preflight=%u pending=%u result=%d waits=%u attempts=%u "
				   "reads=%u walr_acquires=%u walr_releases=%u cookie=" UINT64_FORMAT "\n",
				   preflight, pending, result, entry_waits, retention_attempts, retention_threads,
				   pin_cf_walr_acquires, pin_cf_walr_releases, cluster_cf_owner_cookie(ShareLock));
			UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_OK);
			UT_ASSERT_EQ(entry_waits, 3);
			UT_ASSERT_EQ(retention_attempts, 2);
			UT_ASSERT_EQ(retention_threads, 1);
			UT_ASSERT_EQ(pin_cf_walr_acquires, !preflight);
			UT_ASSERT_EQ(pin_cf_walr_releases, 0);
			UT_ASSERT(active_pin != NULL && !active_pin->poisoned);
			UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
			pin_cf_cleanup();
		}
	}
}

static unsigned pin_cf_fault;
static CfHoldState pin_cf_saved_slot;
static ClusterControlRequestView pin_cf_saved_record;
static ClusterWalRetentionPin pin_cf_saved_pin;
static ClusterWalPinThread pin_cf_saved_thread;
static ClusterWalPinLock pin_cf_saved_reader;
static ClusterWalRetentionInterval pin_cf_interval;
static uint64 pin_cf_epoch, pin_cf_generation, pin_cf_boot;
static int pin_cf_pid;
static bool pin_cf_recovery;

static void
pin_cf_restore(void)
{
	MyProc = &owner_proc;
	MyLatch = &owner_proc.procLatch;
	MyProcPid = pin_cf_pid;
	CurrentResourceOwner = (ResourceOwner)1;
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	entry_native_lock = false;
	ShutdownRequestPending = false;
	InRecovery = pin_cf_recovery;
	cluster_node_id = 0;
	cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
	entry_authority = true;
	owner_epoch = pin_cf_epoch;
	owner_generation = pin_cf_generation;
	control_creator.incarnation = pin_cf_boot;
	pin_cf_existing = pin_cf_saved_pin;
	pin_cf_thread = pin_cf_saved_thread;
	pin_cf_reader = pin_cf_saved_reader;
	pin_cf_interval.end_lsn = 200;
	active_pin = &pin_cf_existing;
	active_root_publish_guard = NULL;
	active_reuse_guard = NULL;
	failure_formation = failure_needs = failure_admissions = pin_cf_walr_live = true;
}
static void
pin_cf_corrupt(void)
{
	retention_ready_after_ack();
	pin_cf_saved_slot = cf_hold_s;
	pin_cf_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	switch (pin_cf_fault) {
	case 0:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 1:
		cluster_node_id++;
		break;
	case 2:
		owner_epoch++;
		break;
	case 3:
		owner_generation++;
		break;
	case 4:
		control_creator.incarnation++;
		break;
	case 5:
		MyProc = NULL;
		break;
	case 6:
		MyLatch = NULL;
		break;
	case 7:
		MyProcPid++;
		break;
	case 8:
		entry_authority = false;
		break;
	case 9:
		entry_native_lock = true;
		break;
	case 10:
		CritSectionCount = 1;
		break;
	case 11:
		cluster_shared_config = false;
		break;
	case 12:
		InRecovery = !InRecovery;
		break;
	case 13:
		cluster_enabled = false;
		break;
	case 14:
		pin_cf_existing.state = CLUSTER_WAL_PIN_STATE_BOUND_ONE;
		break;
	case 15:
		pin_cf_existing.poisoned = true;
		break;
	case 16:
		pin_cf_thread.duty.thread_claim_crc32c++;
		break;
	case 17:
		pin_cf_thread.root_read.root_publish_seq++;
		break;
	case 18:
		pin_cf_thread.pending.generation++;
		break;
	case 19:
		pin_cf_thread.formation = NULL;
		break;
	case 20:
		pin_cf_thread.needs = NULL;
		break;
	case 21:
		pin_cf_thread.admissions = NULL;
		break;
	case 22:
		pin_cf_thread.serial = (ClusterRecoverySerialGuard *)1;
		break;
	case 23:
		pin_cf_thread.walr.request.control_owner_id++;
		break;
	case 24:
		pin_cf_reader.request.control_owner_id++;
		break;
	case 25:
		pin_cf_walr_live = false;
		break;
	case 26:
		failure_formation = false;
		break;
	case 27:
		failure_needs = false;
		break;
	case 28:
		failure_admissions = false;
		break;
	case 29:
		pin_cf_interval.end_lsn++;
		break;
	case 30:
		active_pin = NULL;
		break;
	case 31:
		pin_cf_existing.magic++;
		break;
	case 32:
		pin_cf_existing.owner_pid++;
		break;
	case 33:
		pin_cf_existing.owner = (ResourceOwner)2;
		break;
	case 34:
		pin_cf_existing.nthreads++;
		break;
	case 35:
		pin_cf_existing.threads = NULL;
		break;
	case 36:
		pin_cf_thread.nintervals = 0;
		break;
	case 37:
		cf_hold_s.coordinated = false;
		break;
	case 38:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 39:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 40:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 41:
		cf_hold_s.release_pending = false;
		break;
	case 42:
		cf_hold_x = cf_hold_s;
		break;
	case 43:
		cf_hold_s.owner.state = CLUSTER_LOCK_OWNER_HELD;
		break;
	}
}
UT_TEST(startup_unbound_pin_wait_rechecks_all_inputs_and_exact_cleanup)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		for (pin_cf_fault = 0; pin_cf_fault < 44; pin_cf_fault++) {
			ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

			pin_cf_fixture(true, pending);
			pin_cf_epoch = owner_epoch;
			pin_cf_generation = owner_generation;
			pin_cf_boot = control_creator.incarnation;
			pin_cf_pid = MyProcPid;
			pin_cf_recovery = InRecovery;
			memset(&pin_cf_interval, 0, sizeof(pin_cf_interval));
			pin_cf_interval.start_lsn = 100;
			pin_cf_interval.end_lsn = 200;
			pin_cf_thread.intervals = &pin_cf_interval;
			pin_cf_thread.nintervals = 1;
			pin_cf_saved_pin = pin_cf_existing;
			pin_cf_saved_thread = pin_cf_thread;
			pin_cf_saved_reader = pin_cf_reader;
			entry_on_wait = pin_cf_corrupt;
			UT_ASSERT(pin_cf_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
			UT_ASSERT(strstr(pin_cf_detail, pin_cf_fault < 37
												? "reason=CONTEXT_CHANGED"
												: "reason=CF_WAIT_IDENTITY_OR_COMPLETION")
					  != NULL);
			UT_ASSERT_EQ(entry_waits, 1);
			UT_ASSERT_EQ(retention_attempts, 1);
			UT_ASSERT_EQ(retention_threads, 0);
			UT_ASSERT_EQ(pin_cf_walr_releases, 0);
			UT_ASSERT(pin_cf_existing.poisoned);
			pin_cf_restore();
			cf_hold_s = pin_cf_saved_slot;
			memset(&cf_hold_x, 0, sizeof(cf_hold_x));
			control_requests->records[cf_hold_s.owner.attempt.slot] = pin_cf_saved_record;
			pin_cf_cleanup();
		}
	}
}
static void
pin_cf_change_after_read(void)
{
	if (pin_cf_fault == 0)
		active_pin->state = CLUSTER_WAL_PIN_STATE_BOUND_ONE;
	else if (pin_cf_fault == 1)
		active_pin->poisoned = true;
	else if (pin_cf_fault == 2)
		active_pin->threads[0].nintervals++;
	else
		active_pin->threads[0].pending.generation++;
}
UT_TEST(startup_unbound_pin_read_output_cannot_escape_changed_inputs)
{
	for (unsigned preflight = 0; preflight < 2; preflight++) {
		for (unsigned pending = 0; pending < 2; pending++) {
			for (pin_cf_fault = 0; pin_cf_fault < 4; pin_cf_fault++) {
				ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

				pin_cf_fixture(preflight, pending);
				retention_after_read = pin_cf_change_after_read;
				UT_ASSERT(pin_cf_call(&result));
				UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
				UT_ASSERT(strstr(pin_cf_detail, "reason=CONTEXT_CHANGED_AFTER_READ") != NULL);
				UT_ASSERT_EQ(entry_waits, 3);
				UT_ASSERT_EQ(retention_attempts, 2);
				UT_ASSERT_EQ(retention_threads, 1);
				UT_ASSERT_EQ(pin_cf_walr_releases, !preflight);
				UT_ASSERT(preflight ? pin_cf_existing.poisoned : pin_cf_out == NULL);
				UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
				pin_cf_cleanup();
			}
		}
	}
}
static void
pin_cf_service_ready(void)
{
	/* Dispatch/ACK is still the actual owner service below, not this leaf. */
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_next_request_id++;
	claim_on_reset = NULL;
}
UT_TEST(startup_unbound_pin_receipt_and_wake_rules_preserve_liveness)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		for (unsigned fault = 0; fault < 5; fault++) {
			ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

			pin_cf_fixture(true, pending);
			entry_lost_latch = true;
			if (fault == 0)
				entry_on_wait = retention_remove_receipt;
			else if (fault == 1)
				claim_on_reset = cold_ack_during_reset;
			else if (fault == 2) {
				fail_after_publication = false;
				entry_on_wait = retention_empty_grant;
			} else if (fault == 4) {
				entry_ack_on_service = true;
				claim_on_reset = pin_cf_service_ready;
			}
			UT_ASSERT(pin_cf_call(&result));
			UT_ASSERT_EQ(result, fault == 0 ? CLUSTER_WAL_PIN_STALE : CLUSTER_WAL_PIN_OK);
			UT_ASSERT_EQ(entry_waits, fault == 1 || fault == 4 ? 0 : fault == 3 ? 3 : 1);
			UT_ASSERT_EQ(retention_attempts, fault == 0 ? 1 : 2);
			UT_ASSERT_EQ(retention_threads, fault != 0);
			UT_ASSERT_EQ(pin_cf_walr_releases, 0);
			if (fault == 0)
				cf_retired_s = claim_retiring_cookie;
			pin_cf_cleanup();
		}
	}
}
static void
pin_cf_real_refusal(void)
{
	retention_ready_after_ack();
	if (entry_waits != entry_ack_at)
		return;
	switch (pin_cf_fault) {
	case 0:
		retention_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		break;
	case 1:
		retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		break;
	case 2:
		retention_root_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		break;
	case 3:
		retention_image.records[0].root_publish_seq++;
		break;
	case 4:
		retention_image.records[0].root_flags ^= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
		break;
	case 5:
		retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		break;
	case 6:
		retention_image.header.file_txn_seq++;
		break;
	case 7:
		retention_image.record_crc32c[0]++;
		break;
	case 8:
		pin_cf_subject.kind = CLUSTER_CONTROL_RECOVERY_CURRENT_CHECKPOINT;
		break;
	case 9:
		pin_cf_subject.duty.thread_claim_crc32c++;
		break;
	case 10:
		pin_cf_subject.pending.generation++;
		break;
	}
}
UT_TEST(startup_unbound_pin_keeps_real_token_and_subject_refusals)
{
	for (unsigned preflight = 0; preflight < 2; preflight++) {
		for (unsigned pending = 0; pending < 2; pending++) {
			for (pin_cf_fault = pending ? 8 : 0; pin_cf_fault < (pending ? 11 : 8);
				 pin_cf_fault++) {
				ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

				pin_cf_fixture(preflight, pending);
				entry_on_wait = pin_cf_real_refusal;
				UT_ASSERT(pin_cf_call(&result));
				UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
				UT_ASSERT(strstr(pin_cf_detail, "reason=ROOT_OR_TOKEN_REFUSED") != NULL);
				UT_ASSERT_EQ(entry_waits, 3);
				UT_ASSERT_EQ(retention_attempts, 2);
				UT_ASSERT_EQ(retention_threads, 1);
				UT_ASSERT_EQ(pin_cf_walr_releases, !preflight);
				UT_ASSERT(preflight ? pin_cf_existing.poisoned : pin_cf_out == NULL);
				pin_cf_cleanup();
			}
		}
	}
}
UT_TEST(startup_unbound_pin_hard_untyped_and_preexisting_cf_are_not_retried)
{
	for (unsigned preflight = 0; preflight < 2; preflight++) {
		for (unsigned fault = 0; fault < 7; fault++) {
			ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;
			unsigned before;

			pin_cf_fixture(preflight, false);
			if (fault == 0)
				g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			else if (fault == 1)
				g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
			else if (fault == 2)
				entry_on_wait = entry_hard_error;
			else {
				if (fault == 4)
					g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
				if (fault < 6)
					UT_ASSERT_EQ(cluster_cf_lock(fault == 5 ? ExclusiveLock : ShareLock),
								 fault == 4);
				else
					pin_cf_release_confirmed = false;
			}
			before = g_seven_count;
			if (fault == 6) {
				g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
				UT_ASSERT(pin_cf_call(&result));
				UT_ASSERT_EQ(result,
							 preflight ? CLUSTER_WAL_PIN_STALE : CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
				UT_ASSERT(active_pin != NULL && active_pin->poisoned);
				if (!preflight)
					UT_ASSERT(active_pin->threads[0].walr.release_uncertain);
			} else {
				UT_ASSERT_EQ(pin_cf_call(&result), fault != 2);
				if (fault == 2)
					UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
				else
					UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
			}
			UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
			UT_ASSERT_EQ(g_seven_count, before + (fault < 3 || fault == 6 ? 1 : 0));
			UT_ASSERT_EQ(retention_threads, 0);
			if (fault == 4) {
				claim_teardown = owner_ack_on_wait = true;
				UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
				claim_teardown = owner_ack_on_wait = false;
			}
			if (fault == 5) {
				cold_consumer = false;
				entry_cleanup(ExclusiveLock, cluster_cf_owner_cookie(ExclusiveLock));
			}
			pin_cf_release_confirmed = true;
			pin_cf_cleanup();
		}
	}
}
UT_TEST(startup_unbound_pin_cancel_retains_original_cleanup_responsibility)
{
	for (unsigned preflight = 0; preflight < 2; preflight++) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;
		volatile bool exited = false;

		pin_cf_fixture(preflight, false);
		entry_on_wait = entry_shutdown;
		startup_fixture_exit_hook = entry_owner_exit;
		startup_fixture_armed = true;
		if (sigsetjmp(startup_fixture_exit, 1) == 0)
			UT_ASSERT(!pin_cf_call(&result));
		else
			exited = true;
		UT_ASSERT(exited && result != CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(startup_fixture_exit_code, 1);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(retention_threads, 0);
		UT_ASSERT_EQ(pin_cf_walr_releases, 0);
		UT_ASSERT(active_pin != NULL && active_pin->threads[0].walr.held);
		UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
		PG_exception_stack = NULL;
		error_context_stack = NULL;
		proc_exit_inprogress = shutdown_requested = false;
		startup_fixture_armed = false;
		startup_fixture_exit_hook = NULL;
		pin_cf_cleanup();
	}
}
UT_TEST(ordinary_and_bound_pin_wait_policies_remain_unchanged)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };

	for (unsigned i = 0; i < lengthof(roles); i++) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

		pin_cf_fixture(true, false);
		MyBackendType = roles[i];
		UT_ASSERT(pin_cf_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_threads, 0);
		MyBackendType = B_STARTUP;
		pin_cf_cleanup();
	}
	for (unsigned state = CLUSTER_WAL_PIN_STATE_BOUND_ONE; state <= CLUSTER_WAL_PIN_STATE_SEALED;
		 state++) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_OK;

		pin_cf_fixture(true, false);
		pin_cf_existing.state = state;
		UT_ASSERT(pin_cf_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_INVALID);
		UT_ASSERT_EQ(entry_waits | retention_attempts, 0);
		pin_cf_cleanup();
	}
}
