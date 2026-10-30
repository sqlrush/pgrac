/* Original cold caller and runtime lookup, joined to canonical CF/CONTROL.
 * Physical ROOT/WAL, formation and restart observations remain test leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
int cluster_cold_recovery_plan_memory;
static bool own_cohort_available, own_restart_available;
static bool
own_capture(uint16 thread, ClusterFormationSnapshotV1 *out)
{
	return own_cohort_available && cluster_reconfig_capture_formation_snapshot_v1(thread, out);
}
static bool
own_restart(ClusterWalSourceRef *out)
{
	return own_restart_available && cluster_wal_thread_restart_v2_ref(out);
}
#define acquire_clusterwide_cf prepared_acquire
#define read_canonical_pair prepared_read_pair
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define cluster_reconfig_capture_formation_snapshot_v1 own_capture
#define cluster_wal_thread_restart_v2_ref own_restart
#define clock_gettime cf_budget_clock_gettime
#include "test_cluster_cold_own_root_cf.inc"
#undef clock_gettime
#undef cluster_wal_thread_restart_v2_ref
#undef cluster_reconfig_capture_formation_snapshot_v1
#undef cluster_recovery_duty_key_compare_for_claim
#undef read_canonical_pair
#undef acquire_clusterwide_cf

static ClusterColdTypedV1 own_typed;
static ColdRoot own_root;
static uint64 own_epoch, own_generation, own_boot;
static int own_pid;

static void
own_ready(void)
{
	retention_ready_after_ack();
	/* The first read must select this new cut, never a cached predecessor. */
	retention_image.records[0].root_publish_seq = 42;
}

static void
own_fixture(void)
{
	/* The boot-identity leaf uses the real fixture creator. Initialize it
	 * before capturing the caller context, including when run in isolation. */
	UT_ASSERT(lock_owner_creator());
	retention_fixture(1);
	own_epoch = owner_epoch;
	own_generation = owner_generation;
	own_boot = control_creator.incarnation;
	own_pid = MyProcPid;
	own_cohort_available = own_restart_available = true;
	memset(&own_typed, 0, sizeof(own_typed));
	memset(&own_root, 0, sizeof(own_root));
	own_typed.context = (MemoryContext)1;
	own_typed.replay_roots = &own_root;
	retention_image.records[0].identity.authority_uuid[6] = 0x40;
	retention_image.records[0].identity.authority_uuid[8] = 0x80;
	retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	retention_image.records[0].root_flags = COLD_REQUIRED_ROOT_FLAGS;
	entry_on_wait = own_ready;
}

static bool
own_call(bool *accepted)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*accepted = cold_own_root(&own_typed, 1, &own_root, NULL);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(cold_own_root_wait_retires_before_accepting_fresh_canonical_input)
{
	bool accepted = false;

	own_fixture();
	UT_ASSERT(own_call(&accepted));
	printf("# cold own-root accepted=%d refusal=%d waits=%u attempts=%u reads=%u "
		   "cookie=" UINT64_FORMAT " state=%d\n",
		   accepted, (int)own_typed.refusal, entry_waits, retention_attempts,
		   retention_controls + retention_threads, cluster_cf_owner_cookie(ShareLock),
		   (int)cf_hold_s.owner.state);
	UT_ASSERT(accepted);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(retention_attempts, 2);
	UT_ASSERT_EQ(retention_controls, 1);
	UT_ASSERT_EQ(retention_threads, 1);
	UT_ASSERT_EQ(own_root.token.root_publish_seq, 42);
	UT_ASSERT_EQ(own_root.root.root_publish_seq, 42);
	UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
	cold_cleanup();
}

static unsigned own_fault;
static CfHoldState own_saved_slot;
static ClusterControlRequestView own_saved_record;

static void
own_corrupt(void)
{
	switch (own_fault) {
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
		cf_hold_s.coordinated = false;
		break;
	case 16:
		cf_hold_x = cf_hold_s;
		break;
	case 17:
		MyProc = NULL;
		break;
	case 18:
		MyLatch = NULL;
		break;
	case 19:
		MyProcPid++;
		break;
	case 20:
		MyBackendType = B_CHECKPOINTER;
		break;
	case 21:
		cluster_enabled = false;
		break;
	case 22:
		cluster_shared_config = false;
		break;
	case 23:
		cluster_controlfile_shared_authority = false;
		break;
	case 24:
		cluster_node_id++;
		break;
	case 25:
		own_typed.context = (MemoryContext)2;
		break;
	case 26:
		own_typed.replay_roots = NULL;
		break;
	case 27:
		cf_hold_s.owner.state = CLUSTER_LOCK_OWNER_HELD;
		break;
	case 28:
		cf_hold_s.owner.request.holder.request_id++;
		break;
	case 29:
		control_requests->records[cf_hold_s.owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 30:
		own_cohort_available = false;
		break;
	case 31:
		own_restart_available = false;
		break;
	default:
		UT_ASSERT(false);
		break;
	}
}
static void
own_change_context(void)
{
	own_ready();
	own_saved_slot = cf_hold_s;
	own_saved_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	own_corrupt();
}
static void
own_restore_actor(void)
{
	MyProc = &owner_proc;
	MyLatch = &owner_proc.procLatch;
	MyProcPid = own_pid;
	CurrentResourceOwner = (ResourceOwner)1;
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	entry_native_lock = false;
	cluster_node_id = 0;
	cluster_enabled = cluster_shared_config = cluster_controlfile_shared_authority = true;
	entry_authority = true;
	ShutdownRequestPending = false;
	owner_epoch = own_epoch;
	owner_generation = own_generation;
	control_creator.incarnation = own_boot;
	own_typed.context = (MemoryContext)1;
	own_typed.replay_roots = &own_root;
}
UT_TEST(cold_own_root_wait_rechecks_context_and_exact_request)
{
	for (own_fault = 0; own_fault < 32; own_fault++) {
		bool accepted = false;

		own_fixture();
		entry_on_wait = own_change_context;
		UT_ASSERT(!own_call(&accepted));
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(retention_attempts, 1);
		UT_ASSERT_EQ(retention_controls | retention_threads, 0);
		own_restore_actor();
		cf_hold_s = own_saved_slot;
		memset(&cf_hold_x, 0, sizeof(cf_hold_x));
		control_requests->records[cf_hold_s.owner.attempt.slot] = own_saved_record;
		cold_cleanup();
	}
}
static void
own_change_on_grant(void)
{
	UT_ASSERT(entry_waits != 0);
	own_corrupt();
}
UT_TEST(cold_own_root_fresh_read_cannot_escape_a_changed_context)
{
	const unsigned faults[] = { 0, 1, 2, 3, 4, 6, 8, 20, 25, 26, 30, 31 };

	for (unsigned i = 0; i < lengthof(faults); i++) {
		bool accepted = false;

		own_fixture();
		own_fault = faults[i];
		claim_after_acquire = own_change_on_grant;
		UT_ASSERT(own_call(&accepted));
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		UT_ASSERT(strstr(own_typed.refusal_detail, "predicate=WAIT_CONTEXT") != NULL);
		UT_ASSERT_EQ(own_root.root.identity.system_identifier | own_root.token.root_publish_seq, 0);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(retention_attempts, 2);
		/* ROOT is read-only; all provisional outputs are discarded after
		 * the yielding acquire. No replay admission or mutation runs. */
		/* Actual CF also rejects changed epoch/generation before ROOT. */
		UT_ASSERT(retention_controls <= 1);
		UT_ASSERT_EQ(retention_threads, retention_controls);
		own_restore_actor();
		cold_cleanup();
	}
}
UT_TEST(cold_own_root_receipt_and_wake_rules_preserve_progress)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		bool accepted = false;

		own_fixture();
		entry_lost_latch = true;
		if (fault == 0)
			entry_on_wait = retention_remove_receipt;
		else if (fault == 1)
			claim_on_reset = cold_ack_during_reset;
		else if (fault == 2) {
			fail_after_publication = false;
			entry_on_wait = retention_empty_grant;
		}
		UT_ASSERT_EQ(own_call(&accepted), fault != 0);
		UT_ASSERT_EQ(accepted, fault != 0);
		UT_ASSERT_EQ(entry_waits, fault == 1 ? 0 : fault == 3 ? 3 : 1);
		if (fault == 0) {
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(retention_controls | retention_threads, 0);
			cf_retired_s = claim_retiring_cookie;
		} else {
			UT_ASSERT_EQ(retention_controls, 1);
			UT_ASSERT_EQ(retention_threads, 1);
		}
		cold_cleanup();
	}
}
UT_TEST(cold_own_root_hard_untyped_and_preexisting_requests_are_not_retried)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		bool accepted = false;
		unsigned before;

		own_fixture();
		if (fault == 0)
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		else if (fault == 1)
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		else if (fault == 2)
			entry_on_wait = entry_hard_error;
		else {
			if (fault == 4)
				g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT_EQ(cluster_cf_lock(fault == 5 ? ExclusiveLock : ShareLock), fault == 4);
		}
		before = g_seven_count;
		UT_ASSERT_EQ(own_call(&accepted), fault != 2);
		UT_ASSERT(!accepted);
		if (fault == 2)
			UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
		else
			UT_ASSERT(strstr(own_typed.refusal_detail, "predicate=LOOKUP_RESULT") != NULL);
		UT_ASSERT_EQ(entry_waits, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(g_seven_count, before + (fault < 3 ? 1 : 0));
		UT_ASSERT_EQ(retention_controls | retention_threads, 0);
		if (fault == 4) {
			claim_teardown = owner_ack_on_wait = true;
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
			claim_teardown = owner_ack_on_wait = false;
		}
		if (fault == 5) {
			cold_consumer = false;
			entry_cleanup(ExclusiveLock, cluster_cf_owner_cookie(ExclusiveLock));
		}
		cold_cleanup();
	}
}
static void
own_real_refusal(void)
{
	own_ready();
	if (own_fault == 0)
		retention_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	else if (own_fault == 1)
		retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	else if (own_fault == 2)
		retention_image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	else if (own_fault == 3)
		retention_image.records[0].root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	else
		retention_image.records[0].identity.reserved42 = 1;
}
UT_TEST(cold_own_root_real_root_refusals_preserve_original_diagnostics)
{
	const char *predicates[]
		= { "LOOKUP_RESULT", "LOOKUP_RESULT", "SEALED_LIFECYCLE", "SEALED_FLAGS", "KEY_FORMAT" };

	for (own_fault = 0; own_fault < lengthof(predicates); own_fault++) {
		bool accepted = false;

		own_fixture();
		entry_on_wait = own_real_refusal;
		UT_ASSERT(own_call(&accepted));
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		UT_ASSERT(strstr(own_typed.refusal_detail, predicates[own_fault]) != NULL);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(retention_attempts, 2);
		UT_ASSERT_EQ(retention_controls, 1);
		UT_ASSERT_EQ(retention_threads, own_fault >= 2);
		cold_cleanup();
	}
}
UT_TEST(cold_own_root_original_cancel_keeps_shared_cleanup_responsibility)
{
	bool accepted = false;
	volatile bool exited = false;

	own_fixture();
	entry_on_wait = entry_shutdown;
	startup_fixture_exit_hook = entry_owner_exit;
	startup_fixture_armed = true;
	if (sigsetjmp(startup_fixture_exit, 1) == 0)
		UT_ASSERT(!own_call(&accepted));
	else
		exited = true;
	UT_ASSERT(exited && !accepted);
	UT_ASSERT_EQ(startup_fixture_exit_code, 1);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads, 0);
	UT_ASSERT(control_requests->records[cf_hold_s.owner.attempt.slot].owner_exited);
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	proc_exit_inprogress = shutdown_requested = false;
	startup_fixture_armed = false;
	startup_fixture_exit_hook = NULL;
	cold_cleanup();
}
UT_TEST(ordinary_cold_and_observational_canonical_keep_their_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_LMON };

	for (unsigned role = 0; role < lengthof(roles); role++) {
		bool accepted = false;

		own_fixture();
		MyBackendType = roles[role];
		UT_ASSERT(own_call(&accepted));
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_controls | retention_threads, 0);
		MyBackendType = B_STARTUP;
		cold_cleanup();
	}
	{
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootReadToken token;

		own_fixture();
		UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, NULL, &snapshot, &token),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(snapshot.identity.system_identifier | token.root_publish_seq, 0);
		cold_cleanup();
	}
	/* Missing pass-1 context cannot authorize a wait, but does not add a
	 * new gate to the original immediate-success path. */
	for (unsigned fault = 0; fault < 2; fault++) {
		bool accepted = false;

		own_fixture();
		own_cohort_available = false;
		if (fault == 0)
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(own_call(&accepted), fault == 0);
		UT_ASSERT_EQ(accepted, fault == 0);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, 1);
		cold_cleanup();
	}
}
