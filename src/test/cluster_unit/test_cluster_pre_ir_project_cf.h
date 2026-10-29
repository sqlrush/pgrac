/* Actual pre-IR merge projection caller and CF/owner/CONTROL.
 * Metadata, fence, clock and delivery remain explicit fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned pre_ir_seals;
static ClusterMergeEngage recovery_fence_project_wait(ClusterRecoveryFencePlan *plan,
													  XLogRecPtr own_redo, uint64 replay[2],
													  XLogRecPtr *start);
static void
pre_ir_publish_leaf(ClusterRecoveryFencePlan *plan, ClusterRecoveryFenceOrigin *origin,
					uint32 required)
{
	UT_ASSERT(plan == &cold_deadline_plan);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++pre_ir_seals;
	origin->root_snapshot = retention_image.records[origin->origin_thread - 1];
	UT_ASSERT_EQ(origin->root_snapshot.root_flags & required, required);
	make_read_token(&retention_image, origin->origin_thread, CONTROL_ROOT_SOURCE_PRIMARY,
					&origin->root_token);
}
static void
pre_ir_members_leaf(ClusterRecoveryFencePlan *plan)
{
	(void)plan;
	abort();
}
static void
pg_attribute_noreturn() pre_ir_seal_refused_leaf(ClusterRecoveryFencePlan *plan, const char *detail)
{
	(void)plan;
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED), errmsg("%s", detail)));
	pg_unreachable();
}
static void
pg_attribute_noreturn() pre_ir_unavailable_leaf(int32 node, uint64 boot, int verdict, int reason,
												const char *stage, const char *predicate)
{
	(void)node;
	(void)boot;
	(void)verdict;
	(void)reason;
	(void)stage;
	(void)predicate;
	abort();
}
#define recovery_fence_origin_seal pre_ir_publish_leaf
#define recovery_fence_plan_release_members pre_ir_members_leaf
#define recovery_fence_seal_refused pre_ir_seal_refused_leaf
#define recovery_fence_unavailable pre_ir_unavailable_leaf
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#include "test_cluster_pre_ir_project_cf.inc"
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef recovery_fence_unavailable
#undef recovery_fence_seal_refused
#undef recovery_fence_plan_release_members
#undef recovery_fence_origin_seal

static unsigned pre_ir_partial_sources;

static void
pre_ir_ready(void)
{
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(cold_deadline_plan.retention_pin == NULL);
	UT_ASSERT_EQ(project_sources, pre_ir_partial_sources);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	if (entry_waits == entry_ack_at) {
		retention_phase = 0;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}

static void
pre_ir_fixture(bool postseal, bool middle)
{
	project_fixture();
	pre_ir_seals = 0;
	project_multiple = true;
	project_census.n_crashed_candidate = 2;
	cluster_recovery_plan_candidate_set(&project_census, 3);
	retention_image.present[2] = true;
	retention_image.records[2] = retention_image.records[1];
	retention_image.records[2].identity.origin_node_id = 2;
	retention_image.records[2].identity.origin_thread_id = 3;
	if (postseal) {
		cold_deadline_plan.origin_count = 2;
		for (unsigned i = 0; i < 2; i++) {
			ClusterRecoveryFenceOrigin *origin = &cold_deadline_plan.origins[i];
			origin->origin_thread = i + 2;
			origin->root_snapshot = retention_image.records[i + 1];
			origin->duty = origin->root_snapshot.identity;
			origin->formation = (ClusterFormationWitnessV1 *)1;
			origin->needs = (PgracExternalFenceNeedSetV1 *)2;
			origin->admissions = (PgracExternalFenceAdmissionSetV1 *)3;
		}
	}
	cold_deadline_active = true;
	pre_ir_partial_sources = middle ? 1 : 0;
	retention_phase = middle ? 2 : 0;
	g_seven_result = middle ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	entry_on_wait = pre_ir_ready;
}

static bool
pre_ir_call(ClusterMergeEngage *engage, uint64 replay[2], XLogRecPtr *start)
{
	volatile bool completed = false;
	PG_TRY();
	{
		*engage = recovery_fence_project_wait(&cold_deadline_plan, 90, replay, start);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_initial_projection_restarts_after_exact_cf_completion)
{
	for (unsigned middle = 0; middle < 2; middle++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		pre_ir_fixture(false, middle != 0);
		UT_ASSERT(pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(project_sources, 2 + middle);
		UT_ASSERT_EQ(replay[0], 7);
		UT_ASSERT_EQ(replay[1], 0);
		UT_ASSERT_EQ(start[1], 90);
		UT_ASSERT_EQ(start[2], 150);
		UT_ASSERT_EQ(start[3], 150);
		UT_ASSERT_EQ(ir_cf_held_count(), 0);
		UT_ASSERT(cluster_control_request_empty());
		ir_cf_cleanup();
	}
}

UT_TEST(startup_postseal_projection_restarts_without_rewriting_sealed_origins)
{
	for (unsigned middle = 0; middle < 2; middle++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		ClusterRecoveryFenceOrigin before[2];
		pre_ir_fixture(true, middle != 0);
		memcpy(before, cold_deadline_plan.origins, sizeof(before));
		UT_ASSERT(pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(project_sources, 2 + middle);
		UT_ASSERT_EQ(memcmp(before, cold_deadline_plan.origins, sizeof(before)), 0);
		UT_ASSERT(!cold_deadline_plan.sealed && !cold_deadline_plan.serial_held);
		UT_ASSERT(cold_deadline_plan.retention_pin == NULL);
		UT_ASSERT_EQ(ir_cf_held_count(), 0);
		UT_ASSERT(cluster_control_request_empty());
		ir_cf_cleanup();
	}
}

static void
pre_ir_change_cohort_after_new_grant(void)
{
	if (entry_waits != 0) {
		++cold_cohort.startup_formation_generation;
		project_after_source = NULL;
	}
}

UT_TEST(startup_projection_reproves_cohort_after_the_new_cf_grant)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	pre_ir_fixture(true, false);
	project_after_source = pre_ir_change_cohort_after_new_grant;
	UT_ASSERT(!pre_ir_call(&engage, replay, start));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT(cluster_control_request_empty());
	ir_cf_cleanup();
}

UT_TEST(startup_projection_zero_and_original_deadline_do_not_sleep_past_the_budget)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		pre_ir_fixture(false, false);
		cold_deadline_plan.merge_wait_timeout_ms_snapshot = fault == 0 ? 0 : 150;
		cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns + (fault == 2 ? 0 : 150 * NS_PER_MS);
		UT_ASSERT(!pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, fault == 1 ? 2 : 0);
		if (fault == 1) {
			UT_ASSERT_EQ(cold_deadline_waits[0], 100);
			UT_ASSERT_EQ(cold_deadline_waits[1], 50);
		}
		UT_ASSERT_EQ(project_sources | retention_controls | retention_threads, 0);
		UT_ASSERT_EQ(cluster_cf_held(ShareLock), fault != 2);
		ir_cf_cleanup();
	}
}

UT_TEST(startup_projection_hard_old_untyped_and_ordinary_cf_failures_never_retry)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		pre_ir_fixture(false, false);
		if (fault == 0) {
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		} else if (fault == 1) {
			(void)entry_timeout(ShareLock);
		} else if (fault == 2) {
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		} else {
			MyBackendType = B_BACKEND;
		}
		UT_ASSERT(!pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits | project_sources | ir_cf_held_count(), 0);
		MyBackendType = B_STARTUP;
		ir_cf_cleanup();
	}
}

static unsigned pre_ir_fault;
static void
pre_ir_change_context(void)
{
	pre_ir_ready();
	if (entry_waits == 3 && pre_ir_fault == 4)
		cold_deadline_plan.retention_pin = (ClusterWalRetentionPin *)1;
	if (entry_waits != 1)
		return;
	switch (pre_ir_fault) {
	case 0:
		cold_deadline_plan.owner_pid++;
		break;
	case 1:
		cold_cohort.startup_formation_generation++;
		break;
	case 2:
		cold_restart.timeline++;
		break;
	case 3:
		failure_admissions = false;
		break;
	case 5:
		cold_deadline_plan.own_thread++;
		break;
	case 6:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	}
}
UT_TEST(startup_projection_original_owner_cohort_restart_and_fence_cannot_change)
{
	for (pre_ir_fault = 0; pre_ir_fault < 7; pre_ir_fault++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		pre_ir_fixture(true, false);
		entry_on_wait = pre_ir_change_context;
		UT_ASSERT(!pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT(entry_waits == 1 || entry_waits == 3);
		UT_ASSERT_EQ(project_sources | ir_cf_held_count(), 0);
		CurrentResourceOwner = cold_deadline_plan.owner;
		cold_deadline_plan.retention_pin = NULL;
		cold_deadline_plan.own_thread = 1;
		failure_admissions = true;
		ir_cf_cleanup();
	}
}

UT_TEST(startup_projection_rejects_pin_ir_or_completed_plan_before_cf)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		pre_ir_fixture(false, false);
		if (fault == 0)
			cold_deadline_plan.sealed = true;
		if (fault == 1)
			cold_deadline_plan.serial_held = true;
		if (fault == 2)
			cold_deadline_plan.serial_guards.count = 1;
		if (fault == 3)
			cold_deadline_plan.retention_pin = (ClusterWalRetentionPin *)1;
		if (fault == 4)
			cold_deadline_plan.committed = true;
		UT_ASSERT(!pre_ir_call(&engage, replay, start));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits | retention_attempts | project_sources, 0);
		UT_ASSERT(cluster_control_request_empty());
		ir_cf_cleanup();
	}
}

UT_TEST(startup_projection_native_source_cf_slice_restarts_the_complete_projection)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { UINT64_MAX, UINT64_MAX };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	pre_ir_fixture(true, false);
	pre_ir_partial_sources = 1;
	project_source_timeout = true;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	start[100] = 1234;
	UT_ASSERT(pre_ir_call(&engage, replay, start));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(project_sources, 3);
	UT_ASSERT_EQ(replay[0], 7);
	UT_ASSERT_EQ(replay[1], 0);
	UT_ASSERT_EQ(start[100], 0);
	UT_ASSERT(cluster_control_request_empty());
	ir_cf_cleanup();
}

UT_TEST(startup_projection_exact_ack_between_polls_has_a_live_continuation)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	pre_ir_fixture(false, false);
	claim_on_reset = cold_ack_during_reset;
	UT_ASSERT(pre_ir_call(&engage, replay, start));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(project_sources, 2);
	UT_ASSERT(cluster_control_request_empty());
	ir_cf_cleanup();
}

UT_TEST(startup_projection_zero_ready_preserves_one_complete_attempt)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	pre_ir_fixture(false, false);
	cold_deadline_plan.merge_wait_timeout_ms_snapshot = 0;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(pre_ir_call(&engage, replay, start));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(project_sources, 2);
	UT_ASSERT(cluster_control_request_empty());
	ir_cf_cleanup();
}

UT_TEST(startup_postseal_retry_does_not_repeat_the_original_seal_calls)
{
	for (unsigned middle = 0; middle < 2; middle++) {
		volatile bool completed = false;
		pre_ir_fixture(true, middle != 0);
		cold_deadline_plan.replay_thread_bitmap[0] = 7;
		cold_deadline_plan.start_lsn[1] = 90;
		for (unsigned i = 0; i < 2; i++) {
			cold_deadline_plan.origins[i].root_snapshot.lifecycle
				= CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
			cold_deadline_plan.origins[i].root_snapshot.root_flags
				&= ~CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
		}
		PG_TRY();
		{
			recovery_fence_plan_seal_inputs(&cold_deadline_plan, 1, 90);
			completed = true;
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
		UT_ASSERT(completed);
		UT_ASSERT_EQ(pre_ir_seals, 2);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(cold_deadline_plan.start_lsn[2], 150);
		UT_ASSERT_EQ(cold_deadline_plan.start_lsn[3], 150);
		UT_ASSERT(cluster_control_request_empty());
		ir_cf_cleanup();
	}
}
