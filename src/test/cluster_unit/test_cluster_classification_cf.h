/* Actual cold preflight, ROOT lookup and CF/owner/CONTROL. Initial projection,
 * physical ROOT I/O and subsequent fencing are explicit leaves. These leaves
 * contain no retry policy. Author: SqlRush <sqlrush@gmail.com> */
int cluster_external_fence_acquire_timeout_ms = 5000;
static ClusterRecoveryFencePlan *classification_plan;
static unsigned classification_fail_at, classification_lookups;
static unsigned classification_nodes[32];
static unsigned classification_formations, classification_needs, classification_admits;
static unsigned classification_seals;
static bool classification_faulted;
static bool classification_untyped;
static unsigned classification_drift;
static void (*classification_on_wait)(void);

static bool
classification_census(ClusterRecoveryPlan *out)
{
	(void)out;
	return false;
}
static bool
classification_legacy(uint16 thread)
{
	(void)thread;
	abort();
}
static ClusterMergeEngage
classification_projection(ClusterRecoveryFencePlan *plan, XLogRecPtr redo, uint64 replay[2],
						  XLogRecPtr *start)
{
	classification_plan = plan;
	UT_ASSERT_EQ(redo, 90);
	replay[0] = 7;
	replay[1] = 0;
	start[1] = 90;
	start[2] = start[3] = 150;
	return CLUSTER_MERGE_ENGAGE;
}
static ClusterControlRootResult
classification_lookup(int32 node, ClusterControlRootIdentity *identity,
					  ClusterControlRootSnapshot *snapshot, ClusterControlRootReadToken *token,
					  uint64 deadline)
{
	UT_ASSERT(classification_lookups < lengthof(classification_nodes));
	classification_nodes[classification_lookups++] = node;
	if (classification_untyped)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!classification_faulted && classification_lookups == classification_fail_at) {
		classification_faulted = true;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	} else
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	return cluster_control_root_lookup_owner_deadline_v1(node, identity, snapshot, token, deadline);
}
static void
classification_release(ClusterRecoveryFencePlan *plan)
{
	for (unsigned i = 0; i < plan->origin_count; ++i) {
		plan->origins[i].formation = NULL;
		plan->origins[i].needs = NULL;
		plan->origins[i].admissions = NULL;
	}
}
static void
pg_attribute_noreturn() classification_unavailable(int32 node, uint64 boot, int verdict, int reason,
												   const char *stage, const char *predicate)
{
	(void)node;
	(void)boot;
	(void)verdict;
	(void)reason;
	(void)predicate;
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE), errmsg("%s", stage)));
	pg_unreachable();
}
static ClusterFormationWitnessResult
classification_formation(uint16 thread, int budget, ClusterFormationWitnessV1 **out)
{
	UT_ASSERT(thread == 2 || thread == 3);
	UT_ASSERT(budget > 0 && budget <= 5000);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++classification_formations;
	*out = (ClusterFormationWitnessV1 *)1;
	return CLUSTER_FORMATION_WITNESS_READY;
}
static ClusterFormationWitnessResult
classification_legacy_formation(uint16 thread, bool cold, int budget,
								ClusterFormationWitnessV1 **out)
{
	(void)cold;
	return classification_formation(thread, budget, out);
}
static PgracExternalFenceNeedSetResult
classification_need_set(const ClusterRecoveryDutyKey *duty,
						const ClusterFormationWitnessV1 *formation,
						PgracExternalFenceNeedSetV1 **out)
{
	UT_ASSERT(duty->origin_thread_id == 2 || duty->origin_thread_id == 3);
	UT_ASSERT(formation == (ClusterFormationWitnessV1 *)1);
	++classification_needs;
	*out = (PgracExternalFenceNeedSetV1 *)2;
	return PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
}
static PgracExternalFenceVerdict
classification_admit(const PgracExternalFenceNeedSetV1 *needs,
					 const ClusterFormationWitnessV1 *formation, int budget,
					 PgracExternalFenceAdmissionSetV1 **out)
{
	UT_ASSERT(needs == (PgracExternalFenceNeedSetV1 *)2);
	UT_ASSERT(formation == (ClusterFormationWitnessV1 *)1);
	UT_ASSERT(budget > 0 && budget <= 5000);
	++classification_admits;
	*out = (PgracExternalFenceAdmissionSetV1 *)3;
	return PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED;
}
static const char *
classification_predicate(void)
{
	return NULL;
}
static PgracExternalFenceDenyReason
classification_reason(void)
{
	return PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
}
static void
classification_seal(ClusterRecoveryFencePlan *plan, uint16 thread, XLogRecPtr redo)
{
	UT_ASSERT(plan == classification_plan && thread == 1 && redo == 90);
	UT_ASSERT_EQ(classification_formations, plan->origin_count);
	UT_ASSERT_EQ(classification_needs, plan->origin_count);
	UT_ASSERT_EQ(classification_admits, plan->origin_count);
	++classification_seals;
}

#define cluster_recovery_plan_snapshot classification_census
#define recovery_merge_cold_merge_needed classification_legacy
#define recovery_fence_project_wait classification_projection
#define cluster_control_root_lookup_owner_deadline_v1 classification_lookup
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define recovery_fence_plan_release_members classification_release
#define recovery_fence_unavailable classification_unavailable
#define recovery_fence_seal_refused pre_ir_seal_refused_leaf
#define cluster_formation_witness_build_recovery_control_wait classification_formation
#define cluster_formation_witness_build_wait classification_legacy_formation
#define cluster_external_fence_need_set_build_cold classification_need_set
#define cluster_external_fence_need_set_build classification_need_set
#define cluster_external_fence_need_set_last_predicate classification_predicate
#define cluster_external_fence_admit_set_wait classification_admit
#define cluster_external_fence_last_deny_reason classification_reason
#define recovery_fence_plan_seal_inputs classification_seal
#define clock_gettime cf_budget_clock_gettime
#include "test_cluster_classification_cf.inc"
#undef clock_gettime
#undef recovery_fence_plan_seal_inputs
#undef cluster_external_fence_last_deny_reason
#undef cluster_external_fence_admit_set_wait
#undef cluster_external_fence_need_set_last_predicate
#undef cluster_external_fence_need_set_build
#undef cluster_external_fence_need_set_build_cold
#undef cluster_formation_witness_build_wait
#undef cluster_formation_witness_build_recovery_control_wait
#undef recovery_fence_seal_refused
#undef recovery_fence_unavailable
#undef recovery_fence_plan_release_members
#undef cluster_recovery_duty_key_compare_for_claim
#undef cluster_control_root_lookup_owner_deadline_v1
#undef recovery_fence_project_wait
#undef recovery_merge_cold_merge_needed
#undef cluster_recovery_plan_snapshot

static void
classification_ready(void)
{
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(classification_plan->retention_pin == NULL);
	UT_ASSERT_EQ(classification_plan->origin_count, 0);
	UT_ASSERT_EQ(classification_formations | classification_needs | classification_admits, 0);
	UT_ASSERT_EQ(classification_plan->replay_thread_bitmap[0], 7);
	UT_ASSERT_EQ(classification_plan->foreign_origin_bitmap[0], 6);
	UT_ASSERT_EQ(classification_plan->start_lsn[2], 150);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
	if (classification_on_wait != NULL)
		classification_on_wait();
}
static void
classification_fixture(unsigned fail_at)
{
	pre_ir_fixture(false, false);
	retention_phase = 0;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	for (unsigned i = 0; i < 3; ++i) {
		retention_image.records[i].identity.authority_uuid[6] = 0x40;
		retention_image.records[i].identity.authority_uuid[8] = 0x80;
	}
	classification_plan = NULL;
	classification_faulted = false;
	classification_untyped = false;
	classification_fail_at = fail_at;
	classification_lookups = classification_formations = classification_needs = 0;
	classification_admits = classification_seals = 0;
	classification_on_wait = NULL;
	cluster_recovery_merge_wait_timeout = 10000;
	memset(classification_nodes, 0, sizeof(classification_nodes));
	entry_on_wait = classification_ready;
}
static bool
classification_call(ClusterMergeEngage *engage, ClusterRecoveryFencePlan **out)
{
	volatile bool completed = false;
	PG_TRY();
	{
		*engage = cluster_recovery_merge_preflight_readonly(1, 90, out);
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
classification_cleanup(ClusterRecoveryFencePlan *out)
{
	if (out != NULL) {
		classification_release(out);
		pfree(out);
	}
	classification_plan = NULL;
	cluster_recovery_merge_wait_timeout = 10000;
	cold_cleanup();
}

UT_TEST(startup_root_classification_restarts_before_any_fence_admission)
{
	for (unsigned fail_at = 1; fail_at <= 2; ++fail_at) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		ClusterRecoveryFencePlan *out = NULL;
		classification_fixture(fail_at);
		UT_ASSERT(classification_call(&engage, &out));
		UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
		UT_ASSERT(out != NULL);
		if (out != NULL) {
			UT_ASSERT_EQ(out->origin_count, 2);
			UT_ASSERT_EQ(out->origins[0].origin_thread, 2);
			UT_ASSERT_EQ(out->origins[1].origin_thread, 3);
			UT_ASSERT(out->sealed);
		}
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(classification_lookups, fail_at + 2);
		UT_ASSERT_EQ(classification_nodes[fail_at], 1);
		UT_ASSERT_EQ(classification_formations, 2);
		UT_ASSERT_EQ(classification_needs, 2);
		UT_ASSERT_EQ(classification_admits, 2);
		UT_ASSERT_EQ(classification_seals, 1);
		UT_ASSERT(cluster_control_request_empty());
		classification_cleanup(out);
	}
}

static void
classification_change_root(void)
{
	if (entry_waits != entry_ack_at)
		return;
	if (classification_drift == 0)
		++retention_image.records[1].identity.origin_owner_incarnation;
	else if (classification_drift == 1)
		++retention_image.records[1].checkpoint_lower_lsn;
	else
		++retention_image.record_crc32c[1];
}
UT_TEST(startup_root_classification_rejects_changed_prefix_without_adopting_it)
{
	for (classification_drift = 0; classification_drift < 3; ++classification_drift) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		ClusterRecoveryFencePlan *out = NULL;
		classification_fixture(2);
		classification_on_wait = classification_change_root;
		UT_ASSERT(!classification_call(&engage, &out));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(classification_lookups, 3);
		UT_ASSERT_EQ(classification_nodes[2], 1);
		UT_ASSERT_EQ(classification_formations | classification_admits | classification_seals, 0);
		classification_cleanup(out);
	}
}

UT_TEST(startup_root_classification_excludes_completed_origins_only_after_the_full_pass)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	ClusterRecoveryFencePlan *out = NULL;
	classification_fixture(2);
	retention_image.records[1].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT(classification_call(&engage, &out));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(out != NULL);
	if (out != NULL) {
		UT_ASSERT_EQ(out->origin_count, 1);
		UT_ASSERT_EQ(out->origins[0].origin_thread, 3);
		UT_ASSERT_EQ(out->replay_thread_bitmap[0], 5);
		UT_ASSERT_EQ(out->foreign_origin_bitmap[0], 4);
		UT_ASSERT_EQ(out->start_lsn[2], InvalidXLogRecPtr);
	}
	UT_ASSERT_EQ(classification_lookups, 4);
	UT_ASSERT_EQ(classification_nodes[2], 1);
	UT_ASSERT_EQ(classification_formations, 1);
	UT_ASSERT_EQ(classification_admits, 1);
	classification_cleanup(out);
	out = NULL;

	classification_fixture(0);
	retention_image.records[1].lifecycle = retention_image.records[2].lifecycle
		= CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT(classification_call(&engage, &out));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_NO_NO_CANDIDATES);
	UT_ASSERT(out == NULL);
	UT_ASSERT_EQ(entry_waits | classification_admits | classification_seals, 0);
	classification_cleanup(out);
}

static void
classification_change_guc(void)
{
	cluster_recovery_merge_wait_timeout = 600000;
}
UT_TEST(startup_root_classification_keeps_the_original_deadline_and_zero_never_sleeps)
{
	for (unsigned zero = 0; zero < 2; ++zero) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		ClusterRecoveryFencePlan *out = NULL;
		classification_fixture(1);
		cluster_recovery_merge_wait_timeout = zero ? 0 : 137;
		entry_ack_at = 999;
		classification_on_wait = classification_change_guc;
		UT_ASSERT(!classification_call(&engage, &out));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(classification_lookups, 1);
		UT_ASSERT_EQ(classification_admits | classification_seals, 0);
		UT_ASSERT_EQ(entry_waits, zero ? 0 : 2);
		if (!zero) {
			UT_ASSERT_EQ(cold_deadline_waits[0], 100);
			UT_ASSERT_EQ(cold_deadline_waits[1], 37);
			UT_ASSERT_EQ(set_cf_clock_ns, NS_PER_S + 137 * NS_PER_MS);
		}
		UT_ASSERT(cluster_cf_held(ShareLock));
		classification_cleanup(out);
	}
}

UT_TEST(startup_root_classification_zero_ready_finishes_one_complete_attempt)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	ClusterRecoveryFencePlan *out = NULL;
	classification_fixture(0);
	cluster_recovery_merge_wait_timeout = 0;
	UT_ASSERT(classification_call(&engage, &out));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(classification_lookups, 2);
	UT_ASSERT_EQ(classification_admits, 2);
	classification_cleanup(out);
}

UT_TEST(startup_root_classification_never_retries_hard_or_untyped_failure)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		ClusterRecoveryFencePlan *out = NULL;
		classification_fixture(0);
		if (fault == 0)
			retention_root_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		else if (fault == 1)
			classification_untyped = true;
		else {
			retention_image.records[2].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
			retention_image.records[2].root_flags = 0;
		}
		UT_ASSERT(!classification_call(&engage, &out));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE);
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(classification_lookups, fault == 2 ? 2 : 1);
		UT_ASSERT_EQ(entry_waits | classification_admits | classification_seals, 0);
		classification_cleanup(out);
	}
}

static void
classification_change_context(void)
{
	if (classification_drift == 0)
		++cold_cohort.startup_formation_generation;
	else if (classification_drift == 1)
		++cold_restart.timeline;
	else
		InRecovery = !InRecovery;
}
static void
classification_change_after_grant(void)
{
	if (entry_waits >= entry_ack_at)
		classification_change_context();
}
UT_TEST(startup_root_classification_reproves_context_before_and_after_reapplication)
{
	for (unsigned after = 0; after < 2; ++after) {
		for (classification_drift = 0; classification_drift < 3; ++classification_drift) {
			ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
			ClusterRecoveryFencePlan *out = NULL;
			classification_fixture(1);
			if (after)
				claim_after_acquire = classification_change_after_grant;
			else
				classification_on_wait = classification_change_context;
			UT_ASSERT(!classification_call(&engage, &out));
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT(out == NULL);
			UT_ASSERT_EQ(classification_lookups, after ? 2 : 1);
			UT_ASSERT_EQ(classification_admits | classification_seals, 0);
			claim_after_acquire = NULL;
			classification_cleanup(out);
		}
	}
}

UT_TEST(startup_root_classification_does_not_adopt_preexisting_cf_debt)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	ClusterRecoveryFencePlan *out = NULL;
	classification_fixture(1);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT(!claim_actual_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(!classification_call(&engage, &out));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(out == NULL);
	UT_ASSERT_EQ(entry_waits | classification_admits | classification_seals, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	classification_cleanup(out);
}

static void
classification_io_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("classification ROOT read failed")));
}
UT_TEST(startup_root_classification_preserves_error_cleanup_and_original_sqlstate)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	ClusterRecoveryFencePlan *out = NULL;
	classification_fixture(0);
	retention_after_read = classification_io_error;
	UT_ASSERT(!classification_call(&engage, &out));
	UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
	UT_ASSERT(out == NULL);
	UT_ASSERT_EQ(classification_lookups, 1);
	UT_ASSERT_EQ(entry_waits | classification_admits | classification_seals, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	retention_after_read = NULL;
	classification_cleanup(out);
}

static void
classification_ack_at_deadline(void)
{
	if (entry_waits == entry_ack_at)
		set_cf_clock_ns = classification_plan->merge_deadline_ns;
}
UT_TEST(startup_root_classification_exact_ack_cannot_renew_an_expired_plan)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	ClusterRecoveryFencePlan *out = NULL;
	classification_fixture(1);
	classification_on_wait = classification_ack_at_deadline;
	UT_ASSERT(!classification_call(&engage, &out));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(out == NULL);
	UT_ASSERT_EQ(classification_lookups, 1);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(classification_admits | classification_seals, 0);
	classification_cleanup(out);
}
