/* Actual merge consumer; confirmation is an explicit original-owner leaf.
 * The real registry writer/receipt producer is covered by its owner suite. */
typedef struct MergeConfirmFixture {
	const ClusterRecoveryFencePlan *plan;
	const struct ClusterColdTypedV1 *typed;
	uint64 episode;
	ClusterControlRootReadToken before[NODES + 1], after[NODES + 1];
	bool primary_read_back;
	bool complete;
	bool valid;
} MergeConfirmFixture;

static char confirm_typed_owner;
static MergeConfirmFixture confirm_fixture, next_confirm_fixture;

static const struct ClusterControlRelmapRecoveryCreateV2 *
merge_confirm_fixture(ClusterRecoveryFencePlan *plan, uint64 episode)
{
	memset(&confirm_fixture, 0, sizeof(confirm_fixture));
	confirm_fixture.plan = plan;
	confirm_fixture.typed = (const void *)&confirm_typed_owner;
	confirm_fixture.episode = episode;
	confirm_fixture.valid = true;
	for (uint16 tid = 1; tid <= NODES; tid++) {
		confirm_fixture.before[tid] = tokens[tid];
		confirm_fixture.after[tid] = tokens[tid];
		/* The owner fixture supplies an exact value, never a permissive
		 * sequence range or a token synthesized by the merge consumer. */
		confirm_fixture.after[tid].file_txn_seq = UINT64_C(777);
	}
	return (const void *)&confirm_fixture;
}

#ifdef TEST_CLUSTER_CONFIRM_OWNER_BRIDGE
#define cluster_control_relmap_recovery_create_root_delta_v2 merge_fixture_root_delta_v2
#define cluster_control_relmap_recovery_create_confirmed_root_delta_v2 merge_fixture_confirmed_delta_v2
bool merge_fixture_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation,
	const struct ClusterColdTypedV1 *typed, const ClusterRecoveryFencePlan *plan,
	uint64 episode, const ClusterControlRootReadToken *before,
	ClusterControlRootReadToken *effective);
bool merge_fixture_confirmed_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation,
	const struct ClusterColdTypedV1 *typed, const ClusterRecoveryFencePlan *plan,
	uint64 episode, const ClusterControlRootReadToken *before,
	ClusterControlRootReadToken *effective);
#endif
bool
cluster_control_relmap_recovery_create_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation,
	const struct ClusterColdTypedV1 *typed, const ClusterRecoveryFencePlan *plan,
	uint64 episode, const ClusterControlRootReadToken *before,
	ClusterControlRootReadToken *effective)
{
	uint16 tid;
	const MergeConfirmFixture *fixture;

	fixture = confirmation == (const void *)&confirm_fixture ? &confirm_fixture
		: confirmation == (const void *)&next_confirm_fixture ? &next_confirm_fixture : NULL;
	if (fixture == NULL || !fixture->valid
		|| typed != fixture->typed || plan != fixture->plan
		|| episode != fixture->episode || before == NULL || effective == NULL
		|| (tid = before->origin_thread_id) == 0 || tid > NODES
		|| memcmp(before, &fixture->before[tid], sizeof(*before)))
		return false;
	*effective = fixture->primary_read_back ? fixture->after[tid] : fixture->before[tid];
	return true;
}

bool
cluster_control_relmap_recovery_create_confirmed_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation,
	const struct ClusterColdTypedV1 *typed, const ClusterRecoveryFencePlan *plan, uint64 episode,
	const ClusterControlRootReadToken *before, ClusterControlRootReadToken *effective)
{
	const MergeConfirmFixture *fixture
		= confirmation == (const void *)&confirm_fixture ? &confirm_fixture
		: confirmation == (const void *)&next_confirm_fixture ? &next_confirm_fixture : NULL;
	return fixture != NULL && fixture->primary_read_back && fixture->complete
		&& cluster_control_relmap_recovery_create_root_delta_v2(
			confirmation, typed, plan, episode, before, effective);
}
#ifdef TEST_CLUSTER_CONFIRM_OWNER_BRIDGE
#undef cluster_control_relmap_recovery_create_root_delta_v2
#undef cluster_control_relmap_recovery_create_confirmed_root_delta_v2
#endif

UT_TEST(test_confirm_current_follows_exact_writer_phase_without_changing_baseline)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation;
	ClusterRecoveryFenceOrigin origins[NODES - 1];

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	confirmation = merge_confirm_fixture(plan, episode);
	memcpy(origins, plan->origins, sizeof(origins));
	suspended_cf_exclusive = true;
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
		plan, episode, confirm_fixture.typed, confirmation));
	confirm_fixture.primary_read_back = true;
	for (uint16 tid = 1; tid <= NODES; tid++)
		tokens[tid] = confirm_fixture.after[tid];
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
		plan, episode, confirm_fixture.typed, confirmation));
	UT_ASSERT_EQ(memcmp(origins, plan->origins, sizeof(origins)), 0);
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count == 0);
	suspended_cf_exclusive = false;
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_confirm_delta_carries_exact_tokens_to_resume_but_origin_stays_original)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	ClusterWalRetentionPin *pin = plan->retention_pin;
	unsigned allocations = confirm_context_allocations;
	uint64 episode = 0;
	const struct ClusterControlRelmapRecoveryCreateV2 *confirmation;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	uint16 tid;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	confirmation = merge_confirm_fixture(plan, episode);
	suspended_cf_exclusive = true;
	confirm_fixture.primary_read_back = true;
	confirm_fixture.complete = true;
	for (uint16 i = 1; i <= NODES; i++)
		tokens[i] = confirm_fixture.after[i];
	UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
		plan, episode, confirm_fixture.typed, confirmation));
	UT_ASSERT(cluster_recovery_merge_fence_plan_origin(plan, 0, &tid, &root, &token));
	UT_ASSERT_EQ(memcmp(&token, &confirm_fixture.before[tid], sizeof(token)), 0);
	UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
		plan, episode, confirm_fixture.typed, confirmation));
	UT_ASSERT_EQ(confirm_context_allocations, allocations + 1);
	suspended_cf_exclusive = false;
	UT_ASSERT(cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(plan->retention_pin == pin);
	for (uint16 i = 0; i < plan->origin_count; i++) {
		tid = plan->origins[i].origin_thread;
		UT_ASSERT_EQ(memcmp(&plan->serial_guards.guards[i].root_read_token,
			&confirm_fixture.after[tid], sizeof(token)), 0);
		{
			ClusterThreadRecoveryAuthorityV1 authority;
			UT_ASSERT(cluster_recovery_merge_fence_plan_authority(plan, tid, &authority));
			UT_ASSERT_EQ(memcmp(authority.root_token, &confirm_fixture.after[tid], sizeof(token)), 0);
			UT_ASSERT(authority.root_snapshot == &plan->origins[i].root_snapshot);
			UT_ASSERT(authority.retention_pin == pin);
		}
	}
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_confirm_delta_requires_original_final_proof_and_rejects_other_token_changes)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		uint64 episode = 0;
		const struct ClusterControlRelmapRecoveryCreateV2 *confirmation;
		const struct ClusterColdTypedV1 *typed;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		confirmation = merge_confirm_fixture(plan, episode);
		typed = confirm_fixture.typed;
		suspended_cf_exclusive = true;
		confirm_fixture.primary_read_back = true;
		confirm_fixture.complete = true;
		if (fault == 0) confirm_fixture.complete = false;
		if (fault == 1) confirm_fixture.primary_read_back = false;
		if (fault == 2) confirm_fixture.valid = false;
		if (fault == 3) typed = NULL;
		if (fault == 4) confirm_fixture.before[2].file_txn_seq++;
		if (fault == 5) confirm_fixture.after[2].root_publish_seq++;
		if (fault == 6) confirm_fixture.after[2].record_crc32c++;
		if (fault == 7) deadline_clock_ns += INT64CONST(25000000);
		if (fault == 8) suspended_cf_exclusive = false;
		confirm_pin_drops_cf = fault == 9;
		for (uint16 tid = 1; tid <= NODES; tid++)
			tokens[tid] = confirm_fixture.after[tid];
		UT_ASSERT(!cluster_recovery_merge_fence_plan_confirm_delta_v1(
			plan, episode, typed, confirmation));
		confirm_pin_drops_cf = false;
		confirm_fixture.complete = true;
		confirm_fixture.valid = true;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_held_current_v1(plan, episode));
		suspended_cf_exclusive = false;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}

UT_TEST(test_prior_confirm_recheck_uses_chain_tip_without_replaying_or_rebasing)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;
	const struct ClusterControlRelmapRecoveryCreateV2 *first, *second;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	uint16 tid;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	first = merge_confirm_fixture(plan, episode);
	suspended_cf_exclusive = true;
	confirm_fixture.primary_read_back = confirm_fixture.complete = true;
	for (uint16 i = 1; i <= NODES; i++)
		tokens[i] = confirm_fixture.after[i];
	UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
		plan, episode, confirm_fixture.typed, first));
	next_confirm_fixture = confirm_fixture;
	for (uint16 i = 1; i <= NODES; i++) {
		next_confirm_fixture.before[i] = tokens[i];
		next_confirm_fixture.after[i] = tokens[i];
		next_confirm_fixture.after[i].file_txn_seq = UINT64_C(999);
		tokens[i] = next_confirm_fixture.after[i];
	}
	second = (const void *)&next_confirm_fixture;
	UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
		plan, episode, confirm_fixture.typed, second));
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
		plan, episode, confirm_fixture.typed, first));
	UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
		plan, episode, confirm_fixture.typed, first));
	UT_ASSERT(cluster_recovery_merge_fence_plan_origin(plan, 0, &tid, &root, &token));
	UT_ASSERT_EQ(memcmp(&token, &confirm_fixture.before[tid], sizeof(token)), 0);
	suspended_cf_exclusive = false;
	UT_ASSERT(cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	for (uint16 i = 0; i < plan->origin_count; i++)
		UT_ASSERT_EQ(plan->serial_guards.guards[i].root_read_token.file_txn_seq, UINT64_C(999));
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_confirmation_chain_never_changes_typed_owner_or_uses_revoked_receipts)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		uint64 episode = 0;
		const struct ClusterControlRelmapRecoveryCreateV2 *first, *candidate;
		const struct ClusterColdTypedV1 *typed;
		char other_typed;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		first = merge_confirm_fixture(plan, episode);
		suspended_cf_exclusive = true;
		confirm_fixture.primary_read_back = confirm_fixture.complete = true;
		for (uint16 i = 1; i <= NODES; i++)
			tokens[i] = confirm_fixture.after[i];
		UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
			plan, episode, confirm_fixture.typed, first));
		next_confirm_fixture = confirm_fixture;
		for (uint16 i = 1; i <= NODES; i++) {
			next_confirm_fixture.before[i] = tokens[i];
			next_confirm_fixture.after[i] = tokens[i];
			next_confirm_fixture.after[i].file_txn_seq = 999;
			tokens[i] = next_confirm_fixture.after[i];
		}
		candidate = (const void *)&next_confirm_fixture;
		if (fault == 0) confirm_fixture.complete = false;
		if (fault == 1) next_confirm_fixture.typed = (const void *)&other_typed;
		if (fault == 2) next_confirm_fixture.before[2].file_txn_seq++;
		typed = next_confirm_fixture.typed;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
			plan, episode, typed, candidate));
		UT_ASSERT(!cluster_recovery_merge_fence_plan_confirm_delta_v1(
			plan, episode, typed, candidate));
		suspended_cf_exclusive = false;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(plan->serial_guards.count == 0 && plan->retention_pin != NULL);
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}

UT_TEST(test_publishing_phase_is_not_final_install_or_resume_authority)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;
	const struct ClusterControlRelmapRecoveryCreateV2 *c;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	c = merge_confirm_fixture(plan, episode);
	suspended_cf_exclusive = true;
	confirm_fixture.primary_read_back = true; /* complete is still false */
	for (uint16 i = 1; i <= NODES; i++)
		tokens[i] = confirm_fixture.after[i];
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
		plan, episode, confirm_fixture.typed, c));
	UT_ASSERT(plan->confirmed_deltas == NULL && plan->serial_guards.count == 0);
	suspended_cf_exclusive = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(plan->serial_guards.count == 0 && plan->retention_pin != NULL);
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_confirmation_read_expiry_and_later_receipt_failure_prevent_resume)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		uint64 episode = 0;
		const struct ClusterControlRelmapRecoveryCreateV2 *c;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		c = merge_confirm_fixture(plan, episode);
		suspended_cf_exclusive = true;
		confirm_fixture.primary_read_back = confirm_fixture.complete = true;
		for (uint16 i = 1; i <= NODES; i++)
			tokens[i] = confirm_fixture.after[i];
		if (fault < 2) {
			deadline_source_hook = suspended_late_ir_grant;
			UT_ASSERT(!(fault == 0
				? cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(
					plan, episode, confirm_fixture.typed, c)
				: cluster_recovery_merge_fence_plan_confirm_delta_v1(
					plan, episode, confirm_fixture.typed, c)));
			deadline_source_hook = NULL;
		} else {
			UT_ASSERT(cluster_recovery_merge_fence_plan_confirm_delta_v1(
				plan, episode, confirm_fixture.typed, c));
			confirm_fixture.valid = false;
		}
		suspended_cf_exclusive = false;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(plan->serial_guards.count == 0 && plan->retention_pin != NULL);
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}
