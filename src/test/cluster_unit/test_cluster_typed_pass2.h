/* Actual committed IR/CF/CONTROL producers and original typed matcher.
 * Prepared graph, physical profiles, transport and ACK delivery are leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterColdParticipantV1 pass2_plan_participants[3];
static bool pass2_profiles = true;
static bool
cold_native_profiles_match(const ClusterColdTypedV1 *typed)
{
	return pass2_profiles;
}
static bool
cold_history_side_effects_match(const ClusterColdTypedV1 *typed, uint32 participant)
{
	UT_ASSERT(false); /* This exact-IR component has no history. */
	return false;
}
uint32
cluster_cold_plan_participant_count_v1(const ClusterColdPlanV1 *plan)
{
	UT_ASSERT(plan == own_typed.plan);
	return 3;
}
bool
cluster_cold_plan_participant_matches_v1(const ClusterColdPlanV1 *plan, uint32 index,
										 const ClusterColdParticipantV1 *participant)
{
	return plan == own_typed.plan && index < 3
		   && memcmp(participant, &pass2_plan_participants[index], sizeof(*participant)) == 0;
}
void
cluster_cold_tt_after_redo_reset_v1(ClusterColdTypedV1 *typed)
{
	(void)typed;
}
#include "test_cluster_typed_pass2.inc"

static ColdRoot pass2_roots[3];
static unsigned probe_ir_waits;
static void
pass2_forbidden_wait(void)
{
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	++probe_ir_waits;
	ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("forbidden IR/CF wait")));
}
UT_TEST(committed_ir_source_check_never_enters_cf)
{
	volatile bool matched = false;
	commit_cf_fixture();
	UT_ASSERT(cluster_recovery_merge_commit_plan_nowait(&cold_deadline_plan));
	UT_ASSERT(cold_deadline_plan.committed);
	UT_ASSERT(cluster_recovery_merge_fence_plan_revalidate_nowait(&cold_deadline_plan));
	memset(&own_typed, 0, sizeof(own_typed));
	memset(pass2_roots, 0, sizeof(pass2_roots));
	own_typed.plan = (ClusterColdPlanV1 *)&input_scope;
	own_typed.replay_roots = pass2_roots;
	own_typed.replay_count = own_typed.replay_root_count = own_typed.participant_count = 3;
	own_typed.census_root_captured = true;
	pass2_profiles = true;
	for (unsigned i = 0; i < 3; ++i) {
		ColdRoot *selected = &pass2_roots[i];
		ClusterColdParticipantV1 *participant = &own_typed.participants[i];
		if (i != 0) {
			selected->root = cold_deadline_plan.origins[i - 1].root_snapshot;
			selected->token = cold_deadline_plan.origins[i - 1].root_token;
		} else {
			selected->root = retention_image.records[0];
		}
		selected->source.claim.identity = selected->root.identity;
		selected->source.timeline = selected->root.checkpoint_tli;
		own_typed.sources[i] = selected->source;
		participant->thread_id = selected->root.identity.origin_thread_id;
		participant->owner_incarnation = selected->root.identity.origin_owner_incarnation;
		participant->timeline = selected->root.checkpoint_tli;
		participant->physical_lower = selected->root.checkpoint_lower_lsn;
		participant->tail_end = selected->root.validated_tail_lsn_exclusive;
		participant->native_redo = selected->native_checkpoint.redo = i ? 150 : 90;
		selected->checkpoint_captured = true;
		pass2_plan_participants[i] = *participant;
	}
	recovery_phase = 1;
	recovery_attempts = recovery_reads = recovery_wal_reads = 0;
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	entry_waits = probe_ir_waits = 0;
	entry_on_wait = pass2_forbidden_wait;
	PG_TRY();
	{
		matched = cold_replay_sources_current(&own_typed, &cold_deadline_plan);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	UT_ASSERT(matched);
	UT_ASSERT_EQ(
		probe_ir_waits | entry_waits | recovery_attempts | recovery_reads | recovery_wal_reads, 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	own_typed.sources[2].claim.claim_sha256[31]++;
	UT_ASSERT(!cold_replay_sources_current(&own_typed, &cold_deadline_plan));
	UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(entry_waits | recovery_attempts | recovery_reads, 0);
	commit_cf_cleanup();
}

UT_TEST(post_ir_checkpoint_refresh_uses_original_cf_retirement)
{
	ClusterWalRecoveryCheckpointV1 out;
	ClusterControlRootFileToken old;
	ClusterControlRootResult result;
	input_fixture(1);
	memset(&input_expected, 0, sizeof(input_expected));
	input_expected.identity.system_identifier = 1234;
	input_expected.identity.storage_uuid[0] = 31;
	input_expected.identity.origin_thread_id = 1;
	input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	input_expected_original = input_expected;
	input_claim_reads = input_selections = 0;
	old = input_token;
	old.file_txn_seq--;
	cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns - 1;
	result = cluster_wal_inputs_cold_checkpoint_readback_v1(&input_expected, &old, &out);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(input_reads, 2);
	UT_ASSERT_EQ(input_selections, 2);
	UT_ASSERT_EQ(input_claim_reads, 1);
	UT_ASSERT_EQ(out.checkpoint_start, 100);
	cold_cleanup();
}

static unsigned pass2_wait_fault;
static void
pass2_change_wait_context(void)
{
	if (pass2_wait_fault == 0)
		owner_epoch++;
	else if (pass2_wait_fault == 1)
		CurrentResourceOwner = (ResourceOwner)2;
	else
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("original Startup cancel")));
}
UT_TEST(post_ir_readback_wait_has_original_terminators)
{
	for (pass2_wait_fault = 0; pass2_wait_fault < 3; ++pass2_wait_fault) {
		ClusterWalRecoveryCheckpointV1 out;
		ResourceOwner original_owner;
		uint64 original_epoch;
		volatile bool caught = false;
		input_fixture(1);
		memset(&input_expected, 0, sizeof(input_expected));
		input_expected.identity.system_identifier = 1234;
		input_expected.identity.storage_uuid[0] = 31;
		input_expected.identity.origin_thread_id = 1;
		input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		input_expected_original = input_expected;
		input_claim_reads = input_selections = 0;
		original_owner = CurrentResourceOwner;
		original_epoch = owner_epoch;
		entry_on_wait = pass2_change_wait_context;
		PG_TRY();
		{
			(void)cluster_wal_inputs_cold_checkpoint_readback_v1(&input_expected, &input_token,
																 &out);
		}
		PG_CATCH();
		{
			caught = true;
			FlushErrorState();
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(entry_code, pass2_wait_fault == 2 ? ERRCODE_QUERY_CANCELED
													   : ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(input_reads | input_selections | input_claim_reads, 0);
		UT_ASSERT_EQ(out.checkpoint_start, 0);
		owner_epoch = original_epoch;
		CurrentResourceOwner = original_owner;
		cold_cleanup();
	}
}
static unsigned pass2_stop_pending;
static void
pass2_observe_stop_then_ack(void)
{
	const char *reason;
	BackendType original_role = MyBackendType;
	bool original_under_postmaster = IsUnderPostmaster;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_PENDING);
	MyBackendType = original_role;
	IsUnderPostmaster = original_under_postmaster;
	++pass2_stop_pending;
	input_ready_after_ack();
}
UT_TEST(post_ir_readback_stop_is_pending_until_original_ack)
{
	ClusterWalRecoveryCheckpointV1 out;
	const char *reason;
	input_fixture(1);
	memset(&input_expected, 0, sizeof(input_expected));
	input_expected.identity.system_identifier = 1234;
	input_expected.identity.storage_uuid[0] = 31;
	input_expected.identity.origin_thread_id = 1;
	input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	input_expected_original = input_expected;
	input_claim_reads = input_selections = pass2_stop_pending = 0;
	entry_on_wait = pass2_observe_stop_then_ack;
	UT_ASSERT_EQ(
		cluster_wal_inputs_cold_checkpoint_readback_v1(&input_expected, &input_token, &out),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(pass2_stop_pending, 3);
	MyBackendType = B_LMON;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	MyBackendType = B_STARTUP;
	IsUnderPostmaster = false;
	cold_cleanup();
}

/* The selector body is actual; original origin I/O/native-anchor parsing are
 * leaves. This checks the refreshed source tuple independently of CF waits. */
static ClusterWalOriginInputs pass2_origin;
static ClusterControlRootResult
pass2_origin_read(const ControlRootImage *root, uint32 node, ClusterWalOriginInputs *out)
{
	*out = pass2_origin;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static ClusterControlRootResult
pass2_native_checkpoint(ClusterWalInputsV1 *inputs, const WalInputsWork *work,
						const ClusterWalHistoryRecord *record, bool current)
{
	inputs->items[0].checkpoint = record->snapshot;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
#define cluster_wal_origin_inputs_read_locked pass2_origin_read
#define inputs_checkpoint pass2_native_checkpoint
#define inputs_cold_checkpoint_select pass2_checkpoint_select
#include "test_cluster_typed_pass2_select.inc"
#undef inputs_cold_checkpoint_select
#undef inputs_checkpoint
#undef cluster_wal_origin_inputs_read_locked

UT_TEST(post_ir_checkpoint_selector_preserves_the_original_source_tuple)
{
	ClusterControlRootSnapshot expected = { 0 };
	WalInputsWork work = { 0 };
	ClusterWalInputsV1 scope = { 0 };
	expected.identity.origin_thread_id = 1;
	expected.identity.origin_owner_incarnation = 7;
	expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	work.root.present[0] = true;
	for (unsigned fault = 0; fault < 15; ++fault) {
		ClusterControlRootSnapshot *current;
		memset(&pass2_origin, 0, sizeof(pass2_origin));
		pass2_origin.current.snapshot = expected;
		current = &pass2_origin.current.snapshot;
		switch (fault) {
		case 1:
			current->checkpoint_tli++;
			break;
		case 2:
			current->checkpoint_source_kind++;
			break;
		case 3:
			current->checkpoint_lower_lsn++;
			break;
		case 4:
			current->checkpoint_record_crc32c++;
			break;
		case 5:
			current->tail_tli++;
			break;
		case 6:
			current->tail_validation_kind++;
			break;
		case 7:
			current->validated_tail_lsn_exclusive++;
			break;
		case 8:
			current->tail_last_record_lsn++;
			break;
		case 9:
			current->tail_last_record_crc32c++;
			break;
		case 10:
			current->identity.origin_owner_incarnation++;
			break;
		case 11:
			current->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
			break;
		case 12:
			current->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
			break;
		case 13:
			pass2_origin.has_pending = true;
			break;
		case 14:
			pass2_origin.history.count = 1;
			pass2_origin.history.records[0] = pass2_origin.current;
			break;
		}
		UT_ASSERT_EQ(pass2_checkpoint_select(&scope, &work, &expected, true),
					 fault == 0 || fault == 11 ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
					 : fault == 13			   ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
											   : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		if (fault == 11)
			UT_ASSERT_EQ(
				pass2_checkpoint_select(&scope, &work, &expected, false),
				CLUSTER_CONTROL_ROOT_STALE_TOKEN); /* Ordinary exact-token policy unchanged. */
	}
}

static void
pass2_advance_only_file_token(void)
{
	++input_token.file_txn_seq;
	input_read_hook = NULL;
}
UT_TEST(post_ir_file_token_advancing_between_reads_is_not_source_drift)
{
	for (unsigned readback = 0; readback < 2; ++readback) {
		ClusterWalRecoveryCheckpointV1 out;
		ClusterControlRootResult result;
		input_fixture(1);
		memset(&input_expected, 0, sizeof(input_expected));
		input_expected.identity.system_identifier = 1234;
		input_expected.identity.storage_uuid[0] = 31;
		input_expected.identity.origin_thread_id = 1;
		input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		input_expected_original = input_expected;
		input_claim_reads = input_selections = 0;
		input_read_hook = pass2_advance_only_file_token;
		result = readback
					 ? cluster_wal_inputs_cold_checkpoint_readback_v1(&input_expected, &input_token,
																	  &out)
					 : cluster_wal_inputs_cold_checkpoint_v1(&input_expected, &input_token, &out);
		UT_ASSERT_EQ(result,
					 readback ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(input_reads, 2);
		UT_ASSERT_EQ(out.checkpoint_start, readback ? 100 : 0);
		UT_ASSERT_EQ(input_selections, readback ? 2 : 1);
		cold_cleanup();
	}
}

UT_TEST(post_ir_between_read_source_drift_never_exports_a_checkpoint)
{
	ClusterWalRecoveryCheckpointV1 out;
	input_fixture(1);
	memset(&input_expected, 0, sizeof(input_expected));
	input_expected.identity.system_identifier = 1234;
	input_expected.identity.storage_uuid[0] = 31;
	input_expected.identity.origin_thread_id = 1;
	input_expected.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	input_expected_original = input_expected;
	input_claim_reads = input_selections = 0;
	input_second_selection_drift = true;
	memset(&out, 0xff, sizeof(out));
	UT_ASSERT_EQ(
		cluster_wal_inputs_cold_checkpoint_readback_v1(&input_expected, &input_token, &out),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(out.checkpoint_start, 0);
	UT_ASSERT_EQ(out.source.timeline, 0);
	UT_ASSERT_EQ(input_reads, 2);
	UT_ASSERT_EQ(input_selections, 2);
	UT_ASSERT_EQ(input_claim_reads, 1);
	input_second_selection_drift = false;
	cold_cleanup();
}
