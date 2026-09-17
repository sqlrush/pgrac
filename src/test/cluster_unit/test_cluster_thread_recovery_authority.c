/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_recovery_authority.c
 *    STOP-03/04/05 owner bundle for online recovery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_thread_recovery_authority.h")
#include "cluster/cluster_thread_recovery_authority.h"
#define TEST_HAVE_CLUSTER_THREAD_RECOVERY_AUTHORITY 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_THREAD_RECOVERY_AUTHORITY

UT_TEST(test_thread_recovery_authority_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-7-ONLINE-STOP05-PIN\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_thread_recovery_authority_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static char formation_object;
static char needs_object;
static char admissions_object;
static char pin_object;
static bool serial_current;
static bool fence_current;
static bool pin_current;
static ClusterControlRootResult finalize_publish_results[3];
static int finalize_publish_result_count;
static int finalize_publish_calls;
static ClusterControlRootSnapshot finalize_published_root;
static ClusterControlRootReadToken finalize_published_token;
static ClusterControlRootResult finalize_read_result;
static int finalize_read_calls;
static ClusterControlRootSnapshot finalize_read_root;
static ClusterControlRootReadToken finalize_read_token;
static ClusterWalPinResult finalize_adopt_result;
static int finalize_adopt_calls;

#define FORMATION ((const ClusterFormationWitnessV1 *)&formation_object)
#define NEEDS ((const PgracExternalFenceNeedSetV1 *)&needs_object)
#define ADMISSIONS ((const PgracExternalFenceAdmissionSetV1 *)&admissions_object)
#define PIN ((ClusterWalRetentionPin *)&pin_object)

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard)
{
	return guard != NULL && serial_current ? CLUSTER_RECOVERY_SERIAL_CURRENT
										   : CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *formation)
{
	return formation == FORMATION && fence_current ? CLUSTER_FORMATION_WITNESS_READY
												   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason
			= fence_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE : PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
	return needs == NEEDS && formation == FORMATION && fence_current;
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason
			= fence_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE : PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
	return admissions == ADMISSIONS && needs == NEEDS && formation == FORMATION && fence_current;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	return pin == PIN && pin_current ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

ClusterControlRootResult
cluster_control_root_recovery_complete_publish_v1(const ClusterControlRootReadToken *expected_token,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;

	(void)expected_token;
	(void)patch;
	if (finalize_publish_calls < finalize_publish_result_count)
		result = finalize_publish_results[finalize_publish_calls];
	finalize_publish_calls++;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out_snapshot = finalize_published_root;
		*out_token = finalize_published_token;
	}
	return result;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id,
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
	(void)origin_thread_id;
	(void)expected_identity;
	(void)mode;
	finalize_read_calls++;
	if (finalize_read_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| finalize_read_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*out_snapshot = finalize_read_root;
		*out_token = finalize_read_token;
	}
	return finalize_read_result;
}

ClusterWalPinResult
cluster_wal_retention_pin_adopt_root_readback_v1(
	ClusterWalRetentionPin *pin, const ClusterControlRootSnapshot *expected_snapshot,
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootSnapshot *observed_snapshot,
	const ClusterControlRootReadToken *observed_token)
{
	(void)pin;
	(void)expected_snapshot;
	(void)expected_token;
	(void)observed_snapshot;
	(void)observed_token;
	finalize_adopt_calls++;
	return finalize_adopt_result;
}

static void
init_case(ClusterRecoveryDutyKey *duty, ClusterControlRootSnapshot *root,
		  ClusterControlRootReadToken *token, ClusterRecoverySerialGuard *serial,
		  ClusterThreadRecoveryAuthorityV1 *authority)
{
	memset(duty, 0, sizeof(*duty));
	memset(root, 0, sizeof(*root));
	memset(token, 0, sizeof(*token));
	memset(serial, 0, sizeof(*serial));
	memset(authority, 0, sizeof(*authority));
	duty->origin_thread_id = 3;
	duty->root_lineage_seq = 44;
	memset(duty->authority_uuid, 0x41, sizeof(duty->authority_uuid));
	root->identity = *duty;
	root->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	root->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	root->checkpoint_tli = 7;
	root->tail_tli = 7;
	root->checkpoint_lower_lsn = 100;
	root->validated_tail_lsn_exclusive = 500;
	memcpy(token->authority_uuid, duty->authority_uuid, 16);
	token->origin_thread_id = 3;
	token->source = 1;
	token->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	token->root_lineage_seq = 44;
	token->file_txn_seq = 45;
	token->root_publish_seq = 46;
	token->record_crc32c = 47;
	token->root_flags = root->root_flags;
	serial->held = true;
	serial->duty = *duty;
	serial->root_read_token = *token;
	serial->formation = FORMATION;
	serial->fence_need_set = NEEDS;
	serial->fence_admission_set = ADMISSIONS;
	authority->duty = duty;
	authority->root_snapshot = root;
	authority->root_token = token;
	authority->formation = FORMATION;
	authority->fence_need_set = NEEDS;
	authority->fence_admission_set = ADMISSIONS;
	authority->retention_pin = PIN;
	authority->serial_guard = serial;
	serial_current = true;
	fence_current = true;
	pin_current = true;
	memset(finalize_publish_results, 0, sizeof(finalize_publish_results));
	finalize_publish_result_count = 0;
	finalize_publish_calls = 0;
	memset(&finalize_published_root, 0, sizeof(finalize_published_root));
	memset(&finalize_published_token, 0, sizeof(finalize_published_token));
	finalize_read_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	finalize_read_calls = 0;
	memset(&finalize_read_root, 0, sizeof(finalize_read_root));
	memset(&finalize_read_token, 0, sizeof(finalize_read_token));
	finalize_adopt_result = CLUSTER_WAL_PIN_OK;
	finalize_adopt_calls = 0;
}

UT_TEST(test_exact_root_builds_one_pin_interval)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request;

	init_case(&duty, &root, &token, &serial, &authority);
	UT_ASSERT(cluster_thread_recovery_pin_request_build_v1(3, &duty, &root, &token, FORMATION,
														   NEEDS, ADMISSIONS, &interval, &request));
	UT_ASSERT_EQ(interval.thread_id, 3);
	UT_ASSERT_EQ(interval.tli, 7);
	UT_ASSERT_EQ(interval.start_lsn, 100);
	UT_ASSERT_EQ(interval.end_lsn, 500);
	UT_ASSERT(request.intervals == &interval && request.nintervals == 1);
	UT_ASSERT(request.formation == FORMATION && request.needs == NEEDS
			  && request.admissions == ADMISSIONS);
}

UT_TEST(test_foreign_root_identity_is_zero_output)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request;

	init_case(&duty, &root, &token, &serial, &authority);
	root.identity.root_lineage_seq++;
	memset(&interval, 0x7f, sizeof(interval));
	memset(&request, 0x7f, sizeof(request));
	UT_ASSERT(!cluster_thread_recovery_pin_request_build_v1(
		3, &duty, &root, &token, FORMATION, NEEDS, ADMISSIONS, &interval, &request));
	UT_ASSERT_EQ(interval.end_lsn, 0);
	UT_ASSERT(request.intervals == NULL);
}

UT_TEST(test_cross_timeline_interval_fails_closed)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request;

	init_case(&duty, &root, &token, &serial, &authority);
	root.tail_tli++;
	UT_ASSERT(!cluster_thread_recovery_pin_request_build_v1(
		3, &duty, &root, &token, FORMATION, NEEDS, ADMISSIONS, &interval, &request));
}

UT_TEST(test_empty_or_reversed_interval_fails_closed)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request;

	init_case(&duty, &root, &token, &serial, &authority);
	root.validated_tail_lsn_exclusive = root.checkpoint_lower_lsn;
	UT_ASSERT(!cluster_thread_recovery_pin_request_build_v1(
		3, &duty, &root, &token, FORMATION, NEEDS, ADMISSIONS, &interval, &request));
}

UT_TEST(test_exact_owner_bundle_revalidates)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	UT_ASSERT_EQ(cluster_thread_recovery_authority_revalidate_nowait_v1(&authority),
				 CLUSTER_THREAD_AUTHORITY_OK);
}

UT_TEST(test_serial_root_drift_is_stale)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	serial.root_read_token.root_publish_seq++;
	UT_ASSERT_EQ(cluster_thread_recovery_authority_revalidate_nowait_v1(&authority),
				 CLUSTER_THREAD_AUTHORITY_ROOT_STALE);
}

UT_TEST(test_fence_drift_is_stale)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	fence_current = false;
	UT_ASSERT_EQ(cluster_thread_recovery_authority_revalidate_nowait_v1(&authority),
				 CLUSTER_THREAD_AUTHORITY_FENCE_STALE);
}

UT_TEST(test_pin_drift_is_stale)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	pin_current = false;
	UT_ASSERT_EQ(cluster_thread_recovery_authority_revalidate_nowait_v1(&authority),
				 CLUSTER_THREAD_AUTHORITY_PIN_STALE);
}

UT_TEST(test_exact_window_is_covered_by_held_pin)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	UT_ASSERT(cluster_thread_recovery_authority_covers_window_v1(&authority, 3, 100, 500));
	UT_ASSERT(cluster_thread_recovery_authority_covers_window_v1(&authority, 3, 200, 400));
}

UT_TEST(test_window_outside_held_pin_fails_closed)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;

	init_case(&duty, &root, &token, &serial, &authority);
	UT_ASSERT(!cluster_thread_recovery_authority_covers_window_v1(&authority, 3, 99, 500));
	UT_ASSERT(!cluster_thread_recovery_authority_covers_window_v1(&authority, 3, 100, 501));
	UT_ASSERT(!cluster_thread_recovery_authority_covers_window_v1(&authority, 4, 100, 500));
	UT_ASSERT(!cluster_thread_recovery_authority_covers_window_v1(&authority, 3, 500, 500));
}

UT_TEST(test_complete_window_builds_exact_root_patch)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;

	init_case(&duty, &root, &token, &serial, &authority);
	root.root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	token.root_flags = root.root_flags;
	serial.root_read_token = token;
	root.tail_last_record_lsn = 450;
	root.tail_last_record_crc32c = 88;
	UT_ASSERT(cluster_thread_recovery_root_complete_patch_build_v1(&authority, 500, &patch));
	UT_ASSERT_EQ(patch.mask, CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE
								 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS);
	UT_ASSERT_EQ(patch.expected_lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(patch.desired.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(patch.desired.recovered_tli, 7);
	UT_ASSERT_EQ(patch.desired.recovered_through_lsn_exclusive, 500);
	UT_ASSERT_EQ(patch.desired.recovered_last_record_lsn, 450);
	UT_ASSERT_EQ(patch.desired.recovered_last_record_crc32c, 88);
	UT_ASSERT((patch.desired.root_flags
			   & (CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
				  | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID))
			  != 0);
}

UT_TEST(test_short_window_cannot_build_complete_root_patch)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;

	init_case(&duty, &root, &token, &serial, &authority);
	root.root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	token.root_flags = root.root_flags;
	serial.root_read_token = token;
	root.tail_last_record_lsn = 450;
	root.tail_last_record_crc32c = 88;
	memset(&patch, 0x7f, sizeof(patch));
	UT_ASSERT(!cluster_thread_recovery_root_complete_patch_build_v1(&authority, 499, &patch));
	UT_ASSERT_EQ(patch.mask, 0);
}

static void
prepare_finalize_case(ClusterRecoveryDutyKey *duty, ClusterControlRootSnapshot *root,
					  ClusterControlRootReadToken *token, ClusterRecoverySerialGuard *serial,
					  ClusterThreadRecoveryAuthorityV1 *authority, ClusterControlRootPatch *patch)
{
	init_case(duty, root, token, serial, authority);
	root->root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	token->root_flags = root->root_flags;
	serial->root_read_token = *token;
	root->tail_last_record_lsn = 450;
	root->tail_last_record_crc32c = 88;
	UT_ASSERT(cluster_thread_recovery_root_complete_patch_build_v1(authority, 500, patch));
	serial->held = false;
	finalize_published_root = *root;
	finalize_published_root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	finalize_published_root.root_flags = patch->desired.root_flags;
	finalize_published_root.recovered_tli = patch->desired.recovered_tli;
	finalize_published_root.recovered_through_lsn_exclusive
		= patch->desired.recovered_through_lsn_exclusive;
	finalize_published_root.recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
	finalize_published_root.recovered_last_record_crc32c
		= patch->desired.recovered_last_record_crc32c;
	finalize_published_token = *token;
	finalize_published_token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	finalize_published_token.root_flags = finalize_published_root.root_flags;
	finalize_published_token.root_publish_seq++;
	finalize_published_token.record_crc32c++;
}

UT_TEST(test_root_finalize_exact_cas_succeeds)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;

	prepare_finalize_case(&duty, &root, &token, &serial, &authority, &patch);
	finalize_publish_results[0] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	finalize_publish_result_count = 1;
	UT_ASSERT_EQ(cluster_thread_recovery_root_finalize_after_ir_v1(&authority, &patch, &published),
				 CLUSTER_THREAD_ROOT_FINALIZE_OK);
	UT_ASSERT_EQ(finalize_publish_calls, 1);
	UT_ASSERT_EQ(finalize_read_calls, 0);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
}

UT_TEST(test_root_finalize_stale_token_reconciles_without_page_replay)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;

	prepare_finalize_case(&duty, &root, &token, &serial, &authority, &patch);
	finalize_publish_results[0] = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	finalize_publish_results[1] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	finalize_publish_result_count = 2;
	finalize_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	finalize_read_root = root;
	finalize_read_token = token;
	finalize_read_token.file_txn_seq++;
	finalize_read_token.root_publish_seq++;
	finalize_read_token.record_crc32c++;
	UT_ASSERT_EQ(cluster_thread_recovery_root_finalize_after_ir_v1(&authority, &patch, &published),
				 CLUSTER_THREAD_ROOT_FINALIZE_OK);
	UT_ASSERT_EQ(finalize_publish_calls, 2);
	UT_ASSERT_EQ(finalize_read_calls, 1);
	UT_ASSERT_EQ(finalize_adopt_calls, 1);
}

UT_TEST(test_root_finalize_adopts_already_complete_readback)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;

	prepare_finalize_case(&duty, &root, &token, &serial, &authority, &patch);
	finalize_publish_results[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	finalize_publish_result_count = 1;
	finalize_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	finalize_read_root = finalize_published_root;
	finalize_read_token = finalize_published_token;
	UT_ASSERT_EQ(cluster_thread_recovery_root_finalize_after_ir_v1(&authority, &patch, &published),
				 CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE);
	UT_ASSERT_EQ(finalize_publish_calls, 1);
	UT_ASSERT_EQ(finalize_read_calls, 1);
	UT_ASSERT_EQ(finalize_adopt_calls, 1);
}

UT_TEST(test_root_finalize_immutable_drift_requests_fresh_plan)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;

	prepare_finalize_case(&duty, &root, &token, &serial, &authority, &patch);
	finalize_publish_results[0] = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	finalize_publish_result_count = 1;
	finalize_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	finalize_read_root = root;
	finalize_read_token = token;
	finalize_read_token.root_publish_seq++;
	finalize_adopt_result = CLUSTER_WAL_PIN_STALE;
	UT_ASSERT_EQ(cluster_thread_recovery_root_finalize_after_ir_v1(&authority, &patch, &published),
				 CLUSTER_THREAD_ROOT_FINALIZE_RETRY);
	UT_ASSERT_EQ(finalize_publish_calls, 1);
	UT_ASSERT_EQ(finalize_adopt_calls, 1);
}

UT_TEST(test_root_finalize_release_uncertain_preserves_cleanup_ownership)
{
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;

	prepare_finalize_case(&duty, &root, &token, &serial, &authority, &patch);
	finalize_publish_results[0] = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	finalize_publish_result_count = 1;
	UT_ASSERT_EQ(cluster_thread_recovery_root_finalize_after_ir_v1(&authority, &patch, &published),
				 CLUSTER_THREAD_ROOT_FINALIZE_CLEANUP_UNCERTAIN);
	UT_ASSERT_EQ(finalize_read_calls, 0);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_exact_root_builds_one_pin_interval);
	UT_RUN(test_foreign_root_identity_is_zero_output);
	UT_RUN(test_cross_timeline_interval_fails_closed);
	UT_RUN(test_empty_or_reversed_interval_fails_closed);
	UT_RUN(test_exact_owner_bundle_revalidates);
	UT_RUN(test_serial_root_drift_is_stale);
	UT_RUN(test_fence_drift_is_stale);
	UT_RUN(test_pin_drift_is_stale);
	UT_RUN(test_exact_window_is_covered_by_held_pin);
	UT_RUN(test_window_outside_held_pin_fails_closed);
	UT_RUN(test_complete_window_builds_exact_root_patch);
	UT_RUN(test_short_window_cannot_build_complete_root_patch);
	UT_RUN(test_root_finalize_exact_cas_succeeds);
	UT_RUN(test_root_finalize_stale_token_reconciles_without_page_replay);
	UT_RUN(test_root_finalize_adopts_already_complete_readback);
	UT_RUN(test_root_finalize_immutable_drift_requests_fresh_plan);
	UT_RUN(test_root_finalize_release_uncertain_preserves_cleanup_ownership);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
