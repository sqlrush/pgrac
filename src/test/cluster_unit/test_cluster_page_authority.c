/*-------------------------------------------------------------------------
 *
 * test_cluster_page_authority.c
 *    STOP-03/04/05 PAGE authority adapter.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_authority.h")
#include "cluster/cluster_page_authority.h"
#define TEST_HAVE_CLUSTER_PAGE_AUTHORITY 1
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

#ifndef TEST_HAVE_CLUSTER_PAGE_AUTHORITY

UT_TEST(test_page_authority_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-7-STOP030405-AUTHORITY\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_page_authority_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static char formation_object;
static char needs_object;
static char admissions_object;
static char pin_object;
#define TEST_AUTHORITY_TARGETS 40

static char stable_proof_objects[TEST_AUTHORITY_TARGETS];
static RfPagePinnedSourceV1 source_objects[TEST_AUTHORITY_TARGETS];
static RfContributorVectorV1 vector_objects[TEST_AUTHORITY_TARGETS];
static ClusterControlRootReadToken root_tokens[1];
static ClusterRecoveryDutyKey duty_objects[1];
static bool fence_need_current;
static bool fence_admission_current;
static bool pin_current;
static bool stable_proof_current;
static ClusterRecoverySerialRevalidateResult serial_result;

#define FORMATION ((const ClusterFormationWitnessV1 *)&formation_object)
#define NEEDS ((const PgracExternalFenceNeedSetV1 *)&needs_object)
#define ADMISSIONS ((const PgracExternalFenceAdmissionSetV1 *)&admissions_object)
#define PIN ((ClusterWalRetentionPin *)&pin_object)
#define ROOT_TOKENS (root_tokens)
#define DUTIES (duty_objects)

bool
rf_page_stable_base_proof_matches_v1(
	const RfPageStableBaseProofV1 *proof, const RfPageIdentityV1 *page_identity,
	const RfPageVersionV1 *expected_result, const ClusterRecoveryDutyKey *duties,
	const ClusterControlRootReadToken *roots, const ClusterFormationWitnessV1 *formation,
	const PgracExternalFenceNeedSetV1 *needs,
	const PgracExternalFenceAdmissionSetV1 *fence_admissions, ClusterWalRetentionPin *pin,
	const RfPagePinnedSourceV1 *source, const RfContributorVectorV1 *contributors,
	uint32 participant_count)
{
	int index = -1;
	int i;

	if (!stable_proof_current || duties != DUTIES || roots != ROOT_TOKENS || formation != FORMATION
		|| needs != NEEDS || fence_admissions != ADMISSIONS || pin != PIN || participant_count != 1)
		return false;
	for (i = 0; i < TEST_AUTHORITY_TARGETS; i++)
		if (proof == (const RfPageStableBaseProofV1 *)&stable_proof_objects[i]) {
			index = i;
			break;
		}
	if (index < 0)
		return false;
	if (contributors != &vector_objects[index])
		return false;
	return page_identity != NULL && page_identity->blockno == (uint32)index + 1
		   && expected_result != NULL && expected_result->mutation_token == (uint64)index + 101
		   && memcmp(expected_result->segment_incarnation,
					 (const uint8[16]){ 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 }, 16)
				  == 0
		   && source == &source_objects[index];
}

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed)
{
	return expected == &DUTIES[0] && observed != NULL
				   && observed->origin_thread_id == DUTIES[0].origin_thread_id
				   && observed->root_lineage_seq == DUTIES[0].root_lineage_seq
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard)
{
	return guard != NULL ? serial_result : CLUSTER_RECOVERY_SERIAL_NOT_HELD;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = fence_need_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE
									 : PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH;
	return needs == NEEDS && formation == FORMATION && fence_need_current;
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = fence_admission_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE
										  : PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
	return admissions == ADMISSIONS && needs == NEEDS && formation == FORMATION
		   && fence_admission_current;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	return pin == PIN && pin_current ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

static RfPageIdentityV1
identity(uint32 blockno)
{
	RfPageIdentityV1 value;

	memset(&value, 0, sizeof(value));
	value.system_identifier = 99;
	memset(value.storage_uuid, 3, sizeof(value.storage_uuid));
	value.locator.spcOid = 1;
	value.locator.dbOid = 2;
	value.locator.relNumber = 3;
	value.forknum = MAIN_FORKNUM;
	value.blockno = blockno;
	return value;
}

static void
init_case(RfPageAuthorityBatchRequestV1 *request, RfPageAuthorityTargetV1 targets[2],
		  ClusterRecoverySerialGuard *serial)
{
	memset(request, 0, sizeof(*request));
	memset(targets, 0, sizeof(RfPageAuthorityTargetV1) * 2);
	memset(serial, 0, sizeof(*serial));
	targets[0].page_identity = identity(1);
	targets[1].page_identity = identity(2);
	memset(targets[0].expected_result.segment_incarnation, 7, 16);
	memset(targets[1].expected_result.segment_incarnation, 7, 16);
	targets[0].expected_result.mutation_token = 101;
	targets[1].expected_result.mutation_token = 102;
	targets[0].stable_base = (const RfPageStableBaseProofV1 *)&stable_proof_objects[0];
	targets[1].stable_base = (const RfPageStableBaseProofV1 *)&stable_proof_objects[1];
	targets[0].source = &source_objects[0];
	targets[1].source = &source_objects[1];
	targets[0].contributors = &vector_objects[0];
	targets[1].contributors = &vector_objects[1];
	request->targets = targets;
	request->target_count = 2;
	request->formation = FORMATION;
	request->fence_need_set = NEEDS;
	request->fence_admission_set = ADMISSIONS;
	request->retention_pin = PIN;
	request->duties = DUTIES;
	request->root_tokens = ROOT_TOKENS;
	request->participant_count = 1;
	serial->held = true;
	serial->formation = FORMATION;
	serial->fence_need_set = NEEDS;
	serial->fence_admission_set = ADMISSIONS;
	memset(DUTIES, 0, sizeof(duty_objects));
	memset(ROOT_TOKENS, 0, sizeof(root_tokens));
	DUTIES[0].origin_thread_id = 1;
	DUTIES[0].root_lineage_seq = 91;
	memset(ROOT_TOKENS[0].authority_uuid, 0x31, sizeof(ROOT_TOKENS[0].authority_uuid));
	ROOT_TOKENS[0].origin_thread_id = 1;
	ROOT_TOKENS[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	ROOT_TOKENS[0].root_lineage_seq = 91;
	ROOT_TOKENS[0].file_txn_seq = 92;
	ROOT_TOKENS[0].root_publish_seq = 93;
	ROOT_TOKENS[0].record_crc32c = 94;
	ROOT_TOKENS[0].root_flags = CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	serial->duty = DUTIES[0];
	serial->root_read_token = ROOT_TOKENS[0];
	fence_need_current = true;
	fence_admission_current = true;
	pin_current = true;
	stable_proof_current = true;
	serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;
}

UT_TEST(test_exact_owner_set_promotes_and_revalidates)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_revalidate_nowait_v1(guard, &serial),
				 RF_PAGE_AUTHORITY_OK);
	rf_page_authority_guard_release_v1(&guard);
	rf_page_authority_preflight_destroy_v1(&preflight);
	UT_ASSERT(guard == NULL && preflight == NULL);
}

UT_TEST(test_batch_target_set_is_not_limited_by_record_components)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[TEST_AUTHORITY_TARGETS];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;
	uint32 i;

	init_case(&request, targets, &serial);
	for (i = 2; i < TEST_AUTHORITY_TARGETS; i++) {
		targets[i].page_identity = identity(i + 1);
		memset(targets[i].expected_result.segment_incarnation, 7, 16);
		targets[i].expected_result.mutation_token = i + 101;
		targets[i].stable_base = (const RfPageStableBaseProofV1 *)&stable_proof_objects[i];
		targets[i].source = &source_objects[i];
		targets[i].contributors = &vector_objects[i];
	}
	request.target_count = TEST_AUTHORITY_TARGETS;
	rf_page_guard_shmem_init_v1();
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_revalidate_nowait_v1(guard, &serial),
				 RF_PAGE_AUTHORITY_OK);
	rf_page_authority_guard_release_v1(&guard);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_missing_live_stop04_object_is_invalid)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;

	init_case(&request, targets, &serial);
	request.fence_need_set = NULL;
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_INVALID_ARGUMENT);
	UT_ASSERT(preflight == NULL);
}

UT_TEST(test_serial_pointer_identity_mismatch_is_fence_stale)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	serial.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)&pin_object;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_FENCE_STALE);
	UT_ASSERT(guard == NULL);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_fence_stale_blocks_before_page_promote)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	fence_admission_current = false;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_FENCE_STALE);
	UT_ASSERT(guard == NULL);
	fence_admission_current = true;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_OK);
	rf_page_authority_guard_release_v1(&guard);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_page_conflict_releases_already_promoted_partition)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;
	RfPageGuardPreflightV1 blocker_preflight;
	RfPageGuardV1 blocker;
	RfPageGuardV1 first_probe;
	RfPageGuardPreflightV1 first_preflight;

	init_case(&request, targets, &serial);
	rf_page_guard_shmem_init_v1();
	UT_ASSERT(rf_page_guard_preflight_v1(&targets[1].page_identity, &blocker_preflight));
	memset(&blocker, 0, sizeof(blocker));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&blocker_preflight, &blocker));
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT(guard == NULL);
	UT_ASSERT(rf_page_guard_preflight_v1(&targets[0].page_identity, &first_preflight));
	memset(&first_probe, 0, sizeof(first_probe));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&first_preflight, &first_probe));
	rf_page_guard_release_v1(&first_probe);
	rf_page_guard_release_v1(&blocker);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_stale_pin_after_promote_fails_revalidation)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	rf_page_guard_shmem_init_v1();
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_OK);
	pin_current = false;
	UT_ASSERT_EQ(rf_page_authority_batch_revalidate_nowait_v1(guard, &serial),
				 RF_PAGE_AUTHORITY_RETENTION_STALE);
	rf_page_authority_guard_release_v1(&guard);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_missing_stable_proof_blocks_preflight)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;

	init_case(&request, targets, &serial);
	targets[0].stable_base = NULL;
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_NO_STABLE_BASE);
	UT_ASSERT(preflight == NULL);
}

UT_TEST(test_stable_proof_drift_blocks_page_promotion)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	stable_proof_current = false;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_NO_STABLE_BASE);
	UT_ASSERT(guard == NULL);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_serial_duty_mismatch_blocks_page_promotion)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	serial.duty.root_lineage_seq++;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_GENERATION_STALE);
	UT_ASSERT(guard == NULL);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_serial_root_token_mismatch_blocks_page_promotion)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageAuthorityGuardV1 *guard = NULL;

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	serial.root_read_token.root_publish_seq++;
	UT_ASSERT_EQ(rf_page_authority_batch_promote_nowait_v1(preflight, &serial, &guard),
				 RF_PAGE_AUTHORITY_ROOT_STALE);
	UT_ASSERT(guard == NULL);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_identity_and_incarnation_are_exact)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageIdentityV1 other;
	uint8 incarnation[16];

	init_case(&request, targets, &serial);
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	memset(incarnation, 7, sizeof(incarnation));
	UT_ASSERT(rf_page_authority_preflight_matches_target_v1(preflight, &targets[0].page_identity,
															incarnation));
	other = targets[0].page_identity;
	other.blockno = 3;
	UT_ASSERT(!rf_page_authority_preflight_matches_target_v1(preflight, &other, incarnation));
	incarnation[0]++;
	UT_ASSERT(!rf_page_authority_preflight_matches_target_v1(preflight, &targets[0].page_identity,
															 incarnation));
	rf_page_authority_preflight_destroy_v1(&preflight);
}

UT_TEST(test_install_adapter_runs_promote_publish_release)
{
	RfPageAuthorityBatchRequestV1 request;
	RfPageAuthorityTargetV1 targets[2];
	ClusterRecoverySerialGuard serial;
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageInstallAuthorityAdapterV1 adapter;

	init_case(&request, targets, &serial);
	rf_page_guard_shmem_init_v1();
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 1000, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT(rf_page_install_authority_adapter_init_v1(preflight, &serial, &adapter));
	UT_ASSERT(adapter.ops.validate_identity(adapter.ops.arg, &targets[0].page_identity,
											targets[0].expected_result.segment_incarnation));
	UT_ASSERT(adapter.ops.promote(adapter.ops.arg));
	UT_ASSERT(adapter.ops.publish(adapter.ops.arg));
	UT_ASSERT(adapter.ops.release(adapter.ops.arg));
	UT_ASSERT(adapter.guard == NULL && adapter.proof_published);
	rf_page_authority_preflight_destroy_v1(&preflight);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_exact_owner_set_promotes_and_revalidates);
	UT_RUN(test_batch_target_set_is_not_limited_by_record_components);
	UT_RUN(test_missing_live_stop04_object_is_invalid);
	UT_RUN(test_serial_pointer_identity_mismatch_is_fence_stale);
	UT_RUN(test_fence_stale_blocks_before_page_promote);
	UT_RUN(test_page_conflict_releases_already_promoted_partition);
	UT_RUN(test_stale_pin_after_promote_fails_revalidation);
	UT_RUN(test_missing_stable_proof_blocks_preflight);
	UT_RUN(test_stable_proof_drift_blocks_page_promotion);
	UT_RUN(test_serial_duty_mismatch_blocks_page_promotion);
	UT_RUN(test_serial_root_token_mismatch_blocks_page_promotion);
	UT_RUN(test_identity_and_incarnation_are_exact);
	UT_RUN(test_install_adapter_runs_promote_publish_release);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
