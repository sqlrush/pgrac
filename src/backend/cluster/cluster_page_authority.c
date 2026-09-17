/*-------------------------------------------------------------------------
 *
 * cluster_page_authority.c
 *    STOP-03/04/05 binding for STOP-06 PAGE target protection.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_authority.h"

#ifdef USE_CLUSTER_UNIT
#define authority_alloc0(size_) calloc(1, (size_))
#define authority_free(pointer_) free((pointer_))
#else
#define authority_alloc0(size_) palloc0(size_)
#define authority_free(pointer_) pfree(pointer_)
#endif

#define RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC UINT32_C(0x52504150)
#define RF_PAGE_AUTHORITY_GUARD_MAGIC UINT32_C(0x52504147)

typedef enum RfPageAuthorityStateV1 {
	RF_PAGE_AUTHORITY_STATE_PREFLIGHTED = 1,
	RF_PAGE_AUTHORITY_STATE_PROMOTED = 2,
	RF_PAGE_AUTHORITY_STATE_RELEASED = 3
} RfPageAuthorityStateV1;

struct RfPageAuthorityGuardV1 {
	uint32 magic;
	uint8 state;
	uint8 reserved_zero[3];
	struct RfPageAuthorityPreflightV1 *preflight;
	ClusterRecoverySerialGuard *serial_guard;
	uint32 guard_count;
	uint8 *target_guard_index;
	RfPageGuardV1 page_guards[RF_PAGE_GUARD_PARTITIONS];
};

struct RfPageAuthorityPreflightV1 {
	uint32 magic;
	uint8 state;
	uint8 reserved_zero[3];
	uint32 target_count;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	const ClusterRecoveryDutyKey *duties;
	const ClusterControlRootReadToken *root_tokens;
	uint32 participant_count;
	RfPageAuthorityTargetV1 *targets;
	RfPageGuardPreflightV1 *page_preflights;
	uint8 *target_guard_index;
	RfPageAuthorityGuardV1 guard_storage;
};

static void
authority_preflight_free(RfPageAuthorityPreflightV1 *preflight)
{
	if (preflight == NULL)
		return;
	if (preflight->target_guard_index != NULL)
		authority_free(preflight->target_guard_index);
	if (preflight->page_preflights != NULL)
		authority_free(preflight->page_preflights);
	if (preflight->targets != NULL)
		authority_free(preflight->targets);
	authority_free(preflight);
}

static bool
bytes_nonzero(const uint8 *bytes, Size length)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < length; i++)
		value |= bytes[i];
	return value != 0;
}

static bool
authority_identity_valid(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && bytes_nonzero(identity->storage_uuid, sizeof(identity->storage_uuid))
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

static bool
authority_identity_equal(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(left->locator, right->locator) && left->forknum == right->forknum
		   && left->blockno == right->blockno && left->reserved_zero == 0
		   && right->reserved_zero == 0;
}

static int
identity_compare(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
#define CMP_FIELD(field_)                                                                          \
	do {                                                                                           \
		if (left->field_ < right->field_)                                                          \
			return -1;                                                                             \
		if (left->field_ > right->field_)                                                          \
			return 1;                                                                              \
	} while (0)
	int cmp;

	CMP_FIELD(system_identifier);
	cmp = memcmp(left->storage_uuid, right->storage_uuid, 16);
	if (cmp != 0)
		return cmp;
	CMP_FIELD(locator.spcOid);
	CMP_FIELD(locator.dbOid);
	CMP_FIELD(locator.relNumber);
	CMP_FIELD(forknum);
	CMP_FIELD(blockno);
	return 0;
#undef CMP_FIELD
}

static bool
fence_current(const RfPageAuthorityPreflightV1 *preflight)
{
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;

	return cluster_external_fence_need_set_revalidate_nowait(preflight->fence_need_set,
															 preflight->formation, &reason)
		   && cluster_external_fence_revalidate_set_nowait(preflight->fence_admission_set,
														   preflight->fence_need_set,
														   preflight->formation, &reason);
}

static bool
stable_proofs_current(const RfPageAuthorityPreflightV1 *preflight)
{
	uint32 i;

	for (i = 0; i < preflight->target_count; i++) {
		const RfPageAuthorityTargetV1 *target = &preflight->targets[i];

		if (!rf_page_stable_base_proof_matches_v1(
				target->stable_base, &target->page_identity, &target->expected_result,
				preflight->duties, preflight->root_tokens, preflight->formation,
				preflight->fence_need_set, preflight->fence_admission_set, preflight->retention_pin,
				target->source, target->contributors, preflight->participant_count))
			return false;
	}
	return true;
}

static RfPageAuthorityVerdictV1
owners_current(const RfPageAuthorityPreflightV1 *preflight,
			   ClusterRecoverySerialGuard *serial_guard)
{
	ClusterRecoverySerialRevalidateResult serial_result;

	if (preflight == NULL || serial_guard == NULL
		|| preflight->magic != RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC
		|| preflight->state == RF_PAGE_AUTHORITY_STATE_RELEASED)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	if (!serial_guard->held)
		return RF_PAGE_AUTHORITY_SERIAL_NOT_HELD;
	if (serial_guard->formation != preflight->formation
		|| serial_guard->fence_need_set != preflight->fence_need_set
		|| serial_guard->fence_admission_set != preflight->fence_admission_set)
		return RF_PAGE_AUTHORITY_FENCE_STALE;
	if (preflight->participant_count != 1
		|| cluster_recovery_duty_key_compare(&preflight->duties[0], &serial_guard->duty)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT)
		return RF_PAGE_AUTHORITY_GENERATION_STALE;
	if (memcmp(&preflight->root_tokens[0], &serial_guard->root_read_token,
			   sizeof(serial_guard->root_read_token))
		!= 0)
		return RF_PAGE_AUTHORITY_ROOT_STALE;
	if (!stable_proofs_current(preflight))
		return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
	serial_result = cluster_recovery_serial_revalidate(serial_guard);
	if (serial_result != CLUSTER_RECOVERY_SERIAL_CURRENT) {
		if (serial_result == CLUSTER_RECOVERY_SERIAL_NOT_HELD)
			return RF_PAGE_AUTHORITY_SERIAL_NOT_HELD;
		if (serial_result == CLUSTER_RECOVERY_SERIAL_FENCE_STALE)
			return RF_PAGE_AUTHORITY_FENCE_STALE;
		if (serial_result == CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN)
			return RF_PAGE_AUTHORITY_INTERNAL;
		return RF_PAGE_AUTHORITY_GENERATION_STALE;
	}
	if (!fence_current(preflight))
		return RF_PAGE_AUTHORITY_FENCE_STALE;
	if (cluster_wal_retention_pin_revalidate(preflight->retention_pin) != CLUSTER_WAL_PIN_OK)
		return RF_PAGE_AUTHORITY_RETENTION_STALE;
	return RF_PAGE_AUTHORITY_OK;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_preflight_wait_v1(const RfPageAuthorityBatchRequestV1 *request,
										  int timeout_ms,
										  RfPageAuthorityPreflightV1 **out_preflight)
{
	RfPageAuthorityPreflightV1 *preflight;
	uint32 i;

	if (out_preflight == NULL)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	*out_preflight = NULL;
	if (request == NULL || request->targets == NULL || request->target_count == 0
		|| request->target_count > RF_PAGE_STABLE_MAX_EDGES || request->formation == NULL
		|| request->fence_need_set == NULL || request->fence_admission_set == NULL
		|| request->retention_pin == NULL || request->duties == NULL || request->root_tokens == NULL
		|| request->participant_count == 0 || request->participant_count != 1 || request->flags != 0
		|| timeout_ms < 0)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	for (i = 0; i < request->target_count; i++) {
		if (!authority_identity_valid(&request->targets[i].page_identity)
			|| !bytes_nonzero(request->targets[i].expected_result.segment_incarnation, 16)
			|| request->targets[i].expected_result.mutation_token == 0
			|| request->targets[i].stable_base == NULL || request->targets[i].source == NULL
			|| request->targets[i].contributors == NULL
			|| (i > 0
				&& identity_compare(&request->targets[i - 1].page_identity,
									&request->targets[i].page_identity)
					   >= 0)) {
			if (request->targets[i].stable_base == NULL)
				return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
			if (request->targets[i].contributors == NULL)
				return RF_PAGE_AUTHORITY_CONTRIBUTOR_INCOMPLETE;
			return RF_PAGE_AUTHORITY_IDENTITY_MISMATCH;
		}
	}
	preflight = (RfPageAuthorityPreflightV1 *)authority_alloc0(sizeof(*preflight));
	if (preflight == NULL)
		return RF_PAGE_AUTHORITY_OOM;
	preflight->targets = (RfPageAuthorityTargetV1 *)authority_alloc0((Size)request->target_count
																	 * sizeof(*preflight->targets));
	preflight->page_preflights = (RfPageGuardPreflightV1 *)authority_alloc0(
		(Size)request->target_count * sizeof(*preflight->page_preflights));
	preflight->target_guard_index = (uint8 *)authority_alloc0((Size)request->target_count);
	if (preflight->targets == NULL || preflight->page_preflights == NULL
		|| preflight->target_guard_index == NULL) {
		authority_preflight_free(preflight);
		return RF_PAGE_AUTHORITY_OOM;
	}
	preflight->magic = RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC;
	preflight->state = RF_PAGE_AUTHORITY_STATE_PREFLIGHTED;
	preflight->target_count = request->target_count;
	preflight->formation = request->formation;
	preflight->fence_need_set = request->fence_need_set;
	preflight->fence_admission_set = request->fence_admission_set;
	preflight->retention_pin = request->retention_pin;
	preflight->duties = request->duties;
	preflight->root_tokens = request->root_tokens;
	preflight->participant_count = request->participant_count;
	for (i = 0; i < request->target_count; i++) {
		preflight->targets[i] = request->targets[i];
		if (!rf_page_guard_preflight_v1(&request->targets[i].page_identity,
										&preflight->page_preflights[i])) {
			authority_preflight_free(preflight);
			return RF_PAGE_AUTHORITY_INTERNAL;
		}
	}
	if (!fence_current(preflight)) {
		authority_preflight_free(preflight);
		return RF_PAGE_AUTHORITY_FENCE_STALE;
	}
	if (!stable_proofs_current(preflight)) {
		authority_preflight_free(preflight);
		return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
	}
	*out_preflight = preflight;
	return RF_PAGE_AUTHORITY_OK;
}

static void
release_page_guards(RfPageAuthorityGuardV1 *guard)
{
	uint32 i;

	for (i = guard->guard_count; i > 0; i--)
		rf_page_guard_release_v1(&guard->page_guards[i - 1]);
	guard->guard_count = 0;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_promote_nowait_v1(RfPageAuthorityPreflightV1 *preflight,
										  ClusterRecoverySerialGuard *serial_guard,
										  RfPageAuthorityGuardV1 **out_guard)
{
	RfPageAuthorityGuardV1 *guard;
	RfPageAuthorityVerdictV1 verdict;
	int16 partition_guard_index[RF_PAGE_GUARD_PARTITIONS];
	uint32 i;

	if (out_guard == NULL)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	*out_guard = NULL;
	if (preflight == NULL || preflight->magic != RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC
		|| preflight->state != RF_PAGE_AUTHORITY_STATE_PREFLIGHTED)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	verdict = owners_current(preflight, serial_guard);
	if (verdict != RF_PAGE_AUTHORITY_OK)
		return verdict;
	guard = &preflight->guard_storage;
	memset(guard, 0, sizeof(*guard));
	guard->magic = RF_PAGE_AUTHORITY_GUARD_MAGIC;
	guard->preflight = preflight;
	guard->serial_guard = serial_guard;
	guard->target_guard_index = preflight->target_guard_index;
	memset(partition_guard_index, -1, sizeof(partition_guard_index));
	for (i = 0; i < preflight->target_count; i++) {
		uint32 partition = preflight->page_preflights[i].partition;
		int16 guard_index = partition_guard_index[partition];

		if (guard_index >= 0) {
			guard->target_guard_index[i] = (uint8)guard_index;
			continue;
		}
		if (!rf_page_guard_promote_nowait_v1(&preflight->page_preflights[i],
											 &guard->page_guards[guard->guard_count])) {
			release_page_guards(guard);
			memset(guard, 0, sizeof(*guard));
			return RF_PAGE_AUTHORITY_WOULD_BLOCK;
		}
		guard->target_guard_index[i] = (uint8)guard->guard_count;
		partition_guard_index[partition] = (int16)guard->guard_count;
		guard->guard_count++;
	}
	verdict = owners_current(preflight, serial_guard);
	if (verdict != RF_PAGE_AUTHORITY_OK) {
		release_page_guards(guard);
		memset(guard, 0, sizeof(*guard));
		return verdict;
	}
	guard->state = RF_PAGE_AUTHORITY_STATE_PROMOTED;
	preflight->state = RF_PAGE_AUTHORITY_STATE_PROMOTED;
	*out_guard = guard;
	return RF_PAGE_AUTHORITY_OK;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_revalidate_nowait_v1(const RfPageAuthorityGuardV1 *guard,
											 ClusterRecoverySerialGuard *serial_guard)
{
	RfPageAuthorityVerdictV1 verdict;
	uint32 i;

	if (guard == NULL || guard->magic != RF_PAGE_AUTHORITY_GUARD_MAGIC
		|| guard->state != RF_PAGE_AUTHORITY_STATE_PROMOTED || guard->serial_guard != serial_guard
		|| guard->preflight == NULL)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	verdict = owners_current(guard->preflight, serial_guard);
	if (verdict != RF_PAGE_AUTHORITY_OK)
		return verdict;
	for (i = 0; i < guard->preflight->target_count; i++) {
		uint8 guard_index = guard->target_guard_index[i];

		if (guard_index >= guard->guard_count
			|| !rf_page_guard_covers_nowait_v1(&guard->page_guards[guard_index],
											   &guard->preflight->targets[i].page_identity))
			return RF_PAGE_AUTHORITY_WOULD_BLOCK;
	}
	return RF_PAGE_AUTHORITY_OK;
}

bool
rf_page_authority_preflight_matches_target_v1(const RfPageAuthorityPreflightV1 *preflight,
											  const RfPageIdentityV1 *identity,
											  const uint8 incarnation[16])
{
	uint32 i;

	if (preflight == NULL || identity == NULL || incarnation == NULL
		|| preflight->magic != RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC
		|| preflight->state == RF_PAGE_AUTHORITY_STATE_RELEASED)
		return false;
	for (i = 0; i < preflight->target_count; i++)
		if (authority_identity_equal(&preflight->targets[i].page_identity, identity)
			&& memcmp(preflight->targets[i].expected_result.segment_incarnation, incarnation, 16)
				   == 0)
			return true;
	return false;
}

void
rf_page_authority_guard_release_v1(RfPageAuthorityGuardV1 **guard_pointer)
{
	RfPageAuthorityGuardV1 *guard;

	if (guard_pointer == NULL || *guard_pointer == NULL)
		return;
	guard = *guard_pointer;
	if (guard->magic != RF_PAGE_AUTHORITY_GUARD_MAGIC
		|| guard->state != RF_PAGE_AUTHORITY_STATE_PROMOTED)
		return;
	release_page_guards(guard);
	guard->state = RF_PAGE_AUTHORITY_STATE_RELEASED;
	if (guard->preflight != NULL)
		guard->preflight->state = RF_PAGE_AUTHORITY_STATE_RELEASED;
	*guard_pointer = NULL;
}

void
rf_page_authority_preflight_destroy_v1(RfPageAuthorityPreflightV1 **preflight_pointer)
{
	RfPageAuthorityPreflightV1 *preflight;

	if (preflight_pointer == NULL || *preflight_pointer == NULL)
		return;
	preflight = *preflight_pointer;
	if (preflight->magic != RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC
		|| preflight->state == RF_PAGE_AUTHORITY_STATE_PROMOTED)
		return;
	preflight->magic = 0;
	authority_preflight_free(preflight);
	*preflight_pointer = NULL;
}

static bool
adapter_validate_identity(void *arg, const RfPageIdentityV1 *identity, const uint8 incarnation[16])
{
	RfPageInstallAuthorityAdapterV1 *adapter = (RfPageInstallAuthorityAdapterV1 *)arg;

	if (adapter == NULL
		|| !rf_page_authority_preflight_matches_target_v1(adapter->preflight, identity,
														  incarnation))
		return false;
	if (adapter->guard != NULL)
		return rf_page_authority_batch_revalidate_nowait_v1(adapter->guard, adapter->serial_guard)
			   == RF_PAGE_AUTHORITY_OK;
	return owners_current(adapter->preflight, adapter->serial_guard) == RF_PAGE_AUTHORITY_OK;
}

static bool
adapter_promote(void *arg)
{
	RfPageInstallAuthorityAdapterV1 *adapter = (RfPageInstallAuthorityAdapterV1 *)arg;

	return adapter != NULL && adapter->guard == NULL
		   && rf_page_authority_batch_promote_nowait_v1(adapter->preflight, adapter->serial_guard,
														&adapter->guard)
				  == RF_PAGE_AUTHORITY_OK;
}

static bool
adapter_publish(void *arg)
{
	RfPageInstallAuthorityAdapterV1 *adapter = (RfPageInstallAuthorityAdapterV1 *)arg;

	if (adapter == NULL || adapter->proof_published
		|| rf_page_authority_batch_revalidate_nowait_v1(adapter->guard, adapter->serial_guard)
			   != RF_PAGE_AUTHORITY_OK)
		return false;
	adapter->proof_published = true;
	return true;
}

static bool
adapter_release(void *arg)
{
	RfPageInstallAuthorityAdapterV1 *adapter = (RfPageInstallAuthorityAdapterV1 *)arg;

	if (adapter == NULL || adapter->guard == NULL)
		return false;
	rf_page_authority_guard_release_v1(&adapter->guard);
	return adapter->guard == NULL;
}

bool
rf_page_install_authority_adapter_init_v1(RfPageAuthorityPreflightV1 *preflight,
										  ClusterRecoverySerialGuard *serial_guard,
										  RfPageInstallAuthorityAdapterV1 *adapter)
{
	if (adapter == NULL || preflight == NULL || serial_guard == NULL
		|| preflight->magic != RF_PAGE_AUTHORITY_PREFLIGHT_MAGIC
		|| preflight->state != RF_PAGE_AUTHORITY_STATE_PREFLIGHTED)
		return false;
	memset(adapter, 0, sizeof(*adapter));
	adapter->preflight = preflight;
	adapter->serial_guard = serial_guard;
	adapter->ops.arg = adapter;
	adapter->ops.validate_identity = adapter_validate_identity;
	adapter->ops.promote = adapter_promote;
	adapter->ops.publish = adapter_publish;
	adapter->ops.release = adapter_release;
	return true;
}
