/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin.c
 *    Root-daemon PFRJ rejoin operation core.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <time.h>

#include "pgrac_fenced_rejoin.h"

#define PGRAC_FENCED_REJOIN_FRESHNESS_NS UINT64_C(5000000000)

#define PGRAC_FENCED_DENY_BAD_ARGUMENT UINT32_C(1)
#define PGRAC_FENCED_DENY_STORAGE_IDENTITY UINT32_C(4)
#define PGRAC_FENCED_DENY_PROVIDER_REJECTED UINT32_C(8)
#define PGRAC_FENCED_DENY_PROVIDER_UNKNOWN UINT32_C(9)
#define PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE UINT32_C(10)
#define PGRAC_FENCED_DENY_TIMEOUT UINT32_C(11)
#define PGRAC_FENCED_DENY_JOURNAL UINT32_C(12)
#define PGRAC_FENCED_DENY_REJOIN_OFFER_MISMATCH UINT32_C(23)
#define PGRAC_FENCED_DENY_IO_NOT_DRAINED UINT32_C(30)

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	if (bytes == NULL)
		return false;
	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
monotonic_now_ns(uint64 *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64) now.tv_sec * UINT64_C(1000000000) +
		(uint64) now.tv_nsec;
	return *out != 0;
}

static bool
request_codec_valid(
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	uint16 expected_opcode)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	return request != NULL && request->opcode == expected_opcode &&
		pgrac_external_fence_rejoin_v1_encode(request, frame);
}

static void
record_base(const PgracFencedOperationContextV1 *context, uint16 kind,
			const uint8 operation_id[16], uint64 now,
			PgracFencedJournalRecordV1 *record)
{
	memset(record, 0, sizeof(*record));
	record->record_kind = kind;
	memcpy(record->daemon_boot_id, context->daemon_boot_id,
		   sizeof(record->daemon_boot_id));
	if (operation_id != NULL)
		memcpy(record->operation_id, operation_id,
			   sizeof(record->operation_id));
	record->provider_id = context->provider->provider_id;
	record->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	record->event_mono_ns = now;
	record->mapping_generation = context->config->mapping_generation;
	memcpy(record->semantic_config_digest, context->semantic_config_digest,
		   sizeof(record->semantic_config_digest));
}

static void
negative_response(
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const PgracFencedRejoinOperationV1 *operation,
	const uint8 fallback_operation_id[16], uint16 response_opcode,
	uint32 status, uint32 deny_reason,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	memset(response, 0, sizeof(*response));
	response->opcode = response_opcode;
	memcpy(response->transport_nonce, request->transport_nonce,
		   sizeof(response->transport_nonce));
	if (operation != NULL)
	{
		memcpy(response->operation_id, operation->operation_id,
			   sizeof(response->operation_id));
		response->old_node_id = operation->admin_request.old_node_id;
		response->old_incarnation =
			operation->admin_request.old_incarnation;
		response->candidate_incarnation =
			operation->admin_request.candidate_incarnation;
		if (response_opcode !=
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT)
		{
			response->system_identifier =
				operation->offer_result.system_identifier;
			memcpy(response->rejoin_gate_digest,
				request->rejoin_gate_digest,
				sizeof(response->rejoin_gate_digest));
			memcpy(response->protected_set_digest,
				operation->offer_result.protected_set_digest,
				sizeof(response->protected_set_digest));
		}
	}
	else if (fallback_operation_id != NULL)
		memcpy(response->operation_id, fallback_operation_id,
			   sizeof(response->operation_id));
	if (operation == NULL && response_opcode ==
		PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT)
	{
		response->old_node_id = request->old_node_id;
		response->old_incarnation = request->old_incarnation;
		response->candidate_incarnation = request->candidate_incarnation;
	}
	response->status = status;
	response->deny_reason = deny_reason;
}

static void
configured_target(const PgracFencedOperationContextV1 *context,
			  int32 node_id, PgracFencedTargetV1 *target)
{
	const PgracFencedNodeConfigV1 *node = &context->config->nodes[node_id];

	memset(target, 0, sizeof(*target));
	memcpy(target->target_uuid, node->target_uuid,
		   sizeof(target->target_uuid));
	target->victim_node_id = node_id;
	target->mapping_generation = context->config->mapping_generation;
	target->adapter_config = node->adapter_data;
	target->adapter_config_len = node->adapter_data_len;
}

static bool
resolve_exact(PgracFencedOperationContextV1 *context,
			  const PgracFencedTargetV1 *configured, uint64 deadline_mono_ns,
			  PgracFencedTargetV1 *resolved,
			  PgracFencedProviderResult *provider_result,
			  int32 *native_status)
{
	PgracFencedProviderWorkerResult worker_result;

	memset(resolved, 0, sizeof(*resolved));
	*provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	*native_status = 0;
	worker_result = pgrac_fenced_provider_worker_resolve(context->provider,
		context->allow_test_only, configured, deadline_mono_ns,
		provider_result, resolved, native_status);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
	{
		*provider_result = worker_result ==
			PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
		return false;
	}
	return *provider_result == PGRAC_FENCED_PROVIDER_OK &&
		memcmp(resolved->target_uuid, configured->target_uuid,
			   sizeof(resolved->target_uuid)) == 0 &&
		resolved->victim_node_id == configured->victim_node_id &&
		resolved->reserved0 == 0 &&
		resolved->mapping_generation == configured->mapping_generation;
}

static uint32
provider_deny(PgracFencedProviderResult provider_result,
			 bool deadline_expired)
{
	if (deadline_expired)
		return PGRAC_FENCED_DENY_TIMEOUT;
	if (provider_result == PGRAC_FENCED_PROVIDER_REJECTED)
		return PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	if (provider_result == PGRAC_FENCED_PROVIDER_UNAVAILABLE ||
		provider_result == PGRAC_FENCED_PROVIDER_CONFIG_ERROR)
		return PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
	return PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
}

static uint32
provider_status(PgracFencedProviderResult provider_result,
			bool deadline_expired)
{
	if (deadline_expired)
		return PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
	if (provider_result == PGRAC_FENCED_PROVIDER_REJECTED)
		return PGRAC_FENCED_REJOIN_STATUS_REJECTED;
	if (provider_result == PGRAC_FENCED_PROVIDER_UNAVAILABLE ||
		provider_result == PGRAC_FENCED_PROVIDER_CONFIG_ERROR)
		return PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
	return PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
}

static PgracFencedRejoinOperationV1 *
find_operation(PgracFencedRejoinContextV1 *context,
			   const uint8 operation_id[16])
{
	uint32 i;

	if (context == NULL || operation_id == NULL)
		return NULL;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (context->operations[i].state !=
			PGRAC_FENCED_REJOIN_OPERATION_UNUSED &&
			memcmp(context->operations[i].operation_id, operation_id,
				sizeof(context->operations[i].operation_id)) == 0)
			return &context->operations[i];
	}
	return NULL;
}

static PgracFencedRejoinOperationV1 *
allocate_operation(PgracFencedRejoinContextV1 *context)
{
	uint32 i;

	if (context->operation_count >= PGRAC_FENCED_REJOIN_MAX_OPERATIONS)
		return NULL;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (context->operations[i].state ==
			PGRAC_FENCED_REJOIN_OPERATION_UNUSED)
			return &context->operations[i];
	}
	return NULL;
}

static bool
same_admin_request(
	const PgracExternalFenceProtocolRejoinFrameV1 *left,
	const PgracExternalFenceProtocolRejoinFrameV1 *right)
{
	return left->opcode == right->opcode &&
		memcmp(left->transport_nonce, right->transport_nonce,
			   sizeof(left->transport_nonce)) == 0 &&
		left->old_node_id == right->old_node_id &&
		left->old_incarnation == right->old_incarnation &&
		left->candidate_incarnation == right->candidate_incarnation &&
		left->timeout_ms == right->timeout_ms;
}

static bool
build_rejoin_binding(const PgracFencedOperationContextV1 *context,
				 const PgracFencedRejoinOperationV1 *operation,
				 const uint8 rejoin_gate_digest[32],
				 PgracExternalFenceProtocolRejoinBindingV1 *binding,
				 uint8 digest[32])
{
	memset(binding, 0, sizeof(*binding));
	binding->system_identifier = context->config->system_identifier;
	if (rejoin_gate_digest != NULL)
		memcpy(binding->rejoin_gate_digest, rejoin_gate_digest,
			   sizeof(binding->rejoin_gate_digest));
	binding->old_node_id = operation->admin_request.old_node_id;
	binding->old_incarnation = operation->admin_request.old_incarnation;
	binding->candidate_incarnation =
		operation->admin_request.candidate_incarnation;
	binding->target_mapping_generation =
		context->config->mapping_generation;
	if (!pgrac_fenced_config_protected_set_digest(context->config,
			operation->admin_request.old_node_id, binding->protected_set_digest))
		return false;
	binding->predicate_id = 2;
	binding->predicate_version = 1;
	return pgrac_external_fence_rejoin_binding_digest_v1(binding, digest);
}

static bool
bound_request_matches(const PgracFencedRejoinOperationV1 *operation,
				  const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	return memcmp(request->operation_id, operation->operation_id,
			   sizeof(request->operation_id)) == 0 &&
		request->system_identifier == operation->offer_result.system_identifier &&
		bytes_nonzero(request->rejoin_gate_digest,
			   sizeof(request->rejoin_gate_digest)) &&
		memcmp(request->protected_set_digest,
			   operation->offer_result.protected_set_digest,
			   sizeof(request->protected_set_digest)) == 0 &&
		request->old_node_id == operation->admin_request.old_node_id &&
		request->old_incarnation ==
			operation->admin_request.old_incarnation &&
		request->candidate_incarnation ==
			operation->admin_request.candidate_incarnation;
}

bool
pgrac_fenced_rejoin_init(PgracFencedRejoinContextV1 *context,
				 PgracFencedOperationContextV1 *operation_context)
{
	if (context == NULL || operation_context == NULL ||
		!operation_context->available)
		return false;
	memset(context, 0, sizeof(*context));
	context->operation_context = operation_context;
	return true;
}

bool
pgrac_fenced_rejoin_admin_prepare(PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const uint8 operation_id[16], uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedOperationContextV1 *operation_context;
	PgracFencedRejoinOperationV1 *operation;
	PgracFencedJournalRecordV1 record;
	PgracFencedTargetV1 configured;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult provider_result;
	int32 native_status;
	uint64 now;
	uint32 i;
	bool resolved_ok;

	if (context == NULL || response == NULL ||
		!request_codec_valid(request,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) ||
		operation_id == NULL || !bytes_nonzero(operation_id, 16) ||
		deadline_mono_ns == 0 || context->operation_context == NULL)
		return false;
	operation_context = context->operation_context;
	if (!operation_context->available)
	{
		negative_response(request, NULL, operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, response);
		return true;
	}
	if (request->old_node_id < 0 ||
		request->old_node_id >= PGRAC_FENCED_MAX_NODES ||
		request->old_incarnation == 0 ||
		request->candidate_incarnation <= request->old_incarnation ||
		!operation_context->config->nodes[request->old_node_id].present)
	{
		negative_response(request, NULL, operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_STORAGE_IDENTITY, response);
		return true;
	}
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		operation = &context->operations[i];
		if (operation->state == PGRAC_FENCED_REJOIN_OPERATION_UNUSED ||
			operation->admin_request.old_node_id != request->old_node_id)
			continue;
		if (same_admin_request(&operation->admin_request, request))
		{
			negative_response(request, operation, NULL,
				PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
				PGRAC_FENCED_REJOIN_STATUS_OFFERED, 0, response);
			return true;
		}
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_REJECTED,
			PGRAC_FENCED_DENY_REJOIN_OFFER_MISMATCH, response);
		return true;
	}
	operation = allocate_operation(context);
	if (operation == NULL)
	{
		negative_response(request, NULL, operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE, response);
		return true;
	}
	configured_target(operation_context, request->old_node_id, &configured);
	provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	now = 0;
	resolved_ok = resolve_exact(operation_context, &configured,
		deadline_mono_ns, &resolved, &provider_result, &native_status);
	if (!monotonic_now_ns(&now))
		now = deadline_mono_ns;
	if (!resolved_ok || now >= deadline_mono_ns)
	{
		bool expired = now >= deadline_mono_ns;

		negative_response(request, NULL, operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			provider_status(provider_result, expired),
			provider_deny(provider_result, expired), response);
		return true;
	}
	memset(operation, 0, sizeof(*operation));
	operation->state = PGRAC_FENCED_REJOIN_OPERATION_OFFERED;
	operation->admin_request = *request;
	memcpy(operation->operation_id, operation_id,
		   sizeof(operation->operation_id));
	operation->target = resolved;
	record_base(operation_context,
		PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED,
		request->transport_nonce, now, &record);
	record.provider_result = PGRAC_FENCED_PROVIDER_OK;
	if (!pgrac_fenced_operation_append_journal(operation_context, &record))
	{
		memset(operation, 0, sizeof(*operation));
		negative_response(request, NULL, operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, response);
		return true;
	}
	context->operation_count++;
	negative_response(request, operation, NULL,
		PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT,
		PGRAC_FENCED_REJOIN_STATUS_OFFERED, 0, response);
	return true;
}

static PgracFencedRejoinOperationV1 *
claim_candidate(PgracFencedRejoinContextV1 *context)
{
	PgracFencedRejoinOperationV1 *best = NULL;
	PgracFencedRejoinOperationV1 *operation;
	int compare;
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		operation = &context->operations[i];
		if (operation->state != PGRAC_FENCED_REJOIN_OPERATION_OFFERED)
			continue;
		if (best == NULL || operation->admin_request.old_node_id <
			best->admin_request.old_node_id)
		{
			best = operation;
			continue;
		}
		if (operation->admin_request.old_node_id !=
			best->admin_request.old_node_id)
			continue;
		compare = memcmp(operation->operation_id, best->operation_id,
			sizeof(operation->operation_id));
		if (compare < 0)
			best = operation;
	}
	return best;
}

static bool
append_readback_result(PgracFencedOperationContextV1 *context,
				   const uint8 operation_id[16],
				   const uint8 binding_digest[32],
				   PgracFencedProviderResult provider_result,
				   const PgracFencedReadbackV1 *readback, uint32 deny_reason,
				   uint64 now, uint64 proof_generation,
				   uint64 fresh_until, const uint8 target_digest[32],
				   PgracFencedJournalRecordV1 *record)
{
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT,
		operation_id, now, record);
	memcpy(record->binding_digest, binding_digest,
		   sizeof(record->binding_digest));
	record->provider_result = (uint32) provider_result;
	record->provider_native_status = readback == NULL ? 0 :
		readback->native_status;
	record->deny_reason = deny_reason;
	if (readback != NULL)
	{
		record->target_state = readback->state;
		record->io_drain_state = readback->io_drain_state;
	}
	if (proof_generation != 0)
	{
		record->fresh_until_mono_ns = fresh_until;
		record->proof_generation = proof_generation;
		memcpy(record->target_state_digest, target_digest,
			   sizeof(record->target_state_digest));
	}
	return pgrac_fenced_operation_append_journal(context, record);
}

bool
pgrac_fenced_rejoin_claim(PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedOperationContextV1 *operation_context;
	PgracFencedRejoinOperationV1 *operation;
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	PgracFencedJournalRecordV1 record;
	PgracFencedReadbackV1 readback;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult provider_result;
	PgracFencedProviderWorkerResult worker_result;
	PgracFencedProviderTerminal terminal;
	uint8 binding_digest[32];
	uint8 target_digest[32] = { 0 };
	uint8 zero_operation_id[16];
	uint64 now = 0;
	uint64 proof_generation = 0;
	uint64 fresh_until = 0;
	uint32 deny_reason;
	uint32 status;
	int32 native_status;
	bool deadline_expired;

	memset(zero_operation_id, 0x01, sizeof(zero_operation_id));
	if (context == NULL || response == NULL || deadline_mono_ns == 0 ||
		!request_codec_valid(request,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT) ||
		context->operation_context == NULL)
		return false;
	operation_context = context->operation_context;
	operation = claim_candidate(context);
	if (operation == NULL)
	{
		negative_response(request, NULL, zero_operation_id,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE, response);
		return true;
	}
	operation->state = PGRAC_FENCED_REJOIN_OPERATION_CLAIMED;
	provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (!build_rejoin_binding(operation_context, operation, NULL,
			&binding, binding_digest) ||
		!resolve_exact(operation_context, &operation->target, deadline_mono_ns,
			&resolved, &provider_result, &native_status))
	{
		(void) monotonic_now_ns(&now);
		deadline_expired = now == 0 || now >= deadline_mono_ns;
		operation->state = PGRAC_FENCED_REJOIN_OPERATION_OFFERED;
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER,
			provider_status(provider_result, deadline_expired),
			provider_deny(provider_result, deadline_expired), response);
		return true;
	}
	operation->target = resolved;
	memset(&readback, 0, sizeof(readback));
	worker_result = pgrac_fenced_provider_worker_readback_retry(
		operation_context->provider, operation_context->allow_test_only,
		&operation->target, deadline_mono_ns, &provider_result, &readback);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result ==
			PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
		now = deadline_mono_ns;
	deadline_expired = now >= deadline_mono_ns;
	terminal = pgrac_fenced_provider_classify_recovery(provider_result,
		operation->target.target_uuid, &readback);
	if (deadline_expired)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_OFFERED;
		deny_reason = 0;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_REJECTED;
		deny_reason = readback.state == PGRAC_FENCED_TARGET_OFF &&
			readback.io_drain_state == PGRAC_FENCED_IO_DRAIN_NOT_DRAINED ?
			PGRAC_FENCED_DENY_IO_NOT_DRAINED :
			PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	}
	else
	{
		status = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE :
			PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE :
			PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
	}
	if (status == PGRAC_FENCED_REJOIN_STATUS_OFFERED)
	{
		if (!pgrac_fenced_operation_reserve_proof_generation(
				operation_context, &proof_generation) ||
			UINT64_MAX - now < PGRAC_FENCED_REJOIN_FRESHNESS_NS)
		{
			status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
			deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
			proof_generation = 0;
		}
		else
		{
			fresh_until = now + PGRAC_FENCED_REJOIN_FRESHNESS_NS;
			if (!pgrac_external_fence_target_state_digest_v1(
					operation->target.target_uuid, PGRAC_FENCED_TARGET_OFF,
					PGRAC_FENCED_IO_DRAIN_DRAINED,
					operation_context->config->mapping_generation,
					proof_generation, target_digest))
			{
				status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
				deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
				proof_generation = 0;
			}
		}
	}
	if (!append_readback_result(operation_context,
			request->transport_nonce, binding_digest, provider_result,
			worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK ? &readback : NULL,
			deny_reason, now, proof_generation, fresh_until, target_digest,
			&record))
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
		deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		proof_generation = 0;
	}
	if (status != PGRAC_FENCED_REJOIN_STATUS_OFFERED)
	{
		operation->state = PGRAC_FENCED_REJOIN_OPERATION_OFFERED;
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER, status, deny_reason,
			response);
		return true;
	}
	memset(response, 0, sizeof(*response));
	response->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER;
	memcpy(response->transport_nonce, request->transport_nonce,
		   sizeof(response->transport_nonce));
	memcpy(response->operation_id, operation->operation_id,
		   sizeof(response->operation_id));
	response->system_identifier = binding.system_identifier;
	memcpy(response->protected_set_digest, binding.protected_set_digest,
		   sizeof(response->protected_set_digest));
	response->old_node_id = binding.old_node_id;
	response->old_incarnation = binding.old_incarnation;
	response->candidate_incarnation = binding.candidate_incarnation;
	response->provider_id = operation_context->provider->provider_id;
	response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	response->target_mapping_generation = binding.target_mapping_generation;
	memcpy(response->daemon_boot_id, operation_context->daemon_boot_id,
		   sizeof(response->daemon_boot_id));
	response->journal_seq = record.seq;
	response->verified_mono_ns = now;
	response->fresh_until_mono_ns = fresh_until;
	response->proof_generation = proof_generation;
	memcpy(response->target_state_digest, target_digest,
		   sizeof(response->target_state_digest));
	response->status = PGRAC_FENCED_REJOIN_STATUS_OFFERED;
	operation->offer_result = *response;
	return true;
}

static bool
append_actuation_record(PgracFencedOperationContextV1 *context,
					uint16 kind, const uint8 operation_id[16],
					const uint8 binding_digest[32],
					PgracFencedProviderResult result, int32 native_status,
					uint64 now, PgracFencedJournalRecordV1 *record)
{
	record_base(context, kind, operation_id, now, record);
	memcpy(record->binding_digest, binding_digest,
		   sizeof(record->binding_digest));
	record->provider_result = (uint32) result;
	record->provider_native_status = native_status;
	return pgrac_fenced_operation_append_journal(context, record);
}

static bool
append_reenable_result(PgracFencedOperationContextV1 *context,
				   const uint8 operation_id[16],
				   const uint8 binding_digest[32],
				   PgracFencedProviderResult provider_result,
				   const PgracFencedReadbackV1 *readback, uint32 deny_reason,
				   uint64 now, uint64 fresh_until, uint64 proof_generation,
				   const uint8 target_digest[32],
				   PgracFencedJournalRecordV1 *record)
{
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT,
		operation_id, now, record);
	memcpy(record->binding_digest, binding_digest,
		   sizeof(record->binding_digest));
	record->provider_result = (uint32) provider_result;
	record->provider_native_status = readback == NULL ? 0 :
		readback->native_status;
	record->deny_reason = deny_reason;
	if (readback != NULL)
	{
		record->target_state = readback->state;
		record->io_drain_state = readback->io_drain_state;
	}
	if (proof_generation != 0)
	{
		record->fresh_until_mono_ns = fresh_until;
		record->proof_generation = proof_generation;
		memcpy(record->target_state_digest, target_digest,
			   sizeof(record->target_state_digest));
	}
	return pgrac_fenced_operation_append_journal(context, record);
}

static bool
build_positive_result(PgracFencedOperationContextV1 *context,
				  PgracFencedRejoinOperationV1 *operation,
				  const PgracExternalFenceProtocolRejoinFrameV1 *request,
				  uint16 opcode, uint32 status, uint64 journal_seq,
				  uint64 verified, uint64 fresh_until,
				  uint64 proof_generation, const uint8 target_digest[32],
				  PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	memset(response, 0, sizeof(*response));
	response->opcode = opcode;
	memcpy(response->transport_nonce, request->transport_nonce,
		   sizeof(response->transport_nonce));
	memcpy(response->operation_id, operation->operation_id,
		   sizeof(response->operation_id));
	response->system_identifier = request->system_identifier;
	memcpy(response->rejoin_gate_digest, request->rejoin_gate_digest,
		   sizeof(response->rejoin_gate_digest));
	memcpy(response->protected_set_digest, request->protected_set_digest,
		   sizeof(response->protected_set_digest));
	response->old_node_id = request->old_node_id;
	response->old_incarnation = request->old_incarnation;
	response->candidate_incarnation = request->candidate_incarnation;
	response->provider_id = context->provider->provider_id;
	response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	response->target_mapping_generation = context->config->mapping_generation;
	memcpy(response->daemon_boot_id, context->daemon_boot_id,
		   sizeof(response->daemon_boot_id));
	response->journal_seq = journal_seq;
	response->verified_mono_ns = verified;
	response->fresh_until_mono_ns = fresh_until;
	response->proof_generation = proof_generation;
	memcpy(response->target_state_digest, target_digest,
		   sizeof(response->target_state_digest));
	response->status = status;
	return true;
}

bool
pgrac_fenced_rejoin_authorize_on(PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	bool target_admissions_invalidated, uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedOperationContextV1 *operation_context;
	PgracFencedRejoinOperationV1 *operation;
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	PgracFencedJournalRecordV1 record;
	PgracFencedReadbackV1 readback;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult provider_result;
	PgracFencedProviderWorkerResult worker_result;
	PgracFencedProviderTerminal terminal;
	uint8 binding_digest[32];
	uint8 target_digest[32] = { 0 };
	uint64 now;
	uint64 fresh_until;
	uint64 proof_generation;
	uint32 deny_reason;
	uint32 status;
	int32 native_status;

	if (context == NULL || response == NULL || deadline_mono_ns == 0 ||
		!target_admissions_invalidated ||
		!request_codec_valid(request,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON))
		return false;
	operation = find_operation(context, request->operation_id);
	if (operation == NULL || operation->state !=
		PGRAC_FENCED_REJOIN_OPERATION_CLAIMED ||
		!bound_request_matches(operation, request))
		return false;
	operation_context = context->operation_context;
	provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	now = 0;
	if (!build_rejoin_binding(operation_context, operation,
			request->rejoin_gate_digest, &binding, binding_digest) ||
		memcmp(binding.protected_set_digest,
			request->protected_set_digest,
			sizeof(binding.protected_set_digest)) != 0 ||
		!resolve_exact(operation_context, &operation->target, deadline_mono_ns,
			&resolved, &provider_result, &native_status) ||
		!monotonic_now_ns(&now) || now >= deadline_mono_ns)
	{
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT,
			provider_status(provider_result, now >= deadline_mono_ns),
			provider_deny(provider_result, now >= deadline_mono_ns), response);
		memset(operation, 0, sizeof(*operation));
		context->operation_count--;
		return true;
	}
	operation->target = resolved;
	if (!append_actuation_record(operation_context,
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED,
			request->transport_nonce, binding_digest,
			PGRAC_FENCED_PROVIDER_PENDING, 0, now, &record))
	{
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, response);
		return true;
	}
	worker_result = pgrac_fenced_provider_worker_actuate(
		operation_context->provider, operation_context->allow_test_only, true,
		&operation->target, deadline_mono_ns, &provider_result, &native_status);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result ==
			PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
		now = deadline_mono_ns;
	if (!append_actuation_record(operation_context,
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT,
			request->transport_nonce, binding_digest, provider_result,
			native_status, now, &record))
	{
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT,
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, response);
		return true;
	}
	memset(&readback, 0, sizeof(readback));
	worker_result = pgrac_fenced_provider_worker_readback_retry(
		operation_context->provider, operation_context->allow_test_only,
		&operation->target, deadline_mono_ns, &provider_result, &readback);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result ==
			PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
		now = deadline_mono_ns;
	terminal = pgrac_fenced_provider_classify_rejoin_on(provider_result,
		operation->target.target_uuid, &readback);
	if (now >= deadline_mono_ns)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER;
		deny_reason = 0;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_REJECTED;
		deny_reason = PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	}
	else
	{
		status = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE :
			PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE :
			PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
	}
	proof_generation = 0;
	fresh_until = 0;
	if (status == PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER &&
		pgrac_fenced_operation_reserve_proof_generation(operation_context,
			&proof_generation) &&
		UINT64_MAX - now >= PGRAC_FENCED_REJOIN_FRESHNESS_NS)
	{
		fresh_until = now + PGRAC_FENCED_REJOIN_FRESHNESS_NS;
		if (!pgrac_external_fence_target_state_digest_v1(
				operation->target.target_uuid, PGRAC_FENCED_TARGET_ON,
				PGRAC_FENCED_IO_DRAIN_DRAINED,
				operation_context->config->mapping_generation,
				proof_generation, target_digest))
			proof_generation = 0;
	}
	if (status == PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER &&
		proof_generation == 0)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
		deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
	}
	if (!append_reenable_result(operation_context, request->transport_nonce,
			binding_digest, provider_result,
			worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK ? &readback : NULL,
			deny_reason, now, fresh_until, proof_generation, target_digest,
			&record))
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
		deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		proof_generation = 0;
	}
	if (status != PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER)
	{
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT, status, deny_reason,
			response);
		memset(operation, 0, sizeof(*operation));
		context->operation_count--;
		return true;
	}
	(void) build_positive_result(operation_context, operation, request,
		PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT,
		PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER, record.seq, now,
		fresh_until, proof_generation, target_digest, response);
	operation->on_result = *response;
	operation->state = PGRAC_FENCED_REJOIN_OPERATION_WAITING_JOINER;
	return true;
}

bool
pgrac_fenced_rejoin_refresh_on(PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedOperationContextV1 *operation_context;
	PgracFencedRejoinOperationV1 *operation;
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	PgracFencedJournalRecordV1 record;
	PgracFencedReadbackV1 readback;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult provider_result;
	PgracFencedProviderWorkerResult worker_result;
	PgracFencedProviderTerminal terminal;
	uint8 binding_digest[32];
	uint8 target_digest[32] = { 0 };
	uint64 now;
	uint64 fresh_until;
	uint64 proof_generation;
	uint32 deny_reason;
	uint32 status;
	int32 native_status;

	if (context == NULL || response == NULL || deadline_mono_ns == 0 ||
		!request_codec_valid(request,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON))
		return false;
	operation = find_operation(context, request->operation_id);
	if (operation == NULL ||
		(operation->state != PGRAC_FENCED_REJOIN_OPERATION_WAITING_JOINER &&
		 operation->state != PGRAC_FENCED_REJOIN_OPERATION_READY) ||
		!bound_request_matches(operation, request) ||
		memcmp(request->rejoin_gate_digest,
			operation->on_result.rejoin_gate_digest,
			sizeof(request->rejoin_gate_digest)) != 0)
		return false;
	operation_context = context->operation_context;
	provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (!build_rejoin_binding(operation_context, operation,
			request->rejoin_gate_digest, &binding, binding_digest))
		return false;
	if (!resolve_exact(operation_context, &operation->target,
			deadline_mono_ns, &resolved, &provider_result, &native_status))
	{
		if (!monotonic_now_ns(&now))
			now = deadline_mono_ns;
		status = provider_status(provider_result, now >= deadline_mono_ns);
		deny_reason = provider_deny(provider_result,
			now >= deadline_mono_ns);
		if (!append_reenable_result(operation_context,
				request->transport_nonce, binding_digest, provider_result, NULL,
				deny_reason, now, 0, 0, target_digest, &record))
		{
			status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
			deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		}
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT, status,
			deny_reason, response);
		memset(operation, 0, sizeof(*operation));
		context->operation_count--;
		return true;
	}
	operation->target = resolved;
	memset(&readback, 0, sizeof(readback));
	worker_result = pgrac_fenced_provider_worker_readback_retry(
		operation_context->provider, operation_context->allow_test_only,
		&operation->target, deadline_mono_ns, &provider_result, &readback);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result ==
			PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
		now = deadline_mono_ns;
	terminal = pgrac_fenced_provider_classify_rejoin_on(provider_result,
		operation->target.target_uuid, &readback);
	if (now >= deadline_mono_ns)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_READY;
		deny_reason = 0;
	}
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_REJECTED;
		deny_reason = PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	}
	else
	{
		status = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE :
			PGRAC_FENCED_REJOIN_STATUS_UNKNOWN;
		deny_reason = terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE ?
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE :
			PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
	}
	proof_generation = 0;
	fresh_until = 0;
	if (status == PGRAC_FENCED_REJOIN_STATUS_READY &&
		pgrac_fenced_operation_reserve_proof_generation(operation_context,
			&proof_generation) &&
		UINT64_MAX - now >= PGRAC_FENCED_REJOIN_FRESHNESS_NS)
	{
		fresh_until = now + PGRAC_FENCED_REJOIN_FRESHNESS_NS;
		if (!pgrac_external_fence_target_state_digest_v1(
				operation->target.target_uuid, PGRAC_FENCED_TARGET_ON,
				PGRAC_FENCED_IO_DRAIN_DRAINED,
				operation_context->config->mapping_generation,
				proof_generation, target_digest))
			proof_generation = 0;
	}
	if (status == PGRAC_FENCED_REJOIN_STATUS_READY && proof_generation == 0)
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
		deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
	}
	if (!append_reenable_result(operation_context, request->transport_nonce,
			binding_digest, provider_result,
			worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK ? &readback : NULL,
			deny_reason, now, fresh_until, proof_generation, target_digest,
			&record))
	{
		status = PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE;
		deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		proof_generation = 0;
	}
	if (status != PGRAC_FENCED_REJOIN_STATUS_READY)
	{
		negative_response(request, operation, NULL,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT, status,
			deny_reason, response);
		memset(operation, 0, sizeof(*operation));
		context->operation_count--;
		return true;
	}
	(void) build_positive_result(operation_context, operation, request,
		PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT,
		PGRAC_FENCED_REJOIN_STATUS_READY, record.seq, now, fresh_until,
		proof_generation, target_digest, response);
	operation->ready_result = *response;
	operation->state = PGRAC_FENCED_REJOIN_OPERATION_READY;
	return true;
}

bool
pgrac_fenced_rejoin_cancel(PgracFencedRejoinContextV1 *context,
			   const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	PgracFencedRejoinOperationV1 *operation;

	if (context == NULL || !request_codec_valid(request,
			PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL))
		return false;
	operation = find_operation(context, request->operation_id);
	if (operation == NULL)
		return false;
	memset(operation, 0, sizeof(*operation));
	context->operation_count--;
	return true;
}

const PgracFencedTargetV1 *
pgrac_fenced_rejoin_target(const PgracFencedRejoinContextV1 *context,
				   const uint8 operation_id[16])
{
	uint32 i;

	if (context == NULL || operation_id == NULL)
		return NULL;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (context->operations[i].state !=
			PGRAC_FENCED_REJOIN_OPERATION_UNUSED &&
			memcmp(context->operations[i].operation_id, operation_id,
				sizeof(context->operations[i].operation_id)) == 0)
			return &context->operations[i].target;
	}
	return NULL;
}

const PgracFencedTargetV1 *
pgrac_fenced_rejoin_claim_target(
	const PgracFencedRejoinContextV1 *context)
{
	const PgracFencedRejoinOperationV1 *best = NULL;
	const PgracFencedRejoinOperationV1 *operation;
	int compare;
	uint32 i;

	if (context == NULL)
		return NULL;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		operation = &context->operations[i];
		if (operation->state != PGRAC_FENCED_REJOIN_OPERATION_OFFERED)
			continue;
		if (best == NULL || operation->admin_request.old_node_id <
			best->admin_request.old_node_id)
		{
			best = operation;
			continue;
		}
		if (operation->admin_request.old_node_id !=
			best->admin_request.old_node_id)
			continue;
		compare = memcmp(operation->operation_id, best->operation_id,
			sizeof(operation->operation_id));
		if (compare < 0)
			best = operation;
	}
	return best == NULL ? NULL : &best->target;
}

const PgracFencedTargetV1 *
pgrac_fenced_rejoin_request_target(
	const PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	const PgracFencedRejoinOperationV1 *operation;
	uint32 i;

	if (context == NULL || request == NULL ||
		(request->opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON &&
		 request->opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON) ||
		!request_codec_valid(request, request->opcode))
		return NULL;
	operation = NULL;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (context->operations[i].state !=
				PGRAC_FENCED_REJOIN_OPERATION_UNUSED &&
			memcmp(context->operations[i].operation_id,
				request->operation_id, sizeof(request->operation_id)) == 0)
		{
			operation = &context->operations[i];
			break;
		}
	}
	if (operation == NULL || !bound_request_matches(operation, request))
		return NULL;
	if (request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON)
		return operation->state == PGRAC_FENCED_REJOIN_OPERATION_CLAIMED ?
			&operation->target : NULL;
	if ((operation->state != PGRAC_FENCED_REJOIN_OPERATION_WAITING_JOINER &&
		 operation->state != PGRAC_FENCED_REJOIN_OPERATION_READY) ||
		memcmp(request->rejoin_gate_digest,
			operation->on_result.rejoin_gate_digest,
			sizeof(request->rejoin_gate_digest)) != 0)
		return NULL;
	return &operation->target;
}
