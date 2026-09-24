/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_operation.c
 *    Provider-neutral scalar ACQUIRE operation core.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <time.h>

#include "pgrac_fenced_operation.h"

#define PGRAC_FENCED_VERDICT_WRITE_EXCLUDED UINT32_C(1)
#define PGRAC_FENCED_VERDICT_REJECTED UINT32_C(2)
#define PGRAC_FENCED_VERDICT_UNKNOWN UINT32_C(3)
#define PGRAC_FENCED_VERDICT_UNAVAILABLE UINT32_C(4)

#define PGRAC_FENCED_DENY_BAD_ARGUMENT UINT32_C(1)
#define PGRAC_FENCED_DENY_STORAGE_IDENTITY UINT32_C(4)
#define PGRAC_FENCED_DENY_PROVIDER_REJECTED UINT32_C(8)
#define PGRAC_FENCED_DENY_PROVIDER_UNKNOWN UINT32_C(9)
#define PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE UINT32_C(10)
#define PGRAC_FENCED_DENY_TIMEOUT UINT32_C(11)
#define PGRAC_FENCED_DENY_JOURNAL UINT32_C(12)
#define PGRAC_FENCED_DENY_EXPIRED UINT32_C(14)
#define PGRAC_FENCED_DENY_CONNECTION_CLOSED UINT32_C(16)
#define PGRAC_FENCED_DENY_MAPPING_CHANGED UINT32_C(17)
#define PGRAC_FENCED_DENY_REJOIN_INVALIDATED UINT32_C(19)
#define PGRAC_FENCED_DENY_IO_NOT_DRAINED UINT32_C(30)

#define PGRAC_FENCED_PROOF_FRESHNESS_NS UINT64_C(5000000000)

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
restart_preflight_is_exact_on(PgracFencedProviderResult provider_result,
						  const uint8 expected_target_uuid[16],
						  const PgracFencedReadbackV1 *readback)
{
	return provider_result == PGRAC_FENCED_PROVIDER_OK &&
		readback != NULL && readback->reserved0 == 0 &&
		readback->state == PGRAC_FENCED_TARGET_ON &&
		readback->io_drain_state <= PGRAC_FENCED_IO_DRAIN_NOT_DRAINED &&
		memcmp(readback->observed_target_uuid, expected_target_uuid,
			sizeof(readback->observed_target_uuid)) == 0;
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
config_basic_valid(const PgracFencedConfigV1 *config)
{
	return config != NULL &&
		((config->format_version == 1 &&
		  config->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1) ||
		 (config->format_version == 2 && config->storage_backend_id == 3 &&
		  config->provider_id == PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1)) &&
		config->mapping_generation != 0 && config->system_identifier != 0 &&
		(config->storage_backend_id == 2 || config->storage_backend_id == 3) &&
		bytes_nonzero(config->storage_uuid, sizeof(config->storage_uuid)) &&
		config->provider_id != PGRAC_FENCED_PROVIDER_ID_UNAVAILABLE &&
		config->provider_id != UINT16_MAX &&
		config->provider_abi == PGRAC_FENCED_PROVIDER_ABI_V1 &&
		config->node_count > 0 && config->node_count <= PGRAC_FENCED_MAX_NODES;
}

static void
record_base(const PgracFencedOperationContextV1 *context, uint16 kind,
			const uint8 operation_id[16], uint64 event_mono_ns,
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
	record->event_mono_ns = event_mono_ns;
	record->mapping_generation = context->config->mapping_generation;
	memcpy(record->semantic_config_digest, context->semantic_config_digest,
		sizeof(record->semantic_config_digest));
}

static bool
append_record(PgracFencedOperationContextV1 *context,
			  PgracFencedJournalRecordV1 *record)
{
	PgracFencedJournalAppendResult result;
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];

	if (!context->available)
		return false;
	if (context->journal_append_hook != NULL)
	{
		if (!context->journal_append_hook(context->journal_append_argument,
				record))
		{
			context->available = false;
			return false;
		}
		return true;
	}
	result = pgrac_fenced_journal_append_fd(context->journal_fd,
		context->journal_state, record);
	if (result == PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED)
	{
		if (context->journal_directory_fd < 0 ||
			context->sealed_count >= PGRAC_FENCED_JOURNAL_MAX_SEALED ||
			!pgrac_fenced_journal_rotate_at(context->journal_directory_fd,
				&context->journal_fd, context->sealed_count,
				context->journal_state, sealed_name, sizeof(sealed_name)))
			result = PGRAC_FENCED_JOURNAL_APPEND_UNAVAILABLE;
		else
		{
			context->sealed_count++;
			result = pgrac_fenced_journal_append_fd(context->journal_fd,
				context->journal_state, record);
		}
	}
	if (result != PGRAC_FENCED_JOURNAL_APPEND_OK)
	{
		context->available = false;
		return false;
	}
	return true;
}

static void
negative_response(const PgracFencedOperationContextV1 *context,
				  const PgracExternalFenceProtocolRequestV1 *request,
				  uint32 verdict, uint32 deny_reason,
				  PgracFencedProviderResult provider_result,
				  int32 native_status, uint64 journal_seq,
				  PgracExternalFenceProtocolResponseV1 *response)
{
	memset(response, 0, sizeof(*response));
	if (request != NULL)
		memcpy(response->request_nonce, request->request_nonce,
			sizeof(response->request_nonce));
	response->verdict = verdict;
	response->journal_seq = journal_seq;
	response->provider_result = (uint32) provider_result;
	response->provider_native_status = native_status;
	response->deny_reason = deny_reason;
	if (context != NULL && context->provider != NULL)
	{
		response->provider_id = context->provider->provider_id;
		response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
		memcpy(response->daemon_boot_id, context->daemon_boot_id,
			sizeof(response->daemon_boot_id));
	}
}

static bool
append_provider_record(PgracFencedOperationContextV1 *context,
				   uint16 kind,
				   const PgracExternalFenceProtocolRequestV1 *request,
				   const PgracFencedPreparedAcquireV1 *prepared,
				   const uint8 binding_digest[32],
				   PgracFencedProviderResult provider_result,
				   int32 native_status,
				   const PgracFencedReadbackV1 *readback,
				   uint32 deny_reason,
				   uint64 event_mono_ns,
				   PgracFencedJournalRecordV1 *record)
{
	record_base(context, kind, request->request_nonce, event_mono_ns, record);
	if (context->config->format_version == 2)
	{
		memcpy(record->operation_id, prepared->accepted_record.operation_id, 16);
		record->intent = prepared->accepted_record.intent;
	}
	if (binding_digest != NULL)
		memcpy(record->binding_digest, binding_digest,
			sizeof(record->binding_digest));
	record->provider_result = (uint32) provider_result;
	record->provider_native_status = native_status;
	record->deny_reason = deny_reason;
	if (readback != NULL)
	{
		record->target_state = readback->state;
		record->io_drain_state = readback->io_drain_state;
	}
	return append_record(context, record);
}

static void
provider_negative(const PgracFencedOperationContextV1 *context,
				  const PgracExternalFenceProtocolRequestV1 *request,
				  PgracFencedProviderResult provider_result,
				  int32 native_status, uint64 journal_seq,
				  PgracExternalFenceProtocolResponseV1 *response)
{
	uint32 verdict;
	uint32 reason;

	if (provider_result == PGRAC_FENCED_PROVIDER_REJECTED)
	{
		verdict = PGRAC_FENCED_VERDICT_REJECTED;
		reason = PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	}
	else if (provider_result == PGRAC_FENCED_PROVIDER_UNAVAILABLE ||
		provider_result == PGRAC_FENCED_PROVIDER_CONFIG_ERROR)
	{
		verdict = PGRAC_FENCED_VERDICT_UNAVAILABLE;
		reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
	}
	else
	{
		verdict = PGRAC_FENCED_VERDICT_UNKNOWN;
		reason = PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
	}
	negative_response(context, request, verdict, reason, provider_result,
		native_status, journal_seq, response);
}

bool
pgrac_fenced_operation_context_init(
	PgracFencedOperationContextV1 *context,
	const PgracFencedConfigV1 *config,
	const PgracFencedProviderOpsV1 *provider,
	bool allow_test_only,
	const uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES],
	const uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	int journal_fd,
	PgracFencedJournalScanState *journal_state)
{
	PgracFencedOperationContextV1 candidate;
	PgracFencedJournalRecordV1 record;
	uint64 now;

	if (context == NULL)
		return false;
	memset(context, 0, sizeof(*context));
	if (!config_basic_valid(config) || provider == NULL ||
		!pgrac_fenced_provider_ops_valid(provider, allow_test_only) ||
		provider->provider_id != config->provider_id ||
		provider->abi_version != config->provider_abi ||
		!bytes_nonzero(semantic_config_digest,
			PGRAC_FENCED_CONFIG_DIGEST_BYTES) ||
		!bytes_nonzero(daemon_boot_id,
			PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES) ||
		journal_fd < 0 || journal_state == NULL || !journal_state->available ||
		!monotonic_now_ns(&now))
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.config = config;
	candidate.provider = provider;
	candidate.allow_test_only = allow_test_only;
	memcpy(candidate.semantic_config_digest, semantic_config_digest,
		sizeof(candidate.semantic_config_digest));
	memcpy(candidate.daemon_boot_id, daemon_boot_id,
		sizeof(candidate.daemon_boot_id));
	candidate.journal_fd = journal_fd;
	candidate.journal_directory_fd = -1;
	candidate.sealed_count = 0;
	candidate.journal_state = journal_state;
	candidate.next_proof_generation = 1;
	candidate.available = true;
	record_base(&candidate, PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED, NULL,
		now, &record);
	record.provider_result = PGRAC_FENCED_PROVIDER_OK;
	if (!append_record(&candidate, &record))
		return false;
	*context = candidate;
	return true;
}

bool
pgrac_fenced_operation_enable_rotation(
	PgracFencedOperationContextV1 *context,
	int journal_directory_fd,
	uint32 sealed_count)
{
	struct stat st;

	if (context == NULL || !context->available || journal_directory_fd < 0 ||
		sealed_count > PGRAC_FENCED_JOURNAL_MAX_SEALED ||
		fstat(journal_directory_fd, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	context->journal_directory_fd = journal_directory_fd;
	context->sealed_count = sealed_count;
	return true;
}

bool
pgrac_fenced_operation_set_journal_append_hook(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalAppendHookV1 hook,
	void *argument)
{
	if (context == NULL || !context->available || hook == NULL ||
		argument == NULL || context->journal_append_hook != NULL)
		return false;
	context->journal_append_hook = hook;
	context->journal_append_argument = argument;
	return true;
}

bool
pgrac_fenced_operation_append_journal(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalRecordV1 *record)
{
	return context != NULL && record != NULL && append_record(context, record);
}

bool
pgrac_fenced_operation_set_proof_reserve_hook(
	PgracFencedOperationContextV1 *context,
	PgracFencedProofReserveHookV1 hook,
	void *argument)
{
	if (context == NULL || !context->available || hook == NULL ||
		argument == NULL || context->proof_reserve_hook != NULL)
		return false;
	context->proof_reserve_hook = hook;
	context->proof_reserve_argument = argument;
	return true;
}

bool
pgrac_fenced_operation_reserve_proof_generation(
	PgracFencedOperationContextV1 *context,
	uint64 *proof_generation)
{
	if (context == NULL || proof_generation == NULL || !context->available ||
		context->next_proof_generation == 0)
		return false;
	if (context->proof_reserve_hook != NULL)
	{
		if (!context->proof_reserve_hook(context->proof_reserve_argument,
				proof_generation) || *proof_generation == 0)
		{
			context->available = false;
			return false;
		}
		return true;
	}
	*proof_generation = context->next_proof_generation;
	context->next_proof_generation = *proof_generation == UINT64_MAX ? 0 :
		*proof_generation + 1;
	return true;
}

/* PGRAC: never reinterpret an old operation through a replacement mapping. */
static bool
pending_intent_matches(const PgracFencedOperationContextV1 *context,
					   const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalIntentV2 *intent = &record->intent;
	uint8 digest[32];
	int32 node;

	if (!pgrac_fenced_journal_intent_continues(NULL, record) ||
		record->provider_id != context->provider->provider_id ||
		record->provider_abi_version != context->provider->abi_version ||
		record->mapping_generation != context->config->mapping_generation ||
		intent->system_identifier != context->config->system_identifier ||
		memcmp(record->semantic_config_digest, context->semantic_config_digest, 32) != 0)
		return false;
	node = intent->kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE ?
		intent->request.acquire.need.victim_node_id : intent->request.rejoin.old_node_id;
	return node >= 0 && node < PGRAC_FENCED_MAX_NODES &&
		context->config->nodes[node].present &&
		memcmp(intent->target_uuid, context->config->nodes[node].target_uuid, 16) == 0 &&
		pgrac_fenced_config_protected_set_digest(context->config, node, digest) &&
		memcmp(intent->protected_set_digest, digest, 32) == 0;
}

bool
pgrac_fenced_operation_reconcile_startup(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalReconcileState *reconcile)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 *last;
	uint64 now;
	uint32 i;
	uint32 owned_count = 0;

	if (context == NULL || reconcile == NULL || !context->available ||
		!reconcile->available ||
		!pgrac_fenced_journal_reconcile_finish(reconcile))
		return false;
	/* Preflight the whole set before a legacy summary can change the journal. */
	for (i = 0; i < PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS; i++)
	{
		if (!reconcile->pending[i].used ||
			reconcile->pending[i].last_record.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE)
			continue;
		if (!pending_intent_matches(context, &reconcile->pending[i].last_record))
		{
			context->available = false;
			return false;
		}
		owned_count++;
	}
	for (i = 0; i < PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS; i++)
	{
		if (!reconcile->pending[i].used)
			continue;
		last = &reconcile->pending[i].last_record;
		if (last->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_NONE)
			continue;
		if (!monotonic_now_ns(&now))
			return false;
		record_base(context, PGRAC_FENCED_JOURNAL_KIND_RECONCILED,
			last->operation_id, now, &record);
		memcpy(record.binding_digest, last->binding_digest,
			sizeof(record.binding_digest));
		record.provider_result = last->provider_result;
		record.provider_native_status = last->provider_native_status;
		record.deny_reason = last->deny_reason;
		if (last->target_state == PGRAC_FENCED_JOURNAL_TARGET_OFF ||
			last->target_state == PGRAC_FENCED_JOURNAL_TARGET_ON)
		{
			record.target_state = last->target_state;
			record.io_drain_state = last->io_drain_state;
		}
		else
		{
			record.target_state = PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN;
			record.io_drain_state = PGRAC_FENCED_IO_DRAIN_UNKNOWN;
		}
		if (!append_record(context, &record))
			return false;
		memset(&reconcile->pending[i], 0, sizeof(reconcile->pending[i]));
		reconcile->pending_count--;
	}
	context->restart_fresh_readback_required =
		reconcile->fresh_readback_required;
	context->restart_keep_write_disabled = reconcile->keep_write_disabled;
	context->restart_return_off_before_rejoin =
		reconcile->return_off_before_rejoin;
	return reconcile->pending_count == owned_count;
}

static PgracFencedOperationAcceptResult
persist_prepared_accept(PgracFencedOperationContextV1 *context,
						const PgracExternalFenceProtocolRequestV1 *request,
						PgracFencedPreparedAcquireV1 *prepared,
						PgracExternalFenceProtocolResponseV1 *response)
{
	PgracFencedJournalRecordV1 record;

	record_base(context, PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED,
		request->request_nonce, prepared->accepted_mono_ns, &record);
	record.provider_result = PGRAC_FENCED_PROVIDER_PENDING;
	if (context->config->format_version == 2)
	{
		if (!pg_strong_random(record.operation_id, sizeof(record.operation_id)) ||
			!bytes_nonzero(record.operation_id, sizeof(record.operation_id)) ||
			memcmp(record.operation_id, request->request_nonce, 16) == 0)
		{
			memset(prepared, 0, sizeof(*prepared));
			return PGRAC_FENCED_OPERATION_COMPLETE;
		}
		memcpy(record.binding_digest, prepared->binding_digest, 32);
		record.intent.kind = PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE;
		record.intent.attempt = 1;
		record.intent.system_identifier = context->config->system_identifier;
		memcpy(record.intent.target_uuid, prepared->target.target_uuid, 16);
		memcpy(record.intent.protected_set_digest, request->need.protected_set_digest, 32);
		record.intent.request.acquire = *request;
	}
	if (!append_record(context, &record))
	{
		memset(prepared, 0, sizeof(*prepared));
		response->deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		return PGRAC_FENCED_OPERATION_COMPLETE;
	}
	prepared->accepted_journal_seq = record.seq;
	if (context->config->format_version == 2)
		prepared->accepted_record = record;
	return PGRAC_FENCED_OPERATION_READY;
}

PgracFencedOperationAcceptResult
pgrac_fenced_operation_accept(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	uint64 deadline_mono_ns,
	PgracFencedPreparedAcquireV1 *prepared,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracExternalFenceProtocolBindingV1 binding;
	PgracFencedTargetV1 target;
	uint8 binding_digest[32];
	uint8 protected_set_digest[32];
	uint64 now;

	if (response == NULL || prepared == NULL)
		return PGRAC_FENCED_OPERATION_ERROR;
	memset(response, 0, sizeof(*response));
	memset(prepared, 0, sizeof(*prepared));
	if (context == NULL || request == NULL ||
		!bytes_nonzero(request->request_nonce,
			sizeof(request->request_nonce)))
		return PGRAC_FENCED_OPERATION_ERROR;
	negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
		PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE,
		PGRAC_FENCED_PROVIDER_UNAVAILABLE, 0, 0, response);
	if (!context->available)
	{
		response->deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		return PGRAC_FENCED_OPERATION_COMPLETE;
	}
	if (!pgrac_external_fence_need_v1_valid(&request->need) ||
		request->timeout_ms < PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS ||
		request->timeout_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS)
	{
		response->deny_reason = PGRAC_FENCED_DENY_BAD_ARGUMENT;
		return PGRAC_FENCED_OPERATION_COMPLETE;
	}
	if (!monotonic_now_ns(&now) || now >= deadline_mono_ns)
	{
		response->deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
		return PGRAC_FENCED_OPERATION_COMPLETE;
	}
	if (request->need.system_identifier != context->config->system_identifier ||
		!pgrac_fenced_config_protected_set_digest(context->config,
			request->need.victim_node_id, protected_set_digest) ||
		memcmp(protected_set_digest, request->need.protected_set_digest,
			sizeof(protected_set_digest)) != 0 ||
		!context->config->nodes[request->need.victim_node_id].present)
	{
		response->deny_reason = PGRAC_FENCED_DENY_STORAGE_IDENTITY;
		return PGRAC_FENCED_OPERATION_COMPLETE;
	}
	if (!pgrac_external_fence_binding_from_request_v1(&request->need,
			context->config->mapping_generation, &binding) ||
		!pgrac_external_fence_binding_digest_v1(&binding, binding_digest))
		return PGRAC_FENCED_OPERATION_COMPLETE;
	memset(&target, 0, sizeof(target));
	memcpy(target.target_uuid,
		context->config->nodes[request->need.victim_node_id].target_uuid,
		sizeof(target.target_uuid));
	target.victim_node_id = request->need.victim_node_id;
	target.mapping_generation = context->config->mapping_generation;
	target.adapter_config =
		context->config->nodes[request->need.victim_node_id].adapter_data;
	target.adapter_config_len =
		context->config->nodes[request->need.victim_node_id].adapter_data_len;
	prepared->binding = binding;
	prepared->target = target;
	memcpy(prepared->binding_digest, binding_digest,
		sizeof(prepared->binding_digest));
	prepared->accepted_mono_ns = now;
	return persist_prepared_accept(context, request, prepared, response);
}

static bool
prepared_intent_matches(const PgracFencedOperationContextV1 *context,
						const PgracExternalFenceProtocolRequestV1 *request,
						const PgracFencedPreparedAcquireV1 *prepared)
{
	const PgracFencedJournalRecordV1 *record = &prepared->accepted_record;
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	uint8 original[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 supplied[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	size_t length;

	if (context->config->format_version != 2)
		return record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE;
	return record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE &&
		record->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED &&
		record->seq == prepared->accepted_journal_seq &&
		record->event_mono_ns == prepared->accepted_mono_ns &&
		record->provider_id == context->provider->provider_id &&
		record->provider_abi_version == context->provider->abi_version &&
		record->mapping_generation == context->config->mapping_generation &&
		record->intent.system_identifier == context->config->system_identifier &&
		memcmp(record->daemon_boot_id, context->daemon_boot_id, 16) == 0 &&
		memcmp(record->semantic_config_digest, context->semantic_config_digest, 32) == 0 &&
		memcmp(record->intent.target_uuid, prepared->target.target_uuid, 16) == 0 &&
		memcmp(record->binding_digest, prepared->binding_digest, 32) == 0 &&
		pgrac_external_fence_request_v1_encode(&record->intent.request.acquire, original) &&
		pgrac_external_fence_request_v1_encode(request, supplied) &&
		memcmp(original, supplied, sizeof(original)) == 0 &&
		pgrac_fenced_journal_frame_encode(record, frame, sizeof(frame), &length);
}

bool
pgrac_fenced_operation_prepared_matches(const PgracFencedOperationContextV1 *context,
			 const PgracExternalFenceProtocolRequestV1 *request,
			 const PgracFencedPreparedAcquireV1 *prepared)
{
	PgracExternalFenceProtocolBindingV1 binding;
	uint8 binding_digest[32];
	const PgracFencedNodeConfigV1 *node;

	if (context == NULL || context->config == NULL || context->provider == NULL || request == NULL ||
		prepared == NULL || prepared->accepted_mono_ns == 0 ||
		prepared->accepted_journal_seq == 0 ||
		!prepared_intent_matches(context, request, prepared) ||
		!pgrac_external_fence_binding_from_request_v1(&request->need,
			context->config->mapping_generation, &binding) ||
		!pgrac_external_fence_binding_digest_v1(&binding, binding_digest) ||
		memcmp(&binding, &prepared->binding, sizeof(binding)) != 0 ||
		memcmp(binding_digest, prepared->binding_digest,
			sizeof(binding_digest)) != 0 ||
		request->need.victim_node_id < 0 ||
		request->need.victim_node_id >= PGRAC_FENCED_MAX_NODES)
		return false;
	node = &context->config->nodes[request->need.victim_node_id];
	return node->present && prepared->target.reserved0 == 0 &&
		prepared->target.victim_node_id == request->need.victim_node_id &&
		prepared->target.mapping_generation ==
			context->config->mapping_generation &&
		memcmp(prepared->target.target_uuid, node->target_uuid,
			sizeof(node->target_uuid)) == 0 &&
		prepared->target.adapter_config == node->adapter_data &&
		prepared->target.adapter_config_len == node->adapter_data_len;
}

bool
pgrac_fenced_operation_execute_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracExternalFenceProtocolBindingV1 binding;
	PgracFencedJournalRecordV1 record;
	PgracFencedProviderResult provider_result;
	PgracFencedProviderWorkerResult worker_result;
	PgracFencedProviderTerminal terminal;
	PgracFencedReadbackV1 readback;
	PgracFencedTargetV1 resolved;
	PgracFencedTargetV1 target;
	uint8 binding_digest[32];
	uint64 now;
	uint64 fresh_until;
	uint64 proof_generation;
	uint64 journal_seq;
	uint32 resolve_deny_reason;
	uint32 readback_deny_reason;
	bool resolve_deadline_expired;
	bool preflight_exact_on;
	int32 native_status = 0;

	if (response == NULL)
		return false;
	memset(response, 0, sizeof(*response));
	if (!pgrac_fenced_operation_prepared_matches(context, request, prepared))
		return false;
	negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
		PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE,
		PGRAC_FENCED_PROVIDER_UNAVAILABLE, 0,
		prepared->accepted_journal_seq, response);
	if (!context->available)
	{
		response->deny_reason = PGRAC_FENCED_DENY_JOURNAL;
		return true;
	}
	if (!monotonic_now_ns(&now) || now >= deadline_mono_ns)
	{
		response->deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
		return true;
	}
	binding = prepared->binding;
	target = prepared->target;
	memcpy(binding_digest, prepared->binding_digest,
		sizeof(binding_digest));
	journal_seq = prepared->accepted_journal_seq;
	worker_result = pgrac_fenced_provider_worker_resolve(context->provider,
		context->allow_test_only, &target, deadline_mono_ns, &provider_result,
		&resolved, &native_status);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result == PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (provider_result != PGRAC_FENCED_PROVIDER_OK)
	{
		if (!monotonic_now_ns(&now))
		{
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE,
				PGRAC_FENCED_DENY_TIMEOUT, provider_result, native_status,
				journal_seq, response);
			return true;
		}
		resolve_deadline_expired = now >= deadline_mono_ns;
		resolve_deny_reason = resolve_deadline_expired ?
			PGRAC_FENCED_DENY_TIMEOUT :
			provider_result == PGRAC_FENCED_PROVIDER_REJECTED ?
			PGRAC_FENCED_DENY_PROVIDER_REJECTED :
			provider_result == PGRAC_FENCED_PROVIDER_UNAVAILABLE ||
			provider_result == PGRAC_FENCED_PROVIDER_CONFIG_ERROR ?
			PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE :
			PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
		if (
			!append_provider_record(context,
				PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT, request, prepared,
				binding_digest, provider_result, native_status, NULL,
				resolve_deny_reason, now, &record))
		{
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE,
				PGRAC_FENCED_DENY_JOURNAL, provider_result, native_status,
				journal_seq, response);
			return true;
		}
		if (resolve_deadline_expired)
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE,
				PGRAC_FENCED_DENY_TIMEOUT, provider_result, native_status,
				record.seq, response);
		else
			provider_negative(context, request, provider_result, native_status,
				record.seq, response);
		return true;
	}
	memset(&readback, 0, sizeof(readback));
	if (context->restart_fresh_readback_required)
	{
		worker_result = pgrac_fenced_provider_worker_readback_retry(
			context->provider, context->allow_test_only, &resolved,
			deadline_mono_ns, &provider_result, &readback);
		if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
			provider_result = worker_result ==
				PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
				PGRAC_FENCED_PROVIDER_UNAVAILABLE :
				PGRAC_FENCED_PROVIDER_UNKNOWN;
		if (!monotonic_now_ns(&now))
		{
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE,
				PGRAC_FENCED_DENY_TIMEOUT, provider_result, 0, journal_seq,
				response);
			return true;
		}
		terminal = pgrac_fenced_provider_classify_recovery(provider_result,
			target.target_uuid, &readback);
		preflight_exact_on = restart_preflight_is_exact_on(provider_result,
			target.target_uuid, &readback);
		if (now >= deadline_mono_ns)
			readback_deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
		else if (preflight_exact_on ||
			terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
			readback_deny_reason = 0;
		else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE)
			readback_deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
		else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
			readback_deny_reason = readback.state ==
				PGRAC_FENCED_TARGET_OFF && readback.io_drain_state ==
				PGRAC_FENCED_IO_DRAIN_NOT_DRAINED ?
				PGRAC_FENCED_DENY_IO_NOT_DRAINED :
				PGRAC_FENCED_DENY_PROVIDER_REJECTED;
		else
			readback_deny_reason = PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
		if (!append_provider_record(context,
				PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT, request, prepared,
				binding_digest, provider_result, readback.native_status,
				worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK ?
				&readback : NULL, readback_deny_reason, now, &record))
		{
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE,
				PGRAC_FENCED_DENY_JOURNAL, provider_result,
				readback.native_status, journal_seq, response);
			return true;
		}
		journal_seq = record.seq;
		if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN &&
			now < deadline_mono_ns)
			goto verified_readback;
		if (!preflight_exact_on || now >= deadline_mono_ns)
		{
			if (now >= deadline_mono_ns)
				negative_response(context, request,
					PGRAC_FENCED_VERDICT_UNAVAILABLE,
					PGRAC_FENCED_DENY_TIMEOUT, provider_result,
					readback.native_status, journal_seq, response);
			else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
				negative_response(context, request,
					PGRAC_FENCED_VERDICT_REJECTED,
					readback_deny_reason, provider_result,
					readback.native_status, journal_seq, response);
			else
				provider_negative(context, request, provider_result,
					readback.native_status, journal_seq, response);
			return true;
		}
	}
	if (!monotonic_now_ns(&now) || now >= deadline_mono_ns ||
		!append_provider_record(context,
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED, request, prepared,
			binding_digest, PGRAC_FENCED_PROVIDER_PENDING, 0, NULL, 0, now,
			&record))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			context->available ? PGRAC_FENCED_DENY_TIMEOUT :
			PGRAC_FENCED_DENY_JOURNAL, PGRAC_FENCED_PROVIDER_UNAVAILABLE, 0,
			journal_seq, response);
		return true;
	}
	worker_result = pgrac_fenced_provider_worker_actuate(context->provider,
		context->allow_test_only, false, &resolved, deadline_mono_ns,
		&provider_result, &native_status);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result == PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_TIMEOUT, provider_result, native_status,
			journal_seq, response);
		return true;
	}
	if (!append_provider_record(context,
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT, request, prepared,
			binding_digest, provider_result, native_status, NULL, 0, now,
			&record))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, provider_result, native_status,
			journal_seq, response);
		return true;
	}
	journal_seq = record.seq;
	memset(&readback, 0, sizeof(readback));
	worker_result = pgrac_fenced_provider_worker_readback_retry(context->provider,
		context->allow_test_only, &resolved, deadline_mono_ns, &provider_result,
		&readback);
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		provider_result = worker_result == PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE ?
			PGRAC_FENCED_PROVIDER_UNAVAILABLE : PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (!monotonic_now_ns(&now))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_TIMEOUT, provider_result, 0, journal_seq,
			response);
		return true;
	}
	terminal = pgrac_fenced_provider_classify_recovery(provider_result,
		target.target_uuid, &readback);
	if (now >= deadline_mono_ns)
		readback_deny_reason = PGRAC_FENCED_DENY_TIMEOUT;
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE)
		readback_deny_reason = PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE;
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
		readback_deny_reason = readback.state == PGRAC_FENCED_TARGET_OFF &&
			readback.io_drain_state == PGRAC_FENCED_IO_DRAIN_NOT_DRAINED ?
			PGRAC_FENCED_DENY_IO_NOT_DRAINED :
			PGRAC_FENCED_DENY_PROVIDER_REJECTED;
	else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		readback_deny_reason = 0;
	else
		readback_deny_reason = PGRAC_FENCED_DENY_PROVIDER_UNKNOWN;
	if (!append_provider_record(context,
			PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT, request, prepared,
			binding_digest, provider_result, readback.native_status,
			worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK ? &readback : NULL,
			readback_deny_reason,
			now, &record))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, provider_result,
			readback.native_status, journal_seq, response);
		return true;
	}
	journal_seq = record.seq;

verified_readback:
	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN ||
		now >= deadline_mono_ns || context->next_proof_generation == 0)
	{
		if (now >= deadline_mono_ns)
			negative_response(context, request,
				PGRAC_FENCED_VERDICT_UNAVAILABLE, PGRAC_FENCED_DENY_TIMEOUT,
				provider_result, readback.native_status, journal_seq, response);
		else if (terminal == PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED)
			negative_response(context, request, PGRAC_FENCED_VERDICT_REJECTED,
				readback.state == PGRAC_FENCED_TARGET_OFF &&
				readback.io_drain_state ==
				PGRAC_FENCED_IO_DRAIN_NOT_DRAINED ?
				PGRAC_FENCED_DENY_IO_NOT_DRAINED :
				PGRAC_FENCED_DENY_PROVIDER_REJECTED, provider_result,
				readback.native_status, journal_seq, response);
		else
			provider_negative(context, request, provider_result,
				readback.native_status, journal_seq, response);
		return true;
	}
	if (!pgrac_fenced_operation_reserve_proof_generation(context,
			&proof_generation))
	{
		negative_response(context, request,
			PGRAC_FENCED_VERDICT_UNAVAILABLE, PGRAC_FENCED_DENY_JOURNAL,
			provider_result, readback.native_status, journal_seq, response);
		return true;
	}
	fresh_until = now > UINT64_MAX - PGRAC_FENCED_PROOF_FRESHNESS_NS ?
		UINT64_MAX : now + PGRAC_FENCED_PROOF_FRESHNESS_NS;
	if (fresh_until > deadline_mono_ns)
		fresh_until = deadline_mono_ns;
	if (fresh_until <= now ||
		!pgrac_external_fence_target_state_digest_v1(target.target_uuid,
			readback.state, readback.io_drain_state,
			context->config->mapping_generation, proof_generation,
			response->target_state_digest))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_TIMEOUT, provider_result,
			readback.native_status, journal_seq, response);
		return true;
	}
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED,
		request->request_nonce, now, &record);
	if (context->config->format_version == 2)
	{
		memcpy(record.operation_id, prepared->accepted_record.operation_id, 16);
		record.intent = prepared->accepted_record.intent;
	}
	memcpy(record.binding_digest, binding_digest,
		sizeof(record.binding_digest));
	record.provider_result = PGRAC_FENCED_PROVIDER_OK;
	record.provider_native_status = readback.native_status;
	record.target_state = readback.state;
	record.io_drain_state = readback.io_drain_state;
	record.fresh_until_mono_ns = fresh_until;
	record.proof_generation = proof_generation;
	memcpy(record.target_state_digest, response->target_state_digest,
		sizeof(record.target_state_digest));
	if (!append_record(context, &record))
	{
		negative_response(context, request, PGRAC_FENCED_VERDICT_UNAVAILABLE,
			PGRAC_FENCED_DENY_JOURNAL, provider_result,
			readback.native_status, journal_seq, response);
		return true;
	}
	response->verdict = PGRAC_FENCED_VERDICT_WRITE_EXCLUDED;
	memcpy(response->request_nonce, request->request_nonce,
		sizeof(response->request_nonce));
	response->binding = binding;
	memcpy(response->daemon_boot_id, context->daemon_boot_id,
		sizeof(response->daemon_boot_id));
	response->journal_seq = record.seq;
	response->verified_mono_ns = now;
	response->fresh_until_mono_ns = fresh_until;
	response->proof_generation = proof_generation;
	response->provider_id = context->provider->provider_id;
	response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	response->provider_result = PGRAC_FENCED_PROVIDER_OK;
	response->provider_native_status = readback.native_status;
	response->deny_reason = 0;
	return true;
}

bool
pgrac_fenced_operation_acquire(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedOperationAcceptResult result;

	result = pgrac_fenced_operation_accept(context, request, deadline_mono_ns,
		&prepared, response);
	if (result == PGRAC_FENCED_OPERATION_ERROR)
		return false;
	if (result == PGRAC_FENCED_OPERATION_COMPLETE)
		return true;
	return pgrac_fenced_operation_execute_preaccepted(context, request,
		&prepared, deadline_mono_ns, response);
}

bool
pgrac_fenced_operation_serve_joiner(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	const PgracExternalFenceProtocolResponseV1 *source_response,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracFencedJournalRecordV1 record;
	uint8 source_target_digest[32];
	uint64 fresh_until;
	uint64 now;

	if (response == NULL)
		return false;
	memset(response, 0, sizeof(*response));
	if (!pgrac_fenced_operation_prepared_matches(context, request, prepared) ||
		source_response == NULL || source_response->verdict !=
			PGRAC_FENCED_VERDICT_WRITE_EXCLUDED ||
		source_response->deny_reason != 0 ||
		source_response->provider_id != context->provider->provider_id ||
		source_response->provider_abi_version !=
			PGRAC_FENCED_PROVIDER_ABI_V1 ||
		source_response->provider_result != PGRAC_FENCED_PROVIDER_OK ||
		source_response->proof_generation == 0 ||
		source_response->verified_mono_ns < prepared->accepted_mono_ns ||
		source_response->fresh_until_mono_ns <=
			source_response->verified_mono_ns ||
		memcmp(&source_response->binding, &prepared->binding,
			sizeof(prepared->binding)) != 0 ||
		memcmp(source_response->daemon_boot_id, context->daemon_boot_id,
			sizeof(context->daemon_boot_id)) != 0 ||
		!pgrac_external_fence_target_state_digest_v1(
			prepared->target.target_uuid, PGRAC_FENCED_TARGET_OFF,
			PGRAC_FENCED_IO_DRAIN_DRAINED,
			context->config->mapping_generation,
			source_response->proof_generation, source_target_digest) ||
		memcmp(source_target_digest, source_response->target_state_digest,
			sizeof(source_target_digest)) != 0 ||
		!monotonic_now_ns(&now) || now >= deadline_mono_ns ||
		now >= source_response->fresh_until_mono_ns)
		return false;
	fresh_until = Min(deadline_mono_ns,
		source_response->fresh_until_mono_ns);
	if (fresh_until <= now)
		return false;
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED,
		request->request_nonce, source_response->verified_mono_ns, &record);
	if (context->config->format_version == 2)
	{
		memcpy(record.operation_id, prepared->accepted_record.operation_id, 16);
		record.intent = prepared->accepted_record.intent;
	}
	memcpy(record.binding_digest, prepared->binding_digest,
		sizeof(record.binding_digest));
	record.provider_result = PGRAC_FENCED_PROVIDER_OK;
	record.provider_native_status = source_response->provider_native_status;
	record.target_state = PGRAC_FENCED_TARGET_OFF;
	record.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	record.fresh_until_mono_ns = fresh_until;
	record.proof_generation = source_response->proof_generation;
	memcpy(record.target_state_digest, source_response->target_state_digest,
		sizeof(record.target_state_digest));
	if (!append_record(context, &record))
		return false;
	response->verdict = PGRAC_FENCED_VERDICT_WRITE_EXCLUDED;
	memcpy(response->request_nonce, request->request_nonce,
		sizeof(response->request_nonce));
	response->binding = prepared->binding;
	memcpy(response->daemon_boot_id, context->daemon_boot_id,
		sizeof(response->daemon_boot_id));
	response->journal_seq = record.seq;
	response->verified_mono_ns = source_response->verified_mono_ns;
	response->fresh_until_mono_ns = fresh_until;
	response->proof_generation = source_response->proof_generation;
	memcpy(response->target_state_digest,
		source_response->target_state_digest,
		sizeof(response->target_state_digest));
	response->provider_id = context->provider->provider_id;
	response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	response->provider_result = PGRAC_FENCED_PROVIDER_OK;
	response->provider_native_status = source_response->provider_native_status;
	return true;
}

bool
pgrac_fenced_operation_cancel_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint32 deny_reason,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedProviderResult provider_result;
	uint32 verdict;
	uint64 now;

	if (response == NULL || !pgrac_fenced_operation_prepared_matches(context, request, prepared) ||
		(deny_reason != PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE &&
		 deny_reason != PGRAC_FENCED_DENY_TIMEOUT &&
		 deny_reason != PGRAC_FENCED_DENY_CONNECTION_CLOSED &&
		 deny_reason != PGRAC_FENCED_DENY_MAPPING_CHANGED &&
		 deny_reason != PGRAC_FENCED_DENY_REJOIN_INVALIDATED) ||
		!monotonic_now_ns(&now))
		return false;
	provider_result = deny_reason == PGRAC_FENCED_DENY_TIMEOUT ?
		PGRAC_FENCED_PROVIDER_UNKNOWN : PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	verdict = PGRAC_FENCED_VERDICT_UNAVAILABLE;
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_INVALIDATED,
		request->request_nonce, now, &record);
	/* PGRAC: caller withdrawal is a transport event, not owned-work state.
	 * This must not resurrect work if PROOF_SERVED is durable but its async
	 * COMPLETE has not yet been delivered. Before proof, accepted work remains.
	 */
	memcpy(record.binding_digest, prepared->binding_digest,
		sizeof(record.binding_digest));
	record.provider_result = provider_result;
	record.deny_reason = deny_reason;
	if (!append_record(context, &record))
		return false;
	negative_response(context, request, verdict, deny_reason, provider_result,
		0, record.seq, response);
	return true;
}

bool
pgrac_fenced_operation_prepare_mapping_reload(
	PgracFencedOperationContextV1 *context,
	const PgracFencedConfigV1 *config,
	const PgracFencedProviderOpsV1 *provider,
	const uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES])
{
	PgracFencedOperationContextV1 candidate;
	PgracFencedJournalRecordV1 record;
	uint64 now;

	if (context == NULL || !context->available ||
		context->pending_config != NULL || !config_basic_valid(config) ||
		provider == NULL ||
		!pgrac_fenced_provider_ops_valid(provider, context->allow_test_only) ||
		provider->provider_id != config->provider_id ||
		provider->abi_version != config->provider_abi ||
		config->mapping_generation <= context->config->mapping_generation ||
		!bytes_nonzero(semantic_config_digest,
			PGRAC_FENCED_CONFIG_DIGEST_BYTES) ||
		memcmp(context->semantic_config_digest, semantic_config_digest,
			PGRAC_FENCED_CONFIG_DIGEST_BYTES) == 0 ||
		!monotonic_now_ns(&now))
		return false;
	candidate = *context;
	candidate.config = config;
	candidate.provider = provider;
	memcpy(candidate.semantic_config_digest, semantic_config_digest,
		sizeof(candidate.semantic_config_digest));
	record_base(&candidate, PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED, NULL,
		now, &record);
	record.provider_result = PGRAC_FENCED_PROVIDER_OK;
	if (!append_record(context, &record))
		return false;
	context->pending_config = config;
	context->pending_provider = provider;
	memcpy(context->pending_semantic_config_digest, semantic_config_digest,
		sizeof(context->pending_semantic_config_digest));
	return true;
}

bool
pgrac_fenced_operation_activate_mapping_reload(
	PgracFencedOperationContextV1 *context)
{
	if (context == NULL || !context->available ||
		context->pending_config == NULL || context->pending_provider == NULL ||
		!bytes_nonzero(context->pending_semantic_config_digest,
			PGRAC_FENCED_CONFIG_DIGEST_BYTES))
		return false;
	context->config = context->pending_config;
	context->provider = context->pending_provider;
	memcpy(context->semantic_config_digest,
		context->pending_semantic_config_digest,
		sizeof(context->semantic_config_digest));
	context->pending_config = NULL;
	context->pending_provider = NULL;
	memset(context->pending_semantic_config_digest, 0,
		sizeof(context->pending_semantic_config_digest));
	return true;
}

bool
pgrac_fenced_operation_invalidate(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint32 deny_reason)
{
	PgracFencedJournalRecordV1 record;
	uint8 binding_digest[32];
	uint64 now;

	if (context == NULL || response == NULL || !context->available ||
		response->verdict != PGRAC_FENCED_VERDICT_WRITE_EXCLUDED ||
		response->deny_reason != 0 || response->journal_seq == 0 ||
		response->proof_generation == 0 ||
		response->provider_id != context->provider->provider_id ||
		response->provider_abi_version != PGRAC_FENCED_PROVIDER_ABI_V1 ||
		response->provider_result != PGRAC_FENCED_PROVIDER_OK ||
		response->binding.target_mapping_generation !=
			context->config->mapping_generation ||
		memcmp(response->daemon_boot_id, context->daemon_boot_id,
			sizeof(response->daemon_boot_id)) != 0 ||
		(deny_reason != PGRAC_FENCED_DENY_EXPIRED &&
		 deny_reason != PGRAC_FENCED_DENY_CONNECTION_CLOSED &&
		 deny_reason != PGRAC_FENCED_DENY_MAPPING_CHANGED &&
		 deny_reason != PGRAC_FENCED_DENY_REJOIN_INVALIDATED) ||
		!pgrac_external_fence_binding_digest_v1(&response->binding,
			binding_digest) || !monotonic_now_ns(&now))
		return false;
	record_base(context, PGRAC_FENCED_JOURNAL_KIND_INVALIDATED,
		response->request_nonce, now, &record);
	memcpy(record.binding_digest, binding_digest,
		sizeof(record.binding_digest));
	record.provider_result = response->provider_result;
	record.provider_native_status = response->provider_native_status;
	record.deny_reason = deny_reason;
	return append_record(context, &record);
}
