/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_journal_intent.c
 *    Versioned, complete operation identity in the existing journal chain.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_journal_intent.c
 *
 * NOTES
 *    PGRAC-local durable format, not provider wire or a success certificate.
 *    Old event/request codecs are reused, but the chain hashes the full frame.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/cryptohash.h"
#include "pgrac_fenced_journal.h"
#include "pgrac_fenced_provider.h"
#include "port/pg_crc32c.h"

static void
put_le(uint8 *out, uint64 value, size_t size)
{
	for (size_t i = 0; i < size; ++i)
		out[i] = (uint8)(value >> (8 * i));
}

static uint64
get_le(const uint8 *in, size_t size)
{
	uint64 value = 0;

	for (size_t i = 0; i < size; ++i)
		value |= (uint64)in[i] << (8 * i);
	return value;
}

static bool
all_zero(const void *data, size_t size)
{
	const uint8 *bytes = data;

	for (size_t i = 0; i < size; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static uint32
intent_crc(const uint8 *frame)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, frame, 764);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
acquire_binding_matches(const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalIntentV2 *intent = &record->intent;
	const PgracExternalFenceProtocolRequestV1 *request = &intent->request.acquire;
	PgracExternalFenceProtocolBindingV1 binding;
	uint8 digest[32];

	return record->record_kind != PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED
		   && record->record_kind != PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT
		   && intent->system_identifier == request->need.system_identifier
		   && memcmp(intent->protected_set_digest, request->need.protected_set_digest, 32) == 0
		   && pgrac_external_fence_binding_from_request_v1(&request->need,
														   record->mapping_generation, &binding)
		   && pgrac_external_fence_binding_digest_v1(&binding, digest)
		   && memcmp(record->binding_digest, digest, sizeof(digest)) == 0;
}

static bool
rejoin_binding_matches(const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalIntentV2 *intent = &record->intent;
	const PgracExternalFenceProtocolRejoinFrameV1 *request = &intent->request.rejoin;
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	uint8 digest[32];

	if (record->record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED || request->old_node_id < 0
		|| request->old_node_id > 127 || request->old_incarnation == 0
		|| request->candidate_incarnation <= request->old_incarnation)
		return false;
	if (request->opcode != PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) {
		if ((request->opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
			 && request->opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON)
			|| memcmp(request->operation_id, record->operation_id, 16) != 0
			|| request->system_identifier != intent->system_identifier
			|| memcmp(request->protected_set_digest, intent->protected_set_digest, 32) != 0)
			return false;
	}
	memset(&binding, 0, sizeof(binding));
	binding.system_identifier = intent->system_identifier;
	memcpy(binding.rejoin_gate_digest, request->rejoin_gate_digest, 32);
	binding.old_node_id = request->old_node_id;
	binding.old_incarnation = request->old_incarnation;
	binding.candidate_incarnation = request->candidate_incarnation;
	binding.target_mapping_generation = record->mapping_generation;
	memcpy(binding.protected_set_digest, intent->protected_set_digest, 32);
	binding.predicate_id = 2;
	binding.predicate_version = 1;
	return pgrac_external_fence_rejoin_binding_digest_v1(&binding, digest)
		   && memcmp(record->binding_digest, digest, sizeof(digest)) == 0;
}

static bool
intent_valid(const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalIntentV2 *intent = &record->intent;

	if (record->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1
		|| record->provider_abi_version != 1 || record->mapping_generation == 0
		|| record->record_kind == PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED || intent->attempt == 0
		|| intent->system_identifier == 0 || all_zero(intent->target_uuid, 16)
		|| all_zero(intent->protected_set_digest, 32))
		return false;
	if (intent->kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE)
		return acquire_binding_matches(record);
	if (intent->kind == PGRAC_FENCED_JOURNAL_INTENT_REJOIN)
		return rejoin_binding_matches(record);
	return false;
}

size_t
pgrac_fenced_journal_frame_size(const uint8 *frame, size_t available)
{
	if (frame == NULL || available < 16)
		return 0;
	if (memcmp(frame, "PFGJ", 4) == 0 && get_le(frame + 4, 2) == 1
		&& get_le(frame + 8, 4) == PGRAC_FENCED_JOURNAL_RECORD_BYTES)
		return PGRAC_FENCED_JOURNAL_RECORD_BYTES;
	if (memcmp(frame, "PFG2", 4) == 0 && get_le(frame + 4, 2) == 2
		&& get_le(frame + 8, 4) == PGRAC_FENCED_JOURNAL_INTENT_BYTES)
		return PGRAC_FENCED_JOURNAL_INTENT_BYTES;
	return 0;
}

bool
pgrac_fenced_journal_frame_encode(const PgracFencedJournalRecordV1 *record, uint8 *frame,
								  size_t capacity, size_t *frame_len)
{
	PgracFencedJournalRecordV1 event;
	uint8 encoded[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES] = { 0 };
	size_t length;
	bool ok;

	if (frame_len != NULL)
		*frame_len = 0;
	if (record == NULL || frame == NULL || frame_len == NULL)
		return false;
	length = record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE
				 ? PGRAC_FENCED_JOURNAL_RECORD_BYTES
				 : PGRAC_FENCED_JOURNAL_INTENT_BYTES;
	if (capacity < length)
		return false;
	memset(frame, 0, length);
	if (length == PGRAC_FENCED_JOURNAL_RECORD_BYTES) {
		if (!all_zero(&record->intent, sizeof(record->intent))
			|| !pgrac_fenced_journal_record_encode(record, encoded))
			return false;
	} else {
		if (!intent_valid(record))
			return false;
		event = *record;
		memset(&event.intent, 0, sizeof(event.intent));
		memcpy(encoded, "PFG2", 4);
		put_le(encoded + 4, 2, 2);
		put_le(encoded + 6, record->intent.kind, 2);
		put_le(encoded + 8, length, 4);
		if (!pgrac_fenced_journal_record_encode(&event, encoded + 16))
			return false;
		if (record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE)
			ok = pgrac_external_fence_request_v1_encode(&record->intent.request.acquire,
														encoded + 272);
		else
			ok = pgrac_external_fence_rejoin_v1_encode(&record->intent.request.rejoin,
													   encoded + 272);
		if (!ok)
			return false;
		memcpy(encoded + 528, record->intent.target_uuid, 16);
		put_le(encoded + 544, record->intent.attempt, 8);
		put_le(encoded + 552, record->intent.system_identifier, 8);
		memcpy(encoded + 560, record->intent.protected_set_digest, 32);
		memcpy(encoded + 760, "PF2Z", 4);
		put_le(encoded + 764, intent_crc(encoded), 4);
	}
	memcpy(frame, encoded, length);
	*frame_len = length;
	return true;
}

bool
pgrac_fenced_journal_intent_decode(const uint8 *frame, size_t frame_len,
								   PgracFencedJournalRecordV1 *record)
{
	PgracFencedJournalRecordV1 decoded;
	bool ok;

	if (record == NULL)
		return false;
	memset(record, 0, sizeof(*record));
	if (frame == NULL || frame_len != PGRAC_FENCED_JOURNAL_INTENT_BYTES
		|| pgrac_fenced_journal_frame_size(frame, frame_len) != frame_len
		|| get_le(frame + 12, 4) != 0 || !all_zero(frame + 592, 168)
		|| memcmp(frame + 760, "PF2Z", 4) != 0 || get_le(frame + 764, 4) != intent_crc(frame)
		|| !pgrac_fenced_journal_record_decode(frame + 16, 256, &decoded))
		return false;
	decoded.intent.kind = get_le(frame + 6, 2);
	memcpy(decoded.intent.target_uuid, frame + 528, 16);
	decoded.intent.attempt = get_le(frame + 544, 8);
	decoded.intent.system_identifier = get_le(frame + 552, 8);
	memcpy(decoded.intent.protected_set_digest, frame + 560, 32);
	if (decoded.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE)
		ok = all_zero(frame + 432, 96)
			 && pgrac_external_fence_request_v1_decode(frame + 272, 160,
													   &decoded.intent.request.acquire);
	else if (decoded.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_REJOIN)
		ok = pgrac_external_fence_rejoin_v1_decode(frame + 272, 256,
												   &decoded.intent.request.rejoin);
	else
		return false;
	if (!ok || !intent_valid(&decoded))
		return false;
	*record = decoded;
	return true;
}

bool
pgrac_fenced_journal_frame_digest(const uint8 *frame, size_t frame_len, uint8 digest[32])
{
	pg_cryptohash_ctx *ctx;
	bool ok;

	if (digest == NULL)
		return false;
	memset(digest, 0, 32);
	if (frame == NULL
		|| (frame_len != PGRAC_FENCED_JOURNAL_RECORD_BYTES
			&& frame_len != PGRAC_FENCED_JOURNAL_INTENT_BYTES)
		|| pgrac_fenced_journal_frame_size(frame, frame_len) != frame_len)
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, frame, frame_len) >= 0
		 && pg_cryptohash_final(ctx, digest, 32) >= 0;
	pg_cryptohash_free(ctx);
	if (!ok)
		memset(digest, 0, 32);
	return ok;
}

/* PGRAC: a callback cannot replace a different durable need or attempt. */
bool
pgrac_fenced_journal_intent_continues(const PgracFencedJournalRecordV1 *previous,
									  const PgracFencedJournalRecordV1 *current)
{
	uint8 before[PGRAC_FENCED_JOURNAL_INTENT_BYTES];
	uint8 after[PGRAC_FENCED_JOURNAL_INTENT_BYTES];
	size_t before_len;
	size_t after_len;

	if (current == NULL || current->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE
		|| !pgrac_fenced_journal_frame_encode(current, after, sizeof(after), &after_len))
		return false;
	if (previous == NULL)
		return true;
	if (previous->intent.kind != current->intent.kind || previous->seq >= current->seq
		|| previous->provider_id != current->provider_id
		|| previous->provider_abi_version != current->provider_abi_version
		|| previous->mapping_generation != current->mapping_generation
		|| memcmp(previous->operation_id, current->operation_id, 16) != 0
		|| memcmp(previous->binding_digest, current->binding_digest, 32) != 0
		|| memcmp(previous->semantic_config_digest, current->semantic_config_digest, 32) != 0
		|| !pgrac_fenced_journal_frame_encode(previous, before, sizeof(before), &before_len)
		|| before_len != after_len ||
		/* Canonical request (256), target (16), then sysid/protected-set (40). */
		memcmp(before + 272, after + 272, 272) != 0 || memcmp(before + 552, after + 552, 40) != 0)
		return false;
	if (previous->intent.attempt == current->intent.attempt)
		return memcmp(previous->daemon_boot_id, current->daemon_boot_id, 16) == 0;
	return previous->intent.attempt != UINT64_MAX
		   && current->intent.attempt == previous->intent.attempt + 1
		   && current->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
}
