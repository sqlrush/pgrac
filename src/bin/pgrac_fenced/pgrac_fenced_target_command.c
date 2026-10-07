/*-------------------------------------------------------------------------
 * pgrac_fenced_target_command.c
 *    Canonical management commands, not an isolation fact source.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_command.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <stdarg.h>
#include "pgrac_fenced_target_command.h"

typedef struct CommandBuffer {
	char bytes[PGRAC_TARGET_COMMAND_MAX_BYTES];
	size_t used;
	bool failed;
} CommandBuffer;

static bool
nonzero(const uint8 *bytes, size_t length)
{
	if (bytes == NULL)
		return false;
	for (size_t n = 0; n < length; n++)
		if (bytes[n] != 0)
			return true;
	return false;
}

static void
hex_text(const uint8 *bytes, size_t length, char *out)
{
	static const char hex[] = "0123456789abcdef";
	for (size_t n = 0; n < length; n++) {
		out[n * 2] = hex[bytes[n] >> 4];
		out[n * 2 + 1] = hex[bytes[n] & 15];
	}
	out[length * 2] = '\0';
}

static void append(CommandBuffer *buffer, const char *format, ...) pg_attribute_printf(2, 3);

static void
append(CommandBuffer *buffer, const char *format, ...)
{
	va_list arguments;
	int written;
	size_t available = sizeof(buffer->bytes) - buffer->used;
	if (buffer->failed)
		return;
	va_start(arguments, format);
	written = vsnprintf(buffer->bytes + buffer->used, available, format, arguments);
	va_end(arguments);
	if (written < 0 || (size_t)written >= available)
		buffer->failed = true;
	else
		buffer->used += written;
}

static bool
phase_allows(PgracFencedTargetCommand action, const PgracFencedJournalRecordV1 *record)
{
	uint16 event = record->record_kind;
	uint16 opcode;
	if (record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE)
		return record->target_state == PGRAC_FENCED_JOURNAL_TARGET_NONE
			   && ((action == PGRAC_TARGET_PREPARE_DENY
					&& event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED)
				   || (action == PGRAC_TARGET_COMPLETE_OFF
					   && event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT));
	if (record->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_REJOIN)
		return false;
	opcode = record->intent.request.rejoin.opcode;
	if (record->target_state == PGRAC_FENCED_JOURNAL_TARGET_OFF)
		return (opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
				|| opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON)
			   && ((action == PGRAC_TARGET_REJOIN_PREPARE_REVOKE
					&& event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED)
				   || (action == PGRAC_TARGET_REJOIN_COMPLETE_OFF
					   && event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT));
	if (record->target_state != PGRAC_FENCED_JOURNAL_TARGET_NONE)
		return false;
	if (opcode == PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE)
		return event == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED
			   && (action == PGRAC_TARGET_PREPARE_DENY || action == PGRAC_TARGET_COMPLETE_OFF);
	if (opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON)
		return (event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED
				&& action == PGRAC_TARGET_REJOIN_RESTORE)
			   || (event == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT
				   && action == PGRAC_TARGET_REJOIN_RUNNING);
	return opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON
		   && event == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED
		   && action == PGRAC_TARGET_REJOIN_RUNNING;
}

bool
pgrac_fenced_target_command_encode(PgracFencedTargetCommand action,
								   const PgracFencedJournalRecordV1 *record,
								   const PgracFencedTargetV1 *target, const uint8 target_boot[16],
								   const uint8 challenge[16], char *output, size_t capacity,
								   size_t *length)
{
	static const char *const names[]
		= { "identity",		  "prepare_deny",		   "complete_off",		 "rejoin_restore",
			"rejoin_running", "rejoin_prepare_revoke", "rejoin_complete_off" };
	CommandBuffer buffer = { { 0 }, 0, false };
	char challenge_text[33], boot_text[33], daemon[33], operation[33], digest[65], gate[65];
	uint8 validated[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	size_t validated_length;
	const PgracExternalFenceProtocolRejoinFrameV1 *rejoin = NULL;
	int32 node;

	if (length != NULL)
		*length = 0;
	if (output != NULL && capacity != 0)
		output[0] = '\0';
	if (length == NULL || output == NULL || capacity == 0 || (unsigned)action >= lengthof(names)
		|| !nonzero(challenge, 16))
		return false;
	hex_text(challenge, 16, challenge_text);
	if (action == PGRAC_TARGET_IDENTITY) {
		if (record != NULL || target != NULL || target_boot != NULL)
			return false;
		append(&buffer, "{\"action\":\"identity\",\"challenge\":\"%s\",\"version\":1}\n",
			   challenge_text);
	} else {
		/* The canonical durable codec also validates the original binding digest. */
		if (record == NULL || target == NULL || target->reserved0 != 0 || !nonzero(target_boot, 16)
			|| !phase_allows(action, record)
			|| !pgrac_fenced_journal_frame_encode(record, validated, sizeof(validated),
												  &validated_length)
			|| validated_length != PGRAC_FENCED_JOURNAL_INTENT_BYTES
			|| target->mapping_generation != record->mapping_generation
			|| memcmp(target->target_uuid, record->intent.target_uuid, 16) != 0)
			return false;
		node = record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE
				   ? record->intent.request.acquire.need.victim_node_id
				   : record->intent.request.rejoin.old_node_id;
		if (node != target->victim_node_id)
			return false;
		if (action >= PGRAC_TARGET_REJOIN_RESTORE)
			rejoin = &record->intent.request.rejoin;
		hex_text(target_boot, 16, boot_text);
		hex_text(record->daemon_boot_id, 16, daemon);
		hex_text(record->operation_id, 16, operation);
		hex_text(record->intent.protected_set_digest, 32, digest);
		append(&buffer, "{\"action\":\"%s\",\"attempt\":" UINT64_FORMAT, names[action],
			   record->intent.attempt);
		if (rejoin != NULL)
			append(&buffer, ",\"candidate_incarnation\":" UINT64_FORMAT,
				   rejoin->candidate_incarnation);
		append(
			&buffer,
			",\"challenge\":\"%s\",\"daemon_boot_id\":\"%s\",\"mapping_generation\":" UINT64_FORMAT
			",\"node_id\":%d",
			challenge_text, daemon, record->mapping_generation, node);
		if (rejoin != NULL)
			append(&buffer, ",\"old_incarnation\":" UINT64_FORMAT, rejoin->old_incarnation);
		append(&buffer, ",\"operation_id\":\"%s\"", operation);
		if (action == PGRAC_TARGET_REJOIN_RUNNING)
			append(&buffer, ",\"owner_phase\":\"%s\"",
				   rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON ? "authorize"
																				   : "refresh");
		append(&buffer, ",\"protected_set_digest\":\"%s\"", digest);
		if (rejoin != NULL) {
			hex_text(rejoin->rejoin_gate_digest, 32, gate);
			append(&buffer, ",\"rejoin_gate_digest\":\"%s\"", gate);
		}
		append(&buffer,
			   ",\"system_identifier\":" UINT64_FORMAT
			   ",\"target_boot_id\":\"%s\",\"version\":1}\n",
			   record->intent.system_identifier, boot_text);
	}
	if (buffer.failed || buffer.used >= capacity)
		return false;
	memcpy(output, buffer.bytes, buffer.used + 1);
	*length = buffer.used;
	return true;
}
