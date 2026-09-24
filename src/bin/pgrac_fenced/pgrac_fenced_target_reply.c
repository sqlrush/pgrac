/*-------------------------------------------------------------------------
 * pgrac_fenced_target_reply.c
 *    Verify management observations without granting isolation authority.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_reply.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "common/jsonapi.h"
#include "mb/pg_wchar.h"
#include "pgrac_fenced_target_reply.h"

/* Alphabetical order is part of the existing canonical target JSON contract. */
typedef enum ReplyField {
	RF_ACTION,
	RF_ATTEMPT,
	RF_CANDIDATE,
	RF_CHALLENGE,
	RF_DAEMON,
	RF_INVENTORY,
	RF_JOURNAL_DIGEST,
	RF_JOURNAL_SEQUENCE,
	RF_MAPPING,
	RF_NODE,
	RF_OLD,
	RF_OPERATION,
	RF_OWNER_PHASE,
	RF_PROTECTED_SET,
	RF_REJOIN_GATE,
	RF_ROUTES,
	RF_RUNTIME,
	RF_STATUS,
	RF_SYSTEM,
	RF_TARGET_BOOT,
	RF_VERSION,
	RF_COUNT
} ReplyField;

static const char *const field_names[RF_COUNT] = { "action",
												   "attempt",
												   "candidate_incarnation",
												   "challenge",
												   "daemon_boot_id",
												   "inventory_digest",
												   "journal_digest",
												   "journal_sequence",
												   "mapping_generation",
												   "node_id",
												   "old_incarnation",
												   "operation_id",
												   "owner_phase",
												   "protected_set_digest",
												   "rejoin_gate_digest",
												   "route_phases",
												   "runtime_id",
												   "status",
												   "system_identifier",
												   "target_boot_id",
												   "version" };

#define FIELD_BIT(field) (UINT32_C(1) << (field))

typedef struct ReplyFields {
	uint32 present;
	uint64 number[RF_COUNT];
	char text[RF_COUNT][65];
} ReplyFields;

static bool
numeric_field(int field)
{
	return field == RF_ATTEMPT || field == RF_CANDIDATE || field == RF_JOURNAL_SEQUENCE
		   || field == RF_MAPPING || field == RF_NODE || field == RF_OLD || field == RF_RUNTIME
		   || field == RF_SYSTEM || field == RF_VERSION;
}

static bool
next_token(JsonLexContext *lex)
{
	char *previous_end = lex->token_terminator;
	if (json_lex(lex) != JSON_SUCCESS)
		return false;
	/* The sole final newline is excluded from the lexer. No other whitespace. */
	return lex->token_type == JSON_TOKEN_END ? lex->token_terminator == previous_end
											 : lex->token_start == previous_end;
}

static bool
string_token(JsonLexContext *lex, char out[65])
{
	size_t length;
	if (lex->token_type != JSON_TOKEN_STRING)
		return false;
	length = lex->token_terminator - lex->token_start - 2;
	if (length == 0 || length > 64)
		return false;
	/* All fields in this fixed schema are ASCII identifiers or lowercase hex.
     * Escaping would make them noncanonical; never compare a truncated string.
     */
	for (size_t n = 0; n < length; n++) {
		unsigned char c = lex->token_start[n + 1];
		if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
			  || c == '_'))
			return false;
		out[n] = c;
	}
	out[length] = '\0';
	return true;
}

static bool
uint_token(JsonLexContext *lex, uint64 *out)
{
	uint64 number = 0;
	if (lex->token_type != JSON_TOKEN_NUMBER || lex->token_start == lex->token_terminator)
		return false;
	if (*lex->token_start == '0' && lex->token_terminator - lex->token_start != 1)
		return false;
	for (const char *p = lex->token_start; p != lex->token_terminator; p++) {
		unsigned digit;
		if (*p < '0' || *p > '9')
			return false;
		digit = *p - '0';
		if (number > (UINT64_MAX - digit) / 10)
			return false;
		number = number * 10 + digit;
	}
	*out = number;
	return true;
}

static bool
routes_token(JsonLexContext *lex, uint64 *count)
{
	*count = 0;
	if (lex->token_type != JSON_TOKEN_ARRAY_START || !next_token(lex))
		return false;
	while (lex->token_type != JSON_TOKEN_ARRAY_END) {
		uint64 phase;
		if (!uint_token(lex, &phase) || phase != 3 || ++*count > 128 || !next_token(lex))
			return false;
		if (lex->token_type == JSON_TOKEN_ARRAY_END)
			break;
		if (lex->token_type != JSON_TOKEN_COMMA || !next_token(lex)
			|| lex->token_type == JSON_TOKEN_ARRAY_END)
			return false;
	}
	return true;
}

static bool
read_fields(JsonLexContext *lex, ReplyFields *fields)
{
	int previous = -1;
	if (!next_token(lex) || lex->token_type != JSON_TOKEN_OBJECT_START || !next_token(lex))
		return false;
	while (lex->token_type != JSON_TOKEN_OBJECT_END) {
		char name[65];
		int field;
		if (!string_token(lex, name))
			return false;
		for (field = 0; field < RF_COUNT; field++)
			if (strcmp(name, field_names[field]) == 0)
				break;
		if (field == RF_COUNT || field <= previous || !next_token(lex)
			|| lex->token_type != JSON_TOKEN_COLON || !next_token(lex))
			return false;
		previous = field;
		fields->present |= FIELD_BIT(field);
		if (numeric_field(field)) {
			if (!uint_token(lex, &fields->number[field]))
				return false;
		} else if (field == RF_ROUTES) {
			if (!routes_token(lex, &fields->number[field]))
				return false;
		} else if (!string_token(lex, fields->text[field]))
			return false;
		if (!next_token(lex))
			return false;
		if (lex->token_type == JSON_TOKEN_OBJECT_END)
			break;
		if (lex->token_type != JSON_TOKEN_COMMA || !next_token(lex)
			|| lex->token_type == JSON_TOKEN_OBJECT_END)
			return false;
	}
	return fields->present != 0 && next_token(lex) && lex->token_type == JSON_TOKEN_END;
}

static bool
parse_fields(const char *bytes, size_t length, ReplyFields *fields)
{
	char input[PGRAC_TARGET_REPLY_MAX_BYTES];
	JsonLexContext *lex;
	bool result;
	if (bytes == NULL || length < 3 || length > sizeof(input) || bytes[length - 1] != '\n'
		|| memchr(bytes, '\0', length) != NULL)
		return false;
	memcpy(input, bytes, length);
	input[length - 1] = '\0';
	/* The PG lexer owns syntax checks. This fixed flat schema cannot recurse,
     * and need_escapes=false avoids per-field allocations on invalid input.
     */
	lex = makeJsonLexContextCstringLen(input, (int)length - 1, PG_UTF8, false);
	result = read_fields(lex, fields);
	free(lex);
	return result;
}

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

static bool
hex_bytes(const char *text, uint8 *out, size_t length)
{
	if (strlen(text) != length * 2)
		return false;
	for (size_t n = 0; n < length; n++) {
		unsigned byte = 0;
		for (size_t k = 0; k < 2; k++) {
			unsigned char c = text[n * 2 + k];
			if (c >= '0' && c <= '9')
				byte = (byte << 4) | (c - '0');
			else if (c >= 'a' && c <= 'f')
				byte = (byte << 4) | (c - 'a' + 10);
			else
				return false;
		}
		out[n] = byte;
	}
	return nonzero(out, length);
}

static bool
echo_matches(const ReplyFields *expected, const ReplyFields *reply, uint32 fields)
{
	for (int field = 0; field < RF_COUNT; field++) {
		if (!(fields & FIELD_BIT(field)))
			continue;
		if (numeric_field(field)) {
			if (reply->number[field] != expected->number[field])
				return false;
		} else if (strcmp(reply->text[field], expected->text[field]) != 0)
			return false;
	}
	return true;
}

bool
pgrac_fenced_target_reply_decode(PgracFencedTargetCommand action,
								 const PgracFencedJournalRecordV1 *record,
								 const PgracFencedTargetV1 *target, const uint8 target_boot[16],
								 const uint8 challenge[16], const uint8 inventory_digest[32],
								 uint32 route_count, const char *bytes, size_t length,
								 PgracFencedTargetObservation *out)
{
	static const char *const statuses[] = { "IDENTITY_ONLY",
											"DENY_RECORDED",
											"OFF_DRAIN_UNCERTIFIED",
											"ACCESS_READY_UNCERTIFIED",
											"REJOIN_RUNNING_UNCERTIFIED",
											"REJOIN_REVOKED",
											"OFF_DRAIN_UNCERTIFIED" };
	ReplyFields expected = { 0 }, reply = { 0 };
	PgracFencedTargetObservation observed = { 0 };
	char command[PGRAC_TARGET_COMMAND_MAX_BYTES];
	size_t command_length;
	uint32 echoed, required;
	uint8 observed_inventory[32];
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if ((unsigned)action >= lengthof(statuses) || !nonzero(inventory_digest, 32)
		|| (action == PGRAC_TARGET_IDENTITY ? route_count != 0
											: route_count < 1 || route_count > 128)
		|| !pgrac_fenced_target_command_encode(action, record, target, target_boot, challenge,
											   command, sizeof(command), &command_length)
		|| !parse_fields(command, command_length, &expected)
		|| !parse_fields(bytes, length, &reply))
		return false;
	echoed = expected.present & ~FIELD_BIT(RF_ACTION);
	required = echoed | FIELD_BIT(RF_STATUS);
	if (action == PGRAC_TARGET_IDENTITY)
		required |= FIELD_BIT(RF_INVENTORY) | FIELD_BIT(RF_TARGET_BOOT);
	else
		required |= FIELD_BIT(RF_JOURNAL_SEQUENCE) | FIELD_BIT(RF_JOURNAL_DIGEST);
	if (action == PGRAC_TARGET_COMPLETE_OFF || action == PGRAC_TARGET_REJOIN_COMPLETE_OFF)
		required |= FIELD_BIT(RF_ROUTES);
	if (action == PGRAC_TARGET_REJOIN_RUNNING)
		required |= FIELD_BIT(RF_RUNTIME);
	if (reply.present != required || strcmp(reply.text[RF_STATUS], statuses[action]) != 0
		|| !echo_matches(&expected, &reply, echoed)
		|| !hex_bytes(reply.text[RF_TARGET_BOOT], observed.target_boot_id, 16))
		return false;
	if (action == PGRAC_TARGET_IDENTITY) {
		if (!hex_bytes(reply.text[RF_INVENTORY], observed_inventory, 32)
			|| memcmp(observed_inventory, inventory_digest, 32) != 0)
			return false;
	} else {
		if (reply.number[RF_JOURNAL_SEQUENCE] == 0
			|| !hex_bytes(reply.text[RF_JOURNAL_DIGEST], observed.journal_digest, 32))
			return false;
		observed.journal_sequence = reply.number[RF_JOURNAL_SEQUENCE];
	}
	if (required & FIELD_BIT(RF_ROUTES)) {
		if (reply.number[RF_ROUTES] != route_count)
			return false;
		observed.flushed_routes = route_count;
	}
	if (required & FIELD_BIT(RF_RUNTIME)) {
		if (reply.number[RF_RUNTIME] == 0 || reply.number[RF_RUNTIME] >= UINT32_MAX)
			return false;
		observed.runtime_id = (uint32)reply.number[RF_RUNTIME];
	}
	*out = observed;
	return true;
}
