/*-------------------------------------------------------------------------
 * pgrac_fenced_map_filter.c
 *    Target configuration filter using the common map verifier.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_map_filter.c
 *
 * NOTES
 *    PGRAC-original. No native target action or private-key access.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/pgrac_fence_map.h"
#include "pgrac_fenced_map_filter.h"

static bool
decimal(const char *text, uint64 maximum, uint64 *out)
{
	uint64 value = 0;

	if (text == NULL || *text == '\0')
		return false;
	for (; *text; ++text) {
		unsigned digit;

		if (*text < '0' || *text > '9')
			return false;
		digit = (unsigned)(*text - '0');
		if (value > maximum / 10 || (value == maximum / 10 && digit > maximum % 10))
			return false;
		value = value * 10 + digit;
	}
	*out = value;
	return true;
}

static bool
hex32(const char *text, uint8 out[32])
{
	if (text == NULL || strlen(text) != 64)
		return false;
	for (size_t i = 0; i < 32; ++i) {
		unsigned value = 0;

		for (size_t j = 0; j < 2; ++j) {
			unsigned char ch = text[i * 2 + j];
			unsigned nibble;

			if (ch >= '0' && ch <= '9')
				nibble = ch - '0';
			else if (ch >= 'a' && ch <= 'f')
				nibble = ch - 'a' + 10;
			else
				return false;
			value = value * 16 + nibble;
		}
		out[i] = (uint8)value;
	}
	return true;
}

int
pgrac_fenced_map_filter(int argc, char *const *argv, FILE *input, FILE *output)
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded;
	uint8 bytes[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	uint8 key[32];
	uint64 node;
	size_t length;
	size_t payload_length;

	memset(&expected, 0, sizeof(expected));
	if (argc != 6 || argv == NULL || input == NULL || output == NULL
		|| !decimal(argv[1], UINT64_MAX, &expected.system_identifier)
		|| expected.system_identifier == 0
		|| !decimal(argv[2], PGRAC_FENCE_MAP_V2_MAX_NODES - 1, &node)
		|| !decimal(argv[3], UINT64_MAX, &expected.mapping_generation)
		|| expected.mapping_generation == 0 || !hex32(argv[4], expected.protected_set_digest)
		|| !hex32(argv[5], key))
		return 2;
	expected.victim_node_id = (uint32)node;
	/* Parent owns the operation envelope. Never emit even a header before EOF
	 * and the complete common-code authentication/binding check succeeds. */
	length = fread(bytes, 1, sizeof(bytes), input);
	if (ferror(input) || !feof(input) || length > PGRAC_FENCE_MAP_V2_MAX_BYTES
		|| pgrac_fence_map_v2_verify(bytes, length, key, &expected, &decoded) != PGRAC_FENCE_MAP_OK)
		return 77;
	payload_length = length - PGRAC_FENCE_MAP_V2_HEADER_BYTES - PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES;
	if (fwrite(bytes + PGRAC_FENCE_MAP_V2_HEADER_BYTES, 1, payload_length, output) != payload_length
		|| fflush(output) != 0 || ferror(output))
		return 77;
	return 0;
}
