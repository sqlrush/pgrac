/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_config.c
 *	  Strict canonical parser for the root-owned daemon configuration.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/cryptohash.h"
#include "common/pgrac_external_fence_protocol.h"
#include "common/pgrac_fence_map.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_provider.h"

#define PGRAC_FENCED_CONFIG_MAX_LINE (8192 + 64)

static const uint8 pgrac_fenced_config_digest_domain[] =
	"PGRAC-FENCED-CONFIG-FILE-V1";

StaticAssertDecl(sizeof(pgrac_fenced_config_digest_domain) == 28,
				 "semantic config digest domain changed");

typedef struct ConfigLine
{
	const uint8 *key;
	size_t key_len;
	const uint8 *value;
	size_t value_len;
} ConfigLine;

static bool
next_line(const uint8 *bytes, size_t len, size_t *offset, ConfigLine *line)
{
	size_t start;
	size_t equal = SIZE_MAX;
	size_t i;

	if (*offset >= len)
		return false;
	start = *offset;
	for (i = start; i < len && bytes[i] != '\n'; i++)
	{
		if (bytes[i] == '\r' || bytes[i] == '\0')
			return false;
		if (bytes[i] == '=' && equal == SIZE_MAX)
			equal = i;
	}
	if (i >= len || i == start || i - start > PGRAC_FENCED_CONFIG_MAX_LINE ||
		equal == SIZE_MAX || equal == start)
		return false;
	line->key = bytes + start;
	line->key_len = equal - start;
	line->value = bytes + equal + 1;
	line->value_len = i - equal - 1;
	*offset = i + 1;
	return true;
}

static bool
key_equal(const ConfigLine *line, const char *expected)
{
	size_t len = strlen(expected);

	return line->key_len == len && memcmp(line->key, expected, len) == 0;
}

static bool
parse_uint(const uint8 *text, size_t len, uint64 max_value, uint64 *out)
{
	uint64 value = 0;
	size_t i;

	if (len == 0 || (len > 1 && text[0] == '0'))
		return false;
	for (i = 0; i < len; i++)
	{
		uint32 digit;

		if (text[i] < '0' || text[i] > '9')
			return false;
		digit = (uint32) (text[i] - '0');
		if (value > (max_value - digit) / 10)
			return false;
		value = value * 10 + digit;
	}
	*out = value;
	return true;
}

static int
hex_nibble(uint8 ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

static bool
parse_hex(const uint8 *text, size_t len, uint8 *out, size_t out_max,
		  size_t *out_len, bool require_nonzero)
{
	bool nonzero = false;
	size_t bytes_len;
	size_t i;

	if ((len & 1) != 0)
		return false;
	bytes_len = len / 2;
	if (bytes_len > out_max)
		return false;
	for (i = 0; i < bytes_len; i++)
	{
		int high = hex_nibble(text[i * 2]);
		int low = hex_nibble(text[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		out[i] = (uint8) ((high << 4) | low);
		nonzero = nonzero || out[i] != 0;
	}
	if (require_nonzero && !nonzero)
		return false;
	*out_len = bytes_len;
	return true;
}

static bool
parse_uuid(const ConfigLine *line, uint8 out[PGRAC_FENCED_UUID_BYTES])
{
	size_t out_len = 0;

	return line->value_len == PGRAC_FENCED_UUID_BYTES * 2 &&
		parse_hex(line->value, line->value_len, out,
				  PGRAC_FENCED_UUID_BYTES, &out_len, true) &&
		out_len == PGRAC_FENCED_UUID_BYTES;
}

static bool
parse_node_key(const ConfigLine *line, const char *suffix, uint32 *node_id)
{
	static const char prefix[] = "node.";
	size_t suffix_len = strlen(suffix);
	size_t number_len;
	uint64 parsed;

	if (line->key_len <= sizeof(prefix) - 1 + suffix_len ||
		memcmp(line->key, prefix, sizeof(prefix) - 1) != 0 ||
		memcmp(line->key + line->key_len - suffix_len, suffix,
			   suffix_len) != 0)
		return false;
	number_len = line->key_len - (sizeof(prefix) - 1) - suffix_len;
	if (!parse_uint(line->key + sizeof(prefix) - 1, number_len,
				PGRAC_FENCED_MAX_NODES - 1, &parsed))
		return false;
	*node_id = (uint32) parsed;
	return true;
}

bool
pgrac_fenced_config_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISREG(st->st_mode) && st->st_uid == 0 &&
		st->st_gid == 0 && (st->st_mode & 07777) == 0600 &&
		st->st_size > 0 &&
		(uint64) st->st_size <= PGRAC_FENCED_CONFIG_MAX_BYTES;
}

bool
pgrac_fenced_config_digest_v1(
	const uint8 *bytes, size_t len,
	uint8 out[PGRAC_FENCED_CONFIG_DIGEST_BYTES])
{
	pg_cryptohash_ctx *ctx;
	uint8 encoded_len[4];
	bool ok;

	if (bytes == NULL || out == NULL || len == 0 ||
		len > PGRAC_FENCED_CONFIG_MAX_BYTES || len > UINT32_MAX)
		return false;
	encoded_len[0] = (uint8) len;
	encoded_len[1] = (uint8) (len >> 8);
	encoded_len[2] = (uint8) (len >> 16);
	encoded_len[3] = (uint8) (len >> 24);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 &&
		pg_cryptohash_update(ctx, pgrac_fenced_config_digest_domain,
			sizeof(pgrac_fenced_config_digest_domain)) >= 0 &&
		pg_cryptohash_update(ctx, encoded_len, sizeof(encoded_len)) >= 0 &&
		pg_cryptohash_update(ctx, bytes, len) >= 0 &&
		pg_cryptohash_final(ctx, out,
			PGRAC_FENCED_CONFIG_DIGEST_BYTES) >= 0;
	pg_cryptohash_free(ctx);
	if (!ok)
		memset(out, 0, PGRAC_FENCED_CONFIG_DIGEST_BYTES);
	return ok;
}

PgracFencedConfigReloadDecision
pgrac_fenced_config_reload_decide_v1(
	const PgracFencedConfigV1 *current,
	const uint8 current_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES],
	const PgracFencedConfigV1 *candidate,
	const uint8 candidate_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES])
{
	bool digest_equal;

	if (current == NULL || current_digest == NULL || candidate == NULL ||
		candidate_digest == NULL || current->mapping_generation == 0 ||
		candidate->mapping_generation == 0)
		return PGRAC_FENCED_CONFIG_RELOAD_REJECT_INVALID;
	digest_equal = memcmp(current_digest, candidate_digest,
		PGRAC_FENCED_CONFIG_DIGEST_BYTES) == 0;
	if (candidate->mapping_generation < current->mapping_generation)
		return PGRAC_FENCED_CONFIG_RELOAD_REJECT_REGRESSION;
	if (candidate->mapping_generation == current->mapping_generation)
		return digest_equal ? PGRAC_FENCED_CONFIG_RELOAD_UNCHANGED :
			PGRAC_FENCED_CONFIG_RELOAD_REJECT_SAME_GENERATION_CHANGE;
	/* Canonical bytes include mapping_generation, so equality here is invalid. */
	return digest_equal ? PGRAC_FENCED_CONFIG_RELOAD_REJECT_INVALID :
		PGRAC_FENCED_CONFIG_RELOAD_ADVANCE;
}

static bool
root_directory_is_secure(int fd)
{
	struct stat st;

	return fstat(fd, &st) == 0 && S_ISDIR(st.st_mode) && st.st_uid == 0 &&
		st.st_gid == 0 && (st.st_mode & 0022) == 0;
}

int
pgrac_fenced_config_open_secure(void)
{
	struct stat st;
	int root_fd = -1;
	int etc_fd = -1;
	int pgrac_fd = -1;
	int config_fd = -1;

	root_fd = open("/", O_RDONLY | O_DIRECTORY);
	if (root_fd < 0 || !root_directory_is_secure(root_fd))
		goto done;
	etc_fd = openat(root_fd, "etc", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	if (etc_fd < 0 || !root_directory_is_secure(etc_fd))
		goto done;
	pgrac_fd = openat(etc_fd, "pgrac", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	if (pgrac_fd < 0 || !root_directory_is_secure(pgrac_fd))
		goto done;
	config_fd = openat(pgrac_fd, "pgrac-fenced.conf", O_RDONLY | O_NOFOLLOW);
	if (config_fd < 0 || fstat(config_fd, &st) != 0 ||
		!pgrac_fenced_config_stat_secure(&st))
	{
		if (config_fd >= 0)
			(void) close(config_fd);
		config_fd = -1;
	}

done:
	if (pgrac_fd >= 0)
		(void) close(pgrac_fd);
	if (etc_fd >= 0)
		(void) close(etc_fd);
	if (root_fd >= 0)
		(void) close(root_fd);
	return config_fd;
}

static bool
parse_digest(const ConfigLine *line, uint8 digest[32])
{
	size_t decoded = 0;

	return line->value_len == 64
		&& parse_hex(line->value, line->value_len, digest, 32, &decoded, true)
		&& decoded == 32;
}

static PgracFencedConfigResult
parse_global_fields(const uint8 *bytes, size_t len, size_t *offset,
	uint32 version, PgracFencedConfigV1 *out)
{
	static const char *const global_keys[] = {
		"format_version", "mapping_generation", "system_identifier",
		"storage_backend_id", "storage_uuid", "allowed_db_uid",
		"allowed_db_gid", "provider_id", "provider_abi"
	};
	uint64 values[9];
	ConfigLine line;
	int i;

	for (i = 0; i < 9; i++)
	{
		if (!next_line(bytes, len, offset, &line) ||
			!key_equal(&line, global_keys[i]))
			return PGRAC_FENCED_CONFIG_NONCANONICAL;
		if (i == 4)
		{
			if (!parse_uuid(&line, out->storage_uuid))
				return PGRAC_FENCED_CONFIG_VALUE_INVALID;
			values[i] = 0;
		}
		else if (!parse_uint(line.value, line.value_len, UINT64_MAX,
						 &values[i]))
			return PGRAC_FENCED_CONFIG_VALUE_INVALID;
	}
	if (values[0] != version || values[1] == 0 || values[2] == 0 ||
		(values[3] != 2 && values[3] != 3) ||
		values[5] > UINT32_MAX || values[6] > UINT32_MAX ||
		values[7] > UINT16_MAX || values[7] == UINT16_MAX ||
		values[8] != 1)
		return PGRAC_FENCED_CONFIG_VALUE_INVALID;
	if ((version == 2) != (values[7] == PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1)
		|| (version == 2 && values[3] != 3))
		return PGRAC_FENCED_CONFIG_VALUE_INVALID;
	out->format_version = (uint32) values[0];
	out->mapping_generation = values[1];
	out->system_identifier = values[2];
	out->storage_backend_id = (uint32) values[3];
	out->allowed_db_uid = values[5];
	out->allowed_db_gid = values[6];
	out->provider_id = (uint16) values[7];
	out->provider_abi = (uint16) values[8];
	if (version == 2
		&& (!next_line(bytes, len, offset, &line) || !key_equal(&line, "map_public_key")
			|| !parse_digest(&line, out->map_public_key)))
		return PGRAC_FENCED_CONFIG_VALUE_INVALID;
	return PGRAC_FENCED_CONFIG_OK;
}

static PgracFencedConfigResult
parse_node_fields(const uint8 *bytes, size_t len, size_t offset, PgracFencedConfigV1 *out)
{
	uint32 last_node = 0;
	bool have_last_node = false;

	while (offset < len)
	{
		ConfigLine line;
		ConfigLine adapter;
		uint8 target_uuid[PGRAC_FENCED_UUID_BYTES];
		uint32 node_id;
		uint32 adapter_node_id;
		uint32 prior_node;
		size_t adapter_len = 0;
		uint8 digest[32];

		if (!next_line(bytes, len, &offset, &line) ||
			!parse_node_key(&line, ".target_uuid", &node_id) ||
			(have_last_node && node_id <= last_node) ||
			out->nodes[node_id].present ||
			!parse_uuid(&line, target_uuid))
			return PGRAC_FENCED_CONFIG_NONCANONICAL;
		if (out->format_version == 2
			&& (!next_line(bytes, len, &offset, &line)
				|| !parse_node_key(&line, ".protected_set_digest", &adapter_node_id)
				|| adapter_node_id != node_id
				|| !parse_digest(&line, out->nodes[node_id].protected_set_digest)))
			return PGRAC_FENCED_CONFIG_NONCANONICAL;
		if (!next_line(bytes, len, &offset, &adapter) ||
			!parse_node_key(&adapter, ".adapter_data", &adapter_node_id) ||
			adapter_node_id != node_id ||
			!parse_hex(adapter.value, adapter.value_len,
					   out->nodes[node_id].adapter_data,
					   PGRAC_FENCED_ADAPTER_DATA_MAX_BYTES,
					   &adapter_len, false))
			return PGRAC_FENCED_CONFIG_NONCANONICAL;
		for (prior_node = 0; prior_node < PGRAC_FENCED_MAX_NODES;
			 prior_node++)
		{
			if (out->nodes[prior_node].present &&
				memcmp(out->nodes[prior_node].target_uuid, target_uuid,
					   sizeof(target_uuid)) == 0)
				return PGRAC_FENCED_CONFIG_VALUE_INVALID;
		}
		memcpy(out->nodes[node_id].target_uuid, target_uuid,
			   sizeof(target_uuid));
		out->nodes[node_id].adapter_data_len = (uint16) adapter_len;
		out->nodes[node_id].present = true;
		if (out->format_version == 2
			&& !pgrac_fenced_config_protected_set_digest(out, node_id, digest))
			return PGRAC_FENCED_CONFIG_VALUE_INVALID;
		out->node_count++;
		last_node = node_id;
		have_last_node = true;
	}
	if (out->node_count == 0)
		return PGRAC_FENCED_CONFIG_VALUE_INVALID;
	return PGRAC_FENCED_CONFIG_OK;
}

static PgracFencedConfigResult
parse_config(const uint8 *bytes, size_t len, uint32 version, PgracFencedConfigV1 *out)
{
	PgracFencedConfigResult result;
	size_t offset = 0;

	if (out == NULL)
		return PGRAC_FENCED_CONFIG_BAD_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || len == 0)
		return PGRAC_FENCED_CONFIG_BAD_ARGUMENT;
	if (len > PGRAC_FENCED_CONFIG_MAX_BYTES)
		return PGRAC_FENCED_CONFIG_TOO_LARGE;
	result = parse_global_fields(bytes, len, &offset, version, out);
	if (result == PGRAC_FENCED_CONFIG_OK)
		result = parse_node_fields(bytes, len, offset, out);
	if (result != PGRAC_FENCED_CONFIG_OK)
		memset(out, 0, sizeof(*out));
	return result;
}

PgracFencedConfigResult
pgrac_fenced_config_parse_v1(const uint8 *bytes, size_t len, PgracFencedConfigV1 *out)
{
	return parse_config(bytes, len, 1, out);
}

PgracFencedConfigResult
pgrac_fenced_config_parse(const uint8 *bytes, size_t len, PgracFencedConfigV1 *out)
{
	static const char v2_prefix[] = "format_version=2\n";
	uint32 version = 1;

	if (bytes != NULL && len >= sizeof(v2_prefix) - 1
		&& memcmp(bytes, v2_prefix, sizeof(v2_prefix) - 1) == 0)
		version = 2;
	return parse_config(bytes, len, version, out);
}

bool
pgrac_fenced_config_protected_set_digest(const PgracFencedConfigV1 *config,
	int32 node_id, uint8 digest[32])
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded;
	const PgracFencedNodeConfigV1 *node;

	if (digest == NULL)
		return false;
	memset(digest, 0, PGRAC_FENCED_CONFIG_DIGEST_BYTES);
	if (config == NULL || node_id < 0 || node_id >= PGRAC_FENCED_MAX_NODES
		|| !config->nodes[node_id].present)
		return false;
	if (config->format_version == 1
		&& config->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1)
		return pgrac_external_fence_protected_set_digest_v1(config->storage_backend_id,
			config->storage_uuid, digest);
	if (config->format_version != 2 || config->storage_backend_id != 3
		|| config->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1)
		return false;
	node = &config->nodes[node_id];
	if (node->adapter_data_len > sizeof(node->adapter_data))
		return false;
	memset(&expected, 0, sizeof(expected));
	expected.system_identifier = config->system_identifier;
	expected.victim_node_id = node_id;
	expected.mapping_generation = config->mapping_generation;
	memcpy(expected.protected_set_digest, node->protected_set_digest, 32);
	if (pgrac_fence_map_v2_verify(node->adapter_data, node->adapter_data_len,
		config->map_public_key, &expected, &decoded) != PGRAC_FENCE_MAP_OK
		|| memcmp(decoded.set.guest_uuid, node->target_uuid, 16) != 0
		|| memcmp(decoded.set.storage_uuid, config->storage_uuid, 16) != 0)
		return false;
	memcpy(digest, expected.protected_set_digest, 32);
	return true;
}
