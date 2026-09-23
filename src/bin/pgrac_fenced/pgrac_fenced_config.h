/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_config.h
 *	  Strict root-owned pgrac-fenced configuration ABI.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_CONFIG_H
#define PGRAC_FENCED_CONFIG_H

#include "c.h"

#include <sys/stat.h>

#define PGRAC_FENCED_CONFIG_PATH "/etc/pgrac/pgrac-fenced.conf"
#define PGRAC_FENCED_DB_SOCKET_PATH "/var/run/pgrac/pgrac-fenced.sock"
#define PGRAC_FENCED_ADMIN_SOCKET_PATH "/var/run/pgrac/pgrac-fenced-admin.sock"
#define PGRAC_FENCED_JOURNAL_DIR "/var/lib/pgrac-fenced/journal"
#define PGRAC_FENCED_CONFIG_MAX_BYTES (64 * 1024)
#define PGRAC_FENCED_MAX_NODES 128
#define PGRAC_FENCED_UUID_BYTES 16
#define PGRAC_FENCED_ADAPTER_DATA_MAX_BYTES 4096
#define PGRAC_FENCED_CONFIG_DIGEST_BYTES 32

typedef enum PgracFencedConfigResult
{
	PGRAC_FENCED_CONFIG_OK = 0,
	PGRAC_FENCED_CONFIG_BAD_ARGUMENT = 1,
	PGRAC_FENCED_CONFIG_TOO_LARGE = 2,
	PGRAC_FENCED_CONFIG_NONCANONICAL = 3,
	PGRAC_FENCED_CONFIG_VALUE_INVALID = 4
} PgracFencedConfigResult;

typedef enum PgracFencedConfigReloadDecision
{
	PGRAC_FENCED_CONFIG_RELOAD_UNCHANGED = 0,
	PGRAC_FENCED_CONFIG_RELOAD_ADVANCE = 1,
	PGRAC_FENCED_CONFIG_RELOAD_REJECT_SAME_GENERATION_CHANGE = 2,
	PGRAC_FENCED_CONFIG_RELOAD_REJECT_REGRESSION = 3,
	PGRAC_FENCED_CONFIG_RELOAD_REJECT_INVALID = 4
} PgracFencedConfigReloadDecision;

typedef struct PgracFencedNodeConfigV1
{
	bool present;
	uint8 target_uuid[PGRAC_FENCED_UUID_BYTES];
	uint16 adapter_data_len;
	uint8 adapter_data[PGRAC_FENCED_ADAPTER_DATA_MAX_BYTES];
	uint8 protected_set_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
} PgracFencedNodeConfigV1;

typedef struct PgracFencedConfigV1
{
	/* In-memory configuration view; format_version selects the file grammar. */
	uint32 format_version;
	uint64 mapping_generation;
	uint64 system_identifier;
	uint32 storage_backend_id;
	uint8 storage_uuid[PGRAC_FENCED_UUID_BYTES];
	uint64 allowed_db_uid;
	uint64 allowed_db_gid;
	uint16 provider_id;
	uint16 provider_abi;
	uint16 node_count;
	uint8 map_public_key[32];
	PgracFencedNodeConfigV1 nodes[PGRAC_FENCED_MAX_NODES];
} PgracFencedConfigV1;

extern PgracFencedConfigResult pgrac_fenced_config_parse_v1(
	const uint8 *bytes, size_t len, PgracFencedConfigV1 *out);
extern PgracFencedConfigResult pgrac_fenced_config_parse(
	const uint8 *bytes, size_t len, PgracFencedConfigV1 *out);
extern bool pgrac_fenced_config_protected_set_digest(const PgracFencedConfigV1 *config,
	int32 node_id, uint8 digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES]);
extern bool pgrac_fenced_config_digest_v1(
	const uint8 *bytes, size_t len,
	uint8 out[PGRAC_FENCED_CONFIG_DIGEST_BYTES]);
extern PgracFencedConfigReloadDecision pgrac_fenced_config_reload_decide_v1(
	const PgracFencedConfigV1 *current,
	const uint8 current_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES],
	const PgracFencedConfigV1 *candidate,
	const uint8 candidate_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES]);
extern bool pgrac_fenced_config_stat_secure(const struct stat *st);
extern int pgrac_fenced_config_open_secure(void);

#endif /* PGRAC_FENCED_CONFIG_H */
