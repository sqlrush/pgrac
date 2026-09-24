/*-------------------------------------------------------------------------
 * pgrac_fenced_target_request.c
 *    Bind protected target transport to the existing durable worker context.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_request.c
 * NOTES
 *    PGRAC observations remain separate from isolation proof and power actions.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <time.h>
#include "common/pgrac_fence_map.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_target_client.h"
#include "pgrac_fenced_target_request.h"

static bool
before_deadline(uint64 deadline)
{
	struct timespec now;
	return deadline != 0 && clock_gettime(CLOCK_MONOTONIC, &now) == 0 && now.tv_sec >= 0
		   && (uint64)now.tv_sec <= (UINT64_MAX - UINT64_C(999999999)) / UINT64_C(1000000000)
		   && (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec < deadline;
}

/* Do not derive the expected route count from a target's own response. */
static bool
target_map(const PgracFencedConfigV1 *config, const PgracFencedTargetV1 *target,
		   const PgracFencedJournalRecordV1 *record, uint32 *routes)
{
	PgracFenceMapExpectedV2 expected = { 0 };
	PgracProtectedSetDecodedV2 decoded;
	const PgracFencedNodeConfigV1 *node;
	static const uint8 zero[32] = { 0 };
	if (config == NULL || target == NULL || !config->native.present || config->format_version != 2
		|| config->storage_backend_id != 3
		|| config->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1
		|| config->provider_abi != PGRAC_FENCED_PROVIDER_ABI_V1 || target->reserved0 != 0
		|| target->victim_node_id < 0 || target->victim_node_id >= PGRAC_FENCED_MAX_NODES
		|| target->mapping_generation != config->mapping_generation
		|| config->native.bundle_directory[0] != '/' || config->native.client_config[0] != '/'
		|| strnlen(config->native.bundle_directory, MAXPGPATH) == MAXPGPATH
		|| strnlen(config->native.client_config, MAXPGPATH) == MAXPGPATH
		|| memcmp(config->native.inventory_digest, zero, sizeof(zero)) == 0)
		return false;
	node = &config->nodes[target->victim_node_id];
	if (!node->present || target->adapter_config == NULL || node->adapter_data_len == 0
		|| node->adapter_data_len > sizeof(node->adapter_data)
		|| target->adapter_config_len != node->adapter_data_len
		|| memcmp(target->adapter_config, node->adapter_data, node->adapter_data_len) != 0
		|| memcmp(target->target_uuid, node->target_uuid, 16) != 0
		|| (record != NULL
			&& (record->provider_id != config->provider_id
				|| record->provider_abi_version != config->provider_abi
				|| record->intent.system_identifier != config->system_identifier
				|| memcmp(record->intent.protected_set_digest, node->protected_set_digest, 32)
					   != 0)))
		return false;
	expected.system_identifier = config->system_identifier;
	expected.victim_node_id = target->victim_node_id;
	expected.mapping_generation = config->mapping_generation;
	memcpy(expected.protected_set_digest, node->protected_set_digest, 32);
	if (pgrac_fence_map_v2_verify(node->adapter_data, node->adapter_data_len,
								  config->map_public_key, &expected, &decoded)
			!= PGRAC_FENCE_MAP_OK
		|| memcmp(decoded.set.guest_uuid, node->target_uuid, 16) != 0
		|| memcmp(decoded.set.storage_uuid, config->storage_uuid, 16) != 0)
		return false;
	*routes = decoded.set.route_count;
	return true;
}

/* Each leg uses the original owner deadline, including decode and publication. */
static bool
exchange(const PgracFencedConfigV1 *config, PgracFencedTargetCommand action,
		 const PgracFencedJournalRecordV1 *record, const PgracFencedTargetV1 *target,
		 const uint8 boot[16], const uint8 nonce[16], uint32 routes, uint64 deadline,
		 PgracFencedTargetObservation *out)
{
	PgracFencedTargetClientPaths paths
		= { config->native.bundle_directory, config->native.client_config };
	char command[PGRAC_TARGET_COMMAND_MAX_BYTES];
	char reply[PGRAC_TARGET_REPLY_MAX_BYTES + 1];
	size_t command_length, reply_length;
	return before_deadline(deadline)
		   && pgrac_fenced_target_command_encode(action, record, target, boot, nonce, command,
												 sizeof(command), &command_length)
		   && pgrac_fenced_target_client_exchange(&paths, command, command_length, deadline, reply,
												  sizeof(reply), &reply_length)
		   && before_deadline(deadline)
		   && pgrac_fenced_target_reply_decode(action, record, target, boot, nonce,
											   config->native.inventory_digest, routes, reply,
											   reply_length, out)
		   && before_deadline(deadline);
}

bool
pgrac_fenced_target_request(PgracFencedTargetCommand action, const PgracFencedTargetV1 *target,
							PgracFencedTargetRequestResult *out)
{
	const PgracFencedConfigV1 *config = pgrac_fenced_provider_callback_config();
	const PgracFencedJournalRecordV1 *record = pgrac_fenced_provider_callback_record();
	uint64 deadline = pgrac_fenced_provider_callback_deadline_mono_ns();
	PgracFencedTargetRequestResult result = { 0 };
	PgracFencedTargetObservation identity;
	uint8 nonce[16], action_nonce[16];
	uint32 routes;
	char validation[PGRAC_TARGET_COMMAND_MAX_BYTES];
	size_t length;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!before_deadline(deadline) || !target_map(config, target, record, &routes)
		|| !pg_strong_random(nonce, sizeof(nonce)))
		return false;
	/* Validate action/event BEFORE the identity RPC. The provisional boot is
	 * never sent; the actual command will use only the authenticated boot. */
	if (action != PGRAC_TARGET_IDENTITY
		&& !pgrac_fenced_target_command_encode(action, record, target, nonce, nonce, validation,
											   sizeof(validation), &length))
		return false;
	if (!exchange(config, PGRAC_TARGET_IDENTITY, NULL, NULL, NULL, nonce, 0, deadline, &identity))
		return false;
	if (action == PGRAC_TARGET_IDENTITY) {
		result.observation = identity;
	} else {
		if (!pg_strong_random(action_nonce, sizeof(action_nonce))
			|| memcmp(nonce, action_nonce, sizeof(nonce)) == 0
			|| !exchange(config, action, record, target, identity.target_boot_id, action_nonce,
						 routes, deadline, &result.observation))
			return false;
		memcpy(result.expected.operation_id, record->operation_id, 16);
		result.expected.attempt = record->intent.attempt;
		memcpy(result.expected.daemon_boot_id, record->daemon_boot_id, 16);
		memcpy(result.expected.target_boot_id, identity.target_boot_id, 16);
		memcpy(result.expected.challenge, action_nonce, 16);
		memcpy(result.expected.guest_uuid, target->target_uuid, 16);
		result.expected.mapping_generation = target->mapping_generation;
		memcpy(result.expected.protected_set_digest, record->intent.protected_set_digest, 32);
		result.expected.route_count = routes;
	}
	if (!before_deadline(deadline))
		return false;
	*out = result;
	return true;
}
