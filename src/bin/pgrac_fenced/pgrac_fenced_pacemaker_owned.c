/*-------------------------------------------------------------------------
 * pgrac_fenced_pacemaker_owned.c
 *    Durable preparation precedes native power actions under one deadline.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_pacemaker_owned.c
 * NOTES
 *    This composition cannot produce isolation proof or lock external policy.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <time.h>
#include "pgrac_fenced_cib.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_pacemaker.h"
#include "pgrac_fenced_pacemaker_owned.h"
#include "pgrac_fenced_target_request.h"

#ifdef USE_PACEMAKER
static bool
before_deadline(uint64 deadline)
{
	struct timespec now;
	return deadline != 0 && clock_gettime(CLOCK_MONOTONIC, &now) == 0 && now.tv_sec >= 0
		   && (uint64)now.tv_sec <= (UINT64_MAX - UINT64_C(999999999)) / UINT64_C(1000000000)
		   && (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec < deadline;
}

/* The existing command codec validates the durable event and binding digest.
 * The nonzero placeholder is local preflight only and is never sent remotely. */
static bool
owned_action(const PgracFencedConfigV1 *config, const PgracFencedJournalRecordV1 *record,
			 const PgracFencedTargetV1 *target, bool turn_on, PgracFencedTargetCommand *command,
			 PgracFencedPacemakerActionV1 *action)
{
	const PgracFencedNodeConfigV1 *node;
	uint8 placeholder[16] = { 1 };
	char validated[PGRAC_TARGET_COMMAND_MAX_BYTES];
	size_t length;
	if (config == NULL || record == NULL || target == NULL || !config->native.present
		|| config->format_version != 2
		|| config->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1
		|| config->provider_abi != PGRAC_FENCED_PROVIDER_ABI_V1
		|| record->provider_id != config->provider_id
		|| record->provider_abi_version != config->provider_abi
		|| record->intent.system_identifier != config->system_identifier
		|| target->victim_node_id < 0 || target->victim_node_id >= PGRAC_FENCED_MAX_NODES
		|| target->mapping_generation != config->mapping_generation)
		return false;
	node = &config->nodes[target->victim_node_id];
	if (!node->present || target->adapter_config == NULL
		|| node->adapter_data_len > sizeof(node->adapter_data)
		|| target->adapter_config_len != node->adapter_data_len
		|| memcmp(target->adapter_config, node->adapter_data, node->adapter_data_len) != 0
		|| memcmp(node->target_uuid, target->target_uuid, 16) != 0
		|| memcmp(node->protected_set_digest, record->intent.protected_set_digest, 32) != 0)
		return false;
	if (turn_on) {
		*command = PGRAC_TARGET_REJOIN_RESTORE;
	} else if (record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE) {
		*command = PGRAC_TARGET_PREPARE_DENY;
	} else {
		*command = PGRAC_TARGET_REJOIN_PREPARE_REVOKE;
	}
	if (!pgrac_fenced_target_command_encode(*command, record, target, placeholder, placeholder,
											validated, sizeof(validated), &length))
		return false;
	memset(action, 0, sizeof(*action));
	StaticAssertDecl(sizeof(action->node) == sizeof(config->native.node_names[0]),
					 "native node name sizes must match");
	memcpy(action->node, config->native.node_names[target->victim_node_id], sizeof(action->node));
	memcpy(action->operation_id, record->operation_id, 16);
	action->attempt = record->intent.attempt;
	action->turn_on = turn_on;
	return true;
}

static bool
policy_current(uint64 deadline, const PgracFencedCibPolicy *policy)
{
	PgracFencedCibObservation observation;
	return before_deadline(deadline)
		   && pgrac_fenced_cib_check(deadline, policy, &observation) == PGRAC_FENCED_PROVIDER_OK
		   && before_deadline(deadline);
}
#endif

PgracFencedProviderResult
pgrac_fenced_pacemaker_owned_actuate(const PgracFencedTargetV1 *target, bool turn_on)
{
#ifdef USE_PACEMAKER
	const PgracFencedConfigV1 *config = pgrac_fenced_provider_callback_config();
	const PgracFencedJournalRecordV1 *record = pgrac_fenced_provider_callback_record();
	uint64 deadline = pgrac_fenced_provider_callback_deadline_mono_ns();
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[PGRAC_CIB_POLICY_MAX_NODES];
	PgracFencedPacemakerActionV1 action;
	PgracFencedPacemakerActionResultV1 native;
	PgracFencedTargetCommand command;
	PgracFencedTargetRequestResult prepared;
	if (!owned_action(config, record, target, turn_on, &command, &action)
		|| !pgrac_fenced_config_cib_policy(config, &policy, nodes, lengthof(nodes)))
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	if (!policy_current(deadline, &policy)
		|| !pgrac_fenced_target_request(command, target, &prepared)
		|| !policy_current(deadline, &policy))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	action.deadline_mono_ns = deadline;
	if (!before_deadline(deadline)
		|| pgrac_fenced_pacemaker_action(&action, &native) != PGRAC_FENCED_PROVIDER_OK
		|| !policy_current(deadline, &policy))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	/* No signed drain or rejoin admission has been established by this result. */
	return PGRAC_FENCED_PROVIDER_OK;
#else
	(void)target;
	(void)turn_on;
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
#endif
}
