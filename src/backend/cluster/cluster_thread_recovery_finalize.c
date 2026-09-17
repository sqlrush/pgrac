/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_finalize.c
 *    Post-IR STOP-01 root finalization under a sealed STOP-05 pin.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster_control_root_private.h"

#define CLUSTER_THREAD_ROOT_FINALIZE_MAX_CAS_ATTEMPTS 3

static bool
root_token_matches_snapshot(const ClusterControlRootReadToken *token,
							const ClusterControlRootSnapshot *snapshot,
							const ClusterRecoveryDutyKey *duty)
{
	return token != NULL && snapshot != NULL && duty != NULL
		   && memcmp(&snapshot->identity, duty, sizeof(*duty)) == 0
		   && memcmp(token->authority_uuid, duty->authority_uuid, 16) == 0
		   && token->origin_thread_id == duty->origin_thread_id && token->source != 0
		   && token->lifecycle == snapshot->lifecycle
		   && token->root_lineage_seq == duty->root_lineage_seq && token->reserved20 == 0
		   && token->reserved32 == 0 && token->file_txn_seq != 0 && token->root_publish_seq != 0
		   && token->record_crc32c != 0 && token->root_flags == snapshot->root_flags;
}

static bool
root_complete_matches(const ClusterControlRootSnapshot *snapshot,
					  const ClusterControlRootReadToken *token, const ClusterRecoveryDutyKey *duty,
					  const ClusterControlRootPatch *patch)
{
	const uint32 recovered_flags = CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
								   | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;

	return root_token_matches_snapshot(token, snapshot, duty)
		   && snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		   && (snapshot->root_flags & recovered_flags) == recovered_flags
		   && snapshot->recovered_tli == patch->desired.recovered_tli
		   && snapshot->recovered_through_lsn_exclusive
				  == patch->desired.recovered_through_lsn_exclusive
		   && snapshot->recovered_last_record_lsn == patch->desired.recovered_last_record_lsn
		   && snapshot->recovered_last_record_crc32c == patch->desired.recovered_last_record_crc32c;
}

ClusterThreadRecoveryRootFinalizeResultV1
cluster_thread_recovery_root_finalize_after_ir_v1(const ClusterThreadRecoveryAuthorityV1 *authority,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootSnapshot *out_snapshot)
{
	ClusterControlRootSnapshot current_snapshot;
	ClusterControlRootReadToken current_token;
	int attempt;

	if (out_snapshot == NULL)
		return CLUSTER_THREAD_ROOT_FINALIZE_INVALID;
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (authority == NULL || authority->duty == NULL || authority->root_snapshot == NULL
		|| authority->root_token == NULL || authority->retention_pin == NULL
		|| authority->serial_guard == NULL || authority->serial_guard->held
		|| authority->serial_guard->release_uncertain || patch == NULL
		|| patch->mask
			   != (CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE
				   | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS)
		|| patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		|| !root_token_matches_snapshot(authority->root_token, authority->root_snapshot,
										authority->duty))
		return CLUSTER_THREAD_ROOT_FINALIZE_INVALID;
	current_snapshot = *authority->root_snapshot;
	current_token = *authority->root_token;

	for (attempt = 0; attempt < CLUSTER_THREAD_ROOT_FINALIZE_MAX_CAS_ATTEMPTS; attempt++) {
		ClusterControlRootSnapshot published;
		ClusterControlRootReadToken published_token;
		ClusterControlRootResult result;

		result = cluster_control_root_recovery_complete_publish_v1(&current_token, patch,
																   &published, &published_token);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (!root_complete_matches(&published, &published_token, authority->duty, patch))
				return CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED;
			*out_snapshot = published;
			return CLUSTER_THREAD_ROOT_FINALIZE_OK;
		}
		if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			return CLUSTER_THREAD_ROOT_FINALIZE_CLEANUP_UNCERTAIN;
		if (result != CLUSTER_CONTROL_ROOT_STALE_TOKEN
			&& result != CLUSTER_CONTROL_ROOT_CAS_CONFLICT)
			return result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
					   ? CLUSTER_THREAD_ROOT_FINALIZE_RETRY
					   : (result == CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT
							  ? CLUSTER_THREAD_ROOT_FINALIZE_INVALID
							  : CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED);

		result = cluster_control_root_read_canonical(
			authority->duty->origin_thread_id, authority->duty, CLUSTER_CONTROL_ROOT_READ_STRONG,
			&published, &published_token);
		if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			return CLUSTER_THREAD_ROOT_FINALIZE_CLEANUP_UNCERTAIN;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			return result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
					   ? CLUSTER_THREAD_ROOT_FINALIZE_RETRY
					   : CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED;
		switch (cluster_wal_retention_pin_adopt_root_readback_v1(authority->retention_pin,
																 &current_snapshot, &current_token,
																 &published, &published_token)) {
		case CLUSTER_WAL_PIN_OK:
			break;
		case CLUSTER_WAL_PIN_RELEASE_UNCERTAIN:
			return CLUSTER_THREAD_ROOT_FINALIZE_CLEANUP_UNCERTAIN;
		case CLUSTER_WAL_PIN_STALE:
			return CLUSTER_THREAD_ROOT_FINALIZE_RETRY;
		default:
			return CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED;
		}
		if (published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
			if (!root_complete_matches(&published, &published_token, authority->duty, patch))
				return CLUSTER_THREAD_ROOT_FINALIZE_RETRY;
			*out_snapshot = published;
			return CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE;
		}
		if (published.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
			return CLUSTER_THREAD_ROOT_FINALIZE_RETRY;
		current_snapshot = published;
		current_token = published_token;
	}
	return CLUSTER_THREAD_ROOT_FINALIZE_RETRY;
}
