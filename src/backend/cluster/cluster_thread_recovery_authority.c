/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_authority.c
 *    STOP-03/04/05 owner bundle for online thread recovery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_thread_recovery_authority.h"

static bool
root_owner_exact(uint16 dead_tid, const ClusterRecoveryDutyKey *duty,
				 const ClusterControlRootSnapshot *root, const ClusterControlRootReadToken *token)
{
	return dead_tid != 0 && duty != NULL && root != NULL && token != NULL
		   && dead_tid == duty->origin_thread_id
		   && memcmp(&root->identity, duty, sizeof(*duty)) == 0
		   && root->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && (root->root_flags
			   & (CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
				  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID))
				  == (CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
					  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID)
		   && token->origin_thread_id == dead_tid
		   && token->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && token->root_lineage_seq == duty->root_lineage_seq
		   && memcmp(token->authority_uuid, duty->authority_uuid, 16) == 0
		   && token->root_flags == root->root_flags && token->reserved20 == 0
		   && token->reserved32 == 0;
}

bool
cluster_thread_recovery_pin_request_build_v1(
	uint16 dead_tid, const ClusterRecoveryDutyKey *duty,
	const ClusterControlRootSnapshot *root_snapshot, const ClusterControlRootReadToken *root_token,
	const ClusterFormationWitnessV1 *formation, const PgracExternalFenceNeedSetV1 *fence_need_set,
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set,
	ClusterWalRetentionInterval *out_interval, ClusterWalRetentionPinThreadRequest *out_request)
{
	if (out_interval == NULL || out_request == NULL)
		return false;
	memset(out_interval, 0, sizeof(*out_interval));
	memset(out_request, 0, sizeof(*out_request));
	if (!root_owner_exact(dead_tid, duty, root_snapshot, root_token) || formation == NULL
		|| fence_need_set == NULL || fence_admission_set == NULL
		|| root_snapshot->checkpoint_tli == 0
		|| root_snapshot->checkpoint_tli != root_snapshot->tail_tli
		|| root_snapshot->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| root_snapshot->validated_tail_lsn_exclusive <= root_snapshot->checkpoint_lower_lsn)
		return false;
	out_interval->thread_id = dead_tid;
	out_interval->tli = root_snapshot->checkpoint_tli;
	out_interval->start_lsn = root_snapshot->checkpoint_lower_lsn;
	out_interval->end_lsn = root_snapshot->validated_tail_lsn_exclusive;
	out_request->intervals = out_interval;
	out_request->nintervals = 1;
	out_request->duty = *duty;
	out_request->root_read = *root_token;
	out_request->formation = formation;
	out_request->needs = fence_need_set;
	out_request->admissions = fence_admission_set;
	return true;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	ClusterRecoverySerialGuard *serial;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;

	if (authority == NULL || authority->duty == NULL || authority->root_snapshot == NULL
		|| authority->root_token == NULL || authority->formation == NULL
		|| authority->fence_need_set == NULL || authority->fence_admission_set == NULL
		|| authority->retention_pin == NULL || authority->serial_guard == NULL)
		return CLUSTER_THREAD_AUTHORITY_INVALID;
	serial = authority->serial_guard;
	if (!root_owner_exact(authority->duty->origin_thread_id, authority->duty,
						  authority->root_snapshot, authority->root_token)
		|| memcmp(&serial->duty, authority->duty, sizeof(serial->duty)) != 0
		|| memcmp(&serial->root_read_token, authority->root_token, sizeof(serial->root_read_token))
			   != 0)
		return CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
	if (!serial->held || serial->formation != authority->formation
		|| serial->fence_need_set != authority->fence_need_set
		|| serial->fence_admission_set != authority->fence_admission_set
		|| cluster_recovery_serial_revalidate(serial) != CLUSTER_RECOVERY_SERIAL_CURRENT)
		return CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
	if (cluster_formation_witness_revalidate_nowait(authority->formation)
			!= CLUSTER_FORMATION_WITNESS_READY
		|| !cluster_external_fence_need_set_revalidate_nowait(authority->fence_need_set,
															  authority->formation, &reason)
		|| !cluster_external_fence_revalidate_set_nowait(authority->fence_admission_set,
														 authority->fence_need_set,
														 authority->formation, &reason))
		return CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	if (cluster_wal_retention_pin_revalidate(authority->retention_pin) != CLUSTER_WAL_PIN_OK)
		return CLUSTER_THREAD_AUTHORITY_PIN_STALE;
	return CLUSTER_THREAD_AUTHORITY_OK;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 dead_tid, XLogRecPtr scan_lower,
	XLogRecPtr scan_upper)
{
	if (authority == NULL || authority->duty == NULL || authority->root_snapshot == NULL
		|| authority->root_token == NULL || authority->retention_pin == NULL
		|| authority->serial_guard == NULL
		|| !root_owner_exact(dead_tid, authority->duty, authority->root_snapshot,
							 authority->root_token)
		|| scan_lower == InvalidXLogRecPtr || scan_upper <= scan_lower)
		return false;
	return scan_lower >= authority->root_snapshot->checkpoint_lower_lsn
		   && scan_upper <= authority->root_snapshot->validated_tail_lsn_exclusive;
}

bool
cluster_thread_recovery_root_complete_patch_build_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, XLogRecPtr recovered_through,
	ClusterControlRootPatch *out_patch)
{
	const ClusterControlRootSnapshot *root;
	const uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;

	if (out_patch == NULL)
		return false;
	memset(out_patch, 0, sizeof(*out_patch));
	if (authority == NULL || authority->duty == NULL || authority->root_snapshot == NULL
		|| authority->root_token == NULL || authority->retention_pin == NULL
		|| authority->serial_guard == NULL || !authority->serial_guard->held
		|| memcmp(&authority->serial_guard->duty, authority->duty, sizeof(*authority->duty)) != 0
		|| memcmp(&authority->serial_guard->root_read_token, authority->root_token,
				  sizeof(*authority->root_token))
			   != 0)
		return false;
	root = authority->root_snapshot;
	if (!root_owner_exact(authority->duty->origin_thread_id, authority->duty, root,
						  authority->root_token)
		|| (root->root_flags & required_flags) != required_flags || root->tail_tli == 0
		|| root->tail_last_record_lsn == 0
		|| root->tail_last_record_lsn >= root->validated_tail_lsn_exclusive
		|| root->tail_last_record_crc32c == 0
		|| recovered_through != root->validated_tail_lsn_exclusive)
		return false;
	out_patch->mask
		= CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	out_patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	out_patch->expected_flags_mask = required_flags;
	out_patch->expected_flags_value = required_flags;
	out_patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	out_patch->desired.root_flags = root->root_flags | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
									| CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;
	out_patch->desired.recovered_tli = root->tail_tli;
	out_patch->desired.recovered_through_lsn_exclusive = recovered_through;
	out_patch->desired.recovered_last_record_lsn = root->tail_last_record_lsn;
	out_patch->desired.recovered_last_record_crc32c = root->tail_last_record_crc32c;
	return true;
}
