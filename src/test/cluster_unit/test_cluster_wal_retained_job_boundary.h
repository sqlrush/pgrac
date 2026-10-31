/* Same configured input-scope boundary as the original business tests.
 * These fixtures have no CF/WALR/native ownership or ACK producer. Preserve
 * their configured begin/revalidate faults and scope-open/release accounting.
 * Real job custody and deferred retirement are tested independently by
 * test_cluster_wal_inputs_job_retained; this adapter proves neither.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_WAL_RETAINED_JOB_BOUNDARY_H
#define TEST_CLUSTER_WAL_RETAINED_JOB_BOUNDARY_H

static ClusterWalInputsJobV1 retained_boundary_job;

ClusterControlRootResult
cluster_wal_inputs_job_open_aux_v1(ClusterWalInputsConsumerV1 consumer, ClusterWalInputsJobV1 **out)
{
	if (consumer != WAL_INPUT_JOB_RETAINED_CUT || out == NULL || *out != NULL
		|| retained_boundary_job.inputs != NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(&retained_boundary_job, 0, sizeof(retained_boundary_job));
	retained_boundary_job.consumer = consumer;
	*out = &retained_boundary_job;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_job_begin_v1(ClusterWalInputsJobV1 *job, const uint8 uuid[16],
	uint64 system_identifier, ClusterWalInputsV1 **out)
{
	ClusterControlRootResult result;

	if (job != &retained_boundary_job || out == NULL || job->state != WAL_INPUT_JOB_STATE_NEW)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_wal_inputs_begin_v1(uuid, system_identifier, out);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		job->inputs = *out;
		job->state = WAL_INPUT_JOB_STATE_USABLE;
	} else
		job->state = WAL_INPUT_JOB_STATE_RETIRING_CF;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_job_revalidate_v1(ClusterWalInputsJobV1 *job)
{
	return job == &retained_boundary_job && job->state == WAL_INPUT_JOB_STATE_USABLE
		&& job->inputs != NULL ? cluster_wal_inputs_revalidate_v1(job->inputs)
		: CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}

bool
cluster_wal_inputs_job_revoke_v1(ClusterWalInputsJobV1 *job, bool error)
{
	if (job != &retained_boundary_job)
		return false;
	job->flags |= WAL_INPUT_JOB_FLAG_REVOKED | (error ? WAL_INPUT_JOB_FLAG_ERROR : 0);
	job->state = WAL_INPUT_JOB_STATE_RETIRING_CF;
	return true;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_retire_v1(ClusterWalInputsJobV1 *job)
{
	if (job != &retained_boundary_job)
		return WAL_INPUT_JOB_INVALID;
	cluster_wal_inputs_release_v1(&job->inputs);
	if (job->inputs != NULL)
		return WAL_INPUT_JOB_PENDING;
	job->state = WAL_INPUT_JOB_STATE_DONE;
	return WAL_INPUT_JOB_DONE;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_poll_v1(ClusterWalInputsJobV1 *job)
{
	if (job != &retained_boundary_job)
		return WAL_INPUT_JOB_INVALID;
	return job->state == WAL_INPUT_JOB_STATE_DONE && job->inputs == NULL
		? WAL_INPUT_JOB_DONE : WAL_INPUT_JOB_PENDING;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_finish_v1(ClusterWalInputsJobV1 **out)
{
	ClusterWalInputsJobResultV1 result = out != NULL
		? cluster_wal_inputs_job_poll_v1(*out) : WAL_INPUT_JOB_INVALID;

	if (result == WAL_INPUT_JOB_DONE)
		*out = NULL;
	return result;
}

#endif
