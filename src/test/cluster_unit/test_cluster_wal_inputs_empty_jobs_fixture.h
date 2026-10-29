/* Existing stop/PI fixtures do not construct input jobs. Execute the actual
 * empty-table service and census bodies; any unexpected nonempty locator
 * aborts instead of supplying a fabricated identity or cleanup success.
 * Active job/ACK paths are covered by test_cluster_wal_inputs_job_service.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_WAL_INPUTS_EMPTY_JOBS_FIXTURE_H
#define TEST_CLUSTER_WAL_INPUTS_EMPTY_JOBS_FIXTURE_H

#include "../../backend/cluster/cluster_wal_inputs_job_private.h"

static ClusterWalInputsJobSlotV1 inputs_jobs[CLUSTER_WAL_INPUT_JOB_COUNT];

static ClusterWalInputsJobSlotV1 *
inputs_job_find(ClusterWalInputsJobV1 *job, bool cleanup)
{
	(void)job;
	(void)cleanup;
	abort();
	return NULL;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_poll_v1(ClusterWalInputsJobV1 *job)
{
	(void)job;
	abort();
	return WAL_INPUT_JOB_INVALID;
}

#include "test_cluster_wal_inputs_empty_jobs.inc"

#endif
