/* Caller-owned prepared ROOT publication and exact CF completion.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CONTROL_ROOT_PREPARED_H
#define CLUSTER_CONTROL_ROOT_PREPARED_H

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_control_root.h"

typedef enum ClusterControlRootPreparedWorkPhaseV1 {
	CONTROL_ROOT_PREPARED_WORK_EMPTY = 0,
	CONTROL_ROOT_PREPARED_WORK_WRITING = 1,
	CONTROL_ROOT_PREPARED_WORK_RETIRING = 2,
	CONTROL_ROOT_PREPARED_WORK_CONSUMED = 3
} ClusterControlRootPreparedWorkPhaseV1;

/* Initialize to zero except for a nonzero release.job_generation. Keep this
 * object at the same address, including across ERROR, until its release is
 * confirmed and its result consumed. Never copy or reset live work.
 *
 * The token stays private while release is pending. The original caller
 * consumes the result once and still validates its own round before use.
 * A rejected round retains this object for cleanup. This is process-local
 * completion storage, never permission to access ROOT or acquire CF.
 * First release keeps CF's original bounded confirmation; later polls do
 * not wait or repeat ROOT reads, writes, or acquisition. */
typedef struct ClusterControlRootPreparedWorkV1 {
	ClusterCfJobReceiptV1 release;
	ClusterControlRootFileToken token;
	ClusterControlRootMigrationRoundV1 round;
	const void *address;
	const void *proc;
	const void *resource_owner;
	uint64 generation;
	uint64 owner_generation;
	uint64 cf_cookie;
	int pid;
	int procno;
	ClusterControlRootResult result;
	uint32 phase;
	bool rejected;
} ClusterControlRootPreparedWorkV1;

StaticAssertDecl(sizeof(ClusterControlRootPreparedWorkV1) <= 512,
				 "prepared ROOT work exceeds its byte limit");

extern ClusterControlRootResult cluster_control_root_create_prepared_job_v1(
	const ClusterControlRootMigrationImage *image, const ClusterControlRootMigrationRoundV1 *round,
	ClusterControlRootPreparedWorkV1 *work, ClusterControlRootFileToken *out_token);
extern ClusterControlRootResult cluster_control_root_activate_prepared_job_v1(
	const ClusterControlRootFileToken *expected_token, const uint8 expected_round_sha256[32],
	const ClusterControlRootMigrationRoundV1 *round, ClusterControlRootPreparedWorkV1 *work,
	ClusterControlRootFileToken *out_token);
extern ClusterControlRootResult
cluster_control_root_prepared_job_poll_v1(ClusterControlRootPreparedWorkV1 *work,
										  ClusterControlRootFileToken *out_token);

#endif
