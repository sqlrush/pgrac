/* Bounded named payload retained by the original WAL input caller.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_INPUTS_JOB_PAYLOAD_PRIVATE_H
#define CLUSTER_WAL_INPUTS_JOB_PAYLOAD_PRIVATE_H

#include "cluster_wal_inputs_job_private.h"

/* Each envelope is checked against its actual private type at the type's
 * definition. Reserve the not-yet-installed job backlink in the scope, pin
 * and consumer headers; replace that charge when the real field is wired.
 * This bounds named payload only, excluding allocator overhead and scratch.
 * The existing process-private stable lock owners remain outside the job
 * context and are not silently included in this payload bound. */
#define CLUSTER_WAL_INPUT_JOB_BACKLINK_RESERVE sizeof(ClusterWalInputsJobV1 *)
/* Charge the existing census custody and Startup deadline fields too.
 * The original per-job and four-consumer payload limits remain below. */
#define CLUSTER_WAL_INPUT_JOB_SCOPE_BYTES (112008 + 24 + 16)
#define CLUSTER_WAL_INPUT_JOB_PIN_BYTES 37936
#define CLUSTER_WAL_INPUT_JOB_REMOTE_CUT_BYTES 392
#define CLUSTER_WAL_INPUT_JOB_NOTICE_BYTES 19288
#define CLUSTER_WAL_INPUT_JOB_BATCH_BYTES 32336
#define CLUSTER_WAL_INPUT_JOB_REBUILD_BYTES 4088

#define CLUSTER_WAL_INPUT_JOB_TYPED_PAYLOAD_LIMIT 262144
#define CLUSTER_WAL_INPUT_JOBS_TYPED_PAYLOAD_LIMIT 1048576

#define CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES \
	(sizeof(ClusterWalInputsJobV1) + CLUSTER_WAL_INPUT_JOB_SCOPE_BYTES \
	 + CLUSTER_WAL_INPUT_JOB_PIN_BYTES \
	 + CLUSTER_WAL_INPUTS_MAX * CLUSTER_WAL_INPUT_JOB_REMOTE_CUT_BYTES)
#define CLUSTER_WAL_INPUT_JOBS_TYPED_PAYLOAD_BYTES \
	(CLUSTER_WAL_INPUT_JOB_COUNT * CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES \
	 + CLUSTER_WAL_INPUT_JOB_NOTICE_BYTES + CLUSTER_WAL_INPUT_JOB_BATCH_BYTES \
	 + CLUSTER_WAL_INPUT_JOB_REBUILD_BYTES)

StaticAssertDecl(CLUSTER_WAL_INPUTS_MAX == 128,
				 "WAL input remote descriptor count changed");
StaticAssertDecl(CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES
				 <= CLUSTER_WAL_INPUT_JOB_TYPED_PAYLOAD_LIMIT,
				 "WAL input retained-cut payload exceeds the per-job limit");
StaticAssertDecl(CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES
				 + CLUSTER_WAL_INPUT_JOB_NOTICE_BYTES <= CLUSTER_WAL_INPUT_JOB_TYPED_PAYLOAD_LIMIT,
				 "WAL input notice payload exceeds the per-job limit");
StaticAssertDecl(CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES
				 + CLUSTER_WAL_INPUT_JOB_BATCH_BYTES <= CLUSTER_WAL_INPUT_JOB_TYPED_PAYLOAD_LIMIT,
				 "WAL input batch payload exceeds the per-job limit");
StaticAssertDecl(CLUSTER_WAL_INPUT_JOB_COMMON_PAYLOAD_BYTES
				 + CLUSTER_WAL_INPUT_JOB_REBUILD_BYTES <= CLUSTER_WAL_INPUT_JOB_TYPED_PAYLOAD_LIMIT,
				 "WAL input rebuild payload exceeds the per-job limit");
StaticAssertDecl(CLUSTER_WAL_INPUT_JOBS_TYPED_PAYLOAD_BYTES
				 <= CLUSTER_WAL_INPUT_JOBS_TYPED_PAYLOAD_LIMIT,
				 "WAL input payloads exceed the four-consumer limit");

#endif
