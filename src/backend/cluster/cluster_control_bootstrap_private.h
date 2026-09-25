/*-------------------------------------------------------------------------
 * PGRAC: provisional early-control composition, never startup admission.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H
#define CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H

#include "catalog/pg_control.h"
#include "cluster/cluster_shared_config.h"
#include "common/pgrac_control_binding.h"

typedef struct ClusterControlBootstrapBytes {
	const uint8 *data;
	size_t len;
} ClusterControlBootstrapBytes;

typedef struct ClusterControlBootstrapInput {
	uint32 node_id;
	ClusterControlBootstrapBytes binding;
	ClusterControlBootstrapBytes root_before;
	ClusterControlBootstrapBytes root_after;
	ClusterControlBootstrapBytes common;
	ClusterControlBootstrapBytes config;
	ClusterControlBootstrapBytes claim;
	ClusterControlBootstrapBytes anchor;
} ClusterControlBootstrapInput;

/*
 * A memory-only observation, NOT a CF token, migration/qualification proof or
 * permission to serve. The caller still owns all supplied bytes. No GUCs are
 * applied here. Physical qualification, full startup/recovery/config gates and
 * final real CF-bound revalidation remain mandatory before shared mutation.
 */
typedef struct ClusterControlBootstrapSnapshot {
	PgracControlBinding binding;
	ClusterControlRootIdentity thread;
	ClusterSharedConfigRef config;
	ControlFileData control;
	uint8 root_sha256[32];
	uint64 root_sequence;
	uint32 database_state;
	uint32 activation_state;
} ClusterControlBootstrapSnapshot;

/*
 * Compose only the root-selected objects, using both captured root versions.
 * All inputs must remain immutable during this call. No input may overlap out.
 * Every refusal clears out. Uses the caller's memory/hash resource owner, but
 * performs no I/O, native assignment, locking, retry or publication.
 */
extern ClusterControlRootResult
cluster_control_bootstrap_decode(const ClusterControlBootstrapInput *input,
								 ClusterControlBootstrapSnapshot *out);

#endif /* CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H */
