/* PGRAC: complete retained WAL metadata under one ROOT and WALR read scope.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_INPUTS_H
#define CLUSTER_WAL_INPUTS_H

#include "cluster/cluster_wal_tail.h"

#define CLUSTER_WAL_INPUTS_MAX 128

typedef enum ClusterWalInputKindV1 {
	CLUSTER_WAL_INPUT_CHECKPOINT = 1,
	CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL = 2
} ClusterWalInputKindV1;

typedef struct ClusterWalInputV1 {
	ClusterWalInputKindV1 kind;
	bool current;
	ClusterWalSourceRef source;
	ClusterControlRootSnapshot checkpoint;
	XLogRecPtr checkpoint_start;
	XLogRecPtr native_redo;
	/* Only the terminal alternative uses these fields. It has no checkpoint. */
	XLogRecPtr first_segment;
	ClusterWalStartupObservation terminal;
} ClusterWalInputV1;

typedef struct ClusterWalInputsV1 ClusterWalInputsV1;

/* Native background worker, bgwriter or checkpointer I/O context only;
 * never LMON/LMS dispatch. CF and WALR use their existing native owners.
 * Read all present origins, including non-serving/current/history/terminal
 * generations. No ALIVE filter or partial result. Pending initialization
 * returns RECONFIG_WAIT. Acquire distinct sorted WALR-S outside CF, then
 * select all immutable claims/anchors under the unchanged CF-S ROOT token.
 * Outputs are metadata only: live complete ends, physical WAL validation,
 * directory cuts, DATA and retirement still need their original owners.
 * The caller's ResourceOwner must remain current through release. */
extern ClusterControlRootResult cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16],
															uint64 system_identifier,
															ClusterWalInputsV1 **out);
extern ClusterControlRootResult cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs);
extern uint32 cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs);
extern const ClusterWalInputV1 *cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index);
extern void cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs);

#endif
