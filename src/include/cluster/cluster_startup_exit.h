/*-------------------------------------------------------------------------
 *
 * cluster_startup_exit.h
 *    Root-bound, startup-only prior-exit observations over CONTROL.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_startup_exit.h
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_STARTUP_EXIT_H
#define CLUSTER_STARTUP_EXIT_H

#include "cluster/cluster_conf.h"
#include "cluster/cluster_ic_envelope.h"

#define CLUSTER_STARTUP_EXIT_BYTES 208
#define CLUSTER_STARTUP_EXIT_REQUEST UINT32_C(1)
#define CLUSTER_STARTUP_EXIT_REPLY UINT32_C(2)

/* Decoded memory carriers, not disk/wire overlays or admission tokens. */
typedef struct ClusterStartupExitKey {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint64 config_generation;
	uint64 epoch;
	uint64 root_sequence;
	uint64 coordinator_incarnation;
	uint8 root_sha256[32];
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint32 coordinator;
	uint32 reserved;
} ClusterStartupExitKey;

typedef struct ClusterStartupExitMessage {
	ClusterStartupExitKey key;
	uint64 old_incarnation;
	uint64 observing_incarnation;
	uint64 nonce;
	uint32 node;
	uint32 verb;
	uint8 evidence_sha256[32];
} ClusterStartupExitMessage;

typedef struct ClusterStartupExitCut {
	ClusterStartupExitKey key;
	uint64 required[2];
	uint64 predecessor[CLUSTER_MAX_NODES];
	uint64 observer[CLUSTER_MAX_NODES];
} ClusterStartupExitCut;

typedef enum ClusterStartupExitResult {
	CLUSTER_STARTUP_EXIT_UNAVAILABLE,
	CLUSTER_STARTUP_EXIT_WAITING,
	CLUSTER_STARTUP_EXIT_READY
} ClusterStartupExitResult;

/* Literal versioned carrier. Failure clears output; inputs must not alias it. */
extern bool cluster_startup_exit_encode(const ClusterStartupExitMessage *message,
										uint8 bytes[CLUSTER_STARTUP_EXIT_BYTES]);
extern bool cluster_startup_exit_decode(const void *bytes, Size length,
										ClusterStartupExitMessage *out);

/* Coordinator LMON only. One volatile round, no I/O or lock waits. Its caller
 * supplies an actual root/formation cut and must revalidate both under CF-X
 * before a reservation. READY alone grants no writer/provider permission.
 * Required members may not be shrunk from liveness. Sends are identically
 * retried by the existing coordinator tick, never by a correctness timeout.
 */
extern ClusterStartupExitResult cluster_startup_exit_collect(const ClusterStartupExitCut *cut,
															 uint8 digest[32]);
extern void cluster_startup_exit_cancel(void);
extern void cluster_startup_exit_ingress(const ClusterICEnvelope *env, const void *payload);
extern void cluster_startup_exit_register(void);

/* Native StartupProcess on the coordinator instance delegates CONTROL work
 * to LMON and retains its normal lock/I/O owner. Exact volatile evidence only;
 * caller must still validate root/formation/provider before publication. */
extern void cluster_startup_exit_shmem_register(void);
extern ClusterStartupExitResult cluster_startup_exit_request(const ClusterStartupExitCut *cut,
															 uint8 digest[32]);
extern void cluster_startup_exit_request_cancel(void);
extern void cluster_startup_exit_lmon_tick(void);

#endif /* CLUSTER_STARTUP_EXIT_H */
