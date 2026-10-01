/* PGRAC: original-holder background DATA completion transport.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PI_DATA_H
#define CLUSTER_PI_DATA_H

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pi_write.h"
#include "cluster/cluster_clean_leave.h"

#define CLUSTER_PI_DATA_BYTES 432
#define CLUSTER_PI_DATA_WRITE 1
#define CLUSTER_PI_DATA_WRITTEN 2

/* Untrusted codec projection. Only an authenticated, still-current reply to
 * an original master job can be consumed through ClusterPiDataV1. */
typedef struct ClusterPiDataMessageV1 {
	uint32 verb;
	uint64 nonce;
	uint64 epoch;
	uint64 holder_incarnation;
	ClusterSpaceIdentityKey key;
	ClusterPcmPiWriteCutV1 cut;
	ClusterPageWalBindingV1 binding;
} ClusterPiDataMessageV1;

typedef struct ClusterPiDataV1 ClusterPiDataV1;

extern bool cluster_pi_data_encode_v1(const ClusterPiDataMessageV1 *message,
									  uint8 bytes[CLUSTER_PI_DATA_BYTES]);
extern bool cluster_pi_data_decode_v1(const void *bytes, Size length, ClusterPiDataMessageV1 *out);

/* Original master background context, after its directory cut. One pending
 * outbound job per postmaster; capacity/busy waits create no transaction error.
 * The caller owns the full ROOT/WALR scope, ancestry and physical-ACK work.
 * Poll produces only actual DATA; it never clears PI or advances retention.
 * Handles are process/ResourceOwner local; release on all exits. */
extern ClusterControlRootResult cluster_pi_data_begin_v1(const ClusterSpaceIdentityKey *key,
														 const ClusterPcmPiWriteCutV1 *cut,
														 ClusterPiDataV1 **out);
extern ClusterControlRootResult cluster_pi_data_poll_v1(ClusterPiDataV1 *job,
														ClusterPageDataReceiptV1 **out);
extern void cluster_pi_data_release_v1(ClusterPiDataV1 **job);

/* Bufmgr's import boundary. Revalidates the opaque accepted job, peer boot,
 * epoch, namespace and the master's original exact cut. No raw constructor. */
extern bool cluster_pi_data_read_v1(const ClusterPiDataV1 *job, ClusterPageWalBindingV1 *binding,
									ClusterPcmPiWriteCutV1 *cut);
extern bool cluster_page_data_from_remote_v1(const ClusterPiDataV1 *job,
											 ClusterPageDataReceiptV1 **out);

/* LMON only routes. Native bgwriter performs all buffer/WAL/DATA I/O. */
extern void cluster_pi_data_ingress_v1(const ClusterICEnvelope *env, const void *payload);
extern void cluster_pi_data_lmon_tick_v1(void);
extern bool cluster_pi_data_bgwriter_tick_v1(void);
extern ClusterNormalStopPollResult cluster_pi_data_normal_stop_poll_v1(const char **reason);
extern void cluster_pi_data_register_v1(void);
extern void cluster_pi_data_shmem_register_v1(void);

#endif
