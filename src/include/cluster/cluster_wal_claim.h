/*-------------------------------------------------------------------------
 * PGRAC: root-selected WAL generation claim, explicit version 2 bytes.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_CLAIM_H
#define CLUSTER_WAL_CLAIM_H

#include "cluster/cluster_control_root.h"

#define CLUSTER_WAL_CLAIM_V2_BYTES 112

/* Logical values only, never a native on-disk overlay. */
typedef struct ClusterWalThreadClaimV2 {
	ClusterControlRootIdentity identity;
	uint64 database_incarnation;
	uint64 config_generation;
	uint64 claim_generation;
} ClusterWalThreadClaimV2;

typedef struct ClusterWalThreadClaimRefV2 {
	ClusterControlRootIdentity identity;
	uint64 database_incarnation;
	uint64 max_config_generation;
	uint8 claim_sha256[32];
} ClusterWalThreadClaimRefV2;

/* Logical input shape only; neither authenticates the claim file nor grants
 * writer/recovery authority. Shared by exact namespace consumers. */
extern bool cluster_wal_claim_v2_ref_valid(const ClusterWalThreadClaimRefV2 *ref);

extern ClusterControlRootResult
cluster_wal_claim_v2_encode(const ClusterWalThreadClaimV2 *claim,
							uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES]);
extern ClusterControlRootResult cluster_wal_claim_v2_decode(const uint8 *bytes, size_t len,
															const ClusterWalThreadClaimRefV2 *ref,
															ClusterWalThreadClaimV2 *out);
/* Read only the explicit root-selected generation. Does not create a claim,
 * choose an owner, or grant startup/serving permission. The control consumer
 * holds CF-S/X across root selection and this immutable object read.
 */
extern ClusterControlRootResult cluster_wal_claim_v2_read(const char *wal_root,
														  const ClusterWalThreadClaimRefV2 *ref,
														  ClusterWalThreadClaimV2 *out);

#endif /* CLUSTER_WAL_CLAIM_H */
