/*-------------------------------------------------------------------------
 *
 * cluster_control_root_private.h
 *	  Backend-private publication authority seam (RF-ROOT P5).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_ROOT_PRIVATE_H
#define CLUSTER_CONTROL_ROOT_PRIVATE_H

#include "cluster/cluster_control_root.h"

extern bool
cluster_control_root_create_authority_current_v1(const ClusterControlRootMigrationImage *image,
												 const ClusterControlRootMigrationRoundV1 *round);
extern bool cluster_control_root_activate_authority_current_v1(
	const ClusterControlRootFileToken *expected_token, const uint8 expected_round_sha256[32],
	const ClusterControlRootMigrationRoundV1 *round);
extern bool
cluster_control_root_publish_authority_current_v1(const ClusterControlRootReadToken *expected_token,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootPublishReason reason);
extern ClusterControlRootResult cluster_control_root_recovery_complete_publish_v1(
	const ClusterControlRootReadToken *expected_token, const ClusterControlRootPatch *patch,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token);

#endif /* CLUSTER_CONTROL_ROOT_PRIVATE_H */
