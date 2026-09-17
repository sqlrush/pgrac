/*-------------------------------------------------------------------------
 * cluster_side_online_owner.h
 *    RF-SIDE production owner for immutable protected-set apply.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_ONLINE_OWNER_H
#define CLUSTER_SIDE_ONLINE_OWNER_H

#include "cluster/cluster_side_online_plan.h"

#define CLUSTER_SIDE_ONLINE_OWNER_INTERFACE_V1 1

typedef bool (*RfSideOnlineFreshAuthorityV1)(void *arg);

typedef struct RfSideOnlineProductionOwnerV1 {
	void *authority_arg;
	RfSideOnlineFreshAuthorityV1 revalidate_authority;
	RfSideOnlineProjectionOwnerV1 projection;
	bool protected_set_active;
	bool protected_set_complete;
	uint8 reserved2[6];
} RfSideOnlineProductionOwnerV1;

extern bool
rf_side_online_production_owner_init_v1(RfSideOnlineProductionOwnerV1 *owner, void *authority_arg,
										RfSideOnlineFreshAuthorityV1 revalidate_authority,
										uint32 cluster_epoch, bool failed_origin_redo_retained);
extern RfPageProofDetailV1
rf_side_online_production_preflight_v1(const RfSideOnlinePlanV1 *plan,
									   RfSideOnlineProductionOwnerV1 *owner);
extern RfPageProofDetailV1 rf_side_online_production_apply_v1(const RfSideOnlinePlanV1 *plan,
															  RfSideOnlineProductionOwnerV1 *owner);

#endif
