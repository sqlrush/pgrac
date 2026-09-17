/*-------------------------------------------------------------------------
 *
 * cluster_page_guard.h
 *    STOP-06 no-wait PAGE target protection.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_GUARD_H
#define CLUSTER_PAGE_GUARD_H

#include "cluster/cluster_page_stable_base.h"

#define CLUSTER_PAGE_GUARD_INTERFACE_V1 1
#define RF_PAGE_GUARD_PARTITIONS 256

typedef enum RfPageGuardStateV1 {
	RF_PAGE_GUARD_EMPTY = 0,
	RF_PAGE_GUARD_PREFLIGHTED = 1,
	RF_PAGE_GUARD_PROMOTED = 2,
	RF_PAGE_GUARD_RELEASED = 3
} RfPageGuardStateV1;

typedef struct RfPageGuardPreflightV1 {
	RfPageIdentityV1 page_identity;
	uint64 identity_fingerprint;
	uint32 partition;
	uint8 state;
	uint8 reserved_zero[3];
} RfPageGuardPreflightV1;

typedef struct RfPageGuardV1 {
	RfPageIdentityV1 page_identity;
	uint64 identity_fingerprint;
	uint32 partition;
	int32 owner_pid;
	uint8 state;
	uint8 reserved_zero[7];
} RfPageGuardV1;

extern void rf_page_guard_request_lwlocks_v1(void);
extern void rf_page_guard_shmem_init_v1(void);
extern bool rf_page_guard_preflight_v1(const RfPageIdentityV1 *identity,
									   RfPageGuardPreflightV1 *preflight);
extern bool rf_page_guard_promote_nowait_v1(const RfPageGuardPreflightV1 *preflight,
											RfPageGuardV1 *guard);
extern bool rf_page_guard_revalidate_nowait_v1(const RfPageGuardV1 *guard,
											   const RfPageIdentityV1 *identity);
extern bool rf_page_guard_covers_nowait_v1(const RfPageGuardV1 *guard,
										   const RfPageIdentityV1 *identity);
extern void rf_page_guard_release_v1(RfPageGuardV1 *guard);

#endif /* CLUSTER_PAGE_GUARD_H */
