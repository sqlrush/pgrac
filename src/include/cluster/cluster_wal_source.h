/* PGRAC: exact native WAL source identity; no durability or EMPTY proof.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_SOURCE_H
#define CLUSTER_WAL_SOURCE_H
#include "access/xlogdefs.h"
#include "cluster/cluster_wal_claim.h"
typedef struct ClusterWalSourceRef {
	ClusterWalThreadClaimRefV2 claim;
	TimeLineID timeline;
} ClusterWalSourceRef;
#endif
