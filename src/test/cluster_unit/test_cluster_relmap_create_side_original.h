/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_side_original.h
 *    Original SIDE observations alongside the controlled owner fixture.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_side_original.h
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RELMAP_CREATE_SIDE_ORIGINAL_H
#define TEST_CLUSTER_RELMAP_CREATE_SIDE_ORIGINAL_H
#include "cluster/cluster_side_online_plan.h"
extern bool create_side_original_sources(const RfSideOnlinePlanV1 *, const ClusterWalSourceRef *, uint32);
extern uint32 create_side_original_count(const RfSideOnlinePlanV1 *, const ClusterWalSourceRef *);
extern bool create_side_original_input(const RfSideOnlinePlanV1 *, const ClusterColdRelmapCreateInputV1 *);
extern bool create_side_original_record(const RfSideOnlinePlanV1 *, const ClusterWalSourceRef *, const XLogReaderState *);
#endif
