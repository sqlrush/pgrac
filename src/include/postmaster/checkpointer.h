/*-------------------------------------------------------------------------
 * checkpointer.h
 *   PGRAC: process-local accounting for shutdown checkpoint owner waits.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/include/postmaster/checkpointer.h
 * NOTES
 *   PGRAC-local accounting only; this interface grants no lock authority.
 *-------------------------------------------------------------------------
 */
#ifndef CHECKPOINTER_H
#define CHECKPOINTER_H

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_wal_source.h"

extern uint64 cluster_checkpoint_wait_budget_now(void);
extern void cluster_checkpoint_wait_budget_begin(const ClusterPhase1FullStopPlan *plan,
												 const ClusterWalSourceRef *ref);
extern long cluster_checkpoint_wait_budget_charge(bool waiting, uint64 started_us,
												  const ClusterWalSourceRef *ref, uint64 epoch);
extern void cluster_checkpoint_wait_budget_end(void);
#endif

#endif
