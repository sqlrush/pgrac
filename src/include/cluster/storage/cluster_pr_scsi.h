/*-------------------------------------------------------------------------
 *
 * cluster_pr_scsi.h
 *	  SCSI-3 Persistent Reservation helper surface for pgrac storage.
 *
 *	  This header exposes the narrow spec-6.0a storage-intrinsic fence
 *	  interface used by the raw block_device backend.  It does not perform
 *	  cross-node preempt/evict; that remains the external fencer plane.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_pr_scsi.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, SCSI-3 PR capability probe and own-key registration).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PR_SCSI_H
#define CLUSTER_PR_SCSI_H

#include "cluster/storage/cluster_shared_fs.h"

extern uint64 cluster_pr_scsi_key_for_node(int node_id);
extern ClusterFenceCapability cluster_pr_scsi_probe(int fd);
extern int cluster_pr_scsi_register_key(int fd, int node_id);

#endif /* CLUSTER_PR_SCSI_H */
