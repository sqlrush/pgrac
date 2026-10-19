/* PGRAC: private shared relmap retention coordination.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_RUNTIME_H
#define CLUSTER_RELMAP_RETENTION_RUNTIME_H

#include "cluster/cluster_relmap_owner.h"

/* Original shared-memory registry only. New storage is UNKNOWN for every
 * origin; attachment never rebuilds, clears pins or cancels reservations. */
extern void cluster_relmap_retention_runtime_shmem_register(void);

/* Dormant original-backend preparation, with the same lock/context contract
 * as owner_retention_begin_v2. Supplies the one real runtime retention lock
 * and the original sample's origin directory; callers cannot substitute an
 * empty directory or a different lock. No live caller or COMPLETE publisher
 * is provided here. Without original qualified publication this refuses.
 * OK remains preparation only, not WAL/stage/TT release permission. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_begin_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation);

/* Dormant original ACTIVE-owner adapter, after actual publication and before
 * terminal/release work. Reads the current real FullXid without assignment and
 * the original TT owner's published receipt, then rechecks both the full era
 * and native writer outside the one runtime lock. Under that lock it enrolls
 * all existing same-FullXid pins, including staged pins and all namespaces.
 * No global relmap X lock is required. An empty COMPLETE origin stays empty;
 * UNKNOWN/BUILDING is never a qualified empty set. Inputs cannot be supplied
 * by another backend. Refusal preserves ACTIVE/BIND and pins; the eventual
 * live hook must follow its ERROR path without cancelling publication.
 * OK grants no stage, terminal, TT reuse or pin release permission. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_bind_current_v2(void);

#endif
