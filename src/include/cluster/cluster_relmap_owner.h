/* PGRAC: original relmap producer identity, not pending/retention authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_OWNER_H
#define CLUSTER_RELMAP_OWNER_H

#include "cluster/cluster_wal_writer.h"
#include "common/cluster_relmap_v2.h"

/* Process-local preparation only. Keep the complete native writer reference;
 * the on-disk relmap header cannot substitute for its WAL/claim retention.
 * owner.wal_end is zero: this is not an encodable pending owner. */
typedef struct ClusterRelmapOwnerSampleV2 {
	ClusterWalWriterToken writer;
	ClusterRelmapV2Owner owner;
	int producer_pid;
} ClusterRelmapOwnerSampleV2;

/* Original backend, in a transaction and holding the global relmap X lock.
 * May assign the real top FullXid, so call BEFORE RelationMappingLock and
 * before a relmap WAL/critical window. Top assignment may write transaction
 * metadata. Refusal clears out; no relmap stage or retention permission. */
extern bool cluster_relmap_owner_capture_v2(ClusterRelmapOwnerSampleV2 *out);

/* No XID assignment, adoption of a newer writer, IO or wait. The same backend
 * keeps the global X lock; RelationMappingLock may now be held. This rechecks
 * provenance only, never proves retention, WAL durability, stage or release. */
extern bool cluster_relmap_owner_matches_v2(const ClusterRelmapOwnerSampleV2 *sample);

#endif
