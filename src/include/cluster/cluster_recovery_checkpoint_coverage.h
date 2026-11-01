/* PGRAC: borrowed complete checkpoint coverage, with original source identity. */
#ifndef CLUSTER_RECOVERY_CHECKPOINT_COVERAGE_H
#define CLUSTER_RECOVERY_CHECKPOINT_COVERAGE_H

#include "cluster/cluster_recovery_file_identity.h"
#include "cluster/cluster_wal_source.h"
#include "catalog/pg_control.h"

typedef struct ClusterRecoveryCheckpointCustodyV2 ClusterRecoveryCheckpointCustodyV2;

/* Every interval is in this exact source; LSNs from different sources do not order. */
typedef struct ClusterRecoveryCheckpointSourceV2
{
	ClusterWalSourceRef source;
	CheckPoint	checkpoint;
	XLogRecPtr	checkpoint_start;
	XLogRecPtr	checkpoint_end;
	XLogRecPtr	redo_start;
	XLogRecPtr	replay_end;
	FullTransactionId native_floor;
	FullTransactionId post_flush_issued;
	FullTransactionId next_full_xid;
	TransactionId oldest_clog_xid;
	bool		local_bounds_observed;
} ClusterRecoveryCheckpointSourceV2;

/* A file's coverage must name its sources, not borrow a local FPI LSN for them. */
typedef struct ClusterRecoveryCheckpointFileSourceV2
{
	uint64		source_index;
	XLogRecPtr	redo_start;
	XLogRecPtr	replay_end;
} ClusterRecoveryCheckpointFileSourceV2;

typedef struct ClusterRecoveryCheckpointFileV2
{
	ClusterRecoveryFileIdentityV2 identity;
	uint64		synced_bytes;
	uint64		first_page;
	uint64		page_count;
	uint32		copy_index;
	uint32		copy_count;
	uint8		image_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
	uint64		source_count;
	const ClusterRecoveryCheckpointFileSourceV2 *sources;
} ClusterRecoveryCheckpointFileV2;

/*
 * Read-only borrow from the original checkpoint owner, valid only while its
 * custody lives.  Both vectors describe the COMPLETE selected set, not just a
 * query file or current page.  Counts are not an active-pin capacity.  Fields,
 * digests and a copied custody pointer alone confer no qualification or OPEN.
 * This is an in-memory contract, not a disk/WAL layout.
 */
typedef struct ClusterRecoveryCheckpointCoverageV2
{
	uint64		control_generation;
	uint64		registration_generation;
	uint64		checkpoint_generation;
	uint64		commit_generation;
	uint8		source_set_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
	uint8		file_set_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
	uint64		source_count;
	const ClusterRecoveryCheckpointSourceV2 *sources;
	uint64		file_count;
	const ClusterRecoveryCheckpointFileV2 *files;
	const ClusterRecoveryCheckpointCustodyV2 *custody;
} ClusterRecoveryCheckpointCoverageV2;

/* Invalidate a local view only; borrowed storage and original protections stay. */
static inline void
cluster_recovery_checkpoint_coverage_reset_v2(ClusterRecoveryCheckpointCoverageV2 *view)
{
	if (view != NULL)
		memset(view, 0, sizeof(*view));
}

/*
 * Owner implementation belongs to the checkpoint producer.  On success,
 * refusal or ERROR cleanup, invalidate all views before freeing LOCAL resources,
 * before the original scope/ResourceOwner exits.  Destroy is null-safe and
 * consumes *custody exactly once, even after current checks have failed.  It
 * must not close a borrowed namespace FD, release original IR/WALR/pins, retire
 * persistent debt, clear an attempted latch, or make the attempt retryable.
 */
extern void cluster_recovery_checkpoint_custody_destroy_v2(
	ClusterRecoveryCheckpointCustodyV2 **custody);

#endif							/* CLUSTER_RECOVERY_CHECKPOINT_COVERAGE_H */
