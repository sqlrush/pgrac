/* PGRAC: paged recovery debt views; original owners retain durable responsibility. */
#ifndef CLUSTER_RECOVERY_DEBT_H
#define CLUSTER_RECOVERY_DEBT_H

#include "cluster/cluster_recovery_checkpoint_coverage.h"

typedef struct ClusterRecoveryDebtCustodyV2 ClusterRecoveryDebtCustodyV2;

/* These are observed record states, never caller-set admission certificates. */
typedef enum ClusterRecoveryDebtStateV2
{
	CLUSTER_RECOVERY_DEBT_UNKNOWN_V2 = 0,
	CLUSTER_RECOVERY_DEBT_PREPARED_V2,
	CLUSTER_RECOVERY_DEBT_COMMITTED_V2,
	CLUSTER_RECOVERY_DEBT_RETIRED_V2
} ClusterRecoveryDebtStateV2;

/* Existing normal owners; a debt record does not create a background role. */
typedef enum ClusterRecoveryDebtOwnerV2
{
	CLUSTER_RECOVERY_DEBT_OWNER_UNKNOWN_V2 = 0,
	CLUSTER_RECOVERY_DEBT_OWNER_CHECKPOINTER_V2,
	CLUSTER_RECOVERY_DEBT_OWNER_UNDO_CLEANER_V2,
	CLUSTER_RECOVERY_DEBT_OWNER_CTRC_V2,
	CLUSTER_RECOVERY_DEBT_OWNER_RELMAP_V2
} ClusterRecoveryDebtOwnerV2;

typedef struct ClusterRecoveryDebtEntryV2
{
	uint64		ordinal;
	ClusterRecoveryFileIdentityV2 file;
	ClusterWalSourceRef source;
	FullTransactionId top_full_xid;
	uint64		namespace_id;
	uint64		object_generation;
	uint64		first_page;
	uint64		page_count;
	XLogRecPtr	retain_start;
	XLogRecPtr	retain_end;
	ClusterRecoveryDebtOwnerV2 owner;
	uint64		owner_generation;
} ClusterRecoveryDebtEntryV2;

/* A single budgeted page of a larger batch, not proof of complete enumeration. */
typedef struct ClusterRecoveryDebtPageV2
{
	uint64		index;
	uint64		first_entry;
	uint64		entry_count;
	const ClusterRecoveryDebtEntryV2 *entries;
} ClusterRecoveryDebtPageV2;

/*
 * Borrowed from the original control/retention owner.  No entries[64] limit or
 * long-lived active-pin allocation is imposed by this carrier.  A page is valid
 * only within custody; its totals/digests cannot replace complete conservative
 * guards.  PREPARED or uncertain records remain guarded across every failure.
 * Persistence codecs encode fields explicitly, never this structure's bytes.
 */
typedef struct ClusterRecoveryDebtBatchV2
{
	uint64		episode_generation;
	uint64		control_generation;
	uint64		prepare_generation;
	uint64		commit_generation;
	uint64		retire_generation;
	ClusterRecoveryDebtStateV2 state;
	uint64		total_entry_count;
	uint64		total_page_count;
	uint64		total_bytes;
	uint8		batch_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
	const ClusterRecoveryCheckpointCoverageV2 *coverage;
	ClusterRecoveryDebtPageV2 page;
	const ClusterRecoveryDebtCustodyV2 *custody;
} ClusterRecoveryDebtBatchV2;

/* Forget only this borrowed view.  This does not retire even one debt entry. */
static inline void
cluster_recovery_debt_batch_reset_v2(ClusterRecoveryDebtBatchV2 *view)
{
	if (view != NULL)
		memset(view, 0, sizeof(*view));
}

/*
 * Owner implementation belongs to the debt store.  Invalidate its views and
 * consume *custody once on every exit, before original scope/ResourceOwner
 * cleanup.  Free/close only LOCAL owned storage/FDs, including partial work;
 * borrowed original FDs and IR/WALR/pins are not owned here.  NULL is harmless.
 * Failure/current drift must not skip local cleanup, cancel durable PREPARED or
 * COMMITTED guards, publish success, reset once-latches, or permit retry.
 * Normal-owner durable retirement is a separate operation, never destruction.
 */
extern void cluster_recovery_debt_custody_destroy_v2(
	ClusterRecoveryDebtCustodyV2 **custody);

#endif							/* CLUSTER_RECOVERY_DEBT_H */
