/*-------------------------------------------------------------------------
 *
 * cluster_epoch.c
 *	  pgrac cluster membership epoch shmem state -- single atomic
 *	  uint64 carrying current_epoch, with cache-line padding +
 *	  spec-2.29 reconfig metadata reserved bytes (Q1 修订).
 *
 *	  spec-2.4 期 epoch 永远 = CLUSTER_EPOCH_INITIAL = 0;
 *	  spec-2.29 reconfig 真激活 epoch++ broadcast -- 此 module 当时
 *	  add advance() / set() API + reconfig coordinator hooks.
 *
 *	  Process-local stub variant for --disable-cluster builds:
 *	  cluster_epoch_get_current() returns CLUSTER_EPOCH_INITIAL
 *	  unconditionally, so envelope-build code paths in shared
 *	  module compile portably.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_epoch.c
 *
 * NOTES
 *	  pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"

#ifdef USE_PGRAC_CLUSTER

#include "port/atomics.h"
#include "storage/shmem.h"

#include "cluster/cluster_shmem.h" /* cluster_shmem_register_region */

/*
 * spec-2.4 D1 / Q1 修订:
 *
 *   ClusterEpochShmem -- 64-byte cache-line aligned shmem layout.
 *
 *   Field layout:
 *     [0..7]    current_epoch          (8 B atomic;hot read every envelope)
 *     [8..15]   epoch_changed_at_lsn   (8 B atomic;spec-2.29 reconfig writes
 *                                       on epoch++;reserved 0 in spec-2.4)
 *     [16..63]  _reserved              (48 B;spec-2.29 reconfig coordinator
 *                                       metadata land here in place;
 *                                       MUST stay zero in spec-2.4)
 *
 *   Why 64-byte:
 *     1. False sharing防御 -- current_epoch is read on every envelope
 *        build + verify (hot path);if other cluster atomic counters
 *        landed in the same cache line, modifications elsewhere
 *        would invalidate this line on every reader CPU.
 *     2. spec-2.29 reconfig coordinator metadata (pid / quorum_state /
 *        pending_acks_bitmap) gets 48 reserved bytes to extend in
 *        place without cross-file ABI churn.
 *
 *   shmem layout != catalog ABI (postmaster restart rebuilds shmem;
 *   no datadir compatibility implication).
 */
typedef struct ClusterEpochShmem {
	pg_atomic_uint64 current_epoch;
	pg_atomic_uint64 epoch_changed_at_lsn; /* spec-2.29;reserved 0 here */
	uint8 _reserved[48];				   /* spec-2.29 coordinator metadata */
} ClusterEpochShmem;

StaticAssertDecl(sizeof(ClusterEpochShmem) == 64,
				 "ClusterEpochShmem must be exactly 64 bytes (cache-line + reserved)");

static ClusterEpochShmem *cluster_epoch_state = NULL;


Size
cluster_epoch_shmem_size(void)
{
	return sizeof(ClusterEpochShmem);
}

void
cluster_epoch_shmem_init(void)
{
	bool found;

	cluster_epoch_state
		= ShmemInitStruct("pgrac cluster epoch", cluster_epoch_shmem_size(), &found);
	if (!found) {
		/*
		 * spec-3.16 D4 (recovery contract): crash recovery resets the epoch to
		 * CLUSTER_EPOCH_INITIAL here (postmaster restart rebuilds shmem;
		 * !found).  WAL redo NEVER advances the epoch -- only spec-2.29
		 * reconfig broadcast does.  So after a single-instance crash recovery
		 * the epoch is 0, snapshots taken post-recovery carry read_epoch == 0
		 * == current, and the spec-3.3 epoch fence (heapam_visibility.c) does
		 * NOT false-trip.  Multi-node epoch reconstruction across a crash is a
		 * reconfig-protocol concern (spec-2.29 / #95), forward-linked.
		 */
		pg_atomic_init_u64(&cluster_epoch_state->current_epoch, CLUSTER_EPOCH_INITIAL);
		pg_atomic_init_u64(&cluster_epoch_state->epoch_changed_at_lsn, 0);
		memset(cluster_epoch_state->_reserved, 0, sizeof(cluster_epoch_state->_reserved));
	}
}

uint64
cluster_epoch_get_current(void)
{
	if (cluster_epoch_state == NULL)
		return CLUSTER_EPOCH_INITIAL;
	return pg_atomic_read_u64(&cluster_epoch_state->current_epoch);
}

uint64
cluster_epoch_get_changed_at_lsn(void)
{
	if (cluster_epoch_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_epoch_state->epoch_changed_at_lsn);
}

/*
 * spec-2.29 D18: cluster_epoch_advance_for_reconfig
 *
 *	  Coordinator-only path.  Atomic CAS-loop increment by 1.
 *	  CAS-loop (not pg_atomic_fetch_add_u64) so that we can return
 *	  the pre-CAS old value cleanly back to caller for publish;
 *	  fetch_add doesn't expose pre value with same atomicity
 *	  guarantee in PG's port/atomics interface.
 *
 *	  Defensive CAS-loop also handles the unlikely case of two LMON
 *	  ticks racing during a deterministic-coordinator switch
 *	  mid-tick — both compute self==coordinator, both call this
 *	  function;CAS ensures epoch advances exactly once per call
 *	  attempt, never lost-update.
 */
void
cluster_epoch_advance_for_reconfig(uint64 *old_out, uint64 *new_out)
{
	uint64 old_val;

	Assert(old_out != NULL && new_out != NULL);

	if (cluster_epoch_state == NULL) {
		/* Caller invoked before postmaster shmem init — should
		 * never happen on the LMON tick path, but defensive. */
		*old_out = CLUSTER_EPOCH_INITIAL;
		*new_out = CLUSTER_EPOCH_INITIAL;
		return;
	}

	for (;;) {
		old_val = pg_atomic_read_u64(&cluster_epoch_state->current_epoch);
		if (pg_atomic_compare_exchange_u64(&cluster_epoch_state->current_epoch, &old_val,
										   old_val + 1))
			break;
		/* CAS lost — re-read and retry */
	}

	*old_out = old_val;
	*new_out = old_val + 1;
}

/*
 * spec-5.13 D3 (CL-I3): cluster_epoch_advance_for_reconfig_if_baseline
 *
 *	  Guarded variant of the coordinator advance: bump the epoch to
 *	  baseline+1 ONLY if the current epoch still equals `baseline`.
 *	  Single compare_exchange (no retry loop): a CAS failure here means
 *	  some OTHER reconfig (a real death) already moved the epoch off the
 *	  baseline the clean-leave committed against, so the leave must NOT
 *	  commit on a stale view — return false and let the leaving node
 *	  observe the foreign event and escalate to fail-stop (CL-I3).  This
 *	  closes the check-then-bump TOCTOU in cl_coordinator_commit that is
 *	  benign at 2 nodes (no third party can bump) but unsafe at >=3.
 *
 *	  Returns true iff the epoch advanced (current was baseline); on true,
 *	  *new_out = baseline + 1.  On false the epoch is left untouched.
 */
bool
cluster_epoch_advance_for_reconfig_if_baseline(uint64 baseline, uint64 *new_out)
{
	uint64 expected = baseline;

	Assert(new_out != NULL);

	if (cluster_epoch_state == NULL) {
		*new_out = CLUSTER_EPOCH_INITIAL;
		return false;
	}

	if (pg_atomic_compare_exchange_u64(&cluster_epoch_state->current_epoch, &expected,
									   baseline + 1)) {
		*new_out = baseline + 1;
		return true;
	}

	/* `expected` now holds the racing current epoch; leave it untouched. */
	*new_out = expected;
	return false;
}

void
cluster_epoch_set_changed_at_lsn(uint64 lsn)
{
	if (cluster_epoch_state == NULL)
		return;
	pg_atomic_write_u64(&cluster_epoch_state->epoch_changed_at_lsn, lsn);
}

/*
 * spec-2.29 D18b: cluster_epoch_observe_remote
 *
 *	  CAS-loop max-merge.  Single-shot advance: caller (envelope
 *	  verify body) supplies remote_epoch from a verified inbound
 *	  envelope;we CAS local current_epoch up to remote_epoch if
 *	  and only if local < remote, otherwise no-op.
 *
 *	  Returns true iff CAS succeeded (local advanced).  False iff
 *	  local >= remote already (stale or equal observe — no-op).
 *
 *	  CAS-loop guards against concurrent observe_remote from
 *	  multiple envelope-receiving paths (cluster_ic_tier1 + future
 *	  RDMA tier) racing — at most one observe_remote succeeds for
 *	  any given remote_epoch, but other peers' newer observes can
 *	  still progress.
 *
 *	  Caller MUST gate on remote_epoch - my_epoch <=
 *	  CLUSTER_EPOCH_OBSERVE_MAX_JUMP (per spec-2.29 D20 envelope
 *	  verify path) BEFORE calling this function — hostile-spoof
 *	  defense lives at envelope receive site so the frame can be
 *	  DROP_NO_CLOSE'd cleanly with stats bump rather than silently
 *	  capped here.
 */
bool
cluster_epoch_observe_remote(uint64 remote_epoch)
{
	uint64 cur_val;

	if (cluster_epoch_state == NULL)
		return false;

	for (;;) {
		cur_val = pg_atomic_read_u64(&cluster_epoch_state->current_epoch);
		if (cur_val >= remote_epoch)
			return false; /* monotonic — never retreat */
		if (pg_atomic_compare_exchange_u64(&cluster_epoch_state->current_epoch, &cur_val,
										   remote_epoch))
			return true;
		/* CAS lost — re-read and retry */
	}
}

static const ClusterShmemRegion cluster_epoch_region = {
	.name = "pgrac cluster epoch",
	.size_fn = cluster_epoch_shmem_size,
	.init_fn = cluster_epoch_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_epoch",
	.reserved_flags = 0,
};

void
cluster_epoch_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_epoch_region);
}

#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stub.  Same symbol surface, returns
 * CLUSTER_EPOCH_INITIAL unconditionally.  Required for envelope
 * code paths that include cluster_epoch.h in disable mode.
 */
uint64
cluster_epoch_get_current(void)
{
	return CLUSTER_EPOCH_INITIAL;
}

uint64
cluster_epoch_get_changed_at_lsn(void)
{
	return 0;
}

void
cluster_epoch_advance_for_reconfig(uint64 *old_out, uint64 *new_out)
{
	if (old_out != NULL)
		*old_out = CLUSTER_EPOCH_INITIAL;
	if (new_out != NULL)
		*new_out = CLUSTER_EPOCH_INITIAL;
}

bool
cluster_epoch_advance_for_reconfig_if_baseline(uint64 baseline pg_attribute_unused(),
											   uint64 *new_out)
{
	if (new_out != NULL)
		*new_out = CLUSTER_EPOCH_INITIAL;
	return false;
}

void
cluster_epoch_set_changed_at_lsn(uint64 lsn pg_attribute_unused())
{
	/* no-op stub */
}

bool
cluster_epoch_observe_remote(uint64 remote_epoch pg_attribute_unused())
{
	return false;
}

#endif /* USE_PGRAC_CLUSTER */
