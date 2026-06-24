/*-------------------------------------------------------------------------
 *
 * cluster_hw_shmem.c
 *	  HW (relation extend) cluster block-number authority -- master-side HWM
 *	  table (spec-5.7a D1, §3.1a).
 *
 *	  The authoritative high-water mark for each (rel,fork) lives here, on the
 *	  HW enqueue master for that resource.  A backend holding HW(X) asks the
 *	  master for a block range via the IC HW_ALLOC message; the master advances
 *	  the HWM under this region's lock, durably reserves the advance in WAL
 *	  (HW_RESERVE, XLogFlush before reply), and replies the range.  The HWM is
 *	  NEVER derived from a running node's FileSize (L368); the authority owns
 *	  every relation from block 0 (amend #3: no seed), so HW_RESERVE is the only
 *	  durable HWM origin.  After a crash / remaster the table is rebuilt from the
 *	  durable snapshot (cluster_hw_snapshot, §3.1b) plus the HW_RESERVE WAL tail
 *	  (cluster_hw_apply_hwm), never from FileSize and never seeded 0.
 *
 *	  This file holds the in-memory HWM table + its lock + the observability
 *	  counters, plus the checkpoint-capture / recovery-load orchestration that
 *	  drives the durable snapshot (cluster_hw_snapshot.c does the file I/O).  The
 *	  pure allocation math lives in cluster_hw.c; the HW_RESERVE emit + XLogFlush
 *	  and the IC request/reply orchestration live in cluster_undo_xlog.c /
 *	  cluster_hw_ic.c.
 *
 *	  Lock granularity: a single region LWLock guards the whole HWM table.  An
 *	  entry is touched once per extend batch (>= 1 block, up to 64 for bulk), not
 *	  per row, so contention is low; a partitioned lock is a forward optimisation
 *	  (perf forward spec-5.19a).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw_shmem.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D1/D3, §3.1a / §3.1b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h" /* GetSystemIdentifier */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_grd.h" /* PGRAC_GRD_SHARD_COUNT, shard phase/generation */
#include "cluster/cluster_guc.h" /* cluster_node_id, cluster_shared_data_dir */
#include "cluster/cluster_hw.h"
#include "cluster/cluster_hw_snapshot.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* CLUSTER_HW_AUTHORITY_MAX (authority/snapshot capacity) is shared via cluster_hw.h. */

/*
 * ClusterHwShared -- the HW region header: one LWLock + six counters + the
 * per-GRD-shard "HW rebuilt for this remaster generation" vector.
 *
 *	hw_rebuilt_generation[shard] is the cluster_grd master_generation the HW
 *	authority last rebuilt that shard's HWMs for (§3.1b R4/R9).  The serve gate
 *	(cluster_hw_try_advance) compares it to cluster_grd_shard_master_generation:
 *	when the shard was just remastered (P4 bumped the master generation) but the
 *	HW rebuild has not run, the two differ and serving fails closed -- so a
 *	survivor never auto-creates an adopted (rel,fork) at block 0 over an already
 *	allocated range.  It is a per-shard lock-free atomic: the online-remaster
 *	rebuild is the only writer (one shard at a time, before unfreeze); master
 *	HW_ALLOC handlers read it.  A never-remastered shard reads 0, matching the
 *	GRD master_generation's 0, so boot / steady state serves normally.
 */
typedef struct ClusterHwShared {
	LWLock lwlock; /* guards the authority HTAB */
	/*
	 * snapshot_write_lock (§3.1b R10 single-writer): serializes this owner's
	 * checkpoint snapshot write and online-remaster adoption snapshot write.
	 * Both stage to the SAME per-owner temp path (cluster_hw_snapshot.c writes
	 * <basename>.<owner>.tmp), so concurrent writers would clobber the temp into
	 * a torn image.  Held across the htab capture + durable_rename; the brief
	 * capture nests the region SHARED lock inside, so HW_ALLOC is never blocked
	 * for the file I/O.  Distinct from lwlock so a long snapshot write does not
	 * stall the authority.
	 */
	LWLock snapshot_write_lock;
	pg_atomic_uint64 alloc_count;			 /* HW_ALLOC batches granted */
	pg_atomic_uint64 authority_create_count; /* (rel,fork) entries established at first sight */
	pg_atomic_uint64 reserve_wal_count;		 /* HW_RESERVE records emitted (handler) */
	pg_atomic_uint64 rebuild_count;			 /* entries created/raised by WAL redo */
	pg_atomic_uint64 failclosed_count;		 /* 53RA6 fail-closed (handler) */
	pg_atomic_uint64 not_ready_count;		 /* 53RA6 serve-gate (shard adopted, unrebuilt) */
	pg_atomic_uint64 remaster_done_count;	 /* online-remaster HW rebuild DONE (S5/S7) */
	pg_atomic_uint64 remaster_blocked_count; /* online-remaster HW rebuild fail-closed (S5/S7) */
	pg_atomic_uint32 hw_rebuilt_generation[PGRAC_GRD_SHARD_COUNT]; /* §3.1b R4/R9 gate */
	/*
	 * remaster_launched_episode[node] -- the reconfig episode for which the GRD
	 * FSM last launched an online-remaster rebuild worker for dead origin `node`
	 * (§3.1b R4 / S5d).  Launch idempotency only: the FSM (single LMON writer)
	 * launches once per episode per dead origin and skips while the value already
	 * equals the current episode.  The worker does NOT read it (it captures the
	 * live episode itself for its staleness guard), so there is no FSM<->worker
	 * race on this field.
	 */
	pg_atomic_uint64 remaster_launched_episode[CLUSTER_MAX_NODES];
} ClusterHwShared;

/*
 * ClusterHwEntry -- one (rel,fork) authority entry.  An entry is created on
 * first sight (established at the requester's forced re-stat size -- §3.1c
 * Q14/Q17) or by a snapshot load / HW_RESERVE redo; next_hwm is the
 * authoritative next free block.
 */
typedef struct ClusterHwEntry {
	ClusterResId resid;	  /* HTAB key (must be first; HASH_BLOBS) */
	BlockNumber next_hwm; /* authoritative next free block */
} ClusterHwEntry;

/*
 * ClusterHwReplyKey / ClusterHwReplyEntry -- the requester-side HW_ALLOC reply
 * mailbox, keyed by (source_procno, request_id).  A backend extends one
 * (rel,fork) at a time while holding HW(X), so there is at most one in-flight
 * HW_ALLOC per backend; the HTAB cap is therefore MaxBackends.  The reply
 * handler (LMON) delivers (first_block, granted, status) and wakes the waiter.
 */
typedef struct ClusterHwReplyKey {
	uint32 source_procno; /* MyProc->pgprocno of the waiter */
	uint32 _pad;
	uint64 request_id; /* per-backend monotonic; disambiguates a stale reply */
} ClusterHwReplyKey;

typedef struct ClusterHwReplyEntry {
	ClusterHwReplyKey key; /* HTAB key (must be first; HASH_BLOBS) */
	uint32 first_block;
	uint32 granted;
	uint32 status;	   /* HwAllocReplyStatus */
	uint32 ready;	   /* 0 = waiting, 1 = delivered */
	int waiter_procno; /* SetLatch target */
} ClusterHwReplyEntry;

static ClusterHwShared *hw_state = NULL;
static HTAB *hw_htab = NULL;
static HTAB *hw_reply_htab = NULL;

static const ClusterShmemRegion cluster_hw_region = {
	.name = "pgrac cluster hw",
	.size_fn = cluster_hw_shmem_size,
	.init_fn = cluster_hw_shmem_init,
	.lwlock_count = 2, /* region lock + snapshot_write_lock (informational) */
	.owner_subsys = "spec-5.7 HW relation extend authority",
	.reserved_flags = 0,
};


/* ============================================================
 * Shmem region size / init / register.
 * ============================================================ */

Size
cluster_hw_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterHwShared));

	sz = add_size(sz, hash_estimate_size((Size)CLUSTER_HW_AUTHORITY_MAX, sizeof(ClusterHwEntry)));
	sz = add_size(sz, hash_estimate_size((Size)MaxBackends, sizeof(ClusterHwReplyEntry)));
	return sz;
}

void
cluster_hw_shmem_init(void)
{
	bool found;
	HASHCTL hctl;

	hw_state = (ClusterHwShared *)ShmemInitStruct("pgrac cluster hw",
												  MAXALIGN(sizeof(ClusterHwShared)), &found);

	if (!IsUnderPostmaster) {
		int s;

		LWLockInitialize(&hw_state->lwlock, LWTRANCHE_CLUSTER_HW);
		LWLockInitialize(&hw_state->snapshot_write_lock, LWTRANCHE_CLUSTER_HW);
		pg_atomic_init_u64(&hw_state->alloc_count, 0);
		pg_atomic_init_u64(&hw_state->authority_create_count, 0);
		pg_atomic_init_u64(&hw_state->reserve_wal_count, 0);
		pg_atomic_init_u64(&hw_state->rebuild_count, 0);
		pg_atomic_init_u64(&hw_state->failclosed_count, 0);
		pg_atomic_init_u64(&hw_state->not_ready_count, 0);
		pg_atomic_init_u64(&hw_state->remaster_done_count, 0);
		pg_atomic_init_u64(&hw_state->remaster_blocked_count, 0);
		/* No shard rebuilt yet; 0 matches a never-remastered shard's GRD
		 * master_generation (0), so boot / steady state serves. */
		for (s = 0; s < PGRAC_GRD_SHARD_COUNT; s++)
			pg_atomic_init_u32(&hw_state->hw_rebuilt_generation[s], 0);
		for (s = 0; s < CLUSTER_MAX_NODES; s++)
			pg_atomic_init_u64(&hw_state->remaster_launched_episode[s], 0);
	}

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(ClusterResId);
	hctl.entrysize = sizeof(ClusterHwEntry);
	hw_htab = ShmemInitHash("pgrac cluster hw authority", CLUSTER_HW_AUTHORITY_MAX,
							CLUSTER_HW_AUTHORITY_MAX, &hctl, HASH_ELEM | HASH_BLOBS);

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(ClusterHwReplyKey);
	hctl.entrysize = sizeof(ClusterHwReplyEntry);
	hw_reply_htab = ShmemInitHash("pgrac cluster hw reply wait", MaxBackends, MaxBackends, &hctl,
								  HASH_ELEM | HASH_BLOBS);
}

void
cluster_hw_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_hw_region);
}


/* ============================================================
 * Master-side authority primitives.
 * ============================================================ */

ClusterHwStatus
cluster_hw_try_advance(const ClusterResId *resid, uint32 want, BlockNumber seed_nblocks,
					   BlockNumber *first, uint32 *granted, BlockNumber *new_hwm)
{
	ClusterHwEntry *entry;
	ClusterHwStatus status;
	bool found;
	bool created = false;
	uint32 shard;

	Assert(resid != NULL && first != NULL && granted != NULL && new_hwm != NULL);
	Assert(want > 0);

	if (hw_htab == NULL)
		return CLUSTER_HW_FULL; /* shmem absent: caller fails closed */

	/*
	 * §3.1b R4/R6 serve gate (8.A).  Before touching the authority table -- and
	 * especially before auto-creating an absent (rel,fork) at block 0 -- prove
	 * the resid's GRD shard is steady for its current remaster generation: phase
	 * NORMAL and the HW authority rebuilt this shard for that generation.  A
	 * survivor that just adopted the shard (P4 bumped the master generation but
	 * the snapshot+tail rebuild has not run, so hw_rebuilt_generation lags) must
	 * fail closed -- creating at 0 would re-hand an already-allocated range (R9).
	 * The reads are lock-free atomics, so this runs before the region lock.
	 */
	shard = cluster_grd_shard_for_resource(resid);
	if (!cluster_hw_serve_allowed(cluster_grd_shard_phase(shard),
								  cluster_grd_shard_master_generation(shard),
								  cluster_hw_shard_rebuilt_generation(shard))) {
		cluster_hw_bump_not_ready();
		return CLUSTER_HW_NOT_READY;
	}

	LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
	entry = (ClusterHwEntry *)hash_search(hw_htab, resid, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		status = CLUSTER_HW_FULL; /* authority HTAB full */
	} else {
		uint32 g;
		BlockNumber nh;
		BlockNumber f;

		if (!found) {
			/*
			 * §3.1c Q14/Q17 (v1.4 amend): establish the first-sight HWM at the
			 * requester's forced re-stat of the relation's true committed size
			 * (seed_nblocks), NOT at block 0.  The requester holds HW(X), so the
			 * size is quiescent and -- on cluster_fs -- physically coherent (the
			 * shared file's EOF reflects every committed extend; Q14).  This owns
			 * a relation whose first blocks were created by a non-authority path
			 * (private build, sequence create) from its real EOF, never re-handing
			 * an allocated block.  After this point the running counter +
			 * HW_RESERVE WAL is the sole authority -- FileSize is never read again
			 * for this resid.  The serve gate above already failed closed if this
			 * shard's HWMs were not rebuilt (so a missing entry here is genuinely
			 * unestablished, never a lost/corrupt entry -- R6).
			 */
			entry->next_hwm = seed_nblocks;
			created = true;
		}
		f = cluster_hw_alloc_segment(entry->next_hwm, want, &g, &nh);
		if (g == 0) {
			status = CLUSTER_HW_EXHAUSTED;
		} else {
			entry->next_hwm = nh;
			*first = f;
			*granted = g;
			*new_hwm = nh;
			status = CLUSTER_HW_OK;
		}
	}
	LWLockRelease(&hw_state->lwlock);

	if (created)
		cluster_hw_bump_authority_create(); /* counter: a new (rel,fork) authority entry */
	if (status == CLUSTER_HW_OK)
		cluster_hw_bump_alloc();
	return status;
}

void
cluster_hw_apply_hwm(const ClusterResId *resid, BlockNumber hwm)
{
	ClusterHwEntry *entry;
	bool found;

	Assert(resid != NULL);

	if (hw_htab == NULL)
		return;

	LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
	entry = (ClusterHwEntry *)hash_search(hw_htab, resid, HASH_ENTER_NULL, &found);
	if (entry != NULL) {
		/* Monotone raise-to-max (R3/R11): redo / snapshot load never lowers the
		 * HWM.  Single-sourced with the snapshot rebuild kernel so "never below
		 * any replied end" has one tested definition. */
		if (!found)
			entry->next_hwm = hwm;
		else
			entry->next_hwm = cluster_hw_snapshot_rebuild_value(entry->next_hwm, hwm);
	}
	LWLockRelease(&hw_state->lwlock);

	cluster_hw_bump_rebuild();
}

/*
 * cluster_hw_apply_snapshot -- recovery rebuild, snapshot half (§3.1b R3).
 *
 *	Load each (resid, next_hwm) pair of a deserialized, identity-validated
 *	snapshot into the authority table via the monotone cluster_hw_apply_hwm.
 *	This is the snapshot_lsn-time HWM; the redo handler then replays the
 *	HW_RESERVE tail (lsn >= snapshot_lsn) on top, so the final HWM is
 *	max(snapshot, tail) -- never below any range the dead master already
 *	replied (R5/R11).  Caller has already validated the envelope and identity
 *	(cluster_hw_snapshot_deserialize + cluster_hw_snapshot_identity_ok); a
 *	missing / untrusted snapshot must fail closed before reaching here, never
 *	rebuild from zero.
 */
void
cluster_hw_apply_snapshot(const ClusterHwSnapshotEntry *entries, uint32 n_entries)
{
	uint32 i;

	Assert(n_entries == 0 || entries != NULL);

	for (i = 0; i < n_entries; i++)
		cluster_hw_apply_hwm(&entries[i].resid, entries[i].next_hwm);
}

/*
 * cluster_hw_shard_rebuilt_generation / cluster_hw_mark_shard_rebuilt -- the
 * per-GRD-shard "HW rebuilt for this remaster generation" gate field (§3.1b
 * R4/R9; see ClusterHwShared).  The reader is the serve gate; the writer is the
 * online-remaster rebuild, which records the generation it rebuilt the shard's
 * HWMs for AFTER the dead master's snapshot+tail is applied and the adoption
 * snapshot is durable, just before the shard unfreezes (R9 order step ③).
 */
uint32
cluster_hw_shard_rebuilt_generation(uint32 shard_id)
{
	if (hw_state == NULL || shard_id >= PGRAC_GRD_SHARD_COUNT)
		return 0;
	return pg_atomic_read_u32(&hw_state->hw_rebuilt_generation[shard_id]);
}

void
cluster_hw_mark_shard_rebuilt(uint32 shard_id, uint32 generation)
{
	if (hw_state == NULL || shard_id >= PGRAC_GRD_SHARD_COUNT)
		return;
	pg_atomic_write_u32(&hw_state->hw_rebuilt_generation[shard_id], generation);
}

/*
 * cluster_hw_remaster_launched_episode / cluster_hw_remaster_set_launched -- the
 * per-dead-origin online-remaster launch-idempotency field (S5d).  The GRD FSM
 * launches a rebuild worker for a dead origin once per episode and records the
 * episode here; a later tick skips while the recorded value equals the current
 * episode.  0 (never launched / reverted on a registration failure) always
 * relaunches.
 */
uint64
cluster_hw_remaster_launched_episode(int node_id)
{
	if (hw_state == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;
	return pg_atomic_read_u64(&hw_state->remaster_launched_episode[node_id]);
}

void
cluster_hw_remaster_set_launched(int node_id, uint64 episode)
{
	if (hw_state == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_write_u64(&hw_state->remaster_launched_episode[node_id], episode);
}


/* ============================================================
 * Checkpoint write + recovery load (§3.1b R1/R2; PG-core hooked).
 * ============================================================ */

/*
 * cluster_hw_authority_active -- is the HW authority + its durable snapshot in
 * play?  Only in a multi-node cluster backed by shared storage: that is the
 * only configuration where a relation extend is globalized (D2) and where a
 * survivor must read a dead master's snapshot.  Single-node is a complete
 * no-op, so the checkpoint write and the recovery load never perturb a
 * single-node start.
 */
bool
cluster_hw_authority_active(void)
{
	return cluster_shared_data_dir != NULL && cluster_shared_data_dir[0] != '\0'
		   && cluster_conf_node_count() > 1;
}

/*
 * cluster_hw_snapshot_checkpoint_write -- §3.1b R1/R2: persist this node's
 * authority HWM table, bound to the checkpoint whose redo LSN is `redo_lsn`.
 *
 *	Captures the hw_htab under the region lock (a consistent point-in-time copy;
 *	the lock excludes concurrent HW_ALLOC advances), then writes the per-master
 *	snapshot file outside the lock.  snapshot_lsn = redo_lsn: every HW_RESERVE
 *	with lsn < redo_lsn is already reflected in the copy (advance precedes its
 *	WAL insert, R13), so the snapshot dominates all pre-redo reservations and
 *	recovery replays only the lsn >= redo_lsn tail on top.  Called from
 *	CreateCheckPoint before the checkpoint record, so a completed checkpoint
 *	always has a durable snapshot.  full per-master (Q13=A): shard == owner.
 */
/*
 * hw_snapshot_capture_and_write -- shared capture + durable write for both
 * snapshot triggers (R1/R9/R10/R13).  Holds snapshot_write_lock so this owner's
 * checkpoint write and adoption write never interleave on the shared temp path
 * (R10 single-writer).  snapshot_lsn is fixed by the caller BEFORE the htab copy
 * (R13): a reservation whose HW_RESERVE lsn < snapshot_lsn already shows in the
 * copy (cluster_hw_try_advance advances next_hwm before cluster_hw_emit_reserve),
 * while lsn >= snapshot_lsn is in the replayed tail -- every reservation lands in
 * snapshot OR tail, never lost.  full per-master (Q13=A): shard == owner.
 */
static void
hw_snapshot_capture_and_write(ClusterHwSnapshotKind kind, XLogRecPtr snapshot_lsn,
							  uint32 generation)
{
	ClusterHwSnapshotEntry *entries;
	HASH_SEQ_STATUS seq;
	const ClusterHwEntry *e;
	uint32 n = 0;
	uint32 self;

	if (hw_htab == NULL || hw_state == NULL)
		return;

	self = (uint32)cluster_node_id;
	entries = (ClusterHwSnapshotEntry *)palloc(sizeof(ClusterHwSnapshotEntry)
											   * CLUSTER_HW_AUTHORITY_MAX);

	LWLockAcquire(&hw_state->snapshot_write_lock, LW_EXCLUSIVE);

	LWLockAcquire(&hw_state->lwlock, LW_SHARED);
	hash_seq_init(&seq, hw_htab);
	while ((e = (ClusterHwEntry *)hash_seq_search(&seq)) != NULL) {
		entries[n].resid = e->resid;
		entries[n].next_hwm = e->next_hwm;
		n++;
	}
	LWLockRelease(&hw_state->lwlock);

	cluster_hw_snapshot_write(self, self, generation, snapshot_lsn, GetSystemIdentifier(), kind,
							  entries, n);

	LWLockRelease(&hw_state->snapshot_write_lock);
	pfree(entries);
}

void
cluster_hw_snapshot_checkpoint_write(XLogRecPtr redo_lsn)
{
	hw_snapshot_capture_and_write(CLUSTER_HW_SNAPSHOT_CHECKPOINT, redo_lsn,
								  (uint32)cluster_epoch_get_current());
}

/*
 * cluster_hw_snapshot_adoption_write -- §3.1b R9: durably persist this survivor's
 * full authority table as an ADOPTION snapshot, AFTER an online-remaster rebuild
 * has applied the dead master's snapshot+tail into the htab and BEFORE the shard
 * is marked rebuilt / unfrozen.  Collapses the inherited HWM lineage into this
 * owner's durable anchor so a later remaster reads one snapshot (no ancestor-tail
 * dependency).  snapshot_lsn = the current insert LSN at rebuild-complete: the
 * adopted resids are not served until after this returns (R9 order), so no
 * concurrent HW_ALLOC advances them past snapshot_lsn during the copy.  Shares
 * the R10 single-writer lock with the checkpoint write.  PANICs on a shared-
 * storage I/O failure (mirrors the checkpoint snapshot write / update_controlfile);
 * the rebuild's own logic failures (missing/corrupt dead snapshot, WAL gap) are
 * the result-returning fail-closed (53RA6) path in cluster_hw_remaster.c.
 */
void
cluster_hw_snapshot_adoption_write(void)
{
	hw_snapshot_capture_and_write(CLUSTER_HW_SNAPSHOT_ADOPTION, GetXLogInsertRecPtr(),
								  (uint32)cluster_epoch_get_current());
}

/*
 * cluster_hw_snapshot_recovery_load -- §3.1b R3/R6: load this node's authority
 * snapshot into the hw_htab at the start of recovery, before the HW_RESERVE
 * redo replays the tail on top (rebuild = max(snapshot, tail)).
 *
 *	A VALID snapshot is applied (monotone raise).  A MISSING file (INVALID_
 *	SHORT) starts empty and lets the WAL tail rebuild: a missing snapshot
 *	implies no authority-active checkpoint completed (CreateCheckPoint writes
 *	the snapshot before the checkpoint record), so no HW_RESERVE precedes the
 *	recovery redo point and the tail replay covers them all -- and a first
 *	multi-node start legitimately has none.  A present-but-corrupt (CRC) or
 *	foreign (IDENTITY) snapshot FATALs (R6): a present file implies a completed
 *	checkpoint, so pre-redo HWMs may exist that the tail alone cannot recover;
 *	never trust it, never rebuild from zero or FileSize.
 */
void
cluster_hw_snapshot_recovery_load(void)
{
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotEntry *entries;
	ClusterHwSnapshotValidity v;
	uint32 self;

	if (hw_htab == NULL || !cluster_hw_authority_active())
		return;

	self = (uint32)cluster_node_id;
	entries = (ClusterHwSnapshotEntry *)palloc(sizeof(ClusterHwSnapshotEntry)
											   * CLUSTER_HW_AUTHORITY_MAX);

	v = cluster_hw_snapshot_read(self, GetSystemIdentifier(), self, self, &hdr, entries,
								 CLUSTER_HW_AUTHORITY_MAX);
	switch (v) {
	case CLUSTER_HW_SNAPSHOT_VALID:
		cluster_hw_apply_snapshot(entries, hdr.n_entries);
		ereport(LOG, (errmsg("cluster HW authority: loaded %u entries from node %u snapshot at "
							 "%X/%X",
							 hdr.n_entries, self, LSN_FORMAT_ARGS((XLogRecPtr)hdr.snapshot_lsn))));
		break;
	case CLUSTER_HW_SNAPSHOT_INVALID_SHORT:
		ereport(LOG, (errmsg("cluster HW authority: no snapshot for node %u; rebuilding from the "
							 "HW_RESERVE WAL tail",
							 self)));
		break;
	default:
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
				 errmsg("cluster HW authority snapshot for node %u is unreadable (status %d)", self,
						(int)v),
				 errhint("The shared HW authority snapshot is corrupt or belongs to a different "
						 "cluster.")));
	}
	pfree(entries);
}


/* ============================================================
 * Requester-side HW_ALLOC reply mailbox (keyed by source_procno + request_id).
 * ============================================================ */

void
cluster_hw_reply_slot_arm(uint64 request_id)
{
	ClusterHwReplyKey key;
	ClusterHwReplyEntry *entry;
	bool found;

	if (hw_reply_htab == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.source_procno = (uint32)MyProc->pgprocno;
	key.request_id = request_id;

	LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
	entry = (ClusterHwReplyEntry *)hash_search(hw_reply_htab, &key, HASH_ENTER_NULL, &found);
	if (entry != NULL) {
		entry->first_block = InvalidBlockNumber;
		entry->granted = 0;
		entry->status = HW_ALLOC_REPLY_FAIL_INTERNAL;
		entry->ready = 0;
		entry->waiter_procno = MyProc->pgprocno;
	}
	/* entry == NULL (HTAB full): wait will not find it and times out -> fail closed. */
	LWLockRelease(&hw_state->lwlock);
}

bool
cluster_hw_reply_slot_wait(uint64 request_id, long timeout_ms, HwAllocReply *out)
{
	ClusterHwReplyKey key;
	TimestampTz deadline;
	bool got = false;

	Assert(out != NULL);
	if (hw_reply_htab == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.source_procno = (uint32)MyProc->pgprocno;
	key.request_id = request_id;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

	for (;;) {
		const ClusterHwReplyEntry *entry;
		bool ready = false;
		long ms;
		TimestampTz now;

		LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
		entry = (ClusterHwReplyEntry *)hash_search(hw_reply_htab, &key, HASH_FIND, NULL);
		if (entry != NULL && entry->ready) {
			out->request_id = request_id;
			out->first_block = entry->first_block;
			out->granted = entry->granted;
			out->status = entry->status;
			out->source_procno = key.source_procno;
			hash_search(hw_reply_htab, &key, HASH_REMOVE, NULL);
			ready = true;
		}
		LWLockRelease(&hw_state->lwlock);

		if (ready) {
			got = true;
			break;
		}

		now = GetCurrentTimestamp();
		if (now >= deadline)
			break;
		ms = (long)((deadline - now) / 1000);
		if (ms <= 0)
			ms = 1;

		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, ms,
						WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	if (!got) {
		/* timeout / interrupt: remove the entry so a late reply is dropped. */
		LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
		hash_search(hw_reply_htab, &key, HASH_REMOVE, NULL);
		LWLockRelease(&hw_state->lwlock);
	}
	return got;
}

void
cluster_hw_reply_slot_deliver(const HwAllocReply *reply)
{
	ClusterHwReplyKey key;
	ClusterHwReplyEntry *entry;
	int waiter = -1;

	Assert(reply != NULL);
	if (hw_reply_htab == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.source_procno = reply->source_procno;
	key.request_id = reply->request_id;

	LWLockAcquire(&hw_state->lwlock, LW_EXCLUSIVE);
	entry = (ClusterHwReplyEntry *)hash_search(hw_reply_htab, &key, HASH_FIND, NULL);
	if (entry != NULL && !entry->ready) {
		entry->first_block = reply->first_block;
		entry->granted = reply->granted;
		entry->status = reply->status;
		entry->ready = 1;
		waiter = entry->waiter_procno;
	}
	LWLockRelease(&hw_state->lwlock);

	if (waiter >= 0)
		SetLatch(&GetPGProcByNumber(waiter)->procLatch);
}


/* ============================================================
 * Counters: bumps + accessors.
 * ============================================================ */

#define HW_BUMP(field)                                                                             \
	do {                                                                                           \
		if (hw_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&hw_state->field, 1);                                          \
	} while (0)

#define HW_READ(field) (hw_state != NULL ? pg_atomic_read_u64(&hw_state->field) : 0)

void
cluster_hw_bump_alloc(void)
{
	HW_BUMP(alloc_count);
}
void
cluster_hw_bump_authority_create(void)
{
	HW_BUMP(authority_create_count);
}
void
cluster_hw_bump_reserve_wal(void)
{
	HW_BUMP(reserve_wal_count);
}
void
cluster_hw_bump_rebuild(void)
{
	HW_BUMP(rebuild_count);
}
void
cluster_hw_bump_failclosed(void)
{
	HW_BUMP(failclosed_count);
}
void
cluster_hw_bump_not_ready(void)
{
	HW_BUMP(not_ready_count);
}
void
cluster_hw_bump_remaster_done(void)
{
	HW_BUMP(remaster_done_count);
}
void
cluster_hw_bump_remaster_blocked(void)
{
	HW_BUMP(remaster_blocked_count);
}

uint64
cluster_hw_alloc_count(void)
{
	return HW_READ(alloc_count);
}
uint64
cluster_hw_authority_create_count(void)
{
	return HW_READ(authority_create_count);
}
uint64
cluster_hw_reserve_wal_count(void)
{
	return HW_READ(reserve_wal_count);
}
uint64
cluster_hw_rebuild_count(void)
{
	return HW_READ(rebuild_count);
}
uint64
cluster_hw_failclosed_count(void)
{
	return HW_READ(failclosed_count);
}
uint64
cluster_hw_not_ready_count(void)
{
	return HW_READ(not_ready_count);
}
uint64
cluster_hw_remaster_done_count(void)
{
	return HW_READ(remaster_done_count);
}
uint64
cluster_hw_remaster_blocked_count(void)
{
	return HW_READ(remaster_blocked_count);
}
