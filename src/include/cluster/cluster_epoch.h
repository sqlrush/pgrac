/*-------------------------------------------------------------------------
 *
 * cluster_epoch.h
 *	  pgrac cluster membership epoch -- shmem-backed monotonic counter
 *	  carried by every cross-node IC envelope (Invariant 2 per
 *	  spec-2.0 §3.2).
 *
 *	  spec-2.4 期 epoch 永远 = CLUSTER_EPOCH_INITIAL = 0;
 *	  spec-2.29 reconfig 真激活 epoch++ broadcast.
 *
 *	  Shmem layout (per Q1 修订):
 *	    - 64-byte cache-line padding to prevent false sharing on the
 *	      hot atomic-read path (every envelope build + verify reads
 *	      current_epoch);
 *	    - 48 bytes reserved for spec-2.29 reconfig coordinator
 *	      metadata (pid / quorum_state / pending_acks_bitmap) so
 *	      reconfig can extend in place without cross-file ABI churn.
 *
 *	  shmem layout != catalog ABI:postmaster restart rebuilds shmem,
 *	  so shmem size / field changes do NOT require catversion bump.
 *	  Catversion is reserved for pg_proc / pg_class / system view
 *	  surface.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_epoch.h
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER);disable-cluster builds get a stub
 *	  cluster_epoch_get_current that returns CLUSTER_EPOCH_INITIAL
 *	  unconditionally so envelope-build code paths stay portable.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_EPOCH_H
#define CLUSTER_EPOCH_H

#include "c.h"

/*
 * Initial epoch value used at boot.  spec-2.4 期所有 envelope 写
 * CLUSTER_EPOCH_INITIAL;spec-2.29 reconfig 是首个真 ++ 该值的 spec.
 */
#define CLUSTER_EPOCH_INITIAL ((uint64)0)

/*
 * Snapshot read of the current cluster membership epoch.  Atomic
 * pg_atomic_read_u64 -- never blocks, safe in critical sections,
 * safe in handler / dispatch / send paths.
 */
extern uint64 cluster_epoch_get_current(void);

/*
 * Snapshot read of the LSN at which current_epoch last advanced.  Used
 * by SRF observability (pg_cluster_reconfig_state) + WAL replay.  0
 * iff epoch never advanced since postmaster start.
 */
extern uint64 cluster_epoch_get_changed_at_lsn(void);

/*
 * spec-2.29 D18:  Coordinator-only epoch advance.
 *
 *	  Atomically increments current_epoch by 1 via CAS-loop (defends
 *	  against the unlikely-but-possible race of two LMON ticks racing
 *	  during a deterministic-coordinator switch mid-tick).  Stores the
 *	  pre / post snapshots back to caller for publish to
 *	  pg_cluster_reconfig_state.
 *
 *	  MUST be called ONLY from the coordinator path of
 *	  cluster_reconfig_lmon_tick() — non-coordinator survivors must
 *	  observe via cluster_epoch_observe_remote (envelope piggyback
 *	  receive path).
 */
extern void cluster_epoch_advance_for_reconfig(uint64 *old_out, uint64 *new_out);

/*
 * spec-5.13 D3 (CL-I3):  Guarded coordinator advance — bump to baseline+1 only
 *	  if the current epoch still equals baseline (single CAS, no retry).  Returns
 *	  true iff it advanced; false means a concurrent reconfig (real death) already
 *	  moved the epoch, so a clean leave must escalate instead of committing stale.
 */
extern bool cluster_epoch_advance_for_reconfig_if_baseline(uint64 baseline, uint64 *new_out);

/*
 * spec-2.29 D18:  Coordinator-only setter for epoch_changed_at_lsn.
 *
 *	  Atomic pg_atomic_write_u64 — caller passes GetXLogInsertRecPtr()
 *	  immediately after cluster_epoch_advance_for_reconfig.  Used by
 *	  WAL replay + pg_cluster_reconfig_state SRF observability.
 */
extern void cluster_epoch_set_changed_at_lsn(uint64 lsn);

/*
 * spec-2.29 D18b:  Defense-bounded CAS-loop max-merge observe.
 *
 *	  Called from cluster_ic_envelope_verify (spec-2.29 D20) when an
 *	  inbound envelope carries env_epoch > my_epoch (newer epoch from
 *	  a coordinator's prior tick).  CAS-loops local current_epoch up
 *	  to remote_epoch monotonically — never retreats.  Caller MUST
 *	  have completed CRC + auth + source_node_id verify before
 *	  invoking observe_remote (per spec-2.4 §2.7 contract +
 *	  spec-2.29 I9 envelope-observe-after-verify invariant).
 *
 *	  Returns true iff a CAS actually succeeded (epoch was advanced).
 *	  Returns false if my_epoch >= remote_epoch already (no-op).
 *
 *	  Hostile-spoof defense: caller is responsible for checking
 *	  remote_epoch - my_epoch <= CLUSTER_EPOCH_OBSERVE_MAX_JUMP BEFORE
 *	  calling this function;observe_remote itself does NOT check —
 *	  the bound check happens at the envelope verify site so the
 *	  hostile frame can be dropped cleanly (DROP_NO_CLOSE) rather
 *	  than silently capped here.
 */
extern bool cluster_epoch_observe_remote(uint64 remote_epoch);

/*
 * spec-2.29 D18b:  Maximum permitted epoch jump per single envelope.
 *
 *	  envelope receive path rejects (DROP_NO_CLOSE +
 *	  unreasonable_epoch_jump counter) any frame where
 *	  env_epoch - my_epoch > MAX_JUMP.  Bound is conservative —
 *	  legitimate reconfig advances epoch by exactly 1 per event;
 *	  a peer ahead by 16 events means LMON tick missed 16
 *	  CSSD DEAD edges in this node, which is itself a serious
 *	  liveness gap warranting log + bump rather than silent catchup.
 */
#define CLUSTER_EPOCH_OBSERVE_MAX_JUMP ((uint64)16)

/*
 * Shmem registration.  Called from cluster_init_shmem() in
 * postmaster phase 1.  Size returned by cluster_epoch_shmem_size
 * is added to the cluster-subsystem shmem RequestAddinShmemSpace
 * accumulation.
 */
extern Size cluster_epoch_shmem_size(void);
extern void cluster_epoch_shmem_init(void);
extern void cluster_epoch_shmem_register(void);

#endif /* CLUSTER_EPOCH_H */
