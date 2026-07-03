/*-------------------------------------------------------------------------
 *
 * cluster_hw_lease.h
 *	  pgrac spec-6.12d (static) -- per-node HW space leases.
 *
 *	  A lease is the unconsumed tail of an oversized HW grant: when
 *	  cluster.space_affinity = static, the relation-extend path asks the
 *	  HW master for max(need, cluster.space_lease_blocks) blocks,
 *	  physically zero-extends the WHOLE grant (the file stays dense; the
 *	  HWM advance is already durable via XLOG_HW_RESERVE), consumes what
 *	  it needs now and parks the rest here.  Later writers on THIS node
 *	  consume the parked blocks one at a time BEFORE consulting the
 *	  shared FSM -- each grant is this node's private chunk of the
 *	  extent, which is the per-instance space-affinity grouping (and it
 *	  cuts HW master round-trips by the lease factor).
 *
 *	  Materialization contract (spec-6.12d Q19-A):
 *	    - unconsumed lease blocks stay ZERO pages (EOF-interior zero
 *	      pages are a legal PG state; seqscan skips PageIsNew, vacuum
 *	      lazily repairs, first consumption runs the existing hio.c
 *	      PageIsNew lazy init incl. ITL);
 *	    - they are NEVER advertised in the shared FSM (no victim buffer
 *	      is ever created for them, so the native bulk-extend FSM
 *	      recording cannot see them; a shared-FSM advert would let any
 *	      node steal the chunk and pierce the affinity);
 *	    - a crashed / discarded lease degrades to orphan zero pages:
 *	      bloat only, reclaimed by vacuum -- never corruption (the HWM
 *	      durability that prevents double-allocation is the existing
 *	      XLOG_HW_RESERVE, untouched);
 *	    - bloat is bounded by cluster.space_lease_blocks per active
 *	      (relation, fork) lease and observable through the four
 *	      d_* counters in the xnode_lever dump category.
 *
 *	  Backend-only header.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hw_lease.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HW_LEASE_H
#define CLUSTER_HW_LEASE_H

#include "c.h"
#include "port/atomics.h"
#include "storage/block.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"
#include "common/relpath.h"

/* Fixed slot table: one active lease per (relation, fork), LRU-recycled.
 * Losing a lease to recycling is the documented fail-safe (orphan zero
 * pages, vacuum reclaims); the cap bounds shmem and total bloat. */
#define CLUSTER_HW_LEASE_SLOTS 64

typedef struct ClusterHwLeaseSlot {
	RelFileLocator rloc;
	int32 fork;				/* ForkNumber; -1 = free slot */
	BlockNumber next_block; /* next unconsumed block */
	BlockNumber end_block;	/* one past the lease's last block */
	uint64 use_seq;			/* LRU stamp */
} ClusterHwLeaseSlot;

typedef struct ClusterHwLeaseShared {
	LWLockPadded lock; /* guards every slot + use_seq counter */
	uint64 use_seq;
	ClusterHwLeaseSlot slots[CLUSTER_HW_LEASE_SLOTS];

	/* spec-6.12d D-obs mandated counters (global roll-up; the per-
	 * (relation,fork) accounting lives in the slots and is emitted as
	 * per-lease dump rows). */
	pg_atomic_uint64 d_leased_total; /* blocks ever parked in leases */
	pg_atomic_uint64 d_consumed;	 /* lease blocks handed to writers */
	pg_atomic_uint64 d_orphan_zero;	 /* blocks dropped with a lease
										 * (recycle / replace / discard) */
	pg_atomic_uint64 d_lease_grants; /* oversized HW grants taken */
} ClusterHwLeaseShared;

extern PGDLLIMPORT ClusterHwLeaseShared *ClusterHwLeaseCtl;

extern Size cluster_hw_lease_shmem_size(void);
extern void cluster_hw_lease_shmem_init(void);
extern void cluster_hw_lease_shmem_register(void);

/* True when the static space-affinity lease path is active. */
extern bool cluster_hw_lease_active(void);

/* Park [start, start+len) as this node's lease for (rloc, fork).
 * Replaces any existing lease for the pair (remainder -> orphan). */
extern void cluster_hw_lease_install(RelFileLocator rloc, ForkNumber fork, BlockNumber start,
									 uint32 len);

/* Consume one block from the lease for (rloc, fork); InvalidBlockNumber
 * when no lease / exhausted. */
extern BlockNumber cluster_hw_lease_next_block(RelFileLocator rloc, ForkNumber fork);

/* Drop the lease for (rloc, fork), if any (e.g. the consumer discovered
 * the parked range no longer exists because VACUUM truncated the trailing
 * zero pages).  The remainder counts as orphan bookkeeping only. */
extern void cluster_hw_lease_discard(RelFileLocator rloc, ForkNumber fork);

/* Dump support: iterate active slots (idx 0..CLUSTER_HW_LEASE_SLOTS-1);
 * false = slot free. */
extern bool cluster_hw_lease_slot_snapshot(int idx, ClusterHwLeaseSlot *out);

#endif /* CLUSTER_HW_LEASE_H */
