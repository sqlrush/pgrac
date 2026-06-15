/*-------------------------------------------------------------------------
 *
 * cluster_write_fence.h
 *	  pgrac internal cooperative write-fence -- a shared-storage write is
 *	  allowed only while this node proves it holds the current cluster
 *	  authority; a stale / superseded / lease-expired / self-fenced node
 *	  fails closed (spec-4.12, split-brain recovery guard).
 *
 *	  Scope (spec-4.12 §1, FROZEN v1.0): COOPERATIVE fence -- every
 *	  cooperative shared-storage write path (cluster_smgr write / extend /
 *	  zeroextend / truncate / unlink / create, the WAL write entry, the
 *	  4.10/4.11 recovery write-back) consults the local atomic write-fence
 *	  token before writing; a fenced node fails closed.  This is NOT
 *	  hardware fencing (SCSI-3 PR / STONITH / cloud) -- that is a separate
 *	  failure domain forwarded to 4.12b / spec-2.X (AD-013).
 *
 *	  The authoritative epoch lives in a durable fence marker on the voting
 *	  disk; qvotec (the sole voting-disk I/O owner) polls it, requires a
 *	  quorum-majority of disks to agree on the same marker tuple, orders by
 *	  fence_epoch (monotonic), and refreshes the local token + lease.  A
 *	  partitioned node cannot refresh -> its lease expires -> writes fail
 *	  closed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_write_fence.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.12-cooperative-write-fence.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WRITE_FENCE_H
#define CLUSTER_WRITE_FENCE_H

/*
 * cluster_write_fence_decide -- the PURE cooperative write-fence judge (spec-4.12
 * D3).  A shared-storage write is allowed iff enforcement is on AND the local
 * write-fence token proves this node is authorized:
 *
 *	enforcement_on    cluster.write_fence_enforcement is on (off / dev -> escape
 *	                  hatch, always allowed).
 *	region_attached   the write-fence token shmem region is attached (L110: a
 *	                  detached region fails closed -- never fail-open).
 *	epoch_current     cluster_epoch_get_current().
 *	authorized_epoch  the fence_epoch the qvotec poll locked in from the durable
 *	                  voting-disk marker.  Compared with EXACT == (never >=): a
 *	                  stale node's worst risk is "thinking its epoch is current".
 *	now_us            current time; lease_expire_us the qvotec-refreshed lease.
 *	                  now >= expire means this node failed to refresh (partition)
 *	                  -> fail closed.
 *	self_fenced       this node is in the marker's fenced_dead_bitmap.
 *
 * PURE so every branch is unit-pinned; the .c wrapper reads the live runtime +
 * shmem token and calls this.
 */
static inline bool
cluster_write_fence_decide(bool enforcement_on, bool region_attached,
						   uint64 epoch_current, uint64 authorized_epoch, uint64 now_us,
						   uint64 lease_expire_us, bool self_fenced)
{
	if (!enforcement_on)
		return true; /* escape hatch: off / dev */
	if (!region_attached)
		return false; /* L110 sentinel: fail-closed, never fail-open */
	if (self_fenced)
		return false; /* this node is in the marker's fenced_dead_bitmap */
	if (epoch_current != authorized_epoch)
		return false; /* exact == (P0b / user 修正 3): a stale node must not write */
	if (now_us >= lease_expire_us)
		return false; /* lease expired: failed to refresh (partition) */
	return true;
}

#ifndef FRONTEND

#include "port/atomics.h"
#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */

/*
 * cluster.write_fence_enforcement (spec-4.12 §2.4, Q7).  OFF / DEV are the escape
 * hatch (hot gate is a no-op); ON enforces.  Default ON for multi-node + shared-fs
 * is wired by the D7 GUC registration; the storage global defaults OFF until then
 * so the partially-wired mechanism (before D2 refreshes the lease) never fences.
 */
typedef enum ClusterWriteFenceEnforcement {
	CLUSTER_WRITE_FENCE_ENFORCE_OFF = 0,
	CLUSTER_WRITE_FENCE_ENFORCE_ON,
	CLUSTER_WRITE_FENCE_ENFORCE_DEV,
} ClusterWriteFenceEnforcement;

/* GUC storage (registered in cluster_guc.c, D7; storage defined in cluster_write_fence.c). */
extern int cluster_write_fence_enforcement;
extern int cluster_write_fence_lease_ms;

/*
 * Local write-fence token (the "pgrac cluster write fence" shmem region, spec-4.12
 * D3).  qvotec refreshes it every poll from the durable voting-disk marker (D2);
 * the hot write paths read it via cluster_write_fence_allowed().
 */
typedef struct ClusterWriteFenceShmem {
	pg_atomic_uint64 authorized_epoch;	 /* fence_epoch from the durable majority marker */
	pg_atomic_uint64 lease_expire_at_us; /* qvotec-refreshed lease deadline */
	pg_atomic_uint32 self_fenced;		 /* this node is in fenced_dead_bitmap */
	uint8 fenced_dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	pg_atomic_uint64 fence_event_id;	 /* identity of the active fence (not ordered) */
	pg_atomic_uint64 hot_gate_blocked;	 /* counter: hot-path fail-closed */
	pg_atomic_uint64 durable_check_blocked; /* counter: recovery/rejoin direct-check fail */
	pg_atomic_uint64 minority_marker_ignored; /* counter: partial/minority marker rejected */
} ClusterWriteFenceShmem;

/* shmem region plumbing (ClusterShmemRegion 5-step). */
extern void cluster_write_fence_shmem_register(void);

/*
 * cluster_write_fence_allowed -- the hot-path wrapper: read the live epoch + the
 * shmem token and apply cluster_write_fence_decide.  PURE-read, no-throw,
 * critical-section-safe (no LWLock, no durable I/O); callers decide the
 * fail-closed action (ERROR / PANIC / BLOCKED) by context (spec-4.12 §3, Q1).
 */
extern bool cluster_write_fence_allowed(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_WRITE_FENCE_H */
