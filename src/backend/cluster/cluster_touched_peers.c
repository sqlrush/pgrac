/*-------------------------------------------------------------------------
 *
 * cluster_touched_peers.c
 *	  pgrac per-transaction cross-node "touched_peers" tracking
 *	  (spec-5.14 fail-stop reconfiguration).
 *
 *	  Backend-local, per-transaction 128-bit bitmap (uint8[16]) plus an
 *	  unknown_touched poison flag.  Stamped conservatively at the 5
 *	  cross-node ingress points (D2); consulted by the D4 ProcessInterrupts
 *	  dispatch when a fail-stop reconfig fires.  No shmem, no lock — only
 *	  the owning backend reads/writes its own bitmap; parallel workers
 *	  OR-merge into the leader via a ParallelContext DSM slot (D1b).
 *
 *	  Conservative over-approximation (INV-TP1): an unidentifiable remote
 *	  node poisons rather than silently no-ops; intersects() is fail-closed
 *	  (INV-TP3).  Under-approximation would be false-visible (8.A); over-
 *	  approximation only costs liveness (extra aborts).
 *
 *	  Spec: spec-5.14-fail-stop-reconfig.md (v0.2 FROZEN).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_touched_peers.c
 *
 * NOTES
 *	  Real bodies compile only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); disable-cluster builds get silent no-op stubs
 *	  so caller code paths (xact.c reset, parallel.c DSM, the 5 ingress
 *	  callsites) stay portable.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_touched_peers.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_guc.h"	  /* cluster_node_id */
#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + note hooks */

/*
 * U7 / L218: touched and dead bitmaps MUST be the same width so that
 * intersects() can compare them byte-for-byte without truncation.
 */
StaticAssertDecl(CLUSTER_TOUCHED_PEERS_BYTES == CLUSTER_RECONFIG_DEAD_BITMAP_BYTES,
				 "touched_peers bitmap width must equal reconfig dead_bitmap width");

/*
 * The current transaction's touched_peers.  Reset at every transaction
 * boundary (cluster_touched_peers_reset, wired in xact.c).  A parallel
 * worker accumulates here too and exports on exit (D1b).
 */
static ClusterTouchedPeersSnapshot cluster_touched_self;

/*
 * Parallel-leader registration of the per-worker DSM slot arrays.  The
 * D4 check OR-merges every registered worker slot before deciding (Q7).
 * A tiny fixed table is plenty (one active ParallelContext is the norm);
 * overflow fail-closes to poison rather than silently dropping a context.
 */
#define CLUSTER_TOUCHED_PARALLEL_MAX_REGS 16

typedef struct TouchedParallelReg {
	ClusterTouchedPeersSnapshot *slots; /* nworkers-long array in DSM */
	int nworkers;
} TouchedParallelReg;

static TouchedParallelReg cluster_touched_parallel_regs[CLUSTER_TOUCHED_PARALLEL_MAX_REGS];
static int cluster_touched_parallel_reg_count = 0;
static bool cluster_touched_parallel_reg_overflow = false;


/* ----------
 * Bit helpers (node_id in 0 .. CLUSTER_TOUCHED_PEERS_BITS-1).
 * ----------
 */
static inline bool
touched_node_is_valid_remote(int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_TOUCHED_PEERS_BITS && node_id != cluster_node_id;
}


bool
cluster_touched_peers_stamp(int32 node_id, ClusterTouchKind kind)
{
	bool was_new;

	/* self node: we never depend on our own "remote" state — no-op, no poison */
	if (node_id == cluster_node_id)
		return false;

	if (touched_node_is_valid_remote(node_id)) {
		uint8 mask = (uint8)(1u << (node_id % 8));
		int byte = node_id / 8;

		was_new = (cluster_touched_self.bitmap[byte] & mask) == 0;
		cluster_touched_self.bitmap[byte] |= mask;
	} else {
		/*
		 * Invalid / out-of-range remote node id.  We DID consume remote
		 * volatile state but cannot identify the node: poison rather than
		 * silently drop (under-approximation = false-visible, INV-TP1/TP3).
		 */
		was_new = !cluster_touched_self.unknown_touched;
		cluster_touched_self.unknown_touched = true;
	}

	cluster_reconfig_note_touched_stamp((int)kind);
	return was_new;
}


void
cluster_touched_peers_poison(ClusterTouchKind kind)
{
	cluster_touched_self.unknown_touched = true;
	cluster_reconfig_note_touched_stamp((int)kind);
}


bool
cluster_touched_peers_intersects(const uint8 *dead_bitmap)
{
	int i;

	/* fail-closed: poison intersects any fail-stop (INV-TP3) */
	if (cluster_touched_self.unknown_touched)
		return true;

	/* fail-closed: cannot read the dead set -> assume intersection */
	if (dead_bitmap == NULL)
		return true;

	for (i = 0; i < CLUSTER_TOUCHED_PEERS_BYTES; i++) {
		if ((cluster_touched_self.bitmap[i] & dead_bitmap[i]) != 0)
			return true;
	}
	return false;
}


void
cluster_touched_peers_reset(void)
{
	memset(&cluster_touched_self, 0, sizeof(cluster_touched_self));

	/*
	 * A transaction boundary means every parallel context of the just-ended
	 * transaction has been destroyed, so the parallel registration table and
	 * its overflow poison can be cleared here (the only place that clears the
	 * overflow flag — see cluster_touched_peers_unregister_parallel_slots).
	 */
	cluster_touched_parallel_reg_count = 0;
	cluster_touched_parallel_reg_overflow = false;
}


/*
 * Mark the current transaction's touched state as uncertain (poison) without
 * counting a stamp.  Used by the parallel-context teardown when a worker was
 * killed before it could export its touches (D1b / review P1-B): we cannot
 * know which peers that worker consumed, so fail-closed.
 */
void
cluster_touched_peers_mark_uncertain(void)
{
	cluster_touched_self.unknown_touched = true;
}


void
cluster_touched_peers_export_to_dsm(ClusterTouchedPeersSnapshot *out_snap)
{
	memcpy(out_snap, &cluster_touched_self, sizeof(*out_snap));
	/* publish before the worker exits so the leader's merge sees it */
	pg_write_barrier();
}


void
cluster_touched_peers_merge_from_dsm(const ClusterTouchedPeersSnapshot *worker_snap)
{
	int i;

	for (i = 0; i < CLUSTER_TOUCHED_PEERS_BYTES; i++)
		cluster_touched_self.bitmap[i] |= worker_snap->bitmap[i];

	/* poison MUST survive the merge (review round 2 P1) */
	cluster_touched_self.unknown_touched |= worker_snap->unknown_touched;
}


void
cluster_touched_peers_register_parallel_slots(ClusterTouchedPeersSnapshot *slots, int nworkers)
{
	if (slots == NULL || nworkers <= 0)
		return;

	if (cluster_touched_parallel_reg_count >= CLUSTER_TOUCHED_PARALLEL_MAX_REGS) {
		/* untracked context -> fail-closed: merge_active will poison */
		cluster_touched_parallel_reg_overflow = true;
		return;
	}

	cluster_touched_parallel_regs[cluster_touched_parallel_reg_count].slots = slots;
	cluster_touched_parallel_regs[cluster_touched_parallel_reg_count].nworkers = nworkers;
	cluster_touched_parallel_reg_count++;
}


void
cluster_touched_peers_unregister_parallel_slots(const ClusterTouchedPeersSnapshot *slots)
{
	int i;

	for (i = 0; i < cluster_touched_parallel_reg_count; i++) {
		if (cluster_touched_parallel_regs[i].slots == slots) {
			/* compact: move the last entry into this slot */
			cluster_touched_parallel_regs[i]
				= cluster_touched_parallel_regs[cluster_touched_parallel_reg_count - 1];
			cluster_touched_parallel_reg_count--;
			break;
		}
	}

	/*
	 * Do NOT clear cluster_touched_parallel_reg_overflow here: an overflowed
	 * (untracked) context may still be live even after reg_count returns to 0,
	 * and its touches were never tracked.  The poison stays until the
	 * transaction boundary clears it (cluster_touched_peers_reset), by which
	 * point every parallel context of this transaction is destroyed.
	 */
}


void
cluster_touched_peers_merge_active_parallel_workers(void)
{
	int r;

	/* an overflowed registration means we lost track of a context -> poison */
	if (cluster_touched_parallel_reg_overflow)
		cluster_touched_self.unknown_touched = true;

	if (cluster_touched_parallel_reg_count == 0)
		return;

	/* ensure we observe worker writes published before their exit */
	pg_read_barrier();

	for (r = 0; r < cluster_touched_parallel_reg_count; r++) {
		int w;

		for (w = 0; w < cluster_touched_parallel_regs[r].nworkers; w++)
			cluster_touched_peers_merge_from_dsm(&cluster_touched_parallel_regs[r].slots[w]);
	}
}


void
cluster_touched_peers_self_hex(char *buf, Size buflen)
{
	uint64 low = 0;
	int i;

	/* low 64 bits = nodes 0..63, little-endian byte order */
	for (i = 0; i < 8; i++)
		low |= ((uint64)cluster_touched_self.bitmap[i]) << (8 * i);

	snprintf(buf, buflen, "0x%016" INT64_MODIFIER "X", low);
}

#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stubs.  Same symbol surface so xact.c reset, parallel.c
 * DSM hooks, and the 5 ingress callsites compile cleanly.  All no-ops:
 * a non-cluster build never has cross-node ingress, and the D4 dispatch
 * is itself a stub, so intersects() is never consulted.
 */

bool
cluster_touched_peers_stamp(int32 node_id pg_attribute_unused(),
							ClusterTouchKind kind pg_attribute_unused())
{
	return false;
}

void
cluster_touched_peers_poison(ClusterTouchKind kind pg_attribute_unused())
{}

void
cluster_touched_peers_mark_uncertain(void)
{}

bool
cluster_touched_peers_intersects(const uint8 *dead_bitmap pg_attribute_unused())
{
	return false;
}

void
cluster_touched_peers_reset(void)
{}

void
cluster_touched_peers_export_to_dsm(ClusterTouchedPeersSnapshot *out_snap)
{
	if (out_snap != NULL)
		memset(out_snap, 0, sizeof(*out_snap));
}

void
cluster_touched_peers_merge_from_dsm(
	const ClusterTouchedPeersSnapshot *worker_snap pg_attribute_unused())
{}

void
cluster_touched_peers_register_parallel_slots(
	ClusterTouchedPeersSnapshot *slots pg_attribute_unused(), int nworkers pg_attribute_unused())
{}

void
cluster_touched_peers_unregister_parallel_slots(
	const ClusterTouchedPeersSnapshot *slots pg_attribute_unused())
{}

void
cluster_touched_peers_merge_active_parallel_workers(void)
{}

void
cluster_touched_peers_self_hex(char *buf, Size buflen)
{
	snprintf(buf, buflen, "0x0000000000000000");
}

#endif /* USE_PGRAC_CLUSTER */
