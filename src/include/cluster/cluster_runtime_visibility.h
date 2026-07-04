/*-------------------------------------------------------------------------
 *
 * cluster_runtime_visibility.h
 *	  pgrac spec-6.12 wave 6.12i (缺口 A) — active-runtime cross-instance
 *	  recycled-slot visibility resolution: live authority gate.
 *
 *	  Recovery uses a materialized marker ("origin's merge completed → its
 *	  durable TT covers the whole window") to admit by-xid resolution of a
 *	  recycled remote ITL slot.  Active runtime has no such marker (active
 *	  state != recovery state), so this wave defines a LIVE authority source:
 *	  the origin's LMS co-samples {origin_epoch, live_hwm_lsn, tt_generation}
 *	  into the very undo-block reply that carries the TT (D-i1), and the
 *	  requester admits by-xid resolution only when that authority provably
 *	  covers this tuple's page version.  Proof-insufficient / epoch-changed /
 *	  authority-absent -> fail closed (53R97), never false-visible (8.A).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_runtime_visibility.h
 *
 * NOTES
 *	  cluster_vis_live_authority_covers_policy() is a PURE predicate (no
 *	  shmem, no locks, no elog) so cluster_unit exercises the whole truth
 *	  table standalone.  cluster_vis_live_authority_covers() is the runtime
 *	  wrapper that supplies the local membership epoch.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RUNTIME_VISIBILITY_H
#define CLUSTER_RUNTIME_VISIBILITY_H

#include "access/xlogdefs.h"
#include "cluster/cluster_itl_slot.h" /* UBA */

/*
 * Live authority triple, co-sampled by the origin LMS into the undo-block
 * reply (D-i1) so it is atomic with the undo/TT content it authorizes -- no
 * asynchronous-sampling tear window (spec-6.12 §2.11 "live authority source").
 *
 * origin_epoch is uint64, not the spec sketch's uint32: cluster_epoch is a
 * uint64 everywhere (cluster_epoch_get_current, the GCS wire epoch fields),
 * and the full-width equality gate is strictly stronger (no truncation
 * aliasing) at zero cost.
 */
typedef struct ClusterLiveAuthority {
	uint64 origin_epoch;	 /* origin's view of the membership epoch */
	XLogRecPtr live_hwm_lsn; /* origin durable AND TT-applied high-water */
	uint64 tt_generation;	 /* origin TT-slot generation (anti-alias) */
} ClusterLiveAuthority;

/*
 * PURE gate (D-i2 window check).  Returns true iff the co-sampled authority
 * provably covers `anchor_lsn` (the tuple's page LSN) in the caller's current
 * reconfig generation.  Fail-closed on any doubt:
 *   - origin_epoch != local_epoch  -> authority from a different reconfig gen
 *   - live_hwm_lsn invalid         -> no authority sampled
 *   - live_hwm_lsn < anchor_lsn    -> origin durable TT does not yet cover
 *                                     this page version
 * tt_generation is NOT checked here; it is consumed by the downstream by-xid
 * wrap-qualified resolution (D-i2 condition (a)/(c)).
 */
extern bool cluster_vis_live_authority_covers_policy(XLogRecPtr anchor_lsn,
													 ClusterLiveAuthority auth, uint64 local_epoch);

/*
 * Runtime wrapper: supplies the local membership epoch to the pure gate.
 * (cluster_runtime_visibility.c; the pure policy above is CP1.)
 */
extern bool cluster_vis_live_authority_covers(XLogRecPtr anchor_lsn, ClusterLiveAuthority auth);

/*
 * D-i1 fetch (spec-6.12i CP2): fetch the TT-bearing undo header block named
 * by `uba` from `origin_node`, together with the co-sampled live authority
 * triple.  The visibility slice serves ONLY block 0 (the segment header
 * holding the durable TT slots): TT stamps are pre-commit targeted pwrites,
 * so block 0 has no deferred-WAL staleness window; undo DATA blocks can lag
 * their pool image under the spec-3.25 D1b keep-clean deferral and are
 * refused fail-closed (feature #119 full undo-block CF is the downstream
 * forward of this slice).
 *
 * true  -> out_page (BLCKSZ) holds the origin-fresh block and *auth_out the
 *          authority sampled in the same reply (or a same-epoch cached pair
 *          from the L2 CR pool + per-backend authority memo, Q-i5).
 * false -> fail-closed miss: GUC off, bad UBA, non-header block, wire
 *          timeout / DENIED / checksum / trailer missing.  The caller keeps
 *          the unchanged 53R97 refusal (Rule 8.A).
 */
extern bool cluster_undo_block_fetch_for_visibility(int origin_node, UBA uba, char *out_page,
													ClusterLiveAuthority *auth_out);

#endif /* CLUSTER_RUNTIME_VISIBILITY_H */
