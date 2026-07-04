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
#include "cluster/cluster_scn.h"	  /* SCN */

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

/*
 * D-i2 positive proof over a fetched TT header block (spec-6.12i CP3).
 *
 * POSITIVE PROOF ONLY (user-approved boundary): a terminal verdict must come
 * from exactly ONE occupied slot in the fetched block whose (xid, wrap) pair
 * identifies the transaction — COMMITTED requires a valid commit_scn,
 * ABORTED requires the ABORTED status itself.  Everything else is NO proof:
 *   - 0 matches: a single fetched TT block cannot prove recycled/aborted
 *     (the xid's slot may live in ANOTHER segment of the origin); proving
 *     absence would need a complete origin TT header scan under the same
 *     live authority — a possible future extension, NOT this slice.
 *   - >1 matches: same-valued xid residue (any occupied status counts,
 *     RECYCLABLE included) is ambiguity — refuse.
 *   - ACTIVE / RECYCLABLE / COMMITTED-without-scn single match: not a
 *     terminal proof — refuse.
 *   - header mismatch (segment_id / owner / slot count) or an out-of-range
 *     slot status byte: not provably the asked-for TT — refuse the block.
 *
 * Why one in-block (xid) match cannot alias across a 2^32 xid wraparound:
 * a segment header takes bindings only during one contiguous active era
 * (a rollover seals it; sealed segments take no new bindings until wiped on
 * reuse, spec-3.12 D2b), every real-xid write xact binds one TT slot, and
 * per-slot wrap is capped (TT_WRAP_MAX) — so one era spans far fewer
 * bindings (≤ 48 × 65534) than the 2^32 xids a same-value recurrence
 * requires.  The matched slot's wrap is returned as the exact-identity
 * evidence (D-i2 condition (c)); >1 match still refuses as defense in
 * depth.  Pure; no I/O, no shmem, no elog (unit truth table).
 */
typedef enum ClusterVisTtProof {
	CLUSTER_VIS_TT_PROOF_NONE = 0, /* fail-closed: caller keeps 53R97 */
	CLUSTER_VIS_TT_PROOF_COMMITTED = 1,
	CLUSTER_VIS_TT_PROOF_ABORTED = 2
} ClusterVisTtProof;

extern ClusterVisTtProof cluster_vis_tt_block_positive_proof(const char *block,
															 uint32 expected_segment_id,
															 uint8 expected_owner_instance,
															 TransactionId xid, SCN *out_commit_scn,
															 uint16 *out_wrap);

/*
 * CP3 orchestration (backend): active-runtime resolution of a RECYCLED
 * remote ITL ref via D-i1 fetch + D-i2 gate + positive proof.  true only
 * when a terminal verdict is proven (*out_committed says which; commit_scn
 * valid iff committed); false = caller keeps the pre-existing
 * STALE_OR_AMBIGUOUS -> 53R97 fail-closed (Rule 8.A).
 */
extern bool cluster_runtime_visibility_try_resolve_remote(int origin_node, uint32 undo_segment_id,
														  TransactionId raw_xid,
														  XLogRecPtr anchor_lsn,
														  bool *out_committed, SCN *out_commit_scn);

#endif /* CLUSTER_RUNTIME_VISIBILITY_H */
