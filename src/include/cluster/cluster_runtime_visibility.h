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
	SCN authority_scn;		 /* PGRAC: spec-7.1a D3 -- origin SCN clock
							  * co-sampled with the content; the covers
							  * gate admits only when it is at/after the
							  * caller's demand (read_scn or its clock at
							  * request time).  InvalidScn = absent ->
							  * refuse fail-closed */
} ClusterLiveAuthority;

/*
 * PURE gate (D-i2 window check; spec-7.1a D3 SCN total order).  Returns true
 * iff the co-sampled authority provably covers the caller's demand in the
 * caller's current reconfig generation.  demand_scn is the SCN the answer
 * must be conclusive for: the snapshot read_scn on MVCC legs, or the
 * caller's own clock sampled BEFORE the fetch on no-snapshot legs (writer
 * terminal resolution) -- by AD-008 Lamport, an origin whose co-sampled
 * clock is at/after the demand has already issued (and pre-commit durably
 * stamped) every commit the demand could have observed.  The former
 * `live_hwm_lsn < anchor_lsn` compare was NOT sound across per-thread WAL
 * streams (a page last written by another thread carries a numerically
 * incomparable LSN: false-refuse measured, false-pass latent) and is
 * replaced, never weakened.  Fail-closed on any doubt:
 *   - origin_epoch != local_epoch  -> authority from a different reconfig gen
 *   - live_hwm_lsn invalid         -> no authority sampled
 *   - authority_scn invalid        -> older peer / no SCN co-sample
 *   - demand_scn invalid           -> caller supplied no demand
 *   - authority_scn before demand  -> origin clock not provably conclusive
 *                                     for this demand yet (retry self-heals:
 *                                     the shipped SCNs are Lamport-observed)
 * tt_generation is NOT checked here; it is consumed by the downstream by-xid
 * wrap-qualified resolution (D-i2 condition (a)/(c)).
 */
extern bool cluster_vis_live_authority_covers_policy(SCN demand_scn, ClusterLiveAuthority auth,
													 uint64 local_epoch);

/*
 * Runtime wrapper: supplies the local membership epoch to the pure gate.
 * (cluster_runtime_visibility.c; the pure policy above is CP1.)
 */
extern bool cluster_vis_live_authority_covers(SCN demand_scn, ClusterLiveAuthority auth);

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
	CLUSTER_VIS_TT_PROOF_NONE = 0,		/* fail-closed: caller keeps 53R97 */
	CLUSTER_VIS_TT_PROOF_COMMITTED = 1, /* EVIDENCE only: stamps land at 2PC
										 * pre-commit; consumers finalize via
										 * the origin's C1b verdict leg,
										 * never conclude committed here */
	CLUSTER_VIS_TT_PROOF_ABORTED = 2	/* terminal: an abort is irreversible */
} ClusterVisTtProof;

extern ClusterVisTtProof cluster_vis_tt_block_positive_proof(const char *block,
															 uint32 expected_segment_id,
															 uint8 expected_owner_instance,
															 TransactionId xid, SCN *out_commit_scn,
															 uint16 *out_wrap);

/*
 * CP5 (D-i4) pure structural validation of a shipped verdict page (see
 * cluster_gcs_block.h for the wire struct and the verdict taxonomy).  true
 * only when the page provably answers the asked-for xid: magic / version /
 * widened-xid echo match, the verdict kind is known, its scn fields are
 * consistent with the kind (EXACT needs a valid commit_scn and no horizon;
 * BELOW_HORIZON needs a valid horizon and no commit_scn; ABORTED needs
 * neither), and every reserved byte is zero.  Anything else refuses — the
 * caller keeps the 53R97 fail-closed boundary (Rule 8.A).  Pure: no shmem,
 * no locks, no elog (unit truth table).
 */
struct ClusterGcsUndoVerdictPage; /* cluster_gcs_block.h */
extern bool cluster_vis_undo_verdict_page_usable(const struct ClusterGcsUndoVerdictPage *v,
												 TransactionId asked_xid);

/*
 * CP3 + CP5 orchestration (backend): active-runtime resolution of a RECYCLED
 * remote ITL ref.  Two provable legs, both under the co-sampled live
 * authority gate (D-i2):
 *
 *	 1. single-block positive proof (CP3): D-i1 fetch of the ref's segment
 *	    header + exact xid+wrap slot match on the shipped bytes;
 *	 2. origin verdict (CP5 / D-i4): on a 1-leg NONE, ask the origin for a
 *	    COMPLETE own-TT by-xid verdict (complete scan + CLOG cross-check +
 *	    retention origin legs; ≈ the spec-3.22 retention theorem served
 *	    cross-instance).  A COMMITTED_BELOW_HORIZON verdict carries only a
 *	    bound (the true commit_scn is at or below horizon_scn), so it is
 *	    consumed IFF the caller's read_scn is at/after the horizon
 *	    (requester leg (e)); the shipped
 *	    horizon is Lamport-observed either way (AD-008) so a leg-(e) miss
 *	    self-heals on the next snapshot.
 *
 * read_scn = the caller's snapshot SCN, or InvalidScn for callers without
 * snapshot semantics (below-horizon verdicts are then inadmissible; exact
 * verdicts still resolve).  true only when a terminal verdict is proven
 * (*out_committed says which).  On true with *out_commit_scn_is_bound set,
 * *out_commit_scn is the HORIZON BOUND, not the exact commit_scn: it decides
 * correctly against THIS read_scn only, and must never be stamped/cached as
 * an exact scn (a later smaller read_scn would falsely read it as
 * committed-after — false-invisible, Rule 8.A).  false = caller keeps the
 * pre-existing STALE_OR_AMBIGUOUS -> 53R97 fail-closed.
 */
extern bool cluster_runtime_visibility_try_resolve_remote(int origin_node, uint32 undo_segment_id,
														  TransactionId raw_xid, SCN read_scn,
														  bool *out_committed, SCN *out_commit_scn,
														  bool *out_commit_scn_is_bound);

#endif /* CLUSTER_RUNTIME_VISIBILITY_H */
