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
#include "utils/timestamp.h"

#ifdef USE_PGRAC_CLUSTER
/*
 * Forward declarations for the R4 exact durable-origin provider.  The exact
 * value domains and structures are defined by cluster_tx_resolve.h; keeping
 * this provider on the runtime-visibility surface avoids making the D1
 * consumer the durable evidence authority.
 */
typedef enum ClusterTxResolveMode ClusterTxResolveMode;
typedef enum ClusterTxResolveReason ClusterTxResolveReason;
typedef enum ClusterTxOutcome ClusterTxOutcome;
typedef struct ClusterTxLocator ClusterTxLocator;
typedef struct ClusterTxResolution ClusterTxResolution;
typedef struct ClusterSemanticAdmissionToken ClusterSemanticAdmissionToken;
typedef struct ClusterUndoBlock0Generation ClusterUndoBlock0Generation;
typedef union ClusterUndoBlock0CurrentGuard ClusterUndoBlock0CurrentGuard;
typedef struct ClusterUndoBlock0LogicalKey ClusterUndoBlock0LogicalKey;
typedef struct ClusterUndoBlock0ResolvedRoot ClusterUndoBlock0ResolvedRoot;
typedef struct ClusterTTSlotCurrentOwner ClusterTTSlotCurrentOwner;

/* Stack-only locator for one canonical physical TT slot.  It carries no
 * status or transaction verdict: callers must acquire exact block-zero SCUR
 * and sample the physical bytes before producing authority. */
typedef struct ClusterTTSlotPhysicalLocator {
	uint32 segment_id;
	TransactionId xid;
	uint16 slot_offset;
	uint16 wrap;
	uint32 reserved32;
} ClusterTTSlotPhysicalLocator;

StaticAssertDecl(sizeof(ClusterTTSlotPhysicalLocator) == 16,
				 "physical TT locator must remain stack-only 16 bytes");
typedef struct ClusterTTStatusKey ClusterTTStatusKey;
typedef struct ClusterTTStatusResult ClusterTTStatusResult;
typedef struct ClusterCtrcTxnKeyV1 ClusterCtrcTxnKeyV1;
typedef struct ClusterCtrcParticipantIdentity ClusterCtrcParticipantIdentity;
typedef struct ClusterCurrentMemberProofKey ClusterCurrentMemberProofKey;

/* Stack/process-local continuation for the exact origin DATA -> canonical TT
 * -> DATA proof.  The representation is private to the provider; it is not a
 * wire or shared-memory ABI and never grants authority by itself. */
#define CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_BYTES 8704
typedef union ClusterRuntimeVisibilityOriginPlan {
	uint64 align;
	uint8 opaque[CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_BYTES];
} ClusterRuntimeVisibilityOriginPlan;

typedef enum ClusterRuntimeVisibilityOriginStep {
	CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED = 0,
	CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE,
	CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL
} ClusterRuntimeVisibilityOriginStep;

/* Process-local diagnostics for the canonical-TT sampling step.  These
 * values are observability only: they neither grant authority nor change a
 * resolver result. */
typedef enum ClusterRuntimeVisibilityCanonicalFailure {
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NONE = 0,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_PLAN_CURRENT,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_SAMPLE,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_UNKNOWN,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_OVERFLOW,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_RESIDENT_COPY,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_SLOT_DOMAIN,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_SLOT_IDENTITY,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NATIVE_OUTCOME,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_AUTHORITY_STAMP,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_RECHECK,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_ROOT_RECHECK,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_ADMISSION_RECHECK,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_LIVE_OWNER_SAMPLE,
	CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_LIVE_OWNER_RECHECK
} ClusterRuntimeVisibilityCanonicalFailure;

typedef struct ClusterRuntimeVisibilityCanonicalDiagnostic {
	bool valid;
	ClusterRuntimeVisibilityCanonicalFailure first_failure;
	int32 generation_result;
	bool generation_known;
	uint32 generation_value;
	int32 resident_copy_result;
	uint32 locator_xid;
	uint16 locator_wrap;
	uint16 tt_slot_offset;
	uint8 slot_status;
	uint32 slot_xid;
	uint16 slot_wrap;
	SCN slot_commit_scn;
	bool live_owner_sampled;
	bool live_owner_exact;
	uint32 live_owner_segment_id;
	uint32 live_owner_xid;
	uint16 live_owner_slot_offset;
	uint16 live_owner_wrap;
	uint8 live_owner_status;
	bool native_sampled;
	uint8 native_status;
	bool prepared_sampled;
	bool prepared;
	uint64 root_id;
	uint64 root_generation;
	bool final_root_sampled;
	uint64 final_root_id;
	uint64 final_root_generation;
	bool initial_admission_current;
	bool final_admission_current;
} ClusterRuntimeVisibilityCanonicalDiagnostic;

extern void cluster_runtime_visibility_ensure_exit_hooks(void);
extern bool
cluster_runtime_visibility_zero_epoch_pair_admission_enter(ClusterSemanticAdmissionToken *token);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_exact_origin(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode, uint64 formation_epoch,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_exact_origin_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_terminal_census_retained_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_exact_origin_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterRuntimeVisibilityOriginStep cluster_runtime_visibility_origin_plan_freeze_data_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out);
extern bool cluster_runtime_visibility_origin_plan_canonical_logical(
	const ClusterRuntimeVisibilityOriginPlan *plan, ClusterUndoBlock0LogicalKey *logical_out);
extern bool cluster_runtime_visibility_origin_plan_canonical_physical(
	const ClusterRuntimeVisibilityOriginPlan *plan, ClusterTTSlotPhysicalLocator *locator_out,
	bool *same_segment_out);
extern bool cluster_runtime_visibility_origin_plan_sample_canonical_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolveReason *reason_out);
extern bool cluster_runtime_visibility_origin_plan_canonical_diagnostic(
	const ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterRuntimeVisibilityCanonicalDiagnostic *out);
/* Current-MultiXact consumes the R4 TARGET canonical physical TT slot.  A
 * same-backend published binding is the exact locator across CURRENT segment
 * rollover; otherwise the allocator snapshot is only locator/corroboration.
 * Neither can create an ACTIVE or terminal result from nonmatching bytes. */
extern bool cluster_runtime_visibility_current_owner_sample_held(
	TransactionId xid, const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, bool *ctrc_physical_active_out);
extern bool cluster_runtime_visibility_physical_locator_sample_held(
	const ClusterTTSlotPhysicalLocator *locator, const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out, bool *ctrc_physical_active_out);
extern bool cluster_runtime_visibility_current_owner_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out);
extern bool cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
	TransactionId xid, ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	uint32 *ctrc_grant_out);
extern bool cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
	TransactionId xid, ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	uint32 *ctrc_grant_out, ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out);
/* Local current-MX terminal proof path.  It samples the exact current or
 * rolled physical slot and never creates a CTRC touch/grant/participant. */
extern bool cluster_runtime_visibility_local_terminal_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out);
/* Resolve an updater from its exact page-derived DATA locator.  The function
 * executes the same DATA -> canonical TT -> DATA proof used by the remote
 * origin adapter and never substitutes a by-xid locator. */
extern bool cluster_runtime_visibility_current_mx_updater_provenance_exact(
	const ClusterTxLocator *locator, TimestampTz deadline, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, uint32 *ctrc_grant_out,
	uint32 *participant_capability_generation_out, ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterTxLocator *canonical_locator_out, bool *cross_segment_out);
extern bool cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *proof_key, uint32 ctrc_grant,
	uint32 requester_capability_generation, ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out);
extern ClusterTxOutcome cluster_runtime_visibility_origin_plan_recheck_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
/* Export the exact revalidated resident DATA image under the final guard.
 * A failed proof does not modify data_out; no page is reconstructed from a
 * record or from a canonical TT sample. */
extern ClusterTxOutcome cluster_runtime_visibility_origin_plan_copy_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out, char data_out[BLCKSZ],
	ClusterTxResolveReason *reason_out);
#endif

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
 * S3-P0-03: pure admission for a COMMITTED_BELOW_HORIZON proof.  A valid
 * snapshot may consume the bound only when scn_time_cmp(horizon_scn, read_scn)
 * is nonpositive.  The
 * plain visibility resolver passes InvalidScn for terminal-state-only
 * consumers (Update/Self/Dirty/Toast/writer-chain); those consumers may use
 * the origin+CLOG-proven COMMITTED fact, but must preserve the bound marker
 * and never treat horizon_scn as an exact commit SCN.  An invalid horizon is
 * never evidence.
 */
extern bool cluster_vis_committed_bound_admissible(SCN horizon_scn, SCN read_scn);

/* S8-815PRE-FRESHREF-C1B-01: requester-side immutable tuple gate for the
 * exact retained-page pairing.  Pure and fail-closed; no authority/state is
 * created by a true result. */
extern bool cluster_vis_freshref_c1b_pair_request_eligible(
	TransactionId raw_xid, TransactionId ref_xid, bool has_cached_status, SCN cached_commit_scn,
	uint32 ref_epoch, uint64 current_epoch, int32 origin_node, int32 local_node, uint32 segment_id,
	uint32 expected_tt_slot_id);

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
 * spec-5.22d A1 (D4-8): the scan CORE under the positive-proof wrapper.
 * Same parse discipline, exposed for the dead-owner complete-scan prove:
 * the per-block match COUNT is reported (cross-segment uniqueness is the
 * aggregate's to decide) and unparseable bytes are a DISTINCT status so the
 * aggregate refuses the whole set instead of skip-and-continue (A1.1
 * 完备-或-fail-closed).  OK + nmatch==1 fills out_proof (terminal COMMITTED
 * with a valid scn / terminal ABORTED / NONE for in-doubt shapes) and, on a
 * terminal proof, out_commit_scn / out_wrap.  POISONED covers NULL block,
 * invalid xid, header identity mismatch, over-range slot count, and any
 * out-of-range slot status byte.  Pure; unit truth table.
 */
typedef enum ClusterVisTtBlockScanStatus {
	CLUSTER_VIS_TT_BLOCK_SCAN_OK = 0,
	CLUSTER_VIS_TT_BLOCK_SCAN_POISONED = 1
} ClusterVisTtBlockScanStatus;

extern ClusterVisTtBlockScanStatus
cluster_vis_tt_block_xid_scan(const char *block, uint32 expected_segment_id,
							  uint8 expected_owner_instance, TransactionId xid, int *out_nmatch,
							  ClusterVisTtProof *out_proof, SCN *out_commit_scn, uint16 *out_wrap);

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
 * spec-5.22d D4-6: structural gate for an AUTHORITY-served verdict page
 * (version 2 provenance) + the reply binding predicate.  The authority gate
 * mirrors the v1 discipline but accepts ONLY version 2 and refuses the
 * BELOW_HORIZON kind (the block0 prove has no horizon leg); the binding
 * predicate is the 8.A amend — reply sender == elected authority AND reply
 * epoch == stamped request epoch EXACTLY (the transport HC100 >= is only a
 * pre-filter).  Pure: unit truth tables.
 */
extern bool
cluster_vis_undo_authority_verdict_page_usable(const struct ClusterGcsUndoVerdictPage *v,
											   TransactionId asked_xid);
extern bool cluster_vis_undo_authority_reply_binding_ok(int32 sender_node, int32 authority_node,
														uint64 reply_epoch, uint64 stamped_epoch);

/*
 * spec-7.1 D3-b pure structural validation of a shipped BATCHED multixact
 * member-verdict page (see cluster_gcs_block.h for the wire struct).  true
 * only when the page provably answers the asked-for mxid: magic / version /
 * widened-mxid echo match, status is SERVED (the only status that ships a
 * page; a DENIED reply never reaches here), nmembers is in [1, MAX], every
 * reserved byte is zero, and EACH member is internally consistent -- lock-only
 * members (status <= MultiXactStatusForUpdate) carry no verdict and no scn;
 * updater members (4-5) carry a known verdict whose scn fields match the kind
 * exactly (mirroring cluster_vis_undo_verdict_page_usable).  Anything else
 * refuses so the caller keeps the 53R97 fail-closed boundary (Rule 8.A).
 * Pure: no shmem, no locks, no elog (unit truth table).  The read_scn
 * admissibility of a BELOW_HORIZON bound is decided by the consumer
 * (cluster_multixact_resolve_visibility_served), not here.
 */
struct ClusterGcsUndoMultiVerdictPage; /* cluster_gcs_block.h */
extern bool
cluster_vis_undo_multi_verdict_page_usable(const struct ClusterGcsUndoMultiVerdictPage *v,
										   MultiXactId asked_mxid);

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
 *	    bound (the true commit_scn is at or below horizon_scn).  Snapshot
 *	    callers consume it IFF read_scn is at/after the horizon (requester
 *	    leg (e)); terminal-state-only callers consume only the proven
 *	    COMMITTED fact.  The shipped
 *	    horizon is Lamport-observed either way (AD-008) so a leg-(e) miss
 *	    self-heals on the next snapshot.
 *
 * read_scn = the caller's snapshot SCN, or InvalidScn for terminal-state-only
 * callers without snapshot ordering.  true only when a terminal verdict is
 * proven
 * (*out_committed says which).  On true with *out_commit_scn_is_bound set,
 * *out_commit_scn is the HORIZON BOUND, not the exact commit_scn: a snapshot
 * may compare it only against THIS read_scn, while a terminal-only caller may
 * use only *out_committed.  It must never be stamped/cached as an exact scn
 * (a later smaller read_scn would falsely read it as committed-after —
 * false-invisible, Rule 8.A).  false = caller keeps the
 * pre-existing STALE_OR_AMBIGUOUS -> 53R97 fail-closed.
 */
extern bool cluster_runtime_visibility_try_resolve_remote(int origin_node, uint32 undo_segment_id,
														  TransactionId raw_xid, SCN read_scn,
														  bool authoritative, bool *out_committed,
														  SCN *out_commit_scn,
														  bool *out_commit_scn_is_bound);

#endif /* CLUSTER_RUNTIME_VISIBILITY_H */
