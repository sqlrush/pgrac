/*-------------------------------------------------------------------------
 * cluster_heap_freeze.h
 *    Caller-owned inputs for shared heap freeze preparation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HEAP_FREEZE_H
#define CLUSTER_HEAP_FREEZE_H

#include "access/htup_details.h"
#include "access/transam.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_tx_resolve.h"
#include "storage/buf.h"
#include "storage/bufpage.h"

/*
 * An unproven physical input, not an eligibility or retention certificate.
 * The full image retains page version/LSN, tuple headers and all physical
 * ITL/UBA fields without deriving a transaction identity from a raw XID.
 * Reuse is bounded to one image in the original VACUUM context. No borrowed
 * page pointer survives capture; no durable or wire representation is implied.
 */
struct ClusterHeapFreezePolicySampleV1;
struct ClusterHeapFreezeCohortObservation;

typedef struct ClusterHeapFreezePageInputV1
{
	ClusterSpaceIdentity identity;
	BlockNumber block;
	OffsetNumber max_offset;
	uint16 normal_tuples;
	uint16 multi_tuples;
	/* Original post-T3 use context only; never a fresh grant or F/H proof. */
	bool writer_context_valid;
	ResourceXWriterUseContext writer_context;
	uint64 writer_object_revision;
	bool writer_object_checked;
	/* Borrowed immutable companions in the same original VACUUM context.
	 * Retire with this attempt, before replacing either source. They only
	 * bind numeric bounds to their observation; they grant no F/H,
	 * retention or mutation authority and are never serialized. */
	const struct ClusterHeapFreezePolicySampleV1 *freeze_policy;
	const struct ClusterHeapFreezeCohortObservation *freeze_cohort;
	PGAlignedBlock image;
} ClusterHeapFreezePageInputV1;

typedef enum ClusterHeapFreezePageMatchV1 {
	CLUSTER_HEAP_FREEZE_PAGE_INVALID = 0,
	CLUSTER_HEAP_FREEZE_PAGE_MATCHED,
	CLUSTER_HEAP_FREEZE_PAGE_CHANGED
} ClusterHeapFreezePageMatchV1;

#define CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST UINT8_MAX
#define CLUSTER_HEAP_FREEZE_NO_DATA_GROUP UINT8_MAX
#define CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET UINT8_MAX
#define CLUSTER_HEAP_FREEZE_NO_ABORTED_XMAX_TARGET UINT8_MAX

/*
 * A request about one physical carrier, not a creator or terminal proof.
 * The page wrap remains in the full carrier witness. The existing origin
 * resolver must derive the canonical TT wrap from the exact undo record.
 */
typedef struct ClusterHeapFreezeCarrierRequestV1
{
	ClusterTxLocator locator;
	ClusterItlSlotData carrier;
	/* NO_DATA_GROUP when the legacy locator is not a DATA-record address. */
	uint8 data_group_index;
} ClusterHeapFreezeCarrierRequestV1;

/*
 * Physical read planning only. The complete segment ID includes its encoded
 * origin; a block number alone is never a group key. Distinct carrier requests
 * retain their indexes, even when their raw XIDs or record addresses coincide.
 * No image, canonical TT identity, terminal verdict or authority is implied.
 */
typedef struct ClusterHeapFreezeDataGroupV1
{
	uint32 segment_id;
	uint32 block_no;
	uint8 request_count;
	uint8 request_indexes[CLUSTER_ITL_INITRANS_DEFAULT];
} ClusterHeapFreezeDataGroupV1;

/*
 * Physical row inputs only. The tuple-named ITL can describe a later updater,
 * not the creator. Its bytes, including FREE state, never prove provenance.
 * Missing or out-of-range slot indexes remain unproven rows in the batch.
 */
typedef struct ClusterHeapFreezeTupleInputV1
{
	OffsetNumber offset;
	uint16 tuple_offset;
	uint16 tuple_length;
	TransactionId raw_xmin;
	TransactionId raw_xmax;
	ItemPointerData ctid;
	uint16 infomask;
	uint16 infomask2;
	uint8 header_length;
	uint8 itl_slot_index;
	uint8 carrier_request_index;
	/* Physical association only; never a row-freeze certificate. */
	uint8 creation_target_proof_index;
	/* Exact aborted modifier association only, never rollback completion. */
	uint8 aborted_xmax_target_proof_index;
	/* Optional exact origin observation; Invalid means not qualified.
	 * This association alone grants no F/H, current ownership or freeze. */
	FullTransactionId original_creator_full_xid;
	bool has_carrier;
	ClusterItlSlotData carrier;
} ClusterHeapFreezeTupleInputV1;

/* One caller-owned batch, reused with the captured image for each page. */
typedef struct ClusterHeapFreezePageBatchV1
{
	ClusterSpaceIdentity identity;
	BlockNumber block;
	XLogRecPtr page_lsn;
	SCN page_version;
	uint16 tuple_count;
	uint16 multi_tuples;
	uint8 carrier_request_count;
	uint8 data_group_count;
	/* Negative work counts only; zero does not certify a page as all-frozen. */
	uint16 unproven_xmin_tuples;
	uint16 unproven_xmax_tuples;
	ClusterHeapFreezeCarrierRequestV1 carrier_requests[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterHeapFreezeDataGroupV1 data_groups[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterHeapFreezeTupleInputV1 tuples[MaxHeapTuplesPerPage];
} ClusterHeapFreezePageBatchV1;

/*
 * A proof result is deliberately narrower than a freeze or retention
 * certificate.  It binds one exact row carrier to the origin's canonical
 * TT projection and to one final holder/master/admission fence.  The VACUUM
 * caller still has to perform the final current check, WAL and conditional
 * apply while holding the appropriate ownership; this predicate never does
 * I/O, acquires locks, mutates a page, or grants authority on its own.
 */
typedef enum ClusterHeapFreezeProofStatusV1 {
	CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN = 0,
	CLUSTER_HEAP_FREEZE_PROOF_PROVEN,
	CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED,
	CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR
} ClusterHeapFreezeProofStatusV1;

typedef struct ClusterHeapFreezeFinalFenceV1
{
	ClusterSpaceIdentity identity;
	BlockNumber block;
	XLogRecPtr page_lsn;
	SCN page_version;
	uint64 holder_sequence;
	uint64 master_sequence;
	uint64 admission_generation;
} ClusterHeapFreezeFinalFenceV1;

typedef struct ClusterHeapFreezeCanonicalProofV1
{
	ClusterTxLocator requested_locator;
	ClusterTxResolution resolution;
	ClusterHeapFreezeFinalFenceV1 expected_fence;
	ClusterHeapFreezeFinalFenceV1 current_fence;
	bool c1b_finalized;
	bool authority_current;
	bool admission_current;
} ClusterHeapFreezeCanonicalProofV1;

/*
 * Assemble one canonical proof from an exact transport result and the two
 * caller-owned fence snapshots.  This is a pure copy boundary: it performs
 * no resolver lookup, authority check, allocation, I/O, locking, WAL or page
 * mutation.  NULL input is rejected without changing out.  The resulting
 * object still requires the canonical validator and the conditional-apply
 * predicate before a caller can act on it.
 */
extern bool
cluster_heap_freeze_build_canonical_proof_v1(
	const ClusterTxLocator *requested_locator,
	const ClusterTxResolution *resolution,
	const ClusterHeapFreezeFinalFenceV1 *expected_fence,
	const ClusterHeapFreezeFinalFenceV1 *current_fence,
	bool c1b_finalized, bool authority_current, bool admission_current,
	ClusterHeapFreezeCanonicalProofV1 *out);

/*
 * One bounded proof batch feeds one caller-owned conditional-apply attempt.
 * Every member must describe the same final page fence; no member is merged
 * by raw XID or treated as a page mutation permission.  The bound keeps the
 * proof transport finite and matches the existing per-page ITL request bound.
 */
#define CLUSTER_HEAP_FREEZE_MAX_PROOF_BATCH CLUSTER_ITL_INITRANS_DEFAULT

typedef struct ClusterHeapFreezeProofBatchV1
{
	uint8 proof_count;
	ClusterHeapFreezeCanonicalProofV1 proofs[CLUSTER_HEAP_FREEZE_MAX_PROOF_BATCH];
} ClusterHeapFreezeProofBatchV1;

/*
 * Caller supplies a previously validated live SPACE identity and already
 * holds the source page's original share/cleanup content lock. Source and
 * output must not overlap. No allocation, I/O, buffer acquisition or proof
 * lookup occurs here. Failure leaves output unchanged. A successful copy
 * grants no current/fence/admission authority after the content lock is freed.
 */
extern bool cluster_heap_freeze_capture_page_v1(const ClusterSpaceIdentity *identity,
											  BlockNumber block, Page page,
											  ClusterHeapFreezePageInputV1 *out);

/*
 * Compare an owned input with the caller's already locked page and supplied
 * SPACE identity. Compare SPACE fields, never its in-memory padding. Every
 * page byte participates, including hints/checksum/free space: a difference
 * conservatively returns CHANGED. Malformed arguments/images return INVALID.
 * No output, allocation, I/O, lock acquisition or proof lookup occurs here.
 * MATCHED records only this physical comparison, not an atomic snapshot,
 * refreshed SPACE identity, holder/master fence, proof or permission to freeze.
 */
extern ClusterHeapFreezePageMatchV1
cluster_heap_freeze_validate_page_v1(const ClusterHeapFreezePageInputV1 *input,
									 const ClusterSpaceIdentity *identity, BlockNumber block,
									 Page page);

/*
 * Expand the immutable image in physical offset order. At most one canonical
 * locator request is prepared for each tuple-named physical ITL slot, with
 * per-row indexes or NO_CARRIER_REQUEST. No raw-XID coalescing or search occurs.
 * DATA-record requests are grouped by complete segment/block in first-use
 * order, at most eight groups of eight indexes. Non-DATA legacy locators
 * retain their carrier witness with NO_DATA_GROUP; no read is authorized.
 * Even a valid locator with equal raw xmin is not creator/FullXid provenance.
 * Input and output must not overlap. No allocation, I/O, proof lookup or
 * mutation occurs. Failure leaves output unchanged. Success grants no
 * transaction, retention, freeze or final identity-fence authority.
 */
extern bool cluster_heap_freeze_prepare_page_v1(const ClusterHeapFreezePageInputV1 *input,
											  ClusterHeapFreezePageBatchV1 *out);

/*
 * Validate one provenance-qualified terminal proof and its final identity
 * fence.  KEEP_UNPROVEN means that the evidence is incomplete; RETRY_CHANGED
 * means the final fence no longer names the same current page owner; and
 * HARD_ERROR means a malformed identity, proof or authority value.  PROVEN
 * is evidence only and does not permit a freeze, retention advance or page
 * mutation without the caller's synchronous final fence/apply path.
 */
extern ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_canonical_proof_v1(
	const ClusterHeapFreezeCanonicalProofV1 *proof);

/*
 * Validate a whole bounded batch before one conditional apply.  All members
 * must independently be PROVEN and carry one identical expected/current
 * fence.  A missing member keeps the caller unproven, a changed fence retries
 * the whole batch, and an ambiguous or malformed batch is a hard error.
 * This is a pure predicate: it performs no I/O, locking, WAL, page mutation,
 * authority transfer or retention advance, and it never partially applies a
 * batch.
 */
extern ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_proof_batch_v1(
	const ClusterHeapFreezeProofBatchV1 *batch);

/*
 * Join one prepared physical page batch to one bounded proof batch and the
 * caller's final current fence.  Every carrier request must be covered by an
 * exact locator match before the predicate can return PROVEN; missing or
 * absent transport/fence evidence stays KEEP_UNPROVEN, an owner change
 * returns RETRY_CHANGED, and an ambiguous/malformed mapping is HARD_ERROR.
 * This is transport/admission readiness only.  It performs no I/O, locking,
 * WAL, page mutation, authority transfer or retention advance, and the caller
 * still owns the synchronous final current check and conditional apply.
 */
extern ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_conditional_apply_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence);

/*
 * Validate a nonempty exact subset for row inputs or delayed carrier cleanout.
 * Every supplied proof must satisfy the same canonical and final-fence checks as
 * conditional apply.  Uncovered carriers remain unproven; this predicate
 * grants no whole-page apply, tuple-freeze or retention permission.
 */
extern ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_carrier_cleanout_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence);

/* Reset prior associations and bind only exact committed creation targets to
 * their physical row/carrier in a validated exact proof subset. Uncovered rows
 * lose their previous associations and remain unqualified. An optional
 * exact original FullXid follows only that matching row; its absence stays
 * unqualified. No F/H eligibility, final apply ownership or retention is implied. */
extern uint16 cluster_heap_freeze_bind_creation_targets_v1(
	ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence);

/* Reset prior aborted-modifier associations and bind only a complete exact
 * canonical ABORTED proof to its ordinary UPDATE/DELETE old physical row.
 * The target and canonical terminal state do NOT prove rollback completion;
 * no xmax clear, native plan, F/H, retention or apply authority is granted.
 * Creator associations are independent and remain untouched. */
extern uint16 cluster_heap_freeze_bind_aborted_xmax_targets_v1(
	ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence);

/* Physical rows awaiting independent rollback-completion proof. No native
 * plan, header bits or canonical identity is synthesized by this list. */
typedef struct ClusterHeapFreezeAbortedXmaxCandidateBatchV1
{
	uint16 target_count;
	uint16 tuple_indexes[MaxHeapTuplesPerPage];
} ClusterHeapFreezeAbortedXmaxCandidateBatchV1;

/* Revalidate the complete proof/fence and exact physical modifier target;
 * never trust an earlier aborted_xmax_target_proof_index. Successful empty
 * selection replaces old candidates; any refusal leaves *out unchanged.
 * These are not rollback-completion, xmax-clear or retention certificates. */
extern bool cluster_heap_freeze_select_aborted_xmax_candidates_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence,
	ClusterHeapFreezeAbortedXmaxCandidateBatchV1 *out);

struct HeapTupleFreeze;

/*
 * Build an xmin-only native freeze plan from exact committed creation evidence.
 * The caller must independently qualify the ORIGINAL creator FullXid, the
 * FullXid cutoff and H; this helper neither derives their provenance from a raw
 * XID/current epoch nor grants current ownership, retention or apply authority.
 * Both bounds must be finite; an unconstrained history report is not a freeze
 * cutoff. Finiteness alone does not prove the caller's provenance/admission.
 * It revalidates the physical target/carrier and final-fence evidence rather
 * than trusting creation_target_proof_index.  Success preserves raw xmax/MX
 * and every non-xmin header bit.  Failure leaves *out untouched.
 *
 * No production caller may supply these identities until their provenance and
 * admission contract is closed.  A prepared plan still needs the caller's
 * synchronous final page/fence comparison before native mutation/WAL.
 * The independently qualified creator must also equal the complete original
 * FullXid carried by the matched row and its exact canonical creation proof;
 * missing identities or equal raw XIDs from different epochs cannot qualify.
 * Unselected page carriers may be unproven. Every supplied proof is still
 * validated, and this row must match its own exact committed creation proof;
 * accepting a subset does not qualify the whole page or any unselected row.
 */
extern bool cluster_heap_freeze_prepare_xmin_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence,
	uint16 tuple_index,
	FullTransactionId original_creator_full_xid,
	FullTransactionId freeze_limit_full,
	SCN horizon_scn,
	struct HeapTupleFreeze *out);

struct RelationData;
struct PcmAuthoritySnapshot;
struct ClusterSemanticAdmissionToken;

/* Process-local requested identity; neither field can be inferred from a raw
 * XID. The caller must independently qualify it before native apply. */
typedef struct ClusterHeapFreezeXminTargetV1
{
	uint16 tuple_index;
	FullTransactionId original_creator_full_xid;
} ClusterHeapFreezeXminTargetV1;

/* Unqualified selection from the current attempt's physical/proof inputs.
 * A nonempty batch is not a native plan, F/H certificate, retention handoff
 * or permission to mutate a page. Retire it with the saved page/proof/cohort. */
typedef struct ClusterHeapFreezeXminCandidateBatchV1
{
	uint16 target_count;
	ClusterHeapFreezeXminTargetV1 targets[MaxHeapTuplesPerPage];
} ClusterHeapFreezeXminCandidateBatchV1;

/* Read-only, bounded selection in physical input order. The observed bounds
 * only screen candidates; their positive provenance/admission remains the
 * caller's responsibility. Rechecks exact committed creation evidence and
 * the complete original FullXid; never widens raw XIDs or emits native plans.
 * A validated proof subset selects only matching rows; missing evidence for
 * another carrier cannot qualify it through an old association or equal raw XID.
 * False preserves *out; true replaces it, including an empty selection.
 * Call without page/pin/census locks before later final ownership checks. */
extern bool cluster_heap_freeze_select_xmin_candidates_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence,
	FullTransactionId candidate_limit_full,
	SCN observed_horizon_scn,
	ClusterHeapFreezeXminCandidateBatchV1 *out);

/* Prepare an independently qualified target batch atomically, preserving
 * target order. All outputs remain unchanged on refusal, including an invalid
 * later row, duplicate physical offset or ambiguous native conflict horizon.
 * The caller supplies capacity for out_plans and independently qualifies the
 * original identities and finite F/H bounds under the single-target contract.
 * Proofs may cover only selected carriers; any unproven selected row refuses
 * the entire batch before publication. Unselected rows are not planned.
 * This performs no page/pin acquisition, mutation, WAL or admission. */
extern bool cluster_heap_freeze_prepare_xmin_batch_v1(
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *current_fence,
	const ClusterHeapFreezeXminTargetV1 *targets, uint16 target_count,
	FullTransactionId freeze_limit_full, SCN horizon_scn,
	struct HeapTupleFreeze *out_plans, uint16 out_capacity,
	TransactionId *out_conflict_horizon);

/*
 * Qualified xmin-only mutation using native FREEZE_PAGE WAL and synchronous
 * WAL flush. The caller always owns the original pin and the same R4 TARGET
 * token accepted by ordinary write policy and terminal-census recheck. If it
 * already holds content-X/ACTIVE PCM-X ledger, those resources are borrowed
 * unchanged. Otherwise the same entered token must pass full TARGET and
 * terminal-census policy before the saved original writer/KO fence is
 * conditionally resumed under local content-X; that temporary ledger/lock is released on
 * success, refusal and ERROR, preserving the caller pin/token. No fresh grant,
 * current fetch or proof request is permitted. A preexisting share lock cannot
 * be upgraded. The original ERROR/cancel continues after local cleanup.
 * It rechecks the
 * captured physical image, exact row/carrier and current authority before the
 * native write.  False means no mutation; ERROR propagates to the resource
 * owner.  Success changes the page version/LSN and invalidates the old proof.
 *
 * The input must borrow the original policy/cohort companions for the whole
 * attempt. Their pure same-admission match must reproduce both complete F
 * and H arguments, before writer use and after the final identity callbacks.
 * Every selected creator must also be strictly within that policy legal-next
 * past half-space before a raw native WAL conflict horizon is used. This
 * comparison preserves the original FullXid; it does not infer its epoch.
 * Their separate current/provenance/retention qualification remains required;
 * the match performs no sampling or callback under page locks.
 *
 * Dormant: production VACUUM must not call this until ORIGINAL FullXid,
 * cutoff/H provenance and admission are closed independently of raw XIDs.
 * The batch checks every distinct physical row before one final fence and one
 * native WAL write.  Its creator identities must span less than half the raw
 * XID space so that the native conflict horizon remains unambiguous.  The
 * single-target entry below delegates to this batch with count one.
 */
extern bool cluster_heap_freeze_apply_xmin_batch_v1(
	struct RelationData *rel, Buffer buffer,
	const ClusterHeapFreezePageInputV1 *input,
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence,
	const struct PcmAuthoritySnapshot *authority_before,
	const struct ClusterSemanticAdmissionToken *admission,
	const ClusterHeapFreezeXminTargetV1 *targets, uint16 target_count,
	FullTransactionId freeze_limit_full, SCN horizon_scn);

extern bool cluster_heap_freeze_apply_xmin_v1(
	struct RelationData *rel, Buffer buffer,
	const ClusterHeapFreezePageInputV1 *input,
	const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence,
	const struct PcmAuthoritySnapshot *authority_before,
	const struct ClusterSemanticAdmissionToken *admission,
	uint16 tuple_index,
	FullTransactionId original_creator_full_xid,
	FullTransactionId freeze_limit_full, SCN horizon_scn);

#endif /* CLUSTER_HEAP_FREEZE_H */
