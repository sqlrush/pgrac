/* Process-local composition assembly, not wire or transaction authority. */
#ifndef CLUSTER_MULTIXACT_COMPOSITION_H
#define CLUSTER_MULTIXACT_COMPOSITION_H

#include "cluster/cluster_multixact_current.h"

#define CLUSTER_MX_COMPOSITION_PAGE_MEMBERS 256

typedef enum ClusterMxCompositionReadStatus {
	MXCOMP_READ_OK = 0, MXCOMP_READ_RETRY, MXCOMP_READ_UNKNOWN
} ClusterMxCompositionReadStatus;

typedef enum ClusterMxCompositionReceiveResult {
	MXCOMP_UNKNOWN = 0, MXCOMP_PARTIAL, MXCOMP_COMPLETE
} ClusterMxCompositionReceiveResult;

typedef struct ClusterMxCompositionReceiveStats {
	uint64 accepted_frames;
	uint64 accepted_proof_frames;
	uint64 unknown_results;
	uint64 restart_requests;
} ClusterMxCompositionReceiveStats;

typedef struct ClusterMxCompositionReply {
	const void *source;
	Size source_bytes;
	uint32 total, first, count, next; /* total/first/next are physical positions. */
	uint32 cap; /* Local request bound, never a peer-supplied permission. */
	bool last;
	bool skipped_zero;
	const ClusterCurrentMxMemberDesc *members;
	Size member_bytes, frame_bytes;
} ClusterMxCompositionReply;

/* Native portion of source identity, never a wire/capability or a substitute
 * for the original caller's service/storage/H scope and final checks. */
typedef struct ClusterMxCompositionNativeSource {
	uint64 epoch;
	int32 origin_node;
	MultiXactId multi, oldest_multi;
	MultiXactOffset oldest_offset, start;
	uint32 span;
} ClusterMxCompositionNativeSource;

typedef struct ClusterMxCompositionNativeChunk {
	ClusterMxCompositionNativeSource source;
	uint32 first, next, count;
	bool last, skipped_zero;
	ClusterCurrentMxMemberDesc members[CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];
} ClusterMxCompositionNativeChunk;

extern ClusterMxCompositionReadStatus cluster_mx_composition_read_native(
	MultiXactId multi, const ClusterMxCompositionNativeSource *expected,
	uint32 first, uint32 cap, ClusterMxCompositionNativeChunk *out);
/* Lock-free original-owner recheck under Truncation/Gen, before and after
 * reading. It must not allocate, wait or acquire a new lock. */
typedef bool (*ClusterMxCompositionCheck)(void *arg);
extern ClusterMxCompositionReadStatus cluster_mx_composition_read_native_checked(
	MultiXactId multi, const ClusterMxCompositionNativeSource *expected,
	uint32 first, uint32 cap, ClusterMxCompositionNativeChunk *out,
	ClusterMxCompositionCheck current, void *arg);

typedef struct ClusterMxCompositionPage {
	struct ClusterMxCompositionPage *next;
	uint32 first, count;
	ClusterCurrentMxMemberDesc members[CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];
} ClusterMxCompositionPage;

#define CLUSTER_MX_PROOF_PAGE_MEMBERS 7

/* The canonical carrier's ordinal is frame-local (0..6). Global positions
 * remain explicit uint32 values in this independent transport domain. */
typedef struct ClusterMxFullMemberProof {
	uint32 member_ordinal;
	uint16 origin_node_id, reserved16;
	ClusterCurrentMemberProof proof;
} ClusterMxFullMemberProof;

typedef struct ClusterMxProofReply {
	const void *source;
	Size source_bytes;
	uint32 total, first, count, next;
	bool last;
	const ClusterMxFullMemberProof *proofs;
	Size proof_bytes, frame_bytes;
} ClusterMxProofReply;

typedef struct ClusterMxCompositionProofPage {
	struct ClusterMxCompositionProofPage *next;
	uint32 first, count;
	ClusterMxFullMemberProof proofs[CLUSTER_MX_PROOF_PAGE_MEMBERS];
} ClusterMxCompositionProofPage;

StaticAssertDecl(sizeof(ClusterMxFullMemberProof) == 200, "full proof entry size");

typedef struct ClusterMxCompositionReceive ClusterMxCompositionReceive;

typedef struct ClusterMxCompositionProofWindow {
	const void *source;
	Size source_bytes;
	uint32 total, first, count;
	ClusterCurrentMxMemberDesc members[CLUSTER_MX_PROOF_PAGE_MEMBERS];
} ClusterMxCompositionProofWindow;

/* Bounded next unaccepted logical window. No advance, allocation or authority.
 * Copied members and borrowed source expire with the original operation. */
extern bool cluster_mx_composition_proof_window(const ClusterMxCompositionReceive *op,
	uint32 cap, ClusterMxCompositionProofWindow *window);

/* source is the caller's complete, prevalidated semantic identity, never a
 * hash or raw MXID. Exact bytes are owned/copied; no wire layout is defined
 * here. G1 retention and G5 decoder/capability checks remain caller duties.
 * stats must outlive the operation, starts caller-zeroed and survives failure.
 * total is physical span L; start must be bound inside the same source.
 * next() advances physically, while pages().first and output total are logical.
 * A zero-only physical tail is accepted only as the one verified slot-0 skip.
 * Only operation-local memory is acquired, under CurrentMemoryContext.
 */
extern bool cluster_mx_composition_begin(ClusterMxCompositionReceive **op,
	const void *source, Size source_bytes, uint32 total, MultiXactOffset start,
	ClusterMxCompositionReceiveStats *stats);
extern ClusterMxCompositionReceiveResult cluster_mx_composition_accept(
	ClusterMxCompositionReceive **op, ClusterMxCompositionReadStatus status,
	const ClusterMxCompositionReply *reply);
extern uint32 cluster_mx_composition_next(const ClusterMxCompositionReceive *op);
/* Provisional composition metadata only, never a visibility/conflict result.
 * NULL/zero until complete. Borrowed pages expire on accept, finish or parent
 * context reset; caller still owes all source/grant/tuple/fence final checks.
 */
extern const ClusterMxCompositionPage *cluster_mx_composition_pages(
	const ClusterMxCompositionReceive *op, uint32 *total);
/* Raw operations only. Operations borrowed from describe_full must use its
 * ResourceOwner-aware accept entry. These remain provisional.
 * Original authenticated source, CTRC envelope,
 * grant, tuple and fence requalification are still mandatory before use. */
extern ClusterMxCompositionReceiveResult
cluster_mx_composition_accept_proofs(ClusterMxCompositionReceive **op,
									 ClusterMxCompositionReadStatus status,
									 const ClusterMxProofReply *reply);
extern const ClusterMxCompositionProofPage *
cluster_mx_composition_proofs(const ClusterMxCompositionReceive *op, uint32 *total);
/* Process-local global positions; embedded canonical ordinals stay local to
 * their original proof frame. Neither wrapper is a wire or durable layout. */
typedef struct ClusterMxFullUpdaterChallenge {
	uint32 member_ordinal;
	ClusterCurrentUpdaterChallenge challenge;
} ClusterMxFullUpdaterChallenge;

typedef struct ClusterMxFullUpdaterProof {
	uint32 member_ordinal;
	ClusterCurrentUpdaterProof proof;
} ClusterMxFullUpdaterProof;

/* Pure candidate from one complete, immutable G2 operation. No proof service,
 * qualification or publication is performed. The original caller still owes
 * source/grant/tuple/fence checks and a qualified SELF projection; raw foreign
 * data cannot manufacture SELF. wait_key is nonzero only for final WAIT.
 * Diagnostic ordinals are global int32, within the native positive-int32 N.
 */
extern ClusterCurrentMxDecision cluster_mx_composition_decide(
	const ClusterMxCompositionReceive *op, const ClusterCurrentMxRequestContext *ctx,
	const ClusterMxFullUpdaterChallenge *challenge, const ClusterMxFullUpdaterProof *updater_proof,
	ClusterTTStatusKey *wait_key, ClusterCurrentMxDecisionTrace *trace);

/* Pure normalization plan from the same complete composition/proof operation.
 * No allocation, touch, receipt, grant requalification or publication occurs.
 * The caller supplies ceil(cap / PAGE_MEMBERS) distinct writable pages, each
 * PAGE_MEMBERS long, with cap in [1, INT32_MAX]; only cap entries are cleared.
 * All counts
 * are global uint32, while canonical proof ordinals remain frame-local.
 * Non-OK clears the entire declared output and count. SUPPORTED_LIMIT means
 * this caller-provided buffer is too small, never an MX member-count gate.
 * G4 must reserve production capacity before any proof-touch; this entry is
 * not a reservation interface or permission to bypass the original final
 * source/grant/tuple/fence checks. Input pages must remain immutable.
 */
extern ClusterMxRecomposeResult cluster_mx_composition_recompose(
	const ClusterMxCompositionReceive *op, TransactionId requester_xid,
	MultiXactStatus requester_status, MultiXactMember **normalized_pages,
	uint32 normalized_cap, uint32 *normalized_count);
extern void cluster_mx_composition_finish(ClusterMxCompositionReceive **op);

#endif
