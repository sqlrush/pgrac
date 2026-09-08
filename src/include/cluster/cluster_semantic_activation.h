/*-------------------------------------------------------------------------
 *
 * cluster_semantic_activation.h
 *	  Shared two-stage semantic activation framework.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SEMANTIC_ACTIVATION_H
#define CLUSTER_SEMANTIC_ACTIVATION_H

#include "c.h"
#include "common/sha2.h" /* PG_SHA256_DIGEST_LENGTH (contract seam) */
#include "cluster/cluster_control_root.h" /* file token + round (contract seam) */
#include "cluster/cluster_ic.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "nodes/parsenodes.h"

#define CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1 (UINT64_C(1) << 0)
#define CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1 (UINT64_C(1) << 10)
#define CLUSTER_SEMANTIC_R11_RESOURCE_X_SOURCE_AVAILABLE false
#define CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES 512
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC UINT32_C(0x314B4341)
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION UINT16_C(1)
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES 120
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES 64
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY 256
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_TABLE_BYTES 16496
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS UINT32_C(0x0071B000)
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID UINT32_C(1)
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE UINT32_C(2)
#define CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF UINT32_C(4)

typedef enum ClusterSemanticActivationAckKind {
	CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_INVALID = 0,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST = 1,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK = 2
} ClusterSemanticActivationAckKind;

typedef enum ClusterSemanticActivationAckStage {
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_INVALID = 0,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE = 1,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER = 2,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED = 3,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED = 4,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED = 5
} ClusterSemanticActivationAckStage;

typedef enum ClusterSemanticActivationAckResult {
	CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST = 0,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK = 1,
	CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED = 2
} ClusterSemanticActivationAckResult;

typedef struct ClusterSemanticActivationAckWireV1 {
	uint8 kind;
	uint8 stage;
	uint32 result;
	uint32 reason;
	uint32 coordinator_node;
	uint32 member_node;
	uint64 transition_epoch;
	uint64 record_generation;
	uint64 round_nonce;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 rollback_feature_bitmap;
	uint64 admitted_members_lo;
	uint64 admitted_members_hi;
	uint64 capability_sample_digest;
	uint64 boot_id;
	uint64 admitted_incarnation;
	uint32 capability_word;
} ClusterSemanticActivationAckWireV1;

typedef enum ClusterSemanticAdmissionSide {
	CLUSTER_SEMANTIC_SOURCE_SIDE = 0,
	CLUSTER_SEMANTIC_TARGET_SIDE = 1
} ClusterSemanticAdmissionSide;

typedef enum ClusterSemanticAdmissionResult {
	CLUSTER_SEMANTIC_ADMISSION_OK = 0,
	CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT = 1,
	CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED = 2,
	CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED = 3,
	CLUSTER_SEMANTIC_ADMISSION_CLOSED = 4
} ClusterSemanticAdmissionResult;

/* Stack-only reason for one Resource-X peer OPEN revalidation.  This is a
 * read-only diagnostic result: it is neither shared state nor wire authority. */
typedef enum ClusterSemanticResourceXPeerOpenResult {
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED = 0,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH = 1,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_INVALID_INPUT = 2,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_ADMISSION_DRIFT = 3,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT = 4,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_AUTHORITY_UNAVAILABLE = 5,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_UNAVAILABLE = 6,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_STAGE_MISMATCH = 7,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FLAGS_MISMATCH = 8,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_RECORD_MISMATCH = 9,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FORMATION_MISMATCH = 10,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FEATURE_MISMATCH = 11,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_PEER_ABSENT = 12,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_PEER_ROW_MISMATCH = 13,
	CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT = 14
} ClusterSemanticResourceXPeerOpenResult;

/* One read-only R4 gate decision for the tracked buffer-writer choke point. */
typedef enum ResourceXWriterPath {
	RESOURCE_X_WRITER_SOURCE = 1,
	RESOURCE_X_WRITER_TARGET = 2,
	RESOURCE_X_WRITER_CLOSED = 3
} ResourceXWriterPath;

/* Stack-only R11 owner-phase observation.  It is neither persisted nor sent
 * on the wire and grants no authority; it only identifies exact points in
 * the existing R4 cutover round. */
typedef enum ClusterSemanticR11CutoverPhase {
	CLUSTER_SEMANTIC_R11_CUTOVER_NONE = 0,
	CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED = 1,
	CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL = 2,
	CLUSTER_SEMANTIC_R11_CUTOVER_TARGET_OPEN = 3
} ClusterSemanticR11CutoverPhase;

typedef struct ClusterSemanticR11CutoverSnapshot {
	uint64 record_generation;
	uint64 formation_epoch;
	uint32 phase;
	uint32 reserved;
} ClusterSemanticR11CutoverSnapshot;

StaticAssertDecl(sizeof(ClusterSemanticR11CutoverSnapshot) == 24,
				 "R11 cutover snapshot layout must remain 24 bytes");

typedef struct ClusterSemanticAdmissionToken {
	uint64 feature_bit;
	uint64 record_generation;
	uint64 formation_epoch;
	uint8 side;
	bool entered;
} ClusterSemanticAdmissionToken;

StaticAssertDecl(offsetof(ClusterSemanticAdmissionToken, feature_bit) == 0,
				 "semantic admission feature offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticAdmissionToken, record_generation) == 8,
				 "semantic admission generation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticAdmissionToken, formation_epoch) == 16,
				 "semantic admission formation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticAdmissionToken, side) == 24,
				 "semantic admission side offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticAdmissionToken, entered) == 25,
				 "semantic admission entered offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticAdmissionToken) == 32,
				 "semantic admission token must remain 32 bytes");

typedef enum ClusterSemanticActivationPhase {
	CLUSTER_SEMANTIC_PHASE_NONE = 0,
	CLUSTER_SEMANTIC_PHASE_PREPARE = 1,
	CLUSTER_SEMANTIC_PHASE_COMMIT = 2,
	CLUSTER_SEMANTIC_PHASE_OPEN = 3,
	CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE = 4
} ClusterSemanticActivationPhase;

typedef enum ClusterSemanticActivationResult {
	CLUSTER_SEMANTIC_ACTIVATION_OK = 0,
	CLUSTER_SEMANTIC_ACTIVATION_TARGET_DISABLED = 1,
	CLUSTER_SEMANTIC_ACTIVATION_SOURCE_DORMANT = 2,
	CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED = 3,
	CLUSTER_SEMANTIC_ACTIVATION_HETEROGENEOUS = 4,
	CLUSTER_SEMANTIC_ACTIVATION_MEMBERSHIP_CHANGED = 5,
	CLUSTER_SEMANTIC_ACTIVATION_CAPABILITY_CHANGED = 6,
	CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO = 7,
	CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO = 8,
	CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD = 9,
	CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT = 10,
	CLUSTER_SEMANTIC_ACTIVATION_PARTIAL_APPLY = 11,
	CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE = 12
} ClusterSemanticActivationResult;

typedef struct ClusterSemanticActivationRefusal {
	ClusterSemanticActivationResult result;
	uint64 feature_bit;
	uint64 expected_generation;
} ClusterSemanticActivationRefusal;

typedef struct ClusterSemanticZeroProof {
	uint64 record_generation;
	uint64 debt_count;
	uint64 sample_digest;
} ClusterSemanticZeroProof;

typedef struct ClusterSemanticActivationRecord {
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 transition_epoch;
	uint64 record_generation;
	uint64 admitted_members_lo;
	uint64 admitted_members_hi;
	uint64 capability_sample_digest;
	uint64 rollback_feature_bitmap;
	uint64 coordinator_incarnation;
	uint32 coordinator_node;
	ClusterSemanticActivationPhase phase;
} ClusterSemanticActivationRecord;

typedef struct ClusterSemanticActivationCasRequest {
	uint64 request_seq;
	uint64 expected_generation;
	uint64 expected_source_feature_bitmap;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterSemanticActivationCasRequest;

typedef struct ClusterSemanticActivationReadRequest {
	uint64 request_seq;
} ClusterSemanticActivationReadRequest;

typedef struct ClusterSemanticActivationReadCompletion {
	ClusterSemanticActivationResult result;
	bool implicit_open;
	uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterSemanticActivationReadCompletion;

typedef enum ClusterSemanticAuthorityRequestKind {
	CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE = 0,
	CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS = 1,
	CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR = 2,
	CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ = 3,
	CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ = 4
} ClusterSemanticAuthorityRequestKind;

/* Volatile shared-memory mailbox authority token.  This is never persisted or
 * sent on the wire; it binds one LMON request to the exact current formation
 * and PGSA generation that admitted it. */
typedef struct ClusterSemanticFormationBinding {
	uint64 utility_request_seq;
	uint64 formation_epoch;
	uint64 coordinator_incarnation;
	uint64 expected_record_generation;
} ClusterSemanticFormationBinding;

typedef struct ClusterUndoRootDescriptorRequest {
	uint64 request_seq;
	uint64 system_identifier;
	ClusterSemanticFormationBinding formation;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterUndoRootDescriptorRequest;

typedef struct ClusterUndoRootDescriptorReadRequest {
	uint64 request_seq;
	uint64 system_identifier;
	ClusterSemanticFormationBinding formation;
} ClusterUndoRootDescriptorReadRequest;

typedef struct ClusterUndoRootDescriptorReadCompletion {
	ClusterUndoRootDescriptorState state;
	uint8 selected_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
} ClusterUndoRootDescriptorReadCompletion;

typedef ClusterSemanticActivationResult (*ClusterSemanticReadinessCallback)(
	uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);
typedef ClusterSemanticActivationResult (*ClusterSemanticStageCallback)(uint64 generation);
typedef ClusterSemanticActivationResult (*ClusterSemanticZeroCallback)(
	uint64 generation, ClusterSemanticZeroProof *proof);

/* A prerequisite module contributes only closed transition callbacks.  It has
 * no feature identity and cannot register or activate itself; the terminal
 * cutover owner composes the sole immutable descriptor. */
typedef struct ClusterSemanticActivationCallbackBundle {
	ClusterSemanticReadinessCallback pre_prepare_readiness;
	ClusterSemanticStageCallback close_source_admission;
	ClusterSemanticZeroCallback source_logical_debt_zero;
	ClusterSemanticZeroCallback source_transport_zero;
	ClusterSemanticStageCallback prepare_target;
	ClusterSemanticStageCallback apply_target_closed;
	ClusterSemanticStageCallback revert_source_closed;
	ClusterSemanticStageCallback open_target_admission;
} ClusterSemanticActivationCallbackBundle;

typedef struct ClusterSemanticActivationDescriptor {
	const char *name;
	uint64 feature_bit;
	uint32 required_hello_caps;
	uint64 required_active_bits;
	bool source_available;
	ClusterSemanticReadinessCallback pre_prepare_readiness;
	ClusterSemanticStageCallback close_source_admission;
	ClusterSemanticZeroCallback source_logical_debt_zero;
	ClusterSemanticZeroCallback source_transport_zero;
	ClusterSemanticStageCallback prepare_target;
	ClusterSemanticStageCallback apply_target_closed;
	ClusterSemanticStageCallback revert_source_closed;
	ClusterSemanticStageCallback open_target_admission;
} ClusterSemanticActivationDescriptor;

extern ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token);
extern ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *r4_generation_out);
extern bool cluster_semantic_activation_phase1_pristine(void);
extern bool cluster_semantic_activation_r11_cutover_snapshot(
	ClusterSemanticR11CutoverSnapshot *out);
extern bool cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token);
extern ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(
	ClusterSemanticAdmissionToken *token);
extern bool cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token);
extern bool cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out);
extern bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out);
extern bool
cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out);
extern bool cluster_semantic_activation_peer_open_matches(
	const ClusterSemanticAdmissionToken *token, int32 authenticated_peer_node_id,
	uint32 required_hello_caps, uint32 sampled_capability_generation);
extern bool cluster_semantic_activation_resource_x_peer_open_matches(
	const ClusterSemanticAdmissionToken *token, int32 authenticated_peer_node_id,
	uint32 sampled_capability_generation);
extern ClusterSemanticResourceXPeerOpenResult
cluster_semantic_activation_resource_x_peer_open_check(
	const ClusterSemanticAdmissionToken *token, int32 authenticated_peer_node_id,
	uint32 sampled_capability_generation);
extern bool cluster_semantic_activation_restore_open_proof_if_active(void);
extern void cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token);
extern bool cluster_semantic_activation_backend_has_admission(void);
extern ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable_admission,
									   ClusterSemanticAdmissionToken *token);
extern bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
									 bool writable_admission);
extern Size cluster_semantic_activation_shmem_size(void);
extern void cluster_semantic_activation_shmem_init(void);
/* RF-ROOT P7 (contract §B): the bit22 cutover reader latch — the dual-path
 * gate idiom anchor (contract follow-up contract ②).  False = pre-bit22, the frozen
 * §17.8 wal-state authority; true = post-bit22 root-only. */
extern bool cluster_r4_bit22_cutover_active(void);
/* RF-ROOT P9 verification (implementation): serving/admission gate — only the
 * TARGET_VERIFIED state (phase-4 CF(S) strong revalidation) allows
 * ordinary serving. */
extern bool cluster_r4_bit22_cutover_verified(void);
/* RF-ROOT P9 verification (implementation): source-close freeze for the bit22
 * first-open round (wal-state registry writer gate). */
extern bool cluster_r4_bit22_source_writer_enter(void);
extern void cluster_r4_bit22_source_writer_leave(void);
extern bool cluster_r4_bit22_source_close_begin(uint64 transition_epoch,
												uint64 prepare_generation);
extern bool cluster_r4_bit22_source_close_current(uint64 transition_epoch,
												  uint64 prepare_generation);
extern bool cluster_r4_bit22_cutover_latch_verify(void);
extern bool cluster_r4_bit22_cutover_latch_apply(uint64 transition_epoch,
												 uint64 round_generation);
/* contract: the round driver stages the PREPARED token/sha/round here after
 * create_prepared; the coordinator LMON consumes it at the OPEN_APPLIED
 * advance (step ②). */
extern bool cluster_r4_bit22_cutover_seam_store(
	const ClusterControlRootFileToken *file_token,
	const uint8 round_sha[PG_SHA256_DIGEST_LENGTH],
	const ClusterControlRootMigrationRoundV1 *round);
/* contract step ④c: the round driver entry — coordinator backend, with the
 * constructed migration image + round. */
extern bool cluster_r4_bit22_cutover_begin(
	const ClusterControlRootMigrationImage *image,
	const ClusterControlRootMigrationRoundV1 *round);
extern void
cluster_semantic_activation_register(const ClusterSemanticActivationDescriptor *descriptor);
extern const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_descriptor(uint64 feature_bit);
extern uint64 cluster_semantic_activation_compiled_feature_bitmap(void);
extern bool cluster_semantic_activation_record_encode(const ClusterSemanticActivationRecord *record,
												  uint8 bytes[512]);
extern bool cluster_semantic_activation_record_decode(const uint8 bytes[512],
												  ClusterSemanticActivationRecord *record,
												  ClusterSemanticActivationRefusal *refusal);
extern bool cluster_semantic_activation_ack_wire_encode(
	const ClusterSemanticActivationAckWireV1 *message,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES]);
extern bool cluster_semantic_activation_ack_wire_decode(
	const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES],
	ClusterSemanticActivationAckWireV1 *message);
extern ClusterSemanticActivationResult cluster_semantic_activation_record_cas_write(
	uint64 expected_generation, uint64 expected_source_feature_bitmap, const uint8 bytes[512]);
extern bool
cluster_semantic_activation_qvotec_poll_record_cas(ClusterSemanticActivationCasRequest *out);
extern bool
cluster_semantic_activation_qvotec_complete_record_cas(uint64 request_seq,
												   ClusterSemanticActivationResult result);
extern bool cluster_semantic_activation_qvotec_poll_record_read(
	ClusterSemanticActivationReadRequest *out);
extern bool cluster_semantic_activation_qvotec_complete_record_read(
	uint64 request_seq, ClusterSemanticActivationResult result,
	bool implicit_open,
	const uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES]);
extern bool cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq);
extern bool cluster_semantic_activation_qvotec_pgrd_formation_matches(
	const ClusterSemanticFormationBinding *formation);
extern bool cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
	ClusterUndoRootDescriptorRequest *out);
extern bool cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
	uint64 request_seq, ClusterSemanticActivationResult result);
extern bool
cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationResult *out_result);
extern bool cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
	ClusterUndoRootDescriptorReadRequest *out);
extern bool cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
	uint64 request_seq, ClusterUndoRootDescriptorState state,
	const uint8 selected_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
extern void cluster_semantic_activation_ack_handler(
	const ClusterICEnvelope *env, const void *payload);
extern void cluster_semantic_activation_lmon_tick(void);
/* RF-ROOT P7 G3: the R4 cutover coordinator proof reads the ACK table's
 * COMPLETE state bound to the exact round identity (transition epoch,
 * prepare generation, the expected/observed member sets, the source/target
 * feature bitmaps and the capability sample digest).  True only when every
 * member's ACK was observed (observed == expected) for THIS round AND the
 * table stands at (or beyond) minimum_stage — SAMPLE for the create proof,
 * PREPARED for the activate proof (the W6 clause 3 CLOSED binding). */
extern bool cluster_semantic_activation_ack_complete_matches(
	uint64 transition_epoch, uint64 record_generation,
	uint64 expected_members_lo, uint64 expected_members_hi,
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 capability_sample_digest,
	ClusterSemanticActivationAckStage minimum_stage);
extern ClusterSemanticActivationResult
cluster_semantic_activation_submit(ClusterSemanticActivationAction action,
								   ClusterSemanticActivationRefusal *refusal);
extern void ExecAlterSystemRacTwoStage(AlterSystemRacTwoStageStmt *stmt);
extern const ClusterSemanticActivationDescriptor *cluster_semantic_activation_r4_descriptor(void);
extern const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_r11_resource_x_descriptor(void);

#endif /* CLUSTER_SEMANTIC_ACTIVATION_H */
