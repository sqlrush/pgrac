/*-------------------------------------------------------------------------
 *
 * cluster_semantic_activation.c
 *	  Shared two-stage semantic activation framework.
 *
 * This first dependency-light body is deliberately fail-closed.  It gives
 * the exact frozen public types and the real R4A readiness adapter an
 * executable home before any durable, LMON, IC, parser, or positive
 * activation integration is admitted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_epoch_ballot.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_replacement_wire.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_control_root.h" /* bit22 feature bit (contract OPEN_APPLIED) */
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_state.h" /* GATE-BOUND census self-check (implementation, follow-up contract ②) */
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#define CLUSTER_SEMANTIC_RECORD_MAGIC UINT32_C(0x50475341)
#define CLUSTER_SEMANTIC_RECORD_VERSION 1
#define CLUSTER_SEMANTIC_RECORD_HEADER_LEN 104
#define CLUSTER_SEMANTIC_RECORD_CRC_OFFSET 96
#define CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES 3
#define CLUSTER_SEMANTIC_ADMISSION_COUNTER_TRIES 16
#define CLUSTER_SEMANTIC_UTILITY_WAIT_STEP_US 10000L
#define CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS                                  \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1     \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                 \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)
#define CLUSTER_REPLACEMENT_PHASE3_REQUIRED_CAPS \
	PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1

typedef struct ClusterSemanticRecordSample {
	bool readable;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterSemanticRecordSample;

typedef struct ClusterSemanticActivationShmem {
	pg_atomic_uint64 record_cas_request_seq;
	pg_atomic_uint64 record_cas_completion_seq;
	pg_atomic_uint32 record_cas_result;
	pg_atomic_uint32 record_cas_request_kind;
	uint64 record_cas_expected_generation;
	uint64 record_cas_expected_source_feature_bitmap;
	uint8 record_cas_desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	pg_atomic_uint64 admission_seq;
	pg_atomic_uint64 active_bits;
	pg_atomic_uint64 record_generation;
	pg_atomic_uint64 formation_epoch;
	pg_atomic_uint32 transition_closed;
	pg_atomic_uint32 inflight[2][64];
} ClusterSemanticActivationShmem;

typedef struct ClusterSemanticActivationUtilityMailboxShmem {
	pg_atomic_uint64 utility_request_seq;
	pg_atomic_uint64 utility_completion_seq;
	pg_atomic_uint32 utility_mailbox_state;
	uint32 utility_action;
	uint64 utility_source_feature_bitmap;
	uint64 utility_target_feature_bitmap;
	uint64 utility_rollback_feature_bitmap;
	uint64 utility_expected_record_generation;
	pg_atomic_uint32 utility_result;
	uint64 utility_result_feature_bit;
	uint64 utility_result_expected_generation;
} ClusterSemanticActivationUtilityMailboxShmem;

typedef struct ClusterSemanticActivationPgrdSnapshotShmem {
	pg_atomic_uint64 publication_seq;
	pg_atomic_uint32 present;
	uint32 reserved;
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
} ClusterSemanticActivationPgrdSnapshotShmem;

StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, record_cas_request_kind) == 20,
				 "semantic authority request kind must occupy prior padding");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, admission_seq) == 552,
				 "semantic admission sequence must follow the unchanged CAS mailbox");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, active_bits) == 560,
				 "semantic admission active bitmap offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, record_generation) == 568,
				 "semantic admission record generation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, formation_epoch) == 576,
				 "semantic admission formation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, transition_closed) == 584,
				 "semantic admission closed flag offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, inflight) == 588,
				 "semantic admission inflight offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticActivationShmem) == 1104,
				 "semantic activation shared gate must retain its frozen layout");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_request_seq) == 0,
				 "semantic utility request sequence offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_mailbox_state) == 16,
				 "semantic utility mailbox state offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_expected_record_generation) == 48,
				 "semantic utility expected generation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_result_feature_bit) == 64,
				 "semantic utility refusal feature offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticActivationUtilityMailboxShmem) == 80,
				 "semantic utility mailbox must retain its natural layout");
StaticAssertDecl(offsetof(ClusterSemanticActivationPgrdSnapshotShmem,
					  publication_seq) == 0,
				 "semantic PGRD snapshot publication sequence must be first");
StaticAssertDecl(offsetof(ClusterSemanticActivationPgrdSnapshotShmem,
					  present) == 8,
				 "semantic PGRD snapshot presence offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationPgrdSnapshotShmem,
					  descriptor_bytes) == 16,
				 "semantic PGRD snapshot bytes offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticActivationPgrdSnapshotShmem) == 528,
				 "semantic PGRD snapshot must retain its natural layout");

static ClusterSemanticActivationShmem *SemanticActivationShmem = NULL;
static ClusterSemanticActivationUtilityMailboxShmem
	*SemanticActivationUtilityMailbox = NULL;
static ClusterSemanticActivationPgrdSnapshotShmem
	*SemanticActivationPgrdSnapshot = NULL;

/*
 * RF-ROOT P7 (Stage 8 contract / contract follow-up): the bit22 cutover
 * reader latch.  Frozen §17.8 keeps the wal-state registry as the selected
 * authority until bit22 opens; §17.9's exactly-zero census is a POST-bit22
 * static proof (gate modeling), not a pre-bit22 precondition.  Readers gate
 * on this latch: false -> registry branch (pre-bit22 authority), true ->
 * root-only branch.  The latch is node-local shmem, defaults to 0, is
 * monotonic (a one-shot 0->1 CAS), and any uncertainty (shmem absent) reads
 * as false — fail-closed to the frozen pre-bit22 behavior.  The SETTER is
 * wired by the bit22 first-open round (task 4 / contract §E): a node latches
 * when its cutover FSM reaches OPEN_APPLIED bound to the round identity;
 * until that driver lands the latch never sets and every reader takes the
 * pre-bit22 branch.  Kept OUT of ClusterSemanticActivationShmem whose layout
 * is frozen (StaticAssertDecl sizeof == 1104 above).
 */
/*
 * RF-ROOT P9 verification (verified implementation): the latch is three-state —
 * 0 = SOURCE (pre-bit22, registry authority), 1 = TARGET_BOOTSTRAP (the
 * durable Target OPEN proof was found at startup: recovery planning may
 * select the root, ordinary serving is NOT yet allowed), 2 =
 * TARGET_VERIFIED (the phase-4 CF(S) strong revalidation succeeded:
 * ordinary serving/admission allowed).  Reader gates accept states 1 and
 * 2; serving gates require state 2.
 */
typedef enum ClusterR4Bit22LatchState {
	CLUSTER_R4_BIT22_SOURCE = 0,
	CLUSTER_R4_BIT22_TARGET_BOOTSTRAP = 1,
	CLUSTER_R4_BIT22_TARGET_VERIFIED = 2
} ClusterR4Bit22LatchState;

typedef struct ClusterR4Bit22CutoverLatchShmem {
	pg_atomic_uint32 active; /* ClusterR4Bit22LatchState */
	uint32 reserved;
	/* Round identity.  Atomic since apply() writes BEFORE the CAS
	 * (contract: the CAS loser reads back its own or a same-round winner's
	 * identity to prove the OPEN_APPLIED publication completed). */
	pg_atomic_uint64 transition_epoch;
	pg_atomic_uint64 round_generation;
} ClusterR4Bit22CutoverLatchShmem;

static ClusterR4Bit22CutoverLatchShmem *SemanticActivationBit22Latch = NULL;

/*
 * RF-ROOT P9 verification (verified implementation): source-close shmem for the
 * bit22 first-open round.  closed=1 freezes every wal-state registry
 * writer (new enter() calls refuse); the coordinator waits for
 * writer_count==0 and the all-member BARRIER ACK before building the
 * migration image, so ACTIVE slots are provably frozen — the migration
 * input accepts them (no offline STOPPED requirement).  Round identity
 * (transition_epoch + prepare_generation) binds the freeze to the exact
 * cutover round.
 */
typedef struct ClusterR4Bit22SourceCloseShmem {
	pg_atomic_uint32 closed;
	pg_atomic_uint32 writer_count;
	pg_atomic_uint64 transition_epoch;
	pg_atomic_uint64 prepare_generation;
} ClusterR4Bit22SourceCloseShmem;

static ClusterR4Bit22SourceCloseShmem *SemanticActivationBit22SourceClose = NULL;

/*
 * RF-ROOT P7 (contract, step ②): the bit22 cutover round seam.  The round
 * DRIVER (step ④) stores the PREPARED file token + round sha + round copy
 * here after create_prepared; the coordinator LMON consumes it when the
 * PREPARED-stage all-member ACK is COMPLETE, to call
 * cluster_control_root_activate_prepared (executor: coordinator LMON —
 * CF(X) has no frozen executor, AD-023 §4 binds CF(S) only; follow-up
 * precedent).  Kept OUT of the frozen ACK table and the frozen gate struct.
 */
typedef struct ClusterR4Bit22CutoverSeamShmem {
	pg_atomic_uint32 valid; /* 1 = staged by the driver */
	uint32 reserved;
	uint64 transition_epoch; /* round identity, cross-checked vs the ACK table */
	uint64 prepare_generation;
	ClusterControlRootFileToken file_token;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootMigrationRoundV1 round;
} ClusterR4Bit22CutoverSeamShmem;

static ClusterR4Bit22CutoverSeamShmem *SemanticActivationBit22Seam = NULL;
static uint32 semantic_activation_local_inflight[2][64];

bool
cluster_semantic_activation_backend_has_admission(void)
{
	for (unsigned side = 0; side < lengthof(semantic_activation_local_inflight); side++)
		for (unsigned bit = 0; bit < lengthof(semantic_activation_local_inflight[side]); bit++)
			if (semantic_activation_local_inflight[side][bit] != 0)
				return true;
	return false;
}
static int semantic_activation_exit_hook_pid;
static uint64 semantic_activation_lmon_record_read_seq;
static uint64 semantic_activation_lmon_pgrd_request_seq;
static uint64 semantic_activation_lmon_pgrd_utility_request_seq;
static ClusterSemanticFormationBinding semantic_activation_lmon_pgrd_formation;
static uint64 semantic_activation_lmon_pgrd_candidate_request_seq;
static uint8 semantic_activation_lmon_pgrd_candidate
	[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static uint64 semantic_activation_lmon_pgrd_read_request_seq;
static uint64 semantic_activation_lmon_pgrd_read_utility_request_seq;
static ClusterSemanticFormationBinding semantic_activation_lmon_pgrd_read_formation;
static uint64 semantic_activation_lmon_member_pgrd_read_request_seq;
static uint64 semantic_activation_lmon_prepare_cas_seq;
static uint64 semantic_activation_lmon_prepare_cas_utility_request_seq;
static uint64 semantic_activation_lmon_commit_cas_seq;
static uint64 semantic_activation_lmon_commit_cas_utility_request_seq;
/* RF-ROOT P9 verification (verified implementation): the bit22 cutover round's
 * majority OPEN(P+2) CAS (durable Target OPEN proof). */
static uint64 semantic_activation_lmon_open_cas_seq;
static uint64 semantic_activation_lmon_open_cas_utility_request_seq;

static bool semantic_activation_record_cas_mailbox_submit(
	uint64 expected_generation, uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], uint64 *out_request_seq)
	pg_attribute_unused();
static bool semantic_activation_record_cas_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationResult *out_result) pg_attribute_unused();
static bool semantic_activation_record_read_mailbox_submit(
	uint64 *out_request_seq);
static bool semantic_activation_record_read_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationReadCompletion *out);
static bool semantic_activation_authority_mailbox_submit(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq);
static bool semantic_activation_authority_mailbox_complete(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult result);
static bool semantic_activation_authority_mailbox_completion_matches(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq);
static bool semantic_activation_authority_mailbox_poll_completion(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult *out_result);
static bool semantic_activation_authority_request_formation_binding(
	ClusterSemanticAuthorityRequestKind request_kind,
	ClusterSemanticFormationBinding *out);
static bool semantic_activation_record_cas_formation_matches(
	const ClusterSemanticFormationBinding *formation,
	const ClusterSemanticActivationRecord *desired);
static void semantic_activation_pgrd_snapshot_clear(void);
static bool semantic_activation_pgrd_snapshot_publish(
	const uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
static bool semantic_activation_pgrd_snapshot_copy(
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
static bool semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
	uint64 request_seq, ClusterUndoRootDescriptorReadCompletion *out);
static bool semantic_activation_lmon_shared_pgrd_root_directory(
	char root_directory[MAXPGPATH]);

typedef enum SemanticActivationUtilityMailboxState {
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE = 0,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_WRITING = 1,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING = 2,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE = 3
} SemanticActivationUtilityMailboxState;

typedef struct SemanticActivationUtilityRequest {
	uint64 request_seq;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 rollback_feature_bitmap;
	uint64 expected_record_generation;
	ClusterSemanticActivationAction action;
} SemanticActivationUtilityRequest;

static bool semantic_activation_utility_mailbox_submit(
	ClusterSemanticActivationAction action, uint64 source_feature_bitmap,
	uint64 target_feature_bitmap, uint64 rollback_feature_bitmap,
	uint64 expected_record_generation, uint64 *out_request_seq);
static bool semantic_activation_utility_mailbox_poll(
	SemanticActivationUtilityRequest *out);
static bool semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation);
static bool semantic_activation_utility_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal);
static ClusterSemanticActivationResult
semantic_activation_utility_mailbox_wait(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal);

typedef enum SemanticActivationState {
	SEMANTIC_ACTIVATION_STATE_INVALID = -1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN = 0,
	SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED = 1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY = 2,
	SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO = 3,
	SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER = 4,
	SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO = 5,
	SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER = 6,
	SEMANTIC_ACTIVATION_STATE_TARGET_STAGED = 7,
	SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED = 8,
	SEMANTIC_ACTIVATION_STATE_TARGET_OPEN = 9
} SemanticActivationState;

typedef enum SemanticActivationCallbackKind {
	SEMANTIC_ACTIVATION_CALLBACK_NONE = 0,
	SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE,
	SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET,
	SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED,
	SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET
} SemanticActivationCallbackKind;

typedef struct SemanticActivationFailurePolicy {
	SemanticActivationState target;
	bool admission_closed_until_source_open;
	bool revert_source_closed;
} SemanticActivationFailurePolicy;

typedef struct SemanticActivationAdmissionSnapshot {
	uint64 seq;
	uint64 active_bits;
	uint64 record_generation;
	uint64 formation_epoch;
	bool transition_closed;
} SemanticActivationAdmissionSnapshot;

typedef struct SemanticActivationAckTuple {
	uint32 node_id;
	uint64 boot_id;
	uint64 admitted_incarnation;
	uint64 control_connection_generation;
	uint32 capability_word;
	uint64 capability_generation;
	uint64 transition_epoch;
	uint64 record_generation;
} SemanticActivationAckTuple;

typedef struct ClusterSemanticActivationAckTableV1 {
	pg_atomic_uint64 publication_seq;
	uint32 stage;
	uint32 flags;
	uint32 coordinator_node;
	uint32 reserved;
	uint64 round_nonce;
	uint64 expected_members_lo;
	uint64 expected_members_hi;
	uint64 observed_members_lo;
	uint64 observed_members_hi;
	uint64 transition_epoch;
	uint64 record_generation;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 rollback_feature_bitmap;
	uint64 capability_sample_digest;
	SemanticActivationAckTuple expected[CLUSTER_MAX_NODES];
	SemanticActivationAckTuple observed[CLUSTER_MAX_NODES];
} ClusterSemanticActivationAckTableV1;

typedef struct SemanticActivationAckIngressItem {
	ClusterSemanticActivationAckWireV1 message;
	int32 authenticated_source_node_id;
	int32 local_receiver_node_id;
	uint32 sampled_capability_word;
	uint32 sampled_capability_generation;
} SemanticActivationAckIngressItem;

typedef struct SemanticActivationAckIngress {
	uint64 producer_seq;
	uint64 consumer_seq;
	SemanticActivationAckIngressItem
		items[CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY];
} SemanticActivationAckIngress;

typedef struct SemanticActivationAckPendingSend {
	ClusterSemanticActivationAckWireV1 message;
	uint64 pending_members_lo;
	uint64 pending_members_hi;
	uint32 refusal_connection_generation;
	bool invalidated;
} SemanticActivationAckPendingSend;

typedef struct SemanticActivationAckRequestOrigin {
	SemanticActivationAckPendingSend current;
	uint64 unsent_members_lo;
	bool active;
	bool coordinator_ack_started;
} SemanticActivationAckRequestOrigin;

#define SEMANTIC_ACTIVATION_ACK_STAGE_AHEAD_CAPACITY UINT32_C(4)

typedef struct SemanticActivationAckStageAhead {
	uint32 count;
	SemanticActivationAckIngressItem
		items[SEMANTIC_ACTIVATION_ACK_STAGE_AHEAD_CAPACITY];
} SemanticActivationAckStageAhead;

/* Process-local, single-slot retention for one exact stage-ahead request.
 * BARRIER, a later Resource-X stage, or a later bit22 cutover stage may
 * arrive while the exact local predecessor is still incomplete.  The slot
 * is evidence only; retaining it does not advance the table or create an
 * ACK. */
typedef struct SemanticActivationAckRequestAhead {
	bool valid;
	SemanticActivationAckIngressItem item;
} SemanticActivationAckRequestAhead;

typedef enum SemanticActivationAckRequestAheadState {
	SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID = 0,
	SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT,
	SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY
} SemanticActivationAckRequestAheadState;

typedef enum SemanticActivationAckIngressResult {
	SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED = 0,
	SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED,
	SEMANTIC_ACTIVATION_ACK_INGRESS_FULL
} SemanticActivationAckIngressResult;

typedef enum SemanticActivationAckConsumeResult {
	SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED = 0,
	SEMANTIC_ACTIVATION_ACK_CONSUME_STALE,
	SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED,
	SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE,
	SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
} SemanticActivationAckConsumeResult;

typedef enum SemanticActivationAckSendResult {
	SEMANTIC_ACTIVATION_ACK_SEND_REJECTED = 0,
	SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED,
	SEMANTIC_ACTIVATION_ACK_SEND_RETAINED,
	SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED
} SemanticActivationAckSendResult;

static SemanticActivationAckIngress semantic_activation_ack_local_ingress;
static SemanticActivationAckPendingSend
	semantic_activation_ack_local_pending_send;
static SemanticActivationAckRequestOrigin
	semantic_activation_ack_local_request_origin;
static SemanticActivationAckStageAhead
	semantic_activation_ack_local_stage_ahead;
static SemanticActivationAckRequestAhead
	semantic_activation_ack_local_request_ahead;

/* RF-ROOT P9 verification (cold-formation): the bit22 cutover round's
 * PREPARE-record CAS (majority legacy-zero -> generation 1), driven from
 * the advance at BARRIER COMPLETE.  One-shot: seq latches the in-flight
 * mailbox request, done latches the completed durable write. */
static uint64 semantic_activation_lmon_bit22_prepare_cas_seq = 0;
static bool semantic_activation_lmon_bit22_prepare_cas_done = false;
static uint64 semantic_activation_ack_ingress_result_count[3];

StaticAssertDecl(sizeof(SemanticActivationAckTuple)
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES,
				 "semantic activation ACK tuple must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterSemanticActivationAckTableV1)
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_TABLE_BYTES,
				 "semantic activation ACK table must remain 16496 bytes");
StaticAssertDecl(sizeof(SemanticActivationAckIngressItem) == 136,
				 "semantic activation ACK ingress item must remain 136 bytes");
StaticAssertDecl(sizeof(SemanticActivationAckIngress) == 34832,
				 "semantic activation ACK ingress must remain 34832 bytes");

static ClusterSemanticActivationAckTableV1 *SemanticActivationAckTable = NULL;
static bool semantic_activation_member_pgrd_read_mailbox_submit(
	const ClusterSemanticActivationAckTableV1 *expected_table,
	uint64 *out_request_seq);

/* RF-ROOT P7 (contract): bit22 cutover round — member-side OPEN_APPLIED
 * stage apply.  Round-parameterized (member set driven by the ACK table,
 * target must carry bit22); deliberately does NOT reuse the R4
 * four-member hardcoded checks (contract option A). */
static bool semantic_activation_ack_member_open_applied_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self);
static bool semantic_activation_ack_member_bit22_stage_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint32 stage, SemanticActivationAckTuple *out_self);
static bool semantic_activation_ack_lmon_progress_member_open_applied(
	const ClusterSemanticActivationAckTableV1 *before);
static bool semantic_activation_ack_lmon_finish_member_open_applied(
	const ClusterSemanticActivationAckTableV1 *before,
	bool latch_applied);
static bool semantic_activation_ack_lmon_finish_member_prepared(
	const ClusterSemanticActivationAckTableV1 *before,
	ClusterSemanticActivationResult callback_result);
static bool semantic_activation_ack_member_prepared_image_current_bit22(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self);
static bool semantic_activation_ack_lmon_send_bit22_prepared_requests(
	uint32 stage);
static bool semantic_activation_ack_lmon_bit22_commit_applied_begin(
	const ClusterSemanticActivationAckTableV1 *before,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word);
static bool semantic_activation_ack_lmon_bit22_open_applied_begin(
	const ClusterSemanticActivationAckTableV1 *before,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word);
static bool semantic_activation_ack_lmon_bit22_advance(void);
static bool semantic_activation_ack_lmon_progress_member_commit_applied_bit22(
	const ClusterSemanticActivationAckTableV1 *before);
static bool semantic_activation_ack_lmon_progress_member_barrier_bit22(
	const ClusterSemanticActivationAckTableV1 *before);

static void semantic_activation_ack_ingress_init(
	SemanticActivationAckIngress *ingress) pg_attribute_unused();
static uint32 semantic_activation_ack_ingress_pending(
	const SemanticActivationAckIngress *ingress) pg_attribute_unused();
static bool semantic_activation_ack_ingress_push(
	SemanticActivationAckIngress *ingress,
	const SemanticActivationAckIngressItem *item) pg_attribute_unused();
static bool semantic_activation_ack_ingress_poll(
	SemanticActivationAckIngress *ingress,
	SemanticActivationAckIngressItem *out) pg_attribute_unused();
static SemanticActivationAckSendResult
semantic_activation_ack_pending_send_note_result(
	SemanticActivationAckPendingSend *pending, int32 dest_node_id,
	ClusterICSendResult send_result) pg_attribute_unused();
static bool semantic_activation_ack_pending_send_begin_positive(
	SemanticActivationAckPendingSend *pending,
	const ClusterSemanticActivationAckWireV1 *request,
	int32 local_node_id,
	const SemanticActivationAckTuple *self) pg_attribute_unused();
static bool semantic_activation_ack_pending_send_begin_refused(
	SemanticActivationAckPendingSend *pending,
	const SemanticActivationAckIngressItem *item,
	ClusterSemanticActivationResult reason) pg_attribute_unused();
static bool semantic_activation_ack_lmon_begin_sample_round(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
	pg_attribute_unused();
static bool semantic_activation_ack_lmon_submit_prepare(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
	pg_attribute_unused();
static bool semantic_activation_ack_lmon_install_prepare(
	const SemanticActivationUtilityRequest *request) pg_attribute_unused();
static bool semantic_activation_ack_lmon_submit_commit(
	const SemanticActivationUtilityRequest *request,
	const ClusterSemanticActivationRecord *prepare) pg_attribute_unused();
static bool semantic_activation_ack_lmon_install_commit(
	const SemanticActivationUtilityRequest *request) pg_attribute_unused();
static bool semantic_activation_ack_lmon_install_open(
	const SemanticActivationUtilityRequest *request) pg_attribute_unused();
static bool semantic_activation_ack_lmon_begin_barrier_round(
	const SemanticActivationUtilityRequest *request,
	const ClusterSemanticActivationRecord *prepare) pg_attribute_unused();
static bool semantic_activation_snapshot(
	SemanticActivationAdmissionSnapshot *snapshot);
static SemanticActivationAckIngressResult
semantic_activation_ack_ingress_receive(
	SemanticActivationAckIngress *ingress, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length,
	int32 local_receiver_node_id, uint64 current_epoch) pg_attribute_unused();
static bool semantic_activation_ack_remote_tuple(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	SemanticActivationAckTuple *out) pg_attribute_unused();
static bool semantic_activation_ack_self_tuple(
	int32 local_node_id, uint32 local_capability_word,
	uint64 transition_epoch, uint64 record_generation,
	SemanticActivationAckTuple *out) pg_attribute_unused();
static bool semantic_activation_ack_complete_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id,
	uint32 local_capability_word) pg_attribute_unused();
static bool semantic_activation_ack_current_authority(
	int32 local_node_id, uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch,
	int32 *out_coordinator_node) pg_attribute_unused();
static bool semantic_activation_ack_terminal_identity_not_contradicted(
	const ClusterSemanticActivationAckTableV1 *image);
static bool semantic_activation_ack_member_present(
	uint64 members_lo, uint64 members_hi, int32 node_id);
static bool semantic_activation_ack_wire_value_valid(
	const ClusterSemanticActivationAckWireV1 *message);
static bool semantic_activation_bytes_are_zero(
	const uint8 *bytes, Size len);
static bool semantic_activation_ack_sample_digest(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 *digest_out);
static bool semantic_activation_r4_initial_clean_sample_basis(
	uint64 expected_generation, bool coordinator_only);
static bool semantic_activation_round_descriptor(
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 rollback_feature_bitmap,
	const ClusterSemanticActivationDescriptor **descriptor_out,
	uint32 *required_caps_out);
static bool semantic_activation_ack_round_required_caps(
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 rollback_feature_bitmap, uint32 *required_caps_out);
static bool semantic_activation_ack_member_prepared_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self);
static void semantic_activation_ack_lmon_invalidate_active(void);

static void
semantic_activation_ack_ingress_init(SemanticActivationAckIngress *ingress)
{
	if (ingress != NULL)
		memset(ingress, 0, sizeof(*ingress));
}

static uint32
semantic_activation_ack_ingress_pending(
	const SemanticActivationAckIngress *ingress)
{
	uint64 pending;

	if (ingress == NULL)
		return 0;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return 0;
	return (uint32)pending;
}

static bool
semantic_activation_ack_ingress_push(
	SemanticActivationAckIngress *ingress,
	const SemanticActivationAckIngressItem *item)
{
	uint64 pending;

	if (ingress == NULL || item == NULL)
		return false;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending >= CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return false;
	ingress->items[ingress->producer_seq
				   % CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY] = *item;
	ingress->producer_seq++;
	return true;
}

static bool
semantic_activation_ack_ingress_poll(
	SemanticActivationAckIngress *ingress,
	SemanticActivationAckIngressItem *out)
{
	uint64 pending;

	if (ingress == NULL || out == NULL)
		return false;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending == 0
		|| pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return false;
	*out = ingress->items[ingress->consumer_seq
				   % CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY];
	ingress->consumer_seq++;
	return true;
}

static SemanticActivationAckSendResult
semantic_activation_ack_pending_send_note_result(
	SemanticActivationAckPendingSend *pending, int32 dest_node_id,
	ClusterICSendResult send_result)
{
	uint64 member_bit;
	uint64 *pending_members;

	if (pending == NULL || pending->invalidated
		|| dest_node_id < 0 || dest_node_id >= CLUSTER_MAX_NODES)
		return SEMANTIC_ACTIVATION_ACK_SEND_REJECTED;
	if (dest_node_id < 64) {
		member_bit = UINT64_C(1) << dest_node_id;
		pending_members = &pending->pending_members_lo;
	} else {
		member_bit = UINT64_C(1) << (dest_node_id - 64);
		pending_members = &pending->pending_members_hi;
	}
	if ((*pending_members & member_bit) == 0)
		return SEMANTIC_ACTIVATION_ACK_SEND_REJECTED;

	switch (send_result) {
	case CLUSTER_IC_SEND_DONE:
	case CLUSTER_IC_SEND_WOULD_BLOCK:
		*pending_members &= ~member_bit;
		return SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED;
	case CLUSTER_IC_SEND_NOT_ADMITTED:
		return SEMANTIC_ACTIVATION_ACK_SEND_RETAINED;
	case CLUSTER_IC_SEND_HARD_ERROR:
		pending->pending_members_lo = 0;
		pending->pending_members_hi = 0;
		pending->invalidated = true;
		return SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED;
	}
	return SEMANTIC_ACTIVATION_ACK_SEND_REJECTED;
}

static bool
semantic_activation_ack_pending_send_begin_positive(
	SemanticActivationAckPendingSend *pending,
	const ClusterSemanticActivationAckWireV1 *request,
	int32 local_node_id,
	const SemanticActivationAckTuple *self)
{
	SemanticActivationAckPendingSend candidate;
	uint64 self_bit;

	if (pending == NULL || request == NULL || self == NULL
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| self->node_id != (uint32)local_node_id)
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.message = *request;
	candidate.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	candidate.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	candidate.message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	candidate.message.member_node = (uint32)local_node_id;
	candidate.message.boot_id = self->boot_id;
	candidate.message.admitted_incarnation = self->admitted_incarnation;
	candidate.message.capability_word = self->capability_word;
	candidate.pending_members_lo = request->admitted_members_lo;
	candidate.pending_members_hi = request->admitted_members_hi;
	self_bit = UINT64_C(1) << (local_node_id < 64
								 ? local_node_id : local_node_id - 64);
	if (local_node_id < 64)
		candidate.pending_members_lo &= ~self_bit;
	else
		candidate.pending_members_hi &= ~self_bit;
	if (!semantic_activation_ack_wire_value_valid(&candidate.message))
		return false;
	*pending = candidate;
	return true;
}

/*
 * Build the existing-wire, authority-neutral reply for an initial R4 SAMPLE
 * that reached this LMON before an exact four-member admission snapshot.
 * The ingress item carries the authenticated coordinator connection sample;
 * retaining the send does not install an ACK table row or create a deadline.
 */
static bool
semantic_activation_ack_pending_send_begin_refused(
	SemanticActivationAckPendingSend *pending,
	const SemanticActivationAckIngressItem *item,
	ClusterSemanticActivationResult reason)
{
	SemanticActivationAckPendingSend candidate;
	const ClusterSemanticActivationAckWireV1 *request;
	uint64 coordinator_bit;
	int32 local_node_id;

	if (pending == NULL || item == NULL
		|| reason != CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED)
		return false;
	request = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(request)
		|| request->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| request->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| request->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| request->coordinator_node != UINT32_C(0)
		|| item->authenticated_source_node_id != 0
		|| local_node_id <= 0 || local_node_id >= 4
		|| request->member_node != (uint32)local_node_id
		|| request->admitted_members_lo != UINT64_C(0x0f)
		|| request->admitted_members_hi != 0
		|| request->source_feature_bitmap != 0
		|| request->target_feature_bitmap
		   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| request->rollback_feature_bitmap != 0
		|| request->capability_sample_digest != 0
		|| item->sampled_capability_generation == 0)
		return false;

	memset(&candidate, 0, sizeof(candidate));
	candidate.message = *request;
	candidate.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	candidate.message.result
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	candidate.message.reason = (uint32)reason;
	candidate.message.member_node = (uint32)local_node_id;
	candidate.message.boot_id = 0;
	candidate.message.admitted_incarnation = 0;
	candidate.message.capability_word = 0;
	coordinator_bit = UINT64_C(1) << request->coordinator_node;
	candidate.pending_members_lo = coordinator_bit;
	candidate.refusal_connection_generation
		= item->sampled_capability_generation;
	if (!semantic_activation_ack_wire_value_valid(&candidate.message))
		return false;

	/* The already-admitted exact refusal remains the duplicate receipt. */
	if (!pending->invalidated
		&& pending->message.kind
		   == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		&& pending->message.result
		   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED
		&& memcmp(&pending->message, &candidate.message,
				  sizeof(candidate.message)) == 0
		&& pending->refusal_connection_generation
		   == candidate.refusal_connection_generation)
		return true;
	if (pending->invalidated || pending->pending_members_lo != 0
		|| pending->pending_members_hi != 0)
		return false;
	*pending = candidate;
	return true;
}

static SemanticActivationAckIngressResult
semantic_activation_ack_ingress_receive(
	SemanticActivationAckIngress *ingress, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length,
	int32 local_receiver_node_id, uint64 current_epoch)
{
	ClusterSemanticActivationAckWireV1 message;
	SemanticActivationAckIngressItem item;
	uint32 capability_word;
	uint32 capability_generation;
	uint32 required_caps;
	uint64 pending;
	bool local_is_admitted;
	int32 authenticated_source_node_id;

	if (ingress == NULL || env == NULL || payload == NULL
		|| payload_length != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES
		|| env->payload_length != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES
		|| env->msg_type != PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1
		|| local_receiver_node_id < 0
		|| local_receiver_node_id >= CLUSTER_MAX_NODES
		|| env->source_node_id >= CLUSTER_MAX_NODES
		|| env->source_node_id == (uint32)local_receiver_node_id
		|| env->dest_node_id != (uint32)local_receiver_node_id
		|| env->epoch != current_epoch
		|| !cluster_semantic_activation_ack_wire_decode(
			(const uint8 *)payload, &message)
		|| message.transition_epoch != env->epoch)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	authenticated_source_node_id = (int32)env->source_node_id;
	local_is_admitted
		= local_receiver_node_id < 64
			  ? (message.admitted_members_lo
				 & (UINT64_C(1) << local_receiver_node_id)) != 0
			  : (message.admitted_members_hi
				 & (UINT64_C(1) << (local_receiver_node_id - 64))) != 0;
	if (!local_is_admitted)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	if (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST) {
		if (message.coordinator_node != env->source_node_id
			|| message.member_node != (uint32)local_receiver_node_id)
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	} else if (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK) {
		if (message.member_node != env->source_node_id)
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		if (message.result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK) {
			if (message.boot_id != message.admitted_incarnation)
				return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		} else if (message.result
				   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED) {
			if (message.coordinator_node != (uint32)local_receiver_node_id)
				return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		} else
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	} else
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	if (!semantic_activation_ack_round_required_caps(
			message.source_feature_bitmap,
			message.target_feature_bitmap,
			message.rollback_feature_bitmap, &required_caps))
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	if (!cluster_sf_peer_capability_word_sample(
			authenticated_source_node_id, required_caps,
			&capability_word, &capability_generation)
		|| capability_generation == 0
		|| (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
			&& message.result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
			&& message.capability_word != capability_word))
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	if (pending == CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_FULL;

	memset(&item, 0, sizeof(item));
	item.message = message;
	item.authenticated_source_node_id = authenticated_source_node_id;
	item.local_receiver_node_id = local_receiver_node_id;
	item.sampled_capability_word = capability_word;
	item.sampled_capability_generation = capability_generation;
	if (!semantic_activation_ack_ingress_push(ingress, &item))
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	return SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED;
}

static bool
semantic_activation_ack_remote_tuple(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	SemanticActivationAckTuple *out)
{
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	int32 source_node;
	bool source_is_admitted;
	bool coordinator_is_admitted;

	if (item == NULL || out == NULL
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| (current_members_lo == 0 && current_members_hi == 0))
		return false;
	message = &item->message;
	if (!semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps))
		return false;
	source_node = item->authenticated_source_node_id;
	if (source_node < 0 || source_node >= CLUSTER_MAX_NODES
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			item->local_receiver_node_id)
		|| message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)source_node
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| message->boot_id == 0
		|| message->boot_id != message->admitted_incarnation
		|| item->sampled_capability_generation == 0
		|| item->sampled_capability_word != message->capability_word
		|| (item->sampled_capability_word & required_caps) != required_caps)
		return false;

	source_is_admitted
		= source_node < 64
			  ? (current_members_lo & (UINT64_C(1) << source_node)) != 0
			  : (current_members_hi
				 & (UINT64_C(1) << (source_node - 64))) != 0;
	coordinator_is_admitted
		= current_coordinator_node < 64
			  ? (current_members_lo
				 & (UINT64_C(1) << current_coordinator_node)) != 0
			  : (current_members_hi
				 & (UINT64_C(1) << (current_coordinator_node - 64))) != 0;
	if (!source_is_admitted || !coordinator_is_admitted
		|| cluster_membership_get_state(source_node) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(source_node)
		   != message->admitted_incarnation
		|| !cluster_sf_peer_capability_generation_matches(
			source_node, required_caps,
			item->sampled_capability_generation))
		return false;

	memset(&tuple, 0, sizeof(tuple));
	tuple.node_id = (uint32)source_node;
	tuple.boot_id = message->boot_id;
	tuple.admitted_incarnation = message->admitted_incarnation;
	tuple.control_connection_generation
		= (uint64)item->sampled_capability_generation;
	tuple.capability_word = item->sampled_capability_word;
	tuple.capability_generation
		= (uint64)item->sampled_capability_generation;
	tuple.transition_epoch = message->transition_epoch;
	tuple.record_generation = message->record_generation;
	*out = tuple;
	return true;
}

static bool
semantic_activation_ack_self_tuple(
	int32 local_node_id, uint32 local_capability_word,
	uint64 transition_epoch, uint64 record_generation,
	SemanticActivationAckTuple *out)
{
	SemanticActivationAckTuple tuple;
	uint64 self_incarnation;

	if (out == NULL || local_node_id < 0
		|| local_node_id >= CLUSTER_MAX_NODES
		|| record_generation == 0
		|| (local_capability_word
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		return false;
	self_incarnation = cluster_qvotec_get_self_incarnation();
	if (self_incarnation == 0
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(local_node_id)
		   != self_incarnation)
		return false;

	memset(&tuple, 0, sizeof(tuple));
	tuple.node_id = (uint32)local_node_id;
	tuple.boot_id = self_incarnation;
	tuple.admitted_incarnation = self_incarnation;
	tuple.control_connection_generation = self_incarnation;
	tuple.capability_word = local_capability_word;
	tuple.capability_generation = self_incarnation;
	tuple.transition_epoch = transition_epoch;
	tuple.record_generation = record_generation;
	*out = tuple;
	return true;
}

static bool
semantic_activation_ack_current_authority(
	int32 local_node_id, uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch, int32 *out_coordinator_node)
{
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	uint64 current_epoch;
	int32 coordinator_node = -1;
	int32 node;
	bool local_is_member;

	if (out_members_lo == NULL || out_members_hi == NULL
		|| out_formation_epoch == NULL || out_coordinator_node == NULL)
		return false;
	*out_members_lo = 0;
	*out_members_hi = 0;
	*out_formation_epoch = 0;
	*out_coordinator_node = -1;
	if (local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| !cluster_qvotec_in_quorum()
		|| !cluster_reconfig_lmon_snapshot_admitted_membership(
			&members_lo, &members_hi, &formation_epoch)
		|| members_lo == 0 || members_hi != 0
		|| (members_lo & ~UINT64_C(0x0000ffff)) != 0)
		return false;

	current_epoch = cluster_epoch_get_current();
	local_is_member
		= local_node_id < 64
			  ? (members_lo & (UINT64_C(1) << local_node_id)) != 0
			  : (members_hi
				 & (UINT64_C(1) << (local_node_id - 64))) != 0;
	if (!local_is_member || formation_epoch != current_epoch
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER)
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool is_member
			= node < 64
				  ? (members_lo & (UINT64_C(1) << node)) != 0
				  : (members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if (is_member) {
			coordinator_node = node;
			break;
		}
	}
	if (coordinator_node < 0
		|| cluster_membership_get_state(coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| !cluster_qvotec_in_quorum()
		|| cluster_epoch_get_current() != current_epoch)
		return false;

	*out_members_lo = members_lo;
	*out_members_hi = members_hi;
	*out_formation_epoch = formation_epoch;
	*out_coordinator_node = coordinator_node;
	return true;
}

/*
 * A failed coherent snapshot is not by itself contradictory: QVOTEC may be
 * between publications.  Before retaining a completed OPEN carrier, however,
 * reject every contradiction that is already observable without inventing a
 * replacement authority.  A later consumer still needs the normal coherent
 * snapshot and complete-image revalidation before using the carrier.
 */
static bool
semantic_activation_ack_terminal_identity_not_contradicted(
	const ClusterSemanticActivationAckTableV1 *image)
{
	uint64 snapshot_members_lo = 0;
	uint64 snapshot_members_hi = 0;
	uint64 snapshot_epoch = 0;
	int node;

	if (image == NULL
		|| (image->expected_members_lo == 0
			&& image->expected_members_hi == 0)
		|| cluster_epoch_get_current() != image->transition_epoch)
		return false;

	/* A successfully captured snapshot is positive evidence.  If its exact
	 * identity differs, this is drift rather than temporary unavailability. */
	if (cluster_reconfig_lmon_snapshot_admitted_membership(
			&snapshot_members_lo, &snapshot_members_hi, &snapshot_epoch)
		&& (snapshot_members_lo != image->expected_members_lo
			|| snapshot_members_hi != image->expected_members_hi
			|| snapshot_epoch != image->transition_epoch))
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool expected_member = semantic_activation_ack_member_present(
			image->expected_members_lo, image->expected_members_hi, node);
		ClusterMembershipState state = cluster_membership_get_state(node);

		if (!expected_member) {
			if (state == CLUSTER_MEMBER_MEMBER)
				return false;
			continue;
		}
		if (state != CLUSTER_MEMBER_MEMBER
			|| image->expected[node].node_id != (uint32)node
			|| image->expected[node].admitted_incarnation == 0
			|| cluster_membership_get_last_admitted_incarnation(node)
			   != image->expected[node].admitted_incarnation)
			return false;

		if (node == cluster_node_id) {
			uint64 self_incarnation = cluster_qvotec_get_self_incarnation();

			if (self_incarnation != 0
				&& self_incarnation != image->expected[node].boot_id)
				return false;
		} else {
			uint64 observed_incarnation = 0;
			uint64 observed_generation = 0;

			if (cluster_reconfig_get_observed_slot(
					node, &observed_incarnation, &observed_generation)
				&& (observed_generation == 0
					|| observed_incarnation != image->expected[node].boot_id
					|| cluster_reconfig_get_observed_epoch(node)
					   != image->transition_epoch))
				return false;
		}
	}

	return true;
}

void
cluster_semantic_activation_ack_handler(
	const ClusterICEnvelope *env, const void *payload)
{
	SemanticActivationAckIngressResult result;
	uint32 payload_length = env != NULL ? env->payload_length : 0;

	result = semantic_activation_ack_ingress_receive(
		&semantic_activation_ack_local_ingress, env, payload,
		payload_length, cluster_node_id, cluster_epoch_get_current());
	if (result < SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED
		|| result > SEMANTIC_ACTIVATION_ACK_INGRESS_FULL)
		result = SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	if (semantic_activation_ack_ingress_result_count[result] != UINT64_MAX)
		semantic_activation_ack_ingress_result_count[result]++;
}

/*
 * cluster_semantic_activation_ack_complete_matches -- RF-ROOT P7 G3: the R4
 * cutover coordinator proof.  True iff the ACK table is COMPLETE (every
 * expected member observed == expected) AND bound to the exact round
 * identity passed by the caller (transition epoch, prepare generation,
 * member set, source/target feature bitmaps, capability sample digest) AND
 * standing at (or beyond) minimum_stage.  The round binding prevents a
 * stale table from a previous attempt from authorizing a new round; the
 * COMPLETE check is the all-member-ACK fact the bit22 cutover requires
 * (STOP-01 §17.7 W6 clause 3 binding).  minimum_stage distinguishes the
 * create proof (SAMPLE-round COMPLETE suffices to land PREPARED) from the
 * activate proof (only the PREPARED-stage all-member ACK is the CLOSED
 * binding that opens bit22).
 */
static bool semantic_activation_ack_table_snapshot(
	ClusterSemanticActivationAckTableV1 *out);

bool
cluster_semantic_activation_ack_complete_matches(
	uint64 transition_epoch, uint64 record_generation,
	uint64 expected_members_lo, uint64 expected_members_hi,
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 capability_sample_digest,
	ClusterSemanticActivationAckStage minimum_stage)
{
	ClusterSemanticActivationAckTableV1 current;

	if (minimum_stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| minimum_stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		return false;
	if (!semantic_activation_ack_table_snapshot(&current))
		return false;
	if ((current.flags & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) == 0)
		return false;
	if (current.stage < minimum_stage)
		return false;
	return current.transition_epoch == transition_epoch
		&& current.record_generation == record_generation
		&& current.expected_members_lo == expected_members_lo
		&& current.expected_members_hi == expected_members_hi
		&& current.observed_members_lo == current.expected_members_lo
		&& current.observed_members_hi == current.expected_members_hi
		&& current.source_feature_bitmap == source_feature_bitmap
		&& current.target_feature_bitmap == target_feature_bitmap
		&& current.capability_sample_digest == capability_sample_digest;
}

static bool semantic_activation_ack_table_snapshot(
	ClusterSemanticActivationAckTableV1 *out);

static bool
semantic_activation_ack_table_snapshot(ClusterSemanticActivationAckTableV1 *out)
{
	ClusterSemanticActivationAckTableV1 candidate;
	uint64 seq_before;
	uint64 seq_after;
	int attempt;

	if (SemanticActivationAckTable == NULL || out == NULL)
		return false;
	for (attempt = 0; attempt < 3; attempt++) {
		seq_before = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		memcpy(&candidate, SemanticActivationAckTable, sizeof(candidate));
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if (seq_before == seq_after
			&& (seq_after & UINT64_C(1)) == 0) {
			memcpy(out, &candidate, sizeof(candidate));
			return true;
		}
	}
	return false;
}

static bool semantic_activation_ack_table_publish(
	const ClusterSemanticActivationAckTableV1 *image) pg_attribute_unused();
static bool semantic_activation_ack_matches(
	const SemanticActivationAckTuple *observed,
	const SemanticActivationAckTuple *expected);
static bool semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi);
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_apply_item(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch,
	int32 current_coordinator_node) pg_attribute_unused();
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_install_authorized_request(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word) pg_attribute_unused();
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_sample_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word) pg_attribute_unused();
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_barrier_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
	pg_attribute_unused();
/*
 * semantic_activation_ack_lmon_accept_current_barrier_request_bit22 --
 * RF-ROOT P9 verification (verified implementation): member-side acceptance of the
 * bit22 cutover round's BARRIER REQUEST.  Unlike the R4 accept (SAMPLE ->
 * BARRIER, four-member shape), the bit22 member table is built directly
 * from the wire message: stage BARRIER, round identity from the message,
 * expected tuples = self (voting-slot incarnation) + peers (observed
 * voting-slot incarnation — the presented identity; a fresh 2-node
 * cluster has no JCMK floor).  Capability generation 0 is legal (initial
 * tier1 connection).  Idempotent on a matching BARRIER table.
 */
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_barrier_request_bit22(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 local_capability_word;
	int32 local_node_id;
	int node;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| (message->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| (item->sampled_capability_word
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		|| snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| message->record_generation
		   != snapshot->record_generation + 1
		|| snapshot->active_bits != message->source_feature_bitmap
		|| snapshot->active_bits != 0
		|| message->rollback_feature_bitmap != 0)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	if (current.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		&& current.expected_members_lo == message->admitted_members_lo
		&& current.expected_members_hi == message->admitted_members_hi
		&& current.transition_epoch == message->transition_epoch
		&& current.record_generation == message->record_generation)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;

	local_capability_word = cluster_ic_local_capability_word();
	memset(&next, 0, sizeof(next));
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	next.coordinator_node = message->coordinator_node;
	next.round_nonce = message->round_nonce;
	next.transition_epoch = message->transition_epoch;
	next.record_generation = message->record_generation;
	next.expected_members_lo = message->admitted_members_lo;
	next.expected_members_hi = message->admitted_members_hi;
	next.source_feature_bitmap = message->source_feature_bitmap;
	next.target_feature_bitmap = message->target_feature_bitmap;
	next.rollback_feature_bitmap = message->rollback_feature_bitmap;
	next.capability_sample_digest = message->capability_sample_digest;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		SemanticActivationAckTuple tuple;

		if (!semantic_activation_ack_member_present(
				next.expected_members_lo, next.expected_members_hi, node))
			continue;
		if (node == local_node_id) {
			if (!semantic_activation_ack_self_tuple(
					node, local_capability_word, next.transition_epoch,
					next.record_generation, &next.expected[node]))
				return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
			continue;
		}
		{
			uint64 peer_admitted;
			uint32 peer_word = 0;
			uint32 peer_gen = 0;

			peer_admitted
				= cluster_membership_get_last_admitted_incarnation(node);
			if (peer_admitted == 0
				|| !cluster_sf_peer_capability_word_sample(
					node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
					&peer_word, &peer_gen)
				|| peer_gen == 0)
				return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
			memset(&tuple, 0, sizeof(tuple));
			tuple.node_id = (uint32)node;
			tuple.boot_id = peer_admitted;
			tuple.admitted_incarnation = peer_admitted;
			tuple.control_connection_generation = (uint64)peer_gen;
			tuple.capability_word = peer_word;
			tuple.capability_generation = (uint64)peer_gen;
			tuple.transition_epoch = next.transition_epoch;
			tuple.record_generation = next.record_generation;
			next.expected[node] = tuple;
		}
	}
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static bool semantic_activation_ack_expected_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id, uint32 local_capability_word);

/*
 * semantic_activation_ack_lmon_accept_current_request_bit22 -- RF-ROOT
 * P9 verification (cold-formation): member-side acceptance of the bit22
 * cutover round's PREPARED / COMMIT_APPLIED / OPEN_APPLIED REQUESTs —
 * the round-parameterized twin of the R4 four-member hardcoded accepts
 * (which reject any non-0x0f member set / non-R4 target).  The member's
 * table at each stage observes ONLY itself (the coordinator's observation
 * is coordinator-local), so the previous-stage check is: stage + round
 * identity + EXPECTED_VALID + observed == self-bit + self tuple ==
 * expected[self].  Generation semantics follow the R4 chain:
 *   PREPARED        request gen == snapshot gen        (current: BARRIER,   gen == snapshot gen)
 *   COMMIT_APPLIED  request gen == snapshot gen + 1    (current: PREPARED,   gen == snapshot gen)
 *   OPEN_APPLIED    request gen == snapshot gen + 2    (current: COMMIT_APPLIED, gen == snapshot gen + 1)
 * The source-close BARRIER COMPLETE publishes transition_closed via
 * semantic_activation_lmon_publish_gate (advance), so snapshot gen is the
 * closed-source generation.  The next table inherits the member set and
 * expected tuples from the current table, re-stamps their record
 * generation, clears observed (the member progress re-observes itself),
 * and re-validates through expected_image_current.  Fail-closed on any
 * mismatch; idempotent (DUPLICATE) when the table is already at the
 * requested stage with a matching round identity.
 */
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_request_bit22(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 stage;
	uint32 prev_stage;
	uint64 stage_gen_offset;
	uint64 prev_gen;
	uint64 self_bit;
	int32 local_node_id;
	int node;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	stage = message->stage;
	if (stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED) {
		prev_stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
		stage_gen_offset = 0;
		prev_gen = snapshot->record_generation;
	} else if (stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		prev_stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		stage_gen_offset = 1;
		prev_gen = snapshot->record_generation;
	} else if (stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
		prev_stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
		stage_gen_offset = 2;
		prev_gen = snapshot->record_generation + 1;
	} else
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	self_bit = UINT64_C(1) << local_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage != stage
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| (message->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| (item->sampled_capability_word
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		|| item->sampled_capability_generation == 0
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| !snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| snapshot->record_generation + stage_gen_offset
		   != message->record_generation
		|| snapshot->active_bits != message->source_feature_bitmap
		|| snapshot->active_bits != 0
		|| message->rollback_feature_bitmap != 0
		|| current.stage != prev_stage
		|| current.flags
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != prev_gen
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest == 0
		|| current.capability_sample_digest
		   != message->capability_sample_digest
		|| current.observed_members_lo != self_bit
		|| current.observed_members_hi != 0
		|| !semantic_activation_ack_member_present(
			current.expected_members_lo, current.expected_members_hi,
			local_node_id)
		|| !semantic_activation_ack_matches(
			&current.observed[local_node_id],
			&current.expected[local_node_id]))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	if (current.stage == stage)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;

	next = current;
	next.stage = stage;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.record_generation = message->record_generation;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	memset(next.observed, 0, sizeof(next.observed));
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		if (semantic_activation_ack_member_present(
				next.expected_members_lo, next.expected_members_hi, node))
			next.expected[node].record_generation
				= message->record_generation;
	if (!semantic_activation_ack_expected_image_current(
			&next, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_prepared_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word) pg_attribute_unused();
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_commit_applied_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word) pg_attribute_unused();

static bool
semantic_activation_ack_table_publish(
	const ClusterSemanticActivationAckTableV1 *image)
{
	const Size payload_offset
		= offsetof(ClusterSemanticActivationAckTableV1, stage);
	uint64 seq;

	if (SemanticActivationAckTable == NULL || image == NULL)
		return false;
	seq = pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq);
	if ((seq & UINT64_C(1)) != 0 || seq > UINT64_MAX - 2)
		return false;

	pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 1);
	pg_write_barrier();
	memcpy((uint8 *)SemanticActivationAckTable + payload_offset,
		   (const uint8 *)image + payload_offset,
		   sizeof(*SemanticActivationAckTable) - payload_offset);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 2);
	return true;
}

static bool
semantic_activation_ack_member_present(
	uint64 members_lo, uint64 members_hi, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return node_id < 64
			   ? (members_lo & (UINT64_C(1) << node_id)) != 0
			   : (members_hi & (UINT64_C(1) << (node_id - 64))) != 0;
}

static bool
semantic_activation_ack_tuple_structural(
	const SemanticActivationAckTuple *tuple, int32 node_id,
	uint64 transition_epoch, uint64 record_generation,
	uint32 required_caps)
{
	return tuple != NULL && node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && tuple->node_id == (uint32)node_id
		   && tuple->boot_id != 0
		   && tuple->boot_id == tuple->admitted_incarnation
		   && tuple->control_connection_generation != 0
		   && (tuple->capability_word & required_caps) == required_caps
		   && tuple->capability_generation != 0
		   && tuple->transition_epoch == transition_epoch
		   && tuple->record_generation == record_generation;
}

static bool
semantic_activation_ack_image_structural(
	const ClusterSemanticActivationAckTableV1 *image)
{
	uint32 required_caps;
	int node;

	if (image == NULL
		|| !semantic_activation_ack_round_required_caps(
			image->source_feature_bitmap, image->target_feature_bitmap,
			image->rollback_feature_bitmap, &required_caps)
		|| (image->expected_members_lo == 0
			&& image->expected_members_hi == 0)
		|| image->observed_members_lo != image->expected_members_lo
		|| image->observed_members_hi != image->expected_members_hi)
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!semantic_activation_ack_member_present(
				image->expected_members_lo, image->expected_members_hi, node))
			continue;
		if (!semantic_activation_ack_tuple_structural(
				&image->observed[node], node, image->transition_epoch,
				image->record_generation, required_caps))
			return false;
	}
	return true;
}

static bool
semantic_activation_ack_image_invalidate(
	ClusterSemanticActivationAckTableV1 *image)
{
	if (image == NULL)
		return false;
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
	image->flags = 0;
	image->expected_members_lo = 0;
	image->expected_members_hi = 0;
	image->observed_members_lo = 0;
	image->observed_members_hi = 0;
	memset(image->expected, 0, sizeof(image->expected));
	memset(image->observed, 0, sizeof(image->observed));
	return semantic_activation_ack_table_publish(image);
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_install_authorized_request(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckTuple self;
	const ClusterSemanticActivationAckWireV1 *message;
	const Size payload_offset
		= offsetof(ClusterSemanticActivationAckTableV1, stage);
	uint32 required_caps;
	int32 local_node_id;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| !semantic_activation_ack_self_tuple(
			local_node_id, local_capability_word, current_epoch,
			message->record_generation, &self))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	/* An exact directed SAMPLE REQUEST is idempotent for the lifetime of its
	 * installed round.  Peer ACKs may already have populated observed rows;
	 * replay must not erase them or restart the local positive fan-out. */
	if (current.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		&& (current.expected_members_lo != 0
			|| current.expected_members_hi != 0)) {
		uint64 self_bit = UINT64_C(1)
			<< (local_node_id < 64 ? local_node_id : local_node_id - 64);
		bool exact = current.coordinator_node == message->coordinator_node
			&& current.round_nonce == message->round_nonce
			&& current.expected_members_lo == message->admitted_members_lo
			&& current.expected_members_hi == message->admitted_members_hi
			&& current.transition_epoch == message->transition_epoch
			&& current.record_generation == message->record_generation
			&& current.source_feature_bitmap
			   == message->source_feature_bitmap
			&& current.target_feature_bitmap
			   == message->target_feature_bitmap
			&& current.rollback_feature_bitmap
			   == message->rollback_feature_bitmap
			&& current.capability_sample_digest
			   == message->capability_sample_digest
			&& (current.observed_members_lo
				& ~current.expected_members_lo) == 0
			&& (current.observed_members_hi
				& ~current.expected_members_hi) == 0
			&& (current.flags
				& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) == 0
			&& (local_node_id < 64
					? (current.observed_members_lo & self_bit) != 0
					: (current.observed_members_hi & self_bit) != 0)
			&& semantic_activation_ack_matches(
				&current.observed[local_node_id], &self);

		if (exact)
			return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
		return semantic_activation_ack_image_invalidate(&current)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	memset(&next, 0, sizeof(next));
	next.stage = message->stage;
	next.coordinator_node = message->coordinator_node;
	next.round_nonce = message->round_nonce;
	next.expected_members_lo = current_members_lo;
	next.expected_members_hi = current_members_hi;
	if (local_node_id < 64)
		next.observed_members_lo = UINT64_C(1) << local_node_id;
	else
		next.observed_members_hi
			= UINT64_C(1) << (local_node_id - 64);
	next.transition_epoch = message->transition_epoch;
	next.record_generation = message->record_generation;
	next.source_feature_bitmap = message->source_feature_bitmap;
	next.target_feature_bitmap = message->target_feature_bitmap;
	next.rollback_feature_bitmap = message->rollback_feature_bitmap;
	next.capability_sample_digest = message->capability_sample_digest;
	next.observed[local_node_id] = self;

	if (memcmp((const uint8 *)&current + payload_offset,
			   (const uint8 *)&next + payload_offset,
			   sizeof(current) - payload_offset) == 0)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_apply_item(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	uint64 member_bit;
	bool member_was_observed;
	int32 source_node;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&image))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	if ((image.expected_members_lo == 0
		 && image.expected_members_hi == 0)
		|| image.stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image.stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image.round_nonce == 0)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	if (image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| image.coordinator_node != (uint32)current_coordinator_node) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	message = &item->message;
	if (message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->stage != image.stage
		|| message->coordinator_node != image.coordinator_node
		|| message->round_nonce != image.round_nonce
		|| message->admitted_members_lo != image.expected_members_lo
		|| message->admitted_members_hi != image.expected_members_hi
		|| message->transition_epoch != image.transition_epoch
		|| message->record_generation != image.record_generation
		|| message->source_feature_bitmap != image.source_feature_bitmap
		|| message->target_feature_bitmap != image.target_feature_bitmap
		|| message->rollback_feature_bitmap != image.rollback_feature_bitmap
		|| message->capability_sample_digest
		   != image.capability_sample_digest)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED) {
		uint32 required_caps;
		bool exact_initial_r4_refusal
			= semantic_activation_ack_wire_value_valid(message)
			&& image.stage
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& image.source_feature_bitmap == 0
			&& image.target_feature_bitmap
			   == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			&& image.rollback_feature_bitmap == 0
			&& image.expected_members_lo == UINT64_C(0x0f)
			&& image.expected_members_hi == 0
			&& message->reason
			   == CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED
			&& item->local_receiver_node_id == current_coordinator_node
			&& item->authenticated_source_node_id
			   == (int32)message->member_node
			&& item->authenticated_source_node_id > 0
			&& item->authenticated_source_node_id < 4
			&& cluster_membership_get_state(
				item->authenticated_source_node_id)
			   == CLUSTER_MEMBER_MEMBER
			&& item->sampled_capability_generation != 0
			&& semantic_activation_ack_round_required_caps(
				message->source_feature_bitmap,
				message->target_feature_bitmap,
				message->rollback_feature_bitmap, &required_caps)
			&& (item->sampled_capability_word & required_caps)
			   == required_caps
			&& cluster_sf_peer_capability_generation_matches(
				item->authenticated_source_node_id, required_caps,
				item->sampled_capability_generation);

		if (!semantic_activation_ack_image_invalidate(&image))
			return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
		if (exact_initial_r4_refusal)
			(void)semantic_activation_utility_mailbox_complete(
				message->round_nonce,
				CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED,
				CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				message->record_generation - 1);
		return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
	}
	if (message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| !semantic_activation_ack_remote_tuple(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, &tuple)) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	source_node = item->authenticated_source_node_id;
	if (!semantic_activation_ack_member_present(
			image.expected_members_lo, image.expected_members_hi, source_node)) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}
	member_bit = UINT64_C(1) << (source_node < 64 ? source_node
											: source_node - 64);
	member_was_observed
		= source_node < 64
			  ? (image.observed_members_lo & member_bit) != 0
			  : (image.observed_members_hi & member_bit) != 0;
	if (member_was_observed) {
		if (semantic_activation_ack_matches(
				&image.observed[source_node], &tuple))
			return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}
	if (image.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		&& ((image.flags
			  & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
			|| !semantic_activation_ack_matches(
				&image.expected[source_node], &tuple))) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	image.observed[source_node] = tuple;
	if (source_node < 64)
		image.observed_members_lo |= member_bit;
	else
		image.observed_members_hi |= member_bit;
	if (image.observed_members_lo == image.expected_members_lo
		&& image.observed_members_hi == image.expected_members_hi) {
		if (image.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE) {
			if (!semantic_activation_ack_image_structural(&image)) {
				return semantic_activation_ack_image_invalidate(&image)
						   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
						   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
			}
			memcpy(image.expected, image.observed, sizeof(image.expected));
			image.flags
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
		} else if (semantic_activation_full_ack_table_matches(
					   image.observed, image.observed_members_lo,
					   image.observed_members_hi, image.expected,
					   image.expected_members_lo,
					   image.expected_members_hi))
			image.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
		else {
			return semantic_activation_ack_image_invalidate(&image)
					   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
					   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
		}
	} else
		image.flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;

	return semantic_activation_ack_table_publish(&image)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static bool
semantic_activation_ack_stage_ahead_identity_matches(
	const SemanticActivationAckIngressItem *left,
	const SemanticActivationAckIngressItem *right)
{
	const ClusterSemanticActivationAckWireV1 *a;
	const ClusterSemanticActivationAckWireV1 *b;

	if (left == NULL || right == NULL)
		return false;
	a = &left->message;
	b = &right->message;
	return left->local_receiver_node_id == right->local_receiver_node_id
		   && a->stage == b->stage
		   && a->coordinator_node == b->coordinator_node
		   && a->transition_epoch == b->transition_epoch
		   && a->record_generation == b->record_generation
		   && a->round_nonce == b->round_nonce
		   && a->source_feature_bitmap == b->source_feature_bitmap
		   && a->target_feature_bitmap == b->target_feature_bitmap
		   && a->rollback_feature_bitmap == b->rollback_feature_bitmap
		   && a->admitted_members_lo == b->admitted_members_lo
		   && a->admitted_members_hi == b->admitted_members_hi
		   && a->capability_sample_digest
			  == b->capability_sample_digest;
}

static bool
semantic_activation_ack_stage_ahead_refusal_exact(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	const ClusterSemanticActivationAckWireV1 *message;
	const SemanticActivationAckTuple *expected;
	uint32 required_caps;
	uint64 expected_generation;
	int32 source_node;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&image))
		return false;
	message = &item->message;
	source_node = item->authenticated_source_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED
		|| source_node < 0 || source_node >= CLUSTER_MAX_NODES
		|| source_node == item->local_receiver_node_id
		|| item->local_receiver_node_id != current_coordinator_node
		|| message->member_node != (uint32)source_node
		|| image.stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image.stage >= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| message->stage != image.stage + 1
		|| image.coordinator_node != message->coordinator_node
		|| image.coordinator_node != (uint32)current_coordinator_node
		|| image.round_nonce != message->round_nonce
		|| image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| message->transition_epoch != current_epoch
		|| image.source_feature_bitmap != message->source_feature_bitmap
		|| image.target_feature_bitmap != message->target_feature_bitmap
		|| image.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| image.capability_sample_digest
		   != message->capability_sample_digest
		|| (image.flags
			& (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE))
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, source_node)
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps)
		return false;

	expected_generation
		= image.stage <= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
			  ? image.record_generation
			  : image.record_generation + 1;
	if (image.record_generation == UINT64_MAX
		|| message->record_generation != expected_generation)
		return false;
	expected = &image.expected[source_node];
	return expected->node_id == (uint32)source_node
		&& expected->admitted_incarnation != 0
		&& expected->control_connection_generation
		   == item->sampled_capability_generation
		&& expected->capability_word == item->sampled_capability_word
		&& expected->capability_generation
		   == item->sampled_capability_generation
		&& expected->transition_epoch == current_epoch
		&& cluster_membership_get_state(source_node)
		   == CLUSTER_MEMBER_MEMBER
		&& cluster_membership_get_last_admitted_incarnation(source_node)
		   == expected->admitted_incarnation
		&& cluster_sf_peer_capability_generation_matches(
			source_node, required_caps,
			item->sampled_capability_generation);
}

/*
 * A positive SAMPLE ACK may cross the coordinator's directed SAMPLE
 * REQUEST because the ACK is fanned out by a different peer connection.
 * Retain it only while every field can already be checked against the current
 * formation, admission snapshot, authenticated source, and either the exact
 * initial EMPTY basis or the exact complete OPEN predecessor.  The matching
 * REQUEST remains the sole authority that installs the round; this helper
 * never publishes a table or an ACK.
 */
static bool
semantic_activation_ack_before_sample_request_candidate(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	const Size payload_offset
		= offsetof(ClusterSemanticActivationAckTableV1, stage);
	uint32 local_capability_word;
	uint32 required_caps;
	bool complete_open_predecessor;
	bool exact_basis;
	int32 local_node_id;
	int32 source_node;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&image)
		|| !semantic_activation_snapshot(&snapshot))
		return false;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	source_node = item->authenticated_source_node_id;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| message->reason != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| local_node_id != cluster_node_id
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| source_node < 0 || source_node >= CLUSTER_MAX_NODES
		|| source_node == local_node_id
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)source_node
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_coordinator_node != 0
		|| message->transition_epoch != current_epoch
		|| message->rollback_feature_bitmap != 0
		|| message->capability_sample_digest != 0
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, source_node)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER
		|| (local_capability_word & required_caps) != required_caps
		|| (snapshot.seq & UINT64_C(1)) != 0
		|| snapshot.formation_epoch != current_epoch
		|| snapshot.record_generation == UINT64_MAX
		|| !semantic_activation_ack_remote_tuple(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, &tuple))
		return false;

	exact_basis
		= !snapshot.transition_closed
		  && snapshot.record_generation == 0
		  && snapshot.active_bits == 0
		  && message->source_feature_bitmap == 0
		  && message->record_generation == 1
		  && message->target_feature_bitmap
			 == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  && semantic_activation_r4_initial_clean_sample_basis(0, false)
		  && semantic_activation_bytes_are_zero(
			  (const uint8 *)&image + payload_offset,
			  sizeof(image) - payload_offset);
	if (!exact_basis) {
		complete_open_predecessor
			= image.stage
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			  && image.flags
				 == (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
			  && image.record_generation != UINT64_MAX
			  && message->record_generation
				 == image.record_generation + 1
			  && image.target_feature_bitmap
				 == message->source_feature_bitmap
			  && image.rollback_feature_bitmap == 0
			  && image.capability_sample_digest != 0
			  && semantic_activation_ack_complete_image_current(
				  &image, current_members_lo, current_members_hi,
				  current_epoch, current_coordinator_node, local_node_id,
				  local_capability_word);
		exact_basis
			= complete_open_predecessor
			  && ((!snapshot.transition_closed
				   && snapshot.record_generation
					  == image.record_generation
				   && snapshot.active_bits
					  == image.target_feature_bitmap)
				  || (snapshot.transition_closed
					  && snapshot.record_generation + 1
						 == image.record_generation
					  && snapshot.active_bits
						 == image.source_feature_bitmap));
	}
	if (!exact_basis)
		return false;
	return semantic_activation_ack_tuple_structural(
		&tuple, source_node, current_epoch, message->record_generation,
		required_caps);
}

/* A completed OPEN_APPLIED table may precede this member's local gate
 * publication by one LMON tick.  The next SAMPLE REQUEST remains a bounded,
 * non-authoritative request-ahead item until that exact post-OPEN state is
 * visible. */
static SemanticActivationAckRequestAheadState
semantic_activation_ack_sample_request_ahead_state(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	int32 local_node_id;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| message->source_feature_bitmap
		   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| message->target_feature_bitmap
		   != (CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			   | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1)
		|| message->rollback_feature_bitmap != 0
		|| message->capability_sample_digest != 0
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_coordinator_node != 0
		|| local_node_id <= 0 || local_node_id >= 4
		|| item->authenticated_source_node_id != current_coordinator_node
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| current.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| current.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| current.coordinator_node != message->coordinator_node
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.observed_members_lo != current.expected_members_lo
		|| current.observed_members_hi != current.expected_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation == UINT64_MAX
		|| message->record_generation != current.record_generation + 1
		|| current.target_feature_bitmap
		   != message->source_feature_bitmap
		|| current.rollback_feature_bitmap != 0
		|| current.capability_sample_digest == 0
		|| !semantic_activation_ack_complete_image_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;

	if (!snapshot->transition_closed
		&& snapshot->record_generation == current.record_generation
		&& snapshot->active_bits == current.target_feature_bitmap)
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY;
	if (snapshot->transition_closed
		&& snapshot->record_generation + 1 == current.record_generation
		&& snapshot->active_bits == current.source_feature_bitmap)
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT;
	return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
}

static bool
semantic_activation_ack_expected_predecessor_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id, uint32 local_capability_word)
{
	bool complete;
	int node;

	if (image == NULL
		|| image->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		|| image->stage >= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image->flags
		   & ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| (image->flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| (image->observed_members_lo & ~image->expected_members_lo) != 0
		|| (image->observed_members_hi & ~image->expected_members_hi) != 0
		|| !semantic_activation_ack_expected_image_current(
			image, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, local_node_id,
			local_capability_word))
		return false;

	complete
		= image->observed_members_lo == image->expected_members_lo
		  && image->observed_members_hi == image->expected_members_hi;
	if (complete
		? image->flags
			  != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		: image->flags != CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID)
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = semantic_activation_ack_member_present(
			image->expected_members_lo, image->expected_members_hi, node);
		bool observed = semantic_activation_ack_member_present(
			image->observed_members_lo, image->observed_members_hi, node);

		if (!member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&image->expected[node],
					sizeof(image->expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&image->observed[node],
					sizeof(image->observed[node])))
				return false;
			continue;
		}
		if (observed) {
			if (!semantic_activation_ack_matches(
					&image->observed[node], &image->expected[node]))
				return false;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&image->observed[node],
				   sizeof(image->observed[node])))
			return false;
	}
	return !complete
		   || semantic_activation_ack_complete_image_current(
			   image, current_members_lo, current_members_hi,
			   current_epoch, current_coordinator_node, local_node_id,
			   local_capability_word);
}

static bool
semantic_activation_ack_stage_ahead_candidate(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAckTuple expected;
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 local_capability_word;
	uint64 expected_generation;
	int32 source_node;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&image))
		return false;
	message = &item->message;
	source_node = item->authenticated_source_node_id;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| message->reason != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| item->local_receiver_node_id != cluster_node_id
		|| source_node < 0 || source_node >= CLUSTER_MAX_NODES
		|| message->member_node != (uint32)source_node
		|| image.stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image.stage >= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| message->stage != image.stage + 1
		|| image.coordinator_node != message->coordinator_node
		|| image.coordinator_node != (uint32)current_coordinator_node
		|| image.round_nonce != message->round_nonce
		|| image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| message->transition_epoch != current_epoch
		|| image.source_feature_bitmap != message->source_feature_bitmap
		|| image.target_feature_bitmap != message->target_feature_bitmap
		|| image.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| (image.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			? message->capability_sample_digest == 0
			: image.capability_sample_digest
			  != message->capability_sample_digest)
		|| !(image.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			 ? semantic_activation_ack_complete_image_current(
				   &image, current_members_lo, current_members_hi,
				   current_epoch, current_coordinator_node,
				   cluster_node_id, local_capability_word)
			 : semantic_activation_ack_expected_predecessor_current(
				   &image, current_members_lo, current_members_hi,
				   current_epoch, current_coordinator_node,
				   cluster_node_id, local_capability_word))
		|| !semantic_activation_ack_remote_tuple(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, &tuple))
		return false;

	expected_generation
		= image.stage <= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
			  ? image.record_generation
			  : image.record_generation + 1;
	if (image.record_generation == UINT64_MAX
		|| message->record_generation != expected_generation)
		return false;
	expected = image.expected[source_node];
	expected.record_generation = expected_generation;
	return semantic_activation_ack_matches(&tuple, &expected);
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_retain_stage_ahead(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	SemanticActivationAckStageAhead *retained
		= &semantic_activation_ack_local_stage_ahead;
	uint32 index;

	if (!semantic_activation_ack_stage_ahead_candidate(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node)
		&& !semantic_activation_ack_before_sample_request_candidate(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node)) {
		if (semantic_activation_ack_stage_ahead_refusal_exact(
				item, current_members_lo, current_members_hi, current_epoch,
				current_coordinator_node)) {
			semantic_activation_ack_lmon_invalidate_active();
			return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
		}
		if (retained->count != 0 && item != NULL
			&& item->message.kind
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
			&& item->message.result
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
			&& item->local_receiver_node_id
			   == retained->items[0].local_receiver_node_id
			&& item->message.stage == retained->items[0].message.stage) {
			semantic_activation_ack_lmon_invalidate_active();
			return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
		}
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	}

	if (retained->count != 0
		&& !semantic_activation_ack_stage_ahead_identity_matches(
			item, &retained->items[0])) {
		semantic_activation_ack_lmon_invalidate_active();
		return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
	}
	for (index = 0; index < retained->count; index++) {
		if (retained->items[index].authenticated_source_node_id
			!= item->authenticated_source_node_id)
			continue;
		if (memcmp(&retained->items[index], item, sizeof(*item)) == 0)
			return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
		semantic_activation_ack_lmon_invalidate_active();
		return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
	}
	if (retained->count >= SEMANTIC_ACTIVATION_ACK_STAGE_AHEAD_CAPACITY) {
		semantic_activation_ack_lmon_invalidate_active();
		return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
	}
	retained->items[retained->count++] = *item;
	return SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED;
}

static bool
semantic_activation_ack_lmon_apply_stage_ahead(
	const SemanticActivationAckIngressItem *request,
	SemanticActivationAckConsumeResult request_result,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	SemanticActivationAckStageAhead retained;
	uint32 index;

	if (semantic_activation_ack_local_stage_ahead.count == 0
		|| request == NULL
		|| request->message.stage
		   != semantic_activation_ack_local_stage_ahead.items[0].message.stage)
		return true;
	if (request_result != SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
		|| !semantic_activation_ack_stage_ahead_identity_matches(
			request, &semantic_activation_ack_local_stage_ahead.items[0])) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}

	retained = semantic_activation_ack_local_stage_ahead;
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
	for (index = 0; index < retained.count; index++) {
		SemanticActivationAckConsumeResult result
			= semantic_activation_ack_lmon_apply_item(
				&retained.items[index], current_members_lo,
				current_members_hi, current_epoch,
				current_coordinator_node);

		if (result != SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			&& result != SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
	}
	return true;
}

static SemanticActivationAckRequestAheadState
semantic_activation_ack_barrier_request_ahead_state(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	uint64 digest = 0;
	int32 local_node_id;
	int node;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| message->source_feature_bitmap
		   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| message->target_feature_bitmap
		   != (CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			   | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1)
		|| message->rollback_feature_bitmap != 0
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32) current_coordinator_node
		|| message->member_node != (uint32) local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| message->record_generation
		   != snapshot->record_generation + 1
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != message->record_generation
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest != 0
		|| (current.observed_members_lo & ~current.expected_members_lo) != 0
		|| (current.observed_members_hi & ~current.expected_members_hi) != 0)
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;

	if (current.flags == 0
		&& (current.observed_members_lo != current.expected_members_lo
			|| current.observed_members_hi != current.expected_members_hi)) {
		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			bool member = semantic_activation_ack_member_present(
				current.expected_members_lo, current.expected_members_hi,
				node);
			bool observed = semantic_activation_ack_member_present(
				current.observed_members_lo, current.observed_members_hi,
				node);

			if (!semantic_activation_bytes_are_zero(
					(const uint8 *) &current.expected[node],
					sizeof(current.expected[node])))
				return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
			if (!member || !observed) {
				if (!semantic_activation_bytes_are_zero(
						(const uint8 *) &current.observed[node],
						sizeof(current.observed[node])))
					return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
				continue;
			}
			if (!semantic_activation_ack_tuple_structural(
					&current.observed[node], node, current_epoch,
					current.record_generation, required_caps))
				return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
		}
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT;
	}

	if (current.flags
		!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| current.observed_members_lo != current.expected_members_lo
		|| current.observed_members_hi != current.expected_members_hi
		|| !semantic_activation_ack_complete_image_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word)
		|| !semantic_activation_ack_sample_digest(&current, &digest)
		|| digest != message->capability_sample_digest)
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY;
}

static SemanticActivationAckRequestAheadState
semantic_activation_ack_bit22_later_request_ahead_state(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	uint32 predecessor_stage;
	uint64 predecessor_generation;
	uint64 request_generation;
	uint64 self_bit;
	int32 local_node_id;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (message->stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED) {
		predecessor_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
		if (snapshot->transition_closed) {
			predecessor_generation = snapshot->record_generation;
			request_generation = snapshot->record_generation;
		} else {
			if (snapshot->record_generation == UINT64_MAX)
				return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
			predecessor_generation = snapshot->record_generation + 1;
			request_generation = snapshot->record_generation + 1;
		}
	} else if (message->stage
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		if (snapshot->record_generation == UINT64_MAX)
			return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
		predecessor_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		predecessor_generation = snapshot->record_generation;
		request_generation = snapshot->record_generation + 1;
	} else if (message->stage
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
		if (snapshot->record_generation > UINT64_MAX - 2)
			return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
		predecessor_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
		predecessor_generation = snapshot->record_generation + 1;
		request_generation = snapshot->record_generation + 2;
	} else
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;

	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| (message->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| message->source_feature_bitmap != 0
		|| message->rollback_feature_bitmap != 0
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps)
		   != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.stage != predecessor_stage
		|| current.flags
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != predecessor_generation
		|| message->record_generation != request_generation
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest == 0
		|| current.capability_sample_digest
		   != message->capability_sample_digest
		|| !semantic_activation_ack_expected_predecessor_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;

	/* The local bit22 predecessor has exactly two legal evidence shapes:
	 * no observation while its callback is pending, or the exact self row
	 * after that callback.  Other peer rows are coordinator-local and cannot
	 * be used to make this member ready. */
	self_bit = UINT64_C(1) << local_node_id;
	if (current.observed_members_lo == 0
		&& current.observed_members_hi == 0) {
		if (message->stage
			== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
			&& !snapshot->transition_closed) {
			if (snapshot->record_generation == UINT64_MAX
				|| snapshot->record_generation + 1
				   != message->record_generation)
				return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
		} else if (!snapshot->transition_closed)
			return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT;
	}
	if (!snapshot->transition_closed
		|| current.observed_members_lo != self_bit
		|| current.observed_members_hi != 0
		|| !semantic_activation_ack_matches(
			&current.observed[local_node_id],
			&current.expected[local_node_id]))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY;
}

static SemanticActivationAckRequestAheadState
semantic_activation_ack_later_request_ahead_state(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	uint64 expected_generation;
	int32 local_node_id;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if ((message->target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
		return semantic_activation_ack_bit22_later_request_ahead_state(
			item, snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| message->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_coordinator_node != 0
		|| local_node_id <= 0 || local_node_id >= 4
		|| item->authenticated_source_node_id != current_coordinator_node
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.stage + 1 != message->stage
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest == 0
		|| current.capability_sample_digest
		   != message->capability_sample_digest
		|| !semantic_activation_ack_expected_predecessor_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;

	expected_generation
		= current.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
			  ? current.record_generation
			  : current.record_generation + 1;
	if (current.record_generation == UINT64_MAX
		|| message->record_generation != expected_generation
		|| (message->stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
			? (snapshot->transition_closed
				   ? snapshot->record_generation
					 != message->record_generation
				   : snapshot->record_generation == UINT64_MAX
					 || snapshot->record_generation + 1
						!= message->record_generation)
			: !snapshot->transition_closed
			  || snapshot->record_generation + 1
				 != message->record_generation))
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_INVALID;
	if (!snapshot->transition_closed)
		return SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT;

	return current.observed_members_lo == current.expected_members_lo
			   && current.observed_members_hi == current.expected_members_hi
			 ? SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY
			 : SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT;
}

static SemanticActivationAckRequestAheadState
semantic_activation_ack_request_ahead_state(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	if (item != NULL
		&& item->message.stage
		   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE)
		return semantic_activation_ack_sample_request_ahead_state(
			item, snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	if (item != NULL
		&& item->message.stage
		   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER)
		return semantic_activation_ack_barrier_request_ahead_state(
			item, snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	return semantic_activation_ack_later_request_ahead_state(
		item, snapshot, current_members_lo, current_members_hi,
		current_epoch, current_coordinator_node, local_capability_word);
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_retain_request_ahead(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	SemanticActivationAckRequestAhead *retained
		= &semantic_activation_ack_local_request_ahead;
	SemanticActivationAckRequestAheadState state;

	state = semantic_activation_ack_request_ahead_state(
		item, snapshot, current_members_lo, current_members_hi,
		current_epoch, current_coordinator_node, local_capability_word);
	if (state != SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT) {
		if (retained->valid && item != NULL
			&& item->message.kind
			   == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
			&& item->message.stage == retained->item.message.stage
			&& item->local_receiver_node_id
			   == retained->item.local_receiver_node_id) {
			semantic_activation_ack_lmon_invalidate_active();
			return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
		}
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	}
	if (!retained->valid) {
		retained->valid = true;
		retained->item = *item;
		return SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED;
	}
	if (memcmp(&retained->item, item, sizeof(*item)) == 0)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
	semantic_activation_ack_lmon_invalidate_active();
	return SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED;
}

static bool
semantic_activation_ack_lmon_consume_request_ahead(
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAckIngressItem request;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAckRequestAheadState state;
	SemanticActivationAckConsumeResult result;
	uint32 local_capability_word;

	if (!semantic_activation_ack_local_request_ahead.valid)
		return true;
	if (!semantic_activation_snapshot(&snapshot)) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}
	local_capability_word = cluster_ic_local_capability_word();
	request = semantic_activation_ack_local_request_ahead.item;
	state = semantic_activation_ack_request_ahead_state(
		&request, &snapshot, current_members_lo, current_members_hi,
		current_epoch, current_coordinator_node, local_capability_word);
	if (state == SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_WAIT)
		return true;
	if (state != SEMANTIC_ACTIVATION_ACK_REQUEST_AHEAD_READY) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}
	if (semantic_activation_ack_local_pending_send.invalidated) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}
	if (semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0)
		return true;

	if (request.message.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE)
		result = semantic_activation_ack_lmon_accept_current_sample_request(
			&request, &snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	else if (request.message.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER)
		result = semantic_activation_ack_lmon_accept_current_barrier_request(
			&request, &snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	else if ((request.message.target_feature_bitmap
			  & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
		result = semantic_activation_ack_lmon_accept_current_request_bit22(
			&request, &snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	else if (request.message.stage
			 == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED)
		result = semantic_activation_ack_lmon_accept_current_prepared_request(
			&request, &snapshot, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	else
		result
			= semantic_activation_ack_lmon_accept_current_commit_applied_request(
				&request, &snapshot, current_members_lo,
				current_members_hi, current_epoch,
				current_coordinator_node, local_capability_word);
	if (result != SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
		&& result != SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
	if (!semantic_activation_ack_lmon_apply_stage_ahead(
			&request, result, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node))
		return false;
	if (request.message.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		&& (result != SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			|| !semantic_activation_ack_table_snapshot(&image)
			|| !semantic_activation_ack_pending_send_begin_positive(
				&semantic_activation_ack_local_pending_send,
				&request.message, cluster_node_id,
				&image.observed[cluster_node_id]))) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}

	return true;
}

static void
semantic_activation_ack_lmon_invalidate_active(void)
{
	ClusterSemanticActivationAckTableV1 image;

	semantic_activation_lmon_member_pgrd_read_request_seq = 0;
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
	memset(&semantic_activation_ack_local_request_origin, 0,
		   sizeof(semantic_activation_ack_local_request_origin));
	if (!semantic_activation_ack_table_snapshot(&image)
		|| (image.expected_members_lo == 0
			&& image.expected_members_hi == 0))
		return;
	(void)semantic_activation_ack_image_invalidate(&image);
}

static void
semantic_activation_ack_lmon_send_pending(void)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAckPendingSend *pending
		= &semantic_activation_ack_local_pending_send;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint32 required_caps;
	bool refused;
	bool invalid;
	int32 node;

	if (pending->invalidated
		|| (pending->pending_members_lo == 0
			&& pending->pending_members_hi == 0))
		return;
	refused = pending->message.result
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	invalid = !semantic_activation_ack_round_required_caps(
		pending->message.source_feature_bitmap,
		pending->message.target_feature_bitmap,
		pending->message.rollback_feature_bitmap, &required_caps)
		|| pending->message.kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| !cluster_semantic_activation_ack_wire_encode(
			&pending->message, payload);
	if (!invalid && refused) {
		uint64 coordinator_bit
			= UINT64_C(1) << pending->message.coordinator_node;

		invalid = pending->message.stage
				  != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			|| pending->message.reason
			   != CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED
			|| pending->message.coordinator_node != UINT32_C(0)
			|| pending->message.member_node != (uint32)cluster_node_id
			|| pending->message.admitted_members_lo != UINT64_C(0x0f)
			|| pending->message.admitted_members_hi != 0
			|| pending->message.source_feature_bitmap != 0
			|| pending->message.target_feature_bitmap
			   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			|| pending->message.rollback_feature_bitmap != 0
			|| pending->message.capability_sample_digest != 0
			|| pending->pending_members_lo != coordinator_bit
			|| pending->pending_members_hi != 0
			|| pending->refusal_connection_generation == 0
			|| !cluster_qvotec_in_quorum()
			|| pending->message.transition_epoch
			   != cluster_epoch_get_current()
			|| (cluster_ic_local_capability_word() & required_caps)
			   != required_caps
			|| !semantic_activation_snapshot(&snapshot)
			|| snapshot.transition_closed
			|| snapshot.formation_epoch
			   != pending->message.transition_epoch
			|| snapshot.active_bits
			   != pending->message.source_feature_bitmap
			|| snapshot.record_generation == UINT64_MAX
			|| snapshot.record_generation + 1
			   != pending->message.record_generation
			|| !cluster_sf_peer_capability_generation_matches(
				(int32)pending->message.coordinator_node, required_caps,
				pending->refusal_connection_generation);
	} else if (!invalid) {
		invalid = pending->message.result
				  != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
			|| !semantic_activation_ack_table_snapshot(&image)
			|| (image.expected_members_lo == 0
				&& image.expected_members_hi == 0)
			|| pending->message.stage != image.stage
			|| pending->message.coordinator_node != image.coordinator_node
			|| pending->message.round_nonce != image.round_nonce
			|| pending->message.admitted_members_lo
			   != image.expected_members_lo
			|| pending->message.admitted_members_hi
			   != image.expected_members_hi
			|| pending->message.transition_epoch != image.transition_epoch
			|| pending->message.record_generation != image.record_generation
			|| pending->message.source_feature_bitmap
			   != image.source_feature_bitmap
			|| pending->message.target_feature_bitmap
			   != image.target_feature_bitmap
			|| pending->message.rollback_feature_bitmap
			   != image.rollback_feature_bitmap
			|| pending->message.capability_sample_digest
			   != image.capability_sample_digest;
	}
	if (invalid) {
		pending->pending_members_lo = 0;
		pending->pending_members_hi = 0;
		pending->invalidated = true;
		semantic_activation_ack_lmon_invalidate_active();
		return;
	}

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		SemanticActivationAckSendResult disposition;
		ClusterICSendResult send_result;
		uint32 capability_word;
		uint32 capability_generation;
		bool is_pending
			= node < 64
				  ? (pending->pending_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : (pending->pending_members_hi
					 & (UINT64_C(1) << (node - 64))) != 0;

		if (!is_pending)
			continue;
		if (!cluster_sf_peer_capability_word_sample(
				node, required_caps,
				&capability_word, &capability_generation)
			|| capability_generation == 0
			|| (refused
				&& capability_generation
				   != pending->refusal_connection_generation)) {
			pending->pending_members_lo = 0;
			pending->pending_members_hi = 0;
			pending->invalidated = true;
			semantic_activation_ack_lmon_invalidate_active();
			return;
		}
		send_result = cluster_ic_send_envelope(
			PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1, node,
			payload, sizeof(payload));
		disposition = semantic_activation_ack_pending_send_note_result(
			pending, node, send_result);
		if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED) {
			cluster_ic_tier1_close_peer(
				node, "semantic activation ACK send failed");
			semantic_activation_ack_lmon_invalidate_active();
			return;
		}
		if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_REJECTED) {
			pending->pending_members_lo = 0;
			pending->pending_members_hi = 0;
			pending->invalidated = true;
			semantic_activation_ack_lmon_invalidate_active();
			return;
		}
	}
}

/* A coordinator-owned stage table is local state until the coordinator's
 * exact observation is disseminated as a normal authenticated ACK.  Send
 * directed REQUESTs first so every remote LMON has installed the stage
 * table before this ACK can arrive.  The process-local origin bit prevents
 * a completed ACK fanout from being restaged on every tick; retained sends
 * continue through the existing pending-send retry path. */
static bool
semantic_activation_ack_lmon_send_coordinator_observation(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckRequestOrigin *origin)
{
	SemanticActivationAckPendingSend pending;
	ClusterSemanticActivationAckWireV1 request;
	SemanticActivationAckTuple current_self;
	const SemanticActivationAckTuple *self;
	uint64 self_bit;
	uint32 local_capability_word;
	int32 coordinator_node;

	if (image == NULL || origin == NULL || !origin->active)
		return false;
	coordinator_node = (int32)image->coordinator_node;
	if (coordinator_node != cluster_node_id
		|| coordinator_node < 0 || coordinator_node >= 64)
		return false;
	self_bit = UINT64_C(1) << coordinator_node;
	if ((image->observed_members_lo & self_bit) == 0)
		return true;
	self = &image->observed[coordinator_node];
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_self_tuple(
			coordinator_node, local_capability_word,
			image->transition_epoch, image->record_generation,
			&current_self)
		|| !semantic_activation_ack_matches(self, &current_self)) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}

	if (!origin->coordinator_ack_started) {
		if (semantic_activation_ack_local_pending_send.invalidated
			|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
			|| semantic_activation_ack_local_pending_send.pending_members_hi != 0) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
		memset(&request, 0, sizeof(request));
		request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
		request.stage = image->stage;
		request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
		request.coordinator_node = image->coordinator_node;
		request.member_node = image->coordinator_node;
		request.transition_epoch = image->transition_epoch;
		request.record_generation = image->record_generation;
		request.round_nonce = image->round_nonce;
		request.source_feature_bitmap = image->source_feature_bitmap;
		request.target_feature_bitmap = image->target_feature_bitmap;
		request.rollback_feature_bitmap = image->rollback_feature_bitmap;
		request.admitted_members_lo = image->expected_members_lo;
		request.admitted_members_hi = image->expected_members_hi;
		request.capability_sample_digest
			= image->capability_sample_digest;
		memset(&pending, 0, sizeof(pending));
		if (!semantic_activation_ack_pending_send_begin_positive(
				&pending, &request, coordinator_node, self)) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
		semantic_activation_ack_local_pending_send = pending;
		origin->coordinator_ack_started = true;
	}

	semantic_activation_ack_lmon_send_pending();
	if (semantic_activation_ack_local_pending_send.invalidated)
		return false;
	return true;
}

static bool
semantic_activation_ack_lmon_send_origin_requests(void)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 image;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint32 required_caps;

	if (!origin->active)
		return false;
	if (!semantic_activation_ack_table_snapshot(&image)
		|| !semantic_activation_ack_round_required_caps(
			image.source_feature_bitmap, image.target_feature_bitmap,
			image.rollback_feature_bitmap, &required_caps)
		|| (image.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& image.stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
			&& image.stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
			&& image.stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
			&& image.stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		|| image.coordinator_node != UINT32_C(0)
		|| image.expected_members_lo != UINT64_C(0x0f)
		|| image.expected_members_hi != 0
		|| image.round_nonce == 0
		|| (image.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& image.capability_sample_digest != 0)
		|| (image.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& (image.capability_sample_digest == 0
				|| (image.flags
					& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID)
				   == 0))) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}

	for (;;) {
		SemanticActivationAckPendingSend *pending = &origin->current;
		SemanticActivationAckSendResult disposition;
		ClusterICSendResult send_result;
		uint32 capability_word;
		uint32 capability_generation;
		uint64 member_bit;
		int32 node;

		if (pending->pending_members_lo == 0
			&& pending->pending_members_hi == 0) {
			for (node = 1; node < 4; node++) {
				member_bit = UINT64_C(1) << node;
				if ((origin->unsent_members_lo & member_bit) != 0)
					break;
			}
			if (node == 4)
				return semantic_activation_ack_lmon_send_coordinator_observation(
					&image, origin);
			memset(pending, 0, sizeof(*pending));
			pending->message.kind
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
			pending->message.stage = image.stage;
			pending->message.result
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
			pending->message.coordinator_node = image.coordinator_node;
			pending->message.member_node = (uint32)node;
			pending->message.transition_epoch = image.transition_epoch;
			pending->message.record_generation = image.record_generation;
			pending->message.round_nonce = image.round_nonce;
			pending->message.source_feature_bitmap
				= image.source_feature_bitmap;
			pending->message.target_feature_bitmap
				= image.target_feature_bitmap;
			pending->message.rollback_feature_bitmap
				= image.rollback_feature_bitmap;
			pending->message.admitted_members_lo
				= image.expected_members_lo;
			pending->message.admitted_members_hi
				= image.expected_members_hi;
			pending->message.capability_sample_digest
				= image.capability_sample_digest;
			pending->pending_members_lo = member_bit;
			origin->unsent_members_lo &= ~member_bit;
		}

		node = (int32)pending->message.member_node;
		if (node <= 0 || node >= 4
			|| pending->invalidated) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
		member_bit = UINT64_C(1) << node;
		if (pending->pending_members_hi != 0
			|| pending->pending_members_lo != member_bit
			|| pending->message.kind
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
			|| pending->message.stage != image.stage
			|| pending->message.result
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
			|| pending->message.coordinator_node != image.coordinator_node
			|| pending->message.transition_epoch != image.transition_epoch
			|| pending->message.record_generation != image.record_generation
			|| pending->message.round_nonce != image.round_nonce
			|| pending->message.source_feature_bitmap
			   != image.source_feature_bitmap
			|| pending->message.target_feature_bitmap
			   != image.target_feature_bitmap
			|| pending->message.rollback_feature_bitmap
			   != image.rollback_feature_bitmap
			|| pending->message.admitted_members_lo
			   != image.expected_members_lo
			|| pending->message.admitted_members_hi
			   != image.expected_members_hi
			|| pending->message.capability_sample_digest
			   != image.capability_sample_digest
			|| !cluster_semantic_activation_ack_wire_encode(
				&pending->message, payload)
			|| !cluster_sf_peer_capability_word_sample(
				node, required_caps,
				&capability_word, &capability_generation)
			|| capability_generation == 0) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}

		send_result = cluster_ic_send_envelope(
			PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1, node,
			payload, sizeof(payload));
		disposition = semantic_activation_ack_pending_send_note_result(
			pending, node, send_result);
		if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_RETAINED)
			return true;
		if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED) {
			cluster_ic_tier1_close_peer(
				node, "semantic activation SAMPLE request send failed");
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
		if (disposition != SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
	}
}

static bool
semantic_activation_ack_lmon_begin_sample_round(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckTuple self;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;
	int32 node;

	if (request == NULL || snapshot == NULL
		|| request->request_seq == 0
		|| request->expected_record_generation == UINT64_MAX
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->transition_closed
		|| snapshot->active_bits != request->source_feature_bitmap
		|| snapshot->record_generation
		   != request->expected_record_generation
		|| snapshot->record_generation == UINT64_MAX
		|| !semantic_activation_ack_round_required_caps(
			request->source_feature_bitmap,
			request->target_feature_bitmap,
			request->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| snapshot->formation_epoch != current_epoch)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			snapshot->record_generation + 1, &self))
		return false;

	if (!semantic_activation_ack_table_snapshot(&current))
		return false;
	if (current.expected_members_lo != 0
		|| current.expected_members_hi != 0) {
		if (origin->active) {
			if (current.stage
					!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
				|| current.coordinator_node != UINT32_C(0)
				|| current.round_nonce != request->request_seq
				|| current.expected_members_lo != current_members_lo
				|| current.expected_members_hi != current_members_hi
				|| current.transition_epoch != current_epoch
				|| current.record_generation
				   != snapshot->record_generation + 1
				|| current.source_feature_bitmap
				   != request->source_feature_bitmap
				|| current.target_feature_bitmap
				   != request->target_feature_bitmap
				|| current.rollback_feature_bitmap
				   != request->rollback_feature_bitmap
				|| current.capability_sample_digest != 0
				|| (current.observed_members_lo & UINT64_C(1)) == 0
				|| !semantic_activation_ack_matches(
					&current.observed[0], &self))
				return false;
			return semantic_activation_ack_lmon_send_origin_requests();
		}

		/* A completed prior round is immutable evidence, not an active round.
		 * Replace it only when it is the exact current OPEN predecessor of this
		 * request.  No partial or stale observation is carried into the new
		 * SAMPLE image. */
		if (current.stage
				!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			|| current.flags
			   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
			|| current.record_generation != snapshot->record_generation
			|| current.target_feature_bitmap
			   != request->source_feature_bitmap
			|| current.rollback_feature_bitmap != 0
			|| current.capability_sample_digest == 0
			|| !semantic_activation_ack_complete_image_current(
				&current, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
	}

	for (node = 1; node < 4; node++) {
		uint32 capability_word;
		uint32 capability_generation;

		if (!cluster_sf_peer_capability_word_sample(
				node, required_caps,
				&capability_word, &capability_generation)
			|| capability_generation == 0)
			return false;
	}

	memset(&next, 0, sizeof(next));
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	next.coordinator_node = UINT32_C(0);
	next.round_nonce = request->request_seq;
	next.expected_members_lo = current_members_lo;
	next.expected_members_hi = current_members_hi;
	next.observed_members_lo = UINT64_C(1);
	next.transition_epoch = current_epoch;
	next.record_generation = snapshot->record_generation + 1;
	next.source_feature_bitmap = request->source_feature_bitmap;
	next.target_feature_bitmap = request->target_feature_bitmap;
	next.rollback_feature_bitmap = request->rollback_feature_bitmap;
	next.observed[0] = self;
	if (!semantic_activation_ack_table_publish(&next))
		return false;

	memset(origin, 0, sizeof(*origin));
	origin->unsent_members_lo = UINT64_C(0x0e);
	origin->active = true;
	return semantic_activation_ack_lmon_send_origin_requests();
}

static void
semantic_activation_ack_lmon_revalidate_active(
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;

	if (!semantic_activation_ack_table_snapshot(&image)
		|| (image.expected_members_lo == 0
			&& image.expected_members_hi == 0))
		return;
	if (image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| image.coordinator_node != (uint32)current_coordinator_node)
		(void)semantic_activation_ack_image_invalidate(&image);
}

/* Once durable PREPARE has closed source admission, losing one coherent
 * QVOTEC snapshot is not evidence that the round identity changed.  Keep the
 * exact carrier inert until authority is visible again; every stage driver
 * still performs its normal current-authority revalidation before mutation.
 * Observable epoch, membership, incarnation, capability, tuple, or gate
 * contradictions reject retention and take the ordinary whole-round
 * invalidation path. */
static bool
semantic_activation_ack_closed_carrier_not_contradicted(
	const ClusterSemanticActivationAckTableV1 *image,
	const SemanticActivationAdmissionSnapshot *snapshot)
{
	uint64 observed_mask;
	uint32 local_capability_word;
	bool all_observed;
	int node;

	if (image == NULL || snapshot == NULL
		|| image->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image->round_nonce == 0 || image->record_generation == 0
		|| image->expected_members_lo == 0
		|| image->expected_members_hi != 0
		|| image->observed_members_hi != 0
		|| (image->observed_members_lo & ~image->expected_members_lo) != 0
		|| !snapshot->transition_closed
		|| snapshot->active_bits != image->source_feature_bitmap
		|| snapshot->formation_epoch != image->transition_epoch
		|| cluster_epoch_get_current() != image->transition_epoch
		|| !semantic_activation_ack_terminal_identity_not_contradicted(image))
		return false;

	if (image->stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
		if (snapshot->record_generation == UINT64_MAX
			|| snapshot->record_generation + 1 != image->record_generation)
			return false;
	} else if (snapshot->record_generation != image->record_generation)
		return false;

	all_observed
		= image->observed_members_lo == image->expected_members_lo;
	if (image->flags
		!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			| (all_observed
				   ? CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE
				   : 0))
		|| (image->stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& image->capability_sample_digest != 0)
		|| (image->stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& image->capability_sample_digest == 0))
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_expected_image_current(
			image, image->expected_members_lo, image->expected_members_hi,
			image->transition_epoch, (int32)image->coordinator_node,
			cluster_node_id, local_capability_word))
		return false;

	observed_mask = image->observed_members_lo;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool expected = semantic_activation_ack_member_present(
			image->expected_members_lo, image->expected_members_hi, node);
		bool observed = node < 64
						? (observed_mask & (UINT64_C(1) << node)) != 0
						: false;

		if (!expected) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&image->expected[node],
					sizeof(image->expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&image->observed[node],
					sizeof(image->observed[node])))
				return false;
		} else if (observed) {
			if (!semantic_activation_ack_matches(
					&image->observed[node], &image->expected[node]))
				return false;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&image->observed[node],
				   sizeof(image->observed[node])))
			return false;
	}

	/* The coordinator additionally owns the exact utility/CAS lineage.  A
	 * member has no process-local CAS sequence; its closed gate plus the same
	 * durable generation is the corresponding local proof. */
	if (cluster_node_id == (int32)image->coordinator_node
		&& (semantic_activation_lmon_prepare_cas_seq == 0
			|| semantic_activation_lmon_prepare_cas_utility_request_seq
			   != image->round_nonce))
		return false;

	return true;
}

static void
semantic_activation_ack_lmon_drain(void)
{
	ClusterSemanticActivationAckTableV1 closed_image;
	SemanticActivationAdmissionSnapshot closed_snapshot;
	ClusterSemanticActivationAckTableV1 terminal_image;
	SemanticActivationAdmissionSnapshot terminal_snapshot;
	SemanticActivationAckIngressItem item;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 publication_seq;
	int32 current_coordinator_node;
	uint32 consumed = 0;

	if (semantic_activation_ack_ingress_pending(
			&semantic_activation_ack_local_ingress) == 0) {
		if (SemanticActivationAckTable == NULL)
			return;
		publication_seq = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if ((publication_seq & UINT64_C(1)) == 0
			&& SemanticActivationAckTable->expected_members_lo == 0
			&& SemanticActivationAckTable->expected_members_hi == 0)
			return;
	}
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)) {
		/* A coherent admitted snapshot can be briefly unavailable while QVOTEC
		 * publishes its next observation.  Absence alone cannot erase either a
		 * completed OPEN carrier or an exact source-closed in-progress carrier.
		 * Consumers and stage drivers remain fail-closed until current authority
		 * is visible again; observable contradictions still invalidate the whole
		 * round below. */
		if (semantic_activation_ack_ingress_pending(
				&semantic_activation_ack_local_ingress) == 0
			&& semantic_activation_ack_local_pending_send.pending_members_lo == 0
			&& semantic_activation_ack_local_pending_send.pending_members_hi == 0
			&& !semantic_activation_ack_local_pending_send.invalidated
			&& !semantic_activation_ack_local_request_origin.active
			&& semantic_activation_ack_local_stage_ahead.count == 0
			&& !semantic_activation_ack_local_request_ahead.valid
			&& semantic_activation_ack_table_snapshot(&terminal_image)
			&& semantic_activation_snapshot(&terminal_snapshot)
			&& terminal_image.stage
				== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			&& terminal_image.flags
				== (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
			&& terminal_image.expected_members_lo != 0
			&& terminal_image.observed_members_lo
				== terminal_image.expected_members_lo
			&& terminal_image.observed_members_hi
				== terminal_image.expected_members_hi
			&& terminal_image.rollback_feature_bitmap == 0
			&& semantic_activation_ack_image_structural(&terminal_image)
			&& semantic_activation_full_ack_table_matches(
				terminal_image.observed,
				terminal_image.observed_members_lo,
				terminal_image.observed_members_hi,
				terminal_image.expected,
				terminal_image.expected_members_lo,
				terminal_image.expected_members_hi)
			&& !terminal_snapshot.transition_closed
			&& terminal_snapshot.active_bits
				== terminal_image.target_feature_bitmap
			&& terminal_snapshot.record_generation
				== terminal_image.record_generation
			&& terminal_snapshot.formation_epoch
				== terminal_image.transition_epoch
			&& cluster_epoch_get_current()
				== terminal_snapshot.formation_epoch
			&& semantic_activation_ack_terminal_identity_not_contradicted(
				&terminal_image))
			return;
		if (semantic_activation_ack_table_snapshot(&closed_image)
			&& semantic_activation_snapshot(&closed_snapshot)
			&& semantic_activation_ack_closed_carrier_not_contradicted(
				&closed_image, &closed_snapshot))
			return;
		semantic_activation_ack_lmon_invalidate_active();
		if ((semantic_activation_ack_local_pending_send.pending_members_lo != 0
			 || semantic_activation_ack_local_pending_send.pending_members_hi != 0)
			&& semantic_activation_ack_local_pending_send.message.result
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED) {
			semantic_activation_ack_local_pending_send.pending_members_lo = 0;
			semantic_activation_ack_local_pending_send.pending_members_hi = 0;
			semantic_activation_ack_local_pending_send.invalidated = true;
		}
		while (consumed < CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY
			   && semantic_activation_ack_ingress_poll(
				   &semantic_activation_ack_local_ingress, &item)) {
			consumed++;
			if (!semantic_activation_ack_pending_send_begin_refused(
					&semantic_activation_ack_local_pending_send, &item,
					CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED))
				continue;
		}
		semantic_activation_ack_lmon_send_pending();
		return;
	}
	semantic_activation_ack_lmon_revalidate_active(
		current_members_lo, current_members_hi, current_epoch,
		current_coordinator_node);
	(void) semantic_activation_ack_lmon_consume_request_ahead(
		current_members_lo, current_members_hi, current_epoch,
		current_coordinator_node);

	while (consumed < CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY
		   && semantic_activation_ack_ingress_poll(
			   &semantic_activation_ack_local_ingress, &item)) {
		consumed++;
		if (!semantic_activation_ack_current_authority(
				cluster_node_id, &current_members_lo, &current_members_hi,
				&current_epoch, &current_coordinator_node)) {
			semantic_activation_ack_lmon_invalidate_active();
			continue;
		}
		if (item.message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK) {
			SemanticActivationAckConsumeResult apply_result
				= semantic_activation_ack_lmon_apply_item(
					&item, current_members_lo, current_members_hi,
					current_epoch, current_coordinator_node);

			if (apply_result == SEMANTIC_ACTIVATION_ACK_CONSUME_STALE)
				apply_result
					= semantic_activation_ack_lmon_retain_stage_ahead(
						&item, current_members_lo, current_members_hi,
						current_epoch, current_coordinator_node);

			ereport(LOG,
					(errmsg("bit22 cutover (node %d): coordinator applied "
							"member ACK stage=%u src=%d result=%d",
							cluster_node_id,
							(unsigned) item.message.stage,
							item.authenticated_source_node_id,
							(int) apply_result)));
			(void) apply_result;
			(void) semantic_activation_ack_lmon_consume_request_ahead(
				current_members_lo, current_members_hi, current_epoch,
				current_coordinator_node);
		} else if (item.message.kind
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST) {
			SemanticActivationAdmissionSnapshot snapshot;
			ClusterSemanticActivationAckTableV1 image;
			SemanticActivationAckConsumeResult result;
			uint32 local_capability_word
				= cluster_ic_local_capability_word();

			if (!semantic_activation_snapshot(&snapshot))
				continue;
			if (item.message.stage
				== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE) {
				result
					= semantic_activation_ack_lmon_accept_current_sample_request(
						&item, &snapshot, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node,
						local_capability_word);
				if (result
					== SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED) {
					result
						= semantic_activation_ack_lmon_retain_request_ahead(
							&item, &snapshot, current_members_lo,
							current_members_hi, current_epoch,
							current_coordinator_node,
							local_capability_word);
					if (result
						== SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
						|| result
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE
						|| result
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED)
						continue;
				}
			}
			else if (item.message.stage
					 == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER) {
				bool bit22
					= (item.message.target_feature_bitmap
					   & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0;

				if (bit22)
					result
						= semantic_activation_ack_lmon_accept_current_barrier_request_bit22(
						&item, &snapshot, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node);
				else
					result
						= semantic_activation_ack_lmon_accept_current_barrier_request(
						&item, &snapshot, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node,
						local_capability_word);
				if (!bit22
					&& result
					   == SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED) {
					result
						= semantic_activation_ack_lmon_retain_request_ahead(
							&item, &snapshot, current_members_lo,
							current_members_hi, current_epoch,
							current_coordinator_node,
							local_capability_word);
					if (result
						== SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
						|| result
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE
						|| result
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED)
						continue;
				}
				if (!semantic_activation_ack_lmon_apply_stage_ahead(
						&item, result, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node))
					continue;
				continue;
			} else if (item.message.stage
						== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
						|| item.message.stage
						== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
						|| item.message.stage
						== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
				/* RF-ROOT P9 verification (cold-formation): the bit22
				 * cutover round's later-stage requests use the
				 * round-parameterized accept (member set from the ACK
				 * table, target carries bit22); the R4 accepts are
				 * four-member hardcoded and would reject them. */
				SemanticActivationAckConsumeResult acc;

				if ((item.message.target_feature_bitmap
					 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0) {
					acc
						= semantic_activation_ack_lmon_accept_current_request_bit22(
							&item, &snapshot, current_members_lo,
							current_members_hi, current_epoch,
							current_coordinator_node,
							local_capability_word);

					ereport(LOG,
							(errmsg("bit22 cutover (node %d): member accept "
									"stage=%u result=%d",
									cluster_node_id,
									(unsigned) item.message.stage,
									(int) acc)));
					(void) acc;
				} else if (item.message.stage
						 == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED)
					acc
						= semantic_activation_ack_lmon_accept_current_prepared_request(
							&item, &snapshot, current_members_lo,
							current_members_hi, current_epoch,
							current_coordinator_node,
							local_capability_word);
				else
					acc
						= semantic_activation_ack_lmon_accept_current_commit_applied_request(
							&item, &snapshot, current_members_lo,
							current_members_hi, current_epoch,
							current_coordinator_node,
							local_capability_word);
				if (acc
					== SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED) {
					acc = semantic_activation_ack_lmon_retain_request_ahead(
						&item, &snapshot, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node,
						local_capability_word);
					if (acc == SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
						|| acc
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE
						|| acc
						   == SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED)
						continue;
				}
				(void)semantic_activation_ack_lmon_apply_stage_ahead(
					&item, acc, current_members_lo, current_members_hi,
					current_epoch, current_coordinator_node);
				continue;
			} else
				continue;
			if (!semantic_activation_ack_lmon_apply_stage_ahead(
					&item, result, current_members_lo, current_members_hi,
					current_epoch, current_coordinator_node))
				continue;
			if (result != SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED)
				continue;
			if (!semantic_activation_ack_table_snapshot(&image)
				|| !semantic_activation_ack_pending_send_begin_positive(
					&semantic_activation_ack_local_pending_send,
					&item.message, cluster_node_id,
					&image.observed[cluster_node_id]))
				semantic_activation_ack_lmon_invalidate_active();
		}
	}
	semantic_activation_ack_lmon_send_pending();
}

typedef enum SemanticActivationHeldLocks {
	SEMANTIC_ACTIVATION_HELD_NONE = 0,
	SEMANTIC_ACTIVATION_HELD_RESOURCE = UINT32_C(1),
	SEMANTIC_ACTIVATION_HELD_BUFFER = UINT32_C(2),
	SEMANTIC_ACTIVATION_HELD_SLRU = UINT32_C(4),
	SEMANTIC_ACTIVATION_HELD_UNDO_IO = UINT32_C(8),
	SEMANTIC_ACTIVATION_HELD_IC_DISPATCH = UINT32_C(16),
	SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN = UINT32_C(31)
} SemanticActivationHeldLocks;

typedef enum SemanticActivationWaitEdge {
	SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON = 0,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC = 1,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK = 2,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER = 3
} SemanticActivationWaitEdge;

typedef enum SemanticActivationActor {
	SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY = 0,
	SEMANTIC_ACTIVATION_ACTOR_LMON,
	SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
	SEMANTIC_ACTIVATION_ACTOR_LMS,
	SEMANTIC_ACTIVATION_ACTOR_DATA
} SemanticActivationActor;

typedef enum SemanticActivationEffect {
	SEMANTIC_ACTIVATION_EFFECT_NONE = 0,
	SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION = UINT32_C(1),
	SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE = UINT32_C(2),
	SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE = UINT32_C(4),
	SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN = UINT32_C(8),
	SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION = UINT32_C(16),
	SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE = UINT32_C(32),
	SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE = UINT32_C(64)
} SemanticActivationEffect;

static bool semantic_activation_snapshot(SemanticActivationAdmissionSnapshot *snapshot);
static bool semantic_activation_lmon_publish_gate(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 active_bits,
	uint64 record_generation, uint64 formation_epoch,
	bool transition_closed);
static bool semantic_activation_counter_increment(pg_atomic_uint32 *counter);
static bool semantic_activation_ensure_exit_hook(void);
static void semantic_activation_release_debt(ClusterSemanticAdmissionSide side,
										 int feature_index);

static uint16
semantic_activation_read_u16_le(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
semantic_activation_read_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
semantic_activation_read_u64_le(const uint8 *bytes)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | bytes[i];
	return value;
}

static void
semantic_activation_write_u16_le(uint8 *bytes, uint16 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
}

static void
semantic_activation_write_u32_le(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static void
semantic_activation_write_u64_le(uint8 *bytes, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		bytes[i] = (uint8)value;
		value >>= 8;
	}
}

static bool
semantic_activation_bytes_are_zero(const uint8 *bytes, Size len)
{
	Size i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static void
semantic_activation_set_refusal(ClusterSemanticActivationRefusal *refusal,
								ClusterSemanticActivationResult result, uint64 feature_bit,
								uint64 expected_generation)
{
	if (refusal == NULL)
		return;

	refusal->result = result;
	refusal->feature_bit = feature_bit;
	refusal->expected_generation = expected_generation;
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);

static ClusterSemanticActivationResult
semantic_activation_select_majority(const ClusterSemanticRecordSample *samples, uint32 n_samples,
									uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
									bool *implicit_open)
{
	uint32 majority;
	uint32 zero_count = 0;
	uint32 i;

	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (samples == NULL || n_samples == 0 || selected == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	majority = n_samples / 2 + 1;
	for (i = 0; i < n_samples; i++) {
		if (samples[i].readable
			&& semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES))
			zero_count++;
	}
	if (zero_count >= majority) {
		if (implicit_open != NULL)
			*implicit_open = true;
		return CLUSTER_SEMANTIC_ACTIVATION_OK;
	}

	/* An exact valid byte image, rather than merely a generation, must win. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord record;
		uint32 identical = 0;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &record, NULL))
			continue;

		for (j = 0; j < n_samples; j++) {
			if (samples[j].readable
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   == 0)
				identical++;
		}
		if (identical >= majority) {
			memcpy(selected, samples[i].bytes, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
			return CLUSTER_SEMANTIC_ACTIVATION_OK;
		}
	}

	/* A split record at one generation is never reclassified as legacy zero. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord left;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &left, NULL))
			continue;
		for (j = i + 1; j < n_samples; j++) {
			ClusterSemanticActivationRecord right;

			if (!samples[j].readable
				|| semantic_activation_bytes_are_zero(samples[j].bytes,
													  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
				|| !cluster_semantic_activation_record_decode(samples[j].bytes, &right, NULL))
				continue;
			if (left.record_generation == right.record_generation
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   != 0)
				return CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT;
		}
	}

	return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
}

static bool
semantic_activation_fsm_next(SemanticActivationState current, bool reverse,
							 SemanticActivationState *next)
{
	(void)reverse;
	if (next == NULL || current < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| current >= SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)
		return false;

	*next = (SemanticActivationState)(current + 1);
	return true;
}

static SemanticActivationCallbackKind
semantic_activation_callback_for_state(SemanticActivationState state)
{
	switch (state) {
	case SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED:
		return SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE;
	case SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO;
	case SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO;
	case SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TARGET_STAGED:
		return SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET;
	case SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED:
		return SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED;
	case SEMANTIC_ACTIVATION_STATE_TARGET_OPEN:
		return SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET;
	default:
		return SEMANTIC_ACTIVATION_CALLBACK_NONE;
	}
}

static bool
semantic_activation_source_target_exclusive(bool source_open, bool target_open)
{
	return !(source_open && target_open);
}

static bool
semantic_activation_failure_policy(SemanticActivationState state,
								   SemanticActivationFailurePolicy *policy)
{
	if (policy == NULL || state < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| state > SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
		return false;

	policy->target = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->admission_closed_until_source_open = state != SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->revert_source_closed = state == SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED;
	return true;
}

static bool semantic_activation_ack_tuple_encode(
	const SemanticActivationAckTuple *tuple,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES]) pg_attribute_unused();
static bool semantic_activation_ack_sample_digest(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 *digest_out) pg_attribute_unused();

static bool
semantic_activation_ack_tuple_encode(
	const SemanticActivationAckTuple *tuple,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES];

	if (tuple == NULL || bytes == NULL)
		return false;
	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded, tuple->node_id);
	semantic_activation_write_u64_le(encoded + 8, tuple->boot_id);
	semantic_activation_write_u64_le(encoded + 16, tuple->admitted_incarnation);
	semantic_activation_write_u64_le(encoded + 24,
								 tuple->control_connection_generation);
	semantic_activation_write_u32_le(encoded + 32, tuple->capability_word);
	semantic_activation_write_u64_le(encoded + 40, tuple->capability_generation);
	semantic_activation_write_u64_le(encoded + 48, tuple->transition_epoch);
	semantic_activation_write_u64_le(encoded + 56, tuple->record_generation);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

static bool
semantic_activation_ack_sample_digest(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 *digest_out)
{
	uint8 input[72 + 4 * CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES];
	uint8 sha256[PG_SHA256_DIGEST_LENGTH];
	pg_cryptohash_ctx *ctx;
	uint64 digest = 0;
	bool success = false;
	int node;
	int i;

	if (digest_out == NULL)
		return false;
	*digest_out = 0;
	if (image == NULL
		|| image->stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image->flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| image->coordinator_node != UINT32_C(0)
		|| image->round_nonce == 0
		|| image->expected_members_lo != UINT64_C(0x0f)
		|| image->expected_members_hi != 0
		|| image->observed_members_lo != UINT64_C(0x0f)
		|| image->observed_members_hi != 0
		|| image->record_generation == 0
		|| image->capability_sample_digest != 0
		|| !semantic_activation_ack_image_structural(image)
		|| !semantic_activation_full_ack_table_matches(
			image->observed, image->observed_members_lo,
			image->observed_members_hi, image->expected,
			image->expected_members_lo, image->expected_members_hi))
		return false;

	memset(input, 0, sizeof(input));
	semantic_activation_write_u32_le(input,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC);
	semantic_activation_write_u16_le(input + 4,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION);
	semantic_activation_write_u16_le(
		input + 6, CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES);
	semantic_activation_write_u64_le(input + 8, image->transition_epoch);
	semantic_activation_write_u64_le(input + 16, image->record_generation);
	semantic_activation_write_u64_le(input + 24,
								 image->expected_members_lo);
	semantic_activation_write_u64_le(input + 32,
								 image->expected_members_hi);
	semantic_activation_write_u64_le(input + 40,
								 image->source_feature_bitmap);
	semantic_activation_write_u64_le(input + 48,
								 image->target_feature_bitmap);
	semantic_activation_write_u64_le(input + 56,
								 image->rollback_feature_bitmap);
	semantic_activation_write_u32_le(input + 64, UINT32_C(4));
	for (node = 0; node < 4; node++) {
		if (!semantic_activation_ack_tuple_encode(
				&image->expected[node],
				input + 72
					+ node * CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES))
			return false;
	}

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) < 0
		|| pg_cryptohash_update(ctx, input, sizeof(input)) < 0
		|| pg_cryptohash_final(ctx, sha256, sizeof(sha256)) < 0)
		goto done;
	for (i = 0; i < 8; i++)
		digest = (digest << 8) | sha256[i];
	if (digest == 0)
		goto done;
	*digest_out = digest;
	success = true;

done:
	pg_cryptohash_free(ctx);
	return success;
}

static bool
semantic_activation_ack_matches(const SemanticActivationAckTuple *observed,
								const SemanticActivationAckTuple *expected)
{
	return observed != NULL && expected != NULL && observed->node_id == expected->node_id
		   && observed->boot_id == expected->boot_id
		   && observed->admitted_incarnation == expected->admitted_incarnation
		   && observed->control_connection_generation == expected->control_connection_generation
		   && observed->capability_word == expected->capability_word
		   && observed->capability_generation == expected->capability_generation
		   && observed->transition_epoch == expected->transition_epoch
			   && observed->record_generation == expected->record_generation;
}

/* The ACK bitmap is part of the proof, not a cache hint: no missing or extra
 * node may be ignored, and every member must match its complete current tuple. */
static bool semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi) pg_attribute_unused();

static bool
semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi)
{
	int node;

	if (observed == NULL || expected == NULL
		|| (expected_members_lo == 0 && expected_members_hi == 0)
		|| observed_members_lo != expected_members_lo
		|| observed_members_hi != expected_members_hi)
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 64
					  ? (expected_members_lo & (UINT64_C(1) << node)) != 0
					  : (expected_members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if (!member)
			continue;
		if (expected[node].node_id != (uint32)node
			|| expected[node].boot_id == 0
			|| expected[node].admitted_incarnation == 0
			|| expected[node].control_connection_generation == 0
			|| (expected[node].capability_word
				& CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS)
				   != CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS
			|| expected[node].capability_generation == 0
			|| expected[node].record_generation == 0
			|| !semantic_activation_ack_matches(&observed[node],
											 &expected[node]))
			return false;
	}
	return true;
}

static ClusterSemanticResourceXPeerOpenResult
semantic_activation_ack_complete_image_check(const ClusterSemanticActivationAckTableV1 *image,
											 uint64 current_members_lo, uint64 current_members_hi,
											 uint64 current_epoch, int32 current_coordinator_node,
											 int32 local_node_id, uint32 local_capability_word)
{
	SemanticActivationAckTuple current;
	ClusterSfPeerCap capability;
	uint32 required_caps;
	uint64 admitted_incarnation;
	bool observation_pending = false;
	int node;

	if (image == NULL || local_node_id < 0
		|| local_node_id >= CLUSTER_MAX_NODES
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| image->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image->round_nonce == 0 || image->record_generation == 0
		|| image->coordinator_node != (uint32)current_coordinator_node
		|| image->expected_members_lo != current_members_lo
		|| image->expected_members_hi != current_members_hi
		|| image->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			image->source_feature_bitmap, image->target_feature_bitmap,
			image->rollback_feature_bitmap, &required_caps)
		|| (local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| (image->flags
			& (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE))
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| !semantic_activation_full_ack_table_matches(
			image->observed, image->observed_members_lo,
			image->observed_members_hi, image->expected,
			image->expected_members_lo, image->expected_members_hi))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!semantic_activation_ack_member_present(
				current_members_lo, current_members_hi, node))
			continue;
		admitted_incarnation = cluster_membership_get_last_admitted_incarnation(node);
		if (image->expected[node].transition_epoch != current_epoch
			|| image->expected[node].record_generation != image->record_generation
			|| cluster_membership_get_state(node) != CLUSTER_MEMBER_MEMBER
			|| (admitted_incarnation != 0
				&& (admitted_incarnation != image->expected[node].admitted_incarnation
					|| admitted_incarnation != image->observed[node].admitted_incarnation
					|| (node != local_node_id
						&& admitted_incarnation != image->expected[node].boot_id))))
			return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;
		if (node == local_node_id) {
			uint64 self_incarnation = cluster_qvotec_get_self_incarnation();

			if ((local_capability_word & CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
					!= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
				|| local_capability_word != image->expected[node].capability_word
				|| (self_incarnation != 0
					&& (self_incarnation != image->expected[node].boot_id
						|| self_incarnation != image->observed[node].boot_id
						|| self_incarnation != image->expected[node].control_connection_generation
						|| self_incarnation != image->expected[node].capability_generation
						|| (admitted_incarnation != 0
							&& self_incarnation != admitted_incarnation))))
				return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;
			if (self_incarnation == 0 || admitted_incarnation == 0) {
				observation_pending = true;
				continue;
			}
			memset(&current, 0, sizeof(current));
			current.node_id = (uint32)node;
			current.boot_id = self_incarnation;
			current.admitted_incarnation = admitted_incarnation;
			current.control_connection_generation = self_incarnation;
			current.capability_word = local_capability_word;
			current.capability_generation = self_incarnation;
			current.transition_epoch = current_epoch;
			current.record_generation = image->record_generation;
		} else {
			if (!cluster_sf_peer_capability_record_snapshot(node, &capability))
				return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;
			if (!capability.valid) {
				observation_pending = true;
				continue;
			}
			if (capability.generation == 0 || (capability.bits & required_caps) != required_caps
				|| capability.generation != image->expected[node].capability_generation
				|| capability.generation != image->observed[node].capability_generation
				|| capability.generation != image->expected[node].control_connection_generation
				|| capability.bits != image->expected[node].capability_word
				|| capability.bits != image->observed[node].capability_word)
				return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;
			if (admitted_incarnation == 0) {
				observation_pending = true;
				continue;
			}
			memset(&current, 0, sizeof(current));
			current.node_id = (uint32)node;
			current.boot_id = admitted_incarnation;
			current.admitted_incarnation = admitted_incarnation;
			current.control_connection_generation = (uint64)capability.generation;
			current.capability_word = capability.bits;
			current.capability_generation = (uint64)capability.generation;
			current.transition_epoch = current_epoch;
			current.record_generation = image->record_generation;
		}
		if (!semantic_activation_ack_matches(
				&image->expected[node], &current)
			|| !semantic_activation_ack_matches(
				&image->observed[node], &current))
			return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT;
	}
	return observation_pending ? CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING
							   : CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
}

static bool
semantic_activation_ack_complete_image_current(const ClusterSemanticActivationAckTableV1 *image,
											   uint64 current_members_lo, uint64 current_members_hi,
											   uint64 current_epoch, int32 current_coordinator_node,
											   int32 local_node_id, uint32 local_capability_word)
{
	return semantic_activation_ack_complete_image_check(
			   image, current_members_lo, current_members_hi, current_epoch,
			   current_coordinator_node, local_node_id, local_capability_word)
		   == CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
}

static bool
semantic_activation_ack_expected_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id, uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 candidate;

	if (image == NULL)
		return false;
	candidate = *image;
	candidate.flags
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
		  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	candidate.observed_members_lo = candidate.expected_members_lo;
	candidate.observed_members_hi = candidate.expected_members_hi;
	memcpy(candidate.observed, candidate.expected,
		   sizeof(candidate.observed));
	return semantic_activation_ack_complete_image_current(
		&candidate, current_members_lo, current_members_hi,
		current_epoch, current_coordinator_node, local_node_id,
		local_capability_word);
}

static bool
semantic_activation_ack_lmon_prepare_cas_active(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
{
	ClusterSemanticActivationRecord desired;
	uint64 request_seq;
	uint64 completion_seq;

	if (request == NULL || snapshot == NULL
		|| SemanticActivationShmem == NULL
		|| request->expected_record_generation == UINT64_MAX
		|| semantic_activation_lmon_prepare_cas_seq == 0
		|| semantic_activation_lmon_prepare_cas_utility_request_seq
		   != request->request_seq)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq != semantic_activation_lmon_prepare_cas_seq
		|| (completion_seq != request_seq
			&& (completion_seq == UINT64_MAX
				|| completion_seq + 1 != request_seq)))
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
			!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS
		|| SemanticActivationShmem->record_cas_expected_generation
		   != request->expected_record_generation
		|| SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		   != request->source_feature_bitmap
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL)
		|| desired.phase != CLUSTER_SEMANTIC_PHASE_PREPARE
		|| desired.record_generation
		   != request->expected_record_generation + 1
		|| desired.transition_epoch != snapshot->formation_epoch
		|| desired.source_feature_bitmap
		   != request->source_feature_bitmap
		|| desired.target_feature_bitmap
		   != request->target_feature_bitmap
		|| desired.rollback_feature_bitmap
		   != request->rollback_feature_bitmap
		|| desired.admitted_members_lo != UINT64_C(0x0f)
		|| desired.admitted_members_hi != 0
		|| desired.capability_sample_digest == 0
		|| desired.coordinator_node != UINT32_C(0)
		|| desired.coordinator_incarnation
		   != cluster_qvotec_get_self_incarnation())
		return false;
	return true;
}

static bool
semantic_activation_ack_lmon_commit_cas_active(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
{
	ClusterSemanticActivationRecord desired;
	uint64 request_seq;
	uint64 completion_seq;

	if (request == NULL || snapshot == NULL
		|| SemanticActivationShmem == NULL
		|| request->expected_record_generation > UINT64_MAX - 2
		|| semantic_activation_lmon_commit_cas_seq == 0
		|| semantic_activation_lmon_commit_cas_utility_request_seq
		   != request->request_seq)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq != semantic_activation_lmon_commit_cas_seq
		|| (completion_seq != request_seq
			&& (completion_seq == UINT64_MAX
				|| completion_seq + 1 != request_seq)))
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
			!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS
		|| SemanticActivationShmem->record_cas_expected_generation
		   != request->expected_record_generation + 1
		|| SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		   != request->source_feature_bitmap
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL)
		|| desired.phase != CLUSTER_SEMANTIC_PHASE_COMMIT
		|| desired.record_generation
		   != request->expected_record_generation + 2
		|| desired.source_feature_bitmap
		   != request->source_feature_bitmap
		|| desired.target_feature_bitmap
		   != request->target_feature_bitmap
		|| desired.rollback_feature_bitmap
		   != request->rollback_feature_bitmap
		|| desired.admitted_members_lo != UINT64_C(0x0f)
		|| desired.admitted_members_hi != 0
		|| desired.capability_sample_digest == 0
		|| desired.coordinator_node != UINT32_C(0)
		|| desired.coordinator_incarnation
		   != cluster_qvotec_get_self_incarnation())
		return false;
	return true;
}

static bool
semantic_activation_ack_lmon_open_cas_active(
	const SemanticActivationUtilityRequest *request)
{
	ClusterSemanticActivationRecord desired;
	uint64 request_seq;
	uint64 completion_seq;

	if (request == NULL || SemanticActivationShmem == NULL
		|| request->expected_record_generation > UINT64_MAX - 3
		|| semantic_activation_lmon_open_cas_seq == 0
		|| semantic_activation_lmon_open_cas_utility_request_seq
		   != request->request_seq)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq != semantic_activation_lmon_open_cas_seq
		|| (completion_seq != request_seq
			&& (completion_seq == UINT64_MAX
				|| completion_seq + 1 != request_seq)))
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
			!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS
		|| SemanticActivationShmem->record_cas_expected_generation
		   != request->expected_record_generation + 2
		|| SemanticActivationShmem
			   ->record_cas_expected_source_feature_bitmap
		   != request->source_feature_bitmap
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL)
		|| desired.phase != CLUSTER_SEMANTIC_PHASE_OPEN
		|| desired.record_generation
		   != request->expected_record_generation + 3
		|| desired.source_feature_bitmap
		   != request->source_feature_bitmap
		|| desired.target_feature_bitmap
		   != request->target_feature_bitmap
		|| desired.rollback_feature_bitmap
		   != request->rollback_feature_bitmap
		|| desired.admitted_members_lo != UINT64_C(0x0f)
		|| desired.admitted_members_hi != 0
		|| desired.capability_sample_digest == 0
		|| desired.coordinator_node != UINT32_C(0)
		|| desired.coordinator_incarnation
		   != cluster_qvotec_get_self_incarnation())
		return false;
	return true;
}

static bool
semantic_activation_ack_lmon_install_commit(
	const SemanticActivationUtilityRequest *request)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	ClusterSemanticActivationRecord desired;
	ClusterSemanticActivationRecord open_record;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	const ClusterSemanticActivationDescriptor *round_descriptor = NULL;
	ClusterSemanticActivationResult result;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 current_members_lo = 0;
	uint64 current_members_hi = 0;
	uint64 current_epoch = 0;
	uint64 self_bit;
	uint64 cas_seq;
	uint32 local_capability_word;
	uint32 required_caps = 0;
	int32 current_coordinator_node = -1;
	int node;

	memset(&after, 0, sizeof(after));
	memset(&current_snapshot, 0, sizeof(current_snapshot));
	if (request == NULL || !semantic_activation_snapshot(&snapshot)
		|| !semantic_activation_ack_lmon_commit_cas_active(
			request, &snapshot)
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL))
		return false;
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch)
		return false;
	if (!semantic_activation_ack_table_snapshot(&before)
		|| before.coordinator_node != desired.coordinator_node
		|| before.round_nonce != request->request_seq
		|| before.expected_members_lo != desired.admitted_members_lo
		|| before.expected_members_hi != desired.admitted_members_hi
		|| before.transition_epoch != desired.transition_epoch
		|| before.source_feature_bitmap != desired.source_feature_bitmap
		|| before.target_feature_bitmap != desired.target_feature_bitmap
		|| before.rollback_feature_bitmap
		   != desired.rollback_feature_bitmap
		|| before.capability_sample_digest
		   != desired.capability_sample_digest)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if ((desired.target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		&& (!semantic_activation_round_descriptor(
			desired.source_feature_bitmap,
			desired.target_feature_bitmap,
			desired.rollback_feature_bitmap, &round_descriptor,
			&required_caps)
			|| (local_capability_word & required_caps) != required_caps))
		return false;
	if (snapshot.transition_closed
		&& snapshot.active_bits == desired.source_feature_bitmap
		&& snapshot.record_generation == desired.record_generation
		&& snapshot.formation_epoch == desired.transition_epoch) {
		if (before.stage
				!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
			|| before.record_generation != desired.record_generation
			|| !origin->active
			|| !semantic_activation_ack_expected_image_current(
				&before, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		if (round_descriptor != NULL) {
			if (!semantic_activation_ack_member_prepared_image_current(
					&before, NULL))
				return false;
			self_bit = UINT64_C(1) << cluster_node_id;
			if ((before.observed_members_lo & self_bit) == 0) {
				if (round_descriptor->feature_bit
					== CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1)
					return false;
				result = round_descriptor->apply_target_closed(
					desired.record_generation);
				return semantic_activation_ack_lmon_finish_member_prepared(
					&before, result);
			}
			if ((before.flags
				 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0) {
				open_record = desired;
				open_record.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
				open_record.record_generation++;
				if (!cluster_semantic_activation_record_encode(
						&open_record, desired_bytes)
					|| !semantic_activation_snapshot(&current_snapshot)
					|| current_snapshot.seq != snapshot.seq
					|| current_snapshot.active_bits != snapshot.active_bits
					|| current_snapshot.record_generation
					   != snapshot.record_generation
					|| current_snapshot.formation_epoch
					   != snapshot.formation_epoch
					|| current_snapshot.transition_closed
					   != snapshot.transition_closed
					|| !semantic_activation_ack_current_authority(
						cluster_node_id, &current_members_lo,
						&current_members_hi, &current_epoch,
						&current_coordinator_node)
					|| current_members_lo
					   != desired.admitted_members_lo
					|| current_members_hi
					   != desired.admitted_members_hi
					|| current_epoch != desired.transition_epoch
					|| current_coordinator_node
					   != (int32)desired.coordinator_node
					|| cluster_ic_local_capability_word()
					   != local_capability_word
					|| cluster_qvotec_get_self_incarnation()
					   != desired.coordinator_incarnation
					|| !semantic_activation_ack_table_snapshot(&after)
					|| memcmp(&before, &after, sizeof(before)) != 0
					|| !semantic_activation_ack_complete_image_current(
						&after, current_members_lo,
						current_members_hi, current_epoch,
						current_coordinator_node, cluster_node_id,
						local_capability_word))
					return false;
				if (!semantic_activation_record_cas_mailbox_submit(
						desired.record_generation,
						desired.source_feature_bitmap, desired_bytes,
						&cas_seq))
					return false;
				semantic_activation_lmon_open_cas_seq = cas_seq;
				semantic_activation_lmon_open_cas_utility_request_seq
					= request->request_seq;
				return true;
			}
		} else if (before.flags
				   != CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   || before.observed_members_lo != 0
				   || before.observed_members_hi != 0
				   || !semantic_activation_bytes_are_zero(
					   (const uint8 *)before.observed,
					   sizeof(before.observed)))
			return false;
		return semantic_activation_ack_lmon_send_origin_requests();
	}

	if (!snapshot.transition_closed
		|| snapshot.active_bits != desired.source_feature_bitmap
		|| snapshot.record_generation + 1 != desired.record_generation
		|| snapshot.formation_epoch != desired.transition_epoch
		|| before.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| before.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| before.record_generation != snapshot.record_generation
		|| !semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;
	/* RF-ROOT P7 (contract, step ②): the bit22 cutover round branches off
	 * here — the PREPARED-stage all-member CLOSED-ACK (W6 clause 3) is
	 * COMPLETE, so the coordinator activates the canonical root and
	 * advances the round to OPEN_APPLIED instead of the R4 COMMIT_APPLIED
	 * path (contract option A: the cutover round is an independent stage
	 * sequence). */
	if (!semantic_activation_record_cas_mailbox_poll_completion(
			semantic_activation_lmon_commit_cas_seq, &result))
		return true;
	/* RF-ROOT P9 verification (implementation): the bit22 cutover round now waits for
	 * the majority COMMIT(P+1) record (durable) before advancing — the
	 * root is activated only after COMMIT durability, then the
	 * COMMIT_APPLIED stage verifies the ACTIVE root member-side. */
	if ((desired.target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0) {
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
			return false;
		return semantic_activation_ack_lmon_bit22_commit_applied_begin(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	}
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return false;
	if (!semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed)
		return false;
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| current_coordinator_node != (int32)desired.coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| cluster_qvotec_get_self_incarnation()
		   != desired.coordinator_incarnation)
		return false;
	if (!semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;
	if (round_descriptor != NULL
		&& round_descriptor->feature_bit
		   == CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) {
		result = round_descriptor->apply_target_closed(
			request->expected_record_generation + 1);
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
			|| !semantic_activation_snapshot(&current_snapshot)
			|| current_snapshot.seq != snapshot.seq
			|| current_snapshot.active_bits != snapshot.active_bits
			|| current_snapshot.record_generation
			   != snapshot.record_generation
			|| current_snapshot.formation_epoch
			   != snapshot.formation_epoch
			|| current_snapshot.transition_closed
			   != snapshot.transition_closed
			|| !semantic_activation_ack_current_authority(
				cluster_node_id, &current_members_lo, &current_members_hi,
				&current_epoch, &current_coordinator_node)
			|| current_members_lo != desired.admitted_members_lo
			|| current_members_hi != desired.admitted_members_hi
			|| current_epoch != desired.transition_epoch
			|| current_coordinator_node
			   != (int32)desired.coordinator_node
			|| cluster_ic_local_capability_word()
			   != local_capability_word
			|| !semantic_activation_ack_table_snapshot(&after)
			|| memcmp(&before, &after, sizeof(before)) != 0
			|| !semantic_activation_ack_complete_image_current(
				&after, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
	}
	if (!semantic_activation_lmon_publish_gate(
			&current_snapshot, desired.source_feature_bitmap,
			desired.record_generation, desired.transition_epoch, true))
		return false;

	next = after;
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.record_generation = desired.record_generation;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	memset(next.observed, 0, sizeof(next.observed));
	for (node = 0; node < 4; node++)
		next.expected[node].record_generation = desired.record_generation;
	if (round_descriptor != NULL
		&& round_descriptor->feature_bit
		   == CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) {
		next.observed_members_lo = UINT64_C(1);
		next.observed[0] = next.expected[0];
	}
	if (!semantic_activation_snapshot(&current_snapshot)
		|| !current_snapshot.transition_closed
		|| current_snapshot.active_bits != desired.source_feature_bitmap
		|| current_snapshot.record_generation != desired.record_generation
		|| current_snapshot.formation_epoch != desired.transition_epoch
		|| !semantic_activation_ack_expected_image_current(
			&next, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_table_publish(&next))
		return false;

	memset(origin, 0, sizeof(*origin));
	origin->unsent_members_lo = UINT64_C(0x0e);
	origin->active = true;
	if (round_descriptor != NULL
		&& round_descriptor->feature_bit
		   == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) {
		result = round_descriptor->apply_target_closed(
			desired.record_generation);
		if (result == CLUSTER_SEMANTIC_ACTIVATION_OK)
			return semantic_activation_ack_lmon_finish_member_prepared(
				&next, result);
	}
	return semantic_activation_ack_lmon_send_origin_requests();
}

static bool
semantic_activation_ack_lmon_install_open(
	const SemanticActivationUtilityRequest *request)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	ClusterSemanticActivationRecord desired;
	ClusterSemanticR11CutoverSnapshot cutover;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	const ClusterSemanticActivationDescriptor *round_descriptor;
	ClusterSemanticActivationResult result;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 callback_generation;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;
	bool all_observed;
	int node;

	if (request == NULL
		|| !semantic_activation_ack_lmon_open_cas_active(request)
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL)
		|| desired.phase != CLUSTER_SEMANTIC_PHASE_OPEN
		|| !semantic_activation_round_descriptor(
			desired.source_feature_bitmap,
			desired.target_feature_bitmap,
			desired.rollback_feature_bitmap, &round_descriptor,
			&required_caps)
		|| (round_descriptor->feature_bit
			!= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			&& round_descriptor->feature_bit
			   != CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1)
		|| !semantic_activation_snapshot(&snapshot)
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| !semantic_activation_ack_table_snapshot(&before)
		|| before.coordinator_node != desired.coordinator_node
		|| before.round_nonce != request->request_seq
		|| before.expected_members_lo != desired.admitted_members_lo
		|| before.expected_members_hi != desired.admitted_members_hi
		|| before.transition_epoch != desired.transition_epoch
		|| before.source_feature_bitmap != desired.source_feature_bitmap
		|| before.target_feature_bitmap != desired.target_feature_bitmap
		|| before.rollback_feature_bitmap
		   != desired.rollback_feature_bitmap
		|| before.capability_sample_digest
		   != desired.capability_sample_digest)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps)
		return false;

	if (!snapshot.transition_closed
		&& snapshot.active_bits == desired.target_feature_bitmap
		&& snapshot.record_generation == desired.record_generation
		&& snapshot.formation_epoch == desired.transition_epoch) {
		if (before.stage
				!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			|| before.record_generation != desired.record_generation
			|| !semantic_activation_ack_complete_image_current(
				&before, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word)
			|| !semantic_activation_utility_mailbox_complete(
				request->request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK,
				0, desired.record_generation))
			return false;
		memset(origin, 0, sizeof(*origin));
		semantic_activation_lmon_prepare_cas_seq = 0;
		semantic_activation_lmon_prepare_cas_utility_request_seq = 0;
		semantic_activation_lmon_commit_cas_seq = 0;
		semantic_activation_lmon_commit_cas_utility_request_seq = 0;
		semantic_activation_lmon_open_cas_seq = 0;
		semantic_activation_lmon_open_cas_utility_request_seq = 0;
		return true;
	}

	if (!snapshot.transition_closed
		|| snapshot.active_bits != desired.source_feature_bitmap
		|| snapshot.record_generation == UINT64_MAX
		|| snapshot.record_generation + 1 != desired.record_generation
		|| snapshot.formation_epoch != desired.transition_epoch)
		return false;

	if (before.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		if (before.record_generation + 1 != desired.record_generation
			|| !semantic_activation_ack_complete_image_current(
				&before, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		if (!semantic_activation_record_cas_mailbox_poll_completion(
				semantic_activation_lmon_open_cas_seq, &result))
			return true;
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
			|| !semantic_activation_snapshot(&current_snapshot)
			|| current_snapshot.seq != snapshot.seq
			|| current_snapshot.active_bits != snapshot.active_bits
			|| current_snapshot.record_generation
			   != snapshot.record_generation
			|| current_snapshot.formation_epoch
			   != snapshot.formation_epoch
			|| current_snapshot.transition_closed
			   != snapshot.transition_closed
			|| !semantic_activation_ack_current_authority(
				cluster_node_id, &current_members_lo, &current_members_hi,
				&current_epoch, &current_coordinator_node)
			|| current_members_lo != desired.admitted_members_lo
			|| current_members_hi != desired.admitted_members_hi
			|| current_epoch != desired.transition_epoch
			|| current_coordinator_node
			   != (int32)desired.coordinator_node
			|| cluster_ic_local_capability_word()
			   != local_capability_word
			|| cluster_qvotec_get_self_incarnation()
			   != desired.coordinator_incarnation
			|| !semantic_activation_ack_table_snapshot(&after)
			|| memcmp(&before, &after, sizeof(before)) != 0
			|| !semantic_activation_ack_complete_image_current(
				&after, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;

		next = after;
		next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
		next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
		next.record_generation = desired.record_generation;
		next.observed_members_lo = UINT64_C(1);
		next.observed_members_hi = 0;
		memset(next.observed, 0, sizeof(next.observed));
		for (node = 0; node < 4; node++)
			next.expected[node].record_generation
				= desired.record_generation;
		next.observed[0] = next.expected[0];
		if (!semantic_activation_ack_expected_image_current(
				&next, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word)
			|| !semantic_activation_ack_table_publish(&next))
			return false;
		memset(origin, 0, sizeof(*origin));
		origin->unsent_members_lo = UINT64_C(0x0e);
		origin->active = true;
		return semantic_activation_ack_lmon_send_origin_requests();
	}

	if (before.stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| before.record_generation != desired.record_generation
		|| !origin->active
		|| !semantic_activation_ack_expected_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			cluster_node_id, local_capability_word)
		|| (before.observed_members_lo
			& ~before.expected_members_lo) != 0
		|| before.observed_members_hi != 0)
		return false;
	all_observed
		= before.observed_members_lo == before.expected_members_lo;
	if (before.flags
		!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			| (all_observed
				   ? CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE
				   : 0)))
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 4;
		bool observed
			= node < 64
				  ? (before.observed_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : false;

		if (!member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&before.expected[node],
					sizeof(before.expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&before.observed[node],
					sizeof(before.observed[node])))
				return false;
			continue;
		}
		if (observed) {
			if (!semantic_activation_ack_matches(
					&before.observed[node], &before.expected[node]))
				return false;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&before.observed[node],
				   sizeof(before.observed[node])))
			return false;
	}
	if (!all_observed)
		return semantic_activation_ack_lmon_send_origin_requests();
	if (!semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;

	callback_generation
		= round_descriptor->feature_bit
			  == CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1
			  ? request->expected_record_generation + 1
			  : desired.record_generation;
	result = round_descriptor->open_target_admission(callback_generation);
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK) {
		/* The R8 cutover producer runs after semantic activation in the LMON
		 * tick.  A complete, current R11 OPEN_APPLIED table therefore has one
		 * exact closed interval in which the durable OPEN exists but the same-T
		 * Resource-X proof has not been thawed locally yet.  Retain this utility
		 * round only for that fully revalidated shape; drift and every other
		 * callback refusal remain terminal and fail closed. */
		if (round_descriptor->feature_bit
			== CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1
			&& result == CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
			&& cluster_semantic_activation_r11_cutover_snapshot(&cutover)
			&& cutover.phase
			   == CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL
			&& cutover.record_generation == callback_generation
			&& cutover.formation_epoch == desired.transition_epoch
			&& semantic_activation_ack_table_snapshot(&after)
			&& memcmp(&before, &after, sizeof(before)) == 0)
			return true;
		return false;
	}
	if (!semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| current_coordinator_node != (int32)desired.coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;
	if (!semantic_activation_lmon_publish_gate(
			&current_snapshot, desired.target_feature_bitmap,
			desired.record_generation, desired.transition_epoch, false))
		return false;
	if (!semantic_activation_utility_mailbox_complete(
			request->request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, 0,
			desired.record_generation))
		return false;

	memset(origin, 0, sizeof(*origin));
	semantic_activation_lmon_prepare_cas_seq = 0;
	semantic_activation_lmon_prepare_cas_utility_request_seq = 0;
	semantic_activation_lmon_commit_cas_seq = 0;
	semantic_activation_lmon_commit_cas_utility_request_seq = 0;
	semantic_activation_lmon_open_cas_seq = 0;
	semantic_activation_lmon_open_cas_utility_request_seq = 0;
	return true;
}

/*
 * RF-ROOT P9 verification (verified implementation): coordinator-side bit22
 * cutover advance — stage 1/2.  Called once the majority COMMIT(P+1)
 * record is durable AND the PREPARED-stage all-member ACK is COMPLETE.
 * Executor of the root activation is the coordinator LMON (CF(X) has no
 * frozen executor; AD-023 §4 binds CF(S) only; follow-up precedent).
 * The round driver (step ④) staged the PREPARED file token + round sha +
 * round copy in the seam shmem after create_prepared.  On success the
 * coordinator activates the root (PREPARED -> ACTIVE) and publishes the
 * COMMIT_APPLIED stage: every member must re-verify the ACTIVE root
 * (bootstrap_validate_active_round, bound to the seam round sha) and ACK
 * BEFORE the majority OPEN(P+2) record is CASed.  The coordinator's latch
 * moves AFTER the OPEN CAS (bit22_open_applied_begin) — the durable
 * Target OPEN proof precedes the gate flip.
 */
static bool
semantic_activation_ack_lmon_bit22_commit_applied_begin(
	const ClusterSemanticActivationAckTableV1 *before,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	ClusterControlRootFileToken out_token;
	ClusterControlRootResult act_result;
	uint64 self_bit;
	int node;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| SemanticActivationBit22Seam == NULL
		|| pg_atomic_read_u32(&SemanticActivationBit22Seam->valid) == 0
		|| SemanticActivationBit22Seam->transition_epoch
		   != before->transition_epoch)
		return true;	/* seam not staged / round mismatch: retry later */
	act_result = cluster_control_root_activate_prepared(
		&SemanticActivationBit22Seam->file_token,
		SemanticActivationBit22Seam->round_sha,
		&SemanticActivationBit22Seam->round, &out_token);
	if (act_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& act_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		ereport(LOG,
				(errmsg("bit22 cutover: activate_prepared refused (result %d) "
						"— the round stays PREPARED (fail-closed)",
						(int) act_result)));
		return true;	/* fail-closed: the round stays PREPARED */
	}
	ereport(LOG,
			(errmsg("bit22 cutover: root activated ACTIVE (gen %llu) — "
					"publishing COMMIT_APPLIED",
					(unsigned long long) before->record_generation)));

	if (!semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			after.record_generation, &self)
		|| !semantic_activation_ack_matches(
			&after.expected[cluster_node_id], &self))
		return true;

	self_bit = UINT64_C(1) << cluster_node_id;
	next = after;
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.record_generation = after.record_generation + 1;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	memset(next.observed, 0, sizeof(next.observed));
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		if (semantic_activation_ack_member_present(
				after.expected_members_lo, after.expected_members_hi, node))
			next.expected[node].record_generation
				= after.record_generation + 1;
	/* RF-ROOT P9 verification / cold-formation: the coordinator's own
	 * COMMIT_APPLIED observation is its locally-verified ACTIVE root —
	 * mark it self-observed (its expected tuple, generation-bumped above)
	 * so observed can equal expected and the stage completes.  Same
	 * defect class as the BARRIER/PREPARED tables. */
	if (cluster_node_id < 64)
		next.observed_members_lo = UINT64_C(1) << cluster_node_id;
	else
		next.observed_members_hi = UINT64_C(1) << (cluster_node_id - 64);
	next.observed[cluster_node_id] = next.expected[cluster_node_id];
	if (!semantic_activation_ack_table_publish(&next))
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = next.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = next.transition_epoch;
	request.record_generation = next.record_generation;
	request.round_nonce = next.round_nonce;
	request.source_feature_bitmap = next.source_feature_bitmap;
	request.target_feature_bitmap = next.target_feature_bitmap;
	request.rollback_feature_bitmap = next.rollback_feature_bitmap;
	request.admitted_members_lo = next.expected_members_lo;
	request.admitted_members_hi = next.expected_members_hi;
	request.capability_sample_digest = next.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	/* RF-ROOT P9 verification (cold-formation): the COMMIT_APPLIED REQUEST
	 * must go out through the origin mechanism (send_bit22_prepared_requests)
	 * like the BARRIER/PREPARED requests — send_pending() is the ACK-only
	 * path and silently invalidated the REQUEST (kind check), so the member
	 * never saw this stage and the round stalled after root ACTIVE. */
	{
		SemanticActivationAckRequestOrigin *origin
			= &semantic_activation_ack_local_request_origin;

		memset(origin, 0, sizeof(*origin));
		origin->unsent_members_lo = next.expected_members_lo
			& ~(UINT64_C(1) << cluster_node_id);
		origin->active = true;
	}
	return semantic_activation_ack_lmon_send_bit22_prepared_requests(
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
}

/*
 * RF-ROOT P9 verification (verified implementation): coordinator-side bit22 cutover
 * advance — stage 2/2 (the latch + OPEN_APPLIED publication).  Called
 * once the majority OPEN(P+2) record is durable (the exact Target OPEN
 * proof).  #3 ordering (contract) is preserved: the coordinator's latch
 * flips (return checked) BEFORE its observed bit is published.
 */
static bool
semantic_activation_ack_lmon_bit22_open_applied_begin(
	const ClusterSemanticActivationAckTableV1 *before,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	uint64 self_bit;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
		return false;
	if (!semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			after.record_generation, &self)
		|| !semantic_activation_ack_matches(
			&after.expected[cluster_node_id], &self))
		return true;

	/* RF-ROOT P9 verification (contract / follow-up): the coordinator's latch
	 * MUST flip (and the return be checked) BEFORE its observed bit is
	 * published. */
	if (!cluster_r4_bit22_cutover_latch_apply(
			after.transition_epoch, after.record_generation)) {
		ereport(LOG,
				(errmsg("bit22 cutover: coordinator latch_apply refused "
						"(gen %llu) — OPEN_APPLIED not published",
						(unsigned long long) after.record_generation)));
		return true;
	}
	ereport(LOG,
			(errmsg("bit22 cutover: coordinator latch flipped — publishing "
					"OPEN_APPLIED (gen %llu)",
					(unsigned long long) after.record_generation)));

	self_bit = UINT64_C(1) << cluster_node_id;
	next = after;
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	if (!semantic_activation_ack_table_publish(&next))
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	/* RF-ROOT P9 verification (cold-formation): the OPEN_APPLIED REQUEST
	 * goes out through the origin mechanism like the earlier stages —
	 * send_pending() is ACK-only and silently dropped the REQUEST (the
	 * member would never latch). */
	{
		SemanticActivationAckRequestOrigin *origin
			= &semantic_activation_ack_local_request_origin;

		memset(origin, 0, sizeof(*origin));
		origin->unsent_members_lo = after.expected_members_lo
			& ~(UINT64_C(1) << cluster_node_id);
		origin->active = true;
	}
	return semantic_activation_ack_lmon_send_bit22_prepared_requests(
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
}

/*
 * semantic_activation_ack_lmon_bit22_advance -- RF-ROOT P9 verification
 *	(verified implementation): the coordinator-side driver of the bit22 cutover
 *	round, run from the LMON tick (the round is SQL-driven and has no
 *	utility request, so the R4 utility/install chain never runs for it).
 *
 *	PREPARED COMPLETE
 *	  -> submit majority COMMIT(P+1) (CAS) -> poll durable
 *	  -> activate root (PREPARED -> ACTIVE)
 *	  -> publish COMMIT_APPLIED stage (members verify the ACTIVE root)
 *	COMMIT_APPLIED COMPLETE
 *	  -> submit majority OPEN(P+2) (CAS, the durable Target OPEN proof)
 *	  -> poll durable
 *	  -> coordinator latch + publish OPEN_APPLIED stage
 *	OPEN_APPLIED (members latch + ACK; complete -> done, latch is the gate)
 *
 *	Fail-closed: any refused CAS / activate / latch leaves the stage
 *	unchanged and the tick retries (the driver's deadline bounds the
 *	stall).
 */
static bool
semantic_activation_ack_lmon_bit22_advance(void)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationRecord commit;
	ClusterSemanticActivationRecord desired;
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterSemanticActivationResult result;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 cas_seq;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (!semantic_activation_ack_table_snapshot(&table)
		|| (table.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| SemanticActivationBit22Seam == NULL
		|| cluster_node_id != (int32)table.coordinator_node
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != table.expected_members_lo
		|| current_members_hi != table.expected_members_hi
		|| current_epoch != table.transition_epoch
		|| current_coordinator_node != (int32)table.coordinator_node)
		return false;
	/* The seam is staged only after the BARRIER COMPLETE (create_prepared
	 * runs there); the PREPARED+ stages bind to it. */
	if (table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		&& (pg_atomic_read_u32(&SemanticActivationBit22Seam->valid) == 0
			|| table.transition_epoch
			   != SemanticActivationBit22Seam->transition_epoch))
		return false;
	/* RF-ROOT P9 verification: the BARRIER REQUEST is sent by the LMON —
	 * the ic msg-type gate restricts semantic-activation ACK sends to the
	 * LMON, and the request origin is per-process, so the LMON arms it on
	 * first sight of the BARRIER table (begin() runs in a SQL backend and
	 * cannot arm the LMON's origin).  Idempotent: unsent clears once. */
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER) {
		SemanticActivationAckRequestOrigin *origin
			= &semantic_activation_ack_local_request_origin;

		if (!origin->active) {
			memset(origin, 0, sizeof(*origin));
			origin->unsent_members_lo = table.expected_members_lo
				& ~(UINT64_C(1) << cluster_node_id);
			origin->active = true;
		}
		(void) semantic_activation_ack_lmon_send_bit22_prepared_requests(
			CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	}
	local_capability_word = cluster_ic_local_capability_word();
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER) {
		/* RF-ROOT P9 verification (implementation): all-member source-close BARRIER
		 * COMPLETE — every node's wal-state writers are frozen and every
		 * ACTIVE slot is provably quiesced.  NOW build the migration
		 * image (accepting frozen ACTIVE slots), create the PREPARED root,
		 * stage the seam and publish the PREPARED stage. */
		ClusterControlRootMigrationImage image;
		ClusterControlRootMigrationRoundV1 round;
		ClusterControlRootFileToken token;
		ClusterSemanticActivationAckTableV1 prepared;
		SemanticActivationAckTuple self;
		ClusterControlRootResult create_result;
		uint8 sha[PG_SHA256_DIGEST_LENGTH];
		int node;

		if ((table.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) == 0
			|| !semantic_activation_ack_complete_image_current(
				&table, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		/* RF-ROOT P9 verification (cold-formation): the BARRIER is COMPLETE —
		 * write the PREPARE record (generation 1) over the majority legacy-
		 * zero implicit-OPEN record FIRST.  The R4 chain commits PREPARE
		 * (expected gen 0 -> desired gen 1), then COMMIT (expected gen 1 ->
		 * desired gen 2), then OPEN (expected gen 2 -> desired gen 3); the
		 * bit22 advance skipped the PREPARE CAS, so the COMMIT CAS found the
		 * disk record still at generation 0 and refused (RECORD_CONFLICT).
		 * One-shot: the done flag latches the completed write. */
		if (!semantic_activation_lmon_bit22_prepare_cas_done) {
			ClusterSemanticActivationRecord desired;

			if (semantic_activation_lmon_bit22_prepare_cas_seq == 0) {
				memset(&desired, 0, sizeof(desired));
				desired.source_feature_bitmap = table.source_feature_bitmap;
				desired.target_feature_bitmap = table.target_feature_bitmap;
				desired.transition_epoch = table.transition_epoch;
				desired.record_generation = table.record_generation;
				desired.admitted_members_lo = table.expected_members_lo;
				desired.admitted_members_hi = table.expected_members_hi;
				desired.capability_sample_digest
					= table.capability_sample_digest;
				desired.rollback_feature_bitmap
					= table.rollback_feature_bitmap;
				desired.coordinator_incarnation
					= cluster_qvotec_get_self_incarnation();
				desired.coordinator_node = table.coordinator_node;
				desired.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
				if (!cluster_semantic_activation_record_encode(
						&desired, desired_bytes)
					|| !semantic_activation_record_cas_mailbox_submit(
						0, 0, desired_bytes,
						&semantic_activation_lmon_bit22_prepare_cas_seq)) {
					ereport(LOG,
							(errmsg("bit22 cutover: PREPARE(P) CAS submit "
									"refused (gen %llu)",
									(unsigned long long)
									table.record_generation)));
					return false;
				}
				ereport(LOG,
						(errmsg("bit22 cutover: PREPARE(P) CAS submitted "
								"(seq %llu) — minting the PREPARED root",
								(unsigned long long)
								semantic_activation_lmon_bit22_prepare_cas_seq)));
				return true;
			}
			if (!semantic_activation_record_cas_mailbox_poll_completion(
					semantic_activation_lmon_bit22_prepare_cas_seq, &result))
				return true;
			if (result != CLUSTER_SEMANTIC_ACTIVATION_OK) {
				ereport(LOG,
						(errmsg("bit22 cutover: PREPARE(P) CAS failed "
								"(result %d)",
								(int) result)));
				return false;
			}
			semantic_activation_lmon_bit22_prepare_cas_done = true;
			ereport(LOG,
					(errmsg("bit22 cutover: PREPARE(P) durable (gen %llu) — "
							"minting the PREPARED root",
							(unsigned long long) table.record_generation)));
		}
		memset(&round, 0, sizeof(round));
		memcpy(round.magic, "PCRM", 4);
		round.version = 1;
		round.bytes = sizeof(round);
		round.prepare_generation = table.record_generation;
		round.transition_epoch = table.transition_epoch;
		round.source_feature_bitmap = table.source_feature_bitmap;
		round.target_feature_bitmap = table.target_feature_bitmap;
		round.admitted_bitmap_low = table.expected_members_lo;
		round.admitted_bitmap_high = table.expected_members_hi;
		round.capability_sample_digest = table.capability_sample_digest;
		round.coordinator_incarnation
			= cluster_qvotec_get_self_incarnation();
		round.coordinator_node_id = (int32)table.coordinator_node;
		create_result = cluster_control_root_build_migration_image(
			&round, &image);
		if (create_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& create_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
			ereport(LOG,
					(errmsg("bit22 cutover: migration image build refused "
							"(result %d, round gen %llu)",
							(int) create_result,
							(unsigned long long) round.prepare_generation)));
			return false;
		}
		create_result = cluster_control_root_create_prepared(
			&image, &round, &token);
		if (create_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_control_root_round_sha256(&round, sha)
			|| !cluster_r4_bit22_cutover_seam_store(&token, sha, &round)) {
			ereport(LOG,
					(errmsg("bit22 cutover: create_prepared/seam refused "
							"(result %d, round gen %llu)",
							(int) create_result,
							(unsigned long long) round.prepare_generation)));
			return false;
		}
		ereport(LOG,
				(errmsg("bit22 cutover: PREPARED root minted (gen %llu, "
						"token seq %llu) — staging the PREPARED stage",
						(unsigned long long) round.prepare_generation,
						(unsigned long long) token.file_txn_seq)));
		/* RF-ROOT P9 verification (cold-formation): the all-member
		 * source-close BARRIER is COMPLETE — every node's wal-state
		 * writers are frozen — so publish the closed-source snapshot
		 * (transition_closed=1 at the round generation).  The member-side
		 * PREPARED/COMMIT_APPLIED/OPEN_APPLIED accepts require
		 * transition_closed and bind their generation to it.  Without
		 * this the bit22 round never left the BARRIER stage (the R4
		 * utility path was the only publish_gate caller). */
		if (!semantic_activation_snapshot(&snapshot)
			|| !semantic_activation_lmon_publish_gate(
				&snapshot, table.source_feature_bitmap,
				table.record_generation, table.transition_epoch, true))
			return false;
		memset(&prepared, 0, sizeof(prepared));
		prepared.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		prepared.coordinator_node = (uint32)cluster_node_id;
		prepared.round_nonce = token.file_txn_seq;
		prepared.transition_epoch = round.transition_epoch;
		prepared.record_generation = round.prepare_generation;
		prepared.expected_members_lo = round.admitted_bitmap_low;
		prepared.expected_members_hi = round.admitted_bitmap_high;
		prepared.source_feature_bitmap = round.source_feature_bitmap;
		prepared.target_feature_bitmap = round.target_feature_bitmap;
		prepared.rollback_feature_bitmap = 0;
		prepared.capability_sample_digest = round.capability_sample_digest;
		prepared.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			SemanticActivationAckTuple remote;
			uint32 peer_word;
			uint32 peer_gen;

			if (!semantic_activation_ack_member_present(
					current_members_lo, current_members_hi, node))
				continue;
			if (node == cluster_node_id) {
				if (!semantic_activation_ack_self_tuple(
						node, local_capability_word,
						round.transition_epoch,
						round.prepare_generation,
						&prepared.expected[node]))
					return false;
				continue;
			}
			memset(&remote, 0, sizeof(remote));
			remote.node_id = (uint32)node;
			remote.boot_id
				= cluster_membership_get_last_admitted_incarnation(node);
			remote.admitted_incarnation = remote.boot_id;
			if (remote.boot_id == 0
				|| !cluster_sf_peer_capability_word_sample(
					node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
					&peer_word, &peer_gen)
				|| peer_gen == 0)
				return false;
			remote.control_connection_generation = (uint64)peer_gen;
			remote.capability_word = peer_word;
			remote.capability_generation = (uint64)peer_gen;
			remote.transition_epoch = round.transition_epoch;
			remote.record_generation = round.prepare_generation;
			prepared.expected[node] = remote;
		}
		if (!semantic_activation_ack_self_tuple(
				cluster_node_id, local_capability_word,
				round.transition_epoch, round.prepare_generation, &self)
			|| !semantic_activation_ack_matches(
				&prepared.expected[cluster_node_id], &self))
			return false;
		/* RF-ROOT P9 verification / cold-formation: mark the coordinator
		 * self-observed in the PREPARED stage too — the coordinator's own
		 * CLOSED-ACK binding is its local self tuple (it activated nothing
		 * yet; the PREPARED stage is the W6 clause-3 all-member ACK gate).
		 * Without this observed can never equal expected and the stage
		 * never completes (same defect class as the BARRIER table). */
		if (cluster_node_id < 64)
			prepared.observed_members_lo = UINT64_C(1) << cluster_node_id;
		else
			prepared.observed_members_hi = UINT64_C(1) << (cluster_node_id - 64);
		prepared.observed[cluster_node_id] = prepared.expected[cluster_node_id];
		if (!semantic_activation_ack_table_publish(&prepared))
			return false;
		{
			SemanticActivationAckRequestOrigin *origin
				= &semantic_activation_ack_local_request_origin;

			memset(origin, 0, sizeof(*origin));
			origin->unsent_members_lo = prepared.expected_members_lo
				& ~(UINT64_C(1) << cluster_node_id);
			origin->active = true;
		}
		return semantic_activation_ack_lmon_send_bit22_prepared_requests(
			CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	}
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED) {
		if ((table.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) == 0)
			return false;
		if (!semantic_activation_snapshot(&snapshot)
			|| !snapshot.transition_closed
			|| snapshot.active_bits != table.source_feature_bitmap
			|| snapshot.record_generation != table.record_generation
			|| snapshot.formation_epoch != table.transition_epoch
			|| !semantic_activation_ack_complete_image_current(
				&table, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		if (semantic_activation_lmon_commit_cas_seq == 0) {
			memset(&desired, 0, sizeof(desired));
			desired.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
			desired.record_generation = table.record_generation + 1;
			desired.source_feature_bitmap = table.source_feature_bitmap;
			desired.target_feature_bitmap = table.target_feature_bitmap;
			desired.rollback_feature_bitmap = table.rollback_feature_bitmap;
			desired.admitted_members_lo = table.expected_members_lo;
			desired.admitted_members_hi = table.expected_members_hi;
			desired.transition_epoch = table.transition_epoch;
			desired.capability_sample_digest
				= table.capability_sample_digest;
			desired.coordinator_node = table.coordinator_node;
			desired.coordinator_incarnation
				= cluster_qvotec_get_self_incarnation();
			if (!cluster_semantic_activation_record_encode(
					&desired, desired_bytes)
				|| !semantic_activation_record_cas_mailbox_submit(
					table.record_generation,
					table.source_feature_bitmap, desired_bytes,
					&cas_seq)) {
				ClusterSemanticActivationAckTableV1 dbg;

				ereport(LOG,
						(errmsg("bit22 cutover: COMMIT(P+1) CAS submit refused "
								"(gen %llu, table stage=%u flags=0x%x)",
								(unsigned long long) table.record_generation,
								(unsigned) table.stage,
								(unsigned) table.flags)));
				if (semantic_activation_ack_table_snapshot(&dbg))
					ereport(LOG,
							(errmsg("bit22 cutover: COMMIT(P+1) CAS submit "
									"refused — live table stage=%u flags=0x%x "
									"gen=%llu observed=%llx/%llx expected=%llx/%llx",
									(unsigned) dbg.stage, (unsigned) dbg.flags,
									(unsigned long long) dbg.record_generation,
									(unsigned long long) dbg.observed_members_lo,
									(unsigned long long) dbg.observed_members_hi,
									(unsigned long long) dbg.expected_members_lo,
									(unsigned long long) dbg.expected_members_hi)));
				return false;
			}
			semantic_activation_lmon_commit_cas_seq = cas_seq;
			ereport(LOG,
					(errmsg("bit22 cutover: COMMIT(P+1) CAS submitted (seq %llu) "
							"— awaiting durable record",
							(unsigned long long) cas_seq)));
			return true;
		}
		if (!semantic_activation_record_cas_mailbox_poll_completion(
				semantic_activation_lmon_commit_cas_seq, &result))
			return true;
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK) {
			ereport(LOG,
					(errmsg("bit22 cutover: COMMIT(P+1) CAS failed (result %d)",
							(int) result)));
			return false;
		}
		ereport(LOG,
				(errmsg("bit22 cutover: COMMIT(P+1) durable — activating the "
						"PREPARED root (gen %llu)",
						(unsigned long long) table.record_generation)));
		return semantic_activation_ack_lmon_bit22_commit_applied_begin(
			&table, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	}
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		if ((table.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) == 0)
			return false;
		if (!semantic_activation_ack_complete_image_current(
				&table, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		if (semantic_activation_lmon_open_cas_seq == 0) {
			if (SemanticActivationShmem == NULL
				|| !cluster_semantic_activation_record_decode(
					SemanticActivationShmem->record_cas_desired_bytes,
					&commit, NULL)
				|| commit.phase != CLUSTER_SEMANTIC_PHASE_COMMIT
				|| commit.record_generation != table.record_generation)
				return false;
			desired = commit;
			desired.record_generation = commit.record_generation + 1;
			desired.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
			if (!cluster_semantic_activation_record_encode(
					&desired, desired_bytes)
				|| !semantic_activation_record_cas_mailbox_submit(
					commit.record_generation,
					commit.source_feature_bitmap, desired_bytes,
					&cas_seq)) {
				ereport(LOG,
						(errmsg("bit22 cutover: OPEN(P+2) CAS submit refused "
								"(gen %llu)",
								(unsigned long long) table.record_generation)));
				return false;
			}
			semantic_activation_lmon_open_cas_seq = cas_seq;
			ereport(LOG,
					(errmsg("bit22 cutover: OPEN(P+2) CAS submitted (seq %llu) "
							"— awaiting durable Target OPEN proof",
							(unsigned long long) cas_seq)));
			return true;
		}
		if (!semantic_activation_record_cas_mailbox_poll_completion(
				semantic_activation_lmon_open_cas_seq, &result))
			return true;
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK) {
			ereport(LOG,
					(errmsg("bit22 cutover: OPEN(P+2) CAS failed (result %d)",
							(int) result)));
			return false;
		}
		ereport(LOG,
				(errmsg("bit22 cutover: OPEN(P+2) durable — Target OPEN proof "
						"holds; flipping the coordinator latch")));
		return semantic_activation_ack_lmon_bit22_open_applied_begin(
			&table, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node,
			local_capability_word);
	}
	return false;
}

static bool
semantic_activation_ack_lmon_install_prepare(
	const SemanticActivationUtilityRequest *request)
{
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationRecord desired;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	ClusterSemanticActivationResult result;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 digest;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (request == NULL || !semantic_activation_snapshot(&snapshot))
		return false;
	if (semantic_activation_lmon_open_cas_seq != 0)
		return semantic_activation_ack_lmon_install_open(request);
	if (semantic_activation_lmon_commit_cas_seq != 0)
		return semantic_activation_ack_lmon_install_commit(request);
	if (!semantic_activation_ack_lmon_prepare_cas_active(
			request, &snapshot)
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL))
		return false;

	/* The successful projection is idempotent while the utility owner remains
	 * pending.  In particular, do not rerun the generation-g preflight after
	 * installing the durable generation-(g+1) PREPARE image. */
	if (snapshot.transition_closed
		&& snapshot.active_bits == desired.source_feature_bitmap
		&& snapshot.record_generation == desired.record_generation
		&& snapshot.formation_epoch == desired.transition_epoch)
		return semantic_activation_ack_lmon_begin_barrier_round(
			request, &desired);
	if (snapshot.transition_closed
		|| snapshot.active_bits != request->source_feature_bitmap
		|| snapshot.record_generation
		   != request->expected_record_generation
		|| snapshot.formation_epoch != desired.transition_epoch)
		return false;

	if (!semantic_activation_record_cas_mailbox_poll_completion(
			semantic_activation_lmon_prepare_cas_seq, &result))
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| !semantic_activation_ack_table_snapshot(&before)
		|| before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| before.coordinator_node != desired.coordinator_node
		|| before.round_nonce != request->request_seq
		|| before.transition_epoch != desired.transition_epoch
		|| before.record_generation != desired.record_generation
		|| before.source_feature_bitmap != desired.source_feature_bitmap
		|| before.target_feature_bitmap != desired.target_feature_bitmap
		|| before.rollback_feature_bitmap != desired.rollback_feature_bitmap)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_sample_digest(&before, &digest)
		|| digest != desired.capability_sample_digest)
		return false;

	if (!semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| current_coordinator_node != (int32)desired.coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;

	return semantic_activation_lmon_publish_gate(
		&current_snapshot, desired.source_feature_bitmap,
		desired.record_generation, desired.transition_epoch, true);
}

static bool
semantic_activation_ack_lmon_submit_prepare(
	const SemanticActivationUtilityRequest *request,
	const SemanticActivationAdmissionSnapshot *snapshot)
{
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationRecord desired;
	SemanticActivationAdmissionSnapshot current_snapshot;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 digest;
	uint64 cas_seq;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;

	if (semantic_activation_lmon_prepare_cas_seq != 0)
		return semantic_activation_ack_lmon_prepare_cas_active(
			request, snapshot);
	if (request == NULL || snapshot == NULL
		|| request->request_seq == 0
		|| request->expected_record_generation == UINT64_MAX
		|| snapshot->transition_closed
		|| snapshot->active_bits != request->source_feature_bitmap
		|| snapshot->record_generation
		   != request->expected_record_generation
		|| !semantic_activation_ack_round_required_caps(
			request->source_feature_bitmap,
			request->target_feature_bitmap,
			request->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| snapshot->formation_epoch != current_epoch
		|| !semantic_activation_ack_table_snapshot(&before)
		|| before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| before.coordinator_node != UINT32_C(0)
		|| before.round_nonce != request->request_seq
		|| before.transition_epoch != current_epoch
		|| before.record_generation
		   != request->expected_record_generation + 1
		|| before.source_feature_bitmap
		   != request->source_feature_bitmap
		|| before.target_feature_bitmap
		   != request->target_feature_bitmap
		|| before.rollback_feature_bitmap
		   != request->rollback_feature_bitmap)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_sample_digest(&before, &digest)
		|| digest == 0)
		return false;

	if (!semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot->seq
		|| current_snapshot.active_bits != snapshot->active_bits
		|| current_snapshot.record_generation != snapshot->record_generation
		|| current_snapshot.formation_epoch != snapshot->formation_epoch
		|| current_snapshot.transition_closed != snapshot->transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_epoch != snapshot->formation_epoch
		|| current_coordinator_node != 0
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;

	memset(&desired, 0, sizeof(desired));
	desired.source_feature_bitmap = request->source_feature_bitmap;
	desired.target_feature_bitmap = request->target_feature_bitmap;
	desired.transition_epoch = current_epoch;
	desired.record_generation = request->expected_record_generation + 1;
	desired.admitted_members_lo = current_members_lo;
	desired.admitted_members_hi = current_members_hi;
	desired.capability_sample_digest = digest;
	desired.rollback_feature_bitmap = request->rollback_feature_bitmap;
	desired.coordinator_incarnation
		= cluster_qvotec_get_self_incarnation();
	desired.coordinator_node = UINT32_C(0);
	desired.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	if (desired.coordinator_incarnation == 0
		|| !cluster_semantic_activation_record_encode(
			&desired, desired_bytes)
		|| !semantic_activation_record_cas_mailbox_submit(
			request->expected_record_generation,
			request->source_feature_bitmap, desired_bytes, &cas_seq))
		return false;

	semantic_activation_lmon_prepare_cas_seq = cas_seq;
	semantic_activation_lmon_prepare_cas_utility_request_seq
		= request->request_seq;
	return true;
}

static bool
semantic_activation_ack_lmon_submit_commit(
	const SemanticActivationUtilityRequest *request,
	const ClusterSemanticActivationRecord *prepare)
{
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationRecord desired;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 cas_seq;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (semantic_activation_lmon_commit_cas_seq != 0)
		return request != NULL
			   && semantic_activation_snapshot(&snapshot)
			   && semantic_activation_ack_lmon_commit_cas_active(
				   request, &snapshot);
	if (request == NULL || prepare == NULL
		|| request->expected_record_generation > UINT64_MAX - 2
		|| prepare->phase != CLUSTER_SEMANTIC_PHASE_PREPARE
		|| prepare->record_generation
		   != request->expected_record_generation + 1
		|| prepare->source_feature_bitmap
		   != request->source_feature_bitmap
		|| prepare->target_feature_bitmap
		   != request->target_feature_bitmap
		|| prepare->rollback_feature_bitmap
		   != request->rollback_feature_bitmap
		|| prepare->capability_sample_digest == 0
		|| !semantic_activation_snapshot(&snapshot)
		|| !snapshot.transition_closed
		|| snapshot.active_bits != prepare->source_feature_bitmap
		|| snapshot.record_generation != prepare->record_generation
		|| snapshot.formation_epoch != prepare->transition_epoch
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != prepare->admitted_members_lo
		|| current_members_hi != prepare->admitted_members_hi
		|| current_epoch != prepare->transition_epoch
		|| !semantic_activation_ack_table_snapshot(&before)
		|| before.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| before.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| before.coordinator_node != prepare->coordinator_node
		|| before.round_nonce != request->request_seq
		|| before.expected_members_lo != prepare->admitted_members_lo
		|| before.expected_members_hi != prepare->admitted_members_hi
		|| before.transition_epoch != prepare->transition_epoch
		|| before.record_generation != prepare->record_generation
		|| before.source_feature_bitmap
		   != prepare->source_feature_bitmap
		|| before.target_feature_bitmap
		   != prepare->target_feature_bitmap
		|| before.rollback_feature_bitmap
		   != prepare->rollback_feature_bitmap
		|| before.capability_sample_digest
		   != prepare->capability_sample_digest)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;

	desired = *prepare;
	desired.record_generation = prepare->record_generation + 1;
	desired.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
	if (!cluster_semantic_activation_record_encode(
			&desired, desired_bytes)
		|| !semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != desired.admitted_members_lo
		|| current_members_hi != desired.admitted_members_hi
		|| current_epoch != desired.transition_epoch
		|| current_coordinator_node != (int32)desired.coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| cluster_qvotec_get_self_incarnation()
		   != desired.coordinator_incarnation
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_record_cas_mailbox_submit(
			prepare->record_generation,
			prepare->source_feature_bitmap, desired_bytes, &cas_seq))
		return false;

	semantic_activation_lmon_commit_cas_seq = cas_seq;
	semantic_activation_lmon_commit_cas_utility_request_seq
		= request->request_seq;
	return true;
}

/*
 * Match the current replacement episode to a decoded JCMK-v3 image already
 * selected by the strict-majority reader.  The ADMITTED marker, rather than
 * the overwritten instantaneous phase-3 snapshot, is the historical
 * completion basis for PGSA PREPARE.  This pure match neither proves that the
 * caller ran the majority selector nor grants current block0 authority or a
 * later PGSA ACK.
 */
static bool semantic_activation_r4_prepare_basis_matches(
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode) pg_attribute_unused();

static bool
semantic_activation_r4_prepare_basis_matches(
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode)
{
	if (majority_admitted == NULL || episode == NULL)
		return false;
	if (majority_admitted->magic != CLUSTER_JCMK_MAGIC
		|| majority_admitted->version != CLUSTER_JCMK_REPLACEMENT_VERSION
		|| majority_admitted->target_node_id < 0
		|| majority_admitted->target_node_id >= CLUSTER_MAX_NODES
		|| majority_admitted->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED
		|| majority_admitted->reserved0[0] != 0 || majority_admitted->reserved0[1] != 0
		|| majority_admitted->reserved0[2] != 0
		|| majority_admitted->ready_state_generation == 0
		|| majority_admitted->old_admitted_incarnation == 0
		|| majority_admitted->fresh_incarnation
			   <= majority_admitted->old_admitted_incarnation
		|| majority_admitted->request_nonce == 0
		|| majority_admitted->baseline_epoch == UINT64_MAX
		|| majority_admitted->reserved_or_committed_epoch
			   != majority_admitted->baseline_epoch + 1
		|| majority_admitted->grammar_fingerprint == 0)
		return false;
	if (!cluster_replacement_episode_is_valid(episode)
		|| episode->phase != CLUSTER_REPLACEMENT_EPISODE_ADMITTED
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK)
		return false;

	return episode->target_node_id == majority_admitted->target_node_id
		   && episode->state_generation
			  == majority_admitted->ready_state_generation
		   && episode->request_nonce == majority_admitted->request_nonce
		   && episode->old_admitted_incarnation
			  == majority_admitted->old_admitted_incarnation
		   && episode->fresh_incarnation == majority_admitted->fresh_incarnation
		   && episode->baseline_epoch == majority_admitted->baseline_epoch
		   && episode->reserved_or_committed_epoch
			  == majority_admitted->reserved_or_committed_epoch
		   && memcmp(episode->expected_survivors,
					 majority_admitted->expected_purge_survivors,
					 sizeof(episode->expected_survivors)) == 0
		   && episode->grammar_fingerprint == majority_admitted->grammar_fingerprint;
}

/*
 * Consume the current-coordinator happy-path handoff without manufacturing a
 * durable majority result.  The reconfiguration owner publishes outputs only
 * after co-sampling its ACKed ADMITTED marker, episode, epoch and MEMBER set;
 * this adapter then rechecks their exact D13 lineage.  Formation takeover and
 * the remaining D13 predicates stay fail-closed elsewhere.
 */
static bool
semantic_activation_r4_current_admitted_basis(void)
{
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 marker;

	if (!cluster_reconfig_lmon_snapshot_replacement_admitted(&episode, &marker))
		return false;
	return semantic_activation_r4_prepare_basis_matches(&marker, &episode);
}

static bool
semantic_activation_initial_clean_snapshot_equal(
	const ClusterInitialCleanFormationSnapshot *left,
	const ClusterInitialCleanFormationSnapshot *right)
{
	return left != NULL && right != NULL
		   && memcmp(left, right, sizeof(*left)) == 0;
}

static bool
semantic_activation_initial_clean_pgrd_mirror(
	uint8 out[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 snapshot[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 snapshot_after[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 mirror[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	char root_directory[MAXPGPATH];

	if (out == NULL
		|| !semantic_activation_pgrd_snapshot_copy(snapshot)
		|| cluster_undo_root_descriptor_decode(
			snapshot, GetSystemIdentifier(), &descriptor)
		   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
		|| descriptor.owner_node != -1
		|| !semantic_activation_lmon_shared_pgrd_root_directory(
			root_directory)
		|| cluster_undo_smgr_root_descriptor_read_candidate(
			root_directory, mirror)
		   != CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT
		|| memcmp(snapshot, mirror, sizeof(snapshot)) != 0
		|| !semantic_activation_pgrd_snapshot_copy(snapshot_after)
		|| memcmp(snapshot, snapshot_after, sizeof(snapshot)) != 0)
		return false;
	memcpy(out, snapshot, sizeof(snapshot));
	return true;
}

/* Pre-SAMPLE only.  The exact clean identity and PGRD/mirror permit LMON to
 * ask all four current members for ACKs; they do not authorize RECORD_CAS. */
static bool
semantic_activation_r4_initial_clean_sample_basis(
	uint64 expected_generation, bool coordinator_only)
{
	ClusterInitialCleanFormationSnapshot clean_before;
	ClusterInitialCleanFormationSnapshot clean_after;
	SemanticActivationAdmissionSnapshot gate;
	uint8 pgrd_before[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 pgrd_after[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	uint32 capability_word;
	uint32 capability_generation;
	uint32 local_capability_word;
	int32 coordinator_node;
	int node;

	if (expected_generation != 0
		|| cluster_grd_recovery_state_value() != GRD_RECOVERY_IDLE
		|| !cluster_reconfig_snapshot_initial_clean_formation(
			&clean_before)
		|| (clean_before.formation_marker_generation == 0
			&& clean_before.formation_epoch != CLUSTER_EPOCH_INITIAL)
		|| clean_before.members_lo != UINT64_C(0x0f)
		|| clean_before.members_hi != 0
		|| clean_before.formation_epoch != cluster_epoch_get_current()
		|| !semantic_activation_initial_clean_pgrd_mirror(pgrd_before)
		|| !semantic_activation_snapshot(&gate)
		|| gate.active_bits != 0 || gate.record_generation != 0
		|| gate.transition_closed
		|| gate.formation_epoch != clean_before.formation_epoch
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &members_lo, &members_hi, &formation_epoch,
			&coordinator_node)
		|| members_lo != clean_before.members_lo
		|| members_hi != clean_before.members_hi
		|| formation_epoch != clean_before.formation_epoch
		|| coordinator_node != 0
		|| (coordinator_only && cluster_node_id != coordinator_node))
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word
		 & CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		!= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		return false;
	for (node = 0; node < 4; node++) {
		if (cluster_membership_get_state(node) != CLUSTER_MEMBER_MEMBER
			|| cluster_membership_get_last_admitted_incarnation(node)
			   != clean_before.admitted_incarnation[node])
			return false;
		if (node == cluster_node_id) {
			if (cluster_qvotec_get_self_incarnation()
				!= clean_before.admitted_incarnation[node])
				return false;
			continue;
		}
		if (!cluster_sf_peer_capability_word_sample(
				node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
				&capability_word, &capability_generation)
			|| capability_generation == 0
			|| (capability_word
				& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
			return false;
	}

	return cluster_grd_recovery_state_value() == GRD_RECOVERY_IDLE
		   && cluster_reconfig_snapshot_initial_clean_formation(
			   &clean_after)
		   && semantic_activation_initial_clean_snapshot_equal(
			   &clean_before, &clean_after)
		   && semantic_activation_initial_clean_pgrd_mirror(pgrd_after)
		   && memcmp(pgrd_before, pgrd_after, sizeof(pgrd_before)) == 0
		   && cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == clean_before.formation_epoch;
}

static bool
semantic_activation_r4_initial_clean_sample_candidate(
	uint64 expected_generation)
{
	return semantic_activation_r4_initial_clean_sample_basis(
		expected_generation, true);
}

/*
 * Validate the read-only RECOVER_HEAD result used to repeat the mandatory
 * accepted-invalidator scan after ADMITTED.  CHOSEN means QVOTEC found no
 * strict-majority accepted next value; ADOPTED_OTHER is an invalidator and is
 * never reclassified as the old episode.  This helper starts no mailbox I/O
 * and grants no PGSA or admission authority.
 */
static bool semantic_activation_r4_invalidator_rescan_matches(
	const ClusterQvotecMailboxCompletion *completion,
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode) pg_attribute_unused();

static bool
semantic_activation_r4_invalidator_rescan_matches(
	const ClusterQvotecMailboxCompletion *completion,
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode)
{
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	uint8 expected_subject[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES] = { 0 };

	if (completion == NULL
		|| !semantic_activation_r4_prepare_basis_matches(
			majority_admitted, episode)
		|| completion->request_seq == 0
		|| (completion->request_seq & UINT64_C(1)) != 0
		|| completion->result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
		|| completion->actor_phase != CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B
		|| completion->observed_disk_bitmap == 0 || completion->detail != 0
		|| !cluster_epoch_authority_value_decode(
			completion->completion_value,
			CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &head)
		|| !cluster_epoch_ballot_id_decode(
			completion->completion_ballot, &ballot))
		return false;
	(void)ballot;

	expected_subject[episode->target_node_id / 8]
		= (uint8)(1u << (episode->target_node_id % 8));
	return head.value_version == CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION
		   && head.transition == CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED
		   && head.event_kind == CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		   && head.request_origin_node == episode->target_node_id
		   && head.target_node_id == episode->target_node_id
		   && head.authority_generation != 0
		   && head.authority_generation != UINT64_MAX
		   && head.baseline_epoch == episode->baseline_epoch
		   && head.reserved_epoch == episode->reserved_or_committed_epoch
		   && head.old_incarnation == episode->old_admitted_incarnation
		   && head.fresh_incarnation == episode->fresh_incarnation
		   && head.request_nonce == episode->request_nonce
		   && memcmp(head.authority_member_bitmap,
					 episode->expected_survivors,
					 sizeof(head.authority_member_bitmap)) == 0
		   && memcmp(head.event_subject_bitmap, expected_subject,
					 sizeof(head.event_subject_bitmap)) == 0
		   && head.grammar_fingerprint == episode->grammar_fingerprint;
}

static ClusterSemanticAdmissionResult
semantic_activation_admission_policy(uint64 feature_bit, uint64 active_bits, bool transition_closed,
									 ClusterSemanticAdmissionSide side, uint64 expected_generation,
									 uint64 current_generation)
{
	bool active;

	if (feature_bit == 0
		|| (side != CLUSTER_SEMANTIC_SOURCE_SIDE && side != CLUSTER_SEMANTIC_TARGET_SIDE))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (expected_generation != current_generation)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	if (transition_closed)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	active = (active_bits & feature_bit) != 0;
	if (side == CLUSTER_SEMANTIC_SOURCE_SIDE)
		return active ? CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT : CLUSTER_SEMANTIC_ADMISSION_OK;
	return active ? CLUSTER_SEMANTIC_ADMISSION_OK : CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
}

static ClusterSemanticAdmissionResult
semantic_activation_r4_terminal_census_policy(
	uint64 feature_bit, bool transition_closed,
	ClusterSemanticAdmissionSide side, uint64 expected_generation,
	uint64 current_generation)
{
	if (feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| side != CLUSTER_SEMANTIC_TARGET_SIDE)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (expected_generation != current_generation)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	if (transition_closed)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

static bool
semantic_activation_modifier_policy(uint64 active_bits, uint64 record_generation,
									bool transition_closed)
{
	const uint64 supported_active_bits
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;

	if ((active_bits & ~supported_active_bits) != 0
		|| ((active_bits
			 & CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) != 0
			&& (active_bits & CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) == 0))
		return false;

	/* No durable semantic record means that no modifier side is established. */
	if (record_generation == 0)
		return false;
	if (transition_closed)
		return false;

	/* Both ordinary SOURCE and uniformly-open R4 TARGET admit modifiers. */
	return true;
}

static ClusterSemanticAdmissionResult
semantic_activation_modifier_enter_bootstrap(bool writable_admission,
										 ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot before;
	SemanticActivationAdmissionSnapshot after;
	uint64 epoch_before;
	uint64 epoch_after;
	bool incremented = false;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (!writable_admission || token == NULL || SemanticActivationShmem == NULL
		|| !semantic_activation_ensure_exit_hook())
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	epoch_before = cluster_epoch_get_current();
	if (!semantic_activation_snapshot(&before) || before.formation_epoch != epoch_before
		|| before.record_generation != 0 || before.active_bits != 0)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	HOLD_INTERRUPTS();
	if (semantic_activation_local_inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0] != UINT32_MAX
		&& semantic_activation_counter_increment(
			&SemanticActivationShmem->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0])) {
		semantic_activation_local_inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]++;
		incremented = true;
	}
	pg_write_barrier();
	RESUME_INTERRUPTS();
	if (!incremented)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	if (!semantic_activation_snapshot(&after))
		goto fail;
	epoch_after = cluster_epoch_get_current();
	if (before.seq != after.seq || after.record_generation != 0 || after.active_bits != 0
		|| before.formation_epoch != after.formation_epoch || epoch_before != epoch_after
		|| after.formation_epoch != epoch_after)
		goto fail;

	token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token->record_generation = 0;
	token->formation_epoch = before.formation_epoch;
	token->side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;

fail:
	HOLD_INTERRUPTS();
	semantic_activation_release_debt(CLUSTER_SEMANTIC_SOURCE_SIDE, 0);
	RESUME_INTERRUPTS();
	return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
}

static ClusterSemanticActivationResult
semantic_activation_preflight(ClusterSemanticActivationAction action, uint64 expected_generation,
							  ClusterSemanticActivationRefusal *refusal, uint32 *effects)
{
	ClusterSemanticActivationResult result;

	if (effects != NULL)
		*effects = SEMANTIC_ACTIVATION_EFFECT_NONE;
	if (action < CLUSTER_SEMANTIC_ENABLE_ALL || action > CLUSTER_SEMANTIC_ROLLBACK_ABORT) {
		if (refusal != NULL) {
			refusal->result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
			refusal->feature_bit = 0;
			refusal->expected_generation = expected_generation;
		}
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	result = r4_pre_prepare_readiness(expected_generation, refusal);
	return result;
}

/*
 * The M4 terminal-census exception may establish only its already-frozen
 * PGRD authority while the later RF readiness conjunction is still deferred.
 * This does not admit TARGET or advance the PGSA FSM: the utility completes
 * with the original RF_DEFERRED result after QVOTEC majority plus exact
 * mirror publication.  Every other refusal remains mutation-free.
 */
static bool
semantic_activation_preopen_pgrd_setup_allowed(
	ClusterSemanticActivationAction action,
	ClusterSemanticActivationResult preflight_result)
{
	return action == CLUSTER_SEMANTIC_ENABLE_ALL
		   && (preflight_result == CLUSTER_SEMANTIC_ACTIVATION_OK
			   || preflight_result == CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

static bool
semantic_activation_control_wait_allowed(SemanticActivationWaitEdge edge, uint32 held_locks)
{
	return edge >= SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON
		   && edge <= SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER
		   && held_locks == SEMANTIC_ACTIVATION_HELD_NONE;
}

static bool
semantic_activation_actor_edge_allowed(SemanticActivationActor from, SemanticActivationActor to)
{
	return (from == SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY
			&& to == SEMANTIC_ACTIVATION_ACTOR_LMON)
		   || (from == SEMANTIC_ACTIVATION_ACTOR_LMON && to == SEMANTIC_ACTIVATION_ACTOR_QVOTEC);
}

/*
 * Keep durable and local effects on their frozen owners.  In particular,
 * ProcessUtility can only publish a request, formation LMON owns coordination,
 * and QVOTEC alone owns the PGSA voting-disk write.
 */
static bool semantic_activation_actor_effect_allowed(SemanticActivationActor actor,
											  SemanticActivationEffect effect)
	pg_attribute_unused();

static bool
semantic_activation_actor_effect_allowed(SemanticActivationActor actor,
									  SemanticActivationEffect effect)
{
	switch (actor) {
	case SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY:
		return effect == SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION;
	case SEMANTIC_ACTIVATION_ACTOR_LMON:
		return effect == SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE
			   || effect == SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN
			   || effect == SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION
			   || effect == SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE;
	case SEMANTIC_ACTIVATION_ACTOR_QVOTEC:
		return effect == SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE;
	case SEMANTIC_ACTIVATION_ACTOR_LMS:
	case SEMANTIC_ACTIVATION_ACTOR_DATA:
	default:
		return false;
	}
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal)
{
	bool admitted_basis = semantic_activation_r4_current_admitted_basis();
	bool initial_clean_basis
		= semantic_activation_r4_initial_clean_sample_candidate(
			expected_generation);
	ClusterSemanticActivationResult result
		= (cluster_ctrc_shmem_ready()
		   && (admitted_basis || initial_clean_basis))
			  ? CLUSTER_SEMANTIC_ACTIVATION_OK
			  : CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;

	/* The instantaneous READY snapshot belongs exclusively to target LMON's
	 * phase-3 serializer.  The current-coordinator handoff becomes visible
	 * here only after the majority ADMITTED write and its post-write terminal-
	 * head rescan have both completed; this callback grants entry to PREPARE,
	 * not a PREPARED ACK, COMMIT ACK, target OPEN, or current write authority. */
	if (refusal != NULL) {
		refusal->result = result;
		refusal->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		refusal->expected_generation = expected_generation;
	}
	return result;
}

static ClusterSemanticActivationResult
r4_stage_fail_closed(uint64 generation)
{
	(void)generation;
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_close_source_admission(uint64 generation)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint32 debt;

	if (!semantic_activation_snapshot(&snapshot)
		|| generation == 0 || snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	if (!snapshot.transition_closed
		&& !semantic_activation_lmon_publish_gate(
			&snapshot, snapshot.active_bits, snapshot.record_generation,
			snapshot.formation_epoch, true))
		return CLUSTER_SEMANTIC_ACTIVATION_MEMBERSHIP_CHANGED;

	pg_read_barrier();
	debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]);
	return debt == 0 ? CLUSTER_SEMANTIC_ACTIVATION_OK
					 : CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;
}

static ClusterSemanticActivationResult
r4_source_logical_debt_zero(uint64 generation, ClusterSemanticZeroProof *proof)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint32 source_debt;

	if (proof == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	memset(proof, 0, sizeof(*proof));

	if (generation == 0 || !semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	source_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]);
	if (source_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	/* Closed admission prevents a new source owner after the zero sample;
	 * re-sample the complete authority tuple before exposing the proof. */
	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	if (pg_atomic_read_u32(
			&SemanticActivationShmem
				 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0])
		!= 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	proof->record_generation = generation;
	proof->debt_count = 0;
	proof->sample_digest = 0;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

/*
 * D4's generation-bound transport proof is one fail-closed conjunction:
 * closed admission and zero TARGET debt, the exact worker-0 drain ACK plus
 * four-slot LMON reclaim, no R4 route record, and no live R4 requester slot.
 * The callback owns no new shared state; every sample comes from the existing
 * generation/incarnation-bound owners.
 */
static ClusterSemanticActivationResult
r4_source_transport_zero(uint64 generation, ClusterSemanticZeroProof *proof)
{
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterLmsSharedState *lms_state;
	uint64 worker_incarnation = 0;
	uint32 target_debt;

	if (proof == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	memset(proof, 0, sizeof(*proof));

	if (generation == 0 || !semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	target_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]);
	if (target_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	lms_state = cluster_lms_shared_state();
	if (lms_state == NULL
		|| !cluster_lms_r4_drain_request(
			lms_state, generation, &worker_incarnation))
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;
	cluster_lms_wakeup(0);
	if (!cluster_cr_server_r4_lmon_reclaim_closed(
			worker_incarnation, generation))
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;

	(void)cluster_gcs_block_dedup_r4_route_purge_closed();
	if (cluster_gcs_block_dedup_r4_route_count() != 0
		|| cluster_gcs_block_r4_requester_count() != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;

	/* Bind the proof to a final coherent gate sample; no partial proof escapes
	 * if the formation/generation/close edge moved during convergence. */
	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	target_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]);
	if (target_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	proof->record_generation = generation;
	proof->debt_count = 0;
	/* The approved contract freezes the field but no cross-node digest
	 * formula; zero records the all-zero census without inventing authority. */
	proof->sample_digest = 0;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static bool
semantic_activation_ack_member_prepared_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self)
{
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAckTuple self;
	const ClusterSemanticActivationDescriptor *round_descriptor;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 self_bit;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;
	bool all_observed;
	int node;

	/* RF-ROOT P7 (contract): the bit22 cutover round uses the
	 * round-parameterized check (member set from the ACK table, target
	 * carries bit22) instead of the R4 four-member hardcoded shape. */
	if (image != NULL && (image->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
		return semantic_activation_ack_member_prepared_image_current_bit22(
			image, out_self);

	if (image == NULL || cluster_node_id < 0 || cluster_node_id >= 4
		|| (image->stage
				!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
			&& image->stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
		|| image->coordinator_node != UINT32_C(0)
		|| image->expected_members_lo != UINT64_C(0x0f)
		|| image->expected_members_hi != 0
		|| image->round_nonce == 0 || image->record_generation == 0
		|| !semantic_activation_round_descriptor(
			image->source_feature_bitmap,
			image->target_feature_bitmap,
			image->rollback_feature_bitmap, &round_descriptor,
			&required_caps)
		|| image->capability_sample_digest == 0
		|| (image->observed_members_lo
			& ~image->expected_members_lo) != 0
		|| image->observed_members_hi != 0
		|| semantic_activation_lmon_record_read_seq != 0
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return false;

	all_observed
		= image->observed_members_lo == image->expected_members_lo;
	if (image->flags
		!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			| (all_observed
				   ? CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE
				   : 0)))
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 4;
		bool observed
			= node < 64
				  ? (image->observed_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : false;

		if (!member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&image->expected[node],
					sizeof(image->expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&image->observed[node],
					sizeof(image->observed[node])))
				return false;
			continue;
		}
		if (observed) {
			if (!semantic_activation_ack_matches(
					&image->observed[node], &image->expected[node]))
				return false;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&image->observed[node],
				   sizeof(image->observed[node])))
			return false;
	}

	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != image->expected_members_lo
		|| current_members_hi != image->expected_members_hi
		|| current_epoch != image->transition_epoch
		|| current_coordinator_node != (int32)image->coordinator_node
		|| !semantic_activation_snapshot(&snapshot)
		|| !snapshot.transition_closed
		|| snapshot.active_bits != image->source_feature_bitmap
		|| snapshot.record_generation != image->record_generation
		|| snapshot.formation_epoch != image->transition_epoch)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_expected_image_current(
			image, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			image->record_generation, &self)
		|| !semantic_activation_ack_matches(
			&image->expected[cluster_node_id], &self))
		return false;

	self_bit = UINT64_C(1) << cluster_node_id;
	if ((image->observed_members_lo & self_bit) != 0
		&& !semantic_activation_ack_matches(
			&image->observed[cluster_node_id], &self))
		return false;
	if (out_self != NULL)
		*out_self = self;
	return true;
}

static bool
semantic_activation_ack_lmon_finish_member_prepared(
	const ClusterSemanticActivationAckTableV1 *before,
	ClusterSemanticActivationResult callback_result)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	uint64 self_bit;
	bool all_observed;

	if (callback_result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return true;
	if (!semantic_activation_ack_member_prepared_image_current(
			before, &self)
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_member_prepared_image_current(
			&after, &self))
		return true;

	self_bit = UINT64_C(1) << cluster_node_id;
	if ((after.observed_members_lo & self_bit) != 0)
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = after.stage;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	if (!semantic_activation_ack_pending_send_begin_positive(
			&pending, &request, cluster_node_id, &self))
		return true;

	next = after;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	all_observed
		= next.observed_members_lo == next.expected_members_lo;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	if (all_observed)
		next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	if (!semantic_activation_ack_table_publish(&next))
		return true;
	if (cluster_node_id == (int32)next.coordinator_node)
		return semantic_activation_ack_lmon_send_origin_requests();
	semantic_activation_ack_local_pending_send = pending;
	semantic_activation_ack_lmon_send_pending();
	return true;
}

/*
 * The approved initial-clean D13 branch has no replacement image to
 * reconcile.  Its stage callbacks are therefore read-only validators: they
 * re-prove the exact four-member clean identity, current PGRD/mirror and the
 * stage's authenticated ACK image while the ordinary semantic gate remains
 * closed.  The existing gate publisher, not these callbacks, performs the
 * eventual OPEN mutation.
 *
 * A replacement/recovery round cannot enter this branch because the
 * reconfiguration snapshot positively requires the canonical empty episode.
 * Such a round keeps the original JCMK ADMITTED@88 path and remains
 * fail-closed here.
 */
static bool
r4_initial_clean_stage_current(uint64 generation,
						   ClusterSemanticActivationAckStage stage)
{
	ClusterInitialCleanFormationSnapshot clean_before;
	ClusterInitialCleanFormationSnapshot clean_after;
	ClusterSemanticActivationAckTableV1 table_before;
	ClusterSemanticActivationAckTableV1 table_after;
	SemanticActivationAdmissionSnapshot gate_before;
	SemanticActivationAdmissionSnapshot gate_after;
	uint8 pgrd_before[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 pgrd_after[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 members_lo;
	uint64 members_hi;
	uint64 current_epoch;
	uint64 expected_gate_generation;
	uint32 local_capability_word;
	int32 coordinator_node;
	int node;

	if (generation == 0
		|| (stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
			&& stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
			&& stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		|| cluster_grd_recovery_state_value() != GRD_RECOVERY_IDLE
		|| !cluster_reconfig_snapshot_initial_clean_formation(
			&clean_before)
		|| (clean_before.formation_marker_generation == 0
			&& clean_before.formation_epoch != CLUSTER_EPOCH_INITIAL)
		|| clean_before.members_lo != UINT64_C(0x0f)
		|| clean_before.members_hi != 0
		|| clean_before.arbiter_node != 0
		|| !semantic_activation_initial_clean_pgrd_mirror(pgrd_before)
		|| !semantic_activation_ack_table_snapshot(&table_before)
		|| table_before.stage != stage
		|| table_before.coordinator_node != UINT32_C(0)
		|| table_before.round_nonce == 0
		|| table_before.record_generation != generation
		|| table_before.expected_members_lo != clean_before.members_lo
		|| table_before.expected_members_hi != clean_before.members_hi
		|| table_before.transition_epoch != clean_before.formation_epoch
		|| table_before.source_feature_bitmap != 0
		|| table_before.target_feature_bitmap
		   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| table_before.rollback_feature_bitmap != 0
		|| table_before.capability_sample_digest == 0
		|| !semantic_activation_snapshot(&gate_before)
		|| !gate_before.transition_closed
		|| gate_before.active_bits != 0
		|| gate_before.formation_epoch != clean_before.formation_epoch
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &members_lo, &members_hi, &current_epoch,
			&coordinator_node)
		|| members_lo != clean_before.members_lo
		|| members_hi != clean_before.members_hi
		|| current_epoch != clean_before.formation_epoch
		|| coordinator_node != clean_before.arbiter_node)
		return false;

	expected_gate_generation
		= stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			  ? generation - 1
			  : generation;
	if (gate_before.record_generation != expected_gate_generation
		|| (clean_before.formation_marker_generation == 0
			&& clean_before.arbiter_incarnation != 0)
		|| (clean_before.formation_marker_generation != 0
			&& clean_before.arbiter_incarnation
			   != table_before.expected[0].admitted_incarnation))
		return false;
	for (node = 0; node < 4; node++) {
		if (clean_before.admitted_incarnation[node] == 0
			|| clean_before.admitted_incarnation[node]
			   != table_before.expected[node].admitted_incarnation)
			return false;
	}

	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word
		 & CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		!= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		return false;
	if (stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
		if (!semantic_activation_ack_complete_image_current(
				&table_before, members_lo, members_hi, current_epoch,
				coordinator_node, cluster_node_id, local_capability_word))
			return false;
	} else if (!semantic_activation_ack_member_prepared_image_current(
				   &table_before, NULL))
		return false;

	return cluster_grd_recovery_state_value() == GRD_RECOVERY_IDLE
		   && cluster_reconfig_snapshot_initial_clean_formation(
			   &clean_after)
		   && semantic_activation_initial_clean_snapshot_equal(
			   &clean_before, &clean_after)
		   && semantic_activation_initial_clean_pgrd_mirror(pgrd_after)
		   && memcmp(pgrd_before, pgrd_after, sizeof(pgrd_before)) == 0
		   && semantic_activation_ack_table_snapshot(&table_after)
		   && memcmp(&table_before, &table_after,
					 sizeof(table_before)) == 0
		   && semantic_activation_snapshot(&gate_after)
		   && gate_after.seq == gate_before.seq
		   && gate_after.active_bits == gate_before.active_bits
		   && gate_after.record_generation
			  == gate_before.record_generation
		   && gate_after.formation_epoch == gate_before.formation_epoch
		   && gate_after.transition_closed == gate_before.transition_closed
		   && semantic_activation_ack_current_authority(
			   cluster_node_id, &members_lo, &members_hi, &current_epoch,
			   &coordinator_node)
		   && members_lo == clean_before.members_lo
		   && members_hi == clean_before.members_hi
		   && current_epoch == clean_before.formation_epoch
		   && coordinator_node == clean_before.arbiter_node
		   && cluster_ic_local_capability_word() == local_capability_word;
}

static ClusterSemanticActivationResult
r4_prepare_target(uint64 generation)
{
	return r4_initial_clean_stage_current(
			   generation,
			   CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED)
			   ? CLUSTER_SEMANTIC_ACTIVATION_OK
			   : CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_apply_target_closed(uint64 generation)
{
	return r4_initial_clean_stage_current(
			   generation,
			   CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
			   ? CLUSTER_SEMANTIC_ACTIVATION_OK
			   : CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_open_target_admission(uint64 generation)
{
	return r4_initial_clean_stage_current(
			   generation,
			   CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
			   ? CLUSTER_SEMANTIC_ACTIVATION_OK
			   : CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

/*
 * RF-ROOT P7 (contract): the bit22 cutover round's member-side stage
 * callbacks.  PREPARED has no member action (the activation is the
 * coordinator's; members only ACK the CLOSED-ACK binding), so the callback
 * is a no-op OK — in contrast to R4's cr-sync prepare_target.
 */
static ClusterSemanticActivationResult
bit22_stage_ok(uint64 record_generation)
{
	(void) record_generation;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

/* The retired PCM-X source no longer supplies these callbacks.  The native
 * Resource-X owner retains the same-T R8/R10 pair while the R4 ACK image is
 * revalidated as an independent freshness coordinate.  Neither view grants
 * authority; every callback only verifies one exact local prerequisite. */
static bool
r11_resource_x_cutover_digest_exact(
	uint64 generation, ClusterSemanticR11CutoverPhase expected_phase,
	bool thawed, uint64 *digest_out)
{
	ClusterSemanticR11CutoverSnapshot after;
	ClusterSemanticR11CutoverSnapshot before;
	ResourceXReconfigToken token;

	if (digest_out == NULL)
		return false;
	*digest_out = 0;
	return generation != 0 && generation != UINT64_MAX
		&& cluster_semantic_activation_r11_cutover_snapshot(&before)
		&& before.phase == expected_phase
		&& before.record_generation == generation
		&& before.formation_epoch == cluster_epoch_get_current()
		&& cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
			thawed, &token, digest_out)
		&& token.old_formation != 0
		&& token.old_formation != UINT64_MAX
		&& token.new_formation != 0
		&& token.new_formation != UINT64_MAX
		&& token.old_formation != token.new_formation
		&& token.freeze_generation != 0
		&& token.freeze_generation != UINT64_MAX
		&& token.reserved == 0
		&& *digest_out != 0
		&& cluster_semantic_activation_r11_cutover_snapshot(&after)
		&& memcmp(&before, &after, sizeof(before)) == 0;
}

/* Bound OPEN and FROZEN gates use the ordinary exact snapshot.  The narrow
 * cutover snapshot exists only to admit the pristine, unbound formation-zero
 * gate at the first R11 transition. */
static bool
r11_resource_x_gate_snapshot_exact(ResourceXGateSnapshot *gate)
{
	return gate != NULL
		&& (cluster_pcm_lock_resource_x_gate_snapshot(gate)
			|| cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(gate));
}

static ClusterSemanticActivationResult
r11_resource_x_frozen_stage(uint64 generation)
{
	uint64 digest;

	return r11_resource_x_cutover_digest_exact(
		generation, CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED,
		false, &digest)
		? CLUSTER_SEMANTIC_ACTIVATION_OK
		: CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
}

static ClusterSemanticActivationResult
r11_resource_x_readiness(uint64 expected_generation,
						 ClusterSemanticActivationRefusal *refusal)
{
	ResourceXGateSnapshot gate;
	ResourceXWriterPath writer_path;
	ClusterSemanticActivationResult result;
	uint64 r4_generation = 0;

	writer_path = cluster_resource_x_writer_path_snapshot(&r4_generation);
	result = expected_generation != 0
			 && expected_generation != UINT64_MAX
			 && writer_path == RESOURCE_X_WRITER_CLOSED
			 && r4_generation == expected_generation
			 && r11_resource_x_gate_snapshot_exact(&gate)
			 && gate.phase == RESOURCE_X_GATE_OPEN
			 && gate.freeze_generation == 0 && gate.reserved == 0
			 && gate.formation != UINT64_MAX
			 ? CLUSTER_SEMANTIC_ACTIVATION_OK
			 : CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	semantic_activation_set_refusal(
		refusal, result,
		result == CLUSTER_SEMANTIC_ACTIVATION_OK
			? 0
			: CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		expected_generation);
	return result;
}

static ClusterSemanticActivationResult
r11_resource_x_close_source(uint64 generation)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	ResourceXGateSnapshot gate;

	if (generation == 0 || generation == UINT64_MAX
		|| !cluster_semantic_activation_r11_cutover_snapshot(&cutover)
		|| cutover.phase != CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED
		|| cutover.record_generation != generation
		|| cutover.formation_epoch != cluster_epoch_get_current()
		|| !r11_resource_x_gate_snapshot_exact(&gate)
		|| gate.reserved != 0
		|| (gate.phase != RESOURCE_X_GATE_OPEN
			&& gate.phase != RESOURCE_X_GATE_FROZEN))
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static ClusterSemanticActivationResult
r11_resource_x_logical_zero(uint64 generation,
						ClusterSemanticZeroProof *proof)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	ResourceXGateSnapshot gate;
	uint64 digest = 0;

	if (proof != NULL)
		memset(proof, 0, sizeof(*proof));
	if (proof == NULL || generation == 0 || generation == UINT64_MAX
		|| !cluster_semantic_activation_r11_cutover_snapshot(&cutover)
		|| cutover.phase != CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED
		|| cutover.record_generation != generation
		|| cutover.formation_epoch != cluster_epoch_get_current())
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	if (!r11_resource_x_cutover_digest_exact(
			generation, CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED,
			false, &digest)) {
		if (!r11_resource_x_gate_snapshot_exact(&gate)
			|| gate.reserved != 0
			|| (gate.phase != RESOURCE_X_GATE_OPEN
				&& gate.phase != RESOURCE_X_GATE_FROZEN))
			return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;
	}
	proof->record_generation = generation;
	proof->debt_count = 0;
	proof->sample_digest = digest;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static ClusterSemanticActivationResult
r11_resource_x_transport_zero(uint64 generation,
						  ClusterSemanticZeroProof *proof)
{
	return r11_resource_x_logical_zero(generation, proof);
}

static ClusterSemanticActivationResult
r11_resource_x_prepare_target(uint64 generation)
{
	return r11_resource_x_frozen_stage(generation);
}

static ClusterSemanticActivationResult
r11_resource_x_apply_target_closed(uint64 generation)
{
	return r11_resource_x_frozen_stage(generation);
}

static ClusterSemanticActivationResult
r11_resource_x_revert_source_closed(uint64 generation)
{
	(void) generation;
	return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
}

static ClusterSemanticActivationResult
r11_resource_x_open_target(uint64 generation)
{
	uint64 digest;

	return r11_resource_x_cutover_digest_exact(
		generation,
		CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL,
		true, &digest)
		? CLUSTER_SEMANTIC_ACTIVATION_OK
		: CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
}

static const ClusterSemanticActivationDescriptor r4_descriptor = {
	.name = "R4_SYNC_CR_V1",
	.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
	.required_hello_caps = CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS,
	.required_active_bits = 0,
	.source_available = true,
	.pre_prepare_readiness = r4_pre_prepare_readiness,
	.close_source_admission = r4_close_source_admission,
	.source_logical_debt_zero = r4_source_logical_debt_zero,
	.source_transport_zero = r4_source_transport_zero,
	.prepare_target = r4_prepare_target,
	.apply_target_closed = r4_apply_target_closed,
	.revert_source_closed = r4_stage_fail_closed,
	.open_target_admission = r4_open_target_admission,
};

static const ClusterSemanticActivationDescriptor r11_resource_x_descriptor = {
	.name = "R11_RESOURCE_X_D5_CUTOVER_V1",
	.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
	.required_hello_caps = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						   | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
	.required_active_bits = 0,
	.source_available = CLUSTER_SEMANTIC_R11_RESOURCE_X_SOURCE_AVAILABLE,
	.pre_prepare_readiness = r11_resource_x_readiness,
	.close_source_admission = r11_resource_x_close_source,
	.source_logical_debt_zero = r11_resource_x_logical_zero,
	.source_transport_zero = r11_resource_x_transport_zero,
	.prepare_target = r11_resource_x_prepare_target,
	.apply_target_closed = r11_resource_x_apply_target_closed,
	.revert_source_closed = r11_resource_x_revert_source_closed,
	.open_target_admission = r11_resource_x_open_target,
};

StaticAssertDecl(
	(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
	 & CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) == 0,
	"R11 Resource-X cutover feature bit must be unique");
StaticAssertDecl(PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1
				 == UINT32_C(0x00020000),
	"R11 Resource-X cutover must retain the R10 capability value");

/* Immutable after postmaster module registration.  R4 occupies bit zero in
 * every build; later feature modules register their compile-time descriptor
 * during the same shared-memory registration window. */
static const ClusterSemanticActivationDescriptor
	*SemanticActivationDescriptors[64] = {
		[0] = &r4_descriptor,
		[10] = &r11_resource_x_descriptor,
	};

/* One durable R4 round may add exactly one compiled feature.  R4 itself is
 * the initial 0->bit0 round and remains present as the carrier for every
 * later round.  The descriptor and its HELLO requirements are derived from
 * the immutable registry; the helper owns no activation state. */
static bool
semantic_activation_round_descriptor(
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 rollback_feature_bitmap,
	const ClusterSemanticActivationDescriptor **descriptor_out,
	uint32 *required_caps_out)
{
	const ClusterSemanticActivationDescriptor *descriptor;
	uint64 added;
	uint64 compiled;

	if (descriptor_out == NULL || required_caps_out == NULL)
		return false;
	*descriptor_out = NULL;
	*required_caps_out = 0;
	compiled = cluster_semantic_activation_compiled_feature_bitmap();
	if (rollback_feature_bitmap != 0
		|| (source_feature_bitmap & ~compiled) != 0
		|| (target_feature_bitmap & ~compiled) != 0
		|| (target_feature_bitmap & source_feature_bitmap)
		   != source_feature_bitmap)
		return false;
	added = target_feature_bitmap & ~source_feature_bitmap;
	if (added == 0 || (added & (added - 1)) != 0)
		return false;
	if (added != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		&& (source_feature_bitmap
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) == 0)
		return false;
	descriptor = cluster_semantic_activation_descriptor(added);
	if (descriptor == NULL || descriptor->feature_bit != added
		|| (descriptor->required_active_bits & source_feature_bitmap)
		   != descriptor->required_active_bits)
		return false;
	*descriptor_out = descriptor;
	*required_caps_out = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
						 | descriptor->required_hello_caps;
	return true;
}

/* The RF bit22 round has its own frozen non-registry carrier.  Preserve that
 * contract byte-for-byte; ordinary semantic-activation rounds derive their
 * exact capability set from the one feature added by the compiled registry. */
static bool
semantic_activation_ack_round_required_caps(
	uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 rollback_feature_bitmap, uint32 *required_caps_out)
{
	const ClusterSemanticActivationDescriptor *descriptor;

	if (required_caps_out == NULL)
		return false;
	*required_caps_out = 0;
	if ((target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0) {
		*required_caps_out = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
		return true;
	}
	return semantic_activation_round_descriptor(
		source_feature_bitmap, target_feature_bitmap,
		rollback_feature_bitmap, &descriptor, required_caps_out);
}

/*
 * RF-ROOT P7 (contract): bit22 cutover round — member-side OPEN_APPLIED
 * stage.  The member applies the bit22 latch (one-shot, monotonic; the
 * census self-check is inside the latch apply, so a KNOWN-DEFERRED
 * regression turns the round RED).  Round-parameterized: the member set
 * comes from the ACK table, the round identity from transition_epoch +
 * record_generation, and the round is identified as the bit22 cutover by
 * the bit22 target bit.  Idempotent: a member that already observed
 * itself simply re-ACKs (the latch is monotonic, replay is safe).
 */
static bool
semantic_activation_ack_member_open_applied_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self)
{
	return semantic_activation_ack_member_bit22_stage_image_current(
		image, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED,
		out_self);
}

/* RF-ROOT P9 verification (implementation): stage-parameterized bit22 member image
 * check — the cutover round's COMMIT_APPLIED and OPEN_APPLIED member
 * stages share the same shape (bit22 target, parameterized member set). */
static bool
semantic_activation_ack_member_bit22_stage_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint32 stage, SemanticActivationAckTuple *out_self)
{
	SemanticActivationAckTuple self;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (image == NULL || out_self == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| image->stage != stage
		|| image->coordinator_node == (uint32)cluster_node_id
		|| image->round_nonce == 0
		|| image->record_generation == 0
		|| (image->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| (image->flags
			& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) != 0
		|| (image->flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| image->expected_members_lo == 0
		|| image->expected_members_hi != 0 /* members < 64 (2-node t243) */
		|| (image->observed_members_lo
			& ~image->expected_members_lo) != 0
		|| image->observed_members_hi != 0
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return false;
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != image->expected_members_lo
		|| current_members_hi != image->expected_members_hi
		|| current_epoch != image->transition_epoch
		|| current_coordinator_node != (int32)image->coordinator_node)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_expected_image_current(
			image, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			image->record_generation, &self)
		|| !semantic_activation_ack_matches(
			&image->expected[cluster_node_id], &self))
		return false;
	*out_self = self;
	return true;
}

static bool
semantic_activation_ack_lmon_finish_member_open_applied(
	const ClusterSemanticActivationAckTableV1 *before,
	bool latch_applied)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	uint64 self_bit;
	bool all_observed;

	/* Fail-closed: a refused latch apply (round invalid / census RED
	 * regression) leaves the member un-observed — the round never reaches
	 * COMPLETE and the coordinator's deadline fails the cutover. */
	if (!latch_applied)
		return true;
	if (!semantic_activation_ack_member_open_applied_image_current(
			before, &self)
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_member_open_applied_image_current(
			&after, &self))
		return true;

	self_bit = UINT64_C(1) << cluster_node_id;
	if ((after.observed_members_lo & self_bit) != 0)
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = after.stage;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	if (!semantic_activation_ack_pending_send_begin_positive(
			&pending, &request, cluster_node_id, &self))
		return true;

	next = after;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	all_observed
		= next.observed_members_lo == next.expected_members_lo;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	if (all_observed)
		next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	if (!semantic_activation_ack_table_publish(&next))
		return true;
	semantic_activation_ack_local_pending_send = pending;
	semantic_activation_ack_lmon_send_pending();
	return true;
}

static bool
semantic_activation_ack_lmon_progress_member_open_applied(
	const ClusterSemanticActivationAckTableV1 *before)
{
	SemanticActivationAckTuple self;
	uint64 self_bit;
	bool latch_applied;

	if (before == NULL || before->stage
		!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		return false;
	if (cluster_node_id == (int32)before->coordinator_node)
		return false;	/* the coordinator drives, it does not apply */
	if (!semantic_activation_ack_member_open_applied_image_current(
			before, &self))
		return true;	/* image not current: retry on the next tick */
	self_bit = UINT64_C(1) << cluster_node_id;
	if ((before->observed_members_lo & self_bit) != 0)
		return true;	/* idempotent: this member already applied */

	latch_applied = cluster_r4_bit22_cutover_latch_apply(
		before->transition_epoch, before->record_generation);
	return semantic_activation_ack_lmon_finish_member_open_applied(
		before, latch_applied);
}

/*
 * semantic_activation_ack_lmon_progress_member_barrier_bit22 -- RF-ROOT
 * P9 verification (verified implementation): member side of the bit22 cutover
 * round's source-close BARRIER.  The member freezes its own wal-state
 * writers for the round, waits for in-flight writers to drain (bounded),
 * then ACKs.  A failed freeze / drain leaves the member un-observed — the
 * BARRIER never completes and the round fails closed.
 */
static bool
semantic_activation_ack_lmon_progress_member_barrier_bit22(
	const ClusterSemanticActivationAckTableV1 *before)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	uint64 self_bit;
	bool all_observed;
	int i;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER)
		return false;
	if (cluster_node_id == (int32)before->coordinator_node)
		return false;
	if (!semantic_activation_ack_member_bit22_stage_image_current(
			before, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER, &self))
		return true;
	self_bit = UINT64_C(1) << cluster_node_id;
	if ((before->observed_members_lo & self_bit) != 0)
		return true;	/* idempotent */

	/* Freeze the local source for this exact round, then drain in-flight
	 * writers (bounded). */
	if (!cluster_r4_bit22_source_close_begin(
			before->transition_epoch, before->record_generation))
		return true;	/* fail-closed: retry on the next tick */
	for (i = 0; i < 1000; i++) {
		if (pg_atomic_read_u32(
				&SemanticActivationBit22SourceClose->writer_count) == 0)
			break;
		pg_usleep(5000L); /* 5 ms; up to ~5 s */
	}
	if (pg_atomic_read_u32(
			&SemanticActivationBit22SourceClose->writer_count) != 0)
		return true;	/* writers still draining: retry */

	/* RF-ROOT P9 verification (cold-formation): this member's source is now
	 * frozen — publish the closed-source snapshot (transition_closed=1 at
	 * the round generation), exactly like the coordinator does at BARRIER
	 * COMPLETE.  The later-stage bit22 accepts (PREPARED/COMMIT_APPLIED/
	 * OPEN_APPLIED) bind their generation to this snapshot on the MEMBER
	 * side; without the member-side publish their snapshot gen stays 0 and
	 * every later-stage request is rejected. */
	{
		SemanticActivationAdmissionSnapshot snap;

		if (!semantic_activation_snapshot(&snap)
			|| !semantic_activation_lmon_publish_gate(
				&snap, before->source_feature_bitmap,
				before->record_generation, before->transition_epoch, true))
			return true;
	}

	if (!semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_member_bit22_stage_image_current(
			&after, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER, &self))
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = after.stage;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	if (!semantic_activation_ack_pending_send_begin_positive(
			&pending, &request, cluster_node_id, &self))
		return true;

	next = after;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	all_observed
		= next.observed_members_lo == next.expected_members_lo;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	if (all_observed)
		next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	if (!semantic_activation_ack_table_publish(&next))
		return true;
	semantic_activation_ack_local_pending_send = pending;
	semantic_activation_ack_lmon_send_pending();
	return true;
}

/*
 * semantic_activation_ack_lmon_progress_member_commit_applied_bit22 --
 * RF-ROOT P9 verification (verified implementation): member side of the bit22
 * cutover round's COMMIT_APPLIED stage.  The member re-verifies the
 * now-ACTIVE canonical root bound to this exact round (full canonical
 * validation via bootstrap_validate_active_round against the seam round)
 * and ACKs.  A failed verification leaves the member un-observed — the
 * stage never completes and the round fails closed.
 */
static bool
semantic_activation_ack_lmon_progress_member_commit_applied_bit22(
	const ClusterSemanticActivationAckTableV1 *before)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	ClusterControlRootFileToken token;
	ClusterControlRootResult root_result;
	uint64 self_bit;
	bool all_observed;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
		return false;
	if (cluster_node_id == (int32)before->coordinator_node)
		return false;	/* the coordinator drives, it does not apply */
	if (!semantic_activation_ack_member_bit22_stage_image_current(
			before, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED,
			&self)) {
		ereport(LOG,
				(errmsg("bit22 cutover (node %d): COMMIT_APPLIED stage image "
						"not current (stage=%u flags=0x%x gen=%llu observed=%llx) "
						"— retrying",
						cluster_node_id, (unsigned) before->stage,
						(unsigned) before->flags,
						(unsigned long long) before->record_generation,
						(unsigned long long) before->observed_members_lo)));
		return true;	/* image not current: retry on the next tick */
	}
	self_bit = UINT64_C(1) << cluster_node_id;
	if ((before->observed_members_lo & self_bit) != 0)
		return true;	/* idempotent: this member already applied */

	/* The ACTIVE canonical root must be bound to this exact round.  The
	 * seam (full round + sha) is coordinator-ONLY shmem — other NODES
	 * cannot see it — so the member binds the root to the round identity
	 * it holds (epoch / prepare-generation / bitmaps) plus the non-zero
	 * round sha the coordinator wrote under the create/activate proofs. */
	root_result = cluster_control_root_bootstrap_validate_active_round_fields(
		before->transition_epoch, before->record_generation - 1,
		before->source_feature_bitmap, before->target_feature_bitmap);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		ereport(LOG,
				(errmsg("bit22 cutover (node %d): ACTIVE root field binding "
						"refused (result %d, gen %llu) — no ACK until the "
						"root verifies",
						cluster_node_id, (int) root_result,
						(unsigned long long) before->record_generation)));
		return true;	/* fail-closed: no ACK until the root verifies */
	}

	if (!semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_member_bit22_stage_image_current(
			&after, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED,
			&self))
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = after.stage;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	if (!semantic_activation_ack_pending_send_begin_positive(
			&pending, &request, cluster_node_id, &self))
		return true;

	next = after;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	all_observed
		= next.observed_members_lo == next.expected_members_lo;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	if (all_observed)
		next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	if (!semantic_activation_ack_table_publish(&next))
		return true;
	semantic_activation_ack_local_pending_send = pending;
	semantic_activation_ack_lmon_send_pending();
	return true;
}

static bool
semantic_activation_ack_lmon_progress_member_commit_applied(
	const ClusterSemanticActivationAckTableV1 *before)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationReadCompletion completion;
	ClusterSemanticActivationRecord commit;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	SemanticActivationAckTuple self;
	const ClusterSemanticActivationDescriptor *round_descriptor;
	ClusterSemanticActivationResult result;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 callback_generation;
	uint64 request_seq;
	uint64 self_bit;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;
	bool all_observed;
	int node;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
		return false;
	/* The approved happy path is the exact four-member formation.  Any
	 * mismatch retains closed admission and consumes no legacy sync path. */
	if (cluster_node_id <= 0 || cluster_node_id >= 4
		|| before->coordinator_node != UINT32_C(0)
		|| before->expected_members_lo != UINT64_C(0x0f)
		|| before->expected_members_hi != 0
		|| before->round_nonce == 0 || before->record_generation == 0
		|| !semantic_activation_round_descriptor(
			before->source_feature_bitmap,
			before->target_feature_bitmap,
			before->rollback_feature_bitmap, &round_descriptor,
			&required_caps)
		|| before->capability_sample_digest == 0
		|| (before->flags
			& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) != 0
		|| (before->flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| (before->observed_members_lo
			& ~before->expected_members_lo) != 0
		|| before->observed_members_hi != 0
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return true;

	all_observed
		= before->observed_members_lo == before->expected_members_lo;
	if (all_observed
		!= ((before->flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0))
		return true;
	callback_generation = before->record_generation;
	if (round_descriptor->feature_bit
		== CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) {
		if (callback_generation <= 1)
			return true;
		callback_generation--;
	}
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 4;
		bool observed
			= node < 64
				  ? (before->observed_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : false;

		if (!member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&before->expected[node],
					sizeof(before->expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&before->observed[node],
					sizeof(before->observed[node])))
				return true;
			continue;
		}
		if (observed) {
			if (!semantic_activation_ack_matches(
					&before->observed[node], &before->expected[node]))
				return true;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&before->observed[node],
				   sizeof(before->observed[node])))
			return true;
	}

	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before->expected_members_lo
		|| current_members_hi != before->expected_members_hi
		|| current_epoch != before->transition_epoch
		|| current_coordinator_node != (int32)before->coordinator_node
		|| !semantic_activation_snapshot(&snapshot)
		|| !snapshot.transition_closed
		|| snapshot.active_bits != before->source_feature_bitmap
		|| snapshot.formation_epoch != before->transition_epoch)
		return true;
	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_expected_image_current(
			before, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			before->record_generation, &self)
		|| !semantic_activation_ack_matches(
			&before->expected[cluster_node_id], &self))
		return true;

	self_bit = UINT64_C(1) << cluster_node_id;
	if ((before->observed_members_lo & self_bit) != 0)
		return snapshot.record_generation == before->record_generation
			   && semantic_activation_ack_matches(
				   &before->observed[cluster_node_id], &self);
	if (snapshot.record_generation == before->record_generation) {
		if (semantic_activation_lmon_record_read_seq != 0)
			return true;
		result = round_descriptor->apply_target_closed(
			callback_generation);
		return semantic_activation_ack_lmon_finish_member_prepared(
			before, result);
	}
	if (snapshot.record_generation == UINT64_MAX
		|| snapshot.record_generation + 1 != before->record_generation)
		return true;

	if (semantic_activation_lmon_record_read_seq == 0) {
		if (semantic_activation_record_read_mailbox_submit(&request_seq))
			semantic_activation_lmon_record_read_seq = request_seq;
		return true;
	}
	if (!semantic_activation_record_read_mailbox_poll_completion(
			semantic_activation_lmon_record_read_seq, &completion))
		return true;
	semantic_activation_lmon_record_read_seq = 0;
	if (completion.result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| completion.implicit_open
		|| !cluster_semantic_activation_record_decode(
			completion.selected_bytes, &commit, NULL)
		|| commit.phase != CLUSTER_SEMANTIC_PHASE_COMMIT
		|| commit.record_generation != before->record_generation
		|| commit.transition_epoch != before->transition_epoch
		|| commit.coordinator_node != before->coordinator_node
		|| commit.coordinator_incarnation
		   != before->expected[before->coordinator_node]
			  .admitted_incarnation
		|| commit.admitted_members_lo != before->expected_members_lo
		|| commit.admitted_members_hi != before->expected_members_hi
		|| commit.source_feature_bitmap
		   != before->source_feature_bitmap
		|| commit.target_feature_bitmap
		   != before->target_feature_bitmap
		|| commit.rollback_feature_bitmap
		   != before->rollback_feature_bitmap
		|| commit.capability_sample_digest
		   != before->capability_sample_digest
		|| !semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before->expected_members_lo
		|| current_members_hi != before->expected_members_hi
		|| current_epoch != before->transition_epoch
		|| current_coordinator_node != (int32)before->coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_expected_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_lmon_publish_gate(
			&current_snapshot, commit.source_feature_bitmap,
			commit.record_generation, commit.transition_epoch, true))
		return true;

	result = round_descriptor->apply_target_closed(callback_generation);
	return semantic_activation_ack_lmon_finish_member_prepared(
		&after, result);
}

/* A descriptor-backed round opens only after this member has read the exact
 * majority-durable OPEN record and its exact full-member OPEN_APPLIED table
 * is complete.  Reading OPEN publishes only this member's observation; the
 * descriptor and target-only gate remain closed until all observations have
 * converged locally. */
static bool
semantic_activation_ack_lmon_progress_member_open_applied_round(
	const ClusterSemanticActivationAckTableV1 *before)
{
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	ClusterSemanticActivationReadCompletion completion;
	ClusterSemanticActivationRecord open_record;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationAckWireV1 request;
	const ClusterSemanticActivationDescriptor *round_descriptor;
	ClusterSemanticActivationResult result;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 callback_generation;
	uint64 request_seq;
	uint64 self_bit;
	uint32 local_capability_word;
	uint32 required_caps;
	int32 current_coordinator_node;
	bool all_observed;
	bool post_open;
	int node;

	if (before == NULL || before->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		return false;
	if (cluster_node_id <= 0 || cluster_node_id >= 4
		|| before->coordinator_node != UINT32_C(0)
		|| before->expected_members_lo != UINT64_C(0x0f)
		|| before->expected_members_hi != 0
		|| before->round_nonce == 0 || before->record_generation == 0
		|| !semantic_activation_round_descriptor(
			before->source_feature_bitmap,
			before->target_feature_bitmap,
			before->rollback_feature_bitmap, &round_descriptor,
			&required_caps)
		|| before->capability_sample_digest == 0
		|| (before->flags
			& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) != 0
		|| (before->flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| (before->observed_members_lo
			& ~before->expected_members_lo) != 0
		|| before->observed_members_hi != 0
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return true;

	all_observed
		= before->observed_members_lo == before->expected_members_lo;
	if (all_observed
		!= ((before->flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0))
		return true;
	callback_generation = before->record_generation;
	if (round_descriptor->feature_bit
		== CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) {
		if (callback_generation <= 2)
			return true;
		callback_generation -= 2;
	}
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 4;
		bool observed
			= node < 64
				  ? (before->observed_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : false;

		if (!member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&before->expected[node],
					sizeof(before->expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&before->observed[node],
					sizeof(before->observed[node])))
				return true;
			continue;
		}
		if (observed) {
			if (!semantic_activation_ack_matches(
					&before->observed[node], &before->expected[node]))
				return true;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&before->observed[node],
				   sizeof(before->observed[node])))
			return true;
	}

	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before->expected_members_lo
		|| current_members_hi != before->expected_members_hi
		|| current_epoch != before->transition_epoch
		|| current_coordinator_node != (int32)before->coordinator_node
		|| !semantic_activation_snapshot(&snapshot))
		return true;
	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_expected_image_current(
			before, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			before->record_generation, &self)
		|| !semantic_activation_ack_matches(
			&before->expected[cluster_node_id], &self))
		return true;

	self_bit = UINT64_C(1) << cluster_node_id;
	post_open = !snapshot.transition_closed
		&& snapshot.active_bits == before->target_feature_bitmap
		&& snapshot.record_generation == before->record_generation
		&& snapshot.formation_epoch == before->transition_epoch;
	if (post_open)
		return all_observed
			&& (before->observed_members_lo & self_bit) != 0
			&& semantic_activation_ack_matches(
				&before->observed[cluster_node_id], &self);
	if (!snapshot.transition_closed
		|| snapshot.active_bits != before->source_feature_bitmap
		|| snapshot.record_generation == UINT64_MAX
		|| snapshot.record_generation + 1 != before->record_generation
		|| snapshot.formation_epoch != before->transition_epoch)
		return true;

	if ((before->observed_members_lo & self_bit) == 0) {
		if (semantic_activation_lmon_record_read_seq == 0) {
			if (semantic_activation_record_read_mailbox_submit(&request_seq))
				semantic_activation_lmon_record_read_seq = request_seq;
			return true;
		}
		if (!semantic_activation_record_read_mailbox_poll_completion(
				semantic_activation_lmon_record_read_seq, &completion))
			return true;
		semantic_activation_lmon_record_read_seq = 0;
		if (completion.result != CLUSTER_SEMANTIC_ACTIVATION_OK
			|| completion.implicit_open
			|| !cluster_semantic_activation_record_decode(
				completion.selected_bytes, &open_record, NULL)
			|| open_record.phase != CLUSTER_SEMANTIC_PHASE_OPEN
			|| open_record.record_generation != before->record_generation
			|| open_record.transition_epoch != before->transition_epoch
			|| open_record.coordinator_node != before->coordinator_node
			|| open_record.coordinator_incarnation
			   != before->expected[before->coordinator_node]
				  .admitted_incarnation
			|| open_record.admitted_members_lo
			   != before->expected_members_lo
			|| open_record.admitted_members_hi
			   != before->expected_members_hi
			|| open_record.source_feature_bitmap
			   != before->source_feature_bitmap
			|| open_record.target_feature_bitmap
			   != before->target_feature_bitmap
			|| open_record.rollback_feature_bitmap
			   != before->rollback_feature_bitmap
			|| open_record.capability_sample_digest
			   != before->capability_sample_digest
			|| !semantic_activation_snapshot(&current_snapshot)
			|| current_snapshot.seq != snapshot.seq
			|| current_snapshot.active_bits != snapshot.active_bits
			|| current_snapshot.record_generation
			   != snapshot.record_generation
			|| current_snapshot.formation_epoch
			   != snapshot.formation_epoch
			|| current_snapshot.transition_closed
			   != snapshot.transition_closed
			|| !semantic_activation_ack_current_authority(
				cluster_node_id, &current_members_lo, &current_members_hi,
				&current_epoch, &current_coordinator_node)
			|| current_members_lo != before->expected_members_lo
			|| current_members_hi != before->expected_members_hi
			|| current_epoch != before->transition_epoch
			|| current_coordinator_node
			   != (int32)before->coordinator_node
			|| cluster_ic_local_capability_word()
			   != local_capability_word
			|| !semantic_activation_ack_table_snapshot(&after)
			|| memcmp(before, &after, sizeof(after)) != 0
			|| !semantic_activation_ack_expected_image_current(
				&after, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node, cluster_node_id,
				local_capability_word))
			return true;

		memset(&request, 0, sizeof(request));
		request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
		request.stage = after.stage;
		request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
		request.coordinator_node = after.coordinator_node;
		request.member_node = (uint32)cluster_node_id;
		request.transition_epoch = after.transition_epoch;
		request.record_generation = after.record_generation;
		request.round_nonce = after.round_nonce;
		request.source_feature_bitmap = after.source_feature_bitmap;
		request.target_feature_bitmap = after.target_feature_bitmap;
		request.rollback_feature_bitmap = after.rollback_feature_bitmap;
		request.admitted_members_lo = after.expected_members_lo;
		request.admitted_members_hi = after.expected_members_hi;
		request.capability_sample_digest = after.capability_sample_digest;
		memset(&pending, 0, sizeof(pending));
		if (!semantic_activation_ack_pending_send_begin_positive(
				&pending, &request, cluster_node_id, &self))
			return true;

		next = after;
		next.observed_members_lo |= self_bit;
		next.observed[cluster_node_id] = self;
		all_observed
			= next.observed_members_lo == next.expected_members_lo;
		next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
		if (all_observed)
			next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
		if (!semantic_activation_ack_table_publish(&next))
			return true;
		semantic_activation_ack_local_pending_send = pending;
		semantic_activation_ack_lmon_send_pending();
		return true;
	}

	if (!all_observed || semantic_activation_lmon_record_read_seq != 0
		|| !semantic_activation_ack_complete_image_current(
			before, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0)
		return true;

	result = round_descriptor->open_target_admission(
		callback_generation);
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before->expected_members_lo
		|| current_members_hi != before->expected_members_hi
		|| current_epoch != before->transition_epoch
		|| current_coordinator_node != (int32)before->coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(before, &after, sizeof(after)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_lmon_publish_gate(
			&current_snapshot, before->target_feature_bitmap,
			before->record_generation, before->transition_epoch, false))
		return true;
	return true;
}

/*
 * RF-ROOT P7 (contract): round-parameterized PREPARED image check for the
 * bit22 cutover round — mirrors member_open_applied_image_current with
 * stage PREPARED (member set from the ACK table; no four-member hardcoding).
 */
static bool
semantic_activation_ack_member_prepared_image_current_bit22(
	const ClusterSemanticActivationAckTableV1 *image,
	SemanticActivationAckTuple *out_self)
{
	SemanticActivationAckTuple self;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (image == NULL || out_self == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| image->stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| image->coordinator_node == (uint32)cluster_node_id
		|| image->round_nonce == 0
		|| image->record_generation == 0
		|| (image->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| (image->flags
			& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) != 0
		|| (image->flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| image->expected_members_lo == 0
		|| image->expected_members_hi != 0
		|| (image->observed_members_lo
			& ~image->expected_members_lo) != 0
		|| image->observed_members_hi != 0
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return false;
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != image->expected_members_lo
		|| current_members_hi != image->expected_members_hi
		|| current_epoch != image->transition_epoch
		|| current_coordinator_node != (int32)image->coordinator_node)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_expected_image_current(
			image, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			image->record_generation, &self)
		|| !semantic_activation_ack_matches(
			&image->expected[cluster_node_id], &self))
		return false;
	*out_self = self;
	return true;
}

static bool
semantic_activation_ack_lmon_member_pgrd_ready(
	const ClusterSemanticActivationAckTableV1 *expected_table)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterUndoRootDescriptorReadCompletion completion;
	ClusterUndoSmgrRootMirrorState mirror_state;
	SemanticActivationAckTuple self;
	uint8 mirror[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 published[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	char root_directory[MAXPGPATH];

	if (expected_table == NULL
		|| !semantic_activation_ack_member_prepared_image_current(
			expected_table, &self))
		return false;
	if (semantic_activation_initial_clean_pgrd_mirror(published))
		return true;

	if (semantic_activation_lmon_member_pgrd_read_request_seq == 0) {
		if (!semantic_activation_member_pgrd_read_mailbox_submit(
				expected_table,
				&semantic_activation_lmon_member_pgrd_read_request_seq))
			return false;
		return false;
	}
	memset(&completion, 0, sizeof(completion));
	if (!semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
			semantic_activation_lmon_member_pgrd_read_request_seq,
			&completion)
		|| completion.state != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| !semantic_activation_lmon_shared_pgrd_root_directory(
			root_directory))
		return false;

	mirror_state = cluster_undo_smgr_root_descriptor_read_candidate(
		root_directory, mirror);
	if (mirror_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT
		|| memcmp(mirror, completion.selected_bytes, sizeof(mirror)) != 0
		|| !semantic_activation_ack_table_snapshot(&current)
		|| memcmp(expected_table, &current, sizeof(current)) != 0
		|| !semantic_activation_ack_member_prepared_image_current(
			&current, &self)
		|| !semantic_activation_pgrd_snapshot_publish(
			completion.selected_bytes)
		|| !semantic_activation_initial_clean_pgrd_mirror(published)
		|| memcmp(published, completion.selected_bytes,
				  sizeof(published)) != 0
		|| !semantic_activation_ack_table_snapshot(&current)
		|| memcmp(expected_table, &current, sizeof(current)) != 0
		|| !semantic_activation_ack_member_prepared_image_current(
			&current, &self)) {
		semantic_activation_pgrd_snapshot_clear();
		return false;
	}

	semantic_activation_lmon_member_pgrd_read_request_seq = 0;
	return true;
}

static bool
semantic_activation_ack_lmon_progress_member_barrier(void)
{
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAckPendingSend pending;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationReadCompletion completion;
	ClusterSemanticActivationRecord prepare;
	ClusterSemanticActivationAckWireV1 request;
	ClusterSemanticZeroProof proof;
	ClusterSemanticActivationResult result;
	const ClusterSemanticActivationDescriptor *round_descriptor = NULL;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 request_seq;
	uint64 self_bit;
	uint32 local_capability_word;
	uint32 required_caps = 0;
	int32 current_coordinator_node;
	bool all_observed;
	int node;

	if (!semantic_activation_ack_table_snapshot(&before))
		return false;
	if (cluster_node_id == (int32)before.coordinator_node)
		return false;
	/* RF-ROOT P9 verification (implementation): bit22 cutover round member BARRIER —
	 * freeze the local wal-state source and ACK. */
	if (before.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		&& (before.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
		return semantic_activation_ack_lmon_progress_member_barrier_bit22(
			&before);
	if (before.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		/* RF-ROOT P9 verification (implementation): the bit22 cutover round's member
		 * COMMIT_APPLIED verifies the ACTIVE root (no four-member R4
		 * shape). */
		if ((before.target_feature_bitmap
			 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
			return semantic_activation_ack_lmon_progress_member_commit_applied_bit22(
				&before);
		return semantic_activation_ack_lmon_progress_member_commit_applied(
			&before);
	}
	if (before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		&& before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		&& before.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		return false;
	if ((before.target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0) {
		round_descriptor = &r4_descriptor;
		required_caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	} else if (!semantic_activation_round_descriptor(
			   before.source_feature_bitmap,
			   before.target_feature_bitmap,
			   before.rollback_feature_bitmap, &round_descriptor,
			   &required_caps))
		return true;
	/* RF-ROOT P7 (contract): bit22 cutover round — the member applies the
	 * bit22 latch at OPEN_APPLIED (one-shot, monotonic). */
	if (before.stage
		== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED) {
		if ((before.target_feature_bitmap
			 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
			return semantic_activation_ack_lmon_progress_member_open_applied(
				&before);
		return semantic_activation_ack_lmon_progress_member_open_applied_round(
			&before);
	}
	if (before.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED) {
		if (!semantic_activation_ack_member_prepared_image_current(
				&before, &self))
			return true;
		if (round_descriptor == &r4_descriptor
			&& before.source_feature_bitmap == 0
			&& before.target_feature_bitmap
			   == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			&& before.rollback_feature_bitmap == 0
			&& !semantic_activation_ack_lmon_member_pgrd_ready(&before))
			return true;
		self_bit = UINT64_C(1) << cluster_node_id;
		if ((before.observed_members_lo & self_bit) != 0)
			return true;
		/* RF-ROOT P7 (contract): the bit22 cutover round has no member
		 * PREPARED action — no-op OK; R4 keeps its cr-sync callback. */
		if ((before.target_feature_bitmap
			 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
			result = bit22_stage_ok(before.record_generation);
		else
			result = round_descriptor->prepare_target(
				before.record_generation);
		return semantic_activation_ack_lmon_finish_member_prepared(
			&before, result);
	}
	if (before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER)
		return false;

	/* The approved Stage 8 path is the exact four-member formation.  Once a
	 * member has retained its BARRIER request, any mismatch stays closed and
	 * must not fall through to the legacy majority-zero reader. */
	if (cluster_node_id <= 0 || cluster_node_id >= 4
		|| before.coordinator_node != UINT32_C(0)
		|| before.expected_members_lo != UINT64_C(0x0f)
		|| before.expected_members_hi != 0
		|| before.round_nonce == 0
		|| before.record_generation == 0
		|| before.capability_sample_digest == 0
		|| (before.flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
		|| (before.flags
			& ~(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)) != 0
		|| (before.observed_members_lo
			& ~before.expected_members_lo) != 0
		|| before.observed_members_hi != 0)
		return true;

	all_observed
		= before.observed_members_lo == before.expected_members_lo;
	if (all_observed
		!= ((before.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0))
		return true;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool expected_member = node < 4;
		bool observed_member
			= node < 64
				  ? (before.observed_members_lo
					 & (UINT64_C(1) << node)) != 0
				  : (before.observed_members_hi
					 & (UINT64_C(1) << (node - 64))) != 0;

		if (!expected_member) {
			if (!semantic_activation_bytes_are_zero(
					(const uint8 *)&before.expected[node],
					sizeof(before.expected[node]))
				|| !semantic_activation_bytes_are_zero(
					(const uint8 *)&before.observed[node],
					sizeof(before.observed[node])))
				return true;
			continue;
		}
		if (observed_member) {
			if (!semantic_activation_ack_matches(
					&before.observed[node], &before.expected[node]))
				return true;
		} else if (!semantic_activation_bytes_are_zero(
				   (const uint8 *)&before.observed[node],
				   sizeof(before.observed[node])))
			return true;
	}

	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before.expected_members_lo
		|| current_members_hi != before.expected_members_hi
		|| current_epoch != before.transition_epoch
		|| current_coordinator_node != (int32)before.coordinator_node)
		return true;
	local_capability_word = cluster_ic_local_capability_word();
	if ((local_capability_word & required_caps) != required_caps
		|| !semantic_activation_ack_expected_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			before.record_generation, &self)
		|| !semantic_activation_ack_matches(
			&before.expected[cluster_node_id], &self))
		return true;
	self_bit = UINT64_C(1) << cluster_node_id;
	if ((before.observed_members_lo & self_bit) != 0)
		return semantic_activation_ack_matches(
			&before.observed[cluster_node_id], &self);

	if (semantic_activation_lmon_record_read_seq == 0) {
		if (semantic_activation_record_read_mailbox_submit(&request_seq))
			semantic_activation_lmon_record_read_seq = request_seq;
		return true;
	}
	if (!semantic_activation_record_read_mailbox_poll_completion(
			semantic_activation_lmon_record_read_seq, &completion))
		return true;
	semantic_activation_lmon_record_read_seq = 0;
	if (completion.result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| completion.implicit_open
		|| !cluster_semantic_activation_record_decode(
			completion.selected_bytes, &prepare, NULL)
		|| prepare.phase != CLUSTER_SEMANTIC_PHASE_PREPARE
		|| prepare.record_generation != before.record_generation
		|| prepare.transition_epoch != before.transition_epoch
		|| prepare.coordinator_node != before.coordinator_node
		|| prepare.coordinator_incarnation
		   != before.expected[before.coordinator_node]
			  .admitted_incarnation
		|| prepare.admitted_members_lo != before.expected_members_lo
		|| prepare.admitted_members_hi != before.expected_members_hi
		|| prepare.source_feature_bitmap
		   != before.source_feature_bitmap
		|| prepare.target_feature_bitmap
		   != before.target_feature_bitmap
		|| prepare.rollback_feature_bitmap
		   != before.rollback_feature_bitmap
		|| prepare.capability_sample_digest
		   != before.capability_sample_digest
		|| !semantic_activation_snapshot(&snapshot)
		|| snapshot.formation_epoch != before.transition_epoch
		|| snapshot.active_bits != before.source_feature_bitmap)
		return true;
	if (snapshot.transition_closed) {
		if (snapshot.record_generation != before.record_generation)
			return true;
	} else {
		if (snapshot.record_generation == UINT64_MAX
			|| snapshot.record_generation + 1
			   != before.record_generation
			|| !semantic_activation_lmon_publish_gate(
				&snapshot, before.source_feature_bitmap,
				before.record_generation, before.transition_epoch, true))
			return true;
	}

	result = round_descriptor->close_source_admission(
		before.record_generation);
	if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO)
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return true;
	result = round_descriptor->source_logical_debt_zero(
		before.record_generation, &proof);
	if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO)
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| proof.record_generation != before.record_generation
		|| proof.debt_count != 0)
		return true;
	result = round_descriptor->source_transport_zero(
		before.record_generation, &proof);
	if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO
		|| result == CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO)
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| proof.record_generation != before.record_generation
		|| proof.debt_count != 0)
		return true;

	if (!semantic_activation_snapshot(&snapshot)
		|| !snapshot.transition_closed
		|| snapshot.active_bits != before.source_feature_bitmap
		|| snapshot.record_generation != before.record_generation
		|| snapshot.formation_epoch != before.transition_epoch
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != before.expected_members_lo
		|| current_members_hi != before.expected_members_hi
		|| current_epoch != before.transition_epoch
		|| current_coordinator_node != (int32)before.coordinator_node
		|| cluster_ic_local_capability_word()
		   != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_expected_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			after.record_generation, &self)
		|| !semantic_activation_ack_matches(
			&after.expected[cluster_node_id], &self)
		|| semantic_activation_ack_local_pending_send.pending_members_lo != 0
		|| semantic_activation_ack_local_pending_send.pending_members_hi != 0
		|| semantic_activation_ack_local_pending_send.invalidated)
		return true;

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = after.coordinator_node;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = after.transition_epoch;
	request.record_generation = after.record_generation;
	request.round_nonce = after.round_nonce;
	request.source_feature_bitmap = after.source_feature_bitmap;
	request.target_feature_bitmap = after.target_feature_bitmap;
	request.rollback_feature_bitmap = after.rollback_feature_bitmap;
	request.admitted_members_lo = after.expected_members_lo;
	request.admitted_members_hi = after.expected_members_hi;
	request.capability_sample_digest = after.capability_sample_digest;
	memset(&pending, 0, sizeof(pending));
	if (!semantic_activation_ack_pending_send_begin_positive(
			&pending, &request, cluster_node_id, &self))
		return true;

	next = after;
	next.observed_members_lo |= self_bit;
	next.observed[cluster_node_id] = self;
	all_observed
		= next.observed_members_lo == next.expected_members_lo;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	if (all_observed)
		next.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	if (!semantic_activation_ack_table_publish(&next))
		return true;
	semantic_activation_ack_local_pending_send = pending;
	semantic_activation_ack_lmon_send_pending();
	return true;
}

static bool
semantic_activation_ack_lmon_begin_barrier_round(
	const SemanticActivationUtilityRequest *request,
	const ClusterSemanticActivationRecord *prepare)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 after;
	ClusterSemanticActivationAckTableV1 next;
	SemanticActivationAdmissionSnapshot snapshot;
	SemanticActivationAdmissionSnapshot current_snapshot;
	SemanticActivationAckTuple self;
	ClusterSemanticZeroProof proof;
	ClusterSemanticActivationResult result;
	const ClusterSemanticActivationDescriptor *round_descriptor = NULL;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 digest;
	uint32 local_capability_word;
	uint32 required_caps = 0;
	int32 current_coordinator_node;
	bool bit22_round;

	bit22_round
		= (prepare != NULL
		   && (prepare->target_feature_bitmap
			   & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0);
	if (bit22_round) {
		round_descriptor = &r4_descriptor;
		required_caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	}

	if (request == NULL || prepare == NULL
		|| prepare->phase != CLUSTER_SEMANTIC_PHASE_PREPARE
		|| prepare->record_generation == 0
		|| request->expected_record_generation == UINT64_MAX
		|| prepare->record_generation
		   != request->expected_record_generation + 1
		|| prepare->source_feature_bitmap != request->source_feature_bitmap
		|| prepare->target_feature_bitmap != request->target_feature_bitmap
		|| prepare->rollback_feature_bitmap
		   != request->rollback_feature_bitmap
		|| (!bit22_round
			&& !semantic_activation_round_descriptor(
				prepare->source_feature_bitmap,
				prepare->target_feature_bitmap,
				prepare->rollback_feature_bitmap, &round_descriptor,
				&required_caps))
		|| prepare->capability_sample_digest == 0
		|| !semantic_activation_snapshot(&snapshot)
		|| !snapshot.transition_closed
		|| snapshot.active_bits != prepare->source_feature_bitmap
		|| snapshot.record_generation != prepare->record_generation
		|| snapshot.formation_epoch != prepare->transition_epoch
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != 0 || current_coordinator_node != 0
		|| current_members_lo != prepare->admitted_members_lo
		|| current_members_hi != prepare->admitted_members_hi
		|| current_epoch != prepare->transition_epoch
		|| !semantic_activation_ack_table_snapshot(&before)
		|| before.coordinator_node != prepare->coordinator_node
		|| before.round_nonce != request->request_seq
		|| before.expected_members_lo != prepare->admitted_members_lo
		|| before.expected_members_hi != prepare->admitted_members_hi
		|| before.transition_epoch != prepare->transition_epoch
		|| before.record_generation != prepare->record_generation
		|| before.source_feature_bitmap != prepare->source_feature_bitmap
		|| before.target_feature_bitmap != prepare->target_feature_bitmap
		|| before.rollback_feature_bitmap
		   != prepare->rollback_feature_bitmap)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if ((!bit22_round
		 && (local_capability_word & required_caps) != required_caps)
		|| !semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word, current_epoch,
			prepare->record_generation, &self))
		return false;

	if (before.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED) {
		uint64 self_bit = UINT64_C(1) << cluster_node_id;

		if (before.flags
				!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					| (before.observed_members_lo
						   == before.expected_members_lo
						   ? CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE
						   : 0))
			|| before.capability_sample_digest
			   != prepare->capability_sample_digest
			|| !origin->active
			|| before.observed_members_hi != 0
			|| (before.observed_members_lo
				& ~before.expected_members_lo) != 0
			|| !semantic_activation_ack_expected_image_current(
				&before, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word))
			return false;
		if ((before.observed_members_lo & self_bit) == 0) {
			/* RF-ROOT P7 (contract): bit22 round -> no-op member PREPARED. */
			if (bit22_round)
				result = bit22_stage_ok(before.record_generation);
			else
				result = round_descriptor->prepare_target(
					before.record_generation);
			return semantic_activation_ack_lmon_finish_member_prepared(
				&before, result);
		}
		if (!semantic_activation_ack_matches(
				&before.observed[cluster_node_id],
				&before.expected[cluster_node_id]))
			return false;
		if ((before.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0) {
			if (!semantic_activation_ack_complete_image_current(
					&before, current_members_lo, current_members_hi,
					current_epoch, current_coordinator_node,
					cluster_node_id, local_capability_word))
				return false;
			return semantic_activation_ack_lmon_submit_commit(
				request, prepare);
		}
		return semantic_activation_ack_lmon_send_origin_requests();
	}

	if (before.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER) {
		bool complete_current
			= semantic_activation_ack_complete_image_current(
				&before, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node,
				cluster_node_id, local_capability_word);
		if ((before.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
			|| before.capability_sample_digest
			   != prepare->capability_sample_digest
			|| !origin->active
			|| (before.observed_members_lo & UINT64_C(1)) == 0
			|| !semantic_activation_full_ack_table_matches(
				before.expected, before.expected_members_lo,
				before.expected_members_hi, before.expected,
				before.expected_members_lo, before.expected_members_hi)
			|| !semantic_activation_ack_matches(
				&before.expected[0], &self)
			|| !semantic_activation_ack_matches(
				&before.observed[0], &self))
			return false;
		if ((before.flags
			 & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) == 0)
			return semantic_activation_ack_lmon_send_origin_requests();
		if (before.flags
				!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
			|| !complete_current)
			return false;

		/* Resource-X transport-zero is meaningful only after the exact
		 * admitted connection-incarnation barrier is complete.  Consume it
		 * against that immutable image, then recheck every binding before the
		 * PREPARED publication.  The initial R4 and bit22 contracts retain
		 * their frozen ordering. */
		if (round_descriptor == &r11_resource_x_descriptor) {
			result = round_descriptor->source_transport_zero(
				before.record_generation, &proof);
			if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO
				|| result
				   == CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO)
				return true;
			if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
				|| proof.record_generation != before.record_generation
				|| proof.debt_count != 0
				|| !semantic_activation_snapshot(&current_snapshot)
				|| current_snapshot.seq != snapshot.seq
				|| current_snapshot.active_bits != snapshot.active_bits
				|| current_snapshot.record_generation
				   != snapshot.record_generation
				|| current_snapshot.formation_epoch
				   != snapshot.formation_epoch
				|| current_snapshot.transition_closed
				   != snapshot.transition_closed
				|| !semantic_activation_ack_current_authority(
					cluster_node_id, &current_members_lo,
					&current_members_hi, &current_epoch,
					&current_coordinator_node)
				|| current_members_lo != prepare->admitted_members_lo
				|| current_members_hi != prepare->admitted_members_hi
				|| current_epoch != prepare->transition_epoch
				|| current_coordinator_node
				   != (int32)prepare->coordinator_node
				|| cluster_ic_local_capability_word()
				   != local_capability_word
				|| !semantic_activation_ack_table_snapshot(&after)
				|| memcmp(&before, &after, sizeof(before)) != 0
				|| !semantic_activation_ack_complete_image_current(
					&after, current_members_lo, current_members_hi,
					current_epoch, current_coordinator_node,
					cluster_node_id, local_capability_word))
				return false;
			before = after;
		}

		next = before;
		next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		next.flags
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
		next.observed_members_lo = 0;
		next.observed_members_hi = 0;
		memset(next.observed, 0, sizeof(next.observed));
		if (!semantic_activation_ack_table_publish(&next))
			return false;
		memset(origin, 0, sizeof(*origin));
		origin->unsent_members_lo = UINT64_C(0x0e);
		origin->active = true;
		return semantic_activation_ack_lmon_send_origin_requests();
	}

	if (before.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| before.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| before.capability_sample_digest != 0
		|| !semantic_activation_ack_complete_image_current(
			&before, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word)
		|| !semantic_activation_ack_sample_digest(&before, &digest)
		|| digest != prepare->capability_sample_digest)
		return false;

	result = round_descriptor->close_source_admission(
		prepare->record_generation);
	if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO)
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return false;
	result = round_descriptor->source_logical_debt_zero(
		prepare->record_generation, &proof);
	if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO)
		return true;
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| proof.record_generation != prepare->record_generation
		|| proof.debt_count != 0)
		return false;
	if (round_descriptor != &r11_resource_x_descriptor) {
		result = round_descriptor->source_transport_zero(
			prepare->record_generation, &proof);
		if (result == CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO
			|| result == CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO)
			return true;
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK
			|| proof.record_generation != prepare->record_generation
			|| proof.debt_count != 0)
			return false;
	}

	if (!semantic_activation_snapshot(&current_snapshot)
		|| current_snapshot.seq != snapshot.seq
		|| current_snapshot.active_bits != snapshot.active_bits
		|| current_snapshot.record_generation != snapshot.record_generation
		|| current_snapshot.formation_epoch != snapshot.formation_epoch
		|| current_snapshot.transition_closed != snapshot.transition_closed
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| current_members_lo != prepare->admitted_members_lo
		|| current_members_hi != prepare->admitted_members_hi
		|| current_epoch != prepare->transition_epoch
		|| current_coordinator_node != (int32)prepare->coordinator_node
		|| cluster_ic_local_capability_word() != local_capability_word
		|| !semantic_activation_ack_table_snapshot(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| !semantic_activation_ack_complete_image_current(
			&after, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;

	memcpy(&next, &after, sizeof(next));
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.observed_members_lo = UINT64_C(1);
	next.observed_members_hi = 0;
	next.capability_sample_digest = prepare->capability_sample_digest;
	memset(next.observed, 0, sizeof(next.observed));
	next.observed[0] = self;
	if (!semantic_activation_ack_matches(
			&next.expected[0], &next.observed[0])
		|| !semantic_activation_ack_table_publish(&next))
		return false;

	memset(origin, 0, sizeof(*origin));
	origin->unsent_members_lo = UINT64_C(0x0e);
	origin->active = true;
	return semantic_activation_ack_lmon_send_origin_requests();
}

static bool
semantic_activation_feature_bit_index(uint64 feature_bit, int *feature_index)
{
	int index = 0;

	if (feature_bit == 0 || feature_index == NULL
		|| (feature_bit & (feature_bit - 1)) != 0)
		return false;
	while ((feature_bit & UINT64_C(1)) == 0) {
		feature_bit >>= 1;
		index++;
	}
	*feature_index = index;
	return true;
}

static bool
semantic_activation_feature_index(uint64 feature_bit, int *feature_index)
{
	int index;

	if (!semantic_activation_feature_bit_index(feature_bit, &index)
		|| SemanticActivationDescriptors[index] == NULL)
		return false;
	*feature_index = index;
	return true;
}

static bool
semantic_activation_snapshot(SemanticActivationAdmissionSnapshot *snapshot)
{
	int attempt;

	if (SemanticActivationShmem == NULL || snapshot == NULL)
		return false;

	for (attempt = 0; attempt < CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES; attempt++) {
		uint64 seq_before;
		uint64 seq_after;

		seq_before = pg_atomic_read_u64(&SemanticActivationShmem->admission_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		snapshot->active_bits = pg_atomic_read_u64(&SemanticActivationShmem->active_bits);
		snapshot->record_generation
			= pg_atomic_read_u64(&SemanticActivationShmem->record_generation);
		snapshot->formation_epoch = pg_atomic_read_u64(&SemanticActivationShmem->formation_epoch);
		snapshot->transition_closed
			= pg_atomic_read_u32(&SemanticActivationShmem->transition_closed) != 0;
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(&SemanticActivationShmem->admission_seq);
		if (seq_before == seq_after && (seq_after & UINT64_C(1)) == 0) {
			snapshot->seq = seq_after;
			return true;
		}
	}
	return false;
}

/*
 * Phase-1 two-stage voting-device harness gate.  This is deliberately a
 * read-only exact-zero witness over the existing semantic state: it creates no
 * activation identity and cannot make a later transition admissible.  A table
 * or PGRD publication sequence that ever advanced is non-pristine even when its
 * current payload was invalidated back to empty.
 */
bool
cluster_semantic_activation_phase1_pristine(void)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 empty_table;
	SemanticActivationAdmissionSnapshot snapshot;
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 zero_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 seq_before;
	uint64 seq_after;
	uint32 present;
	uint32 reserved;
	int attempt;

	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.active_bits != 0
		|| snapshot.record_generation != 0
		|| snapshot.formation_epoch != 0
		|| snapshot.transition_closed
		|| !semantic_activation_ack_table_snapshot(&table))
		return false;
	memset(&empty_table, 0, sizeof(empty_table));
	if (memcmp(&table, &empty_table, sizeof(table)) != 0
		|| SemanticActivationPgrdSnapshot == NULL)
		return false;
	memset(zero_bytes, 0, sizeof(zero_bytes));

	for (attempt = 0;
		 attempt < CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES;
		 attempt++) {
		seq_before = pg_atomic_read_u64(
			&SemanticActivationPgrdSnapshot->publication_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		present = pg_atomic_read_u32(
			&SemanticActivationPgrdSnapshot->present);
		reserved = SemanticActivationPgrdSnapshot->reserved;
		memcpy(descriptor_bytes,
			   SemanticActivationPgrdSnapshot->descriptor_bytes,
			   sizeof(descriptor_bytes));
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(
			&SemanticActivationPgrdSnapshot->publication_seq);
		if (seq_before == seq_after && (seq_after & UINT64_C(1)) == 0)
			return seq_after == 0 && present == 0 && reserved == 0
				&& memcmp(descriptor_bytes, zero_bytes,
						  sizeof(descriptor_bytes)) == 0;
	}
	return false;
}

ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *r4_generation_out)
{
	SemanticActivationAdmissionSnapshot snapshot;

	if (r4_generation_out == NULL)
		return RESOURCE_X_WRITER_CLOSED;
	*r4_generation_out = 0;
	if (!semantic_activation_snapshot(&snapshot))
		return RESOURCE_X_WRITER_CLOSED;
	*r4_generation_out = snapshot.record_generation;
	if (snapshot.transition_closed
		|| (snapshot.record_generation != 0
			&& snapshot.formation_epoch != cluster_epoch_get_current()))
		return RESOURCE_X_WRITER_CLOSED;
	if ((snapshot.active_bits
		 & CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1) != 0)
		return RESOURCE_X_WRITER_TARGET;
	return CLUSTER_SEMANTIC_R11_RESOURCE_X_SOURCE_AVAILABLE
		? RESOURCE_X_WRITER_SOURCE
		: RESOURCE_X_WRITER_CLOSED;
}

bool
cluster_semantic_activation_r11_cutover_snapshot(
	ClusterSemanticR11CutoverSnapshot *out)
{
	const uint64 source_bits = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target_bits
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 sample_digest;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation == 0
		|| snapshot.record_generation == UINT64_MAX
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| snapshot.formation_epoch != current_epoch)
		return false;

	if (!snapshot.transition_closed
		&& snapshot.active_bits == target_bits) {
		out->record_generation = snapshot.record_generation;
		out->formation_epoch = snapshot.formation_epoch;
		out->phase = CLUSTER_SEMANTIC_R11_CUTOVER_TARGET_OPEN;
		return true;
	}
	if (!snapshot.transition_closed || snapshot.active_bits != source_bits
		|| !semantic_activation_ack_table_snapshot(&table)
		|| table.coordinator_node != (uint32)current_coordinator_node
		|| table.expected_members_lo != current_members_lo
		|| table.expected_members_hi != current_members_hi
		|| table.transition_epoch != current_epoch
		|| table.source_feature_bitmap != source_bits
		|| table.target_feature_bitmap != target_bits
		|| table.rollback_feature_bitmap != 0)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_expected_image_current(
			&table, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word))
		return false;
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE) {
		if (table.capability_sample_digest != 0
			|| !semantic_activation_ack_sample_digest(&table, &sample_digest)
			|| sample_digest == 0)
			return false;
	} else if (table.capability_sample_digest == 0)
		return false;

	if (table.stage >= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		&& table.stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		&& table.record_generation == snapshot.record_generation) {
		out->record_generation
			= table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
				  ? table.record_generation - 1
				  : table.record_generation;
		if (out->record_generation == 0)
			return false;
		out->formation_epoch = table.transition_epoch;
		out->phase = CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED;
		return true;
	}
	if (table.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		&& snapshot.record_generation + 1 == table.record_generation
		&& table.record_generation > 2
		&& semantic_activation_ack_complete_image_current(
			&table, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, cluster_node_id,
			local_capability_word)) {
		out->record_generation = table.record_generation - 2;
		out->formation_epoch = table.transition_epoch;
		out->phase
			= CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL;
		return true;
	}
	return false;
}

/*
 * A remote member cannot inspect the coordinator's process-local utility
 * mailbox.  It accepts the authenticated current coordinator's SAMPLE nonce
 * only against its own stable D10 durable SOURCE-open projection.  The caller
 * still owns current-formation sampling; this helper remains outside the raw
 * ingress drain until ACK send/retry ownership is installed.
 */
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_sample_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;

	if (item == NULL || snapshot == NULL)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	if ((snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| message->record_generation
		   != snapshot->record_generation + 1
		|| message->source_feature_bitmap != snapshot->active_bits
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| (local_capability_word & required_caps) != required_caps)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	return semantic_activation_ack_lmon_install_authorized_request(
		item, current_members_lo, current_members_hi, current_epoch,
		current_coordinator_node, local_capability_word);
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_barrier_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	int32 local_node_id;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| local_node_id == current_coordinator_node
		|| item->authenticated_source_node_id
		   != current_coordinator_node
		|| message->coordinator_node
		   != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi,
			current_coordinator_node)
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps) != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| message->record_generation
		   != snapshot->record_generation + 1
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != message->record_generation
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	if (current.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER) {
		if (current.flags
				!= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			|| current.capability_sample_digest
			   != message->capability_sample_digest
			|| current.observed_members_lo != 0
			|| current.observed_members_hi != 0
			|| !semantic_activation_bytes_are_zero(
				(const uint8 *)current.observed,
				sizeof(current.observed)))
			return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
		return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
	}
	if (current.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| current.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| current.capability_sample_digest != 0
		|| current.observed_members_lo != current.expected_members_lo
		|| current.observed_members_hi != current.expected_members_hi
		|| !semantic_activation_full_ack_table_matches(
			current.observed, current.observed_members_lo,
			current.observed_members_hi, current.expected,
			current.expected_members_lo, current.expected_members_hi)
		|| !semantic_activation_ack_complete_image_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	memcpy(&next, &current, sizeof(next));
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	next.capability_sample_digest = message->capability_sample_digest;
	memset(next.observed, 0, sizeof(next.observed));
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_prepared_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	int32 local_node_id;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_coordinator_node != 0
		|| local_node_id <= 0 || local_node_id >= 4
		|| item->authenticated_source_node_id != current_coordinator_node
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps)
		   != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| !snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation != message->record_generation
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.stage
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER
		|| current.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != message->record_generation
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest == 0
		|| current.capability_sample_digest
		   != message->capability_sample_digest
		|| current.observed_members_lo != current.expected_members_lo
		|| current.observed_members_hi != current.expected_members_hi
		|| !semantic_activation_ack_complete_image_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	next = current;
	next.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	memset(next.observed, 0, sizeof(next.observed));
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_accept_current_commit_applied_request(
	const SemanticActivationAckIngressItem *item,
	const SemanticActivationAdmissionSnapshot *snapshot,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	uint32 local_capability_word)
{
	ClusterSemanticActivationAckTableV1 current;
	ClusterSemanticActivationAckTableV1 next;
	const ClusterSemanticActivationAckWireV1 *message;
	uint32 required_caps;
	int32 local_node_id;
	uint32 predecessor_stage;
	int node;

	if (item == NULL || snapshot == NULL
		|| !semantic_activation_ack_table_snapshot(&current))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	message = &item->message;
	local_node_id = item->local_receiver_node_id;
	if (!semantic_activation_ack_wire_value_valid(message)
		|| message->kind
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| (message->stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
			&& message->stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		|| message->result
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
		|| current_members_lo != UINT64_C(0x0f)
		|| current_members_hi != 0
		|| current_coordinator_node != 0
		|| local_node_id <= 0 || local_node_id >= 4
		|| item->authenticated_source_node_id != current_coordinator_node
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)local_node_id
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| !semantic_activation_ack_round_required_caps(
			message->source_feature_bitmap,
			message->target_feature_bitmap,
			message->rollback_feature_bitmap, &required_caps)
		|| cluster_membership_get_state(current_coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| item->sampled_capability_generation == 0
		|| (item->sampled_capability_word & required_caps)
		   != required_caps
		|| (local_capability_word & required_caps) != required_caps
		|| !cluster_sf_peer_capability_generation_matches(
			current_coordinator_node, required_caps,
			item->sampled_capability_generation)
		|| (snapshot->seq & UINT64_C(1)) != 0
		|| !snapshot->transition_closed
		|| snapshot->formation_epoch != current_epoch
		|| snapshot->record_generation == UINT64_MAX
		|| snapshot->record_generation + 1
		   != message->record_generation
		|| snapshot->active_bits != message->source_feature_bitmap
		|| current.stage
		   != (message->stage
				   == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
				   ? CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
				   : CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
		|| current.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| current.coordinator_node != message->coordinator_node
		|| current.round_nonce != message->round_nonce
		|| current.expected_members_lo != message->admitted_members_lo
		|| current.expected_members_hi != message->admitted_members_hi
		|| current.transition_epoch != message->transition_epoch
		|| current.record_generation != snapshot->record_generation
		|| current.source_feature_bitmap
		   != message->source_feature_bitmap
		|| current.target_feature_bitmap
		   != message->target_feature_bitmap
		|| current.rollback_feature_bitmap
		   != message->rollback_feature_bitmap
		|| current.capability_sample_digest == 0
		|| current.capability_sample_digest
		   != message->capability_sample_digest
		|| current.observed_members_lo != current.expected_members_lo
		|| current.observed_members_hi != current.expected_members_hi
		|| !semantic_activation_ack_complete_image_current(
			&current, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;

	predecessor_stage = current.stage;
	next = current;
	next.stage = message->stage;
	next.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	next.record_generation = message->record_generation;
	next.observed_members_lo = 0;
	next.observed_members_hi = 0;
	memset(next.observed, 0, sizeof(next.observed));
	for (node = 0; node < 4; node++)
		next.expected[node].record_generation = message->record_generation;
	/* Preserve an explicit predecessor-stage read in this shared COMMIT/OPEN
	 * validator so a future stage extension cannot silently skip one link. */
	if ((message->stage
		 == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
		 && predecessor_stage
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED)
		|| (message->stage
			== CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
			&& predecessor_stage
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	if (!semantic_activation_ack_expected_image_current(
			&next, current_members_lo, current_members_hi,
			current_epoch, current_coordinator_node, local_node_id,
			local_capability_word))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	return semantic_activation_ack_table_publish(&next)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static bool
semantic_activation_counter_increment(pg_atomic_uint32 *counter)
{
	int attempt;
	uint32 observed;

	observed = pg_atomic_read_u32(counter);
	for (attempt = 0; attempt < CLUSTER_SEMANTIC_ADMISSION_COUNTER_TRIES; attempt++) {
		uint32 expected = observed;

		if (observed == UINT32_MAX)
			return false;
		if (pg_atomic_compare_exchange_u32(counter, &expected, observed + 1))
			return true;
		observed = expected;
	}
	return false;
}

static void
semantic_activation_counter_subtract(pg_atomic_uint32 *counter, uint32 amount)
{
	uint32 observed = pg_atomic_read_u32(counter);

	for (;;) {
		uint32 expected = observed;

		if (amount == 0)
			return;
		if (observed < amount)
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("semantic activation admission debt underflow"),
							errhint("Restart the failed process and retain the shared-memory image "
									"for diagnosis.")));
		if (pg_atomic_compare_exchange_u32(counter, &expected, observed - amount))
			return;
		observed = expected;
	}
}

static void
semantic_activation_exit_cleanup(int code, Datum arg)
{
	int side;
	int feature_index;
	int registered_pid = DatumGetInt32(arg);

	(void)code;
	if (registered_pid != MyProcPid || semantic_activation_exit_hook_pid != MyProcPid
		|| SemanticActivationShmem == NULL)
		return;

	HOLD_INTERRUPTS();
	for (side = 0; side < 2; side++) {
		for (feature_index = 0; feature_index < 64; feature_index++) {
			uint32 local = semantic_activation_local_inflight[side][feature_index];
			uint32 shared;

			if (local == 0)
				continue;
			shared = pg_atomic_read_u32(&SemanticActivationShmem->inflight[side][feature_index]);
			if (shared < local)
				ereport(
					PANIC,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("semantic activation exit debt is inconsistent"),
					 errhint("Retain the shared-memory image and restart the failed process.")));
		}
	}
	for (side = 0; side < 2; side++) {
		for (feature_index = 0; feature_index < 64; feature_index++) {
			uint32 local = semantic_activation_local_inflight[side][feature_index];

			if (local == 0)
				continue;
			semantic_activation_counter_subtract(
				&SemanticActivationShmem->inflight[side][feature_index], local);
			semantic_activation_local_inflight[side][feature_index] = 0;
		}
	}
	semantic_activation_exit_hook_pid = 0;
	RESUME_INTERRUPTS();
}

static bool
semantic_activation_ensure_exit_hook(void)
{
	if (MyProcPid <= 0)
		return false;
	if (semantic_activation_exit_hook_pid == MyProcPid)
		return true;

	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	on_shmem_exit(semantic_activation_exit_cleanup, Int32GetDatum(MyProcPid));
	semantic_activation_exit_hook_pid = MyProcPid;
	return true;
}

static void
semantic_activation_release_debt(ClusterSemanticAdmissionSide side, int feature_index)
{
	uint32 *local = &semantic_activation_local_inflight[side][feature_index];

	if (*local == 0)
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("semantic activation local debt is missing"),
						errhint("Retain the process and shared-memory state for diagnosis.")));
	semantic_activation_counter_subtract(&SemanticActivationShmem->inflight[side][feature_index],
										 1);
	(*local)--;
}

static ClusterSemanticAdmissionResult
semantic_activation_enter_internal(uint64 feature_bit,
	ClusterSemanticAdmissionSide side, ClusterSemanticAdmissionToken *token,
	bool terminal_census)
{
	SemanticActivationAdmissionSnapshot before;
	SemanticActivationAdmissionSnapshot after;
	ClusterSemanticAdmissionResult result;
	uint64 epoch_before;
	uint64 epoch_after;
	int feature_index;
	bool incremented = false;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL || SemanticActivationShmem == NULL
		|| (side != CLUSTER_SEMANTIC_SOURCE_SIDE && side != CLUSTER_SEMANTIC_TARGET_SIDE)
		|| !semantic_activation_feature_index(feature_bit, &feature_index)
		|| !semantic_activation_ensure_exit_hook())
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	epoch_before = cluster_epoch_get_current();
	if (!semantic_activation_snapshot(&before))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (before.formation_epoch != epoch_before)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	if (terminal_census)
		result = semantic_activation_r4_terminal_census_policy(
			feature_bit, before.transition_closed, side,
			before.record_generation, before.record_generation);
	else
		result = semantic_activation_admission_policy(
			feature_bit, before.active_bits, before.transition_closed, side,
			before.record_generation, before.record_generation);
	if (result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return result;

	HOLD_INTERRUPTS();
	if (semantic_activation_local_inflight[side][feature_index] != UINT32_MAX
		&& semantic_activation_counter_increment(
			&SemanticActivationShmem->inflight[side][feature_index])) {
		semantic_activation_local_inflight[side][feature_index]++;
		incremented = true;
	}
	pg_write_barrier();
	RESUME_INTERRUPTS();
	if (!incremented)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	if (!semantic_activation_snapshot(&after))
		result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	else {
		epoch_after = cluster_epoch_get_current();
		if (before.seq != after.seq || before.record_generation != after.record_generation
			|| before.formation_epoch != after.formation_epoch || epoch_before != epoch_after
			|| after.formation_epoch != epoch_after)
			result = CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
		else if (terminal_census)
			result = semantic_activation_r4_terminal_census_policy(
				feature_bit, after.transition_closed, side,
				before.record_generation, after.record_generation);
		else
			result = semantic_activation_admission_policy(
				feature_bit, after.active_bits, after.transition_closed, side,
				before.record_generation, after.record_generation);
	}
	if (result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		HOLD_INTERRUPTS();
		semantic_activation_release_debt(side, feature_index);
		RESUME_INTERRUPTS();
		return result;
	}

	token->feature_bit = feature_bit;
	token->record_generation = before.record_generation;
	token->formation_epoch = before.formation_epoch;
	token->side = (uint8)side;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit,
	ClusterSemanticAdmissionSide side, ClusterSemanticAdmissionToken *token)
{
	return semantic_activation_enter_internal(feature_bit, side, token, false);
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(
	ClusterSemanticAdmissionToken *token)
{
	return semantic_activation_enter_internal(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, token, true);
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable_admission,
									   ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterSemanticAdmissionSide side;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL || !semantic_activation_snapshot(&snapshot))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	/*
	 * Before the first PGSA record, the ordinary-join write gate is the
	 * durable discriminator between a steady SOURCE member and a replacement
	 * MEMBER that is deliberately still closed.  We still take D10 debt so a
	 * later close can drain already-running ordinary modifiers.
	 */
	if (snapshot.record_generation == 0) {
		if (snapshot.active_bits != 0 || !writable_admission)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		return semantic_activation_modifier_enter_bootstrap(writable_admission, token);
	} else {
		if (!semantic_activation_modifier_policy(
				snapshot.active_bits, snapshot.record_generation, snapshot.transition_closed))
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		side = (snapshot.active_bits & CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0
				   ? CLUSTER_SEMANTIC_TARGET_SIDE
				   : CLUSTER_SEMANTIC_SOURCE_SIDE;
	}

	return cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, side, token);
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
									 bool writable_admission)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;

	if (token == NULL || !token->entered || token->side > CLUSTER_SEMANTIC_TARGET_SIDE
		|| !semantic_activation_snapshot(&snapshot))
		return false;
	current_epoch = cluster_epoch_get_current();
	if (snapshot.record_generation != token->record_generation
		|| snapshot.formation_epoch != token->formation_epoch
		|| current_epoch != token->formation_epoch)
		return false;
	if (snapshot.record_generation == 0)
		return token->side == CLUSTER_SEMANTIC_SOURCE_SIDE && snapshot.active_bits == 0
			   && writable_admission;
	return semantic_activation_modifier_policy(
		snapshot.active_bits, snapshot.record_generation, snapshot.transition_closed);
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;
	int feature_index;

	if (token == NULL || !token->entered)
		return false;
	if (!semantic_activation_feature_index(token->feature_bit, &feature_index)
		|| token->side > CLUSTER_SEMANTIC_TARGET_SIDE || SemanticActivationShmem == NULL)
		return false;
	(void)feature_index;
	if (!semantic_activation_snapshot(&snapshot))
		return false;
	current_epoch = cluster_epoch_get_current();
	if (snapshot.formation_epoch != current_epoch || token->formation_epoch != current_epoch)
		return false;

	return semantic_activation_admission_policy(
			   token->feature_bit, snapshot.active_bits, snapshot.transition_closed,
			   (ClusterSemanticAdmissionSide)token->side, token->record_generation,
			   snapshot.record_generation)
		   == CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;

	if (token == NULL || !token->entered
		|| token->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| SemanticActivationShmem == NULL
		|| !semantic_activation_snapshot(&snapshot))
		return false;
	current_epoch = cluster_epoch_get_current();
	if (snapshot.formation_epoch != current_epoch
		|| token->formation_epoch != current_epoch)
		return false;

	return semantic_activation_r4_terminal_census_policy(
			   token->feature_bit, snapshot.transition_closed,
			   (ClusterSemanticAdmissionSide)token->side,
			   token->record_generation, snapshot.record_generation)
		   == CLUSTER_SEMANTIC_ADMISSION_OK;
}

static void
semantic_activation_pgrd_snapshot_clear(void)
{
	uint64 seq;

	if (SemanticActivationPgrdSnapshot == NULL)
		return;
	seq = pg_atomic_read_u64(
		&SemanticActivationPgrdSnapshot->publication_seq);
	if ((seq & UINT64_C(1)) != 0) {
		pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 0);
		return;
	}
	if (seq > UINT64_MAX - 2) {
		pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						UINT64_MAX);
		pg_write_barrier();
		pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 0);
		return;
	}

	pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						seq + 1);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 0);
	memset(SemanticActivationPgrdSnapshot->descriptor_bytes, 0,
		   sizeof(SemanticActivationPgrdSnapshot->descriptor_bytes));
	SemanticActivationPgrdSnapshot->reserved = 0;
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						seq + 2);
}

static bool
semantic_activation_pgrd_snapshot_publish(
	const uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint64 seq;

	if (SemanticActivationPgrdSnapshot == NULL || descriptor_bytes == NULL
		|| cluster_undo_root_descriptor_decode(
			   descriptor_bytes, GetSystemIdentifier(), &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
		|| descriptor.owner_node != -1)
		return false;
	seq = pg_atomic_read_u64(
		&SemanticActivationPgrdSnapshot->publication_seq);
	if ((seq & UINT64_C(1)) != 0)
		return false;
	if (seq > UINT64_MAX - 2) {
		pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						UINT64_MAX);
		pg_write_barrier();
		pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 0);
		return false;
	}

	pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						seq + 1);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 0);
	memcpy(SemanticActivationPgrdSnapshot->descriptor_bytes,
		   descriptor_bytes,
		   sizeof(SemanticActivationPgrdSnapshot->descriptor_bytes));
	SemanticActivationPgrdSnapshot->reserved = 0;
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationPgrdSnapshot->present, 1);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationPgrdSnapshot->publication_seq,
						seq + 2);
	return true;
}

static bool
semantic_activation_pgrd_snapshot_copy(
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	uint64 seq_before;
	uint64 seq_after;
	uint32 reserved;
	int attempt;

	if (SemanticActivationPgrdSnapshot == NULL || descriptor_bytes == NULL)
		return false;
	for (attempt = 0; attempt < CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES;
		 attempt++) {
		seq_before = pg_atomic_read_u64(
			&SemanticActivationPgrdSnapshot->publication_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		if (pg_atomic_read_u32(&SemanticActivationPgrdSnapshot->present)
			!= 1)
			return false;
		memcpy(descriptor_bytes,
			   SemanticActivationPgrdSnapshot->descriptor_bytes,
			   CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES);
		reserved = SemanticActivationPgrdSnapshot->reserved;
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(
			&SemanticActivationPgrdSnapshot->publication_seq);
		if (seq_before == seq_after
			&& (seq_after & UINT64_C(1)) == 0)
			return reserved == 0;
	}
	return false;
}

static bool
semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out, bool terminal_census)
{
	ClusterUndoBlock0ResolvedRoot resolved;
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (token == NULL || out == NULL || !token->entered
		|| token->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| !(terminal_census
			 ? cluster_semantic_activation_recheck_r4_terminal_census(token)
			 : cluster_semantic_activation_recheck(token))
		|| !semantic_activation_pgrd_snapshot_copy(descriptor_bytes)
		|| cluster_undo_root_descriptor_decode(
			   descriptor_bytes, GetSystemIdentifier(), &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| !cluster_undo_root_descriptor_resolve(
			&descriptor, intent, owner_instance, segment_id, &resolved)
		|| !(terminal_census
			 ? cluster_semantic_activation_recheck_r4_terminal_census(token)
			 : cluster_semantic_activation_recheck(token)))
		return false;

	*out = resolved;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	return semantic_activation_resolve_shared_undo_root(
		token, intent, owner_instance, segment_id, out, false);
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	if (intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED)
		return false;
	return semantic_activation_resolve_shared_undo_root(
		token, intent, owner_instance, segment_id, out, true);
}

bool
cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	ClusterUndoBlock0ResolvedRoot resolved;
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (token == NULL || out == NULL || !token->entered
		|| token->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| token->side != CLUSTER_SEMANTIC_SOURCE_SIDE
		|| intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| cluster_node_id < 0
		|| owner_instance != (uint32)cluster_node_id + 1
		|| !cluster_semantic_activation_recheck(token)
		|| !semantic_activation_pgrd_snapshot_copy(descriptor_bytes)
		|| cluster_undo_root_descriptor_decode(
			descriptor_bytes, GetSystemIdentifier(), &descriptor)
			!= CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| !cluster_undo_root_descriptor_resolve(
			&descriptor, intent, owner_instance, segment_id, &resolved)
		|| !cluster_semantic_activation_recheck(token))
		return false;

	*out = resolved;
	return true;
}

/*
 * cluster_semantic_activation_restore_open_proof_if_active -- R4 cutover contract
 *	contract (verified implementation): restart/reformation OPEN_PROOF
 *	reconstruction.
 *
 *	The volatile ACK table is zeroed at postmaster start and is neither
 *	WAL-logged nor durable (§4), so a post-bit22 restart/reformation must
 *	re-run the recovery-only OPEN_APPLIED bootstrap from the
 *	majority-selected durable OPEN: "every member first reads the same
 *	durable OPEN; a complete current observed image is then promoted to
 *	expected and OPEN_PROOF atomically."  This is a local proof
 *	reconstruction — no wire frame, no ACK inheritance, not a durable FSM
 *	phase.  The identity is epoch + generation; no pre-restart nonce or
 *	row is assumed.
 *
 *	Fail-closed: absent latch / unreadable OPEN / ACTIVE-root binding
 *	refusal / formation or membership drift leaves the table without
 *	OPEN_PROOF and the post-bit22 gates closed (the LMON tick retries).
 *	Idempotent: once the table carries OPEN_PROOF the function returns
 *	true without mutation.
 */
bool
cluster_semantic_activation_restore_open_proof_if_active(void)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 empty_table;
	ClusterSemanticActivationAckTableV1 rebuilt;
	ClusterSemanticActivationRecord open;
	SemanticActivationAckTuple self;
	ClusterSemanticActivationResult qv_result;
	ClusterControlRootResult root_result;
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;
	bool implicit_open = false;
	int node;

	if (SemanticActivationAckTable == NULL
		|| !cluster_r4_bit22_cutover_active()
		|| !semantic_activation_ack_table_snapshot(&table))
		return false;
	if ((table.flags & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) != 0)
		return true;
	/* Reconstruction is authorized only for the byte-zero volatile image
	 * created at postmaster start.  A nonempty table belongs to a live or
	 * invalidated protocol round; in particular, never add the recovery-only
	 * OPEN_PROOF flag to an ordinary Resource-X OPEN_APPLIED carrier. */
	memset(&empty_table, 0, sizeof(empty_table));
	if (memcmp(&table, &empty_table, sizeof(table)) != 0)
		return false;

	/* The DURABLE Target OPEN proof: a strict-majority OPEN record on the
	 * voting disks (the cutover round's final record), cross-matched to
	 * the ACTIVE canonical root's round identity — the same validation as
	 * the latch restore (contract).  `implicit_open` is NOT a gate: it
	 * only reports the all-zero pre-R4 sentinel, while a real durable
	 * OPEN(P+2) record reads back nonzero. */
	qv_result = cluster_qvotec_bootstrap_read_semantic_activation(
		selected, &implicit_open);
	(void) implicit_open;
	if (qv_result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !cluster_semantic_activation_record_decode(selected, &open, NULL)
		|| open.phase != CLUSTER_SEMANTIC_PHASE_OPEN
		|| open.record_generation < 3)
		return false;
	root_result = cluster_control_root_bootstrap_validate_active_round_fields(
		open.transition_epoch, open.record_generation - 2,
		open.source_feature_bitmap, open.target_feature_bitmap);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return false;

	/* Current formation/member revalidation (§4). */
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &members_lo, &members_hi, &formation_epoch,
			&current_coordinator_node)
		|| members_lo != open.admitted_members_lo
		|| members_hi != open.admitted_members_hi
		|| (uint32) current_coordinator_node != open.coordinator_node
		|| formation_epoch != open.transition_epoch)
		return false;
	current_epoch = cluster_epoch_get_current();
	if (formation_epoch != current_epoch)
		return false;
	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_self_tuple(
			cluster_node_id, local_capability_word,
			open.transition_epoch, open.record_generation, &self))
		return false;

	memset(&rebuilt, 0, sizeof(rebuilt));
	rebuilt.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	rebuilt.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE
					| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF;
	rebuilt.coordinator_node = open.coordinator_node;
	rebuilt.round_nonce = 1;	/* reconstruction: no pre-restart nonce
								 * survives; identity is epoch+generation */
	rebuilt.transition_epoch = open.transition_epoch;
	rebuilt.record_generation = open.record_generation;
	rebuilt.expected_members_lo = open.admitted_members_lo;
	rebuilt.expected_members_hi = open.admitted_members_hi;
	rebuilt.observed_members_lo = open.admitted_members_lo;
	rebuilt.observed_members_hi = open.admitted_members_hi;
	rebuilt.source_feature_bitmap = open.source_feature_bitmap;
	rebuilt.target_feature_bitmap = open.target_feature_bitmap;
	rebuilt.rollback_feature_bitmap = open.rollback_feature_bitmap;
	rebuilt.capability_sample_digest = open.capability_sample_digest;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		SemanticActivationAckTuple remote;
		uint32 peer_word;
		uint32 peer_gen;

		if (!semantic_activation_ack_member_present(
				members_lo, members_hi, node))
			continue;
		if (node == cluster_node_id) {
			rebuilt.expected[node] = self;
			rebuilt.observed[node] = self;
			continue;
		}
		memset(&remote, 0, sizeof(remote));
		remote.node_id = (uint32) node;
		remote.boot_id
			= cluster_membership_get_last_admitted_incarnation(node);
		remote.admitted_incarnation = remote.boot_id;
		if (remote.boot_id == 0
			|| !cluster_sf_peer_capability_word_sample(
				node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
				&peer_word, &peer_gen)
			|| peer_gen == 0)
			return false;
		remote.control_connection_generation = (uint64) peer_gen;
		remote.capability_word = peer_word;
		remote.capability_generation = (uint64) peer_gen;
		remote.transition_epoch = open.transition_epoch;
		remote.record_generation = open.record_generation;
		rebuilt.expected[node] = remote;
		rebuilt.observed[node] = remote;
	}
	if (!semantic_activation_ack_table_publish(&rebuilt))
		return false;
	ereport(LOG,
			(errmsg("bit22 cutover: OPEN_PROOF reconstructed from durable "
					"OPEN (gen %llu, epoch %llu) — post-bit22 gates armed",
					(unsigned long long) open.record_generation,
					(unsigned long long) open.transition_epoch)));
	return true;
}

bool
cluster_semantic_activation_peer_open_matches(
	const ClusterSemanticAdmissionToken *token, int32 authenticated_peer_node_id,
	uint32 required_hello_caps, uint32 sampled_capability_generation)
{
	ClusterSemanticActivationAckTableV1 table;

	if (token == NULL || !token->entered
		|| token->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE || authenticated_peer_node_id < 0
		|| authenticated_peer_node_id >= CLUSTER_MAX_NODES || required_hello_caps == 0)
		return false;
	if (!cluster_semantic_activation_recheck(token))
		return false;
	if (!cluster_sf_peer_capability_generation_matches(
			authenticated_peer_node_id, required_hello_caps, sampled_capability_generation))
		return false;

	/* R4 cutover contract §9: only a stable local table with OPEN_PROOF for the
	 * entered TARGET token's exact record generation and formation epoch
	 * authorizes a peer-positive path; the authenticated peer must be in
	 * the exact member bitmap with its observed tuple byte-equal to its
	 * expected row.  Missing storage, local self, non-OPEN stage or any
	 * drift returns false without mutation. */
	if (SemanticActivationAckTable == NULL
		|| !semantic_activation_ack_table_snapshot(&table)
		|| (table.flags & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) == 0
		|| table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| table.record_generation != token->record_generation
		|| table.transition_epoch != token->formation_epoch
		|| !semantic_activation_ack_member_present(
			table.expected_members_lo, table.expected_members_hi,
			authenticated_peer_node_id)
		|| !semantic_activation_ack_matches(
			&table.observed[authenticated_peer_node_id],
			&table.expected[authenticated_peer_node_id]))
		return false;
	return true;
}

/* Resource-X owns a distinct target-only admission bit.  Its peer proof is
 * the exact ordinary R11 OPEN_APPLIED carrier, not the recovery-only R4
 * OPEN_PROOF flag.  Revalidate the complete current four-member image before
 * accepting the authenticated peer generation so a partial or cross-round
 * ACK table never authorizes kind-9 traffic. */
ClusterSemanticResourceXPeerOpenResult
cluster_semantic_activation_resource_x_peer_open_check(
	const ClusterSemanticAdmissionToken *token,
	int32 authenticated_peer_node_id,
	uint32 sampled_capability_generation)
{
	const uint64 source_bits = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target_bits
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 table;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;
	ClusterSfPeerCap capability;
	ClusterSemanticResourceXPeerOpenResult image_result;
	bool capability_pending;

	if (token == NULL || !token->entered
		|| token->feature_bit
		   != CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| authenticated_peer_node_id < 0
		|| authenticated_peer_node_id >= CLUSTER_MAX_NODES
		|| sampled_capability_generation == 0)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_INVALID_INPUT;
	if (!cluster_semantic_activation_recheck(token))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_ADMISSION_DRIFT;
	if (!cluster_sf_peer_capability_record_snapshot(authenticated_peer_node_id, &capability))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_INVALID_INPUT;
	capability_pending = !capability.valid;
	if (!capability_pending
		&& ((capability.bits & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) == 0
			|| capability.generation != sampled_capability_generation))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT;
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_AUTHORITY_UNAVAILABLE;
	if (SemanticActivationAckTable == NULL)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_UNAVAILABLE;
	if (!semantic_activation_ack_table_snapshot(&table))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_TORN;
	if (table.stage
		!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_STAGE_MISMATCH;
	if (table.flags
		!= (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FLAGS_MISMATCH;
	if (table.record_generation != token->record_generation)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_RECORD_MISMATCH;
	if (table.transition_epoch != token->formation_epoch)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FORMATION_MISMATCH;
	if (table.source_feature_bitmap != source_bits
		|| table.target_feature_bitmap != target_bits
		|| table.rollback_feature_bitmap != 0)
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FEATURE_MISMATCH;
	if (!semantic_activation_ack_member_present(
			table.expected_members_lo, table.expected_members_hi,
			authenticated_peer_node_id))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_PEER_ABSENT;
	if (!semantic_activation_ack_matches(
			&table.observed[authenticated_peer_node_id],
			&table.expected[authenticated_peer_node_id]))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_PEER_ROW_MISMATCH;

	local_capability_word = cluster_ic_local_capability_word();
	image_result = semantic_activation_ack_complete_image_check(
		&table, current_members_lo, current_members_hi, current_epoch, current_coordinator_node,
		cluster_node_id, local_capability_word);
	if (image_result != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH
		&& image_result != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING)
		return image_result;
	return capability_pending ? CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_NOT_READY
							  : image_result;
}

bool
cluster_semantic_activation_resource_x_peer_open_matches(
	const ClusterSemanticAdmissionToken *token,
	int32 authenticated_peer_node_id,
	uint32 sampled_capability_generation)
{
	return cluster_semantic_activation_resource_x_peer_open_check(
		token, authenticated_peer_node_id,
		sampled_capability_generation)
		== CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	int feature_index;

	if (token == NULL || !token->entered)
		return;
	if (SemanticActivationShmem == NULL || token->side > CLUSTER_SEMANTIC_TARGET_SIDE
		|| !semantic_activation_feature_index(token->feature_bit, &feature_index))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("semantic activation token is invalid"),
				 errhint("Retain the process and shared-memory state for diagnosis.")));

	HOLD_INTERRUPTS();
	semantic_activation_release_debt((ClusterSemanticAdmissionSide)token->side, feature_index);
	memset(token, 0, sizeof(*token));
	RESUME_INTERRUPTS();
}

Size
cluster_semantic_activation_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterSemanticActivationShmem))
		   + MAXALIGN(sizeof(ClusterSemanticActivationUtilityMailboxShmem))
		   + MAXALIGN(sizeof(ClusterSemanticActivationAckTableV1))
		   + MAXALIGN(sizeof(ClusterSemanticActivationPgrdSnapshotShmem))
		   + MAXALIGN(sizeof(ClusterR4Bit22CutoverLatchShmem))
		   + MAXALIGN(sizeof(ClusterR4Bit22CutoverSeamShmem))
		   + MAXALIGN(sizeof(ClusterR4Bit22SourceCloseShmem));
}

void
cluster_semantic_activation_shmem_init(void)
{
	bool gate_found;
	bool mailbox_found;
	bool ack_table_found;
	bool pgrd_snapshot_found;
	bool latch_found;
	bool seam_found;
	bool source_close_found;
	int side;
	int feature_index;

	SemanticActivationShmem = (ClusterSemanticActivationShmem *)ShmemInitStruct(
		"pgrac cluster semantic activation gate",
		MAXALIGN(sizeof(ClusterSemanticActivationShmem)), &gate_found);
	SemanticActivationUtilityMailbox
		= (ClusterSemanticActivationUtilityMailboxShmem *)ShmemInitStruct(
			"pgrac cluster semantic activation utility mailbox",
			MAXALIGN(sizeof(ClusterSemanticActivationUtilityMailboxShmem)),
			&mailbox_found);
	SemanticActivationAckTable
		= (ClusterSemanticActivationAckTableV1 *)ShmemInitStruct(
			"pgrac cluster semantic activation ACK table",
			MAXALIGN(sizeof(ClusterSemanticActivationAckTableV1)),
			&ack_table_found);
	SemanticActivationPgrdSnapshot
		= (ClusterSemanticActivationPgrdSnapshotShmem *)ShmemInitStruct(
			"pgrac cluster semantic activation PGRD snapshot",
			MAXALIGN(sizeof(ClusterSemanticActivationPgrdSnapshotShmem)),
			&pgrd_snapshot_found);
	SemanticActivationBit22Latch
		= (ClusterR4Bit22CutoverLatchShmem *)ShmemInitStruct(
			"pgrac cluster r4 bit22 cutover latch",
			MAXALIGN(sizeof(ClusterR4Bit22CutoverLatchShmem)),
			&latch_found);
	SemanticActivationBit22Seam
		= (ClusterR4Bit22CutoverSeamShmem *)ShmemInitStruct(
			"pgrac cluster r4 bit22 cutover seam",
			MAXALIGN(sizeof(ClusterR4Bit22CutoverSeamShmem)),
			&seam_found);
	SemanticActivationBit22SourceClose
		= (ClusterR4Bit22SourceCloseShmem *)ShmemInitStruct(
			"pgrac cluster r4 bit22 source close",
			MAXALIGN(sizeof(ClusterR4Bit22SourceCloseShmem)),
			&source_close_found);
	if (SemanticActivationShmem == NULL
		|| SemanticActivationUtilityMailbox == NULL
		|| SemanticActivationAckTable == NULL
		|| SemanticActivationPgrdSnapshot == NULL
		|| SemanticActivationBit22Latch == NULL
		|| SemanticActivationBit22Seam == NULL
		|| SemanticActivationBit22SourceClose == NULL)
		return;
	if (!ack_table_found) {
		memset(SemanticActivationAckTable, 0,
			   sizeof(*SemanticActivationAckTable));
		pg_atomic_init_u64(&SemanticActivationAckTable->publication_seq, 0);
	}
	if (!pgrd_snapshot_found) {
		memset(SemanticActivationPgrdSnapshot, 0,
			   sizeof(*SemanticActivationPgrdSnapshot));
		pg_atomic_init_u64(
			&SemanticActivationPgrdSnapshot->publication_seq, 0);
		pg_atomic_init_u32(&SemanticActivationPgrdSnapshot->present, 0);
	}

	if (!gate_found) {
		pg_atomic_init_u64(&SemanticActivationShmem->record_cas_request_seq, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->record_cas_completion_seq, 0);
		pg_atomic_init_u32(&SemanticActivationShmem->record_cas_result,
						   CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
		pg_atomic_init_u32(&SemanticActivationShmem->record_cas_request_kind,
						   CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE);
		SemanticActivationShmem->record_cas_expected_generation = 0;
		SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 0;
		memset(SemanticActivationShmem->record_cas_desired_bytes, 0,
			   sizeof(SemanticActivationShmem->record_cas_desired_bytes));
		pg_atomic_init_u64(&SemanticActivationShmem->admission_seq, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->active_bits, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->record_generation, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->formation_epoch, 0);
		pg_atomic_init_u32(&SemanticActivationShmem->transition_closed, 1);
		for (side = 0; side < 2; side++) {
			for (feature_index = 0; feature_index < 64; feature_index++)
				pg_atomic_init_u32(
					&SemanticActivationShmem->inflight[side][feature_index], 0);
		}
	}
	if (!mailbox_found) {
		pg_atomic_init_u64(
			&SemanticActivationUtilityMailbox->utility_request_seq, 0);
		pg_atomic_init_u64(
			&SemanticActivationUtilityMailbox->utility_completion_seq, 0);
		pg_atomic_init_u32(
			&SemanticActivationUtilityMailbox->utility_mailbox_state,
			SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
		SemanticActivationUtilityMailbox->utility_action
			= CLUSTER_SEMANTIC_ENABLE_ALL;
		SemanticActivationUtilityMailbox->utility_source_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_target_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_expected_record_generation = 0;
		pg_atomic_init_u32(&SemanticActivationUtilityMailbox->utility_result,
						   CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
		SemanticActivationUtilityMailbox->utility_result_feature_bit = 0;
		SemanticActivationUtilityMailbox->utility_result_expected_generation = 0;
	}
	if (!latch_found) {
		pg_atomic_init_u32(&SemanticActivationBit22Latch->active, 0);
		SemanticActivationBit22Latch->reserved = 0;
		pg_atomic_init_u64(&SemanticActivationBit22Latch->transition_epoch, 0);
		pg_atomic_init_u64(&SemanticActivationBit22Latch->round_generation, 0);
	}
	if (!source_close_found) {
		pg_atomic_init_u32(&SemanticActivationBit22SourceClose->closed, 0);
		pg_atomic_init_u32(&SemanticActivationBit22SourceClose->writer_count, 0);
		pg_atomic_init_u64(&SemanticActivationBit22SourceClose->transition_epoch, 0);
		pg_atomic_init_u64(&SemanticActivationBit22SourceClose->prepare_generation, 0);
	}
	if (!seam_found) {
		pg_atomic_init_u32(&SemanticActivationBit22Seam->valid, 0);
		memset(&SemanticActivationBit22Seam->file_token, 0,
			   sizeof(SemanticActivationBit22Seam->file_token));
		memset(SemanticActivationBit22Seam->round_sha, 0,
			   sizeof(SemanticActivationBit22Seam->round_sha));
		memset(&SemanticActivationBit22Seam->round, 0,
			   sizeof(SemanticActivationBit22Seam->round));
	}
}

/*
 * cluster_r4_bit22_cutover_active -- RF-ROOT P7 (contract §B, follow-up contract
 *	②): the dual-path reader gate idiom anchor.  Lock-free atomic read; the
 *	census gate modeling recognizes this exact call as the gate.  Fail-closed:
 *	shmem absent (early startup / unattached) reads as pre-bit22.
 */
bool
cluster_r4_bit22_cutover_active(void)
{
	/* Reader gate: TARGET_BOOTSTRAP (1) and TARGET_VERIFIED (2) both
	 * select the root. */
	return SemanticActivationBit22Latch != NULL
		&& pg_atomic_read_u32(&SemanticActivationBit22Latch->active) != 0;
}

/*
 * cluster_r4_bit22_cutover_verified -- RF-ROOT P9 verification (implementation):
 * serving/admission gate.  Only TARGET_VERIFIED (2) — the phase-4 CF(S)
 * strong revalidation succeeded — allows ordinary serving.
 */
bool
cluster_r4_bit22_cutover_verified(void)
{
	return SemanticActivationBit22Latch != NULL
		&& pg_atomic_read_u32(&SemanticActivationBit22Latch->active)
		   == CLUSTER_R4_BIT22_TARGET_VERIFIED;
}

/*
 * cluster_r4_bit22_cutover_latch_verify -- RF-ROOT P9 verification (implementation):
 * upgrade TARGET_BOOTSTRAP -> TARGET_VERIFIED once the phase-4 CF(S)
 * strong revalidation of the ACTIVE root succeeded.  Idempotent; a latch
 * at SOURCE (not yet restored) is left untouched.
 */
bool
cluster_r4_bit22_cutover_latch_verify(void)
{
	uint32 expected;

	if (SemanticActivationBit22Latch == NULL)
		return false;
	expected = CLUSTER_R4_BIT22_TARGET_BOOTSTRAP;
	if (!pg_atomic_compare_exchange_u32(&SemanticActivationBit22Latch->active,
										&expected,
										CLUSTER_R4_BIT22_TARGET_VERIFIED))
		return false;
	return true;
}

/*
 * cluster_r4_bit22_cutover_latch_apply -- the one-shot latch setter.  Wired
 *	by the bit22 first-open round (contract §E): a node latches when its
 *	cutover FSM reaches OPEN_APPLIED bound to the round identity.  The 0->1
 *	CAS is the publication: only the winning call records the round identity
 *	(identity fields are observability-only; the gate reads `active` alone),
 *	so a losing apply never overwrites the bound round.  Monotonic: a second
 *	apply (any round) is rejected.  Returns true iff this call flipped the
 *	latch.
 *
 *	contract follow-up contract ②: the runtime census self-check moved HERE from
 *	the activate proof (recovery_duty.c) — census GREEN is the POST-bit22
 *	proof, so it binds INSIDE the cutover round: the round must close every
 *	KNOWN-DEFERRED correctness site (hw_remaster) before the latch flips.
 *	While any deferred site remains the apply is refused (fail-closed).
 */
bool
cluster_r4_bit22_cutover_latch_apply(uint64 transition_epoch,
									 uint64 round_generation)
{
	uint32 expected = CLUSTER_R4_BIT22_SOURCE;

	/* RF-ROOT P9 verification: a fresh cluster's bit22 round legitimately
	 * runs at formation epoch 0 (no R4 history); only the round
	 * generation must be nonzero. */
	if (SemanticActivationBit22Latch == NULL
		|| round_generation == 0)
		return false;
	if (!cluster_wal_state_correctness_census_ok())
		return false;
	/* RF-ROOT P9 verification (contract): idempotent apply.  The coordinator and
	 * every member latch at OPEN_APPLIED; exactly one 0->1 CAS wins and
	 * the losers must still publish their observed+ACK or the round stalls
	 * waiting on them.  When the latch is ALREADY set the identity is
	 * authoritative: a same-round apply returns true (publication
	 * completed), a different round fails closed — and the identity is NOT
	 * rewritten (a wrong-round apply must not pollute the bound round; the
	 * short re-read tolerates a winner between its CAS and its identity
	 * write).  When unset, the identity is written before the CAS so a CAS
	 * loser reads back either its own value (winner not yet overwritten)
	 * or the same-round winner's — both match.  Cross-round concurrent
	 * applies cannot occur (cutover rounds are driver-serialized). */
	if (pg_atomic_read_u32(&SemanticActivationBit22Latch->active) != 0)
	{
		int		i;

		if (pg_atomic_read_u64(
				&SemanticActivationBit22Latch->transition_epoch) == transition_epoch
			&& pg_atomic_read_u64(
				&SemanticActivationBit22Latch->round_generation) == round_generation)
			return true;
		for (i = 0; i < 8; i++)
		{
			pg_read_barrier();
			if (pg_atomic_read_u64(
					&SemanticActivationBit22Latch->transition_epoch) == transition_epoch
				&& pg_atomic_read_u64(
					&SemanticActivationBit22Latch->round_generation) == round_generation)
				return true;
		}
		return false;
	}
	pg_atomic_write_u64(&SemanticActivationBit22Latch->transition_epoch,
						transition_epoch);
	pg_atomic_write_u64(&SemanticActivationBit22Latch->round_generation,
						round_generation);
	pg_write_barrier();
	if (pg_atomic_compare_exchange_u32(&SemanticActivationBit22Latch->active,
										&expected,
										CLUSTER_R4_BIT22_TARGET_BOOTSTRAP))
		return true;
	return pg_atomic_read_u64(
			   &SemanticActivationBit22Latch->transition_epoch) == transition_epoch
		   && pg_atomic_read_u64(
			   &SemanticActivationBit22Latch->round_generation) == round_generation;
}

/*
 * cluster_r4_bit22_source_writer_enter -- RF-ROOT P9 verification (implementation):
 * a wal-state registry writer (telemetry / checkpoint / FPW / thread
 * open-close / STOPPED) enters its critical section.  Refused once the
 * source is closed (the cutover BARRIER): the slot freezes as-is.
 * Fail-closed: absent shmem (early startup) refuses.
 */
bool
cluster_r4_bit22_source_writer_enter(void)
{
	if (SemanticActivationBit22SourceClose == NULL
		|| pg_atomic_read_u32(&SemanticActivationBit22SourceClose->closed) != 0)
		return false;
	pg_atomic_fetch_add_u32(&SemanticActivationBit22SourceClose->writer_count, 1);
	return true;
}

/*
 * cluster_r4_bit22_source_writer_leave -- RF-ROOT P9 verification (implementation):
 * leave the writer critical section (pairs with enter).
 */
void
cluster_r4_bit22_source_writer_leave(void)
{
	if (SemanticActivationBit22SourceClose == NULL)
		return;
	pg_atomic_fetch_sub_u32(&SemanticActivationBit22SourceClose->writer_count, 1);
}

/*
 * cluster_r4_bit22_source_close_begin -- RF-ROOT P9 verification (implementation):
 * freeze the local source for the cutover round (closed=1 + round
 * identity).  Idempotent for the same round; a different round while
 * closed is refused.  The caller then waits for writer_count==0.
 */
bool
cluster_r4_bit22_source_close_begin(uint64 transition_epoch,
									uint64 prepare_generation)
{
	uint32 expected = 0;

	if (SemanticActivationBit22SourceClose == NULL
		|| prepare_generation == 0)
		return false;
	if (pg_atomic_read_u32(&SemanticActivationBit22SourceClose->closed) != 0)
		return pg_atomic_read_u64(
				   &SemanticActivationBit22SourceClose->transition_epoch)
				   == transition_epoch
			   && pg_atomic_read_u64(
				   &SemanticActivationBit22SourceClose->prepare_generation)
				   == prepare_generation;
	pg_atomic_write_u64(&SemanticActivationBit22SourceClose->transition_epoch,
						transition_epoch);
	pg_atomic_write_u64(&SemanticActivationBit22SourceClose->prepare_generation,
						prepare_generation);
	pg_write_barrier();
	return pg_atomic_compare_exchange_u32(
		&SemanticActivationBit22SourceClose->closed, &expected, 1);
}

/*
 * cluster_r4_bit22_source_close_current -- RF-ROOT P9 verification (implementation):
 * is the source frozen for exactly this round?
 */
bool
cluster_r4_bit22_source_close_current(uint64 transition_epoch,
									  uint64 prepare_generation)
{
	return SemanticActivationBit22SourceClose != NULL
		&& pg_atomic_read_u32(&SemanticActivationBit22SourceClose->closed) != 0
		&& pg_atomic_read_u64(
			&SemanticActivationBit22SourceClose->transition_epoch)
		   == transition_epoch
		&& pg_atomic_read_u64(
			&SemanticActivationBit22SourceClose->prepare_generation)
		   == prepare_generation;
}

/*
 * cluster_r4_bit22_cutover_begin -- RF-ROOT P7 (contract, step ④c): the
 * round DRIVER entry.  Coordinator backend calls this with the constructed
 * migration image + round: create_prepared (root PREPARED), round sha,
 * seam stage, then publishes the PREPARED-stage ACK-table image and sends
 * the PREPARED REQUEST to the members — skipping the R4 SAMPLE/BARRIER
 * four-member stages (W6 clause 3 needs only the PREPARED CLOSED-ACK).
 * Member expected-tuples are filled by actively sampling each member's
 * admitted incarnation + capability (IC) — no SAMPLE round needed.  The
 * member side applies the parameterized PREPARED check (step ④a), ACKs,
 * and the coordinator's open_applied_advance (step ②) takes over once the
 * CLOSED-ACK is COMPLETE.
 */
bool
cluster_r4_bit22_cutover_begin(
	const ClusterControlRootMigrationImage *image,
	const ClusterControlRootMigrationRoundV1 *round)
{
	ClusterControlRootFileToken token;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckPendingSend pending;
	ClusterSemanticActivationAckWireV1 request;
	SemanticActivationAckTuple self;
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterControlRootResult create_result;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint32 local_capability_word;
	int32 current_coordinator_node;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	int node;

	if (round == NULL
		|| (round->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| SemanticActivationAckTable == NULL
		|| SemanticActivationBit22Seam == NULL
		|| !semantic_activation_snapshot(&snapshot)
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != current_coordinator_node
		|| round->transition_epoch != current_epoch
		|| round->admitted_bitmap_low != current_members_lo
		|| round->admitted_bitmap_high != current_members_hi
		|| round->prepare_generation == 0)
		return false;

	/* RF-ROOT P9 verification (implementation): source-close BARRIER first.  Freeze
	 * the local wal-state writers and wait for in-flight writers to drain
	 * (bounded); the all-member BARRIER then freezes every node's source.
	 * The migration image is built ONLY after the all-member BARRIER
	 * COMPLETE (LMON tick, bit22_advance), so ACTIVE slots are provably
	 * quiesced — no offline STOPPED requirement. */
	if (!cluster_r4_bit22_source_close_begin(
			round->transition_epoch, round->prepare_generation))
		return false;
	{
		int i;

		for (i = 0; i < 1000; i++) {
			if (pg_atomic_read_u32(
					&SemanticActivationBit22SourceClose->writer_count) == 0)
				break;
			pg_usleep(5000L); /* 5 ms; up to ~5 s */
		}
		if (pg_atomic_read_u32(
				&SemanticActivationBit22SourceClose->writer_count) != 0)
			return false;
	}

	memset(&table, 0, sizeof(table));
	table.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	table.coordinator_node = (uint32)cluster_node_id;
	table.round_nonce = 1; /* provisional: the real nonce comes from
							* create_prepared at the BARRIER COMPLETE */
	table.transition_epoch = round->transition_epoch;
	table.record_generation = round->prepare_generation;
	table.expected_members_lo = round->admitted_bitmap_low;
	table.expected_members_hi = round->admitted_bitmap_high;
	table.source_feature_bitmap = round->source_feature_bitmap;
	table.target_feature_bitmap = round->target_feature_bitmap;
	table.rollback_feature_bitmap = 0;
	table.capability_sample_digest = round->capability_sample_digest;
	table.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	local_capability_word = cluster_ic_local_capability_word();
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		SemanticActivationAckTuple remote;
		uint32 peer_word;
		uint32 peer_gen;

		if (!semantic_activation_ack_member_present(
				current_members_lo, current_members_hi, node))
			continue;
		if (node == cluster_node_id) {
			if (!semantic_activation_ack_self_tuple(
					node, local_capability_word, round->transition_epoch,
					round->prepare_generation, &table.expected[node]))
				return false;
			continue;
		}
		memset(&remote, 0, sizeof(remote));
		remote.node_id = (uint32)node;
		remote.boot_id
			= cluster_membership_get_last_admitted_incarnation(node);
		remote.admitted_incarnation = remote.boot_id;
		if (remote.boot_id == 0
			|| !cluster_sf_peer_capability_word_sample(
				node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
				&peer_word, &peer_gen)
			|| peer_gen == 0)
			return false;
		remote.control_connection_generation = (uint64)peer_gen;
		remote.capability_word = peer_word;
		remote.capability_generation = (uint64)peer_gen;
		remote.transition_epoch = round->transition_epoch;
		remote.record_generation = round->prepare_generation;
		table.expected[node] = remote;
	}
	/* RF-ROOT P9 verification / cold-formation (2026-08-19): the
	 * coordinator's own source-close completed inside this function, so its
	 * expected tuple is already its observed tuple — mark the coordinator
	 * self-observed in the BARRIER table.  Without this, observed can never
	 * equal expected (the coordinator never ACKs itself over the wire) and
	 * the BARRIER never reaches COMPLETE (observed 2-node run: BARRIER
	 * stalled at flags=0x1 forever).  Mirrors the R4 install (observed[0]
	 * = self at publish) and the OPEN_APPLIED stage (open_applied_begin). */
	if (cluster_node_id < 64)
		table.observed_members_lo = UINT64_C(1) << cluster_node_id;
	else
		table.observed_members_hi = UINT64_C(1) << (cluster_node_id - 64);
	table.observed[cluster_node_id] = table.expected[cluster_node_id];
	if (!semantic_activation_ack_table_publish(&table))
		return false;

	(void) request;
	(void) pending;
	(void) self;
	(void) create_result;
	(void) sha;
	(void) token;
	{
		SemanticActivationAckRequestOrigin *origin
			= &semantic_activation_ack_local_request_origin;

		memset(origin, 0, sizeof(*origin));
		origin->unsent_members_lo = table.expected_members_lo
			& ~(UINT64_C(1) << cluster_node_id);
		origin->active = true;
	}
	/* RF-ROOT P9 verification: the BARRIER REQUEST is sent by the LMON
	 * tick — the ic msg-type gate rejects semantic-activation ACK
	 * messages from backend (SQL) senders; begin() only stages the
	 * origin, and bit22_advance() drains it. */
	return true;
}

/*
 * semantic_activation_ack_lmon_send_bit22_prepared_requests -- RF-ROOT P7
 * (contract step ④c): per-member PREPARED REQUEST send for the bit22 cutover
 * round — mirrors the R4 send_origin_requests loop with round-parameterized
 * validation (member set from the ACK table, target carries bit22; no
 * four-member hardcoding).  Returns true once every member has been sent;
 * a failed send invalidates the origin (retried by the driver).
 */
static bool
semantic_activation_ack_lmon_send_bit22_prepared_requests(uint32 stage)
{
	SemanticActivationAckRequestOrigin *origin
		= &semantic_activation_ack_local_request_origin;
	ClusterSemanticActivationAckTableV1 image;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	if (!origin->active)
		return false;
	if (!semantic_activation_ack_table_snapshot(&image)
		|| image.stage != stage
		|| image.round_nonce == 0
		|| image.record_generation == 0
		|| (image.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| image.expected_members_lo == 0
		|| image.expected_members_hi != 0
		|| image.capability_sample_digest == 0
		|| (image.flags
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0) {
		semantic_activation_ack_lmon_invalidate_active();
		return false;
	}

	for (;;) {
		SemanticActivationAckPendingSend *pending = &origin->current;
		ClusterICSendResult send_result;
		uint64 member_bit;
		int32 node;

		if (pending->pending_members_lo == 0
			&& pending->pending_members_hi == 0) {
			for (node = 0; node < CLUSTER_MAX_NODES; node++) {
				member_bit = UINT64_C(1) << node;
				if ((origin->unsent_members_lo & member_bit) != 0)
					break;
			}
			if (node == CLUSTER_MAX_NODES)
				return true;
			memset(pending, 0, sizeof(*pending));
			pending->message.kind
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
			pending->message.stage = image.stage;
			pending->message.result
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
			pending->message.coordinator_node = image.coordinator_node;
			pending->message.member_node = (uint32)node;
			pending->message.transition_epoch = image.transition_epoch;
			pending->message.record_generation = image.record_generation;
			pending->message.round_nonce = image.round_nonce;
			pending->message.source_feature_bitmap
				= image.source_feature_bitmap;
			pending->message.target_feature_bitmap
				= image.target_feature_bitmap;
			pending->message.rollback_feature_bitmap
				= image.rollback_feature_bitmap;
			pending->message.admitted_members_lo
				= image.expected_members_lo;
			pending->message.admitted_members_hi
				= image.expected_members_hi;
			pending->message.capability_sample_digest
				= image.capability_sample_digest;
			pending->pending_members_lo = member_bit;
			origin->unsent_members_lo &= ~member_bit;
		}

		node = (int32)pending->message.member_node;
		if (node < 0 || node >= CLUSTER_MAX_NODES
			|| pending->invalidated) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}
		member_bit = UINT64_C(1) << node;
		if (pending->pending_members_hi != 0
			|| pending->pending_members_lo != member_bit
			|| pending->message.kind
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
			|| pending->message.stage != image.stage
			|| pending->message.result
			   != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
			|| pending->message.coordinator_node != image.coordinator_node
			|| pending->message.transition_epoch != image.transition_epoch
			|| pending->message.record_generation != image.record_generation
			|| pending->message.round_nonce != image.round_nonce
			|| pending->message.source_feature_bitmap
			   != image.source_feature_bitmap
			|| pending->message.target_feature_bitmap
			   != image.target_feature_bitmap
			|| pending->message.rollback_feature_bitmap
			   != image.rollback_feature_bitmap
			|| pending->message.admitted_members_lo
			   != image.expected_members_lo
			|| pending->message.admitted_members_hi
			   != image.expected_members_hi
			|| pending->message.capability_sample_digest
			   != image.capability_sample_digest
			|| !cluster_semantic_activation_ack_wire_encode(
				&pending->message, payload)) {
			semantic_activation_ack_lmon_invalidate_active();
			return false;
		}

		send_result = cluster_ic_send_envelope(
			PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1, node,
			payload, sizeof(payload));
		{
			SemanticActivationAckSendResult disposition
				= semantic_activation_ack_pending_send_note_result(
					pending, node, send_result);

			if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_RETAINED)
				return true;
			if (disposition == SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED) {
				cluster_ic_tier1_close_peer(
					node, "semantic activation PREPARED request send failed");
				semantic_activation_ack_lmon_invalidate_active();
				return false;
			}
			if (disposition != SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED) {
				semantic_activation_ack_lmon_invalidate_active();
				return false;
			}
			/* ADMITTED: continue to the next member. */
		}
	}
}

/*
 * cluster_r4_bit22_cutover_seam_store -- RF-ROOT P7 (contract, step ②/④):
 * the round DRIVER stages the PREPARED root token + round sha + round copy
 * after create_prepared.  The coordinator LMON consumes the seam at the
 * OPEN_APPLIED advance.  One-shot per round: a later store re-stages (the
 * advance cross-checks transition_epoch vs the ACK table, so a stale seam
 * is inert).  Fail-closed on NULL/zero inputs.
 */
bool
cluster_r4_bit22_cutover_seam_store(
	const ClusterControlRootFileToken *file_token,
	const uint8 round_sha[PG_SHA256_DIGEST_LENGTH],
	const ClusterControlRootMigrationRoundV1 *round)
{
	if (SemanticActivationBit22Seam == NULL || file_token == NULL
		|| round_sha == NULL || round == NULL
		|| file_token->file_txn_seq == 0
		|| semantic_activation_bytes_are_zero(
			round_sha, PG_SHA256_DIGEST_LENGTH))
		return false;
	SemanticActivationBit22Seam->file_token = *file_token;
	memcpy(SemanticActivationBit22Seam->round_sha, round_sha,
		   PG_SHA256_DIGEST_LENGTH);
	SemanticActivationBit22Seam->round = *round;
	SemanticActivationBit22Seam->transition_epoch = round->transition_epoch;
	SemanticActivationBit22Seam->prepare_generation
		= round->prepare_generation;
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationBit22Seam->valid, 1);
	return true;
}

/*
 * One single-slot ProcessUtility -> formation-LMON request/result mailbox.  The
 * state word is the publication fence: writers own WRITING, LMON alone
 * consumes PENDING, and only the publishing backend consumes COMPLETE.
 * This mailbox carries no PGSA bytes and cannot bypass QVOTEC.
 */
static bool
semantic_activation_utility_mailbox_submit(
	ClusterSemanticActivationAction action, uint64 source_feature_bitmap,
	uint64 target_feature_bitmap, uint64 rollback_feature_bitmap,
	uint64 expected_record_generation, uint64 *out_request_seq)
{
	uint32 expected_state = SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE;
	uint64 request_seq;

	if (SemanticActivationUtilityMailbox == NULL || out_request_seq == NULL
		|| action < CLUSTER_SEMANTIC_ENABLE_ALL
		|| action > CLUSTER_SEMANTIC_ROLLBACK_ABORT
		|| !pg_atomic_compare_exchange_u32(
			&SemanticActivationUtilityMailbox->utility_mailbox_state,
			&expected_state, SEMANTIC_ACTIVATION_UTILITY_MAILBOX_WRITING))
		return false;

	request_seq = pg_atomic_read_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq);
	if (request_seq == UINT64_MAX) {
		pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
							SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
		return false;
	}

	request_seq++;
	SemanticActivationUtilityMailbox->utility_action = (uint32)action;
	SemanticActivationUtilityMailbox->utility_source_feature_bitmap
		= source_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_target_feature_bitmap
		= target_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap
		= rollback_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_expected_record_generation
		= expected_record_generation;
	pg_atomic_write_u64(&SemanticActivationUtilityMailbox->utility_request_seq,
						request_seq);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
						SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING);
	*out_request_seq = request_seq;
	return true;
}

static bool
semantic_activation_utility_mailbox_poll(SemanticActivationUtilityRequest *out)
{
	uint64 request_seq;

	if (SemanticActivationUtilityMailbox == NULL || out == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING)
		return false;

	pg_read_barrier();
	request_seq = pg_atomic_read_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq);
	if (request_seq == 0
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_completion_seq)
			   + 1 != request_seq
		|| SemanticActivationUtilityMailbox->utility_action
			   > CLUSTER_SEMANTIC_ROLLBACK_ABORT)
		return false;

	out->request_seq = request_seq;
	out->action = (ClusterSemanticActivationAction)
		SemanticActivationUtilityMailbox->utility_action;
	out->source_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_source_feature_bitmap;
	out->target_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_target_feature_bitmap;
	out->rollback_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap;
	out->expected_record_generation
		= SemanticActivationUtilityMailbox->utility_expected_record_generation;
	return true;
}

static bool
semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation)
{
	if (SemanticActivationUtilityMailbox == NULL || request_seq == 0
		|| result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != request_seq)
		return false;

	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_result,
						(uint32)result);
	SemanticActivationUtilityMailbox->utility_result_feature_bit = feature_bit;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= expected_generation;
	pg_atomic_write_u64(&SemanticActivationUtilityMailbox->utility_completion_seq,
						request_seq);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
						SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE);
	return true;
}

static bool
semantic_activation_utility_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal)
{
	uint32 expected_state = SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE;

	if (SemanticActivationUtilityMailbox == NULL || request_seq == 0
		|| out_refusal == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_completion_seq)
			   != request_seq)
		return false;

	pg_read_barrier();
	out_refusal->result = (ClusterSemanticActivationResult)
		pg_atomic_read_u32(&SemanticActivationUtilityMailbox->utility_result);
	out_refusal->feature_bit
		= SemanticActivationUtilityMailbox->utility_result_feature_bit;
	out_refusal->expected_generation
		= SemanticActivationUtilityMailbox->utility_result_expected_generation;
	return pg_atomic_compare_exchange_u32(
		&SemanticActivationUtilityMailbox->utility_mailbox_state,
		&expected_state,
		SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
}

static ClusterSemanticActivationResult
semantic_activation_utility_mailbox_wait(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal)
{
	if (out_refusal == NULL || request_seq == 0) {
		semantic_activation_set_refusal(
			out_refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	for (;;) {
		if (semantic_activation_utility_mailbox_poll_completion(
				request_seq, out_refusal))
			return out_refusal->result;
		CHECK_FOR_INTERRUPTS();
		pg_usleep(CLUSTER_SEMANTIC_UTILITY_WAIT_STEP_US);
	}
}

static bool
semantic_activation_record_cas_mailbox_submit(
	uint64 expected_generation, uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], uint64 *out_request_seq)
{
	ClusterSemanticActivationRecord desired;
	ClusterSemanticFormationBinding formation;

	if (expected_generation == UINT64_MAX || desired_bytes == NULL
		|| SemanticActivationUtilityMailbox == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_semantic_activation_record_decode(
			desired_bytes, &desired, NULL)
		|| desired.record_generation != expected_generation + 1
		|| desired.coordinator_node != (uint32)cluster_node_id)
		return false;
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = pg_atomic_read_u64(
			&SemanticActivationUtilityMailbox->utility_request_seq),
		.formation_epoch = desired.transition_epoch,
		.coordinator_incarnation = desired.coordinator_incarnation,
		.expected_record_generation = expected_generation,
	};
	if (!semantic_activation_record_cas_formation_matches(
			&formation, &desired))
		return false;

	/* RECORD_CAS already carries epoch/incarnation in its durable desired
	 * image.  Reuse the utility result words for the pending utility sequence
	 * and a second incarnation copy, so utility-slot ABA and byte mutation are
	 * rejected without widening either shared mailbox. */
	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation.utility_request_seq;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation.coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, expected_generation,
		expected_source_feature_bitmap, desired_bytes, out_request_seq);
}

static bool
semantic_activation_record_read_mailbox_submit(uint64 *out_request_seq)
{
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, 0, 0, zero,
		out_request_seq);
}

static bool
semantic_activation_authority_mailbox_submit(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || desired_bytes == NULL
		|| out_request_seq == NULL
		|| (request_kind != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ))
		return false;

	request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq != completion_seq || request_seq == UINT64_MAX)
		return false;

	SemanticActivationShmem->record_cas_expected_generation = expected_generation;
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		= expected_source_feature_bitmap;
	memcpy(SemanticActivationShmem->record_cas_desired_bytes, desired_bytes,
		   sizeof(SemanticActivationShmem->record_cas_desired_bytes));
	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_request_kind,
						request_kind);
	pg_write_barrier();
	request_seq++;
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_request_seq, request_seq);
	*out_request_seq = request_seq;
	return true;
}

/* Reconstruct the exact member-local PGRD read binding from the current R4
 * PREPARED carrier.  The authority mailbox already stores the round nonce in
 * expected_source_feature_bitmap; the remaining identity is re-derived from
 * the current ACK table and local admitted incarnation on every poll. */
static bool
semantic_activation_member_pgrd_formation_binding(
	ClusterSemanticFormationBinding *out)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckTuple self;

	if (out == NULL || SemanticActivationShmem == NULL
		|| pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
			   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ
		|| !semantic_activation_ack_table_snapshot(&table)
		|| table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| table.source_feature_bitmap != 0
		|| table.target_feature_bitmap
			   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| table.rollback_feature_bitmap != 0
		|| table.round_nonce == 0
		|| table.round_nonce
			   != SemanticActivationShmem
				  ->record_cas_expected_source_feature_bitmap
		|| cluster_node_id < 0 || cluster_node_id >= 4
		|| cluster_node_id == (int32)table.coordinator_node
		|| !semantic_activation_ack_member_prepared_image_current(
			&table, &self))
		return false;

	out->utility_request_seq = table.round_nonce;
	out->formation_epoch = table.transition_epoch;
	out->coordinator_incarnation = self.admitted_incarnation;
	out->expected_record_generation = table.record_generation;
	return out->coordinator_incarnation != 0;
}

static bool
semantic_activation_authority_request_formation_binding(
	ClusterSemanticAuthorityRequestKind request_kind,
	ClusterSemanticFormationBinding *out)
{
	ClusterSemanticActivationRecord desired;
	uint64 expected_generation;

	if (SemanticActivationShmem == NULL
		|| SemanticActivationUtilityMailbox == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;

	switch (request_kind) {
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS:
		expected_generation
			= SemanticActivationShmem->record_cas_expected_generation;
		if (expected_generation == UINT64_MAX || cluster_node_id < 0
			|| cluster_node_id >= CLUSTER_MAX_NODES
			|| !cluster_semantic_activation_record_decode(
				SemanticActivationShmem->record_cas_desired_bytes,
				&desired, NULL)
			|| desired.record_generation != expected_generation + 1
			|| desired.coordinator_node != (uint32)cluster_node_id
			|| desired.coordinator_incarnation
				   != SemanticActivationUtilityMailbox
					  ->utility_result_expected_generation)
			return false;
		out->utility_request_seq
			= SemanticActivationUtilityMailbox->utility_result_feature_bit;
		out->formation_epoch = desired.transition_epoch;
		out->coordinator_incarnation = desired.coordinator_incarnation;
		out->expected_record_generation = expected_generation;
		return true;
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR:
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ:
		if (request_kind
				== CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ
			&& semantic_activation_member_pgrd_formation_binding(out))
			return true;
		out->utility_request_seq
			= SemanticActivationShmem
			  ->record_cas_expected_source_feature_bitmap;
		out->formation_epoch
			= SemanticActivationUtilityMailbox->utility_result_feature_bit;
		out->coordinator_incarnation
			= SemanticActivationUtilityMailbox
			  ->utility_result_expected_generation;
		out->expected_record_generation
			= SemanticActivationUtilityMailbox
			  ->utility_expected_record_generation;
		return true;
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE:
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ:
	default:
		return false;
	}
}

bool
cluster_semantic_activation_qvotec_poll_record_cas(ClusterSemanticActivationCasRequest *out)
{
	ClusterSemanticActivationRecord desired;
	ClusterSemanticFormationBinding formation;
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;

	request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;

	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS)
		return false;
	if (!semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, &formation)
		|| !cluster_semantic_activation_record_decode(
			SemanticActivationShmem->record_cas_desired_bytes,
			&desired, NULL)
		|| !semantic_activation_record_cas_formation_matches(
			&formation, &desired)) {
		(void)cluster_semantic_activation_qvotec_complete_record_cas(
			request_seq, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		memset(out, 0, sizeof(*out));
		return false;
	}
	out->request_seq = request_seq;
	out->expected_generation = SemanticActivationShmem->record_cas_expected_generation;
	out->expected_source_feature_bitmap
		= SemanticActivationShmem->record_cas_expected_source_feature_bitmap;
	memcpy(out->desired_bytes, SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->desired_bytes));
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_record_cas(uint64 request_seq,
												   ClusterSemanticActivationResult result)
{
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, request_seq, result);
}

bool
cluster_semantic_activation_qvotec_poll_record_read(
	ClusterSemanticActivationReadRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ)
		return false;
	out->request_seq = request_seq;
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_record_read(
	uint64 request_seq, ClusterSemanticActivationResult result,
	bool implicit_open,
	const uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	ClusterSemanticActivationRecord decoded;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	if (!semantic_activation_authority_mailbox_completion_matches(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq)
		|| result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| (result == CLUSTER_SEMANTIC_ACTIVATION_OK
			&& (selected_bytes == NULL
				|| (implicit_open
					&& !semantic_activation_bytes_are_zero(
						selected_bytes, sizeof(zero)))
				|| (!implicit_open
					&& !cluster_semantic_activation_record_decode(
						selected_bytes, &decoded, NULL)))))
		return false;

	memcpy(SemanticActivationShmem->record_cas_desired_bytes,
		   result == CLUSTER_SEMANTIC_ACTIVATION_OK ? selected_bytes : zero,
		   sizeof(zero));
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		= result == CLUSTER_SEMANTIC_ACTIVATION_OK && implicit_open ? 1 : 0;
	pg_write_barrier();
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq, result);
}

static bool
semantic_activation_authority_mailbox_completion_matches(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq)
{
	uint64 current_request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL)
		return false;

	current_request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (current_request_seq != request_seq || completion_seq == UINT64_MAX
		|| completion_seq + 1 != request_seq)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;
	return true;
}

static bool
semantic_activation_authority_mailbox_complete(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult result)
{
	if (result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| !semantic_activation_authority_mailbox_completion_matches(
			request_kind, request_seq))
		return false;

	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result, (uint32)result);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_completion_seq, request_seq);
	return true;
}

static bool
semantic_activation_record_cas_mailbox_poll_completion(uint64 request_seq,
										ClusterSemanticActivationResult *out_result)
{
	ClusterSemanticActivationRecord desired;
	ClusterSemanticFormationBinding formation;
	ClusterSemanticActivationResult result;

	if (out_result == NULL
		|| !semantic_activation_authority_mailbox_poll_completion(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, request_seq,
			&result))
		return false;
	if (result == CLUSTER_SEMANTIC_ACTIVATION_OK
		&& (!semantic_activation_authority_request_formation_binding(
				CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, &formation)
			|| !cluster_semantic_activation_record_decode(
				SemanticActivationShmem->record_cas_desired_bytes,
				&desired, NULL)
			|| !semantic_activation_record_cas_formation_matches(
				&formation, &desired)))
		result = CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	*out_result = result;
	return true;
}

static bool
semantic_activation_record_read_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationReadCompletion *out)
{
	ClusterSemanticActivationResult result;

	if (out == NULL
		|| !semantic_activation_authority_mailbox_poll_completion(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq,
			&result))
		return false;

	memset(out, 0, sizeof(*out));
	out->result = result;
	out->implicit_open
		= SemanticActivationShmem
		  ->record_cas_expected_source_feature_bitmap
		  != 0;
	memcpy(out->selected_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->selected_bytes));
	return true;
}

static bool
semantic_activation_authority_mailbox_poll_completion(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult *out_result)
{
	if (SemanticActivationShmem == NULL || out_result == NULL
		|| pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq)
			   != request_seq)
		return false;

	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;
	*out_result = (ClusterSemanticActivationResult)pg_atomic_read_u32(
		&SemanticActivationShmem->record_cas_result);
	return true;
}

bool
cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq)
{
	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| system_identifier == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			formation))
		return false;

	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation->formation_epoch;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation->coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		system_identifier, formation->utility_request_seq, desired_bytes,
		out_request_seq);
}

bool
cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
	ClusterUndoRootDescriptorRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR)
		return false;
	out->request_seq = request_seq;
	out->system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	memcpy(out->desired_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->desired_bytes));
	if (out->system_identifier == 0
		|| !semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
			&out->formation)
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			&out->formation)) {
		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
			request_seq, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		memset(out, 0, sizeof(*out));
		return false;
	}
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
	uint64 request_seq, ClusterSemanticActivationResult result)
{
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		request_seq, result);
}

bool
cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationResult *out_result)
{
	return semantic_activation_authority_mailbox_poll_completion(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		request_seq, out_result);
}

static bool
semantic_activation_undo_root_descriptor_read_mailbox_submit(
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier, uint64 *out_request_seq)
{
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };

	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| system_identifier == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			formation))
		return false;
	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation->formation_epoch;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation->coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
		system_identifier, formation->utility_request_seq, zero,
		out_request_seq);
}

static bool
semantic_activation_member_pgrd_read_mailbox_submit(
	const ClusterSemanticActivationAckTableV1 *expected_table,
	uint64 *out_request_seq)
{
	ClusterSemanticActivationAckTableV1 current;
	SemanticActivationAckTuple self;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
	uint64 system_identifier = GetSystemIdentifier();

	if (expected_table == NULL || out_request_seq == NULL
		|| system_identifier == 0
		|| !semantic_activation_ack_table_snapshot(&current)
		|| memcmp(expected_table, &current, sizeof(current)) != 0
		|| current.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED
		|| current.source_feature_bitmap != 0
		|| current.target_feature_bitmap
			   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| current.rollback_feature_bitmap != 0
		|| current.round_nonce == 0
		|| cluster_node_id < 0 || cluster_node_id >= 4
		|| cluster_node_id == (int32)current.coordinator_node
		|| !semantic_activation_ack_member_prepared_image_current(
			&current, &self))
		return false;

	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
		system_identifier, current.round_nonce, zero, out_request_seq);
}

bool
cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
	ClusterUndoRootDescriptorReadRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ)
		return false;
	out->request_seq = request_seq;
	out->system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	if (out->system_identifier == 0
		|| !semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
			&out->formation)
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			&out->formation)) {
		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
			request_seq, CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD, zero);
		memset(out, 0, sizeof(*out));
		return false;
	}
	return true;
}

static bool
semantic_activation_qvotec_formation_matches_expected(
	const ClusterSemanticFormationBinding *formation,
	uint64 utility_expected_record_generation,
	bool require_r4_admitted_basis)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;
	uint64 current_incarnation;

	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| SemanticActivationShmem == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != utility_expected_record_generation
		|| !cluster_qvotec_in_quorum())
		return false;

	current_epoch = cluster_epoch_get_current();
	current_incarnation = cluster_qvotec_get_self_incarnation();
	if (current_epoch != formation->formation_epoch
		|| current_incarnation != formation->coordinator_incarnation
		|| !semantic_activation_snapshot(&snapshot)
		|| snapshot.formation_epoch != formation->formation_epoch
		|| snapshot.record_generation
			   != formation->expected_record_generation
		|| (require_r4_admitted_basis
			&& !semantic_activation_r4_current_admitted_basis()))
		return false;

	return cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == current_epoch
		   && cluster_qvotec_get_self_incarnation() == current_incarnation;
}

static bool
semantic_activation_r4_initial_clean_cas_matches(
	const ClusterSemanticFormationBinding *formation,
	const ClusterSemanticActivationRecord *desired,
	uint64 utility_expected_record_generation)
{
	ClusterInitialCleanFormationSnapshot clean_before;
	ClusterInitialCleanFormationSnapshot clean_after;
	ClusterSemanticActivationAckTableV1 table;
	uint8 pgrd_before[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 pgrd_after[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	uint64 capability_sample_digest;
	uint32 local_capability_word;
	uint32 expected_table_stage;
	uint64 expected_table_generation;
	int32 coordinator_node;
	int node;

	if (formation == NULL || desired == NULL
		|| utility_expected_record_generation != 0
		|| desired->source_feature_bitmap != 0
		|| desired->target_feature_bitmap
		   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| desired->rollback_feature_bitmap != 0
		|| desired->capability_sample_digest == 0)
		return false;
	switch (desired->phase) {
	case CLUSTER_SEMANTIC_PHASE_PREPARE:
		if (desired->record_generation != 1)
			return false;
		expected_table_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
		expected_table_generation = desired->record_generation;
		break;
	case CLUSTER_SEMANTIC_PHASE_COMMIT:
		if (desired->record_generation != 2)
			return false;
		expected_table_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		expected_table_generation = desired->record_generation - 1;
		break;
	case CLUSTER_SEMANTIC_PHASE_OPEN:
		if (desired->record_generation != 3)
			return false;
		expected_table_stage
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
		expected_table_generation = desired->record_generation - 1;
		break;
	default:
		return false;
	}

	if (cluster_grd_recovery_state_value() != GRD_RECOVERY_IDLE
		|| !semantic_activation_qvotec_formation_matches_expected(
			formation, utility_expected_record_generation, false)
		|| !cluster_reconfig_snapshot_initial_clean_formation(
			&clean_before)
		|| (clean_before.formation_marker_generation == 0
			&& clean_before.formation_epoch != CLUSTER_EPOCH_INITIAL)
		|| clean_before.members_lo != UINT64_C(0x0f)
		|| clean_before.members_hi != 0
		|| clean_before.formation_epoch != desired->transition_epoch
		|| desired->admitted_members_lo != clean_before.members_lo
		|| desired->admitted_members_hi != clean_before.members_hi
		|| desired->coordinator_node != 0
		|| desired->coordinator_incarnation
		   != clean_before.admitted_incarnation[0]
		|| !semantic_activation_initial_clean_pgrd_mirror(pgrd_before)
		|| !semantic_activation_ack_table_snapshot(&table)
		|| table.stage != expected_table_stage
		|| table.flags
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| table.transition_epoch != desired->transition_epoch
		|| table.record_generation != expected_table_generation
		|| table.coordinator_node != desired->coordinator_node
		|| table.round_nonce != formation->utility_request_seq
		|| table.expected_members_lo != desired->admitted_members_lo
		|| table.expected_members_hi != desired->admitted_members_hi
		|| table.source_feature_bitmap != desired->source_feature_bitmap
		|| table.target_feature_bitmap != desired->target_feature_bitmap
		|| table.rollback_feature_bitmap != desired->rollback_feature_bitmap
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &members_lo, &members_hi, &formation_epoch,
			&coordinator_node)
		|| members_lo != clean_before.members_lo
		|| members_hi != clean_before.members_hi
		|| formation_epoch != clean_before.formation_epoch
		|| coordinator_node != 0 || cluster_node_id != coordinator_node)
		return false;

	local_capability_word = cluster_ic_local_capability_word();
	if (!semantic_activation_ack_complete_image_current(
			&table, members_lo, members_hi, formation_epoch,
			coordinator_node, cluster_node_id, local_capability_word))
		return false;
	if (desired->phase == CLUSTER_SEMANTIC_PHASE_PREPARE) {
		if (table.capability_sample_digest != 0
			|| !semantic_activation_ack_sample_digest(
				&table, &capability_sample_digest)
			|| capability_sample_digest
				   != desired->capability_sample_digest)
			return false;
	} else if (table.capability_sample_digest
			   != desired->capability_sample_digest)
		return false;
	for (node = 0; node < 4; node++) {
		if (table.expected[node].admitted_incarnation
				!= clean_before.admitted_incarnation[node]
			|| table.observed[node].admitted_incarnation
				   != clean_before.admitted_incarnation[node])
			return false;
	}

	return cluster_grd_recovery_state_value() == GRD_RECOVERY_IDLE
		   && cluster_reconfig_snapshot_initial_clean_formation(
			   &clean_after)
		   && semantic_activation_initial_clean_snapshot_equal(
			   &clean_before, &clean_after)
		   && semantic_activation_initial_clean_pgrd_mirror(pgrd_after)
		   && memcmp(pgrd_before, pgrd_after, sizeof(pgrd_before)) == 0
		   && cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == clean_before.formation_epoch;
}

/* RECORD_CAS keeps the utility episode's immutable starting generation while
 * each serial durable edge binds the generation it actually compares.  The
 * approved happy path currently reaches PREPARE(g+1) and COMMIT(g+2) only. */
/*
 * semantic_activation_bit22_cas_table_binding_matches -- RF-ROOT P9 verification
 *	closure (verified implementation): round-identity binding for the bit22 cutover
 *	round's RECORD_CAS requests.  The cutover round has no utility request;
 *	its CAS desired image must match the ACK table exactly (the table is
 *	the round's authoritative identity: begin() fills it from the round,
 *	every member ACKs it).  Phase/generation cross-check: a COMMIT desired
 *	stands one generation above the PREPARED table, an OPEN desired one
 *	generation above the COMMIT_APPLIED table.
 */
static bool
semantic_activation_bit22_cas_table_binding_matches(
	const ClusterSemanticActivationRecord *desired)
{
	ClusterSemanticActivationAckTableV1 table;
	uint64 expected_table_generation;

	if (desired == NULL || SemanticActivationAckTable == NULL
		|| !semantic_activation_ack_table_snapshot(&table)
		|| (table.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		|| table.transition_epoch != desired->transition_epoch
		|| table.coordinator_node != desired->coordinator_node
		|| table.expected_members_lo != desired->admitted_members_lo
		|| table.expected_members_hi != desired->admitted_members_hi
		|| table.source_feature_bitmap != desired->source_feature_bitmap
		|| table.target_feature_bitmap != desired->target_feature_bitmap
		|| table.rollback_feature_bitmap != desired->rollback_feature_bitmap
		|| table.capability_sample_digest
		   != desired->capability_sample_digest
		|| desired->coordinator_incarnation
		   != cluster_qvotec_get_self_incarnation())
		return false;
	switch (desired->phase) {
	case CLUSTER_SEMANTIC_PHASE_PREPARE:
		/* RF-ROOT P9 verification (cold-formation): the bit22 round's
		 * PREPARE-record CAS (majority legacy-zero -> generation 1) is
		 * submitted at BARRIER COMPLETE, where the table still stands at
		 * the BARRIER stage and the PREPARE generation equals the table's
		 * generation (the round's P). */
		if (table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER)
			return false;
		expected_table_generation = desired->record_generation;
		break;
	case CLUSTER_SEMANTIC_PHASE_COMMIT:
		if (table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED)
			return false;
		expected_table_generation = desired->record_generation - 1;
		break;
	case CLUSTER_SEMANTIC_PHASE_OPEN:
		if (table.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED)
			return false;
		expected_table_generation = desired->record_generation - 1;
		break;
	default:
		return false;
	}
	return table.record_generation == expected_table_generation;
}

static bool
semantic_activation_record_cas_formation_matches(
	const ClusterSemanticFormationBinding *formation,
	const ClusterSemanticActivationRecord *desired)
{
	uint64 utility_expected_record_generation;
	uint32 required_caps;
	bool require_r4_admitted_basis;

	if (formation == NULL || desired == NULL
		|| formation->expected_record_generation == UINT64_MAX
		|| desired->record_generation
		   != formation->expected_record_generation + 1
		|| desired->transition_epoch != formation->formation_epoch
		|| desired->coordinator_incarnation
		   != formation->coordinator_incarnation
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| desired->coordinator_node != (uint32)cluster_node_id)
		return false;

	/* RF-ROOT P9 verification (verified implementation): the bit22 cutover round is
	 * SQL-driven and has NO utility request, so its RECORD_CAS requests
	 * (majority COMMIT(P+1) and majority OPEN(P+2)) cannot bind to the
	 * utility mailbox.  They bind to the ACK table's round identity
	 * instead — the table is constructed by begin() from the same round
	 * and every member ACKs it before the CAS is submitted. */
	if ((desired->target_feature_bitmap
		 & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0)
		return semantic_activation_bit22_cas_table_binding_matches(
			desired);

	if (!semantic_activation_ack_round_required_caps(
			desired->source_feature_bitmap,
			desired->target_feature_bitmap,
			desired->rollback_feature_bitmap, &required_caps)
		|| required_caps == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| SemanticActivationUtilityMailbox->utility_action
			   != CLUSTER_SEMANTIC_ENABLE_ALL
		|| SemanticActivationUtilityMailbox->utility_source_feature_bitmap
			   != desired->source_feature_bitmap
		|| SemanticActivationUtilityMailbox->utility_target_feature_bitmap
			   != desired->target_feature_bitmap
		|| SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap
			   != desired->rollback_feature_bitmap)
		return false;
	require_r4_admitted_basis
		= desired->source_feature_bitmap == 0
		  && desired->target_feature_bitmap
			 == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;

	switch (desired->phase) {
	case CLUSTER_SEMANTIC_PHASE_PREPARE:
		utility_expected_record_generation
			= formation->expected_record_generation;
		break;
	case CLUSTER_SEMANTIC_PHASE_COMMIT:
		if (formation->expected_record_generation == 0)
			return false;
		utility_expected_record_generation
			= formation->expected_record_generation - 1;
		break;
	case CLUSTER_SEMANTIC_PHASE_OPEN:
		/* RF-ROOT P9 verification (verified implementation): the bit22 cutover
		 * round's OPEN record (majority OPEN(P+2)) is the durable Target
		 * OPEN proof the latch restore keys on.  OPEN is COMMIT+1 =
		 * PREPARE+2, so the utility side's expected generation is
		 * expected - 2. */
		if (formation->expected_record_generation < 2)
			return false;
		utility_expected_record_generation
			= formation->expected_record_generation - 2;
		break;
	default:
		return false;
	}

	if (!require_r4_admitted_basis)
		return semantic_activation_qvotec_formation_matches_expected(
			formation, utility_expected_record_generation, false);
	if (semantic_activation_qvotec_formation_matches_expected(
			formation, utility_expected_record_generation, true))
		return true;
	return semantic_activation_r4_initial_clean_cas_matches(
		formation, desired, utility_expected_record_generation);
}

bool
cluster_semantic_activation_qvotec_pgrd_formation_matches(
	const ClusterSemanticFormationBinding *formation)
{
	ClusterSemanticFormationBinding member_binding;
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;
	uint64 current_incarnation;

	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| SemanticActivationShmem == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_qvotec_in_quorum())
		return false;

	memset(&member_binding, 0, sizeof(member_binding));
	if (semantic_activation_member_pgrd_formation_binding(
			&member_binding)
		&& member_binding.utility_request_seq
			   == formation->utility_request_seq
		&& member_binding.formation_epoch == formation->formation_epoch
		&& member_binding.coordinator_incarnation
			   == formation->coordinator_incarnation
		&& member_binding.expected_record_generation
			   == formation->expected_record_generation) {
		current_epoch = cluster_epoch_get_current();
		current_incarnation = cluster_qvotec_get_self_incarnation();
		return current_epoch == formation->formation_epoch
			   && current_incarnation
				  == formation->coordinator_incarnation
			   && cluster_qvotec_in_quorum()
			   && cluster_epoch_get_current() == current_epoch
			   && cluster_qvotec_get_self_incarnation()
				  == current_incarnation;
	}

	if (SemanticActivationUtilityMailbox == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_action
			   != CLUSTER_SEMANTIC_ENABLE_ALL
		|| SemanticActivationUtilityMailbox->utility_source_feature_bitmap != 0
		|| SemanticActivationUtilityMailbox->utility_target_feature_bitmap
			   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap != 0
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		)
		return false;

	current_epoch = cluster_epoch_get_current();
	current_incarnation = cluster_qvotec_get_self_incarnation();
	if (current_epoch != formation->formation_epoch
		|| current_incarnation != formation->coordinator_incarnation
		|| !semantic_activation_snapshot(&snapshot)
		|| snapshot.formation_epoch != formation->formation_epoch
		|| snapshot.record_generation
			   != formation->expected_record_generation
		|| snapshot.active_bits != 0 || snapshot.transition_closed)
		return false;

	return cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == current_epoch
		   && cluster_qvotec_get_self_incarnation() == current_incarnation;
}

bool
cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
	uint64 request_seq, ClusterUndoRootDescriptorState state,
	const uint8 selected_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
	uint64 system_identifier;

	if (!semantic_activation_authority_mailbox_completion_matches(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
			request_seq)
		|| state < CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
		|| state > CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD
		|| selected_bytes == NULL)
		return false;
	system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	if ((state == CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
		 && !semantic_activation_bytes_are_zero(
			 selected_bytes, sizeof(zero)))
		|| (state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
			&& (cluster_undo_root_descriptor_decode(
					selected_bytes, system_identifier, &descriptor)
					!= CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				|| descriptor.descriptor_incarnation != 1
				|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
				|| descriptor.owner_node != -1)))
		return false;

	memcpy(SemanticActivationShmem->record_cas_desired_bytes,
		   state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID ? selected_bytes
													 : zero,
		   sizeof(zero));
	pg_write_barrier();
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
		request_seq, (ClusterSemanticActivationResult)state);
}

static bool
semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
	uint64 request_seq, ClusterUndoRootDescriptorReadCompletion *out)
{
	uint32 state;

	if (SemanticActivationShmem == NULL || out == NULL
		|| pg_atomic_read_u64(
			   &SemanticActivationShmem->record_cas_completion_seq)
			   != request_seq)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ)
		return false;
	state = pg_atomic_read_u32(&SemanticActivationShmem->record_cas_result);
	if (state > CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD)
		return false;
	out->state = (ClusterUndoRootDescriptorState)state;
	memcpy(out->selected_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->selected_bytes));
	return true;
}

static bool
semantic_activation_lmon_submit_pgrd_candidate(
	const uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint64 request_seq;

	if (candidate == NULL || system_identifier == 0 || out_request_seq == NULL
		|| cluster_undo_root_descriptor_decode(
			   candidate, system_identifier, &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.descriptor_incarnation != 1
		|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
		|| descriptor.owner_node != -1)
		return false;

	if (!cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
			formation, system_identifier, candidate, &request_seq))
		return false;
	memcpy(semantic_activation_lmon_pgrd_candidate, candidate,
		   sizeof(semantic_activation_lmon_pgrd_candidate));
	semantic_activation_lmon_pgrd_candidate_request_seq = request_seq;
	*out_request_seq = request_seq;
	return true;
}

static bool
semantic_activation_lmon_submit_pgrd_exact_retry(
	const char *root_directory,
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (cluster_undo_smgr_root_descriptor_read_candidate(
			root_directory, candidate)
		!= CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
		return false;
	return semantic_activation_lmon_submit_pgrd_candidate(
		candidate, formation, system_identifier, out_request_seq);
}

static bool
semantic_activation_lmon_shared_pgrd_root_directory(
	char root_directory[MAXPGPATH])
{
	int path_len;

	if (root_directory == NULL || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] == '\0')
		return false;
	path_len = snprintf(root_directory, MAXPGPATH, "%s/pg_undo",
					cluster_shared_data_dir);
	return path_len >= 0 && path_len < MAXPGPATH;
}

static bool
semantic_activation_lmon_publish_fresh_shared_pgrd(
	const char *root_directory,
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoSmgrRootMirrorState publish_state;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (root_directory == NULL || system_identifier == 0
		|| out_request_seq == NULL)
		return false;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	descriptor.system_identifier = system_identifier;
	if (!cluster_undo_root_namespace_id(
			descriptor.descriptor_incarnation, descriptor.root_ordinal,
			&descriptor.namespace_id)
		|| !pg_strong_random(descriptor.root_uuid,
						 sizeof(descriptor.root_uuid))
		|| !cluster_undo_root_descriptor_encode(&descriptor, desired))
		return false;
	publish_state = cluster_undo_smgr_root_descriptor_publish(
		root_directory, desired);
	if (publish_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED
		&& publish_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
		return false;

	return semantic_activation_lmon_submit_pgrd_candidate(
		desired, formation, system_identifier, out_request_seq);
}

void
cluster_semantic_activation_register(const ClusterSemanticActivationDescriptor *descriptor)
{
	int feature_index;
	int index;

	if (descriptor == NULL || descriptor->name == NULL
		|| descriptor->name[0] == '\0'
		|| !semantic_activation_feature_bit_index(
			descriptor->feature_bit, &feature_index)
		|| descriptor->required_hello_caps == 0
		|| descriptor->pre_prepare_readiness == NULL
		|| descriptor->close_source_admission == NULL
		|| descriptor->source_logical_debt_zero == NULL
		|| descriptor->source_transport_zero == NULL
		|| descriptor->prepare_target == NULL
		|| descriptor->apply_target_closed == NULL
		|| descriptor->revert_source_closed == NULL
		|| descriptor->open_target_admission == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid semantic activation descriptor")));

	if (SemanticActivationDescriptors[feature_index] != NULL) {
		if (SemanticActivationDescriptors[feature_index] == descriptor)
			return;
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("duplicate semantic activation feature bit")));
	}
	for (index = 0; index < 64; index++) {
		const ClusterSemanticActivationDescriptor *registered
			= SemanticActivationDescriptors[index];

		if (registered != NULL && strcmp(registered->name, descriptor->name) == 0)
			ereport(FATAL,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("duplicate semantic activation feature name")));
	}
	SemanticActivationDescriptors[feature_index] = descriptor;
}

const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_descriptor(uint64 feature_bit)
{
	int feature_index;

	if (!semantic_activation_feature_index(feature_bit, &feature_index))
		return NULL;
	return SemanticActivationDescriptors[feature_index];
}

uint64
cluster_semantic_activation_compiled_feature_bitmap(void)
{
	uint64 bitmap = 0;
	int index;

	for (index = 0; index < 64; index++)
		if (SemanticActivationDescriptors[index] != NULL)
			bitmap |= UINT64_C(1) << index;
	return bitmap;
}

static bool
semantic_activation_ack_wire_value_valid(
	const ClusterSemanticActivationAckWireV1 *message)
{
	if (message == NULL
		|| message->kind < CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->kind > CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| message->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| message->result > CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED
		|| message->coordinator_node >= CLUSTER_MAX_NODES
		|| message->member_node >= CLUSTER_MAX_NODES
		|| (message->coordinator_node < 64
			? (message->admitted_members_lo
				   & (UINT64_C(1) << message->coordinator_node)) == 0
			: (message->admitted_members_hi
				   & (UINT64_C(1) << (message->coordinator_node - 64))) == 0)
		|| (message->member_node < 64
			? (message->admitted_members_lo
				   & (UINT64_C(1) << message->member_node)) == 0
			: (message->admitted_members_hi
				   & (UINT64_C(1) << (message->member_node - 64))) == 0)
		|| message->record_generation == 0 || message->round_nonce == 0
		|| (message->stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& message->capability_sample_digest != 0)
		|| (message->stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& message->capability_sample_digest == 0))
		return false;
	if (message->kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST)
		return message->result
				   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
			&& message->reason == 0
			&& message->boot_id == 0
			&& message->admitted_incarnation == 0
			&& message->capability_word == 0;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK)
		return message->reason == 0
			&& message->boot_id != 0
			&& message->admitted_incarnation != 0
			&& message->capability_word != 0;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED)
		return message->reason > CLUSTER_SEMANTIC_ACTIVATION_OK
			&& message->reason <= CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
			&& message->boot_id == 0
			&& message->admitted_incarnation == 0
			&& message->capability_word == 0;
	return false;
}

bool
cluster_semantic_activation_ack_wire_encode(
	const ClusterSemanticActivationAckWireV1 *message,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	if (bytes == NULL || !semantic_activation_ack_wire_value_valid(message))
		return false;

	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC);
	semantic_activation_write_u16_le(encoded + 4,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION);
	encoded[6] = message->kind;
	encoded[7] = message->stage;
	semantic_activation_write_u32_le(encoded + 8, message->result);
	semantic_activation_write_u32_le(encoded + 12, message->reason);
	semantic_activation_write_u32_le(encoded + 16, message->coordinator_node);
	semantic_activation_write_u32_le(encoded + 20, message->member_node);
	semantic_activation_write_u64_le(encoded + 24, message->transition_epoch);
	semantic_activation_write_u64_le(encoded + 32, message->record_generation);
	semantic_activation_write_u64_le(encoded + 40, message->round_nonce);
	semantic_activation_write_u64_le(encoded + 48, message->source_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 56, message->target_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 64, message->rollback_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 72, message->admitted_members_lo);
	semantic_activation_write_u64_le(encoded + 80, message->admitted_members_hi);
	semantic_activation_write_u64_le(encoded + 88, message->capability_sample_digest);
	semantic_activation_write_u64_le(encoded + 96, message->boot_id);
	semantic_activation_write_u64_le(encoded + 104, message->admitted_incarnation);
	semantic_activation_write_u32_le(encoded + 112, message->capability_word);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_semantic_activation_ack_wire_decode(
	const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES],
	ClusterSemanticActivationAckWireV1 *message)
{
	ClusterSemanticActivationAckWireV1 decoded;

	if (bytes == NULL || message == NULL)
		return false;
	if (semantic_activation_read_u32_le(bytes)
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC
		|| semantic_activation_read_u16_le(bytes + 4)
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION
		|| !semantic_activation_bytes_are_zero(bytes + 116, 4))
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.kind = bytes[6];
	decoded.stage = bytes[7];
	decoded.result = semantic_activation_read_u32_le(bytes + 8);
	decoded.reason = semantic_activation_read_u32_le(bytes + 12);
	decoded.coordinator_node = semantic_activation_read_u32_le(bytes + 16);
	decoded.member_node = semantic_activation_read_u32_le(bytes + 20);
	decoded.transition_epoch = semantic_activation_read_u64_le(bytes + 24);
	decoded.record_generation = semantic_activation_read_u64_le(bytes + 32);
	decoded.round_nonce = semantic_activation_read_u64_le(bytes + 40);
	decoded.source_feature_bitmap = semantic_activation_read_u64_le(bytes + 48);
	decoded.target_feature_bitmap = semantic_activation_read_u64_le(bytes + 56);
	decoded.rollback_feature_bitmap = semantic_activation_read_u64_le(bytes + 64);
	decoded.admitted_members_lo = semantic_activation_read_u64_le(bytes + 72);
	decoded.admitted_members_hi = semantic_activation_read_u64_le(bytes + 80);
	decoded.capability_sample_digest = semantic_activation_read_u64_le(bytes + 88);
	decoded.boot_id = semantic_activation_read_u64_le(bytes + 96);
	decoded.admitted_incarnation = semantic_activation_read_u64_le(bytes + 104);
	decoded.capability_word = semantic_activation_read_u32_le(bytes + 112);
	if (!semantic_activation_ack_wire_value_valid(&decoded))
		return false;
	*message = decoded;
	return true;
}

bool
cluster_semantic_activation_record_encode(const ClusterSemanticActivationRecord *record,
										  uint8 bytes[512])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	pg_crc32c crc;

	if (record == NULL || bytes == NULL || record->record_generation == 0
		|| record->phase < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| record->phase > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| (record->phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
			&& record->rollback_feature_bitmap != 0))
		return false;

	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded, CLUSTER_SEMANTIC_RECORD_MAGIC);
	semantic_activation_write_u16_le(encoded + 4, CLUSTER_SEMANTIC_RECORD_VERSION);
	semantic_activation_write_u16_le(encoded + 6, CLUSTER_SEMANTIC_RECORD_HEADER_LEN);
	semantic_activation_write_u64_le(encoded + 8, record->record_generation);
	encoded[16] = (uint8)record->phase;
	semantic_activation_write_u64_le(encoded + 24, record->source_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 32, record->target_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 40, record->transition_epoch);
	semantic_activation_write_u32_le(encoded + 48, record->coordinator_node);
	semantic_activation_write_u64_le(encoded + 56, record->coordinator_incarnation);
	semantic_activation_write_u64_le(encoded + 64, record->admitted_members_lo);
	semantic_activation_write_u64_le(encoded + 72, record->admitted_members_hi);
	semantic_activation_write_u64_le(encoded + 80, record->capability_sample_digest);
	semantic_activation_write_u64_le(encoded + 88, record->rollback_feature_bitmap);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, encoded, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	semantic_activation_write_u32_le(encoded + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET, (uint32)crc);

	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_semantic_activation_record_decode(const uint8 bytes[512],
										  ClusterSemanticActivationRecord *record,
										  ClusterSemanticActivationRefusal *refusal)
{
	ClusterSemanticActivationRecord decoded;
	uint32 stored_crc;
	pg_crc32c crc;

	if (bytes == NULL || record == NULL) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return false;
	}

	memset(&decoded, 0, sizeof(decoded));
	if (semantic_activation_read_u32_le(bytes) != CLUSTER_SEMANTIC_RECORD_MAGIC
		|| semantic_activation_read_u16_le(bytes + 4) != CLUSTER_SEMANTIC_RECORD_VERSION
		|| semantic_activation_read_u16_le(bytes + 6) != CLUSTER_SEMANTIC_RECORD_HEADER_LEN
		|| semantic_activation_read_u64_le(bytes + 8) == 0
		|| bytes[16] < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| bytes[16] > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| !semantic_activation_bytes_are_zero(bytes + 17, 7)
		|| !semantic_activation_bytes_are_zero(bytes + 52, 4)
		|| !semantic_activation_bytes_are_zero(bytes + 100,
											   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES - 100)) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	stored_crc = semantic_activation_read_u32_le(bytes + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	if (stored_crc != (uint32)crc) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	decoded.record_generation = semantic_activation_read_u64_le(bytes + 8);
	decoded.phase = (ClusterSemanticActivationPhase)bytes[16];
	decoded.source_feature_bitmap = semantic_activation_read_u64_le(bytes + 24);
	decoded.target_feature_bitmap = semantic_activation_read_u64_le(bytes + 32);
	decoded.transition_epoch = semantic_activation_read_u64_le(bytes + 40);
	decoded.coordinator_node = semantic_activation_read_u32_le(bytes + 48);
	decoded.coordinator_incarnation = semantic_activation_read_u64_le(bytes + 56);
	decoded.admitted_members_lo = semantic_activation_read_u64_le(bytes + 64);
	decoded.admitted_members_hi = semantic_activation_read_u64_le(bytes + 72);
	decoded.capability_sample_digest = semantic_activation_read_u64_le(bytes + 80);
	decoded.rollback_feature_bitmap = semantic_activation_read_u64_le(bytes + 88);
	if (decoded.phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		&& decoded.rollback_feature_bitmap != 0) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										decoded.record_generation);
		return false;
	}

	*record = decoded;
	semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_OK, 0,
									decoded.record_generation);
	return true;
}

static void
semantic_activation_lmon_consume_phase3(void)
{
	ClusterReplacementPhase3HandoffItem item;

	while (cluster_replacement_phase3_handoff_poll_local(&item)) {
		const ClusterReplacementWireMessage *message = &item.message;
		uint64 current_epoch = cluster_epoch_get_current();

		/* The GES ingress already checked these fields.  Formation LMON
		 * rechecks them after the process-local handoff so a reconnect or
		 * formation change between producer and consumer is zero-mutation. */
		if (message->phase
				!= CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY
			|| message->target_node_id != item.authenticated_source_node_id
			|| item.local_receiver_node_id != cluster_node_id
			|| item.control_connection_generation == 0
			|| message->epoch == UINT64_MAX
			|| message->epoch + 1 != current_epoch
			|| message->body.phase3.jcmk_generation == 0
			|| message->body.phase3.episode_state_generation == 0
			|| message->body.phase3.reserved != 0
			|| message->grammar_fingerprint
				   != CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT)
			continue;
		if (!cluster_sf_peer_capability_generation_matches(
				item.authenticated_source_node_id,
				CLUSTER_REPLACEMENT_PHASE3_REQUIRED_CAPS,
				item.control_connection_generation))
			continue;
		(void)cluster_reconfig_lmon_observe_replacement_ready(&item);
	}
}

static void
semantic_activation_lmon_consume_utility(void)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	ClusterSemanticActivationResult result;
	const ClusterSemanticActivationDescriptor *round_descriptor = NULL;
	uint32 required_caps = 0;
	uint32 effects = SEMANTIC_ACTIVATION_EFFECT_NONE;

	if (!semantic_activation_utility_mailbox_poll(&request))
		return;

	memset(&refusal, 0, sizeof(refusal));
	if (request.action != CLUSTER_SEMANTIC_ENABLE_ALL
		|| !semantic_activation_round_descriptor(
			request.source_feature_bitmap,
			request.target_feature_bitmap,
			request.rollback_feature_bitmap, &round_descriptor,
			&required_caps)) {
		semantic_activation_set_refusal(
			&refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
			request.expected_record_generation);
	} else {
		if (semantic_activation_lmon_prepare_cas_seq != 0) {
			if (semantic_activation_ack_lmon_install_prepare(&request))
				return;
			semantic_activation_set_refusal(
				&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
				CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				request.expected_record_generation);
			goto complete;
		}
		if (round_descriptor != &r4_descriptor) {
			SemanticActivationAdmissionSnapshot snapshot;
			uint32 local_capability_word;

			result = round_descriptor->pre_prepare_readiness(
				request.expected_record_generation, &refusal);
			local_capability_word = cluster_ic_local_capability_word();
			if (result == CLUSTER_SEMANTIC_ACTIVATION_OK
				&& (local_capability_word & required_caps) == required_caps
			&& semantic_activation_snapshot(&snapshot)
			&& semantic_activation_ack_lmon_begin_sample_round(
				&request, &snapshot)) {
				(void) semantic_activation_ack_lmon_submit_prepare(
					&request, &snapshot);
				return;
			}
			if (result == CLUSTER_SEMANTIC_ACTIVATION_OK)
				semantic_activation_set_refusal(
					&refusal,
					CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
					round_descriptor->feature_bit,
					request.expected_record_generation);
			goto complete;
		}
		result = semantic_activation_preflight(
			request.action, request.expected_record_generation, &refusal,
			&effects);
		if (semantic_activation_preopen_pgrd_setup_allowed(
				request.action, result)) {
			ClusterSemanticFormationBinding formation = {
				.utility_request_seq = request.request_seq,
				.formation_epoch = cluster_epoch_get_current(),
				.coordinator_incarnation
					= cluster_qvotec_get_self_incarnation(),
				.expected_record_generation
					= request.expected_record_generation,
			};
			ClusterUndoRootDescriptorReadCompletion pgrd_read_completion;
			ClusterSemanticActivationResult pgrd_result;
			ClusterUndoSmgrRootMirrorState mirror_state;
			uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
			char root_directory[MAXPGPATH];
			uint64 system_identifier = GetSystemIdentifier();
			bool have_root_directory;

			if (semantic_activation_lmon_pgrd_request_seq != 0) {
				if (semantic_activation_lmon_pgrd_utility_request_seq
						!= request.request_seq) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (!cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
						   semantic_activation_lmon_pgrd_request_seq,
						   &pgrd_result)) {
					return;
				} else {
					uint64 completed_request_seq
						= semantic_activation_lmon_pgrd_request_seq;
					bool proof_valid = false;

					semantic_activation_lmon_pgrd_request_seq = 0;
					semantic_activation_lmon_pgrd_utility_request_seq = 0;
					have_root_directory
						= semantic_activation_lmon_shared_pgrd_root_directory(
							root_directory);
					if (pgrd_result == CLUSTER_SEMANTIC_ACTIVATION_OK
						&& semantic_activation_lmon_pgrd_candidate_request_seq
							   == completed_request_seq
						&& cluster_semantic_activation_qvotec_pgrd_formation_matches(
							&semantic_activation_lmon_pgrd_formation)
						&& have_root_directory) {
						mirror_state
							= cluster_undo_smgr_root_descriptor_read_candidate(
								root_directory, candidate);
						proof_valid
							= mirror_state
								  == CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT
							  && memcmp(candidate,
										semantic_activation_lmon_pgrd_candidate,
										CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
									 == 0
							  && semantic_activation_pgrd_snapshot_publish(
								  semantic_activation_lmon_pgrd_candidate);
					}
					semantic_activation_lmon_pgrd_candidate_request_seq = 0;
					memset(semantic_activation_lmon_pgrd_candidate, 0,
						   sizeof(semantic_activation_lmon_pgrd_candidate));
					if (!proof_valid) {
						semantic_activation_pgrd_snapshot_clear();
						if (pgrd_result != CLUSTER_SEMANTIC_ACTIVATION_OK)
						semantic_activation_set_refusal(
							&refusal, pgrd_result,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
						else
						semantic_activation_set_refusal(
							&refusal,
							CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
					}
				}
			} else if (semantic_activation_lmon_pgrd_read_request_seq != 0) {
				if (semantic_activation_lmon_pgrd_read_utility_request_seq
						!= request.request_seq) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (!semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
						   semantic_activation_lmon_pgrd_read_request_seq,
						   &pgrd_read_completion)) {
					return;
				} else {
					semantic_activation_lmon_pgrd_read_request_seq = 0;
					semantic_activation_lmon_pgrd_read_utility_request_seq = 0;
					semantic_activation_lmon_pgrd_formation
						= semantic_activation_lmon_pgrd_read_formation;
					have_root_directory
						= semantic_activation_lmon_shared_pgrd_root_directory(
							root_directory);
					if (!cluster_semantic_activation_qvotec_pgrd_formation_matches(
							&semantic_activation_lmon_pgrd_read_formation)
						|| pgrd_read_completion.state
							!= CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
						|| !have_root_directory
						|| !semantic_activation_lmon_publish_fresh_shared_pgrd(
							root_directory,
							&semantic_activation_lmon_pgrd_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_request_seq)) {
						semantic_activation_pgrd_snapshot_clear();
						semantic_activation_set_refusal(
							&refusal,
							CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
					} else {
						semantic_activation_lmon_pgrd_utility_request_seq
							= request.request_seq;
						return;
					}
				}
			} else {
				have_root_directory
					= semantic_activation_lmon_shared_pgrd_root_directory(
						root_directory);
				if (have_root_directory
					&& !cluster_semantic_activation_qvotec_pgrd_formation_matches(
						&formation)) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (have_root_directory) {
					mirror_state
						= cluster_undo_smgr_root_descriptor_read_candidate(
							root_directory, candidate);
					if (mirror_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
						semantic_activation_pgrd_snapshot_clear();
					semantic_activation_lmon_pgrd_formation = formation;
					if (mirror_state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT
						&& semantic_activation_lmon_submit_pgrd_candidate(
							candidate,
							&semantic_activation_lmon_pgrd_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_request_seq)) {
						semantic_activation_lmon_pgrd_utility_request_seq
							= request.request_seq;
						return;
					}
					semantic_activation_lmon_pgrd_read_formation = formation;
					if (mirror_state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT
						&& semantic_activation_undo_root_descriptor_read_mailbox_submit(
							&semantic_activation_lmon_pgrd_read_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_read_request_seq)) {
						semantic_activation_lmon_pgrd_read_utility_request_seq
							= request.request_seq;
						return;
					}
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				}
			}

			/* ADMITTED grants entry to PREPARE only.  Source close is the
			 * next durable-FSM edge after the PREPARE CAS, so this carrier
			 * remains nonterminal until that state is installed. */
			if (refusal.result == CLUSTER_SEMANTIC_ACTIVATION_OK) {
				SemanticActivationAdmissionSnapshot snapshot;
				uint32 local_capability_word
					= cluster_ic_local_capability_word();

				if ((local_capability_word & required_caps)
					== required_caps) {
					if (semantic_activation_snapshot(&snapshot)
						&& semantic_activation_ack_lmon_begin_sample_round(
							&request, &snapshot)) {
						(void)semantic_activation_ack_lmon_submit_prepare(
							&request, &snapshot);
						return;
					}
					semantic_activation_set_refusal(
						&refusal,
						CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						round_descriptor->feature_bit,
						request.expected_record_generation);
				}
			}
			if (refusal.result == CLUSTER_SEMANTIC_ACTIVATION_OK) {
				result = CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
				semantic_activation_set_refusal(
					&refusal, result,
					CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
					request.expected_record_generation);
			}
		}
	}

complete:
	(void)semantic_activation_utility_mailbox_complete(
		request.request_seq, refusal.result, refusal.feature_bit,
		refusal.expected_generation);
}

static bool
semantic_activation_lmon_publish_gate(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 active_bits,
	uint64 record_generation, uint64 formation_epoch, bool transition_closed)
{
	uint64 expected_seq;

	if (snapshot == NULL || SemanticActivationShmem == NULL
		|| snapshot->seq > UINT64_MAX - 2)
		return false;
	expected_seq = snapshot->seq;
	if (!pg_atomic_compare_exchange_u64(
			&SemanticActivationShmem->admission_seq, &expected_seq,
			snapshot->seq + 1))
		return false;
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->active_bits, active_bits);
	pg_atomic_write_u64(&SemanticActivationShmem->record_generation,
						record_generation);
	pg_atomic_write_u64(&SemanticActivationShmem->formation_epoch,
						formation_epoch);
	pg_atomic_write_u32(&SemanticActivationShmem->transition_closed,
						transition_closed ? 1 : 0);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->admission_seq,
						snapshot->seq + 2);
	return true;
}

static void
semantic_activation_lmon_sync_durable_record(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 current_epoch)
{
	ClusterSemanticActivationReadCompletion completion;
	uint64 request_seq;
	uint64 members_lo;
	uint64 members_hi;
	uint64 membership_epoch;
	bool local_is_member;

	if (semantic_activation_lmon_record_read_seq == 0) {
		if (semantic_activation_record_read_mailbox_submit(&request_seq))
			semantic_activation_lmon_record_read_seq = request_seq;
		return;
	}
	if (!semantic_activation_record_read_mailbox_poll_completion(
			semantic_activation_lmon_record_read_seq, &completion))
		return;
	semantic_activation_lmon_record_read_seq = 0;

	if (completion.result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !completion.implicit_open
		|| !semantic_activation_bytes_are_zero(
			completion.selected_bytes,
			sizeof(completion.selected_bytes)))
		return;

	/* Majority legacy zero has no member tuple of its own.  It may open only
	 * the legacy SOURCE gate after the current formation has a nonempty exact
	 * membership SSOT containing this coordinator and remains on one epoch
	 * across the sample.  Nonzero PGSA records stay closed until the full
	 * member/capability ACK table is installed. */
	if (!cluster_reconfig_lmon_snapshot_admitted_membership(
			&members_lo, &members_hi, &membership_epoch)
		|| membership_epoch != current_epoch
		|| cluster_epoch_get_current() != current_epoch
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;
	local_is_member
		= cluster_node_id < 64
			  ? (members_lo & (UINT64_C(1) << cluster_node_id)) != 0
			  : (members_hi
				 & (UINT64_C(1) << (cluster_node_id - 64))) != 0;
	if (!local_is_member)
		return;

	(void)semantic_activation_lmon_publish_gate(
		snapshot, 0, 0, current_epoch, false);
}

void
cluster_semantic_activation_lmon_tick(void)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;

	semantic_activation_ack_lmon_drain();
	if (SemanticActivationShmem == NULL)
		return;
	/* R4 cutover contract (verified implementation): after a post-bit22 postmaster
	 * restart / reformation the volatile ACK table is zeroed — re-run the
	 * recovery-only OPEN_APPLIED bootstrap from the durable OPEN and arm
	 * OPEN_PROOF before any gate/advance consumes the table.  Fail-closed
	 * and idempotent; the tick retries until every precondition holds. */
	(void) cluster_semantic_activation_restore_open_proof_if_active();
	semantic_activation_lmon_consume_phase3();
	if (semantic_activation_ack_lmon_progress_member_barrier())
		return;
	/* RF-ROOT P9 verification (implementation): the bit22 cutover round is
	 * SQL-driven (no utility request); the coordinator-side stage machine
	 * runs from the tick. */
	if (semantic_activation_ack_lmon_bit22_advance())
		return;
	semantic_activation_lmon_consume_utility();
	/*
	 * D13 owns validated majority-zero/durable-OPEN publication.  Until that
	 * proof is available, odd or unreadable state remains fail-closed.
	 */
	if (!semantic_activation_snapshot(&snapshot))
		return;

	current_epoch = cluster_epoch_get_current();
	if (snapshot.formation_epoch == current_epoch) {
		if (snapshot.transition_closed
			&& semantic_activation_lmon_prepare_cas_seq == 0)
			semantic_activation_lmon_sync_durable_record(
				&snapshot, current_epoch);
		return;
	}
	if (snapshot.seq > UINT64_MAX - 2)
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("semantic activation admission sequence exhausted"),
					errhint("Retain the shared-memory image and restart the cluster.")));

	semantic_activation_lmon_member_pgrd_read_request_seq = 0;
	semantic_activation_pgrd_snapshot_clear();
	(void)semantic_activation_lmon_publish_gate(
		&snapshot, snapshot.active_bits, snapshot.record_generation,
		current_epoch, true);
}

ClusterSemanticActivationResult
cluster_semantic_activation_submit(ClusterSemanticActivationAction action,
								   ClusterSemanticActivationRefusal *refusal)
{
	SemanticActivationAdmissionSnapshot snapshot;
	const ClusterSemanticActivationDescriptor *round_descriptor = NULL;
	ClusterSemanticActivationResult result;
	uint64 compiled_bits;
	uint64 remaining_bits;
	uint64 target_bits;
	uint64 request_seq;
	uint32 required_caps = 0;
	uint32 effects;

	if (!semantic_activation_snapshot(&snapshot)) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	compiled_bits = cluster_semantic_activation_compiled_feature_bitmap();
	if (action != CLUSTER_SEMANTIC_ENABLE_ALL
		|| (snapshot.active_bits & ~compiled_bits) != 0) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
			snapshot.record_generation);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	if (snapshot.active_bits == 0)
		target_bits = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	else {
		remaining_bits = compiled_bits & ~snapshot.active_bits;
		if (snapshot.transition_closed
			|| (snapshot.active_bits
				& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) == 0
			|| remaining_bits == 0
			|| (remaining_bits & (remaining_bits - 1)) != 0) {
			semantic_activation_set_refusal(
				refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
				snapshot.record_generation);
			return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
		}
		target_bits = snapshot.active_bits | remaining_bits;
	}

	if (!semantic_activation_round_descriptor(
			snapshot.active_bits, target_bits, 0, &round_descriptor,
			&required_caps)) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
			snapshot.record_generation);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	if (round_descriptor == &r4_descriptor) {
		result = semantic_activation_preflight(
			action, snapshot.record_generation, refusal, &effects);
		if (!semantic_activation_preopen_pgrd_setup_allowed(action, result))
			return result;
	} else {
		result = round_descriptor->pre_prepare_readiness(
			snapshot.record_generation, refusal);
		if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
			return result;
	}

	if (!semantic_activation_utility_mailbox_submit(
			action, snapshot.active_bits,
			target_bits, 0,
			snapshot.record_generation, &request_seq)) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD, 0,
			snapshot.record_generation);
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	}

	return semantic_activation_utility_mailbox_wait(request_seq, refusal);
}

const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_r4_descriptor(void)
{
	return &r4_descriptor;
}

const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_r11_resource_x_descriptor(void)
{
	return &r11_resource_x_descriptor;
}

#ifdef USE_PGRAC_CLUSTER

#include "fmgr.h"
#include "miscadmin.h" /* superuser() */

PG_FUNCTION_INFO_V1(pgrac_r4_bit22_cutover_begin);

/*
 * cutover_round_capability_digest -- RF-ROOT P7 (contract step ④e): a
 * deterministic round-identity digest built from the members' admitted
 * incarnations and capability samples (the bit22 round skips the R4 SAMPLE
 * stage, so the coordinator constructs the digest directly).  The digest
 * only needs to bind the round identity — members never verify its value,
 * the ACK table carries it for round identity binding.
 */
static uint64
cutover_round_capability_digest(uint64 members_lo)
{
	uint64 digest = UINT64_C(0x9e3779b97f4a7c15); /* FNV-ish seed */
	int node;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		uint32 word;
		uint32 gen;

		if (node >= 64 || (members_lo & (UINT64_C(1) << node)) == 0)
			continue;
		digest ^= (uint64)node * UINT64_C(0x100000001b3);
		digest ^= cluster_membership_get_last_admitted_incarnation(node);
		if (cluster_sf_peer_capability_word_sample(
				node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
				&word, &gen))
			digest ^= (uint64)word ^ ((uint64)gen << 32);
	}
	return digest;
}

/*
 * pgrac_r4_bit22_cutover_begin -- RF-ROOT P7 (contract step ④e): operator
 * entry.  Coordinator-only (superuser + coordinator identity).  Constructs
 * the round from the current formation, builds the migration image from the
 * live shared state, then stages create_prepared + seam + PREPARED REQUEST
 * via cluster_r4_bit22_cutover_begin.  Returns true when staged; the round
 * completes asynchronously (member CLOSED-ACK -> OPEN_APPLIED advance).
 */
Datum
pgrac_r4_bit22_cutover_begin(PG_FUNCTION_ARGS)
{
	ClusterControlRootMigrationRoundV1 round;
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	int32 current_coordinator_node;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for pgrac_r4_bit22_cutover_begin"),
				 errhint("Only the cluster superuser may drive the bit22 cutover.")));
	if (!semantic_activation_snapshot(&snapshot)
		|| !semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)
		|| cluster_node_id != current_coordinator_node)
		PG_RETURN_BOOL(false);

	memset(&round, 0, sizeof(round));
	/* RF-ROOT P9 verification (contract): every wire-encoded field must be
	 * filled — encode_round rejects a zeroed magic/version/bytes and the
	 * create proof binds the coordinator incarnation. */
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = snapshot.record_generation + 1;
	round.transition_epoch = current_epoch;
	round.source_feature_bitmap = snapshot.active_bits;
	round.target_feature_bitmap = snapshot.active_bits
		| PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = current_members_lo;
	round.admitted_bitmap_high = current_members_hi;
	round.capability_sample_digest
		= cutover_round_capability_digest(current_members_lo);
	round.coordinator_incarnation = cluster_qvotec_get_self_incarnation();
	round.coordinator_node_id = cluster_node_id;

	/* RF-ROOT P9 verification (implementation): the migration image is built by the
	 * LMON tick after the all-member source-close BARRIER COMPLETE (the
	 * online first-open round freezes every member's writers first). */
	{
		bool br = cluster_r4_bit22_cutover_begin(NULL, &round);
		PG_RETURN_BOOL(br);
	}
}

#endif /* USE_PGRAC_CLUSTER */
