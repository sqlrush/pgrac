/*-------------------------------------------------------------------------
 *
 * cluster_pcm_x_convert.h
 *	  PCM-X conversion wire ABI and external shared-memory substrate.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pcm_x_convert.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Exported symbols use the PcmX or
 *	  cluster_pcm_x prefix.  Shared-memory references are offsets or slot
 *	  indexes; no process-local pointer is stored in the region.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_X_CONVERT_H
#define CLUSTER_PCM_X_CONVERT_H

#include "access/transam.h"
#include "cluster/cluster_lmd.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"


#define PCM_X_PROTOCOL_NODE_LIMIT 32
#define PCM_X_SHMEM_REGION_NAME "pgrac cluster pcm convert queue"
#define PCM_X_SHMEM_MAGIC ((uint32)0x50435851) /* "PCXQ" */
#define PCM_X_SHMEM_LAYOUT_VERSION ((uint32)12)
#define PCM_X_INVALID_SLOT_INDEX ((Size) - 1)
#define PCM_X_LOCK_PARTITIONS NUM_BUFFER_PARTITIONS
#define PCM_X_LWLOCK_COUNT (1 + 2 * PCM_X_LOCK_PARTITIONS)
#define PCM_X_HEADER_LOCK_PADDING 92
/* Ticket ids share the low 63 bits of the zero-growth GrdEntry pending-X
 * cookie; the high bit is the queue-vs-legacy namespace discriminator. */
#define PCM_X_MASTER_TICKET_ID_MAX UINT64CONST(0x7fffffffffffffff)

/* Image ids are opaque on the wire.  Reserve high bits 110 for PCM-X so an
 * image pull carried by the legacy block request/reply transport cannot
 * collide with requester ids (0x) or supported-node local-upgrade ids (10x).
 * Bit 61 is canonical zero; decoders reject the 111 alias.  The allocator
 * passes its raw monotonic sequence without masking; values past the 56-bit
 * domain are exhausted, never wrapped. */
#define PCM_X_IMAGE_ID_DOMAIN UINT64CONST(0xc000000000000000)
#define PCM_X_IMAGE_ID_DOMAIN_MASK UINT64CONST(0xe000000000000000)
#define PCM_X_IMAGE_ID_MASTER_SHIFT 56
#define PCM_X_IMAGE_ID_MASTER_MASK UINT64CONST(0x1f)
#define PCM_X_IMAGE_ID_SEQ_MASK ((UINT64CONST(1) << PCM_X_IMAGE_ID_MASTER_SHIFT) - 1)

static inline bool
cluster_pcm_x_image_id_encode(int32 master_node, uint64 raw_sequence, uint64 *image_id_out)
{
	if (image_id_out != NULL)
		*image_id_out = 0;
	if (image_id_out == NULL || master_node < 0 || master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| raw_sequence == 0 || raw_sequence > PCM_X_IMAGE_ID_SEQ_MASK)
		return false;
	*image_id_out = PCM_X_IMAGE_ID_DOMAIN | ((uint64)master_node << PCM_X_IMAGE_ID_MASTER_SHIFT)
					| raw_sequence;
	return true;
}

static inline bool
cluster_pcm_x_image_id_decode(uint64 image_id, int32 *master_node_out, uint64 *raw_sequence_out)
{
	uint64 raw_sequence;

	if ((image_id & PCM_X_IMAGE_ID_DOMAIN_MASK) != PCM_X_IMAGE_ID_DOMAIN)
		return false;
	raw_sequence = image_id & PCM_X_IMAGE_ID_SEQ_MASK;
	if (raw_sequence == 0)
		return false;
	if (master_node_out != NULL)
		*master_node_out
			= (int32)((image_id >> PCM_X_IMAGE_ID_MASTER_SHIFT) & PCM_X_IMAGE_ID_MASTER_MASK);
	if (raw_sequence_out != NULL)
		*raw_sequence_out = raw_sequence;
	return true;
}

/* Every pool shares these lifecycle values in its persistent slot header. */
#define PCM_X_SLOT_FREE ((uint32)0)
#define PCM_X_SLOT_RESERVED_NONVISIBLE ((uint32)1)
#define PCM_X_SLOT_DETACHING ((uint32)9)
#define PCM_X_SLOT_RECOVERY_BLOCKED ((uint32)10)
#define PCM_X_SLOT_GENERATION_EXHAUSTED ((uint32)11)
/* state_flags packs the lifecycle value below local queue flag bits. */
#define PCM_X_SLOT_STATE_BITS 8
#define PCM_X_SLOT_STATE_MASK UINT32_C(0xff)
#define PCM_X_SLOT_FLAGS_SHIFT PCM_X_SLOT_STATE_BITS
#define PCM_X_SLOT_FLAGS_MASK (~PCM_X_SLOT_STATE_MASK)

/* Immutable waiter and ticket identities carried by the PCM-X protocol. */
typedef struct PcmXWaitIdentity {
	BufferTag tag;
	int32 node_id;
	uint32 procno;
	TransactionId xid;
	uint64 cluster_epoch;
	uint64 request_id;
	uint64 wait_seq;
	uint64 base_own_generation;
} PcmXWaitIdentity;

/* Exact local-holder identity; the same buffer may have many backend holders. */
typedef struct PcmXLocalHolderKey {
	PcmXWaitIdentity identity;
	int32 buffer_id;
	uint32 reserved;
} PcmXLocalHolderKey;

/* Process-local exact reference to one persistent shared-memory slot. */
typedef struct PcmXSlotRef {
	Size slot_index;
	uint64 slot_generation;
} PcmXSlotRef;

typedef struct PcmXTicketHandle {
	uint64 ticket_id;
	uint64 queue_generation;
} PcmXTicketHandle;

typedef struct PcmXPrehandleKey {
	uint64 sender_session_incarnation;
	uint64 prehandle_sequence;
} PcmXPrehandleKey;

typedef struct PcmXTicketRef {
	PcmXWaitIdentity identity;
	PcmXTicketHandle handle;
	uint64 grant_generation;
} PcmXTicketRef;

typedef struct PcmXImageToken {
	uint64 image_id;
	uint64 source_own_generation;
	uint64 page_scn;
	uint64 page_lsn;
	uint32 source_node;
	uint32 page_checksum;
} PcmXImageToken;

StaticAssertDecl(sizeof(PcmXWaitIdentity) == 64, "PCM-X wait identity ABI");
StaticAssertDecl(sizeof(PcmXLocalHolderKey) == 72, "PCM-X local holder key ABI");
StaticAssertDecl(sizeof(PcmXSlotRef) == 16, "PCM-X slot ref ABI");
StaticAssertDecl(sizeof(PcmXTicketHandle) == 16, "PCM-X ticket handle ABI");
StaticAssertDecl(sizeof(PcmXPrehandleKey) == 16, "PCM-X prehandle key ABI");
StaticAssertDecl(sizeof(PcmXTicketRef) == 88, "PCM-X ticket ref ABI");
StaticAssertDecl(sizeof(PcmXImageToken) == 40, "PCM-X image token ABI");


/* Exact-length DATA payloads.  Envelope framing is owned by the IC layer. */
typedef struct PcmXEnqueuePayload {
	PcmXWaitIdentity identity;
	PcmXPrehandleKey prehandle;
} PcmXEnqueuePayload;

typedef struct PcmXPrehandleCancelPayload {
	PcmXWaitIdentity identity;
	PcmXPrehandleKey prehandle;
} PcmXPrehandleCancelPayload;

typedef struct PcmXAdmitAckPayload {
	PcmXTicketRef ref;
	PcmXPrehandleKey prehandle;
	uint32 result;
	uint16 phase;
	uint16 flags;
} PcmXAdmitAckPayload;

/*
 * Generic 96-byte phase payload.  For type 48, bytes [88,96) are one
 * lossless blocker-set generation encoded across reason/phase/flags:
 * all-zero is the master->holder PROBE request; any nonzero value is the
 * generation-exact master->holder ACK for a completed 45-47 set.  Both arms
 * keep the same direction, payload size and capability gate.
 */
typedef struct PcmXPhasePayload {
	PcmXTicketRef ref;
	uint32 reason;
	uint16 phase;
	uint16 flags;
} PcmXPhasePayload;

typedef struct PcmXRevokePayload {
	PcmXTicketRef ref;
	uint64 image_id;
} PcmXRevokePayload;

typedef struct PcmXGrantPayload {
	PcmXTicketRef ref;
	PcmXImageToken image;
} PcmXGrantPayload;

typedef struct PcmXInstallReadyPayload {
	PcmXTicketRef ref;
	uint64 image_id;
	uint32 result;
	uint16 phase;
	uint16 flags;
} PcmXInstallReadyPayload;

typedef struct PcmXFinalAckPayload {
	PcmXTicketRef ref;
	uint64 image_id;
	uint64 committed_own_generation;
} PcmXFinalAckPayload;

typedef struct PcmXBlockerSetHeaderPayload {
	PcmXTicketRef ref;
	uint64 set_generation;
	uint32 nblockers;
	uint32 set_crc32c;
} PcmXBlockerSetHeaderPayload;

typedef struct PcmXBlockerChunkPayload {
	BufferTag tag;
	int32 requester_node;
	uint32 requester_procno;
	uint32 chunk_no;
	uint64 cluster_epoch;
	uint64 request_id;
	PcmXTicketHandle handle;
	uint64 grant_generation;
	uint64 set_generation;
	ClusterLmdVertex blocker;
} PcmXBlockerChunkPayload;

typedef struct PcmXRetirePayload {
	uint64 cluster_epoch;
	uint64 master_session_incarnation;
	uint64 retire_through_ticket_id;
	int32 sender_node;
	uint32 flags;
} PcmXRetirePayload;

typedef struct PcmXDrainPollPayload {
	PcmXTicketRef ref;
	uint64 drain_generation;
} PcmXDrainPollPayload;

StaticAssertDecl(sizeof(PcmXEnqueuePayload) == 80, "PCM-X ENQUEUE ABI");
StaticAssertDecl(sizeof(PcmXPrehandleCancelPayload) == 80, "PCM-X pre-handle CANCEL ABI");
StaticAssertDecl(sizeof(PcmXAdmitAckPayload) == 112, "PCM-X ADMIT_ACK ABI");
StaticAssertDecl(sizeof(PcmXPhasePayload) == 96, "PCM-X phase ABI");
StaticAssertDecl(sizeof(PcmXRevokePayload) == 96, "PCM-X revoke ABI");
StaticAssertDecl(sizeof(PcmXGrantPayload) == 128, "PCM-X grant ABI");
StaticAssertDecl(sizeof(PcmXInstallReadyPayload) == 104, "PCM-X INSTALL_READY ABI");
StaticAssertDecl(sizeof(PcmXFinalAckPayload) == 104, "PCM-X FINAL_ACK ABI");
StaticAssertDecl(sizeof(PcmXBlockerSetHeaderPayload) == 104, "PCM-X blocker header ABI");
StaticAssertDecl(sizeof(PcmXBlockerChunkPayload) == 128, "PCM-X blocker chunk ABI");
StaticAssertDecl(sizeof(PcmXRetirePayload) == 32, "PCM-X tombstone retire ABI");
StaticAssertDecl(sizeof(PcmXDrainPollPayload) == 96, "PCM-X drain-poll ABI");


/* The region contains exactly these five logical pools. */
typedef enum PcmXPoolKind {
	PCM_X_POOL_MASTER_TAG = 0,
	PCM_X_POOL_MASTER_TICKET,
	PCM_X_POOL_BLOCKER,
	PCM_X_POOL_LOCAL_TAG,
	PCM_X_POOL_LOCAL_MEMBERSHIP,
	PCM_X_POOL_COUNT
} PcmXPoolKind;

/* One membership pool has two fixed, independently managed partitions. */
typedef enum PcmXAllocatorKind {
	PCM_X_ALLOC_MASTER_TAG = 0,
	PCM_X_ALLOC_MASTER_TICKET,
	PCM_X_ALLOC_BLOCKER,
	PCM_X_ALLOC_LOCAL_TAG,
	PCM_X_ALLOC_LOCAL_WAIT,
	PCM_X_ALLOC_LOCAL_HOLDER,
	PCM_X_ALLOC_COUNT
} PcmXAllocatorKind;

/* Seven bounded key spaces over the five logical pools. */
typedef enum PcmXDirectoryKind {
	PCM_X_DIR_MASTER_TAG = 0,
	PCM_X_DIR_MASTER_TICKET_PREHANDLE,
	PCM_X_DIR_MASTER_TICKET_HANDLE,
	PCM_X_DIR_MASTER_TICKET_RETIRE,
	PCM_X_DIR_LOCAL_TAG,
	PCM_X_DIR_LOCAL_WAIT,
	PCM_X_DIR_LOCAL_HOLDER,
	PCM_X_DIR_COUNT
} PcmXDirectoryKind;

typedef enum PcmXAllocatorResult {
	PCM_X_ALLOC_OK = 0,
	PCM_X_ALLOC_NO_CAPACITY,
	PCM_X_ALLOC_STALE_REF,
	PCM_X_ALLOC_BAD_STATE,
	PCM_X_ALLOC_CORRUPT,
	PCM_X_ALLOC_INVALID
} PcmXAllocatorResult;

typedef enum PcmXDirectoryResult {
	PCM_X_DIRECTORY_OK = 0,
	PCM_X_DIRECTORY_NOT_FOUND,
	PCM_X_DIRECTORY_EXISTS,
	PCM_X_DIRECTORY_FULL,
	PCM_X_DIRECTORY_STALE_REF,
	PCM_X_DIRECTORY_CORRUPT,
	PCM_X_DIRECTORY_INVALID
} PcmXDirectoryResult;


typedef enum PcmXRuntimeState {
	PCM_X_RUNTIME_RECOVERY_BLOCKED = 0,
	PCM_X_RUNTIME_ACTIVE,
	PCM_X_RUNTIME_SHUTTING_DOWN
} PcmXRuntimeState;

/* The low 3 bits are the gate phase; the upper 29 bits are an ABA generation. */
#define PCM_X_RUNTIME_GATE_STATE_BITS 3
#define PCM_X_RUNTIME_GATE_STATE_MASK 0x7U
#define PCM_X_RUNTIME_GATE_GENERATION_MAX (PG_UINT32_MAX >> PCM_X_RUNTIME_GATE_STATE_BITS)

/* A gate state and the master-session authority published with it. */
typedef struct PcmXRuntimeSnapshot {
	uint64 master_session_incarnation;
	uint32 gate_generation;
	PcmXRuntimeState state;
} PcmXRuntimeSnapshot;

StaticAssertDecl(sizeof(PcmXRuntimeSnapshot) == 16, "PCM-X runtime snapshot ABI");

typedef enum PcmXMasterTicketState {
	PCM_XT_FREE = PCM_X_SLOT_FREE,
	PCM_XT_RESERVED_NONVISIBLE = PCM_X_SLOT_RESERVED_NONVISIBLE,
	PCM_XT_ADMITTING = 2,
	PCM_XT_QUEUED,
	PCM_XT_ACTIVE_PROBE,
	PCM_XT_ACTIVE_TRANSFER,
	PCM_XT_COMPLETE,
	PCM_XT_CANCELLED,
	PCM_XT_RETIRE_CREDIT,
	PCM_XT_DETACHING = PCM_X_SLOT_DETACHING,
	PCM_XT_RECOVERY_BLOCKED = PCM_X_SLOT_RECOVERY_BLOCKED,
	PCM_XT_GENERATION_EXHAUSTED = PCM_X_SLOT_GENERATION_EXHAUSTED
} PcmXMasterTicketState;

typedef enum PcmXTagState {
	PCM_X_TAG_FREE = PCM_X_SLOT_FREE,
	PCM_X_TAG_RESERVED_NONVISIBLE = PCM_X_SLOT_RESERVED_NONVISIBLE,
	PCM_X_TAG_LIVE = 2,
	PCM_X_TAG_DETACHING = PCM_X_SLOT_DETACHING,
	PCM_X_TAG_RECOVERY_BLOCKED = PCM_X_SLOT_RECOVERY_BLOCKED,
	PCM_X_TAG_GENERATION_EXHAUSTED = PCM_X_SLOT_GENERATION_EXHAUSTED
} PcmXTagState;

typedef enum PcmXLocalMembershipState {
	PCM_XL_FREE = PCM_X_SLOT_FREE,
	PCM_XL_RESERVED_NONVISIBLE = PCM_X_SLOT_RESERVED_NONVISIBLE,
	PCM_XL_JOINED_NONWAITABLE = 2,
	PCM_XL_WAITABLE_FOLLOWER,
	PCM_XL_NODE_LEADER,
	PCM_XL_REMOTE_WAIT,
	PCM_XL_CONTENT_ACTIVE,
	PCM_XL_GRANTED,
	PCM_XL_CANCELLED,
	PCM_XL_DETACHING = PCM_X_SLOT_DETACHING,
	PCM_XL_RECOVERY_BLOCKED = PCM_X_SLOT_RECOVERY_BLOCKED,
	PCM_XL_GENERATION_EXHAUSTED = PCM_X_SLOT_GENERATION_EXHAUSTED,
	/* Holder-only states; values are local shmem lifecycle, never wire ABI. */
	PCM_XL_HOLDER_ACQUIRING = 12,
	PCM_XL_HOLDER_ACTIVE = PCM_XL_CONTENT_ACTIVE,
	PCM_XL_HOLDER_RELEASING = 13
} PcmXLocalMembershipState;

typedef enum PcmXBlockerState {
	PCM_XB_FREE = PCM_X_SLOT_FREE,
	PCM_XB_RESERVED_NONVISIBLE = PCM_X_SLOT_RESERVED_NONVISIBLE,
	PCM_XB_LIVE = 2,
	PCM_XB_DETACHING = PCM_X_SLOT_DETACHING,
	PCM_XB_RECOVERY_BLOCKED = PCM_X_SLOT_RECOVERY_BLOCKED,
	PCM_XB_GENERATION_EXHAUSTED = PCM_X_SLOT_GENERATION_EXHAUSTED
} PcmXBlockerState;

#define PCM_X_BLOCKER_DIRECTION_MASTER UINT32_C(1)
#define PCM_X_BLOCKER_DIRECTION_LOCAL UINT32_C(2)

typedef enum PcmXMasterEvent {
	PCM_X_EVENT_PUBLISH_ADMIT = 0,
	PCM_X_EVENT_ADMIT_CONFIRM,
	PCM_X_EVENT_PROMOTE_HEAD,
	PCM_X_EVENT_BLOCKERS_COMMITTED,
	PCM_X_EVENT_COMMIT_COMPLETE,
	PCM_X_EVENT_CANCEL_EXACT,
	PCM_X_EVENT_DRAIN_EXACT,
	PCM_X_EVENT_RETIRE_ACK_EXACT,
	PCM_X_EVENT_DETACH_EXACT,
	PCM_X_EVENT_FAULT
} PcmXMasterEvent;

typedef enum PcmXStepResult {
	PCM_X_STEP_APPLIED = 0,
	PCM_X_STEP_DUPLICATE,
	PCM_X_STEP_STALE,
	PCM_X_STEP_BLOCKED
} PcmXStepResult;

/* Composite queue operations never expose a partially published admission. */
typedef enum PcmXQueueResult {
	PCM_X_QUEUE_OK = 0,
	PCM_X_QUEUE_DUPLICATE,
	PCM_X_QUEUE_RETIRED,
	PCM_X_QUEUE_NOT_FOUND,
	PCM_X_QUEUE_STALE,
	PCM_X_QUEUE_NO_CAPACITY,
	PCM_X_QUEUE_COUNTER_EXHAUSTED,
	PCM_X_QUEUE_NOT_READY,
	PCM_X_QUEUE_BUSY,
	PCM_X_QUEUE_BAD_STATE,
	PCM_X_QUEUE_CORRUPT,
	PCM_X_QUEUE_INVALID,
	/* Holder admission classifications; process-local only, never wire. */
	PCM_X_QUEUE_GATE_RETRY,
	PCM_X_QUEUE_BARRIER_CLOSED
} PcmXQueueResult;

/* A generation-exact type-47 replay can prove the blocker set DUPLICATE
 * after the ticket has already advanced beyond ACTIVE_PROBE.  Its type-48
 * ACK is still safe to replay; only structural corruption warrants closing
 * the cluster runtime.  A first application retains the stricter rule
 * because its ACK may otherwise outrun an unconsumed probe leg. */
static inline bool
cluster_pcm_x_blocker_commit_completion_requires_recovery(PcmXQueueResult stage_result,
														  PcmXQueueResult completion_result)
{
	if (completion_result == PCM_X_QUEUE_OK || completion_result == PCM_X_QUEUE_DUPLICATE)
		return false;
	if (stage_result == PCM_X_QUEUE_DUPLICATE && completion_result != PCM_X_QUEUE_CORRUPT)
		return false;
	return true;
}

/* One reliable terminal application leg persisted in the master ticket. */
typedef enum PcmXTerminalLegKind {
	PCM_X_TERMINAL_LEG_NONE = 0,
	PCM_X_TERMINAL_LEG_DRAIN,
	PCM_X_TERMINAL_LEG_RETIRE
} PcmXTerminalLegKind;

/* Process-local send token.  Slot indexes and pointers never cross the wire. */
typedef struct PcmXTerminalLegToken {
	uint64 state_sequence;
	uint64 expected_responder_session;
	uint64 drain_generation;
	int32 expected_responder_node;
	uint16 kind;
	uint16 reserved;
} PcmXTerminalLegToken;

StaticAssertDecl(sizeof(PcmXTerminalLegToken) == 32, "PCM-X terminal leg token ABI");

#define PCM_X_ADMIT_F_EXACT_REPLAY UINT32_C(0x01)
#define PCM_X_ADMIT_F_NODE_COALESCED UINT32_C(0x02)
#define PCM_X_ADMIT_F_QUEUE_HEAD UINT32_C(0x04)

/* Process-local admission result.  Slot references are never sent on wire. */
typedef struct PcmXMasterAdmission {
	PcmXTicketRef ref;
	PcmXPrehandleKey prehandle;
	PcmXSlotRef tag_slot;
	PcmXSlotRef ticket_slot;
	uint64 master_session_incarnation;
	uint64 admission_sequence;
	uint32 flags;
	uint32 reserved;
} PcmXMasterAdmission;

/*
 * Process-local two-phase WFG token.  The caller publishes blockers after the
 * snapshot returns, then presents this token to prove that the queue structure
 * used by that graph replace is still exact.  Slot indexes never cross wire.
 */
#define PCM_X_MASTER_WFG_MAX_BLOCKERS 2
typedef struct PcmXMasterWfgToken {
	PcmXTicketRef ticket;
	PcmXSlotRef tag_slot;
	PcmXSlotRef ticket_slot;
	PcmXSlotRef predecessor_slot;
	PcmXSlotRef active_slot;
	uint64 queue_state_sequence;
} PcmXMasterWfgToken;

typedef struct PcmXMasterWfgSnapshot {
	PcmXMasterWfgToken token;
	ClusterLmdVertex blockers[PCM_X_MASTER_WFG_MAX_BLOCKERS];
	uint32 blocker_count;
	uint32 reserved;
} PcmXMasterWfgSnapshot;

typedef enum PcmXLocalRole {
	PCM_X_LOCAL_ROLE_NONE = 0,
	PCM_X_LOCAL_ROLE_NODE_LEADER,
	PCM_X_LOCAL_ROLE_FOLLOWER
} PcmXLocalRole;

/* Fixed suballocator identity persisted in each local-membership slot. */
typedef enum PcmXLocalMembershipPartition {
	PCM_X_LOCAL_PARTITION_WAIT = 0,
	PCM_X_LOCAL_PARTITION_HOLDER
} PcmXLocalMembershipPartition;

/* Exact local queue locator plus the immutable waiter identity it names. */
typedef struct PcmXLocalHandle {
	PcmXWaitIdentity identity;
	PcmXSlotRef tag_slot;
	PcmXSlotRef membership_slot;
	uint64 local_sequence;
	uint32 local_round;
	uint16 role;
	uint16 flags;
} PcmXLocalHandle;

/* Caller-owned, process-local wake hints published only after an entire local
 * RETIRE watermark commits.  Items written while preflighting are invisible
 * until count is atomically published by the engine's return boundary. */
typedef struct PcmXLocalWakeBatch {
	PcmXWaitIdentity *items;
	Size capacity;
	Size count;
} PcmXLocalWakeBatch;

StaticAssertDecl(sizeof(PcmXLocalWakeBatch) == 24, "PCM-X local wake batch ABI");

/*
 * Process-local proof that one follower's WFG edge still names the exact
 * active local writer, or the node leader when no writer is active, sampled
 * before graph publication.  No slot address and no shared/wire ABI crosses
 * this boundary.
 */
typedef struct PcmXLocalFollowerWfgSnapshot {
	PcmXLocalHandle waiter;
	PcmXWaitIdentity blocker;
	PcmXSlotRef blocker_slot;
	uint64 waiter_graph_generation;
	Size leader_index;
	Size active_writer_index;
	uint64 leader_slot_generation;
	uint64 active_writer_slot_generation;
	uint64 holder_set_generation;
	uint32 local_round;
	uint32 reserved;
} PcmXLocalFollowerWfgSnapshot;

/* One-shot local active-writer claim.  Exact release records durable local
 * completion on the membership, so later writers can prove FIFO order and
 * the same request identity can never be re-armed. */
typedef struct PcmXLocalWriterClaim {
	PcmXLocalHandle writer;
	PcmXSlotRef active_slot;
	uint64 claim_generation;
	uint32 local_round;
	uint16 role;
	uint16 flags;
} PcmXLocalWriterClaim;

/*
 * Read-only, process-local view of one exact queue membership.  This is the
 * only supported polling surface for pcm_lock/bufmgr callers: shared-memory
 * slot addresses and allocator indexes never escape the queue engine.
 */
typedef struct PcmXLocalProgress {
	PcmXWaitIdentity identity;
	PcmXTicketRef ref;
	PcmXImageToken image;
	uint64 local_sequence;
	uint64 reliable_state_sequence;
	uint32 local_round;
	uint32 member_state;
	uint16 role;
	uint16 pending_opcode;
	uint16 last_response_opcode;
	uint16 phase;
	/* Process-local authority snapshot; never persisted or sent on wire. */
	uint64 master_session_incarnation;
	int32 master_node;
	uint32 reserved;
} PcmXLocalProgress;

StaticAssertDecl(sizeof(PcmXLocalProgress) == 240, "PCM-X local progress ABI");

/* Read-only process-local view of the independent holder transfer lane. */
typedef struct PcmXLocalHolderProgress {
	PcmXTicketRef ref;
	PcmXImageToken image;
	uint64 reliable_state_sequence;
	uint16 pending_opcode;
	uint16 last_response_opcode;
	uint16 phase;
	uint16 flags;
	/* Process-local authority snapshot; never persisted or sent on wire. */
	uint64 master_session_incarnation;
	int32 master_node;
	uint32 reserved;
} PcmXLocalHolderProgress;

StaticAssertDecl(sizeof(PcmXLocalHolderProgress) == 160, "PCM-X local holder progress ABI");

typedef struct PcmXLocalCutoff {
	PcmXSlotRef tag_slot;
	uint64 cutoff_sequence;
	uint64 master_session_incarnation;
	uint32 closed_round;
	uint32 next_round;
	int32 master_node;
	uint32 reserved;
} PcmXLocalCutoff;

/* Process-local exact holder locator; never stored in shmem or sent on wire. */
typedef struct PcmXLocalHolderHandle {
	PcmXLocalHolderKey key;
	PcmXSlotRef tag_slot;
	PcmXSlotRef holder_slot;
} PcmXLocalHolderHandle;

/* Exact holder-set token returned with one canonical bounded enumeration. */
typedef struct PcmXLocalHolderSnapshot {
	PcmXSlotRef tag_slot;
	uint64 holder_set_generation;
	Size holder_count;
} PcmXLocalHolderSnapshot;

/* Immutable local outbound blocker-set token used for whole-set replay. */
typedef struct PcmXLocalBlockerSnapshot {
	PcmXTicketRef ref;
	uint64 set_generation;
	uint64 reliable_state_sequence;
	uint32 blocker_count;
	uint32 set_crc32c;
} PcmXLocalBlockerSnapshot;

typedef enum PcmXLocalReliablePhase {
	PCM_X_LOCAL_RELIABLE_PHASE_NONE = 0,
	PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE,
	PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM,
	PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL,
	PCM_X_LOCAL_RELIABLE_PHASE_CANCEL
} PcmXLocalReliablePhase;

/* Exact process-local resend token; scheduling fields never identify a leg. */
typedef struct PcmXLocalReliableToken {
	PcmXWaitIdentity identity;
	PcmXPrehandleKey prehandle;
	PcmXTicketRef ref;
	uint64 state_sequence;
	uint64 retry_deadline_ms;
	uint64 expected_responder_session;
	uint32 retry_count;
	int32 expected_responder_node;
	uint16 pending_opcode;
	uint16 phase;
	uint16 flags;
	uint16 reserved;
} PcmXLocalReliableToken;

/* One exact blocker-slot observation returned in canonical chunk order. */
typedef struct PcmXMasterBlockerEntry {
	PcmXSlotRef slot_ref;
	ClusterLmdVertex blocker;
	uint32 chunk_no;
	uint32 reserved;
} PcmXMasterBlockerEntry;

/* Immutable token for bounded copy + exact generation revalidation. */
typedef struct PcmXMasterBlockerSnapshot {
	PcmXTicketRef ref;
	uint64 set_generation;
	uint64 graph_generation;
	uint32 blocker_count;
	uint32 set_crc32c;
} PcmXMasterBlockerSnapshot;

/* Exact current-X probe destination; never stored on wire. */
typedef struct PcmXMasterProbeToken {
	PcmXTicketRef ref;
	uint64 state_sequence;
	uint64 expected_responder_session;
	int32 expected_responder_node;
	uint32 reserved;
} PcmXMasterProbeToken;

/*
 * Immutable process-local view used by the tag-sharded master driver.  It
 * carries every mutable field that can change a holder-probe or invalidation
 * decision; bitmap replacement must present the exact snapshot back to the
 * engine.  No allocator address or slot index escapes this API.
 */
typedef struct PcmXMasterDriveSnapshot {
	PcmXTicketRef ref;
	PcmXImageToken image;
	uint64 master_session_incarnation;
	uint64 blocker_set_generation;
	uint64 blocker_stage_set_generation;
	uint64 graph_generation;
	uint64 state_sequence;
	uint64 retry_deadline_ms;
	uint64 response_tombstone_mask;
	uint64 expected_responder_session;
	uint32 pending_s_holders_bitmap;
	uint32 acked_s_holders_bitmap;
	uint32 involved_nodes_bitmap;
	uint32 blocker_count;
	uint32 retry_count;
	uint32 last_responder_node;
	int32 expected_responder_node;
	uint16 ticket_state;
	uint16 pending_opcode;
	uint16 last_response_opcode;
	uint16 phase;
	uint16 flags;
	uint16 reserved;
} PcmXMasterDriveSnapshot;

/* Immutable barrier token: FINAL_ACK validation is separated from the
 * engine mutation so the caller can commit external GRD authority first. */
typedef struct PcmXMasterFinalAckToken {
	PcmXFinalAckPayload final_ack;
	PcmXImageToken image;
	uint64 master_session_incarnation;
	uint64 state_sequence;
	uint64 authenticated_session;
	int32 authenticated_node;
	uint32 reserved;
} PcmXMasterFinalAckToken;

/* Process-local continuation for the master-lock -> GRD -> master-lock
 * cancellation choreography.  It never crosses wire or shmem boundaries. */
typedef struct PcmXMasterPendingXReleaseToken {
	PcmXTicketRef ref;
	uint64 master_session_incarnation;
	uint64 state_sequence;
} PcmXMasterPendingXReleaseToken;

StaticAssertDecl(sizeof(PcmXMasterAdmission) == 160, "PCM-X master admission ABI");
StaticAssertDecl(sizeof(PcmXMasterWfgToken) == 160, "PCM-X master WFG token ABI");
StaticAssertDecl(sizeof(PcmXMasterWfgSnapshot) == 264, "PCM-X master WFG snapshot ABI");
StaticAssertDecl(sizeof(PcmXLocalHandle) == 112, "PCM-X local handle ABI");
StaticAssertDecl(sizeof(PcmXLocalFollowerWfgSnapshot) == 248,
				 "PCM-X local follower WFG snapshot process-local ABI");
StaticAssertDecl(offsetof(PcmXLocalFollowerWfgSnapshot, waiter_graph_generation) == 192,
				 "PCM-X local follower sampled graph offset");
StaticAssertDecl(sizeof(PcmXLocalWriterClaim) == 144, "PCM-X local writer claim process-local ABI");
StaticAssertDecl(offsetof(PcmXLocalWriterClaim, claim_generation) == 128,
				 "PCM-X local writer claim generation offset");
StaticAssertDecl(sizeof(PcmXLocalCutoff) == 48, "PCM-X local cutoff process-local ABI");
StaticAssertDecl(offsetof(PcmXLocalCutoff, master_session_incarnation) == 24,
				 "PCM-X local cutoff session offset");
StaticAssertDecl(offsetof(PcmXLocalCutoff, master_node) == 40, "PCM-X local cutoff master offset");
StaticAssertDecl(sizeof(PcmXLocalHolderHandle) == 104, "PCM-X local holder handle ABI");
StaticAssertDecl(sizeof(PcmXLocalHolderSnapshot) == 32, "PCM-X local holder snapshot ABI");
StaticAssertDecl(sizeof(PcmXLocalBlockerSnapshot) == 112, "PCM-X local blocker snapshot ABI");
StaticAssertDecl(sizeof(PcmXLocalReliableToken) == 208, "PCM-X local reliable token ABI");
StaticAssertDecl(sizeof(PcmXMasterBlockerEntry) == 72, "PCM-X master blocker entry ABI");
StaticAssertDecl(sizeof(PcmXMasterBlockerSnapshot) == 112, "PCM-X master blocker snapshot ABI");
StaticAssertDecl(sizeof(PcmXMasterProbeToken) == 112, "PCM-X master probe token ABI");
StaticAssertDecl(sizeof(PcmXMasterDriveSnapshot) == 232,
				 "PCM-X master drive snapshot process-local ABI");
StaticAssertDecl(sizeof(PcmXMasterFinalAckToken) == 176,
				 "PCM-X master FINAL_ACK token process-local ABI");
StaticAssertDecl(sizeof(PcmXMasterPendingXReleaseToken) == 104,
				 "PCM-X pending-X release token process-local ABI");

#define PCM_X_G_EXACT UINT32_C(0x01)
#define PCM_X_G_WFG_READY UINT32_C(0x02)
#define PCM_X_G_HEAD UINT32_C(0x04)
#define PCM_X_G_REVERSIBLE UINT32_C(0x08)
#define PCM_X_G_DRAINED UINT32_C(0x10)
#define PCM_X_G_RETIRED UINT32_C(0x20)
#define PCM_X_G_CONFIRM_TOMBSTONE UINT32_C(0x40)
#define PCM_X_G_COMPLETE_TOMBSTONE UINT32_C(0x80)
#define PCM_X_G_CANCEL_TOMBSTONE UINT32_C(0x100)

/* Durable application outcomes carried by PcmXReliableLegState. */
#define PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM UINT64_C(0x01)
#define PCM_X_RESPONSE_TOMBSTONE_COMPLETE UINT64_C(0x02)
#define PCM_X_RESPONSE_TOMBSTONE_CANCEL UINT64_C(0x04)

/* Local-tag queue bits stored above the lifecycle value in state_flags. */
#define PCM_X_LOCAL_TAG_F_REVOKE_BARRIER UINT32_C(0x01)
#define PCM_X_LOCAL_TAG_F_ADMISSION_GATE UINT32_C(0x02)
#define PCM_X_LOCAL_TAG_F_TERMINAL_READY UINT32_C(0x04)
#define PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED UINT32_C(0x08)
#define PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_READY UINT32_C(0x10)
#define PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_DRAINED UINT32_C(0x20)
#define PCM_X_LOCAL_TAG_F_TERMINAL_MASK                                                            \
	(PCM_X_LOCAL_TAG_F_TERMINAL_READY | PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED)
#define PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK                                                     \
	(PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_READY | PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_DRAINED)

/* Local-membership bits use the same packed flag field in a disjoint slot
 * type.  COMPLETE is written only by exact active-claim release; it is the
 * durable one-shot/FIFO proof for later local writers in the same round. */
#define PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE UINT32_C(0x01)

/* Master-ticket bits use the same packed flag field in a disjoint slot type.
 * PENDING_X_CLAIMED is the durable ticket-exact proof that this ACTIVE_PROBE
 * owns the external GRD pending-X barrier.  It survives ACTIVE_TRANSFER and
 * is cleared only after exact FINAL confirmation or the two-phase cancel has
 * proved that the WFG edge and GRD cookie are no longer this ticket's.
 * CANCEL_REQUESTED persists a requester-death/cancel intent while an already
 * armed holder PROBE reliable leg finishes its application ACK; it forbids
 * ACTIVE_TRANSFER but does not discard the holder's FIFO evidence. */
#define PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED UINT32_C(0x01)
#define PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING UINT32_C(0x02)
#define PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED UINT32_C(0x04)
#define PCM_X_MASTER_TICKET_F_PENDING_X_KNOWN                                                      \
	(PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING     \
	 | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED)

/* Open-address directory entries refer to pool indexes, never pointers. */
#define PCM_X_DIRECTORY_EMPTY ((uint32)0)
#define PCM_X_DIRECTORY_OCCUPIED ((uint32)1)

typedef struct PcmXDirectoryEntry {
	uint64 key_hash;
	Size slot_index;
	uint64 slot_generation;
	uint32 state;
	uint32 reserved;
} PcmXDirectoryEntry;

StaticAssertDecl(sizeof(PcmXDirectoryEntry) == 32, "PCM-X directory entry ABI");
StaticAssertDecl(offsetof(PcmXDirectoryEntry, slot_index) == 8,
				 "PCM-X directory slot index offset");
StaticAssertDecl(offsetof(PcmXDirectoryEntry, slot_generation) == 16,
				 "PCM-X directory slot generation offset");
StaticAssertDecl(offsetof(PcmXDirectoryEntry, state) == 24, "PCM-X directory state offset");

/*
 * Persistent slot prefix.  Generation and state survive EXEC_BACKEND attach;
 * next_free is meaningful only in FREE state.  Every pool keeps this prefix at
 * offset zero so allocator code never needs a process-local pointer in shmem.
 *
 * generation_change_seq protects the two generation halves: a writer makes it
 * odd before replacing either half and even after both halves are stable.  A
 * reader accepts only equal even values bracketing its copy.  Slot writers are
 * serialized by the allocator lock.  state_flags atomically packs
 * the lifecycle value in its low bits and pool-specific flags in the remainder.
 */
typedef struct PcmXSlotHeader {
	Size next_free;
	pg_atomic_uint32 generation_change_seq;
	uint32 slot_generation_lo;
	uint32 slot_generation_hi;
	pg_atomic_uint32 state_flags;
} PcmXSlotHeader;

/* One application request/ACK leg.  retry_deadline_ms is scheduling only. */
typedef struct PcmXReliableLegState {
	uint64 state_sequence;
	uint64 retry_deadline_ms;
	uint64 response_tombstone_mask;
	uint64 expected_responder_session;
	uint32 retry_count;
	uint32 last_responder_node;
	int32 expected_responder_node;
	uint16 pending_opcode;
	uint16 last_response_opcode;
	uint16 phase;
	uint16 flags;
	uint32 reserved;
} PcmXReliableLegState;

typedef struct PcmXMasterTagSlot {
	PcmXSlotHeader slot;
	BufferTag tag;
	pg_atomic_uint32 queued_node_bitmap;
	uint64 cluster_epoch;
	uint64 next_admission_sequence;
	uint64 queue_state_sequence;
	uint64 active_slot_generation;
	Size head_index;
	Size tail_index;
	Size active_index;
	pg_atomic_uint32 admission_gate;
	uint32 reserved;
	Size outstanding_ticket_count;
} PcmXMasterTagSlot;

typedef struct PcmXMasterTicketSlot {
	PcmXSlotHeader slot;
	PcmXTicketRef ref;
	PcmXPrehandleKey prehandle;
	PcmXImageToken image;
	PcmXReliableLegState reliable;
	uint64 master_session_incarnation;
	uint64 admission_sequence;
	uint64 blocker_set_generation;
	uint64 graph_generation;
	uint64 drain_generation;
	uint64 tag_slot_generation;
	Size next_index;
	Size prev_index;
	Size tag_slot_index;
	Size blocker_head_index;
	Size blocker_stage_head_index;
	uint64 blocker_stage_set_generation;
	uint64 blocker_stage_source_session;
	uint64 blocker_set_source_session;
	uint32 blocker_count;
	uint32 blocker_set_crc32c;
	uint32 blocker_stage_count;
	uint32 blocker_stage_crc32c;
	uint32 blocker_stage_next_chunk;
	int32 blocker_stage_source_node;
	int32 blocker_set_source_node;
	uint32 pending_s_holders_bitmap;
	uint32 acked_s_holders_bitmap;
	uint32 involved_nodes_bitmap;
	uint32 drained_nodes_bitmap;
	uint32 retire_acked_nodes_bitmap;
} PcmXMasterTicketSlot;

typedef struct PcmXBlockerSlot {
	PcmXSlotHeader slot;
	PcmXTicketHandle handle;
	ClusterLmdVertex blocker;
	uint64 owner_slot_generation;
	uint64 set_generation;
	Size next_index;
	Size owner_slot_index;
	uint32 chunk_no;
	uint32 direction;
} PcmXBlockerSlot;

typedef struct PcmXLocalTagSlot {
	PcmXSlotHeader slot;
	BufferTag tag;
	int32 master_node;
	uint64 cluster_epoch;
	uint64 next_sequence;
	uint64 cutoff_sequence;
	uint64 blocker_set_generation;
	uint64 writer_claim_generation;
	uint64 master_session_incarnation;
	PcmXPrehandleKey prehandle;
	PcmXTicketRef ref;
	PcmXImageToken image;
	PcmXReliableLegState reliable;
	uint64 leader_slot_generation;
	uint64 active_writer_slot_generation;
	uint64 holder_set_generation;
	Size head_index;
	Size tail_index;
	Size leader_index;
	Size active_writer_index;
	Size active_holder_head_index;
	Size blocker_snapshot_head_index;
	uint32 local_round;
	uint32 blocker_snapshot_count;
	uint32 blocker_snapshot_crc32c;
	uint32 blocker_snapshot_next_chunk;
	Size membership_count;
	Size closed_round_member_count;
	uint64 terminal_drain_generation;
	uint64 committed_own_generation;
	PcmXTicketRef holder_ref;
	PcmXImageToken holder_image;
	PcmXReliableLegState holder_reliable;
	uint64 holder_terminal_drain_generation;
	PcmXTicketRef blocker_snapshot_ref;
	PcmXReliableLegState blocker_snapshot_reliable;
} PcmXLocalTagSlot;

typedef struct PcmXLocalMembershipSlot {
	PcmXSlotHeader slot;
	PcmXWaitIdentity identity;
	PcmXTicketHandle handle;
	uint64 local_sequence;
	uint64 tag_slot_generation;
	uint64 graph_generation;
	Size tag_slot_index;
	Size next_index;
	Size prev_index;
	int32 buffer_id;
	uint16 role;
	uint16 partition;
	uint32 admitted_round;
} PcmXLocalMembershipSlot;

StaticAssertDecl(sizeof(PcmXSlotHeader) == 24, "PCM-X slot header ABI");
StaticAssertDecl(offsetof(PcmXSlotHeader, next_free) == 0, "PCM-X free-link offset");
StaticAssertDecl(offsetof(PcmXSlotHeader, generation_change_seq) == 8,
				 "PCM-X generation seqlock offset");
StaticAssertDecl(offsetof(PcmXSlotHeader, slot_generation_lo) == 12,
				 "PCM-X generation low-half offset");
StaticAssertDecl(offsetof(PcmXSlotHeader, slot_generation_hi) == 16,
				 "PCM-X generation high-half offset");
StaticAssertDecl(offsetof(PcmXSlotHeader, state_flags) == 20, "PCM-X lifecycle state/flags offset");
StaticAssertDecl(sizeof(PcmXReliableLegState) == 56, "PCM-X reliable leg ABI");
StaticAssertDecl(sizeof(PcmXMasterTagSlot) == 120, "PCM-X master tag slot ABI");
StaticAssertDecl(offsetof(PcmXMasterTagSlot, outstanding_ticket_count) == 112,
				 "PCM-X master outstanding-ticket count offset");
StaticAssertDecl(sizeof(PcmXMasterTicketSlot) == 384, "PCM-X master ticket slot ABI");
StaticAssertDecl(sizeof(PcmXBlockerSlot) == 128, "PCM-X blocker slot ABI");
StaticAssertDecl(sizeof(PcmXLocalTagSlot) == 752, "PCM-X local tag slot ABI");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, membership_count) == 384,
				 "PCM-X local membership count offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, closed_round_member_count) == 392,
				 "PCM-X local closed-round count offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, terminal_drain_generation) == 400,
				 "PCM-X local terminal drain generation offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, committed_own_generation) == 408,
				 "PCM-X local committed ownership generation offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, holder_ref) == 416, "PCM-X local holder ticket offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, holder_image) == 504,
				 "PCM-X local holder image offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, holder_reliable) == 544,
				 "PCM-X local holder reliable-leg offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, holder_terminal_drain_generation) == 600,
				 "PCM-X local holder terminal drain generation offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, blocker_snapshot_ref) == 608,
				 "PCM-X local blocker snapshot ticket offset");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, blocker_snapshot_reliable) == 696,
				 "PCM-X local blocker snapshot reliable-leg offset");
StaticAssertDecl(sizeof(PcmXLocalMembershipSlot) == 168, "PCM-X local membership slot ABI");
StaticAssertDecl(offsetof(PcmXLocalMembershipSlot, admitted_round) == 160,
				 "PCM-X local admitted round offset");


typedef struct PcmXPoolLayout {
	Size slots_offset;
	Size slot_size;
	Size capacity;
	Size directory_offset;
	Size directory_capacity;
} PcmXPoolLayout;

/* Master tickets have three independent key spaces over the same slot pool. */
typedef struct PcmXMasterTicketDirectoryLayout {
	Size prehandle_offset;
	Size prehandle_capacity;
	Size handle_offset;
	Size handle_capacity;
	Size retire_offset;
	Size retire_capacity;
} PcmXMasterTicketDirectoryLayout;

typedef struct PcmXMembershipPartitionLayout {
	Size first_slot_index;
	Size slots_offset;
	Size capacity;
	Size directory_offset;
	Size directory_capacity;
} PcmXMembershipPartitionLayout;

typedef struct PcmXShmemLayout {
	uint32 magic;
	uint32 version;
	uint32 pool_count;
	uint32 allocator_count;
	uint32 lock_partition_count;
	uint32 lwlock_count;
	Size header_size;
	Size total_size;
	Size max_backends;
	Size auxiliary_procs;
	Size n_buffers;
	Size max_wait_edges;
	Size process_capacity;
	Size locks_per_process;
	Size active_holder_capacity;
	Size holder_tag_capacity;
	Size node_ticket_capacity;
	PcmXPoolLayout pools[PCM_X_POOL_COUNT];
	PcmXMasterTicketDirectoryLayout master_ticket_directories;
	PcmXMembershipPartitionLayout local_wait;
	PcmXMembershipPartitionLayout local_holder;
} PcmXShmemLayout;

typedef struct PcmXAllocatorState {
	Size free_head;
	Size used;
	Size high_water;
	Size generation_exhausted;
} PcmXAllocatorState;

/* Bounded steady-state replay/retirement authority for one requester node. */
typedef struct PcmXPeerFrontier {
	uint64 cluster_epoch;
	uint64 sender_session_incarnation;
	uint64 next_expected_prehandle_sequence;
	uint64 retired_prehandle_sequence;
	uint64 local_retired_ticket_id;
	uint64 local_retire_in_progress_ticket_id;
} PcmXPeerFrontier;

/* Lifetime counters and exact current queue depth in the existing PCM-X region. */
typedef struct PcmXStats {
	pg_atomic_uint64 enqueue_count;
	pg_atomic_uint64 admit_count;
	pg_atomic_uint64 confirm_count;
	pg_atomic_uint64 promotion_count;
	pg_atomic_uint64 transfer_count;
	pg_atomic_uint64 complete_count;
	pg_atomic_uint64 cancel_count;
	pg_atomic_uint64 revoke_count;
	pg_atomic_uint64 coalesced_count;
	pg_atomic_uint64 wait_count;
	pg_atomic_uint64 full_count;
	pg_atomic_uint64 stale_count;
	pg_atomic_uint64 miss_count;
	pg_atomic_uint64 recovery_blocked_count;
	pg_atomic_uint64 activating_reset_count;
	pg_atomic_uint64 depth;
	pg_atomic_uint64 depth_high_water;
	pg_atomic_uint64 own_begin_count;
	pg_atomic_uint64 own_commit_count;
	pg_atomic_uint64 own_abort_count;
	pg_atomic_uint64 own_busy_count;
	pg_atomic_uint64 own_corrupt_count;
} PcmXStats;

/* Process-local copy used by debug views and acceptance gates. */
typedef struct PcmXStatsSnapshot {
	uint64 enqueue_count;
	uint64 admit_count;
	uint64 confirm_count;
	uint64 promotion_count;
	uint64 transfer_count;
	uint64 complete_count;
	uint64 cancel_count;
	uint64 revoke_count;
	uint64 coalesced_count;
	uint64 wait_count;
	uint64 full_count;
	uint64 stale_count;
	uint64 miss_count;
	uint64 recovery_blocked_count;
	uint64 activating_reset_count;
	uint64 depth;
	uint64 depth_high_water;
	uint64 active_tags;
	uint64 live_tickets;
	uint64 live_slots;
	/* Cold-path RETIRE episode gauges.  local_retire_gate is the raw
	 * owner-plus-one value; marker_ticket_id is the first nonzero peer marker
	 * and marker_count exposes orphan/duplicate evidence. */
	uint64 local_retire_gate;
	uint64 local_retire_marker_count;
	uint64 local_retire_marker_ticket_id;
	uint64 own_begin_count;
	uint64 own_commit_count;
	uint64 own_abort_count;
	uint64 own_busy_count;
	uint64 own_corrupt_count;
} PcmXStatsSnapshot;

/* Optional peer authority installed while the runtime gate is ACTIVATING. */
typedef struct PcmXPeerBinding {
	uint64 cluster_epoch;
	uint64 peer_session_incarnation;
} PcmXPeerBinding;

/* Per-target outbound pre-handle authority; no process-local pointer lives here. */
#define PCM_X_OUTBOUND_TARGET_INITIALIZED UINT32_C(0x01)
#define PCM_X_OUTBOUND_TARGET_BOUND UINT32_C(0x02)
#define PCM_X_OUTBOUND_TARGET_KNOWN_FLAGS                                                          \
	(PCM_X_OUTBOUND_TARGET_INITIALIZED | PCM_X_OUTBOUND_TARGET_BOUND)

typedef struct PcmXOutboundTargetFrontier {
	pg_atomic_uint32 mint_gate;
	uint32 flags;
	uint64 cluster_epoch;
	uint64 target_session_incarnation;
	uint64 next_prehandle_sequence;
} PcmXOutboundTargetFrontier;

/* Bytes reserved for the "file:line" fail-closed provenance string. */
#define PCM_X_FAIL_CLOSED_SITE_LEN 80

typedef struct PcmXShmemHeader {
	PcmXShmemLayout layout;
	PcmXAllocatorState allocator[PCM_X_ALLOC_COUNT];
	volatile uint64 master_session_incarnation;
	uint64 next_ticket_id;
	uint64 next_image_id;
	uint64 fully_retired_ticket_id;
	pg_atomic_uint32 runtime_gate;
	/* Nonzero only after exact recovery of a stranded ACTIVATING publisher. */
	pg_atomic_uint32 activation_retry_generation;
	/* Serializes a multi-tag local RETIRE watermark against every ordinary
	 * local-domain mutator without adding a sixth pool or moving lock offsets. */
	pg_atomic_uint32 local_retire_gate;
	uint8 lock_padding[PCM_X_HEADER_LOCK_PADDING];
	LWLockPadded allocator_lock;
	LWLockPadded master_locks[PCM_X_LOCK_PARTITIONS];
	LWLockPadded local_locks[PCM_X_LOCK_PARTITIONS];
	PcmXPeerFrontier peer_frontiers[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXStats stats;
	PcmXOutboundTargetFrontier outbound_targets[PCM_X_PROTOCOL_NODE_LIMIT];
	/* "file:line" of the call site that most recently won the transition into
	 * PCM_X_RUNTIME_RECOVERY_BLOCKED.  Single-writer by construction: only the
	 * CAS winner inside pcm_x_runtime_fail_closed_at() writes it, and two
	 * successful transitions are always separated by a full reactivation.
	 * Readers are diagnostic-only (pg_cluster_state dump) and tolerate an
	 * in-progress overwrite after such a reactivation. */
	char fail_closed_site[PCM_X_FAIL_CLOSED_SITE_LEN];
} PcmXShmemHeader;

StaticAssertDecl(sizeof(PcmXShmemLayout) == 440, "PCM-X shmem layout ABI");
StaticAssertDecl(sizeof(PcmXAllocatorState) == 32, "PCM-X allocator state ABI");
StaticAssertDecl(sizeof(PcmXPeerFrontier) == 48, "PCM-X peer frontier ABI");
StaticAssertDecl(sizeof(PcmXStats) == 176, "PCM-X stats ABI");
StaticAssertDecl(sizeof(PcmXStatsSnapshot) == 224, "PCM-X stats snapshot ABI");
StaticAssertDecl(sizeof(PcmXPeerBinding) == 16, "PCM-X peer binding ABI");
StaticAssertDecl(sizeof(PcmXOutboundTargetFrontier) == 32, "PCM-X outbound target frontier ABI");
StaticAssertDecl(offsetof(PcmXOutboundTargetFrontier, mint_gate) == 0,
				 "PCM-X outbound target short gate offset");
StaticAssertDecl(offsetof(PcmXOutboundTargetFrontier, next_prehandle_sequence) == 24,
				 "PCM-X outbound pre-handle counter offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, activation_retry_generation) == 668,
				 "PCM-X activation retry marker offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, local_retire_gate) == 672,
				 "PCM-X local retire gate offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, allocator_lock) == 768,
				 "PCM-X allocator lock cache-line offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, master_locks) == 896, "PCM-X master lock array offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, local_locks) == 17280, "PCM-X local lock array offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, peer_frontiers) == 33664,
				 "PCM-X peer frontier array offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, stats) == 35200, "PCM-X stats offset");
StaticAssertDecl(offsetof(PcmXShmemHeader, outbound_targets) == 35376,
				 "PCM-X outbound target frontier array offset");
StaticAssertDecl(sizeof(PcmXShmemHeader) == 36480, "PCM-X shmem header ABI");

typedef enum PcmXAttachResult {
	PCM_X_ATTACH_OK = 0,
	PCM_X_ATTACH_NULL,
	PCM_X_ATTACH_MAGIC_MISMATCH,
	PCM_X_ATTACH_VERSION_MISMATCH,
	PCM_X_ATTACH_LAYOUT_MISMATCH
} PcmXAttachResult;


extern PcmXShmemHeader *ClusterPcmXConvertShmem;

extern void cluster_pcm_x_layout_compute(Size max_backends, Size auxiliary_procs, Size n_buffers,
										 Size max_wait_edges, PcmXShmemLayout *layout);
extern PcmXAttachResult cluster_pcm_x_validate_attach(const PcmXShmemHeader *header,
													  const PcmXShmemLayout *expected);
extern bool cluster_pcm_x_generation_next(uint64 current, uint64 *next);
extern PcmXAllocatorResult cluster_pcm_x_allocator_reserve(PcmXAllocatorKind kind,
														   PcmXSlotRef *ref_out,
														   PcmXSlotHeader **slot_out);
extern PcmXAllocatorResult cluster_pcm_x_allocator_release_exact(PcmXAllocatorKind kind,
																 PcmXSlotRef expected,
																 uint32 expected_state);
extern PcmXSlotHeader *cluster_pcm_x_slot_ref_revalidate(PcmXAllocatorKind kind,
														 PcmXSlotRef expected,
														 uint32 expected_state,
														 const BufferTag *expected_tag,
														 uint32 tag_hash);
extern bool cluster_pcm_x_directory_key_hash(PcmXDirectoryKind kind, const void *key,
											 uint64 *hash_out);
extern PcmXDirectoryResult cluster_pcm_x_directory_find(PcmXDirectoryKind kind, const void *key,
														PcmXSlotRef *found_out);
extern PcmXDirectoryResult cluster_pcm_x_directory_insert(PcmXDirectoryKind kind, const void *key,
														  PcmXSlotRef candidate,
														  PcmXSlotRef *existing_out);
extern PcmXDirectoryResult
cluster_pcm_x_directory_delete_exact(PcmXDirectoryKind kind, const void *key, PcmXSlotRef expected);
extern uint32 cluster_pcm_x_tag_hash(const BufferTag *tag);
extern uint32 cluster_pcm_x_lock_partition(uint32 tag_hash);
extern PcmXRuntimeSnapshot cluster_pcm_x_runtime_snapshot(void);
extern bool cluster_pcm_x_stats_snapshot(PcmXStatsSnapshot *snapshot_out);
extern void cluster_pcm_x_stats_note_enqueue(void);
extern void cluster_pcm_x_stats_note_wait(void);
extern void cluster_pcm_x_stats_note_queue_result(PcmXQueueResult result);
extern void cluster_pcm_x_stats_note_own_begin(void);
extern void cluster_pcm_x_stats_note_own_commit(void);
extern void cluster_pcm_x_stats_note_own_abort(void);
extern void cluster_pcm_x_stats_note_own_busy(void);
extern void cluster_pcm_x_stats_note_own_corrupt(void);
extern bool cluster_pcm_x_runtime_activate(uint64 master_session_incarnation);
extern bool
cluster_pcm_x_runtime_activate_bound(uint64 master_session_incarnation,
									 const PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT]);
extern PcmXQueueResult cluster_pcm_x_runtime_peer_binding_revalidate_exact(int32 peer_node,
																		   uint64 cluster_epoch,
																		   uint64 peer_session);
extern bool cluster_pcm_x_runtime_reset_activating(uint32 expected_gate_generation);
extern bool cluster_pcm_x_runtime_transition(PcmXRuntimeState expected, PcmXRuntimeState desired);
/* Cross-layer fail-closed seam for adapters that have already consumed an
 * irreversible external side effect and cannot safely retry it as ordinary
 * queue work.  The macro records the fusing arm (file:line) so a fused node
 * can be diagnosed post-mortem without per-site plumbing. */
extern void cluster_pcm_x_runtime_fail_closed_at(const char *site_file, int site_line);
#define cluster_pcm_x_runtime_fail_closed() \
	cluster_pcm_x_runtime_fail_closed_at(__FILE__, __LINE__)
/* Copies the recorded fail-closed site into buf; false when never fused. */
extern bool cluster_pcm_x_runtime_fail_closed_site(char *buf, Size buflen);
extern PcmXStepResult cluster_pcm_x_master_step(PcmXMasterTicketState current,
												PcmXMasterEvent event, uint32 guards,
												PcmXMasterTicketState *next);
extern bool cluster_pcm_x_ownership_reservation_allowed(PcmXMasterTicketState state);
extern bool cluster_pcm_x_ticket_drain_ready(const PcmXMasterTicketSlot *ticket);
extern bool cluster_pcm_x_ticket_retire_ready(const PcmXMasterTicketSlot *ticket);

/* Atomic composite queue operations; none may leak allocator/directory state. */
extern PcmXQueueResult cluster_pcm_x_peer_bind_ack_publish(int32 peer_node, uint64 cluster_epoch,
														   uint64 peer_session);
extern PcmXQueueResult cluster_pcm_x_master_admit_begin(const PcmXEnqueuePayload *request,
														PcmXMasterAdmission *admission_out);
extern PcmXQueueResult
cluster_pcm_x_master_admit_wfg_snapshot_exact(const PcmXTicketRef *ref,
											  PcmXMasterWfgSnapshot *snapshot_out);
extern PcmXQueueResult
cluster_pcm_x_master_admit_confirm_revalidate_exact(const PcmXMasterWfgToken *token,
													uint64 graph_generation);
extern PcmXQueueResult cluster_pcm_x_master_admit_confirm_exact(const PcmXTicketRef *ref,
																uint64 graph_generation);
extern PcmXQueueResult cluster_pcm_x_master_promote_head_exact(const BufferTag *tag,
															   uint64 cluster_epoch,
															   PcmXTicketRef *active_out);
extern PcmXQueueResult cluster_pcm_x_master_pending_x_claim_state_exact(const PcmXTicketRef *ref,
																		bool *claimed_out);
extern PcmXQueueResult cluster_pcm_x_master_pending_x_claim_exact(const PcmXTicketRef *ref);
extern PcmXQueueResult
cluster_pcm_x_master_pending_x_cancel_prepare_exact(const PcmXTicketRef *ref,
													PcmXMasterPendingXReleaseToken *token_out);
extern PcmXQueueResult
cluster_pcm_x_master_pending_x_cancel_finalize_exact(const PcmXMasterPendingXReleaseToken *token);
extern PcmXQueueResult cluster_pcm_x_master_cancel_ack_snapshot_exact(
	const PcmXTicketRef *ref, PcmXMasterAdmission *cancelled_out, bool *prehandle_out);
extern PcmXQueueResult cluster_pcm_x_master_drive_work_next(Size *cursor_io, Size scan_budget,
															BufferTag *tag_out,
															uint64 *cluster_epoch_out);
extern PcmXQueueResult
cluster_pcm_x_master_drive_snapshot_exact(const BufferTag *tag, uint64 cluster_epoch,
										  PcmXMasterDriveSnapshot *snapshot_out);
extern PcmXQueueResult cluster_pcm_x_master_drive_bitmap_replace_exact(
	const PcmXMasterDriveSnapshot *expected, uint32 pending_s_holders_bitmap,
	uint32 acked_s_holders_bitmap, PcmXMasterDriveSnapshot *snapshot_out);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_set_replace_exact(const PcmXTicketRef *ref, uint64 set_generation,
											   const ClusterLmdVertex *blockers, Size nblockers,
											   uint32 set_crc32c);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_probe_arm_exact(const PcmXTicketRef *ref, int32 responder_node,
											 uint64 responder_session, PcmXPhasePayload *probe_out,
											 PcmXMasterProbeToken *token_out);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_stage_begin_exact(const PcmXBlockerSetHeaderPayload *begin,
											   int32 authenticated_source_node,
											   uint64 authenticated_source_session);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_stage_edge_exact(const PcmXBlockerChunkPayload *edge,
											  int32 authenticated_source_node,
											  uint64 authenticated_source_session);
extern PcmXQueueResult cluster_pcm_x_master_blocker_stage_commit_exact(
	const PcmXBlockerSetHeaderPayload *commit, int32 authenticated_source_node,
	uint64 authenticated_source_session, PcmXMasterBlockerEntry *entries_out, Size entry_capacity,
	PcmXMasterBlockerSnapshot *snapshot_out);
extern PcmXQueueResult cluster_pcm_x_master_blocker_stage_abort_exact(
	const PcmXTicketRef *ref, uint64 set_generation, int32 authenticated_source_node,
	uint64 authenticated_source_session, uint64 expected_state_sequence);
extern uint32 cluster_pcm_x_blocker_set_crc32c(const ClusterLmdVertex *blockers, Size nblockers);
extern PcmXQueueResult cluster_pcm_x_master_blocker_snapshot_exact(
	const PcmXTicketRef *ref, PcmXMasterBlockerEntry *entries_out, Size entry_capacity,
	PcmXMasterBlockerSnapshot *snapshot_out);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_snapshot_revalidate_exact(const PcmXMasterBlockerSnapshot *snapshot,
													   const PcmXMasterBlockerEntry *entries,
													   Size entry_count);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_graph_commit_exact(const PcmXMasterBlockerSnapshot *snapshot,
												const PcmXMasterBlockerEntry *entries,
												Size entry_count, uint64 graph_generation);
extern PcmXQueueResult
cluster_pcm_x_master_blocker_probe_complete_exact(const PcmXTicketRef *ref, uint64 set_generation,
												  int32 authenticated_source_node,
												  uint64 authenticated_source_session);
extern PcmXQueueResult cluster_pcm_x_master_begin_transfer_exact(const PcmXTicketRef *ref,
																 uint64 graph_generation,
																 PcmXTicketRef *transfer_out);
extern PcmXQueueResult cluster_pcm_x_master_revoke_arm_exact(const PcmXTicketRef *ref,
															 int32 responder_node,
															 uint64 responder_session,
															 PcmXRevokePayload *revoke_out);
extern PcmXQueueResult cluster_pcm_x_master_image_ready_exact(const PcmXGrantPayload *image_ready,
															  int32 authenticated_node,
															  uint64 authenticated_session,
															  PcmXGrantPayload *prepare_out);
extern PcmXQueueResult
cluster_pcm_x_master_install_ready_exact(const PcmXInstallReadyPayload *install_ready,
										 int32 authenticated_node, uint64 authenticated_session,
										 PcmXPhasePayload *commit_out);
extern PcmXQueueResult cluster_pcm_x_master_final_ack_exact(const PcmXFinalAckPayload *final_ack,
															int32 authenticated_node,
															uint64 authenticated_session,
															PcmXPhasePayload *final_commit_out);
extern PcmXQueueResult
cluster_pcm_x_master_final_ack_prepare_exact(const PcmXFinalAckPayload *final_ack,
											 int32 authenticated_node, uint64 authenticated_session,
											 PcmXMasterFinalAckToken *token_out);
extern PcmXQueueResult
cluster_pcm_x_master_final_ack_finalize_exact(const PcmXMasterFinalAckToken *token,
											  PcmXPhasePayload *final_commit_out);
extern PcmXQueueResult
cluster_pcm_x_master_final_confirm_exact(const PcmXPhasePayload *final_confirm,
										 int32 authenticated_node, uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_master_complete_exact(const PcmXTicketRef *ref);
extern PcmXQueueResult cluster_pcm_x_master_cancel_exact(const PcmXTicketRef *ref);
extern PcmXQueueResult cluster_pcm_x_master_cancel_reversible_exact(const PcmXTicketRef *ref);
extern PcmXQueueResult
cluster_pcm_x_master_prehandle_cancel_exact(const PcmXPrehandleCancelPayload *request,
											PcmXMasterAdmission *cancelled_out);
extern PcmXQueueResult cluster_pcm_x_master_terminal_leg_arm_exact(const PcmXTicketRef *ref,
																   PcmXTerminalLegKind kind,
																   int32 responder_node,
																   uint64 responder_session,
																   PcmXTerminalLegToken *token_out);
/*
 * Wire ACKs do not echo send-side state_sequence or drain_generation.  The
 * DRAIN carries an immutable ref; this API proves its one pending leg against
 * authenticated peer context.  RETIRE uses the byte-exact payload API below.
 */
extern PcmXQueueResult cluster_pcm_x_master_terminal_leg_ack_exact(const PcmXTicketRef *ref,
																   PcmXTerminalLegKind actual_kind,
																   int32 authenticated_node,
																   uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_master_retire_ack_exact(const PcmXRetirePayload *ack,
															 int32 authenticated_node,
															 uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_master_retire_ack_resolve_exact(const PcmXRetirePayload *ack,
																	 int32 authenticated_node,
																	 uint64 authenticated_session,
																	 PcmXTicketRef *ref_out);
extern PcmXQueueResult cluster_pcm_x_master_terminal_work_next(PcmXTicketRef *ref_out);
extern PcmXQueueResult cluster_pcm_x_master_detach_terminal_exact(const PcmXTicketRef *ref);
extern PcmXQueueResult cluster_pcm_x_local_join_begin(const PcmXWaitIdentity *identity,
													  int32 master_node,
													  uint64 master_session_incarnation,
													  PcmXLocalHandle *handle_out);
extern PcmXQueueResult cluster_pcm_x_local_lookup_exact(const PcmXWaitIdentity *identity,
														PcmXLocalHandle *handle_out);
/* A follower promoted after a completed round keeps its FIFO position but
 * must bind the next ENQUEUE to the caller's freshly revalidated ownership
 * generation.  This moves only the exact local-wait directory identity; no
 * slot, sequence, round, or role is reallocated. */
extern PcmXQueueResult cluster_pcm_x_local_leader_rekey_generation_exact(
	const PcmXLocalHandle *leader, uint64 base_own_generation, PcmXLocalHandle *rekeyed_out);
extern PcmXQueueResult cluster_pcm_x_local_progress_exact(const PcmXLocalHandle *handle,
														  PcmXLocalProgress *progress_out);
extern PcmXQueueResult
cluster_pcm_x_local_follower_wfg_snapshot_exact(const PcmXLocalHandle *follower,
												PcmXLocalFollowerWfgSnapshot *snapshot_out);
extern PcmXQueueResult
cluster_pcm_x_local_follower_wfg_commit_exact(const PcmXLocalFollowerWfgSnapshot *snapshot,
											  uint64 graph_generation);
extern PcmXQueueResult cluster_pcm_x_local_follower_wfg_clear_exact(const PcmXLocalHandle *follower,
																	uint64 graph_generation);
extern PcmXQueueResult cluster_pcm_x_local_writer_claim_exact(const PcmXLocalHandle *writer,
															  PcmXLocalWriterClaim *claim_out);
extern PcmXQueueResult
cluster_pcm_x_local_writer_claim_release_collect_exact(const PcmXLocalWriterClaim *claim,
													   PcmXWaitIdentity *successor_out);
extern PcmXQueueResult
cluster_pcm_x_local_writer_claim_release_exact(const PcmXLocalWriterClaim *claim);
extern PcmXQueueResult
cluster_pcm_x_local_writer_claim_abort_exact(const PcmXLocalWriterClaim *claim);
extern PcmXQueueResult
cluster_pcm_x_local_holder_progress_exact(const BufferTag *tag,
										  PcmXLocalHolderProgress *progress_out);
/* Authorize a generation-less legacy INVALIDATE only when it names the exact
 * frozen non-source queue participant whose blocker set has been ACKed by the
 * currently bound master.  This is a read-only predicate for the passive-pin
 * S->N adapter; ordinary/late INVALIDATEs retain their pin-intolerant path. */
extern PcmXQueueResult cluster_pcm_x_local_queue_invalidate_authorize_exact(
	const BufferTag *tag, uint64 cluster_epoch, uint64 request_id, int32 requester_node,
	int32 authenticated_master_node, uint64 authenticated_master_session);
/* `holders` proves the complete exact pin/barrier registry.  `blockers`
 * contains only separately observed, stable live nested waits bound to those
 * holder processes; a torn outer PGPROC snapshot must be retried as BUSY and
 * must never be normalized to blocker_count=0. */
extern PcmXQueueResult cluster_pcm_x_local_blocker_snapshot_arm_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session,
	const PcmXLocalHolderSnapshot *holder_snapshot, const PcmXLocalHolderHandle *holders,
	Size holder_count, const ClusterLmdVertex *blockers, Size blocker_count,
	PcmXLocalBlockerSnapshot *snapshot_out);
extern PcmXQueueResult cluster_pcm_x_local_blocker_snapshot_lookup_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session,
	PcmXLocalBlockerSnapshot *snapshot_out);
extern PcmXQueueResult cluster_pcm_x_local_blocker_snapshot_copy_exact(
	const PcmXLocalBlockerSnapshot *snapshot, PcmXBlockerSetHeaderPayload *header_out,
	PcmXBlockerChunkPayload *edges_out, Size edge_capacity);
extern PcmXQueueResult cluster_pcm_x_local_blocker_ack_exact(const PcmXTicketRef *ref,
															 uint64 set_generation,
															 int32 authenticated_master_node,
															 uint64 authenticated_master_session);
extern PcmXQueueResult
cluster_pcm_x_local_holder_revoke_apply_exact(const PcmXRevokePayload *revoke,
											  int32 authenticated_master_node,
											  uint64 authenticated_master_session);
extern PcmXQueueResult cluster_pcm_x_local_holder_image_ready_arm_exact(
	const PcmXGrantPayload *image_ready, int32 authenticated_master_node,
	uint64 authenticated_master_session, PcmXGrantPayload *replay_out);
extern PcmXQueueResult cluster_pcm_x_local_enqueue_arm_exact(const PcmXLocalHandle *leader,
															 PcmXEnqueuePayload *payload_out,
															 PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_apply_admit_ack_exact(const PcmXLocalHandle *leader,
																 const PcmXAdmitAckPayload *ack,
																 int32 authenticated_node,
																 uint64 authenticated_session);
extern PcmXQueueResult
cluster_pcm_x_local_admit_confirm_arm_exact(const PcmXLocalHandle *leader,
											PcmXPhasePayload *payload_out,
											PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_admit_confirm_ack_exact(const PcmXLocalHandle *leader,
																   const PcmXPhasePayload *ack,
																   int32 authenticated_node,
																   uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_local_prepare_grant_exact(const PcmXLocalHandle *leader,
															   const PcmXGrantPayload *prepare,
															   int32 authenticated_node,
															   uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_local_install_ready_arm_exact(
	const PcmXLocalHandle *leader, const PcmXTicketRef *expected_ref,
	const PcmXImageToken *expected_image, PcmXInstallReadyPayload *install_ready_out,
	PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_commit_x_exact(const PcmXLocalHandle *leader,
														  const PcmXPhasePayload *commit,
														  int32 authenticated_node,
														  uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_local_final_ack_arm_exact(const PcmXLocalHandle *leader,
															   uint64 committed_own_generation,
															   PcmXFinalAckPayload *final_ack_out,
															   PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_final_commit_ack_exact(
	const PcmXLocalHandle *leader, const PcmXPhasePayload *final_commit, int32 authenticated_node,
	uint64 authenticated_session, PcmXPhasePayload *final_confirm_out,
	PcmXLocalReliableToken *token_out);
extern PcmXQueueResult
cluster_pcm_x_local_prehandle_cancel_arm_exact(const PcmXLocalHandle *leader,
											   PcmXPrehandleCancelPayload *payload_out,
											   PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_cancel_arm_exact(const PcmXLocalHandle *leader,
															PcmXPhasePayload *payload_out,
															PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_prehandle_cancel_ack_exact(
	const PcmXLocalHandle *leader, const PcmXAdmitAckPayload *ack, int32 authenticated_node,
	uint64 authenticated_session, PcmXLocalHandle *new_leader_out);
extern PcmXQueueResult cluster_pcm_x_local_cancel_ack_exact(const PcmXLocalHandle *leader,
															const PcmXPhasePayload *ack,
															int32 authenticated_node,
															uint64 authenticated_session,
															PcmXLocalHandle *new_leader_out);
/*
 * Cancellation may publish after its exact application ACK.  Successful
 * transfer must not publish merely because type 50 or 56 was sent: the 49-56
 * lane first consumes an exact DRAIN_POLL as positive proof that the master
 * applied that message, clears the corresponding holder/requester resend leg,
 * and unlinks its terminal participant.  DRAIN and RETIRE then retain the
 * evidence in the existing pools; no sixth pool is introduced.
 */
extern PcmXQueueResult cluster_pcm_x_local_terminal_publish_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session);
extern PcmXQueueResult cluster_pcm_x_local_drain_poll_exact(const PcmXDrainPollPayload *poll,
															int32 authenticated_master_node,
															uint64 authenticated_master_session);
extern PcmXQueueResult cluster_pcm_x_local_retire_up_to_exact(const PcmXRetirePayload *request,
															  int32 authenticated_master_node,
															  uint64 authenticated_master_session);
extern PcmXQueueResult cluster_pcm_x_local_retire_up_to_collect_exact(
	const PcmXRetirePayload *request, int32 authenticated_master_node,
	uint64 authenticated_master_session, PcmXLocalWakeBatch *wake_batch);
extern PcmXQueueResult
cluster_pcm_x_local_reliable_snapshot_exact(const PcmXLocalHandle *leader,
											PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_reliable_retry_exact(
	const PcmXLocalHandle *leader, const PcmXLocalReliableToken *expected, uint64 retry_deadline_ms,
	PcmXLocalReliableToken *token_out);
extern PcmXQueueResult cluster_pcm_x_local_reliable_ack_exact(
	const PcmXLocalHandle *leader, const PcmXLocalReliableToken *expected,
	const PcmXTicketRef *actual_ref, const PcmXPrehandleKey *actual_prehandle,
	uint16 response_opcode, int32 authenticated_node, uint64 authenticated_session);
extern PcmXQueueResult cluster_pcm_x_local_begin_revoke_cutoff_exact(const PcmXLocalHandle *leader,
																	 PcmXLocalCutoff *cutoff_out);
extern PcmXQueueResult cluster_pcm_x_local_current_cutoff_snapshot_exact(
	const BufferTag *tag, uint64 cluster_epoch, int32 master_node,
	uint64 master_session_incarnation, PcmXLocalCutoff *cutoff_out);
extern PcmXQueueResult cluster_pcm_x_local_retire_round_exact(const BufferTag *tag,
															  uint64 cluster_epoch,
															  const PcmXLocalCutoff *cutoff,
															  PcmXLocalHandle *next_leader_out);
extern PcmXQueueResult cluster_pcm_x_local_cancel_exact(const PcmXLocalHandle *handle,
														PcmXLocalHandle *new_leader_out);
extern PcmXQueueResult cluster_pcm_x_local_detach_terminal_exact(const PcmXLocalHandle *handle);
/* Holder caller order is strict: register/prepare before BufferDesc content
 * LWLock; activate while content is held; mark_releasing before unlock; exact
 * unregister only after unlock.  The middle two calls are lock-free W1+CAS. */
extern PcmXQueueResult cluster_pcm_x_local_holder_register(const PcmXLocalHolderKey *key,
														   PcmXLocalHolderHandle *handle_out);
/* A live FIFO writer claim may register its content holder inside the closed
 * revoke cohort.  The returned generation is sampled from the same exact
 * committed authority that admitted the holder. */
extern PcmXQueueResult cluster_pcm_x_local_writer_holder_register_exact(
	const PcmXLocalHolderKey *key, const PcmXLocalWriterClaim *claim,
	PcmXLocalHolderHandle *handle_out, uint64 *committed_own_generation_out);
/* Roll back a successfully published registration only while it is still in
 * ACQUIRING (for example, if the following content-LWLock acquire throws).
 * Both cleanup APIs below swallow an internal detach ERROR and return CORRUPT;
 * a caller already inside PG_CATCH must therefore copy and flush its original
 * ErrorData before entry, then explicitly ReThrowError that saved copy. */
extern PcmXQueueResult
cluster_pcm_x_local_holder_abort_acquiring_exact(const PcmXLocalHolderHandle *handle);
/* Error cleanup may detach any holder content state, but only after the
 * caller's content LWLock is demonstrably no longer held. */
extern PcmXQueueResult
cluster_pcm_x_local_holder_exceptional_detach_exact(const PcmXLocalHolderHandle *handle,
													LWLock *content_lock);
extern PcmXQueueResult
cluster_pcm_x_local_holder_activate_exact(const PcmXLocalHolderHandle *handle);
extern PcmXQueueResult
cluster_pcm_x_local_holder_mark_releasing_exact(const PcmXLocalHolderHandle *handle);
extern PcmXQueueResult
cluster_pcm_x_local_holder_unregister_exact(const PcmXLocalHolderHandle *handle);
extern PcmXQueueResult cluster_pcm_x_local_holder_snapshot(const BufferTag *tag,
														   PcmXLocalHolderHandle *holders_out,
														   Size holder_capacity,
														   PcmXLocalHolderSnapshot *snapshot_out);
/*
 * Freeze one master-authenticated type-48 PROBE before sampling holder
 * wait-state.  The existing local round barrier and blocker_snapshot_ref are
 * the only durable reservation; no parallel pin/barrier lifecycle is created.
 * A count-only call may return NO_CAPACITY after persisting the freeze, and an
 * exact same-ref replay may then copy the canonical holder set.
 */
extern PcmXQueueResult cluster_pcm_x_local_probe_freeze_snapshot_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session,
	PcmXLocalHolderHandle *holders_out, Size holder_capacity,
	PcmXLocalHolderSnapshot *snapshot_out);
/* Called after publishing MyProc's wait-state and before the first sleep.
 * It snapshots held shared-buffer content locks without taking a lock in the
 * ForEachLWLockHeldByMe callback, then rejects any tag whose local PROBE
 * barrier is closed.  BUSY/CORRUPT are closed decisions, never "inactive". */
extern PcmXQueueResult cluster_pcm_x_nested_wait_guard_before_block(void);
extern PcmXQueueResult
cluster_pcm_x_local_holder_snapshot_revalidate(const BufferTag *tag,
											   const PcmXLocalHolderSnapshot *snapshot);

extern Size cluster_pcm_x_convert_shmem_size(void);
extern void cluster_pcm_x_convert_shmem_init(void);
extern void cluster_pcm_x_convert_shmem_register(void);


#endif /* CLUSTER_PCM_X_CONVERT_H */
