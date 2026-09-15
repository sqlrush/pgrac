/*-------------------------------------------------------------------------
 *
 * cluster_terminal_ref_census.h
 *	  Canonical terminal-reference census and release protocol.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_terminal_ref_census.h
 *
 * NOTES
 *	  This is a pgrac-original file.  CTRC records reference-release
 *	  evidence only; canonical transaction status remains in the physical
 *	  transaction-table slot.  All exported symbols use the cluster_ctrc_
 *	  prefix.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TERMINAL_REF_CENSUS_H
#define CLUSTER_TERMINAL_REF_CENSUS_H

#include "c.h"

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_format.h"
#include "storage/block.h"

#define CLUSTER_CTRC_FORMAT_VERSION UINT8_C(1)
#define CLUSTER_CTRC_TXN_KEY_BYTES 96
#define CLUSTER_CTRC_SEAL_REQUEST_BYTES 136
#define CLUSTER_CTRC_REPLY_HEADER_BYTES 64
#define CLUSTER_CTRC_LOCAL_ACK_BYTES 416
#define CLUSTER_CTRC_REPLY_ACK_OFFSET 64
#define CLUSTER_CTRC_REPLY_TAIL_OFFSET 480
#define CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES 143
#define CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES 239
#define CLUSTER_CTRC_ROW_ENCODING_MAX_BYTES 623
#define CLUSTER_CTRC_WIRE_MAGIC UINT32_C(0x50474354)
#define CLUSTER_CTRC_WIRE_VERSION UINT16_C(2)
#define CLUSTER_CTRC_SELECTOR_VERSION UINT8_C(1)
#define CLUSTER_CTRC_FORWARD_KIND UINT8_C(11)
#define CLUSTER_CTRC_INTERNAL_ENDPOINT ((int32)-2)
#define CLUSTER_CTRC_MAX_PARTICIPANTS CLUSTER_SF_DEP_MAX_ORIGINS
#define CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID UINT16_MAX

#define CTRC_ACK_FLAG_ZERO_RANGE UINT16_C(0x0001)
#define CTRC_ACK_FLAG_ALL_DURABLE UINT16_C(0x0002)

typedef enum ClusterCtrcSealSuboperation
{
	CTRC_SEAL_CLOSE_AND_CLEAN = 1,
	CTRC_SEAL_CERTIFICATE_COMMITTED = 2
} ClusterCtrcSealSuboperation;

typedef enum ClusterCtrcSealReplyResult
{
	CTRC_SEAL_REPLY_DENIED = 0,
	CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK = 1,
	CTRC_SEAL_REPLY_PENDING_DRAIN = 2,
	CTRC_SEAL_REPLY_BLOCKED_RETAIN = 3,
	CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED = 4
} ClusterCtrcSealReplyResult;

typedef enum ClusterCtrcSealReason
{
	CTRC_SEAL_REASON_MALFORMED = 1,
	CTRC_SEAL_REASON_IDENTITY = 2,
	CTRC_SEAL_REASON_PREPARED = 3,
	CTRC_SEAL_REASON_CLEANOUT = 4,
	CTRC_SEAL_REASON_ACK_UNAVAILABLE = 5
} ClusterCtrcSealReason;

/* One counter per frozen §14.1 semantic.  These are observability only and
 * never participate in authority or release decisions. */
typedef enum ClusterCtrcStatId {
	CTRC_STAT_GRANT_ISSUED = 0,
	CTRC_STAT_GRANT_REFUSED,
	CTRC_STAT_RECEIPT_PREPARED,
	CTRC_STAT_RECEIPT_APPLIED,
	CTRC_STAT_RECEIPT_CANCELLED,
	CTRC_STAT_RECEIPT_CAPACITY_REFUSED,
	CTRC_STAT_SEAL_STARTED,
	CTRC_STAT_SEAL_BLOCKED,
	CTRC_STAT_TARGET_ABSENT,
	CTRC_STAT_TARGET_REWRITTEN,
	CTRC_STAT_TARGET_RETAINED,
	CTRC_STAT_ACK_FROZEN,
	CTRC_STAT_ACK_RESENT,
	CTRC_STAT_CERTIFICATE_APPLIED,
	CTRC_STAT_CERTIFICATE_REPLAYED,
	CTRC_STAT_L11_RELEASE_SAMPLE,
	CTRC_STAT_L12_RECYCLE,
	CTRC_STAT_ORDINARY_PUBLICATION_AFTER_APPLY,
	CTRC_STAT_CURRENT_MX_PUBLICATION_AFTER_APPLY,
	CTRC_STAT_PUBLICATION_ORDER_VIOLATION,
	CTRC_STAT_CLEANER_PASS,
	CTRC_STAT_CLEANER_ATTEMPT,
	CTRC_STAT_SEMANTIC_PROGRESS,
	CTRC_STAT_PENDING_DUPLICATE,
	CTRC_STAT_DISPATCH_BACKLOG,
	CTRC_STAT_CERTIFICATE_BACKLOG,
	CTRC_STAT_PENDING_OBSERVED_AGE_MS,
	CTRC_STAT_OBSERVED_AT_US,
	CTRC_STAT_OBSERVATION_AGE_MS,
	CTRC_STAT_COUNT
} ClusterCtrcStatId;

typedef enum ClusterCtrcCleanerReason
{
	CTRC_CLEANER_REASON_NONE = 0,
	CTRC_CLEANER_REASON_PREPARED_DRAIN,
	CTRC_CLEANER_REASON_RESOURCE_X,
	CTRC_CLEANER_REASON_PAGE_REVALIDATE,
	CTRC_CLEANER_REASON_WAL_DURABILITY,
	CTRC_CLEANER_REASON_PARTICIPANT_ACK,
	CTRC_CLEANER_REASON_BLOCK0_CERTIFICATE,
	CTRC_CLEANER_REASON_BLOCKED,
	CTRC_CLEANER_REASON_COUNT
} ClusterCtrcCleanerReason;

#define CLUSTER_CTRC_CLEANER_WORKERS 8

/* Scheduling observations only; neither a coherent authority snapshot nor
 * permission to release a reference. Each row has one cleaner writer. */
typedef struct ClusterCtrcCleanerWorkerObservation {
	uint64 dispatch_backlog;
	uint64 certificate_backlog;
	uint64 pending_observed_age_ms;
	uint64 observed_at_us;
	uint32 reason;
	uint32 reserved;
} ClusterCtrcCleanerWorkerObservation;

/* A single shared scheduling seam.  The phase value selects a safe point;
 * the seam never changes proof, receipt, ACK, certificate or verdict bytes. */
typedef enum ClusterCtrcTestBarrierPhase
{
	CTRC_TEST_BARRIER_NONE = 0,
	CTRC_TEST_BARRIER_ACTIVE_PROOF_READY = 1,
	CTRC_TEST_BARRIER_ACK_DURABLE = 2,
	CTRC_TEST_BARRIER_CERTIFICATE_READY = 3,
	CTRC_TEST_BARRIER_REQUESTER_REQUALIFY = 4,
	CTRC_TEST_BARRIER_COUNT
} ClusterCtrcTestBarrierPhase;

typedef enum ClusterCtrcReferenceKind
{
	CTRC_REF_HEAP_ITL_UBA = 1,
	CTRC_REF_CURRENT_MX_LOCKER = 2,
	CTRC_REF_CURRENT_MX_UPDATER = 3,
	CTRC_REF_RECOMPOSED_SURVIVOR = 4,
	CTRC_REF_HOT_FOLLOW_EDGE = 5
} ClusterCtrcReferenceKind;

typedef enum ClusterCtrcTargetKind
{
	CTRC_TARGET_EXACT_ITL_SLOT = 1,
	CTRC_TARGET_EXACT_TID = 2,
	CTRC_TARGET_PAGE_PENDING_ITL_SLOT = 3,
	CTRC_TARGET_PAGE_PENDING_OFFNUM = 4
} ClusterCtrcTargetKind;

typedef enum ClusterCtrcProofClass
{
	CTRC_PROOF_UNKNOWN = 0,
	CTRC_PROOF_SELF = 1,
	CTRC_PROOF_ACTIVE = 2,
	CTRC_PROOF_COMMITTED = 3,
	CTRC_PROOF_ABORTED = 4
} ClusterCtrcProofClass;

typedef enum ClusterCtrcReceiptState
{
	CTRC_RECEIPT_FREE = 0,
	CTRC_RECEIPT_PREPARED,
	CTRC_RECEIPT_APPLIED,
	CTRC_RECEIPT_CLEANED,
	CTRC_RECEIPT_CANCELLED,
	CTRC_RECEIPT_ACK_FROZEN,
	CTRC_RECEIPT_BLOCKED,
	/* Transient, shared-memory-only ownership of an APPLIED ordinary ITL
	 * receipt while its exact target is replaced.  It is never a positive
	 * release state and keeps the existing applied_count outstanding. */
	CTRC_RECEIPT_RETARGETING
} ClusterCtrcReceiptState;

/* Parallel open-addressing metadata for the bounded receipt array.  This is
 * volatile shared-memory state, not wire, WAL, or persistent ABI. */
typedef enum ClusterCtrcReceiptProbeState
{
	CTRC_RECEIPT_PROBE_EMPTY = 0,
	CTRC_RECEIPT_PROBE_OCCUPIED,
	CTRC_RECEIPT_PROBE_TOMBSTONE
} ClusterCtrcReceiptProbeState;

typedef enum ClusterCtrcReleaseDisposition
{
	CTRC_RELEASE_NONE = 0,
	CTRC_RELEASE_CANCELLED_PREMUTATION = 1,
	CTRC_RELEASE_CLEANED_ABSENT = 2,
	CTRC_RELEASE_CLEANED_TERMINAL_REWRITE = 3,
	CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED = 4
} ClusterCtrcReleaseDisposition;

typedef enum ClusterCtrcParticipantState
{
	CTRC_PARTICIPANT_EMPTY = 0,
	CTRC_PARTICIPANT_OPEN,
	CTRC_PARTICIPANT_CLOSED_DRAINING,
	CTRC_PARTICIPANT_ACK_READY,
	CTRC_PARTICIPANT_ACK_FROZEN,
	CTRC_PARTICIPANT_BLOCKED
} ClusterCtrcParticipantState;

typedef enum ClusterCtrcOriginState
{
	CTRC_ORIGIN_EMPTY = 0,
	CTRC_ORIGIN_OPEN,
	CTRC_ORIGIN_SEALING,
	CTRC_ORIGIN_SEALED,
	CTRC_ORIGIN_CLEANING,
	CTRC_ORIGIN_CERTIFYING,
	CTRC_ORIGIN_RELEASE_PROVEN,
	CTRC_ORIGIN_BLOCKED
} ClusterCtrcOriginState;

typedef struct ClusterCtrcTxnKeyV1
{
	uint8 format_version;
	uint8 owner_instance;
	uint16 origin_node_id;
	uint32 segment_id;
	uint32 segment_generation;
	uint16 slot_offset;
	uint16 slot_wrap;
	TransactionId xid;
	uint32 cluster_epoch;
	uint64 system_identifier;
	uint64 origin_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 root_descriptor_incarnation;
	uint64 root_id;
	uint64 root_generation;
	uint8 reserved[16];
} ClusterCtrcTxnKeyV1;

typedef struct ClusterCtrcPublicationIdV1
{
	uint16 requester_node_id;
	uint64 requester_boot_incarnation;
	uint32 capability_record_generation;
	int32 requester_backend_id;
	uint64 wire_request_id;
	uint64 operation_id;
	uint32 attempt_generation;
	uint64 descriptor_hash;
	uint16 member_ordinal;
	uint8 member_role;
	uint8 reference_kind;
	uint8 target_kind;
	uint8 reserved8;
	uint64 journal_sequence;
	uint64 key_sequence;
	uint64 journal_slot_generation;
	uint32 grant_generation;
} ClusterCtrcPublicationIdV1;

typedef struct ClusterCtrcTargetV1
{
	uint8 kind;
	uint8 relation_persistence;
	uint8 needs_wal;
	uint8 page_operation_kind;
	uint32 spc_oid;
	uint32 db_oid;
	uint32 rel_number;
	int32 fork_number;
	BlockNumber block_number;
	uint16 predecessor_page_lsn_origin_node_id;
	uint16 predecessor_page_lsn_reserved16;
	uint64 predecessor_page_lsn;
	uint64 predecessor_page_scn;
	uint64 publication_own_generation;
	uint64 publication_acquisition_epoch;
	uint16 itl_slot_index;
	uint16 itl_slot_wrap;
	TransactionId itl_xid;
	uint8 itl_class;
	uint8 reserved8[3];
	uint8 uba[16];
	uint8 planned_predecessor_sha256[32];
	uint8 planned_successor_sha256[32];
	uint16 offset_number;
	uint16 itemid_flags;
	uint16 itemid_offset;
	uint16 itemid_length;
	uint8 tuple_header_sha256[32];
	uint16 mx_origin_node_id;
	uint16 reserved16;
	uint32 multixact_id;
	uint32 mx_cluster_epoch;
	uint64 descriptor_hash;
	uint8 successor_topology_sha256[32];
	uint64 intended_descriptor_hash;
} ClusterCtrcTargetV1;

typedef enum ClusterCtrcPageVersionOrder
{
	CTRC_PAGE_VERSION_CURRENT = 0,
	CTRC_PAGE_VERSION_REGRESSED,
	CTRC_PAGE_VERSION_UNKNOWN
} ClusterCtrcPageVersionOrder;

/* Immutable unpublished ITL intent: identity is exact, its page version is
 * an observation floor. This predicate supplies no page authority. */
extern bool cluster_ctrc_pending_itl_target_recheck(const ClusterCtrcTargetV1 *stored,
													const ClusterCtrcTargetV1 *observed);

extern ClusterCtrcPageVersionOrder cluster_ctrc_page_version_order(
	uint16 predecessor_origin, XLogRecPtr predecessor_lsn,
	SCN predecessor_scn, uint16 current_origin, XLogRecPtr current_lsn,
	SCN current_scn);

typedef struct ClusterCtrcParticipantIdentity
{
	uint16 node_id;
	uint16 reserved16;
	uint32 capability_record_generation;
	uint64 boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
} ClusterCtrcParticipantIdentity;

/* Stack-only close work item owned by the existing undo cleaner.  It is not
 * shared-memory or wire ABI; the 136-byte encoder remains the sole wire
 * representation. */
typedef struct ClusterCtrcCloseDispatch
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity participant;
	uint64 request_id;
	uint64 seal_generation;
	uint32 grant_generation;
	uint8 suboperation;
	uint8 reserved8[3];
	uint32 reserved32;
} ClusterCtrcCloseDispatch;

typedef struct ClusterCtrcOriginEntry
{
	ClusterCtrcTxnKeyV1 key;
	uint64 reservation_generation;
	uint32 grant_generation;
	uint32 touched_bitmap;
	uint64 seal_generation;
	uint32 close_dispatched_bitmap;
	uint32 close_confirmed_bitmap;
	uint32 ack_bitmap;
	uint8 state;
	uint8 touched_count;
	uint8 reserved8[6];
	ClusterCtrcParticipantIdentity touched[CLUSTER_CTRC_MAX_PARTICIPANTS];
	uint64 close_request_id[CLUSTER_CTRC_MAX_PARTICIPANTS];
} ClusterCtrcOriginEntry;

typedef enum ClusterCtrcOriginReservationKind
{
	CTRC_ORIGIN_RESERVATION_INVALID = 0,
	CTRC_ORIGIN_RESERVATION_PENDING_OWNED = 1,
	CTRC_ORIGIN_RESERVATION_EXISTING_OPEN = 2
} ClusterCtrcOriginReservationKind;

/* Stack-only handle for the EMPTY-reserved -> OPEN origin transition. */
typedef struct ClusterCtrcOriginReservation
{
	ClusterCtrcTxnKeyV1 key;
	uint64 origin_index;
	uint64 reservation_generation;
	uint8 kind;
	bool valid;
	uint8 reserved8[6];
} ClusterCtrcOriginReservation;

typedef struct ClusterCtrcParticipantEntry
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity identity;
	uint32 grant_generation;
	uint8 state;
	uint8 reserved8[3];
	uint64 seal_generation;
	uint64 next_key_sequence;
	uint64 last_key_sequence;
	uint64 receipt_count;
	uint64 prepared_count;
	uint64 applied_count;
	uint64 cancelled_count;
	uint64 cleaned_count;
	uint64 ack_frozen_count;
} ClusterCtrcParticipantEntry;

typedef struct ClusterCtrcReceipt
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	uint32 state;
	uint8 disposition;
	uint8 reserved8[3];
	XLogRecPtr highest_local_wal_lsn;
	XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
} ClusterCtrcReceipt;

typedef struct ClusterCtrcApplyToken
{
	bool valid;
	uint8 reserved8[7];
	uint64 journal_sequence;
	uint64 key_sequence;
	uint64 journal_slot_generation;
} ClusterCtrcApplyToken;

/* Backend-local ownership handle for one bounded shared receipt slot.  This
 * is not a second receipt, wire object, shared ABI or transaction authority. */
typedef struct ClusterCtrcReceiptHandle
{
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcReceipt *receipt;
	ClusterCtrcTxnKeyV1 key;
	uint64 participant_index;
	uint64 receipt_index;
	uint64 journal_slot_generation;
	bool valid;
	uint8 reserved8[7];
} ClusterCtrcReceiptHandle;

typedef struct ClusterCtrcDurability
{
	XLogRecPtr highest_local_lsn;
	XLogRecPtr local_flush_lsn;
	XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
	XLogRecPtr durable_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
} ClusterCtrcDurability;

/* Backend-local expected identity supplied by an exact page owner when an
 * ITL terminal rewrite attempts to discharge its receipt.  This is neither a
 * wire nor shared-memory ABI: the shared wrapper compares every field while
 * holding the receipt-table locks, so a stale/recycled handle cannot clean a
 * different ITL reference. */
typedef struct ClusterCtrcItlTargetIdentity
{
	uint32 spc_oid;
	uint32 db_oid;
	uint32 rel_number;
	int32 fork_number;
	BlockNumber block_number;
	TransactionId itl_xid;
	uint16 itl_slot_index;
	uint16 itl_slot_wrap;
	uint8 itl_class;
	bool needs_wal;
	uint8 reserved8[2];
	uint8 uba[16];
} ClusterCtrcItlTargetIdentity;

typedef struct ClusterCtrcLocalReleaseAckV1
{
	ClusterCtrcTxnKeyV1 transaction_key;
	uint32 grant_generation;
	uint8 result;
	uint8 first_failed_predicate;
	uint16 flags;
	uint64 seal_generation;
	uint16 participant_node_id;
	uint16 dependency_entry_count;
	uint32 capability_record_generation;
	uint64 participant_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 first_key_sequence;
	uint64 last_key_sequence;
	uint64 minimum_journal_sequence;
	uint64 maximum_journal_sequence;
	uint64 total_receipt_count;
	uint64 prepared_count;
	uint64 applied_count;
	uint64 cancelled_count;
	uint64 cleaned_count;
	uint64 ack_frozen_count;
	uint8 row_digest_sha256[32];
	XLogRecPtr highest_local_cleanout_lsn;
	XLogRecPtr required_lsn_vector[CLUSTER_SF_DEP_MAX_ORIGINS];
	uint8 reserved[20];
	uint32 crc32c;
} ClusterCtrcLocalReleaseAckV1;

typedef struct ClusterCtrcSealRequestV1
{
	uint64 request_id;
	uint64 cluster_epoch;
	uint8 tt_status_key_0_19[20];
	int32 original_requester_node;
	int32 requester_backend_id;
	uint8 tt_status_key_20_23[4];
	uint32 grant_generation;
	uint16 slot_wrap;
	uint8 owner_instance;
	uint8 suboperation;
	uint32 segment_generation;
	uint16 request_flags;
	uint8 selector_version;
	uint8 forward_kind;
	uint32 magic;
	uint16 wire_version;
	uint16 wire_length;
	uint64 origin_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 root_descriptor_incarnation;
	uint64 root_id;
	uint64 root_generation;
	uint64 seal_generation;
	uint32 participant_capability_record_generation;
	uint32 reserved_tail;
} ClusterCtrcSealRequestV1;

typedef struct ClusterCtrcSealReplyHeaderV1
{
	uint32 magic;
	uint16 wire_version;
	uint16 header_length;
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 seal_generation;
	int32 source_node;
	int32 destination_node;
	uint8 result;
	uint8 suboperation;
	uint16 first_reason;
	uint16 reply_flags;
	uint16 ack_length;
	uint8 request_sha256_prefix[8];
	uint32 body_length;
	uint32 header_crc32c;
} ClusterCtrcSealReplyHeaderV1;

typedef enum ClusterCtrcOriginOpenResult
{
	CLUSTER_CTRC_ORIGIN_REFUSED = 0,
	CLUSTER_CTRC_ORIGIN_OPENED = 1,
	CLUSTER_CTRC_ORIGIN_DUPLICATE = 2
} ClusterCtrcOriginOpenResult;

typedef enum ClusterCtrcOriginReserveResult
{
	CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED = 0,
	CLUSTER_CTRC_ORIGIN_RESERVED_PENDING = 1,
	CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN = 2,
	CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED = 3
} ClusterCtrcOriginReserveResult;

typedef enum ClusterCtrcTouchResult
{
	CLUSTER_CTRC_TOUCH_REFUSED = 0,
	CLUSTER_CTRC_TOUCH_RECORDED = 1,
	CLUSTER_CTRC_TOUCH_DUPLICATE = 2,
	CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT = 3
} ClusterCtrcTouchResult;

typedef enum ClusterCtrcParticipantOpenResult
{
	CLUSTER_CTRC_PARTICIPANT_REFUSED = 0,
	CLUSTER_CTRC_PARTICIPANT_OPENED = 1,
	CLUSTER_CTRC_PARTICIPANT_DUPLICATE = 2
} ClusterCtrcParticipantOpenResult;

typedef enum ClusterCtrcPrepareResult
{
	CLUSTER_CTRC_PREPARE_REFUSED = 0,
	CLUSTER_CTRC_PREPARE_READY = 1,
	CLUSTER_CTRC_PREPARE_DUPLICATE = 2,
	CLUSTER_CTRC_PREPARE_CAPACITY = 3
} ClusterCtrcPrepareResult;

typedef enum ClusterCtrcApplyResult
{
	CLUSTER_CTRC_APPLY_FAIL_CLOSED = 0,
	CLUSTER_CTRC_APPLY_APPLIED = 1,
	CLUSTER_CTRC_APPLY_RETRY_REQUIRED = 2
} ClusterCtrcApplyResult;

typedef enum ClusterCtrcItlProjection
{
	CTRC_ITL_NEEDS_CLEANOUT = 0,
	CTRC_ITL_HINT_SKIPPED,
	CTRC_ITL_TERMINAL_INDEPENDENT,
	CTRC_ITL_TARGET_ABSENT
} ClusterCtrcItlProjection;

typedef enum ClusterCtrcDischargeResult
{
	CLUSTER_CTRC_DISCHARGE_RETAIN = 0,
	CLUSTER_CTRC_DISCHARGE_CLEANED = 1
} ClusterCtrcDischargeResult;

typedef enum ClusterCtrcItlCleanoutApplyResult
{
	CLUSTER_CTRC_ITL_CLEANOUT_RETAIN = 0,
	CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL = 1,
	CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN = 2
} ClusterCtrcItlCleanoutApplyResult;

typedef enum ClusterCtrcCloseResult
{
	CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN = 0,
	CLUSTER_CTRC_CLOSE_PENDING_DRAIN = 1,
	CLUSTER_CTRC_CLOSE_ACK_READY = 2
} ClusterCtrcCloseResult;

typedef enum ClusterCtrcLossResult
{
	CLUSTER_CTRC_LOSS_BLOCKED = 0
} ClusterCtrcLossResult;

typedef enum ClusterCtrcTargetState
{
	CTRC_TARGET_ABSENT = 0,
	CTRC_TARGET_AMBIGUOUS,
	CTRC_TARGET_TERMINAL_LOCK_ONLY,
	CTRC_TARGET_ABORTED_UPDATER,
	CTRC_TARGET_COMMITTED_UPDATER,
	CTRC_TARGET_ACTIVE_SURVIVOR
} ClusterCtrcTargetState;

typedef struct ClusterCtrcCleanReferenceInput
{
	uint8 target_state;
	bool source_transition_censused;
	bool page_authority_exact;
	bool successor_topology_exact;
	uint16 active_companions;
	uint16 unknown_companions;
} ClusterCtrcCleanReferenceInput;

typedef enum ClusterCtrcCleanResult
{
	CTRC_CLEAN_RETAIN = 0,
	CTRC_CLEANED_ABSENT = 1,
	CTRC_CLEANED_TERMINAL_REWRITE = 2,
	CTRC_CLEANED_SUCCESSOR_REPLACED = 3
} ClusterCtrcCleanResult;

typedef enum ClusterCtrcCurrentMxRewriteKind
{
	CTRC_CURRENT_MX_REWRITE_RETAIN = 0,
	CTRC_CURRENT_MX_REWRITE_INVALIDATE,
	CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER,
	CTRC_CURRENT_MX_REWRITE_SUCCESSOR
} ClusterCtrcCurrentMxRewriteKind;

/* Pure descriptor-level plan.  It contains no shared-memory pointer and is
 * produced before allocating a successor descriptor or touching a page. */
typedef struct ClusterCtrcCurrentMxRewritePlan
{
	uint8 kind;
	uint8 clean_result;
	uint16 survivor_count;
	TransactionId committed_updater_xid;
	SCN committed_updater_scn;
	MultiXactMember survivors[CLUSTER_CURRENT_MX_MAX_MEMBERS];
} ClusterCtrcCurrentMxRewritePlan;

typedef struct ClusterCtrcTransferState
{
	uint16 active_survivor_count;
	uint16 successor_receipt_count;
	bool descriptor_durable;
	bool predecessor_removed;
} ClusterCtrcTransferState;

typedef enum ClusterCtrcTransferResult
{
	CLUSTER_CTRC_TRANSFER_REFUSED = 0,
	CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR = 1,
	CLUSTER_CTRC_TRANSFER_READY = 2,
	CLUSTER_CTRC_TRANSFER_REMOVED = 3
} ClusterCtrcTransferResult;

typedef enum ClusterCtrcAckResult
{
	CLUSTER_CTRC_ACK_DENIED = 0,
	CLUSTER_CTRC_ACK_RELEASED = 1,
	CTRC_ACK_RELEASED = CLUSTER_CTRC_ACK_RELEASED
} ClusterCtrcAckResult;

typedef struct ClusterCtrcCertificateInput
{
	const ClusterCtrcLocalReleaseAckV1 *acks;
	uint16 ack_count;
	uint16 reserved16;
	uint64 frozen_touched_bitmap;
	uint64 seal_generation;
	bool block0_terminal_exact;
	bool all_dependencies_durable;
} ClusterCtrcCertificateInput;

/* Stack-only immutable copy used across the no-CTRC-lock block-0 certificate
 * phase.  It is neither shared-memory nor wire ABI. */
typedef struct ClusterCtrcOriginCertificateSnapshot
{
	uint64 origin_index;
	ClusterCtrcOriginEntry origin;
	ClusterCtrcLocalReleaseAckV1 acks[CLUSTER_CTRC_MAX_PARTICIPANTS];
	uint16 ack_count;
	uint8 reserved[6];
} ClusterCtrcOriginCertificateSnapshot;

typedef enum ClusterCtrcCertificateResult
{
	CLUSTER_CTRC_CERTIFICATE_RETAIN = 0,
	CLUSTER_CTRC_CERTIFICATE_READY = 1
} ClusterCtrcCertificateResult;

typedef enum ClusterCtrcTerminalStatus
{
	CTRC_TERMINAL_UNKNOWN = 0,
	CTRC_TERMINAL_COMMITTED = 1,
	CTRC_TERMINAL_ABORTED = 2
} ClusterCtrcTerminalStatus;

typedef struct ClusterCtrcRecycleInput
{
	uint8 status;
	bool release_proven;
	bool durable_aborted;
	bool horizon_valid;
	uint64 commit_scn;
	uint64 horizon_scn;
} ClusterCtrcRecycleInput;

typedef enum ClusterCtrcCrashCut
{
	CTRC_CRASH_BEFORE_TOUCH = 1,
	CTRC_CRASH_AFTER_TOUCH_BEFORE_PROOF,
	CTRC_CRASH_PREPARED_BEFORE_APPLIED,
	CTRC_CRASH_APPLIED_BEFORE_PAGE_MUTATION,
	CTRC_CRASH_CLEANOUT_WAL_BEFORE_DURABLE,
	CTRC_CRASH_ALL_ACKS_BEFORE_CERTIFICATE,
	CTRC_CRASH_CERTIFICATE_BEFORE_FLUSH,
	CTRC_CRASH_DURABLE_CERTIFICATE_BEFORE_NOTIFICATION,
	CTRC_CRASH_ORIGIN_PRECERT_LOSS
} ClusterCtrcCrashCut;

typedef enum ClusterCtrcCrashDisposition
{
	CLUSTER_CTRC_CRASH_RETAIN = 0,
	CLUSTER_CTRC_CRASH_RELEASE_PROVEN = 1
} ClusterCtrcCrashDisposition;

typedef struct ClusterCtrcCapacity
{
	uint64 origin_key_entries;
	uint64 participant_key_entries;
	uint64 receipt_entries;
	uint64 participant_ack_summary_entries;
	uint64 origin_ack_inbox_entries;
	Size total_bytes;
} ClusterCtrcCapacity;

typedef struct ClusterCtrcDebugSnapshot
{
	uint64 origin_open;
	uint64 origin_sealing;
	uint64 origin_release_proven;
	uint64 origin_blocked;
	uint64 participant_open;
	uint64 participant_draining;
	uint64 participant_ack_ready;
	uint64 participant_ack_frozen;
	uint64 participant_blocked;
	uint64 receipt_prepared;
	uint64 receipt_applied;
	uint64 receipt_cleaned;
	uint64 receipt_cancelled;
	uint64 receipt_ack_frozen;
	uint64 receipt_blocked;
	uint64 full_refusal_count;
	uint64 test_barrier_hit_count;
	uint32 test_barrier_phase;
	uint32 cleaner_reason;
} ClusterCtrcDebugSnapshot;

typedef enum ClusterCtrcCapacityDisposition
{
	CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION = 0
} ClusterCtrcCapacityDisposition;

StaticAssertDecl(sizeof(ClusterCtrcTxnKeyV1) == CLUSTER_CTRC_TXN_KEY_BYTES,
				 "CTRC transaction key must remain exactly 96 bytes");
StaticAssertDecl(sizeof(ClusterCtrcSealRequestV1) == CLUSTER_CTRC_SEAL_REQUEST_BYTES,
				 "CTRC seal request must remain exactly 136 bytes");
StaticAssertDecl(sizeof(ClusterCtrcSealReplyHeaderV1) == CLUSTER_CTRC_REPLY_HEADER_BYTES,
				 "CTRC reply header must remain exactly 64 bytes");
StaticAssertDecl(sizeof(ClusterCtrcLocalReleaseAckV1) == CLUSTER_CTRC_LOCAL_ACK_BYTES,
				 "CTRC local release ACK must remain exactly 416 bytes");
StaticAssertDecl(offsetof(ClusterCtrcLocalReleaseAckV1, row_digest_sha256) == 224,
				 "CTRC ACK digest offset must remain 224");
StaticAssertDecl(offsetof(ClusterCtrcLocalReleaseAckV1, crc32c) == 412,
				 "CTRC ACK CRC offset must remain 412");

extern const uint8 cluster_ctrc_empty_sha256[32];
extern bool cluster_ctrc_sha256_exact(const void *bytes, Size length,
	uint8 digest[32]);
extern bool cluster_ctrc_seal_request_encode(
	const ClusterCtrcTxnKeyV1 *key, uint64 request_id,
	uint32 grant_generation, uint64 seal_generation,
	uint32 participant_capability_record_generation,
	ClusterCtrcSealSuboperation suboperation,
	uint8 *bytes, Size length);
extern bool cluster_ctrc_seal_request_decode(
	const uint8 *bytes, Size length, uint64 authenticated_system_identifier,
	int32 envelope_source_node, int32 local_node, uint64 current_epoch,
	ClusterCtrcSealRequestV1 *request_out, ClusterCtrcTxnKeyV1 *key_out);
extern bool cluster_ctrc_local_release_ack_encode(
	const ClusterCtrcLocalReleaseAckV1 *ack,
	uint8 bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES]);
extern bool cluster_ctrc_local_release_ack_decode(
	const uint8 bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES],
	ClusterCtrcLocalReleaseAckV1 *ack_out);
extern bool cluster_ctrc_seal_reply_encode(
	const uint8 *request_bytes, Size request_length,
	int32 source_node, int32 destination_node,
	ClusterCtrcSealReplyResult result, uint16 first_reason,
	const ClusterCtrcLocalReleaseAckV1 *ack,
	uint8 *page, Size page_length);
extern bool cluster_ctrc_seal_reply_decode(
	const uint8 *page, Size page_length,
	const uint8 *request_bytes, Size request_length,
	int32 expected_source_node, int32 expected_destination_node,
	ClusterCtrcSealReplyHeaderV1 *header_out,
	ClusterCtrcLocalReleaseAckV1 *ack_out);

extern bool cluster_ctrc_capacity_compute(Size n_buffers, int max_backends,
										  int declared_nodes,
										  ClusterCtrcCapacity *capacity);
extern bool cluster_ctrc_participant_index_compute(
	uint64 origin_index, uint64 origin_entries, uint64 participant_entries,
	uint16 participant_node_id, uint64 *index_out);
extern ClusterCtrcCapacityDisposition cluster_ctrc_runtime_full_disposition(void);
extern Size cluster_ctrc_shmem_size(void);
extern void cluster_ctrc_shmem_init(void);
extern void cluster_ctrc_shmem_register(void);
extern bool cluster_ctrc_shmem_ready(void);
extern const char *cluster_ctrc_stat_name(ClusterCtrcStatId stat);
extern uint64 cluster_ctrc_stat_get(ClusterCtrcStatId stat);
extern void cluster_ctrc_stat_bump(ClusterCtrcStatId stat);
extern const char *cluster_ctrc_cleaner_reason_name(
	ClusterCtrcCleanerReason reason);
extern ClusterCtrcCleanerReason cluster_ctrc_cleaner_reason_get(void);
extern void cluster_ctrc_cleaner_reason_set(ClusterCtrcCleanerReason reason);
extern bool cluster_ctrc_cleaner_bind_worker(unsigned worker_id);
extern uint64 cluster_ctrc_cleaner_local_progress(void);
extern uint64 cluster_ctrc_cleaner_local_passes(void);
extern ClusterCtrcCleanerReason cluster_ctrc_cleaner_worker_reason(unsigned worker_id);
extern bool cluster_ctrc_cleaner_worker_observation(unsigned worker_id,
													ClusterCtrcCleanerWorkerObservation *out);
extern bool cluster_ctrc_debug_snapshot(ClusterCtrcDebugSnapshot *snapshot);
extern bool cluster_ctrc_test_barrier_control(
	ClusterCtrcTestBarrierPhase phase, bool armed);
extern void cluster_ctrc_test_barrier_wait(ClusterCtrcTestBarrierPhase phase);
extern void cluster_ctrc_note_publication_after_apply(
	const ClusterCtrcReceiptHandle *handle, bool current_mx);
extern bool cluster_ctrc_origin_grant_publishable_entry(
	const ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	uint32 grant_generation);
extern bool cluster_ctrc_origin_grant_publishable(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	uint32 grant_generation);

extern ClusterCtrcOriginReserveResult cluster_ctrc_origin_reserve_active(
	const ClusterCtrcTxnKeyV1 *key,
	ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_release_overlap_pending(
	const ClusterCtrcTxnKeyV1 *key);
extern ClusterCtrcOriginReserveResult cluster_ctrc_origin_reserve_entry(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint64 origin_index, uint64 reservation_generation,
	ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_cancel_pre_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_block_post_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_cancel_pre_bind(
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_block_post_bind(
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_open_reserved(
	const ClusterCtrcOriginReservation *reservation,
	uint32 *grant_generation);
extern ClusterCtrcTouchResult cluster_ctrc_origin_touch_exact(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	ClusterCtrcProofClass proof_class, uint32 *grant_out);

extern ClusterCtrcOriginOpenResult cluster_ctrc_origin_open_active(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint32 grant_generation);
extern ClusterCtrcTouchResult cluster_ctrc_origin_record_touched(
	ClusterCtrcOriginEntry *origin,
	const ClusterCtrcParticipantIdentity *participant,
	ClusterCtrcProofClass proof_class, uint32 *grant_out);
extern bool cluster_ctrc_origin_has_exact_touch(
	const ClusterCtrcOriginEntry *origin,
	const ClusterCtrcParticipantIdentity *participant);
extern bool cluster_ctrc_origin_begin_seal_entry(
	ClusterCtrcOriginEntry *origin, uint64 seal_generation);
extern bool cluster_ctrc_origin_arm_close_entry(
	ClusterCtrcOriginEntry *origin, uint16 participant_node_id,
	uint64 request_id);
extern bool cluster_ctrc_origin_note_close_reply_entry(
	ClusterCtrcOriginEntry *origin, uint16 participant_node_id,
	uint64 request_id, ClusterCtrcSealReplyResult result);
extern bool cluster_ctrc_origin_arm_certificate_entry(
	ClusterCtrcOriginEntry *origin, uint16 participant_node_id,
	uint64 request_id);
extern bool cluster_ctrc_origin_note_certificate_reply_entry(
	ClusterCtrcOriginEntry *origin, uint16 participant_node_id,
	uint64 request_id, ClusterCtrcSealReplyResult result);
extern bool cluster_ctrc_origin_begin_cleaning_entry(
	ClusterCtrcOriginEntry *origin);
extern bool cluster_ctrc_origin_request_snapshot_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcTxnKeyV1 *key_out,
	ClusterCtrcParticipantIdentity *identity_out,
	uint32 *grant_generation_out, uint64 *seal_generation_out,
	ClusterCtrcSealSuboperation *suboperation_out);
extern bool cluster_ctrc_origin_close_request_snapshot_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcTxnKeyV1 *key_out,
	ClusterCtrcParticipantIdentity *identity_out,
	uint32 *grant_generation_out, uint64 *seal_generation_out);
extern bool cluster_ctrc_origin_note_close_reply_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcSealReplyResult result);
extern bool cluster_ctrc_origin_note_certificate_reply_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcSealReplyResult result);
/* False means unsupported batch shape, without mutation; true is consumption,
 * not collective release proof. Caller retains each rechecked admission. */
extern bool cluster_ctrc_origin_note_local_certificate_batch_shared(
	const ClusterCtrcCloseDispatch *dispatches,
	const ClusterCtrcSealReplyResult *results, Size count);
extern bool cluster_ctrc_origin_note_local_close_batch_shared(
	const ClusterCtrcCloseDispatch *dispatches, const ClusterCtrcSealReplyResult *results,
	const ClusterCtrcLocalReleaseAckV1 *acks, Size count);
extern bool cluster_ctrc_origin_next_open_shared(ClusterCtrcTxnKeyV1 *key_out);
extern bool cluster_ctrc_origin_begin_seal_shared(
	const ClusterCtrcTxnKeyV1 *key);
extern bool cluster_ctrc_origin_next_close_dispatch_shared(
	ClusterCtrcCloseDispatch *dispatch_out);
extern bool cluster_ctrc_cleaner_run_pass(void);

typedef enum ClusterCtrcNormalStopDomain {
	CTRC_STOP_DOMAIN_NONE = 0,
	CTRC_STOP_DOMAIN_ORIGIN,
	CTRC_STOP_DOMAIN_ORIGIN_ACK,
	CTRC_STOP_DOMAIN_PARTICIPANT,
	CTRC_STOP_DOMAIN_PARTICIPANT_ACK,
	CTRC_STOP_DOMAIN_RECEIPT
} ClusterCtrcNormalStopDomain;

typedef enum ClusterCtrcNormalStopReason {
	CTRC_STOP_REASON_NONE = 0,
	CTRC_STOP_REASON_UNINITIALIZED,
	CTRC_STOP_REASON_NOT_RECLAIMED,
	CTRC_STOP_REASON_RESERVED,
	CTRC_STOP_REASON_BLOCKED,
	CTRC_STOP_REASON_MALFORMED
} ClusterCtrcNormalStopReason;

/* Caller-owned diagnostics; never stored as a release certificate. */
typedef struct ClusterCtrcNormalStopObservation {
	ClusterNormalStopPollResult result;
	ClusterCtrcNormalStopDomain domain;
	ClusterCtrcNormalStopReason reason;
	uint32 state;
	uint64 object_index;
	ClusterCtrcTxnKeyV1 key;
} ClusterCtrcNormalStopObservation;

extern ClusterNormalStopPollResult
cluster_ctrc_normal_stop_poll(ClusterCtrcNormalStopObservation *observation);
/* The calling cleaner only; the outer undo pass must also have returned. */
extern bool cluster_ctrc_cleaner_local_idle(void);

typedef enum ClusterCtrcCapacityProbeResult {
	CLUSTER_CTRC_CAPACITY_REFUSE = 0,
	CLUSTER_CTRC_CAPACITY_WAIT = 1,
	CLUSTER_CTRC_CAPACITY_RETRY = 2
} ClusterCtrcCapacityProbeResult;
extern ClusterCtrcCapacityProbeResult
cluster_ctrc_capacity_probe_current(uint32 segment_id, SCN horizon, uint64 epoch,
									ClusterCtrcTxnKeyV1 *continuation);

/*
 * L11/L12 canonical release sampler for the current allocator GC.  The
 * caller holds no allocator/page/CTRC lock.  Success means one exact local
 * terminal slot was sampled under admitted block-0 current authority with
 * the durable CTRC release bit set, a stable native terminal bracket and the
 * supplied horizon-fold epoch still current.  It never mutates block 0.
 */
/* Read-only publication barrier, never terminal/release authority. The
 * invalid slot sentinel checks all physical origins in the header. */
extern bool cluster_ctrc_reuse_handoff_complete(const struct UndoSegmentHeaderData *header,
												uint16 slot_offset);
extern bool cluster_ctrc_terminal_release_sample_exact(
	uint32 segment_id, uint16 slot_offset, TransactionId xid,
	uint16 slot_wrap, uint8 terminal_status, SCN terminal_scn,
	uint64 expected_epoch);
extern ClusterCtrcParticipantOpenResult cluster_ctrc_participant_open(
	ClusterCtrcParticipantEntry *participant, const ClusterCtrcTxnKeyV1 *key,
	uint32 grant_generation,
	const ClusterCtrcParticipantIdentity *identity);
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceipt *receipt);
/* Caller holds the participant and receipt table locks. */
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare_table_locked(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceipt *receipts, uint8 *probe_states, Size receipt_count,
	uint64 journal_sequence, uint64 *receipt_index_out,
	Size *probe_count_out);
extern bool cluster_ctrc_receipt_reclaim_frozen_table_locked(
	const ClusterCtrcTxnKeyV1 *key, uint64 expected_receipt_count,
	ClusterCtrcReceipt *receipts, uint8 *probe_states, Size receipt_count,
	Size *reclaimed_count_out);
#define CLUSTER_CTRC_RECLAIM_BATCH_MAX 64
/* Call-local work only; not a shared, durable, or wire authority record. */
typedef struct ClusterCtrcReclaimWork {
	ClusterCtrcTxnKeyV1 key;
	uint64 expected_count;
	Size found;
	bool reclaimed;
} ClusterCtrcReclaimWork;
extern bool cluster_ctrc_receipts_reclaim_batch_locked(ClusterCtrcReclaimWork *work,
													   Size work_count,
													   ClusterCtrcReceipt *receipts,
													   uint8 *probe_states, Size receipt_count);
/* Caller has authenticated each local certificate request separately. */
extern bool
cluster_ctrc_participant_certificate_batch_shared(const ClusterCtrcCloseDispatch *dispatches,
												  Size count, ClusterCtrcSealReplyResult *results);
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_apply_prepared(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_apply_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_retarget_itl(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_retarget_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern bool cluster_ctrc_receipt_itl_reuse_candidate_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcItlTargetIdentity *current_target,
	const uint8 current_slot_sha256[32]);
extern bool cluster_ctrc_receipt_cancel_prepared(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt);
extern bool cluster_ctrc_receipt_cancel_shared(
	const ClusterCtrcReceiptHandle *handle);
extern ClusterCtrcDischargeResult cluster_ctrc_receipt_discharge_itl(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability);
extern bool cluster_ctrc_itl_target_identity_matches(
	const ClusterCtrcTargetV1 *target,
	const ClusterCtrcItlTargetIdentity *expected);
extern ClusterCtrcDischargeResult cluster_ctrc_receipt_discharge_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcItlTargetIdentity *expected_target,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability);
extern ClusterCtrcDischargeResult cluster_ctrc_receipt_discharge_current_mx(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *expected_target,
	ClusterCtrcCleanResult clean_result,
	const ClusterCtrcDurability *durability);
extern ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_current_mx_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *expected_target,
	ClusterCtrcCleanResult clean_result,
	const ClusterCtrcDurability *durability);
extern ClusterCtrcItlCleanoutApplyResult cluster_ctrc_itl_cleanout_slot(
	const ClusterCtrcTxnKeyV1 *key, const ClusterCtrcTargetV1 *target,
	ClusterCtrcTerminalStatus terminal_status, SCN commit_scn,
	ClusterItlSlotData *slot);
extern ClusterCtrcCloseResult cluster_ctrc_participant_close(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation);
extern ClusterCtrcCloseResult cluster_ctrc_participant_close_or_tombstone(
	ClusterCtrcParticipantEntry *participant, const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation);
extern ClusterCtrcSealReplyResult cluster_ctrc_participant_request_apply(
	ClusterCtrcParticipantEntry *participant,
	ClusterCtrcLocalReleaseAckV1 *ack_summary,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation, ClusterCtrcSealSuboperation suboperation,
	uint16 *first_reason, ClusterCtrcLocalReleaseAckV1 *ack_out);
extern ClusterCtrcSealReplyResult cluster_ctrc_participant_request_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation, ClusterCtrcSealSuboperation suboperation,
	uint16 *first_reason, ClusterCtrcLocalReleaseAckV1 *ack_out);
extern bool cluster_ctrc_origin_ack_land_entry(
	ClusterCtrcOriginEntry *origin, uint64 request_id,
	const ClusterCtrcLocalReleaseAckV1 *ack,
	ClusterCtrcLocalReleaseAckV1 *ack_slot);
extern bool cluster_ctrc_origin_ack_land_shared(
	uint64 request_id, const ClusterCtrcLocalReleaseAckV1 *ack);
extern ClusterCtrcLossResult cluster_ctrc_participant_note_owner_loss(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt);
extern ClusterCtrcCleanResult cluster_ctrc_clean_reference(
	const ClusterCtrcCleanReferenceInput *input);
extern bool cluster_ctrc_current_mx_terminal_proof_exact(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCurrentMxKey *descriptor_key,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	ClusterCtrcTerminalStatus *terminal_status_out, SCN *commit_scn_out);
extern bool cluster_ctrc_current_mx_rewrite_plan(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	ClusterCtrcCurrentMxRewritePlan *plan);
extern bool cluster_ctrc_native_current_mx_mutation_allowed(
	bool peer_mode, int mx_origin_slot);
extern bool cluster_ctrc_relation_removal_ready_from_snapshot(
	const ClusterCtrcReceipt *receipts, Size receipt_count,
	uint32 spc_oid, uint32 db_oid, uint32 rel_number);
extern bool cluster_ctrc_relation_removal_ready_shared(
	uint32 spc_oid, uint32 db_oid, uint32 rel_number);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_note_successor_receipt(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_note_descriptor_durable(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_remove_predecessor(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcAckResult cluster_ctrc_participant_build_ack(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcDurability *durability,
	ClusterCtrcLocalReleaseAckV1 *ack);
extern ClusterCtrcAckResult cluster_ctrc_participant_ack_from_snapshot(
	const ClusterCtrcParticipantEntry *participant,
	ClusterCtrcReceipt *receipts, Size receipt_count,
	const ClusterCtrcDurability *durability,
	ClusterCtrcLocalReleaseAckV1 *ack);
extern ClusterCtrcCertificateResult cluster_ctrc_origin_certificate_validate(
	const ClusterCtrcCertificateInput *input);
extern bool cluster_ctrc_origin_certificate_digest(
	const ClusterCtrcCertificateInput *input, uint8 digest[32]);
extern bool cluster_ctrc_origin_certificate_snapshot_entry(
	const ClusterCtrcOriginEntry *origin,
	const ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS],
	uint64 origin_index, ClusterCtrcOriginCertificateSnapshot *snapshot);
extern bool cluster_ctrc_origin_certificate_commit_entry(
	ClusterCtrcOriginEntry *origin,
	const ClusterCtrcOriginCertificateSnapshot *snapshot);
extern bool cluster_ctrc_terminal_recyclable(
	const ClusterCtrcRecycleInput *input);
extern ClusterCtrcCrashDisposition cluster_ctrc_crash_cut_disposition(
	ClusterCtrcCrashCut cut);

#endif /* CLUSTER_TERMINAL_REF_CENSUS_H */
