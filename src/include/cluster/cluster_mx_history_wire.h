/*-------------------------------------------------------------------------
 * cluster_mx_history_wire.h
 *    Exact, private-consumption history frames. Codecs confer no authority.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MX_HISTORY_WIRE_H
#define CLUSTER_MX_HISTORY_WIRE_H

#include "c.h"

#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
#include "cluster/cluster_gcs_block.h"
#endif

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_MX_HISTORY_VERSION 1
#define CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION 2
#define CLUSTER_MX_HISTORY_KIND 5
#define CLUSTER_MX_HISTORY_REPLY_STATUS 31
#define CLUSTER_MX_HISTORY_REQUEST_BYTES 424
#define CLUSTER_MX_HISTORY_PEER_BYTES 136
#define CLUSTER_MX_HISTORY_REPLY_BYTES 8816
#define CLUSTER_MX_HISTORY_REPLY_BODY_BYTES 8768
#define CLUSTER_MX_HISTORY_EXPECT_OWNER UINT16_C(1)
#define CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION UINT16_C(2)
#define CLUSTER_MX_HISTORY_CANCEL UINT16_C(0x8000)
#define CLUSTER_MX_HISTORY_ORDERED UINT32_C(1)
#define CLUSTER_MX_HISTORY_HAS_GRANT UINT32_C(2)

typedef enum ClusterMxHistoryEdge {
	CMXH_ROOT = 1,
	CMXH_GLOBAL_PREV = 2,
	CMXH_PAGE_PRIOR = 3,
	CMXH_CREATOR_ROOT = 4
} ClusterMxHistoryEdge;

typedef enum ClusterMxHistoryRecordClass {
	CMXH_V1_TOP = 1,
	CMXH_OPERATION = 2,
	CMXH_ROLLBACK_CONTROL = 3,
	CMXH_FOREIGN_CREATOR = 4
} ClusterMxHistoryRecordClass;

typedef struct ClusterMxHistoryRequestWire {
	uint8 bytes[424];
} ClusterMxHistoryRequestWire;
typedef struct ClusterMxHistoryPeerWire {
	uint8 bytes[136];
} ClusterMxHistoryPeerWire;
typedef struct ClusterMxHistoryReplyWire {
	uint8 bytes[8816];
} ClusterMxHistoryReplyWire;

StaticAssertDecl(sizeof(ClusterMxHistoryRequestWire) == 424, "history request size");
StaticAssertDecl(sizeof(ClusterMxHistoryPeerWire) == 136, "history peer size");
StaticAssertDecl(sizeof(ClusterMxHistoryReplyWire) == 8816, "history reply size");

#ifndef FRONTEND

/* Native compare inputs. No native padding or enum representation goes on
 * the wire. Generation zero is valid; presence is represented by flags. */
typedef struct ClusterMxHistoryRequest {
	GcsBlockForwardPayload forward;
	ClusterTxLocator locator;
	uint8 edge_kind, root_ordinal, edge_itl_slot;
	uint16 flags;
	uint64 collector_id, capture_generation, backend_incarnation;
	uint32 requester_capability_generation, origin_capability_generation;
	uint64 feature_bit, admission_generation;
	/* CREATOR uses H/formation epoch/zero/local collector scope here. */
	SCN read_scn;
	uint64 read_epoch;
	SCN retained_floor;
	uint64 snapshot_identity, descriptor_hash;
	uint32 multixact_id;
	uint16 member_count;
	RelFileLocator target_locator;
	ForkNumber target_fork;
	BlockNumber target_block;
	OffsetNumber target_offset;
	ClusterR4CrOwnerChallenge requester_owner;
	ClusterItlSlotData root, edge;
	UBA parent;
	ClusterTTSlotPhysicalLocator expected_owner;
	uint32 expected_owner_generation, expected_data_generation;
	FullTransactionId expected_top_fxid;
	uint32 parent_data_generation, parent_record_crc;
} ClusterMxHistoryRequest;

typedef struct ClusterMxHistoryPeer {
	uint32 flags;
	uint8 record_class;
	ClusterTxOutcome outcome;
	uint8 effect;
	ClusterTxProofKind proof_kind;
	ClusterTTSlotPhysicalLocator canonical;
	uint32 canonical_generation;
	FullTransactionId top_fxid, operation_fxid;
	uint64 operation_sequence;
	UBA record, previous;
	uint32 data_generation, data_crc;
	XLogRecPtr data_lsn;
	SCN commit_scn;
	uint64 formation_epoch, admission_generation;
	uint32 ctrc_grant;
} ClusterMxHistoryPeer;

/* Backend-private landing, not a shared cache or a local TT resolution. */
typedef struct ClusterMxHistoryPeerResult {
	ClusterMxHistoryRequestWire request;
	uint8 data[8192];
	uint8 auth[16];
	ClusterMxHistoryPeerWire peer;
	uint8 admitted, consumed, zero[6];
} ClusterMxHistoryPeerResult;

/* Separate caller-private type; never a local TT or live companion. */
typedef struct ClusterForeignCreatorResult {
	ClusterMxHistoryRequestWire request;
	uint8 data[8192];
	uint8 auth[16];
	ClusterMxHistoryPeerWire peer;
	uint8 admitted, consumed, zero[6];
} ClusterForeignCreatorResult;
StaticAssertDecl(sizeof(ClusterForeignCreatorResult) == 8776, "private creator result size");
StaticAssertDecl(offsetof(ClusterForeignCreatorResult, data) == 424, "creator DATA offset");
StaticAssertDecl(offsetof(ClusterForeignCreatorResult, peer) == 8632, "creator peer offset");

typedef enum ClusterMxHistoryStep {
	CMXH_UNKNOWN = 0,
	CMXH_PENDING = 1,
	CMXH_COMPLETE = 2,
	CMXH_REQUALIFY = 3
} ClusterMxHistoryStep;

/* Route a private graph edge using actual backend/admission/capability state.
 * COMPLETE means a reserved request, PENDING means local slot backpressure;
 * unavailable identity is UNKNOWN. No DATA acquisition occurs here. */
extern ClusterMxHistoryStep
cluster_gcs_block_mx_history_prepare_routed(const ClusterMxHistoryRequest *edge,
											ClusterMxHistoryRequestWire *out);
/* Prepare reserves an original requester slot and allocates its original
 * request ID. The input ID must be zero. Call outside temporary ERROR-cleanup
 * brackets, after backend buffer-access initialization. Queue backpressure
 * preserves this exact attempt; poll has no SQL deadline. */
extern bool cluster_gcs_block_mx_history_prepare(const ClusterMxHistoryRequest *request,
												 ClusterMxHistoryRequestWire *out);
extern bool cluster_gcs_block_mx_history_submit(const ClusterMxHistoryRequestWire *request);
/* Outside heap content locks: wait on original reply/capacity/outbound events.
 * NULL requests backend slot capacity. PENDING means re-poll, including after
 * a periodic wake; it never grants authority or imposes a SQL deadline.
 * The caller retains ownership of exact request cancellation on failure. */
extern ClusterMxHistoryStep
cluster_gcs_block_mx_history_wait_event(const ClusterMxHistoryRequestWire *request);
extern ClusterMxHistoryStep
cluster_gcs_block_mx_history_poll(const ClusterMxHistoryRequestWire *request,
								  ClusterMxHistoryPeerResult *out);
extern ClusterMxHistoryStep
cluster_gcs_block_foreign_creator_poll(const ClusterMxHistoryRequestWire *request,
									   ClusterForeignCreatorResult *out);
extern void cluster_gcs_block_mx_history_cancel(const ClusterMxHistoryRequestWire *request);
/* Original DATA-ring disposal notification; no wire message or new authority. */
extern void cluster_gcs_block_mx_history_send_failed(const void *payload, uint16 length,
													 int32 origin);

StaticAssertDecl(sizeof(ClusterMxHistoryPeerResult) == 8776, "private history result size");
StaticAssertDecl(offsetof(ClusterMxHistoryPeerResult, data) == 424, "history DATA offset");
StaticAssertDecl(offsetof(ClusterMxHistoryPeerResult, peer) == 8632, "history peer offset");

/* Failure clears outputs. Inputs and outputs must not overlap. Source,
 * destination and epoch come from the authenticated transport/current scope.
 * Callers still own admission, retention, ordered sampling and heap recheck. */
extern bool cluster_mx_history_request_encode(const ClusterMxHistoryRequest *request,
											  ClusterMxHistoryRequestWire *out);
extern bool cluster_mx_history_request_decode(const void *payload, uint32 length, int32 source,
											  int32 origin, uint64 epoch,
											  ClusterMxHistoryRequest *out);
extern bool cluster_mx_history_request_cancel(const ClusterMxHistoryRequestWire *request,
											  ClusterMxHistoryRequestWire *out);
extern bool cluster_mx_history_request_same(const ClusterMxHistoryRequestWire *left,
											const ClusterMxHistoryRequestWire *right,
											bool allow_cancel);
extern bool cluster_mx_history_peer_encode(const ClusterMxHistoryPeer *peer,
										   ClusterMxHistoryPeerWire *out);
extern bool cluster_mx_history_peer_decode(const ClusterMxHistoryPeerWire *wire,
										   ClusterMxHistoryPeer *out);
extern bool cluster_mx_history_reply_encode(const ClusterMxHistoryRequestWire *request,
											int32 origin, XLogRecPtr authority_lsn,
											const uint8 data[8192], const uint8 auth[16],
											const ClusterMxHistoryPeerWire *peer,
											ClusterMxHistoryReplyWire *out);
/* A valid original 48B DENIED/RETRY is true with status set and no result.
 * Unknown/malformed input is false with every output zero. A codec result
 * is not publication: backend landing must recheck its actual slot/scope. */
extern bool cluster_mx_history_reply_decode(const void *payload, uint32 length, int32 source,
											const ClusterMxHistoryRequestWire *expected,
											uint8 *status, ClusterMxHistoryPeerResult *out);

#endif /* !FRONTEND */
#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_MX_HISTORY_WIRE_H */
