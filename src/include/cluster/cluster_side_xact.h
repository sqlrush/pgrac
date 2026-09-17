/*-------------------------------------------------------------------------
 *
 * cluster_side_xact.h
 *    RF-SIDE immutable XACT decode contract.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_XACT_H
#define CLUSTER_SIDE_XACT_H

#include "access/xact.h"
#include "access/twophase.h"
#include "access/xlogreader.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_tt_2pc.h"

#define CLUSTER_SIDE_XACT_INTERFACE_V1 1

typedef enum RfSideXactKindV1 {
	RF_SIDE_XACT_INVALID = 0,
	RF_SIDE_XACT_COMMIT = 1,
	RF_SIDE_XACT_ABORT = 2,
	RF_SIDE_XACT_PREPARE = 3,
	RF_SIDE_XACT_COMMIT_PREPARED = 4,
	RF_SIDE_XACT_ABORT_PREPARED = 5
} RfSideXactKindV1;

typedef struct RfSideXactOperationV1 {
	uint64 system_identifier;
	RfSideXactKindV1 kind;
	uint16 origin_thread;
	uint16 reserved_zero;
	TransactionId xid;
	Oid database;
	Oid prepared_owner;
	uint32 xinfo;
	uint32 prepare_payload_length;
	TimestampTz prepared_at;
	SCN terminal_scn;
	TimestampTz terminal_timestamp;
	bool has_tt_delta;
	uint8 reserved49[7];
	xl_xact_tt_commit tt_delta;
	uint8 prepare_binding[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];
	char prepare_gid[GIDSIZE];
	uint16 prepared_record_version;
	uint16 prepared_binding_count;
	uint32 prepared_sublink_count;
	ClusterTT2PCBinding prepared_bindings[CLUSTER_TT_2PC_MAX_BINDINGS];
	ClusterTT2PCSubLink prepared_sublinks[CLUSTER_TT_2PC_MAX_SUBLINKS];
	UBA prepared_heads[CLUSTER_TT_2PC_MAX_BINDINGS];
} RfSideXactOperationV1;

extern bool rf_side_xact_decode_v1(XLogReaderState *record, uint64 system_identifier,
								   uint16 origin_thread, RfSideXactOperationV1 *out);
extern bool rf_side_xact_structural_preflight_v1(const RfSideXactOperationV1 *operation);

typedef enum RfSideXactApplyResultV1 {
	RF_SIDE_XACT_APPLY_OK = 0,
	RF_SIDE_XACT_APPLY_BLOCKED = 1,
	RF_SIDE_XACT_APPLY_CONFLICT = 2,
	RF_SIDE_XACT_APPLY_POST_READ_FAILED = 3
} RfSideXactApplyResultV1;

typedef struct RfSideXactTTCommitRequirementV1 {
	uint8 instance;
	bool requires_apply;
	uint16 slot_offset;
	uint16 wrap;
	uint16 reserved_zero;
	uint32 segment_id;
	TransactionId xid;
	SCN commit_scn;
} RfSideXactTTCommitRequirementV1;

typedef struct RfSideXactCommitPreparedRequirementsV1 {
	uint16 count;
	uint16 reserved_zero;
	RfSideXactTTCommitRequirementV1 bindings[CLUSTER_TT_2PC_MAX_BINDINGS];
} RfSideXactCommitPreparedRequirementsV1;

typedef struct RfSideXactTTAbortRequirementV1 {
	uint8 instance;
	bool requires_apply;
	uint16 reserved_zero;
	uint32 segment_id;
	uint16 slot_offset;
	uint16 wrap;
	TransactionId xid;
} RfSideXactTTAbortRequirementV1;

typedef struct RfSideXactAbortPreparedRequirementsV1 {
	uint16 count;
	uint16 reserved_zero;
	RfSideXactTTAbortRequirementV1 bindings[CLUSTER_TT_2PC_MAX_BINDINGS];
} RfSideXactAbortPreparedRequirementsV1;

/* Apply one already-decoded operation.  This function never reads WAL. */
extern RfSideXactApplyResultV1 rf_side_xact_apply_v1(const RfSideXactOperationV1 *operation);
extern RfSideXactApplyResultV1
rf_side_xact_target_preflight_owned_v1(const RfSideXactOperationV1 *operation,
									   const uint8 *owned_payload, uint32 owned_payload_length);
extern RfSideXactApplyResultV1 rf_side_xact_commit_prepared_requirements_v1(
	const RfSideXactOperationV1 *operation,
	RfSideXactCommitPreparedRequirementsV1 *out_requirements);
extern RfSideXactApplyResultV1 rf_side_xact_abort_prepared_requirements_v1(
	const RfSideXactOperationV1 *operation,
	RfSideXactAbortPreparedRequirementsV1 *out_requirements);
extern RfSideXactApplyResultV1 rf_side_xact_apply_owned_v1(const RfSideXactOperationV1 *operation,
														   const uint8 *owned_payload,
														   uint32 owned_payload_length);

#endif
