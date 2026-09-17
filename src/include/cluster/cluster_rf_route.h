/*-------------------------------------------------------------------------
 *
 * cluster_rf_route.h
 *    Exhaustive STOP-06 recovery-fabric WAL opcode routing manifest.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RF_ROUTE_H
#define CLUSTER_RF_ROUTE_H

#include "access/rmgr.h"

#define CLUSTER_RF_ROUTE_INTERFACE_V1 1

typedef enum RfRecordRouteOwnerV1 {
	RF_ROUTE_OWNER_INVALID = 0,
	RF_ROUTE_OWNER_PAGE_CODEC = 1,
	RF_ROUTE_OWNER_SIDE_TYPED = 2,
	RF_ROUTE_OWNER_LOGICAL_NOOP = 3
} RfRecordRouteOwnerV1;

typedef enum RfRouteBlockPolicyV1 {
	RF_ROUTE_BLOCKS_FORBIDDEN = 0,
	RF_ROUTE_BLOCKS_REQUIRED = 1,
	RF_ROUTE_BLOCKS_OPTIONAL_TYPED = 2
} RfRouteBlockPolicyV1;

typedef enum RfRouteCodecV1 {
	RF_ROUTE_CODEC_NONE = 0,
	RF_ROUTE_CODEC_XLOG_FPI = 1,
	RF_ROUTE_CODEC_HEAP2 = 2,
	RF_ROUTE_CODEC_HEAP = 3,
	RF_ROUTE_CODEC_BTREE = 4,
	RF_ROUTE_CODEC_HASH = 5,
	RF_ROUTE_CODEC_GIN = 6,
	RF_ROUTE_CODEC_GIST = 7,
	RF_ROUTE_CODEC_SEQ = 8,
	RF_ROUTE_CODEC_SPGIST = 9,
	RF_ROUTE_CODEC_BRIN = 10,
	RF_ROUTE_CODEC_GENERIC = 11,
	RF_ROUTE_CODEC_SIDE_STANDARD = 12,
	RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO = 13,
	RF_ROUTE_CODEC_SIDE_RAW_LAYOUT = 14,
	RF_ROUTE_CODEC_LOGICAL_NOOP = 15,
	RF_ROUTE_CODEC_SIDE_ADG_BARRIER = 16,
	RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM = 17
} RfRouteCodecV1;

typedef struct RfOpcodeRouteV1 {
	uint8 rmid;
	uint8 normalized_info;
	uint8 legal_info_flags;
	uint8 record_owner;
	uint8 block_policy;
	uint8 codec_id;
	uint16 reserved_zero;
} RfOpcodeRouteV1;

StaticAssertDecl(sizeof(RfOpcodeRouteV1) == 8, "RfOpcodeRouteV1 must remain an 8-byte ABI");

typedef enum RfOpcodeRouteLookupResultV1 {
	RF_OPCODE_ROUTE_OK = 0,
	RF_OPCODE_ROUTE_RMID_UNSUPPORTED = 1,
	RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED = 2,
	RF_OPCODE_ROUTE_FLAG_ILLEGAL = 3,
	RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID = 4,
	RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID = 5,
	RF_OPCODE_ROUTE_NESTED_VALUE_INVALID = 6
} RfOpcodeRouteLookupResultV1;

extern size_t rf_opcode_route_manifest_count_v1(void);
extern size_t rf_opcode_route_manifest_live_count_v1(void);
extern bool rf_opcode_route_manifest_entry_v1(size_t index, RfOpcodeRouteV1 *route, bool *active);
extern RfOpcodeRouteLookupResultV1 rf_opcode_route_lookup_v1(uint8 rmid, uint8 raw_info,
															 bool has_blocks, bool space_active,
															 RfOpcodeRouteV1 *route);

#endif /* CLUSTER_RF_ROUTE_H */
