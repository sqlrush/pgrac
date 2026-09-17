/*-------------------------------------------------------------------------
 *
 * cluster_page_detached.h
 *    STOP-06 whole-record detached page codec contract.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_DETACHED_H
#define CLUSTER_PAGE_DETACHED_H

#include "access/xlogreader.h"
#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_rf_route.h"

#define CLUSTER_PAGE_DETACHED_INTERFACE_V1 1

typedef enum RfDetachedComponentOwnerV1 {
	RF_DETACHED_COMPONENT_INVALID = 0,
	RF_DETACHED_COMPONENT_PAGE_CODEC = 1,
	RF_DETACHED_COMPONENT_REBUILDABLE = 2,
	RF_DETACHED_COMPONENT_SIDE_TYPED = 3
} RfDetachedComponentOwnerV1;

typedef struct RfDetachedComponentPlanV1 {
	uint8 block_id;
	uint8 page_class;
	uint8 owner;
	uint8 codec_id;
	uint16 component_ordinal;
	uint16 edge_flags;
	uint8 before_kind;
	uint8 result_kind;
	uint8 reserved_zero[6];
	RfPageVersionV1 before;
	RfPageVersionV1 result;
} RfDetachedComponentPlanV1;

typedef struct RfDetachedRecordPlanV1 {
	XLogReaderState *source_record;
	RfOpcodeRouteV1 route;
	uint64 result_token;
	uint32 component_count;
	RfDetachedComponentPlanV1 components[RF_PAGE_STABLE_MAX_COMPONENTS];
	bool preflight_complete;
} RfDetachedRecordPlanV1;

typedef RfPageProofDetailV1 (*RfDetachedOwnerPreflightV1)(void *arg, const RfOpcodeRouteV1 *route,
														  const RfPageVersionEdgeEntryV1 *edge,
														  const DecodedBkpBlock *block);

typedef struct RfDetachedOwnerOpsV1 {
	void *arg;
	RfDetachedOwnerPreflightV1 preflight_side_record;
	RfDetachedOwnerPreflightV1 preflight_side_component;
	RfDetachedOwnerPreflightV1 preflight_rebuildable_component;
} RfDetachedOwnerOpsV1;

struct RfDetachedPageCodecV1;

typedef RfPageProofDetailV1 (*RfDetachedCodecPreflightV1)(const struct RfDetachedPageCodecV1 *codec,
														  const RfOpcodeRouteV1 *route,
														  const RfPageVersionEdgeEntryV1 *edge,
														  const DecodedBkpBlock *block);

typedef RfPageProofDetailV1 (*RfDetachedCodecApplyV1)(XLogReaderState *record, uint8 block_id,
													  uint64 result_token,
													  const char old_page[BLCKSZ],
													  char new_page[BLCKSZ]);

typedef struct RfDetachedPageCodecV1 {
	uint8 codec_id;
	uint8 rmid;
	uint16 abi_version;
	RfDetachedCodecPreflightV1 preflight;
	RfDetachedCodecApplyV1 apply;
} RfDetachedPageCodecV1;

extern const RfDetachedPageCodecV1 *rf_page_detached_codec_lookup_v1(uint8 codec_id);
extern RfPageProofDetailV1 rf_page_detached_preflight_v1(XLogReaderState *record, bool space_active,
														 const RfDetachedOwnerOpsV1 *owner_ops,
														 RfDetachedRecordPlanV1 *plan);
extern RfPageProofDetailV1 rf_page_detached_apply_v1(const RfDetachedRecordPlanV1 *plan,
													 uint32 component_index,
													 const char old_page[BLCKSZ],
													 char new_page[BLCKSZ]);

#endif /* CLUSTER_PAGE_DETACHED_H */
