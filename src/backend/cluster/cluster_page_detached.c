/*-------------------------------------------------------------------------
 *
 * cluster_page_detached.c
 *    STOP-06 whole-record detached page codec preflight and apply.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rmgr.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_page_detached.h"
#include "storage/bufpage.h"

static RfPageProofDetailV1 detached_codec_preflight(const RfDetachedPageCodecV1 *codec,
													const RfOpcodeRouteV1 *route,
													const RfPageVersionEdgeEntryV1 *edge,
													const DecodedBkpBlock *block);
static RfPageProofDetailV1 detached_codec_apply(XLogReaderState *record, uint8 block_id,
												uint64 result_token, const char old_page[BLCKSZ],
												char new_page[BLCKSZ]);

#define RF_DETACHED_CODEC(codec_, rmid_)                                                           \
	{                                                                                              \
		codec_, rmid_, CLUSTER_PAGE_DETACHED_INTERFACE_V1, detached_codec_preflight,               \
			detached_codec_apply                                                                   \
	}

static const RfDetachedPageCodecV1 detached_codecs[]
	= { RF_DETACHED_CODEC(RF_ROUTE_CODEC_XLOG_FPI, RM_XLOG_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_HEAP2, RM_HEAP2_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_HEAP, RM_HEAP_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_BTREE, RM_BTREE_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_HASH, RM_HASH_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_GIN, RM_GIN_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_GIST, RM_GIST_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_SEQ, RM_SEQ_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_SPGIST, RM_SPGIST_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_BRIN, RM_BRIN_ID),
		RF_DETACHED_CODEC(RF_ROUTE_CODEC_GENERIC, RM_GENERIC_ID) };

#undef RF_DETACHED_CODEC

const RfDetachedPageCodecV1 *
rf_page_detached_codec_lookup_v1(uint8 codec_id)
{
	size_t i;

	for (i = 0; i < lengthof(detached_codecs); i++)
		if (detached_codecs[i].codec_id == codec_id)
			return &detached_codecs[i];
	return NULL;
}

static bool
ordinary_fork(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM || forknum == VISIBILITYMAP_FORKNUM || forknum == INIT_FORKNUM;
}

static RfPageProofDetailV1
detached_codec_preflight(const RfDetachedPageCodecV1 *codec, const RfOpcodeRouteV1 *route,
						 const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	if (codec == NULL || route == NULL || edge == NULL || block == NULL
		|| route->record_owner != RF_ROUTE_OWNER_PAGE_CODEC || route->codec_id != codec->codec_id
		|| route->rmid != codec->rmid || edge->page_class != RF_PAGE_CLASS_ORDINARY
		|| !block->in_use)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	if (block->has_image && block->apply_image
		&& (block->bkp_image == NULL || block->bimg_len == 0))
		return RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static bool
detached_page_layout_valid(const char page[BLCKSZ])
{
	const PageHeader header = (const PageHeader)page;

	return !PageIsNew((Page)page) && header->pd_lower >= SizeOfPageHeaderData
		   && header->pd_lower <= header->pd_upper && header->pd_upper <= header->pd_special
		   && header->pd_special <= BLCKSZ;
}

static RfPageProofDetailV1
detached_codec_apply(XLogReaderState *record, uint8 block_id, uint64 result_token,
					 const char old_page[BLCKSZ], char new_page[BLCKSZ])
{
	PGAlignedBlock scratch;
	ClusterBlkApplyResult result;

	if (record == NULL || old_page == NULL || new_page == NULL || result_token == 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memcpy(scratch.data, old_page, BLCKSZ);
	result = cluster_block_apply_one(record, block_id, scratch.data);
	if (result == CLUSTER_BLKAPPLY_UNSUPPORTED)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	if (result != CLUSTER_BLKAPPLY_OK)
		return RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED;
	if (!detached_page_layout_valid(scratch.data))
		return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
	((PageHeader)scratch.data)->pd_block_scn = (SCN)result_token;
	memcpy(new_page, scratch.data, BLCKSZ);
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
preflight_external_owner(const RfDetachedOwnerOpsV1 *owner_ops, RfDetachedOwnerPreflightV1 callback,
						 const RfOpcodeRouteV1 *route, const RfPageVersionEdgeEntryV1 *edge,
						 const DecodedBkpBlock *block)
{
	if (owner_ops == NULL || callback == NULL)
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	return callback(owner_ops->arg, route, edge, block);
}

static void
set_component_versions(RfDetachedComponentPlanV1 *component, const RfPageVersionEdgeEntryV1 *edge,
					   uint64 result_token)
{
	component->before = edge->before;
	memcpy(component->result.segment_incarnation, edge->result_incarnation, 16);
	component->result.mutation_token = result_token;
}

RfPageProofDetailV1
rf_page_detached_preflight_v1(XLogReaderState *record, bool space_active,
							  const RfDetachedOwnerOpsV1 *owner_ops, RfDetachedRecordPlanV1 *plan)
{
	DecodedXLogRecord *decoded;
	RfDetachedRecordPlanV1 candidate;
	RfOpcodeRouteLookupResultV1 lookup;
	uint32 block_count = 0;
	uint32 i;

	if (record == NULL || record->record == NULL || plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	decoded = record->record;
	if (decoded->max_block_id > XLR_MAX_BLOCK_ID)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	if (decoded->max_block_id >= 0)
		for (i = 0; i <= (uint32)decoded->max_block_id; i++)
			if (decoded->blocks[i].in_use)
				block_count++;

	memset(&candidate, 0, sizeof(candidate));
	lookup = rf_opcode_route_lookup_v1(decoded->header.xl_rmid, decoded->header.xl_info,
									   block_count != 0, space_active, &candidate.route);
	if (lookup != RF_OPCODE_ROUTE_OK)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	candidate.source_record = record;

	if (candidate.route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC) {
		RfPageProofDetailV1 detail;

		if (decoded->has_page_version_edge)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		detail = preflight_external_owner(
			owner_ops, owner_ops != NULL ? owner_ops->preflight_side_record : NULL,
			&candidate.route, NULL, NULL);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			return detail;
		candidate.preflight_complete = true;
		*plan = candidate;
		return RF_PAGE_PROOF_DETAIL_OK;
	}

	if (!decoded->has_page_version_edge || block_count == 0
		|| decoded->page_version_edge.entry_count != block_count
		|| block_count > RF_PAGE_STABLE_MAX_COMPONENTS
		|| decoded->page_version_edge.result_token == 0)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	candidate.result_token = decoded->page_version_edge.result_token;
	candidate.component_count = block_count;

	for (i = 0; i < block_count; i++) {
		const RfPageVersionEdgeEntryV1 *edge = &decoded->page_version_edge.entries[i];
		const DecodedBkpBlock *block;
		RfDetachedComponentPlanV1 *component = &candidate.components[i];
		RfPageProofDetailV1 detail;

		if (edge->block_id > decoded->max_block_id)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		block = &decoded->blocks[edge->block_id];
		if (!block->in_use || edge->component_ordinal != block->component_ordinal)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		component->block_id = edge->block_id;
		component->page_class = edge->page_class;
		component->component_ordinal = edge->component_ordinal;
		component->edge_flags = edge->edge_flags;
		component->before_kind = edge->before_kind;
		component->result_kind = edge->result_kind;
		set_component_versions(component, edge, candidate.result_token);

		switch (edge->page_class) {
		case RF_PAGE_CLASS_ORDINARY: {
			const RfDetachedPageCodecV1 *codec;

			if (!ordinary_fork(block->forknum) || !rf_page_version_present_v1(&component->result))
				return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
			codec = rf_page_detached_codec_lookup_v1(candidate.route.codec_id);
			if (codec == NULL)
				return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
			detail = codec->preflight(codec, &candidate.route, edge, block);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				return detail;
			component->owner = RF_DETACHED_COMPONENT_PAGE_CODEC;
			component->codec_id = codec->codec_id;
			break;
		}
		case RF_PAGE_CLASS_REBUILDABLE_FSM:
			if (block->forknum != FSM_FORKNUM)
				return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
			detail = preflight_external_owner(
				owner_ops, owner_ops != NULL ? owner_ops->preflight_rebuildable_component : NULL,
				&candidate.route, edge, block);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				return detail;
			component->owner = RF_DETACHED_COMPONENT_REBUILDABLE;
			break;
		case RF_PAGE_CLASS_ROUTED_SPACE:
			if (!space_active || block->forknum != (ForkNumber)4)
				return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
			/* FALLTHROUGH */
		case RF_PAGE_CLASS_ROUTED_HEADER:
		case RF_PAGE_CLASS_ROUTED_SIDE:
			detail = preflight_external_owner(
				owner_ops, owner_ops != NULL ? owner_ops->preflight_side_component : NULL,
				&candidate.route, edge, block);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				return detail;
			component->owner = RF_DETACHED_COMPONENT_SIDE_TYPED;
			break;
		default:
			return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
		}
	}

	candidate.preflight_complete = true;
	*plan = candidate;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_detached_apply_v1(const RfDetachedRecordPlanV1 *plan, uint32 component_index,
						  const char old_page[BLCKSZ], char new_page[BLCKSZ])
{
	const RfDetachedComponentPlanV1 *component;
	const RfDetachedPageCodecV1 *codec;

	if (plan == NULL || !plan->preflight_complete || plan->source_record == NULL
		|| component_index >= plan->component_count || old_page == NULL || new_page == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	component = &plan->components[component_index];
	if (component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
		|| component->page_class != RF_PAGE_CLASS_ORDINARY
		|| component->result_kind != RF_PAGE_STATE_PRESENT
		|| !rf_page_version_present_v1(&component->result)
		|| component->result.mutation_token != plan->result_token)
		return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
	codec = rf_page_detached_codec_lookup_v1(component->codec_id);
	if (codec == NULL || codec->codec_id != plan->route.codec_id || codec->rmid != plan->route.rmid)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	return codec->apply(plan->source_record, component->block_id, plan->result_token, old_page,
						new_page);
}
