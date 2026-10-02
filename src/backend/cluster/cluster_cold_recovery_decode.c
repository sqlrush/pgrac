/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_decode.c
 *	  Map decoded native WAL records to typed cold replay plan input.
 *
 *	  Classification uses the same closed route registry and detached page
 *	  preflight as online thread recovery, so cold and online replay consume
 *	  one opcode table.  The cold owner policy accepts typed side records and
 *	  rebuildable FSM components, and refuses routed side components inside
 *	  page records until their typed cold owner exists.  Relation lifecycle
 *	  and prepared-transaction records are flagged for the plan, which judges
 *	  them against each generation's native redo start.
 *
 *	  Read-only: no buffers, storage, locks or authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_decode.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/heapam_xlog.h"
#include "access/xact.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"

static RfPageProofDetailV1
cold_preflight_side_record(void *arg, const RfOpcodeRouteV1 *route,
						   const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL
				   && (route->record_owner == RF_ROUTE_OWNER_SIDE_TYPED
					   || route->record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
				   && edge == NULL && block == NULL
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

/* A routed SPACE/HEADER/SIDE component inside a page record has no cold
 * owner yet; refusing it keeps the plan from silently omitting its effect. */
static RfPageProofDetailV1
cold_preflight_side_component(void *arg, const RfOpcodeRouteV1 *route,
							  const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	(void)arg;
	(void)route;
	(void)edge;
	(void)block;
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
cold_preflight_rebuildable_component(void *arg, const RfOpcodeRouteV1 *route,
									 const RfPageVersionEdgeEntryV1 *edge,
									 const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC && edge != NULL
				   && block != NULL && edge->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
}

/* Commit/abort that drops relations is a lifecycle change; prepared
 * transactions are outside the supported profile. */
static ClusterColdDetailV1
cold_xact_flags(XLogReaderState *reader, uint8 *flags)
{
	uint8 info = XLogRecGetInfo(reader);

	switch (info & XLOG_XACT_OPMASK) {
	case XLOG_XACT_COMMIT: {
		xl_xact_parsed_commit parsed;

		if (!ParseCommitRecord(info, (xl_xact_commit *)XLogRecGetData(reader),
							   XLogRecGetDataLen(reader), &parsed))
			return CLUSTER_COLD_COMPONENT_INVALID;
		*flags = parsed.nrels > 0 ? CLUSTER_COLD_RECORD_STRUCTURAL : 0;
		return CLUSTER_COLD_OK;
	}
	case XLOG_XACT_ABORT: {
		xl_xact_parsed_abort parsed;

		if (XLogRecGetDataLen(reader) < MinSizeOfXactAbort)
			return CLUSTER_COLD_COMPONENT_INVALID;
		ParseAbortRecord(info, (xl_xact_abort *)XLogRecGetData(reader), &parsed);
		*flags = parsed.nrels > 0 ? CLUSTER_COLD_RECORD_STRUCTURAL : 0;
		return CLUSTER_COLD_OK;
	}
	case XLOG_XACT_PREPARE:
	case XLOG_XACT_COMMIT_PREPARED:
	case XLOG_XACT_ABORT_PREPARED:
		*flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
		return CLUSTER_COLD_OK;
	default:
		*flags = 0;
		return CLUSTER_COLD_OK;
	}
}

static ClusterColdDetailV1
cold_record_flags(XLogReaderState *reader, uint8 *flags)
{
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	*flags = 0;
	switch (XLogRecGetRmid(reader)) {
	case RM_XACT_ID:
		return cold_xact_flags(reader, flags);
	case RM_SMGR_ID:
		if (info == XLOG_SMGR_TRUNCATE || info == XLOG_SMGR_SPACE_IDENTITY)
			*flags = CLUSTER_COLD_RECORD_STRUCTURAL;
		return CLUSTER_COLD_OK;
	case RM_HEAP2_ID:
		if ((info & XLOG_HEAP_OPMASK) == XLOG_HEAP2_REWRITE)
			*flags = CLUSTER_COLD_RECORD_STRUCTURAL;
		return CLUSTER_COLD_OK;
	case RM_DBASE_ID:
	case RM_TBLSPC_ID:
	case RM_RELMAP_ID:
	case RM_REPLORIGIN_ID:
		*flags = CLUSTER_COLD_RECORD_STRUCTURAL;
		return CLUSTER_COLD_OK;
	default:
		return CLUSTER_COLD_OK;
	}
}

static ClusterColdDetailV1
cold_map_components(XLogReaderState *reader, const RfDetachedRecordPlanV1 *plan,
					uint64 system_identifier, const uint8 storage_uuid[16],
					ClusterColdDecodedV1 *out)
{
	const DecodedXLogRecord *decoded = reader->record;
	uint16 count = 0;
	uint32 i;

	if (plan->component_count > CLUSTER_COLD_MAX_COMPONENTS)
		return CLUSTER_COLD_COMPONENT_INVALID;
	for (i = 0; i < plan->component_count; i++) {
		const RfDetachedComponentPlanV1 *source = &plan->components[i];
		const DecodedBkpBlock *block;
		ClusterColdComponentV1 *component;

		if (source->owner == RF_DETACHED_COMPONENT_REBUILDABLE)
			continue; /* rebuildable FSM: no version duty */
		if (source->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
			|| source->page_class != RF_PAGE_CLASS_ORDINARY)
			return CLUSTER_COLD_OPCODE_UNSUPPORTED;
		if (decoded->max_block_id < 0 || source->block_id > decoded->max_block_id
			|| !decoded->blocks[source->block_id].in_use)
			return CLUSTER_COLD_COMPONENT_INVALID;
		block = &decoded->blocks[source->block_id];
		component = &out->components[count++];
		memset(component, 0, sizeof(*component));
		component->page.system_identifier = system_identifier;
		memcpy(component->page.storage_uuid, storage_uuid, 16);
		component->page.locator = block->rlocator;
		component->page.forknum = (uint32)block->forknum;
		component->page.blockno = block->blkno;
		component->block_id = source->block_id;
		component->page_class = source->page_class;
		component->before_kind = source->before_kind;
		component->result_kind = source->result_kind;
		component->edge_flags = source->edge_flags;
		component->component_ordinal = source->component_ordinal;
		component->before = source->before;
		component->result = source->result;
	}
	out->record.component_count = count;
	out->record.components = count > 0 ? out->components : NULL;
	return CLUSTER_COLD_OK;
}

ClusterColdDetailV1
cluster_cold_recovery_decode_v1(struct XLogReaderState *reader, uint64 system_identifier,
								const uint8 storage_uuid[16], bool space_active,
								ClusterColdDecodedV1 *out)
{
	RfDetachedOwnerOpsV1 owner_ops;
	RfDetachedRecordPlanV1 plan;
	RfPageProofDetailV1 preflight;
	const DecodedXLogRecord *decoded;
	ClusterColdDetailV1 detail;

	if (out == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (reader == NULL || reader->record == NULL || storage_uuid == NULL || system_identifier == 0)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	decoded = reader->record;
	out->record.read_rec_ptr = reader->ReadRecPtr;
	out->record.end_rec_ptr = reader->EndRecPtr;
	out->record.scn = decoded->header.xl_scn;
	out->record.record_crc = (uint32)decoded->header.xl_crc;
	out->record.rmid = decoded->header.xl_rmid;
	out->record.info = decoded->header.xl_info;

	memset(&owner_ops, 0, sizeof(owner_ops));
	owner_ops.preflight_side_record = cold_preflight_side_record;
	owner_ops.preflight_side_component = cold_preflight_side_component;
	owner_ops.preflight_rebuildable_component = cold_preflight_rebuildable_component;
	memset(&plan, 0, sizeof(plan));
	preflight = rf_page_detached_preflight_v1(reader, space_active, &owner_ops, &plan);
	if (preflight != RF_PAGE_PROOF_DETAIL_OK) {
		out->route_detail = (uint8)preflight;
		return CLUSTER_COLD_OPCODE_UNSUPPORTED;
	}
	out->route_owner = plan.route.record_owner;
	detail = cold_record_flags(reader, &out->record.record_flags);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	return cold_map_components(reader, &plan, system_identifier, storage_uuid, out);
}

#endif /* USE_PGRAC_CLUSTER */
