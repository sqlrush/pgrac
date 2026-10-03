/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_decode.c
 *	  Map decoded native WAL records to typed cold replay plan input.
 *
 *	  Classification uses the same closed route registry and detached page
 *	  preflight as online thread recovery: one opcode table and one page
 *	  component codec.  The cold owner policy is narrower than online
 *	  recovery's typed side consumers: page records route to the plan; SPACE
 *	  identity and reservation changes and a commit's relation drops become
 *	  typed SPACE effects for the SPACE owner; the founder's own non-page
 *	  records keep their native redo owner, as does relation file creation;
 *	  another generation's other non-page records, the remaining relation
 *	  lifecycle and prepared-transaction records are flagged so the plan
 *	  refuses them after the native redo start.  Routed side components
 *	  inside page records are refused until their typed cold owner exists.
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
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_space_reservation.h"
#include "replication/message.h"
#include "storage/standbydefs.h"
#include "utils/memutils.h"

#ifdef USE_CLUSTER_UNIT
#define cold_ops_alloc(pointer_, size_) realloc((pointer_), (size_))
#define cold_ops_free(pointer_) free(pointer_)
#else
#define cold_ops_alloc(pointer_, size_)                                                            \
	((pointer_) == NULL                                                                            \
		 ? palloc_extended((size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM)                           \
		 : repalloc_extended((pointer_), (size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM))
#define cold_ops_free(pointer_) pfree(pointer_)
#endif

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

/* Room for count SPACE effects, zeroed and attached to the record. */
static ClusterColdSpaceOpV1 *
cold_space_reserve(ClusterColdDecodedV1 *out, uint32 count)
{
	if (count > out->space_capacity) {
		void *grown;

		if ((Size)count > MaxAllocHugeSize / sizeof(ClusterColdSpaceOpV1))
			return NULL;
		grown = cold_ops_alloc(out->space_ops, (Size)count * sizeof(ClusterColdSpaceOpV1));
		if (grown == NULL)
			return NULL;
		out->space_ops = (ClusterColdSpaceOpV1 *)grown;
		out->space_capacity = count;
	}
	memset(out->space_ops, 0, (Size)count * sizeof(ClusterColdSpaceOpV1));
	out->record.space_count = count;
	out->record.space_ops = out->space_ops;
	return out->space_ops;
}

static bool
cold_space_key_matches(const ClusterSpaceIdentityKey *key, uint64 system_identifier,
					   const uint8 storage_uuid[16])
{
	return key->system_identifier == system_identifier
		   && memcmp(key->storage_uuid, storage_uuid, 16) == 0
		   && key->locator.relNumber != InvalidRelFileNumber;
}

/* A commit's relation drops: one SPACE tombstone per cluster relation. */
static ClusterColdDetailV1
cold_space_drops(const xl_xact_parsed_commit *parsed, uint64 system_identifier,
				 const uint8 storage_uuid[16], ClusterColdDecodedV1 *out)
{
	ClusterColdSpaceOpV1 *ops;
	uint32 i;

	if (parsed->nspace_drops > (uint32)Max(parsed->nrels, 0) || parsed->space_drops == NULL)
		return CLUSTER_COLD_SPACE_INVALID;
	ops = cold_space_reserve(out, parsed->nspace_drops);
	if (ops == NULL)
		return CLUSTER_COLD_OOM;
	for (i = 0; i < parsed->nspace_drops; i++) {
		const char *bytes = parsed->space_drops + (Size)i * CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
		ClusterSpaceStructureChange change;

		if (!cluster_space_structure_wal_decode(bytes, CLUSTER_SPACE_STRUCTURE_WAL_BYTES, &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE
			|| !cold_space_key_matches(&change.identity.result.key, system_identifier,
									   storage_uuid))
			return CLUSTER_COLD_SPACE_INVALID;
		ops[i].kind = CLUSTER_COLD_SPACE_DROP;
		ops[i].locator = change.identity.result.key.locator;
		memcpy(ops[i].before_incarnation, change.identity.expected.incarnation, 16);
		ops[i].payload = bytes;
		ops[i].payload_length = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	}
	return CLUSTER_COLD_OK;
}

/*
 * A commit's relation drops become SPACE effects; the native commit redo
 * still owns the files.  Relations dropped by an abort have no SPACE
 * tombstone and remain a lifecycle change; prepared transactions are
 * outside the supported profile.
 */
static ClusterColdDetailV1
cold_xact_flags(XLogReaderState *reader, uint64 system_identifier, const uint8 storage_uuid[16],
				ClusterColdDecodedV1 *out, uint8 *flags)
{
	uint8 info = XLogRecGetInfo(reader);

	switch (info & XLOG_XACT_OPMASK) {
	case XLOG_XACT_COMMIT: {
		xl_xact_parsed_commit parsed;

		if (!ParseCommitRecord(info, (xl_xact_commit *)XLogRecGetData(reader),
							   XLogRecGetDataLen(reader), &parsed))
			return CLUSTER_COLD_COMPONENT_INVALID;
		*flags = 0;
		return parsed.nspace_drops > 0
				   ? cold_space_drops(&parsed, system_identifier, storage_uuid, out)
				   : CLUSTER_COLD_OK;
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

/* SMGR SPACE_IDENTITY: a CREATE or TRUNCATE.  A tombstone travels only in
 * its commit. */
static ClusterColdDetailV1
cold_space_identity(XLogReaderState *reader, uint64 system_identifier, const uint8 storage_uuid[16],
					ClusterColdDecodedV1 *out)
{
	ClusterSpaceStructureChange change;
	ClusterColdSpaceOpV1 *op;

	if (!cluster_space_structure_wal_decode(XLogRecGetData(reader), XLogRecGetDataLen(reader),
											&change)
		|| (change.identity.action != CLUSTER_SPACE_WAL_CREATE
			&& change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE)
		|| !cold_space_key_matches(&change.identity.result.key, system_identifier, storage_uuid))
		return CLUSTER_COLD_SPACE_INVALID;
	op = cold_space_reserve(out, 1);
	if (op == NULL)
		return CLUSTER_COLD_OOM;
	op->locator = change.identity.result.key.locator;
	op->nblocks = change.identity.nblocks;
	memcpy(op->result_incarnation, change.identity.result.incarnation, 16);
	if (change.identity.action == CLUSTER_SPACE_WAL_CREATE)
		op->kind = CLUSTER_COLD_SPACE_CREATE;
	else {
		op->kind = CLUSTER_COLD_SPACE_TRUNCATE;
		memcpy(op->before_incarnation, change.identity.expected.incarnation, 16);
	}
	op->payload = XLogRecGetData(reader);
	op->payload_length = XLogRecGetDataLen(reader);
	return CLUSTER_COLD_OK;
}

/* SMGR SPACE_RESERVATION: only an ADVANCE stands alone. */
static ClusterColdDetailV1
cold_space_reservation(XLogReaderState *reader, uint64 system_identifier,
					   const uint8 storage_uuid[16], ClusterColdDecodedV1 *out)
{
	ClusterSpaceReservationChange change;
	ClusterColdSpaceOpV1 *op;

	if (!cluster_space_reservation_wal_decode(XLogRecGetData(reader), XLogRecGetDataLen(reader),
											  &change)
		|| change.action != CLUSTER_SPACE_RESERVATION_ADVANCE
		|| !cold_space_key_matches(&change.result.identity.key, system_identifier, storage_uuid))
		return CLUSTER_COLD_SPACE_INVALID;
	op = cold_space_reserve(out, 1);
	if (op == NULL)
		return CLUSTER_COLD_OOM;
	op->kind = CLUSTER_COLD_SPACE_ADVANCE;
	op->locator = change.result.identity.key.locator;
	memcpy(op->result_incarnation, change.result.identity.incarnation, 16);
	op->payload = XLogRecGetData(reader);
	op->payload_length = XLogRecGetDataLen(reader);
	return CLUSTER_COLD_OK;
}

static ClusterColdDetailV1
cold_record_flags(XLogReaderState *reader, uint64 system_identifier, const uint8 storage_uuid[16],
				  ClusterColdDecodedV1 *out, uint8 *flags)
{
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	*flags = 0;
	switch (XLogRecGetRmid(reader)) {
	case RM_XACT_ID:
		return cold_xact_flags(reader, system_identifier, storage_uuid, out, flags);
	case RM_SMGR_ID:
		if (info == XLOG_SMGR_SPACE_IDENTITY)
			return cold_space_identity(reader, system_identifier, storage_uuid, out);
		if (info == XLOG_SMGR_SPACE_RESERVATION)
			return cold_space_reservation(reader, system_identifier, storage_uuid, out);
		/* A truncation without a SPACE identity change has no cold owner. */
		if (info == XLOG_SMGR_TRUNCATE)
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
	case RM_CLUSTER_XID_STRIPE_ID:
		/* Stripe JOIN/RETIRE are cluster-wide facts whose order across
		 * generations a per-participant drain cannot keep. */
		*flags = CLUSTER_COLD_RECORD_STRUCTURAL;
		return CLUSTER_COLD_OK;
	default:
		return CLUSTER_COLD_OK;
	}
}

/*
 * Another generation's native control record, in its native shape.  Crash
 * recovery gives these no effect on the founder: checkpoints, NEXTOID,
 * parameter, FPW and timeline records describe that writer's own control
 * file and counters (the founder's come from its own stream; XIDs and
 * MultiXacts are striped per instance and OIDs leased cluster-wide), standby
 * records act only during hot standby, a logical message only for logical
 * decoding, and switch, no-op, backup-end, restore-point and contrecord
 * records carry nothing to apply.
 */
static bool
cold_foreign_control(XLogReaderState *reader, const RfDetachedRecordPlanV1 *plan)
{
	uint32 length = XLogRecGetDataLen(reader);
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (XLogRecMaxBlockId(reader) >= 0 || (XLogRecGetInfo(reader) & XLR_INFO_MASK) != 0)
		return false;
	if (plan->route.record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
		return XLogRecGetRmid(reader) == RM_LOGICALMSG_ID && info == XLOG_LOGICAL_MESSAGE;
	if (plan->route.record_owner != RF_ROUTE_OWNER_SIDE_TYPED)
		return false;
	if (XLogRecGetRmid(reader) == RM_STANDBY_ID)
		return info == XLOG_STANDBY_LOCK || info == XLOG_RUNNING_XACTS
			   || info == XLOG_INVALIDATIONS;
	if (XLogRecGetRmid(reader) != RM_XLOG_ID)
		return false;
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
		return length == sizeof(CheckPoint);
	case XLOG_NEXTOID:
		return length == sizeof(Oid);
	case XLOG_PARAMETER_CHANGE:
		return length == sizeof(xl_parameter_change);
	case XLOG_FPW_CHANGE:
		return length == sizeof(bool);
	case XLOG_SWITCH:
		return length == 0;
	case XLOG_NOOP:
		return true;
	case XLOG_BACKUP_END:
		return length == sizeof(XLogRecPtr);
	case XLOG_RESTORE_POINT:
		return length == sizeof(xl_restore_point);
	case XLOG_END_OF_RECOVERY:
		return length == sizeof(xl_end_of_recovery);
	case XLOG_OVERWRITE_CONTRECORD:
		return length == sizeof(xl_overwrite_contrecord);
	default:
		return false;
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
								const uint8 storage_uuid[16], bool space_active, bool foreign,
								ClusterColdDecodedV1 *out)
{
	RfDetachedOwnerOpsV1 owner_ops;
	RfDetachedRecordPlanV1 plan;
	RfPageProofDetailV1 preflight;
	const DecodedXLogRecord *decoded;
	ClusterColdDetailV1 detail;
	ClusterColdSpaceOpV1 *space_ops;
	uint32 space_capacity;
	bool has_owner;

	if (out == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	space_ops = out->space_ops;
	space_capacity = out->space_capacity;
	memset(out, 0, sizeof(*out));
	out->space_ops = space_ops;
	out->space_capacity = space_capacity;
	if (reader == NULL || reader->record == NULL || storage_uuid == NULL || system_identifier == 0)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	decoded = reader->record;
	out->record.read_rec_ptr = reader->ReadRecPtr;
	out->record.end_rec_ptr = reader->EndRecPtr;
	out->record.prev_rec_ptr = decoded->header.xl_prev;
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
	detail = cold_record_flags(reader, system_identifier, storage_uuid, out,
							   &out->record.record_flags);
	if (detail != CLUSTER_COLD_OK)
		return detail;

	/*
	 * Another generation's side effects (outcomes, undo, SLRU) have no typed
	 * cold owner yet; never a silent no-op.  Its SPACE changes have the SPACE
	 * owner, and creating a relation file is idempotent, so their native redo
	 * is the owner's.  Its native control records are typed no-ops.
	 */
	has_owner = XLogRecGetRmid(reader) == RM_SMGR_ID
				&& ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE
					|| out->record.space_count > 0);
	if (foreign && plan.route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC && !has_owner)
		out->record.record_flags |= cold_foreign_control(reader, &plan)
										? CLUSTER_COLD_RECORD_FOREIGN_CONTROL
										: CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	return cold_map_components(reader, &plan, system_identifier, storage_uuid, out);
}

void
cluster_cold_decoded_release_v1(ClusterColdDecodedV1 *decoded)
{
	if (decoded == NULL)
		return;
	if (decoded->space_ops != NULL)
		cold_ops_free(decoded->space_ops);
	decoded->space_ops = NULL;
	decoded->space_capacity = 0;
	decoded->record.space_ops = NULL;
	decoded->record.space_count = 0;
}

#endif /* USE_PGRAC_CLUSTER */
