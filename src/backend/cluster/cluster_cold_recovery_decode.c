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
 *	  record-local foreign native effects use their original-origin adapters;
 *	  the other non-page records, remaining relation lifecycle and
 *	  prepared-transaction records are flagged so the plan
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
#include "access/clog.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "replication/message.h"
#include "replication/origin.h"
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

/* All elements belong to the native consumer. Validate the full array
 * before exporting even its family: a short record is never an empty batch. */
static bool
cold_counted_shape(XLogReaderState *reader, Size base, Size count_offset, Size item_size)
{
	uint32 len = XLogRecGetDataLen(reader);
	const char *data = XLogRecGetData(reader);
	int count;

	if (data == NULL || len < base || base < sizeof(count) || count_offset > base - sizeof(count))
		return false;
	memcpy(&count, data + count_offset, sizeof(count));
	return count >= 0 && (Size)count <= (len - base) / item_size
		   && base + (Size)count * item_size == len;
}

static uint32
cold_completion_effects(uint32 xinfo, int children, int relations, int stats, int messages)
{
	uint32 effects = CLUSTER_COLD_EFFECT_TERMINAL;

	if (children > 0)
		effects |= CLUSTER_COLD_EFFECT_CHILDREN;
	if (relations > 0)
		effects |= CLUSTER_COLD_EFFECT_RELATIONS;
	if (stats > 0)
		effects |= CLUSTER_COLD_EFFECT_STATS;
	if (messages > 0)
		effects |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
	if (xinfo & XACT_XINFO_HAS_TT_COMMIT)
		effects |= CLUSTER_COLD_EFFECT_TT;
	if (xinfo & XACT_XINFO_HAS_SCN)
		effects |= CLUSTER_COLD_EFFECT_SCN;
	if (xinfo & XACT_XINFO_HAS_ORIGIN)
		effects |= CLUSTER_COLD_EFFECT_ORIGIN;
	if (xinfo & XACT_XINFO_HAS_AE_LOCKS)
		effects |= CLUSTER_COLD_EFFECT_LOCKS;
	if (xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE)
		effects |= CLUSTER_COLD_EFFECT_RELCACHE;
	if (xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT)
		effects |= CLUSTER_COLD_EFFECT_SYNC;
	if (xinfo & XACT_COMPLETION_APPLY_FEEDBACK)
		effects |= CLUSTER_COLD_EFFECT_FEEDBACK;
	return effects;
}

/*
 * A commit's relation drops become SPACE effects; the native commit redo
 * still owns the files.  Relations dropped by an abort have no SPACE
 * tombstone and remain a lifecycle change; prepared transactions are
 * outside the supported profile.
 */
static ClusterColdDetailV1
cold_xact_flags(XLogReaderState *reader, uint64 system_identifier, const uint8 storage_uuid[16],
				ClusterColdDecodedV1 *out, uint8 *flags, uint32 *effects)
{
	uint8 info = XLogRecGetInfo(reader);

	switch (info & XLOG_XACT_OPMASK) {
	case XLOG_XACT_COMMIT: {
		xl_xact_parsed_commit parsed;

		if (!ParseCommitRecord(info, (xl_xact_commit *)XLogRecGetData(reader),
							   XLogRecGetDataLen(reader), &parsed))
			return CLUSTER_COLD_COMPONENT_INVALID;
		/* Native parsing bounds the delta but does not validate its version.
		 * An unknown delta must not reach the original exact-slot writer. */
		if (parsed.has_tt_commit
			&& (parsed.tt_commit.format_version != CLUSTER_XACT_TT_COMMIT_VERSION
				|| parsed.tt_commit.flags != 0 || parsed.tt_commit.reserved != 0))
			return CLUSTER_COLD_OPCODE_UNSUPPORTED;
		*flags = 0;
		*effects |= cold_completion_effects(parsed.xinfo, parsed.nsubxacts, parsed.nrels,
											parsed.nstats, parsed.nmsgs);
		/* The opcode alone cannot hide a prepared transaction subrecord. */
		if (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE)
			*flags |= CLUSTER_COLD_RECORD_UNSUPPORTED;
		return parsed.nspace_drops > 0
				   ? cold_space_drops(&parsed, system_identifier, storage_uuid, out)
				   : CLUSTER_COLD_OK;
	}
	case XLOG_XACT_ABORT: {
		xl_xact_parsed_abort parsed;

		/* ParseAbortRecord trusts the xinfo it reads; check every section
		 * lies within the data first (a damaged record is refused). */
		if (!rf_side_xact_completion_shape_v1(reader, false))
			return CLUSTER_COLD_COMPONENT_INVALID;
		ParseAbortRecord(info, (xl_xact_abort *)XLogRecGetData(reader), &parsed);
		*flags = parsed.nrels > 0 ? CLUSTER_COLD_RECORD_STRUCTURAL : 0;
		*effects |= cold_completion_effects(parsed.xinfo, parsed.nsubxacts, parsed.nrels,
											parsed.nstats, 0);
		if (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE)
			*flags |= CLUSTER_COLD_RECORD_UNSUPPORTED;
		return CLUSTER_COLD_OK;
	}
	case XLOG_XACT_ASSIGNMENT:
		/* Native/history acceptance must validate the original top/child
		 * set itself; a rejected optional SIDE capture cannot do it later. */
		if (!rf_side_xact_assignment_preflight_v1((const uint8 *)XLogRecGetData(reader),
												  XLogRecGetDataLen(reader)))
			return CLUSTER_COLD_COMPONENT_INVALID;
		*effects |= CLUSTER_COLD_EFFECT_CHILDREN;
		return CLUSTER_COLD_OK;
	case XLOG_XACT_INVALIDATIONS:
		if (!cold_counted_shape(reader, MinSizeOfXactInvals, offsetof(xl_xact_invals, nmsgs),
								sizeof(SharedInvalidationMessage)))
			return CLUSTER_COLD_COMPONENT_INVALID;
		*effects |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
		return CLUSTER_COLD_OK;
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

/* LogLogicalMessage writes a native header, a strlen(prefix)+1 C string,
 * then exactly message_size arbitrary bytes. Physical redo is a no-op,
 * but that does not qualify incomplete bytes in either retained stream. */
static bool
cold_logical_message_shape(XLogReaderState *reader)
{
	const char *data = XLogRecGetData(reader);
	uint32 length = XLogRecGetDataLen(reader);
	Size prefix_size, message_size, remaining;

	if (XLogRecGetInfo(reader) != XLOG_LOGICAL_MESSAGE || XLogRecHasAnyBlockRefs(reader)
		|| data == NULL || length < SizeOfLogicalMessage)
		return false;
	/* Do not read an invalid native bool representation as bool. */
	if ((unsigned char)data[offsetof(xl_logical_message, transactional)] > 1)
		return false;
	memcpy(&prefix_size, data + offsetof(xl_logical_message, prefix_size), sizeof(prefix_size));
	memcpy(&message_size, data + offsetof(xl_logical_message, message_size), sizeof(message_size));
	remaining = length - SizeOfLogicalMessage;
	if (prefix_size == 0 || prefix_size > remaining || message_size != remaining - prefix_size)
		return false;
	return memchr(data + SizeOfLogicalMessage, '\0', prefix_size)
		   == data + SizeOfLogicalMessage + prefix_size - 1;
}

/* Native control records need the same complete frame before either native
 * redo or a foreign no-effect route. Padding and parameter values retain
 * their native meaning; inspect bool bytes without loading invalid bools. */
static bool
cold_xlog_control_shape(XLogReaderState *reader)
{
	const char *data = XLogRecGetData(reader);
	uint32 length = XLogRecGetDataLen(reader);
	uint8 info = XLogRecGetInfo(reader);

	if ((info & XLR_INFO_MASK) != 0 || XLogRecHasAnyBlockRefs(reader)
		|| (length != 0 && data == NULL))
		return false;
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
		return length == sizeof(CheckPoint)
			   && (unsigned char)data[offsetof(CheckPoint, fullPageWrites)] <= 1;
	case XLOG_NEXTOID:
		return length == sizeof(Oid);
	case XLOG_PARAMETER_CHANGE:
		return length == sizeof(xl_parameter_change)
			   && (unsigned char)data[offsetof(xl_parameter_change, wal_log_hints)] <= 1
			   && (unsigned char)data[offsetof(xl_parameter_change, track_commit_timestamp)] <= 1;
	case XLOG_FPW_CHANGE:
		return length == sizeof(bool) && (unsigned char)data[0] <= 1;
	case XLOG_SWITCH:
		return length == 0;
	case XLOG_NOOP:
		return true;
	case XLOG_BACKUP_END:
		return length == sizeof(XLogRecPtr);
	case XLOG_RESTORE_POINT:
		return length == sizeof(xl_restore_point)
			   && memchr(data + offsetof(xl_restore_point, rp_name), '\0', MAXFNAMELEN) != NULL;
	case XLOG_END_OF_RECOVERY:
		return length == sizeof(xl_end_of_recovery);
	case XLOG_OVERWRITE_CONTRECORD:
		return length == sizeof(xl_overwrite_contrecord);
	default:
		return false;
	}
}

/* Native smgr_redo and the touched-file collector read these fixed structs
 * directly. Check their complete carrier before either can see an effect,
 * including our own source and records retained before its redo cut. */
static bool
cold_smgr_native_shape(XLogReaderState *reader)
{
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	RelFileLocator locator;

	if (XLogRecGetData(reader) == NULL || XLogRecHasAnyBlockRefs(reader)
		|| (XLogRecGetInfo(reader) & ~XLR_CHECK_CONSISTENCY) != (info | XLR_SPECIAL_REL_UPDATE))
		return false;
	if (info == XLOG_SMGR_CREATE) {
		xl_smgr_create create;

		if (XLogRecGetDataLen(reader) != sizeof(create))
			return false;
		memcpy(&create, XLogRecGetData(reader), sizeof(create));
		if (create.forkNum < MAIN_FORKNUM || create.forkNum > MAX_FORKNUM)
			return false;
		locator = create.rlocator;
	} else if (info == XLOG_SMGR_TRUNCATE) {
		xl_smgr_truncate truncate;

		if (XLogRecGetDataLen(reader) != sizeof(truncate))
			return false;
		memcpy(&truncate, XLogRecGetData(reader), sizeof(truncate));
		if (truncate.blkno == InvalidBlockNumber || truncate.flags == 0
			|| (truncate.flags & ~SMGR_TRUNCATE_ALL) != 0)
			return false;
		locator = truncate.rlocator;
	} else
		return false;
	return OidIsValid(locator.spcOid) && locator.relNumber != InvalidRelFileNumber
		   && ((locator.spcOid == GLOBALTABLESPACE_OID) == (locator.dbOid == InvalidOid));
}

static ClusterColdDetailV1
cold_record_flags(XLogReaderState *reader, uint64 system_identifier, const uint8 storage_uuid[16],
				  bool foreign, ClusterColdDecodedV1 *out, uint8 *flags, uint32 *effects)
{
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	*flags = 0;
	switch (XLogRecGetRmid(reader)) {
	case RM_XLOG_ID:
		/* Page/FPI records retain the detached page preflight below. */
		if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT)
			return CLUSTER_COLD_OK;
		return cold_xlog_control_shape(reader) ? CLUSTER_COLD_OK : CLUSTER_COLD_COMPONENT_INVALID;
	case RM_XACT_ID:
		return cold_xact_flags(reader, system_identifier, storage_uuid, out, flags, effects);
	case RM_CLUSTER_UNDO_ID:
		if (info == XLOG_UNDO_TT_SLOT_COMMIT || info == XLOG_UNDO_TT_SLOT_SET_HEAD
			|| info == XLOG_UNDO_TT_SLOT_ABORT) {
			ClusterUndoDecoded undo;

			if (!cluster_undo_decode(reader, &undo) || !cluster_undo_preflight(&undo))
				return CLUSTER_COLD_COMPONENT_INVALID;
			/* Prepared resolution can emit these standalone carriers before
			 * its XACT terminal. Refuse them even in retained history: a
			 * checkpoint is not a complete prepared-transaction consumer.
			 * Ordinary abort has its separate exact-generation carrier. */
			if (info != XLOG_UNDO_TT_SLOT_ABORT
				|| undo.format_version != CLUSTER_UNDO_TT_ABORT_EXACT_VERSION)
				*flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
		} else if (info == XLOG_UNDO_RECOVERY_TT_ABORT) {
			ClusterRecoveryTTAbortV1 c0;

			if (XLogRecGetInfo(reader) != XLOG_UNDO_RECOVERY_TT_ABORT
				|| XLogRecHasAnyBlockRefs(reader) || TransactionIdIsValid(XLogRecGetXid(reader))
				|| TransactionIdIsValid(reader->record->toplevel_xid)
				|| XLogRecGetOrigin(reader) != InvalidRepOriginId
				|| !cluster_recovery_tt_abort_decode_v1((const uint8 *)XLogRecGetData(reader),
														XLogRecGetDataLen(reader), &c0)
				|| c0.writer_thread != reader->cluster_expected_thread_id)
				return CLUSTER_COLD_COMPONENT_INVALID;
			/* Only the original foreign dispatch has a target consumer. It
			 * still binds both retained sources and the exact target authority;
			 * own native redo cannot apply this record or skip its history. */
			*effects |= CLUSTER_COLD_EFFECT_TT | CLUSTER_COLD_EFFECT_TERMINAL;
			*flags = foreign ? 0 : CLUSTER_COLD_RECORD_UNSUPPORTED;
		}
		return CLUSTER_COLD_OK;
	case RM_COMMIT_TS_ID:
		/* Shared commit_ts has no recovery consumer, including own history. */
		*flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
		return CLUSTER_COLD_OK;
	case RM_LOGICALMSG_ID:
		return cold_logical_message_shape(reader) ? CLUSTER_COLD_OK
												  : CLUSTER_COLD_COMPONENT_INVALID;
	case RM_STANDBY_ID:
		/* A primary-context no-effect route still needs complete native
		 * bytes, including retained history and our own source. */
		if (!rf_side_online_standby_record_shape_v1(reader))
			return CLUSTER_COLD_COMPONENT_INVALID;
		if (info == XLOG_INVALIDATIONS) {
			xl_invalidations invalidations;

			memcpy(&invalidations, XLogRecGetData(reader), MinSizeOfInvalidations);
			*effects |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
			if (invalidations.relcacheInitFileInval)
				*effects |= CLUSTER_COLD_EFFECT_RELCACHE;
		}
		return CLUSTER_COLD_OK;
	case RM_SMGR_ID:
		if (info == XLOG_SMGR_SPACE_IDENTITY)
			return cold_space_identity(reader, system_identifier, storage_uuid, out);
		if (info == XLOG_SMGR_SPACE_RESERVATION)
			return cold_space_reservation(reader, system_identifier, storage_uuid, out);
		if (!cold_smgr_native_shape(reader))
			return CLUSTER_COLD_COMPONENT_INVALID;
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
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (XLogRecMaxBlockId(reader) >= 0 || (XLogRecGetInfo(reader) & XLR_INFO_MASK) != 0)
		return false;
	if (plan->route.record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
		return XLogRecGetRmid(reader) == RM_LOGICALMSG_ID && info == XLOG_LOGICAL_MESSAGE;
	if (plan->route.record_owner != RF_ROUTE_OWNER_SIDE_TYPED)
		return false;
	/* xact_redo ignores the logical decoder's command-level marker.
	 * COMMIT and standby invalidations retain their actual consumers. */
	if (XLogRecGetRmid(reader) == RM_XACT_ID)
		return plan->route.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD
			   && info == XLOG_XACT_INVALIDATIONS
			   && cold_counted_shape(reader, MinSizeOfXactInvals, offsetof(xl_xact_invals, nmsgs),
									 sizeof(SharedInvalidationMessage));
	if (XLogRecGetRmid(reader) == RM_STANDBY_ID)
		/* INVALIDATIONS includes persistent relcache-init work. A foreign
		 * adapter must perform it; a fresh process is not its completion. */
		return info == XLOG_STANDBY_LOCK || info == XLOG_RUNNING_XACTS;
	return XLogRecGetRmid(reader) == RM_XLOG_ID && cold_xlog_control_shape(reader);
}

/* Only existing record-local native consumers may enter this route. The
 * immutable SIDE capture and whole-source consumer preflight still validate
 * the complete payload and original-origin storage before any mutation. */
static bool
cold_foreign_side(XLogReaderState *reader, const RfDetachedRecordPlanV1 *plan,
				  uint64 system_identifier)
{
	uint8 info = XLogRecGetRmid(reader) == RM_XACT_ID
					 ? XLogRecGetInfo(reader) & XLOG_XACT_OPMASK
					 : XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (XLogRecMaxBlockId(reader) >= 0
		|| ((XLogRecGetInfo(reader) & XLR_INFO_MASK) != 0
			&& !(XLogRecGetRmid(reader) == RM_XACT_ID && info == XLOG_XACT_COMMIT
				 && (XLogRecGetInfo(reader) & XLR_INFO_MASK) == XLR_SPECIAL_REL_UPDATE))
		|| plan->route.record_owner != RF_ROUTE_OWNER_SIDE_TYPED)
		return false;
	if (XLogRecGetRmid(reader) == RM_CLUSTER_UNDO_ID) {
		ClusterUndoDecoded undo;

		/* The complete C0 envelope and payload were checked by
		 * cold_record_flags. Its dual-source consumer is separate from the
		 * ordinary original-origin undo decoder below. */
		if (info == XLOG_UNDO_RECOVERY_TT_ABORT)
			return plan->route.codec_id == RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO;
		/* Use only connected record-local consumers. Exact ABORT restores
		 * its header; legacy terminals cannot borrow the whole-source owner. */
		return plan->route.codec_id == RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO
			   && (info == XLOG_UNDO_BLOCK_WRITE || info == XLOG_UNDO_BLOCK_WRITE_MULTI
				   || info == XLOG_UNDO_SEGMENT_INIT || info == XLOG_UNDO_SEGMENT_REUSE
				   || info == XLOG_UNDO_SEGMENT_RECYCLE || info == XLOG_UNDO_TT_SLOT_BIND
				   || info == XLOG_UNDO_TT_SLOT_SET_HEAD || info == XLOG_UNDO_TT_SLOT_ABORT
				   || info == XLOG_UNDO_TT_SLOT_CTRC_RELEASE)
			   && XLogRecGetData(reader) != NULL && cluster_undo_decode(reader, &undo)
			   && cluster_undo_preflight(&undo)
			   && (info != XLOG_UNDO_TT_SLOT_ABORT
				   || undo.format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION);
	}
	if (plan->route.codec_id != RF_ROUTE_CODEC_SIDE_STANDARD)
		return false;
	switch (XLogRecGetRmid(reader)) {
	case RM_CLOG_ID:
		return (info == CLOG_ZEROPAGE || info == CLOG_TRUNCATE)
			   && rf_side_online_projection_record_shape_v1(reader);
	case RM_MULTIXACT_ID:
		return (info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE
				|| info == XLOG_MULTIXACT_CREATE_ID || info == XLOG_MULTIXACT_TRUNCATE_ID)
			   && rf_side_online_projection_record_shape_v1(reader);
	case RM_XACT_ID:
		/* Ordinary COMMIT and ABORT are record-local native consumers only
		 * after the recovery decoder has validated the complete payload. A
		 * relation/SPACE suffix remains with the structural owner; prepared
		 * transaction opcodes never enter this route. */
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_ABORT) {
			RfSideXactOperationV1 operation;
			uint16 origin_thread = reader->cluster_expected_thread_id;

			/* Classification must not turn malformed/unsupported effects into
			 * a foreign consumer. The cold reader binds the source thread before
			 * decoding; use that identity for TT validation instead of assuming
			 * the first WAL thread. The immutable plan still revalidates it later. */
			return rf_side_xact_decode_recovery_v1(reader, system_identifier, origin_thread,
												   &operation)
				   && ((info == XLOG_XACT_COMMIT && operation.kind == RF_SIDE_XACT_COMMIT)
					   || (info == XLOG_XACT_ABORT && operation.kind == RF_SIDE_XACT_ABORT))
				   /* Strict SIDE may decode replication-origin metadata, but the
					* foreign cold owner has no consumer for that effect. */
				   && (operation.xinfo & XACT_XINFO_HAS_ORIGIN) == 0
				   && ((operation.relation_count == 0 && operation.space_drop_count == 0
						&& (XLogRecGetInfo(reader) & XLR_INFO_MASK) == 0)
					   || (info == XLOG_XACT_COMMIT && operation.relation_count > 0
						   && operation.space_drop_count > 0
						   && !reader->record->has_page_version_edge
						   && XLogRecGetOrigin(reader) == InvalidRepOriginId
						   && !TransactionIdIsValid(reader->record->toplevel_xid)));
		}
		return info == XLOG_XACT_ASSIGNMENT;
	case RM_STANDBY_ID:
		return info == XLOG_INVALIDATIONS;
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

/* Export the original per-record policy in both cold decoder passes.
 * Native XACT families are expanded above; other native records remain one
 * whole-record obligation. This summary does not run consumers or replace
 * the original record_flags gate or global side-owner readiness handshake. */
static void
cold_effect_routes(const RfDetachedRecordPlanV1 *plan, uint32 required, ClusterColdDecodedV1 *out)
{
	ClusterColdEffectRoutesV1 *routes = &out->effects;
	uint8 flags = out->record.record_flags;
	uint32 remaining;

	for (uint32 i = 0; i < plan->component_count; i++)
		required |= plan->components[i].owner == RF_DETACHED_COMPONENT_REBUILDABLE
						? CLUSTER_COLD_EFFECT_FSM
						: CLUSTER_COLD_EFFECT_PAGE;
	if (out->record.space_count > 0)
		required |= CLUSTER_COLD_EFFECT_SPACE;
	routes->required = required;
	if (flags & (CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_UNSUPPORTED)) {
		routes->refused = required;
		return;
	}
	routes->space_apply = required & CLUSTER_COLD_EFFECT_SPACE;
	/* A standalone SPACE record is entirely consumed by SPACE, never by
	 * native redo. A compound COMMIT still has its native terminal owner. */
	if (routes->space_apply != 0 && plan->route.rmid == RM_SMGR_ID)
		routes->space_apply |= CLUSTER_COLD_EFFECT_RECORD;
	remaining = required & ~routes->space_apply;
	if (flags & CLUSTER_COLD_RECORD_SIDE_UNOWNED)
		routes->refused = remaining;
	else if (flags & CLUSTER_COLD_RECORD_FOREIGN_CONTROL)
		routes->no_effect = remaining;
	else if (plan->route.record_owner == RF_ROUTE_OWNER_PAGE_CODEC)
		routes->page_apply = remaining;
	else
		routes->native_apply = remaining;
}

/* TransactionIdLatest's reverse scan and native wrap-aware comparison,
 * reading bounded children without assuming main-data alignment. Invalid
 * raw XIDs cannot furnish a counter input. This does not classify losers. */
static bool
cold_counter_latest(TransactionId xid, int count, const TransactionId *children,
					TransactionId *latest)
{
	if (!TransactionIdIsNormal(xid))
		return false;
	*latest = xid;
	while (--count >= 0) {
		TransactionId child;

		memcpy(&child, (const char *)children + (Size)count * sizeof(child), sizeof(child));
		if (!TransactionIdIsNormal(child))
			return false;
		if (NormalTransactionIdPrecedes(*latest, child))
			*latest = child;
	}
	return true;
}

/* Describe the original thread's native cold-replay counter effects. The
 * founder deliberately does not apply foreign inputs to its own allocator.
 * Record-local completeness is independent of effect ownership: an unowned
 * terminal's inputs never close its missing consumer or prove an endpoint. */
static void
cold_xid_input(XLogReaderState *reader, ClusterColdDecodedV1 *out)
{
	ClusterColdXidInputV1 input = { 0 };
	uint8 rmid = XLogRecGetRmid(reader);
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (out->record.record_flags
		& (CLUSTER_COLD_RECORD_UNSUPPORTED | CLUSTER_COLD_RECORD_STRUCTURAL))
		return;
	input.record_xid = XLogRecGetXid(reader);
	if (rmid == RM_XACT_ID && (info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT) {
		xl_xact_parsed_commit parsed;

		if (!ParseCommitRecord(XLogRecGetInfo(reader), (xl_xact_commit *)XLogRecGetData(reader),
							   XLogRecGetDataLen(reader), &parsed)
			|| !cold_counter_latest(input.record_xid, parsed.nsubxacts, parsed.subxacts,
									&input.payload_xid))
			return;
	} else if (rmid == RM_XACT_ID && (info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT) {
		xl_xact_parsed_abort parsed;

		/* cold_xact_flags has already bounded every section. */
		ParseAbortRecord(XLogRecGetInfo(reader), (xl_xact_abort *)XLogRecGetData(reader), &parsed);
		if (!cold_counter_latest(input.record_xid, parsed.nsubxacts, parsed.subxacts,
								 &input.payload_xid))
			return;
	} else if (rmid == RM_MULTIXACT_ID && info == XLOG_MULTIXACT_CREATE_ID) {
		xl_multixact_create header;

		if (!cold_counted_shape(reader, SizeOfMultiXactCreate,
								offsetof(xl_multixact_create, nmembers), sizeof(MultiXactMember)))
			return;
		memcpy(&header, XLogRecGetData(reader), SizeOfMultiXactCreate);
		if (header.nmembers <= 0 || !MultiXactIdIsValid(header.mid) || header.moff == 0)
			return;
		input.payload_xid = input.record_xid;
		for (int i = 0; i < header.nmembers; i++) {
			MultiXactMember member;

			memcpy(&member, XLogRecGetData(reader) + SizeOfMultiXactCreate
							+ (Size)i * sizeof(member),
				   sizeof(member));
			if (!TransactionIdIsNormal(member.xid) || member.status < MultiXactStatusForKeyShare
				|| member.status > MaxMultiXactStatus)
				return;
			if (!TransactionIdIsNormal(input.payload_xid)
				|| NormalTransactionIdPrecedes(input.payload_xid, member.xid))
				input.payload_xid = member.xid;
		}
	} else if (rmid == RM_XLOG_ID
			   && (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE)) {
		CheckPoint checkpoint;

		if (XLogRecGetData(reader) == NULL || XLogRecGetDataLen(reader) != sizeof(checkpoint))
			return;
		memcpy(&checkpoint, XLogRecGetData(reader), sizeof(checkpoint));
		if (!TransactionIdIsNormal(XidFromFullTransactionId(checkpoint.nextXid)))
			return;
		input.checkpoint_next_xid = U64FromFullTransactionId(checkpoint.nextXid);
		input.checkpoint_shutdown = info == XLOG_CHECKPOINT_SHUTDOWN;
	}
	/* Assignment and standby cache population are hot-standby-only. All
	 * other admitted cold records advance only past their header xid. */
	input.complete = true;
	out->xid_input = input;
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
	uint32 effects = CLUSTER_COLD_EFFECT_RECORD;
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
	detail = cold_record_flags(reader, system_identifier, storage_uuid, foreign, out,
							   &out->record.record_flags, &effects);
	if (detail != CLUSTER_COLD_OK)
		return detail;

	/* Other generations' record-local native adapters retain their original
	 * origin. Undo blocks/headers use their exact prefix consumer; outcomes,
	 * undo heads and remaining lifecycle effects still require
	 * their ordered whole-source owners, never founder native redo. */
	has_owner = XLogRecGetRmid(reader) == RM_SMGR_ID
				&& ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE
					|| out->record.space_count > 0);
	if (foreign && plan.route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC && !has_owner) {
		if (cold_foreign_control(reader, &plan))
			out->record.record_flags |= CLUSTER_COLD_RECORD_FOREIGN_CONTROL;
		else if (cold_foreign_side(reader, &plan, system_identifier))
			out->record.record_flags |= CLUSTER_COLD_RECORD_FOREIGN_SIDE;
		else
			out->record.record_flags |= CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	}
	detail = cold_map_components(reader, &plan, system_identifier, storage_uuid, out);
	if (detail == CLUSTER_COLD_OK) {
		cold_effect_routes(&plan, effects, out);
		out->detached = plan;
		cold_xid_input(reader, out);
	}
	return detail;
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
	memset(&decoded->detached, 0, sizeof(decoded->detached));
}

#endif /* USE_PGRAC_CLUSTER */
