/*-------------------------------------------------------------------------
 *
 * cluster_side_undo.c
 *	  RF-SIDE D-SIDE-02 — TT/undo decode + preflight primitives
 *	  (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-02 / §2.1 / §5.1 U-SIDE-04.
 *
 *	  decode() is pure parsing: length check + field extraction, no I/O,
 *	  no mutation, no raw-record re-read.  preflight() combines the
 *	  D-SIDE-01 route with field-integrity checks; any false means the
 *	  caller performs ZERO mutation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_side_route.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h" /* TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h"

static ClusterUndoDecodedKind
cluster_undo_kind_for_opcode(uint16 opcode)
{
	switch (opcode) {
	case XLOG_UNDO_SEGMENT_INIT:
		return CLUSTER_UNDO_KIND_SEGMENT_INIT;
	case XLOG_UNDO_TT_SLOT_BIND:
		return CLUSTER_UNDO_KIND_TT_BIND;
	case XLOG_UNDO_TT_SLOT_COMMIT:
		return CLUSTER_UNDO_KIND_TT_COMMIT;
	case XLOG_UNDO_TT_SLOT_ABORT:
		return CLUSTER_UNDO_KIND_TT_ABORT;
	case XLOG_UNDO_TT_SLOT_SET_HEAD:
		return CLUSTER_UNDO_KIND_TT_SET_HEAD;
	case XLOG_UNDO_TT_SLOT_CTRC_RELEASE:
		return CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
	case XLOG_UNDO_SEGMENT_RECYCLE:
		return CLUSTER_UNDO_KIND_SEGMENT_RECYCLE;
	case XLOG_UNDO_SEGMENT_REUSE:
		return CLUSTER_UNDO_KIND_SEGMENT_REUSE;
	case XLOG_UNDO_BLOCK_WRITE:
		return CLUSTER_UNDO_KIND_BLOCK_WRITE;
	case XLOG_UNDO_BLOCK_WRITE_MULTI:
		return CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI;
	case XLOG_HW_RESERVE:
		return CLUSTER_UNDO_KIND_HW_RESERVE;
	}
	return CLUSTER_UNDO_KIND_UNKNOWN;
}

bool
cluster_undo_decode(XLogReaderState *record, ClusterUndoDecoded *out)
{
	uint16 opcode;
	ClusterUndoDecodedKind kind;

	if (record == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (XLogRecGetRmid(record) != RM_CLUSTER_UNDO_ID)
		return false; /* not a cluster-undo record */
	opcode = XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK;
	kind = cluster_undo_kind_for_opcode(opcode);
	if (kind == CLUSTER_UNDO_KIND_UNKNOWN)
		return false; /* unknown/illegal info: BLOCKED */
	out->kind = kind;
	out->opcode = opcode;

	/* Per-kind parse: exact payload length + field extraction (the same
	 * shapes the production redo handlers validate). */
	switch (kind) {
	case CLUSTER_UNDO_KIND_TT_BIND: {
		xl_undo_tt_slot_bind rec;

		if (XLogRecGetDataLen(record) != sizeof(rec))
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec.format_version != CLUSTER_UNDO_TT_BIND_VERSION || rec.flags != 0
			|| rec.reserved8 != 0 || rec.reserved32 != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->expected_generation = rec.segment_generation;
		out->slot_offset = rec.slot_offset;
		out->wrap = rec.wrap;
		out->xid = rec.xid;
		out->format_version = rec.format_version;
		out->flags = rec.flags;
		break;
	}
	case CLUSTER_UNDO_KIND_TT_COMMIT: {
		xl_undo_tt_slot_commit rec;

		if (XLogRecGetDataLen(record) != sizeof(rec))
			return false; /* malformed: BLOCKED (U-SIDE-04) */
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec._pad[0] != 0 || rec._pad[1] != 0 || rec._pad[2] != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->slot_offset = rec.slot_offset;
		out->wrap = rec.wrap;
		out->xid = rec.xid;
		out->commit_scn = rec.commit_scn;
		break;
	}
	case CLUSTER_UNDO_KIND_TT_ABORT: {
		if (XLogRecGetDataLen(record) == sizeof(xl_undo_tt_slot_abort_exact)) {
			xl_undo_tt_slot_abort_exact rec;

			memcpy(&rec, XLogRecGetData(record), sizeof(rec));
			if (rec.format_version != CLUSTER_UNDO_TT_ABORT_EXACT_VERSION || rec.flags != 0
				|| rec.reserved != 0)
				return false;
			out->instance = rec.instance;
			out->segment_id = rec.segment_id;
			out->expected_generation = rec.segment_generation;
			out->slot_offset = rec.slot_offset;
			out->wrap = rec.wrap;
			out->xid = rec.xid;
			out->format_version = rec.format_version;
			out->flags = rec.flags;
		} else {
			xl_undo_tt_slot_abort rec;

			if (XLogRecGetDataLen(record) != sizeof(rec))
				return false;
			memcpy(&rec, XLogRecGetData(record), sizeof(rec));
			if (rec._pad[0] != 0 || rec._pad[1] != 0 || rec._pad[2] != 0)
				return false;
			out->instance = rec.instance;
			out->segment_id = rec.segment_id;
			out->slot_offset = rec.slot_offset;
			out->wrap = rec.wrap;
			out->xid = rec.xid;
		}
		break;
	}
	case CLUSTER_UNDO_KIND_TT_SET_HEAD: {
		xl_undo_tt_slot_set_head rec;

		if (XLogRecGetDataLen(record) != sizeof(rec))
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec._pad[0] != 0 || rec._pad[1] != 0 || rec._pad[2] != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->slot_offset = rec.slot_offset;
		out->wrap = rec.wrap;
		out->xid = rec.xid;
		out->first_undo_block = rec.first_undo_block;
		break;
	}
	case CLUSTER_UNDO_KIND_TT_CTRC_RELEASE: {
		xl_undo_tt_slot_ctrc_release_v1 rec;

		if (XLogRecGetDataLen(record) != CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES
			|| !cluster_undo_tt_ctrc_release_decode((const uint8 *)XLogRecGetData(record), &rec))
			return false;
		out->instance = rec.owner_instance;
		out->segment_id = rec.segment_id;
		out->expected_generation = rec.segment_generation;
		out->slot_offset = rec.slot_offset;
		out->wrap = rec.slot_wrap;
		out->xid = rec.xid;
		out->cluster_epoch = rec.cluster_epoch;
		out->root_id = rec.root_id;
		out->root_generation = rec.root_generation;
		out->formation_epoch = rec.formation_epoch;
		out->admission_record_generation = rec.admission_record_generation;
		out->seal_generation = rec.seal_generation;
		out->touched_nodes_low = rec.touched_nodes_low;
		out->touched_nodes_high = rec.touched_nodes_high;
		memcpy(out->ack_set_digest, rec.ack_set_digest, sizeof(out->ack_set_digest));
		out->format_version = rec.format_version;
		out->flags = rec.flags;
		out->terminal_status = rec.terminal_status;
		break;
	}
	case CLUSTER_UNDO_KIND_SEGMENT_RECYCLE: {
		xl_undo_segment_recycle rec;

		if (XLogRecGetDataLen(record) != sizeof(rec))
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec._pad != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->expected_generation = rec.expected_generation;
		out->old_state = rec.old_state;
		out->new_state = rec.new_state;
		break;
	}
	case CLUSTER_UNDO_KIND_SEGMENT_REUSE: {
		xl_undo_segment_reuse rec;

		if (XLogRecGetDataLen(record) != sizeof(rec) + BLCKSZ)
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec._pad[0] != 0 || rec._pad[1] != 0 || rec._pad[2] != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->expected_generation = rec.old_generation;
		out->new_generation = rec.new_generation;
		out->has_payload = true;
		out->has_fpi = true;
		out->payload_offset = sizeof(rec);
		out->payload_length = BLCKSZ;
		break;
	}
	case CLUSTER_UNDO_KIND_BLOCK_WRITE: {
		xl_undo_block_write rec;
		uint32 body_len;
		uint32 expected;

		if (XLogRecGetDataLen(record) < sizeof(rec))
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		body_len = XLogRecGetDataLen(record) - sizeof(rec);
		if (rec.has_fpi > 1 || rec.block_no == 0)
			return false;
		if (rec.has_fpi == 1) {
			if (body_len != BLCKSZ || rec.rec_off != 0 || rec.rec_len != 0 || rec.slot_off != 0)
				return false;
		} else {
			expected = UNDO_BLOCK_HDR_PREFIX_LEN + rec.rec_len + sizeof(UndoSlotDirEntry);
			if (rec.rec_off < sizeof(UndoBlockHeader) || rec.rec_len == 0
				|| (uint32)rec.rec_off + rec.rec_len > BLCKSZ
				|| rec.slot_off < sizeof(UndoBlockHeader)
				|| (uint32)rec.slot_off + sizeof(UndoSlotDirEntry) > BLCKSZ || body_len != expected)
				return false;
		}
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->block_no = rec.block_no;
		out->has_payload = true;
		out->has_fpi = rec.has_fpi == 1;
		out->rec_off = rec.rec_off;
		out->rec_len = rec.rec_len;
		out->slot_off = rec.slot_off;
		out->slot_len = sizeof(UndoSlotDirEntry);
		out->payload_offset = sizeof(rec);
		out->payload_length = body_len;
		break;
	}
	case CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI: {
		xl_undo_block_write_multi rec;
		uint32 body_len;
		uint32 expected;

		if (XLogRecGetDataLen(record) < sizeof(rec))
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		body_len = XLogRecGetDataLen(record) - sizeof(rec);
		if (rec._pad != 0 || rec.has_fpi > 1 || rec.block_no == 0)
			return false;
		if (rec.has_fpi == 1) {
			if (body_len != BLCKSZ || rec.rec_off != 0 || rec.rec_len != 0 || rec.slot_off != 0
				|| rec.slot_len != 0)
				return false;
		} else {
			expected = UNDO_BLOCK_HDR_PREFIX_LEN + rec.rec_len + rec.slot_len;
			if (rec.rec_off < sizeof(UndoBlockHeader) || rec.rec_len == 0
				|| (uint32)rec.rec_off + rec.rec_len > BLCKSZ
				|| rec.slot_off < sizeof(UndoBlockHeader) || rec.slot_len == 0
				|| rec.slot_len % sizeof(UndoSlotDirEntry) != 0
				|| (uint32)rec.slot_off + rec.slot_len > BLCKSZ || body_len != expected)
				return false;
		}
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->block_no = rec.block_no;
		out->has_payload = true;
		out->has_fpi = rec.has_fpi == 1;
		out->rec_off = rec.rec_off;
		out->rec_len = rec.rec_len;
		out->slot_off = rec.slot_off;
		out->slot_len = rec.slot_len;
		out->payload_offset = sizeof(rec);
		out->payload_length = body_len;
		break;
	}
	case CLUSTER_UNDO_KIND_SEGMENT_INIT: {
		xl_cluster_undo_segment_init rec;

		if (XLogRecGetDataLen(record) != sizeof(rec) + BLCKSZ)
			return false;
		memcpy(&rec, XLogRecGetData(record), sizeof(rec));
		if (rec._pad[0] != 0 || rec._pad2[0] != 0 || rec._pad2[1] != 0)
			return false;
		out->instance = rec.instance;
		out->segment_id = rec.segment_id;
		out->has_payload = true;
		out->has_fpi = true;
		out->payload_offset = sizeof(rec);
		out->payload_length = BLCKSZ;
		break;
	}
	case CLUSTER_UNDO_KIND_HW_RESERVE: {
		const xl_hw_reserve *rec;

		if (XLogRecGetDataLen(record) != sizeof(*rec))
			return false;
		rec = (const xl_hw_reserve *)XLogRecGetData(record);
		/* HWM carries relation/block facts, not undo segment
				 * identity; it stays BLOCKED at preflight anyway
				 * (STOP-RF-SIDE-SPACE-ABI). */
		out->block_no = rec->new_hwm;
		break;
	}
	default:
		return false;
	}
	return true;
}

bool
cluster_undo_preflight(const ClusterUndoDecoded *decoded)
{
	ClusterSideRouteRow route;
	uint16 opcode;

	if (decoded == NULL || decoded->kind == CLUSTER_UNDO_KIND_UNKNOWN)
		return false;

	/* §2.1-2 route: the D-SIDE-01 registry is the single judgement.
	 * XLOG_HW_RESERVE routes BLOCKED while STOP-RF-SIDE-SPACE-ABI is
	 * active (spec §3.2: never raise shmem and call complete). */
	opcode = decoded->opcode;
	if (!cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, opcode, &route))
		return false;
	if (route.kind != CLUSTER_SIDE_ROUTE_TT_UNDO)
		return false;

	/* Field integrity: TT slots must be in range (mirrors the production
	 * handler's PANIC checks as pre-mutation gates — a malformed record
	 * is BLOCKED, never half-applied). */
	if (decoded->kind != CLUSTER_UNDO_KIND_HW_RESERVE) {
		uint32 owner;

		if (decoded->instance == 0 || decoded->instance > 128 || decoded->segment_id == 0)
			return false;
		owner = ((decoded->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1;
		if (owner != decoded->instance)
			return false;
	}
	switch (decoded->kind) {
	case CLUSTER_UNDO_KIND_TT_BIND:
		if (decoded->slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(decoded->xid)
			|| decoded->wrap == TT_WRAP_INVALID
			|| decoded->format_version != CLUSTER_UNDO_TT_BIND_VERSION || decoded->flags != 0)
			return false;
		break;
	case CLUSTER_UNDO_KIND_TT_COMMIT:
	case CLUSTER_UNDO_KIND_TT_SET_HEAD:
		if (decoded->slot_offset >= TT_SLOTS_PER_SEGMENT)
			return false;
		if (!TransactionIdIsNormal(decoded->xid) || decoded->wrap == 0)
			return false;
		if (decoded->kind == CLUSTER_UNDO_KIND_TT_COMMIT && !SCN_VALID(decoded->commit_scn))
			return false;
		if (decoded->kind == CLUSTER_UNDO_KIND_TT_SET_HEAD) {
			uint32 segment_id;
			uint32 block_no;
			uint16 slot_offset;
			uint16 row_offset;

			if (!uba_decode_record(decoded->first_undo_block, &segment_id, &block_no, &slot_offset,
								   &row_offset)
				|| segment_id != decoded->segment_id || slot_offset != decoded->slot_offset)
				return false;
		}
		break;
	case CLUSTER_UNDO_KIND_TT_ABORT:
		if (decoded->slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(decoded->xid))
			return false;
		if (decoded->format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION) {
			if (decoded->expected_generation == UINT32_MAX || decoded->wrap == TT_WRAP_INVALID
				|| decoded->flags != 0)
				return false;
		} else if (decoded->format_version != 0 || decoded->wrap == 0)
			return false;
		break;
	case CLUSTER_UNDO_KIND_TT_CTRC_RELEASE:
		if (decoded->slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(decoded->xid)
			|| decoded->expected_generation == 0 || decoded->wrap == TT_WRAP_INVALID
			|| decoded->cluster_epoch == 0 || decoded->root_id == 0 || decoded->root_generation == 0
			|| decoded->formation_epoch == 0 || decoded->admission_record_generation == 0
			|| decoded->seal_generation == 0 || decoded->touched_nodes_high != 0
			|| (decoded->touched_nodes_low & ~UINT64_C(0xffff)) != 0
			|| (decoded->terminal_status != TT_SLOT_COMMITTED
				&& decoded->terminal_status != TT_SLOT_ABORTED)
			|| decoded->format_version != CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION
			|| decoded->flags != CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED)
			return false;
		break;
	case CLUSTER_UNDO_KIND_SEGMENT_RECYCLE:
		if (decoded->old_state != SEGMENT_COMMITTED || decoded->new_state != SEGMENT_RECYCLABLE)
			return false;
		break;
	case CLUSTER_UNDO_KIND_SEGMENT_REUSE:
		if (decoded->new_generation != decoded->expected_generation + 1 || !decoded->has_fpi
			|| decoded->payload_length != BLCKSZ)
			return false;
		break;
	case CLUSTER_UNDO_KIND_SEGMENT_INIT:
		if (!decoded->has_fpi || decoded->payload_length != BLCKSZ)
			return false;
		break;
	case CLUSTER_UNDO_KIND_BLOCK_WRITE:
	case CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI:
		if (decoded->block_no == 0 || !decoded->has_payload || decoded->payload_length == 0)
			return false;
		break;
	default:
		break;
	}
	return true;
}

static bool
cluster_undo_ctrc_release_from_decoded(const ClusterUndoDecoded *decoded,
									   xl_undo_tt_slot_ctrc_release_v1 *record)
{
	if (decoded == NULL || record == NULL || decoded->kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE)
		return false;
	memset(record, 0, sizeof(*record));
	record->segment_id = decoded->segment_id;
	record->segment_generation = decoded->expected_generation;
	record->xid = decoded->xid;
	record->cluster_epoch = decoded->cluster_epoch;
	record->root_id = decoded->root_id;
	record->root_generation = decoded->root_generation;
	record->formation_epoch = decoded->formation_epoch;
	record->admission_record_generation = decoded->admission_record_generation;
	record->seal_generation = decoded->seal_generation;
	record->touched_nodes_low = decoded->touched_nodes_low;
	record->touched_nodes_high = decoded->touched_nodes_high;
	memcpy(record->ack_set_digest, decoded->ack_set_digest, sizeof(record->ack_set_digest));
	record->slot_offset = decoded->slot_offset;
	record->slot_wrap = decoded->wrap;
	record->owner_instance = decoded->instance;
	record->terminal_status = decoded->terminal_status;
	record->format_version = decoded->format_version;
	record->flags = decoded->flags;
	return cluster_undo_tt_ctrc_release_valid(record);
}

ClusterUndoTargetPreflightV1
cluster_undo_preflight_tt_target_v1(const ClusterUndoDecoded *decoded)
{
	TTSlot slot;
	ClusterTTActiveTransitionDecision bind_decision;
	ClusterTTTerminalTransitionDecision abort_decision;
	ClusterUndoTtCtrcReleaseRedoDecision release_decision;
	xl_undo_tt_slot_ctrc_release_v1 release_record;

	if (!cluster_undo_preflight(decoded))
		return CLUSTER_UNDO_TARGET_BLOCKED;
	if (decoded->kind == CLUSTER_UNDO_KIND_TT_BIND) {
		bind_decision = cluster_tt_durable_bind_preflight_exact(
			decoded->instance, decoded->segment_id, decoded->expected_generation,
			decoded->slot_offset, decoded->wrap, decoded->xid);
		if (bind_decision == CLUSTER_TT_ACTIVE_APPLY)
			return CLUSTER_UNDO_TARGET_APPLY;
		if (bind_decision == CLUSTER_TT_ACTIVE_IDEMPOTENT)
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		if (bind_decision == CLUSTER_TT_ACTIVE_STALE)
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		return CLUSTER_UNDO_TARGET_BLOCKED;
	}
	if (decoded->kind == CLUSTER_UNDO_KIND_TT_ABORT
		&& decoded->format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION) {
		abort_decision = cluster_tt_durable_abort_preflight_exact(
			decoded->instance, decoded->segment_id, decoded->expected_generation,
			decoded->slot_offset, decoded->wrap, decoded->xid);
		if (abort_decision == CLUSTER_TT_TERMINAL_APPLY)
			return CLUSTER_UNDO_TARGET_APPLY;
		if (abort_decision == CLUSTER_TT_TERMINAL_IDEMPOTENT
			|| abort_decision == CLUSTER_TT_TERMINAL_STALE)
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		return CLUSTER_UNDO_TARGET_BLOCKED;
	}
	if (decoded->kind == CLUSTER_UNDO_KIND_TT_CTRC_RELEASE) {
		if (!cluster_undo_ctrc_release_from_decoded(decoded, &release_record))
			return CLUSTER_UNDO_TARGET_BLOCKED;
		release_decision = cluster_tt_durable_ctrc_release_preflight_exact(&release_record);
		if (release_decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY)
			return CLUSTER_UNDO_TARGET_APPLY;
		if (release_decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_IDEMPOTENT
			|| release_decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_SKIP_STALE)
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		return CLUSTER_UNDO_TARGET_BLOCKED;
	}
	if (decoded->kind != CLUSTER_UNDO_KIND_TT_COMMIT && decoded->kind != CLUSTER_UNDO_KIND_TT_ABORT
		&& decoded->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD)
		return CLUSTER_UNDO_TARGET_BLOCKED;
	if (!cluster_tt_slot_durable_read_exact_stable(decoded->segment_id, decoded->slot_offset,
												   decoded->xid, decoded->wrap, &slot))
		return CLUSTER_UNDO_TARGET_BLOCKED;

	switch (decoded->kind) {
	case CLUSTER_UNDO_KIND_TT_BIND:
		cluster_tt_durable_redo_bind_slot(decoded->instance, decoded->segment_id,
										  decoded->expected_generation, decoded->slot_offset,
										  decoded->wrap, decoded->xid);
		break;
	case CLUSTER_UNDO_KIND_TT_COMMIT:
		if (slot.status == TT_SLOT_COMMITTED && slot.commit_scn == decoded->commit_scn
			&& UBA_is_invalid(slot.first_undo_block))
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		if (slot.status == TT_SLOT_ACTIVE && !SCN_VALID(slot.commit_scn))
			return CLUSTER_UNDO_TARGET_APPLY;
		break;
	case CLUSTER_UNDO_KIND_TT_ABORT:
		if (slot.status == TT_SLOT_ABORTED && !SCN_VALID(slot.commit_scn)
			&& UBA_is_invalid(slot.first_undo_block))
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		if (slot.status == TT_SLOT_ACTIVE && !SCN_VALID(slot.commit_scn))
			return CLUSTER_UNDO_TARGET_APPLY;
		break;
	case CLUSTER_UNDO_KIND_TT_SET_HEAD:
		if (slot.status != TT_SLOT_ABORTED || SCN_VALID(slot.commit_scn))
			break;
		if (memcmp(&slot.first_undo_block, &decoded->first_undo_block, sizeof(UBA)) == 0)
			return CLUSTER_UNDO_TARGET_PROVED_NOOP;
		if (UBA_is_invalid(slot.first_undo_block))
			return CLUSTER_UNDO_TARGET_APPLY;
		break;
	default:
		break;
	}
	return CLUSTER_UNDO_TARGET_BLOCKED;
}

ClusterUndoApplyResultV1
cluster_undo_apply_tt_v1(const ClusterUndoDecoded *decoded)
{
	TTSlot post_slot;
	ClusterUndoTargetPreflightV1 target;
	xl_undo_tt_slot_ctrc_release_v1 release_record;

	target = cluster_undo_preflight_tt_target_v1(decoded);
	if (target == CLUSTER_UNDO_TARGET_BLOCKED)
		return CLUSTER_UNDO_APPLY_BLOCKED;
	if (target == CLUSTER_UNDO_TARGET_PROVED_NOOP)
		return CLUSTER_UNDO_APPLY_OK;
	switch (decoded->kind) {
	case CLUSTER_UNDO_KIND_TT_BIND:
		cluster_tt_durable_redo_bind_slot(decoded->instance, decoded->segment_id,
										  decoded->expected_generation, decoded->slot_offset,
										  decoded->wrap, decoded->xid);
		break;
	case CLUSTER_UNDO_KIND_TT_COMMIT:
		cluster_tt_durable_redo_stamp_slot(decoded->instance, decoded->segment_id,
										   decoded->slot_offset, decoded->wrap, decoded->xid,
										   decoded->commit_scn);
		break;
	case CLUSTER_UNDO_KIND_TT_ABORT:
		if (decoded->format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION) {
			cluster_tt_durable_redo_abort_slot_exact(
				decoded->instance, decoded->segment_id, decoded->expected_generation,
				decoded->slot_offset, decoded->wrap, decoded->xid);
			if (cluster_tt_durable_abort_preflight_exact(
					decoded->instance, decoded->segment_id, decoded->expected_generation,
					decoded->slot_offset, decoded->wrap, decoded->xid)
				!= CLUSTER_TT_TERMINAL_IDEMPOTENT)
				return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
			return CLUSTER_UNDO_APPLY_OK;
		}
		cluster_tt_durable_redo_abort_slot(decoded->instance, decoded->segment_id,
										   decoded->slot_offset, decoded->wrap, decoded->xid);
		break;
	case CLUSTER_UNDO_KIND_TT_CTRC_RELEASE:
		if (!cluster_undo_ctrc_release_from_decoded(decoded, &release_record))
			return CLUSTER_UNDO_APPLY_BLOCKED;
		cluster_tt_durable_redo_ctrc_release_slot_exact(&release_record);
		if (cluster_undo_preflight_tt_target_v1(decoded) != CLUSTER_UNDO_TARGET_PROVED_NOOP)
			return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
		return CLUSTER_UNDO_APPLY_OK;
	case CLUSTER_UNDO_KIND_TT_SET_HEAD:
		cluster_tt_durable_redo_set_head_slot(decoded->instance, decoded->segment_id,
											  decoded->slot_offset, decoded->wrap, decoded->xid,
											  decoded->first_undo_block);
		break;
	default:
		return CLUSTER_UNDO_APPLY_BLOCKED;
	}
	if (!cluster_tt_slot_durable_read_exact_stable(decoded->segment_id, decoded->slot_offset,
												   decoded->xid, decoded->wrap, &post_slot))
		return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
	switch (decoded->kind) {
	case CLUSTER_UNDO_KIND_TT_BIND:
		if (post_slot.status != TT_SLOT_ACTIVE || SCN_VALID(post_slot.commit_scn)
			|| post_slot.flags != TT_FLAGS_RESERVED || !UBA_is_invalid(post_slot.first_undo_block))
			return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
		break;
	case CLUSTER_UNDO_KIND_TT_COMMIT:
		if (post_slot.status != TT_SLOT_COMMITTED || post_slot.commit_scn != decoded->commit_scn
			|| !UBA_is_invalid(post_slot.first_undo_block))
			return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
		break;
	case CLUSTER_UNDO_KIND_TT_ABORT:
		if (post_slot.status != TT_SLOT_ABORTED || SCN_VALID(post_slot.commit_scn)
			|| !UBA_is_invalid(post_slot.first_undo_block))
			return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
		break;
	case CLUSTER_UNDO_KIND_TT_SET_HEAD:
		if (post_slot.status != TT_SLOT_ABORTED || SCN_VALID(post_slot.commit_scn)
			|| memcmp(&post_slot.first_undo_block, &decoded->first_undo_block, sizeof(UBA)) != 0)
			return CLUSTER_UNDO_APPLY_POST_READ_FAILED;
		break;
	default:
		return CLUSTER_UNDO_APPLY_BLOCKED;
	}
	return CLUSTER_UNDO_APPLY_OK;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
