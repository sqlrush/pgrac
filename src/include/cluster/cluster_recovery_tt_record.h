/*-------------------------------------------------------------------------
 * cluster_recovery_tt_record.h
 *    Exact recovery TT ABORT payload and read-only slot comparison.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_TT_RECORD_H
#define CLUSTER_RECOVERY_TT_RECORD_H

#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_uba.h"

#define CLUSTER_RECOVERY_TT_ABORT_VERSION UINT16_C(1)
#define CLUSTER_RECOVERY_TT_ABORT_BYTES 152
#define CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN UINT32_C(1)

/* Logical fields only; encode/decode define the persistent byte order. */
typedef struct ClusterRecoveryTTAbortV1 {
	uint16 version;
	uint16 length;
	uint32 flags;
	uint16 target_thread;
	uint16 writer_thread;
	uint32 reserved;
	uint64 target_incarnation;
	uint64 writer_incarnation;
	uint64 recovery_serial;
	uint64 target_epoch;
	uint64 target_full_xid;
	uint64 top_full_xid;
	uint8 target_claim_sha256[32];
	xl_undo_tt_slot_abort_exact exact;
	TTSlot before;
} ClusterRecoveryTTAbortV1;

typedef enum ClusterRecoveryTTAbortMatchV1 {
	CLUSTER_RECOVERY_TT_ABORT_REFUSED = 0,
	CLUSTER_RECOVERY_TT_ABORT_APPLY,
	CLUSTER_RECOVERY_TT_ABORT_IDEMPOTENT
} ClusterRecoveryTTAbortMatchV1;

/* Payload validation alone grants neither recovery authority nor stale skip. */
static inline bool
cluster_recovery_tt_abort_valid_v1(const ClusterRecoveryTTAbortV1 *record)
{
	static const uint8 zero_claim[32] = { 0 };
	const xl_undo_tt_slot_abort_exact *exact;
	uint32 segment, block;
	uint16 slot, row;

	if (record == NULL || record->version != CLUSTER_RECOVERY_TT_ABORT_VERSION
		|| record->length != CLUSTER_RECOVERY_TT_ABORT_BYTES
		|| record->flags != CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN || record->reserved != 0
		|| record->target_thread == 0 || record->target_thread > UNDO_OWNER_INSTANCE_MAX
		|| record->writer_thread == 0 || record->writer_thread > UNDO_OWNER_INSTANCE_MAX
		|| record->target_incarnation == 0 || record->writer_incarnation == 0
		|| record->recovery_serial == 0 || record->target_epoch == 0
		|| record->target_epoch > UINT32_MAX || record->target_full_xid != record->top_full_xid
		|| memcmp(record->target_claim_sha256, zero_claim, sizeof(zero_claim)) == 0)
		return false;
	exact = &record->exact;
	if (exact->segment_id == 0 || exact->segment_generation == UINT32_MAX
		|| (exact->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != record->target_thread
		|| exact->instance != record->target_thread
		|| exact->format_version != CLUSTER_UNDO_TT_ABORT_EXACT_VERSION || exact->flags != 0
		|| exact->reserved != 0 || exact->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| exact->wrap == TT_WRAP_INVALID || !TransactionIdIsNormal(exact->xid)
		|| (uint32)record->target_full_xid != exact->xid || record->before.xid != exact->xid
		|| record->before.wrap != exact->wrap || record->before.status != TT_SLOT_ACTIVE
		|| record->before.flags != TT_FLAGS_RESERVED || record->before.commit_scn != InvalidScn)
		return false;
	if (!UBA_is_invalid(record->before.first_undo_block)
		&& (!uba_decode_record(record->before.first_undo_block, &segment, &block, &slot, &row)
			|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != record->target_thread
			|| slot != exact->slot_offset))
		return false;
	return true;
}

static inline bool
cluster_recovery_tt_abort_encode_v1(const ClusterRecoveryTTAbortV1 *record, uint8 *bytes,
									size_t length)
{
	ClusterRecoveryTTAbortV1 value;

	if (bytes == NULL)
		return false;
	if (length != CLUSTER_RECOVERY_TT_ABORT_BYTES || !cluster_recovery_tt_abort_valid_v1(record)) {
		memset(bytes, 0, Min(length, (size_t)CLUSTER_RECOVERY_TT_ABORT_BYTES));
		return false;
	}
	value = *record;
	memset(bytes, 0, length);
	cluster_undo_tt_ctrc_put_u16_le(bytes, value.version);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 2, value.length);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 4, value.flags);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 8, value.target_thread);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 10, value.writer_thread);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, value.target_incarnation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, value.writer_incarnation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 32, value.recovery_serial);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 40, value.target_epoch);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 48, value.target_full_xid);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 56, value.top_full_xid);
	memcpy(bytes + 64, value.target_claim_sha256, sizeof(value.target_claim_sha256));
	cluster_undo_tt_ctrc_put_u32_le(bytes + 96, value.exact.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 100, value.exact.segment_generation);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 104, value.exact.xid);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 108, value.exact.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 110, value.exact.wrap);
	bytes[112] = value.exact.instance;
	bytes[113] = value.exact.format_version;
	cluster_undo_tt_ctrc_put_u32_le(bytes + 120, value.before.xid);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 124, value.before.wrap);
	bytes[126] = value.before.status;
	bytes[127] = value.before.flags;
	cluster_undo_tt_ctrc_put_u64_le(bytes + 128, value.before.commit_scn);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 136, value.before.first_undo_block.raw[0]);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 144, value.before.first_undo_block.raw[1]);
	return true;
}

static inline bool
cluster_recovery_tt_abort_decode_v1(const uint8 *bytes, size_t length,
									ClusterRecoveryTTAbortV1 *out)
{
	ClusterRecoveryTTAbortV1 value;

	if (out == NULL)
		return false;
	if (bytes == NULL || length != CLUSTER_RECOVERY_TT_ABORT_BYTES) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	memset(&value, 0, sizeof(value));
	value.version = cluster_undo_tt_ctrc_get_u16_le(bytes);
	value.length = cluster_undo_tt_ctrc_get_u16_le(bytes + 2);
	value.flags = cluster_undo_tt_ctrc_get_u32_le(bytes + 4);
	value.target_thread = cluster_undo_tt_ctrc_get_u16_le(bytes + 8);
	value.writer_thread = cluster_undo_tt_ctrc_get_u16_le(bytes + 10);
	value.reserved = cluster_undo_tt_ctrc_get_u32_le(bytes + 12);
	value.target_incarnation = cluster_undo_tt_ctrc_get_u64_le(bytes + 16);
	value.writer_incarnation = cluster_undo_tt_ctrc_get_u64_le(bytes + 24);
	value.recovery_serial = cluster_undo_tt_ctrc_get_u64_le(bytes + 32);
	value.target_epoch = cluster_undo_tt_ctrc_get_u64_le(bytes + 40);
	value.target_full_xid = cluster_undo_tt_ctrc_get_u64_le(bytes + 48);
	value.top_full_xid = cluster_undo_tt_ctrc_get_u64_le(bytes + 56);
	memcpy(value.target_claim_sha256, bytes + 64, sizeof(value.target_claim_sha256));
	value.exact.segment_id = cluster_undo_tt_ctrc_get_u32_le(bytes + 96);
	value.exact.segment_generation = cluster_undo_tt_ctrc_get_u32_le(bytes + 100);
	value.exact.xid = cluster_undo_tt_ctrc_get_u32_le(bytes + 104);
	value.exact.slot_offset = cluster_undo_tt_ctrc_get_u16_le(bytes + 108);
	value.exact.wrap = cluster_undo_tt_ctrc_get_u16_le(bytes + 110);
	value.exact.instance = bytes[112];
	value.exact.format_version = bytes[113];
	value.exact.flags = cluster_undo_tt_ctrc_get_u16_le(bytes + 114);
	value.exact.reserved = cluster_undo_tt_ctrc_get_u32_le(bytes + 116);
	value.before.xid = cluster_undo_tt_ctrc_get_u32_le(bytes + 120);
	value.before.wrap = cluster_undo_tt_ctrc_get_u16_le(bytes + 124);
	value.before.status = bytes[126];
	value.before.flags = bytes[127];
	value.before.commit_scn = cluster_undo_tt_ctrc_get_u64_le(bytes + 128);
	value.before.first_undo_block.raw[0] = cluster_undo_tt_ctrc_get_u64_le(bytes + 136);
	value.before.first_undo_block.raw[1] = cluster_undo_tt_ctrc_get_u64_le(bytes + 144);
	if (!cluster_recovery_tt_abort_valid_v1(&value)) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	*out = value;
	return true;
}

static inline bool
cluster_recovery_tt_abort_result_v1(const ClusterRecoveryTTAbortV1 *record, TTSlot *out)
{
	TTSlot result;

	if (out == NULL)
		return false;
	if (!cluster_recovery_tt_abort_valid_v1(record)) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	result = record->before;
	result.status = TT_SLOT_ABORTED;
	*out = result;
	return true;
}

static inline ClusterRecoveryTTAbortMatchV1
cluster_recovery_tt_abort_match_v1(const ClusterRecoveryTTAbortV1 *record, uint32 generation,
								   const TTSlot *slot)
{
	TTSlot result;

	if (slot == NULL || !cluster_recovery_tt_abort_result_v1(record, &result)
		|| generation != record->exact.segment_generation)
		return CLUSTER_RECOVERY_TT_ABORT_REFUSED;
	if (memcmp(slot, &record->before, sizeof(*slot)) == 0)
		return CLUSTER_RECOVERY_TT_ABORT_APPLY;
	if (memcmp(slot, &result, sizeof(*slot)) == 0)
		return CLUSTER_RECOVERY_TT_ABORT_IDEMPOTENT;
	return CLUSTER_RECOVERY_TT_ABORT_REFUSED;
}

#endif
