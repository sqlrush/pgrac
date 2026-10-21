/*-------------------------------------------------------------------------
 * cluster_recovery_ctrc_record.c
 *    Field-coded recovery CTRC release payload, without a WAL writer.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#ifdef USE_PGRAC_CLUSTER

#include "access/xlogrecord.h"
#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"

static bool
ctrc_nonzero_digest(const uint8 digest[32])
{
	static const uint8 zero[32] = { 0 };

	return memcmp(digest, zero, sizeof(zero)) != 0;
}

/* Structural checks only; caller digests are not owner completion proofs. */
static bool
ctrc_shape_valid(const ClusterRecoveryCTRCReleaseV2 *r)
{
	const TTSlot *before = &r->before;
	const TTSlot *after = &r->after;
	uint32 segment, block;
	uint16 slot, row;

	if (r->version != CLUSTER_RECOVERY_CTRC_VERSION || r->length != CLUSTER_RECOVERY_CTRC_BYTES
		|| r->flags != CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN || r->root_id == 0
		|| r->root_generation == 0 || r->formation_epoch == 0 || r->recovery_serial == 0
		|| r->database_incarnation == 0 || r->writer_boot == 0 || r->cohort_bitmap == 0
		|| r->origin_node_id > SCN_MAX_VALID_NODE_ID || r->writer_thread == 0
		|| r->writer_thread > UNDO_OWNER_INSTANCE_MAX || r->target_owner == 0
		|| r->target_owner > UNDO_OWNER_INSTANCE_MAX || r->origin_node_id + 1 != r->target_owner
		|| r->segment_id == 0 || r->segment_generation == UINT32_MAX
		|| (r->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != r->target_owner
		|| r->slot_offset >= TT_SLOTS_PER_SEGMENT || r->wrap == TT_WRAP_INVALID
		|| !TransactionIdIsNormal(r->xid)
		|| (r->terminal_outcome != TT_SLOT_COMMITTED && r->terminal_outcome != TT_SLOT_ABORTED)
		|| !ctrc_nonzero_digest(r->graph_sha256) || !ctrc_nonzero_digest(r->input_sha256))
		return false;
	if (before->xid != r->xid || before->wrap != r->wrap || before->status != r->terminal_outcome
		|| before->flags != TT_FLAGS_RESERVED
		|| (before->status == TT_SLOT_COMMITTED && before->commit_scn == InvalidScn)
		|| (before->status == TT_SLOT_ABORTED && before->commit_scn != InvalidScn)
		|| after->xid != before->xid || after->wrap != before->wrap
		|| after->status != before->status || after->flags != TT_SLOT_FLAG_CTRC_RELEASE_PROVEN
		|| after->commit_scn != before->commit_scn
		|| after->first_undo_block.raw[0] != before->first_undo_block.raw[0]
		|| after->first_undo_block.raw[1] != before->first_undo_block.raw[1])
		return false;
	if (!UBA_is_invalid(before->first_undo_block)
		&& (!uba_decode_record(before->first_undo_block, &segment, &block, &slot, &row)
			|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != r->target_owner
			|| slot != r->slot_offset))
		return false;
	return true;
}

static void
ctrc_put_slot(uint8 *bytes, const TTSlot *slot)
{
	cluster_undo_tt_ctrc_put_u32_le(bytes, slot->xid);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 4, slot->wrap);
	bytes[6] = slot->status;
	bytes[7] = slot->flags;
	cluster_undo_tt_ctrc_put_u64_le(bytes + 8, slot->commit_scn);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, slot->first_undo_block.raw[0]);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, slot->first_undo_block.raw[1]);
}

static void
ctrc_get_slot(const uint8 *bytes, TTSlot *slot)
{
	slot->xid = cluster_undo_tt_ctrc_get_u32_le(bytes);
	slot->wrap = cluster_undo_tt_ctrc_get_u16_le(bytes + 4);
	slot->status = bytes[6];
	slot->flags = bytes[7];
	slot->commit_scn = cluster_undo_tt_ctrc_get_u64_le(bytes + 8);
	slot->first_undo_block.raw[0] = cluster_undo_tt_ctrc_get_u64_le(bytes + 16);
	slot->first_undo_block.raw[1] = cluster_undo_tt_ctrc_get_u64_le(bytes + 24);
}

static bool
ctrc_sha256(const uint8 *bytes, size_t length, uint8 out[32])
{
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	bool ok;

	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) == 0 && pg_cryptohash_update(hash, bytes, length) == 0
		 && pg_cryptohash_final(hash, out, 32) == 0;
	pg_cryptohash_free(hash);
	return ok;
}

static bool
ctrc_identity_sha256(const uint8 *bytes, uint64 system_identifier, uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-D0-IDENTITY-V2";
	uint8 transcript[sizeof(domain) - 1 + 34 + 48 + 40 + 32];
	uint8 *next = transcript;

	/* No NUL. T is system/dbinc/origin/segment/generation/slot/wrap/xid,
	 * followed by the exact ROOT-round/source fields and full input digest. */
	memcpy(next, domain, sizeof(domain) - 1);
	next += sizeof(domain) - 1;
	cluster_undo_tt_ctrc_put_u64_le(next, system_identifier);
	next += 8;
	memcpy(next, bytes + 40, 8);
	next += 8;
	memcpy(next, bytes + 96, 2);
	next += 2;
	memcpy(next, bytes + 108, 8);
	next += 8;
	memcpy(next, bytes + 102, 4);
	next += 4;
	memcpy(next, bytes + 116, 4);
	next += 4;
	memcpy(next, bytes + 8, 48);
	next += 48;
	memcpy(next, bytes + 88, 40);
	next += 40;
	memcpy(next, bytes + 256, 32);
	return ctrc_sha256(transcript, sizeof(transcript), out);
}

static bool
ctrc_payload_sha256(const uint8 *bytes, uint8 out[32])
{
	/* Exactly eleven ASCII bytes followed by five zero bytes. */
	static const uint8 domain[16] = "PGRAC-D0-V2";
	uint8 transcript[304];

	memcpy(transcript, domain, sizeof(domain));
	memcpy(transcript + 16, bytes, 192);
	memcpy(transcript + 208, bytes + 224, 96);
	return ctrc_sha256(transcript, sizeof(transcript), out);
}

bool
cluster_recovery_ctrc_record_hash_v2(const uint8 source_identity_digest[32], XLogRecPtr start,
									 XLogRecPtr end, const uint8 *raw_record, size_t raw_length,
									 uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-D0-RECORD-V2";
	XLogRecord header;
	pg_crc32c crc;
	pg_cryptohash_ctx *hash;
	uint8 positions[20];
	uint8 digest[32];
	bool ok;

	if (out == NULL)
		return false;
	if (source_identity_digest == NULL || raw_record == NULL
		|| !ctrc_nonzero_digest(source_identity_digest) || raw_length < SizeOfXLogRecord
		|| raw_length > XLogRecordMaxSize || start == InvalidXLogRecPtr || end <= start
		|| end - start < raw_length)
		goto refuse;
	/* No aligned dereference of caller bytes, and no read beyond raw_length. */
	memcpy(&header, raw_record, SizeOfXLogRecord);
	if (header.xl_tot_len != raw_length)
		goto refuse;

	/* Same native CRC order as ValidXLogRecord: data, then header before CRC.
	 * This detects damage; it cannot establish reader/source authority. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, raw_record + SizeOfXLogRecord, raw_length - SizeOfXLogRecord);
	COMP_CRC32C(crc, raw_record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(header.xl_crc, crc))
		goto refuse;

	cluster_undo_tt_ctrc_put_u64_le(positions, start);
	cluster_undo_tt_ctrc_put_u64_le(positions + 8, end);
	cluster_undo_tt_ctrc_put_u32_le(positions + 16, (uint32)raw_length);
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		goto refuse;
	/* Do not hash a terminator, page headers or alignment padding. The final
	 * CRC remains inside raw_record, never replaced by this external SHA. */
	ok = pg_cryptohash_init(hash) == 0
		 && pg_cryptohash_update(hash, domain, sizeof(domain) - 1) == 0
		 && pg_cryptohash_update(hash, source_identity_digest, 32) == 0
		 && pg_cryptohash_update(hash, positions, sizeof(positions)) == 0
		 && pg_cryptohash_update(hash, raw_record, raw_length) == 0
		 && pg_cryptohash_final(hash, digest, sizeof(digest)) == 0;
	pg_cryptohash_free(hash);
	if (!ok)
		goto refuse;
	/* Publish last, including when output aliases source or raw bytes. */
	memcpy(out, digest, sizeof(digest));
	return true;

refuse:
	memset(out, 0, 32);
	return false;
}

bool
cluster_recovery_ctrc_encode_v2(const ClusterRecoveryCTRCReleaseV2 *record,
								uint64 system_identifier, uint8 *bytes, size_t length)
{
	ClusterRecoveryCTRCReleaseV2 value;

	if (bytes == NULL)
		return false;
	if (record == NULL || system_identifier == 0 || length != CLUSTER_RECOVERY_CTRC_BYTES) {
		memset(bytes, 0, Min(length, (size_t)CLUSTER_RECOVERY_CTRC_BYTES));
		return false;
	}
	/* Preserve a logical input that aliases the output buffer. */
	value = *record;
	memset(bytes, 0, length);
	if (!ctrc_shape_valid(&value))
		return false;
	cluster_undo_tt_ctrc_put_u16_le(bytes, value.version);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 2, value.length);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 4, value.flags);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 8, value.root_id);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, value.root_generation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, value.formation_epoch);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 32, value.recovery_serial);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 40, value.database_incarnation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 48, value.writer_boot);
	/* 56..71 and 120..127 stay zero: no self-referential WAL location. */
	cluster_undo_tt_ctrc_put_u64_le(bytes + 72, value.covered_history);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 80, value.highest_fpi_end);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 88, value.cohort_bitmap);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 96, value.origin_node_id);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 98, value.writer_thread);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 100, value.target_owner);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 102, value.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 104, value.wrap);
	bytes[106] = value.terminal_outcome;
	cluster_undo_tt_ctrc_put_u32_le(bytes + 108, value.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 112, value.segment_generation);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 116, value.xid);
	ctrc_put_slot(bytes + 128, &value.before);
	ctrc_put_slot(bytes + 160, &value.after);
	memcpy(bytes + 224, value.graph_sha256, 32);
	memcpy(bytes + 256, value.input_sha256, 32);
	if (!ctrc_identity_sha256(bytes, system_identifier, bytes + 288)
		|| !ctrc_payload_sha256(bytes, bytes + 192)) {
		memset(bytes, 0, length);
		return false;
	}
	return true;
}

bool
cluster_recovery_ctrc_decode_v2(const uint8 *bytes, size_t length, uint64 system_identifier,
								ClusterRecoveryCTRCReleaseV2 *out)
{
	ClusterRecoveryCTRCReleaseV2 value;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
	uint8 identity_sha256[32];
	uint8 payload_sha256[32];

	if (out == NULL)
		return false;
	if (bytes == NULL || length != sizeof(payload) || system_identifier == 0) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	/* Copy before clearing: the caller may decode in place. */
	memcpy(payload, bytes, sizeof(payload));
	memset(out, 0, sizeof(*out));
	if (cluster_undo_tt_ctrc_get_u64_le(payload + 56) != 0
		|| cluster_undo_tt_ctrc_get_u64_le(payload + 64) != 0 || payload[107] != 0
		|| cluster_undo_tt_ctrc_get_u64_le(payload + 120) != 0)
		return false;
	memset(&value, 0, sizeof(value));
	value.version = cluster_undo_tt_ctrc_get_u16_le(payload);
	value.length = cluster_undo_tt_ctrc_get_u16_le(payload + 2);
	value.flags = cluster_undo_tt_ctrc_get_u32_le(payload + 4);
	value.root_id = cluster_undo_tt_ctrc_get_u64_le(payload + 8);
	value.root_generation = cluster_undo_tt_ctrc_get_u64_le(payload + 16);
	value.formation_epoch = cluster_undo_tt_ctrc_get_u64_le(payload + 24);
	value.recovery_serial = cluster_undo_tt_ctrc_get_u64_le(payload + 32);
	value.database_incarnation = cluster_undo_tt_ctrc_get_u64_le(payload + 40);
	value.writer_boot = cluster_undo_tt_ctrc_get_u64_le(payload + 48);
	value.covered_history = cluster_undo_tt_ctrc_get_u64_le(payload + 72);
	value.highest_fpi_end = cluster_undo_tt_ctrc_get_u64_le(payload + 80);
	value.cohort_bitmap = cluster_undo_tt_ctrc_get_u64_le(payload + 88);
	value.origin_node_id = cluster_undo_tt_ctrc_get_u16_le(payload + 96);
	value.writer_thread = cluster_undo_tt_ctrc_get_u16_le(payload + 98);
	value.target_owner = cluster_undo_tt_ctrc_get_u16_le(payload + 100);
	value.slot_offset = cluster_undo_tt_ctrc_get_u16_le(payload + 102);
	value.wrap = cluster_undo_tt_ctrc_get_u16_le(payload + 104);
	value.terminal_outcome = payload[106];
	value.segment_id = cluster_undo_tt_ctrc_get_u32_le(payload + 108);
	value.segment_generation = cluster_undo_tt_ctrc_get_u32_le(payload + 112);
	value.xid = cluster_undo_tt_ctrc_get_u32_le(payload + 116);
	ctrc_get_slot(payload + 128, &value.before);
	ctrc_get_slot(payload + 160, &value.after);
	memcpy(value.graph_sha256, payload + 224, 32);
	memcpy(value.input_sha256, payload + 256, 32);
	if (!ctrc_shape_valid(&value)
		|| !ctrc_identity_sha256(payload, system_identifier, identity_sha256)
		|| memcmp(identity_sha256, payload + 288, 32) != 0
		|| !ctrc_payload_sha256(payload, payload_sha256)
		|| memcmp(payload_sha256, payload + 192, 32) != 0)
		return false;
	*out = value;
	return true;
}
#endif
