/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_journal.c
 *	  Exact manual-LE journal codec, scanner and durable append primitive.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/cryptohash.h"
#include "pgrac_fenced_journal.h"
#include "port/pg_crc32c.h"

#define PGRAC_FENCED_JOURNAL_CRC_OFFSET 252U

static const uint8 config_digest_domain[] =
	"PGRAC-FENCED-CONFIG-FILE-V1";

StaticAssertDecl(sizeof(config_digest_domain) == 28,
				 "config digest domain changed");

static void
put_u16_le(uint8 *out, uint16 value)
{
	out[0] = (uint8) value;
	out[1] = (uint8) (value >> 8);
}

static void
put_u32_le(uint8 *out, uint32 value)
{
	out[0] = (uint8) value;
	out[1] = (uint8) (value >> 8);
	out[2] = (uint8) (value >> 16);
	out[3] = (uint8) (value >> 24);
}

static void
put_u64_le(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8) (value >> (i * 8));
}

static uint16
get_u16_le(const uint8 *in)
{
	return ((uint16) in[0]) | ((uint16) in[1] << 8);
}

static uint32
get_u32_le(const uint8 *in)
{
	return ((uint32) in[0]) |
		((uint32) in[1] << 8) |
		((uint32) in[2] << 16) |
		((uint32) in[3] << 24);
}

static uint64
get_u64_le(const uint8 *in)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= ((uint64) in[i]) << (i * 8);
	return value;
}

static bool
bytes_all_zero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static uint32
journal_crc32c(const uint8 *frame)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, frame, PGRAC_FENCED_JOURNAL_CRC_OFFSET);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static bool
record_semantics_valid(const PgracFencedJournalRecordV1 *record)
{
	if (record == NULL ||
		record->record_kind < PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED ||
		record->record_kind > PGRAC_FENCED_JOURNAL_KIND_RECONCILED ||
		record->seq == 0 ||
		bytes_all_zero(record->daemon_boot_id,
					   sizeof(record->daemon_boot_id)) ||
		bytes_all_zero(record->semantic_config_digest,
					   sizeof(record->semantic_config_digest)) ||
		record->provider_result > PGRAC_FENCED_JOURNAL_PROVIDER_IO_ERROR ||
		record->target_state > 4 || record->io_drain_state > 2 ||
		record->deny_reason > 31 || record->event_mono_ns == 0)
		return false;
	if (record->record_kind != PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED &&
		bytes_all_zero(record->operation_id, sizeof(record->operation_id)))
		return false;
	if (record->record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED)
	{
		if (bytes_all_zero(record->binding_digest,
						   sizeof(record->binding_digest)) ||
			record->provider_id == 0 || record->provider_abi_version != 1 ||
			record->provider_result != PGRAC_FENCED_JOURNAL_PROVIDER_OK ||
			record->target_state != 1 || record->io_drain_state != 1 ||
			record->mapping_generation == 0 || record->proof_generation == 0 ||
			record->fresh_until_mono_ns <= record->event_mono_ns ||
			bytes_all_zero(record->target_state_digest,
						   sizeof(record->target_state_digest)) ||
			record->deny_reason != 0)
			return false;
	}
	if (record->record_kind == PGRAC_FENCED_JOURNAL_KIND_RECONCILED &&
		record->target_state != PGRAC_FENCED_JOURNAL_TARGET_OFF &&
		record->target_state != PGRAC_FENCED_JOURNAL_TARGET_ON &&
		record->target_state != PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN)
		return false;
	return true;
}

bool
pgrac_fenced_journal_record_encode(
	const PgracFencedJournalRecordV1 *record,
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES])
{
	if (frame == NULL || !record_semantics_valid(record) ||
		!bytes_all_zero((const uint8 *) &record->intent, sizeof(record->intent)))
		return false;
	memset(frame, 0, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	memcpy(frame, "PFGJ", 4);
	put_u16_le(frame + 4, 1);
	put_u16_le(frame + 6, record->record_kind);
	put_u32_le(frame + 8, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	put_u64_le(frame + 16, record->seq);
	memcpy(frame + 24, record->daemon_boot_id,
		   sizeof(record->daemon_boot_id));
	memcpy(frame + 40, record->operation_id, sizeof(record->operation_id));
	memcpy(frame + 56, record->previous_record_digest,
		   sizeof(record->previous_record_digest));
	memcpy(frame + 88, record->binding_digest,
		   sizeof(record->binding_digest));
	put_u16_le(frame + 120, record->provider_id);
	put_u16_le(frame + 122, record->provider_abi_version);
	put_u32_le(frame + 124, record->provider_result);
	put_u32_le(frame + 128, (uint32) record->provider_native_status);
	put_u32_le(frame + 132, record->target_state);
	put_u64_le(frame + 136, record->event_mono_ns);
	put_u64_le(frame + 144, record->fresh_until_mono_ns);
	put_u64_le(frame + 152, record->mapping_generation);
	put_u64_le(frame + 160, record->proof_generation);
	memcpy(frame + 168, record->target_state_digest,
		   sizeof(record->target_state_digest));
	memcpy(frame + 200, record->semantic_config_digest,
		   sizeof(record->semantic_config_digest));
	put_u32_le(frame + 232, record->deny_reason);
	put_u32_le(frame + 236, record->io_drain_state);
	memcpy(frame + 248, "PFGZ", 4);
	put_u32_le(frame + PGRAC_FENCED_JOURNAL_CRC_OFFSET,
			   journal_crc32c(frame));
	return true;
}

bool
pgrac_fenced_journal_record_decode(const uint8 *frame, size_t frame_len,
							   PgracFencedJournalRecordV1 *record)
{
	PgracFencedJournalRecordV1 decoded;

	if (record != NULL)
		memset(record, 0, sizeof(*record));
	if (frame_len == PGRAC_FENCED_JOURNAL_INTENT_BYTES)
		return pgrac_fenced_journal_intent_decode(frame, frame_len, record);
	if (frame == NULL || record == NULL ||
		frame_len != PGRAC_FENCED_JOURNAL_RECORD_BYTES ||
		memcmp(frame, "PFGJ", 4) != 0 || get_u16_le(frame + 4) != 1 ||
		get_u32_le(frame + 8) != PGRAC_FENCED_JOURNAL_RECORD_BYTES ||
		get_u32_le(frame + 12) != 0 ||
		!bytes_all_zero(frame + 240, 8) || memcmp(frame + 248, "PFGZ", 4) != 0 ||
		get_u32_le(frame + PGRAC_FENCED_JOURNAL_CRC_OFFSET) !=
		journal_crc32c(frame))
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.record_kind = get_u16_le(frame + 6);
	decoded.seq = get_u64_le(frame + 16);
	memcpy(decoded.daemon_boot_id, frame + 24,
		   sizeof(decoded.daemon_boot_id));
	memcpy(decoded.operation_id, frame + 40, sizeof(decoded.operation_id));
	memcpy(decoded.previous_record_digest, frame + 56,
		   sizeof(decoded.previous_record_digest));
	memcpy(decoded.binding_digest, frame + 88,
		   sizeof(decoded.binding_digest));
	decoded.provider_id = get_u16_le(frame + 120);
	decoded.provider_abi_version = get_u16_le(frame + 122);
	decoded.provider_result = get_u32_le(frame + 124);
	decoded.provider_native_status = (int32) get_u32_le(frame + 128);
	decoded.target_state = get_u32_le(frame + 132);
	decoded.event_mono_ns = get_u64_le(frame + 136);
	decoded.fresh_until_mono_ns = get_u64_le(frame + 144);
	decoded.mapping_generation = get_u64_le(frame + 152);
	decoded.proof_generation = get_u64_le(frame + 160);
	memcpy(decoded.target_state_digest, frame + 168,
		   sizeof(decoded.target_state_digest));
	memcpy(decoded.semantic_config_digest, frame + 200,
		   sizeof(decoded.semantic_config_digest));
	decoded.deny_reason = get_u32_le(frame + 232);
	decoded.io_drain_state = get_u32_le(frame + 236);
	if (!record_semantics_valid(&decoded))
		return false;
	*record = decoded;
	return true;
}

bool
pgrac_fenced_journal_record_digest(
	const uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES],
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES])
{
	pg_cryptohash_ctx *ctx;
	bool ok;

	if (frame == NULL || digest == NULL)
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 &&
		pg_cryptohash_update(ctx, frame,
						 PGRAC_FENCED_JOURNAL_RECORD_BYTES) >= 0 &&
		pg_cryptohash_final(ctx, digest,
						PGRAC_FENCED_JOURNAL_DIGEST_BYTES) >= 0;
	pg_cryptohash_free(ctx);
	if (!ok)
		memset(digest, 0, PGRAC_FENCED_JOURNAL_DIGEST_BYTES);
	return ok;
}

bool
pgrac_fenced_journal_config_digest_v1(
	const uint8 *config_bytes, size_t config_len,
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES])
{
	pg_cryptohash_ctx *ctx;
	uint8 length_le[4];
	bool ok;

	if (config_bytes == NULL || config_len == 0 || config_len > UINT32_MAX ||
		digest == NULL)
		return false;
	put_u32_le(length_le, (uint32) config_len);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 &&
		pg_cryptohash_update(ctx, config_digest_domain,
						 sizeof(config_digest_domain)) >= 0 &&
		pg_cryptohash_update(ctx, length_le, sizeof(length_le)) >= 0 &&
		pg_cryptohash_update(ctx, config_bytes, config_len) >= 0 &&
		pg_cryptohash_final(ctx, digest,
						PGRAC_FENCED_JOURNAL_DIGEST_BYTES) >= 0;
	pg_cryptohash_free(ctx);
	if (!ok)
		memset(digest, 0, PGRAC_FENCED_JOURNAL_DIGEST_BYTES);
	return ok;
}

void
pgrac_fenced_journal_scan_state_init(PgracFencedJournalScanState *state)
{
	if (state == NULL)
		return;
	memset(state, 0, sizeof(*state));
	state->next_seq = 1;
	state->segment_first_seq = 1;
	state->available = true;
}

/* PGRAC: shared admission predicate for bootstrap and ordinary append. */
bool
pgrac_fenced_journal_has_room(const PgracFencedJournalScanState *state,
	size_t frame_bytes)
{
	return state != NULL && state->available &&
		(frame_bytes == PGRAC_FENCED_JOURNAL_RECORD_BYTES ||
		 frame_bytes == PGRAC_FENCED_JOURNAL_INTENT_BYTES) &&
		state->partial_record_bytes == 0 &&
		state->segment_record_count < PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS &&
		state->valid_bytes <= PGRAC_FENCED_JOURNAL_SEGMENT_BYTES &&
		state->valid_bytes % PGRAC_FENCED_JOURNAL_RECORD_BYTES == 0 &&
		frame_bytes <= PGRAC_FENCED_JOURNAL_SEGMENT_BYTES - state->valid_bytes;
}

PgracFencedJournalScanResult
pgrac_fenced_journal_scan_bytes(const uint8 *bytes, size_t len,
							bool allow_final_partial,
							PgracFencedJournalScanState *state)
{
	PgracFencedJournalScanState next;
	size_t offset = 0;

	if (state == NULL || !state->available || (bytes == NULL && len != 0))
		return PGRAC_FENCED_JOURNAL_SCAN_CORRUPT;
	if (len > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES)
		goto corrupt;
	next = *state;
	next.segment_first_seq = next.next_seq;
	next.segment_record_count = 0;
	next.valid_bytes = 0;
	next.last_record_bytes = 0;
	next.partial_record_bytes = 0;
	while (len - offset >= PGRAC_FENCED_JOURNAL_RECORD_BYTES)
	{
		PgracFencedJournalRecordV1 record;
		uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
		const uint8 *frame = bytes + offset;
		size_t frame_len = pgrac_fenced_journal_frame_size(frame, len - offset);

		if (frame_len == 0)
			goto corrupt;
		if (len - offset < frame_len)
		{
			next.partial_record_bytes = frame_len;
			break;
		}
		if (next.next_seq == 0 ||
			!pgrac_fenced_journal_record_decode(frame, frame_len, &record) ||
			record.seq != next.next_seq ||
			memcmp(record.previous_record_digest,
				   next.previous_record_digest,
				   sizeof(record.previous_record_digest)) != 0 ||
			next.segment_record_count >= PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS ||
			!pgrac_fenced_journal_frame_digest(frame, frame_len, digest))
			goto corrupt;
		memcpy(next.previous_record_digest, digest, sizeof(digest));
		next.segment_record_count++;
		offset += frame_len;
		next.valid_bytes = offset;
		next.last_record_bytes = frame_len;
		if (next.next_seq == UINT64_MAX)
		{
			next.next_seq = 0;
			next.available = false;
			*state = next;
			return PGRAC_FENCED_JOURNAL_SCAN_SEQUENCE_EXHAUSTED;
		}
		next.next_seq++;
	}
	if (offset != len)
	{
		if (!allow_final_partial)
			goto corrupt;
		if (next.partial_record_bytes == 0)
		{
			next.partial_record_bytes = pgrac_fenced_journal_frame_size(bytes + offset, len - offset);
			if (next.partial_record_bytes == 0)
				next.partial_record_bytes = PGRAC_FENCED_JOURNAL_RECORD_BYTES;
		}
		*state = next;
		return PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL;
	}
	*state = next;
	return PGRAC_FENCED_JOURNAL_SCAN_OK;

corrupt:
	state->available = false;
	return PGRAC_FENCED_JOURNAL_SCAN_CORRUPT;
}

PgracFencedJournalAppendResult
pgrac_fenced_journal_append_fd(int fd, PgracFencedJournalScanState *state,
						   PgracFencedJournalRecordV1 *record)
{
	PgracFencedJournalRecordV1 candidate;
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	size_t frame_len;
	ssize_t written;
	int flags;

	if (state == NULL || record == NULL || !state->available ||
		state->next_seq == 0)
		return PGRAC_FENCED_JOURNAL_APPEND_UNAVAILABLE;
	if (state->segment_record_count >= PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS)
		return PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED;
	flags = fd >= 0 ? fcntl(fd, F_GETFL) : -1;
	if (flags < 0 || (flags & O_APPEND) == 0)
		goto unavailable;
	candidate = *record;
	candidate.seq = state->next_seq;
	memcpy(candidate.previous_record_digest, state->previous_record_digest,
		   sizeof(candidate.previous_record_digest));
	if (state->partial_record_bytes != 0 ||
		!pgrac_fenced_journal_frame_encode(&candidate, frame, sizeof(frame), &frame_len))
		goto unavailable;
	if (state->valid_bytes > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES ||
		state->valid_bytes % PGRAC_FENCED_JOURNAL_RECORD_BYTES != 0)
		goto unavailable;
	if (!pgrac_fenced_journal_has_room(state, frame_len))
		return PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED;
	do
	{
		written = write(fd, frame, frame_len);
	} while (written < 0 && errno == EINTR);
	if (written != (ssize_t) frame_len || fsync(fd) != 0 ||
		!pgrac_fenced_journal_frame_digest(frame, frame_len, digest))
		goto unavailable;
	*record = candidate;
	memcpy(state->previous_record_digest, digest, sizeof(digest));
	state->segment_record_count++;
	state->valid_bytes += frame_len;
	state->last_record_bytes = frame_len;
	if (state->next_seq == UINT64_MAX)
	{
		state->next_seq = 0;
		state->available = false;
	}
	else
		state->next_seq++;
	return PGRAC_FENCED_JOURNAL_APPEND_OK;

unavailable:
	state->available = false;
	return PGRAC_FENCED_JOURNAL_APPEND_UNAVAILABLE;
}

bool
pgrac_fenced_journal_repair_partial_tail_fd(
	int fd, PgracFencedJournalScanState *state)
{
	struct stat st;
	int flags;

	if (fd < 0 || state == NULL || !state->available ||
		state->valid_bytes % PGRAC_FENCED_JOURNAL_RECORD_BYTES != 0 ||
		(state->partial_record_bytes != PGRAC_FENCED_JOURNAL_RECORD_BYTES &&
		 state->partial_record_bytes != PGRAC_FENCED_JOURNAL_INTENT_BYTES) ||
		state->valid_bytes > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES)
		goto unavailable;
	flags = fcntl(fd, F_GETFL);
	if (flags < 0 || (flags & O_APPEND) == 0 || fstat(fd, &st) != 0 ||
		!S_ISREG(st.st_mode) || st.st_size <= (off_t) state->valid_bytes ||
		(uint64) st.st_size >=
		state->valid_bytes + state->partial_record_bytes ||
		ftruncate(fd, (off_t) state->valid_bytes) != 0 || fsync(fd) != 0)
		goto unavailable;
	state->partial_record_bytes = 0;
	return true;

unavailable:
	if (state != NULL)
		state->available = false;
	return false;
}

bool
pgrac_fenced_journal_restart_action(
	const PgracFencedJournalRecordV1 *last_record,
	PgracFencedJournalRestartAction *action)
{
	if (action == NULL)
		return false;
	*action = PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE;
	if (last_record == NULL)
		return false;
	if (last_record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_REJOIN)
	{
		/* A lost caller cannot reopen writes for an unfinished rejoin. */
		*action = last_record->target_state == PGRAC_FENCED_JOURNAL_TARGET_ON ?
			PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN :
			PGRAC_FENCED_JOURNAL_RESTART_KEEP_WRITE_DISABLED;
		return true;
	}
	if (last_record->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_NONE &&
		(last_record->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED ||
		 last_record->record_kind == PGRAC_FENCED_JOURNAL_KIND_INVALIDATED))
	{
		*action = PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK;
		return true;
	}
	switch (last_record->record_kind)
	{
		case PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED:
			*action = PGRAC_FENCED_JOURNAL_RESTART_NO_OPERATION;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED:
		case PGRAC_FENCED_JOURNAL_KIND_INVALIDATED:
			*action = PGRAC_FENCED_JOURNAL_RESTART_WAIT_NEW_REQUEST;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED:
		case PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT:
		case PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT:
		case PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED:
			*action = PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED:
			*action = PGRAC_FENCED_JOURNAL_RESTART_KEEP_WRITE_DISABLED;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT:
			if (last_record->target_state != PGRAC_FENCED_JOURNAL_TARGET_ON)
				return false;
			*action = PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_RECONCILED:
			if (last_record->target_state == PGRAC_FENCED_JOURNAL_TARGET_ON)
			{
				*action =
					PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN;
				return true;
			}
			if (last_record->target_state == PGRAC_FENCED_JOURNAL_TARGET_OFF ||
				last_record->target_state ==
				PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN)
			{
				*action = PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK;
				return true;
			}
			return false;
		default:
			return false;
	}
}

void
pgrac_fenced_journal_reconcile_state_init(
	PgracFencedJournalReconcileState *state)
{
	if (state == NULL)
		return;
	memset(state, 0, sizeof(*state));
	state->available = true;
}

static int
reconcile_find_operation(const PgracFencedJournalReconcileState *state,
					 const uint8 operation_id[PGRAC_FENCED_JOURNAL_ID_BYTES])
{
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS; i++)
	{
		if (state->pending[i].used &&
			memcmp(state->pending[i].last_record.operation_id, operation_id,
				PGRAC_FENCED_JOURNAL_ID_BYTES) == 0)
			return (int) i;
	}
	return -1;
}

static void
reconcile_remove_operation(PgracFencedJournalReconcileState *state,
					   const uint8 operation_id[PGRAC_FENCED_JOURNAL_ID_BYTES])
{
	int slot = reconcile_find_operation(state, operation_id);

	if (slot < 0)
		return;
	memset(&state->pending[slot], 0, sizeof(state->pending[slot]));
	state->pending_count--;
}

static bool
reconcile_remember_operation(PgracFencedJournalReconcileState *state,
						 const PgracFencedJournalRecordV1 *record)
{
	uint32 i;
	int slot = reconcile_find_operation(state, record->operation_id);

	if (slot >= 0)
	{
		state->pending[slot].last_record = *record;
		return true;
	}
	if (state->pending_count >= PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS)
		return false;
	for (i = 0; i < PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS; i++)
	{
		if (!state->pending[i].used)
		{
			state->pending[i].used = true;
			state->pending[i].first_seq = record->seq;
			state->pending[i].last_record = *record;
			state->pending_count++;
			return true;
		}
	}
	return false;
}

bool
pgrac_fenced_journal_reconcile_observe(
	PgracFencedJournalReconcileState *state,
	const PgracFencedJournalRecordV1 *record)
{
	int slot;
	const PgracFencedJournalRecordV1 *previous;

	if (state == NULL || record == NULL || !state->available ||
		record->record_kind < PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED ||
		record->record_kind > PGRAC_FENCED_JOURNAL_KIND_RECONCILED)
		return false;
	if (record->record_kind == PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED)
		return true;
	if (bytes_all_zero(record->operation_id, sizeof(record->operation_id)))
		goto unavailable;
	slot = reconcile_find_operation(state, record->operation_id);
	previous = slot >= 0 ? &state->pending[slot].last_record : NULL;
	if (record->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_NONE)
	{
		if (!pgrac_fenced_journal_intent_continues(previous, record))
			goto unavailable;
		if (record->record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED)
			reconcile_remove_operation(state, record->operation_id);
		else if (!reconcile_remember_operation(state, record))
			goto unavailable;
		return true;
	}
	if (previous != NULL && previous->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_NONE)
		goto unavailable;
	switch (record->record_kind)
	{
		case PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED:
		case PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED:
		case PGRAC_FENCED_JOURNAL_KIND_INVALIDATED:
			reconcile_remove_operation(state, record->operation_id);
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED:
		case PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT:
		case PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT:
		case PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED:
		case PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT:
			if (!reconcile_remember_operation(state, record))
				goto unavailable;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_RECONCILED:
			reconcile_remove_operation(state, record->operation_id);
			state->fresh_readback_required = true;
			if (record->target_state == PGRAC_FENCED_JOURNAL_TARGET_ON)
			{
				state->keep_write_disabled = true;
				state->return_off_before_rejoin = true;
			}
			else if (record->target_state !=
					 PGRAC_FENCED_JOURNAL_TARGET_OFF &&
					 record->target_state !=
					 PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN)
				goto unavailable;
			return true;
		case PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED:
			return true;
	}

unavailable:
	state->available = false;
	return false;
}

bool
pgrac_fenced_journal_reconcile_finish(
	PgracFencedJournalReconcileState *state)
{
	PgracFencedJournalRestartAction action;
	uint32 i;

	if (state == NULL || !state->available)
		return false;
	for (i = 0; i < PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS; i++)
	{
		if (!state->pending[i].used)
			continue;
		if (!pgrac_fenced_journal_restart_action(
				&state->pending[i].last_record, &action))
			goto unavailable;
		switch (action)
		{
			case PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK:
				state->fresh_readback_required = true;
				break;
			case PGRAC_FENCED_JOURNAL_RESTART_KEEP_WRITE_DISABLED:
				state->fresh_readback_required = true;
				state->keep_write_disabled = true;
				break;
			case PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN:
				state->fresh_readback_required = true;
				state->keep_write_disabled = true;
				state->return_off_before_rejoin = true;
				break;
			case PGRAC_FENCED_JOURNAL_RESTART_NO_OPERATION:
			case PGRAC_FENCED_JOURNAL_RESTART_WAIT_NEW_REQUEST:
				break;
			case PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE:
				goto unavailable;
		}
	}
	return true;

unavailable:
	state->available = false;
	return false;
}

static bool
sealed_filename(uint64 first_seq, uint64 last_seq,
				const uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES],
				char *name, size_t name_size)
{
	static const char hex[] = "0123456789abcdef";
	char digest_hex[PGRAC_FENCED_JOURNAL_DIGEST_BYTES * 2 + 1];
	size_t i;
	int length;

	if (first_seq == 0 || last_seq < first_seq || digest == NULL ||
		name == NULL || name_size == 0 || bytes_all_zero(digest,
			PGRAC_FENCED_JOURNAL_DIGEST_BYTES))
		return false;
	for (i = 0; i < PGRAC_FENCED_JOURNAL_DIGEST_BYTES; i++)
	{
		digest_hex[i * 2] = hex[digest[i] >> 4];
		digest_hex[i * 2 + 1] = hex[digest[i] & 0x0f];
	}
	digest_hex[sizeof(digest_hex) - 1] = '\0';
	length = snprintf(name, name_size,
				  "journal." UINT64_FORMAT "-" UINT64_FORMAT ".%s.sealed",
				  first_seq, last_seq, digest_hex);
	return length > 0 && (size_t) length < name_size;
}

bool
pgrac_fenced_journal_rotate_at(int directory_fd, int *active_fd,
						   uint32 sealed_count,
						   PgracFencedJournalScanState *state,
						   char *sealed_name, size_t sealed_name_size)
{
	struct stat st;
	uint64 last_seq;
	int new_fd = -1;
	int flags;
	bool renamed = false;

	if (sealed_name != NULL && sealed_name_size > 0)
		sealed_name[0] = '\0';
	if (state == NULL || !state->available)
		return false;
	if (sealed_count >= PGRAC_FENCED_JOURNAL_MAX_SEALED)
		goto unavailable;
	if (directory_fd < 0 || active_fd == NULL || *active_fd < 0 ||
		sealed_name == NULL || sealed_name_size == 0 ||
		state->segment_record_count < PGRAC_FENCED_JOURNAL_MIN_SEALED_RECORDS ||
		state->segment_record_count > PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS ||
		state->valid_bytes < PGRAC_FENCED_JOURNAL_MIN_SEALED_BYTES ||
		state->valid_bytes > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES ||
		state->valid_bytes % PGRAC_FENCED_JOURNAL_RECORD_BYTES != 0 ||
		state->partial_record_bytes != 0 ||
		state->segment_first_seq == 0 || state->next_seq == 0 ||
		state->next_seq <= state->segment_first_seq ||
		state->next_seq - state->segment_first_seq != state->segment_record_count)
		goto unavailable;
	last_seq = state->next_seq - 1;
	if (!sealed_filename(state->segment_first_seq, last_seq,
					 state->previous_record_digest, sealed_name,
					 sealed_name_size) ||
		fstat(*active_fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		(uint64) st.st_size != state->valid_bytes ||
		(st.st_mode & 07777) != 0600)
		goto unavailable;
	flags = fcntl(*active_fd, F_GETFL);
	if (flags < 0 || (flags & O_APPEND) == 0 || fsync(*active_fd) != 0)
		goto unavailable;
	if (fstatat(directory_fd, sealed_name, &st, AT_SYMLINK_NOFOLLOW) == 0 ||
		errno != ENOENT)
		goto unavailable;
	if (renameat(directory_fd, PGRAC_FENCED_JOURNAL_ACTIVE_NAME,
				 directory_fd, sealed_name) != 0)
		goto unavailable;
	renamed = true;
	if (fsync(directory_fd) != 0)
		goto unavailable;
	new_fd = openat(directory_fd, PGRAC_FENCED_JOURNAL_ACTIVE_NAME,
				O_RDWR | O_CREAT | O_EXCL | O_APPEND | O_NOFOLLOW, 0600);
	if (new_fd < 0 || fchmod(new_fd, 0600) != 0 || fsync(new_fd) != 0 ||
		fsync(directory_fd) != 0)
		goto unavailable;
	(void) close(*active_fd);
	*active_fd = new_fd;
	state->segment_first_seq = state->next_seq;
	state->segment_record_count = 0;
	state->valid_bytes = 0;
	state->last_record_bytes = 0;
	state->partial_record_bytes = 0;
	return true;

unavailable:
	if (new_fd >= 0)
		(void) close(new_fd);
	state->available = false;
	if (!renamed && sealed_name != NULL && sealed_name_size > 0)
		sealed_name[0] = '\0';
	return false;
}
