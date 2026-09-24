/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_ctl.c
 *	  Pure fixed-frame semantics shared by pgrac-fencedctl and unit tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_ctl.h"

#define PGRAC_FENCED_CTL_MAX_NODES 128

static bool
bytes_all_zero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

bool
pgrac_fenced_ctl_prepare_rejoin_valid(
	const PgracFencedCtlPrepareRejoinV1 *request)
{
	return request != NULL && request->node_id >= 0 &&
		request->node_id < PGRAC_FENCED_CTL_MAX_NODES &&
		request->old_incarnation != 0 &&
		request->candidate_incarnation > request->old_incarnation &&
		request->timeout_ms >= PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS &&
		request->timeout_ms <= PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS;
}

bool
pgrac_fenced_ctl_prepare_rejoin_frame(
	const PgracFencedCtlPrepareRejoinV1 *request,
	const uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES])
{
	PgracExternalFenceProtocolRejoinFrameV1 rejoin;

	if (!pgrac_fenced_ctl_prepare_rejoin_valid(request) ||
		transport_nonce == NULL || frame == NULL ||
		bytes_all_zero(transport_nonce,
			PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES))
		return false;
	memset(&rejoin, 0, sizeof(rejoin));
	rejoin.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	memcpy(rejoin.transport_nonce, transport_nonce,
		   sizeof(rejoin.transport_nonce));
	rejoin.old_node_id = request->node_id;
	rejoin.old_incarnation = request->old_incarnation;
	rejoin.candidate_incarnation = request->candidate_incarnation;
	rejoin.timeout_ms = request->timeout_ms;
	return pgrac_external_fence_rejoin_v1_encode(&rejoin, frame);
}

bool
pgrac_fenced_ctl_prepare_rejoin_response(
	const PgracFencedCtlPrepareRejoinV1 *request,
	const uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	const uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES],
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracExternalFenceProtocolRejoinFrameV1 decoded;

	if (response != NULL)
		memset(response, 0, sizeof(*response));
	if (!pgrac_fenced_ctl_prepare_rejoin_valid(request) ||
		transport_nonce == NULL || frame == NULL || response == NULL ||
		bytes_all_zero(transport_nonce,
			PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES) ||
		!pgrac_external_fence_rejoin_v1_decode(frame,
			PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES, &decoded))
		return false;

	/* ADMIN_PREPARE_RESULT is an inert offer acknowledgement.  It echoes the
	 * exact tuple and nonce but carries no provider, proof, root, mapping or
	 * target-state authority. */
	if (decoded.opcode !=
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT ||
		memcmp(decoded.transport_nonce, transport_nonce,
			   sizeof(decoded.transport_nonce)) != 0 ||
		decoded.old_node_id != request->node_id ||
		decoded.old_incarnation != request->old_incarnation ||
		decoded.candidate_incarnation != request->candidate_incarnation ||
		decoded.timeout_ms != 0 || decoded.system_identifier != 0 ||
		!bytes_all_zero(decoded.rejoin_gate_digest,
			sizeof(decoded.rejoin_gate_digest)) ||
		!bytes_all_zero(decoded.protected_set_digest,
			sizeof(decoded.protected_set_digest)) ||
		decoded.provider_id != 0 || decoded.provider_abi_version != 0 ||
		decoded.target_mapping_generation != 0 ||
		!bytes_all_zero(decoded.daemon_boot_id,
			sizeof(decoded.daemon_boot_id)) ||
		decoded.journal_seq != 0 || decoded.verified_mono_ns != 0 ||
		decoded.fresh_until_mono_ns != 0 ||
		decoded.proof_generation != 0 ||
		!bytes_all_zero(decoded.target_state_digest,
			sizeof(decoded.target_state_digest)))
		return false;

	*response = decoded;
	return true;
}

bool
pgrac_fenced_ctl_journal_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISREG(st->st_mode) && st->st_uid == 0 &&
		st->st_gid == 0 && (st->st_mode & 07777) == 0600 &&
		st->st_size >= 0 &&
		(uint64) st->st_size <= PGRAC_FENCED_JOURNAL_SEGMENT_BYTES &&
		(uint64) st->st_size % PGRAC_FENCED_JOURNAL_RECORD_BYTES == 0;
}

bool
pgrac_fenced_ctl_journal_scan(const uint8 *bytes, size_t len,
						  PgracFencedCtlJournalSummaryV1 *summary)
{
	PgracFencedJournalRecordV1 first;
	PgracFencedJournalScanState state;
	size_t first_len;

	if (summary == NULL || (bytes == NULL && len != 0) ||
		len > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES ||
		len % PGRAC_FENCED_JOURNAL_RECORD_BYTES != 0)
		return false;
	memset(summary, 0, sizeof(*summary));
	if (len == 0)
		return true;
	first_len = pgrac_fenced_journal_frame_size(bytes, len);
	if (first_len == 0 || first_len > len ||
		!pgrac_fenced_journal_record_decode(bytes, first_len, &first))
		return false;

	pgrac_fenced_journal_scan_state_init(&state);
	state.next_seq = first.seq;
	if (first.seq != 1)
		memcpy(state.previous_record_digest, first.previous_record_digest,
			sizeof(state.previous_record_digest));
	if (pgrac_fenced_journal_scan_bytes(bytes, len, false, &state) !=
		PGRAC_FENCED_JOURNAL_SCAN_OK || state.next_seq == 0)
		return false;

	summary->first_seq = first.seq;
	summary->last_seq = state.next_seq - 1;
	summary->record_count = state.segment_record_count;
	memcpy(summary->tail_digest, state.previous_record_digest,
		sizeof(summary->tail_digest));
	return true;
}
