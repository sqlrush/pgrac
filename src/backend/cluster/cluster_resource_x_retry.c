/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_retry.c
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_retry.h"

static uint64
resource_x_retry_saturating_add(uint64 base, uint64 delta)
{
	return base > UINT64_MAX - delta ? UINT64_MAX : base + delta;
}


static bool
resource_x_retry_policy_encode(uint32 max_retries, uint32 initial_backoff_ms, uint16 *encoded_out)
{
	uint32 encoded;

	if (encoded_out == NULL || max_retries > RESOURCE_X_RETRY_MAX_RETRIES
		|| initial_backoff_ms < RESOURCE_X_RETRY_MIN_BACKOFF_MS
		|| initial_backoff_ms > RESOURCE_X_RETRY_MAX_BACKOFF_MS)
		return false;
	encoded = max_retries * RESOURCE_X_RETRY_MAX_BACKOFF_MS
			  + (initial_backoff_ms - RESOURCE_X_RETRY_MIN_BACKOFF_MS);
	if (encoded > UINT16_MAX)
		return false;
	*encoded_out = (uint16)encoded;
	return true;
}


static bool
resource_x_retry_policy_decode(uint16 encoded, uint32 *max_retries_out,
							   uint32 *initial_backoff_ms_out)
{
	uint32 max_retries;
	uint32 initial_backoff_ms;

	if (max_retries_out == NULL || initial_backoff_ms_out == NULL)
		return false;
	max_retries = encoded / RESOURCE_X_RETRY_MAX_BACKOFF_MS;
	initial_backoff_ms
		= (encoded % RESOURCE_X_RETRY_MAX_BACKOFF_MS) + RESOURCE_X_RETRY_MIN_BACKOFF_MS;
	if (max_retries > RESOURCE_X_RETRY_MAX_RETRIES)
		return false;
	*max_retries_out = max_retries;
	*initial_backoff_ms_out = initial_backoff_ms;
	return true;
}


static bool
resource_x_retry_state_valid(const ResourceXRetryStateV1 *state)
{
	uint32 initial_backoff_ms;
	uint32 max_retries;

	if (state == NULL || state->state_generation == 0
		|| !resource_x_attempt_matches(&state->attempt, &state->attempt)
		|| state->first_submit_mono_us == 0
		|| state->next_retry_due_mono_us < state->first_submit_mono_us
		|| state->terminal_deadline_mono_us < state->next_retry_due_mono_us
		|| !resource_x_retry_policy_decode(state->flags, &max_retries, &initial_backoff_ms)
		|| state->retry_count > max_retries || state->last_phase < RESOURCE_X_RETRY_PRE_NO_RETURN
		|| state->last_phase > RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED)
		return false;
	if (state->last_phase == RESOURCE_X_RETRY_TERMINAL)
		return state->terminal_errcode != 0;
	return state->terminal_errcode == 0;
}

ResourceXTerminalReason
resource_x_terminal_reason_decode(uint32 reason)
{
	switch (reason) {
	case 0:
		return RESOURCE_X_TERMINAL_REASON_LEGACY_CANCEL;
	case ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED:
		return RESOURCE_X_TERMINAL_REASON_RETRY_EXHAUSTED;
	case ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT:
		return RESOURCE_X_TERMINAL_REASON_INVALIDATE_TIMEOUT;
	case ERRCODE_CLUSTER_LOST_WRITE_DETECTED:
		return RESOURCE_X_TERMINAL_REASON_LOST_WRITE;
	default:
		return RESOURCE_X_TERMINAL_REASON_INVALID;
	}
}

void
resource_x_terminal_record_clear(ResourceXTerminalRecordV1 *record)
{
	if (record != NULL)
		memset(record, 0, sizeof(*record));
}

bool
resource_x_terminal_record_is_clear(const ResourceXTerminalRecordV1 *record)
{
	ResourceXTerminalRecordV1 zero;

	if (record == NULL)
		return false;
	memset(&zero, 0, sizeof(zero));
	return memcmp(record, &zero, sizeof(zero)) == 0;
}

bool
resource_x_terminal_record_publish(const ResourceXRetryStateV1 *terminal,
								   ResourceXTerminalRecordV1 *record_out)
{
	ResourceXTerminalRecordV1 record;

	if (terminal == NULL || record_out == NULL || !resource_x_retry_state_valid(terminal)
		|| terminal->last_phase != RESOURCE_X_RETRY_TERMINAL
		|| terminal->next_retry_due_mono_us != terminal->terminal_deadline_mono_us)
		return false;
	memset(&record, 0, sizeof(record));
	record.state = *terminal;
	*record_out = record;
	return true;
}

bool
resource_x_terminal_record_replay(const ResourceXTerminalRecordV1 *record,
								  const ResourceXAttemptWitness *attempt,
								  ResourceXRetryStateV1 *state_out)
{
	resource_x_retry_state_clear(state_out);
	if (record == NULL || attempt == NULL || state_out == NULL
		|| !resource_x_retry_state_valid(&record->state)
		|| record->state.last_phase != RESOURCE_X_RETRY_TERMINAL
		|| record->state.next_retry_due_mono_us != record->state.terminal_deadline_mono_us
		|| !resource_x_attempt_matches(&record->state.attempt, attempt))
		return false;
	*state_out = record->state;
	return true;
}

static bool
resource_x_retry_transport_valid(const ResourceXTransportWitness *transport)
{
	return transport != NULL && transport->cluster_epoch != 0
		   && transport->peer_session_incarnation != 0 && transport->connection_generation != 0
		   && transport->flags == 0;
}

bool
resource_x_retry_state_init(const ResourceXAttemptWitness *attempt, uint64 first_submit_mono_us,
							uint64 terminal_deadline_mono_us, uint32 max_retries,
							uint32 initial_backoff_ms, uint32 state_generation,
							ResourceXRetryStateV1 *out)
{
	ResourceXRetryStateV1 candidate;
	uint64 initial_delay_us;
	uint64 next_retry_due_mono_us;
	uint16 encoded_policy;

	if (attempt == NULL || out == NULL || !resource_x_attempt_matches(attempt, attempt)
		|| first_submit_mono_us == 0 || terminal_deadline_mono_us < first_submit_mono_us
		|| !resource_x_retry_policy_encode(max_retries, initial_backoff_ms, &encoded_policy)
		|| state_generation == 0)
		return false;
	initial_delay_us = (uint64)initial_backoff_ms * UINT64_C(1000);
	next_retry_due_mono_us
		= resource_x_retry_saturating_add(first_submit_mono_us, initial_delay_us);
	if (next_retry_due_mono_us > terminal_deadline_mono_us)
		next_retry_due_mono_us = terminal_deadline_mono_us;
	memset(&candidate, 0, sizeof(candidate));
	candidate.attempt = *attempt;
	candidate.first_submit_mono_us = first_submit_mono_us;
	candidate.next_retry_due_mono_us = next_retry_due_mono_us;
	candidate.terminal_deadline_mono_us = terminal_deadline_mono_us;
	candidate.last_phase = RESOURCE_X_RETRY_PRE_NO_RETURN;
	candidate.flags = encoded_policy;
	candidate.state_generation = state_generation;
	*out = candidate;
	return true;
}

void
resource_x_retry_state_clear(ResourceXRetryStateV1 *state)
{
	if (state != NULL)
		memset(state, 0, sizeof(*state));
}

bool
resource_x_retry_state_is_clear(const ResourceXRetryStateV1 *state)
{
	ResourceXRetryStateV1 zero;

	if (state == NULL)
		return false;
	memset(&zero, 0, sizeof(zero));
	return memcmp(state, &zero, sizeof(zero)) == 0;
}


bool
resource_x_retry_policy_exact(const ResourceXRetryStateV1 *state, uint32 *max_retries_out,
							  uint32 *initial_backoff_ms_out)
{
	return state != NULL
		   && resource_x_retry_policy_decode(state->flags, max_retries_out, initial_backoff_ms_out);
}


bool
resource_x_retry_next_due_exact(const ResourceXRetryStateV1 *state, uint64 last_admitted_mono_us,
								uint32 admitted_retry_count, uint64 *next_due_out)
{
	uint64 delay_us;
	uint64 next_due;
	uint32 initial_backoff_ms;
	uint32 max_retries;

	if (next_due_out == NULL || !resource_x_retry_state_valid(state)
		|| !resource_x_retry_policy_exact(state, &max_retries, &initial_backoff_ms)
		|| admitted_retry_count > max_retries || last_admitted_mono_us < state->first_submit_mono_us
		|| last_admitted_mono_us > state->terminal_deadline_mono_us)
		return false;
	delay_us = (uint64)initial_backoff_ms * UINT64_C(1000);
	if (admitted_retry_count >= 64 || delay_us > (UINT64_MAX >> admitted_retry_count))
		delay_us = UINT64_MAX;
	else
		delay_us <<= admitted_retry_count;
	next_due = resource_x_retry_saturating_add(last_admitted_mono_us, delay_us);
	if (next_due > state->terminal_deadline_mono_us)
		next_due = state->terminal_deadline_mono_us;
	*next_due_out = next_due;
	return true;
}


ResourceXRetryApplyResult
resource_x_retry_terminalize_exact(const ResourceXRetryStateV1 *current,
								   const ResourceXRetryStateV1 *expected, uint32 terminal_errcode,
								   uint64 terminal_at_mono_us, ResourceXRetryStateV1 *terminal_out)
{
	ResourceXRetryStateV1 terminal;

	resource_x_retry_state_clear(terminal_out);
	if (current == NULL || expected == NULL || terminal_out == NULL || terminal_errcode == 0
		|| terminal_at_mono_us == 0
		|| resource_x_terminal_reason_decode(terminal_errcode) == RESOURCE_X_TERMINAL_REASON_INVALID
		|| !resource_x_retry_state_valid(current) || !resource_x_retry_state_valid(expected))
		return RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED;
	if (current->last_phase == RESOURCE_X_RETRY_TERMINAL) {
		if (!resource_x_attempt_matches(&current->attempt, &expected->attempt))
			return RESOURCE_X_RETRY_APPLY_STALE;
		if (current->terminal_errcode != terminal_errcode)
			return RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED;
		*terminal_out = *current;
		return RESOURCE_X_RETRY_APPLY_DUPLICATE;
	}
	if (!resource_x_attempt_matches(&current->attempt, &expected->attempt)
		|| current->state_generation != expected->state_generation
		|| current->retry_count != expected->retry_count)
		return RESOURCE_X_RETRY_APPLY_STALE;
	if (current->last_phase == RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED
		|| current->state_generation == PG_UINT32_MAX
		|| terminal_at_mono_us < current->first_submit_mono_us)
		return RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED;
	if (current->last_phase == RESOURCE_X_RETRY_POST_NO_RETURN)
		return RESOURCE_X_RETRY_APPLY_ROLL_FORWARD;
	if (current->last_phase != RESOURCE_X_RETRY_PRE_NO_RETURN)
		return RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED;
	terminal = *current;
	terminal.next_retry_due_mono_us = terminal_at_mono_us;
	terminal.terminal_deadline_mono_us = terminal_at_mono_us;
	terminal.terminal_errcode = terminal_errcode;
	terminal.last_phase = RESOURCE_X_RETRY_TERMINAL;
	terminal.state_generation++;
	*terminal_out = terminal;
	return RESOURCE_X_RETRY_APPLY_APPLIED;
}

ResourceXRetryDecision
resource_x_retry_classify_exact(const ResourceXRetryStateV1 *state,
								const ResourceXAttemptWitness *current_attempt,
								const ResourceXTransportWitness *fresh_transport,
								uint64 now_mono_us, ResourceXRetryAction *out)
{
	ResourceXRetryAction action;
	uint32 initial_backoff_ms;
	uint32 max_retries;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !resource_x_retry_state_valid(state)
		|| !resource_x_attempt_matches(&state->attempt, current_attempt))
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (fresh_transport != NULL && fresh_transport->flags != 0)
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (state->last_phase == RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED)
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (state->last_phase == RESOURCE_X_RETRY_TERMINAL)
		return RESOURCE_X_RETRY_TERMINAL_DENIED;
	if (now_mono_us >= state->terminal_deadline_mono_us)
		return state->last_phase == RESOURCE_X_RETRY_PRE_NO_RETURN
				   ? RESOURCE_X_RETRY_TERMINAL_EXHAUSTED
				   : RESOURCE_X_RETRY_ROLL_FORWARD;
	if (!resource_x_retry_policy_exact(state, &max_retries, &initial_backoff_ms))
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (state->retry_count >= max_retries)
		return state->last_phase == RESOURCE_X_RETRY_PRE_NO_RETURN
				   ? RESOURCE_X_RETRY_TERMINAL_EXHAUSTED
				   : RESOURCE_X_RETRY_ROLL_FORWARD;
	if (now_mono_us < state->next_retry_due_mono_us)
		return RESOURCE_X_RETRY_NOT_DUE;
	if (!resource_x_retry_transport_valid(fresh_transport))
		return RESOURCE_X_RETRY_WAIT_SCHEDULER;

	memset(&action, 0, sizeof(action));
	action.attempt = state->attempt;
	action.transport = *fresh_transport;
	action.expected_state_generation = state->state_generation;
	action.expected_retry_count = state->retry_count;
	*out = action;
	return RESOURCE_X_RETRY_STAGE_EXACT;
}
