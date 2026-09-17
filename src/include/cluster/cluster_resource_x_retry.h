/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_retry.h
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 * The state is local shared-memory state.  It is neither a wire record nor
 * part of Resource-X logical attempt identity.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RESOURCE_X_RETRY_H
#define CLUSTER_RESOURCE_X_RETRY_H

#include "cluster/cluster_resource_x_identity.h"

#define RESOURCE_X_RETRY_MAX_RETRIES 8
#define RESOURCE_X_RETRY_MIN_BACKOFF_MS 1
#define RESOURCE_X_RETRY_MAX_BACKOFF_MS 5000

typedef enum ResourceXRetryPhase {
	RESOURCE_X_RETRY_PRE_NO_RETURN = 1,
	RESOURCE_X_RETRY_POST_NO_RETURN = 2,
	RESOURCE_X_RETRY_TERMINAL = 3,
	RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED = 4
} ResourceXRetryPhase;

typedef struct ResourceXRetryStateV1 {
	ResourceXAttemptWitness attempt;
	uint64 first_submit_mono_us;
	uint64 next_retry_due_mono_us;
	uint64 terminal_deadline_mono_us;
	uint32 retry_count;
	uint32 terminal_errcode;
	uint16 last_phase;
	/* Mixed-radix snapshot: max_retries * 5000 + (backoff_ms - 1). */
	uint16 flags;
	uint32 state_generation;
} ResourceXRetryStateV1;

typedef enum ResourceXRetryDecision {
	RESOURCE_X_RETRY_NOT_DUE = 0,
	RESOURCE_X_RETRY_STAGE_EXACT,
	RESOURCE_X_RETRY_WAIT_SCHEDULER,
	RESOURCE_X_RETRY_TERMINAL_DENIED,
	RESOURCE_X_RETRY_TERMINAL_EXHAUSTED,
	RESOURCE_X_RETRY_ROLL_FORWARD,
	RESOURCE_X_RETRY_RECOVERY_BLOCKED
} ResourceXRetryDecision;

typedef struct ResourceXRetryAction {
	ResourceXAttemptWitness attempt;
	ResourceXTransportWitness transport;
	uint32 expected_state_generation;
	uint32 expected_retry_count;
} ResourceXRetryAction;

typedef enum ResourceXRetryApplyResult {
	RESOURCE_X_RETRY_APPLY_APPLIED = 0,
	RESOURCE_X_RETRY_APPLY_DUPLICATE,
	RESOURCE_X_RETRY_APPLY_STALE,
	RESOURCE_X_RETRY_APPLY_ROLL_FORWARD,
	RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED
} ResourceXRetryApplyResult;

/* Internal terminal classifications retained by Resource-X.  These values
 * are not wire encodings. */
typedef enum ResourceXTerminalReason {
	RESOURCE_X_TERMINAL_REASON_INVALID = 0,
	RESOURCE_X_TERMINAL_REASON_LEGACY_CANCEL,
	RESOURCE_X_TERMINAL_REASON_RETRY_EXHAUSTED,
	RESOURCE_X_TERMINAL_REASON_INVALIDATE_TIMEOUT,
	RESOURCE_X_TERMINAL_REASON_LOST_WRITE
} ResourceXTerminalReason;

/* Immutable per-attempt terminal tombstone.  It retains the complete exact
 * state so a successor cannot read a predecessor result by resource alone. */
typedef struct ResourceXTerminalRecordV1 {
	ResourceXRetryStateV1 state;
} ResourceXTerminalRecordV1;

StaticAssertDecl(sizeof(ResourceXRetryStateV1) == 72, "Resource-X retry state ABI");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, attempt) == 0, "Resource-X retry attempt offset");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, first_submit_mono_us) == 32
					 && offsetof(ResourceXRetryStateV1, next_retry_due_mono_us) == 40
					 && offsetof(ResourceXRetryStateV1, terminal_deadline_mono_us) == 48,
				 "Resource-X retry deadline offsets");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, retry_count) == 56
					 && offsetof(ResourceXRetryStateV1, terminal_errcode) == 60,
				 "Resource-X retry result offsets");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, last_phase) == 64
					 && offsetof(ResourceXRetryStateV1, flags) == 66
					 && offsetof(ResourceXRetryStateV1, state_generation) == 68,
				 "Resource-X retry lifecycle offsets");
StaticAssertDecl(sizeof(ResourceXRetryAction) == 64, "Resource-X retry action ABI");
StaticAssertDecl(offsetof(ResourceXRetryAction, attempt) == 0
					 && offsetof(ResourceXRetryAction, transport) == 32
					 && offsetof(ResourceXRetryAction, expected_state_generation) == 56
					 && offsetof(ResourceXRetryAction, expected_retry_count) == 60,
				 "Resource-X retry action offsets");
StaticAssertDecl(sizeof(ResourceXTerminalRecordV1) == 72, "Resource-X terminal record ABI");

extern bool resource_x_retry_state_init(const ResourceXAttemptWitness *attempt,
										uint64 first_submit_mono_us,
										uint64 terminal_deadline_mono_us, uint32 max_retries,
										uint32 initial_backoff_ms, uint32 state_generation,
										ResourceXRetryStateV1 *out);
extern void resource_x_retry_state_clear(ResourceXRetryStateV1 *state);
extern bool resource_x_retry_state_is_clear(const ResourceXRetryStateV1 *state);
extern ResourceXTerminalReason resource_x_terminal_reason_decode(uint32 reason);
extern void resource_x_terminal_record_clear(ResourceXTerminalRecordV1 *record);
extern bool resource_x_terminal_record_is_clear(const ResourceXTerminalRecordV1 *record);
extern bool resource_x_terminal_record_publish(const ResourceXRetryStateV1 *terminal,
											   ResourceXTerminalRecordV1 *record_out);
extern bool resource_x_terminal_record_replay(const ResourceXTerminalRecordV1 *record,
											  const ResourceXAttemptWitness *attempt,
											  ResourceXRetryStateV1 *state_out);
extern bool resource_x_retry_policy_exact(const ResourceXRetryStateV1 *state,
										  uint32 *max_retries_out, uint32 *initial_backoff_ms_out);
extern bool resource_x_retry_next_due_exact(const ResourceXRetryStateV1 *state,
											uint64 last_admitted_mono_us,
											uint32 admitted_retry_count, uint64 *next_due_out);
extern ResourceXRetryApplyResult
resource_x_retry_terminalize_exact(const ResourceXRetryStateV1 *current,
								   const ResourceXRetryStateV1 *expected, uint32 terminal_errcode,
								   uint64 terminal_at_mono_us, ResourceXRetryStateV1 *terminal_out);
extern ResourceXRetryDecision
resource_x_retry_classify_exact(const ResourceXRetryStateV1 *state,
								const ResourceXAttemptWitness *current_attempt,
								const ResourceXTransportWitness *fresh_transport,
								uint64 now_mono_us, ResourceXRetryAction *out);

#endif /* CLUSTER_RESOURCE_X_RETRY_H */
