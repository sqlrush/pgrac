/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_operation.h
 *    Provider-neutral scalar ACQUIRE operation core.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_OPERATION_H
#define PGRAC_FENCED_OPERATION_H

#include "c.h"

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_journal.h"
#include "pgrac_fenced_provider.h"

typedef bool (*PgracFencedJournalAppendHookV1)(
	void *argument,
	PgracFencedJournalRecordV1 *record);

typedef bool (*PgracFencedProofReserveHookV1)(
	void *argument,
	uint64 *proof_generation);

typedef enum PgracFencedOperationAcceptResult
{
	PGRAC_FENCED_OPERATION_ERROR = 0,
	PGRAC_FENCED_OPERATION_COMPLETE = 1,
	PGRAC_FENCED_OPERATION_READY = 2
} PgracFencedOperationAcceptResult;

typedef struct PgracFencedPreparedAcquireV1
{
	PgracExternalFenceProtocolBindingV1 binding;
	PgracFencedTargetV1 target;
	uint8 binding_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint64 accepted_mono_ns;
	uint64 accepted_journal_seq;
	/* PGRAC: immutable durable identity, independent of client-slot lifetime. */
	PgracFencedJournalRecordV1 accepted_record;
} PgracFencedPreparedAcquireV1;

typedef struct PgracFencedOperationContextV1
{
	const PgracFencedConfigV1 *config;
	const PgracFencedProviderOpsV1 *provider;
	bool allow_test_only;
	uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
	uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	int journal_fd;
	int journal_directory_fd;
	uint32 sealed_count;
	PgracFencedJournalScanState *journal_state;
	uint64 next_proof_generation;
	PgracFencedJournalAppendHookV1 journal_append_hook;
	void *journal_append_argument;
	PgracFencedProofReserveHookV1 proof_reserve_hook;
	void *proof_reserve_argument;
	bool restart_fresh_readback_required;
	bool restart_keep_write_disabled;
	bool restart_return_off_before_rejoin;
	bool available;
	const PgracFencedConfigV1 *pending_config;
	const PgracFencedProviderOpsV1 *pending_provider;
	uint8 pending_semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
} PgracFencedOperationContextV1;

extern bool pgrac_fenced_operation_context_init(
	PgracFencedOperationContextV1 *context,
	const PgracFencedConfigV1 *config,
	const PgracFencedProviderOpsV1 *provider,
	bool allow_test_only,
	const uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES],
	const uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	int journal_fd,
	PgracFencedJournalScanState *journal_state);
extern bool pgrac_fenced_operation_acquire(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response);
extern PgracFencedOperationAcceptResult pgrac_fenced_operation_accept(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	uint64 deadline_mono_ns,
	PgracFencedPreparedAcquireV1 *prepared,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_fenced_operation_execute_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_fenced_operation_prepared_matches(
	const PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared);
extern bool pgrac_fenced_operation_serve_joiner(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	const PgracExternalFenceProtocolResponseV1 *source_response,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_fenced_operation_cancel_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint32 deny_reason,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_fenced_operation_enable_rotation(
	PgracFencedOperationContextV1 *context,
	int journal_directory_fd,
	uint32 sealed_count);
extern bool pgrac_fenced_operation_set_journal_append_hook(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalAppendHookV1 hook,
	void *argument);
extern bool pgrac_fenced_operation_set_proof_reserve_hook(
	PgracFencedOperationContextV1 *context,
	PgracFencedProofReserveHookV1 hook,
	void *argument);
extern bool pgrac_fenced_operation_append_journal(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalRecordV1 *record);
extern bool pgrac_fenced_operation_reserve_proof_generation(
	PgracFencedOperationContextV1 *context,
	uint64 *proof_generation);
extern bool pgrac_fenced_operation_reconcile_startup(
	PgracFencedOperationContextV1 *context,
	PgracFencedJournalReconcileState *reconcile);
extern bool pgrac_fenced_operation_prepare_mapping_reload(
	PgracFencedOperationContextV1 *context,
	const PgracFencedConfigV1 *config,
	const PgracFencedProviderOpsV1 *provider,
	const uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES]);
extern bool pgrac_fenced_operation_activate_mapping_reload(
	PgracFencedOperationContextV1 *context);
extern bool pgrac_fenced_operation_invalidate(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint32 deny_reason);

#endif /* PGRAC_FENCED_OPERATION_H */
