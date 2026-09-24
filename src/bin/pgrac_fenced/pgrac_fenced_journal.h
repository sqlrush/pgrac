/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_journal.h
 *	  Exact corruption-evident local journal ABI for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_JOURNAL_H
#define PGRAC_FENCED_JOURNAL_H

#include "c.h"
#include "common/pgrac_external_fence_protocol.h"

#define PGRAC_FENCED_JOURNAL_RECORD_BYTES 256U
#define PGRAC_FENCED_JOURNAL_INTENT_BYTES 768U
#define PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES PGRAC_FENCED_JOURNAL_INTENT_BYTES
#define PGRAC_FENCED_JOURNAL_DIGEST_BYTES 32U
#define PGRAC_FENCED_JOURNAL_ID_BYTES 16U
#define PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS UINT32_C(262144)
#define PGRAC_FENCED_JOURNAL_SEGMENT_BYTES UINT64_C(67108864)
#define PGRAC_FENCED_JOURNAL_MIN_SEALED_BYTES \
	(PGRAC_FENCED_JOURNAL_SEGMENT_BYTES - PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES + \
	 PGRAC_FENCED_JOURNAL_RECORD_BYTES)
#define PGRAC_FENCED_JOURNAL_MIN_SEALED_RECORDS \
	((PGRAC_FENCED_JOURNAL_MIN_SEALED_BYTES + PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES - 1) / \
	 PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES)
#define PGRAC_FENCED_JOURNAL_MAX_SEALED UINT32_C(8)
#define PGRAC_FENCED_JOURNAL_ACTIVE_NAME "journal.active"
#define PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX 128U
#define PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS UINT32_C(128)

typedef enum PgracFencedJournalRecordKind
{
	PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED = 1,
	PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED = 2,
	PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED = 3,
	PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT = 4,
	PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT = 5,
	PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED = 6,
	PGRAC_FENCED_JOURNAL_KIND_INVALIDATED = 7,
	PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED = 8,
	PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT = 9,
	PGRAC_FENCED_JOURNAL_KIND_RECONCILED = 10
} PgracFencedJournalRecordKind;

typedef enum PgracFencedJournalProviderResult
{
	PGRAC_FENCED_JOURNAL_PROVIDER_OK = 0,
	PGRAC_FENCED_JOURNAL_PROVIDER_PENDING = 1,
	PGRAC_FENCED_JOURNAL_PROVIDER_REJECTED = 2,
	PGRAC_FENCED_JOURNAL_PROVIDER_UNKNOWN = 3,
	PGRAC_FENCED_JOURNAL_PROVIDER_UNAVAILABLE = 4,
	PGRAC_FENCED_JOURNAL_PROVIDER_CONFIG_ERROR = 5,
	PGRAC_FENCED_JOURNAL_PROVIDER_IO_ERROR = 6
} PgracFencedJournalProviderResult;

typedef enum PgracFencedJournalTargetState
{
	PGRAC_FENCED_JOURNAL_TARGET_NONE = 0,
	PGRAC_FENCED_JOURNAL_TARGET_OFF = 1,
	PGRAC_FENCED_JOURNAL_TARGET_ON = 2,
	PGRAC_FENCED_JOURNAL_TARGET_TRANSITIONING = 3,
	PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN = 4
} PgracFencedJournalTargetState;

typedef enum PgracFencedJournalRestartAction
{
	PGRAC_FENCED_JOURNAL_RESTART_NO_OPERATION = 0,
	PGRAC_FENCED_JOURNAL_RESTART_WAIT_NEW_REQUEST = 1,
	PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK = 2,
	PGRAC_FENCED_JOURNAL_RESTART_KEEP_WRITE_DISABLED = 3,
	PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN = 4,
	PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE = 5
} PgracFencedJournalRestartAction;

typedef enum PgracFencedJournalScanResult
{
	PGRAC_FENCED_JOURNAL_SCAN_OK = 0,
	PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL = 1,
	PGRAC_FENCED_JOURNAL_SCAN_CORRUPT = 2,
	PGRAC_FENCED_JOURNAL_SCAN_SEQUENCE_EXHAUSTED = 3
} PgracFencedJournalScanResult;

typedef enum PgracFencedJournalAppendResult
{
	PGRAC_FENCED_JOURNAL_APPEND_OK = 0,
	PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED = 1,
	PGRAC_FENCED_JOURNAL_APPEND_UNAVAILABLE = 2
} PgracFencedJournalAppendResult;

/* PGRAC: semantic intent, never a wire struct or an isolation certificate. */
typedef enum PgracFencedJournalIntentKind
{
	PGRAC_FENCED_JOURNAL_INTENT_NONE = 0,
	PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE = 1,
	PGRAC_FENCED_JOURNAL_INTENT_REJOIN = 2
} PgracFencedJournalIntentKind;

typedef struct PgracFencedJournalIntentV2
{
	uint16 kind;
	uint8 target_uuid[16];
	uint64 attempt;
	uint64 system_identifier;
	uint8 protected_set_digest[32];
	union
	{
		PgracExternalFenceProtocolRequestV1 acquire;
		PgracExternalFenceProtocolRejoinFrameV1 rejoin;
	} request;
} PgracFencedJournalIntentV2;

typedef struct PgracFencedJournalRecordV1
{
	uint16 record_kind;
	uint64 seq;
	uint8 daemon_boot_id[PGRAC_FENCED_JOURNAL_ID_BYTES];
	uint8 operation_id[PGRAC_FENCED_JOURNAL_ID_BYTES];
	uint8 previous_record_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	uint8 binding_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	uint16 provider_id;
	uint16 provider_abi_version;
	uint32 provider_result;
	int32 provider_native_status;
	uint32 target_state;
	uint64 event_mono_ns;
	uint64 fresh_until_mono_ns;
	uint64 mapping_generation;
	uint64 proof_generation;
	uint8 target_state_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	uint8 semantic_config_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	uint32 deny_reason;
	uint32 io_drain_state;
	/* Zero for v1 events; complete persistent identity for v2 events. */
	PgracFencedJournalIntentV2 intent;
} PgracFencedJournalRecordV1;

typedef struct PgracFencedJournalScanState
{
	uint64 next_seq;
	uint64 segment_first_seq;
	uint32 segment_record_count;
	size_t valid_bytes;
	size_t last_record_bytes;
	size_t partial_record_bytes;
	uint8 previous_record_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	bool available;
} PgracFencedJournalScanState;

typedef struct PgracFencedJournalReconcileEntry
{
	bool used;
	/* PGRAC: original replay order, not the most recent attempt's sequence. */
	uint64 first_seq;
	/* PGRAC: reconstructed original need, not a new on-disk field. */
	PgracFencedJournalRecordV1 first_record;
	PgracFencedJournalRecordV1 last_record;
} PgracFencedJournalReconcileEntry;

typedef struct PgracFencedJournalReconcileState
{
	uint32 pending_count;
	bool fresh_readback_required;
	bool keep_write_disabled;
	bool return_off_before_rejoin;
	bool available;
	PgracFencedJournalReconcileEntry
		pending[PGRAC_FENCED_JOURNAL_MAX_PENDING_OPERATIONS];
} PgracFencedJournalReconcileState;

extern bool pgrac_fenced_journal_record_encode(
	const PgracFencedJournalRecordV1 *record,
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES]);
extern bool pgrac_fenced_journal_record_decode(
	const uint8 *frame, size_t frame_len,
	PgracFencedJournalRecordV1 *record);
extern bool pgrac_fenced_journal_record_digest(
	const uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES],
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES]);
/* PGRAC: capacity-aware codec and whole-frame digest for mixed journal history. */
extern bool pgrac_fenced_journal_frame_encode(
	const PgracFencedJournalRecordV1 *record, uint8 *frame, size_t capacity,
	size_t *frame_len);
extern size_t pgrac_fenced_journal_frame_size(const uint8 *frame, size_t available);
extern bool pgrac_fenced_journal_frame_digest(
	const uint8 *frame, size_t frame_len,
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES]);
/* Internal v2 dispatch; ordinary callers use record_decode. */
extern bool pgrac_fenced_journal_intent_decode(
	const uint8 *frame, size_t frame_len, PgracFencedJournalRecordV1 *record);
extern bool pgrac_fenced_journal_intent_continues(
	const PgracFencedJournalRecordV1 *previous,
	const PgracFencedJournalRecordV1 *current);
extern bool pgrac_fenced_journal_config_digest_v1(
	const uint8 *config_bytes, size_t config_len,
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES]);
extern void pgrac_fenced_journal_scan_state_init(
	PgracFencedJournalScanState *state);
extern bool pgrac_fenced_journal_has_room(
	const PgracFencedJournalScanState *state, size_t frame_bytes);
extern PgracFencedJournalScanResult pgrac_fenced_journal_scan_bytes(
	const uint8 *bytes, size_t len, bool allow_final_partial,
	PgracFencedJournalScanState *state);
extern PgracFencedJournalAppendResult pgrac_fenced_journal_append_fd(
	int fd, PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *record);
extern bool pgrac_fenced_journal_repair_partial_tail_fd(
	int fd, PgracFencedJournalScanState *state);
extern bool pgrac_fenced_journal_restart_action(
	const PgracFencedJournalRecordV1 *last_record,
	PgracFencedJournalRestartAction *action);
extern bool pgrac_fenced_journal_rejoin_terminal(
	const PgracFencedJournalRecordV1 *record);
extern void pgrac_fenced_journal_reconcile_state_init(
	PgracFencedJournalReconcileState *state);
extern bool pgrac_fenced_journal_reconcile_observe(
	PgracFencedJournalReconcileState *state,
	const PgracFencedJournalRecordV1 *record);
extern bool pgrac_fenced_journal_reconcile_finish(
	PgracFencedJournalReconcileState *state);
extern bool pgrac_fenced_journal_rotate_at(
	int directory_fd, int *active_fd, uint32 sealed_count,
	PgracFencedJournalScanState *state,
	char *sealed_name, size_t sealed_name_size);

#endif /* PGRAC_FENCED_JOURNAL_H */
