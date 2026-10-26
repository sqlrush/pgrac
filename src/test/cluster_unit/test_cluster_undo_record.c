/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_record.c
 *	  pgrac spec-3.7 D12 — cluster_unit encode/decode round-trip tests
 *	  for UndoRecordHeader + 4 op payloads + slot directory.
 *
 *	  19 tests covering:
 *	    T1   UndoRecordHeader encode + decode round-trip (full fields)
 *	    T2   UndoInsertPayload encode + decode round-trip
 *	    T3   UndoUpdatePayload encode + decode round-trip
 *	    T4   UndoDeletePayload encode + decode round-trip
 *	    T5   UndoItlPayload encode + decode round-trip (40B all fields)
 *	    T6   multi-record in single block (slot dir advance)
 *	    T7   block has-space invariant at boundary (7K record OK)
 *	    T8   block has-space invariant rejection (8K + 1 byte)
 *	    T9   slot dir grow-downward addressing (slot N at offset BLCKSZ - 8*(N+1))
 *	    T10  PGRAC_UNDO_BLOCK_MAGIC roundtrip after init
 *	    T11  flags + record_type byte-for-byte fidelity
 *	    T12  prev_uba 16B preserved through encode → decode chain
 *	    T13–T19  R1 O4 complete-pool scan, failure classification,
 *	              once-per-postmaster publication, restart reconstruction,
 *	              create/reuse cardinality and effective-cap floor
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/twophase.h"
#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_undo_smgr.h"
#include "storage/backendid.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_undo_operation.inc"

/* Let the exact parent caller run, so missing observations are behavioral
 * failures rather than an absent-type compile failure. */
#ifndef CLUSTER_CTRC_PREPARE_PATH_PRESENT
typedef enum ClusterCtrcPreparePath {
	CLUSTER_CTRC_PREPARE_PATH_UNKNOWN = 0,
	CLUSTER_CTRC_PREPARE_PATH_NAMESPACE = 1,
	CLUSTER_CTRC_PREPARE_PATH_INPUT = 2,
	CLUSTER_CTRC_PREPARE_PATH_JOURNAL = 3,
	CLUSTER_CTRC_PREPARE_PATH_CHAIN = 4,
	CLUSTER_CTRC_PREPARE_PATH_LOCATOR = 5,
	CLUSTER_CTRC_PREPARE_PATH_OBJECT_EXISTING = 6,
	CLUSTER_CTRC_PREPARE_PATH_TABLE = 7
} ClusterCtrcPreparePath;
#endif

#ifndef UNDO_RECORD_SOURCE_PATH
#error "UNDO_RECORD_SOURCE_PATH must identify cluster_undo_record.c"
#endif
#ifndef UNDO_ALLOC_SOURCE_PATH
#error "UNDO_ALLOC_SOURCE_PATH must identify cluster_undo_alloc.c"
#endif
#ifndef HEAPAM_SOURCE_PATH
#error "HEAPAM_SOURCE_PATH must identify heapam.c"
#endif
#ifndef UNDO_XLOG_HEADER_PATH
#error "UNDO_XLOG_HEADER_PATH must identify cluster_undo_xlog.h"
#endif
#ifndef UNDO_XLOG_SOURCE_PATH
#error "UNDO_XLOG_SOURCE_PATH must identify cluster_undo_xlog.c"
#endif
#ifndef TT_SLOT_HEADER_PATH
#error "TT_SLOT_HEADER_PATH must identify cluster_tt_slot.h"
#endif

/* Compile the exact owner, including its backend-local reservation, without
 * exposing test-only product APIs.  Section GC retains only exercised paths. */
#include UNDO_RECORD_SOURCE_PATH

/* Keep the drain predicate seam below, but run the real unchanged recycle
 * predicate on the producer's actual successor, not a test-side imitation. */
bool undo_test_real_drain_predicate(const UndoSegmentHeaderData *hdr,
									ClusterUndoActiveBoundary boundary,
									bool any_unresolved_prepared, uint32 fixed_first_segment_id,
									uint32 active_record_segment_id, uint32 active_tt_segment_id,
									bool recovery_in_progress);
#define cluster_undo_record_segment_drainable undo_test_real_drain_predicate
#include "../../backend/cluster/cluster_undo_retention.c"
#undef cluster_undo_record_segment_drainable


#define UNDO_TEST_SHMEM_BYTES 16384

static union {
	max_align_t alignment;
	char bytes[UNDO_TEST_SHMEM_BYTES];
} undo_test_shmem;
static bool undo_test_shmem_found;
static char undo_test_root[MAXPGPATH];
static char undo_test_open_fail_path[MAXPGPATH];
static int undo_test_observer_open_calls;
static bool undo_test_track_observer_opens;
static bool undo_test_stop_tracking;
static int undo_test_stop_lock_depth;
static uint32 undo_test_wait_event_info;
static PGAlignedBlock undo_test_lifecycle_disk;
static bool undo_test_publish_tt_after_block0_read;
static TimestampTz receipt_clock_us;
static TimestampTz receipt_clock_after_unref;
static bool receipt_log_enabled;
static char receipt_last_log[8192];
static char receipt_all_logs[32768];
static int receipt_log_count, receipt_owner_calls;
static unsigned receipt_observe_calls;
static bool receipt_observe_available, receipt_observe_throw;
static bool receipt_trace_lwlock_held, receipt_lifecycle_controls;
static bool receipt_fail_sync_after_prepare;
static const char *receipt_fail_log_event;
static ClusterCtrcPrepareResult receipt_prepare_result;
static ClusterCtrcPreparePath receipt_prepare_path;
static int receipt_prepare_observed_calls;
static int receipt_owner_probe_case;
static TimestampTz receipt_clock_after_lock;
static bool receipt_modifier_valid;
static bool receipt_block0_valid;
static bool receipt_block0_conditional_available;
static bool receipt_buffer_available;
static bool receipt_buffer_locked;
static int receipt_prepare_calls;
static int receipt_apply_calls;
static int receipt_cancel_calls;
static int receipt_cancel_fail_at;
static uint32 receipt_child_attempt;
static int receipt_leave_calls;
static int receipt_unref_calls;
static int receipt_install_calls;
static int receipt_wal_calls;
static int receipt_wal_install_calls;
static bool receipt_wal_throw;
static PGAlignedBlock receipt_wal_image;
/* PGRAC: trap only the explicitly exercised abort boundary. */
static bool receipt_trap_error;
static sigjmp_buf receipt_error_jump;
static int receipt_error_level;
static int receipt_error_code;
static char receipt_error_detail[1024];
static ClusterCtrcApplyResult receipt_apply_result = CLUSTER_CTRC_APPLY_APPLIED;
static bool receipt_fail_sync_after_apply;
static bool receipt_throw_apply;
static bool receipt_zero_evidence;
static bool receipt_evidence_valid = true;
static unsigned receipt_after_apply_injection_hits;
int cluster_injection_armed_count;

void
cluster_injection_run(const char *name)
{
	if (strcmp(name, "cluster-undo-ctrc-apply-return") != 0)
		abort();
	receipt_after_apply_injection_hits++;
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("injected after shared return")));
}


char *DataDir = NULL;
bool cluster_enabled = false;
bool cluster_shared_config = false;
bool IsUnderPostmaster = false;
bool cluster_undo_gcs_coherence = false;
bool cluster_trace_normal_stop = false;
volatile uint32 CritSectionCount = 0;
int cluster_node_id = 0;
int cluster_undo_segments_max_per_instance = CLUSTER_UNDO_SEGS_PER_INSTANCE;
int cluster_undo_extent_blocks = 1;
int cluster_undo_record_inline_max_bytes = 2048;
bool cluster_undo_record_segment_commit_on_rollover = true;
ClusterConf *ClusterConfShmem = NULL;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
bool cluster_xnode_profile_enabled = false;
int MaxBackends = 1;
BackendId MyBackendId = InvalidBackendId;
BackendType MyBackendType = B_BACKEND;
PGPROC *MyProc = NULL;
int max_prepared_xacts = 0;
uint32 *my_wait_event_info = &undo_test_wait_event_info;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;


static char *
read_source_file(const char *path)
{
	FILE *fp;
	char *source;
	long length;

	fp = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(fp);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	if (length <= 0) {
		fclose(fp);
		return NULL;
	}
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, fp), length);
	source[length] = '\0';
	fclose(fp);
	return source;
}

static char *
read_undo_record_source(void)
{
	return read_source_file(UNDO_RECORD_SOURCE_PATH);
}

static char *
read_undo_alloc_source(void)
{
	return read_source_file(UNDO_ALLOC_SOURCE_PATH);
}

static char *
read_heapam_source(void)
{
	return read_source_file(HEAPAM_SOURCE_PATH);
}

extern bool
cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt *receipt,
										   uint8 required_mask);

TimestampTz
GetCurrentTimestamp(void)
{
	return receipt_clock_us;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		if (!receipt_trap_error)
			abort();
		receipt_error_level = elevel;
		return true;
	}
	if (receipt_log_enabled && elevel == LOG) {
		receipt_error_level = LOG;
		return true;
	}
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (receipt_error_level == LOG) {
		UT_ASSERT_EQ(undo_test_stop_lock_depth, 0);
		UT_ASSERT(!receipt_buffer_locked && !receipt_trace_lwlock_held);
		receipt_log_count++;
		strncat(receipt_all_logs, receipt_last_log, sizeof(receipt_all_logs) - strlen(receipt_all_logs) - 2);
		strncat(receipt_all_logs, "\n", sizeof(receipt_all_logs) - strlen(receipt_all_logs) - 1);
		if (receipt_fail_log_event != NULL && strstr(receipt_last_log, receipt_fail_log_event) != NULL)
			siglongjmp(receipt_error_jump, 1);
	}
	if (receipt_trap_error && receipt_error_level >= ERROR)
		siglongjmp(receipt_error_jump, 1);
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list arguments;

	va_start(arguments, fmt);
	vsnprintf(receipt_last_log, sizeof(receipt_last_log), fmt, arguments);
	va_end(arguments);
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errcode(int code)
{
	receipt_error_code = code;
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list arguments;

	if (!receipt_trap_error)
		abort();
	va_start(arguments, fmt);
	vsnprintf(receipt_error_detail, sizeof(receipt_error_detail), fmt, arguments);
	va_end(arguments);
	return 0;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
											 bool writable)
{
	return receipt_modifier_valid && token->entered && writable;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token->entered);
	receipt_leave_calls++;
	token->entered = false;
}

bool
cluster_undo_block0_current_live_owner_publication_recheck_conditional(
	const ClusterUndoBlock0LiveOwnerPublication *publication pg_attribute_unused())
{
	return receipt_block0_valid && receipt_block0_conditional_available;
}

bool
cluster_undo_block0_current_live_owner_publication_recheck(
	const ClusterUndoBlock0LiveOwnerPublication *publication pg_attribute_unused())
{
	return receipt_block0_valid;
}

bool
cluster_undo_buf_lock_ref_conditional(int slot, uint32 segment_id, uint8 owner, uint32 block_no)
{
	UT_ASSERT_EQ(slot, 7);
	UT_ASSERT_EQ(segment_id, 1);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 1);
	if (!receipt_buffer_available)
		return false;
	UT_ASSERT(!receipt_buffer_locked);
	receipt_buffer_locked = true;
	if (receipt_clock_after_lock != 0)
		receipt_clock_us = receipt_clock_after_lock;
	return true;
}

void
cluster_undo_buf_unlock_ref(int slot)
{
	UT_ASSERT_EQ(slot, 7);
	UT_ASSERT(receipt_buffer_locked);
	receipt_buffer_locked = false;
}

void
cluster_undo_buf_unref_slot(int slot)
{
	UT_ASSERT_EQ(slot, 7);
	receipt_unref_calls++;
	if (receipt_clock_after_unref != 0)
		receipt_clock_us = receipt_clock_after_unref;
}

void
cluster_undo_buf_install_ref_locked(int slot, uint32 segment_id, uint8 owner, uint32 block_no,
									const char image[BLCKSZ])
{
	UT_ASSERT_EQ(slot, 7);
	UT_ASSERT_EQ(segment_id, 1);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 1);
	UT_ASSERT(receipt_buffer_locked);
	UT_ASSERT_EQ(receipt_apply_calls, cluster_undo_record_reservation.receipt.ctrc_applied_mask == 3 ? 2 : 1);
	UT_ASSERT_EQ(((const UndoBlockHeader *)image)->slot_count, 1);
	receipt_install_calls++;
}

XLogRecPtr
cluster_undo_emit_block_write_multi(uint8 instance, uint32 segment_id, uint32 block_no,
								 const char *image, XLogRecPtr old_lsn, uint16 rec_off,
								 uint16 rec_len, uint16 slot_off, uint16 slot_len)
{
	UT_ASSERT(cluster_shared_config);
	UT_ASSERT(receipt_buffer_locked);
	UT_ASSERT(MyProc != NULL && (MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);
	UT_ASSERT_EQ(receipt_install_calls, 0);
	UT_ASSERT_EQ(instance, 1);
	UT_ASSERT_EQ(segment_id, 1);
	UT_ASSERT_EQ(block_no, 1);
	UT_ASSERT_EQ(old_lsn, InvalidXLogRecPtr);
	UT_ASSERT_EQ(rec_off, sizeof(UndoBlockHeader));
	UT_ASSERT_EQ(rec_len, ((const UndoBlockHeader *)image)->free_offset - rec_off);
	UT_ASSERT_EQ(slot_off, UNDO_SLOT_DIR_OFFSET(0));
	UT_ASSERT_EQ(slot_len, sizeof(UndoSlotDirEntry));
	receipt_wal_calls++;
	memcpy(receipt_wal_image.data, image, BLCKSZ);
	if (receipt_wal_throw) {
		UT_ASSERT(PG_exception_stack != NULL);
		siglongjmp(*PG_exception_stack, 1);
	}
	return (XLogRecPtr)500;
}

void
cluster_undo_buf_install_wal_ref_locked(int slot, uint32 segment_id, uint8 owner,
									  uint32 block_no, const char image[BLCKSZ], XLogRecPtr wal_lsn)
{
	UT_ASSERT_EQ(receipt_wal_calls, 1);
	UT_ASSERT_EQ(wal_lsn, 500);
	UT_ASSERT_EQ(((const UndoBlockHeader *)image)->block_lsn, wal_lsn);
	UT_ASSERT(MyProc != NULL && (MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);
	cluster_undo_buf_install_ref_locked(slot, segment_id, owner, block_no, image);
	receipt_wal_install_calls++;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

#ifdef CLUSTER_SAVEPOINT_OPERATION_PRESENT
static ClusterSavepointOperation receipt_operation;
static bool receipt_capture_ok;
static bool receipt_scope_valid;
static int receipt_capture_calls;

bool
cluster_savepoint_capture_operation(ClusterSavepointOperation *out)
{
	receipt_capture_calls++;
	memset(out, 0, sizeof(*out));
	if (!receipt_capture_ok)
		return false;
	*out = receipt_operation;
	return true;
}
bool
cluster_savepoint_operation_matches_current(const ClusterSavepointOperation *operation)
{
	return receipt_scope_valid && memcmp(operation, &receipt_operation, sizeof(*operation)) == 0;
}
#endif

TransactionId
GetTopTransactionIdIfAny(void)
{
	return (TransactionId)700;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return GetTopTransactionIdIfAny();
}

SCN
cluster_scn_advance(void)
{
	return (SCN)1000;
}

SCN
cluster_scn_current(void)
{
	return (SCN)900;
}

XLogRecPtr
GetXLogWriteRecPtr(void)
{
	return (XLogRecPtr)1000;
}

bool
cluster_tt_local_peek_binding(TransactionId xid pg_attribute_unused(), uint32 *segment,
							  uint16 *offset, uint32 *slot, uint32 *epoch, uint16 *wrap)
{
	*segment = 1;
	*offset = cluster_undo_record_reservation.receipt.tt_slot_offset;
	*slot = cluster_tt_slot_offset_to_id(*offset);
	*epoch = 1;
	*wrap = 1;
	return true;
}

bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result, uint32 *grant,
	ClusterCtrcTxnKeyV1 *ctrc_key, ClusterCtrcParticipantIdentity *participant)
{
	receipt_owner_calls++;
	UT_ASSERT_EQ(xid, (TransactionId)700);
	key->undo_segment_id = 1;
	key->tt_slot_id
		= cluster_tt_slot_offset_to_id(cluster_undo_record_reservation.receipt.tt_slot_offset);
	result->status = CLUSTER_TT_STATUS_IN_PROGRESS;
	*grant = 1;
	memset(ctrc_key, 0, sizeof(*ctrc_key));
	memset(participant, 0, sizeof(*participant));
	participant->boot_incarnation = 1;
	participant->capability_record_generation = 1;
	if (receipt_lifecycle_controls) {
		ctrc_key->system_identifier = 4321;
		ctrc_key->origin_node_id = 0; ctrc_key->owner_instance = 1;
		ctrc_key->origin_boot_incarnation = 11; ctrc_key->cluster_epoch = 71;
		ctrc_key->formation_epoch = 88; ctrc_key->admission_record_generation = 99;
		ctrc_key->root_descriptor_incarnation = 5; ctrc_key->root_id = 66;
		ctrc_key->root_generation = 5; ctrc_key->segment_id = 1;
		ctrc_key->segment_generation = 4; ctrc_key->slot_offset = 128;
		ctrc_key->slot_wrap = 9; ctrc_key->xid = xid;
		participant->formation_epoch = 88; participant->admission_record_generation = 99;
	}
	if (receipt_owner_probe_case == 1)
		return false;
	if (receipt_owner_probe_case == 2)
		result->status = CLUSTER_TT_STATUS_UNKNOWN;
	if (receipt_owner_probe_case == 3)
		*grant = 0;
	if (receipt_owner_probe_case == 4)
		key->undo_segment_id = 2;
	if (receipt_owner_probe_case == 5)
		key->tt_slot_id = 2;
	return true;
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
	const ClusterCtrcParticipantIdentity *participant pg_attribute_unused(), uint32 grant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target pg_attribute_unused(), ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT_EQ(grant, 1);
	UT_ASSERT_EQ(publication->wire_request_id, cluster_undo_record_reservation.sequence);
	receipt_child_attempt = publication->attempt_generation;
	memset(handle, 0, sizeof(*handle));
	handle->valid = true;
	receipt_prepare_calls++;
	if (receipt_lifecycle_controls) {
		handle->key = *key;
		handle->participant_index = 4; handle->receipt_index = 5;
		handle->journal_slot_generation = 9;
		/* Deliberately not dereferenceable: caller logs only the owned handle. */
		handle->receipt = (ClusterCtrcReceipt *)1;
		handle->participant = (ClusterCtrcParticipantEntry *)1;
		if (receipt_fail_sync_after_prepare)
			cluster_undo_record_reservation.sequence++;
		if (receipt_prepare_result != CLUSTER_CTRC_PREPARE_READY
			&& receipt_prepare_result != CLUSTER_CTRC_PREPARE_DUPLICATE)
			handle->valid = false;
		return receipt_prepare_result;
	}
	return CLUSTER_CTRC_PREPARE_READY;
}

#ifdef CLUSTER_CTRC_PREPARE_PATH_PRESENT
ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared_observed(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant, uint32 grant,
	const ClusterCtrcPublicationIdV1 *publication, const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle, ClusterCtrcPreparePath *path)
{
	ClusterCtrcPrepareResult result = cluster_ctrc_receipt_prepare_shared(
		key, participant, grant, publication, target, handle);

	receipt_prepare_observed_calls++;
	if (path != NULL)
		*path = receipt_prepare_path;
	return result;
}
#endif

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(const ClusterCtrcReceiptHandle *handle,
								  const ClusterCtrcTargetV1 *target pg_attribute_unused(),
								  ClusterCtrcApplyToken *token)
{
	UT_ASSERT(handle->valid);
	memset(token, 0, sizeof(*token));
	receipt_apply_calls++;
	if (receipt_throw_apply)
		siglongjmp(receipt_error_jump, 1);
	if (receipt_fail_sync_after_apply)
		cluster_undo_record_reservation.sequence++;
	/* Only an explicit branch proof can distinguish RETRY from unknown. */
#ifdef CLUSTER_CTRC_APPLY_EVIDENCE_PRESENT
	if (receipt_zero_evidence) {
		token->evidence = cluster_undo_record_reservation.apply_evidence[0];
		token->evidence.effect = CLUSTER_CTRC_EFFECT_ZERO_NO_MUTATION;
	}
#endif
	return receipt_apply_result;
}

#ifdef CLUSTER_CTRC_APPLY_EVIDENCE_PRESENT
bool
cluster_ctrc_receipt_capture_apply_evidence(const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *pending, bool reuse, ClusterCtrcApplyEvidence *out)
{
	memset(out, 0, sizeof(*out));
	out->handle = *handle;
	out->prior_target = *pending;
	out->state_before = reuse ? CTRC_RECEIPT_APPLIED : CTRC_RECEIPT_PREPARED;
	out->publication.requester_backend_id = MyBackendId;
	out->publication.wire_request_id = cluster_undo_record_reservation.sequence;
	out->publication.attempt_generation = cluster_undo_record_reservation.receipt.ctrc_attempt_generation;
	return receipt_evidence_valid;
}

bool
cluster_ctrc_receipt_apply_evidence_recheck(const ClusterCtrcApplyEvidence *evidence)
{
	(void)evidence;
	return receipt_evidence_valid;
}
#endif

ClusterCtrcApplyResult
cluster_ctrc_receipt_retarget_itl_shared(const ClusterCtrcReceiptHandle *handle,
										 const ClusterCtrcTargetV1 *pending pg_attribute_unused(),
										 const ClusterCtrcTargetV1 *target,
										 ClusterCtrcApplyToken *token)
{
	return cluster_ctrc_receipt_apply_shared(handle, target, token);
}

bool
cluster_ctrc_receipt_observe_shared(const ClusterCtrcReceiptHandle *handle,
								  ClusterCtrcReceiptObservation *out)
{
	receipt_observe_calls++;
	UT_ASSERT(CritSectionCount == 0);
	UT_ASSERT(handle != NULL && handle->valid);
	memset(out, 0, sizeof(*out));
	if (receipt_observe_throw)
		siglongjmp(receipt_error_jump, 1);
	if (!receipt_observe_available)
		return false;
	/* Controlled shared boundary only; the real accessor has its own
	 * NO-UNIT fixture. Never dereference the dummy handle's shared pointers. */
	out->key = handle->key;
	out->participant.node_id = 0;
	out->participant.boot_incarnation = 1101;
	out->participant.capability_record_generation = 19;
	out->participant.formation_epoch = 1201;
	out->participant.admission_record_generation = 1301;
	out->publication.operation_id = 73;
	out->publication.wire_request_id = 601;
	out->publication.requester_backend_id = 77;
	out->publication.requester_boot_incarnation = 1101;
	out->publication.attempt_generation = 3;
	out->publication.journal_sequence = 501;
	out->publication.key_sequence = 17;
	out->publication.journal_slot_generation = handle->journal_slot_generation;
	out->grant_generation = 7;
	out->participant_state = CTRC_PARTICIPANT_OPEN;
	out->receipt_count = 1;
	out->state_before = out->state_after = receipt_cancel_calls > 0
		? CTRC_RECEIPT_CANCELLED : CTRC_RECEIPT_PREPARED;
	out->prepared_count = receipt_cancel_calls == 0 ? 1 : 0;
	out->cancelled_count = receipt_cancel_calls > 0 ? 1 : 0;
	out->target.spc_oid = 1663;
	out->target.db_oid = 1;
	out->target.rel_number = 16388;
	out->target.block_number = 137;
	out->target.uba[0] = 0xab;
	out->target.itl_slot_index = 2;
	out->target.offset_number = 7;
	if (receipt_cancel_calls > 0)
		UT_ASSERT(!cluster_undo_record_reservation.active);
	return true;
}

bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT(handle->valid);
	receipt_cancel_calls++;
	return receipt_cancel_calls != receipt_cancel_fail_at;
}


/*
 * The O4 tests below link the real record/allocator implementation, trimmed to
 * the functions reachable from this binary.  These stubs are the runtime
 * boundaries actually crossed by that observation slice; no product branch is
 * replaced.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

Size
add_size(Size s1, Size s2)
{
	if (s1 > SIZE_MAX - s2)
		abort();
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	if (s1 != 0 && s2 > SIZE_MAX / s1)
		abort();
	return s1 * s2;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (size > sizeof(undo_test_shmem))
		abort();
	if (foundPtr != NULL)
		*foundPtr = undo_test_shmem_found;
	undo_test_shmem_found = true;
	return undo_test_shmem.bytes;
}

int
LWLockNewTrancheId(void)
{
	return 1;
}

void
LWLockRegisterTranche(int tranche_id pg_attribute_unused(),
					  const char *tranche_name pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	if (undo_test_stop_tracking) {
		UT_ASSERT(lock == &UndoRecordShared->registry_lock.lock);
		UT_ASSERT_EQ(undo_test_stop_lock_depth, 0);
		undo_test_stop_lock_depth++;
	}
	return true;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (receipt_trace_lwlock_held || undo_test_stop_lock_depth != 0 || receipt_buffer_locked)
		callback(&UndoRecordShared->registry_lock.lock, LW_EXCLUSIVE, context);
}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	if (undo_test_stop_tracking) {
		UT_ASSERT(lock == &UndoRecordShared->registry_lock.lock);
		UT_ASSERT_EQ(undo_test_stop_lock_depth, 1);
		undo_test_stop_lock_depth--;
	}
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(), uint32 segment_id,
							 uint8 owner_instance, uint32 block_no, char *buf)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;

	if (segment_id != disk->segment_id || owner_instance != disk->owner_instance || block_no != 0
		|| buf == NULL)
		return false;
	memcpy(buf, undo_test_lifecycle_disk.data, BLCKSZ);
	if (undo_test_publish_tt_after_block0_read) {
		TTSlot *slot = &disk->tt_slots[7];

		undo_test_publish_tt_after_block0_read = false;
		memset(slot, 0, sizeof(*slot));
		slot->status = TT_SLOT_ACTIVE;
		slot->xid = (TransactionId)700;
		slot->wrap = 3;
		slot->commit_scn = InvalidScn;
	}
	return true;
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent pg_attribute_unused(), uint32 segment_id,
							  uint8 owner_instance, uint32 block_no, const char *buf,
							  bool do_fsync pg_attribute_unused())
{
	if (segment_id != 1 || owner_instance != 1 || block_no != 0 || buf == NULL)
		return false;
	memcpy(undo_test_lifecycle_disk.data, buf, BLCKSZ);
	return true;
}

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
									 uint32 segment_id, uint8 owner_instance, uint32 offset,
									 const char *buf, uint32 len)
{
	if (segment_id != 1 || owner_instance != 1 || buf == NULL || len == 0
		|| (uint64)offset + (uint64)len > BLCKSZ)
		return false;
	memcpy(undo_test_lifecycle_disk.data + offset, buf, len);
	return true;
}

bool
cluster_undo_smgr_fsync_segment_file(uint32 segment_id, uint8 owner_instance)
{
	return segment_id == 1 && owner_instance == 1;
}

static ClusterUndoBlock0Result undo_test_lifecycle_ensure_result = CLUSTER_UNDO_BLOCK0_OK;
static ClusterUndoBlock0Result undo_test_lifecycle_mutate_result = CLUSTER_UNDO_BLOCK0_OK;

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident(const ClusterUndoBlock0LogicalKey *key,
													   int timeout_ms)
{
	return key != NULL && key->segment_id == 1 && key->owner_instance == 1 && timeout_ms > 0
			   ? undo_test_lifecycle_ensure_result
			   : CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
}

static unsigned undo_test_lifecycle_mutate_calls;

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_mutate_exact(const ClusterUndoBlock0LogicalKey *key,
													const ClusterUndoBlock0Generation *expected,
													const char predecessor_page[BLCKSZ],
													const char successor_page[BLCKSZ],
													int timeout_ms)
{
	PGAlignedBlock rebased;
	UndoSegmentHeaderData *current = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	UndoSegmentHeaderData *next = (UndoSegmentHeaderData *)rebased.data;
	const UndoSegmentHeaderData *requested = (const UndoSegmentHeaderData *)successor_page;

	undo_test_lifecycle_mutate_calls++;

	if (key == NULL || expected == NULL || !expected->known || predecessor_page == NULL
		|| successor_page == NULL || timeout_ms <= 0 || key->segment_id != current->segment_id
		|| key->owner_instance != current->owner_instance || expected->value != current->wrap_count)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (undo_test_lifecycle_mutate_result != CLUSTER_UNDO_BLOCK0_OK)
		return undo_test_lifecycle_mutate_result;
	memcpy(rebased.data, successor_page, BLCKSZ);
	memcpy(next->tt_slots, current->tt_slots, sizeof(next->tt_slots));
	memcpy(undo_test_lifecycle_disk.data, rebased.data, BLCKSZ);
	return requested->wrap_count == expected->value ? CLUSTER_UNDO_BLOCK0_OK
													: CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
}

int
GetNumberOfPreparedTransactions(void)
{
	return 0;
}

bool
RecoveryInProgress(void)
{
	return false;
}

uint32
cluster_tt_slot_current_segment(int node_id pg_attribute_unused())
{
	return 0;
}

static bool undo_test_drain_allowed;

bool
cluster_undo_record_segment_drainable(const UndoSegmentHeaderData *hdr pg_attribute_unused(),
									  ClusterUndoActiveBoundary boundary pg_attribute_unused(),
									  bool any_unresolved_prepared pg_attribute_unused(),
									  uint32 fixed_first_segment_id pg_attribute_unused(),
									  uint32 active_record_segment_id pg_attribute_unused(),
									  uint32 active_tt_segment_id pg_attribute_unused(),
									  bool recovery_in_progress pg_attribute_unused())
{
	return undo_test_drain_allowed;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return a < b ? -1 : a > b ? 1 : 0;
}

int
BasicOpenFile(const char *fileName, int fileFlags)
{
	if (undo_test_track_observer_opens)
		undo_test_observer_open_calls++;
	if (undo_test_open_fail_path[0] != '\0' && strcmp(fileName, undo_test_open_fail_path) == 0) {
		errno = EIO;
		return -1;
	}
	if ((fileFlags & O_CREAT) != 0)
		return open(fileName, fileFlags, S_IRUSR | S_IWUSR);
	return open(fileName, fileFlags);
}

bool
cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent pg_attribute_unused(),
								   bool peer_mode pg_attribute_unused(),
								   bool coherence_on pg_attribute_unused())
{
	return false;
}

/* This local encode/decode fixture never enters a qualified recovery scope. */
ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	return cluster_undo_intent_for_owner(owner);
}
int
cluster_undo_recovery_path_resolve_v1(uint8 owner pg_attribute_unused(),
									  uint32 segment pg_attribute_unused(), char *path, size_t size)
{
	if (path != NULL && size > 0)
		path[0] = '\0';
	return -1;
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner_instance pg_attribute_unused(),
									uint32 segment_id pg_attribute_unused(),
									char *buf pg_attribute_unused(),
									size_t buf_size pg_attribute_unused())
{
	return -1;
}


static void
undo_test_segment_path(uint32 segment_id, char *path, size_t path_size)
{
	(void)snprintf(path, path_size, "%s/pg_undo/instance_0/seg_%u.dat", undo_test_root,
				   (unsigned)segment_id);
}

static bool
undo_test_fixture_begin(void)
{
	char template[] = "/tmp/pgrac-r1-undo-XXXXXX";
	char path[MAXPGPATH];

	if (mkdtemp(template) == NULL) {
		UT_ASSERT(false);
		return false;
	}
	(void)snprintf(undo_test_root, sizeof(undo_test_root), "%s", template);
	if (strncmp(undo_test_root, "/tmp/pgrac-r1-undo-", strlen("/tmp/pgrac-r1-undo-")) != 0) {
		UT_ASSERT(false);
		return false;
	}

	(void)snprintf(path, sizeof(path), "%s/pg_undo", undo_test_root);
	if (mkdir(path, S_IRWXU) != 0) {
		UT_ASSERT(false);
		return false;
	}
	(void)snprintf(path, sizeof(path), "%s/pg_undo/instance_0", undo_test_root);
	if (mkdir(path, S_IRWXU) != 0) {
		UT_ASSERT(false);
		return false;
	}

	DataDir = undo_test_root;
	cluster_node_id = 0;
	cluster_shared_config = false;
	cluster_enabled = false;
	cluster_undo_gcs_coherence = false;
	cluster_undo_segments_max_per_instance = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	undo_test_open_fail_path[0] = '\0';
	undo_test_observer_open_calls = 0;
	undo_test_track_observer_opens = false;
	return true;
}

static void
undo_test_fixture_end(void)
{
	char path[MAXPGPATH];
	uint32 segment_id;

	if (undo_test_root[0] == '\0')
		return;
	if (strncmp(undo_test_root, "/tmp/pgrac-r1-undo-", strlen("/tmp/pgrac-r1-undo-")) != 0)
		abort();

	for (segment_id = 1; segment_id <= CLUSTER_UNDO_SEGS_PER_INSTANCE; segment_id++) {
		undo_test_segment_path(segment_id, path, sizeof(path));
		if (unlink(path) != 0 && errno != ENOENT)
			abort();
	}
	(void)snprintf(path, sizeof(path), "%s/pg_undo/instance_0", undo_test_root);
	if (rmdir(path) != 0)
		abort();
	(void)snprintf(path, sizeof(path), "%s/pg_undo", undo_test_root);
	if (rmdir(path) != 0)
		abort();
	if (rmdir(undo_test_root) != 0)
		abort();

	DataDir = NULL;
	undo_test_root[0] = '\0';
}

static void
undo_test_make_header(uint32 segment_id, uint8 owner_instance, uint8 state, char *page)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)page;

	memset(page, 0, BLCKSZ);
	header->pd_flags = PD_UNDO_SEG_HEADER;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->segment_id = segment_id;
	header->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header->segment_state = state;
	header->owner_instance = owner_instance;
	header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
}

static bool
undo_test_write_header(uint32 segment_id, uint8 state)
{
	char page[BLCKSZ];
	char path[MAXPGPATH];
	ssize_t written;
	int fd;

	undo_test_segment_path(segment_id, path, sizeof(path));
	undo_test_make_header(segment_id, 1, state, page);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (fd < 0)
		return false;
	written = write(fd, page, sizeof(page));
	if (close(fd) != 0)
		return false;
	return written == sizeof(page);
}

/* The complete-pool probe must never fall through to fresh-file creation. */
int
cluster_shared_fs_undo_instance_dir_resolve(uint8 owner pg_attribute_unused(),
											char *buf pg_attribute_unused(),
											size_t size pg_attribute_unused())
{
	abort();
}

XLogRecPtr
cluster_undo_emit_segment_init(uint8 owner pg_attribute_unused(),
							   uint32 segment pg_attribute_unused(),
							   const char *page pg_attribute_unused())
{
	abort();
}

int
errcode_for_file_access(void)
{
	abort();
}

void
fsync_fname(const char *path pg_attribute_unused(), bool isdir pg_attribute_unused())
{
	abort();
}

int
pg_fsync(int fd pg_attribute_unused())
{
	abort();
}

static bool
undo_test_write_invalid_header(uint32 segment_id)
{
	char page[BLCKSZ];
	char path[MAXPGPATH];
	ssize_t written;
	int fd;

	memset(page, 0, sizeof(page));
	undo_test_segment_path(segment_id, path, sizeof(path));
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (fd < 0)
		return false;
	written = write(fd, page, sizeof(page));
	if (close(fd) != 0)
		return false;
	return written == sizeof(page);
}

static void
undo_test_reset_record_shmem(void)
{
	memset(&undo_test_shmem, 0, sizeof(undo_test_shmem));
	undo_test_shmem_found = false;
	cluster_undo_record_shmem_init();
}


/* ---- T1: UndoRecordHeader encode + decode round-trip ---- */
UT_TEST(test_record_header_roundtrip)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.record_type = UNDO_RECORD_INSERT;
	src.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	src.payload_length = 12;
	src.xid = 12345;
	src.origin_node_id = 2;
	src.tt_slot_segment_id = 1;
	src.tt_slot_id = 7;
	src.write_scn = 999;
	src.target_fork = MAIN_FORKNUM;
	src.target_block = 100;
	src.target_offset = 5;

	memcpy(buf, &src, sizeof(src));
	memset(&dst, 0xff, sizeof(dst));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.record_type, src.record_type);
	UT_ASSERT_EQ(dst.flags, src.flags);
	UT_ASSERT_EQ(dst.payload_length, src.payload_length);
	UT_ASSERT_EQ((long long)dst.xid, (long long)src.xid);
	UT_ASSERT_EQ(dst.origin_node_id, src.origin_node_id);
	UT_ASSERT_EQ(dst.target_fork, src.target_fork);
	UT_ASSERT_EQ((long long)dst.target_block, (long long)src.target_block);
	UT_ASSERT_EQ(dst.target_offset, src.target_offset);
}

/* ---- T2: UndoInsertPayload encode + decode ---- */
UT_TEST(test_insert_payload_roundtrip)
{
	UndoInsertPayload src = { .inserted_tuple_len = 256, .flags = 1 };
	UndoInsertPayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.inserted_tuple_len, 256);
	UT_ASSERT_EQ(dst.flags, 1);
}

/* ---- T3: UndoUpdatePayload round-trip ---- */
UT_TEST(test_update_payload_roundtrip)
{
	UndoUpdatePayload src;
	UndoUpdatePayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.new_block = 200;
	src.new_offset = 8;
	src.old_tuple_length = 128;
	src.old_tuple_offset = sizeof(UndoUpdatePayload);
	src.flags = 2;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.new_block, 200LL);
	UT_ASSERT_EQ(dst.new_offset, 8);
	UT_ASSERT_EQ(dst.old_tuple_length, 128);
	UT_ASSERT_EQ(dst.old_tuple_offset, sizeof(UndoUpdatePayload));
	UT_ASSERT_EQ(dst.flags, 2);
}

/* ---- T4: UndoDeletePayload round-trip ---- */
UT_TEST(test_delete_payload_roundtrip)
{
	UndoDeletePayload src
		= { .full_tuple_length = 200, .full_tuple_offset = sizeof(UndoDeletePayload), .flags = 4 };
	UndoDeletePayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.full_tuple_length, 200);
	UT_ASSERT_EQ(dst.full_tuple_offset, sizeof(UndoDeletePayload));
	UT_ASSERT_EQ((long long)dst.flags, 4LL);
}

/* ---- T5: UndoItlPayload round-trip (40B) ---- */
UT_TEST(test_itl_payload_roundtrip)
{
	UndoItlPayload src;
	UndoItlPayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.itl_slot_idx = 3;
	src.prev_flags = 0;
	src.new_flags = 5;
	src.lock_mode = 2;
	src.lock_xid = 7777;
	src.prev_xmax = 5555;
	src.prev_infomask = 0x0100;
	src.prev_infomask2 = 0x0080;
	src.prev_commit_scn = 88888;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.itl_slot_idx, 3);
	UT_ASSERT_EQ(dst.new_flags, 5);
	UT_ASSERT_EQ(dst.lock_mode, 2);
	UT_ASSERT_EQ((long long)dst.lock_xid, 7777LL);
	UT_ASSERT_EQ(dst.prev_infomask, 0x0100);
	UT_ASSERT_EQ((long long)dst.prev_commit_scn, 88888LL);
}

/* ---- T6: multi-record in single block (slot dir advance) ---- */
UT_TEST(test_multi_record_block)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;
	UndoSlotDirEntry *slot0;
	UndoSlotDirEntry *slot1;
	UndoSlotDirEntry *slot2;
	uint32 record_off_0 = sizeof(UndoBlockHeader);
	uint32 record_off_1 = record_off_0 + sizeof(UndoRecordHeader) + 4;
	uint32 record_off_2 = record_off_1 + sizeof(UndoRecordHeader) + 12;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->slot_count = 3;
	blkhdr->free_offset = record_off_2 + sizeof(UndoRecordHeader) + 8;

	slot0 = UNDO_SLOT_DIR_PTR(block, 0);
	slot0->record_offset = record_off_0;
	slot0->record_length = sizeof(UndoRecordHeader) + 4;
	slot0->record_type = UNDO_RECORD_INSERT;

	slot1 = UNDO_SLOT_DIR_PTR(block, 1);
	slot1->record_offset = record_off_1;
	slot1->record_length = sizeof(UndoRecordHeader) + 12;
	slot1->record_type = UNDO_RECORD_UPDATE;

	slot2 = UNDO_SLOT_DIR_PTR(block, 2);
	slot2->record_offset = record_off_2;
	slot2->record_length = sizeof(UndoRecordHeader) + 8;
	slot2->record_type = UNDO_RECORD_DELETE;

	/* Verify slot dir reads back correctly */
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 0)->record_offset, (long long)record_off_0);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 1)->record_offset, (long long)record_off_1);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 2)->record_offset, (long long)record_off_2);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 0)->record_type, UNDO_RECORD_INSERT);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 1)->record_type, UNDO_RECORD_UPDATE);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 2)->record_type, UNDO_RECORD_DELETE);
}

/* ---- T7: block has-space invariant at boundary (7K record OK) ---- */
UT_TEST(test_block_has_space_boundary_ok)
{
	UT_ASSERT(cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, 7000));
}

/* ---- T8: block has-space invariant rejection ---- */
UT_TEST(test_block_has_space_overflow)
{
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, BLCKSZ));
	/* with slot_count=255 the dir takes 256×8 = 2048 bytes from end */
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 255, BLCKSZ - 2048));
}

/* ---- T9: slot dir grow-downward addressing ---- */
UT_TEST(test_slot_dir_addressing)
{
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(0), BLCKSZ - 8);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(1), BLCKSZ - 16);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(255), BLCKSZ - 2048);
}

/* ---- T10: block magic + version after manual init ---- */
UT_TEST(test_block_magic_init)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->free_offset = UNDO_BLOCK_INIT_FREE_OFFSET;

	UT_ASSERT_EQ((long long)blkhdr->magic, (long long)PGRAC_UNDO_BLOCK_MAGIC);
	UT_ASSERT_EQ(blkhdr->block_version, 1);
	UT_ASSERT_EQ((long long)blkhdr->free_offset, (long long)sizeof(UndoBlockHeader));
}

/* ---- T11: flags + record_type byte-for-byte fidelity ---- */
UT_TEST(test_record_type_flags_bytes)
{
	UndoRecordHeader hdr;
	const uint8 *bytes;

	memset(&hdr, 0, sizeof(hdr));
	hdr.record_type = UNDO_RECORD_ITL;
	hdr.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED;

	bytes = (const uint8 *)&hdr;
	UT_ASSERT_EQ(bytes[0], UNDO_RECORD_ITL);
	UT_ASSERT_EQ(bytes[1], UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED);
}

/* ---- T12: prev_uba 16B preserved through copy ---- */
UT_TEST(test_prev_uba_preserved)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.prev_uba.raw[0] = 0x1234567890abcdefULL;
	src.prev_uba.raw[1] = 0xfedcba0987654321ULL;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.prev_uba.raw[0], (long long)src.prev_uba.raw[0]);
	UT_ASSERT_EQ((long long)dst.prev_uba.raw[1], (long long)src.prev_uba.raw[1]);
}


/* ---- R1-A O4: real full-pool observer and record publication ---- */
UT_TEST(test_undo_pool_observer_scans_all_256_slots_across_gaps)
{
	ClusterUndoPoolObservation observation;
	ClusterUndoPoolObservationResult result;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ALLOCATED));
	UT_ASSERT(undo_test_write_header(3, SEGMENT_ACTIVE));

	undo_test_track_observer_opens = true;
	result = cluster_undo_segment_observe_pool(1, &observation);
	undo_test_track_observer_opens = false;

	UT_ASSERT_EQ(result, CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);
	UT_ASSERT_EQ(observation.configured_cap, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(observation.effective_cap, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(undo_test_observer_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	undo_test_fixture_end();
}

UT_TEST(test_undo_pool_observer_rejects_invalid_owner_and_null_output)
{
	ClusterUndoPoolObservation observation = { 7, 8, 9 };

	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(0, &observation),
				 CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
	UT_ASSERT_EQ(observation.allocated_count, 0);
	UT_ASSERT_EQ(observation.configured_cap, 0);
	UT_ASSERT_EQ(observation.effective_cap, 0);
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, NULL), CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
}

UT_TEST(test_undo_pool_observer_distinguishes_io_failure_and_invalid_header)
{
	ClusterUndoPoolObservation observation;
	char path[MAXPGPATH];

	if (!undo_test_fixture_begin())
		return;
	undo_test_segment_path(19, path, sizeof(path));
	(void)snprintf(undo_test_open_fail_path, sizeof(undo_test_open_fail_path), "%s", path);
	undo_test_track_observer_opens = true;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation),
				 CLUSTER_UNDO_POOL_OBS_IO_ERROR);
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, 19);
	UT_ASSERT_EQ(observation.allocated_count, 0);

	undo_test_open_fail_path[0] = '\0';
	undo_test_observer_open_calls = 0;
	UT_ASSERT(undo_test_write_invalid_header(7));
	undo_test_track_observer_opens = true;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation),
				 CLUSTER_UNDO_POOL_OBS_INVALID_HEADER);
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, 7);
	UT_ASSERT_EQ(observation.allocated_count, 0);
	undo_test_fixture_end();
}

UT_TEST(test_undo_record_restart_reconstructs_and_dump_ensure_never_rescans)
{
	int first_scan_calls;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ALLOCATED));
	UT_ASSERT(undo_test_write_header(3, SEGMENT_COMMITTED));
	undo_test_reset_record_shmem();

	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	first_scan_calls = undo_test_observer_open_calls;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "READY");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 2);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 2);
	UT_ASSERT_EQ(first_scan_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);

	/* A later file must not make a diagnostic dump rescan this incarnation. */
	UT_ASSERT(undo_test_write_header(5, SEGMENT_RECYCLABLE));
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, first_scan_calls);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 2);

	/* A fresh postmaster incarnation reconstructs all three on-disk headers. */
	undo_test_reset_record_shmem();
	undo_test_observer_open_calls = 0;
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "READY");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 3);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 3);
	UT_ASSERT_EQ(undo_test_observer_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	undo_test_fixture_end();
}

UT_TEST(test_undo_record_failed_lazy_scan_is_attempted_once)
{
	char path[MAXPGPATH];
	int failed_scan_calls;

	if (!undo_test_fixture_begin())
		return;
	undo_test_segment_path(4, path, sizeof(path));
	(void)snprintf(undo_test_open_fail_path, sizeof(undo_test_open_fail_path), "%s", path);
	undo_test_reset_record_shmem();

	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	failed_scan_calls = undo_test_observer_open_calls;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "UNAVAILABLE_IO_ERROR");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 0);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 0);
	UT_ASSERT_EQ(failed_scan_calls, 4);

	undo_test_open_fail_path[0] = '\0';
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, failed_scan_calls);
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "UNAVAILABLE_IO_ERROR");
	undo_test_fixture_end();
}

UT_TEST(test_undo_pool_create_increments_but_reuse_keeps_cardinality)
{
	ClusterUndoPoolObservation observation;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ACTIVE));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 1);

	UT_ASSERT(undo_test_write_header(2, SEGMENT_ALLOCATED));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);

	/* Rebirth rewrites one valid identity in place; it cannot create a row. */
	UT_ASSERT(undo_test_write_header(2, SEGMENT_RECYCLABLE));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);
	undo_test_fixture_end();
}

UT_TEST(test_undo_effective_cap_clamps_and_never_falls_below_current)
{
	ClusterUndoPoolObservation observation;
	uint32 segment_id;

	if (!undo_test_fixture_begin())
		return;
	for (segment_id = 1; segment_id <= 17; segment_id++)
		UT_ASSERT(undo_test_write_header(segment_id, SEGMENT_ALLOCATED));

	cluster_undo_segments_max_per_instance = 4;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 17);
	UT_ASSERT_EQ(observation.configured_cap, 16);
	UT_ASSERT_EQ(observation.effective_cap, 17);

	undo_test_reset_record_shmem();
	cluster_undo_record_observation_ensure();
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 17);
	cluster_undo_segments_max_per_instance = 32;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 32);
	cluster_undo_segments_max_per_instance = 999;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), CLUSTER_UNDO_SEGS_PER_INSTANCE);
	cluster_undo_segments_max_per_instance = 4;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 17);
	undo_test_fixture_end();
}

UT_TEST(test_full_undo_pool_selects_exact_recyclable_supply_without_rewrite)
{
	ClusterUndoSegmentExtendPlan plan;
	uint32 segment_id;

	if (!undo_test_fixture_begin())
		return;
	/* Exercise the actual allocator against all 256 present slot files. */
	for (segment_id = 1; segment_id <= CLUSTER_UNDO_SEGS_PER_INSTANCE; segment_id++)
		UT_ASSERT(undo_test_write_header(segment_id, SEGMENT_ACTIVE));
	UT_ASSERT(!cluster_undo_segment_extend_or_create(1, &plan));
	UT_ASSERT(plan.at_hard_cap);
	UT_ASSERT_EQ(plan.segment_id, 0);

	/* The first/fixed and current slots remain live; only slot 10 is reusable. */
	UT_ASSERT(undo_test_write_header(10, SEGMENT_RECYCLABLE));
	UT_ASSERT(cluster_undo_segment_extend_or_create(1, &plan));
	UT_ASSERT(!plan.at_hard_cap);
	UT_ASSERT(plan.needs_reuse);
	UT_ASSERT_EQ(plan.segment_id, 10);
	UT_ASSERT_EQ(plan.generation, 0);
	/* A plan is not a rewrite or generation publication. */
	UT_ASSERT_EQ(cluster_undo_segment_generation(10, 1), plan.generation);
	undo_test_fixture_end();
}

UT_TEST(test_allocated_nonempty_tt_is_not_fresh_supply)
{
	ClusterUndoSegmentExtendPlan plan;
	TTSlot old_slot = { 0 };
	TTSlot observed;
	char path[MAXPGPATH];
	int fd;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ALLOCATED));
	UT_ASSERT(undo_test_write_header(2, SEGMENT_ALLOCATED));
	old_slot.xid = 4195185;
	old_slot.status = TT_SLOT_COMMITTED;
	old_slot.commit_scn = 14274853;
	undo_test_segment_path(1, path, sizeof(path));
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(
			pwrite(fd, &old_slot, sizeof(old_slot), offsetof(UndoSegmentHeaderData, tt_slots)),
			sizeof(old_slot));
		UT_ASSERT(cluster_undo_segment_extend_or_create(1, &plan));
		UT_ASSERT_EQ(plan.segment_id, 2);
		UT_ASSERT(!plan.needs_reuse);
		UT_ASSERT_EQ(
			pread(fd, &observed, sizeof(observed), offsetof(UndoSegmentHeaderData, tt_slots)),
			sizeof(observed));
		UT_ASSERT_EQ(memcmp(&observed, &old_slot, sizeof(old_slot)), 0);
		close(fd);
	}
	undo_test_fixture_end();
}

UT_TEST(test_absent_segment_selection_does_not_publish_a_file)
{
	ClusterUndoSegmentExtendPlan plan;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ACTIVE));
	UT_ASSERT(cluster_undo_segment_extend_or_create(1, &plan));
	UT_ASSERT_EQ(plan.segment_id, 2);
	UT_ASSERT_EQ(plan.generation, 0);
	UT_ASSERT(plan.needs_provision);
	UT_ASSERT(!plan.needs_reuse);
	UT_ASSERT(!plan.at_hard_cap);
	UT_ASSERT(!cluster_undo_segment_file_exists(1, 2));
	undo_test_fixture_end();
}

/* Spec 8.4A I18/I19: one admission debt covers record allocation and every
 * lifecycle/record-seal block0 mutation it can reach. */
UT_TEST(test_record_allocator_owns_modifier_debt_outside_lifecycle_locks)
{
	char *source = read_undo_record_source();
	const char *body;
	const char *start;
	const char *end;
	const char *enter;
	const char *try_block;
	const char *recheck;
	const char *call_body;
	const char *finally_block;
	const char *leave;

	if (source == NULL)
		return;
	body = strstr(source, "\ncluster_undo_record_alloc_body(");
	start = strstr(source, "\ncluster_undo_record_alloc(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\n\n/*\n * cluster_undo_get_record");
	enter = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_enter(");
	try_block = start == NULL ? NULL : strstr(start, "PG_TRY();");
	recheck = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_recheck(");
	call_body = start == NULL ? NULL : strstr(start, "cluster_undo_record_alloc_body(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL ? NULL
								  : strstr(finally_block, "cluster_semantic_activation_leave(");

	UT_ASSERT_NOT_NULL(body);
	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(enter);
	UT_ASSERT_NOT_NULL(try_block);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(call_body);
	UT_ASSERT_NOT_NULL(finally_block);
	UT_ASSERT_NOT_NULL(leave);
	if (body != NULL && start != NULL && end != NULL && enter != NULL && try_block != NULL
		&& recheck != NULL && call_body != NULL && finally_block != NULL && leave != NULL)
		UT_ASSERT(body < start && start < enter && enter < try_block && try_block < recheck
				  && recheck < call_body && call_body < finally_block && finally_block < leave
				  && leave < end);
	free(source);
}

/* The TT-only segment must become block0-current resident before the allocator
 * publishes it as the canonical TT binding.  XCUR may not run while the
 * lifecycle LWLock is held, so the producer is bracketed by an unlock and an
 * exact current-segment recheck after relock. */
UT_TEST(test_tt_rollover_publishes_current_before_binding_exposure)
{
	char *source = read_undo_record_source();
	const char *start;
	const char *end;
	const char *mark_active;
	const char *freeze_logical;
	const char *unlock_for_current;
	const char *ensure_current;
	const char *relock;
	const char *recheck_current;
	const char *recheck_publication;
	const char *publish_binding;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_undo_tt_rollover_locked(");
	end = start == NULL ? NULL
						: strstr(start, "\nuint64\ncluster_undo_tt_retention_rollover_count(");
	mark_active = start == NULL ? NULL : strstr(start, "cluster_undo_segment_mark_active(");
	freeze_logical
		= mark_active == NULL ? NULL : strstr(mark_active, "logical.segment_id = new_segment_id;");
	unlock_for_current
		= freeze_logical == NULL
			  ? NULL
			  : strstr(freeze_logical, "LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);");
	ensure_current = unlock_for_current == NULL
						 ? NULL
						 : strstr(unlock_for_current,
								  "cluster_undo_block0_current_live_owner_ensure_resident_exact(");
	relock = ensure_current == NULL
				 ? NULL
				 : strstr(ensure_current,
						  "LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);");
	recheck_current
		= relock == NULL ? NULL : strstr(relock, "cluster_tt_slot_current_segment(node_id)");
	recheck_publication
		= recheck_current == NULL
			  ? NULL
			  : strstr(recheck_current,
					   "cluster_undo_block0_current_live_owner_publication_recheck(");
	publish_binding = recheck_publication == NULL
						  ? NULL
						  : strstr(recheck_publication, "cluster_tt_slot_rollover(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(mark_active);
	UT_ASSERT_NOT_NULL(freeze_logical);
	UT_ASSERT_NOT_NULL(unlock_for_current);
	UT_ASSERT_NOT_NULL(ensure_current);
	UT_ASSERT_NOT_NULL(relock);
	UT_ASSERT_NOT_NULL(recheck_current);
	UT_ASSERT_NOT_NULL(recheck_publication);
	UT_ASSERT_NOT_NULL(publish_binding);
	if (start != NULL && end != NULL && mark_active != NULL && freeze_logical != NULL
		&& unlock_for_current != NULL && ensure_current != NULL && relock != NULL
		&& recheck_current != NULL && recheck_publication != NULL && publish_binding != NULL)
		UT_ASSERT(start < mark_active && mark_active < freeze_logical
				  && freeze_logical < unlock_for_current && unlock_for_current < ensure_current
				  && ensure_current < relock && relock < recheck_current
				  && recheck_current < recheck_publication && recheck_publication < publish_binding
				  && publish_binding < end);
	free(source);
}

UT_TEST(test_recycle_releases_lifecycle_before_exact_current_transition)
{
	char *source = read_undo_record_source();
	const char *start;
	const char *end;
	const char *acquire;
	const char *epoch_fence;
	const char *release;
	const char *transition;
	const char *expected_arg;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_undo_segment_advance_recyclable(");
	end = start == NULL ? NULL
						: strstr(start, "\n\n/* spec-3.13 D4: allocator-side reuse counter hook");
	acquire = start == NULL
				  ? NULL
				  : strstr(start,
						   "LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);");
	epoch_fence = acquire == NULL
					  ? NULL
					  : strstr(acquire, "cluster_undo_horizon_epoch_fence_tripped(expected_epoch)");
	release = epoch_fence == NULL
				  ? NULL
				  : strstr(epoch_fence, "LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);");
	transition
		= release == NULL ? NULL : strstr(release, "cluster_undo_segment_try_mark_recyclable(");
	expected_arg = transition == NULL ? NULL : strstr(transition, "expected_epoch");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(acquire);
	UT_ASSERT_NOT_NULL(epoch_fence);
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(transition);
	UT_ASSERT_NOT_NULL(expected_arg);
	if (start != NULL && end != NULL && acquire != NULL && epoch_fence != NULL && release != NULL
		&& transition != NULL && expected_arg != NULL)
		UT_ASSERT(start < acquire && acquire < epoch_fence && epoch_fence < release
				  && release < transition && transition < expected_arg && expected_arg < end);
	free(source);
}

UT_TEST(test_extend_selects_reuse_without_mutating_block0)
{
	char *source = read_undo_alloc_source();
	const char *start;
	const char *end;
	const char *plan;
	const char *candidate;
	const char *forbidden_reuse;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_undo_segment_extend_or_create(");
	end = start == NULL ? NULL : strstr(start, "\n\n/*\n * cluster_undo_segment_scan_max_existing");
	plan = start == NULL ? NULL : strstr(start, "ClusterUndoSegmentExtendPlan *plan");
	candidate = plan == NULL ? NULL : strstr(plan, "plan->needs_reuse = true;");
	forbidden_reuse = start == NULL ? NULL : strstr(start, "cluster_undo_segment_reuse_in_place(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(plan);
	UT_ASSERT_NOT_NULL(candidate);
	if (start != NULL && end != NULL && plan != NULL && candidate != NULL)
		UT_ASSERT(start < plan && plan < candidate && candidate < end);
	UT_ASSERT(forbidden_reuse == NULL || forbidden_reuse >= end);
	free(source);
}

UT_TEST(test_reuse_wrapper_routes_only_through_exact_current_owner)
{
	char *source = read_undo_alloc_source();
	const char *start;
	const char *end;
	const char *fresh;
	const char *current_exact;
	const char *invalidate;
	const char *note;
	const char *raw_emit;
	const char *raw_flush;
	const char *raw_write;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_undo_segment_reuse_in_place(");
	end = start == NULL ? NULL : strstr(start, "\n\n/*\n * cluster_undo_segment_generation");
	fresh = start == NULL ? NULL : strstr(start, "cluster_undo_segment_make_header_bytes(");
	current_exact = fresh == NULL
						? NULL
						: strstr(fresh, "cluster_undo_block0_current_live_owner_reuse_exact(");
	invalidate = current_exact == NULL
					 ? NULL
					 : strstr(current_exact, "cluster_undo_buf_invalidate_segment(");
	note = invalidate == NULL ? NULL
							  : strstr(invalidate, "cluster_undo_record_note_segment_reuse();");
	raw_emit = start == NULL ? NULL : strstr(start, "cluster_undo_emit_segment_reuse(");
	raw_flush = start == NULL ? NULL : strstr(start, "XLogFlush(");
	raw_write = start == NULL ? NULL : strstr(start, "write_segment_header_via_smgr(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(fresh);
	UT_ASSERT_NOT_NULL(current_exact);
	UT_ASSERT_NOT_NULL(invalidate);
	UT_ASSERT_NOT_NULL(note);
	if (start != NULL && end != NULL && fresh != NULL && current_exact != NULL && invalidate != NULL
		&& note != NULL)
		UT_ASSERT(start < fresh && fresh < current_exact && current_exact < invalidate
				  && invalidate < note && note < end);
	UT_ASSERT(raw_emit == NULL || raw_emit >= end);
	UT_ASSERT(raw_flush == NULL || raw_flush >= end);
	UT_ASSERT(raw_write == NULL || raw_write >= end);
	free(source);
}

/* Every operation that can claim/extend/recycle an extent or
 * wait for 0xFB block0 current belongs to the pre-lock prepare phase.  The
 * prepared consumer may only validate the frozen receipt and append the
 * record; moving either slow producer back into the consumer is the bug this
 * test catches. */
UT_TEST(test_record_prepare_owns_extent_and_block0_slow_paths)
{
	char *source = read_undo_record_source();
	const char *prepare;
	const char *prepare_end;
	const char *consume;
	const char *consume_end;
	const char *claim;
	const char *ensure;
	const char *retention_guard;
	const char *consume_claim;
	const char *consume_ensure;
	const char *consume_extend;
	const char *consume_read;

	if (source == NULL)
		return;
	prepare = strstr(source, "\ncluster_undo_record_prepare(");
	prepare_end
		= prepare == NULL ? NULL : strstr(prepare, "\n}\n\n\nClusterUndoRecordConsumeResult\n");
	consume = prepare_end == NULL ? NULL
								  : strstr(prepare_end, "\ncluster_undo_record_consume_prepared(");
	consume_end
		= consume == NULL ? NULL : strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	claim = prepare == NULL ? NULL : strstr(prepare, "claim_undo_extent(");
	ensure = prepare == NULL ? NULL : strstr(prepare, "cluster_undo_record_ensure_block0_current(");
	retention_guard
		= prepare == NULL ? NULL : strstr(prepare, "cluster_undo_active_write_register(");
	consume_claim = consume == NULL ? NULL : strstr(consume, "claim_undo_extent(");
	consume_ensure
		= consume == NULL
			  ? NULL
			  : strstr(consume, "cluster_undo_block0_current_live_owner_ensure_resident_exact(");
	consume_extend
		= consume == NULL ? NULL : strstr(consume, "cluster_undo_segment_extend_or_create(");
	consume_read = consume == NULL ? NULL : strstr(consume, "read_undo_block(");

	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(prepare_end);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(ensure);
	UT_ASSERT_NOT_NULL(retention_guard);
	if (prepare != NULL && prepare_end != NULL && claim != NULL && ensure != NULL
		&& retention_guard != NULL)
		UT_ASSERT(prepare < retention_guard && retention_guard < claim && claim < ensure
				  && ensure < prepare_end);
	UT_ASSERT(consume_claim == NULL || consume_claim >= consume_end);
	UT_ASSERT(consume_ensure == NULL || consume_ensure >= consume_end);
	UT_ASSERT(consume_extend == NULL || consume_extend >= consume_end);
	UT_ASSERT(consume_read == NULL || consume_read >= consume_end);
	free(source);
}

/* Ordinary existing-other starts must not reach segment creation before the
 * recovered semantic OPEN cut is visible; the STARTUP owner remains exempt. */
UT_TEST(test_record_prepare_recovered_open_gate_precedes_segment_creation)
{
	char *source = read_undo_record_source();
	const char *prepare;
	const char *guard;
	const char *gate;
	const char *segment_create;

	if (source == NULL)
		return;
	prepare = strstr(source, "cluster_undo_record_prepare_trace_impl(");
	guard = prepare == NULL ? NULL : strstr(prepare, "if (MyBackendType != B_STARTUP");
	gate = guard == NULL ? NULL : strstr(guard, "cluster_semantic_normal_start_state()");
	segment_create = prepare == NULL
					 ? NULL
					 : strstr(prepare, "cluster_undo_active_segment_for_node_or_create(");
	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(guard);
	UT_ASSERT_NOT_NULL(gate);
	UT_ASSERT_NOT_NULL(segment_create);
	if (prepare != NULL && guard != NULL && gate != NULL && segment_create != NULL)
		UT_ASSERT(prepare < guard && guard < gate && gate < segment_create);
	UT_ASSERT(strstr(gate == NULL ? "" : gate,
					"!cluster_semantic_activation_recovered_open_current()") != NULL);
	free(source);
}

/* A prepared receipt is an exact, one-shot identity.  The in-lock consumer
 * must compare the backend reservation sequence, complete extent cursor and
 * exact block0 publication before the first record byte or UBA is exposed. */
UT_TEST(test_prepared_consumer_rechecks_exact_receipt_before_record_mutation)
{
	char *source = read_undo_record_source();
	const char *preflight;
	const char *preflight_end;
	const char *consume;
	const char *consume_end;
	const char *recheck;
	const char *extent_match;
	const char *first_mutation;

	if (source == NULL)
		return;
	preflight = strstr(source, "\ncluster_undo_record_consume_preflight(");
	preflight_end = preflight == NULL
						? NULL
						: strstr(preflight, "\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_"
											"record_consume_prepared(");
	consume = preflight_end;
	consume_end
		= consume == NULL ? NULL : strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	recheck
		= preflight == NULL
			  ? NULL
			  : strstr(preflight,
					   "cluster_undo_block0_current_live_owner_publication_recheck_conditional(");
	extent_match = preflight == NULL
					   ? NULL
					   : strstr(preflight, "cluster_undo_record_receipt_extent_matches(");
	first_mutation = consume == NULL ? NULL : strstr(consume, "cluster_scn_advance(");

	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(preflight_end);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(extent_match);
	UT_ASSERT_NOT_NULL(first_mutation);
	if (preflight != NULL && preflight_end != NULL && consume != NULL && consume_end != NULL
		&& recheck != NULL && extent_match != NULL && first_mutation != NULL)
		UT_ASSERT(preflight < extent_match && extent_match < recheck && recheck < preflight_end
				  && preflight_end == consume && consume < first_mutation
				  && first_mutation < consume_end);
	free(source);
}


/* The undo DATA content lock is the final fallible local prerequisite for a
 * prepared consume.  It must be acquired conditionally before the first CTRC
 * APPLY, while a miss can still take the zero-apply retry edge.  Once APPLY
 * starts, consume may only install through that already-held exact lock; it
 * must not attempt a second conditional acquisition that can fail after the
 * receipt has become irreversible. */
UT_TEST(test_prepared_consume_lock_precedes_first_receipt_apply)
{
	char *record_source = read_undo_record_source();
	char *heap_source = read_heapam_source();
	const char *preflight;
	const char *preflight_end;
	const char *conditional_lock;
	const char *consume;
	const char *consume_end;
	const char *locked_install;
	const char *late_conditional;
	const char *boundary;
	const char *boundary_end;
	const char *boundary_preflight;
	const char *first_apply;

	if (record_source == NULL || heap_source == NULL) {
		free(record_source);
		free(heap_source);
		return;
	}
	preflight = strstr(record_source, "\ncluster_undo_record_consume_preflight(");
	preflight_end = preflight == NULL
						? NULL
						: strstr(preflight, "\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_"
											"record_consume_prepared(");
	conditional_lock
		= preflight == NULL ? NULL : strstr(preflight, "cluster_undo_buf_lock_ref_conditional(");
	consume = preflight_end;
	consume_end
		= consume == NULL ? NULL : strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	locked_install = consume == NULL
						 ? NULL
						 : strstr(consume, "cluster_undo_record_install_prepared_resident_locked(");
	late_conditional
		= consume == NULL ? NULL : strstr(consume, "cluster_undo_buf_lock_ref_conditional(");

	boundary = strstr(heap_source, "/* One caller-owned final boundary.");
	boundary
		= boundary == NULL ? NULL : strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL : strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	boundary_preflight
		= boundary == NULL ? NULL : strstr(boundary, "cluster_undo_record_consume_preflight(");
	first_apply
		= boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_apply_undo_target(");

	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(preflight_end);
	UT_ASSERT_NOT_NULL(conditional_lock);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	UT_ASSERT_NOT_NULL(locked_install);
	UT_ASSERT(late_conditional == NULL || late_conditional >= consume_end);
	UT_ASSERT_NOT_NULL(boundary);
	UT_ASSERT_NOT_NULL(boundary_end);
	UT_ASSERT_NOT_NULL(boundary_preflight);
	UT_ASSERT_NOT_NULL(first_apply);
	if (preflight != NULL && preflight_end != NULL && conditional_lock != NULL)
		UT_ASSERT(preflight < conditional_lock && conditional_lock < preflight_end);
	if (consume != NULL && consume_end != NULL && locked_install != NULL)
		UT_ASSERT(consume < locked_install && locked_install < consume_end);
	if (boundary != NULL && boundary_end != NULL && boundary_preflight != NULL
		&& first_apply != NULL)
		UT_ASSERT(boundary < boundary_preflight && boundary_preflight < first_apply
				  && first_apply < boundary_end);

	free(record_source);
	free(heap_source);
}

/* The terminal census may drop and reacquire heap content authority.  The
 * split heap pipeline must therefore finish capacity cleanup before capturing
 * the DML guard, recheck the exact receipt before allocating an ITL, and then
 * recheck every completed plan before the first APPLY and final consume. */
UT_TEST(test_terminal_census_precedes_final_receipt_recheck_and_itl_allocation)
{
	char *source = read_heapam_source();
	const char *prepare_helper;
	const char *prepare_end;
	const char *plan_helper;
	const char *plan_end;
	const char *boundary;
	const char *boundary_end;
	const char *ensure;
	const char *prepare_guard;
	const char *prepare_receipt_recheck;
	const char *plan_guard;
	const char *plan_receipt_recheck;
	const char *alloc_once;
	const char *bind_slot;
	const char *plan_guard_recheck;
	const char *boundary_recheck;
	const char *boundary_apply;
	const char *consume;
	const char *typed_retry;
	const char *prepare_retry_route;
	const char *update_retry_route;

	if (source == NULL)
		return;
	prepare_helper = strstr(source, "\ncluster_heap_itl_prepare_prepared_undo(");
	prepare_end = prepare_helper == NULL ? NULL : strstr(prepare_helper, "\n}\n");
	ensure = prepare_helper == NULL
				 ? NULL
				 : strstr(prepare_helper, "cluster_heap_itl_ensure_capacity_with_terminal_census(");
	prepare_guard = prepare_helper == NULL
						? NULL
						: strstr(prepare_helper, "cluster_heap_dml_authority_guard_capture(");
	prepare_receipt_recheck = prepare_helper == NULL
								  ? NULL
								  : strstr(prepare_helper, "cluster_undo_record_prepared_recheck(");

	plan_helper = strstr(source, "\ncluster_heap_itl_plan_prepared_undo_target(");
	plan_end
		= plan_helper == NULL ? NULL : strstr(plan_helper, "\n}\n\n\n#ifndef USE_CLUSTER_UNIT");
	plan_guard = plan_helper == NULL
					 ? NULL
					 : strstr(plan_helper, "cluster_heap_dml_authority_guard_capture(");
	plan_receipt_recheck
		= plan_helper == NULL ? NULL : strstr(plan_helper, "cluster_undo_record_prepared_recheck(");
	alloc_once = plan_helper == NULL ? NULL : strstr(plan_helper, "cluster_heap_itl_alloc_once(");
	bind_slot = plan_helper == NULL
					? NULL
					: strstr(plan_helper, "cluster_heap_dml_authority_guard_bind_itl_slot(");
	plan_guard_recheck
		= bind_slot == NULL ? NULL : strstr(bind_slot, "cluster_heap_dml_authority_guard_recheck(");

	boundary = strstr(source, "/* One caller-owned final boundary.");
	boundary
		= boundary == NULL ? NULL : strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL : strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	boundary_recheck
		= boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_undo_target_recheck(");
	boundary_apply
		= boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_apply_undo_target(");
	consume = boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_consume_undo(");
	typed_retry = strstr(source, "CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");
	prepare_retry_route = prepare_helper == NULL
							  ? NULL
							  : strstr(prepare_helper, "CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");
	update_retry_route
		= strstr(source, "old_capacity_result == CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");

	UT_ASSERT_NOT_NULL(prepare_helper);
	UT_ASSERT_NOT_NULL(prepare_end);
	UT_ASSERT_NOT_NULL(ensure);
	UT_ASSERT_NOT_NULL(prepare_guard);
	UT_ASSERT_NOT_NULL(prepare_receipt_recheck);
	UT_ASSERT_NOT_NULL(plan_helper);
	UT_ASSERT_NOT_NULL(plan_end);
	UT_ASSERT_NOT_NULL(plan_guard);
	UT_ASSERT_NOT_NULL(plan_receipt_recheck);
	UT_ASSERT_NOT_NULL(alloc_once);
	UT_ASSERT_NOT_NULL(bind_slot);
	UT_ASSERT_NOT_NULL(plan_guard_recheck);
	UT_ASSERT_NOT_NULL(boundary);
	UT_ASSERT_NOT_NULL(boundary_end);
	UT_ASSERT_NOT_NULL(boundary_recheck);
	UT_ASSERT_NOT_NULL(boundary_apply);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(typed_retry);
	UT_ASSERT_NOT_NULL(prepare_retry_route);
	UT_ASSERT_NOT_NULL(update_retry_route);
	if (prepare_helper != NULL && prepare_end != NULL && ensure != NULL && prepare_guard != NULL
		&& prepare_receipt_recheck != NULL)
		UT_ASSERT(prepare_helper < ensure && ensure < prepare_guard
				  && prepare_guard < prepare_receipt_recheck
				  && prepare_receipt_recheck < prepare_end);
	if (plan_helper != NULL && plan_end != NULL && plan_guard != NULL
		&& plan_receipt_recheck != NULL && alloc_once != NULL && bind_slot != NULL
		&& plan_guard_recheck != NULL)
		UT_ASSERT(plan_helper < plan_guard && plan_guard < plan_receipt_recheck
				  && plan_receipt_recheck < alloc_once && alloc_once < bind_slot
				  && bind_slot < plan_guard_recheck && plan_guard_recheck < plan_end);
	if (boundary != NULL && boundary_end != NULL && boundary_recheck != NULL
		&& boundary_apply != NULL && consume != NULL)
		UT_ASSERT(boundary < boundary_recheck && boundary_recheck < boundary_apply
				  && boundary_apply < consume && consume < boundary_end);
	free(source);
}

/* A cross-page UPDATE owns two independent page-reference receipts.  The
 * pre-mutation gate must reject a partial pair, a missing exact handle, or a
 * receipt that has already crossed APPLY. */
UT_TEST(test_ctrc_cross_page_update_requires_both_prepared_receipts)
{
	ClusterUndoRecordPrepareReceipt receipt = { 0 };
	receipt.ctrc_attempt_generation = 1;

	receipt.ctrc_pending_mask = UINT8_C(3);
	receipt.ctrc_prepared_mask = UINT8_C(3);
	receipt.ctrc_handles[0].valid = true;
	receipt.ctrc_handles[1].valid = true;
	UT_ASSERT(cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = UINT8_C(1);
	UT_ASSERT(cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = UINT8_C(4);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = 0;

	receipt.ctrc_prepared_mask = UINT8_C(1);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	receipt.ctrc_prepared_mask = UINT8_C(3);
	receipt.ctrc_handles[1].valid = false;
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	receipt.ctrc_handles[1].valid = true;
	receipt.ctrc_applied_mask = UINT8_C(1);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(3)));
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, 0));
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(&receipt, UINT8_C(4)));
}

/* The UPDATE caller must route both same-page and cross-page passes through
 * the shared required-mask gate.  It first adopts an exact same-ITL receipt
 * while both page locks remain held; only a miss reaches blocking PREPARE
 * after unlock. */
UT_TEST(test_heap_update_applies_required_receipts_before_undo_and_page_mutation)
{
	char *source = read_heapam_source();
	const char *update;
	const char *update_end;
	const char *stage;
	const char *reuse;
	const char *prepare;
	const char *required;
	const char *unlock;

	if (source == NULL)
		return;
	update = strstr(source, "\nheap_update(");
	update_end = update == NULL ? NULL : strstr(update, "\nheap_lock_tuple_internal(");
	stage = update == NULL ? NULL : strstr(update, "cluster_undo_record_ctrc_stage_pending(");
	reuse = update == NULL ? NULL : strstr(update, "cluster_heap_ctrc_stage_reusable_itl_receipt(");
	prepare = update == NULL ? NULL : strstr(update, "cluster_undo_record_ctrc_prepare_pending(");
	required
		= update == NULL ? NULL : strstr(update, "cluster_undo_record_ctrc_required_prepared(");
	unlock = reuse == NULL ? NULL : strstr(reuse, "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);");

	UT_ASSERT_NOT_NULL(update);
	UT_ASSERT_NOT_NULL(update_end);
	UT_ASSERT_NOT_NULL(stage);
	UT_ASSERT_NOT_NULL(reuse);
	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(required);
	UT_ASSERT_NOT_NULL(unlock);
	if (update != NULL && update_end != NULL && stage != NULL && reuse != NULL && prepare != NULL
		&& required != NULL && unlock != NULL)
		UT_ASSERT(update < stage && stage < reuse && reuse < required && required < unlock
				  && unlock < prepare && prepare < update_end);
	free(source);
}

/* MXA-T33: terminal transition, redo and fresh reuse must all share one
 * canonical release-bit contract.  The focused CTRC runtime test exercises
 * the transition table; this anchor additionally prevents the three product
 * owners from silently omitting the frozen opcode or clear-on-reuse symbol. */
UT_TEST(test_ctrc_release_flag_terminal_recycle_reuse_and_redo_matrix)
{
	char *xlog_header = read_source_file(UNDO_XLOG_HEADER_PATH);
	char *xlog_source = read_source_file(UNDO_XLOG_SOURCE_PATH);
	char *format_header = read_source_file(TT_SLOT_HEADER_PATH);

	if (xlog_header == NULL || xlog_source == NULL || format_header == NULL) {
		free(xlog_header);
		free(xlog_source);
		free(format_header);
		return;
	}
	UT_ASSERT_NOT_NULL(strstr(xlog_header, "XLOG_UNDO_TT_SLOT_CTRC_RELEASE"));
	UT_ASSERT_NOT_NULL(strstr(xlog_header, "xl_undo_tt_slot_ctrc_release_v1"));
	UT_ASSERT_NOT_NULL(strstr(xlog_source, "cluster_undo_xlog_insert_tt_ctrc_release"));
	UT_ASSERT_NOT_NULL(strstr(xlog_source, "cluster_tt_durable_redo_ctrc_release_slot_exact"));
	UT_ASSERT_NOT_NULL(strstr(format_header, "TT_SLOT_FLAG_CTRC_RELEASE_PROVEN"));
	free(xlog_header);
	free(xlog_source);
	free(format_header);
}

/* A pre-lock prepare may legitimately lose a conditional block0/current
 * recheck under same-node concurrency.  All four heap DML callers must route
 * through one bounded helper which freezes the absolute deadline once,
 * retries only RETRY_REQUIRED with that same value, and terminates REFUSED.
 * This keeps the retry outside heap content authority without turning a
 * transient prepare result into a user-visible SQL error. */
UT_TEST(test_heap_prepare_retries_transient_result_under_one_deadline)
{
	char *source = read_heapam_source();
	const char *helper;
	const char *helper_end;
	const char *deadline;
	const char *prepare_call;
	const char *production_alias;
	const char *ready;
	const char *retry;
	const char *refused;
	const char *interrupts;
	const char *hit;
	int helper_mentions = 0;
	int wrapper_calls = 0;
	int raw_prepare_calls = 0;

	if (source == NULL)
		return;
	helper = strstr(source, "\ncluster_heap_prepare_undo_record_exact(");
	helper_end = helper == NULL
					 ? NULL
					 : strstr(helper, "\n}\n\ntypedef enum ClusterHeapPreparedUndoResult");
	deadline = helper == NULL ? NULL : strstr(helper, "uint64 absolute_deadline_us");
	prepare_call = helper == NULL ? NULL : strstr(helper, "cluster_heap_prepare_undo_record_call(");
	production_alias = strstr(
		source, "#define cluster_heap_prepare_undo_record_call cluster_undo_record_prepare");
	ready = helper == NULL ? NULL : strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_READY");
	retry = helper == NULL ? NULL : strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED");
	refused = helper == NULL ? NULL : strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_REFUSED");
	interrupts = helper == NULL ? NULL : strstr(helper, "CHECK_FOR_INTERRUPTS()");

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(deadline);
	UT_ASSERT_NOT_NULL(prepare_call);
	UT_ASSERT_NOT_NULL(production_alias);
	UT_ASSERT_NOT_NULL(ready);
	UT_ASSERT_NOT_NULL(retry);
	UT_ASSERT_NOT_NULL(refused);
	UT_ASSERT_NOT_NULL(interrupts);
	if (helper != NULL && helper_end != NULL && deadline != NULL && prepare_call != NULL
		&& ready != NULL && retry != NULL && refused != NULL && interrupts != NULL)
		UT_ASSERT(helper < deadline && deadline < prepare_call && prepare_call < ready
				  && ready < retry && retry < refused && refused < interrupts
				  && interrupts < helper_end);

	for (hit = source; (hit = strstr(hit, "cluster_heap_prepare_undo_record_exact(")) != NULL;
		 hit++)
		helper_mentions++;
	for (hit = source; (hit = strstr(hit, "cluster_heap_prepare_undo_record_call(")) != NULL; hit++)
		wrapper_calls++;
	for (hit = source; (hit = strstr(hit, "cluster_undo_record_prepare(")) != NULL; hit++)
		raw_prepare_calls++;
	/* Definition, retry wrapper, unit seam, five initial producer sites,
	 * and the shared post-nested-producer preparation phase. */
	UT_ASSERT_EQ(helper_mentions, 9);
	UT_ASSERT_EQ(wrapper_calls, 2);
	UT_ASSERT_EQ(raw_prepare_calls, 0);
	free(source);
}

/* An in-lock conditional miss is not a terminal allocation failure.  The
 * split planner must preserve RETRY_REQUIRED before allocation, and the final
 * boundary may expose a retry only while zero receipts have crossed APPLY.
 * Once APPLY begins, every later failure is terminal for that attempt. */
UT_TEST(test_inlock_consume_preserves_retry_required_for_heap_unwind)
{
	char *source = read_heapam_source();
	const char *result_enum;
	const char *plan;
	const char *plan_end;
	const char *recheck;
	const char *alloc_once;
	const char *retry_result;
	const char *refused_result;
	const char *boundary;
	const char *boundary_end;
	const char *preflight;
	const char *apply;
	const char *consume;
	const char *zero_apply_result;

	if (source == NULL)
		return;
	result_enum = strstr(source, "typedef enum ClusterHeapPreparedUndoResult");
	plan = strstr(source, "\ncluster_heap_itl_plan_prepared_undo_target(");
	plan_end = plan == NULL ? NULL : strstr(plan, "\n}\n\n\n#ifndef USE_CLUSTER_UNIT");
	recheck = plan == NULL ? NULL : strstr(plan, "cluster_undo_record_prepared_recheck(");
	alloc_once = plan == NULL ? NULL : strstr(plan, "cluster_heap_itl_alloc_once(");
	retry_result = plan == NULL ? NULL : strstr(plan, "CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED");
	refused_result = plan == NULL ? NULL : strstr(plan, "CLUSTER_HEAP_PREPARED_UNDO_REFUSED");

	boundary = strstr(source, "/* One caller-owned final boundary.");
	boundary
		= boundary == NULL ? NULL : strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL : strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	preflight
		= boundary == NULL ? NULL : strstr(boundary, "cluster_undo_record_consume_preflight(");
	apply = boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_apply_undo_target(");
	consume = boundary == NULL ? NULL : strstr(boundary, "cluster_heap_boundary_consume_undo(");
	zero_apply_result
		= boundary == NULL ? NULL : strstr(boundary, "CLUSTER_HEAP_BOUNDARY_ZERO_APPLY_RETRY");

	UT_ASSERT_NOT_NULL(result_enum);
	UT_ASSERT_NOT_NULL(plan);
	UT_ASSERT_NOT_NULL(plan_end);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(alloc_once);
	UT_ASSERT_NOT_NULL(retry_result);
	UT_ASSERT_NOT_NULL(refused_result);
	UT_ASSERT_NOT_NULL(boundary);
	UT_ASSERT_NOT_NULL(boundary_end);
	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(apply);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(zero_apply_result);
	if (plan != NULL && plan_end != NULL && recheck != NULL && alloc_once != NULL
		&& retry_result != NULL && refused_result != NULL)
		UT_ASSERT(plan < recheck && recheck < alloc_once && retry_result < plan_end
				  && refused_result < plan_end);
	if (boundary != NULL && boundary_end != NULL && preflight != NULL && apply != NULL
		&& consume != NULL && zero_apply_result != NULL)
		UT_ASSERT(boundary < zero_apply_result && zero_apply_result < preflight && preflight < apply
				  && apply < consume && consume < boundary_end);
	free(source);
}

/* INSERT, DELETE, UPDATE, heap_lock_tuple, and the update-chain lock producer
 * each own different heap-lock unwind mechanics.  Their five CTRC PREPARE
 * sites must remain outside heap content authority.  The four primary DML
 * callers additionally reuse one frozen operation deadline across their
 * initial preparation and full native restart paths. */
UT_TEST(test_all_heap_dml_callers_reprepare_outside_content_lock)
{
	char *source = read_heapam_source();
	const char *helper;
	const char *helper_end;
	const char *deadline_parameter;
	const char *hit;
	int ctrc_prepare_sites = 0;
	int cancel_sites = 0;
	int deadline_locals = 0;

	if (source == NULL)
		return;
	helper = strstr(source, "\ncluster_heap_prepare_undo_record_exact(");
	helper_end = helper == NULL ? NULL : strstr(helper, "\n}\n");
	deadline_parameter = helper == NULL ? NULL : strstr(helper, "uint64 absolute_deadline_us");

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(deadline_parameter);
	if (helper != NULL && helper_end != NULL && deadline_parameter != NULL) {
		UT_ASSERT(deadline_parameter < helper_end);
		UT_ASSERT(strstr(helper, "cluster_undo_record_prepare_deadline_us()") == NULL
				  || strstr(helper, "cluster_undo_record_prepare_deadline_us()") >= helper_end);
	}

	for (hit = source; (hit = strstr(hit, "cluster_undo_record_ctrc_prepare_pending(")) != NULL;
		 hit++)
		ctrc_prepare_sites++;
	for (hit = source; (hit = strstr(hit, "cluster_undo_record_cancel_prepared(")) != NULL; hit++)
		cancel_sites++;
	for (hit = source; (hit = strstr(hit, "undo_prepare_deadline_us")) != NULL; hit++)
		deadline_locals++;

	/* Each heap DML family prepares its CTRC receipt only after dropping the
	 * content lock.  Do not count enum/result spellings: UPDATE legitimately
	 * has more than one pre-mutation retry classification. */
	UT_ASSERT_EQ(ctrc_prepare_sites, 5);
	/* Two chain exits, TOAST, the nested row-lock adapter and the row-lock
	 * unpublished normal-return cleanup. Actual-C tests cover both new owners. */
	UT_ASSERT_EQ(cancel_sites, 5);
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_undo_record_requalify_for_retry("));
	/* Four declarations plus initial/reprepare uses on all callers. */
	UT_ASSERT(deadline_locals >= 12);
	free(source);
}

/* Native WAL insertion locks remain legal under the standard heap lock order.
 * The forbidden set is narrower: the prepared consumer may not enter any
 * cluster/undo producer, wait, miss-fill, lifecycle, victim, or storage-I/O
 * path.  Removing one of these exclusions would reintroduce the rank inversion
 * while a blanket LWLockAcquire ban would incorrectly reject PostgreSQL WAL. */
UT_TEST(test_prepared_consumer_excludes_only_cluster_undo_slow_paths)
{
	char *source = read_undo_record_source();
	const char *preflight;
	const char *consume;
	const char *consume_end;
	const char *forbidden[] = {
		"claim_undo_extent(",
		"cluster_undo_block0_current_live_owner_ensure_resident_exact(",
		"cluster_undo_segment_extend_or_create(",
		"cluster_undo_pending_flush_internal(",
		"cluster_undo_buf_pin(",
		"read_undo_block(",
		"write_undo_block_ext(",
		"cluster_undo_wal_protect_block(",
		"pg_usleep(",
	};
	size_t i;

	if (source == NULL)
		return;
	preflight = strstr(source, "\ncluster_undo_record_consume_preflight(");
	consume
		= preflight == NULL ? NULL : strstr(preflight, "\ncluster_undo_record_consume_prepared(");
	consume_end
		= consume == NULL ? NULL : strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	if (preflight != NULL && consume != NULL && consume_end != NULL) {
		for (i = 0; i < lengthof(forbidden); i++) {
			const char *hit = strstr(preflight, forbidden[i]);

			UT_ASSERT(hit == NULL || hit >= consume_end);
		}
		UT_ASSERT_NOT_NULL(strstr(preflight, "cluster_undo_buf_lock_ref_conditional("));
		UT_ASSERT_NOT_NULL(
			strstr(consume, "cluster_undo_record_install_prepared_resident_locked("));
	}
	free(source);
}

/* Prepare-only reservation is bounded and cancelable.  A later receipt
 * supersedes the old sequence; cancel consumes only the exact active
 * reservation, and every consumer invalidates it before returning. */
UT_TEST(test_prepared_receipt_is_single_use_and_exactly_cancelable)
{
	char *source = read_undo_record_source();
	const char *prepare;
	const char *cancel;
	const char *consume;
	const char *sequence_publish;
	const char *cancel_match;
	const char *consume_invalidate;

	if (source == NULL)
		return;
	prepare = strstr(source, "\ncluster_undo_record_prepare(");
	cancel = strstr(source, "\ncluster_undo_record_cancel_prepared(");
	consume = strstr(source, "\ncluster_undo_record_consume_prepared(");
	sequence_publish = prepare == NULL ? NULL : strstr(prepare, "receipt->reservation_sequence =");
	cancel_match = cancel == NULL ? NULL : strstr(cancel, "reservation_sequence");
	consume_invalidate = consume == NULL
							 ? NULL
							 : strstr(consume, "cluster_undo_record_reservation.active = false;");

	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(cancel);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(sequence_publish);
	UT_ASSERT_NOT_NULL(cancel_match);
	UT_ASSERT_NOT_NULL(consume_invalidate);
	free(source);
}

/* The prepare-only reservation must retain the existing R4 modifier
 * admission debt until exact cancel or successful single-use consume.  The
 * in-lock recheck is the bounded atomic snapshot path; it must never enter a
 * new admission round or refresh the caller's absolute deadline. */
UT_TEST(test_prepared_receipt_retains_exact_admission_until_close)
{
	char *source = read_undo_record_source();
	const char *prepare;
	const char *prepare_end;
	const char *recheck;
	const char *recheck_end;
	const char *cancel;
	const char *cancel_end;
	const char *preflight;
	const char *preflight_end;
	const char *consume;
	const char *consume_end;

	if (source == NULL)
		return;
	prepare = strstr(source, "\ncluster_undo_record_prepare(");
	prepare_end
		= prepare == NULL ? NULL : strstr(prepare, "\ncluster_undo_record_prepared_recheck(");
	recheck = prepare_end;
	recheck_end = recheck == NULL ? NULL : strstr(recheck, "\n}\n");
	cancel = strstr(source, "\ncluster_undo_record_cancel_prepared(");
	cancel_end = cancel == NULL
					 ? NULL
					 : strstr(cancel, "\n}\n\n\nClusterUndoRecordConsumePreflightResult\ncluster_"
									  "undo_record_consume_preflight(");
	preflight = cancel_end;
	preflight_end = preflight == NULL
						? NULL
						: strstr(preflight, "\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_"
											"record_consume_prepared(");
	consume = preflight_end;
	consume_end
		= consume == NULL ? NULL : strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");

	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(prepare_end);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(recheck_end);
	UT_ASSERT_NOT_NULL(cancel);
	UT_ASSERT_NOT_NULL(cancel_end);
	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(preflight_end);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	if (prepare != NULL && prepare_end != NULL) {
		const char *enter = strstr(prepare, "cluster_semantic_activation_modifier_enter(");
		const char *freeze = strstr(prepare, "modifier_admission");

		UT_ASSERT(enter != NULL && enter < prepare_end);
		UT_ASSERT(freeze != NULL && freeze < prepare_end);
	}
	if (recheck != NULL && recheck_end != NULL) {
		const char *exact = strstr(recheck, "cluster_semantic_activation_modifier_recheck(");

		UT_ASSERT(exact != NULL && exact < recheck_end);
		UT_ASSERT(strstr(recheck, "cluster_undo_record_prepare_deadline_us(") == NULL);
	}
	if (cancel != NULL && cancel_end != NULL) {
		const char *leave = strstr(cancel, "cluster_semantic_activation_leave(");

		UT_ASSERT(leave != NULL && leave < cancel_end);
	}
	if (preflight != NULL && preflight_end != NULL) {
		const char *exact = strstr(preflight, "cluster_semantic_activation_modifier_recheck(");

		UT_ASSERT(exact != NULL && exact < preflight_end);
		UT_ASSERT(strstr(preflight, "cluster_undo_record_prepare_deadline_us(") == NULL);
	}
	if (consume != NULL && consume_end != NULL) {
		const char *leave = strstr(consume, "cluster_semantic_activation_leave(");
		const char *enter = strstr(consume, "cluster_semantic_activation_modifier_enter(");

		UT_ASSERT(leave != NULL && leave < consume_end);
		UT_ASSERT(enter == NULL || enter >= consume_end);
		UT_ASSERT(strstr(consume, "cluster_undo_record_prepare_deadline_us(") == NULL);
	}
	free(source);
}

UT_TEST(test_lifecycle_activation_refusal_reports_original_current_stage)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		PGAlignedBlock before;
		char expected[64];
		ClusterUndoBlock0Result failure = fault == 0 ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
													   : CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

		undo_test_make_header(1, 1, SEGMENT_ALLOCATED, undo_test_lifecycle_disk.data);
		memcpy(before.data, undo_test_lifecycle_disk.data, BLCKSZ);
		undo_test_lifecycle_mutate_calls = 0;
		undo_test_lifecycle_ensure_result = fault == 0 ? failure : CLUSTER_UNDO_BLOCK0_OK;
		undo_test_lifecycle_mutate_result = fault == 1 ? failure : CLUSTER_UNDO_BLOCK0_OK;
		receipt_last_log[0] = '\0';
		receipt_log_enabled = true;
		UT_ASSERT(!cluster_undo_segment_mark_active(1, 1));
		receipt_log_enabled = false;
		UT_ASSERT_EQ(undo_test_lifecycle_mutate_calls, fault);
		UT_ASSERT_EQ(memcmp(before.data, undo_test_lifecycle_disk.data, BLCKSZ), 0);
		UT_ASSERT(strstr(receipt_last_log, fault == 0 ? "stage=ensure_resident"
														  : "stage=mutate_exact") != NULL);
		snprintf(expected, sizeof(expected), "result=%d", (int)failure);
		UT_ASSERT(strstr(receipt_last_log, expected) != NULL);
		UT_ASSERT(strstr(receipt_last_log, "segment=1 owner=1 generation=0") != NULL);
	}
	undo_test_lifecycle_ensure_result = undo_test_lifecycle_mutate_result = CLUSTER_UNDO_BLOCK0_OK;
}

/* A lifecycle RMW may have read block zero just before a canonical publisher
 * installs a different TT slot.  The lifecycle update owns only its metadata
 * bytes; applying it must preserve that concurrent canonical slot. */
UT_TEST(test_lifecycle_bitmap_update_preserves_concurrent_canonical_tt_slot)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	TTSlot expected;

	undo_test_make_header(1, 1, SEGMENT_ACTIVE, undo_test_lifecycle_disk.data);
	memset(&expected, 0, sizeof(expected));
	expected.status = TT_SLOT_ACTIVE;
	expected.xid = (TransactionId)700;
	expected.wrap = 3;
	expected.commit_scn = InvalidScn;
	undo_test_publish_tt_after_block0_read = true;

	UT_ASSERT(cluster_undo_segment_mark_block_range_used(1, 1, 8, 4));
	UT_ASSERT_EQ(memcmp(&disk->tt_slots[7], &expected, sizeof(expected)), 0);
	UT_ASSERT((disk->free_block_bitmap[1] & UINT8_C(0x0f)) == UINT8_C(0x0f));
}

/* The record-segment seal/commit path is another lifecycle-only block-zero
 * writer.  A canonical publisher may install a TT slot after this path reads
 * its lifecycle snapshot; persisting the seal must not write that stale slot
 * image back over the canonical authority. */
UT_TEST(test_record_segment_seal_preserves_concurrent_canonical_tt_slot)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	TTSlot expected;

	undo_test_reset_record_shmem();
	undo_test_make_header(1, 1, SEGMENT_ACTIVE, undo_test_lifecycle_disk.data);
	memset(&expected, 0, sizeof(expected));
	expected.status = TT_SLOT_ACTIVE;
	expected.xid = (TransactionId)700;
	expected.wrap = 3;
	expected.commit_scn = InvalidScn;
	undo_test_publish_tt_after_block0_read = true;

	cluster_undo_try_mark_record_segment_committed(1, 1, (SCN)42);
	UT_ASSERT_EQ(memcmp(&disk->tt_slots[7], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ((long long)UndoSegmentHeader_record_seal_upper_scn(disk), 42LL);
}

/* The six-gate predicate is covered by the real retention suite. Here its
 * result is the runtime seam: exercise the real record-owner publication. */
UT_TEST(test_record_drain_owner_stamps_only_after_gate)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	PGAlignedBlock retained;

	undo_test_reset_record_shmem();
	undo_test_make_header(2, 1, SEGMENT_ACTIVE, undo_test_lifecycle_disk.data);
	UndoSegmentHeader_set_record_seal_upper_scn(disk, 42);
	undo_test_drain_allowed = false;
	memcpy(retained.data, disk, BLCKSZ);
	cluster_undo_try_mark_record_segment_committed(2, 1, InvalidScn);
	UT_ASSERT_EQ(memcmp(retained.data, disk, BLCKSZ), 0);
	undo_test_drain_allowed = true;
	cluster_undo_try_mark_record_segment_committed(2, 1, InvalidScn);
	undo_test_drain_allowed = false;
	UT_ASSERT_EQ(disk->segment_state, SEGMENT_COMMITTED);
	UT_ASSERT_EQ(UndoSegmentHeader_record_seal_upper_scn(disk), 42);
	UT_ASSERT_EQ(UndoSegmentHeader_record_drain_upper_scn(disk), 1000);
	memcpy(retained.data, disk, BLCKSZ);
	cluster_undo_try_mark_record_segment_committed(2, 1, 2000);
	UT_ASSERT_EQ(memcmp(retained.data, disk, BLCKSZ), 0);

	undo_test_make_header(2, 1, SEGMENT_ACTIVE, undo_test_lifecycle_disk.data);
	UndoSegmentHeader_set_record_seal_upper_scn(disk, 1001);
	memcpy(retained.data, disk, BLCKSZ);
	undo_test_drain_allowed = true;
	cluster_undo_try_mark_record_segment_committed(2, 1, InvalidScn);
	undo_test_drain_allowed = false;
	UT_ASSERT_EQ(memcmp(retained.data, disk, BLCKSZ), 0);
}

UT_TEST(test_terminal_tt_only_sweep_preserves_actual_recycle_eligibility)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	PGAlignedBlock before;
	int slot;
	int pass;

	undo_test_reset_record_shmem();
	undo_test_make_header(2, 1, SEGMENT_COMMITTED, undo_test_lifecycle_disk.data);
	for (slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++) {
		TTSlot *tt = &disk->tt_slots[slot];

		memset(tt, 0, sizeof(*tt));
		tt->xid = 700 + slot;
		tt->wrap = 1;
		tt->status = TT_SLOT_COMMITTED;
		tt->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		tt->commit_scn = 100;
	}
	UT_ASSERT(cluster_undo_segment_recyclable_for_mode(disk, 1000, true));
	memcpy(before.data, disk, BLCKSZ);
	undo_test_lifecycle_mutate_calls = 0;
	for (pass = 0; pass < 3; pass++) {
		/* The cleaner-facing wrapper supplies a fresh nonzero seal clock. */
		cluster_undo_segment_advance_committed(2);
		UT_ASSERT_EQ(memcmp(before.data, disk, BLCKSZ), 0);
		UT_ASSERT(cluster_undo_segment_recyclable_for_mode(disk, 1000, true));
	}
	UT_ASSERT_EQ(undo_test_lifecycle_mutate_calls, 0);

	/* Old malformed terminal history is retained, never repaired by a sweep. */
	UndoSegmentHeader_set_record_seal_upper_scn(disk, 200);
	memcpy(before.data, disk, BLCKSZ);
	cluster_undo_segment_advance_committed(2);
	UT_ASSERT_EQ(memcmp(before.data, disk, BLCKSZ), 0);
	UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(disk, 1000, true));
	UT_ASSERT_EQ(undo_test_lifecycle_mutate_calls, 0);
}

UT_TEST(test_record_seal_owner_ignores_every_nonactive_predecessor)
{
	const uint8 states[] = { SEGMENT_ALLOCATED, SEGMENT_COMMITTED, SEGMENT_RECYCLABLE };
	PGAlignedBlock before;
	unsigned i;

	for (i = 0; i < lengthof(states); i++) {
		undo_test_reset_record_shmem();
		undo_test_make_header(2, 1, states[i], undo_test_lifecycle_disk.data);
		memcpy(before.data, undo_test_lifecycle_disk.data, BLCKSZ);
		undo_test_lifecycle_mutate_calls = 0;
		cluster_undo_try_mark_record_segment_committed(2, 1, 42);
		UT_ASSERT_EQ(memcmp(before.data, undo_test_lifecycle_disk.data, BLCKSZ), 0);
		UT_ASSERT_EQ(undo_test_lifecycle_mutate_calls, 0);
	}
}

/* Live block-zero lifecycle bytes and canonical TT slots share one
 * current/resident mutation domain.  Partial raw smgr writers are not a
 * second legal serialization domain, even when they preserve tt_slots[]. */
UT_TEST(test_live_lifecycle_writers_use_only_exact_block0_current)
{
	char *alloc_source = read_undo_alloc_source();
	char *record_source = read_undo_record_source();
	const char *helper;
	const char *ensure_resident;
	const char *mutate_exact;

	if (alloc_source == NULL || record_source == NULL) {
		free(alloc_source);
		free(record_source);
		return;
	}

	UT_ASSERT(strstr(alloc_source, "write_segment_lifecycle_header_via_smgr") == NULL);
	UT_ASSERT(strstr(record_source, "cluster_undo_write_record_lifecycle_header") == NULL);
	UT_ASSERT_NOT_NULL(
		strstr(alloc_source, "cluster_undo_block0_current_live_owner_lifecycle_exact("));
	UT_ASSERT_NOT_NULL(
		strstr(record_source, "cluster_undo_block0_current_live_owner_mutate_exact("));
	helper = strstr(alloc_source, "cluster_undo_block0_current_live_owner_lifecycle_exact(");
	ensure_resident
		= helper == NULL
			  ? NULL
			  : strstr(helper, "cluster_undo_block0_current_live_owner_ensure_resident(");
	mutate_exact = helper == NULL
					   ? NULL
					   : strstr(helper, "cluster_undo_block0_current_live_owner_mutate_exact(");
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(ensure_resident);
	UT_ASSERT_NOT_NULL(mutate_exact);
	if (helper != NULL && ensure_resident != NULL && mutate_exact != NULL)
		UT_ASSERT(helper < ensure_resident && ensure_resident < mutate_exact);

	free(alloc_source);
	free(record_source);
}


static void
receipt_fixture_ready(uint8 record_type, ClusterUndoRecordPrepareReceipt *receipt)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;
	UndoBlockHeader *header;

	undo_test_reset_record_shmem();
	memset(reservation, 0, sizeof(*reservation));
	memset(&cluster_undo_current_extent, 0, sizeof(cluster_undo_current_extent));
	memset(&cluster_undo_pending, 0, sizeof(cluster_undo_pending));
	cluster_undo_local_head_count = 0;
	memset(&cluster_undo_retry_cancel_snapshot, 0, sizeof(cluster_undo_retry_cancel_snapshot));
	cluster_node_id = 0;
	cluster_shared_config = false;
	MyBackendId = 1;
	receipt_trap_error = false;
	receipt_error_level = receipt_error_code = 0;
	receipt_error_detail[0] = '\0';
	receipt_apply_result = CLUSTER_CTRC_APPLY_APPLIED;
	receipt_fail_sync_after_apply = false;
	receipt_throw_apply = false;
	receipt_zero_evidence = false;
	receipt_evidence_valid = true;
	cluster_injection_armed_count = 0;
	receipt_after_apply_injection_hits = 0;
	receipt_clock_us = 99;
	receipt_clock_after_unref = 0;
	receipt_log_enabled = false;
	receipt_owner_probe_case = 0;
	cluster_trace_normal_stop = false;
	CritSectionCount = 0;
	receipt_last_log[0] = '\0';
	receipt_all_logs[0] = '\0'; receipt_log_count = receipt_owner_calls = 0;
	receipt_observe_calls = 0;
	receipt_observe_available = receipt_observe_throw = false;
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	memset(&undo_receipt_consumer_trace, 0, sizeof(undo_receipt_consumer_trace));
	undo_receipt_consumer_overwritten = undo_receipt_consumer_critical_skipped = undo_receipt_consumer_lwlock_skipped = 0;
#endif
	receipt_trace_lwlock_held = receipt_lifecycle_controls = false;
	receipt_fail_sync_after_prepare = false; receipt_fail_log_event = NULL;
	receipt_prepare_result = CLUSTER_CTRC_PREPARE_READY;
	receipt_prepare_path = CLUSTER_CTRC_PREPARE_PATH_TABLE;
	receipt_prepare_observed_calls = 0;
	receipt_clock_after_lock = 0;
	receipt_modifier_valid = true;
	receipt_block0_valid = true;
	receipt_block0_conditional_available = true;
	receipt_buffer_available = true;
	receipt_buffer_locked = false;
	receipt_prepare_calls = receipt_apply_calls = receipt_cancel_calls = 0;
	receipt_cancel_fail_at = 0;
	receipt_child_attempt = 0;
	receipt_leave_calls = receipt_unref_calls = receipt_install_calls = 0;
	receipt_wal_calls = receipt_wal_install_calls = 0;
	receipt_wal_throw = false;
	cluster_undo_current_extent.segment_id = 1;
	cluster_undo_current_extent.first_block = 1;
	cluster_undo_current_extent.nblocks = 1;
	cluster_undo_current_extent.cur_block = 1;
	cluster_undo_current_extent.cur_free_offset = sizeof(UndoBlockHeader);
	memset(receipt, 0, sizeof(*receipt));
	receipt->magic = CLUSTER_UNDO_RECORD_RECEIPT_MAGIC;
	receipt->record_type = record_type;
	receipt->owner_instance = 1;
	receipt->payload_capacity = 128;
	receipt->tt_slot_segment_id = 1;
	receipt->tt_slot_offset = cluster_tt_slot_id_to_offset(1);
	receipt->actual_segment_id = 1;
	receipt->reservation_sequence = 17;
	receipt->absolute_deadline_us = 100;
	receipt->ctrc_attempt_generation = 1;
	receipt->extent = cluster_undo_current_extent;
	receipt->modifier_admission.entered = true;
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	memset(&receipt_operation, 0, sizeof(receipt_operation));
	receipt_operation.top.segment_id = 1;
	receipt_operation.top.segment_generation = 0;
	receipt_operation.top.xid = 700;
	receipt_operation.top.slot_wrap = 7;
	receipt_operation.top.origin_instance = 1;
	receipt_operation.top.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	receipt_operation.top.active_lsn = 100;
	receipt_operation.top_fxid = FullTransactionIdFromEpochAndXid(5, 700);
	receipt_operation.operation_fxid = receipt_operation.top_fxid;
	receipt_operation.operation_sequence = 31;
	receipt_operation.command_id = 9;
	receipt_operation.subid = TopSubTransactionId;
	receipt->operation = receipt_operation;
	receipt_capture_ok = receipt_scope_valid = true;
	receipt_capture_calls = 0;
#endif
	reservation->active = true;
	reservation->owns_ref = true;
	reservation->ref_slot = 7;
	reservation->sequence = receipt->reservation_sequence;
	reservation->receipt = *receipt;
	header = (UndoBlockHeader *)reservation->block.data;
	header->magic = PGRAC_UNDO_BLOCK_MAGIC;
	header->block_version = UNDO_BLOCK_VERSION_1;
	header->free_offset = sizeof(UndoBlockHeader);
}

/* PGRAC: the baseline build selects the original caller's exact cleanup. */
#ifndef UNDO_RECORD_BASELINE_ABORT
extern void cluster_undo_record_abort_prepared_after_lwlock_release(void);
#endif

static void
receipt_abort_after_lwlock_release(void)
{
	/* Model PG's completed LWLockReleaseAll, preserving the owner's stale bit. */
	receipt_buffer_locked = false;
#ifdef UNDO_RECORD_BASELINE_ABORT
	cluster_undo_record_cancel_prepared(&cluster_undo_record_reservation.receipt);
#else
	cluster_undo_record_abort_prepared_after_lwlock_release();
#endif
}

static void
receipt_abort_fixture(ClusterUndoRecordPrepareReceipt *receipt)
{
	UndoItlHistoryEntry history = { 0 };

	receipt_fixture_ready(UNDO_RECORD_UPDATE, receipt);
	receipt->ctrc_pending_mask = 1;
	receipt->ctrc_pending_targets[0].block_number = 37;
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(receipt, 0));
	history.block = 37;
	history.after_kind = ITL_FLAG_ACTIVE;
	history.after_write_scn = 999;
	UT_ASSERT(cluster_undo_record_stage_history(receipt, 0, &history));
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(receipt, 32),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
}

UT_TEST(test_abort_prepared_after_lock_release_is_idempotent)
{
	int context;

	/* Top and subabort share the same exact-owner hook; neither may reset
	 * the parent's unrelated local heads or pending undo. */
	for (context = 0; context < 2; context++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };

		receipt_abort_fixture(&receipt);
		target.block_number = 38; /* Local validation refuses before APPLY. */
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
					 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
		UT_ASSERT_EQ(receipt_apply_calls, 0);
		receipt_abort_after_lwlock_release();
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT(!cluster_undo_record_reservation.consume_locked);
		UT_ASSERT_EQ(receipt_cancel_calls, 1);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT_EQ(cluster_undo_local_head_count, 1);
		receipt_abort_after_lwlock_release();
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
	}
}

UT_TEST(test_abort_prepared_cancel_failure_and_borrowed_owner_do_not_panic)
{
	int variant;

	for (variant = 0; variant < 3; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;

		receipt_abort_fixture(&receipt);
		if (variant == 0) {
			/* Borrowed handle is staged only; no retarget was called. */
			receipt.ctrc_reuse_mask = 1;
			UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		} else
			/* CAS failure, including a participant no longer OPEN. */
			receipt_cancel_fail_at = 1;
		receipt_abort_after_lwlock_release();
		UT_ASSERT_EQ(receipt_apply_calls, 0);
		UT_ASSERT_EQ(receipt_cancel_calls, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT(!cluster_undo_record_reservation.active);
	}
}

UT_TEST(test_abort_attempted_or_applied_retains_debt_and_panics)
{
	int variant;

	for (variant = 0; variant < 7; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };

		receipt_abort_fixture(&receipt);
		target.block_number = 37;
		if (variant == 0) {
			receipt.ctrc_applied_mask = 1;
			UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		} else {
			if (variant == 1)
				receipt_apply_result = CLUSTER_CTRC_APPLY_FAIL_CLOSED;
			else if (variant == 2)
				receipt_fail_sync_after_apply = true;
			else if (variant == 3) {
				receipt.ctrc_reuse_mask = 1;
				UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
			} else if (variant == 4)
				receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
			(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
			UT_ASSERT_EQ(receipt_apply_calls, 1);
			if (variant == 2)
				UT_ASSERT_EQ(cluster_undo_record_reservation.receipt.ctrc_applied_mask, 0);
			if (variant == 4) {
				uint64 metrics[CLUSTER_UNDO_RECEIPT_METRIC_COUNT];

				(void)cluster_undo_record_requalify_for_retry(&receipt, 32, false);
				UT_ASSERT(cluster_undo_record_receipt_stats_snapshot(metrics));
				UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_POST_APPLY_REPREPARE], 1);
			}
			if (variant == 5) {
				/* The following MX publisher is an external failure boundary:
				 * its ERROR cannot discharge the successful undo APPLY. */
				UT_ASSERT_EQ(receipt.ctrc_applied_mask, 1);
			} else if (variant == 6) {
				ClusterUndoRecordTarget data_target = { 0 };
				char payload[32] = { 0 };
				UBA uba;

				/* A real consume refuses a damaged footer before DATA install. */
				receipt.itl_history_reserved[0] = 1;
				UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
				UT_ASSERT_EQ(cluster_undo_record_consume_prepared(&receipt, &data_target, payload,
																  sizeof(payload), &uba),
							 CLUSTER_UNDO_RECORD_CONSUME_REFUSED);
				UT_ASSERT_EQ(receipt_install_calls, 0);
			}
		}
		receipt_trap_error = true;
		if (sigsetjmp(receipt_error_jump, 1) == 0) {
			receipt_abort_after_lwlock_release();
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(receipt_error_level, PANIC);
			UT_ASSERT_EQ(receipt_error_code, ERRCODE_INTERNAL_ERROR);
			UT_ASSERT(strstr(receipt_error_detail, "UNDO_ABORT_EFFECT_UNCERTAIN") != NULL);
		}
		receipt_trap_error = false;
		UT_ASSERT(cluster_undo_record_reservation.active);
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
	}
}

UT_TEST(test_ready_receipt_survives_prepare_deadline_through_real_consume)
{
	/* INSERT and multi-insert share the INSERT receipt; UPDATE after the
	 * existing TEMP_LOCK/TOAST handoff uses the same UPDATE receipt owner. */
	const uint8 types[] = { UNDO_RECORD_INSERT, UNDO_RECORD_DELETE, UNDO_RECORD_UPDATE,
							UNDO_RECORD_INSERT, UNDO_RECORD_ITL,	UNDO_RECORD_UPDATE };
	size_t i;

	for (i = 0; i < lengthof(types); i++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterUndoRecordPrepareReceipt zero = { 0 };
		ClusterUndoRecordTarget target = { 0 };
		ClusterCtrcTargetV1 final_target = { 0 };
		ClusterCtrcApplyToken token;
		char payload[64] = { 0 };
		uint16 payload_length;
		UndoItlHistoryEntry history = { 0 };
		UBA uba;
		ClusterUndoRecordConsumePreflightResult preflight;

		receipt_fixture_ready(types[i], &receipt);
		receipt.ctrc_pending_mask = 1;
		receipt.ctrc_pending_targets[0].block_number = 37;
		target.locator.spcOid = 1663;
		target.locator.dbOid = 5;
		target.locator.relNumber = 20000;
		target.forknum = MAIN_FORKNUM;
		target.blockno = 37;
		target.offnum = 1;
		final_target.block_number = 37;
		history.block = 37;
		history.after_write_scn = 999;
		history.after_kind
			= types[i] == UNDO_RECORD_ITL ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
		if (types[i] == UNDO_RECORD_INSERT)
			payload_length = sizeof(UndoInsertPayload);
		else if (types[i] == UNDO_RECORD_ITL) {
			UndoItlPayload itl = { 0 };

			itl.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
			itl.lock_xid = 700;
			memcpy(payload, &itl, sizeof(itl));
			payload_length = sizeof(itl);
		} else if (types[i] == UNDO_RECORD_UPDATE) {
			UndoUpdatePayload update = { 37, 2, 32, sizeof(UndoUpdatePayload), 0 };

			memcpy(payload, &update, sizeof(update));
			payload_length = sizeof(update) + 32;
		} else {
			UndoDeletePayload deleted = { 32, sizeof(UndoDeletePayload), 0 };

			memcpy(payload, &deleted, sizeof(deleted));
			payload_length = sizeof(deleted) + 32;
		}
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		receipt_clock_us = 101;
		UT_ASSERT(cluster_undo_record_prepared_recheck(&receipt, sizeof(payload)));
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		UT_ASSERT_EQ(receipt_prepare_calls, 1);
		UT_ASSERT(cluster_undo_record_stage_history(&receipt, 0, &history));
		preflight = cluster_undo_record_consume_preflight(&receipt, payload_length);
		UT_ASSERT_EQ(preflight, CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
		if (preflight != CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY) {
			cluster_undo_record_cancel_prepared(&receipt);
			continue;
		}
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &final_target, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		UT_ASSERT_EQ(
			cluster_undo_record_consume_prepared(&receipt, &target, payload, payload_length, &uba),
			CLUSTER_UNDO_RECORD_CONSUME_APPLIED);
		UT_ASSERT(!UBA_is_invalid(uba));
		UT_ASSERT_EQ(receipt_install_calls, 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT_EQ(receipt_unref_calls, 0); /* Reference transferred to pending WAL. */
		{
			const UndoSlotDirEntry *directory = UNDO_SLOT_DIR_PTR(cluster_undo_pending.buf, 0);
			const UndoRecordHeader *stored
				= (const UndoRecordHeader *)(cluster_undo_pending.buf + directory->record_offset);
			UndoItlHistoryTrailer stored_history;
			uint16 stored_body_length;

			UT_ASSERT_EQ(directory->record_length,
						 sizeof(*stored) + payload_length + sizeof(stored_history));
			UT_ASSERT_EQ(directory->flags, stored->flags);
			UT_ASSERT(cluster_undo_record_decode_payload(
				stored, (const char *)stored + sizeof(*stored), stored->payload_length,
				&stored_body_length, &stored_history));
			UT_ASSERT_EQ(stored_body_length, payload_length);
			UT_ASSERT_EQ(memcmp(&stored_history.entries[0], &history, sizeof(history)), 0);
		}
		UT_ASSERT_EQ(memcmp(&receipt, &zero, sizeof(receipt)), 0);
		UT_ASSERT_EQ(memcmp(&cluster_undo_record_reservation.receipt, &zero, sizeof(zero)), 0);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT(!receipt_buffer_locked);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, 0); /* Shared APPLIED ownership survives. */
	}
}

static void
receipt_native_insert_fixture(ClusterUndoRecordPrepareReceipt *receipt,
							  ClusterUndoRecordTarget *target)
{
	UndoItlHistoryEntry history = { 0 };
	ClusterCtrcTargetV1 final_target = { 0 };
	ClusterCtrcApplyToken token;

	receipt_fixture_ready(UNDO_RECORD_INSERT, receipt);
	cluster_shared_config = true;
	receipt->ctrc_pending_mask = 1;
	receipt->ctrc_pending_targets[0].block_number = 37;
	memset(target, 0, sizeof(*target));
	target->locator.spcOid = 1663;
	target->locator.dbOid = 5;
	target->locator.relNumber = 20000;
	target->forknum = MAIN_FORKNUM;
	target->blockno = 37;
	target->offnum = 1;
	history.block = final_target.block_number = 37;
	history.after_kind = ITL_FLAG_ACTIVE;
	history.after_write_scn = 999;
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(receipt, 0));
	UT_ASSERT(cluster_undo_record_stage_history(receipt, 0, &history));
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(receipt, sizeof(UndoInsertPayload)),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(receipt, 0, &final_target, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
}

/* Catches a returned native UBA whose undo exists only in the old clean,
 * process-local pending window.  The real consumer must publish WAL first. */
UT_TEST(test_native_prepared_undo_is_wal_backed_before_uba_publication)
{
	PGPROC proc = { 0 };
	PGPROC *saved_proc = MyProc;
	const int saved_flags[] = { DELAY_CHKPT_COMPLETE, DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE };

	MyProc = &proc;
	for (size_t i = 0; i < lengthof(saved_flags); i++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterUndoRecordPrepareReceipt zero = { 0 };
		ClusterUndoRecordTarget target;
		UndoInsertPayload payload = { 0 };
		UBA uba;

		receipt_native_insert_fixture(&receipt, &target);
		proc.delayChkptFlags = saved_flags[i];
		UT_ASSERT_EQ(cluster_undo_record_consume_prepared(&receipt, &target, &payload,
														 sizeof(payload), &uba),
					 CLUSTER_UNDO_RECORD_CONSUME_APPLIED);
		UT_ASSERT(!UBA_is_invalid(uba));
		UT_ASSERT_EQ(receipt_wal_calls, 1);
		UT_ASSERT_EQ(receipt_wal_install_calls, 1);
		UT_ASSERT_EQ(receipt_install_calls, 1);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT_EQ(proc.delayChkptFlags, saved_flags[i]);
		UT_ASSERT(!cluster_undo_pending.active);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT(!receipt_buffer_locked);
		UT_ASSERT_EQ(memcmp(&receipt, &zero, sizeof(receipt)), 0);
	}
	cluster_shared_config = false;
	MyProc = saved_proc;
}

/* Catches WAL insertion failure being converted to success or discharging
 * the original APPLY/ref owner; the checkpoint exclusion must still end. */
UT_TEST(test_native_prepared_undo_wal_error_retains_original_debt)
{
	PGPROC proc = { 0 };
	PGPROC *saved_proc = MyProc;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordTarget target;
	UndoInsertPayload payload = { 0 };
	UBA uba = InvalidUba;
	volatile bool caught = false;

	receipt_native_insert_fixture(&receipt, &target);
	MyProc = &proc;
	proc.delayChkptFlags = DELAY_CHKPT_COMPLETE;
	receipt_wal_throw = true;
	PG_TRY();
	{
		(void)cluster_undo_record_consume_prepared(&receipt, &target, &payload, sizeof(payload),
												  &uba);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(UBA_is_invalid(uba));
	UT_ASSERT_EQ(receipt_wal_calls, 1);
	UT_ASSERT_EQ(receipt_install_calls, 0);
	UT_ASSERT_EQ(receipt_unref_calls + receipt_leave_calls + receipt_cancel_calls, 0);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT(cluster_undo_record_reservation.consume_locked);
	UT_ASSERT(cluster_undo_record_reservation.owns_ref);
	UT_ASSERT_EQ(cluster_undo_record_reservation.receipt.ctrc_applied_mask, 1);
	UT_ASSERT(!cluster_undo_pending.active);
	UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE);
	cluster_shared_config = false;
	MyProc = saved_proc;
}

UT_TEST(test_ready_preflight_crosses_old_deadline_while_taking_lock)
{
	ClusterUndoRecordPrepareReceipt receipt;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt_clock_after_lock = 101;
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 64),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	UT_ASSERT_EQ(receipt_clock_us, 101);
	UT_ASSERT(receipt_buffer_locked);
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
}

UT_TEST(test_ready_contention_retries_without_cancel_or_new_deadline)
{
	ClusterUndoRecordPrepareReceipt receipt, before;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	before = receipt;
	receipt_buffer_available = false;
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 64),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED);
	UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(receipt)), 0);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_leave_calls + receipt_unref_calls, 0);
	receipt_buffer_available = true;
	receipt_clock_us = 101;
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 64),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	UT_ASSERT_EQ(receipt.absolute_deadline_us, before.absolute_deadline_us);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_ready_identity_mismatch_is_not_hidden_by_lifetime_fix)
{
	ClusterUndoRecordPrepareReceipt receipt, changed;

	receipt_fixture_ready(UNDO_RECORD_DELETE, &receipt);
	changed = receipt;
	changed.reservation_sequence++;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&changed, 64));
	changed = receipt;
	changed.owner_instance++;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&changed, 64));
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 129));
	cluster_undo_current_extent.cur_block++;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 64));
	cluster_undo_current_extent.cur_block--;
	receipt_modifier_valid = false;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 64));
	receipt_modifier_valid = true;
	receipt_block0_valid = false;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 64));
	receipt_block0_valid = true;
	UT_ASSERT(cluster_undo_record_prepared_recheck(&receipt, 64));
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 64));
	UT_ASSERT_EQ(receipt_leave_calls, 1);
}

UT_TEST(test_partial_apply_cancel_releases_only_unapplied_nonreuse_handle)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 target = { 0 };
	ClusterCtrcApplyToken token;
	int reuse;

	for (reuse = 0; reuse <= 1; reuse++) {
		UndoItlHistoryEntry history = { 0 };

		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		receipt.ctrc_pending_mask = receipt.ctrc_prepared_mask = 3;
		receipt.ctrc_reuse_mask = reuse ? 2 : 0;
		receipt.ctrc_handles[0].valid = receipt.ctrc_handles[1].valid = true;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		history.after_kind = ITL_FLAG_ACTIVE;
		history.after_write_scn = 999;
		UT_ASSERT(cluster_undo_record_stage_history(&receipt, 0, &history));
		UT_ASSERT(cluster_undo_record_stage_history(&receipt, 1, &history));
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, reuse ? 0 : 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, reuse ? 0 : 1);
	}
}

UT_TEST(test_prepare_budget_remains_ten_seconds_without_refresh)
{
	receipt_clock_us = 100;
	UT_ASSERT_EQ(cluster_undo_record_prepare_deadline_us(), UINT64_C(10000100));
	receipt_clock_us = 0;
	UT_ASSERT_EQ(cluster_undo_record_prepare_deadline_us(), 0);
	receipt_clock_us = PG_INT64_MAX - 1;
	UT_ASSERT_EQ(cluster_undo_record_prepare_deadline_us(), 0);
}

static void
receipt_update_targets_fixture(ClusterUndoRecordPrepareReceipt *receipt,
							   ClusterCtrcTargetV1 targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS])
{
	receipt_fixture_ready(UNDO_RECORD_UPDATE, receipt);
	memset(targets, 0, sizeof(*targets) * CLUSTER_UNDO_RECORD_CTRC_TARGETS);
	for (int i = 0; i < CLUSTER_UNDO_RECORD_CTRC_TARGETS; i++) {
		targets[i].kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
		targets[i].spc_oid = 1663;
		targets[i].db_oid = 5;
		targets[i].rel_number = 16386;
		targets[i].fork_number = MAIN_FORKNUM;
		targets[i].block_number = 16949 + i;
		targets[i].predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
		targets[i].publication_own_generation = 1;
		targets[i].publication_acquisition_epoch = 7;
		targets[i].relation_persistence = 'p';
		targets[i].needs_wal = true;
		targets[i].page_operation_kind = UNDO_RECORD_UPDATE;
		receipt->ctrc_pending_targets[i] = targets[i];
		receipt->ctrc_handles[i].valid = true;
	}
	receipt->ctrc_pending_mask = receipt->ctrc_prepared_mask = 3;
	receipt->itl_history_mask = 3;
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	receipt_clock_us = 101;
}

UT_TEST(test_update_target_retry_keeps_ready_extent_deadline_and_owns_children)
{
	ClusterUndoRecordPrepareReceipt receipt, before;
	ClusterCtrcTargetV1 targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS];

	receipt_update_targets_fixture(&receipt, targets);
	before = receipt;
	for (int pass = 0; pass < 3; pass++) {
		uint8 mask = pass == 1 ? 1 : 3;
		int cancels_before = receipt_cancel_calls;
		int expected_cancels = pass == 2 ? 1 : 2;

		targets[1].block_number += 5;
		UT_ASSERT_EQ(cluster_undo_record_reset_update_targets(&receipt, targets, mask),
					 CLUSTER_UNDO_TARGET_RESET_READY);
		UT_ASSERT(cluster_undo_record_reservation.active);
		UT_ASSERT(cluster_undo_record_reservation.owns_ref);
		UT_ASSERT_EQ(receipt.magic, before.magic);
		UT_ASSERT_EQ(receipt.reservation_sequence, before.reservation_sequence);
		UT_ASSERT_EQ(receipt.absolute_deadline_us, before.absolute_deadline_us);
		UT_ASSERT_EQ(memcmp(&receipt.extent, &before.extent, sizeof(receipt.extent)), 0);
		UT_ASSERT_EQ(memcmp(&receipt.modifier_admission, &before.modifier_admission,
							sizeof(receipt.modifier_admission)),
					 0);
		UT_ASSERT_EQ(receipt.ctrc_attempt_generation, 2 + pass);
		UT_ASSERT_EQ(receipt.ctrc_pending_mask, mask);
		UT_ASSERT_EQ(receipt.ctrc_prepared_mask | receipt.itl_history_mask, 0);
		UT_ASSERT_EQ(receipt_cancel_calls - cancels_before, expected_cancels);
		UT_ASSERT_EQ(receipt_leave_calls + receipt_unref_calls + receipt_apply_calls, 0);
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		if (mask == 3)
			UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 1));
		UT_ASSERT_EQ(receipt_child_attempt, 2 + pass);
	}
}

UT_TEST(test_update_target_retry_refuses_authority_and_publication_drift)
{
	for (int leg = 0; leg < 14; leg++) {
		ClusterUndoRecordPrepareReceipt receipt, before;
		ClusterCtrcTargetV1 targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS];
		uint8 mask = 3;

		receipt_update_targets_fixture(&receipt, targets);
		targets[1].block_number++;
		switch (leg) {
		case 0:
			receipt.record_type = UNDO_RECORD_DELETE;
			break;
		case 1:
			receipt.ctrc_applied_mask = 1;
			break;
		case 2:
			cluster_undo_record_reservation.consume_locked = true;
			break;
		case 3:
			cluster_undo_current_extent.cur_block++;
			break;
		case 4:
			receipt_modifier_valid = false;
			break;
		case 5:
			receipt_block0_valid = false;
			break;
		case 6:
			targets[0].block_number++;
			break;
		case 7:
			targets[1].rel_number++;
			break;
		case 8:
			targets[1].publication_acquisition_epoch++;
			break;
		case 9:
			targets[1].page_operation_kind = UNDO_RECORD_INSERT;
			break;
		case 10:
			receipt.ctrc_attempt_generation = UINT32_MAX;
			break;
		case 11:
			mask = 2;
			break;
		case 12:
			targets[1].publication_own_generation = 0;
			break;
		case 13:
			targets[1].block_number--;
			break;
		}
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		before = receipt;
		UT_ASSERT(!cluster_undo_record_reset_update_targets(&receipt, targets, mask));
		UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(receipt)), 0);
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_leave_calls + receipt_unref_calls, 0);
	}
}

UT_TEST(test_update_target_retry_retains_borrowed_and_failed_cancel_owners)
{
	for (int leg = 0; leg < 3; leg++) {
		ClusterUndoRecordPrepareReceipt receipt, before;
		ClusterCtrcTargetV1 targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS];

		receipt_update_targets_fixture(&receipt, targets);
		targets[1].block_number++;
		if (leg == 0)
			receipt.ctrc_reuse_mask = 1;
		else
			receipt_cancel_fail_at = leg;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		before = receipt;
		UT_ASSERT_EQ(cluster_undo_record_reset_update_targets(&receipt, targets, 3),
					 leg == 0 ? CLUSTER_UNDO_TARGET_RESET_READY
							  : CLUSTER_UNDO_TARGET_RESET_REFUSED);
		UT_ASSERT_EQ(receipt_cancel_calls, leg == 2 ? 2 : 1);
		UT_ASSERT_EQ(receipt_leave_calls + receipt_unref_calls + receipt_apply_calls, 0);
		if (leg != 0)
			UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(receipt)), 0);
	}
}

UT_TEST(test_heap_retry_preserves_exact_ready_before_considering_cancel)
{
	char *source = read_heapam_source();
	const char *restart;
	const char *end;
	const char *requalify;
	const char *deadline_reclass;

	if (source == NULL)
		return;
	restart = strstr(source, "\ncluster_heap_restart_update_undo_record_exact(");
	end = restart == NULL ? NULL : strstr(restart, "\n}\n");
	requalify = restart == NULL ? NULL : strstr(restart, "cluster_heap_retry_undo_record_exact(");
	UT_ASSERT(restart != NULL && end != NULL && requalify != NULL && requalify < end);
	deadline_reclass
		= strstr(source, "return receipt->absolute_deadline_us > (uint64)GetCurrentTimestamp()");
	UT_ASSERT(deadline_reclass == NULL);
	/* UPDATE's one direct cancel is the explicit nested TOAST handoff.
	 * The other direct site is heap_lock_tuple's unpublished return cleanup;
	 * neither allows UPDATE retries to bypass READY requalification. */
	{
		const char *cancel = strstr(source, "cluster_undo_record_cancel_prepared(&undo_receipt)");
		const char *update = strstr(source, "\nheap_update(");
		const char *lock = strstr(source, "\nheap_lock_tuple_internal(");
		const char *next
			= cancel == NULL
				  ? NULL
				  : strstr(cancel + 1, "cluster_undo_record_cancel_prepared(&undo_receipt)");
		const char *unpublished
			= lock == NULL
				  ? NULL
				  : strstr(lock,
						   "if (undo_receipt.magic != 0 && undo_receipt.ctrc_applied_mask == 0)");
		UT_ASSERT_NOT_NULL(cancel);
		UT_ASSERT_NOT_NULL(update);
		UT_ASSERT_NOT_NULL(lock);
		UT_ASSERT_NOT_NULL(next);
		UT_ASSERT_NOT_NULL(unpublished);
		if (cancel != NULL && update != NULL && lock != NULL && next != NULL
			&& unpublished != NULL) {
			UT_ASSERT(update < cancel && cancel < lock && lock < unpublished && unpublished < next);
			UT_ASSERT(strstr(next + 1, "cluster_undo_record_cancel_prepared(&undo_receipt)")
					  == NULL);
		}
	}
	free(source);
}

UT_TEST(test_retry_requalification_keeps_ready_and_proves_actual_invalidation)
{
	ClusterUndoRecordPrepareReceipt receipt, before;
	uint64 metrics[CLUSTER_UNDO_RECEIPT_METRIC_COUNT];

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	before = receipt;
	receipt_clock_us = 101;
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(before)), 0);
	UT_ASSERT(cluster_undo_record_receipt_stats_snapshot(metrics));
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_RETRY_PRESERVED], 1);
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_REPREPARE_PREMATURE], 0);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_leave_calls + receipt_unref_calls, 0);
	/* Dropping heap authority permits a blocking resident sample.  A prior
	 * conditional block0 miss alone cannot invalidate the receipt. */
	receipt_block0_conditional_available = false;
	UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 64));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(before)), 0);
	receipt_block0_conditional_available = true;
	/* A proven target change cannot refresh the expired original budget. */
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, true),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT_EQ(strcmp(cluster_undo_record_receipt_last_reason(),
						"PREPARE_BUDGET_EXHAUSTED_AFTER_EXACT_INVALIDATION"),
				 0);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT(cluster_undo_record_receipt_stats_snapshot(metrics));
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_INVALIDATED_BUDGET_EXHAUSTED], 1);
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_REPREPARE_PREMATURE], 0);
	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	cluster_undo_current_extent.cur_block++;
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
}

UT_TEST(test_retry_refusal_records_exact_cause_including_expiry_during_cancel)
{
	static const char *causes[]
		= { "TARGET_INVALIDATED",		  "CAPACITY_REFUSAL",			"RESERVATION_MISMATCH",
			"MODIFIER_ADMISSION_CHANGED", "BLOCK0_PUBLICATION_CHANGED", "TARGET_INVALIDATED" };
	size_t variant;

	for (variant = 0; variant < lengthof(causes); variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		bool targets_invalidated = variant == 0 || variant == 5;

		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		receipt_log_enabled = true;
		receipt_clock_us = variant == 5 ? 99 : 101;
		if (variant == 2)
			cluster_undo_current_extent.cur_block++;
		if (variant == 3)
			receipt_modifier_valid = false;
		if (variant == 4)
			receipt_block0_valid = false;
		if (variant == 5)
			receipt_clock_after_unref = 101;
		UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, variant == 1 ? 129 : 64,
															 targets_invalidated),
					 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
		UT_ASSERT(strstr(receipt_last_log, "undo retry refusal evidence: reservation=17") != NULL);
		UT_ASSERT(strstr(receipt_last_log, causes[variant]) != NULL);
		UT_ASSERT(strstr(receipt_last_log, "deadline=100 now=101 applied=0 reuse=0") != NULL);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		UT_ASSERT_EQ(receipt_apply_calls, 0);
	}
	receipt_log_enabled = false;
}

UT_TEST(test_retry_after_apply_refuses_without_canceling_shared_owner)
{
	ClusterUndoRecordPrepareReceipt receipt, before;
	uint64 metrics[CLUSTER_UNDO_RECEIPT_METRIC_COUNT];

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_applied_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	before = receipt;
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_leave_calls + receipt_unref_calls, 0);
	UT_ASSERT(cluster_undo_record_receipt_stats_snapshot(metrics));
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_POST_APPLY_REPREPARE], 1);
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 64),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_REFUSED);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_cancel_before_snapshot_violation_counter_is_live)
{
	ClusterUndoRecordPrepareReceipt receipt;
	uint64 metrics[CLUSTER_UNDO_RECEIPT_METRIC_COUNT];

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	/* Deliberate negative control for the actual cancel-side diagnostic. */
	cluster_undo_retry_cancel_snapshot.armed = true;
	cluster_undo_retry_cancel_snapshot.exact_ready = true;
	cluster_undo_retry_cancel_snapshot.targets_invalidated = false;
	cluster_undo_retry_cancel_snapshot.sequence = receipt.reservation_sequence;
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(cluster_undo_record_receipt_stats_snapshot(metrics));
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_REPREPARE_PREMATURE], 1);
	UT_ASSERT_EQ(metrics[CLUSTER_UNDO_RECEIPT_CANCEL], 1);
}

UT_TEST(test_update_toast_releases_outer_receipt_before_nested_producers)
{
	char *source = read_heapam_source();
	const char *update, *toast, *release, *resume, *relock;

	if (source == NULL)
		return;
	update = strstr(source, "\nheap_update(");
	toast = update == NULL ? NULL : strstr(update, "heaptup = heap_toast_insert_or_update(");
	release = update == NULL ? NULL
							 : strstr(update, "cluster_undo_record_cancel_prepared(&undo_receipt)");
	resume = toast == NULL ? NULL : strstr(toast, "cluster_heap_resume_update_undo_record_exact(");
	relock = toast == NULL ? NULL : strstr(toast, "l_pgrac_reacquire:");
	UT_ASSERT(update != NULL && release != NULL && toast != NULL && release < toast);
	UT_ASSERT(resume != NULL && relock != NULL && toast < resume && resume < relock);
	if (resume != NULL && relock != NULL) {
		const char *phase_budget = strstr(resume, "&undo_prepare_deadline_us");
		UT_ASSERT(phase_budget != NULL && phase_budget < relock);
	}
	free(source);
}

UT_TEST(test_update_itl_wait_returns_through_receipt_and_page_requalification)
{
	char *source = read_heapam_source();
	char *compact = source != NULL ? malloc(strlen(source) + 1) : NULL;
	char *write = compact;
	const char *read;
	char *update = source != NULL ? strstr(source, "\nheap_update(Relation ") : NULL;
	char *update_end = update != NULL ? strstr(update, "\n}\n") : NULL;
	char *deadline = update ? strstr(update, "itl_capacity_absolute_deadline_us = 0;") : NULL;
	char *wait = update ? strstr(update, "\nl_pgrac_itl_capacity_wait:") : NULL;
	char *old_full = update ? strstr(update, "itl_capacity_wait_buffer = buffer;") : NULL;
	char *new_full = update ? strstr(update, "itl_capacity_wait_buffer = newbuf;") : NULL;
	char *call = wait ? strstr(wait, "cluster_heap_itl_wait_capacity_after_census(") : NULL;
	char *receipt = call ? strstr(call, "cluster_heap_restart_update_undo_record_exact(") : NULL;
	char *reacquire = receipt ? strstr(receipt, "goto l_pgrac_reacquire;") : NULL;
	char *same_page = receipt ? strstr(receipt, "goto l2;") : NULL;

	/* This is an adjacency contract, not a clang-format indentation contract. */
	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_NOT_NULL(compact);
	UT_ASSERT_NOT_NULL(update_end);
	if (compact == NULL || update_end == NULL) {
		free(compact);
		free(source);
		return;
	}
	for (read = source; *read; read++)
		if (!isspace((unsigned char)*read))
			*write++ = *read;
	*write = '\0';
	UT_ASSERT_NOT_NULL(strstr(compact, "if(cluster_writer_res==TM_BeingModified)gotol1;"));
	UT_ASSERT_NOT_NULL(strstr(compact, "if(cluster_writer_res==TM_BeingModified)gotol2;"));
	UT_ASSERT_NOT_NULL(strstr(compact, "if(cwres==TM_BeingModified)gotol3;"));

	UT_ASSERT_NOT_NULL(deadline);
	UT_ASSERT_NOT_NULL(old_full);
	UT_ASSERT_NOT_NULL(new_full);
	UT_ASSERT_NOT_NULL(call);
	UT_ASSERT_NOT_NULL(receipt);
	UT_ASSERT_NOT_NULL(reacquire);
	UT_ASSERT_NOT_NULL(same_page);
	if (deadline && wait) {
		const char *next_deadline = strstr(deadline + 1, "itl_capacity_absolute_deadline_us = 0;");
		UT_ASSERT(deadline < update_end && wait < update_end);
		UT_ASSERT(next_deadline == NULL || next_deadline > update_end);
	}
	free(compact);
	free(source);
}

UT_TEST(test_update_vm_observation_never_repins_or_indexes_a_local_buffer)
{
	char *source = read_heapam_source();
	char *update = source != NULL ? strstr(source, "\nheap_update(") : NULL;
	char *old_probe = update != NULL ? strstr(update, "bool vm_visible_before") : NULL;
	char *new_probe = old_probe != NULL ? strstr(old_probe + 1, "bool vm_visible_before") : NULL;
	char *old_local = old_probe != NULL ? strstr(old_probe, "!BufferIsLocal(vmbuffer)") : NULL;
	char *new_local = new_probe != NULL ? strstr(new_probe, "!BufferIsLocal(vmbuffer_new)") : NULL;

	UT_ASSERT_NOT_NULL(update);
	UT_ASSERT_NOT_NULL(old_probe);
	UT_ASSERT_NOT_NULL(new_probe);
	UT_ASSERT(old_local != NULL && old_local < new_probe);
	UT_ASSERT_NOT_NULL(new_local);
	UT_ASSERT(old_probe != NULL
			  && strstr(old_probe, "visibilitymap_pin_ok(BufferGetBlockNumber(buffer), vmbuffer)")
					 != NULL);
	UT_ASSERT(
		new_probe != NULL
		&& strstr(new_probe, "visibilitymap_pin_ok(BufferGetBlockNumber(newbuf), vmbuffer_new)")
			   != NULL);
	free(source);
}

static void
make_history_codec_record(UndoRecordHeader *record, char *body, UndoItlHistoryTrailer *history)
{
	UndoInsertPayload insert = { 32, 0 };

	memset(record, 0, sizeof(*record));
	record->record_type = UNDO_RECORD_INSERT;
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY;
	record->payload_length = sizeof(insert) + sizeof(*history);
	record->xid = 100;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = 1;
	record->tt_slot_id = 1;
	record->tt_wrap_plus1 = 1;
	record->write_scn = 201;
	record->target_locator.spcOid = 1663;
	record->target_locator.dbOid = 5;
	record->target_locator.relNumber = 20000;
	record->target_fork = MAIN_FORKNUM;
	record->target_block = 37;
	record->target_offset = 1;
	memset(history, 0, sizeof(*history));
	history->magic = UNDO_ITL_HISTORY_MAGIC;
	history->version = UNDO_ITL_HISTORY_VERSION;
	history->count = 1;
	history->entries[0].block = 37;
	history->entries[0].after_kind = ITL_FLAG_ACTIVE;
	history->entries[0].after_write_scn = 200;
	history->entries[0].prior.wrap = 7;
	memcpy(body, &insert, sizeof(insert));
	memcpy(body + sizeof(insert), history, sizeof(*history));
}

UT_TEST(test_history_stage_syncs_both_receipts_and_freezes_after_apply)
{
	ClusterUndoRecordPrepareReceipt receipt;
	UndoItlHistoryEntry entry = { 0 };
	ClusterUndoRecordPrepareReceipt before;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_pending_mask = 1;
	receipt.ctrc_pending_targets[0].block_number = 37;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	entry.block = 37;
	entry.after_kind = ITL_FLAG_ACTIVE;
	entry.after_write_scn = 1000;
	entry.prior.wrap = 7;
	UT_ASSERT(cluster_undo_record_stage_history(&receipt, 0, &entry));
	UT_ASSERT_EQ(receipt.itl_history_mask, 1);
	UT_ASSERT_EQ(memcmp(&receipt, &cluster_undo_record_reservation.receipt, sizeof(receipt)), 0);
	UT_ASSERT(cluster_undo_record_history_matches(&receipt, 0, &entry));
	entry.after_write_scn++;
	UT_ASSERT(cluster_undo_record_stage_history(&receipt, 0, &entry));
	UT_ASSERT(cluster_undo_record_history_matches(&receipt, 0, &entry));
	receipt.ctrc_applied_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	before = receipt;
	entry.after_write_scn++;
	UT_ASSERT(!cluster_undo_record_stage_history(&receipt, 0, &entry));
	UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(receipt)), 0);
	UT_ASSERT(!cluster_undo_record_history_matches(&receipt, 0, &entry));
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt.itl_history_mask, 0);
}

UT_TEST(test_history_stage_rejects_unowned_target_and_tampered_receipt)
{
	ClusterUndoRecordPrepareReceipt receipt;
	UndoItlHistoryEntry entry = { 0 };

	receipt_fixture_ready(UNDO_RECORD_INSERT, &receipt);
	entry.block = 37;
	entry.after_kind = ITL_FLAG_ACTIVE;
	entry.after_write_scn = 1000;
	UT_ASSERT(!cluster_undo_record_stage_history(&receipt, 0, &entry));
	receipt.ctrc_pending_mask = 1;
	receipt.ctrc_pending_targets[0].block_number = 37;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	entry.block++;
	UT_ASSERT(!cluster_undo_record_stage_history(&receipt, 0, &entry));
	entry.block--;
	entry.prior.xid = 700;
	UT_ASSERT(!cluster_undo_record_stage_history(&receipt, 0, &entry));
	entry.prior.xid = InvalidTransactionId;
	receipt.absolute_deadline_us++;
	UT_ASSERT(!cluster_undo_record_stage_history(&receipt, 0, &entry));
	receipt.absolute_deadline_us--;
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_history_consume_refuses_missing_metadata_before_install)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordTarget target = { 0 };
	UndoInsertPayload payload = { 0 };
	UBA uba;

	receipt_fixture_ready(UNDO_RECORD_INSERT, &receipt);
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, sizeof(payload)),
				 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	UT_ASSERT_EQ(
		cluster_undo_record_consume_prepared(&receipt, &target, &payload, sizeof(payload), &uba),
		CLUSTER_UNDO_RECORD_CONSUME_REFUSED);
	UT_ASSERT_EQ(receipt_install_calls, 0);
	UT_ASSERT(UBA_is_invalid(uba));
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_history_planned_uba_accounts_for_footer_capacity)
{
	ClusterUndoRecordPrepareReceipt receipt;
	UBA uba;

	receipt_fixture_ready(UNDO_RECORD_INSERT, &receipt);
	receipt.extent.cur_slot_count = 0;
	receipt.extent.cur_free_offset
		= BLCKSZ - sizeof(UndoSlotDirEntry) - sizeof(UndoRecordHeader) - sizeof(UndoInsertPayload);
	cluster_undo_current_extent = receipt.extent;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(!cluster_undo_record_prepared_uba_exact(&receipt, sizeof(UndoInsertPayload), &uba));
	UT_ASSERT(UBA_is_invalid(uba));
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_history_codec_unaligned_complete_prior_and_logical_body)
{
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoItlHistoryTrailer decoded;
	char storage[512];
	char *body = storage + 1;
	uint16 length = 0;

	make_history_codec_record(&record, body, &history);
	history.entries[0].prior.xid = 90;
	history.entries[0].prior.flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	history.entries[0].prior.commit_scn = 180;
	history.entries[0].prior.write_scn = 150;
	history.entries[0].prior.undo_segment_head = uba_encode(1, 7, 0, 0);
	history.entries[0].prior.first_change_lsn = 321;
	memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
	UT_ASSERT(cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												 &decoded));
	UT_ASSERT_EQ(length, sizeof(UndoInsertPayload));
	UT_ASSERT_EQ(memcmp(&decoded, &history, sizeof(history)), 0);
}

UT_TEST(test_history_codec_rejects_flag_length_magic_and_reserved_drift)
{
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoItlHistoryTrailer decoded;
	char body[512];
	uint16 length;
	unsigned i;

	for (i = 0; i < 10; i++) {
		make_history_codec_record(&record, body, &history);
		switch (i) {
		case 0:
			record.flags |= UNDO_REC_FLAG_CONTINUED;
			break;
		case 1:
			record.flags |= UNDO_REC_FLAG_TOAST;
			break;
		case 2:
			record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
			break;
		case 3:
			history.magic++;
			break;
		case 4:
			history.version++;
			break;
		case 5:
			history.reserved = 1;
			break;
		case 6:
			history.entries[0].reserved = 1;
			break;
		case 7:
			history.entries[1].block = 1;
			break;
		case 8:
			history.count = 0;
			break;
		case 9:
			history.count = 3;
			break;
		}
		memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
		UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
													  &decoded));
	}
	make_history_codec_record(&record, body, &history);
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length - 1, &length,
												  &decoded));
	record.payload_length = sizeof(history) - 1;
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												  &decoded));
}

UT_TEST(test_history_codec_rejects_forged_slot_identity_role_and_time)
{
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoItlHistoryTrailer decoded;
	char body[512];
	uint16 length;
	unsigned i;

	for (i = 0; i < 11; i++) {
		make_history_codec_record(&record, body, &history);
		switch (i) {
		case 0:
			history.entries[0].block++;
			break;
		case 1:
			history.entries[0].slot_index = 8;
			break;
		case 2:
			history.entries[0].after_kind = ITL_FLAG_COMMITTED;
			break;
		case 3:
			history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
			break;
		case 4:
			history.entries[0].after_write_scn = InvalidScn;
			break;
		case 5:
			history.entries[0].after_write_scn = 202;
			break;
		case 6:
			history.entries[0].prior.xid = 99;
			break;
		case 7:
			history.entries[0].prior.flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			break;
		case 8:
			history.entries[0].prior.flags = ITL_FLAG_ACTIVE;
			break;
		case 9:
			history.entries[0].prior.first_change_lsn = 1;
			break;
		case 10:
			history.entries[0].prior.undo_segment_head = uba_encode(1, 7, 0, 0);
			break;
		}
		memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
		UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
													  &decoded));
	}
}

UT_TEST(test_history_codec_cross_page_update_requires_exactly_two_targets)
{
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoItlHistoryTrailer decoded;
	UndoUpdatePayload update = { 38, 2, 32, sizeof(UndoUpdatePayload), 0 };
	char body[512];
	uint16 length = 0;
	Size body_length = sizeof(update) + update.old_tuple_length;

	make_history_codec_record(&record, body, &history);
	record.record_type = UNDO_RECORD_UPDATE;
	record.payload_length = body_length + sizeof(history);
	memset(body, 0, sizeof(body));
	memcpy(body, &update, sizeof(update));
	memcpy(body + body_length, &history, sizeof(history));
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												  &decoded));
	history.count = 2;
	history.entries[1] = history.entries[0];
	history.entries[1].block = 38;
	memcpy(body + body_length, &history, sizeof(history));
	UT_ASSERT(cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												 &decoded));
	UT_ASSERT_EQ(length, body_length);
	history.entries[1].block = 37;
	memcpy(body + body_length, &history, sizeof(history));
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												  &decoded));
	update.new_block = 37;
	memcpy(body, &update, sizeof(update));
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												  &decoded));
	history.count = 1;
	memset(&history.entries[1], 0, sizeof(history.entries[1]));
	memcpy(body + body_length, &history, sizeof(history));
	UT_ASSERT(cluster_undo_record_decode_payload(&record, body, record.payload_length, &length,
												 &decoded));
}

static PGPROC undo_stop_proc;

static void
undo_stop_fixture_ready(void)
{
	MaxBackends = 17;
	undo_test_reset_record_shmem();
	memset(&cluster_undo_pending, 0, sizeof(cluster_undo_pending));
	memset(&cluster_undo_record_reservation, 0, sizeof(cluster_undo_record_reservation));
	cluster_undo_write_registered = false;
	cluster_node_id = 0;
	MyBackendId = 17;
	MyProc = &undo_stop_proc;
	IsUnderPostmaster = true;
	undo_test_stop_lock_depth = 0;
	undo_test_stop_tracking = true;
}

static void
undo_stop_fixture_done(void)
{
	UT_ASSERT_EQ(undo_test_stop_lock_depth, 0);
	undo_test_stop_tracking = false;
	MyProc = NULL;
	MyBackendId = InvalidBackendId;
	IsUnderPostmaster = false;
	MaxBackends = 1;
}

UT_TEST(test_a148_undo_stop_original_active_writer_requires_original_commit_release)
{
	int backend;
	const char *reason;
	SCN *saved;
	undo_stop_fixture_ready();
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	cluster_undo_active_write_register(scn_encode(0, 71));
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(&backend, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(backend, 17);
	UT_ASSERT_EQ(cluster_undo_write_registry[16], scn_encode(0, 71));
	UT_ASSERT(cluster_undo_write_registered);
	cluster_undo_record_xact_commit_release();
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	saved = cluster_undo_write_registry;
	cluster_undo_write_registry = NULL;
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_INVALID);
	cluster_undo_write_registry = saved;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyProc = NULL;
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_INVALID);
	undo_stop_fixture_done();
}

UT_TEST(test_a148_undo_stop_all_slots_and_late_invalid_do_not_clear_debt)
{
	int i, backend;
	const char *reason;
	undo_stop_fixture_ready();
	for (i = 1; i <= MaxBackends; i++) {
		MyBackendId = i;
		cluster_undo_active_write_register(scn_encode(0, 72));
		UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(&backend, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(backend, i);
		cluster_undo_active_write_unregister();
		UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL),
					 CLUSTER_NORMAL_STOP_READY);
	}
	MyBackendId = 1;
	cluster_undo_active_write_register(scn_encode(0, 73));
	cluster_undo_write_registry[16] = scn_encode(1, 74); /* corrupted own registry fixture */
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(&backend, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(backend, 17);
	UT_ASSERT(strcmp(reason, "UNDO_ACTIVE_WRITE_SCN_INVALID") == 0);
	UT_ASSERT_EQ(cluster_undo_write_registry[0], scn_encode(0, 73));
	UT_ASSERT_EQ(cluster_undo_write_registry[16], scn_encode(1, 74));
	cluster_undo_write_registry[16] = InvalidScn; /* restore fault input, not product cleanup */
	cluster_undo_active_write_unregister();
	undo_stop_fixture_done();
}

UT_TEST(test_a148_undo_stop_does_not_confuse_extent_and_cursor_cache_with_writer)
{
	ClusterUndoExtent saved;
	undo_stop_fixture_ready();
	UndoRecordShared->active_segment_id = 33;
	UndoRecordShared->current_block = 9;
	UndoRecordShared->next_extent_block = 41;
	cluster_undo_current_extent.segment_id = 33;
	cluster_undo_current_extent.first_block = 9;
	cluster_undo_current_extent.nblocks = 4;
	saved = cluster_undo_current_extent;
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(memcmp(&saved, &cluster_undo_current_extent, sizeof(saved)), 0);
	UT_ASSERT_EQ(UndoRecordShared->next_extent_block, 41);
	MaxBackends = 0;
	UT_ASSERT_EQ(cluster_undo_active_write_normal_stop_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_INVALID);
	undo_stop_fixture_done();
}

UT_TEST(test_ctrc_prepare_diagnostic_is_off_by_default_and_observation_only)
{
	ClusterUndoRecordPrepareReceipt receipt, before;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_pending_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	before = receipt;
	receipt_owner_probe_case = 3;
	receipt_log_enabled = true;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_last_log[0], '\0');
	cluster_trace_normal_stop = true;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT(strstr(receipt_last_log, "predicate=owner.grant_nonzero") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "expected=1 current=0") != NULL);
	UT_ASSERT_EQ(memcmp(&before, &receipt, sizeof(before)), 0);
	UT_ASSERT_EQ(receipt_prepare_calls + receipt_cancel_calls, 0);
	UT_ASSERT_EQ(strcmp(cluster_undo_record_receipt_last_reason(),
						"CTRC_PENDING_IDENTITY_MISMATCH"), 0);
	cluster_trace_normal_stop = false;
}

UT_TEST(test_ctrc_prepare_reports_first_actual_predicate_and_available_identity)
{
	const char *predicates[] = { "receipt.present", "target.ordinal", "mask.pending",
		"mask.prepared_zero", "reservation.active", "reservation.sequence",
		"extent.cur_block", "owner.lookup_exact", "owner.in_progress",
		"owner.grant_nonzero", "owner.tt_segment", "owner.tt_slot", "reservation.image_byte" };

	for (unsigned i = 0; i < lengthof(predicates); i++) {
		ClusterUndoRecordPrepareReceipt receipt;
		uint8 ordinal = 0;

		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		receipt.ctrc_pending_mask = 1;
		receipt.ctrc_pending_targets[0].spc_oid = 1663;
		receipt.ctrc_pending_targets[0].db_oid = 5;
		receipt.ctrc_pending_targets[0].rel_number = 16388;
		receipt.ctrc_pending_targets[0].block_number = 137;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		if (i == 1)
			ordinal = CLUSTER_UNDO_RECORD_CTRC_TARGETS;
		if (i == 2)
			receipt.ctrc_pending_mask = 0;
		if (i == 3) {
			receipt.ctrc_prepared_mask = 1;
			receipt.ctrc_handles[0].valid = true;
		}
		if (i == 4)
			cluster_undo_record_reservation.active = false;
		if (i == 5)
			receipt.reservation_sequence++;
		if (i == 6)
			cluster_undo_current_extent.cur_block++;
		if (i >= 7 && i <= 11)
			receipt_owner_probe_case = i - 6;
		if (i == 12)
			receipt.payload_capacity++;
		receipt_log_enabled = cluster_trace_normal_stop = true;
		UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(i == 0 ? NULL : &receipt, ordinal));
		UT_ASSERT(strstr(receipt_last_log, "PGRAC_FAMILY=CTRC_RECEIPT") != NULL);
		UT_ASSERT(strstr(receipt_last_log, predicates[i]) != NULL);
		UT_ASSERT(strstr(receipt_last_log, "event=prepare.reject") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "authoritative=0") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "canonical_key_known=") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "ref_slot=7") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "event_seq=") != NULL);
		if (i == 12) {
			UT_ASSERT(strstr(receipt_last_log, "expected=128 current=129") != NULL);
			UT_ASSERT(strstr(receipt_last_log, "receipt_byte_offset_known=1") != NULL);
		}
		UT_ASSERT_EQ(receipt_prepare_calls, 0);
	}
	cluster_trace_normal_stop = false;
}

UT_TEST(test_ctrc_diagnostic_skips_critical_output_without_changing_decisions)
{
	ClusterUndoRecordPrepareReceipt receipt;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_pending_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	receipt_owner_probe_case = 3;
	receipt_log_enabled = cluster_trace_normal_stop = true;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_last_log[0], '\0');
	CritSectionCount = 0;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT(strstr(receipt_last_log, "trace_critical_skipped=1") != NULL);
	UT_ASSERT_EQ(receipt_prepare_calls, 0);
	/* Default OFF success remains quiet; enabled caller lifecycle is tested separately. */
	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_pending_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	receipt_log_enabled = true; cluster_trace_normal_stop = false;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_last_log[0], '\0');
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	cluster_trace_normal_stop = false;
}

static void
receipt_lifecycle_fixture(ClusterUndoRecordPrepareReceipt *receipt)
{
	receipt_fixture_ready(UNDO_RECORD_UPDATE, receipt);
	receipt_lifecycle_controls = true;
	receipt->ctrc_pending_mask = 1;
	receipt->ctrc_pending_targets[0].kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	receipt->ctrc_pending_targets[0].spc_oid = 1663;
	receipt->ctrc_pending_targets[0].db_oid = 5;
	receipt->ctrc_pending_targets[0].rel_number = 16388;
	receipt->ctrc_pending_targets[0].block_number = 137;
	receipt->ctrc_pending_targets[0].itl_slot_wrap = 22;
	receipt->ctrc_pending_targets[0].itl_xid = 700;
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	receipt_log_enabled = cluster_trace_normal_stop = true;
}

UT_TEST(test_receipt_lifecycle_default_off_keeps_prepare_cancel_effects)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); cluster_trace_normal_stop = false;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_prepare_observed_calls, 0);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
	UT_ASSERT_EQ(receipt_log_count, 0);
	UT_ASSERT_EQ(receipt_observe_calls, 0);
}
#ifndef CLUSTER_CTRC_PREPARE_TABLE_PATH_PRESENT
#define CLUSTER_CTRC_PREPARE_PATH_TABLE_SLOT_PREPARE ((ClusterCtrcPreparePath)16)
#endif

UT_TEST(test_receipt_lifecycle_reports_original_shared_path_without_replay)
{
	for (unsigned path = CLUSTER_CTRC_PREPARE_PATH_NAMESPACE;
		 path <= CLUSTER_CTRC_PREPARE_PATH_TABLE_SLOT_PREPARE; path++) {
		ClusterUndoRecordPrepareReceipt receipt;
		char expected[96];

		receipt_lifecycle_fixture(&receipt);
		receipt_prepare_result = CLUSTER_CTRC_PREPARE_REFUSED;
		receipt_prepare_path = (ClusterCtrcPreparePath)path;
		UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		UT_ASSERT_EQ(receipt_prepare_calls, 1);
		UT_ASSERT_EQ(receipt_prepare_observed_calls, 1);
		UT_ASSERT_EQ(receipt_cancel_calls, 0);
		UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 0);
		snprintf(expected, sizeof(expected), "shared_prepare_path_known=1 shared_prepare_path=%u", path);
		UT_ASSERT(strstr(receipt_last_log, "event=prepare.refused") != NULL);
		UT_ASSERT(strstr(receipt_last_log, expected) != NULL);
		UT_ASSERT(strcmp(cluster_undo_record_receipt_last_reason(), "CTRC_PREPARE_REFUSED") == 0);
	}
}
UT_TEST(test_receipt_lifecycle_begin_unknown_and_cancel_do_not_borrow_shared_path)
{
	ClusterUndoRecordPrepareReceipt receipt;
	const char *begin, *end, *unknown;

	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_prepare_observed_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepare_path_known=1 shared_prepare_path=7") != NULL);
	begin = strstr(receipt_all_logs, "event=prepare.begin");
	UT_ASSERT_NOT_NULL(begin);
	end = strchr(begin, '\n');
	unknown = strstr(begin, "shared_prepare_path_known=0 shared_prepare_path=0");
	UT_ASSERT(end != NULL && unknown != NULL && unknown < end);
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(strstr(receipt_last_log, "event=cancel.local_retired") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepare_path_known=0 shared_prepare_path=0") != NULL);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);

	receipt_lifecycle_fixture(&receipt);
	receipt_prepare_path = CLUSTER_CTRC_PREPARE_PATH_UNKNOWN;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT(strstr(receipt_last_log, "event=prepare.ready") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepare_path_known=0 shared_prepare_path=0") != NULL);
}
UT_TEST(test_receipt_lifecycle_owner_publication_ready_are_ordered)
{
	ClusterUndoRecordPrepareReceipt receipt;
	const char *begin, *ready;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	begin = strstr(receipt_all_logs, "event=prepare.begin");
	ready = strstr(receipt_all_logs, "event=prepare.ready");
	UT_ASSERT(begin && ready && begin < ready);
	UT_ASSERT_EQ(receipt_owner_calls, 1);
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT(strstr(receipt_all_logs, "key_source=original_owner_lookup") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "system_identifier=4321") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "slot_wrap=9 canonical_xid=700") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "itl_wrap=22 itl_xid=700") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "publication_known=1") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "attempt=1") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "participant_index=4 receipt_index=5 journal_slot_generation=9") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "shared_state_known=0") != NULL);
	UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
}
UT_TEST(test_receipt_lifecycle_original_prepare_refusal_is_not_handle_or_receipt)
{
	const ClusterCtrcPrepareResult variants[] = {CLUSTER_CTRC_PREPARE_REFUSED, CLUSTER_CTRC_PREPARE_CAPACITY};
	for (unsigned i = 0; i < lengthof(variants); i++) {
		ClusterUndoRecordPrepareReceipt receipt;
		receipt_lifecycle_fixture(&receipt); receipt_prepare_result = variants[i];
		UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		UT_ASSERT_EQ(receipt_prepare_calls, 1);
		UT_ASSERT_EQ(receipt_cancel_calls, 0);
		UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 0);
		UT_ASSERT(strstr(receipt_last_log, "event=prepare.refused") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "handle_known=0") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "prepare_result_known=1") != NULL);
		UT_ASSERT(strcmp(cluster_undo_record_receipt_last_reason(), "CTRC_PREPARE_REFUSED") == 0);
	}
}
UT_TEST(test_receipt_lifecycle_duplicate_keeps_original_acceptance)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); receipt_prepare_result = CLUSTER_CTRC_PREPARE_DUPLICATE;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT(strstr(receipt_last_log, "event=prepare.ready") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "prepare_result=2") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_sync_result=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepare_path_known=1 shared_prepare_path=7") != NULL);
	UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
}
UT_TEST(test_receipt_lifecycle_sync_refusal_reports_original_cancel_return)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); receipt_fail_sync_after_prepare = true;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "event=prepare.sync_refused") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_sync_result=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepare_path_known=1 shared_prepare_path=7") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "before_local_known=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "cancel_results_known=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "cancel_attempted_mask=1 cancel_succeeded_mask=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_retired_known=0") != NULL);
}
UT_TEST(test_receipt_lifecycle_begin_log_error_has_no_unregistered_shared_effect)
{
	ClusterUndoRecordPrepareReceipt receipt;
	volatile bool caught = false;
	receipt_lifecycle_fixture(&receipt);
	receipt_fail_log_event = "event=prepare.begin";
	if (sigsetjmp(receipt_error_jump, 1) == 0)
		(void)cluster_undo_record_ctrc_prepare_pending(&receipt, 0);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(receipt_prepare_calls, 0);
	receipt_fail_log_event = NULL;
	receipt_abort_after_lwlock_release();
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
	UT_ASSERT_EQ(receipt_cancel_calls, 0);
}
UT_TEST(test_receipt_lifecycle_ready_log_error_preserves_exact_cleanup_owner)
{
	ClusterUndoRecordPrepareReceipt receipt;
	volatile bool caught = false;
	receipt_lifecycle_fixture(&receipt); receipt_fail_log_event = "event=prepare.ready";
	if (sigsetjmp(receipt_error_jump, 1) == 0)
		(void)cluster_undo_record_ctrc_prepare_pending(&receipt, 0);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(cluster_undo_record_reservation.receipt.ctrc_prepared_mask, 1);
	receipt_fail_log_event = NULL;
	receipt_abort_after_lwlock_release();
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
}
UT_TEST(test_receipt_lifecycle_cancel_log_follows_complete_local_cleanup)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "event=cancel.local_retired") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "before_ref_slot=7") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_active=0 local_ref_owned=0 local_ref_slot=-1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "key_source=owned_handle_projection") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "publication_known=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "raw_xid=0 raw_xid_known=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_retired_known=0") != NULL);
}
UT_TEST(test_receipt_lifecycle_cancel_log_error_cannot_repeat_cleanup)
{
	ClusterUndoRecordPrepareReceipt receipt;
	volatile bool caught = false;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	receipt_fail_log_event = "event=cancel.local_retired";
	if (sigsetjmp(receipt_error_jump, 1) == 0)
		cluster_undo_record_cancel_prepared(&receipt);
	else
		caught = true;
	UT_ASSERT(caught && !cluster_undo_record_reservation.active);
	receipt_fail_log_event = NULL;
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
}
UT_TEST(test_receipt_lifecycle_borrowed_or_applied_handle_is_not_cancelled)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		receipt_lifecycle_fixture(&receipt);
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		if (variant == 0) receipt.ctrc_reuse_mask = 1;
		else receipt.ctrc_applied_mask = 1;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, 0);
		UT_ASSERT(strstr(receipt_last_log, "event=cancel.local_retired") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "cancel_attempted_mask=0 cancel_succeeded_mask=0") != NULL);
		UT_ASSERT(strstr(receipt_last_log, "shared_retired_known=0") != NULL);
	}
}
UT_TEST(test_receipt_lifecycle_failed_cancel_is_not_shared_retirement)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	receipt_cancel_fail_at = 1;
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "cancel_attempted_mask=1 cancel_succeeded_mask=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_retired_known=0") != NULL);
	UT_ASSERT(!cluster_undo_record_reservation.active);
}
UT_TEST(test_receipt_lifecycle_critical_and_held_lock_gaps_do_not_change_prepare)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); CritSectionCount = 1;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_log_count, 0);
	CritSectionCount = 0; cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(strstr(receipt_last_log, "trace_critical_skipped=2") != NULL);
	receipt_lifecycle_fixture(&receipt); receipt_trace_lwlock_held = true;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_log_count, 0);
	receipt_trace_lwlock_held = false; cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(strstr(receipt_last_log, "trace_lwlock_skipped=2") != NULL);
}
UT_TEST(test_receipt_lifecycle_two_targets_keep_distinct_publication_operations)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt);
	receipt.ctrc_pending_mask = 3;
	receipt.ctrc_pending_targets[1] = receipt.ctrc_pending_targets[0];
	receipt.ctrc_pending_targets[1].block_number = 138;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 1));
	{
		char first[64], second[64];
		snprintf(first, sizeof(first), "operation_id=" UINT64_FORMAT " ", receipt.reservation_sequence);
		snprintf(second, sizeof(second), "operation_id=" UINT64_FORMAT " ", receipt.reservation_sequence + 1);
		UT_ASSERT(strstr(receipt_all_logs, first) != NULL);
		UT_ASSERT(strstr(receipt_all_logs, second) != NULL);
	}
	UT_ASSERT(strstr(receipt_all_logs, "target_ordinal=1") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "block=138") != NULL);
	UT_ASSERT_EQ(receipt_owner_calls, 2);
	UT_ASSERT_EQ(receipt_prepare_calls, 2);
}
UT_TEST(test_receipt_lifecycle_cancel_is_idempotent_and_does_not_read_shared_pointer)
{
	ClusterUndoRecordPrepareReceipt receipt;
	int prior;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT((uintptr_t)receipt.ctrc_handles[0].receipt == 1);
	cluster_undo_record_cancel_prepared(&receipt); prior = receipt_log_count;
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT(strstr(receipt_all_logs, "event=cancel.local_retired") != NULL);
	UT_ASSERT_EQ(receipt_log_count, prior);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
}

UT_TEST(test_receipt_lifecycle_shared_sample_uses_actual_publication_fields)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); receipt_observe_available = true;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_observe_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "event=prepare.ready") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_observed=1 shared_mutable_atomic=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_journal_sequence=501 shared_key_sequence=17") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_key_matches_reported=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_wire_request=601 shared_attempt=3 shared_requester_backend=77") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_participant_formation=1201 shared_participant_admission=1301") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_itl_slot=2 shared_sample_offset=7") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_uba=ab000000000000000000000000000000") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "trace_sequence_exhausted=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_state_known=0 shared_retired_known=0") != NULL);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_receipt_lifecycle_shared_sample_after_original_local_retirement)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt); receipt_observe_available = true;
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt_observe_calls, 2);
	UT_ASSERT(strstr(receipt_last_log, "event=cancel.local_retired") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_observed=1 shared_mutable_atomic=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_prepared_count=0 shared_applied_count=0 shared_cancelled_count=1") != NULL);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
}

UT_TEST(test_receipt_lifecycle_unknown_shared_sample_does_not_borrow_expected_target)
{
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_lifecycle_fixture(&receipt);
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_observe_calls, 1);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_observed=0 shared_mutable_atomic=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_sample_rel=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_journal_sequence=0 shared_key_sequence=0") != NULL);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_receipt_lifecycle_sample_error_keeps_registered_cleanup_owner)
{
	ClusterUndoRecordPrepareReceipt receipt;
	volatile bool caught = false;
	receipt_lifecycle_fixture(&receipt); receipt_observe_throw = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0)
		(void)cluster_undo_record_ctrc_prepare_pending(&receipt, 0);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(cluster_undo_record_reservation.receipt.ctrc_prepared_mask, 1);
	receipt_observe_throw = false;
	receipt_abort_after_lwlock_release();
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT(!cluster_undo_record_reservation.active);
}


/* Exercise real APPLY/consume with the existing explicit shared/storage
 * boundaries. The native heap/ResourceOwner path is not simulated as proof. */
static void
receipt_consumer_fixture(ClusterUndoRecordPrepareReceipt *receipt, uint8 count, bool lock)
{
	receipt_fixture_ready(count == 2 ? UNDO_RECORD_UPDATE : UNDO_RECORD_INSERT, receipt);
	receipt_lifecycle_controls = true;
	receipt->ctrc_pending_mask = (UINT8_C(1) << count) - 1;
	for (uint8 i = 0; i < count; i++) {
		receipt->ctrc_pending_targets[i].kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
		receipt->ctrc_pending_targets[i].spc_oid = 1663;
		receipt->ctrc_pending_targets[i].db_oid = 5;
		receipt->ctrc_pending_targets[i].rel_number = 16388;
		receipt->ctrc_pending_targets[i].block_number = 137 + i;
		receipt->ctrc_pending_targets[i].itl_slot_index = i;
	}
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	for (uint8 i = 0; i < count; i++) {
		UndoItlHistoryEntry history = {0};
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(receipt, i));
		history.block = 137 + i; history.slot_index = i;
		history.after_kind = ITL_FLAG_ACTIVE; history.after_write_scn = 999;
		UT_ASSERT(cluster_undo_record_stage_history(receipt, i, &history));
	}
	if (lock)
		UT_ASSERT_EQ(cluster_undo_record_consume_preflight(receipt, count == 2
			? sizeof(UndoUpdatePayload) + 32 : sizeof(UndoInsertPayload)),
					 CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	receipt_log_enabled = cluster_trace_normal_stop = true;
	receipt_last_log[0] = receipt_all_logs[0] = '\0'; receipt_log_count = 0;
}

static void
receipt_consumer_apply(ClusterUndoRecordPrepareReceipt *receipt, uint8 ordinal)
{
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = receipt->ctrc_pending_targets[ordinal];
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(receipt, ordinal, &target, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
}

static void
receipt_consumer_finish(ClusterUndoRecordPrepareReceipt *receipt, UBA *uba)
{
	ClusterUndoRecordTarget target = {0};
	char payload[sizeof(UndoUpdatePayload) + 32] = {0};
	uint16 payload_length = sizeof(UndoInsertPayload);
	if (receipt->record_type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update = {138, 2, 32, sizeof(UndoUpdatePayload), 0};
		memcpy(payload, &update, sizeof(update)); payload_length = sizeof(payload);
	}
	target.locator.spcOid = 1663; target.locator.dbOid = 5; target.locator.relNumber = 16388;
	target.forknum = MAIN_FORKNUM; target.blockno = 137; target.offnum = 1;
	UT_ASSERT_EQ(cluster_undo_record_consume_prepared(receipt, &target, payload, payload_length, uba),
				 CLUSTER_UNDO_RECORD_CONSUME_APPLIED);
}

UT_TEST(test_receipt_consumer_observation_off_preserves_effects)
{
	ClusterUndoRecordPrepareReceipt receipt;
	UBA uba;
	for (uint8 count = 1; count <= 2; count++) {
		receipt_consumer_fixture(&receipt, count, true); cluster_trace_normal_stop = false;
		for (uint8 i = 0; i < count; i++)
			receipt_consumer_apply(&receipt, i);
		receipt_consumer_finish(&receipt, &uba);
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
		undo_receipt_consumer_trace_flush("unit.off");
#endif
		UT_ASSERT_EQ(receipt_apply_calls, count); UT_ASSERT_EQ(receipt_install_calls, 1);
		UT_ASSERT_EQ(receipt_log_count, 0); UT_ASSERT(!UBA_is_invalid(uba));
	}
}

UT_TEST(test_receipt_consumer_observation_survives_original_erasure)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt, zero = {0};
	UBA uba;
	receipt_consumer_fixture(&receipt, 1, true); CritSectionCount = 1;
	receipt_consumer_apply(&receipt, 0); receipt_consumer_finish(&receipt, &uba);
	UT_ASSERT_EQ(receipt_log_count, 0);
	UT_ASSERT_EQ(memcmp(&receipt, &zero, sizeof(receipt)), 0);
	UT_ASSERT(!cluster_undo_record_reservation.active && cluster_undo_pending.active);
	CritSectionCount = 0; undo_receipt_consumer_trace_flush("unit.after_consume");
	UT_ASSERT_EQ(receipt_log_count, 1);
	UT_ASSERT(strstr(receipt_last_log, "PGRAC_FAMILY=CTRC_CONSUME") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "consumer_sequence=17") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "shared_result_known=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_sync_known=1 local_sync_ok=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "consume_succeeded=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "data_rel=16388 data_fork=0 data_block=137 data_offset=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "authoritative=0 shared_retired_known=0") != NULL);
	UT_ASSERT(!undo_receipt_consumer_trace.valid);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_observation_unsafe_drain_retains_exact_sample)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	UBA uba;
	receipt_consumer_fixture(&receipt, 1, true);
	receipt_consumer_apply(&receipt, 0); receipt_consumer_finish(&receipt, &uba);
	CritSectionCount = 1; undo_receipt_consumer_trace_flush("unit.critical");
	CritSectionCount = 0; receipt_trace_lwlock_held = true;
	undo_receipt_consumer_trace_flush("unit.lock");
	UT_ASSERT_EQ(receipt_log_count, 0); UT_ASSERT(undo_receipt_consumer_trace.valid);
	receipt_trace_lwlock_held = false; undo_receipt_consumer_trace_flush("unit.safe");
	UT_ASSERT_EQ(receipt_log_count, 1);
	UT_ASSERT(strstr(receipt_last_log, "critical_skipped=1 lwlock_skipped=1") != NULL);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_observation_two_targets_keep_borrowed_route)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	UBA uba;
	receipt_consumer_fixture(&receipt, 2, true);
	receipt.ctrc_reuse_mask = 2;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	receipt_consumer_apply(&receipt, 0); receipt_consumer_apply(&receipt, 1);
	receipt_consumer_finish(&receipt, &uba); undo_receipt_consumer_trace_flush("unit.two");
	UT_ASSERT_EQ(receipt_apply_calls, 2); UT_ASSERT_EQ(receipt_log_count, 2);
	UT_ASSERT(strstr(receipt_all_logs, "target_ordinal=0 route_retarget=0") != NULL);
	UT_ASSERT(strstr(receipt_all_logs, "target_ordinal=1 route_retarget=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "final_block=138") != NULL);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_observation_original_shared_refusal_is_not_success)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	receipt_consumer_fixture(&receipt, 1, false);
	receipt_apply_result = CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &receipt.ctrc_pending_targets[0], &token),
				 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	undo_receipt_consumer_trace_flush("unit.refusal");
	UT_ASSERT(strstr(receipt_last_log, "shared_result_known=1") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_sync_known=0 local_sync_ok=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "consume_succeeded=0") != NULL);
	UT_ASSERT(cluster_undo_record_reservation.active && cluster_undo_record_reservation.apply_attempted);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_install_calls, 0);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_observation_shared_success_local_sync_failure_separate)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	receipt_consumer_fixture(&receipt, 1, false); receipt_fail_sync_after_apply = true;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &receipt.ctrc_pending_targets[0], &token),
				 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	undo_receipt_consumer_trace_flush("unit.sync_failure");
	UT_ASSERT(strstr(receipt_last_log, "local_sync_known=1 local_sync_ok=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "consume_succeeded=0") != NULL);
	UT_ASSERT_EQ(receipt.ctrc_applied_mask, 1);
	UT_ASSERT(cluster_undo_record_reservation.active && cluster_undo_record_reservation.apply_attempted);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_log_error_cannot_replay_or_change_pending_owner)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	UBA uba;
	ClusterUndoPendingBlock before;
	receipt_consumer_fixture(&receipt, 1, true);
	receipt_consumer_apply(&receipt, 0); receipt_consumer_finish(&receipt, &uba);
	before = cluster_undo_pending; receipt_fail_log_event = "PGRAC_FAMILY=CTRC_CONSUME";
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		undo_receipt_consumer_trace_flush("unit.log_error"); UT_ASSERT(false);
	}
	UT_ASSERT(!undo_receipt_consumer_trace.valid);
	UT_ASSERT_EQ(memcmp(&before, &cluster_undo_pending, sizeof(before)), 0);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	receipt_fail_log_event = NULL; undo_receipt_consumer_trace_flush("unit.repeat");
	UT_ASSERT_EQ(receipt_log_count, 1); UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls, 0);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_consumer_safe_flush_wiring_precedes_new_effect_and_follows_pending_flush)
{
	char *source = read_undo_record_source();
	const char *prepare, *new_effect, *flush, *precommit, *pending, *check, *early;
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) return;
	prepare = strstr(source, "\ncluster_undo_record_ctrc_prepare_pending(");
	precommit = strstr(source, "\ncluster_undo_xact_precommit_flush(");
	flush = prepare == NULL ? NULL : strstr(prepare, "undo_receipt_consumer_trace_flush(\"next_prepare\")");
	new_effect = prepare == NULL ? NULL : strstr(prepare, "result = cluster_ctrc_receipt_prepare_shared(");
	pending = precommit == NULL ? NULL : strstr(precommit, "cluster_undo_pending_flush_internal(true)");
	check = precommit == NULL ? NULL : strstr(precommit, "undo_receipt_consumer_trace_flush(\"precommit_after_pending\")");
	early = precommit == NULL ? NULL : strstr(precommit, "if (cluster_undo_touched_seg_count == 0)");
	UT_ASSERT(prepare != NULL && flush != NULL && new_effect != NULL && flush < new_effect && new_effect < precommit);
	UT_ASSERT(pending != NULL && check != NULL && early != NULL && pending < check && check < early);
	free(source);
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	{
		ClusterUndoRecordPrepareReceipt receipt;
		UBA uba;
		unsigned prior_prepare;
		receipt_consumer_fixture(&receipt, 1, true);
		receipt_consumer_apply(&receipt, 0); receipt_consumer_finish(&receipt, &uba);
		prior_prepare = receipt_prepare_calls;
		UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(NULL, CLUSTER_UNDO_RECORD_CTRC_TARGETS));
		UT_ASSERT(strstr(receipt_all_logs, "emit_stage=next_prepare") != NULL);
		UT_ASSERT_EQ(receipt_prepare_calls, prior_prepare);
	}
#endif
}


UT_TEST(test_receipt_consumer_later_apply_error_cannot_reuse_earlier_result)
{
#ifdef CLUSTER_UNDO_RECEIPT_CONSUME_TRACE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	receipt_consumer_fixture(&receipt, 1, true);
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	receipt_zero_evidence = true;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0,
		&receipt.ctrc_pending_targets[0], &token), CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
#ifdef CLUSTER_CTRC_APPLY_EVIDENCE_PRESENT
	/* The second call is a legitimate new preflight, not an overwrite of
	 * earlier uncertainty on the same ordinal. Its trace must start unknown. */
	UT_ASSERT(cluster_undo_record_release_zero_effect_preflight(&receipt));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 32, false),
		CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 32),
		CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
#endif
	receipt_zero_evidence = false;
	receipt_throw_apply = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0,
			&receipt.ctrc_pending_targets[0], &token);
		UT_ASSERT(false);
	}
	receipt_throw_apply = false;
	/* ERROR unwinding drops native LWLocks before this safe diagnostic drain;
	 * retain the reservation's stale lock bit and unknown-effect obligation. */
	receipt_buffer_locked = false;
	undo_receipt_consumer_trace_flush("unit.later_error");
	UT_ASSERT(strstr(receipt_last_log, "apply_call_count=2 shared_result_known=0") != NULL);
	UT_ASSERT(strstr(receipt_last_log, "local_sync_known=0 local_sync_ok=0") != NULL);
	UT_ASSERT(cluster_undo_record_reservation.active && cluster_undo_record_reservation.apply_attempted);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls, 0);
#else
	UT_ASSERT(false);
#endif
}

static bool
receipt_release_exact_zero(ClusterUndoRecordPrepareReceipt *receipt)
{
#ifdef CLUSTER_CTRC_APPLY_EVIDENCE_PRESENT
	return cluster_undo_record_release_zero_effect_preflight(receipt);
#else
	/* Baseline's original owner cannot requalify after consume_preflight. */
	return cluster_undo_record_requalify_for_retry(receipt, 32, false)
		== CLUSTER_UNDO_RECORD_PREPARE_READY;
#endif
}

UT_TEST(test_exact_zero_releases_preflight_once_then_requalifies)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordPrepareReceipt before;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };
	bool released;

	receipt_abort_fixture(&receipt);
	before = receipt;
	target.block_number = 37;
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	receipt_zero_evidence = true;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	released = receipt_release_exact_zero(&receipt);
	UT_ASSERT(released);
	if (!released)
		return;
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT(!cluster_undo_record_reservation.consume_locked);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
	UT_ASSERT_EQ(memcmp(&before, &receipt, sizeof(receipt)), 0);
	UT_ASSERT(!receipt_release_exact_zero(&receipt));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 32, false),
		CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT_EQ(memcmp(&before, &receipt, sizeof(receipt)), 0);
	/* A new preflight can retry the same ordinal; it cannot inherit an old
	 * attempt's proof or renew its reservation/deadline. */
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 32),
		CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(receipt_release_exact_zero(&receipt));
	UT_ASSERT_EQ(memcmp(&before, &receipt, sizeof(receipt)), 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(test_exact_zero_abort_after_lwlock_release_is_cleanup_not_panic)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_abort_fixture(&receipt);
	target.block_number = 37;
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	receipt_zero_evidence = true;
	(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0)
		receipt_abort_after_lwlock_release();
	else
		UT_ASSERT(false);
	receipt_trap_error = false;
	if (cluster_undo_record_reservation.active)
		return;
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
	receipt_abort_after_lwlock_release();
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
}

UT_TEST(test_native_after_apply_error_keeps_original_branch_cleanup_polarity)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };
		bool injected = false;
		bool should_inject = variant != 3 && variant != 4;
		bool safe = variant == 0 || variant == 3 || variant == 4;

		receipt_abort_fixture(&receipt);
		target.block_number = 37;
		receipt_apply_result = variant == 2 ? CLUSTER_CTRC_APPLY_APPLIED
			: CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
		receipt_zero_evidence = variant != 1 && variant != 2;
		cluster_trace_normal_stop = variant != 3;
		CritSectionCount = variant == 4 ? 1 : 0;
		cluster_injection_armed_count = 1;
		receipt_trap_error = true;
		if (sigsetjmp(receipt_error_jump, 1) == 0)
			(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
		else
			injected = true;
		UT_ASSERT_EQ(injected, should_inject);
		UT_ASSERT_EQ(receipt_after_apply_injection_hits, should_inject ? 1 : 0);
		cluster_trace_normal_stop = false;
		CritSectionCount = 0;
		cluster_injection_armed_count = 0;
		receipt_error_level = 0;
		if (sigsetjmp(receipt_error_jump, 1) == 0) {
			receipt_abort_after_lwlock_release();
			UT_ASSERT(safe);
		} else {
			UT_ASSERT(variant == 1 || variant == 2);
			UT_ASSERT_EQ(receipt_error_level, PANIC);
			UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
		}
		receipt_trap_error = false;
		if (safe) {
			UT_ASSERT(!cluster_undo_record_reservation.active);
			UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
		}
	}
}

UT_TEST(test_exact_zero_never_accepts_unknown_drift_or_new_apply)
{
	int variant;
	for (variant = 0; variant < 6; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };

		receipt_abort_fixture(&receipt);
		target.block_number = 37;
		receipt_apply_result = variant == 2 ? CLUSTER_CTRC_APPLY_APPLIED
			: CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
		receipt_zero_evidence = variant != 0 && variant != 2;
		(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
		if (variant == 5) {
			UT_ASSERT(receipt_release_exact_zero(&receipt));
			receipt_evidence_valid = false;
			UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, 32),
				CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_REFUSED);
			UT_ASSERT(!receipt_buffer_locked);
			continue;
		}
		if (variant == 1)
			receipt_evidence_valid = false;
		if (variant == 3) {
			receipt.ctrc_handles[0].journal_slot_generation++;
			UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		}
		if (variant == 4) {
			receipt.ctrc_pending_mask |= 4; /* Unsupported ordinal cannot be ignored. */
			UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		}
		UT_ASSERT(!receipt_release_exact_zero(&receipt));
		UT_ASSERT(receipt_buffer_locked);
		UT_ASSERT(cluster_undo_record_reservation.active);
		receipt_trap_error = true;
		if (sigsetjmp(receipt_error_jump, 1) == 0) {
			receipt_abort_after_lwlock_release();
			UT_ASSERT(false);
		} else
			UT_ASSERT_EQ(receipt_error_level, PANIC);
		receipt_trap_error = false;
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
	}
}

UT_TEST(test_exact_zero_cannot_overwrite_an_earlier_unknown_call)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_abort_fixture(&receipt);
	target.block_number = 37;
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_RETRY_REQUIRED); /* UNKNOWN: may have performed a CAS. */
	receipt_zero_evidence = true;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	UT_ASSERT_EQ(receipt_apply_calls, 1);
	UT_ASSERT(!receipt_release_exact_zero(&receipt));
}

static void
receipt_zero_two_target_fixture(ClusterUndoRecordPrepareReceipt *receipt, uint8 reuse)
{
	uint8 ordinal;
	receipt_fixture_ready(UNDO_RECORD_UPDATE, receipt);
	receipt->ctrc_pending_mask = 3;
	for (ordinal = 0; ordinal < 2; ordinal++) {
		UndoItlHistoryEntry history = { 0 };
		receipt->ctrc_pending_targets[ordinal].block_number = 37 + ordinal;
		receipt->ctrc_pending_targets[ordinal].itl_slot_index = ordinal;
		UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(receipt, ordinal));
		history.block = 37 + ordinal;
		history.slot_index = ordinal;
		history.after_kind = ITL_FLAG_ACTIVE;
		history.after_write_scn = 999;
		UT_ASSERT(cluster_undo_record_stage_history(receipt, ordinal, &history));
	}
	/* Runtime-boundary fixture supplies original borrowed APPLIED handles. */
	receipt->ctrc_reuse_mask = reuse;
	UT_ASSERT(cluster_undo_record_receipt_sync(receipt));
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(receipt, 32),
		CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
}

UT_TEST(test_exact_zero_all_ordinals_keep_unattempted_and_borrowed_owners)
{
	uint8 reuse;
	for (reuse = 0; reuse < 4; reuse++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };
		bool released;

		receipt_zero_two_target_fixture(&receipt, reuse);
		target.block_number = 37;
		receipt_zero_evidence = true;
		receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
			CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
		released = receipt_release_exact_zero(&receipt);
		UT_ASSERT(released);
		if (!released)
			continue;
		UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 32, false),
			CLUSTER_UNDO_RECORD_PREPARE_READY);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, 2 - ((reuse & 1) != 0) - ((reuse & 2) != 0));
	}
}

UT_TEST(test_exact_zero_later_ordinal_does_not_erase_first_apply)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_zero_two_target_fixture(&receipt, 0);
	target.block_number = 37;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_APPLIED);
	target.block_number = 38;
	target.itl_slot_index = 1;
	receipt_zero_evidence = true;
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 1, &target, &token);
	UT_ASSERT(!receipt_release_exact_zero(&receipt));
	UT_ASSERT_EQ(receipt.ctrc_applied_mask, 1);
	UT_ASSERT(receipt_buffer_locked);
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		receipt_abort_after_lwlock_release();
		UT_ASSERT(false);
	} else
		UT_ASSERT_EQ(receipt_error_level, PANIC);
	receipt_trap_error = false;
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
}

static void
receipt_assert_uncertain_abort(void)
{
	receipt_error_level = 0;
	receipt_error_detail[0] = '\0';
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		receipt_abort_after_lwlock_release();
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(receipt_error_level, PANIC);
		UT_ASSERT(strstr(receipt_error_detail, "PGRAC_REASON=UNDO_ABORT_EFFECT_UNCERTAIN ") != NULL);
	}
	receipt_trap_error = false;
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
}

/* The native return-point ERROR precedes the local applied bit. A PANIC
 * without the original shared result must not stand in for an APPLIED cut. */
UT_TEST(test_abort_detail_identifies_applied_before_local_accounting)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_abort_fixture(&receipt);
	target.block_number = 37;
	cluster_trace_normal_stop = true;
	cluster_injection_armed_count = 1;
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
		UT_ASSERT(false);
	} else
		UT_ASSERT_EQ(receipt_error_level, ERROR);
	cluster_injection_armed_count = 0;
	UT_ASSERT_EQ(receipt.ctrc_applied_mask, 0);
	receipt_assert_uncertain_abort();
	UT_ASSERT(strstr(receipt_error_detail, "attempted_mask=1 zero_effect_mask=0 pending_mask=1 prepared_mask=1 ") != NULL);
	UT_ASSERT(strstr(receipt_error_detail, "trace_sample_mask=1 shared_result_known_mask=1 shared_result_0=1 shared_result_1=0 apply_call_count_0=1 apply_call_count_1=0 authoritative=0") != NULL);
}

UT_TEST(test_abort_detail_does_not_invent_a_shared_result_after_error)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_abort_fixture(&receipt);
	target.block_number = 37;
	cluster_trace_normal_stop = true;
	receipt_throw_apply = true;
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token);
		UT_ASSERT(false);
	}
	receipt_throw_apply = false;
	UT_ASSERT_EQ(receipt_apply_calls, 1);
	receipt_assert_uncertain_abort();
	UT_ASSERT(strstr(receipt_error_detail, "trace_sample_mask=1 shared_result_known_mask=0 shared_result_0=0 shared_result_1=0 apply_call_count_0=1 apply_call_count_1=0 authoritative=0") != NULL);
}

UT_TEST(test_abort_detail_keeps_both_ordinals_at_second_return_error)
{
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };

	receipt_zero_two_target_fixture(&receipt, 0);
	cluster_trace_normal_stop = true;
	target.block_number = 37;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_APPLIED);
	target.block_number = 38;
	target.itl_slot_index = 1;
	cluster_injection_armed_count = 1;
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		(void)cluster_undo_record_ctrc_apply_prepared(&receipt, 1, &target, &token);
		UT_ASSERT(false);
	} else
		UT_ASSERT_EQ(receipt_error_level, ERROR);
	cluster_injection_armed_count = 0;
	UT_ASSERT_EQ(receipt.ctrc_applied_mask, 1);
	receipt_assert_uncertain_abort();
	UT_ASSERT(strstr(receipt_error_detail, "applied=1 reuse=0 attempted_mask=3 zero_effect_mask=0 pending_mask=3 prepared_mask=3 ") != NULL);
	UT_ASSERT(strstr(receipt_error_detail, "trace_sample_mask=3 shared_result_known_mask=3 shared_result_0=1 shared_result_1=1 apply_call_count_0=1 apply_call_count_1=1 authoritative=0") != NULL);
}

UT_TEST(test_abort_detail_refuses_stale_or_disabled_sample_identity)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterCtrcApplyToken token;
		ClusterCtrcTargetV1 target = { 0 };

		receipt_abort_fixture(&receipt);
		target.block_number = 37;
		cluster_trace_normal_stop = true;
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
			CLUSTER_CTRC_APPLY_APPLIED);
		switch (variant) {
			case 0: cluster_trace_normal_stop = false; break;
			case 1: undo_receipt_consumer_trace.valid = false; break;
			case 2: undo_receipt_consumer_trace.sequence++; break;
			case 3: undo_receipt_consumer_trace.attempt++; break;
			case 4: undo_receipt_consumer_trace.handles[0].journal_slot_generation++; break;
			case 5: undo_receipt_consumer_trace.attempted = 0; break;
			case 6: cluster_undo_record_reservation.receipt.reservation_sequence++; break;
		}
		receipt_assert_uncertain_abort();
		UT_ASSERT(strstr(receipt_error_detail, "trace_sample_mask=0 shared_result_known_mask=0 shared_result_0=0 shared_result_1=0 apply_call_count_0=0 apply_call_count_1=0 authoritative=0") != NULL);
	}
}

/* Capture itself is exercised with actual native frames in
 * test_cluster_savepoint. Here those boundaries are explicit stubs, while
 * receipt validation, APPLY, abort and the record consumer are actual code. */
UT_TEST(test_operation_capture_precedes_reservation_side_effects)
{
	char *source = read_undo_record_source();
	const char *prepare = source == NULL ? NULL : strstr(source,
		"\ncluster_undo_record_prepare_trace_impl(uint8 record_type");
	const char *capture = prepare == NULL ? NULL : strstr(prepare, "undo_record_capture_operation(");
	const char *cancel = prepare == NULL ? NULL : strstr(prepare, "cluster_undo_record_cancel_prepared(");
	const char *create = prepare == NULL ? NULL : strstr(prepare,
		"cluster_undo_active_segment_for_node_or_create(");
	UT_ASSERT_NOT_NULL(capture);
	UT_ASSERT_NOT_NULL(cancel);
	UT_ASSERT_NOT_NULL(create);
	if (capture != NULL && cancel != NULL && create != NULL)
		UT_ASSERT(capture < cancel && capture < create);
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	{
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterSavepointOperation captured;
		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		UT_ASSERT(undo_record_capture_operation(1, 0, &captured));
		UT_ASSERT_EQ(memcmp(&captured, &receipt_operation, sizeof(captured)), 0);
		UT_ASSERT_EQ(receipt_capture_calls, 1);
		UT_ASSERT_EQ(receipt_prepare_calls + receipt_apply_calls + receipt_cancel_calls, 0);
	}
#endif
	free(source);
}

UT_TEST(test_operation_capture_refuses_unproved_owner_and_old_format_child)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		ClusterSavepointOperation captured;
		ClusterSavepointOperation empty = { 0 };
		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		switch (variant) {
			case 0: receipt_capture_ok = false; break;
			case 1: receipt_operation.top.segment_id++; break;
			case 2: receipt_operation.top.slot_offset++; break;
			case 3:
				receipt_operation.operation_fxid = FullTransactionIdFromEpochAndXid(5, 701);
				receipt_operation.subid++;
				break;
		}
		memset(&captured, 0xa5, sizeof(captured));
		UT_ASSERT(!undo_record_capture_operation(1, 0, &captured));
		UT_ASSERT_EQ(memcmp(&captured, &empty, sizeof(captured)), 0);
		UT_ASSERT_EQ(receipt_capture_calls, 1);
		UT_ASSERT_EQ(receipt_prepare_calls + receipt_apply_calls + receipt_cancel_calls
			+ receipt_leave_calls + receipt_unref_calls, 0);
		UT_ASSERT(cluster_undo_record_reservation.active);
	}
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_receipt_sync_cannot_replace_frozen_operation)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordPrepareReceipt original;
	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	original = receipt;
	receipt.operation.operation_sequence++;
	UT_ASSERT(!cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT_EQ(memcmp(&cluster_undo_record_reservation.receipt,
		&original, sizeof(original)), 0);
	cluster_undo_record_cancel_prepared(&original);
	UT_ASSERT(!cluster_undo_record_reservation.active);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_prepared_operation_recheck_refuses_scope_and_full_identity_drift)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	for (unsigned variant = 0; variant < 6; variant++) {
		ClusterUndoRecordPrepareReceipt receipt;
		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		switch (variant) {
			case 0: receipt_scope_valid = false; break;
			case 1: receipt_operation.operation_fxid = FullTransactionIdFromEpochAndXid(6, 700); break;
			case 2: receipt_operation.top_fxid = FullTransactionIdFromEpochAndXid(6, 700); break;
			case 3: receipt_operation.subid++; break;
			case 4: receipt_operation.top.segment_generation++; break;
			case 5: receipt_operation.top.slot_wrap++; break;
		}
		UT_ASSERT(!cluster_undo_record_prepared_recheck(&receipt, 32));
		UT_ASSERT_EQ(receipt_capture_calls, 0);
		UT_ASSERT_EQ(receipt_prepare_calls + receipt_apply_calls, 0);
		/* Scope drift cannot steal the original reservation's cleanup. */
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
	}
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_ctrc_prepare_cannot_rebind_receipt_to_current_scope)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	receipt.ctrc_pending_mask = 1;
	receipt.ctrc_pending_targets[0].block_number = 37;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	receipt_operation.subid++;
	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
	UT_ASSERT_EQ(receipt_prepare_calls + receipt_owner_calls, 0);
	UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 0);
	cluster_undo_record_cancel_prepared(&receipt);
	UT_ASSERT_EQ(receipt_cancel_calls, 0);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_preflight_and_apply_refuse_scope_drift_before_shared_mutation)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target;
	receipt_consumer_fixture(&receipt, 1, false);
	receipt_scope_valid = false;
	UT_ASSERT_EQ(cluster_undo_record_consume_preflight(&receipt, sizeof(UndoInsertPayload)),
		CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_REFUSED);
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT_EQ(receipt_apply_calls, 0);
	cluster_undo_record_cancel_prepared(&receipt);
	receipt_consumer_fixture(&receipt, 1, true);
	target = receipt.ctrc_pending_targets[0];
	receipt_scope_valid = false;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	UT_ASSERT(!cluster_undo_record_reservation.apply_attempted);
	UT_ASSERT_EQ(receipt_apply_calls, 0);
	receipt_abort_after_lwlock_release();
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_consumer_uses_frozen_writer_and_canonical_wrap)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterSavepointOperation captured;
	UBA uba;
	const UndoSlotDirEntry *slot;
	const UndoRecordHeader *record;
	receipt_consumer_fixture(&receipt, 1, true);
	captured = receipt.operation;
	receipt_consumer_apply(&receipt, 0);
	receipt_consumer_finish(&receipt, &uba);
	UT_ASSERT(!UBA_is_invalid(uba));
	slot = UNDO_SLOT_DIR_PTR(cluster_undo_pending.buf, 0);
	record = (const UndoRecordHeader *)(cluster_undo_pending.buf + slot->record_offset);
	UT_ASSERT_EQ(record->xid, XidFromFullTransactionId(captured.operation_fxid));
	/* The old current-binding probe returns wrap=1 in this fixture. */
	UT_ASSERT_EQ(record->tt_wrap_plus1, captured.top.slot_wrap + 1);
	UT_ASSERT_EQ(record->payload_length, sizeof(UndoInsertPayload) + sizeof(UndoItlHistoryTrailer));
	UT_ASSERT_EQ(record->flags & ~UNDO_REC_FLAG_FIRST_IN_TX, UNDO_REC_FLAG_HAS_ITL_HISTORY);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_post_apply_identity_drift_retains_original_fail_stop_duty)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordTarget target = { 0 };
	UndoInsertPayload payload = { 0 };
	UBA uba;
	receipt_consumer_fixture(&receipt, 1, true);
	receipt_consumer_apply(&receipt, 0);
	receipt_scope_valid = false;
	UT_ASSERT_EQ(cluster_undo_record_consume_prepared(&receipt, &target, &payload,
		sizeof(payload), &uba), CLUSTER_UNDO_RECORD_CONSUME_REFUSED);
	UT_ASSERT(UBA_is_invalid(uba));
	UT_ASSERT_EQ(receipt_install_calls, 0);
	receipt_assert_uncertain_abort();
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_exact_zero_abort_keeps_cleanup_after_native_scope_ends)
{
#ifdef CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcTargetV1 target = { 0 };
	receipt_abort_fixture(&receipt);
	target.block_number = 37;
	receipt_apply_result = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	receipt_zero_evidence = true;
	UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &target, &token),
		CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	receipt_scope_valid = false;
	receipt_abort_after_lwlock_release();
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
#else
	UT_ASSERT(false);
#endif
}

int
main(int argc, char **argv)
{
	UT_PLAN(141);
	UT_RUN(test_operation_codec_unaligned_live_and_discarded);
	UT_RUN(test_operation_codec_all_original_body_shapes);
	UT_RUN(test_operation_codec_epoch_and_counter_boundaries);
	UT_RUN(test_operation_codec_rejects_unproved_tail_fields);
	UT_RUN(test_operation_codec_rejects_record_domain_drift);
	UT_RUN(test_operation_codec_short_null_and_missing_outputs);
	UT_RUN(test_operation_codec_keeps_original_body_and_history_guards);
	UT_RUN(test_operation_codec_legacy_consumers_stay_closed);
	UT_RUN(test_receipt_sync_cannot_replace_frozen_operation);
	UT_RUN(test_operation_capture_precedes_reservation_side_effects);
	UT_RUN(test_operation_capture_refuses_unproved_owner_and_old_format_child);
	UT_RUN(test_prepared_operation_recheck_refuses_scope_and_full_identity_drift);
	UT_RUN(test_ctrc_prepare_cannot_rebind_receipt_to_current_scope);
	UT_RUN(test_preflight_and_apply_refuse_scope_drift_before_shared_mutation);
	UT_RUN(test_consumer_uses_frozen_writer_and_canonical_wrap);
	UT_RUN(test_post_apply_identity_drift_retains_original_fail_stop_duty);
	UT_RUN(test_exact_zero_abort_keeps_cleanup_after_native_scope_ends);
	UT_RUN(test_abort_detail_identifies_applied_before_local_accounting);
	UT_RUN(test_abort_detail_does_not_invent_a_shared_result_after_error);
	UT_RUN(test_abort_detail_keeps_both_ordinals_at_second_return_error);
	UT_RUN(test_abort_detail_refuses_stale_or_disabled_sample_identity);
	UT_RUN(test_native_after_apply_error_keeps_original_branch_cleanup_polarity);
	UT_RUN(test_exact_zero_later_ordinal_does_not_erase_first_apply);
	UT_RUN(test_exact_zero_cannot_overwrite_an_earlier_unknown_call);
	UT_RUN(test_exact_zero_all_ordinals_keep_unattempted_and_borrowed_owners);
	UT_RUN(test_exact_zero_releases_preflight_once_then_requalifies);
	UT_RUN(test_exact_zero_abort_after_lwlock_release_is_cleanup_not_panic);
	UT_RUN(test_exact_zero_never_accepts_unknown_drift_or_new_apply);
	UT_RUN(test_lifecycle_activation_refusal_reports_original_current_stage);
	UT_RUN(test_receipt_lifecycle_reports_original_shared_path_without_replay);
	UT_RUN(test_receipt_lifecycle_begin_unknown_and_cancel_do_not_borrow_shared_path);
	UT_RUN(test_receipt_consumer_later_apply_error_cannot_reuse_earlier_result);
	UT_RUN(test_receipt_consumer_observation_off_preserves_effects);
	UT_RUN(test_receipt_consumer_observation_survives_original_erasure);
	UT_RUN(test_receipt_consumer_observation_unsafe_drain_retains_exact_sample);
	UT_RUN(test_receipt_consumer_observation_two_targets_keep_borrowed_route);
	UT_RUN(test_receipt_consumer_observation_original_shared_refusal_is_not_success);
	UT_RUN(test_receipt_consumer_observation_shared_success_local_sync_failure_separate);
	UT_RUN(test_receipt_consumer_log_error_cannot_replay_or_change_pending_owner);
	UT_RUN(test_receipt_consumer_safe_flush_wiring_precedes_new_effect_and_follows_pending_flush);

	UT_RUN(test_receipt_lifecycle_shared_sample_uses_actual_publication_fields);
	UT_RUN(test_receipt_lifecycle_shared_sample_after_original_local_retirement);
	UT_RUN(test_receipt_lifecycle_unknown_shared_sample_does_not_borrow_expected_target);
	UT_RUN(test_receipt_lifecycle_sample_error_keeps_registered_cleanup_owner);

	UT_RUN(test_abort_prepared_after_lock_release_is_idempotent);
	UT_RUN(test_abort_prepared_cancel_failure_and_borrowed_owner_do_not_panic);
	UT_RUN(test_abort_attempted_or_applied_retains_debt_and_panics);
	UT_RUN(test_a148_undo_stop_original_active_writer_requires_original_commit_release);
	UT_RUN(test_a148_undo_stop_all_slots_and_late_invalid_do_not_clear_debt);
	UT_RUN(test_a148_undo_stop_does_not_confuse_extent_and_cursor_cache_with_writer);
	UT_RUN(test_allocated_nonempty_tt_is_not_fresh_supply);
	UT_RUN(test_absent_segment_selection_does_not_publish_a_file);
	UT_RUN(test_terminal_tt_only_sweep_preserves_actual_recycle_eligibility);
	UT_RUN(test_record_seal_owner_ignores_every_nonactive_predecessor);
	UT_RUN(test_history_consume_refuses_missing_metadata_before_install);
	UT_RUN(test_history_planned_uba_accounts_for_footer_capacity);
	UT_RUN(test_history_stage_syncs_both_receipts_and_freezes_after_apply);
	UT_RUN(test_history_stage_rejects_unowned_target_and_tampered_receipt);
	UT_RUN(test_history_codec_unaligned_complete_prior_and_logical_body);
	UT_RUN(test_history_codec_rejects_flag_length_magic_and_reserved_drift);
	UT_RUN(test_history_codec_rejects_forged_slot_identity_role_and_time);
	UT_RUN(test_history_codec_cross_page_update_requires_exactly_two_targets);
	UT_RUN(test_full_undo_pool_selects_exact_recyclable_supply_without_rewrite);
	UT_RUN(test_update_vm_observation_never_repins_or_indexes_a_local_buffer);

	UT_RUN(test_record_header_roundtrip);
	UT_RUN(test_insert_payload_roundtrip);
	UT_RUN(test_update_payload_roundtrip);
	UT_RUN(test_delete_payload_roundtrip);
	UT_RUN(test_itl_payload_roundtrip);
	UT_RUN(test_multi_record_block);
	UT_RUN(test_block_has_space_boundary_ok);
	UT_RUN(test_block_has_space_overflow);
	UT_RUN(test_slot_dir_addressing);
	UT_RUN(test_block_magic_init);
	UT_RUN(test_record_type_flags_bytes);
	UT_RUN(test_prev_uba_preserved);
	UT_RUN(test_undo_pool_observer_scans_all_256_slots_across_gaps);
	UT_RUN(test_undo_pool_observer_rejects_invalid_owner_and_null_output);
	UT_RUN(test_undo_pool_observer_distinguishes_io_failure_and_invalid_header);
	UT_RUN(test_undo_record_restart_reconstructs_and_dump_ensure_never_rescans);
	UT_RUN(test_undo_record_failed_lazy_scan_is_attempted_once);
	UT_RUN(test_undo_pool_create_increments_but_reuse_keeps_cardinality);
	UT_RUN(test_undo_effective_cap_clamps_and_never_falls_below_current);
	UT_RUN(test_record_allocator_owns_modifier_debt_outside_lifecycle_locks);
	UT_RUN(test_tt_rollover_publishes_current_before_binding_exposure);
	UT_RUN(test_recycle_releases_lifecycle_before_exact_current_transition);
	UT_RUN(test_extend_selects_reuse_without_mutating_block0);
	UT_RUN(test_reuse_wrapper_routes_only_through_exact_current_owner);
	UT_RUN(test_record_prepare_owns_extent_and_block0_slow_paths);
	UT_RUN(test_record_prepare_recovered_open_gate_precedes_segment_creation);
	UT_RUN(test_prepared_consumer_rechecks_exact_receipt_before_record_mutation);
	UT_RUN(test_prepared_consume_lock_precedes_first_receipt_apply);
	UT_RUN(test_terminal_census_precedes_final_receipt_recheck_and_itl_allocation);
	UT_RUN(test_ctrc_cross_page_update_requires_both_prepared_receipts);
	UT_RUN(test_heap_update_applies_required_receipts_before_undo_and_page_mutation);
	UT_RUN(test_ctrc_release_flag_terminal_recycle_reuse_and_redo_matrix);
	UT_RUN(test_heap_prepare_retries_transient_result_under_one_deadline);
	UT_RUN(test_inlock_consume_preserves_retry_required_for_heap_unwind);
	UT_RUN(test_all_heap_dml_callers_reprepare_outside_content_lock);
	UT_RUN(test_prepared_consumer_excludes_only_cluster_undo_slow_paths);
	UT_RUN(test_prepared_receipt_is_single_use_and_exactly_cancelable);
	UT_RUN(test_prepared_receipt_retains_exact_admission_until_close);
	UT_RUN(test_lifecycle_bitmap_update_preserves_concurrent_canonical_tt_slot);
	UT_RUN(test_record_segment_seal_preserves_concurrent_canonical_tt_slot);
	UT_RUN(test_record_drain_owner_stamps_only_after_gate);
	UT_RUN(test_live_lifecycle_writers_use_only_exact_block0_current);
	UT_RUN(test_ready_receipt_survives_prepare_deadline_through_real_consume);
	UT_RUN(test_native_prepared_undo_is_wal_backed_before_uba_publication);
	UT_RUN(test_native_prepared_undo_wal_error_retains_original_debt);
	UT_RUN(test_ready_preflight_crosses_old_deadline_while_taking_lock);
	UT_RUN(test_ready_contention_retries_without_cancel_or_new_deadline);
	UT_RUN(test_ready_identity_mismatch_is_not_hidden_by_lifetime_fix);
	UT_RUN(test_partial_apply_cancel_releases_only_unapplied_nonreuse_handle);
	UT_RUN(test_prepare_budget_remains_ten_seconds_without_refresh);
	UT_RUN(test_update_target_retry_keeps_ready_extent_deadline_and_owns_children);
	UT_RUN(test_update_target_retry_refuses_authority_and_publication_drift);
	UT_RUN(test_update_target_retry_retains_borrowed_and_failed_cancel_owners);
	UT_RUN(test_heap_retry_preserves_exact_ready_before_considering_cancel);
	UT_RUN(test_retry_requalification_keeps_ready_and_proves_actual_invalidation);
	UT_RUN(test_retry_after_apply_refuses_without_canceling_shared_owner);
	UT_RUN(test_retry_refusal_records_exact_cause_including_expiry_during_cancel);
	UT_RUN(test_cancel_before_snapshot_violation_counter_is_live);
	UT_RUN(test_update_toast_releases_outer_receipt_before_nested_producers);
	UT_RUN(test_update_itl_wait_returns_through_receipt_and_page_requalification);
	UT_RUN(test_ctrc_prepare_diagnostic_is_off_by_default_and_observation_only);
	UT_RUN(test_ctrc_prepare_reports_first_actual_predicate_and_available_identity);
	UT_RUN(test_ctrc_diagnostic_skips_critical_output_without_changing_decisions);

	UT_RUN(test_receipt_lifecycle_default_off_keeps_prepare_cancel_effects);
	UT_RUN(test_receipt_lifecycle_owner_publication_ready_are_ordered);
	UT_RUN(test_receipt_lifecycle_original_prepare_refusal_is_not_handle_or_receipt);
	UT_RUN(test_receipt_lifecycle_duplicate_keeps_original_acceptance);
	UT_RUN(test_receipt_lifecycle_sync_refusal_reports_original_cancel_return);
	UT_RUN(test_receipt_lifecycle_begin_log_error_has_no_unregistered_shared_effect);
	UT_RUN(test_receipt_lifecycle_ready_log_error_preserves_exact_cleanup_owner);
	UT_RUN(test_receipt_lifecycle_cancel_log_follows_complete_local_cleanup);
	UT_RUN(test_receipt_lifecycle_cancel_log_error_cannot_repeat_cleanup);
	UT_RUN(test_receipt_lifecycle_borrowed_or_applied_handle_is_not_cancelled);
	UT_RUN(test_receipt_lifecycle_failed_cancel_is_not_shared_retirement);
	UT_RUN(test_receipt_lifecycle_critical_and_held_lock_gaps_do_not_change_prepare);
	UT_RUN(test_receipt_lifecycle_two_targets_keep_distinct_publication_operations);
	UT_RUN(test_receipt_lifecycle_cancel_is_idempotent_and_does_not_read_shared_pointer);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
