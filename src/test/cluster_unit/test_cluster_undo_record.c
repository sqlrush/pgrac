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
static uint32 undo_test_wait_event_info;
static PGAlignedBlock undo_test_lifecycle_disk;
static bool undo_test_publish_tt_after_block0_read;
static TimestampTz receipt_clock_us;
static TimestampTz receipt_clock_after_lock;
static bool receipt_modifier_valid;
static bool receipt_block0_valid;
static bool receipt_block0_conditional_available;
static bool receipt_buffer_available;
static bool receipt_buffer_locked;
static int receipt_prepare_calls;
static int receipt_apply_calls;
static int receipt_cancel_calls;
static int receipt_leave_calls;
static int receipt_unref_calls;
static int receipt_install_calls;

char *DataDir = NULL;
bool cluster_enabled = false;
bool cluster_undo_gcs_coherence = false;
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

extern bool cluster_undo_record_ctrc_required_prepared(
	const ClusterUndoRecordPrepareReceipt *receipt, uint8 required_mask);

TimestampTz
GetCurrentTimestamp(void)
{
	return receipt_clock_us;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
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
{}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
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
errcode(int code pg_attribute_unused())
{
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
	UT_ASSERT_EQ(receipt_apply_calls, 1);
	UT_ASSERT_EQ(((const UndoBlockHeader *)image)->slot_count, 1);
	receipt_install_calls++;
}

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
	memset(handle, 0, sizeof(*handle));
	handle->valid = true;
	receipt_prepare_calls++;
	return CLUSTER_CTRC_PREPARE_READY;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(const ClusterCtrcReceiptHandle *handle,
								  const ClusterCtrcTargetV1 *target pg_attribute_unused(),
								  ClusterCtrcApplyToken *token)
{
	UT_ASSERT(handle->valid);
	memset(token, 0, sizeof(*token));
	receipt_apply_calls++;
	return CLUSTER_CTRC_APPLY_APPLIED;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_retarget_itl_shared(const ClusterCtrcReceiptHandle *handle,
										 const ClusterCtrcTargetV1 *pending pg_attribute_unused(),
										 const ClusterCtrcTargetV1 *target,
										 ClusterCtrcApplyToken *token)
{
	return cluster_ctrc_receipt_apply_shared(handle, target, token);
}

bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT(handle->valid);
	receipt_cancel_calls++;
	return true;
}


/*
 * The O4 tests below link the real record/allocator implementation, trimmed to
 * the functions reachable from this binary.  These stubs are the runtime
 * boundaries actually crossed by that observation slice; no product branch is
 * replaced.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
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
	return true;
}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							 uint32 segment_id, uint8 owner_instance,
							 uint32 block_no, char *buf)
{
	UndoSegmentHeaderData *disk
		= (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;

	if (segment_id != 1 || owner_instance != 1 || block_no != 0 || buf == NULL)
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
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							  uint32 segment_id, uint8 owner_instance,
							  uint32 block_no, const char *buf,
							  bool do_fsync pg_attribute_unused())
{
	if (segment_id != 1 || owner_instance != 1 || block_no != 0 || buf == NULL)
		return false;
	memcpy(undo_test_lifecycle_disk.data, buf, BLCKSZ);
	return true;
}

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
								 uint32 segment_id, uint8 owner_instance,
								 uint32 offset, const char *buf, uint32 len)
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

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms)
{
	return key != NULL && key->segment_id == 1 && key->owner_instance == 1
		&& timeout_ms > 0
		? CLUSTER_UNDO_BLOCK0_OK
		: CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_mutate_exact(
	const ClusterUndoBlock0LogicalKey *key,
	const ClusterUndoBlock0Generation *expected,
	const char predecessor_page[BLCKSZ],
	const char successor_page[BLCKSZ], int timeout_ms)
{
	PGAlignedBlock rebased;
	UndoSegmentHeaderData *current
		= (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	UndoSegmentHeaderData *next = (UndoSegmentHeaderData *)rebased.data;
	const UndoSegmentHeaderData *requested
		= (const UndoSegmentHeaderData *)successor_page;

	if (key == NULL || expected == NULL || !expected->known
		|| predecessor_page == NULL || successor_page == NULL
		|| timeout_ms <= 0 || key->segment_id != current->segment_id
		|| key->owner_instance != current->owner_instance
		|| expected->value != current->wrap_count)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	memcpy(rebased.data, successor_page, BLCKSZ);
	memcpy(next->tt_slots, current->tt_slots, sizeof(next->tt_slots));
	memcpy(undo_test_lifecycle_disk.data, rebased.data, BLCKSZ);
	return requested->wrap_count == expected->value
		? CLUSTER_UNDO_BLOCK0_OK
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

bool
cluster_undo_record_segment_drainable(
	const UndoSegmentHeaderData *hdr pg_attribute_unused(),
	ClusterUndoActiveBoundary boundary pg_attribute_unused(),
	bool any_unresolved_prepared pg_attribute_unused(),
	uint32 fixed_first_segment_id pg_attribute_unused(),
	uint32 active_record_segment_id pg_attribute_unused(),
	uint32 active_tt_segment_id pg_attribute_unused(),
	bool recovery_in_progress pg_attribute_unused())
{
	return false;
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
	if (undo_test_open_fail_path[0] != '\0'
		&& strcmp(fileName, undo_test_open_fail_path) == 0) {
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
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, NULL),
				 CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
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
	recheck = start == NULL
				  ? NULL
				  : strstr(start, "cluster_semantic_activation_modifier_recheck(");
	call_body = start == NULL ? NULL : strstr(start, "cluster_undo_record_alloc_body(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL
				? NULL
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
	mark_active = start == NULL ? NULL
		: strstr(start, "cluster_undo_segment_mark_active(");
	freeze_logical = mark_active == NULL ? NULL
		: strstr(mark_active, "logical.segment_id = new_segment_id;");
	unlock_for_current = freeze_logical == NULL ? NULL
		: strstr(freeze_logical,
			"LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);");
	ensure_current = unlock_for_current == NULL ? NULL
		: strstr(unlock_for_current,
			"cluster_undo_block0_current_live_owner_ensure_resident_exact(");
	relock = ensure_current == NULL ? NULL
		: strstr(ensure_current,
			"LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);");
	recheck_current = relock == NULL ? NULL
		: strstr(relock, "cluster_tt_slot_current_segment(node_id)");
	recheck_publication = recheck_current == NULL ? NULL
		: strstr(recheck_current,
			"cluster_undo_block0_current_live_owner_publication_recheck(");
	publish_binding = recheck_publication == NULL ? NULL
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
	if (start != NULL && end != NULL && mark_active != NULL
		&& freeze_logical != NULL
		&& unlock_for_current != NULL && ensure_current != NULL
		&& relock != NULL && recheck_current != NULL
		&& recheck_publication != NULL
		&& publish_binding != NULL)
		UT_ASSERT(start < mark_active && mark_active < freeze_logical
				  && freeze_logical < unlock_for_current
				  && unlock_for_current < ensure_current
				  && ensure_current < relock && relock < recheck_current
				  && recheck_current < recheck_publication
				  && recheck_publication < publish_binding
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
	acquire = start == NULL ? NULL
		: strstr(start,
			"LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);");
	epoch_fence = acquire == NULL ? NULL
		: strstr(acquire, "cluster_undo_horizon_epoch_fence_tripped(expected_epoch)");
	release = epoch_fence == NULL ? NULL
		: strstr(epoch_fence,
			"LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);");
	transition = release == NULL ? NULL
		: strstr(release, "cluster_undo_segment_try_mark_recyclable(");
	expected_arg = transition == NULL ? NULL
		: strstr(transition, "expected_epoch");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(acquire);
	UT_ASSERT_NOT_NULL(epoch_fence);
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(transition);
	UT_ASSERT_NOT_NULL(expected_arg);
	if (start != NULL && end != NULL && acquire != NULL
		&& epoch_fence != NULL && release != NULL && transition != NULL
		&& expected_arg != NULL)
		UT_ASSERT(start < acquire && acquire < epoch_fence
			&& epoch_fence < release && release < transition
			&& transition < expected_arg && expected_arg < end);
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
	end = start == NULL ? NULL
		: strstr(start, "\n\n/*\n * cluster_undo_segment_scan_max_existing");
	plan = start == NULL ? NULL : strstr(start, "ClusterUndoSegmentExtendPlan *plan");
	candidate = plan == NULL ? NULL : strstr(plan, "plan->needs_reuse = true;");
	forbidden_reuse = start == NULL ? NULL
		: strstr(start, "cluster_undo_segment_reuse_in_place(");

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
	end = start == NULL ? NULL
		: strstr(start, "\n\n/*\n * cluster_undo_segment_generation");
	fresh = start == NULL ? NULL
		: strstr(start, "cluster_undo_segment_make_header_bytes(");
	current_exact = fresh == NULL ? NULL
		: strstr(fresh, "cluster_undo_block0_current_live_owner_reuse_exact(");
	invalidate = current_exact == NULL ? NULL
		: strstr(current_exact, "cluster_undo_buf_invalidate_segment(");
	note = invalidate == NULL ? NULL
		: strstr(invalidate, "cluster_undo_record_note_segment_reuse();");
	raw_emit = start == NULL ? NULL
		: strstr(start, "cluster_undo_emit_segment_reuse(");
	raw_flush = start == NULL ? NULL : strstr(start, "XLogFlush(");
	raw_write = start == NULL ? NULL
		: strstr(start, "write_segment_header_via_smgr(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(fresh);
	UT_ASSERT_NOT_NULL(current_exact);
	UT_ASSERT_NOT_NULL(invalidate);
	UT_ASSERT_NOT_NULL(note);
	if (start != NULL && end != NULL && fresh != NULL && current_exact != NULL
		&& invalidate != NULL && note != NULL)
		UT_ASSERT(start < fresh && fresh < current_exact
			&& current_exact < invalidate && invalidate < note && note < end);
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
	prepare_end = prepare == NULL ? NULL
		: strstr(prepare, "\n}\n\n\nClusterUndoRecordConsumeResult\n");
	consume = prepare_end == NULL ? NULL
		: strstr(prepare_end, "\ncluster_undo_record_consume_prepared(");
	consume_end = consume == NULL ? NULL
		: strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	claim = prepare == NULL ? NULL : strstr(prepare, "claim_undo_extent(");
	ensure = prepare == NULL ? NULL
		: strstr(prepare, "cluster_undo_record_ensure_block0_current(");
	retention_guard = prepare == NULL ? NULL
		: strstr(prepare, "cluster_undo_active_write_register(");
	consume_claim = consume == NULL ? NULL : strstr(consume, "claim_undo_extent(");
	consume_ensure = consume == NULL ? NULL
		: strstr(consume,
			"cluster_undo_block0_current_live_owner_ensure_resident_exact(");
	consume_extend = consume == NULL ? NULL
		: strstr(consume, "cluster_undo_segment_extend_or_create(");
	consume_read = consume == NULL ? NULL
		: strstr(consume, "read_undo_block(");

	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(prepare_end);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(ensure);
	UT_ASSERT_NOT_NULL(retention_guard);
	if (prepare != NULL && prepare_end != NULL && claim != NULL && ensure != NULL
		&& retention_guard != NULL)
		UT_ASSERT(prepare < retention_guard && retention_guard < claim
				  && claim < ensure && ensure < prepare_end);
	UT_ASSERT(consume_claim == NULL || consume_claim >= consume_end);
	UT_ASSERT(consume_ensure == NULL || consume_ensure >= consume_end);
	UT_ASSERT(consume_extend == NULL || consume_extend >= consume_end);
	UT_ASSERT(consume_read == NULL || consume_read >= consume_end);
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
	preflight_end = preflight == NULL ? NULL
		: strstr(preflight,
			"\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_record_consume_prepared(");
	consume = preflight_end;
	consume_end = consume == NULL ? NULL
		: strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	recheck = preflight == NULL ? NULL
		: strstr(preflight,
			"cluster_undo_block0_current_live_owner_publication_recheck_conditional(");
	extent_match = preflight == NULL ? NULL
		: strstr(preflight, "cluster_undo_record_receipt_extent_matches(");
	first_mutation = consume == NULL ? NULL
		: strstr(consume, "cluster_scn_advance(");

	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(preflight_end);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(extent_match);
	UT_ASSERT_NOT_NULL(first_mutation);
	if (preflight != NULL && preflight_end != NULL && consume != NULL
		&& consume_end != NULL && recheck != NULL && extent_match != NULL
		&& first_mutation != NULL)
		UT_ASSERT(preflight < extent_match && extent_match < recheck
				  && recheck < preflight_end && preflight_end == consume
				  && consume < first_mutation && first_mutation < consume_end);
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

	if (record_source == NULL || heap_source == NULL)
	{
		free(record_source);
		free(heap_source);
		return;
	}
	preflight = strstr(record_source,
		"\ncluster_undo_record_consume_preflight(");
	preflight_end = preflight == NULL ? NULL
		: strstr(preflight,
			"\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_record_consume_prepared(");
	conditional_lock = preflight == NULL ? NULL
		: strstr(preflight, "cluster_undo_buf_lock_ref_conditional(");
	consume = preflight_end;
	consume_end = consume == NULL ? NULL
		: strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	locked_install = consume == NULL ? NULL
		: strstr(consume,
			"cluster_undo_record_install_prepared_resident_locked(");
	late_conditional = consume == NULL ? NULL
		: strstr(consume, "cluster_undo_buf_lock_ref_conditional(");

	boundary = strstr(heap_source, "/* One caller-owned final boundary.");
	boundary = boundary == NULL ? NULL
		: strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL
		: strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	boundary_preflight = boundary == NULL ? NULL
		: strstr(boundary, "cluster_undo_record_consume_preflight(");
	first_apply = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_apply_undo_target(");

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
	if (preflight != NULL && preflight_end != NULL
		&& conditional_lock != NULL)
		UT_ASSERT(preflight < conditional_lock
				  && conditional_lock < preflight_end);
	if (consume != NULL && consume_end != NULL && locked_install != NULL)
		UT_ASSERT(consume < locked_install && locked_install < consume_end);
	if (boundary != NULL && boundary_end != NULL
		&& boundary_preflight != NULL && first_apply != NULL)
		UT_ASSERT(boundary < boundary_preflight
				  && boundary_preflight < first_apply
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
	prepare_helper = strstr(source,
		"\ncluster_heap_itl_prepare_prepared_undo(");
	prepare_end = prepare_helper == NULL ? NULL
		: strstr(prepare_helper,
			"\n}\n\n\nstatic ClusterHeapPreparedUndoResult\ncluster_heap_itl_plan_prepared_undo_target(");
	ensure = prepare_helper == NULL ? NULL
		: strstr(prepare_helper,
			"cluster_heap_itl_ensure_capacity_with_terminal_census(");
	prepare_guard = prepare_helper == NULL ? NULL
		: strstr(prepare_helper, "cluster_heap_dml_authority_guard_capture(");
	prepare_receipt_recheck = prepare_helper == NULL ? NULL
		: strstr(prepare_helper, "cluster_undo_record_prepared_recheck(");

	plan_helper = strstr(source,
		"\ncluster_heap_itl_plan_prepared_undo_target(");
	plan_end = plan_helper == NULL ? NULL
		: strstr(plan_helper, "\n}\n\n\n#ifndef USE_CLUSTER_UNIT");
	plan_guard = plan_helper == NULL ? NULL
		: strstr(plan_helper, "cluster_heap_dml_authority_guard_capture(");
	plan_receipt_recheck = plan_helper == NULL ? NULL
		: strstr(plan_helper, "cluster_undo_record_prepared_recheck(");
	alloc_once = plan_helper == NULL ? NULL
		: strstr(plan_helper, "cluster_heap_itl_alloc_once(");
	bind_slot = plan_helper == NULL ? NULL
		: strstr(plan_helper, "cluster_heap_dml_authority_guard_bind_itl_slot(");
	plan_guard_recheck = bind_slot == NULL ? NULL
		: strstr(bind_slot, "cluster_heap_dml_authority_guard_recheck(");

	boundary = strstr(source, "/* One caller-owned final boundary.");
	boundary = boundary == NULL ? NULL
		: strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL
		: strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	boundary_recheck = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_undo_target_recheck(");
	boundary_apply = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_apply_undo_target(");
	consume = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_consume_undo(");
	typed_retry = strstr(source,
		"CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");
	prepare_retry_route = prepare_helper == NULL ? NULL
		: strstr(prepare_helper,
			"CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");
	update_retry_route = strstr(source,
		"old_capacity_result == CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY");

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
	if (prepare_helper != NULL && prepare_end != NULL && ensure != NULL
		&& prepare_guard != NULL && prepare_receipt_recheck != NULL)
		UT_ASSERT(prepare_helper < ensure && ensure < prepare_guard
				  && prepare_guard < prepare_receipt_recheck
				  && prepare_receipt_recheck < prepare_end);
	if (plan_helper != NULL && plan_end != NULL && plan_guard != NULL
		&& plan_receipt_recheck != NULL && alloc_once != NULL
		&& bind_slot != NULL && plan_guard_recheck != NULL)
		UT_ASSERT(plan_helper < plan_guard
				  && plan_guard < plan_receipt_recheck
				  && plan_receipt_recheck < alloc_once
				  && alloc_once < bind_slot
				  && bind_slot < plan_guard_recheck
				  && plan_guard_recheck < plan_end);
	if (boundary != NULL && boundary_end != NULL
		&& boundary_recheck != NULL && boundary_apply != NULL
		&& consume != NULL)
		UT_ASSERT(boundary < boundary_recheck
				  && boundary_recheck < boundary_apply
				  && boundary_apply < consume && consume < boundary_end);
	free(source);
}

/* A cross-page UPDATE owns two independent page-reference receipts.  The
 * pre-mutation gate must reject a partial pair, a missing exact handle, or a
 * receipt that has already crossed APPLY. */
UT_TEST(test_ctrc_cross_page_update_requires_both_prepared_receipts)
{
	ClusterUndoRecordPrepareReceipt receipt = {0};

	receipt.ctrc_pending_mask = UINT8_C(3);
	receipt.ctrc_prepared_mask = UINT8_C(3);
	receipt.ctrc_handles[0].valid = true;
	receipt.ctrc_handles[1].valid = true;
	UT_ASSERT(cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = UINT8_C(1);
	UT_ASSERT(cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = UINT8_C(4);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	receipt.ctrc_reuse_mask = 0;

	receipt.ctrc_prepared_mask = UINT8_C(1);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	receipt.ctrc_prepared_mask = UINT8_C(3);
	receipt.ctrc_handles[1].valid = false;
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	receipt.ctrc_handles[1].valid = true;
	receipt.ctrc_applied_mask = UINT8_C(1);
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(3)));
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, 0));
	UT_ASSERT(!cluster_undo_record_ctrc_required_prepared(
		&receipt, UINT8_C(4)));
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
	update_end = update == NULL ? NULL : strstr(update, "\nheap_lock_tuple(");
	stage = update == NULL ? NULL
		: strstr(update, "cluster_undo_record_ctrc_stage_pending(");
	reuse = update == NULL ? NULL
		: strstr(update, "cluster_heap_ctrc_stage_reusable_itl_receipt(");
	prepare = update == NULL ? NULL
		: strstr(update, "cluster_undo_record_ctrc_prepare_pending(");
	required = update == NULL ? NULL
		: strstr(update, "cluster_undo_record_ctrc_required_prepared(");
	unlock = reuse == NULL ? NULL
		: strstr(reuse, "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);");

	UT_ASSERT_NOT_NULL(update);
	UT_ASSERT_NOT_NULL(update_end);
	UT_ASSERT_NOT_NULL(stage);
	UT_ASSERT_NOT_NULL(reuse);
	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(required);
	UT_ASSERT_NOT_NULL(unlock);
	if (update != NULL && update_end != NULL && stage != NULL && reuse != NULL
		&& prepare != NULL && required != NULL && unlock != NULL)
		UT_ASSERT(update < stage && stage < reuse && reuse < required
			&& required < unlock && unlock < prepare && prepare < update_end);
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

	if (xlog_header == NULL || xlog_source == NULL || format_header == NULL)
	{
		free(xlog_header);
		free(xlog_source);
		free(format_header);
		return;
	}
	UT_ASSERT_NOT_NULL(strstr(xlog_header,
		"XLOG_UNDO_TT_SLOT_CTRC_RELEASE"));
	UT_ASSERT_NOT_NULL(strstr(xlog_header,
		"xl_undo_tt_slot_ctrc_release_v1"));
	UT_ASSERT_NOT_NULL(strstr(xlog_source,
		"cluster_undo_xlog_insert_tt_ctrc_release"));
	UT_ASSERT_NOT_NULL(strstr(xlog_source,
		"cluster_tt_durable_redo_ctrc_release_slot_exact"));
	UT_ASSERT_NOT_NULL(strstr(format_header,
		"TT_SLOT_FLAG_CTRC_RELEASE_PROVEN"));
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
	helper_end = helper == NULL ? NULL
		: strstr(helper, "\n}\n\ntypedef enum ClusterHeapPreparedUndoResult");
	deadline = helper == NULL ? NULL
		: strstr(helper, "uint64 absolute_deadline_us");
	prepare_call = helper == NULL ? NULL
		: strstr(helper, "cluster_heap_prepare_undo_record_call(");
	production_alias = strstr(source,
		"#define cluster_heap_prepare_undo_record_call cluster_undo_record_prepare");
	ready = helper == NULL ? NULL
		: strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_READY");
	retry = helper == NULL ? NULL
		: strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED");
	refused = helper == NULL ? NULL
		: strstr(helper, "CLUSTER_UNDO_RECORD_PREPARE_REFUSED");
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
	if (helper != NULL && helper_end != NULL && deadline != NULL
		&& prepare_call != NULL && ready != NULL && retry != NULL
		&& refused != NULL && interrupts != NULL)
		UT_ASSERT(helper < deadline && deadline < prepare_call
				  && prepare_call < ready && ready < retry && retry < refused
				  && refused < interrupts && interrupts < helper_end);

	for (hit = source;
		 (hit = strstr(hit, "cluster_heap_prepare_undo_record_exact(")) != NULL;
		 hit++)
		helper_mentions++;
	for (hit = source;
		 (hit = strstr(hit, "cluster_heap_prepare_undo_record_call(")) != NULL;
		 hit++)
		wrapper_calls++;
	for (hit = source;
		 (hit = strstr(hit, "cluster_undo_record_prepare(")) != NULL;
		 hit++)
		raw_prepare_calls++;
	/* Definition + retry wrapper + unit seam + five initial producer sites.
	 * Existing retry sites now preserve READY through their dedicated owner. */
	UT_ASSERT_EQ(helper_mentions, 9); /* Includes explicit post-TOAST resume. */
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
	plan_end = plan == NULL ? NULL
		: strstr(plan, "\n}\n\n\n#ifndef USE_CLUSTER_UNIT");
	recheck = plan == NULL ? NULL
		: strstr(plan, "cluster_undo_record_prepared_recheck(");
	alloc_once = plan == NULL ? NULL
		: strstr(plan, "cluster_heap_itl_alloc_once(");
	retry_result = plan == NULL ? NULL
		: strstr(plan, "CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED");
	refused_result = plan == NULL ? NULL
		: strstr(plan, "CLUSTER_HEAP_PREPARED_UNDO_REFUSED");

	boundary = strstr(source, "/* One caller-owned final boundary.");
	boundary = boundary == NULL ? NULL
		: strstr(boundary, "\ncluster_heap_no_retry_boundary_apply(");
	boundary_end = boundary == NULL ? NULL
		: strstr(boundary, "\n}\n\n\n#ifdef USE_CLUSTER_UNIT");
	preflight = boundary == NULL ? NULL
		: strstr(boundary, "cluster_undo_record_consume_preflight(");
	apply = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_apply_undo_target(");
	consume = boundary == NULL ? NULL
		: strstr(boundary, "cluster_heap_boundary_consume_undo(");
	zero_apply_result = boundary == NULL ? NULL
		: strstr(boundary, "CLUSTER_HEAP_BOUNDARY_ZERO_APPLY_RETRY");

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
	if (plan != NULL && plan_end != NULL && recheck != NULL
		&& alloc_once != NULL && retry_result != NULL && refused_result != NULL)
		UT_ASSERT(plan < recheck && recheck < alloc_once
				  && retry_result < plan_end && refused_result < plan_end);
	if (boundary != NULL && boundary_end != NULL && preflight != NULL
		&& apply != NULL && consume != NULL && zero_apply_result != NULL)
		UT_ASSERT(boundary < zero_apply_result
				  && zero_apply_result < preflight && preflight < apply
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
	helper_end = helper == NULL ? NULL
		: strstr(helper, "\n}\n\ntypedef enum ClusterHeapPreparedUndoResult");
	deadline_parameter = helper == NULL ? NULL
		: strstr(helper, "uint64 absolute_deadline_us");

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(deadline_parameter);
	if (helper != NULL && helper_end != NULL && deadline_parameter != NULL)
	{
		UT_ASSERT(deadline_parameter < helper_end);
		UT_ASSERT(strstr(helper,
			"cluster_undo_record_prepare_deadline_us()") == NULL
			|| strstr(helper, "cluster_undo_record_prepare_deadline_us()")
				>= helper_end);
	}

	for (hit = source;
		 (hit = strstr(hit,
			"cluster_undo_record_ctrc_prepare_pending(")) != NULL;
		 hit++)
		ctrc_prepare_sites++;
	for (hit = source;
		 (hit = strstr(hit, "cluster_undo_record_cancel_prepared(")) != NULL;
		 hit++)
		cancel_sites++;
	for (hit = source;
		 (hit = strstr(hit, "undo_prepare_deadline_us")) != NULL;
		 hit++)
		deadline_locals++;

	/* Each heap DML family prepares its CTRC receipt only after dropping the
	 * content lock.  Do not count enum/result spellings: UPDATE legitimately
	 * has more than one pre-mutation retry classification. */
	UT_ASSERT_EQ(ctrc_prepare_sites, 5);
	UT_ASSERT_EQ(cancel_sites, 3); /* Two chain exits and the explicit TOAST handoff. */
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
	consume = preflight == NULL ? NULL
		: strstr(preflight, "\ncluster_undo_record_consume_prepared(");
	consume_end = consume == NULL ? NULL
		: strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");
	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(consume_end);
	if (preflight != NULL && consume != NULL && consume_end != NULL)
	{
		for (i = 0; i < lengthof(forbidden); i++)
		{
			const char *hit = strstr(preflight, forbidden[i]);

			UT_ASSERT(hit == NULL || hit >= consume_end);
		}
		UT_ASSERT_NOT_NULL(strstr(preflight,
			"cluster_undo_buf_lock_ref_conditional("));
		UT_ASSERT_NOT_NULL(strstr(consume,
			"cluster_undo_record_install_prepared_resident_locked("));
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
	sequence_publish = prepare == NULL ? NULL
		: strstr(prepare, "receipt->reservation_sequence =");
	cancel_match = cancel == NULL ? NULL
		: strstr(cancel, "reservation_sequence");
	consume_invalidate = consume == NULL ? NULL
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
	cancel_end = cancel == NULL ? NULL
		: strstr(cancel,
			"\n}\n\n\nClusterUndoRecordConsumePreflightResult\ncluster_undo_record_consume_preflight(");
	preflight = cancel_end;
	preflight_end = preflight == NULL ? NULL
		: strstr(preflight,
			"\n}\n\n\nClusterUndoRecordConsumeResult\ncluster_undo_record_consume_prepared(");
	consume = preflight_end;
	consume_end = consume == NULL ? NULL
		: strstr(consume, "\n}\n\n\nUBA\ncluster_undo_record_alloc(");

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
	if (prepare != NULL && prepare_end != NULL)
	{
		const char *enter = strstr(prepare,
			"cluster_semantic_activation_modifier_enter(");
		const char *freeze = strstr(prepare, "modifier_admission");

		UT_ASSERT(enter != NULL && enter < prepare_end);
		UT_ASSERT(freeze != NULL && freeze < prepare_end);
	}
	if (recheck != NULL && recheck_end != NULL)
	{
		const char *exact = strstr(recheck,
			"cluster_semantic_activation_modifier_recheck(");

		UT_ASSERT(exact != NULL && exact < recheck_end);
		UT_ASSERT(strstr(recheck, "cluster_undo_record_prepare_deadline_us(")
			== NULL);
	}
	if (cancel != NULL && cancel_end != NULL)
	{
		const char *leave = strstr(cancel,
			"cluster_semantic_activation_leave(");

		UT_ASSERT(leave != NULL && leave < cancel_end);
	}
	if (preflight != NULL && preflight_end != NULL)
	{
		const char *exact = strstr(preflight,
			"cluster_semantic_activation_modifier_recheck(");

		UT_ASSERT(exact != NULL && exact < preflight_end);
		UT_ASSERT(strstr(preflight,
			"cluster_undo_record_prepare_deadline_us(") == NULL);
	}
	if (consume != NULL && consume_end != NULL)
	{
		const char *leave = strstr(consume,
			"cluster_semantic_activation_leave(");
		const char *enter = strstr(consume,
			"cluster_semantic_activation_modifier_enter(");

		UT_ASSERT(leave != NULL && leave < consume_end);
		UT_ASSERT(enter == NULL || enter >= consume_end);
		UT_ASSERT(strstr(consume, "cluster_undo_record_prepare_deadline_us(")
			== NULL);
	}
	free(source);
}

/* A lifecycle RMW may have read block zero just before a canonical publisher
 * installs a different TT slot.  The lifecycle update owns only its metadata
 * bytes; applying it must preserve that concurrent canonical slot. */
UT_TEST(test_lifecycle_bitmap_update_preserves_concurrent_canonical_tt_slot)
{
	UndoSegmentHeaderData *disk
		= (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	TTSlot expected;

	undo_test_make_header(1, 1, SEGMENT_ACTIVE,
		undo_test_lifecycle_disk.data);
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
	UndoSegmentHeaderData *disk
		= (UndoSegmentHeaderData *)undo_test_lifecycle_disk.data;
	TTSlot expected;

	undo_test_reset_record_shmem();
	undo_test_make_header(1, 1, SEGMENT_COMMITTED,
		undo_test_lifecycle_disk.data);
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

	UT_ASSERT(strstr(alloc_source,
		"write_segment_lifecycle_header_via_smgr") == NULL);
	UT_ASSERT(strstr(record_source,
		"cluster_undo_write_record_lifecycle_header") == NULL);
	UT_ASSERT_NOT_NULL(strstr(alloc_source,
		"cluster_undo_block0_current_live_owner_lifecycle_exact("));
	UT_ASSERT_NOT_NULL(strstr(record_source,
		"cluster_undo_block0_current_live_owner_mutate_exact("));
	helper = strstr(alloc_source,
		"cluster_undo_block0_current_live_owner_lifecycle_exact(");
	ensure_resident = helper == NULL ? NULL : strstr(helper,
		"cluster_undo_block0_current_live_owner_ensure_resident(");
	mutate_exact = helper == NULL ? NULL : strstr(helper,
		"cluster_undo_block0_current_live_owner_mutate_exact(");
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
	MyBackendId = 1;
	receipt_clock_us = 99;
	receipt_clock_after_lock = 0;
	receipt_modifier_valid = true;
	receipt_block0_valid = true;
	receipt_block0_conditional_available = true;
	receipt_buffer_available = true;
	receipt_buffer_locked = false;
	receipt_prepare_calls = receipt_apply_calls = receipt_cancel_calls = 0;
	receipt_leave_calls = receipt_unref_calls = receipt_install_calls = 0;
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
	receipt->extent = cluster_undo_current_extent;
	receipt->modifier_admission.entered = true;
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
		UBA uba;
		ClusterUndoRecordConsumePreflightResult preflight;

		receipt_fixture_ready(types[i], &receipt);
		receipt.ctrc_pending_mask = 1;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
		receipt_clock_us = 101;
		UT_ASSERT(cluster_undo_record_prepared_recheck(&receipt, sizeof(payload)));
		UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, 0));
		UT_ASSERT_EQ(receipt_prepare_calls, 1);
		preflight = cluster_undo_record_consume_preflight(&receipt, sizeof(payload));
		UT_ASSERT_EQ(preflight, CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY);
		if (preflight != CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY) {
			cluster_undo_record_cancel_prepared(&receipt);
			continue;
		}
		UT_ASSERT_EQ(cluster_undo_record_ctrc_apply_prepared(&receipt, 0, &final_target, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		UT_ASSERT_EQ(
			cluster_undo_record_consume_prepared(&receipt, &target, payload, sizeof(payload), &uba),
			CLUSTER_UNDO_RECORD_CONSUME_APPLIED);
		UT_ASSERT(!UBA_is_invalid(uba));
		UT_ASSERT_EQ(receipt_install_calls, 1);
		UT_ASSERT_EQ(receipt_leave_calls, 1);
		UT_ASSERT_EQ(receipt_unref_calls, 0); /* Reference transferred to pending WAL. */
		UT_ASSERT_EQ(memcmp(&receipt, &zero, sizeof(receipt)), 0);
		UT_ASSERT_EQ(memcmp(&cluster_undo_record_reservation.receipt, &zero, sizeof(zero)), 0);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT(!receipt_buffer_locked);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT_EQ(receipt_cancel_calls, 0); /* Shared APPLIED ownership survives. */
	}
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
		receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
		receipt.ctrc_pending_mask = receipt.ctrc_prepared_mask = 3;
		receipt.ctrc_reuse_mask = reuse ? 2 : 0;
		receipt.ctrc_handles[0].valid = receipt.ctrc_handles[1].valid = true;
		UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
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
	/* The one direct outer-receipt cancel is the explicit nested TOAST
	 * handoff, independently checked below; retries must use requalification. */
	{
		const char *cancel = strstr(source, "cluster_undo_record_cancel_prepared(&undo_receipt)");
		UT_ASSERT_NOT_NULL(cancel);
		if (cancel != NULL)
			UT_ASSERT(strstr(cancel + 1, "cluster_undo_record_cancel_prepared(&undo_receipt)")
					  == NULL);
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
	resume = toast == NULL ? NULL : strstr(toast, "cluster_heap_prepare_undo_record_exact(");
	relock = toast == NULL ? NULL : strstr(toast, "l_pgrac_reacquire:");
	UT_ASSERT(update != NULL && release != NULL && toast != NULL && release < toast);
	UT_ASSERT(resume != NULL && relock != NULL && toast < resume && resume < relock);
	if (resume != NULL && relock != NULL) {
		const char *original_budget = strstr(resume, "undo_prepare_deadline_us");
		UT_ASSERT(original_budget != NULL && original_budget < relock);
	}
	free(source);
}

int
main(int argc, char **argv)
{
	UT_PLAN(51);

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
	UT_RUN(test_live_lifecycle_writers_use_only_exact_block0_current);
	UT_RUN(test_ready_receipt_survives_prepare_deadline_through_real_consume);
	UT_RUN(test_ready_preflight_crosses_old_deadline_while_taking_lock);
	UT_RUN(test_ready_contention_retries_without_cancel_or_new_deadline);
	UT_RUN(test_ready_identity_mismatch_is_not_hidden_by_lifetime_fix);
	UT_RUN(test_partial_apply_cancel_releases_only_unapplied_nonreuse_handle);
	UT_RUN(test_prepare_budget_remains_ten_seconds_without_refresh);
	UT_RUN(test_heap_retry_preserves_exact_ready_before_considering_cancel);
	UT_RUN(test_retry_requalification_keeps_ready_and_proves_actual_invalidation);
	UT_RUN(test_retry_after_apply_refuses_without_canceling_shared_owner);
	UT_RUN(test_cancel_before_snapshot_violation_counter_is_live);
	UT_RUN(test_update_toast_releases_outer_receipt_before_nested_producers);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
