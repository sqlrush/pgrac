/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_visible_version.c
 *    Native visible-bit publication records both exact page versions.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_visible_version.c
 *
 * NOTES
 *    PGRAC: actual setter/logger and version helpers, with buffer and WAL
 *    I/O fixtures. This does not simulate a visibility proof or recovery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/heapam_xlog.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/rel.h"
#include "unit_test.h"
#include "cluster/cluster_mode.h"
#include "access/visibilitymap.h"
#include "test_cluster_heap_maintenance_policy.inc"

UT_DEFINE_GLOBALS();

/* Online page/version fixtures use TARGET_READY; recovered admission is
 * controlled separately and is never granted by these storage leaves. */
BackendType MyBackendType = B_BACKEND;
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_TARGET_READY;
}
bool
cluster_semantic_activation_phase1_pristine(void)
{
	return false;
}
bool
cluster_semantic_activation_recovered_open_current(void)
{
	return false;
}


#if defined(USE_ASSERT_CHECKING) && !defined(FIXTURE_NO_ASSERTS)
/* Preserve assertion failure while allowing directives inside the native call
 * argument: the standalone analyzer cannot expand those as macro arguments. */
static void
fixture_assert(bool condition)
{
	if (!condition)
		ExceptionalCondition("native VM assertion", __FILE__, __LINE__);
}
#undef Assert
#define Assert fixture_assert
#endif
#ifdef FIXTURE_NO_ASSERTS
#undef Assert
#define Assert(condition) ((void)0)
#endif

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / 4)
#define HEAPBLK_TO_OFFSET(x) (((x) % 4) * 2)

bool cluster_enabled = true, cluster_shared_config = true, InRecovery, wal_log_hints;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA, NBuffers = 2, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[2], before[2], images[2];
static RelationData relation;
static FormData_pg_class relform;
static ClusterSpaceIdentity identity;
static RfPageVersionEdgeEntryV1 edges[2];
static xl_heap_visible published_visible_payload;
static uint64 next_token, edge_token;
static unsigned edge_count, inserts, dirty[2], registered[2], lock_calls;
static unsigned assertion_failures;
static RelFileLocator fixture_locators[2];
static ForkNumber fixture_forks[2];
static BlockNumber fixture_blocks[2];
static TransactionId expected_cutoff;
static int nonpermanent_buffer;
static uint8 register_flags[2];
static bool begun, expecting_error, checksums, repairing, repairing_vm_only;
static jmp_buf error_jump;

/* Empty-page entry uses actual publication/scan functions, with only the
 * buffer/current/transport scheduling leaves controlled here. */
static bool empty_fixture, held[2], pinned[2], writer[2];
static bool empty_vm_writer, empty_heap_writer, empty_resume, empty_aux;
static int empty_race, empty_vm_race, empty_repin_misses;
static unsigned empty_aux_calls, empty_resumes, empty_heap_waits, empty_cleanup_waits;
static unsigned empty_repins, empty_local_unlocks, empty_fsm;
static bool empty_error_after_resume, empty_error_at_capture, empty_cancel_repin;
static bool empty_cancel_after_refusal, empty_refusal_keeps_pin, empty_cancel_after_capture;
static bool empty_cancel_caught;
static unsigned empty_pin_calls;
static Buffer empty_cancel_vm_handle;
static bool empty_identity_available;
static uint64 empty_identity_generation;
static unsigned empty_identity_captures, empty_identity_checks;
static int empty_identity_change_phase;
static uint64 empty_publication_metrics[PCM_VM_METRIC_COUNT];
static unsigned empty_publication_notes;
/* Deferred repair owns no heap pin/lock; the lower X service may drop its
 * only VM pin before returning a clean reobservation result. */
static bool repair_retry_fixture, repair_pinned, repair_locked, repair_aux_driving;
static bool repair_retain_pin, repair_clear_on_repin, repair_cancel, repair_caught;
static unsigned repair_refusals, repair_attempts, repair_pin_calls, repair_unpins;
static unsigned repair_error_at;
static Buffer repair_vm_handle;
static ResourceXAuxiliaryAcquireContext *repair_context;
static PGAlignedBlock repair_current;
/* Two-page repair retains its original heap pin across a content-lock
 * handoff; no VM pin may survive a resumed heap-X acquisition. */
static bool pair_fixture, pair_retain_pin, pair_aux_driving, pair_caught;
static bool pair_cancel_refusal, pair_cancel_miss, pair_peer_change;
static unsigned pair_refusals, pair_attempts, pair_pin_calls, pair_unpins;
static unsigned pair_repins, pair_repin_misses, pair_heap_waits, pair_heap_unlocks;
static unsigned pair_warm_success, pair_nested_success, pair_error_at, pair_reobserves;
static Buffer pair_vm_handle;
static ResourceXAuxiliaryAcquireContext *pair_context;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;
void
ProcessInterrupts(void)
{
	if (pair_fixture) {
		UT_ASSERT(pinned[0] && !held[0] && !held[1] && !pinned[1]);
		UT_ASSERT(pair_cancel_refusal || pair_cancel_miss);
		InterruptPending = false;
		elog(ERROR, "controlled cancellation at unlocked two-page repair retry");
	}
	if (repair_retry_fixture) {
		UT_ASSERT(repair_cancel && !repair_pinned && !repair_locked);
		InterruptPending = false;
		elog(ERROR, "controlled cancellation at unpinned VM repair retry");
	}
	UT_ASSERT(empty_fixture && !held[0] && !held[1] && !pinned[1]);
	UT_ASSERT(empty_cancel_repin || empty_cancel_after_refusal || empty_cancel_after_capture);
	InterruptPending = false;
	elog(ERROR, "controlled cancellation at unpinned VM retry");
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	UT_ASSERT(expecting_error);
	longjmp(error_jump, 1);
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	if (expecting_error) {
		assertion_failures++;
		longjmp(error_jump, 1);
	}
	printf("# assertion %s at %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errhint(const char *format, ...)
{
	(void)format;
	return 0;
}

int
errcode(int code)
{
	(void)code;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (!expecting_error) {
		printf("# unexpected error %s:%d %s\n", file, line, function);
		abort();
	}
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	longjmp(error_jump, 1);
}
bool
RecoveryInProgress(void)
{
	return InRecovery;
}
bool
DataChecksumsEnabled(void)
{
	return checksums;
}
bool
IsCatalogRelation(Relation rel)
{
	UT_ASSERT(rel == &relation);
	return false;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	(void)out;
	abort();
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT_EQ(locator.dbOid, 5);
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return 1;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	*locator = fixture_locators[buffer - 1];
	*forknum = fixture_forks[buffer - 1];
	*block = fixture_blocks[buffer - 1];
}

void
cluster_pcm_vm_metric_note(const BufferTag *tag, PcmVmMetric metric)
{
	BufferTag expected;

	UT_ASSERT(empty_fixture && held[0] && held[1] && writer[0] && writer[1]);
	UT_ASSERT(pinned[0] && pinned[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	InitBufferTag(&expected, &relation.rd_locator, VISIBILITYMAP_FORKNUM, 0);
	UT_ASSERT(BufferTagsEqual(tag, &expected));
	UT_ASSERT((unsigned)metric < PCM_VM_METRIC_COUNT);
	empty_publication_metrics[metric]++;
	empty_publication_notes++;
}

/* The parent has no outcome keys or notes. Keep its original functions intact
 * and expose that absence as a zero diagnostic count, rather than a skip. */
static uint64
empty_publication_metric(const char *key)
{
#define FIXTURE_VM_KEY(id, name) name,
	static const char *const keys[] = { PCM_VM_METRICS(FIXTURE_VM_KEY) };
#undef FIXTURE_VM_KEY

	for (unsigned i = 0; i < lengthof(keys); i++)
		if (strcmp(keys[i], key) == 0)
			return empty_publication_metrics[i];
	return 0;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	return fixture_blocks[buffer - 1];
}
bool
BufferIsPermanent(Buffer buffer)
{
	return (buffer == 1 || buffer == 2) && buffer != nonpermanent_buffer;
}
bool
visibilitymap_pin_ok(BlockNumber block, Buffer buffer)
{
	return block == 0 && buffer == 2;
}
/* Model a real insertion, including a used LP and tuple storage. A zeroed
 * extra line pointer is LP_UNUSED and cannot represent an inserted row. */
static void
empty_insert_row_at(OffsetNumber offset)
{
	Page page = pages[0].data;
	PageHeader header = (PageHeader)page;
	HeapTupleHeader tuple;
	Size lower = SizeOfPageHeaderData + offset * sizeof(ItemIdData);

	if (header->pd_lower < lower)
		header->pd_lower = lower;
	header->pd_upper = header->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(PageGetItemId(page, offset), header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)(page + header->pd_upper);
	memset(tuple, 0, SizeofHeapTupleHeader);
	tuple->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(tuple, 17);
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (pair_fixture) {
		UT_ASSERT(buffer == 1 || buffer == 2);
		UT_ASSERT_EQ(CritSectionCount, 0);
		if (mode == BUFFER_LOCK_UNLOCK) {
			UT_ASSERT(held[buffer - 1]);
			held[buffer - 1] = writer[buffer - 1] = false;
			if (buffer == 1) {
				pair_heap_unlocks++;
				if (pair_peer_change) {
					((PageHeader)pages[0].data)->pd_block_scn = 60;
					((PageHeader)pages[1].data)->pd_block_scn = 70;
					PageSetAllVisible(pages[0].data);
					pages[0].data[BLCKSZ - 1] = 51;
					memcpy(before, pages, sizeof(pages));
					pair_peer_change = false;
				}
			}
		} else {
			UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
			UT_ASSERT(pinned[buffer - 1] && !held[buffer - 1]);
			if (buffer == 1) {
				UT_ASSERT(!held[1] && !pinned[1]);
				pair_heap_waits++;
			} else if (!pair_aux_driving && pair_refusals != 0) {
				/* The ordinary nested API reports a barrier refusal as an
				 * error rather than returning a replaceable handle. */
				elog(ERROR, "ordinary nested VM lock cannot return a clean refusal");
			}
			held[buffer - 1] = writer[buffer - 1] = true;
		}
		return;
	}
	if (repair_retry_fixture) {
		UT_ASSERT_EQ(buffer, 2);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!held[0] && !pinned[0]);
		if (mode == BUFFER_LOCK_UNLOCK) {
			UT_ASSERT(repair_locked && repair_pinned);
			repair_locked = false;
		} else {
			UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
			UT_ASSERT(repair_pinned && !repair_locked);
			/* The ordinary API cannot return the replaced handle to its
			 * owner. Model its existing pre-mutation error boundary. */
			if (!repair_aux_driving && repair_refusals != 0) {
				ReleaseBuffer(buffer);
				fixture_locators[1].relNumber++;
				elog(ERROR, "ordinary VM lock cannot return a replaced pin");
			}
			repair_locked = true;
		}
		lock_calls++;
		return;
	}
	if (empty_fixture) {
		UT_ASSERT(buffer == 1 || buffer == 2);
		if (cluster_shared_config)
			UT_ASSERT_EQ(CritSectionCount, 0);
		if (mode == BUFFER_LOCK_UNLOCK) {
			UT_ASSERT(held[buffer - 1]);
			held[buffer - 1] = writer[buffer - 1] = false;
		} else {
			UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
			UT_ASSERT(pinned[buffer - 1] && !held[buffer - 1]);
			if (cluster_shared_config)
				UT_ASSERT(!held[buffer == 1 ? 1 : 0]);
			if (buffer == 1) {
				if (cluster_shared_config)
					UT_ASSERT(!pinned[1]);
				empty_heap_waits++;
				if (empty_identity_change_phase == 2)
					empty_identity_generation++;
				if (empty_race == 1)
					empty_insert_row_at(FirstOffsetNumber);
				else if (empty_race == 2)
					memset(pages[0].data, 0, BLCKSZ);
				empty_race = 0;
			}
			held[buffer - 1] = writer[buffer - 1] = true;
		}
		return;
	}
	UT_ASSERT_EQ(buffer, 2);
	UT_ASSERT(mode == BUFFER_LOCK_EXCLUSIVE || mode == BUFFER_LOCK_UNLOCK);
	if (cluster_shared_config)
		UT_ASSERT_EQ(CritSectionCount, 0);
	lock_calls++;
}
XLogRecPtr
log_newpage_buffer(Buffer buffer, bool standard)
{
	(void)buffer;
	(void)standard;
	abort(); /* Shared initialization has its own record, not a new FPI here. */
}
SCN
cluster_scn_advance(void)
{
	if (pair_fixture)
		UT_ASSERT(held[0] && held[1] && pinned[0] && pinned[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	return ++next_token;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	if (cluster_shared_config || !repairing)
		UT_ASSERT(CritSectionCount > 0);
	dirty[buffer - 1]++;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun && CritSectionCount > 0);
	begun = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(length, SizeOfHeapVisible);
	UT_ASSERT_EQ(((xl_heap_visible *)data)->snapshotConflictHorizon, expected_cutoff);
	memcpy(&published_visible_payload, data, SizeOfHeapVisible);
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun && id < 2);
	UT_ASSERT_EQ(buffer, id == 0 ? 2 : 1);
	registered[id]++;
	register_flags[id] = flags;
	images[id] = pages[buffer - 1];
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(count, repairing_vm_only ? 1 : 2);
	memcpy(edges, entries, count * sizeof(*edges));
	edge_count = count;
	edge_token = token;
}
static XLogRecPtr insert_end_lsn = UINT64_C(0x9000);

XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun && dirty[1] > 0);
	if (cluster_shared_config && !repairing_vm_only)
		UT_ASSERT(dirty[0] > 0);
	UT_ASSERT_EQ(rmgr, repairing ? RM_XLOG_ID : RM_HEAP2_ID);
	UT_ASSERT_EQ(info, repairing ? XLOG_FPI : XLOG_HEAP2_VISIBLE);
	begun = false;
	inserts++;
	return insert_end_lsn;
}

/* These storage leaves must stay unreachable with the original exact pin. */
static Buffer
vm_readbuf(Relation rel, BlockNumber block, bool extend)
{
	(void)rel;
	(void)block;
	(void)extend;
	abort();
}

void
ReleaseBuffer(Buffer buffer)
{
	if (pair_fixture) {
		UT_ASSERT(buffer == 1 || buffer == 2);
		UT_ASSERT(pinned[buffer - 1] && !held[buffer - 1]);
		pinned[buffer - 1] = false;
		if (buffer == 2) {
			pair_unpins++;
			pair_vm_handle = InvalidBuffer;
		}
		return;
	}
	if (repair_retry_fixture) {
		UT_ASSERT_EQ(buffer, 2);
		UT_ASSERT(repair_pinned && !repair_locked);
		repair_pinned = false;
		repair_unpins++;
		return;
	}
	UT_ASSERT(empty_fixture && (buffer == 1 || buffer == 2));
	UT_ASSERT(pinned[buffer - 1] && !held[buffer - 1]);
	pinned[buffer - 1] = false;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

void
LockBufferForCleanup(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	empty_cleanup_waits++;
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
}

bool
ClusterLockBufferExclusiveAuxiliaryAware(Buffer *buffer, ResourceXAuxiliaryAcquireContext *context)
{
	if (pair_fixture) {
		UT_ASSERT(context != NULL && pinned[0] && pinned[1] && !held[1]);
		UT_ASSERT_EQ(*buffer, 2);
		UT_ASSERT_EQ(CritSectionCount, 0);
		pair_attempts++;
		if (!context->active) {
			UT_ASSERT_EQ(pair_reobserves, 0);
			pair_context = context;
			context->active = true;
			context->absolute_deadline_us = 81;
			context->diagnostic_request_sequence = 91;
		} else {
			UT_ASSERT(context == pair_context);
			UT_ASSERT_EQ(context->absolute_deadline_us, 81);
			UT_ASSERT_EQ(context->diagnostic_request_sequence, 91);
			UT_ASSERT_EQ(context->reobserve_count, pair_reobserves);
		}
		if (pair_attempts == pair_error_at)
			elog(ERROR, "controlled two-page repair acquisition failure");
		if (pair_refusals != 0) {
			pair_refusals--;
			context->reobserve = !pair_retain_pin;
			if (!pair_retain_pin) {
				context->reobserve_count = ++pair_reobserves;
				ReleaseBuffer(*buffer);
				*buffer = InvalidBuffer;
				pair_vm_handle = InvalidBuffer;
				fixture_locators[1].relNumber++;
			}
			if (pair_cancel_refusal)
				InterruptPending = true;
			return false;
		}
		if (held[0])
			pair_nested_success++;
		else
			pair_warm_success++;
		pair_aux_driving = true;
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
		pair_aux_driving = false;
		memset(context, 0, sizeof(*context));
		pair_reobserves = 0;
		return true;
	}
	if (repair_retry_fixture) {
		UT_ASSERT(context != NULL && repair_pinned && !repair_locked);
		UT_ASSERT_EQ(*buffer, 2);
		UT_ASSERT(!held[0] && !pinned[0]);
		repair_attempts++;
		if (repair_attempts == 1) {
			UT_ASSERT(!context->active);
			repair_context = context;
			context->active = true;
			context->absolute_deadline_us = 81;
			context->diagnostic_request_sequence = 91;
		} else {
			UT_ASSERT(context == repair_context && context->active);
			UT_ASSERT_EQ(context->absolute_deadline_us, 81);
			UT_ASSERT_EQ(context->diagnostic_request_sequence, 91);
			UT_ASSERT_EQ(context->reobserve_count,
						 repair_retain_pin ? 0 : repair_attempts - 1);
		}
		if (repair_attempts == repair_error_at)
			elog(ERROR, "controlled VM repair acquisition failure");
		if (repair_refusals != 0) {
			repair_refusals--;
			context->reobserve = !repair_retain_pin;
			if (!repair_retain_pin)
				context->reobserve_count++;
			if (!repair_retain_pin) {
				ReleaseBuffer(*buffer);
				*buffer = InvalidBuffer;
				fixture_locators[1].relNumber++;
			}
			if (repair_cancel)
				InterruptPending = true;
			return false;
		}
		repair_aux_driving = true;
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
		repair_aux_driving = false;
		memset(context, 0, sizeof(*context));
		return true;
	}
	if (repairing_vm_only) {
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
		return true;
	}
	if (repairing && !empty_fixture) {
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
		return true;
	}
	UT_ASSERT(empty_fixture && context != NULL && !held[0]);
	UT_ASSERT_EQ(*buffer, 2);
	empty_aux_calls++;
	if (!empty_aux) {
		if (!empty_refusal_keeps_pin) {
			ReleaseBuffer(*buffer);
			*buffer = InvalidBuffer;
		}
		if (empty_cancel_after_refusal)
			InterruptPending = true;
		return false;
	}
	LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
	return true;
}

bool
ClusterBufferCaptureMutationContext(Buffer buffer, ResourceXWriterUseContext *out, uint64 *revision,
									bool *checked)
{
	UT_ASSERT(empty_fixture && buffer >= 1 && buffer <= 2);
	if (!pinned[buffer - 1] || !held[buffer - 1])
		return false;
	if (buffer == 2 && empty_error_at_capture && !held[0])
		elog(ERROR, "controlled failure before original VM writer capture");
	if (buffer == 1 && empty_error_after_resume && held[1])
		elog(ERROR, "controlled failure after original VM writer resume");
	if (!writer[buffer - 1] || !(buffer == 1 ? empty_heap_writer : empty_vm_writer))
		return false;
	memset(out, 0, sizeof(*out));
	out->writer_activation_token = 80 + buffer;
	*revision = 33;
	*checked = true;
	if (buffer == 2 && !held[0] && empty_cancel_after_capture)
		InterruptPending = true;
	if (buffer == 2 && !held[0] && empty_identity_change_phase == 1)
		empty_identity_generation++;
	return true;
}

bool
ClusterConditionalLockBufferWithMutationContext(Buffer buffer,
												const ResourceXWriterUseContext *expected,
												uint64 revision, bool checked)
{
	UT_ASSERT(empty_fixture && buffer == 2 && held[0] && !held[1] && pinned[1]);
	UT_ASSERT_EQ(expected->writer_activation_token, 82);
	UT_ASSERT_EQ(revision, 33);
	UT_ASSERT(checked);
	empty_resumes++;
	if (!empty_resume)
		return false;
	held[1] = writer[1] = true;
	if (empty_identity_change_phase == 3)
		empty_identity_generation++;
	return true;
}

void
ClusterUnlockBufferAfterConditionalMutation(Buffer buffer)
{
	UT_ASSERT(empty_fixture && buffer == 2 && held[0] && held[1] && writer[1]);
	held[1] = writer[1] = false;
	empty_local_unlocks++;
}

bool
visibilitymap_pin_recent(Relation rel, BlockNumber block, Buffer recent, Buffer *buffer)
{
	if (pair_fixture) {
		UT_ASSERT(rel == &relation && block == 0 && recent == 2);
		UT_ASSERT(held[0] && pinned[0] && !held[1] && !pinned[1]);
		UT_ASSERT_EQ(*buffer, InvalidBuffer);
		pair_repins++;
		if (pair_repin_misses != 0) {
			pair_repin_misses--;
			if (pair_cancel_miss)
				InterruptPending = true;
			return false;
		}
		pinned[1] = true;
		*buffer = pair_vm_handle = 2;
		return true;
	}
	UT_ASSERT(empty_fixture && rel == &relation && block == 0 && recent == 2);
	UT_ASSERT(held[0] && !held[1] && !pinned[1]);
	empty_repins++;
	if (empty_repin_misses > 0) {
		empty_repin_misses--;
		if (empty_cancel_repin)
			InterruptPending = true;
		return false;
	}
	pinned[1] = true;
	*buffer = 2;
	if (empty_vm_race == 1)
		((PageHeader)pages[1].data)->pd_block_scn++;
	else if (empty_vm_race == 2)
		PageSetLSN(pages[1].data, 999);
	empty_vm_race = 0;
	return true;
}

Size
GetRecordedFreeSpace(Relation rel, BlockNumber block)
{
	UT_ASSERT(empty_fixture && rel == &relation && block == 0 && !held[0]);
	return 0;
}

void
RecordPageWithFreeSpace(Relation rel, BlockNumber block, Size space)
{
	UT_ASSERT(empty_fixture && rel == &relation && block == 0);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0]);
	UT_ASSERT(space > 0);
	empty_fsm++;
}

void
visibilitymap_pin(Relation rel, BlockNumber block, Buffer *buffer)
{
	if (pair_fixture) {
		UT_ASSERT(rel == &relation && block == 0);
		UT_ASSERT(pinned[0] && !held[0] && !held[1] && !pinned[1]);
		UT_ASSERT_EQ(*buffer, InvalidBuffer);
		UT_ASSERT_EQ(CritSectionCount, 0);
		pair_pin_calls++;
		pinned[1] = true;
		fixture_locators[1] = relation.rd_locator;
		*buffer = pair_vm_handle = 2;
		return;
	}
	UT_ASSERT(rel == &relation);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT(!BufferIsValid(*buffer) || *buffer == 2);
	if (repair_retry_fixture) {
		UT_ASSERT(!repair_locked && !held[0] && !pinned[0]);
		UT_ASSERT_EQ(CritSectionCount, 0);
		repair_pin_calls++;
		repair_pinned = true;
		fixture_locators[1] = relation.rd_locator;
		if (repair_clear_on_repin && repair_pin_calls > 1)
			*PageGetContents(pages[1].data) &= ~VISIBILITYMAP_VALID_BITS;
		repair_current = pages[1];
	}
	if (empty_fixture) {
		UT_ASSERT(!held[0] && !held[1]);
		empty_pin_calls++;
		pinned[1] = true;
	}
	*buffer = 2;
}

#include "test_cluster_vm_buffer_matches.inc"
#include "test_cluster_vm_repair_clear.inc"
#include "test_cluster_vm_visible_log.inc"
static void
run_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf, XLogRecPtr recptr, Buffer vmBuf,
		TransactionId cutoff_xid, uint8 flags, const ClusterSpaceIdentity *identity)
#include "test_cluster_vm_visible_body.inc"
#define visibilitymap_set_locked run_set
#include "test_cluster_vm_visible_wrapper.inc"
static bool
fixture_empty_identity_current(Relation rel,
							   const ClusterSpaceIdentityObservationV1 *expected) pg_attribute_unused();
#define cluster_space_relation_identity_current_v1 fixture_empty_identity_current
#include "test_cluster_vm_empty_set.inc"
#undef cluster_space_relation_identity_current_v1
#undef visibilitymap_set_locked

/* Adapt only the argument type when executing the unchanged parent setter. */
static bool
fixture_set_empty_observed(const ClusterSpaceIdentityObservationV1 *observation)
{
#ifdef FIXTURE_HAS_VM_EMPTY_OBSERVATION
	return visibilitymap_set_empty(&relation, 0, 1, 2, observation);
#else
	return visibilitymap_set_empty(&relation, 0, 1, 2,
								   observation != NULL ? &observation->identity : NULL);
#endif
}

/* Original direct-entry negative cases keep their input mutations. */
static bool
fixture_set_empty_identity(Relation rel, BlockNumber heapblk, Buffer heapbuf, Buffer vmbuf,
						   const ClusterSpaceIdentity *input)
{
	ClusterSpaceIdentityObservationV1 observation = {0};

	UT_ASSERT(rel == &relation && heapblk == 0 && heapbuf == 1 && vmbuf == 2);
	observation.identity = *input;
	observation.invalidations = empty_identity_generation;
	return fixture_set_empty_observed(&observation);
}

	static void reset(bool shared, bool visible, uint8 vm_bits)
{
	pair_fixture = false;
	empty_fixture = false;
	repair_retry_fixture = false;
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&identity, 0, sizeof(identity));
	memset(edges, 0, sizeof(edges));
	memset(dirty, 0, sizeof(dirty));
	memset(registered, 0, sizeof(registered));
	memset(register_flags, 0, sizeof(register_flags));
	BufferBlocks = pages[0].data;
	cluster_enabled = true;
	cluster_shared_config = shared;
	CritSectionCount = 0;
	edge_count = inserts = 0;
	lock_calls = 0;
	assertion_failures = 0;
	expected_cutoff = 70;
	nonpermanent_buffer = 0;
	begun = expecting_error = InRecovery = wal_log_hints = checksums = repairing = false;
	repairing_vm_only = false;
	next_token = 100;
	edge_token = 0;
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	for (int i = 0; i < 2; i++) {
		fixture_locators[i] = relation.rd_locator;
		fixture_forks[i] = i == 0 ? MAIN_FORKNUM : VISIBILITYMAP_FORKNUM;
		fixture_blocks[i] = 0;
	}
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	identity.key.system_identifier = 11;
	identity.key.database_incarnation = 12;
	memset(identity.key.storage_uuid, 13, 16);
	identity.key.locator = relation.rd_locator;
	memset(identity.incarnation, 30, 16);
	identity.sequence = 1;
	identity.operation = 40;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	PageInitHeapPage(pages[0].data, BLCKSZ, 0);
	PageInit(pages[1].data, BLCKSZ, 0);
	((PageHeader)pages[0].data)->pd_block_scn = 20;
	((PageHeader)pages[1].data)->pd_block_scn = 30;
	if (visible)
		PageSetAllVisible(pages[0].data);
	*PageGetContents(pages[1].data) = vm_bits;
	pages[1].data[BLCKSZ - 1] = 42;
	memcpy(before, pages, sizeof(pages));
}

UT_TEST(test_visible_exact_two_page_batch)
{
	reset(true, true, 0);
	run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS, &identity);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(edge_token, 101);
	UT_ASSERT_EQ(edges[0].block_id, 0);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
	UT_ASSERT_EQ(edges[1].block_id, 1);
	UT_ASSERT_EQ(edges[1].before.mutation_token, 20);
	for (int i = 0; i < 2; i++) {
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(((PageHeader)images[i].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(PageGetLSN(pages[i].data), UINT64_C(0x9000));
		UT_ASSERT_EQ(registered[i], 1);
	}
	UT_ASSERT_EQ(register_flags[0], REGBUF_FORCE_IMAGE);
	UT_ASSERT_EQ(images[0].data[BLCKSZ - 1], 42);
	UT_ASSERT_EQ(*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS);
}
UT_TEST(test_no_change_allocates_no_version)
{
	reset(true, true, VISIBILITYMAP_VALID_BITS);
	run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS, &identity);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
}
UT_TEST(test_nonshared_preserves_native_hint_lsn_rules)
{
	for (int hints = 0; hints <= 1; hints++) {
		reset(false, true, 0);
		wal_log_hints = hints;
		run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS, NULL);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(register_flags[0], 0);
		UT_ASSERT_EQ(register_flags[1], REGBUF_STANDARD | (hints ? 0 : REGBUF_NO_IMAGE));
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), hints ? UINT64_C(0x9000) : 0);
	}
}
UT_TEST(test_versioned_visible_keeps_heap_full_page_protection)
{
	for (int hints = 0; hints <= 1; hints++) {
		for (int checksum = 0; checksum <= 1; checksum++) {
			reset(true, false, 0);
			wal_log_hints = hints;
			checksums = checksum;
			run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS, &identity);
			/* Advancing this LSN must not suppress checkpoint-first FPI. */
			UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x9000));
			UT_ASSERT_EQ(register_flags[1], REGBUF_STANDARD);
			UT_ASSERT_EQ(edge_count, 2);
		}
	}
}
UT_TEST(test_shared_visible_refuses_before_locks_or_capture)
{
	for (int bits = 0; bits <= VISIBILITYMAP_VALID_BITS; bits++) {
		reset(true, false, bits);
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			visibilitymap_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70,
							  VISIBILITYMAP_VALID_BITS, &identity);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(edge_count + lock_calls + inserts + CritSectionCount, 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
	}
}

UT_TEST(test_bad_component_refuses_before_either_page_changes)
{
	for (int variant = 0; variant < 10; variant++) {
		reset(true, false, 0);
		switch (variant) {
		case 0:
			identity.key.database_incarnation++;
			break;
		case 1:
			identity.key.locator.relNumber++;
			break;
		case 2:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			memset(identity.incarnation, 0, 16);
			break;
		case 4:
			nonpermanent_buffer = 1;
			break;
		case 5:
			nonpermanent_buffer = 2;
			break;
		case 6:
			((PageHeader)pages[0].data)->pd_block_scn = 0;
			break;
		case 7:
			((PageHeader)pages[1].data)->pd_block_scn = 0;
			break;
		case 8:
			((PageHeader)pages[0].data)->pd_flags |= PD_UNDO_SEG_HEADER;
			break;
		default:
			break; /* Null identity. */
		}
		memcpy(before, pages, sizeof(pages));
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS,
					variant == 9 ? NULL : &identity);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
	}
}
UT_TEST(test_recovery_uses_record_lsn_without_runtime_identity_or_wal)
{
	reset(true, false, 0);
	InRecovery = true;
	run_set(&relation, 0, InvalidBuffer, UINT64_C(0x8000), 2, 70, VISIBILITYMAP_VALID_BITS, NULL);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0], 0);
	UT_ASSERT_EQ(dirty[1], 1);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x8000));
	UT_ASSERT(memcmp(before[0].data, pages[0].data, BLCKSZ) == 0);
}
#include "test_cluster_vm_repair_versioned.inc"

static void
run_actual_visibility_repair(bool lpdead)
{
	struct {
		Relation rel;
		const char *relname;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state, *vacrel = &state;
	Buffer buf pg_attribute_unused() = 1, vmbuffer = 2;
	BlockNumber blkno = 0;
	Page page pg_attribute_unused() = pages[0].data;

	state.rel = &relation;
	state.relname = "visibility_repair";
	state.versioned = cluster_shared_config;
	state.identity = identity;
	repairing = true;
	if (lpdead) {
#include "test_cluster_vm_repair_lpdead.inc"
	} else {
#include "test_cluster_vm_repair_missing_heap.inc"
	}
	if (pair_fixture)
		pair_vm_handle = vmbuffer;
	repairing = false;
}

UT_TEST(test_deferred_vm_clear_logs_only_vm_and_preserves_heap_and_other_bits)
{
	Buffer vm = InvalidBuffer;

	reset(true, false, VISIBILITYMAP_VALID_BITS | (VISIBILITYMAP_VALID_BITS << 2));
	repairing = repairing_vm_only = true;
	lazy_clear_vm_after_heap(&relation, &identity, 0, &vm);
	UT_ASSERT_EQ(vm, 2);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(edges[0].block_id, 0);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
	UT_ASSERT_EQ(edge_token, 101);
	UT_ASSERT_EQ(registered[0], 1);
	UT_ASSERT_EQ(registered[1], 0);
	UT_ASSERT_EQ(register_flags[0], REGBUF_FORCE_IMAGE);
	UT_ASSERT_EQ(dirty[0], 0);
	UT_ASSERT_EQ(dirty[1], 1);
	UT_ASSERT_EQ(memcmp(pages[0].data, before[0].data, BLCKSZ), 0);
	UT_ASSERT_EQ(*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS << 2);
	UT_ASSERT_EQ(*PageGetContents(images[0].data), VISIBILITYMAP_VALID_BITS << 2);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x9000));
	UT_ASSERT_EQ(lock_calls, 2);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_deferred_vm_clear_rechecks_current_before_allocating_version)
{
	Buffer vm = 2;

	reset(true, false, 0);
	repairing = repairing_vm_only = true;
	lazy_clear_vm_after_heap(&relation, &identity, 0, &vm);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(lock_calls, 2);
	UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
}

UT_TEST(test_deferred_vm_clear_refuses_missing_version_before_modification)
{
	Buffer vm = 2;

	reset(true, false, VISIBILITYMAP_VALID_BITS);
	((PageHeader)pages[1].data)->pd_block_scn = 0;
	memcpy(before, pages, sizeof(pages));
	repairing = repairing_vm_only = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		lazy_clear_vm_after_heap(&relation, &identity, 0, &vm);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
}

UT_TEST(test_actual_vacuum_repairs_version_both_pages)
{
	for (int lpdead = 0; lpdead < 2; lpdead++) {
		reset(true, lpdead != 0, VISIBILITYMAP_VALID_BITS);
		run_actual_visibility_repair(lpdead != 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ(next_token, 101);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
		UT_ASSERT_EQ(edges[1].before.mutation_token, 20);
		UT_ASSERT_EQ(edges[0].block_id, 0);
		UT_ASSERT_EQ(edges[1].block_id, 1);
		UT_ASSERT_EQ(registered[0] + registered[1], 2);
		UT_ASSERT((register_flags[0] & REGBUF_FORCE_IMAGE) != 0);
		UT_ASSERT((register_flags[1] & REGBUF_FORCE_IMAGE) != 0);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x9000));
		UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x9000));
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(*PageGetContents(pages[1].data), 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_actual_vacuum_repair_refuses_before_modification)
{
	for (int lpdead = 0; lpdead < 2; lpdead++) {
		volatile bool caught = false;
		reset(true, lpdead != 0, VISIBILITYMAP_VALID_BITS);
		nonpermanent_buffer = 2;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_actual_visibility_repair(lpdead != 0);
		else
			caught = true;
		expecting_error = repairing = false;
		UT_ASSERT(caught);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_nonshared_vacuum_repairs_keep_original_behavior)
{
	for (int lpdead = 0; lpdead < 2; lpdead++) {
		reset(false, lpdead != 0, VISIBILITYMAP_VALID_BITS);
		run_actual_visibility_repair(lpdead != 0);
		UT_ASSERT_EQ(inserts + edge_count, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(dirty[0], lpdead);
		UT_ASSERT_EQ(dirty[1], 1);
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(*PageGetContents(pages[1].data), 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_actual_empty_vacuum_does_not_acquire_vm_in_critical)
{
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state, *vacrel = &state;
	/* Native caller locals bind the extracted branch, not a new fixture scope. */
	// cppcheck-suppress variableScope
	Buffer buf = 1;
	// cppcheck-suppress variableScope
	Buffer vm = 2, *vmbuffer = &vm;
	// cppcheck-suppress variableScope
	BlockNumber blkno = 0;
	Page page = pages[0].data;

	reset(true, false, 0);
	vacrel->rel = &relation;
	vacrel->versioned = true;
	vacrel->shared_maintenance_restricted = true;
	vacrel->identity = identity;
	expected_cutoff = InvalidTransactionId;
#include "test_cluster_vm_visible_empty.inc"
	UT_ASSERT_EQ(lock_calls, 0);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(inserts, 0);
	UT_ASSERT(!PageIsAllVisible(page));
	UT_ASSERT_EQ(CritSectionCount, 0);
}

/* Wrong masks must be ordinary runtime refusals, including without cassert.
 * The parent can assert or mutate instead; neither is an acceptable refusal. */
UT_TEST(test_clear_rejects_invalid_masks_before_lock_or_mutation)
{
	static const uint8 masks[] = { 0, VISIBILITYMAP_ALL_VISIBLE, 4, 7, 255 };

	for (int locked = 0; locked < 2; locked++) {
		for (unsigned i = 0; i < lengthof(masks); i++) {
			reset(false, true, 255);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				if (locked)
					(void)visibilitymap_clear_locked(&relation, 0, 2, masks[i]);
				else
					(void)visibilitymap_clear(&relation, 0, 2, masks[i]);
				expecting_error = false;
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(lock_calls + dirty[0] + dirty[1] + inserts + edge_count, 0);
			UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		}
	}
}

UT_TEST(test_set_rejects_invalid_masks_before_lock_or_mutation)
{
	static const uint8 masks[] = { 0, VISIBILITYMAP_ALL_FROZEN, 4, 5, 255 };

	for (int locked = 0; locked < 2; locked++) {
		for (unsigned i = 0; i < lengthof(masks); i++) {
			reset(false, true, 0);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				if (locked)
					run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, masks[i], NULL);
				else
					visibilitymap_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, masks[i], NULL);
				expecting_error = false;
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(lock_calls + dirty[0] + dirty[1] + inserts + edge_count, 0);
			UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		}
	}
}

static void
change_tag(int buffer, int field)
{
	switch (field) {
	case 0:
		fixture_locators[buffer - 1].spcOid++;
		break;
	case 1:
		fixture_locators[buffer - 1].dbOid++;
		break;
	case 2:
		fixture_locators[buffer - 1].relNumber++;
		break;
	case 3:
		fixture_forks[buffer - 1] = FSM_FORKNUM;
		break;
	case 4:
		fixture_blocks[buffer - 1]++;
		break;
	}
}

UT_TEST(test_vm_mutators_reject_wrong_exact_target_before_lock_or_write)
{
	for (int entry = 0; entry < 4; entry++) {
		for (int field = 0; field < 5; field++) {
			reset(false, true, entry < 2 ? 255 : 0);
			change_tag(2, field);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				if (entry == 0)
					(void)visibilitymap_clear(&relation, 0, 2, VISIBILITYMAP_VALID_BITS);
				else if (entry == 1)
					(void)visibilitymap_clear_locked(&relation, 0, 2, VISIBILITYMAP_VALID_BITS);
				else if (entry == 2)
					visibilitymap_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70,
									  VISIBILITYMAP_VALID_BITS, NULL);
				else
					run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS,
							NULL);
				expecting_error = false;
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(lock_calls + dirty[0] + dirty[1] + inserts + edge_count, 0);
			UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		}
	}
}

UT_TEST(test_set_rejects_wrong_heap_identity_before_lock_or_write)
{
	for (int locked = 0; locked < 2; locked++) {
		for (int field = 0; field < 5; field++) {
			reset(false, true, 0);
			change_tag(1, field);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				if (locked)
					run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS,
							NULL);
				else
					visibilitymap_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70,
									  VISIBILITYMAP_VALID_BITS, NULL);
				expecting_error = false;
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(lock_calls + dirty[0] + dirty[1] + inserts + edge_count, 0);
			UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		}
	}
}

UT_TEST(test_legal_clear_preserves_other_bits_and_is_idempotent)
{
	for (int locked = 0; locked < 2; locked++) {
		for (int frozen_only = 0; frozen_only < 2; frozen_only++) {
			uint8 flags = frozen_only ? VISIBILITYMAP_ALL_FROZEN : VISIBILITYMAP_VALID_BITS;
			reset(false, true, 255);
			/* The native caller owns the heap WAL critical section. */
			CritSectionCount = 1;
			if (locked) {
				UT_ASSERT(visibilitymap_clear_locked(&relation, 1, 2, flags));
				UT_ASSERT(!visibilitymap_clear_locked(&relation, 1, 2, flags));
			} else {
				UT_ASSERT(visibilitymap_clear(&relation, 1, 2, flags));
				UT_ASSERT(!visibilitymap_clear(&relation, 1, 2, flags));
			}
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), frozen_only ? 247 : 243);
			UT_ASSERT_EQ(dirty[1], 1);
			UT_ASSERT_EQ(dirty[0] + inserts + edge_count, 0);
			UT_ASSERT_EQ(lock_calls, locked ? 0 : 4);
			UT_ASSERT(memcmp(before[0].data, pages[0].data, BLCKSZ) == 0);
			CritSectionCount = 0;
		}
	}
}

/* Bind the original scanner entry, not just its native setter branch. */
typedef struct LVRelState {
	Relation rel;
	bool versioned;
	bool shared_maintenance_restricted;
	ClusterSpaceIdentity identity;
	/* Native page-vacuum composition keeps its own dead-item owner. */
	VacDeadItems *dead_items;
	int nindexes;
	bool do_index_vacuuming;
} LVRelState;
/* Identity transport leaves are controlled here; the cache fixture separately
 * executes the real storage guards. Keep every original publication body. */
static bool
fixture_empty_identity_capture(Relation rel, const ClusterSpaceIdentity *input,
							   ClusterSpaceIdentityObservationV1 *out) pg_attribute_unused();
static bool
fixture_empty_identity_capture(Relation rel, const ClusterSpaceIdentity *input,
							   ClusterSpaceIdentityObservationV1 *out)
{
	UT_ASSERT(rel == &relation && input != NULL && held[0] && !held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	empty_identity_captures++;
	memset(out, 0, sizeof(*out));
	if (!empty_identity_available)
		return false;
	out->identity = *input;
	out->invalidations = empty_identity_generation;
	return true;
}

static bool
fixture_empty_identity_current(Relation rel, const ClusterSpaceIdentityObservationV1 *expected)
{
	UT_ASSERT(rel == &relation && held[0] && held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	empty_identity_checks++;
	return empty_identity_available && expected != NULL
		   && expected->invalidations == empty_identity_generation
		   && memcmp(&expected->identity, &identity, sizeof(identity)) == 0;
}

#define cluster_space_relation_identity_cached_capture_v1 fixture_empty_identity_capture
#define cluster_space_relation_identity_current_v1 fixture_empty_identity_current
#include "test_cluster_vm_empty_scan.inc"
#undef cluster_space_relation_identity_cached_capture_v1
#undef cluster_space_relation_identity_current_v1

static LVRelState
empty_reset(bool visible, uint8 bits)
{
	LVRelState state;

	reset(true, visible, bits);
	empty_fixture = true;
	held[0] = pinned[0] = pinned[1] = true;
	held[1] = writer[0] = writer[1] = false;
	empty_vm_writer = empty_heap_writer = empty_resume = empty_aux = true;
	empty_race = empty_vm_race = empty_repin_misses = 0;
	empty_aux_calls = empty_resumes = empty_heap_waits = empty_cleanup_waits = 0;
	empty_repins = empty_local_unlocks = empty_fsm = 0;
	empty_error_after_resume = empty_error_at_capture = empty_cancel_repin = false;
	empty_cancel_after_refusal = empty_refusal_keeps_pin = empty_cancel_after_capture = false;
	empty_cancel_caught = false;
	empty_pin_calls = 0;
	empty_identity_available = true;
	empty_identity_generation = 1;
	empty_identity_captures = empty_identity_checks = 0;
	empty_identity_change_phase = 0;
	memset(empty_publication_metrics, 0, sizeof(empty_publication_metrics));
	empty_publication_notes = 0;
	InterruptPending = false;
	expected_cutoff = InvalidTransactionId;
	state.rel = &relation;
	state.versioned = state.shared_maintenance_restricted = true;
	state.identity = identity;
	return state;
}

UT_TEST(test_empty_shared_publishes_exact_native_two_page_versions)
{
	for (int share = 0; share <= 1; share++) {
		LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
		Buffer vm = 2;

		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ(next_token, 101);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
		UT_ASSERT_EQ(edges[1].before.mutation_token, 20);
		UT_ASSERT(PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x9000));
		UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x9000));
		UT_ASSERT_EQ(empty_aux_calls, 1);
		UT_ASSERT_EQ(empty_resumes, 1);
		UT_ASSERT_EQ(empty_local_unlocks, 1);
		UT_ASSERT_EQ(empty_heap_waits, 1);
		UT_ASSERT_EQ(empty_cleanup_waits, !share);
		UT_ASSERT(!pinned[0] && pinned[1] && !held[0] && !held[1]);
		UT_ASSERT_EQ(empty_fsm, 1);
	}
}

UT_TEST(test_empty_shared_upgrades_av_and_keeps_qualified_noop)
{
	for (int bits = 1; bits <= 3; bits += 2) {
		LVRelState state = empty_reset(true, bits);
		Buffer vm = 2;

		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 3);
		UT_ASSERT_EQ(inserts, bits == 1);
		UT_ASSERT_EQ(next_token, bits == 1 ? 101 : 100);
		UT_ASSERT_EQ(empty_local_unlocks, 1);
	}
}

UT_TEST(test_empty_shared_rechecks_insert_after_lock_handoff)
{
	LVRelState state = empty_reset(false, 0);
	Buffer vm = 2;

	empty_race = 1;
	UT_ASSERT(!lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, true, &vm));
	UT_ASSERT(held[0] && writer[0] && pinned[0] && pinned[1] && !held[1]);
	UT_ASSERT_EQ(inserts + edge_count + empty_fsm, 0);
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ(next_token, 100);
	UnlockReleaseBuffer(1);
}

UT_TEST(test_empty_shared_rechecks_new_page_without_publishing)
{
	LVRelState state = empty_reset(false, 0);
	Buffer vm = 2;

	empty_race = 2;
	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
	UT_ASSERT(PageIsNew(pages[0].data));
	UT_ASSERT_EQ(inserts + edge_count, 0);
	UT_ASSERT_EQ(empty_fsm, 1);
	UT_ASSERT(!pinned[0] && !held[0] && !held[1]);
}

UT_TEST(test_empty_shared_refuses_unproved_writer_version_space_or_layout)
{
	for (int variant = 0; variant < 10; variant++) {
		LVRelState state = empty_reset(false, 0);
		Buffer vm = 2;

		switch (variant) {
		case 0:
			empty_vm_writer = false;
			break;
		case 1:
			empty_heap_writer = false;
			break;
		case 2:
			empty_resume = false;
			break;
		case 3:
			empty_aux = false;
			break;
		case 4:
			empty_vm_race = 1;
			break;
		case 5:
			empty_vm_race = 2;
			break;
		case 6:
			state.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 7:
			((PageHeader)pages[1].data)->pd_block_scn = 0;
			break;
		case 8:
			((PageHeader)pages[0].data)->pd_block_scn = 0;
			break;
		case 9:
			((PageHeader)pages[1].data)->pd_upper--;
			break;
		}
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	}
}

UT_TEST(test_empty_shared_repin_misses_do_not_refetch_vm_current)
{
	LVRelState state = empty_reset(false, 0);
	Buffer vm = 2;

	empty_repin_misses = 3;
	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(empty_aux_calls, 1);
	UT_ASSERT_EQ(empty_heap_waits, 4);
	UT_ASSERT_EQ(empty_repins, 4);
	UT_ASSERT_EQ(empty_resumes, 1);
	UT_ASSERT_EQ(empty_local_unlocks, 1);
	UT_ASSERT_EQ(empty_fsm, 1);
}

UT_TEST(test_empty_direct_entry_requires_both_original_writers_and_exact_tags)
{
	for (int variant = 0; variant < 19; variant++) {
		LVRelState state = empty_reset(false, 0);

		held[1] = writer[0] = writer[1] = true;
		if (variant < 10)
			change_tag(variant < 5 ? 1 : 2, variant % 5);
		else
			switch (variant) {
			case 10:
				pinned[0] = false;
				break;
			case 11:
				pinned[1] = false;
				break;
			case 12:
				held[0] = false;
				break;
			case 13:
				held[1] = false;
				break;
			case 14:
				writer[0] = false;
				break;
			case 15:
				writer[1] = false;
				break;
			case 16:
				state.identity.key.locator.relNumber++;
				break;
			case 17:
				memset(state.identity.incarnation, 0, 16);
				break;
			case 18:
				state.identity.key.database_incarnation++;
				break;
			}
		UT_ASSERT(!fixture_set_empty_identity(&relation, 0, 1, 2, &state.identity));
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		UT_ASSERT_EQ(empty_aux_calls + empty_heap_waits + empty_resumes, 0);
	}
}

UT_TEST(test_empty_direct_entry_rejects_nonempty_new_and_malformed_heap)
{
	for (int variant = 0; variant < 12; variant++) {
		LVRelState state = empty_reset(false, 0);
		PageHeader header = (PageHeader)pages[0].data;

		held[1] = writer[0] = writer[1] = true;
		switch (variant) {
		case 0:
			empty_insert_row_at(FirstOffsetNumber);
			break;
		case 1:
			memset(header, 0, BLCKSZ);
			break;
		case 2:
			header->pd_lower--;
			break;
		case 3:
			header->pd_upper = header->pd_lower - 1;
			break;
		case 4:
			header->pd_special--;
			break;
		case 5:
			header->pd_special = BLCKSZ + 1;
			break;
		case 6:
			header->pd_flags &= ~PD_HAS_ITL;
			break;
		case 7:
			header->pd_flags |= PD_UNDO_SEG_HEADER;
			break;
		case 8:
			header->pd_block_scn = 0;
			break;
		case 9:
			PageSetPageSizeAndVersion((Page)header, BLCKSZ, 0);
			break;
		case 10:
			header->pd_special = BLCKSZ;
			break;
		case 11:
			header->pd_upper = header->pd_special + 1;
			break;
		}
		memcpy(before, pages, sizeof(pages));
		UT_ASSERT(!fixture_set_empty_identity(&relation, 0, 1, 2, &state.identity));
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		UT_ASSERT(held[0] && held[1] && writer[0] && writer[1]);
	}
}

UT_TEST(test_empty_publication_error_unwinds_only_its_vm_scope)
{
	for (int phase = 0; phase < 2; phase++) {
		LVRelState state = empty_reset(false, 0);
		Buffer vm = 2;
		volatile bool caught = false;

		empty_error_at_capture = phase == 0;
		empty_error_after_resume = phase == 1;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			(void)lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
		UT_ASSERT(!held[1] && !writer[1] && pinned[0] && pinned[1]);
		UT_ASSERT_EQ(held[0], phase == 1);
		UT_ASSERT_EQ(empty_local_unlocks, phase == 1);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		/* Original caller/ResourceOwner owns these surviving references. */
		if (held[0])
			LockBuffer(1, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(1);
		ReleaseBuffer(2);
	}
}

UT_TEST(test_empty_cancel_at_repin_miss_keeps_no_vm_pin_or_writer)
{
	LVRelState state = empty_reset(false, 0);
	Buffer vm = 2;
	volatile bool caught = false;

	empty_repin_misses = 1;
	empty_cancel_repin = expecting_error = true;
	if (setjmp(error_jump) == 0)
		(void)lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(vm, InvalidBuffer);
	UT_ASSERT(!held[0] && !held[1] && !writer[0] && !writer[1] && !pinned[1]);
	UT_ASSERT(pinned[0]);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	ReleaseBuffer(1);
}

UT_TEST(test_original_new_page_and_nonshared_empty_paths_preserved)
{
	for (int variant = 0; variant < 3; variant++) {
		LVRelState state = empty_reset(false, 0);
		Buffer vm = 2;

		if (variant == 0)
			memset(pages[0].data, 0, BLCKSZ);
		else {
			cluster_enabled = cluster_shared_config = false;
			state.versioned = state.shared_maintenance_restricted = false;
			UT_ASSERT(!cluster_heap_maintenance_restricted(&relation));
			PageSetLSN(pages[0].data, 123);
		}
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, variant == 2, &vm));
		UT_ASSERT_EQ(empty_aux_calls + empty_resumes, 0);
		UT_ASSERT_EQ(inserts, variant != 0);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(empty_fsm, 1);
		UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	}
}

UT_TEST(test_pinned_vm_status_decodes_exact_bytes_without_mutation_or_acquisition)
{
	const BlockNumber blocks[]
		= { 0, 1, 2, 3, 4, HEAPBLOCKS_PER_PAGE - 1, HEAPBLOCKS_PER_PAGE, HEAPBLOCKS_PER_PAGE + 3 };

	for (unsigned i = 0; i < lengthof(blocks); i++) {
		for (uint8 bits = 0; bits <= VISIBILITYMAP_VALID_BITS; bits++) {
			uint8 status = 77;
			uint8 *byte;
			bool valid;

			reset(true, false, 0);
			fixture_blocks[1] = HEAPBLK_TO_MAPBLOCK(blocks[i]);
			byte = (uint8 *)PageGetContents(pages[1].data) + HEAPBLK_TO_MAPBYTE(blocks[i]);
			*byte = (0x55 & ~(VISIBILITYMAP_VALID_BITS << HEAPBLK_TO_OFFSET(blocks[i])))
					| (bits << HEAPBLK_TO_OFFSET(blocks[i]));
			memcpy(before, pages, sizeof(pages));
			valid = visibilitymap_get_status_pinned(&relation, blocks[i], 2, &status);
			UT_ASSERT_EQ(valid, bits != VISIBILITYMAP_ALL_FROZEN);
			UT_ASSERT_EQ(status, valid ? bits : 0);
			UT_ASSERT_EQ(lock_calls + inserts + edge_count + dirty[0] + dirty[1], 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
		}
	}
}

UT_TEST(test_pinned_vm_status_rejects_wrong_targets_and_invalid_arguments)
{
	for (int variant = 0; variant < 9; variant++) {
		uint8 status = 77;
		Relation rel = &relation;
		BlockNumber heap_block = 0;
		Buffer vm = 2;

		reset(true, false, VISIBILITYMAP_VALID_BITS);
		if (variant < 5)
			change_tag(2, variant);
		else if (variant == 5)
			rel = NULL;
		else if (variant == 6)
			heap_block = InvalidBlockNumber;
		else if (variant == 7)
			vm = InvalidBuffer;
		UT_ASSERT(
			!visibilitymap_get_status_pinned(rel, heap_block, vm, variant == 8 ? NULL : &status));
		UT_ASSERT_EQ(status, variant == 8 ? 77 : 0);
		UT_ASSERT_EQ(assertion_failures, 0);
		UT_ASSERT_EQ(lock_calls + inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
	}
}

UT_TEST(test_pinned_vm_status_refuses_unknown_zero_and_accepts_only_pristine_new_page)
{
	for (int variant = 0; variant < 5; variant++) {
		uint8 status = 77;
		PageHeader header;

		reset(true, false, 0);
		header = (PageHeader)pages[1].data;
		if (variant == 0)
			header->pd_block_scn = 0;
		else if (variant == 1)
			header->pd_lower--;
		else {
			memset(pages[1].data, 0, BLCKSZ);
			if (variant == 3)
				pages[1].data[BLCKSZ - 1] = 1;
			else if (variant == 4)
				header->pd_block_scn = 1;
		}
		memcpy(before, pages, sizeof(pages));
		UT_ASSERT_EQ(visibilitymap_get_status_pinned(&relation, 0, 2, &status), variant == 2);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(lock_calls + inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
	}
}

static void
setup_repair_reobservation(bool retain_pin, unsigned refusals, bool cached)
{
	reset(true, false, VISIBILITYMAP_VALID_BITS | (VISIBILITYMAP_VALID_BITS << 2));
	repair_retry_fixture = repairing = repairing_vm_only = true;
	repair_pinned = cached;
	repair_locked = repair_aux_driving = false;
	repair_retain_pin = retain_pin;
	repair_clear_on_repin = repair_cancel = repair_caught = false;
	repair_refusals = refusals;
	repair_attempts = repair_pin_calls = repair_unpins = repair_error_at = 0;
	repair_vm_handle = cached ? 2 : InvalidBuffer;
	repair_context = NULL;
	InterruptPending = false;
	memset(held, 0, sizeof(held));
	memset(pinned, 0, sizeof(pinned));
}

static void
run_repair_with_error_boundary(void)
{
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		lazy_clear_vm_after_heap(&relation, &identity, 0, &repair_vm_handle);
	else
		repair_caught = true;
	expecting_error = false;
}

UT_TEST(test_vm_only_repair_reobserves_replaced_and_retained_pins)
{
	for (unsigned retain = 0; retain < 2; retain++) {
		for (unsigned cached = 0; cached < 2; cached++) {
			for (unsigned refusals = 1; refusals <= 4; refusals += 3) {
				setup_repair_reobservation(retain != 0, refusals, cached != 0);
				run_repair_with_error_boundary();
				UT_ASSERT(!repair_caught);
				UT_ASSERT_EQ(repair_attempts, refusals + 1);
				UT_ASSERT_EQ(repair_pin_calls, refusals + 1);
				UT_ASSERT_EQ(repair_unpins, refusals);
				UT_ASSERT(repair_pinned && !repair_locked);
				UT_ASSERT_EQ(repair_vm_handle, 2);
				UT_ASSERT_EQ(assertion_failures, 0);
				UT_ASSERT_EQ(CritSectionCount, 0);
				UT_ASSERT_EQ(inserts, 1);
				UT_ASSERT_EQ(edge_count, 1);
				UT_ASSERT_EQ(next_token, 101);
				UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
				UT_ASSERT_EQ(registered[0], 1);
				UT_ASSERT_EQ(registered[1], 0);
				UT_ASSERT_EQ(dirty[0], 0);
				UT_ASSERT_EQ(dirty[1], 1);
				UT_ASSERT_EQ(*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS << 2);
				UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x9000));
				UT_ASSERT_EQ(memcmp(pages[0].data, before[0].data, BLCKSZ), 0);
				if (repair_pinned)
					ReleaseBuffer(repair_vm_handle);
			}
		}
	}
}

UT_TEST(test_vm_only_repair_rechecks_clear_after_refetch_without_wal)
{
	for (unsigned retain = 0; retain < 2; retain++) {
		setup_repair_reobservation(retain != 0, 1, false);
		repair_clear_on_repin = true;
		run_repair_with_error_boundary();
		UT_ASSERT(!repair_caught);
		UT_ASSERT_EQ(repair_attempts, 2);
		UT_ASSERT_EQ(repair_pin_calls, 2);
		UT_ASSERT_EQ(repair_unpins, 1);
		UT_ASSERT(repair_pinned && !repair_locked);
		UT_ASSERT_EQ(assertion_failures, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages[0].data, before[0].data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(pages[1].data, repair_current.data, BLCKSZ), 0);
		UT_ASSERT_EQ(*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS << 2);
		if (repair_pinned)
			ReleaseBuffer(repair_vm_handle);
	}
}

UT_TEST(test_vm_only_repair_cancel_stops_before_next_pin_or_mutation)
{
	for (unsigned retain = 0; retain < 2; retain++) {
		setup_repair_reobservation(retain != 0, 4, false);
		repair_cancel = true;
		run_repair_with_error_boundary();
		UT_ASSERT(repair_caught && !InterruptPending);
		UT_ASSERT_EQ(repair_attempts, 1);
		UT_ASSERT_EQ(repair_pin_calls, 1);
		UT_ASSERT_EQ(repair_unpins, 1);
		UT_ASSERT_EQ(repair_vm_handle, InvalidBuffer);
		UT_ASSERT(!repair_pinned && !repair_locked);
		UT_ASSERT_EQ(assertion_failures, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
	}
}

UT_TEST(test_vm_only_repair_error_preserves_only_the_callers_current_pin)
{
	for (unsigned retain = 0; retain < 2; retain++) {
		setup_repair_reobservation(retain != 0, 1, false);
		repair_error_at = 2;
		run_repair_with_error_boundary();
		UT_ASSERT(repair_caught);
		UT_ASSERT_EQ(repair_attempts, 2);
		UT_ASSERT_EQ(repair_pin_calls, 2);
		UT_ASSERT_EQ(repair_unpins, 1);
		UT_ASSERT_EQ(repair_vm_handle, 2);
		UT_ASSERT(repair_pinned && !repair_locked);
		UT_ASSERT_EQ(assertion_failures, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
		if (repair_pinned)
			ReleaseBuffer(repair_vm_handle);
		UT_ASSERT(!repair_pinned);
	}
}


static void
setup_pair_repair(bool lpdead, bool retain_pin, unsigned refusals)
{
	reset(true, lpdead, VISIBILITYMAP_VALID_BITS | (VISIBILITYMAP_VALID_BITS << 2));
	pair_fixture = true;
	memset(held, 0, sizeof(held));
	memset(pinned, 0, sizeof(pinned));
	memset(writer, 0, sizeof(writer));
	pinned[0] = pinned[1] = held[0] = writer[0] = true;
	pair_retain_pin = retain_pin;
	pair_aux_driving = pair_caught = false;
	pair_cancel_refusal = pair_cancel_miss = pair_peer_change = false;
	pair_refusals = refusals;
	pair_attempts = pair_pin_calls = pair_unpins = pair_repins = pair_repin_misses = 0;
	pair_heap_waits = pair_heap_unlocks = pair_warm_success = pair_nested_success = 0;
	pair_error_at = pair_reobserves = 0;
	pair_vm_handle = 2;
	pair_context = NULL;
	InterruptPending = false;
}

static void
run_pair_repair(bool lpdead)
{
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		run_actual_visibility_repair(lpdead);
	else
		pair_caught = true;
	expecting_error = false;
}

static void
release_pair_repair(void)
{
	for (int i = 0; i < 2; i++) {
		if (held[i])
			LockBuffer(i + 1, BUFFER_LOCK_UNLOCK);
		if (pinned[i])
			ReleaseBuffer(i + 1);
	}
}

static void
check_pair_repair(uint64 heap_before, uint64 vm_before)
{
	UT_ASSERT(!pair_caught);
	UT_ASSERT(held[0] && pinned[0] && !held[1] && pinned[1]);
	UT_ASSERT_EQ(pair_vm_handle, 2);
	UT_ASSERT_EQ(assertion_failures, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(edge_token, 101);
	UT_ASSERT_EQ(edges[0].before.mutation_token, vm_before);
	UT_ASSERT_EQ(edges[1].before.mutation_token, heap_before);
	UT_ASSERT_EQ(next_token, 101);
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ(*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS << 2);
	for (int i = 0; i < 2; i++) {
		/* clear_locked and the outer repair both dirty the VM. */
		UT_ASSERT_EQ(dirty[i], i == 0 ? 1 : 2);
		UT_ASSERT_EQ(registered[i], 1);
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(PageGetLSN(pages[i].data), UINT64_C(0x9000));
	}
	UT_ASSERT_EQ(register_flags[0], REGBUF_FORCE_IMAGE);
	UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
}

UT_TEST(test_pair_repair_reobserves_with_original_heap_pin)
{
	for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
		for (unsigned retain = 0; retain < 2; retain++) {
			for (unsigned refusals = 0; refusals <= 4; refusals += refusals == 0 ? 1 : 3) {
				setup_pair_repair(lpdead != 0, retain != 0, refusals);
				run_pair_repair(lpdead != 0);
				check_pair_repair(20, 30);
				UT_ASSERT_EQ(pair_attempts, refusals == 0 ? 1 : refusals + 2);
				UT_ASSERT_EQ(pair_heap_waits, refusals == 0 ? 0 : 1);
				UT_ASSERT_EQ(pair_heap_unlocks, pair_heap_waits);
				UT_ASSERT_EQ(pair_warm_success, refusals == 0 ? 0 : 1);
				UT_ASSERT_EQ(pair_nested_success, 1);
				UT_ASSERT_EQ(pair_pin_calls, refusals);
				UT_ASSERT_EQ(pair_unpins, refusals == 0 ? 0 : refusals + 1);
				release_pair_repair();
			}
		}
	}
}

UT_TEST(test_pair_repair_resident_misses_release_heap_before_vm_io)
{
	for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
		for (unsigned misses = 1; misses <= 3; misses += 2) {
			setup_pair_repair(lpdead != 0, false, 1);
			pair_repin_misses = misses;
			run_pair_repair(lpdead != 0);
			check_pair_repair(20, 30);
			UT_ASSERT_EQ(pair_attempts, 3);
			UT_ASSERT_EQ(pair_repins, misses + 1);
			UT_ASSERT_EQ(pair_heap_waits, misses + 1);
			UT_ASSERT_EQ(pair_heap_unlocks, misses + 1);
			UT_ASSERT_EQ(pair_pin_calls, misses + 1);
			UT_ASSERT_EQ(pair_unpins, misses + 2);
			release_pair_repair();
		}
	}
}

UT_TEST(test_pair_repair_captures_only_fresh_two_page_predecessors)
{
	for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
		setup_pair_repair(lpdead != 0, false, 1);
		pair_peer_change = true;
		run_pair_repair(lpdead != 0);
		check_pair_repair(60, 70);
		UT_ASSERT(!pair_peer_change);
		UT_ASSERT_EQ(pages[0].data[BLCKSZ - 1], 51);
		UT_ASSERT_EQ(images[1].data[BLCKSZ - 1], 51);
		release_pair_repair();
	}
}

UT_TEST(test_pair_repair_cancel_keeps_only_original_heap_pin)
{
	for (unsigned at_miss = 0; at_miss < 2; at_miss++) {
		for (unsigned retain = 0; retain < 2; retain++) {
			setup_pair_repair(true, retain != 0, 1);
			pair_cancel_refusal = at_miss == 0;
			pair_cancel_miss = at_miss != 0;
			pair_repin_misses = at_miss;
			run_pair_repair(true);
			UT_ASSERT(pair_caught && !InterruptPending);
			UT_ASSERT(pinned[0] && !held[0] && !pinned[1] && !held[1]);
			UT_ASSERT_EQ(pair_vm_handle, InvalidBuffer);
			UT_ASSERT_EQ(pair_attempts, at_miss == 0 ? 1 : 2);
			UT_ASSERT_EQ(pair_heap_waits, at_miss);
			UT_ASSERT_EQ(pair_pin_calls, at_miss);
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
			UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
			release_pair_repair();
		}
	}
}

UT_TEST(test_pair_repair_error_or_unknown_version_precedes_mutation)
{
	for (unsigned at_capture = 0; at_capture < 2; at_capture++) {
		for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
			setup_pair_repair(lpdead != 0, false, 1);
			if (at_capture != 0) {
				((PageHeader)pages[0].data)->pd_block_scn = 0;
				memcpy(before, pages, sizeof(pages));
			} else
				pair_error_at = 2;
			run_pair_repair(lpdead != 0);
			UT_ASSERT(pair_caught && pinned[0] && pinned[1]);
			UT_ASSERT_EQ(held[0], at_capture != 0);
			UT_ASSERT_EQ(held[1], at_capture != 0);
			UT_ASSERT_EQ(assertion_failures, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
			UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
			release_pair_repair();
		}
	}
}


static void
run_empty_cancel_scan(LVRelState *state, bool share)
{
	empty_cancel_vm_handle = 2;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		(void)lazy_scan_new_or_empty(state, 1, 0, pages[0].data, share, &empty_cancel_vm_handle);
	else
		empty_cancel_caught = true;
	expecting_error = false;
}

static void
check_empty_cancel_before_heap_wait(void)
{
	UT_ASSERT(empty_cancel_caught && !InterruptPending);
	UT_ASSERT(!held[0] && !held[1] && pinned[0] && !pinned[1]);
	UT_ASSERT_EQ(empty_cancel_vm_handle, InvalidBuffer);
	UT_ASSERT_EQ(empty_aux_calls, 1);
	UT_ASSERT_EQ(empty_pin_calls + empty_heap_waits + empty_cleanup_waits + empty_repins, 0);
	UT_ASSERT_EQ(empty_resumes + empty_local_unlocks + empty_fsm, 0);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(assertion_failures, 0);
	UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
}

/* Explicit fixture cleanup after catch; this is not native ResourceOwner
 * unwind coverage. The original heap pin belongs to the caller on ERROR. */
static void
release_empty_cancel_fixture(void)
{
	for (int i = 0; i < 2; i++) {
		if (held[i])
			LockBuffer(i + 1, BUFFER_LOCK_UNLOCK);
		if (pinned[i])
			ReleaseBuffer(i + 1);
	}
}

UT_TEST(test_empty_cancel_after_vm_refusal_precedes_repin_and_heap_wait)
{
	for (unsigned retain = 0; retain < 2; retain++) {
		for (unsigned share = 0; share < 2; share++) {
			LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);

			empty_aux = false;
			empty_refusal_keeps_pin = retain != 0;
			empty_cancel_after_refusal = true;
			run_empty_cancel_scan(&state, share != 0);
			check_empty_cancel_before_heap_wait();
			release_empty_cancel_fixture();
		}
	}
}

UT_TEST(test_empty_cancel_after_vm_warm_precedes_heap_wait_and_publication)
{
	for (unsigned share = 0; share < 2; share++) {
		LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);

		empty_cancel_after_capture = true;
		run_empty_cancel_scan(&state, share != 0);
		check_empty_cancel_before_heap_wait();
		release_empty_cancel_fixture();
	}
}

UT_TEST(test_empty_publication_missing_original_identity_preserves_pages_and_scope)
{
	for (int share = 0; share < 2; share++) {
		LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
		Buffer vm = 2;

		empty_identity_available = false;
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(empty_identity_captures, 1);
		UT_ASSERT_EQ(empty_identity_checks + empty_aux_calls + empty_resumes, 0);
		UT_ASSERT_EQ(empty_cleanup_waits, !share);
		UT_ASSERT_EQ(empty_fsm, 1);
		UT_ASSERT(!pinned[0] && pinned[1] && !held[0] && !held[1]);
	}
}

UT_TEST(test_empty_publication_rejects_identity_drift_across_each_acquisition)
{
	for (int phase = 1; phase <= 3; phase++) {
		for (int share = 0; share < 2; share++) {
			LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
			Buffer vm = 2;

			empty_identity_change_phase = phase;
			UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
			UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
			UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT_EQ(empty_identity_captures, 1);
			UT_ASSERT_EQ(empty_identity_checks, 1);
			UT_ASSERT_EQ(empty_resumes, 1);
			UT_ASSERT_EQ(empty_local_unlocks, 1);
			UT_ASSERT_EQ(empty_fsm, 1);
			UT_ASSERT(!pinned[0] && pinned[1] && !held[0] && !held[1]);
		}
	}
}

UT_TEST(test_empty_publication_current_identity_keeps_exact_positive_wal)
{
	LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
	Buffer vm = 2;

	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
	UT_ASSERT_EQ(empty_identity_captures, 1);
	UT_ASSERT_EQ(empty_identity_checks, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(next_token, 101);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
	UT_ASSERT_EQ(edges[1].before.mutation_token, 20);
	UT_ASSERT(PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
	UT_ASSERT(!pinned[0] && pinned[1] && !held[0] && !held[1]);
}

UT_TEST(test_empty_direct_setter_refuses_absent_stale_and_zero_observation)
{
	for (int reason = 0; reason < 4; reason++) {
		LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
		ClusterSpaceIdentityObservationV1 observation;

		UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
		held[1] = writer[0] = writer[1] = true;
		if (reason == 1)
			empty_identity_available = false;
		else if (reason == 2)
			empty_identity_generation++;
		else if (reason == 3)
			memset(&observation, 0, sizeof(observation));
		UT_ASSERT(!fixture_set_empty_observed(reason == 0 ? NULL : &observation));
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(held[0] && held[1] && pinned[0] && pinned[1] && writer[0] && writer[1]);
	}
}

UT_TEST(test_empty_direct_setter_refuses_replaced_original_identity)
{
	LVRelState state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
	ClusterSpaceIdentityObservationV1 observation;

	UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
	observation.identity.sequence++;
	held[1] = writer[0] = writer[1] = true;
	UT_ASSERT(!fixture_set_empty_observed(&observation));
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT(held[0] && held[1] && pinned[0] && pinned[1] && writer[0] && writer[1]);
}

UT_TEST(test_empty_direct_setter_current_observation_keeps_wal_and_qualified_noop)
{
	for (int noop = 0; noop < 2; noop++) {
		LVRelState state = empty_reset(noop != 0,
			VISIBILITYMAP_VALID_BITS << 2 | (noop ? VISIBILITYMAP_VALID_BITS : 0));
		ClusterSpaceIdentityObservationV1 observation;

		UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
		held[1] = writer[0] = writer[1] = true;
		UT_ASSERT(fixture_set_empty_observed(&observation));
		UT_ASSERT_EQ(empty_identity_checks, 1);
		UT_ASSERT_EQ(inserts, !noop);
		UT_ASSERT_EQ(edge_count, noop ? 0 : 2);
		UT_ASSERT_EQ(next_token, noop ? 100 : 101);
		UT_ASSERT(PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
		UT_ASSERT(held[0] && held[1] && pinned[0] && pinned[1] && writer[0] && writer[1]);
	}
}

UT_TEST(test_empty_outcome_counts_actual_wal_for_each_changed_input)
{
	for (int visible = 0; visible < 2; visible++) {
		for (int bits = 0; bits <= VISIBILITYMAP_VALID_BITS; bits++) {
			LVRelState state = empty_reset(visible != 0, bits | (VISIBILITYMAP_VALID_BITS << 2));
			Buffer vm = 2;

			if (visible && bits == VISIBILITYMAP_VALID_BITS)
				continue;
			UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(edge_count, 2);
			UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_wal_count"), inserts);
			UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_noop_count"), 0);
			UT_ASSERT_EQ(empty_publication_notes, 1);
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
			UT_ASSERT_EQ(empty_aux_calls, 1);
			UT_ASSERT_EQ(empty_resumes, 1);
			UT_ASSERT_EQ(empty_heap_waits, 1);
			UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
		}
	}
}

UT_TEST(test_empty_outcome_counts_qualified_noop_separately_from_wal)
{
	LVRelState state = empty_reset(true, 15);
	Buffer vm = 2;

	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, true, &vm));
	UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_noop_count"), 1);
	UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_wal_count"), 0);
	UT_ASSERT_EQ(empty_publication_notes, 1);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	UT_ASSERT_EQ(empty_aux_calls, 1);
	UT_ASSERT_EQ(empty_resumes, 1);
	UT_ASSERT_EQ(empty_heap_waits, 1);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
}

UT_TEST(test_empty_outcome_refusals_cannot_count_success_or_noop)
{
	for (int reason = 0; reason < 6; reason++) {
		LVRelState state = empty_reset(true, 15);
		Buffer vm = 2;

		switch (reason) {
		case 0: empty_identity_available = false; break;
		case 1: empty_identity_change_phase = 1; break;
		case 2: empty_identity_change_phase = 2; break;
		case 3: empty_identity_change_phase = 3; break;
		case 4: empty_vm_writer = false; break;
		case 5: empty_resume = false; break;
		}
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
		UT_ASSERT_EQ(empty_publication_notes, 0);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	}
}

UT_TEST(test_empty_outcome_error_does_not_count_completed_publication)
{
	for (int phase = 0; phase < 2; phase++) {
		LVRelState state = empty_reset(false, 12);
		Buffer vm = 2;

		empty_error_at_capture = phase == 0;
		empty_error_after_resume = phase == 1;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void)lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(empty_publication_notes, 0);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT(!held[1] && !writer[1] && pinned[0] && pinned[1]);
		UT_ASSERT_EQ(held[0], phase == 1);
		if (held[0])
			LockBuffer(1, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(1);
		ReleaseBuffer(2);
	}
}

static void
empty_unused_slots(OffsetNumber count)
{
	Page page = pages[0].data;
	PageHeader header = (PageHeader)page;

	UT_ASSERT(count > 0 && count <= MaxHeapTuplesPerPage + 1);
	header->pd_lower = SizeOfPageHeaderData + count * sizeof(ItemIdData);
	for (OffsetNumber offset = FirstOffsetNumber; offset <= count; offset++)
		ItemIdSetUnused(PageGetItemId(page, offset));
	PageSetHasFreeLinePointers(page);
	memcpy(before, pages, sizeof(pages));
}

UT_TEST(test_unused_only_scan_publishes_exact_native_versions)
{
	static const OffsetNumber counts[] = { 1, 8, MaxHeapTuplesPerPage };

	for (unsigned n = 0; n < lengthof(counts); n++) {
		for (int share = 0; share < 2; share++) {
			LVRelState state = empty_reset(false, 12);
			Buffer vm = 2;
			Size lower;

			empty_unused_slots(counts[n]);
			lower = ((PageHeader)pages[0].data)->pd_lower;
			UT_ASSERT(!PageIsEmpty(pages[0].data));
			UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(edge_count, 2);
			UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
			UT_ASSERT_EQ(edges[1].before.mutation_token, 20);
			UT_ASSERT_EQ(next_token, 101);
			UT_ASSERT(PageIsAllVisible(pages[0].data));
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
			UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_lower, lower);
			UT_ASSERT(memcmp(pages[0].data + SizeOfPageHeaderData,
							 before[0].data + SizeOfPageHeaderData,
							 lower - SizeOfPageHeaderData) == 0);
			UT_ASSERT(memcmp(images[1].data + SizeOfPageHeaderData,
							 before[0].data + SizeOfPageHeaderData,
							 lower - SizeOfPageHeaderData) == 0);
			UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_wal_count"), 1);
			UT_ASSERT_EQ(empty_aux_calls, 1);
			UT_ASSERT_EQ(empty_heap_waits, 1);
			UT_ASSERT_EQ(empty_cleanup_waits, !share);
			UT_ASSERT_EQ(empty_resumes, 1);
			UT_ASSERT_EQ(empty_fsm, 1);
			UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
		}
	}
}

UT_TEST(test_unused_direct_setter_upgrades_av_and_preserves_qualified_noop)
{
	for (int bits = 1; bits <= 3; bits += 2) {
		LVRelState state = empty_reset(true, bits | 12);
		ClusterSpaceIdentityObservationV1 observation;

		empty_unused_slots(8);
		UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
		held[1] = writer[0] = writer[1] = true;
		UT_ASSERT(fixture_set_empty_observed(&observation));
		UT_ASSERT_EQ(inserts, bits == 1);
		UT_ASSERT_EQ(edge_count, bits == 1 ? 2 : 0);
		UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_wal_count"), bits == 1);
		UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_noop_count"), bits == 3);
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
		UT_ASSERT_EQ(empty_aux_calls + empty_resumes + empty_heap_waits, 0);
		UT_ASSERT(held[0] && held[1] && writer[0] && writer[1] && pinned[0] && pinned[1]);
		if (bits == 3)
			UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	}
}

UT_TEST(test_unused_direct_setter_rejects_each_used_or_stored_slot)
{
	for (int last = 0; last < 2; last++) {
		for (int kind = 0; kind < 4; kind++) {
			LVRelState state = empty_reset(false, 12);
			ClusterSpaceIdentityObservationV1 observation;
			OffsetNumber offset = last ? 8 : FirstOffsetNumber;
			ItemId item;

			empty_unused_slots(8);
			UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
			held[1] = writer[0] = writer[1] = true;
			item = PageGetItemId(pages[0].data, offset);
			if (kind == 0)
				ItemIdSetDead(item);
			else if (kind == 1)
				ItemIdSetRedirect(item, last ? 1 : 8);
			else {
				empty_insert_row_at(offset);
				if (kind == 3)
					item->lp_flags = LP_UNUSED; /* Malformed UNUSED with storage. */
			}
			memcpy(before, pages, sizeof(pages));
			UT_ASSERT(!fixture_set_empty_observed(&observation));
			UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1] + empty_publication_notes, 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
			UT_ASSERT(held[0] && held[1] && writer[0] && writer[1]);
		}
	}
}

UT_TEST(test_unused_direct_setter_rejects_malformed_or_excess_lp_array)
{
	for (int kind = 0; kind < 8; kind++) {
		LVRelState state = empty_reset(false, 12);
		ClusterSpaceIdentityObservationV1 observation;
		PageHeader header = (PageHeader)pages[0].data;

		empty_unused_slots(kind == 0 ? MaxHeapTuplesPerPage + 1 : 8);
		UT_ASSERT(fixture_empty_identity_capture(&relation, &state.identity, &observation));
		held[1] = writer[0] = writer[1] = true;
		switch (kind) {
		case 1: header->pd_lower++; break;
		case 2: header->pd_lower = SizeOfPageHeaderData - 1; break;
		case 3: header->pd_upper = header->pd_lower - 1; break;
		case 4: header->pd_special--; break;
		case 5: header->pd_flags &= ~PD_HAS_ITL; break;
		case 6: header->pd_block_scn = 0; break;
		case 7: PageSetPageSizeAndVersion((Page)header, BLCKSZ, 0); break;
		}
		memcpy(before, pages, sizeof(pages));
		UT_ASSERT(!fixture_set_empty_observed(&observation));
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1] + empty_publication_notes, 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT(held[0] && held[1] && writer[0] && writer[1]);
	}
}

UT_TEST(test_unused_scan_rechecks_real_insert_before_publication)
{
	for (int share = 0; share < 2; share++) {
		LVRelState state = empty_reset(false, 12);
		Buffer vm = 2;

		empty_unused_slots(8);
		empty_race = 1;
		UT_ASSERT(!lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
		UT_ASSERT(ItemIdIsNormal(PageGetItemId(pages[0].data, FirstOffsetNumber)));
		UT_ASSERT_EQ(empty_aux_calls, 1);
		UT_ASSERT_EQ(empty_heap_waits, 1);
		UT_ASSERT_EQ(inserts + edge_count + empty_publication_notes + empty_fsm, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(held[0] && writer[0] && pinned[0] && pinned[1] && !held[1]);
		UnlockReleaseBuffer(1);
		ReleaseBuffer(2);
	}
}

UT_TEST(test_unused_scan_stale_original_observation_cannot_publish)
{
	for (int phase = 1; phase <= 3; phase++) {
		LVRelState state = empty_reset(true, 15);
		Buffer vm = 2;

		empty_unused_slots(8);
		empty_identity_change_phase = phase;
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, true, &vm));
		UT_ASSERT_EQ(empty_identity_captures, 1);
		UT_ASSERT_EQ(empty_identity_checks, 1);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1] + empty_publication_notes, 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(empty_fsm, 1);
		UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	}
}

UT_TEST(test_unused_nonshared_scan_keeps_original_native_path)
{
	LVRelState state = empty_reset(false, 12);
	Buffer vm = 2;

	empty_unused_slots(8);
	state.shared_maintenance_restricted = state.versioned = false;
	cluster_shared_config = false;
	UT_ASSERT(!lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, true, &vm));
	UT_ASSERT_EQ(empty_identity_captures + empty_aux_calls + empty_heap_waits + empty_fsm, 0);
	UT_ASSERT_EQ(inserts + edge_count + empty_publication_notes, 0);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	UT_ASSERT(held[0] && pinned[0] && pinned[1] && !held[1]);
	UnlockReleaseBuffer(1);
	ReleaseBuffer(2);
}

int
main(void)
{
	UT_PLAN(61);
	UT_RUN(test_visible_exact_two_page_batch);
	UT_RUN(test_no_change_allocates_no_version);
	UT_RUN(test_nonshared_preserves_native_hint_lsn_rules);
	UT_RUN(test_versioned_visible_keeps_heap_full_page_protection);
	UT_RUN(test_shared_visible_refuses_before_locks_or_capture);
	UT_RUN(test_bad_component_refuses_before_either_page_changes);
	UT_RUN(test_recovery_uses_record_lsn_without_runtime_identity_or_wal);
	UT_RUN(test_actual_empty_vacuum_does_not_acquire_vm_in_critical);
	UT_RUN(test_deferred_vm_clear_logs_only_vm_and_preserves_heap_and_other_bits);
	UT_RUN(test_deferred_vm_clear_rechecks_current_before_allocating_version);
	UT_RUN(test_deferred_vm_clear_refuses_missing_version_before_modification);
	UT_RUN(test_actual_vacuum_repairs_version_both_pages);
	UT_RUN(test_actual_vacuum_repair_refuses_before_modification);
	UT_RUN(test_nonshared_vacuum_repairs_keep_original_behavior);
	UT_RUN(test_clear_rejects_invalid_masks_before_lock_or_mutation);
	UT_RUN(test_set_rejects_invalid_masks_before_lock_or_mutation);
	UT_RUN(test_vm_mutators_reject_wrong_exact_target_before_lock_or_write);
	UT_RUN(test_set_rejects_wrong_heap_identity_before_lock_or_write);
	UT_RUN(test_legal_clear_preserves_other_bits_and_is_idempotent);
	UT_RUN(test_empty_shared_publishes_exact_native_two_page_versions);
	UT_RUN(test_empty_shared_upgrades_av_and_keeps_qualified_noop);
	UT_RUN(test_empty_shared_rechecks_insert_after_lock_handoff);
	UT_RUN(test_empty_shared_rechecks_new_page_without_publishing);
	UT_RUN(test_empty_shared_refuses_unproved_writer_version_space_or_layout);
	UT_RUN(test_empty_shared_repin_misses_do_not_refetch_vm_current);
	UT_RUN(test_empty_direct_entry_requires_both_original_writers_and_exact_tags);
	UT_RUN(test_empty_direct_entry_rejects_nonempty_new_and_malformed_heap);
	UT_RUN(test_empty_publication_error_unwinds_only_its_vm_scope);
	UT_RUN(test_empty_cancel_at_repin_miss_keeps_no_vm_pin_or_writer);
	UT_RUN(test_original_new_page_and_nonshared_empty_paths_preserved);
	UT_RUN(test_pinned_vm_status_decodes_exact_bytes_without_mutation_or_acquisition);
	UT_RUN(test_pinned_vm_status_rejects_wrong_targets_and_invalid_arguments);
	UT_RUN(test_pinned_vm_status_refuses_unknown_zero_and_accepts_only_pristine_new_page);
	UT_RUN(test_vm_only_repair_reobserves_replaced_and_retained_pins);
	UT_RUN(test_vm_only_repair_rechecks_clear_after_refetch_without_wal);
	UT_RUN(test_vm_only_repair_cancel_stops_before_next_pin_or_mutation);
	UT_RUN(test_vm_only_repair_error_preserves_only_the_callers_current_pin);
	UT_RUN(test_pair_repair_reobserves_with_original_heap_pin);
	UT_RUN(test_pair_repair_resident_misses_release_heap_before_vm_io);
	UT_RUN(test_pair_repair_captures_only_fresh_two_page_predecessors);
	UT_RUN(test_pair_repair_cancel_keeps_only_original_heap_pin);
	UT_RUN(test_pair_repair_error_or_unknown_version_precedes_mutation);
	UT_RUN(test_empty_cancel_after_vm_refusal_precedes_repin_and_heap_wait);
	UT_RUN(test_empty_cancel_after_vm_warm_precedes_heap_wait_and_publication);
	UT_RUN(test_empty_publication_missing_original_identity_preserves_pages_and_scope);
	UT_RUN(test_empty_publication_rejects_identity_drift_across_each_acquisition);
	UT_RUN(test_empty_publication_current_identity_keeps_exact_positive_wal);
	UT_RUN(test_empty_direct_setter_refuses_absent_stale_and_zero_observation);
	UT_RUN(test_empty_direct_setter_refuses_replaced_original_identity);
	UT_RUN(test_empty_direct_setter_current_observation_keeps_wal_and_qualified_noop);
	UT_RUN(test_empty_outcome_counts_actual_wal_for_each_changed_input);
	UT_RUN(test_empty_outcome_counts_qualified_noop_separately_from_wal);
	UT_RUN(test_empty_outcome_refusals_cannot_count_success_or_noop);
	UT_RUN(test_empty_outcome_error_does_not_count_completed_publication);
	UT_RUN(test_unused_only_scan_publishes_exact_native_versions);
	UT_RUN(test_unused_direct_setter_upgrades_av_and_preserves_qualified_noop);
	UT_RUN(test_unused_direct_setter_rejects_each_used_or_stored_slot);
	UT_RUN(test_unused_direct_setter_rejects_malformed_or_excess_lp_array);
	UT_RUN(test_unused_scan_rechecks_real_insert_before_publication);
	UT_RUN(test_unused_scan_stale_original_observation_cannot_publish);
	UT_RUN(test_unused_nonshared_scan_keeps_original_native_path);
	UT_DONE();
	return ut_failed_count != 0;
}
