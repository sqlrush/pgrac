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
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;
void
ProcessInterrupts(void)
{
	UT_ASSERT(empty_fixture && !held[0] && !held[1] && !pinned[1]);
	UT_ASSERT(empty_cancel_repin);
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
void
LockBuffer(Buffer buffer, int mode)
{
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
				if (empty_race == 1)
					((PageHeader)pages[0].data)->pd_lower += sizeof(ItemIdData);
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
	return UINT64_C(0x9000);
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
	UT_ASSERT(empty_fixture && context != NULL && !held[0]);
	UT_ASSERT_EQ(*buffer, 2);
	empty_aux_calls++;
	if (!empty_aux) {
		ReleaseBuffer(*buffer);
		*buffer = InvalidBuffer;
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
	UT_ASSERT(rel == &relation);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT(!BufferIsValid(*buffer) || *buffer == 2);
	if (empty_fixture) {
		UT_ASSERT(!held[0] && !held[1]);
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
#include "test_cluster_vm_empty_set.inc"
#undef visibilitymap_set_locked

	static void reset(bool shared, bool visible, uint8 vm_bits)
{
	empty_fixture = false;
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
} LVRelState;
#include "test_cluster_vm_empty_scan.inc"

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
		UT_ASSERT(!visibilitymap_set_empty(&relation, 0, 1, 2, &state.identity));
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
			header->pd_lower += sizeof(ItemIdData);
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
		UT_ASSERT(!visibilitymap_set_empty(&relation, 0, 1, 2, &state.identity));
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

int
main(void)
{
	UT_PLAN(33);
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
	UT_DONE();
	return ut_failed_count != 0;
}
