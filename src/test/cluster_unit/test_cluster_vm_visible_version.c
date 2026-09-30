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
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / 4)
#define HEAPBLK_TO_OFFSET(x) (((x) % 4) * 2)

bool cluster_enabled = true, cluster_shared_config = true, InRecovery, wal_log_hints;
int cluster_node_id, wal_level = WAL_LEVEL_REPLICA, NBuffers = 2, NLocBuffer;
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
static TransactionId expected_cutoff;
static int nonpermanent_buffer;
static uint8 register_flags[2];
static bool begun, expecting_error, checksums;
static jmp_buf error_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
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
	*locator = (RelFileLocator){ 1663, 5, 900 };
	*forknum = buffer == 1 ? MAIN_FORKNUM : VISIBILITYMAP_FORKNUM;
	*block = 0;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	return 0;
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
	UT_ASSERT_EQ(count, 2);
	memcpy(edges, entries, sizeof(edges));
	edge_count = count;
	edge_token = token;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun && dirty[1] > 0);
	if (cluster_shared_config)
		UT_ASSERT(dirty[0] > 0);
	UT_ASSERT_EQ(rmgr, RM_HEAP2_ID);
	UT_ASSERT_EQ(info, XLOG_HEAP2_VISIBLE);
	begun = false;
	inserts++;
	return UINT64_C(0x9000);
}

#include "test_cluster_vm_visible_log.inc"
static void
run_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf, XLogRecPtr recptr, Buffer vmBuf,
		TransactionId cutoff_xid, uint8 flags, const ClusterSpaceIdentity *identity)
#include "test_cluster_vm_visible_body.inc"
#define visibilitymap_set_locked run_set
#include "test_cluster_vm_visible_wrapper.inc"
#undef visibilitymap_set_locked

	static void reset(bool shared, bool visible, uint8 vm_bits)
{
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&identity, 0, sizeof(identity));
	memset(edges, 0, sizeof(edges));
	memset(dirty, 0, sizeof(dirty));
	memset(registered, 0, sizeof(registered));
	BufferBlocks = pages[0].data;
	cluster_shared_config = shared;
	CritSectionCount = 0;
	edge_count = inserts = 0;
	lock_calls = 0;
	expected_cutoff = 70;
	nonpermanent_buffer = 0;
	begun = expecting_error = InRecovery = wal_log_hints = checksums = false;
	next_token = 100;
	edge_token = 0;
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 900 };
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
			run_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70,
					VISIBILITYMAP_VALID_BITS, &identity);
			/* Advancing this LSN must not suppress checkpoint-first FPI. */
			UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x9000));
			UT_ASSERT_EQ(register_flags[1], REGBUF_STANDARD);
			UT_ASSERT_EQ(edge_count, 2);
		}
	}
}
UT_TEST(test_visible_heap_flag_changes_only_after_capture)
{
	for (int bits = 0; bits <= VISIBILITYMAP_VALID_BITS; bits++) {
		reset(true, false, bits);
		visibilitymap_set(&relation, 0, 1, InvalidXLogRecPtr, 2, 70, VISIBILITYMAP_VALID_BITS,
						  &identity);
		UT_ASSERT(PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ(lock_calls, 2);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(CritSectionCount, 0);
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
UT_TEST(test_actual_empty_vacuum_does_not_acquire_vm_in_critical)
{
	struct {
		Relation rel;
		bool versioned;
		ClusterSpaceIdentity identity;
	} state, *vacrel = &state;
	Buffer buf = 1, vmbuffer = 2;
	BlockNumber blkno = 0;
	Page page = pages[0].data;

	reset(true, false, 0);
	vacrel->rel = &relation;
	vacrel->versioned = true;
	vacrel->identity = identity;
	expected_cutoff = InvalidTransactionId;
#include "test_cluster_vm_visible_empty.inc"
	UT_ASSERT_EQ(lock_calls, 2);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(PageIsAllVisible(page));
	UT_ASSERT_EQ(CritSectionCount, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_visible_exact_two_page_batch);
	UT_RUN(test_no_change_allocates_no_version);
	UT_RUN(test_nonshared_preserves_native_hint_lsn_rules);
	UT_RUN(test_versioned_visible_keeps_heap_full_page_protection);
	UT_RUN(test_visible_heap_flag_changes_only_after_capture);
	UT_RUN(test_bad_component_refuses_before_either_page_changes);
	UT_RUN(test_recovery_uses_record_lsn_without_runtime_identity_or_wal);
	UT_RUN(test_actual_empty_vacuum_does_not_acquire_vm_in_critical);
	UT_DONE();
	return ut_failed_count != 0;
}
