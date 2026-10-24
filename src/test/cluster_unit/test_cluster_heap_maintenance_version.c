/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_maintenance_version.c
 *    Native maintenance changes carry the exact heap page version.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_maintenance_version.c
 *
 * NOTES
 *    PGRAC test of native mutation/WAL sections, freeze planning and page
 *    helpers. Prune eligibility and transaction/WAL I/O are fixture inputs;
 *    this is not a full VACUUM or distributed recovery test.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
#include "utils/rel.h"
#include "unit_test.h"
#include "cluster/cluster_mode.h"
#include "access/visibilitymap.h"
#include "test_cluster_heap_maintenance_policy.inc"

UT_DEFINE_GLOBALS();
#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_vacuum_freeze_locators.inc"
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA, NBuffers = 1, NLocBuffer;
static BufferDescPadded descriptors[1];
BufferDescPadded *BufferDescriptors = descriptors;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
HotStandbyState standbyState = STANDBY_DISABLED;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock current_page, before_page, wal_image;
static RelationData relation_data;
static FormData_pg_class relform;
static ClusterSpaceIdentity space_identity;
static RfPageVersionEdgeEntryV1 edge;
static uint64 next_token, edge_token;
static unsigned inserts, edges, dirties, hints;
static unsigned tx_checks, redo_dirties, redo_releases, freeze_data_calls;
static int expected_nplans;
static PGAlignedBlock freeze_main_data, freeze_block_data;
static Size freeze_block_length;
static uint8 expected_info;
static bool begun, expecting_error, tx_committed;
static bool redoing, redo_locked;
static jmp_buf error_jump;
static bool local_x, writer_current, route_current, authority_current, admission_current;
static int32 backend_pin_count;
static unsigned pin_checks;
static bool flush_error;
static uint8 writer_state;
static uint64 route_holder, route_master;
static unsigned flushes;
static unsigned writer_checks, route_checks, authority_checks, admission_checks;
static SCN fixture_wal_clock, inserted_wal_clock;
static unsigned wal_clock_reads;
static bool full_target_current, target_recheck_error, revoke_target_census;
static unsigned full_target_checks;

/* The real native apply reads this clock before mutation. WAL insertion's
 * existing production xl_scn stamp uses the same clock; this leaf captures
 * that value without simulating physical WAL insertion or persistence. */
SCN
cluster_scn_current(void)
{
	wal_clock_reads++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT_EQ(admission_checks, 1);
	return fixture_wal_clock;
}

/* Only the process-local private refcount is a leaf double. Compile the
 * actual pin reader as well as the native apply bodies below. */
static int32
GetPrivateRefCount(Buffer buffer)
{
	pin_checks++;
	UT_ASSERT_EQ(buffer, 1);
	return backend_pin_count;
}
#include "test_cluster_heap_borrowed_pin.inc"

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return local_x;
}
static ResourceXWriterUseContext original_writer_context;
bool
ClusterBufferCaptureMutationContext(Buffer buffer, ResourceXWriterUseContext *out,
	uint64 *object_revision, bool *object_checked)
{
	UT_ASSERT_EQ(buffer, 1);
	if (!writer_current || !local_x || backend_pin_count <= 0)
		return false;
	*out = original_writer_context;
	*object_revision = 7;
	*object_checked = true;
	return true;
}
#include "test_cluster_heap_writer_context_match.inc"

bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	writer_checks++;
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(RelFileLocatorEquals(BufTagGetRelFileLocator(tag), relation_data.rd_locator));
	UT_ASSERT_EQ(tag->forkNum, MAIN_FORKNUM);
	UT_ASSERT_EQ(tag->blockNum, 7);
	UT_ASSERT(local_x);
	*generation = 14;
	*epoch = 15;
	*state = writer_state;
	return writer_current;
}
bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *out,
	uint64 *master, SCN *scn)
{
	route_checks++;
	UT_ASSERT_EQ(tag.blockNum, 7);
	memset(out, 0, sizeof(*out));
	out->transition_count = route_holder;
	*master = route_master;
	*scn = 20;
	return route_current;
}
bool
cluster_pcm_lock_authority_matches(BufferTag tag, const PcmAuthoritySnapshot *expected)
{
	authority_checks++;
	UT_ASSERT_EQ(tag.blockNum, 7);
	UT_ASSERT_EQ(expected->transition_count, 11);
	return authority_current;
}
bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	admission_checks++;
	UT_ASSERT(token != NULL && token->entered);
	return admission_current;
}
/* Existing full TARGET policy is stronger than census-read permission. */
bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	full_target_checks++;
	UT_ASSERT(token != NULL && token->entered);
	UT_ASSERT(local_x && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(writer_checks, 1);
	UT_ASSERT_EQ(route_checks, 1);
	UT_ASSERT_EQ(authority_checks, 1);
	UT_ASSERT_EQ(admission_checks, 0);
	UT_ASSERT_EQ(wal_clock_reads + inserts + dirties + edges + flushes, 0);
	if (target_recheck_error) elog(ERROR, "injected full TARGET recheck error");
	if (revoke_target_census) admission_current = false;
	return full_target_current;
}

void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(lsn, UINT64_C(0x9000));
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(local_x && writer_current);
	flushes++;
	if (flush_error)
		elog(ERROR, "injected WAL flush failure");
}


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
	longjmp(error_jump, 1);
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
IsCatalogRelation(Relation rel)
{
	UT_ASSERT(rel == &relation_data);
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
	UT_ASSERT_EQ(buffer, 1);
	*locator = (RelFileLocator){ 1663, 5, 900 };
	*forknum = MAIN_FORKNUM;
	*block = 7;
}
bool
BufferIsPermanent(Buffer buffer)
{
	return buffer == 1;
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	return ++next_token;
}
bool
TransactionIdDidCommit(TransactionId xid)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(xid, 77);
	tx_checks++;
	return tx_committed;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	if (redoing) {
		UT_ASSERT(redo_locked);
		UT_ASSERT_EQ(CritSectionCount, 0);
		redo_dirties++;
		return;
	}
	UT_ASSERT_EQ(CritSectionCount, 1);
	dirties++;
}
void
MarkBufferDirtyHint(Buffer buffer, bool standard)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(standard);
	hints++;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun && dirties == 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	begun = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun);
	if (expected_info == XLOG_HEAP2_PRUNE) {
		UT_ASSERT_EQ(length, SizeOfHeapPrune);
		UT_ASSERT_EQ(((xl_heap_prune *)data)->nredirected, 1);
	} else if (expected_info == XLOG_HEAP2_FREEZE_PAGE) {
		UT_ASSERT_EQ(length, SizeOfHeapFreezePage);
		UT_ASSERT_EQ(((xl_heap_freeze_page *)data)->nplans, expected_nplans);
		memcpy(freeze_main_data.data, data, length);
	} else {
		UT_ASSERT_EQ(length, SizeOfHeapVacuum);
		UT_ASSERT_EQ(((xl_heap_vacuum *)data)->nunused, 1);
	}
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(flags,
				 REGBUF_STANDARD | (expected_info == XLOG_FPI_FOR_HINT ? REGBUF_FORCE_IMAGE : 0));
	memcpy(wal_image.data, current_page.data, BLCKSZ);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT(begun && data != NULL && length > 0);
	UT_ASSERT_EQ(id, 0);
	if (expected_info == XLOG_HEAP2_FREEZE_PAGE) {
		UT_ASSERT(freeze_block_length + length <= BLCKSZ);
		memcpy(freeze_block_data.data + freeze_block_length, data, length);
		freeze_block_length += length;
		freeze_data_calls++;
	}
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(count, 1);
	edge = entries[0];
	edge_token = token;
	edges++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(rmgr, expected_info == XLOG_FPI_FOR_HINT ? RM_XLOG_ID : RM_HEAP2_ID);
	UT_ASSERT_EQ(info, expected_info);
	begun = false;
	inserted_wal_clock = fixture_wal_clock;
	inserts++;
	return UINT64_C(0x9000);
}

static void page_verify_redirects(Page page);
#include "test_cluster_heap_prune_helpers.inc"
#include "test_cluster_heap_freeze_helpers.inc"

/* Plans here are fixture-qualified, not shared FullXid/provenance evidence. */
static void
heap_freeze_apply_prepared(Relation rel, Buffer buffer,
						   TransactionId snapshotConflictHorizon,
						   HeapTupleFreeze *tuples, int ntuples,
						   const ClusterSpaceIdentity *identity)
#include "test_cluster_heap_freeze_apply.inc"

#include "test_cluster_heap_freeze_xmin_apply.inc"

/* Execute the full native freeze body with the planned pre-lock identity. */
static void
run_freeze(Relation rel, Buffer buffer, TransactionId snapshotConflictHorizon,
		   HeapTupleFreeze *tuples, int ntuples,
		   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_heap_freeze_body.inc"

XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	UT_ASSERT(redoing && !redo_locked);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(record->EndRecPtr, UINT64_C(0x9000));
	*buffer = 1;
	redo_locked = true;
	return BLK_NEEDS_REDO;
}

char *
XLogRecGetBlockData(XLogReaderState *record, uint8 id, Size *length)
{
	UT_ASSERT(redoing && redo_locked && record != NULL);
	UT_ASSERT_EQ(id, 0);
	if (length != NULL)
		*length = freeze_block_length;
	return freeze_block_data.data;
}

void
XLogRecGetBlockTag(XLogReaderState *record, uint8 id, RelFileLocator *locator,
				   ForkNumber *forknum, BlockNumber *block)
{
	/* Hot-standby conflict handling is outside this native replay fixture. */
	(void)record;
	(void)id;
	(void)locator;
	(void)forknum;
	(void)block;
	abort();
}

void
ResolveRecoveryConflictWithSnapshot(TransactionId xid, bool catalog, RelFileLocator locator)
{
	(void)xid;
	(void)catalog;
	(void)locator;
	abort();
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(redoing && redo_locked);
	UT_ASSERT_EQ(buffer, 1);
	redo_locked = false;
	redo_releases++;
}

#include "test_cluster_heap_freeze_redo.inc"

	/* Native eligibility/retention is covered by test_cluster_r4_lock_order.
 * Here the exact approved redirect/remove plan enters the real WAL section. */
	static void run_prune(bool change, const ClusterSpaceIdentity *identity pg_attribute_unused())
{
	Relation relation = &relation_data;
	Buffer buffer = 1;
	Page page = current_page.data;
	bool versioned pg_attribute_unused() = cluster_shared_config;
	RfPageProducerBatchV1 version_batch pg_attribute_unused();
	struct {
		int nredirected, ndead, nunused;
		OffsetNumber redirected[2], nowdead[1], nowunused[1];
		TransactionId new_prune_xid, snapshotConflictHorizon;
	} prstate = { 0 };

	if (change) {
		prstate.nredirected = prstate.nunused = 1;
		prstate.redirected[0] = 1;
		prstate.redirected[1] = 3;
		prstate.nowunused[0] = 2;
	}
#include "test_cluster_heap_prune_version.inc"
}

/* Only the native LP_DEAD mutation/WAL section is included. The later VM
 * decision is a separate producer and is deliberately not simulated here. */
static void
run_vacuum(const ClusterSpaceIdentity *identity)
{
	struct {
		int num_items;
		ItemPointerData items[1];
	} items = { 1 }, *dead_items = &items;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Buffer buffer = 1;
	BlockNumber blkno = 7;
	Page page = current_page.data;
	int index = 0, nunused = 0, saved_err_info = 0;
	OffsetNumber unused[MaxHeapTuplesPerPage];
	RfPageProducerBatchV1 version_batch pg_attribute_unused();

	vacrel->rel = &relation_data;
	vacrel->versioned = cluster_shared_config;
	if (identity != NULL)
		vacrel->identity = *identity;
	ItemPointerSet(&items.items[0], 7, 3);
#define update_vacuum_error_info(a, b, c, d, e) ((void)(a), (void)(b), (void)(d), (void)(e))
#include "test_cluster_heap_vacuum_version.inc"
#undef update_vacuum_error_info
	UT_ASSERT_EQ(index, 1);
}

static void
reset(uint8 opcode, bool shared)
{
	PGAlignedBlock tuple_bytes;
	HeapTupleHeader tuple = (HeapTupleHeader)tuple_bytes.data;
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relform, 0, sizeof(relform));
	memset(&space_identity, 0, sizeof(space_identity));
	memset(&edge, 0, sizeof(edge));
	memset(tuple_bytes.data, 0, BLCKSZ);
	BufferBlocks = current_page.data;
	cluster_shared_config = shared;
	cluster_enabled = shared;
	CritSectionCount = 0;
	edges = inserts = dirties = hints = flushes = 0;
	writer_checks = route_checks = authority_checks = admission_checks = 0;
	fixture_wal_clock = scn_encode(0, 100);
	inserted_wal_clock = InvalidScn;
	wal_clock_reads = 0;
	full_target_current = true;
	target_recheck_error = revoke_target_census = false;
	full_target_checks = 0;
	local_x = writer_current = route_current = authority_current = admission_current = true;
	backend_pin_count = 1;
	pin_checks = 0;
	writer_state = PCM_STATE_X;
	route_holder = 11;
	route_master = 12;
	flush_error = false;
	tx_checks = redo_dirties = redo_releases = freeze_data_calls = 0;
	freeze_block_length = 0;
	expected_nplans = 1;
	memset(freeze_main_data.data, 0, BLCKSZ);
	memset(freeze_block_data.data, 0, BLCKSZ);
	redoing = redo_locked = false;
	begun = expecting_error = false;
	tx_committed = true;
	next_token = 100;
	edge_token = 0;
	expected_info = opcode;
	relation_data.rd_rel = &relform;
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	space_identity.key.system_identifier = 11;
	space_identity.key.database_incarnation = 12;
	memset(space_identity.key.storage_uuid, 13, 16);
	space_identity.key.locator = relation_data.rd_locator;
	memset(space_identity.incarnation, 30, 16);
	space_identity.sequence = 1;
	space_identity.operation = 40;
	space_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	PageInitHeapPage(current_page.data, BLCKSZ, 0);
	((PageHeader)current_page.data)->pd_block_scn = 20;
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple, 77);
	for (int i = 1; i <= 3; i++) {
		tuple->t_infomask2 = i == 1 ? 0 : HEAP_ONLY_TUPLE;
		ItemPointerSet(&tuple->t_ctid, 7, i);
		UT_ASSERT_EQ(PageAddItem(current_page.data, (Item)tuple, 64, i, false, true), i);
	}
	if (opcode == XLOG_HEAP2_VACUUM) {
		ItemIdSetDead(PageGetItemId(current_page.data, 3));
		PageRepairFragmentation(current_page.data);
	} else if (opcode == XLOG_HEAP2_PRUNE) {
		PageSetFull(current_page.data);
		((PageHeader)current_page.data)->pd_prune_xid = 77;
	}
	memcpy(before_page.data, current_page.data, BLCKSZ);
}

static void
execute(uint8 opcode, const ClusterSpaceIdentity *identity)
{
	if (opcode == XLOG_HEAP2_PRUNE)
		run_prune(true, identity);
	else if (opcode == XLOG_HEAP2_VACUUM)
		run_vacuum(identity);
	else {
		HeapTupleFreeze tuples[2] = { { 0 } };
		for (int i = 0; i < 2; i++) {
			tuples[i].offset = i + 1;
			tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
			tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
		}
		run_freeze(&relation_data, 1, 70, tuples, 2, identity);
	}
}

static void
check_publication(bool shared)
{
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(PageGetLSN(current_page.data), UINT64_C(0x9000));
	UT_ASSERT_EQ(edges, shared ? 1 : 0);
	UT_ASSERT_EQ(next_token, shared ? 101 : 100);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, shared ? 101 : 20);
	UT_ASSERT_EQ(((PageHeader)wal_image.data)->pd_block_scn, shared ? 101 : 20);
	if (shared) {
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(edge.before.mutation_token, 20);
		UT_ASSERT_EQ(edge.block_id, 0);
		UT_ASSERT_EQ(edge.before_kind, RF_PAGE_STATE_PRESENT);
		UT_ASSERT_EQ(edge.result_kind, RF_PAGE_STATE_PRESENT);
		UT_ASSERT(memcmp(edge.before.segment_incarnation, space_identity.incarnation, 16) == 0);
		UT_ASSERT(memcmp(edge.result_incarnation, space_identity.incarnation, 16) == 0);
	}
}

static HeapTupleHeader
freeze_tuple(Page page, OffsetNumber offset)
{
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offset));
}

static void
replay_freeze_record(void)
{
	PGAlignedBlock written;
	XLogReaderState reader = { 0 };
	DecodedXLogRecord *decoded = calloc(1, offsetof(DecodedXLogRecord, blocks)
										 + sizeof(DecodedBkpBlock));

	UT_ASSERT_NOT_NULL(decoded);
	if (decoded == NULL)
		return;
	memcpy(written.data, current_page.data, BLCKSZ);
	memcpy(current_page.data, before_page.data, BLCKSZ);
	decoded->main_data = freeze_main_data.data;
	decoded->main_data_len = SizeOfHeapFreezePage;
	reader.record = decoded;
	reader.EndRecPtr = UINT64_C(0x9000);
	redoing = true;
	heap_xlog_freeze_page(&reader);
	redoing = false;
	UT_ASSERT_EQ(redo_dirties, 1);
	UT_ASSERT_EQ(redo_releases, 1);
	UT_ASSERT(!redo_locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	/* SPACE edge replay is an outer layer, not native heap freeze redo. */
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
	((PageHeader)written.data)->pd_block_scn = 20;
	UT_ASSERT(memcmp(written.data, current_page.data, BLCKSZ) == 0);
	free(decoded);
}

UT_TEST(test_qualified_freeze_kernel_changes_only_planned_headers)
{
	HeapTupleFreeze tuples[2] = { { 0 } };

	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	tx_committed = false;
	for (int i = 0; i < 2; i++) {
		tuples[i].offset = i + 1;
		tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
		tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
	}
	heap_freeze_apply_prepared(&relation_data, 1, 70, tuples, 2, &space_identity);
	check_publication(true);
	UT_ASSERT_EQ(tx_checks, 0);
	UT_ASSERT_EQ(freeze_data_calls, 2);
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
		HeapTupleHeader original = freeze_tuple(before_page.data, i);
		Size length = ItemIdGetLength(PageGetItemId(current_page.data, i));

		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
		UT_ASSERT_EQ(HeapTupleHeaderXminFrozen(tuple), i <= 2);
		UT_ASSERT(ItemPointerEquals(&tuple->t_ctid, &original->t_ctid));
		UT_ASSERT_EQ(tuple->t_hoff, original->t_hoff);
		UT_ASSERT(memcmp((char *)tuple + tuple->t_hoff,
						 (char *)original + original->t_hoff,
						 length - tuple->t_hoff) == 0);
	}
	UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
					 freeze_tuple(before_page.data, 3), 64) == 0);
	if (inserts == 1)
		replay_freeze_record();
}

UT_TEST(test_exact_xmin_plan_preserves_xmax_through_native_wal_and_redo)
{
	const uint16 xmax_masks[] = {
		HEAP_XMAX_INVALID,
		HEAP_XMAX_COMMITTED,
		HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK
	};

	for (unsigned variant = 0; variant < lengthof(xmax_masks); variant++) {
		ClusterHeapFreezePageBatchV1 page = { 0 };
		ClusterHeapFreezeProofBatchV1 proofs = { .proof_count = 1 };
		ClusterHeapFreezeCanonicalProofV1 *proof = &proofs.proofs[0];
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeTupleInputV1 *input = &page.tuples[0];
		HeapTupleFreeze plan;
		HeapTupleHeader tuple;
		ItemId item;
		bool prepared;

		reset(XLOG_HEAP2_FREEZE_PAGE, true);
		/* The canonical final fence requires an already-persisted page LSN. */
		PageSetLSN(current_page.data, UINT64_C(0x8000));
		tuple = freeze_tuple(current_page.data, 1);
		item = PageGetItemId(current_page.data, 1);
		HeapTupleHeaderSetXmax(tuple, 88);
		tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_UPDATED | xmax_masks[variant];
		tuple->t_infomask2 = HEAP_HOT_UPDATED | 7;
		memcpy(before_page.data, current_page.data, BLCKSZ);

		/* These identities are qualified fixture inputs, not an epoch producer. */
		fence = (ClusterHeapFreezeFinalFenceV1){
			.identity = space_identity, .block = 7, .page_lsn = UINT64_C(0x8000), .page_version = 20,
			.holder_sequence = 11, .master_sequence = 12, .admission_generation = 13 };
		proof->requested_locator.uba = uba_encode(1, 1, 0, 1);
		proof->requested_locator.xid = 77;
		proof->requested_locator.tt_wrap = TT_WRAP_INVALID;
		proof->requested_locator.itl_kind = ITL_FLAG_ACTIVE;
		proof->resolution.locator_echo = proof->requested_locator;
		proof->resolution.locator_echo.tt_wrap = 2;
		proof->resolution.outcome = CLUSTER_TX_COMMITTED;
		proof->resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		proof->resolution.commit_scn = 9;
		proof->resolution.authority.origin_epoch = 1;
		proof->resolution.authority.live_hwm_lsn = 1;
		proof->resolution.authority.tt_generation = 1;
		proof->resolution.authority.authority_scn = 9;
		proof->resolution.local_canonical_valid = true;
		proof->resolution.local_canonical_generation = 1;
		proof->resolution.local_canonical.segment_id = 1;
		proof->resolution.local_canonical.xid = 77;
		proof->resolution.local_canonical.wrap = 2;
		proof->resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
		proof->resolution.local_creation_target = (ClusterTxCreationTarget){
			.locator = relation_data.rd_locator, .fork = MAIN_FORKNUM,
			.block = 7, .offset = 1, .tuple_length = ItemIdGetLength(item), .valid = true };
		proof->c1b_finalized = proof->authority_current = proof->admission_current = true;
		proof->expected_fence = proof->current_fence = fence;
		page.identity = space_identity;
		page.block = 7;
		page.page_lsn = fence.page_lsn;
		page.page_version = fence.page_version;
		page.tuple_count = page.carrier_request_count = 1;
		page.carrier_requests[0].locator = proof->requested_locator;
		input->offset = 1;
		input->tuple_offset = ItemIdGetOffset(item);
		input->tuple_length = ItemIdGetLength(item);
		input->header_length = tuple->t_hoff;
		input->raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
		input->raw_xmax = HeapTupleHeaderGetRawXmax(tuple);
		input->infomask = tuple->t_infomask;
		input->infomask2 = tuple->t_infomask2;
		input->ctid = tuple->t_ctid;
		input->has_carrier = true;
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
		prepared = cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 0,
			FullTransactionIdFromEpochAndXid(1, 77),
			FullTransactionIdFromEpochAndXid(1, 100), 10, &plan);
		UT_ASSERT(prepared);
		if (!prepared)
			continue;
		tx_committed = false;
		heap_freeze_apply_prepared(&relation_data, 1, 70, &plan, 1, &space_identity);
		check_publication(true);
		UT_ASSERT_EQ(tx_checks, 0);
		UT_ASSERT_EQ(freeze_data_calls, 2);
		UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), 88);
		UT_ASSERT_EQ(tuple->t_infomask,
			HEAP_XMIN_FROZEN | HEAP_UPDATED | xmax_masks[variant]);
		UT_ASSERT_EQ(tuple->t_infomask2, HEAP_HOT_UPDATED | 7);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
			freeze_tuple(before_page.data, 2), 64) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
			freeze_tuple(before_page.data, 3), 64) == 0);
		if (inserts == 1)
			replay_freeze_record();
	}
}

/* Qualified identities and ownership below are leaf fixture inputs.  The real
 * capture, preparation, final apply, native WAL assembly and redo are exercised. */
typedef struct QualifiedXminFixture
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	PcmAuthoritySnapshot authority;
	ClusterSemanticAdmissionToken admission;
} QualifiedXminFixture;

static void
qualified_xmin_setup(QualifiedXminFixture *f, uint16 xmax_mask)
{
	ClusterHeapFreezeCanonicalProofV1 *proof;
	ClusterItlSlotData *slot;
	HeapTupleHeader tuple;

	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	memset(f, 0, sizeof(*f));
	PageSetLSN(current_page.data, UINT64_C(0x8000));
	slot = ClusterPageGetItlSlot(current_page.data, 0);
	memset(slot, 0, sizeof(*slot));
	slot->xid = 77;
	slot->wrap = 2;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = uba_encode(1, 11, 21, 31);
	slot->write_scn = 100;
	slot->first_change_lsn = 200;
	for (int i = 1; i <= 3; i++)
		freeze_tuple(current_page.data, i)->t_itl_slot_idx = 0;
	tuple = freeze_tuple(current_page.data, 1);
	HeapTupleHeaderSetXmax(tuple, 88);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_UPDATED | xmax_mask;
	tuple->t_infomask2 = HEAP_HOT_UPDATED | 7;
	memcpy(before_page.data, current_page.data, BLCKSZ);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f->input));
	memset(&original_writer_context, 0, sizeof(original_writer_context));
	InitBufferTag(&original_writer_context.ref.assertion.resource,
		&relation_data.rd_locator, MAIN_FORKNUM, 7);
	original_writer_context.ref.assertion.requester_node = 0;
	original_writer_context.ref.formation = 21;
	original_writer_context.ref.acquisition_generation = 22;
	original_writer_context.r4_record_generation = 13;
	original_writer_context.buffer_ownership_generation = 14;
	f->input.writer_context = original_writer_context;
	f->input.writer_context_valid = true;
	f->input.writer_object_revision = 7;
	f->input.writer_object_checked = true;
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&f->input, &f->page));
	UT_ASSERT_EQ(f->page.carrier_request_count, 1);
	f->fence = (ClusterHeapFreezeFinalFenceV1){
		.identity = space_identity, .block = 7, .page_lsn = UINT64_C(0x8000), .page_version = 20,
		.holder_sequence = 11, .master_sequence = 12, .admission_generation = 13 };
	f->proofs.proof_count = 1;
	proof = &f->proofs.proofs[0];
	proof->requested_locator = f->page.carrier_requests[0].locator;
	proof->resolution.locator_echo = proof->requested_locator;
	proof->resolution.locator_echo.tt_wrap = 2;
	proof->resolution.outcome = CLUSTER_TX_COMMITTED;
	proof->resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	proof->resolution.commit_scn = 9;
	proof->resolution.authority.origin_epoch = 1;
	proof->resolution.authority.live_hwm_lsn = 1;
	proof->resolution.authority.tt_generation = 1;
	proof->resolution.authority.authority_scn = 9;
	proof->resolution.local_canonical_valid = true;
	proof->resolution.local_canonical_generation = 1;
	proof->resolution.local_canonical.segment_id = 1;
	proof->resolution.local_canonical.xid = 77;
	proof->resolution.local_canonical.wrap = 2;
	proof->resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
	proof->resolution.local_creation_target = (ClusterTxCreationTarget){
		.locator = relation_data.rd_locator, .fork = MAIN_FORKNUM,
		.block = 7, .offset = 1, .tuple_length = 64, .valid = true };
	proof->c1b_finalized = proof->authority_current = proof->admission_current = true;
	proof->expected_fence = proof->current_fence = f->fence;
	f->authority.transition_count = 11;
	f->admission.entered = true;
	f->admission.record_generation = 13;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f->page, &f->proofs, &f->fence), 1);
}

static bool
qualified_xmin_apply(QualifiedXminFixture *f, uint16 index)
{
	return cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f->input,
		&f->page, &f->proofs, &f->fence, &f->authority, &f->admission, index,
		FullTransactionIdFromEpochAndXid(1, 77),
		FullTransactionIdFromEpochAndXid(1, 100), 10);
}

static void
qualified_xmin_batch_setup(QualifiedXminFixture *f,
	ClusterHeapFreezeXminTargetV1 targets[3])
{
	const TransactionId xids[] = { 77, UINT32_MAX - 2, 66 };
	const uint16 order[] = { 2, 0, 1 };
	ClusterHeapFreezeCanonicalProofV1 seed;
	ClusterItlSlotData carrier;

	qualified_xmin_setup(f, HEAP_XMAX_INVALID);
	seed = f->proofs.proofs[0];
	carrier = *ClusterPageGetItlSlot(current_page.data, 0);
	for (int i = 0; i < 3; i++) {
		ClusterItlSlotData *slot = ClusterPageGetItlSlot(current_page.data, i);
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i + 1);

		*slot = carrier;
		slot->xid = xids[i];
		slot->wrap += i;
		slot->undo_segment_head = uba_encode(1, 11, 21 + i, 31);
		HeapTupleHeaderSetXmin(tuple, xids[i]);
		tuple->t_itl_slot_idx = i;
	}
	memcpy(before_page.data, current_page.data, BLCKSZ);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f->input));
	f->input.writer_context = original_writer_context;
	f->input.writer_context_valid = true;
	f->input.writer_object_revision = 7;
	f->input.writer_object_checked = true;
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&f->input, &f->page));
	UT_ASSERT_EQ(f->page.carrier_request_count, 3);
	f->proofs.proof_count = 3;
	for (int i = 0; i < 3; i++) {
		ClusterHeapFreezeCanonicalProofV1 *proof = &f->proofs.proofs[i];

		*proof = seed;
		proof->requested_locator = f->page.carrier_requests[i].locator;
		proof->resolution.locator_echo = proof->requested_locator;
		proof->resolution.locator_echo.tt_wrap = 2 + i;
		proof->resolution.local_canonical.xid = xids[i];
		proof->resolution.local_canonical.wrap = 2 + i;
		proof->resolution.local_original_full_xid =
			FullTransactionIdFromEpochAndXid(i == 1 ? 0 : 1, xids[i]);
		proof->resolution.local_creation_target.offset = i + 1;
		targets[i].tuple_index = order[i];
		targets[i].original_creator_full_xid =
			FullTransactionIdFromEpochAndXid(order[i] == 1 ? 0 : 1, xids[order[i]]);
	}
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f->page, &f->proofs, &f->fence), 3);
	expected_nplans = 2;
}

static bool
qualified_xmin_batch_apply(QualifiedXminFixture *f,
	const ClusterHeapFreezeXminTargetV1 *targets, uint16 count)
{
	return cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1, &f->input,
		&f->page, &f->proofs, &f->fence, &f->authority, &f->admission, targets,
		count, FullTransactionIdFromEpochAndXid(1, 100), 10);
}

UT_TEST(test_xmin_apply_requires_original_backend_pin_before_page_or_writer_use)
{
	QualifiedXminFixture f;

	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	backend_pin_count = 0;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(pin_checks, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + writer_checks
		+ route_checks + authority_checks + admission_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 0);
}

UT_TEST(test_xmin_batch_requires_original_backend_pin_without_repin_or_write)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	backend_pin_count = 0;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(pin_checks, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + writer_checks
		+ route_checks + authority_checks + admission_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 0);
}

UT_TEST(test_xmin_apply_rejects_unbounded_cutoffs_before_native_write)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 3; variant++) {
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		if (variant != 0)
			limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant != 1)
			horizon = (SCN)UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
			&f.page, &f.proofs, &f.fence, &f.authority, &f.admission, 0,
			FullTransactionIdFromEpochAndXid(1, 77), limit, horizon));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_unbounded_cutoffs_before_native_write)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		qualified_xmin_batch_setup(&f, targets);
		if (variant != 0)
			limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant != 1)
			horizon = (SCN)UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1, &f.input,
			&f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
			targets, 3, limit, horizon));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_apply_rejects_unbound_original_identity_before_wal)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 5; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		switch (variant) {
		case 0: f.page.tuples[0].original_creator_full_xid = InvalidFullTransactionId; break;
		case 1: f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 2: f.proofs.proofs[0].resolution.local_original_full_xid = InvalidFullTransactionId; break;
		case 3: f.proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 4:
			f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 77);
			f.proofs.proofs[0].resolution.local_original_full_xid = f.page.tuples[0].original_creator_full_xid;
			break;
		}
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_later_origin_epoch_mismatch_is_atomic)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		/* The first row prepares; a later target keeps the same raw XID
		 * while its observed original epoch no longer matches the caller. */
		if (variant == 0)
			f.page.tuples[1].original_creator_full_xid = InvalidFullTransactionId;
		if (variant == 1)
			f.proofs.proofs[1].resolution.local_original_full_xid =
				FullTransactionIdFromEpochAndXid(1, UINT32_MAX - 2);
		if (variant == 2) {
			f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 77);
			f.proofs.proofs[0].resolution.local_original_full_xid = f.page.tuples[0].original_creator_full_xid;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_one_wal_fence_fullxid_horizon_and_redo)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	UT_ASSERT(qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(writer_checks, 1);
	UT_ASSERT_EQ(route_checks, 1);
	UT_ASSERT_EQ(authority_checks, 1);
	UT_ASSERT_EQ(admission_checks, 1);
	UT_ASSERT_EQ(((xl_heap_freeze_page *)freeze_main_data.data)->snapshotConflictHorizon, 77);
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
		HeapTupleHeader old = freeze_tuple(before_page.data, i);

		UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmin(old));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), HeapTupleHeaderGetRawXmax(old));
		UT_ASSERT_EQ(tuple->t_infomask, old->t_infomask | HEAP_XMIN_FROZEN);
		UT_ASSERT_EQ(tuple->t_infomask2, old->t_infomask2);
		UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, i - 1),
			ClusterPageGetItlSlot(before_page.data, i - 1), sizeof(ClusterItlSlotData)) == 0);
	}
	if (inserts == 1) {
		check_publication(true);
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		replay_freeze_record();
	}
}

UT_TEST(test_xmin_batch_later_unqualified_target_changes_nothing)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 4; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1; /* RED scaffold can write its first row. */
		switch (variant) {
		case 0: f.proofs.proofs[1].resolution.local_creation_target.offset = 1; break;
		case 1: f.proofs.proofs[1].resolution.commit_scn = 10; break;
		case 2: targets[2].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 99); break;
		case 3: targets[2].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, UINT32_MAX - 2); break;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + admission_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_duplicate_physical_rows)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int alias = 0; alias < 2; alias++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1;
		targets[1] = targets[0];
		if (alias) {
			/* Distinct request indexes cannot authorize the same physical row. */
			f.page.tuples[1] = f.page.tuples[2];
			targets[1].tuple_index = 1;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 2));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_runtime_bounds_and_empty_reject)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 4; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		if (variant == 3)
			targets[0].tuple_index = UINT16_MAX;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, variant == 1 ? NULL : targets,
			variant == 0 ? 0 : variant == 2 ? MaxHeapTuplesPerPage + 1 : 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_later_stale_row_rejects_before_fence)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1;
		if (variant == 0)
			f.page.tuples[1].raw_xmax++;
		else if (variant == 1)
			f.page.tuples[1].tuple_offset += MAXIMUM_ALIGNOF;
		else
			f.page.tuples[1].infomask2 ^= HEAP_ONLY_TUPLE;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_ambiguous_native_horizon)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	expected_nplans = 1;
	targets[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 66);
	f.proofs.proofs[2].resolution.local_original_full_xid = targets[0].original_creator_full_xid;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f.page, &f.proofs, &f.fence), 3);
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_batch_final_authority_refusal_changes_nothing)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		if (variant == 0)
			admission_current = false;
		else if (variant == 1)
			authority_current = false;
		else
			route_master++;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(writer_checks, 1);
		UT_ASSERT_EQ(route_checks, 1);
		UT_ASSERT_EQ(wal_clock_reads, 0);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_single_and_batch_reject_unknown_or_behind_wal_clock)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int unknown = 0; unknown < 2; unknown++) {
			if (batch)
				qualified_xmin_batch_setup(&f, targets);
			else
				qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			/* Larger node bits do not make an older clock reach H. */
			fixture_wal_clock = unknown ? InvalidScn : scn_encode(7, 9);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(wal_clock_reads, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			UT_ASSERT(local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_xmin_single_and_batch_wal_clock_accept_time_equal_or_ahead)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	const SCN horizon = scn_encode(7, 10);
	const FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);

	for (int batch = 0; batch < 2; batch++) {
		for (int ahead = 0; ahead < 2; ahead++) {
			bool applied;

			if (batch)
				qualified_xmin_batch_setup(&f, targets);
			else
				qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			/* A lower node id at the same time is still sufficient. */
			fixture_wal_clock = scn_encode(0, 10 + ahead);
			applied = batch ? cluster_heap_freeze_apply_xmin_batch_v1(
				&relation_data, 1, &f.input, &f.page, &f.proofs, &f.fence,
				&f.authority, &f.admission, targets, 3, limit, horizon)
				: cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
					&f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
					0, FullTransactionIdFromEpochAndXid(1, 77), limit, horizon);
			UT_ASSERT(applied);
			UT_ASSERT_EQ(wal_clock_reads, 1);
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT(scn_time_cmp(inserted_wal_clock, horizon) >= 0);
			UT_ASSERT(local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_xmin_batch_flush_error_keeps_borrowed_resources)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	flush_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_batch_apply(&f, targets, 3);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writer_checks, 1);
		UT_ASSERT_EQ(admission_checks, 1);
		for (int i = 1; i <= 3; i++)
			UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, i)));
		UT_ASSERT(local_x && f.admission.entered);
	}
	expecting_error = false;
}

UT_TEST(test_qualified_xmin_apply_wal_flush_and_redo)
{
	const uint16 masks[] = { HEAP_XMAX_INVALID, HEAP_XMAX_COMMITTED,
		HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK };
	QualifiedXminFixture f;

	for (unsigned i = 0; i < lengthof(masks); i++) {
		qualified_xmin_setup(&f, masks[i]);
		UT_ASSERT(qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, 1)));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(freeze_tuple(current_page.data, 1)), 77);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(freeze_tuple(current_page.data, 1)), 88);
		UT_ASSERT_EQ(freeze_tuple(current_page.data, 1)->t_infomask,
			HEAP_XMIN_FROZEN | HEAP_UPDATED | masks[i]);
		UT_ASSERT_EQ(freeze_tuple(current_page.data, 1)->t_infomask2, HEAP_HOT_UPDATED | 7);
		UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, 0),
			ClusterPageGetItlSlot(before_page.data, 0), sizeof(ClusterItlSlotData)) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
			freeze_tuple(before_page.data, 2), 64) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
			freeze_tuple(before_page.data, 3), 64) == 0);
		if (inserts == 1) {
			check_publication(true);
			UT_ASSERT(!qualified_xmin_apply(&f, 0));
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(flushes, 1);
			replay_freeze_record();
		}
	}
}

UT_TEST(test_qualified_xmin_rejects_missing_or_changed_ownership)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 10; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		switch (variant) {
			case 0: local_x = false; break;
			case 1: writer_current = false; break; /* ConditionalLock alone */
			case 2: writer_state = PCM_STATE_N; break;
			case 3: route_current = false; break;
			case 4: authority_current = false; break;
			case 5: route_holder++; break;
			case 6: route_master++; break;
			case 7: admission_current = false; break;
			case 8: f.admission.record_generation++; break;
			case 9: relform.relpersistence = RELPERSISTENCE_UNLOGGED; break;
		}
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_qualified_xmin_rejects_stale_physical_row_and_bounds)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 7; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		switch (variant) {
			case 0: ClusterPageGetItlSlot(current_page.data, 0)->wrap++; break;
			case 1: f.page.tuples[0].raw_xmax++; break;
			case 2: f.page.tuples[0].tuple_offset++; break;
			case 3: f.page.tuples[0].itl_slot_index = UINT8_MAX; break;
			case 4: f.proofs.proofs[0].resolution.local_creation_target.offset++; break;
			case 5: f.proofs.proof_count = UINT8_MAX; break;
			case 6: break; /* out-of-range physical row */
		}
		UT_ASSERT(!qualified_xmin_apply(&f, variant == 6 ? UINT16_MAX : 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	}
}

UT_TEST(test_qualified_xmin_flush_error_propagates_after_native_write)
{
	QualifiedXminFixture f;

	qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
	flush_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_apply(&f, 0);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, 1)));
		UT_ASSERT(local_x); /* borrowed resources remain with the caller */
	}
	expecting_error = false;
}

UT_TEST(test_qualified_xmin_rejects_recapture_with_old_proof_version)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 2; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		if (variant == 0)
			PageSetLSN(current_page.data, UINT64_C(0x8001));
		else
			((PageHeader)current_page.data)->pd_block_scn++;
		/* A fresh physical capture cannot refresh the old batch/proof fence. */
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
			current_page.data, &f.input));
		memcpy(before_page.data, current_page.data, BLCKSZ);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_freeze_native_wal_dedup_and_redo)
{
	for (int shared = 0; shared <= 1; shared++) {
		HeapTupleFreeze tuples[3] = { { 0 } };
		const OffsetNumber order[] = { 3, 1, 2 };
		xl_heap_freeze_page *header;
		xl_heap_freeze_plan *plans;
		OffsetNumber *offsets;

		reset(XLOG_HEAP2_FREEZE_PAGE, shared);
		expected_nplans = 2;
		for (int i = 1; i <= 3; i++) {
			HeapTupleHeader tuple = freeze_tuple(current_page.data, i);

			tuple->t_infomask |= i == 1 ? HEAP_MOVED_OFF : HEAP_MOVED_IN;
			HeapTupleHeaderSetXvac(tuple, 89 + i);
		}
		memcpy(before_page.data, current_page.data, BLCKSZ);
		for (int i = 0; i < 3; i++) {
			tuples[i].offset = order[i];
			tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
			tuples[i].t_infomask2 = order[i] == 1 ? 0 : HEAP_ONLY_TUPLE;
			tuples[i].frzflags = order[i] == 1 ? XLH_INVALID_XVAC : XLH_FREEZE_XVAC;
			tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
		}
		if (shared)
			heap_freeze_apply_prepared(&relation_data, 1, 70, tuples, 3, &space_identity);
		else
			run_freeze(&relation_data, 1, 70, tuples, 3, NULL);
		check_publication(shared);
		UT_ASSERT_EQ(tx_checks, shared ? 0 : 3);
		if (inserts != 1)
			continue;
		UT_ASSERT_EQ(freeze_data_calls, 2);
		UT_ASSERT_EQ(freeze_block_length,
					 2 * sizeof(xl_heap_freeze_plan) + 3 * sizeof(OffsetNumber));
		header = (xl_heap_freeze_page *)freeze_main_data.data;
		UT_ASSERT_EQ(header->snapshotConflictHorizon, 70);
		UT_ASSERT(!header->isCatalogRel);
		UT_ASSERT_EQ(header->nplans, 2);
		plans = (xl_heap_freeze_plan *)freeze_block_data.data;
		offsets = (OffsetNumber *)(freeze_block_data.data + 2 * sizeof(*plans));
		for (int i = 0; i < 2; i++) {
			UT_ASSERT_EQ(plans[i].xmax, InvalidTransactionId);
			UT_ASSERT_EQ(plans[i].t_infomask, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID);
			UT_ASSERT_EQ(plans[i].t_infomask2, i == 0 ? 0 : HEAP_ONLY_TUPLE);
			UT_ASSERT_EQ(plans[i].frzflags, i == 0 ? XLH_INVALID_XVAC : XLH_FREEZE_XVAC);
			UT_ASSERT_EQ(plans[i].ntuples, i == 0 ? 1 : 2);
		}
		for (int i = 0; i < 3; i++) {
			HeapTupleHeader tuple = freeze_tuple(current_page.data, i + 1);

			UT_ASSERT_EQ(offsets[i], i + 1);
			/* Xvac's raw field remains observable after MOVED bits clear. */
			UT_ASSERT_EQ(HeapTupleHeaderGetRawCommandId(tuple),
						 i == 0 ? InvalidTransactionId : FrozenTransactionId);
			UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
			UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
			UT_ASSERT_EQ(tuple->t_infomask & HEAP_MOVED, 0);
		}
		replay_freeze_record();
	}
}

UT_TEST(test_freeze_kernel_rejects_invalid_space_before_write)
{
	for (int bad = 0; bad < 7; bad++) {
		HeapTupleFreeze tuple = { 0 };

		reset(XLOG_HEAP2_FREEZE_PAGE, true);
		tuple.offset = 1;
		tuple.t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
		switch (bad) {
		case 0:
			space_identity.key.database_incarnation++;
			break;
		case 1:
			space_identity.key.locator.relNumber++;
			break;
		case 2:
			space_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			((PageHeader)current_page.data)->pd_block_scn = 0;
			break;
		case 4:
			break; /* no pre-lock identity */
		case 5:
			memset(space_identity.incarnation, 0, 16);
			break;
		case 6:
			relation_data.rd_locator.relNumber++;
			break;
		}
		memcpy(before_page.data, current_page.data, BLCKSZ);
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			heap_freeze_apply_prepared(&relation_data, 1, 70, &tuple, 1,
									 bad == 4 ? NULL : &space_identity);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(tx_checks + inserts + dirties + edges + CritSectionCount, 0);
		UT_ASSERT_EQ(next_token, 100);
	}
}

UT_TEST(test_prune_native_plan_and_version)
{
	reset(XLOG_HEAP2_PRUNE, true);
	execute(XLOG_HEAP2_PRUNE, &space_identity);
	check_publication(true);
	UT_ASSERT(ItemIdIsRedirected(PageGetItemId(current_page.data, 1)));
	UT_ASSERT_EQ(ItemIdGetRedirect(PageGetItemId(current_page.data, 1)), 3);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId(current_page.data, 2)));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId(current_page.data, 3)));
}
UT_TEST(test_shared_freeze_refuses_before_tuple_or_wal)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		execute(XLOG_HEAP2_FREEZE_PAGE, &space_identity);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(inserts + dirties + edges + CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(before_page.data, current_page.data, BLCKSZ), 0);
	for (int i = 1; i <= 2; i++)
		UT_ASSERT(!HeapTupleHeaderXminFrozen(
			(HeapTupleHeader)PageGetItem(current_page.data, PageGetItemId(current_page.data, i))));
}
UT_TEST(test_vacuum_native_unused_and_version)
{
	reset(XLOG_HEAP2_VACUUM, true);
	execute(XLOG_HEAP2_VACUUM, &space_identity);
	check_publication(true);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(current_page.data), 2);
}
UT_TEST(test_nonshared_native_maintenance)
{
	const uint8 ops[] = { XLOG_HEAP2_PRUNE, XLOG_HEAP2_FREEZE_PAGE, XLOG_HEAP2_VACUUM };
	for (unsigned i = 0; i < lengthof(ops); i++) {
		reset(ops[i], false);
		execute(ops[i], NULL);
		check_publication(false);
	}
}
UT_TEST(test_prune_hint_only_versions_actual_change)
{
	reset(XLOG_HEAP2_PRUNE, true);
	expected_info = XLOG_FPI_FOR_HINT;
	run_prune(false, &space_identity);
	check_publication(true);
	UT_ASSERT_EQ(hints, 0);
	UT_ASSERT(!PageIsFull(current_page.data));
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_prune_xid, InvalidTransactionId);
	for (int i = 1; i <= 3; i++)
		UT_ASSERT(ItemIdIsNormal(PageGetItemId(current_page.data, i)));
}
UT_TEST(test_prune_hint_nonshared_keeps_native_behavior)
{
	reset(XLOG_HEAP2_PRUNE, false);
	run_prune(false, NULL);
	UT_ASSERT_EQ(hints, 1);
	UT_ASSERT_EQ(dirties + inserts + edges, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
}
UT_TEST(test_prune_hint_no_change_has_no_version_or_wal)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	run_prune(false, NULL);
	UT_ASSERT_EQ(hints + dirties + inserts + edges, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
}
UT_TEST(test_prune_hint_missing_identity_refuses_before_mutation)
{
	reset(XLOG_HEAP2_PRUNE, true);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		run_prune(false, NULL);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hints + dirties + inserts + edges + CritSectionCount, 0);
	UT_ASSERT_EQ(next_token, 100);
}

UT_TEST(test_invalid_identity_refuses_before_mutation)
{
	const uint8 ops[] = { XLOG_HEAP2_PRUNE, XLOG_HEAP2_FREEZE_PAGE, XLOG_HEAP2_VACUUM };
	for (unsigned i = 0; i < lengthof(ops); i++)
		for (int bad = 0; bad < 7; bad++) {
			reset(ops[i], true);
			switch (bad) {
			case 0:
				space_identity.key.database_incarnation++;
				break;
			case 1:
				space_identity.key.locator.relNumber++;
				break;
			case 2:
				space_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
				break;
			case 3:
				((PageHeader)current_page.data)->pd_block_scn = 0;
				break;
			case 4:
				break; /* no pre-lock identity */
			case 5:
				memset(space_identity.incarnation, 0, 16);
				break;
			case 6:
				relation_data.rd_locator.relNumber++;
				break;
			}
			memcpy(before_page.data, current_page.data, BLCKSZ);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				execute(ops[i], bad == 4 ? NULL : &space_identity);
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(inserts + dirties + edges + hints, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 100);
		}
}

UT_TEST(test_freeze_preserves_native_transaction_sanity_gate)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, false);
	tx_committed = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		execute(XLOG_HEAP2_FREEZE_PAGE, &space_identity);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(inserts + dirties + edges, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(next_token, 100);
}

static unsigned identity_reads;
static bool identity_ready;
static unsigned maintenance_requests;
static bool maintenance_request_ready;

bool
cluster_heap_freeze_request_maintenance(void)
{
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	maintenance_requests++;
	return maintenance_request_ready;
}

static bool
maintenance_identity_io(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT(rel == &relation_data);
	UT_ASSERT_EQ(CritSectionCount, 0);
	identity_reads++;
	*out = space_identity;
	return identity_ready;
}

UT_TEST(test_vacuum_native_entry_copies_prelock_identity)
{
	VacuumParams options = { .options = VACOPT_VACUUM }, *params = &options;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;
	reset(XLOG_HEAP2_VACUUM, true);
	identity_reads = 0;
	identity_ready = true;
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
	UT_ASSERT(vacrel->versioned && vacrel->rel == rel);
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT(memcmp(&vacrel->identity, &space_identity, sizeof(space_identity)) == 0);
	space_identity.incarnation[0]++;
	UT_ASSERT(vacrel->identity.incarnation[0] != space_identity.incarnation[0]);
}

UT_TEST(test_vacuum_native_entry_refuses_missing_identity)
{
	VacuumParams options = { .options = VACOPT_VACUUM | VACOPT_FREEZE }, *params = &options;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;
	reset(XLOG_HEAP2_VACUUM, true);
	identity_reads = 0;
	identity_ready = false;
	maintenance_requests = 0;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(maintenance_requests, 0);
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
}

static bool
maintenance_entry(bits32 options)
{
	VacuumParams settings = { .options = options, .freeze_min_age = 0 }, *params = &settings;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;

	identity_reads = maintenance_requests = 0;
	identity_ready = true;
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	return vacrel->shared_maintenance_restricted;
}

UT_TEST(test_explicit_shared_freeze_requests_before_content_locks)
{
	reset(XLOG_HEAP2_VACUUM, true);
	maintenance_request_ready = true;
	UT_ASSERT(maintenance_entry(VACOPT_VACUUM | VACOPT_FREEZE));
	UT_ASSERT_EQ(maintenance_requests, 1);
}

UT_TEST(test_ordinary_or_nonshared_vacuum_does_not_request)
{
	for (int scope = 0; scope < 5; scope++) {
		reset(XLOG_HEAP2_VACUUM, true);
		if (scope == 1)
			cluster_shared_config = false;
		else if (scope == 2)
			cluster_enabled = false;
		else if (scope == 3)
			relform.relpersistence = RELPERSISTENCE_TEMP;
		else if (scope == 4)
			relform.relpersistence = RELPERSISTENCE_UNLOGGED;
		(void)maintenance_entry(VACOPT_VACUUM | (scope == 0 ? 0 : VACOPT_FREEZE));
		UT_ASSERT_EQ(maintenance_requests, 0);
	}
}

UT_TEST(test_deferred_request_preserves_shared_freeze_restriction)
{
	reset(XLOG_HEAP2_VACUUM, true);
	maintenance_request_ready = false;
	UT_ASSERT(maintenance_entry(VACOPT_VACUUM | VACOPT_FREEZE));
	UT_ASSERT_EQ(maintenance_requests, 1);
}

UT_TEST(test_xmin_single_and_batch_require_full_target_not_census_only)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		full_target_current = false;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(admission_checks + wal_clock_reads + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT(local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_target_admission_keeps_original_census_recheck_last)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	revoke_target_census = true;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(full_target_checks, 1);
	UT_ASSERT_EQ(admission_checks, 1);
	UT_ASSERT_EQ(wal_clock_reads + inserts + edges + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
}

UT_TEST(test_xmin_full_target_error_keeps_borrowed_resources_before_wal)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	target_recheck_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_batch_apply(&f, targets, 3);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(wal_clock_reads + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT(local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_bad_final_authority_never_checks_full_target)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	authority_current = false;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(full_target_checks, 0);
	UT_ASSERT_EQ(admission_checks + wal_clock_reads + inserts + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_apply_requires_original_writer_context_before_wal)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	f.input.writer_context_valid = false;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_apply_rejects_replacement_t3_with_same_buffer_generation)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	original_writer_context.ref.acquisition_generation++;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_page_recapture_drops_original_writer_context)
{
	QualifiedXminFixture f;
	ResourceXWriterUseContext zero = {0};
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	UT_ASSERT(f.input.writer_context_valid);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f.input));
	UT_ASSERT(!f.input.writer_context_valid);
	UT_ASSERT_EQ(f.input.writer_object_revision, 0);
	UT_ASSERT(!f.input.writer_object_checked);
	UT_ASSERT(memcmp(&f.input.writer_context, &zero, sizeof(zero)) == 0);
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
}
UT_TEST(test_xmin_batch_rejects_replacement_original_writer_before_wal)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	original_writer_context.ref.acquisition_generation++;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_single_batch_keep_original_writer_object_fence)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	for (int batch = 0; batch < 2; batch++)
		for (int which = 0; which < 2; which++) {
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			if (which) f.input.writer_object_checked = false;
			else f.input.writer_object_revision++;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
}

int
main(void)
{
	UT_PLAN(49);
	UT_RUN(test_xmin_single_batch_keep_original_writer_object_fence);
	UT_RUN(test_page_recapture_drops_original_writer_context);
	UT_RUN(test_xmin_batch_rejects_replacement_original_writer_before_wal);
	UT_RUN(test_xmin_apply_requires_original_writer_context_before_wal);
	UT_RUN(test_xmin_apply_rejects_replacement_t3_with_same_buffer_generation);
	UT_RUN(test_xmin_single_and_batch_require_full_target_not_census_only);
	UT_RUN(test_xmin_target_admission_keeps_original_census_recheck_last);
	UT_RUN(test_xmin_full_target_error_keeps_borrowed_resources_before_wal);
	UT_RUN(test_xmin_bad_final_authority_never_checks_full_target);
	UT_RUN(test_prune_native_plan_and_version);
	UT_RUN(test_shared_freeze_refuses_before_tuple_or_wal);
	UT_RUN(test_vacuum_native_unused_and_version);
	UT_RUN(test_nonshared_native_maintenance);
	UT_RUN(test_prune_hint_only_versions_actual_change);
	UT_RUN(test_prune_hint_nonshared_keeps_native_behavior);
	UT_RUN(test_prune_hint_no_change_has_no_version_or_wal);
	UT_RUN(test_prune_hint_missing_identity_refuses_before_mutation);
	UT_RUN(test_invalid_identity_refuses_before_mutation);
	UT_RUN(test_freeze_preserves_native_transaction_sanity_gate);
	UT_RUN(test_qualified_freeze_kernel_changes_only_planned_headers);
	UT_RUN(test_exact_xmin_plan_preserves_xmax_through_native_wal_and_redo);
	UT_RUN(test_qualified_xmin_apply_wal_flush_and_redo);
	UT_RUN(test_xmin_apply_requires_original_backend_pin_before_page_or_writer_use);
	UT_RUN(test_xmin_batch_requires_original_backend_pin_without_repin_or_write);
	UT_RUN(test_qualified_xmin_rejects_missing_or_changed_ownership);
	UT_RUN(test_qualified_xmin_rejects_stale_physical_row_and_bounds);
	UT_RUN(test_qualified_xmin_flush_error_propagates_after_native_write);
	UT_RUN(test_freeze_native_wal_dedup_and_redo);
	UT_RUN(test_freeze_kernel_rejects_invalid_space_before_write);
	UT_RUN(test_vacuum_native_entry_copies_prelock_identity);
	UT_RUN(test_vacuum_native_entry_refuses_missing_identity);
	UT_RUN(test_explicit_shared_freeze_requests_before_content_locks);
	UT_RUN(test_ordinary_or_nonshared_vacuum_does_not_request);
	UT_RUN(test_deferred_request_preserves_shared_freeze_restriction);
	UT_RUN(test_qualified_xmin_rejects_recapture_with_old_proof_version);
	UT_RUN(test_xmin_apply_rejects_unbounded_cutoffs_before_native_write);
	UT_RUN(test_xmin_batch_rejects_unbounded_cutoffs_before_native_write);
	UT_RUN(test_xmin_apply_rejects_unbound_original_identity_before_wal);
	UT_RUN(test_xmin_batch_later_origin_epoch_mismatch_is_atomic);
	UT_RUN(test_xmin_batch_one_wal_fence_fullxid_horizon_and_redo);
	UT_RUN(test_xmin_batch_later_unqualified_target_changes_nothing);
	UT_RUN(test_xmin_batch_rejects_duplicate_physical_rows);
	UT_RUN(test_xmin_batch_runtime_bounds_and_empty_reject);
	UT_RUN(test_xmin_batch_later_stale_row_rejects_before_fence);
	UT_RUN(test_xmin_batch_rejects_ambiguous_native_horizon);
	UT_RUN(test_xmin_batch_final_authority_refusal_changes_nothing);
	UT_RUN(test_xmin_single_and_batch_reject_unknown_or_behind_wal_clock);
	UT_RUN(test_xmin_single_and_batch_wal_clock_accept_time_equal_or_ahead);
	UT_RUN(test_xmin_batch_flush_error_keeps_borrowed_resources);
	UT_DONE();
	return ut_failed_count != 0;
}
