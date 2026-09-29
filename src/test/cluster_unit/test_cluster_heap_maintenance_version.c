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
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id, wal_level = WAL_LEVEL_REPLICA, NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock current_page, before_page, wal_image;
static RelationData relation_data;
static FormData_pg_class relform;
static ClusterSpaceIdentity space_identity;
static RfPageVersionEdgeEntryV1 edge;
static uint64 next_token, edge_token;
static unsigned inserts, edges, dirties, hints;
static uint8 expected_info;
static bool begun, expecting_error, tx_committed;
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
	return false;
}
bool
IsCatalogRelation(Relation rel)
{
	UT_ASSERT(rel == &relation_data);
	return false;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
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
	return tx_committed;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
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
		UT_ASSERT_EQ(((xl_heap_freeze_page *)data)->nplans, 1);
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
	UT_ASSERT_EQ(flags, REGBUF_STANDARD);
	memcpy(wal_image.data, current_page.data, BLCKSZ);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT(begun && data != NULL && length > 0);
	UT_ASSERT_EQ(id, 0);
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
	UT_ASSERT_EQ(rmgr, RM_HEAP2_ID);
	UT_ASSERT_EQ(info, expected_info);
	begun = false;
	inserts++;
	return UINT64_C(0x9000);
}

static void page_verify_redirects(Page page);
#include "test_cluster_heap_prune_helpers.inc"
#include "test_cluster_heap_freeze_helpers.inc"

/* Execute the full native freeze body with the planned pre-lock identity. */
static void
run_freeze(Relation rel, Buffer buffer, TransactionId snapshotConflictHorizon,
		   HeapTupleFreeze *tuples, int ntuples,
		   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_heap_freeze_body.inc"

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
	CritSectionCount = 0;
	edges = inserts = dirties = hints = 0;
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
UT_TEST(test_freeze_native_tuple_and_version)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	execute(XLOG_HEAP2_FREEZE_PAGE, &space_identity);
	check_publication(true);
	for (int i = 1; i <= 2; i++)
		UT_ASSERT(HeapTupleHeaderXminFrozen(
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
UT_TEST(test_prune_hint_only_has_no_version_or_wal)
{
	reset(XLOG_HEAP2_PRUNE, true);
	run_prune(false, NULL);
	UT_ASSERT_EQ(hints, 1);
	UT_ASSERT_EQ(dirties + inserts + edges, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
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
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
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
	struct {
		Relation rel;
		bool versioned;
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
	struct {
		Relation rel;
		bool versioned;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;
	reset(XLOG_HEAP2_VACUUM, true);
	identity_reads = 0;
	identity_ready = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_prune_native_plan_and_version);
	UT_RUN(test_freeze_native_tuple_and_version);
	UT_RUN(test_vacuum_native_unused_and_version);
	UT_RUN(test_nonshared_native_maintenance);
	UT_RUN(test_prune_hint_only_has_no_version_or_wal);
	UT_RUN(test_invalid_identity_refuses_before_mutation);
	UT_RUN(test_freeze_preserves_native_transaction_sanity_gate);
	UT_RUN(test_vacuum_native_entry_copies_prelock_identity);
	UT_RUN(test_vacuum_native_entry_refuses_missing_identity);
	UT_DONE();
	return ut_failed_count != 0;
}
