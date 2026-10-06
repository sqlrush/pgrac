/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_build_version.c
 *    Execute the native bulk writer with exact SPACE/version helpers.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_build_version.c
 *
 * NOTES
 *    PGRAC: SMgr and WAL I/O are explicit fixtures. The native write body,
 *    page and version encoders execute unchanged, not a policy model.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/nbtree.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

#include "test_cluster_btree_build_state.inc"

static RelationData relation;
static FormData_pg_class relform;
static SMgrRelationData smgr;
static ClusterSpaceIdentity identity;
static BTWriteState state;
static PGAlignedBlock pages[64], source, pending[64], image, zero;
static RfPageVersionEdgeEntryV1 entries[64];
static uint64 token;
static XLogRecPtr flushed;
static unsigned count, writes, reads, wal_count, legacy_wal, edge_count, flush_count, free_count;
static unsigned sync_count;
static bool begun, expecting_error, fail_flush, checksums;
static BlockNumber reserved;
static unsigned reservations;
static bool fail_reserve;
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
SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	(void)backend;
	return &smgr;
}
void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	*owner = rel;
}
void *
palloc_aligned(Size size, Size alignment, int flags)
{
	UT_ASSERT_EQ(size, BLCKSZ);
	UT_ASSERT_EQ(alignment, PG_IO_ALIGN_SIZE);
	UT_ASSERT(flags & MCXT_ALLOC_ZERO);
	memset(zero.data, 0, BLCKSZ);
	return zero.data;
}
void
pfree(void *ptr)
{
	bool known = ptr == source.data;
	for (unsigned i = 0; i < lengthof(pending); i++)
		known |= ptr == pending[i].data;
	UT_ASSERT(known);
	free_count++;
}
bool
DataChecksumsEnabled(void)
{
	return checksums;
}
void
smgrextend(SMgrRelation rel, ForkNumber forknum, BlockNumber block, const void *data,
		   bool skip_sync)
{
	Page page = (Page)data;
	UT_ASSERT(rel == &smgr && forknum == MAIN_FORKNUM && skip_sync);
	UT_ASSERT_EQ(block, count);
	UT_ASSERT(block < lengthof(pages));
	if (cluster_shared_config)
		UT_ASSERT(block < reserved);
	if (cluster_shared_config && !PageIsNew(page))
		UT_ASSERT(flushed >= PageGetLSN(page) && flushed != InvalidXLogRecPtr);
	memcpy(pages[block].data, page, BLCKSZ);
	count++;
	writes++;
}
void
smgrwrite(SMgrRelation rel, ForkNumber forknum, BlockNumber block, const void *data, bool skip_sync)
{
	Page page = (Page)data;
	UT_ASSERT(rel == &smgr && forknum == MAIN_FORKNUM && skip_sync);
	UT_ASSERT(block < count);
	if (cluster_shared_config)
		UT_ASSERT(flushed >= PageGetLSN(page) && flushed != InvalidXLogRecPtr);
	memcpy(pages[block].data, page, BLCKSZ);
	writes++;
}
void
smgrread(SMgrRelation rel, ForkNumber forknum, BlockNumber block, void *page)
{
	UT_ASSERT(rel == &smgr && forknum == MAIN_FORKNUM && block < count);
	memcpy(page, pages[block].data, BLCKSZ);
	reads++;
}
XLogRecPtr
log_newpage(RelFileLocator *locator, ForkNumber forknum, BlockNumber block, Page page,
			bool standard)
{
	UT_ASSERT(RelFileLocatorEquals(*locator, relation.rd_locator));
	UT_ASSERT(forknum == MAIN_FORKNUM && block < lengthof(pages) && standard);
	legacy_wal++;
	PageSetLSN(page, 50);
	return 50;
}
SCN
cluster_scn_advance(void)
{
	return ++token;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun);
	begun = true;
}
void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber forknum, BlockNumber block,
				  Page page, uint8 flags)
{
	UT_ASSERT(begun && id == 0 && forknum == MAIN_FORKNUM && block < lengthof(pages));
	UT_ASSERT(block < reserved);
	UT_ASSERT(RelFileLocatorEquals(*locator, relation.rd_locator));
	UT_ASSERT_EQ(flags, REGBUF_FORCE_IMAGE);
	memcpy(image.data, page, BLCKSZ);
}
void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *edge, uint8 n)
{
	UT_ASSERT(begun && n == 1 && edge_count < lengthof(entries));
	UT_ASSERT_EQ(result, token);
	entries[edge_count++] = edge[0];
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(rmgr, RM_XLOG_ID);
	UT_ASSERT_EQ(info, XLOG_FPI);
	begun = false;
	return 100 + ++wal_count;
}
void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 100 + wal_count);
	if (fail_flush)
		elog(ERROR, "fixture WAL fsync failure");
	flushed = lsn;
	flush_count++;
}

/* The real reservation owner has its own WAL/buffer fixture. Here its
 * durable-grant boundary checks the native bulk caller's ordering. */
static bool
fixture_reserve(const ClusterSpaceIdentity *id, BlockNumber first, uint32 want)
{
	UT_ASSERT(want > 0);
	if (fail_reserve || first != reserved || memcmp(id, &identity, sizeof(identity)) != 0)
		return false;
	reserved += want;
	reservations++;
	return true;
}
#define cluster_space_reserve_exact fixture_reserve
#include "test_cluster_btree_build_writer.inc"
#undef cluster_space_reserve_exact

void
smgrimmedsync(SMgrRelation rel, ForkNumber forknum)
{
	UT_ASSERT(rel == &smgr && forknum == MAIN_FORKNUM);
	UT_ASSERT_EQ(state.pending_count, 0);
	UT_ASSERT_EQ(free_count, wal_count + legacy_wal);
	sync_count++;
}

static void
_bt_uppershutdown(BTWriteState *wstate, void *unused)
{
	/* Tuple construction is outside this I/O-order test. */
	UT_ASSERT(wstate == &state && unused == NULL);
}

static void
finish_build(void)
{
	BTWriteState *wstate = &state;
	void *state = NULL;
#include "test_cluster_btree_build_finish.inc"
}

static void
reset(bool shared)
{
	memset(&state, 0, sizeof(state));
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&identity, 0, sizeof(identity));
	memset(pages, 0, sizeof(pages));
	memset(entries, 0, sizeof(entries));
	count = writes = reads = wal_count = legacy_wal = edge_count = flush_count = free_count = 0;
	sync_count = 0;
	reserved = reservations = 0;
	fail_reserve = false;
	begun = expecting_error = fail_flush = checksums = false;
	flushed = InvalidXLogRecPtr;
	token = 200;
	cluster_shared_config = shared;
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relation.rd_smgr = &smgr;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	identity.key.system_identifier = 11;
	identity.key.database_incarnation = 12;
	memset(identity.key.storage_uuid, 13, 16);
	identity.key.locator = relation.rd_locator;
	memset(identity.incarnation, 30, 16);
	identity.sequence = 1;
	identity.operation = 40;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	state.index = &relation;
	state.btws_use_wal = true;
	state.versioned = shared;
	state.identity = identity;
	PageInit(source.data, BLCKSZ, sizeof(BTPageOpaqueData));
	BTPageGetOpaque(source.data)->btpo_flags = BTP_LEAF;
}

UT_TEST(test_new_btree_page_is_versioned_before_data_write)
{
	reset(true);
	_bt_blwritepage(&state, source.data, 0);
	finish_build();
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(entries[0].before_kind, RF_PAGE_STATE_ABSENT);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 201);
	UT_ASSERT_EQ(PageGetLSN(pages[0].data), 101);
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(legacy_wal, 0);
	UT_ASSERT_EQ(free_count, 1);
	UT_ASSERT_EQ(sync_count, 1);
}
UT_TEST(test_out_of_order_build_has_exact_zero_predecessor)
{
	reset(true);
	pending[0] = source;
	_bt_blwritepage(&state, pending[0].data, 3);
	UT_ASSERT_EQ(count, 0);
	PageInit(source.data, BLCKSZ, sizeof(BTPageOpaqueData));
	BTPageGetOpaque(source.data)->btpo_flags = BTP_META;
	_bt_blwritepage(&state, source.data, 0);
	UT_ASSERT_EQ(count, 4);
	UT_ASSERT_EQ(flush_count, 1);
	finish_build();
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(entries[0].before_kind, RF_PAGE_STATE_ABSENT);
	UT_ASSERT_EQ(entries[1].before_kind, RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 202);
	UT_ASSERT_EQ(((PageHeader)pages[3].data)->pd_block_scn, 201);
	UT_ASSERT_EQ(flush_count, 2);
	UT_ASSERT_EQ(reads, 1);
}
UT_TEST(test_nonshared_keeps_native_build_wal)
{
	reset(false);
	_bt_blwritepage(&state, source.data, 0);
	UT_ASSERT_EQ(legacy_wal, 1);
	UT_ASSERT_EQ(edge_count + flush_count + reads, 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 0);
	UT_ASSERT_EQ(count, 1);
}
UT_TEST(test_unknown_existing_page_is_never_overwritten)
{
	for (int variant = 0; variant < 2; variant++) {
		PGAlignedBlock old, before_source;

		reset(true);
		count = state.btws_pages_written = 1;
		if (variant == 0)
			PageInit(pages[0].data, BLCKSZ, 0);
		else
			pages[0].data[BLCKSZ - 1] = 42;
		old = pages[0];
		before_source = source;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			_bt_blwritepage(&state, source.data, 0);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT(memcmp(old.data, pages[0].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(before_source.data, source.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(wal_count + edge_count + writes + free_count, 0);
		UT_ASSERT_EQ(token, 200);
	}
}
UT_TEST(test_invalid_identity_allocates_no_version_or_data)
{
	PGAlignedBlock before_source;

	reset(true);
	state.identity.key.database_incarnation++;
	before_source = source;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		_bt_blwritepage(&state, source.data, 0);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(memcmp(before_source.data, source.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(wal_count + edge_count + writes + count + free_count, 0);
	UT_ASSERT_EQ(token, 200);
}
UT_TEST(test_wal_flush_failure_precedes_any_data_or_zero_fill)
{
	reset(true);
	fail_flush = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		_bt_blwritepage(&state, source.data, 3);
		finish_build();
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(writes + count + free_count, 0);
	UT_ASSERT_EQ(state.btws_pages_written, 0);
}
UT_TEST(test_native_checksum_is_applied_to_final_version_and_lsn)
{
	PGAlignedBlock expected;

	reset(true);
	checksums = true;
	_bt_blwritepage(&state, source.data, 0);
	finish_build();
	expected = image;
	PageSetLSN(expected.data, 101);
	PageSetChecksumInplace(expected.data, 0);
	UT_ASSERT(memcmp(expected.data, pages[0].data, BLCKSZ) == 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 201);
}

UT_TEST(test_adjacent_pages_do_not_flush_or_write_one_at_a_time)
{
	reset(true);
	for (unsigned i = 0; i < 3; i++) {
		pending[i] = source;
		_bt_blwritepage(&state, pending[i].data, i);
	}
	UT_ASSERT_EQ(wal_count, 3);
	UT_ASSERT_EQ(flush_count, 0);
	UT_ASSERT_EQ(writes + free_count, 0);
	finish_build();
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(writes, 3);
	UT_ASSERT_EQ(free_count, 3);
	UT_ASSERT_EQ(sync_count, 1);
}

UT_TEST(test_full_batch_and_final_partial_batch_preserve_each_page)
{
	reset(true);
	checksums = true;
	for (unsigned i = 0; i < 40; i++) {
		pending[i] = source;
		_bt_blwritepage(&state, pending[i].data, i);
	}
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(writes, 32);
	UT_ASSERT_EQ(free_count, 32);
	UT_ASSERT_EQ(state.pending_count, 8);
	finish_build();
	UT_ASSERT_EQ(flush_count, 2);
	UT_ASSERT_EQ(writes, 40);
	UT_ASSERT_EQ(free_count, 40);
	UT_ASSERT_EQ(sync_count, 1);
	for (unsigned i = 0; i < 40; i++) {
		UT_ASSERT_EQ(PageGetLSN(pages[i].data), 101 + i);
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, 201 + i);
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_checksum, pg_checksum_page(pages[i].data, i));
	}
}

UT_TEST(test_revisiting_queued_page_cannot_overwrite_it_as_absent)
{
	reset(true);
	pending[0] = source;
	_bt_blwritepage(&state, pending[0].data, 0);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		_bt_blwritepage(&state, source.data, 0);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(free_count, 1);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 201);
}

UT_TEST(test_bulk_reservations_cover_batches_and_backfills)
{
	reset(true);
	for (unsigned i = 0; i < 40; i++) {
		pending[i] = source;
		_bt_blwritepage(&state, pending[i].data, i);
	}
	finish_build();
	UT_ASSERT_EQ(reservations, 2);
	UT_ASSERT_EQ(reserved, 64);
	UT_ASSERT_EQ(writes, 40);
}

UT_TEST(test_bulk_refuses_before_wal_or_data_without_reservation)
{
	for (int variant = 0; variant < 2; variant++) {
		PGAlignedBlock saved;

		reset(true);
		fail_reserve = variant == 0;
		if (variant == 1)
			reserved = 1;
		saved = source;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			_bt_blwritepage(&state, source.data, 3);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT(memcmp(source.data, saved.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(wal_count + edge_count + writes + free_count + reservations, 0);
	}
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_new_btree_page_is_versioned_before_data_write);
	UT_RUN(test_out_of_order_build_has_exact_zero_predecessor);
	UT_RUN(test_nonshared_keeps_native_build_wal);
	UT_RUN(test_unknown_existing_page_is_never_overwritten);
	UT_RUN(test_invalid_identity_allocates_no_version_or_data);
	UT_RUN(test_wal_flush_failure_precedes_any_data_or_zero_fill);
	UT_RUN(test_native_checksum_is_applied_to_final_version_and_lsn);
	UT_RUN(test_adjacent_pages_do_not_flush_or_write_one_at_a_time);
	UT_RUN(test_full_batch_and_final_partial_batch_preserve_each_page);
	UT_RUN(test_revisiting_queued_page_cannot_overwrite_it_as_absent);
	UT_RUN(test_bulk_reservations_cover_batches_and_backfills);
	UT_RUN(test_bulk_refuses_before_wal_or_data_without_reservation);
	UT_DONE();
	return ut_failed_count != 0;
}
