/* PGRAC: actual replay_one_block with scalar page-apply and buffer/smgr
 * fixtures. This tests base selection across records, not physical redo.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xact.h"
#include "access/heapam_xlog.h"
#include "catalog/storage_xlog.h"
#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_apply.h"
#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_xnode_lever.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config, cluster_shared_catalog, cluster_past_image = true;
static unsigned storage_value, pi_value, pi_reads, storage_reads, writes, discards;
static bool pi_present;
static bool relation_exists = true;
static BlockNumber relation_blocks = 1, requested_block;
static ForkNumber requested_fork = MAIN_FORKNUM;
static DecodedXLogRecord records[3];
static unsigned record_count, next_record, terminal_calls;
static RelFileLocator relation_locator = { 1663, 1, 9000 };

void *
palloc(Size n)
{
	return malloc(n);
}
void *
repalloc(void *p, Size n)
{
	return realloc(p, n);
}
void
pfree(void *p)
{
	free(p);
}

#undef elog
#define elog(level, ...)                                                                           \
	do {                                                                                           \
		UT_ASSERT((level) < ERROR);                                                                \
	} while (0)
#include "test_cluster_thread_replay_missing.inc"

void
ExceptionalCondition(const char *c, const char *f, int line)
{
	fprintf(stderr, "%s %s:%d\n", c, f, line);
	abort();
}
bool
XLogRecGetBlockTagExtended(XLogReaderState *reader, uint8 id, RelFileLocator *rl, ForkNumber *fork,
						   BlockNumber *block, Buffer *buffer)
{
	(void)reader;
	(void)id;
	(void)buffer;
	*rl = relation_locator;
	*fork = requested_fork;
	*block = requested_block;
	return true;
}
int
cluster_smgr_which_for(RelFileLocator rl, BackendId backend)
{
	(void)rl;
	(void)backend;
	return 1;
}
SMgrRelation
smgropen(RelFileLocator rl, BackendId backend)
{
	(void)rl;
	(void)backend;
	return (SMgrRelation)(uintptr_t)1;
}
bool
smgrexists(SMgrRelation rel, ForkNumber fork)
{
	(void)rel;
	(void)fork;
	return relation_exists;
}
BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber fork)
{
	(void)rel;
	(void)fork;
	return relation_blocks;
}
void
smgrread(SMgrRelation rel, ForkNumber fork, BlockNumber block, void *page)
{
	(void)rel;
	(void)fork;
	(void)block;
	storage_reads++;
	memcpy(page, &storage_value, sizeof(storage_value));
}
void
smgrwrite(SMgrRelation rel, ForkNumber fork, BlockNumber block, const void *page, bool skip)
{
	(void)rel;
	(void)fork;
	(void)block;
	(void)skip;
	writes++;
	memcpy(&storage_value, page, sizeof(storage_value));
}
void
PageSetChecksumInplace(Page page, BlockNumber block)
{
	(void)page;
	(void)block;
}
bool
cluster_bufmgr_block_is_pi(BufferTag tag)
{
	(void)tag;
	return pi_present;
}
bool
cluster_bufmgr_snapshot_pi_block(BufferTag tag, char *page, SCN *scn)
{
	(void)tag;
	pi_reads++;
	memcpy(page, &pi_value, sizeof(pi_value));
	*scn = 5;
	return true;
}
bool
cluster_bufmgr_discard_pi_block(BufferTag tag)
{
	(void)tag;
	discards++;
	if (cluster_shared_config)
		return false; /* exact retirement owner is mandatory */
	pi_present = false;
	return true;
}
void
cluster_lever_h_note_recovery_base(bool used)
{
	(void)used;
}
ClusterThreadApplyResult
cluster_thread_apply_record_to_page(XLogReaderState *reader, uint8 id, char *page, XLogRecPtr *end)
{
	unsigned value;
	(void)reader;
	(void)id;
	memcpy(&value, page, sizeof(value));
	value++;
	memcpy(page, &value, sizeof(value));
	*end = 100;
	return CLUSTER_THREADAPPLY_APPLIED;
}
ClusterThreadApplyResult
cluster_pi_thread_apply_record_to_page(XLogReaderState *reader, uint8 id, char *page, SCN scn,
									   XLogRecPtr *end)
{
	(void)scn;
	return cluster_thread_apply_record_to_page(reader, id, page, end);
}
static void
touched_add(ClusterThreadTouchedRels *touched, const RelFileLocator *rl, ForkNumber fork)
{
	(void)touched;
	(void)rl;
	(void)fork;
}
static void
cluster_thread_recovery_page_judge(XLogReaderState *reader, uint8 id, const RelFileLocator *rl,
								   ForkNumber fork, BlockNumber block)
{
	(void)reader;
	(void)id;
	(void)rl;
	(void)fork;
	(void)block;
}
#undef ereport
#define ereport(level, rest) ((void)0)
#include "test_cluster_thread_replay_pi.inc"

XLogRecord *
XLogReadRecord(XLogReaderState *reader, char **error)
{
	*error = NULL;
	if (next_record == record_count)
		return NULL;
	reader->record = &records[next_record++];
	reader->ReadRecPtr = reader->record->lsn;
	reader->EndRecPtr = reader->record->next_lsn;
	return &reader->record->header;
}

void
cluster_remote_xact_apply(int origin, XLogReaderState *reader, bool online)
{
	xl_xact_parsed_commit commit;
	UT_ASSERT(origin == 1 && online && XLogRecGetRmid(reader) == RM_XACT_ID);
	UT_ASSERT(ParseCommitRecord(XLogRecGetInfo(reader), (xl_xact_commit *)XLogRecGetData(reader),
								XLogRecGetDataLen(reader), &commit));
	UT_ASSERT(TransactionIdIsNormal(XLogRecGetXid(reader)) && SCN_VALID(commit.scn));
	UT_ASSERT(!cluster_remote_xact_terminal_blocked_shared_catalog(commit.nsubxacts, commit.xinfo,
																   XACT_XINFO_HAS_TWOPHASE));
	terminal_calls++;
	/* Controlled durable DROP boundary, retaining the zero-length MAIN.
	 * Actual DROP I/O and window-close authority are not supplied here. */
}

static void
cluster_thread_recovery_prepared_judge(XLogReaderState *reader, uint8 info)
{
	(void)reader;
	(void)info;
}
static void
cluster_thread_recovery_projection_judge(XLogReaderState *reader)
{
	(void)reader;
}
#include "test_cluster_thread_replay_stream.inc"

static void
two_records(bool shared)
{
	PGAlignedBlock page;
	ClusterThreadReplayStats stats = { 0 };
	cluster_shared_config = shared;
	relation_exists = true;
	relation_blocks = 1;
	requested_fork = MAIN_FORKNUM;
	requested_block = 0;
	storage_value = pi_value = pi_reads = storage_reads = writes = discards = 0;
	pi_present = true;
	UT_ASSERT(replay_one_block(NULL, 0, page.data, 1, NULL, NULL, &stats));
	UT_ASSERT_EQ(storage_value, 1);
	UT_ASSERT(replay_one_block(NULL, 0, page.data, 1, NULL, NULL, &stats));
	UT_ASSERT_EQ(storage_value, 2);
	UT_ASSERT_EQ(stats.blocks_applied, 2);
	UT_ASSERT_EQ(writes, 2);
}
UT_TEST(shared_never_reuses_unretired_pi_for_next_record)
{
	two_records(true);
	UT_ASSERT_EQ(pi_reads, 0);
	UT_ASSERT_EQ(discards, 0);
	UT_ASSERT_EQ(storage_reads, 2);
	UT_ASSERT(pi_present);
}
UT_TEST(legacy_retired_pi_yields_to_storage_for_next_record)
{
	two_records(false);
	UT_ASSERT_EQ(pi_reads, 1);
	UT_ASSERT_EQ(discards, 1);
	UT_ASSERT_EQ(storage_reads, 1);
	UT_ASSERT(!pi_present);
}

static ClusterThreadRecResult
run_retained_main_window(bool drop, bool other_locator, unsigned structural, bool visibility,
						 ClusterThreadReplayStats *stats)
{
	XLogReaderState reader = { 0 };
	ClusterThreadVisCtx vis = { visibility, 1 };
	PGAlignedBlock payload, structure;
	xl_xact_xinfo xinfo = { XACT_XINFO_HAS_SCN | (drop ? XACT_XINFO_HAS_RELFILELOCATORS : 0) };
	xl_xact_scn commit_scn = { 30 };
	RelFileLocator dropped = relation_locator;
	int nrels = 1;
	char *p;

	memset(records, 0, sizeof(records));
	memset(&payload, 0, sizeof(payload));
	memset(&structure, 0, sizeof(structure));
	cluster_shared_config = cluster_shared_catalog = true;
	storage_reads = writes = pi_reads = discards = terminal_calls = next_record = 0;
	pi_present = true;
	record_count = structural ? 3 : 2;
	for (unsigned i = 0; i < record_count; i++) {
		records[i].lsn = 100 + i * 100;
		records[i].next_lsn = records[i].lsn + 100;
		records[i].header.xl_scn = 10 + i;
		records[i].header.xl_xid = 42;
		records[i].max_block_id = -1;
	}
	records[0].header.xl_rmid = RM_HEAP_ID;
	records[0].header.xl_info = XLOG_HEAP_UPDATE;
	records[0].max_block_id = 0;
	if (structural) {
		records[1].header.xl_rmid = RM_SMGR_ID;
		records[1].main_data = structure.data;
		if (structural == 1) {
			xl_smgr_create *create = (xl_smgr_create *)structure.data;
			create->rlocator = relation_locator;
			create->rlocator.relNumber++;
			create->forkNum = MAIN_FORKNUM;
			records[1].header.xl_info = XLOG_SMGR_CREATE;
			records[1].main_data_len = sizeof(*create);
		} else {
			xl_smgr_truncate *truncate = (xl_smgr_truncate *)structure.data;
			truncate->rlocator = relation_locator;
			truncate->blkno = 0;
			truncate->flags = SMGR_TRUNCATE_HEAP;
			records[1].header.xl_info = XLOG_SMGR_TRUNCATE;
			records[1].main_data_len = sizeof(*truncate);
		}
	}
	if (other_locator)
		dropped.relNumber++;
	p = payload.data + MinSizeOfXactCommit;
	memcpy(p, &xinfo, sizeof(xinfo));
	p += sizeof(xinfo);
	if (drop) {
		memcpy(p, &nrels, sizeof(nrels));
		p += sizeof(nrels);
		memcpy(p, &dropped, sizeof(dropped));
		p += sizeof(dropped);
	}
	memcpy(p, &commit_scn, sizeof(commit_scn));
	p += sizeof(commit_scn);
	records[record_count - 1].header.xl_rmid = RM_XACT_ID;
	records[record_count - 1].header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	records[record_count - 1].main_data = payload.data;
	records[record_count - 1].main_data_len = p - payload.data;
	return cluster_thread_recovery_replay_stream_ex(&reader, records[record_count - 1].next_lsn,
													&vis, NULL, stats);
}

UT_TEST(zero_main_update_then_terminal_drop_retries_without_page_io)
{
	ClusterThreadReplayStats stats;
	relation_exists = true;
	relation_blocks = requested_block = 0;
	requested_fork = MAIN_FORKNUM;
	/* SQL DROP and relation-replacing TRUNCATE both drop the old locator
	 * in the terminal record; the retained MAIN survives both attempts. */
	for (unsigned truncate = 0; truncate < 2; truncate++)
		for (unsigned retry = 0; retry < 2; retry++) {
			UT_ASSERT_EQ(run_retained_main_window(true, false, truncate, true, &stats),
						 CLUSTER_THREADREC_DONE);
			UT_ASSERT_EQ(stats.blocks_missing_deferred, 1);
			UT_ASSERT_EQ(terminal_calls, 1);
			UT_ASSERT_EQ(storage_reads + writes + pi_reads + discards, 0);
		}
}

UT_TEST(zero_main_requires_exact_later_drop_and_visibility)
{
	ClusterThreadReplayStats stats;
	relation_exists = true;
	relation_blocks = requested_block = 0;
	requested_fork = MAIN_FORKNUM;
	for (unsigned scenario = 0; scenario < 4; scenario++) {
		/* No DROP; wrong locator; SMGR truncate without DROP; data-only. */
		UT_ASSERT_EQ(run_retained_main_window(scenario == 1 || scenario == 3, scenario == 1,
											  scenario == 2 ? 2 : 0, scenario != 3, &stats),
					 CLUSTER_THREADREC_BLOCKED);
		UT_ASSERT_EQ(stats.blocks_missing_deferred, scenario == 3 ? 0 : 1);
		UT_ASSERT_EQ(storage_reads + writes + pi_reads + discards, 0);
	}
}

UT_TEST(nonzero_eof_and_empty_auxiliary_stay_immediately_blocked)
{
	ClusterThreadReplayStats stats;
	relation_exists = true;
	for (unsigned auxiliary = 0; auxiliary < 2; auxiliary++) {
		requested_fork = auxiliary ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM;
		relation_blocks = requested_block = auxiliary ? 0 : 1;
		UT_ASSERT_EQ(run_retained_main_window(true, false, false, true, &stats),
					 CLUSTER_THREADREC_BLOCKED);
		UT_ASSERT_EQ(stats.blocks_missing_deferred, 0);
		UT_ASSERT_EQ(terminal_calls + storage_reads + writes + pi_reads + discards, 0);
	}
}
int
main(void)
{
	UT_PLAN(5);
	UT_RUN(shared_never_reuses_unretired_pi_for_next_record);
	UT_RUN(legacy_retired_pi_yields_to_storage_for_next_record);
	UT_RUN(zero_main_update_then_terminal_drop_retries_without_page_io);
	UT_RUN(zero_main_requires_exact_later_drop_and_visibility);
	UT_RUN(nonzero_eof_and_empty_auxiliary_stay_immediately_blocked);
	UT_DONE();
	return ut_failed_count != 0;
}
