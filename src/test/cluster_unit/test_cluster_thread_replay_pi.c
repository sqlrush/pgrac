/* PGRAC: actual replay_one_block with scalar page-apply and buffer/smgr
 * fixtures. This tests base selection across records, not physical redo.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_apply.h"
#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_xnode_lever.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config, cluster_past_image = true;
static unsigned storage_value, pi_value, pi_reads, storage_reads, writes, discards;
static bool pi_present;
typedef struct ClusterThreadMissingRels ClusterThreadMissingRels;

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
	*rl = (RelFileLocator){ 1663, 1, 9000 };
	*fork = MAIN_FORKNUM;
	*block = 0;
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
	return true;
}
BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber fork)
{
	(void)rel;
	(void)fork;
	return 1;
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
missing_add(ClusterThreadMissingRels *missing, const RelFileLocator *rl)
{
	(void)missing;
	(void)rl;
	abort();
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

static void
two_records(bool shared)
{
	PGAlignedBlock page;
	ClusterThreadReplayStats stats = { 0 };
	cluster_shared_config = shared;
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
int
main(void)
{
	UT_PLAN(2);
	UT_RUN(shared_never_reuses_unretired_pi_for_next_record);
	UT_RUN(legacy_retired_pi_yields_to_storage_for_next_record);
	UT_DONE();
	return ut_failed_count != 0;
}
