/* PGRAC: execute actual precommit/publication callbacks with transport fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include "../../backend/utils/cache/inval.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true;
bool cluster_shared_catalog;
int cluster_node_id = 0;
static ClusterConf conf = { .node_count = 2 };
ClusterConf *ClusterConfShmem = &conf;
static int observed_error, send_calls, prepare_calls, clear_calls, sync_calls;
static ClusterSinvalAckResult send_result;
static jmp_buf error_jump;

bool
errstart(int level, const char *domain)
{
	observed_error = level;
	if (level >= ERROR)
		longjmp(error_jump, 1);
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}
int
errmsg(const char *format, ...)
{
	return 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
void
ForceSyncCommit(void)
{
	sync_calls++;
}
void
cluster_sinval_prepare_commit(void)
{
	prepare_calls++;
}
void
cluster_sinval_clear_commit(void)
{
	clear_calls++;
}
ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	Assert(n > 0);
	send_calls++;
	return send_result;
}

UT_TEST(test_postcommit_error_policy_matches_publication_mode)
{
	SharedInvalidationMessage msg = { 0 };
	cluster_shared_catalog = false;
	send_result = CLUSTER_SINVAL_ACK_TIMEOUT;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(observed_error, 0);
	cluster_shared_catalog = true;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(observed_error, PANIC);
	observed_error = 0;
	send_result = CLUSTER_SINVAL_ACK_DONE;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(observed_error, 0);
}

UT_TEST(test_precommit_snapshots_only_shared_pending_catalog_changes)
{
	TransInvalidationInfo info = { 0 };
	transInvalInfo = &info;
	cluster_shared_catalog = true;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 0);
	info.ii.CurrentCmdInvalidMsgs.nextmsg[CatCacheMsgs] = 1;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(sync_calls, 1);
	cluster_shared_catalog = false;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(clear_calls, 3);
	transInvalInfo = NULL;
}

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_postcommit_error_policy_matches_publication_mode);
	UT_RUN(test_precommit_snapshots_only_shared_pending_catalog_changes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
