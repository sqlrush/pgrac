/*-------------------------------------------------------------------------
 * test_cluster_checkpoint_cleanup.c
 *   PGRAC: execute the three production checkpoint cleanup bodies with
 *   deterministic failures at their resource-release boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_checkpoint_cleanup.c
 * NOTES
 *   PGRAC unit fixture for real cleanup functions, not replacement policies.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlogreader.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_reconfig.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "../../backend/cluster/cluster_recovery_anchor_private.h"
#include "utils/memutils.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#include "test_cluster_checkpoint_cleanup_types.inc"

enum { CLOSE, CF, WALR, HISTORY, ANCHOR, OPERATION, INSTALL, ACTIONS };
static unsigned calls[ACTIONS];
static ClusterControlRootResult failures[ACTIONS];
static unsigned memory_frees, reader_frees, log_count;
static StartupInstallWork work;

void
pfree(void *pointer)
{
	UT_ASSERT(pointer != NULL);
	++memory_frees;
}

void
XLogReaderFree(XLogReaderState *reader)
{
	UT_ASSERT(reader != NULL);
	++reader_frees;
}

void
cluster_wal_prefix_identity_free(ClusterWalPrefixIdentity **identity)
{
	*identity = NULL;
}

static int
fixture_close(int fd)
{
	UT_ASSERT(fd >= 10 && fd < 15);
	++calls[CLOSE];
	return failures[CLOSE] == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? 0 : -1;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	++calls[CF];
	return failures[CF] == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? CLUSTER_CF_RELEASE_CONFIRMED
														   : CLUSTER_CF_RELEASE_UNCONFIRMED;
}

uint64
cluster_cf_owner_cookie(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	return 41;
}

ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT(*guard != NULL);
	++calls[WALR];
	if (failures[WALR] != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	*guard = NULL;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

ClusterControlRootResult
cluster_wal_history_discard(ClusterWalHistoryStage *stage)
{
	unsigned action = stage->owner_pid == 1 ? HISTORY : INSTALL;
	++calls[action];
	return failures[action];
}

ClusterControlRootResult
cluster_recovery_anchor_v2_discard(ClusterRecoveryAnchorStageV2 *stage)
{
	UT_ASSERT_EQ(stage->owner_pid, 2);
	++calls[ANCHOR];
	return failures[ANCHOR];
}

ClusterControlRootResult
cluster_wal_startup_discard(ClusterWalStartupStage *stage)
{
	UT_ASSERT_EQ(stage->owner_pid, 3);
	++calls[OPERATION];
	return failures[OPERATION];
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	UT_ASSERT_EQ(level, LOG);
	++log_count;
	return true;
}

int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{}

#define close fixture_close
#include "test_cluster_checkpoint_cleanup.inc"
#undef close

static void
reset_fixture(void)
{
	CheckpointV2Work *scan = &work.checkpoint.scan;
	memset(&work, 0, sizeof(work));
	memset(calls, 0, sizeof(calls));
	memset(failures, 0, sizeof(failures));
	memory_frees = reader_frees = log_count = 0;
	scan->close_history = (void *)1;
	scan->checkpoint_history = (void *)2;
	scan->checkpoint_history_check = (void *)3;
	scan->wal_reader = (void *)4;
	scan->walr = (void *)5;
	scan->cf_mode = ExclusiveLock;
	for (unsigned i = 0; i < lengthof(scan->wal_segments); ++i)
		scan->wal_segments[i] = 10 + i;
	for (unsigned i = 0; i < lengthof(scan->wal_dirs); ++i)
		scan->wal_dirs[i] = 12 + i;
	scan->checkpoint_history_stage.owner_pid = 1;
	scan->stage.owner_pid = 2;
	work.checkpoint.operation_stage.owner_pid = 3;
	work.history_stage.owner_pid = 4;
}

static ClusterControlRootResult
run_cleanup(unsigned depth, ClusterControlRootResult primary)
{
	if (depth == 0)
		return checkpoint_v2_cleanup(&work.checkpoint.scan, primary);
	if (depth == 1)
		return startup_checkpoint_cleanup(&work.checkpoint, primary);
	return startup_install_cleanup(&work, primary);
}

static bool
primary_is_hard(ClusterControlRootResult result)
{
	return result == CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
		   || result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
}

UT_TEST(test_cleanup_failure_overrides_only_retryable_primary)
{
	const ClusterControlRootResult primaries[]
		= { CLUSTER_CONTROL_ROOT_OK_PRIMARY,		CLUSTER_CONTROL_ROOT_CAS_CONFLICT,
			CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE,	CLUSTER_CONTROL_ROOT_RECONFIG_WAIT,
			CLUSTER_CONTROL_ROOT_ADMISSION_PENDING, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH,
			CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN };
	for (unsigned depth = 0; depth < 3; ++depth)
		for (unsigned p = 0; p < lengthof(primaries); ++p)
			for (unsigned action = 0; action <= ANCHOR + depth; ++action) {
				ClusterControlRootResult hard = action == CF || action == WALR
													? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN
													: CLUSTER_CONTROL_ROOT_IO_ERROR;
				reset_fixture();
				failures[action] = hard;
				UT_ASSERT_EQ(run_cleanup(depth, primaries[p]),
							 primary_is_hard(primaries[p]) ? primaries[p] : hard);
				for (unsigned a = 0; a <= ANCHOR + depth; ++a)
					UT_ASSERT_EQ(calls[a], a == CLOSE ? 5 : 1);
			}
}

UT_TEST(test_discard_refusal_is_hard_and_cleanup_is_once)
{
	const ClusterControlRootResult refusals[]
		= { CLUSTER_CONTROL_ROOT_CAS_CONFLICT, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE,
			CLUSTER_CONTROL_ROOT_RECONFIG_WAIT, CLUSTER_CONTROL_ROOT_ADMISSION_PENDING };
	for (unsigned depth = 0; depth < 3; ++depth)
		for (unsigned r = 0; r < lengthof(refusals); ++r)
			for (unsigned action = HISTORY; action <= ANCHOR + depth; ++action) {
				reset_fixture();
				failures[action] = refusals[r];
				UT_ASSERT_EQ(run_cleanup(depth, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE),
							 CLUSTER_CONTROL_ROOT_IO_ERROR);
				(void)run_cleanup(depth, CLUSTER_CONTROL_ROOT_IO_ERROR);
				for (unsigned a = 0; a <= ANCHOR + depth; ++a)
					UT_ASSERT_EQ(calls[a], a == CLOSE ? 5 : 1);
				UT_ASSERT_EQ(memory_frees, 3);
				UT_ASSERT_EQ(reader_frees, 1);
			}
}

UT_TEST(test_first_hard_cleanup_wins_and_later_failures_are_logged)
{
	reset_fixture();
	failures[INSTALL] = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	failures[OPERATION] = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	failures[CLOSE] = CLUSTER_CONTROL_ROOT_IO_ERROR;
	failures[CF] = failures[WALR] = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	UT_ASSERT_EQ(run_cleanup(2, CLUSTER_CONTROL_ROOT_OK_PRIMARY),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	for (unsigned a = 0; a < ACTIONS; ++a)
		UT_ASSERT_EQ(calls[a], a == CLOSE ? 5 : 1);
	UT_ASSERT(log_count >= 5);
	(void)run_cleanup(2, CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	for (unsigned a = 0; a < ACTIONS; ++a)
		UT_ASSERT_EQ(calls[a], a == CLOSE ? 5 : 1);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_cleanup_failure_overrides_only_retryable_primary);
	UT_RUN(test_discard_refusal_is_hard_and_cleanup_is_once);
	UT_RUN(test_first_hard_cleanup_wins_and_later_failures_are_logged);
	UT_DONE();
	return ut_failed_count != 0;
}
