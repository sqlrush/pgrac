/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_enqueue.c
 *	  Unit tests for the CF enqueue layer (spec-5.6 Db1/Db2).
 *
 *	  U1 covers the pure resid encoder.  The Db2 tests stub the GES
 *	  seven-step / S5 promote / S6 release entry points so the
 *	  correctness-critical result-to-action mapping in cluster_cf_lock /
 *	  cluster_cf_unlock can be exercised deterministically: a grant must
 *	  register a holder and the matching release must drain exactly that CF
 *	  holder; an OK_NATIVE (cluster layer inactive) must NOT register a
 *	  holder, so release is a no-op; and any failure must fail closed
 *	  without claiming the lock.  The real cross-node grant/release is the
 *	  2-node TAP (t/288).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Db1/Db2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_sequence.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

AuxProcType MyAuxProcType = NotAnAuxProcess;
BackendType MyBackendType = B_INVALID;
static ClusterNormalStopPollResult g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
ClusterNormalStopPollResult
cluster_cf_normal_stop_shared_poll(bool post_checkpoint, const char **reason)
{
	(void)post_checkpoint;
	if (reason)
		*reason = "FIXTURE_CF_SHARED";
	return g_shared_stop_result;
}
bool cluster_controlfile_shared_authority = false;
int cluster_node_id = 0;
char *cluster_config_file = NULL;
static int g_node_count = 1;

int
cluster_conf_node_count(void)
{
	return g_node_count;
}

FILE *
AllocateFile(const char *name, const char *mode)
{
	return fopen(name, mode);
}

int
FreeFile(FILE *file)
{
	return fclose(file);
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* RF-ROOT P6 (L4/L5 diag refs): cluster_cf_enqueue.o samples the phase word,
 * the clean-leave write gate, the GRD shard/master surface and the process
 * context; the pure unit pins them inert so the binary stays standalone. */
bool IsUnderPostmaster = false;
int MyProcPid = 0;

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
cluster_clean_leave_node_refuses_writes(void)
{
	return false;
}

int
cluster_current_phase(void)
{
	return 0;
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

int32
cluster_grd_lookup_master(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

/* ---- GES substrate stubs (settable outcomes) ---- */
static ClusterLockAcquireResult g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static int g_s6_count = 0;
static uint8 g_s6_last_resid_type = 0;
/* spec-5.6 Dc4b: capture what cluster_cf_lock threaded into the request. */
static int g_last_timeout_ms = -999;
static uint32 g_last_wait_event = 0xFFFFFFFFu;

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	ClusterLockAcquireRequest *mut = (ClusterLockAcquireRequest *)req;

	/* spec-5.6 Dc4b: record the CF acquire's timeout + wait-event override. */
	g_last_timeout_ms = req->timeout_ms;
	g_last_wait_event = req->wait_event;

	/* simulate S3 fill_request_holder filling the holder + request id */
	mut->holder.node_id = 1;
	mut->holder.procno = 42;
	mut->request_id = 7;
	return g_seven_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{
	(void)req;
	return g_s5_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	g_s6_count++;
	g_s6_last_resid_type = req->resid.type;
	return g_s6_result;
}

/* spec-5.6 Dc4: cluster_cf_lock bumps CF acquire/fail-closed counters;
 * cluster_cf_stats.o is not linked here, so a no-op stub satisfies the link
 * (the counter mechanism is covered by test_cluster_cf_stats). */
void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

/* spec-5.6 Dc4b: cluster_cf_lock reads this GUC into req.timeout_ms; cluster_
 * guc.o is not linked here, so define it locally. */
int cluster_cf_enqueue_timeout_ms = 30000;

/* spec-5.6 increment (iii) follow-up: join-readonly is now a cross-process CF
 * shmem flag (cluster_cf_stats.o, not linked here).  A stateful stub keeps the
 * set->get behaviour the write-permission test exercises. */
static bool g_join_ro = false;
void
cluster_cf_stats_set_join_readonly(bool on)
{
	g_join_ro = on;
}
bool
cluster_cf_stats_get_join_readonly(void)
{
	return g_join_ro;
}

/* R18: model the real actor-bound shared phase while testing the enqueue-local
 * permission and eligibility wrapper without linking cluster_cf_stats.o. */
static ClusterCfOwnerEorPhase g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;

ClusterCfOwnerEorPhase
cluster_cf_owner_eor_phase_read(void)
{
	return g_owner_eor_phase;
}

bool
cluster_cf_owner_eor_phase_install(void)
{
	if (!AmStartupProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_EMPTY)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_INSTALLED;
	return true;
}

bool
cluster_cf_owner_eor_phase_activate(void)
{
	if (!AmCheckpointerProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_INSTALLED)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_ACTIVE;
	return true;
}

bool
cluster_cf_owner_eor_phase_done(void)
{
	if (!AmCheckpointerProcess() || g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_ACTIVE)
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_DONE;
	return true;
}

bool
cluster_cf_owner_eor_phase_clear(void)
{
	if (!AmStartupProcess()
		|| (g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_INSTALLED
			&& g_owner_eor_phase != CLUSTER_CF_OWNER_EOR_DONE))
		return false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

/* ======================================================================
 * U1 -- CF resid encoding
 * ====================================================================== */
UT_TEST(test_cf_resid_encode)
{
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	cluster_cf_resid_encode(&r);

	UT_ASSERT_EQ(r.field1, 0);
	UT_ASSERT_EQ(r.field2, 0);
	UT_ASSERT_EQ(r.field3, 0);
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_CF_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF1);
	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* ======================================================================
 * Db2 -- a cluster grant registers a holder; release drains the CF holder
 * ====================================================================== */
UT_TEST(test_lock_grant_then_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;
	g_s6_last_resid_type = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held(ExclusiveLock)); /* held while locked */
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock)); /* released */

	UT_ASSERT_EQ(g_s6_count, 1);			  /* exactly one release */
	UT_ASSERT_EQ(g_s6_last_resid_type, 0xF1); /* of the CF resid, not a locktag */
}

/* ======================================================================
 * Db3 -- write permission: held CF X, or the bootstrap authority window
 * ====================================================================== */
UT_TEST(test_held_and_write_permitted)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;

	/* nothing held -> no write permitted */
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_write_permitted());

	/* holding CF X permits a write */
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_write_permitted());

	/* the bootstrap window permits a write without a held CF X */
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_set_bootstrap_authority(false);
	UT_ASSERT(!cluster_cf_write_permitted());

	/*
	 * Join read-only (increment ii) is an orthogonal signal: it marks an
	 * attaching node whose recovery writes are skipped, and it must NEVER by
	 * itself grant write permission (a join node is a reader, not a writer).
	 */
	UT_ASSERT(!cluster_cf_join_readonly());
	cluster_cf_set_join_readonly(true);
	UT_ASSERT(cluster_cf_join_readonly());
	UT_ASSERT(!cluster_cf_write_permitted()); /* join != write permission */
	cluster_cf_set_join_readonly(false);
	UT_ASSERT(!cluster_cf_join_readonly());

	/*
	 * The process-local bring-up write-skip is what the chokepoint consults; it
	 * is orthogonal to write permission (skipping a write is not permission to
	 * write) and independent of the node-wide join flag.
	 */
	UT_ASSERT(!cluster_cf_write_skip());
	cluster_cf_set_write_skip(true);
	UT_ASSERT(cluster_cf_write_skip());
	UT_ASSERT(!cluster_cf_write_permitted()); /* write-skip != write permission */
	cluster_cf_set_write_skip(false);
	UT_ASSERT(!cluster_cf_write_skip());
}

/* ======================================================================
 * R18 -- exact OWNER->EOR eligibility, local authority and abort boundary
 * ====================================================================== */
UT_TEST(test_owner_eor_handoff_gates_and_lifecycle)
{
	cluster_controlfile_shared_authority = true;
	g_node_count = 1;
	g_join_ro = false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	cluster_cf_set_bootstrap_authority(false);
	cluster_cf_owner_eor_abort();

	/* INSTALL is Startup-only and exact-one-node only. */
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	MyAuxProcType = StartupProcess;
	g_node_count = 2;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	g_node_count = 1;
	g_join_ro = true;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	g_join_ro = false;
	cluster_controlfile_shared_authority = false;
	UT_ASSERT(!cluster_cf_owner_eor_install());
	cluster_controlfile_shared_authority = true;
	UT_ASSERT(cluster_cf_owner_eor_install());
	UT_ASSERT(cluster_cf_in_bootstrap_window());
	UT_ASSERT(cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_INSTALLED);

	/* Simulate the separate checkpointer process: phase alone grants no write. */
	cluster_cf_set_bootstrap_authority(false);
	UT_ASSERT(!cluster_cf_write_permitted());

	/* CONSUME requires actor, EOR, authority, exact role, JOIN=false and identity. */
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true)); /* still Startup */
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_consume(false, true));
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, false));
	cluster_controlfile_shared_authority = false;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	cluster_controlfile_shared_authority = true;
	g_node_count = 2;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	g_node_count = 1;
	g_join_ro = true;
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));
	g_join_ro = false;
	UT_ASSERT(cluster_cf_owner_eor_consume(true, true));
	UT_ASSERT(cluster_cf_owner_eor_local_active());
	UT_ASSERT(cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_ACTIVE);

	UT_ASSERT(cluster_cf_owner_eor_complete());
	UT_ASSERT(!cluster_cf_owner_eor_local_active());
	UT_ASSERT(!cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_DONE);

	/* Original Startup permission remains local there until the final close. */
	MyAuxProcType = StartupProcess;
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT(cluster_cf_owner_eor_close());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_EMPTY);
	UT_ASSERT(!cluster_cf_in_bootstrap_window());
	UT_ASSERT(!cluster_cf_write_permitted());

	cluster_controlfile_shared_authority = false;
	MyAuxProcType = NotAnAuxProcess;
}

UT_TEST(test_owner_eor_disabled_seed_uses_exact_declared_node)
{
	char path[] = "/tmp/pgrac-cf-seed-XXXXXX";
	int fd;
	FILE *f;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	f = fdopen(fd, "w");
	UT_ASSERT(f != NULL);
	UT_ASSERT(fprintf(f, "[cluster]\nname = pgrac\n\n[node.0]\n"
						 "interconnect_addr = 127.0.0.1:6433\n")
			  > 0);
	UT_ASSERT_EQ(fclose(f), 0);

	g_node_count = 0;
	cluster_node_id = 0;
	cluster_config_file = path;
	UT_ASSERT(cluster_cf_exactly_one_declared_node());

	cluster_node_id = 1;
	UT_ASSERT(!cluster_cf_exactly_one_declared_node());

	UT_ASSERT_EQ(unlink(path), 0);
	cluster_config_file = NULL;
	cluster_node_id = 0;
	g_node_count = 1;
}

UT_TEST(test_owner_eor_abort_retains_active_and_blocks_retry)
{
	cluster_controlfile_shared_authority = true;
	g_node_count = 1;
	g_join_ro = false;
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_INSTALLED;
	cluster_cf_set_bootstrap_authority(false);
	MyAuxProcType = CheckpointerProcess;

	UT_ASSERT(cluster_cf_owner_eor_consume(true, true));
	UT_ASSERT(cluster_cf_owner_eor_local_active());
	cluster_cf_owner_eor_abort();
	UT_ASSERT(!cluster_cf_owner_eor_local_active());
	UT_ASSERT(!cluster_cf_write_permitted());
	UT_ASSERT_EQ(g_owner_eor_phase, CLUSTER_CF_OWNER_EOR_ACTIVE);
	UT_ASSERT(!cluster_cf_owner_eor_consume(true, true));

	/* Test-process cleanup only; product abort/close never performs this edge. */
	g_owner_eor_phase = CLUSTER_CF_OWNER_EOR_EMPTY;
	cluster_controlfile_shared_authority = false;
	MyAuxProcType = NotAnAuxProcess;
}

/* ======================================================================
 * Db2 -- OK_NATIVE (cluster layer inactive) registers no holder
 * ====================================================================== */
UT_TEST(test_lock_native_no_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	cluster_cf_unlock(ShareLock);

	UT_ASSERT_EQ(g_s6_count, 0); /* uncoordinated -> no S6 */
}

UT_TEST(test_confirmed_release_requires_clusterwide_s6_success)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));

	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
}

UT_TEST(test_native_hold_never_becomes_clusterwide_authority)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_NOT_HELD);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(g_s6_count, 0);
}

/* ======================================================================
 * Db2 -- a GES failure fails closed and registers nothing
 * ====================================================================== */
UT_TEST(test_lock_failclosed_timeout)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	g_s6_count = 0;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock); /* not held -> no-op */

	UT_ASSERT_EQ(g_s6_count, 0);
}

/* ======================================================================
 * Db2 -- a granted reservation that fails the S5 promote fails closed
 * ====================================================================== */
UT_TEST(test_lock_s5_fail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	g_s6_count = 0;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock); /* not held -> no-op */
	UT_ASSERT_EQ(g_s6_count, 0);

	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* reset */
}

/* ======================================================================
 * Db2 -- a try-conflict (NOT_AVAIL) does not claim the lock
 * ====================================================================== */
UT_TEST(test_lock_notavail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;

	UT_ASSERT(!cluster_cf_lock(ShareLock));
}

/* ======================================================================
 * Dc4b -- the CF acquire carries cluster.cf_enqueue_timeout_ms +
 * WAIT_EVENT_CLUSTER_CF_ENQUEUE so the GES wait is bounded + observable
 * ====================================================================== */
UT_TEST(test_lock_timeout_and_wait_event)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_last_timeout_ms = -999;
	g_last_wait_event = 0xFFFFFFFFu;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock);

	/* CF threads the GUC timeout + its own wait-event label into the request */
	UT_ASSERT_EQ(g_last_timeout_ms, cluster_cf_enqueue_timeout_ms);
	UT_ASSERT_EQ(g_last_wait_event, (uint32)WAIT_EVENT_CLUSTER_CF_ENQUEUE);
}

UT_TEST(test_stop_cf_original_holds_and_confirmed_retirement)
{
	const char *reason;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	cluster_cf_set_bootstrap_authority(false);
	cluster_cf_set_write_skip(false);
	cluster_cf_owner_eor_abort();
	g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
	g_seven_result = g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	for (int i = 0; i < 2; i++) {
		LOCKMODE mode = i ? ExclusiveLock : ShareLock;
		int releases;
		UT_ASSERT(cluster_cf_lock(mode));
		releases = g_s6_count;
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(g_s6_count, releases);
		UT_ASSERT(cluster_cf_held(mode));
		g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_UNCONFIRMED);
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_PENDING);
		g_shared_stop_result = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
		g_shared_stop_result = CLUSTER_NORMAL_STOP_READY;
		g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(cluster_cf_unlock_confirmed(mode), CLUSTER_CF_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	}
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_BACKEND;
	MyAuxProcType = NotAnAuxProcess;
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_stop_cf_separates_old_skip_from_post_checkpoint_permission)
{
	const char *reason;
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
	cluster_cf_set_write_skip(true); /* previous EOR's scoped value */
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(false, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(reason, "CF_POST_CHECKPOINT_WRITE_SKIP") == 0);
	UT_ASSERT(cluster_cf_write_skip());
	/* The original next normal checkpoint, not the observer, clears it. */
	cluster_cf_set_write_skip(false);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(true, &reason), CLUSTER_NORMAL_STOP_READY);
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT_EQ(cluster_cf_normal_stop_poll(false, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(cluster_cf_in_bootstrap_window());
	cluster_cf_set_bootstrap_authority(false);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_cf_resid_encode);
	UT_RUN(test_lock_grant_then_release);
	UT_RUN(test_held_and_write_permitted);
	UT_RUN(test_owner_eor_handoff_gates_and_lifecycle);
	UT_RUN(test_owner_eor_disabled_seed_uses_exact_declared_node);
	UT_RUN(test_owner_eor_abort_retains_active_and_blocks_retry);
	UT_RUN(test_lock_native_no_release);
	UT_RUN(test_confirmed_release_requires_clusterwide_s6_success);
	UT_RUN(test_native_hold_never_becomes_clusterwide_authority);
	UT_RUN(test_lock_failclosed_timeout);
	UT_RUN(test_lock_s5_fail);
	UT_RUN(test_lock_notavail);
	UT_RUN(test_lock_timeout_and_wait_event);
	UT_RUN(test_stop_cf_original_holds_and_confirmed_retirement);
	UT_RUN(test_stop_cf_separates_old_skip_from_post_checkpoint_permission);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
