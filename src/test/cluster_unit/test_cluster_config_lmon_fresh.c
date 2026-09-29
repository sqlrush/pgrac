/*-------------------------------------------------------------------------
 *
 * test_cluster_config_lmon_fresh.c
 *    Original LMON fresh-duty branches with explicit authority/IO fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_lmon_fresh.c
 * NOTES
 *    Wrap tick/handlers are the whole production source. The auto-restore
 *    entry body is extracted verbatim; its private state and downstream
 *    restore-point operation are fixtures, not live backup/durability proof.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"
#include "cluster/cluster_backup.h"
#include "cluster/cluster_config_producers.h"
#include "../../backend/cluster/cluster_xid_wrap_barrier.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_catalog = true;
bool cluster_xid_wrap_barrier_force = true, cluster_enable_pitr_restore_points = true;
int cluster_node_id = 0, cluster_pitr_restore_point_interval_ms = 1000;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool fresh, marked, done, settled, authority, recovery;
static uint64 acknowledgments;
static uint32 peer_mask;
static unsigned stamps, admissions, disables, sends, creates, locks;
static ClusterNodeInfo node;
ClusterConf *ClusterConfShmem;
static ClusterConf conf;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

/* Only fields touched by the verbatim auto-entry body, not a shared ABI. */
static struct {
	LWLockPadded lock;
	pg_atomic_uint32 commit_fence_active;
	struct {
		bool in_progress;
	} status;
	TimestampTz last_auto_restore_point_at;
	uint64 auto_restore_point_seq;
} backup_fixture;
static __typeof__(backup_fixture) *cluster_backup_state = &backup_fixture;

bool
cluster_config_producers_fresh_allowed(ClusterConfigProducerStage stage)
{
	UT_ASSERT_EQ(stage, CLUSTER_CONFIG_PRODUCERS_QUIET);
	return fresh;
}
bool
cluster_xid_wrap_barrier_passed(void)
{
	return done;
}
bool
cluster_xid_wrap_barrier_marked(void)
{
	return marked;
}
void
cluster_xid_wrap_barrier_set_done(void)
{
	done = true;
}
void
cluster_xid_wrap_barrier_set_marked(void)
{
	marked = true;
}
uint64
cluster_xid_wrap_barrier_ack_bitmap(void)
{
	return acknowledgments;
}
void
cluster_xid_wrap_barrier_note_ack(int32 id)
{
	acknowledgments |= UINT64CONST(1) << id;
}
uint64
cluster_xid_stripe_cluster_max_hwm(void)
{
	return 1;
}
FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(1);
}
bool
cluster_xid_authority_read(ClusterXidAuthorityHeader *out)
{
	memset(out, 0, sizeof(*out));
	return authority;
}
void
cluster_xid_authority_mark_native_raw_reused(void)
{
	stamps++;
}
bool
cluster_xid_authority_raw_reused_settled(void)
{
	return settled;
}
bool
cluster_xid_authority_epoch_gate_admitted_settled(void)
{
	return settled;
}
void
cluster_xid_authority_mark_epoch_gate_admitted(void)
{
	admissions++;
}
void
cluster_cr_native_prehistory_disable(void)
{
	disables++;
}
bool
cluster_sf_peer_supports_xid_authority_flock(int32 id)
{
	return id == 1;
}
bool
cluster_sf_peer_supports_xid_native_disable(int32 id)
{
	return id == 1;
}
uint32
cluster_sinval_compute_alive_peer_mask(void)
{
	return peer_mask;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 id)
{
	return id == 1 ? &node : NULL;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *bytes, uint32 len)
{
	UT_ASSERT(type == PGRAC_IC_MSG_XID_NATIVE_DISABLE
			  || type == PGRAC_IC_MSG_XID_NATIVE_DISABLE_ACK);
	UT_ASSERT(dest == 1 && bytes != NULL && len == sizeof(ClusterXidNativeDisablePayload));
	sends++;
	return CLUSTER_IC_SEND_DONE;
}
bool
errstart(int level, const char *domain)
{
	(void)level;
	(void)domain;
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
	(void)file;
	(void)line;
	(void)func;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
void
FlushErrorState(void)
{}
void
pg_re_throw(void)
{
	abort();
}
bool
RecoveryInProgress(void)
{
	return recovery;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 10000000;
}
bool
TimestampDifferenceExceeds(TimestampTz start, TimestampTz stop, int ms)
{
	return stop - start > (int64)ms * 1000;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &backup_fixture.lock.lock && mode == LW_EXCLUSIVE && locks == 0);
	locks++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &backup_fixture.lock.lock && locks == 1);
	locks--;
}
static void
cluster_backup_make_cluster_restore_point(const char *name, const char *path,
										  ClusterRestorePoint *out)
{
	UT_ASSERT(name != NULL && path == NULL && out != NULL && locks == 0);
	creates++;
}
#include "test_cluster_config_auto_restore.inc"

static void
reset(void)
{
	fresh = authority = settled = true;
	marked = done = recovery = false;
	memset(&conf, 0, sizeof(conf));
	conf.node_count = 1;
	ClusterConfShmem = &conf;
	acknowledgments = 0;
	peer_mask = 2;
	stamps = admissions = disables = sends = creates = locks = 0;
	cluster_enabled = cluster_shared_catalog = cluster_xid_wrap_barrier_force = true;
	cluster_enable_pitr_restore_points = true;
	cluster_pitr_restore_point_interval_ms = 1000;
	memset(&backup_fixture, 0, sizeof(backup_fixture));
	pg_atomic_init_u32(&backup_fixture.commit_fence_active, 0);
	cluster_backup_state = &backup_fixture;
}
UT_TEST(quiet_does_not_initiate_wrap)
{
	reset();
	fresh = false;
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT_EQ(stamps, 0);
	UT_ASSERT(!marked && !done);
	UT_ASSERT_EQ(sends, 0);
}
UT_TEST(original_marked_wrap_finishes_while_quiet)
{
	reset();
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT(marked && !done && sends == 1);
	fresh = false;
	acknowledgments = 2;
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT(done && admissions == 1);
}
UT_TEST(post_done_durable_repair_is_not_paused)
{
	reset();
	fresh = false;
	marked = done = true;
	settled = false;
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT_EQ(admissions, 1);
	UT_ASSERT(done);
}
UT_TEST(incoming_disable_and_ack_remain_runnable)
{
	ClusterXidNativeDisablePayload p;
	ClusterICEnvelope env = { 0 };
	reset();
	fresh = false;
	wb_build_payload(&p);
	p.node_id = 1;
	p.crc = wb_payload_crc(&p);
	env.source_node_id = 1;
	env.payload_length = sizeof(p);
	wb_disable_handler(&env, &p);
	UT_ASSERT(disables == 1 && sends == 1);
	wb_ack_handler(&env, &p);
	UT_ASSERT_EQ(acknowledgments, 2);
}
UT_TEST(unmanaged_original_wrap_preconditions_preserved)
{
	reset();
	cluster_xid_wrap_barrier_force = false;
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT_EQ(stamps, 0);
	cluster_xid_wrap_barrier_force = true;
	authority = false;
	cluster_xid_wrap_barrier_lmon_tick();
	UT_ASSERT_EQ(stamps, 0);
}
UT_TEST(quiet_does_not_create_automatic_restore_point)
{
	reset();
	fresh = false;
	cluster_backup_maybe_auto_restore_point();
	UT_ASSERT_EQ(creates, 0);
	UT_ASSERT_EQ(backup_fixture.auto_restore_point_seq, 0);
	UT_ASSERT_EQ(backup_fixture.last_auto_restore_point_at, 0);
}
UT_TEST(original_auto_restore_and_existing_preconditions_remain)
{
	reset();
	cluster_backup_maybe_auto_restore_point();
	UT_ASSERT_EQ(creates, 1);
	UT_ASSERT_EQ(backup_fixture.auto_restore_point_seq, 1);
	reset();
	conf.node_count = 2;
	cluster_backup_maybe_auto_restore_point();
	UT_ASSERT_EQ(creates, 0);
	reset();
	recovery = true;
	cluster_backup_maybe_auto_restore_point();
	UT_ASSERT_EQ(creates, 0);
}
int
main(void)
{
	UT_PLAN(7);
	UT_RUN(quiet_does_not_initiate_wrap);
	UT_RUN(original_marked_wrap_finishes_while_quiet);
	UT_RUN(post_done_durable_repair_is_not_paused);
	UT_RUN(incoming_disable_and_ack_remain_runnable);
	UT_RUN(unmanaged_original_wrap_preconditions_preserved);
	UT_RUN(quiet_does_not_create_automatic_restore_point);
	UT_RUN(original_auto_restore_and_existing_preconditions_remain);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
