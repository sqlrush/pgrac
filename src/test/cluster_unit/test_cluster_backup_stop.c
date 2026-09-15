/* Author: SqlRush <sqlrush@gmail.com> */
/* A148: original backup state/producers. No live backup, disk or socket I/O. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "../../backend/cluster/cluster_backup.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMON;
bool cluster_enabled = true;
int cluster_node_id = 0;
volatile sig_atomic_t InterruptPending;
volatile uint32 CritSectionCount;
volatile uint32 InterruptHoldoffCount;
static ClusterBackupSharedState region;
static unsigned lock_depth, send_count;
static bool foreign_lock;
static SessionBackupState native_backup;
static bool admit_new_command = true;
static unsigned admission_calls, wake_count;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	UT_ASSERT(modifies_data);
	admission_calls++;
	return admit_new_command;
}

void
cluster_lmon_wakeup(void)
{
	wake_count++;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", condition, file, line);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(strcmp(name, "pgrac cluster backup") == 0);
	UT_ASSERT_EQ(size, sizeof(region));
	*found = false;
	return &region;
}
void
LWLockInitialize(LWLock *lock, int tranche)
{
	(void)lock;
	(void)tranche;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &region.lock.lock);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	lock_depth++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &region.lock.lock);
	UT_ASSERT_EQ(lock_depth, 1);
	lock_depth--;
}
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	if (foreign_lock || lock_depth)
		callback(&region.lock.lock, LW_SHARED, arg);
}
SessionBackupState
get_backup_status(void)
{
	return native_backup;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	(void)function;
	(void)arg;
}
void
ProcessInterrupts(void)
{
	UT_ASSERT(false);
}
bool
errstart_cold(int level, const char *domain)
{
	(void)level;
	(void)domain;
	return true;
}
int
errcode(int code)
{
	(void)code;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	fprintf(stderr, "unexpected ERROR at %s:%d %s\n", file, line, function);
	abort();
}
void
pg_usleep(long microsec)
{
	(void)microsec;
	UT_ASSERT(false);
}
void
MemoryContextDelete(MemoryContext context)
{
	(void)context;
}
void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty)
{
	UT_ASSERT_EQ(duty, CLUSTER_LMON_DUTY_BACKUP);
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 bytes)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_BACKUP_ACK);
	UT_ASSERT_EQ(dest, 2);
	UT_ASSERT(payload != NULL);
	UT_ASSERT_EQ(bytes, sizeof(ClusterBackupWireAck));
	send_count++;
	return CLUSTER_IC_SEND_DONE; /* native transport boundary, not global drain proof */
}
static void
fresh_region(void)
{
	cluster_backup_shmem_init();
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	native_backup = SESSION_BACKUP_NONE;
	foreign_lock = false;
	lock_depth = send_count = 0;
	admit_new_command = true;
	admission_calls = wake_count = 0;
	cluster_backup_lmon_restore_point_held = false;
	cluster_backup_lmon_restore_point_prepare_pending = false;
	cluster_backup_lmon_state = NULL;
	cluster_backup_lmon_context = NULL;
	cluster_backup_lmon_tablespace_map = NULL;
	cluster_backup_pending_commit_registered = false;
	cluster_backup_pending_commit_exit_registered = false;
}
UT_TEST(test_original_region_role_and_lock_boundary)
{
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	fresh_region();
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_LMON;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	foreign_lock = true;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	foreign_lock = false;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_original_commit_owner_exit_does_not_discharge_fence)
{
	fresh_region();
	cluster_backup_pending_commit_enter();
	UT_ASSERT_EQ(pg_atomic_read_u32(&region.pending_commit_count), 1);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	/* Original CAS publication; no redo/backup work is called. */
	UT_ASSERT_EQ(cluster_backup_restore_point_fence_start(), 1000000);
	cluster_backup_pending_commit_abort(0, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&region.pending_commit_count), 0);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_restore_point_fence_end();
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_original_peer_reply_and_private_prepare_are_independent)
{
	ClusterBackupWireAck reply = { .request_id = 27, .op = CLUSTER_BACKUP_WIRE_OP_STOP };
	fresh_region();
	cluster_backup_lmon_queue_peer_reply(&reply, 2);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(region.peer_reply_pending);
	UT_ASSERT_EQ(send_count, 0);
	cluster_backup_lmon_send_peer_reply();
	UT_ASSERT_EQ(send_count, 1);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	/* Original prepare body publication boundary: no backup I/O performed. */
	cluster_backup_lmon_restore_point_prepare_pending = true;
	cluster_backup_lmon_prepare_request.request_id = 28;
	cluster_backup_lmon_set_prepare_pending(true);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_lmon_set_prepare_pending(false); /* shared empty is insufficient */
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_lmon_clear_prepare_pending();
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	cluster_backup_lmon_restore_point_held = true;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_lmon_reset_context();
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_original_session_completion_and_retained_history)
{
	static BackupState backup;
	fresh_region();
	region.status.in_progress = true; /* frontend publication boundary */
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_mark_native_stopped(NULL);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(region.status.stopped_at, 1000000);
	cluster_backup_lmon_state = &backup;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_backup_lmon_reset_context();
	native_backup = SESSION_BACKUP_RUNNING;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	native_backup = SESSION_BACKUP_NONE;
	region.have_manifest = true;
	region.next_request_id = 99;
	region.restore_point_count = CLUSTER_BACKUP_RESTORE_POINT_MAX;
	region.last_auto_restore_point_at = 100;
	region.auto_restore_point_seq = 88;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(region.next_request_id, 99);
	UT_ASSERT_EQ(region.auto_restore_point_seq, 88);
}
UT_TEST(test_all_peer_obligations_and_late_invalid_override_pending)
{
	const char *domain, *reason;
	uint64 key;
	fresh_region();
	region.coordinator_request.request_id = 41;
	/* Original coordinator locked publication boundary; per-peer completion
  * masks deliberately remain after the original request returns. */
	for (int node = 0; node < CLUSTER_MAX_NODES; node++) {
		cluster_backup_bitmap_set(region.coordinator_expected, node);
		UT_ASSERT_EQ(cluster_backup_normal_stop_poll(&domain, &key, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(key, node);
		cluster_backup_bitmap_set(node % 2 ? region.coordinator_nacked : region.coordinator_acked,
								  node);
		UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	}
	region.peer_command_pending = true;
	region.peer_command.request_id = 42;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u32(&region.commit_fence_active, 2);
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	pg_atomic_write_u32(&region.commit_fence_active, 0);
	region.peer_reply_pending = true;
	region.peer_reply_dest = CLUSTER_MAX_NODES;
	UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(region.peer_command_pending);
	UT_ASSERT(region.peer_reply_pending);
}

UT_TEST(test_sealed_new_backup_commands_do_not_enter_original_mailbox)
{
	const ClusterBackupWireOp ops[] = { CLUSTER_BACKUP_WIRE_OP_START,
										CLUSTER_BACKUP_WIRE_OP_STOP,
										CLUSTER_BACKUP_WIRE_OP_ABORT,
										CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT,
										CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT_PREPARE,
										CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT_RELEASE };
	for (unsigned i = 0; i < lengthof(ops); i++) {
		ClusterBackupWireRequest request = { .magic = CLUSTER_BACKUP_IC_MAGIC,
											 .version = CLUSTER_BACKUP_IC_VERSION,
											 .op = ops[i],
											 .request_id = 61 + i,
											 .coordinator_node_id = 2 };
		ClusterICEnvelope env = { .source_node_id = 2, .payload_length = sizeof(request) };
		ClusterBackupWireRequest empty = { 0 };
		fresh_region();
		cluster_backup_wire_request_compute_crc(&request);
		UT_ASSERT(cluster_backup_wire_request_valid(&request));
		admit_new_command = false;
		cluster_backup_request_handler(&env, &request);
		UT_ASSERT(!region.peer_command_pending);
		UT_ASSERT(memcmp(&region.peer_command, &empty, sizeof(empty)) == 0);
		UT_ASSERT_EQ(admission_calls, 1);
		UT_ASSERT_EQ(wake_count, 0);
		UT_ASSERT_EQ(lock_depth, 0);
		UT_ASSERT_EQ(send_count, 0);
		admit_new_command = true;
		cluster_backup_request_handler(&env, &request);
		UT_ASSERT(region.peer_command_pending);
		UT_ASSERT(memcmp(&region.peer_command, &request, sizeof(request)) == 0);
		admit_new_command = false;
		/* An occupied mailbox is not another producer. Its original consumer
		 * still owns the admitted command; there is no cached-success shortcut. */
		cluster_backup_request_handler(&env, &request);
		UT_ASSERT_EQ(admission_calls, 2);
		UT_ASSERT(memcmp(&region.peer_command, &request, sizeof(request)) == 0);
		UT_ASSERT_EQ(cluster_backup_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
	}
}

UT_TEST(test_backup_validation_precedes_seal_and_old_reply_remains_owned)
{
	ClusterBackupWireRequest request = { .magic = CLUSTER_BACKUP_IC_MAGIC,
										 .version = CLUSTER_BACKUP_IC_VERSION,
										 .op = CLUSTER_BACKUP_WIRE_OP_START,
										 .request_id = 71,
										 .coordinator_node_id = 2 };
	ClusterICEnvelope env = { .source_node_id = 2, .payload_length = sizeof(request) };
	ClusterBackupWireAck reply = { .request_id = 71, .op = CLUSTER_BACKUP_WIRE_OP_STOP };
	fresh_region();
	admit_new_command = false;
	cluster_backup_wire_request_compute_crc(&request);
	env.source_node_id = 3;
	cluster_backup_request_handler(&env, &request);
	env.source_node_id = 2;
	request.crc ^= 1;
	cluster_backup_request_handler(&env, &request);
	UT_ASSERT_EQ(admission_calls, 0);
	UT_ASSERT(!region.peer_command_pending);
	cluster_backup_lmon_queue_peer_reply(&reply, 2);
	cluster_backup_lmon_send_peer_reply();
	UT_ASSERT_EQ(send_count, 1);
	UT_ASSERT(!region.peer_reply_pending);
	UT_ASSERT_EQ(admission_calls, 0);
}
int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_original_region_role_and_lock_boundary);
	UT_RUN(test_original_commit_owner_exit_does_not_discharge_fence);
	UT_RUN(test_original_peer_reply_and_private_prepare_are_independent);
	UT_RUN(test_original_session_completion_and_retained_history);
	UT_RUN(test_all_peer_obligations_and_late_invalid_override_pending);
	UT_RUN(test_sealed_new_backup_commands_do_not_enter_original_mailbox);
	UT_RUN(test_backup_validation_precedes_seal_and_old_reply_remains_owned);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
