/*-------------------------------------------------------------------------
 *
 * cluster_backup.c
 *	  Cluster-aware backup / restore / PITR SQL surface and shmem state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_backup.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linked in both build modes because
 *	  pg_proc.dat references the SQL symbols unconditionally.
 *	  Spec: spec-6.5-cluster-aware-backup-restore-pitr.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "access/xlogbackup.h"
#include "catalog/catversion.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "cluster/cluster_backup.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "port/atomics.h"

PG_FUNCTION_INFO_V1(pg_cluster_backup_start);
PG_FUNCTION_INFO_V1(pg_cluster_backup_stop);
PG_FUNCTION_INFO_V1(pg_cluster_create_restore_point);
PG_FUNCTION_INFO_V1(cluster_get_backup_state);
PG_FUNCTION_INFO_V1(cluster_get_backup_history);
PG_FUNCTION_INFO_V1(cluster_get_restore_points);
PG_FUNCTION_INFO_V1(cluster_get_pitr_status);

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_wal_thread.h"

typedef struct ClusterBackupSharedState {
	LWLockPadded lock;
	ClusterBackupStatus status;
	bool have_manifest;
	ClusterBackupManifest last_manifest;
	int restore_point_count;
	int restore_point_next;
	ClusterRestorePoint restore_points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
	pg_atomic_uint32 pending_commit_count;
	pg_atomic_uint32 commit_fence_active;

	uint64 next_request_id;
	bool coordinator_send_pending;
	ClusterBackupWireRequest coordinator_request;
	uint8 coordinator_expected[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	uint8 coordinator_backup_peers[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	uint8 coordinator_acked[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	uint8 coordinator_nacked[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	ClusterBackupWireAck coordinator_acks[CLUSTER_MAX_NODES];
	ClusterBackupManifestThread coordinator_peer_threads[CLUSTER_MAX_NODES];
	SCN coordinator_peer_cut_scn[CLUSTER_MAX_NODES];

	bool peer_command_pending;
	ClusterBackupWireRequest peer_command;
	bool peer_reply_pending;
	int32 peer_reply_dest;
	ClusterBackupWireAck peer_reply;
} ClusterBackupSharedState;

static ClusterBackupSharedState *cluster_backup_state = NULL;
static BackupState *cluster_backup_session_state = NULL;
static StringInfo cluster_backup_tablespace_map = NULL;
static MemoryContext cluster_backup_context = NULL;
static BackupState *cluster_backup_lmon_state = NULL;
static StringInfo cluster_backup_lmon_tablespace_map = NULL;
static MemoryContext cluster_backup_lmon_context = NULL;
static bool cluster_backup_pending_commit_registered = false;
static bool cluster_backup_pending_commit_exit_registered = false;

#define CLUSTER_BACKUP_RESTORE_POINT_DRAIN_TIMEOUT_MS 30000

static inline void
cluster_backup_bitmap_set(uint8 *bitmap, int node_id)
{
	if (bitmap == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	bitmap[node_id / 8] |= (uint8)(1u << (node_id % 8));
}

static inline bool
cluster_backup_bitmap_test(const uint8 *bitmap, int node_id)
{
	if (bitmap == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return (bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

static uint16
cluster_backup_local_thread_id(void)
{
	uint16 thread_id = cluster_wal_thread_id();

	if (thread_id == XLP_THREAD_ID_LEGACY)
		thread_id = 1;
	return thread_id;
}

static const char *
cluster_pitr_action_name(int action)
{
	switch (action) {
	case CLUSTER_RECOVERY_TARGET_ACTION_PAUSE:
		return "pause";
	case CLUSTER_RECOVERY_TARGET_ACTION_PROMOTE:
		return "promote";
	case CLUSTER_RECOVERY_TARGET_ACTION_SHUTDOWN:
		return "shutdown";
	}
	return "unknown";
}

Size
cluster_backup_shmem_size(void)
{
	return sizeof(ClusterBackupSharedState);
}

void
cluster_backup_shmem_init(void)
{
	bool found;

	cluster_backup_state
		= ShmemInitStruct("pgrac cluster backup", cluster_backup_shmem_size(), &found);
	if (!found) {
		MemSet(cluster_backup_state, 0, sizeof(*cluster_backup_state));
		LWLockInitialize(&cluster_backup_state->lock.lock, LWTRANCHE_CLUSTER_BACKUP);
		pg_atomic_init_u32(&cluster_backup_state->pending_commit_count, 0);
		pg_atomic_init_u32(&cluster_backup_state->commit_fence_active, 0);
	}
}

static const ClusterShmemRegion cluster_backup_region = {
	.name = "pgrac cluster backup",
	.size_fn = cluster_backup_shmem_size,
	.init_fn = cluster_backup_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_backup",
	.reserved_flags = 0,
};

void
cluster_backup_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_backup_region);
}

static void cluster_backup_cleanup_session_context(void);
static void cluster_backup_session_exit(int code, Datum arg);
static void cluster_backup_mark_native_stopped(const BackupState *state);
static void cluster_backup_restore_point_fence_begin(void);
static void cluster_backup_restore_point_fence_end(void);

static void
cluster_backup_error_if_unavailable(const char *op)
{
	if (!cluster_enabled)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s requires cluster.enabled", op)));
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s requires a valid cluster.node_id", op)));
	if (cluster_backup_state == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup shared state is not initialized")));
}

static void
cluster_backup_fail_closed_unimplemented(const char *op, const char *missing)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("%s is not available in the current cluster backup substrate", op),
					errdetail("%s is required before this operation can return a sound "
							  "cluster backup/PITR result.",
							  missing),
					errhint("Refusing to create an unsound cluster restore point or "
							"backup manifest.")));
}

static void
cluster_backup_abort_local_session_if_running(void)
{
	if (get_backup_status() == SESSION_BACKUP_RUNNING)
		do_pg_abort_backup(0, DatumGetBool(false));
	cluster_backup_mark_native_stopped(NULL);
	cluster_backup_cleanup_session_context();
}

typedef struct ClusterBackupCoordWaitResult {
	bool ok;
	bool timed_out;
	int32 node_id;
	ClusterBackupWireResult result;
} ClusterBackupCoordWaitResult;

static void
cluster_backup_cleanup_session_context(void)
{
	cluster_backup_session_state = NULL;
	cluster_backup_tablespace_map = NULL;
	if (cluster_backup_context != NULL) {
		MemoryContextDelete(cluster_backup_context);
		cluster_backup_context = NULL;
	}
}

static void
cluster_backup_mark_native_stopped(const BackupState *state)
{
	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status.in_progress = false;
	if (state != NULL)
		cluster_backup_state->status.stop_cut_lsn = state->stoppoint;
	cluster_backup_state->status.stopped_at = GetCurrentTimestamp();
	LWLockRelease(&cluster_backup_state->lock.lock);
}

void
cluster_backup_get_status(ClusterBackupStatus *out)
{
	if (out == NULL)
		return;
	MemSet(out, 0, sizeof(*out));
	if (cluster_backup_state == NULL)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	*out = cluster_backup_state->status;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

bool
cluster_backup_get_last_manifest(ClusterBackupManifest *out)
{
	bool have_manifest;

	if (out == NULL)
		return false;
	MemSet(out, 0, sizeof(*out));
	if (cluster_backup_state == NULL)
		return false;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	have_manifest = cluster_backup_state->have_manifest;
	if (have_manifest)
		*out = cluster_backup_state->last_manifest;
	LWLockRelease(&cluster_backup_state->lock.lock);
	return have_manifest;
}

int
cluster_backup_get_restore_points(ClusterRestorePoint *out, int max_points)
{
	int count;
	int start;
	int i;

	if (out == NULL || max_points <= 0 || cluster_backup_state == NULL)
		return 0;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	count = Min(cluster_backup_state->restore_point_count, max_points);
	start = cluster_backup_state->restore_point_next - cluster_backup_state->restore_point_count;
	if (start < 0)
		start += CLUSTER_BACKUP_RESTORE_POINT_MAX;
	for (i = 0; i < count; i++) {
		int slot = (start + i) % CLUSTER_BACKUP_RESTORE_POINT_MAX;

		out[i] = cluster_backup_state->restore_points[slot];
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
	return count;
}

static void
cluster_backup_init_wire_ack(ClusterBackupWireAck *ack, const ClusterBackupWireRequest *request,
							 ClusterBackupWireResult result)
{
	MemSet(ack, 0, sizeof(*ack));
	ack->magic = CLUSTER_BACKUP_IC_MAGIC;
	ack->version = CLUSTER_BACKUP_IC_VERSION;
	ack->op = request != NULL ? request->op : CLUSTER_BACKUP_WIRE_OP_NONE;
	ack->result = (uint16)result;
	ack->sender_node_id = cluster_node_id;
	ack->thread_id = cluster_backup_local_thread_id();
	ack->request_id = request != NULL ? request->request_id : 0;
}

static void
cluster_backup_lmon_reset_context(void)
{
	cluster_backup_lmon_state = NULL;
	cluster_backup_lmon_tablespace_map = NULL;
	if (cluster_backup_lmon_context != NULL) {
		MemoryContextDelete(cluster_backup_lmon_context);
		cluster_backup_lmon_context = NULL;
	}
}

static void
cluster_backup_session_exit(int code, Datum arg)
{
	if (cluster_backup_state != NULL && cluster_backup_session_state != NULL)
		cluster_backup_mark_native_stopped(NULL);
	cluster_backup_cleanup_session_context();
}

void
cluster_backup_pending_commit_enter(void)
{
	if (!cluster_enabled || cluster_backup_state == NULL)
		return;

	if (!cluster_backup_pending_commit_exit_registered) {
		before_shmem_exit(cluster_backup_pending_commit_abort, (Datum)0);
		cluster_backup_pending_commit_exit_registered = true;
	}

	for (;;) {
		while (pg_atomic_read_u32(&cluster_backup_state->commit_fence_active) != 0) {
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
		}

		pg_atomic_fetch_add_u32(&cluster_backup_state->pending_commit_count, 1);
		cluster_backup_pending_commit_registered = true;
		pg_memory_barrier();
		if (pg_atomic_read_u32(&cluster_backup_state->commit_fence_active) == 0)
			return;

		cluster_backup_pending_commit_exit();
	}
}

void
cluster_backup_pending_commit_exit(void)
{
	if (!cluster_backup_pending_commit_registered)
		return;

	if (!cluster_enabled || cluster_backup_state == NULL) {
		cluster_backup_pending_commit_registered = false;
		return;
	}
	pg_atomic_fetch_sub_u32(&cluster_backup_state->pending_commit_count, 1);
	cluster_backup_pending_commit_registered = false;
}

void
cluster_backup_pending_commit_abort(int code, Datum arg)
{
	cluster_backup_pending_commit_exit();
}

static void
cluster_backup_restore_point_fence_begin(void)
{
	uint32 expected = 0;
	TimestampTz started_at;

	if (cluster_backup_state == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup shared state is not initialized")));

	if (!pg_atomic_compare_exchange_u32(&cluster_backup_state->commit_fence_active, &expected, 1))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT),
						errmsg("cluster restore-point commit fence is already active")));

	started_at = GetCurrentTimestamp();
	for (;;) {
		if (pg_atomic_read_u32(&cluster_backup_state->pending_commit_count) == 0)
			return;

		if (TimestampDifferenceExceeds(started_at, GetCurrentTimestamp(),
									   CLUSTER_BACKUP_RESTORE_POINT_DRAIN_TIMEOUT_MS)) {
			cluster_backup_restore_point_fence_end();
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT),
							errmsg("timed out waiting for cluster restore-point commit drain"),
							errdetail("Pending commits did not drain within %d ms.",
									  CLUSTER_BACKUP_RESTORE_POINT_DRAIN_TIMEOUT_MS)));
		}

		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
	}
}

static void
cluster_backup_restore_point_fence_end(void)
{
	if (cluster_backup_state == NULL)
		return;

	pg_atomic_write_u32(&cluster_backup_state->commit_fence_active, 0);
}

static ClusterBackupWireAck
cluster_backup_lmon_execute_request(const ClusterBackupWireRequest *request)
{
	ClusterBackupWireAck ack;
	ClusterBackupWireResult result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;

	cluster_backup_init_wire_ack(&ack, request, result);

	if (request == NULL || !cluster_backup_wire_request_valid(request)
		|| request->coordinator_node_id < 0 || request->coordinator_node_id >= CLUSTER_MAX_NODES) {
		ack.result = CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST;
		cluster_backup_wire_ack_compute_crc(&ack);
		return ack;
	}

	PG_TRY();
	{
		switch ((ClusterBackupWireOp)request->op) {
		case CLUSTER_BACKUP_WIRE_OP_START:
			if (request->backup_id[0] == '\0')
				result = CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST;
			else if (cluster_backup_lmon_state != NULL
					 || get_backup_status() == SESSION_BACKUP_RUNNING)
				result = CLUSTER_BACKUP_WIRE_RESULT_BUSY;
			else
				result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
			break;

		case CLUSTER_BACKUP_WIRE_OP_STOP:
			if (cluster_backup_lmon_state == NULL || get_backup_status() != SESSION_BACKUP_RUNNING)
				result = CLUSTER_BACKUP_WIRE_RESULT_NOT_IN_BACKUP;
			else {
				do_pg_abort_backup(0, DatumGetBool(false));
				cluster_backup_lmon_reset_context();
				result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
			}
			break;

		case CLUSTER_BACKUP_WIRE_OP_ABORT:
			if (cluster_backup_lmon_state != NULL
				|| get_backup_status() == SESSION_BACKUP_RUNNING) {
				do_pg_abort_backup(0, DatumGetBool(false));
				cluster_backup_lmon_reset_context();
			}
			result = CLUSTER_BACKUP_WIRE_RESULT_OK;
			break;

		case CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT:
			if (request->restore_point_name[0] == '\0')
				result = CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST;
			else
				result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
			break;

		case CLUSTER_BACKUP_WIRE_OP_NONE:
			result = CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST;
			break;
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
		if ((ClusterBackupWireOp)request->op == CLUSTER_BACKUP_WIRE_OP_START)
			cluster_backup_lmon_reset_context();
		else if ((ClusterBackupWireOp)request->op == CLUSTER_BACKUP_WIRE_OP_STOP
				 && get_backup_status() != SESSION_BACKUP_RUNNING)
			cluster_backup_lmon_reset_context();
		result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
	}
	PG_END_TRY();

	ack.result = (uint16)result;
	cluster_backup_wire_ack_compute_crc(&ack);
	return ack;
}

static void
cluster_backup_request_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterBackupWireRequest *request = (const ClusterBackupWireRequest *)payload;

	if (cluster_backup_state == NULL || env == NULL || payload == NULL)
		return;
	if (env->payload_length != sizeof(ClusterBackupWireRequest))
		return;
	if (!cluster_backup_wire_request_valid(request))
		return;
	if (request->coordinator_node_id != (int32)env->source_node_id)
		return;
	if (request->coordinator_node_id == cluster_node_id)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	if (!cluster_backup_state->peer_command_pending && !cluster_backup_state->peer_reply_pending) {
		cluster_backup_state->peer_command = *request;
		cluster_backup_state->peer_command_pending = true;
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
	cluster_lmon_wakeup();
}

static void
cluster_backup_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterBackupWireAck *ack = (const ClusterBackupWireAck *)payload;
	int32 node_id;

	if (cluster_backup_state == NULL || env == NULL || payload == NULL)
		return;
	if (env->payload_length != sizeof(ClusterBackupWireAck))
		return;
	if (!cluster_backup_wire_ack_valid(ack))
		return;
	if (ack->sender_node_id != (int32)env->source_node_id)
		return;
	node_id = ack->sender_node_id;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	if (cluster_backup_state->coordinator_request.request_id == ack->request_id
		&& cluster_backup_state->coordinator_request.op == ack->op
		&& cluster_backup_bitmap_test(cluster_backup_state->coordinator_expected, node_id)) {
		cluster_backup_state->coordinator_acks[node_id] = *ack;
		if (ack->result == CLUSTER_BACKUP_WIRE_RESULT_OK) {
			ClusterBackupManifestThread *thread
				= &cluster_backup_state->coordinator_peer_threads[node_id];

			cluster_backup_bitmap_set(cluster_backup_state->coordinator_acked, node_id);
			if (ack->op == CLUSTER_BACKUP_WIRE_OP_START) {
				MemSet(thread, 0, sizeof(*thread));
				thread->present = true;
				thread->wal_included = false;
				thread->undo_included = false;
				thread->tt_included = false;
				thread->thread_id = ack->thread_id;
				thread->node_id = node_id;
				thread->start_redo_lsn = ack->start_redo_lsn;
				thread->checkpoint_lsn = ack->checkpoint_lsn;
				thread->start_tli = ack->timeline;
			} else if (ack->op == CLUSTER_BACKUP_WIRE_OP_STOP) {
				if (!thread->present && ack->start_redo_lsn != InvalidXLogRecPtr
					&& ack->checkpoint_lsn != InvalidXLogRecPtr) {
					thread->present = true;
					thread->wal_included = false;
					thread->undo_included = false;
					thread->tt_included = false;
					thread->thread_id = ack->thread_id;
					thread->node_id = node_id;
					thread->start_redo_lsn = ack->start_redo_lsn;
					thread->checkpoint_lsn = ack->checkpoint_lsn;
					thread->start_tli = ack->timeline;
				}
				thread->stop_cut_lsn = ack->stop_cut_lsn;
				cluster_backup_state->coordinator_peer_cut_scn[node_id] = ack->cut_scn;
			}
		} else
			cluster_backup_bitmap_set(cluster_backup_state->coordinator_nacked, node_id);
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
}

void
cluster_backup_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo request_info = {
		.msg_type = PGRAC_IC_MSG_BACKUP_REQUEST,
		.name = "backup_request",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = true,
		.handler = cluster_backup_request_handler,
	};
	const ClusterICMsgTypeInfo ack_info = {
		.msg_type = PGRAC_IC_MSG_BACKUP_ACK,
		.name = "backup_ack",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = cluster_backup_ack_handler,
	};

	cluster_ic_register_msg_type(&request_info);
	cluster_ic_register_msg_type(&ack_info);
}

static void
cluster_backup_record_send_nak(int32 node_id, const ClusterBackupWireRequest *request,
							   ClusterBackupWireResult result)
{
	ClusterBackupWireAck ack;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES || request == NULL)
		return;

	cluster_backup_init_wire_ack(&ack, request, result);
	ack.sender_node_id = node_id;
	cluster_backup_wire_ack_compute_crc(&ack);

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	if (cluster_backup_state->coordinator_request.request_id == request->request_id
		&& cluster_backup_state->coordinator_request.op == request->op
		&& cluster_backup_bitmap_test(cluster_backup_state->coordinator_expected, node_id)) {
		cluster_backup_state->coordinator_acks[node_id] = ack;
		cluster_backup_bitmap_set(cluster_backup_state->coordinator_nacked, node_id);
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_lmon_send_coord_request(void)
{
	ClusterBackupWireRequest request;
	uint8 expected[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];
	int node_id;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	if (!cluster_backup_state->coordinator_send_pending) {
		LWLockRelease(&cluster_backup_state->lock.lock);
		return;
	}
	request = cluster_backup_state->coordinator_request;
	memcpy(expected, cluster_backup_state->coordinator_expected, sizeof(expected));
	cluster_backup_state->coordinator_send_pending = false;
	LWLockRelease(&cluster_backup_state->lock.lock);

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_BACKUP_REQUEST, &request, (uint32)sizeof(request),
									per_peer);
	for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
		if (!cluster_backup_bitmap_test(expected, node_id))
			continue;
		if (per_peer[node_id] == CLUSTER_IC_FANOUT_HARD_ERROR
			|| per_peer[node_id] == CLUSTER_IC_FANOUT_PEER_DOWN)
			cluster_backup_record_send_nak(node_id, &request,
										   CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR);
	}
}

static void
cluster_backup_lmon_send_peer_reply(void)
{
	ClusterBackupWireAck reply;
	int32 dest;
	bool have_reply;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	have_reply = cluster_backup_state->peer_reply_pending;
	reply = cluster_backup_state->peer_reply;
	dest = cluster_backup_state->peer_reply_dest;
	cluster_backup_state->peer_reply_pending = false;
	LWLockRelease(&cluster_backup_state->lock.lock);

	if (!have_reply || dest < 0)
		return;
	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_BACKUP_ACK, dest, &reply, (uint32)sizeof(reply));
}

static void
cluster_backup_lmon_process_peer_command(void)
{
	ClusterBackupWireRequest request;
	ClusterBackupWireAck reply;
	bool have_command;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	have_command = cluster_backup_state->peer_command_pending;
	request = cluster_backup_state->peer_command;
	cluster_backup_state->peer_command_pending = false;
	LWLockRelease(&cluster_backup_state->lock.lock);

	if (!have_command)
		return;

	reply = cluster_backup_lmon_execute_request(&request);

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->peer_reply = reply;
	cluster_backup_state->peer_reply_dest = request.coordinator_node_id;
	cluster_backup_state->peer_reply_pending = true;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

void
cluster_backup_lmon_tick(void)
{
	if (cluster_backup_state == NULL || !cluster_enabled)
		return;

	cluster_backup_lmon_send_peer_reply();
	cluster_backup_lmon_send_coord_request();
	cluster_backup_lmon_process_peer_command();
	cluster_backup_lmon_send_peer_reply();
}

static void
cluster_backup_require_single_node_success_path(const char *op)
{
	if (RecoveryInProgress())
		cluster_backup_fail_closed_unimplemented(op, "backup-from-standby restore-point proof");
	if (cluster_conf_has_peers())
		cluster_backup_fail_closed_unimplemented(
			op, "cross-node commit-drain and per-thread capture ACKs");
	if (cluster_conf_node_count() != 1)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RESTORE_INCOMPATIBLE),
						errmsg("%s requires a single declared cluster node in this build", op)));
}

static void
cluster_backup_prepare_session_context(void)
{
	MemoryContext oldcontext;

	if (cluster_backup_context == NULL)
		cluster_backup_context = AllocSetContextCreate(TopMemoryContext, "cluster backup context",
													   ALLOCSET_START_SMALL_SIZES);
	else {
		cluster_backup_session_state = NULL;
		cluster_backup_tablespace_map = NULL;
		MemoryContextReset(cluster_backup_context);
	}

	oldcontext = MemoryContextSwitchTo(cluster_backup_context);
	cluster_backup_session_state = (BackupState *)palloc0(sizeof(BackupState));
	cluster_backup_tablespace_map = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);
}

static void
cluster_backup_store_restore_point(const ClusterRestorePoint *point)
{
	int slot;

	if (point == NULL)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	slot = cluster_backup_state->restore_point_next % CLUSTER_BACKUP_RESTORE_POINT_MAX;
	cluster_backup_state->restore_points[slot] = *point;
	cluster_backup_state->restore_point_next = (slot + 1) % CLUSTER_BACKUP_RESTORE_POINT_MAX;
	if (cluster_backup_state->restore_point_count < CLUSTER_BACKUP_RESTORE_POINT_MAX)
		cluster_backup_state->restore_point_count++;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_make_restore_point(const char *name, ClusterRestorePoint *point)
{
	SCN thread_scn[CLUSTER_MAX_NODES];
	XLogRecPtr thread_lsn[CLUSTER_MAX_NODES];
	SCN cut_scn;
	XLogRecPtr cut_lsn;
	uint16 thread_id;
	int thread_index;
	ClusterRestorePointCutReason reason;

	MemSet(thread_scn, 0, sizeof(thread_scn));
	MemSet(thread_lsn, 0, sizeof(thread_lsn));

	cluster_backup_restore_point_fence_begin();
	PG_TRY();
	{
		cut_scn = cluster_scn_current();
		if (!SCN_VALID(cut_scn))
			cut_scn = cluster_scn_advance();
		cut_lsn = XLogRestorePoint(name);
		XLogFlush(cut_lsn);
	}
	PG_CATCH();
	{
		cluster_backup_restore_point_fence_end();
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_backup_restore_point_fence_end();

	thread_id = cluster_backup_local_thread_id();
	thread_index = (int)thread_id - 1;
	if (thread_index < 0 || thread_index >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster restore point produced invalid local WAL thread id %u",
							   thread_id)));
	thread_scn[thread_index] = cut_scn;
	thread_lsn[thread_index] = cut_lsn;

	reason = cluster_restore_point_build(point, name, thread_scn, thread_lsn, CLUSTER_MAX_NODES,
										 true, true, 1);
	if (reason != CLUSTER_RESTORE_POINT_CUT_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("could not build cluster restore point cut"),
						errdetail("Reason: %s.", cluster_restore_point_cut_reason_name(reason))));
	point->created_at = GetCurrentTimestamp();
}

Datum
pg_cluster_backup_start(PG_FUNCTION_ARGS)
{
#define CLUSTER_BACKUP_START_COLS 4
	TupleDesc tupdesc;
	Datum values[CLUSTER_BACKUP_START_COLS] = { 0 };
	bool nulls[CLUSTER_BACKUP_START_COLS] = { 0 };
	text *backupid = PG_GETARG_TEXT_PP(0);
	bool fast = PG_GETARG_BOOL(1);
	char *backupidstr;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to start a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_start");
	cluster_backup_require_single_node_success_path("pg_cluster_backup_start");
	if (get_backup_status() == SESSION_BACKUP_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("a backup is already in progress in this session")));

	backupidstr = text_to_cstring(backupid);
	if (strlen(backupidstr) >= CLUSTER_BACKUP_ID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cluster backup id is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_BACKUP_ID_MAX - 1)));

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	if (cluster_backup_state->status.in_progress) {
		LWLockRelease(&cluster_backup_state->lock.lock);
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_IN_PROGRESS),
						errmsg("a cluster backup is already in progress")));
	}
	MemSet(&cluster_backup_state->status, 0, sizeof(cluster_backup_state->status));
	cluster_backup_state->status.in_progress = true;
	strlcpy(cluster_backup_state->status.backup_id, backupidstr,
			sizeof(cluster_backup_state->status.backup_id));
	cluster_backup_state->status.coordinator_node_id = cluster_node_id;
	cluster_backup_state->status.started_at = GetCurrentTimestamp();
	LWLockRelease(&cluster_backup_state->lock.lock);

	cluster_backup_prepare_session_context();
	register_persistent_abort_backup_handler();
	before_shmem_exit(cluster_backup_session_exit, (Datum)0);

	PG_TRY();
	{
		do_pg_backup_start(backupidstr, fast, NULL, cluster_backup_session_state,
						   cluster_backup_tablespace_map);
	}
	PG_CATCH();
	{
		cluster_backup_mark_native_stopped(NULL);
		cluster_backup_cleanup_session_context();
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status.start_redo_lsn = cluster_backup_session_state->startpoint;
	cluster_backup_state->status.checkpoint_lsn = cluster_backup_session_state->checkpointloc;
	cluster_backup_state->status.start_tli = cluster_backup_session_state->starttli;
	LWLockRelease(&cluster_backup_state->lock.lock);

	values[0] = CStringGetTextDatum(backupidstr);
	values[1] = LSNGetDatum(cluster_backup_session_state->startpoint);
	values[2] = LSNGetDatum(cluster_backup_session_state->checkpointloc);
	values[3] = Int32GetDatum((int32)cluster_backup_session_state->starttli);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
#undef CLUSTER_BACKUP_START_COLS
}

Datum
pg_cluster_backup_stop(PG_FUNCTION_ARGS)
{
#define CLUSTER_BACKUP_STOP_COLS 4
	TupleDesc tupdesc;
	Datum values[CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool nulls[CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool waitforarchive = PG_GETARG_BOOL(0);
	ClusterRestorePoint stop_point;
	ClusterBackupManifest manifest;
	ClusterBackupManifestThread thread;
	ClusterBackupManifestReason manifest_reason;
	char restore_point_name[CLUSTER_RESTORE_POINT_NAME_MAX];
	char *backup_label = NULL;
	uint16 thread_id;
	int thread_index;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to stop a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_stop");
	cluster_backup_require_single_node_success_path("pg_cluster_backup_stop");

	if (!waitforarchive || !XLogArchivingActive()) {
		cluster_backup_abort_local_session_if_running();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup stop requires durable WAL archive proof"),
						errdetail("Call pg_cluster_backup_stop(true) with archive_mode enabled so "
								  "required WAL is archived before the manifest is published.")));
	}
	if (get_backup_status() != SESSION_BACKUP_RUNNING || cluster_backup_session_state == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup is not in progress"),
						errhint("Did you call pg_cluster_backup_start()?")));

	snprintf(restore_point_name, sizeof(restore_point_name), "cluster_backup_stop_%s",
			 cluster_backup_session_state->name);
	restore_point_name[CLUSTER_RESTORE_POINT_NAME_MAX - 1] = '\0';

	PG_TRY();
	{
		cluster_backup_make_restore_point(restore_point_name, &stop_point);
		do_pg_backup_stop(cluster_backup_session_state, waitforarchive);

		backup_label = build_backup_content(cluster_backup_session_state, false);

		cluster_backup_manifest_init(&manifest, cluster_backup_session_state->name);
		manifest.consistent_scn = stop_point.cut_scn;
		manifest.scn_durable_peak = stop_point.cut_scn;
		manifest.timeline = cluster_backup_session_state->stoptli;
		manifest.catversion = CATALOG_VERSION_NO;
		manifest.incarnation = stop_point.incarnation;
		manifest.backend_storage_id = (uint32)cluster_shared_storage_backend;
		manifest.node_count = 1;
		manifest.control_included = true;
		manifest.voting_included = true;

		thread_id = cluster_backup_local_thread_id();
		thread_index = (int)thread_id - 1;
		if (thread_index < 0 || thread_index >= CLUSTER_MAX_NODES)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					 errmsg("cluster backup produced invalid local WAL thread id %u", thread_id)));
		MemSet(&thread, 0, sizeof(thread));
		thread.present = true;
		thread.wal_included = true;
		thread.undo_included = true;
		thread.tt_included = true;
		thread.thread_id = thread_id;
		thread.node_id = cluster_node_id;
		thread.start_redo_lsn = cluster_backup_session_state->startpoint;
		thread.checkpoint_lsn = cluster_backup_session_state->checkpointloc;
		thread.start_tli = cluster_backup_session_state->starttli;
		thread.stop_cut_lsn = stop_point.cut_lsn[thread_index];
		if (!cluster_backup_manifest_set_thread(&manifest, thread_index, &thread))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					 errmsg("could not record local WAL thread in cluster backup manifest")));
		cluster_backup_manifest_seal(&manifest);
		manifest_reason = cluster_backup_manifest_validate(&manifest);
		if (manifest_reason != CLUSTER_BACKUP_MANIFEST_OK)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster backup manifest validation failed"),
							errdetail("Reason: %s.",
									  cluster_backup_manifest_reason_name(manifest_reason))));

		cluster_backup_store_restore_point(&stop_point);

		LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
		cluster_backup_state->last_manifest = manifest;
		cluster_backup_state->have_manifest = true;
		cluster_backup_state->status.in_progress = false;
		cluster_backup_state->status.stop_cut_lsn = thread.stop_cut_lsn;
		cluster_backup_state->status.consistent_scn = manifest.consistent_scn;
		cluster_backup_state->status.manifest_crc = manifest.manifest_crc;
		cluster_backup_state->status.stopped_at = GetCurrentTimestamp();
		LWLockRelease(&cluster_backup_state->lock.lock);
	}
	PG_CATCH();
	{
		if (get_backup_status() == SESSION_BACKUP_RUNNING)
			do_pg_abort_backup(0, DatumGetBool(false));
		cluster_backup_mark_native_stopped(NULL);
		cluster_backup_cleanup_session_context();
		PG_RE_THROW();
	}
	PG_END_TRY();

	values[0] = Int64GetDatum((int64)manifest.consistent_scn);
	values[1] = LSNGetDatum(thread.stop_cut_lsn);
	values[2] = Int64GetDatum((int64)manifest.manifest_crc);
	values[3] = CStringGetTextDatum(backup_label);

	pfree(backup_label);
	cluster_backup_cleanup_session_context();

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
#undef CLUSTER_BACKUP_STOP_COLS
}

Datum
pg_cluster_create_restore_point(PG_FUNCTION_ARGS)
{
#define CLUSTER_CREATE_RESTORE_POINT_COLS 3
	TupleDesc tupdesc;
	Datum values[CLUSTER_CREATE_RESTORE_POINT_COLS] = { 0 };
	bool nulls[CLUSTER_CREATE_RESTORE_POINT_COLS] = { 0 };
	text *restore_name = PG_GETARG_TEXT_PP(0);
	char *restore_name_str;
	ClusterRestorePoint point;
	uint16 thread_id;
	int thread_index;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to create a cluster restore point")));
	cluster_backup_error_if_unavailable("pg_cluster_create_restore_point");
	cluster_backup_require_single_node_success_path("pg_cluster_create_restore_point");
	if (RecoveryInProgress())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("recovery is in progress"),
						errhint("WAL control functions cannot be executed during recovery.")));
	if (!XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for creating a restore point"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server start.")));

	restore_name_str = text_to_cstring(restore_name);
	if (strlen(restore_name_str) >= CLUSTER_RESTORE_POINT_NAME_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster restore point name is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_RESTORE_POINT_NAME_MAX - 1)));
	cluster_backup_make_restore_point(restore_name_str, &point);
	cluster_backup_store_restore_point(&point);

	thread_id = cluster_backup_local_thread_id();
	thread_index = (int)thread_id - 1;
	if (thread_index < 0 || thread_index >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster restore point produced invalid local WAL thread id %u",
							   thread_id)));
	values[0] = CStringGetTextDatum(point.name);
	values[1] = Int64GetDatum((int64)point.cut_scn);
	values[2] = LSNGetDatum(point.cut_lsn[thread_index]);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
#undef CLUSTER_CREATE_RESTORE_POINT_COLS
}

Datum
cluster_get_backup_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupStatus status;
	Datum values[14];
	bool nulls[14];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	cluster_backup_get_status(&status);
	MemSet(nulls, false, sizeof(nulls));
	values[0] = BoolGetDatum(status.in_progress);
	if (status.backup_id[0] == '\0')
		nulls[1] = true;
	else
		values[1] = CStringGetTextDatum(status.backup_id);
	values[2] = Int32GetDatum(status.coordinator_node_id);
	values[3] = LSNGetDatum(status.start_redo_lsn);
	values[4] = LSNGetDatum(status.checkpoint_lsn);
	values[5] = LSNGetDatum(status.stop_cut_lsn);
	values[6] = Int64GetDatum((int64)status.consistent_scn);
	values[7] = Int64GetDatum((int64)status.manifest_crc);
	if (status.started_at == 0)
		nulls[8] = true;
	else
		values[8] = TimestampTzGetDatum(status.started_at);
	if (status.stopped_at == 0)
		nulls[9] = true;
	else
		values[9] = TimestampTzGetDatum(status.stopped_at);
	values[10] = Int32GetDatum(cluster_backup_parallel_channels);
	values[11] = Int32GetDatum(cluster_backup_wal_retention);
	values[12] = BoolGetDatum(cluster_enable_pitr_restore_points);
	values[13] = Int32GetDatum(cluster_pitr_restore_point_interval_ms);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

Datum
cluster_get_backup_history(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupManifest manifest;
	Datum values[9];
	bool nulls[9];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled || !cluster_backup_get_last_manifest(&manifest))
		return (Datum)0;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = CStringGetTextDatum(manifest.backup_id);
	values[1] = Int64GetDatum((int64)manifest.consistent_scn);
	values[2] = Int64GetDatum((int64)manifest.scn_durable_peak);
	values[3] = Int32GetDatum((int32)manifest.timeline);
	values[4] = Int64GetDatum((int64)manifest.catversion);
	values[5] = Int32GetDatum((int32)manifest.backend_storage_id);
	values[6] = Int32GetDatum((int32)manifest.node_count);
	values[7] = Int32GetDatum((int32)manifest.thread_count);
	values[8] = Int64GetDatum((int64)manifest.manifest_crc);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

Datum
cluster_get_restore_points(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterRestorePoint points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
	int count;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
	for (i = 0; i < count; i++) {
		Datum values[5];
		bool nulls[5] = { false };

		values[0] = CStringGetTextDatum(points[i].name);
		values[1] = Int64GetDatum((int64)points[i].cut_scn);
		values[2] = Int32GetDatum((int32)points[i].thread_count);
		values[3] = Int32GetDatum((int32)points[i].incarnation);
		if (points[i].created_at == 0)
			nulls[4] = true;
		else
			values[4] = TimestampTzGetDatum(points[i].created_at);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum)0;
}

Datum
cluster_get_pitr_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupManifest manifest;
	ClusterRestorePoint points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
	ClusterRestorePoint chosen;
	ClusterPitrTargetReason reason;
	Datum values[6];
	bool nulls[6] = { false };
	int count;
	int i;
	const char *target_action = cluster_pitr_action_name(cluster_recovery_target_action);
	bool have_requested_scn = false;
	bool invalid_requested_scn = false;
	SCN requested_scn = InvalidScn;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	if ((cluster_recovery_target_scn == NULL || cluster_recovery_target_scn[0] == '\0')
		&& (cluster_recovery_target_name == NULL || cluster_recovery_target_name[0] == '\0')
		&& cluster_recovery_target_cluster_time != NULL
		&& cluster_recovery_target_cluster_time[0] != '\0') {
		values[0] = CStringGetTextDatum("cluster_time");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("unsupported_target_type");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (cluster_recovery_target_scn != NULL && cluster_recovery_target_scn[0] != '\0') {
		int64 parsed = pg_strtoint64(cluster_recovery_target_scn);

		have_requested_scn = true;
		if (parsed > 0)
			requested_scn = (SCN)parsed;
		else
			invalid_requested_scn = true;
	}

	if (invalid_requested_scn) {
		values[0] = CStringGetTextDatum("scn");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("invalid_target");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!have_requested_scn && cluster_recovery_target_name != NULL
		&& cluster_recovery_target_name[0] != '\0') {
		if (!cluster_backup_get_last_manifest(&manifest)) {
			values[0] = CStringGetTextDatum("name");
			values[1] = CStringGetTextDatum(target_action);
			values[2] = BoolGetDatum(false);
			values[3] = CStringGetTextDatum("manifest");
			nulls[4] = true;
			nulls[5] = true;
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			return (Datum)0;
		}

		count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
		for (i = 0; i < count; i++) {
			if (!points[i].present)
				continue;
			if (strcmp(points[i].name, cluster_recovery_target_name) == 0) {
				if (scn_time_cmp(points[i].cut_scn, manifest.consistent_scn) < 0) {
					values[0] = CStringGetTextDatum("name");
					values[1] = CStringGetTextDatum(target_action);
					values[2] = BoolGetDatum(false);
					values[3] = CStringGetTextDatum("before_backup");
					nulls[4] = true;
					nulls[5] = true;
					tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
					return (Datum)0;
				}
				if (points[i].thread_count == 0 || points[i].thread_count > CLUSTER_MAX_NODES) {
					values[0] = CStringGetTextDatum("name");
					values[1] = CStringGetTextDatum(target_action);
					values[2] = BoolGetDatum(false);
					values[3] = CStringGetTextDatum("missing_thread");
					nulls[4] = true;
					nulls[5] = true;
					tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
					return (Datum)0;
				}

				values[0] = CStringGetTextDatum("name");
				values[1] = CStringGetTextDatum(target_action);
				values[2] = BoolGetDatum(true);
				values[3] = CStringGetTextDatum("ok");
				values[4] = Int64GetDatum((int64)points[i].cut_scn);
				values[5] = CStringGetTextDatum(points[i].name);
				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
				return (Datum)0;
			}
		}
		values[0] = CStringGetTextDatum("name");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("no_restore_point");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!SCN_VALID(requested_scn)) {
		values[0] = CStringGetTextDatum("latest");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(true);
		values[3] = CStringGetTextDatum("ok");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!cluster_backup_get_last_manifest(&manifest)) {
		values[0] = CStringGetTextDatum("scn");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("manifest");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
	reason
		= cluster_pitr_resolve_scn(points, count, requested_scn, manifest.consistent_scn, &chosen);
	values[0] = CStringGetTextDatum("scn");
	values[1] = CStringGetTextDatum(target_action);
	values[2] = BoolGetDatum(reason == CLUSTER_PITR_TARGET_OK);
	values[3] = CStringGetTextDatum(cluster_pitr_target_reason_name(reason));
	if (reason == CLUSTER_PITR_TARGET_OK) {
		values[4] = Int64GetDatum((int64)chosen.cut_scn);
		values[5] = CStringGetTextDatum(chosen.name);
	} else {
		nulls[4] = true;
		nulls[5] = true;
	}
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_backup_shmem_size(void)
{
	return 0;
}

void
cluster_backup_shmem_init(void)
{}

void
cluster_backup_shmem_register(void)
{}

void
cluster_backup_get_status(ClusterBackupStatus *out)
{
	if (out != NULL)
		MemSet(out, 0, sizeof(*out));
}

bool
cluster_backup_get_last_manifest(ClusterBackupManifest *out)
{
	if (out != NULL)
		MemSet(out, 0, sizeof(*out));
	return false;
}

int
cluster_backup_get_restore_points(ClusterRestorePoint *out, int max_points)
{
	return 0;
}

Datum
pg_cluster_backup_start(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_backup_start requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_backup_stop(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_backup_stop requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_create_restore_point(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_create_restore_point requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_backup_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_backup_state requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_backup_history(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_backup_history requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_restore_points(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_restore_points requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_pitr_status(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_pitr_status requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
