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

static bool
cluster_backup_bitmap_all_acked(const uint8 *expected, const uint8 *acked)
{
	int i;

	for (i = 0; i < CLUSTER_BACKUP_NODE_BITMAP_BYTES; i++) {
		if ((expected[i] & ~acked[i]) != 0)
			return false;
	}
	return true;
}

static bool
cluster_backup_bitmap_any_set(const uint8 *bitmap)
{
	int i;

	for (i = 0; i < CLUSTER_BACKUP_NODE_BITMAP_BYTES; i++) {
		if (bitmap[i] != 0)
			return true;
	}
	return false;
}

static bool
cluster_backup_bitmap_equal(const uint8 *left, const uint8 *right)
{
	int i;

	if (left == NULL || right == NULL)
		return false;
	for (i = 0; i < CLUSTER_BACKUP_NODE_BITMAP_BYTES; i++) {
		if (left[i] != right[i])
			return false;
	}
	return true;
}

static int
cluster_backup_bitmap_first_missing(const uint8 *expected, const uint8 *acked, const uint8 *nacked)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_backup_bitmap_test(expected, i) && !cluster_backup_bitmap_test(acked, i)
			&& !cluster_backup_bitmap_test(nacked, i))
			return i;
	}
	return -1;
}

static int
cluster_backup_bitmap_first_set(const uint8 *bitmap)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_backup_bitmap_test(bitmap, i))
			return i;
	}
	return -1;
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
cluster_backup_wire_op_name(ClusterBackupWireOp op)
{
	switch (op) {
	case CLUSTER_BACKUP_WIRE_OP_START:
		return "start";
	case CLUSTER_BACKUP_WIRE_OP_STOP:
		return "stop";
	case CLUSTER_BACKUP_WIRE_OP_ABORT:
		return "abort";
	case CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT:
		return "restore_point";
	case CLUSTER_BACKUP_WIRE_OP_NONE:
		break;
	}
	return "unknown";
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

static SCN
cluster_backup_current_scn(void)
{
	SCN scn;

	scn = cluster_scn_current();
	if (!SCN_VALID(scn))
		scn = cluster_scn_advance();
	return scn;
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
cluster_backup_build_current_peer_bitmap(uint8 *bitmap)
{
	int i;

	MemSet(bitmap, 0, CLUSTER_BACKUP_NODE_BITMAP_BYTES);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) != NULL)
			cluster_backup_bitmap_set(bitmap, i);
	}
}

static void
cluster_backup_init_wire_request(ClusterBackupWireRequest *request, ClusterBackupWireOp op,
								 uint64 request_id, const char *backup_id,
								 const char *restore_point_name, bool fast, bool waitforarchive,
								 SCN requested_scn)
{
	MemSet(request, 0, sizeof(*request));
	request->magic = CLUSTER_BACKUP_IC_MAGIC;
	request->version = CLUSTER_BACKUP_IC_VERSION;
	request->op = (uint16)op;
	request->request_id = request_id;
	request->coordinator_node_id = cluster_node_id;
	request->fast = fast;
	request->waitforarchive = waitforarchive;
	request->requested_scn = requested_scn;
	if (backup_id != NULL)
		strlcpy(request->backup_id, backup_id, sizeof(request->backup_id));
	if (restore_point_name != NULL)
		strlcpy(request->restore_point_name, restore_point_name,
				sizeof(request->restore_point_name));
	cluster_backup_wire_request_compute_crc(request);
}

static bool
cluster_backup_get_backup_peer_bitmap(uint8 *bitmap)
{
	bool have_peers;

	if (bitmap == NULL || cluster_backup_state == NULL)
		return false;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	memcpy(bitmap, cluster_backup_state->coordinator_backup_peers,
		   CLUSTER_BACKUP_NODE_BITMAP_BYTES);
	have_peers = cluster_backup_bitmap_any_set(bitmap);
	LWLockRelease(&cluster_backup_state->lock.lock);
	return have_peers;
}

static bool
cluster_backup_begin_coord_request(ClusterBackupWireOp op, const uint8 *expected_override,
								   const char *backup_id, const char *restore_point_name, bool fast,
								   bool waitforarchive, SCN requested_scn, uint64 *request_id)
{
	ClusterBackupWireRequest request;
	uint8 expected[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	uint64 id;

	if (request_id != NULL)
		*request_id = 0;
	if (cluster_backup_state == NULL)
		return false;

	if (expected_override != NULL)
		memcpy(expected, expected_override, sizeof(expected));
	else
		cluster_backup_build_current_peer_bitmap(expected);

	if (!cluster_backup_bitmap_any_set(expected))
		return false;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	id = ++cluster_backup_state->next_request_id;
	if (id == 0)
		id = ++cluster_backup_state->next_request_id;
	LWLockRelease(&cluster_backup_state->lock.lock);

	cluster_backup_init_wire_request(&request, op, id, backup_id, restore_point_name, fast,
									 waitforarchive, requested_scn);

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->coordinator_request = request;
	memcpy(cluster_backup_state->coordinator_expected, expected, sizeof(expected));
	MemSet(cluster_backup_state->coordinator_acked, 0,
		   sizeof(cluster_backup_state->coordinator_acked));
	MemSet(cluster_backup_state->coordinator_nacked, 0,
		   sizeof(cluster_backup_state->coordinator_nacked));
	MemSet(cluster_backup_state->coordinator_acks, 0,
		   sizeof(cluster_backup_state->coordinator_acks));
	if (op == CLUSTER_BACKUP_WIRE_OP_START) {
		memcpy(cluster_backup_state->coordinator_backup_peers, expected, sizeof(expected));
		MemSet(cluster_backup_state->coordinator_peer_threads, 0,
			   sizeof(cluster_backup_state->coordinator_peer_threads));
		MemSet(cluster_backup_state->coordinator_peer_cut_scn, 0,
			   sizeof(cluster_backup_state->coordinator_peer_cut_scn));
	}
	cluster_backup_state->coordinator_send_pending = true;
	LWLockRelease(&cluster_backup_state->lock.lock);

	if (request_id != NULL)
		*request_id = id;
	cluster_lmon_wakeup();
	return true;
}

static bool
cluster_backup_wait_for_peer_acks(ClusterBackupWireOp op, uint64 request_id,
								  ClusterBackupCoordWaitResult *result)
{
	TimestampTz deadline;

	if (result != NULL) {
		MemSet(result, 0, sizeof(*result));
		result->node_id = -1;
		result->result = CLUSTER_BACKUP_WIRE_RESULT_OK;
	}

	if (request_id == 0)
		return true;

	deadline = GetCurrentTimestamp()
			   + (TimestampTz)cluster_recovery_merge_wait_timeout * INT64CONST(1000);

	for (;;) {
		bool all_acked;
		bool any_nacked;
		int bad_node;
		int missing_node;
		ClusterBackupWireResult bad_result = CLUSTER_BACKUP_WIRE_RESULT_OK;

		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
		if (cluster_backup_state->coordinator_request.request_id != request_id
			|| cluster_backup_state->coordinator_request.op != (uint16)op) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			if (result != NULL) {
				result->node_id = -1;
				result->result = CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST;
			}
			return false;
		}
		all_acked = cluster_backup_bitmap_all_acked(cluster_backup_state->coordinator_expected,
													cluster_backup_state->coordinator_acked);
		any_nacked = cluster_backup_bitmap_any_set(cluster_backup_state->coordinator_nacked);
		bad_node = cluster_backup_bitmap_first_set(cluster_backup_state->coordinator_nacked);
		if (bad_node >= 0)
			bad_result
				= (ClusterBackupWireResult)cluster_backup_state->coordinator_acks[bad_node].result;
		missing_node = cluster_backup_bitmap_first_missing(
			cluster_backup_state->coordinator_expected, cluster_backup_state->coordinator_acked,
			cluster_backup_state->coordinator_nacked);
		LWLockRelease(&cluster_backup_state->lock.lock);

		if (all_acked) {
			if (result != NULL)
				result->ok = true;
			return true;
		}
		if (any_nacked) {
			if (result != NULL) {
				result->node_id = bad_node;
				result->result = bad_result;
			}
			return false;
		}
		if (cluster_recovery_merge_wait_timeout <= 0 || GetCurrentTimestamp() >= deadline) {
			if (result != NULL) {
				result->timed_out = true;
				result->node_id = missing_node;
			}
			return false;
		}

		pg_usleep(10000L);
	}
}

static void
cluster_backup_raise_peer_failure(ClusterBackupWireOp op,
								  const ClusterBackupCoordWaitResult *result)
{
	const char *op_name = cluster_backup_wire_op_name(op);
	int32 node_id = (result != NULL) ? result->node_id : -1;

	if (result != NULL && result->timed_out)
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
				 errmsg("cluster backup %s did not receive ACK from all peer nodes", op_name),
				 errdetail("First missing peer node: %d.", node_id)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
				 errmsg("cluster backup %s was rejected by a peer node", op_name),
				 errdetail("Peer node %d returned %s.", node_id,
						   cluster_backup_wire_result_name(
							   result != NULL ? result->result
											  : CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR))));
}

static void
cluster_backup_raise_coord_enqueue_failure(ClusterBackupWireOp op)
{
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					errmsg("cluster backup %s could not enqueue a peer coordination request",
						   cluster_backup_wire_op_name(op))));
}

static void
cluster_backup_abort_peers_best_effort(const uint8 *expected, const char *backup_id)
{
	uint64 abort_request_id = 0;
	ClusterBackupCoordWaitResult ignored;

	if (expected == NULL || !cluster_backup_bitmap_any_set(expected))
		return;
	if (!cluster_backup_begin_coord_request(CLUSTER_BACKUP_WIRE_OP_ABORT, expected, backup_id, NULL,
											false, false, InvalidScn, &abort_request_id))
		return;
	(void)cluster_backup_wait_for_peer_acks(CLUSTER_BACKUP_WIRE_OP_ABORT, abort_request_id,
											&ignored);
}

static void
cluster_backup_update_start(const char *backup_id, const BackupState *state)
{
	ClusterBackupStatus status;

	MemSet(&status, 0, sizeof(status));
	status.in_progress = true;
	strlcpy(status.backup_id, backup_id, sizeof(status.backup_id));
	status.coordinator_node_id = cluster_node_id;
	status.start_redo_lsn = state->startpoint;
	status.checkpoint_lsn = state->checkpointloc;
	status.start_tli = state->starttli;
	status.started_at = GetCurrentTimestamp();

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status = status;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_update_stop(const BackupState *state, const ClusterBackupManifest *manifest,
						   SCN cut_scn)
{
	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status.in_progress = false;
	cluster_backup_state->status.stop_cut_lsn = state->stoppoint;
	cluster_backup_state->status.consistent_scn = cut_scn;
	cluster_backup_state->status.manifest_crc = manifest->manifest_crc;
	cluster_backup_state->status.stopped_at = GetCurrentTimestamp();
	cluster_backup_state->last_manifest = *manifest;
	cluster_backup_state->have_manifest = true;
	LWLockRelease(&cluster_backup_state->lock.lock);
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

static void
cluster_backup_add_restore_point(const ClusterRestorePoint *point)
{
	int slot;

	if (point == NULL || !point->present)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	slot = cluster_backup_state->restore_point_next;
	cluster_backup_state->restore_points[slot] = *point;
	cluster_backup_state->restore_point_next
		= (cluster_backup_state->restore_point_next + 1) % CLUSTER_BACKUP_RESTORE_POINT_MAX;
	if (cluster_backup_state->restore_point_count < CLUSTER_BACKUP_RESTORE_POINT_MAX)
		cluster_backup_state->restore_point_count++;
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
cluster_backup_fill_local_manifest(ClusterBackupManifest *manifest, const BackupState *state,
								   SCN cut_scn)
{
	ClusterBackupManifestThread thread;
	uint16 thread_id = cluster_backup_local_thread_id();
	int thread_index;

	thread_index = (int)thread_id - 1;

	cluster_backup_manifest_init(manifest, state->name);
	manifest->consistent_scn = cut_scn;
	manifest->scn_durable_peak = cut_scn;
	manifest->timeline = state->stoptli;
	manifest->catversion = CATALOG_VERSION_NO;
	manifest->incarnation = 0;
	manifest->backend_storage_id = (uint32)cluster_shared_storage_backend;
	manifest->node_count = 1;
	manifest->control_included = true;
	manifest->voting_included = false;

	MemSet(&thread, 0, sizeof(thread));
	thread.present = true;
	thread.wal_included = true;
	thread.undo_included = true;
	thread.tt_included = true;
	thread.thread_id = thread_id;
	thread.node_id = cluster_node_id;
	thread.start_redo_lsn = state->startpoint;
	thread.checkpoint_lsn = state->checkpointloc;
	thread.start_tli = state->starttli;
	thread.stop_cut_lsn = state->stoppoint;

	if (!cluster_backup_manifest_set_thread(manifest, thread_index, &thread))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("could not add local WAL thread to cluster backup manifest")));
}

static void
cluster_backup_seal_manifest_or_error(ClusterBackupManifest *manifest)
{
	ClusterBackupManifestReason reason;

	if (cluster_backup_manifest_checksums != CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifests require crc32c checksums")));
	cluster_backup_manifest_seal(manifest);
	reason = cluster_backup_manifest_validate(manifest);
	if (reason != CLUSTER_BACKUP_MANIFEST_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifest failed self-validation"),
						errdetail("Manifest validation reason: %s.",
								  cluster_backup_manifest_reason_name(reason))));
}

static void
cluster_backup_add_peer_stop_threads(ClusterBackupManifest *manifest, uint64 stop_request_id,
									 SCN *thread_scn, XLogRecPtr *thread_lsn)
{
	int node_id;

	if (stop_request_id == 0)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
		ClusterBackupManifestThread thread;
		ClusterBackupWireAck ack;
		SCN cut_scn;
		int thread_index;

		if (!cluster_backup_bitmap_test(cluster_backup_state->coordinator_expected, node_id))
			continue;
		if (!cluster_backup_bitmap_test(cluster_backup_state->coordinator_acked, node_id)) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster backup manifest is missing a peer STOP ACK"),
							errdetail("Missing peer node: %d.", node_id)));
		}

		ack = cluster_backup_state->coordinator_acks[node_id];
		thread = cluster_backup_state->coordinator_peer_threads[node_id];
		cut_scn = cluster_backup_state->coordinator_peer_cut_scn[node_id];

		if (ack.request_id != stop_request_id || ack.op != CLUSTER_BACKUP_WIRE_OP_STOP
			|| ack.result != CLUSTER_BACKUP_WIRE_RESULT_OK || !thread.present
			|| ack.stop_cut_lsn == InvalidXLogRecPtr || !SCN_VALID(cut_scn)) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster backup manifest has incomplete peer STOP metadata"),
							errdetail("Peer node: %d.", node_id)));
		}

		thread.stop_cut_lsn = ack.stop_cut_lsn;
		thread_index = (int)thread.thread_id - 1;
		if (thread_index < 0 || thread_index >= CLUSTER_MAX_NODES
			|| manifest->threads[thread_index].present) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					 errmsg("cluster backup manifest has duplicate or invalid WAL thread"),
					 errdetail("Peer node %d reported thread %u.", node_id, thread.thread_id)));
		}

		if (!cluster_backup_manifest_set_thread(manifest, thread_index, &thread)) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					 errmsg("could not add peer WAL thread to cluster backup manifest"),
					 errdetail("Peer node %d reported thread %u.", node_id, thread.thread_id)));
		}

		manifest->node_count++;
		if (!SCN_VALID(manifest->consistent_scn)
			|| scn_time_cmp(cut_scn, manifest->consistent_scn) > 0) {
			manifest->consistent_scn = cut_scn;
			manifest->scn_durable_peak = cut_scn;
		}
		if (thread_scn != NULL)
			thread_scn[thread_index] = cut_scn;
		if (thread_lsn != NULL)
			thread_lsn[thread_index] = ack.stop_cut_lsn;
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_add_peer_restore_point_acks(uint64 request_id, SCN *thread_scn,
										   XLogRecPtr *thread_lsn)
{
	int node_id;

	if (request_id == 0)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
		ClusterBackupWireAck ack;
		int thread_index;

		if (!cluster_backup_bitmap_test(cluster_backup_state->coordinator_expected, node_id))
			continue;
		if (!cluster_backup_bitmap_test(cluster_backup_state->coordinator_acked, node_id)) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster restore point is missing a peer ACK"),
							errdetail("Missing peer node: %d.", node_id)));
		}

		ack = cluster_backup_state->coordinator_acks[node_id];
		if (ack.request_id != request_id || ack.op != CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT
			|| ack.result != CLUSTER_BACKUP_WIRE_RESULT_OK || ack.stop_cut_lsn == InvalidXLogRecPtr
			|| !SCN_VALID(ack.cut_scn) || ack.thread_id == 0 || ack.thread_id > CLUSTER_MAX_NODES) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster restore point has incomplete peer metadata"),
							errdetail("Peer node: %d.", node_id)));
		}

		thread_index = (int)ack.thread_id - 1;
		if (thread_lsn[thread_index] != InvalidXLogRecPtr || SCN_VALID(thread_scn[thread_index])) {
			LWLockRelease(&cluster_backup_state->lock.lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster restore point has duplicate WAL thread metadata"),
							errdetail("Peer node %d reported thread %u.", node_id, ack.thread_id)));
		}
		thread_lsn[thread_index] = ack.stop_cut_lsn;
		thread_scn[thread_index] = ack.cut_scn;
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static char *
cluster_backup_build_label(const BackupState *state, const ClusterBackupManifest *manifest,
						   SCN cut_scn)
{
	StringInfoData buf;
	char *native;

	native = build_backup_content((BackupState *)state, false);
	initStringInfo(&buf);
	appendStringInfoString(&buf, native);
	appendStringInfo(&buf, "CLUSTER_BACKUP_ID: %s\n", manifest->backup_id);
	appendStringInfo(&buf, "CLUSTER_CONSISTENT_SCN: " UINT64_FORMAT "\n", (uint64)cut_scn);
	appendStringInfo(&buf, "CLUSTER_MANIFEST_CRC32C: %u\n", manifest->manifest_crc);
	appendStringInfo(&buf, "CLUSTER_NODE_COUNT: %u\n", manifest->node_count);
	appendStringInfo(&buf, "CLUSTER_THREAD_COUNT: %u\n", manifest->thread_count);
	pfree(native);
	return buf.data;
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
cluster_backup_lmon_prepare_context(void)
{
	MemoryContext oldcontext;

	if (cluster_backup_lmon_context == NULL)
		cluster_backup_lmon_context = AllocSetContextCreate(
			TopMemoryContext, "cluster backup lmon context", ALLOCSET_START_SMALL_SIZES);
	else {
		cluster_backup_lmon_state = NULL;
		cluster_backup_lmon_tablespace_map = NULL;
		MemoryContextReset(cluster_backup_lmon_context);
	}

	oldcontext = MemoryContextSwitchTo(cluster_backup_lmon_context);
	cluster_backup_lmon_state = (BackupState *)palloc0(sizeof(BackupState));
	cluster_backup_lmon_tablespace_map = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);
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
			else {
				cluster_backup_lmon_prepare_context();
				register_persistent_abort_backup_handler();
				do_pg_backup_start(request->backup_id, request->fast, NULL,
								   cluster_backup_lmon_state, cluster_backup_lmon_tablespace_map);
				ack.start_redo_lsn = cluster_backup_lmon_state->startpoint;
				ack.checkpoint_lsn = cluster_backup_lmon_state->checkpointloc;
				ack.timeline = cluster_backup_lmon_state->starttli;
				result = CLUSTER_BACKUP_WIRE_RESULT_OK;
			}
			break;

		case CLUSTER_BACKUP_WIRE_OP_STOP:
			if (cluster_backup_lmon_state == NULL || get_backup_status() != SESSION_BACKUP_RUNNING)
				result = CLUSTER_BACKUP_WIRE_RESULT_NOT_IN_BACKUP;
			else {
				ack.start_redo_lsn = cluster_backup_lmon_state->startpoint;
				ack.checkpoint_lsn = cluster_backup_lmon_state->checkpointloc;
				do_pg_backup_stop(cluster_backup_lmon_state, request->waitforarchive);
				ack.stop_cut_lsn = cluster_backup_lmon_state->stoppoint;
				ack.cut_scn = cluster_backup_current_scn();
				ack.timeline = cluster_backup_lmon_state->stoptli;
				cluster_backup_lmon_reset_context();
				result = CLUSTER_BACKUP_WIRE_RESULT_OK;
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
			else if (RecoveryInProgress() || !XLogIsNeeded())
				result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
			else {
				ack.stop_cut_lsn = XLogRestorePoint(request->restore_point_name);
				ack.cut_scn = cluster_backup_current_scn();
				result = CLUSTER_BACKUP_WIRE_RESULT_OK;
			}
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
				thread->wal_included = true;
				thread->undo_included = true;
				thread->tt_included = true;
				thread->thread_id = ack->thread_id;
				thread->node_id = node_id;
				thread->start_redo_lsn = ack->start_redo_lsn;
				thread->checkpoint_lsn = ack->checkpoint_lsn;
				thread->start_tli = ack->timeline;
			} else if (ack->op == CLUSTER_BACKUP_WIRE_OP_STOP) {
				if (!thread->present && ack->start_redo_lsn != InvalidXLogRecPtr
					&& ack->checkpoint_lsn != InvalidXLogRecPtr) {
					thread->present = true;
					thread->wal_included = true;
					thread->undo_included = true;
					thread->tt_included = true;
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

Datum
pg_cluster_backup_start(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_BACKUP_START_COLS 4
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_BACKUP_START_COLS] = { 0 };
	bool nulls[PG_CLUSTER_BACKUP_START_COLS] = { 0 };
	text *backupid = PG_GETARG_TEXT_PP(0);
	bool fast = PG_GETARG_BOOL(1);
	char *backupidstr;
	SessionBackupState status;
	MemoryContext oldcontext;
	uint8 start_peers[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	bool have_peers;
	uint64 start_request_id = 0;
	ClusterBackupCoordWaitResult wait_result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to start a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_start");

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	backupidstr = text_to_cstring(backupid);
	if (strlen(backupidstr) >= CLUSTER_BACKUP_ID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cluster backup id is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_BACKUP_ID_MAX - 1)));

	status = get_backup_status();
	if (status == SESSION_BACKUP_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_IN_PROGRESS),
						errmsg("a backup is already in progress in this session")));

	cluster_backup_build_current_peer_bitmap(start_peers);
	have_peers = cluster_backup_bitmap_any_set(start_peers);

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

	register_persistent_abort_backup_handler();
	do_pg_backup_start(backupidstr, fast, NULL, cluster_backup_session_state,
					   cluster_backup_tablespace_map);
	cluster_backup_update_start(backupidstr, cluster_backup_session_state);

	if (have_peers) {
		if (!cluster_backup_begin_coord_request(CLUSTER_BACKUP_WIRE_OP_START, start_peers,
												backupidstr, NULL, fast, false, InvalidScn,
												&start_request_id)) {
			if (get_backup_status() == SESSION_BACKUP_RUNNING)
				do_pg_abort_backup(0, DatumGetBool(false));
			cluster_backup_mark_native_stopped(NULL);
			cluster_backup_cleanup_session_context();
			cluster_backup_raise_coord_enqueue_failure(CLUSTER_BACKUP_WIRE_OP_START);
		}
		if (!cluster_backup_wait_for_peer_acks(CLUSTER_BACKUP_WIRE_OP_START, start_request_id,
											   &wait_result)) {
			cluster_backup_abort_peers_best_effort(start_peers, backupidstr);
			if (get_backup_status() == SESSION_BACKUP_RUNNING)
				do_pg_abort_backup(0, DatumGetBool(false));
			cluster_backup_mark_native_stopped(NULL);
			cluster_backup_cleanup_session_context();
			cluster_backup_raise_peer_failure(CLUSTER_BACKUP_WIRE_OP_START, &wait_result);
		}
	}

	values[0] = CStringGetTextDatum(backupidstr);
	values[1] = LSNGetDatum(cluster_backup_session_state->startpoint);
	values[2] = LSNGetDatum(cluster_backup_session_state->checkpointloc);
	values[3] = Int32GetDatum((int32)cluster_backup_session_state->starttli);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
pg_cluster_backup_stop(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_BACKUP_STOP_COLS 4
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool nulls[PG_CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool waitforarchive = PG_GETARG_BOOL(0);
	ClusterBackupManifest manifest;
	ClusterRestorePoint point;
	SCN cut_scn;
	char *backup_label;
	XLogRecPtr thread_lsn[CLUSTER_MAX_NODES];
	SCN thread_scn[CLUSTER_MAX_NODES];
	int thread_index;
	uint16 thread_id;
	uint8 stop_peers[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	uint8 current_peers[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	bool have_peers;
	uint64 stop_request_id = 0;
	ClusterBackupCoordWaitResult wait_result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to stop a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_stop");

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (get_backup_status() != SESSION_BACKUP_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup is not in progress"),
						errhint("Did you call pg_cluster_backup_start()?")));
	if (cluster_backup_session_state == NULL || cluster_backup_tablespace_map == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup session state is missing")));

	have_peers = cluster_backup_get_backup_peer_bitmap(stop_peers);
	if (have_peers) {
		cluster_backup_build_current_peer_bitmap(current_peers);
		if (!cluster_backup_bitmap_equal(stop_peers, current_peers))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
							errmsg("cluster backup peer topology changed during backup"),
							errhint("Stop or abort the backup and retry after cluster membership "
									"settles.")));
		if (!cluster_backup_begin_coord_request(CLUSTER_BACKUP_WIRE_OP_STOP, stop_peers,
												cluster_backup_session_state->name, NULL, false,
												waitforarchive, InvalidScn, &stop_request_id))
			cluster_backup_raise_coord_enqueue_failure(CLUSTER_BACKUP_WIRE_OP_STOP);
	}

	do_pg_backup_stop(cluster_backup_session_state, waitforarchive);
	cluster_backup_mark_native_stopped(cluster_backup_session_state);
	cut_scn = cluster_backup_current_scn();

	MemSet(thread_lsn, 0, sizeof(thread_lsn));
	MemSet(thread_scn, 0, sizeof(thread_scn));
	thread_id = cluster_backup_local_thread_id();
	thread_index = (int)thread_id - 1;
	thread_lsn[thread_index] = cluster_backup_session_state->stoppoint;
	thread_scn[thread_index] = cut_scn;

	if (have_peers
		&& !cluster_backup_wait_for_peer_acks(CLUSTER_BACKUP_WIRE_OP_STOP, stop_request_id,
											  &wait_result)) {
		cluster_backup_cleanup_session_context();
		cluster_backup_raise_peer_failure(CLUSTER_BACKUP_WIRE_OP_STOP, &wait_result);
	}

	cluster_backup_fill_local_manifest(&manifest, cluster_backup_session_state, cut_scn);
	if (have_peers)
		cluster_backup_add_peer_stop_threads(&manifest, stop_request_id, thread_scn, thread_lsn);
	cut_scn = manifest.consistent_scn;
	cluster_backup_seal_manifest_or_error(&manifest);
	backup_label = cluster_backup_build_label(cluster_backup_session_state, &manifest, cut_scn);
	cluster_backup_update_stop(cluster_backup_session_state, &manifest, cut_scn);

	if (cluster_restore_point_build(&point, manifest.backup_id, thread_scn, thread_lsn,
									CLUSTER_MAX_NODES, true, true, manifest.incarnation)
		== CLUSTER_RESTORE_POINT_CUT_OK) {
		point.created_at = GetCurrentTimestamp();
		cluster_backup_add_restore_point(&point);
	}

	values[0] = Int64GetDatum((int64)cut_scn);
	values[1] = LSNGetDatum(cluster_backup_session_state->stoppoint);
	values[2] = Int64GetDatum((int64)manifest.manifest_crc);
	values[3] = CStringGetTextDatum(backup_label);

	pfree(backup_label);
	cluster_backup_cleanup_session_context();

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
pg_cluster_create_restore_point(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_RESTORE_POINT_COLS 3
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_RESTORE_POINT_COLS] = { 0 };
	bool nulls[PG_CLUSTER_RESTORE_POINT_COLS] = { 0 };
	text *restore_name = PG_GETARG_TEXT_PP(0);
	char *restore_name_str;
	XLogRecPtr restorepoint;
	SCN cut_scn;
	SCN thread_scn[CLUSTER_MAX_NODES];
	XLogRecPtr thread_lsn[CLUSTER_MAX_NODES];
	uint16 thread_id;
	int thread_index;
	ClusterRestorePoint point;
	ClusterRestorePointCutReason reason;
	uint8 restore_point_peers[CLUSTER_BACKUP_NODE_BITMAP_BYTES];
	bool have_peers;
	uint64 restore_request_id = 0;
	ClusterBackupCoordWaitResult wait_result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to create a cluster restore point")));
	cluster_backup_error_if_unavailable("pg_cluster_create_restore_point");
	if (RecoveryInProgress())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("recovery is in progress"),
						errhint("WAL control functions cannot be executed during recovery.")));
	if (!XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for creating a restore point"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server start.")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	restore_name_str = text_to_cstring(restore_name);
	if (strlen(restore_name_str) >= CLUSTER_RESTORE_POINT_NAME_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster restore point name is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_RESTORE_POINT_NAME_MAX - 1)));

	cluster_backup_build_current_peer_bitmap(restore_point_peers);
	have_peers = cluster_backup_bitmap_any_set(restore_point_peers);
	if (have_peers) {
		if (!cluster_backup_begin_coord_request(CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT,
												restore_point_peers, NULL, restore_name_str, false,
												false, InvalidScn, &restore_request_id))
			cluster_backup_raise_coord_enqueue_failure(CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT);
	}

	restorepoint = XLogRestorePoint(restore_name_str);
	cut_scn = cluster_backup_current_scn();

	MemSet(thread_scn, 0, sizeof(thread_scn));
	MemSet(thread_lsn, 0, sizeof(thread_lsn));
	thread_id = cluster_backup_local_thread_id();
	thread_index = (int)thread_id - 1;
	thread_scn[thread_index] = cut_scn;
	thread_lsn[thread_index] = restorepoint;
	if (have_peers
		&& !cluster_backup_wait_for_peer_acks(CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT,
											  restore_request_id, &wait_result))
		cluster_backup_raise_peer_failure(CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT, &wait_result);
	if (have_peers)
		cluster_backup_add_peer_restore_point_acks(restore_request_id, thread_scn, thread_lsn);
	reason = cluster_restore_point_build(&point, restore_name_str, thread_scn, thread_lsn,
										 CLUSTER_MAX_NODES, true, true, 0);
	if (reason != CLUSTER_RESTORE_POINT_CUT_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT),
						errmsg("could not build cluster restore point cut: %s",
							   cluster_restore_point_cut_reason_name(reason))));
	point.created_at = GetCurrentTimestamp();
	cluster_backup_add_restore_point(&point);
	cut_scn = point.cut_scn;

	values[0] = CStringGetTextDatum(restore_name_str);
	values[1] = Int64GetDatum((int64)cut_scn);
	values[2] = LSNGetDatum(restorepoint);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
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
