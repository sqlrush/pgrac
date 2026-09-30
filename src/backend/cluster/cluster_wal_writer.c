/*-------------------------------------------------------------------------
 * PGRAC: native WAL writer ownership at device-I/O boundaries.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "cluster_control_root_private.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"

/* Process-local only. No ordinary backend or postmaster child can adopt this
 * binding; the shared current-writer reference remains a separate INSTALL
 * obligation. Root selection/CF occurs outside WALWriteLock, never in flush. */
static struct {
	bool valid;
	int pid;
	uint64 epoch;
	XLogRecPtr first_lsn;
	uint8 operation_uuid[16];
	ClusterWalSourceRef ref;
} writer_startup;

static bool
writer_select(ClusterWalWriterToken *work)
{
	if (MyBackendType == B_STARTUP) {
		if (!writer_startup.valid || writer_startup.pid != MyProcPid
			|| writer_startup.epoch != work->epoch || ShutdownRequestPending)
			return false;
		work->ref = writer_startup.ref;
		work->startup_first_lsn = writer_startup.first_lsn;
		return true;
	}
	work->startup_first_lsn = 0;
	return cluster_wal_thread_current_v2_ref(&work->ref);
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *work)
{
	ClusterWriteFenceObservation fence;
	ClusterWalWriterToken fresh = { 0 };
	const ClusterControlRootIdentity *id = &work->ref.claim.identity;
	fresh.epoch = work->epoch;

	if (!cluster_enabled || !cluster_shared_config || !enableFsync
		|| !cluster_external_fence_runtime_active() || cluster_node_id != id->origin_node_id
		|| id->system_identifier != GetSystemIdentifier()
		|| cluster_qvotec_get_self_incarnation() != id->origin_owner_incarnation
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id)
			   != id->origin_owner_incarnation
		|| work->epoch == 0 || !writer_select(&fresh)
		|| fresh.startup_first_lsn != work->startup_first_lsn
		|| memcmp(&fresh.ref, &work->ref, sizeof(fresh.ref)) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	cluster_write_fence_observe(&fence);
	if (!fence.enforcing || !fence.attached || !fence.engaged || fence.self_fenced
		|| fence.epoch_current == 0 || (fence.expiry_us != 0 && fence.now_us >= fence.expiry_us))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	/* A healthy survivor's epoch can advance before QVOTEC republishes its
	 * token (lease is zero during that publication). None of these observations
	 * permits I/O/ACK. Defer to the native caller, which releases WALWriteLock
	 * before waiting and starts a fresh exact attempt. Never adopt a new epoch
	 * inside an already-running WAL write. */
	if (fence.expiry_us == 0 || fence.epoch_current != work->epoch
		|| fence.authorized_epoch != work->epoch || cluster_epoch_get_current() != work->epoch
		|| cluster_reconfig_has_pending_prebump_stage())
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	return fence.allowed ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *work)
{
	if (work == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(work, 0, sizeof(*work));
	work->epoch = cluster_epoch_get_current();
	if (!IsValidWalSegSize(wal_segment_size) || cluster_wal_threads_dir == NULL
		|| cluster_wal_threads_dir[0] == '\0' || DataDir == NULL || DataDir[0] == '\0'
		|| !writer_select(work) || work->ref.timeline != timeline || timeline == 0
		|| !cluster_wal_claim_v2_ref_valid(&work->ref.claim))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return cluster_wal_writer_check(work);
}

ClusterControlRootResult
cluster_wal_writer_ready(TimeLineID timeline)
{
	ClusterWalWriterToken work;
	return cluster_wal_writer_begin(timeline, &work);
}

ClusterControlRootResult
cluster_wal_writer_startup_prepare(const ClusterControlRootIdentity *self,
									const uint8 operation_uuid[16], XLogRecPtr *first_segment)
{
	ClusterWalStartupImage op, after;
	pg_cryptohash_ctx *hash;
	bool hashed;
	ClusterWalSourceRef input;
	ClusterControlRootResult result;
	ClusterWalWriterToken work = { 0 };
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];

	if (first_segment == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* Do not clear an output which aliases the identity before rejecting it. */
	if ((self != NULL && (uintptr_t)first_segment < (uintptr_t)self + sizeof(*self)
		 && (uintptr_t)self < (uintptr_t)first_segment + sizeof(*first_segment))
		|| (operation_uuid != NULL && (uintptr_t)first_segment < (uintptr_t)operation_uuid + 16
			&& (uintptr_t)operation_uuid < (uintptr_t)first_segment + sizeof(*first_segment)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*first_segment = 0;
	if (self == NULL || operation_uuid == NULL || CritSectionCount != 0
		|| MyBackendType != B_STARTUP || ShutdownRequestPending || writer_startup.valid
		|| LWLockHeldByMeInMode(WALWriteLock, LW_EXCLUSIVE) || !cluster_enabled
		|| !cluster_shared_config || DataDir == NULL || DataDir[0] == '\0'
		|| cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_v3_startup_read_writer(self, operation_uuid, &op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (op.phase != CLUSTER_WAL_STARTUP_INITIALIZING || op.segment_size != wal_segment_size
		|| !IsValidWalSegSize(wal_segment_size) || op.first_segment_lsn == 0
		|| op.first_segment_lsn % wal_segment_size != 0
		|| op.first_segment_lsn > UINT64_MAX - wal_segment_size
		|| !cluster_wal_thread_restart_v2_ref(&input)
		|| memcmp(&input.claim.identity, &op.predecessor.snapshot.identity,
				  sizeof(input.claim.identity))
			   != 0
		|| memcmp(input.claim.claim_sha256, op.predecessor.refs.claim_sha256, 32) != 0
		|| input.claim.database_incarnation != op.database_incarnation
		|| input.timeline != op.input_timeline)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = cluster_wal_claim_v2_encode(&op.claim, claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work.ref.claim.identity = op.claim.identity;
	work.ref.claim.database_incarnation = op.database_incarnation;
	work.ref.claim.max_config_generation = op.config_generation;
	work.ref.timeline = op.timeline;
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hashed = pg_cryptohash_init(hash) == 0
		&& pg_cryptohash_update(hash, claim, sizeof(claim)) == 0
		&& pg_cryptohash_final(hash, work.ref.claim.claim_sha256, 32) == 0;
	pg_cryptohash_free(hash);
	if (!hashed)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work.epoch = op.formation_epoch;
	work.startup_first_lsn = op.first_segment_lsn;
	result = cluster_control_bootstrap_wal_route(DataDir, cluster_wal_threads_dir, &work.ref);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* The selected owner verifies actual EMPTY and its exact root twice;
	 * a claim or a route alone cannot establish an empty generation. */
	result = cluster_control_root_v3_startup_read_writer(self, operation_uuid, &after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&after, &op, sizeof(op)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	writer_startup.pid = MyProcPid;
	writer_startup.epoch = work.epoch;
	writer_startup.first_lsn = op.first_segment_lsn;
	memcpy(writer_startup.operation_uuid, op.operation_uuid, 16);
	writer_startup.ref = work.ref;
	writer_startup.valid = true;
	result = cluster_wal_writer_check(&work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(&writer_startup, 0, sizeof(writer_startup));
	else
		*first_segment = op.first_segment_lsn;
	return result;
}

bool
cluster_wal_writer_startup_matches(const ClusterControlRootIdentity *self,
									const uint8 operation_uuid[16], XLogRecPtr first_segment)
{
	ClusterWalWriterToken work = { 0 };
	work.epoch = cluster_epoch_get_current();
	return self != NULL && operation_uuid != NULL && MyBackendType == B_STARTUP
		   && writer_select(&work) && work.startup_first_lsn == first_segment
		   && memcmp(self, &work.ref.claim.identity, sizeof(*self)) == 0
		   && memcmp(operation_uuid, writer_startup.operation_uuid, 16) == 0
		   && cluster_wal_writer_check(&work) == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
