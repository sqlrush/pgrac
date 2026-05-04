/* ----------
 * wait_event.c
 *	  Wait event reporting infrastructure.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/wait_event.c
 *
 * NOTES
 *
 * To make pgstat_report_wait_start() and pgstat_report_wait_end() as
 * lightweight as possible, they do not check if shared memory (MyProc
 * specifically, where the wait event is stored) is already available. Instead
 * we initially set my_wait_event_info to a process local variable, which then
 * is redirected to shared memory using pgstat_set_wait_event_storage(). For
 * the same reason pgstat_track_activities is not checked - the check adds
 * more work than it saves.
 *
 * ----------
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  Stage:        0.11
 *
 *	  Wired the 10 cluster wait classes (PG_WAIT_CLUSTER_* defined in
 *	  cluster_wait_events.h) into pgstat_get_wait_event_type() and
 *	  pgstat_get_wait_event(), and added 10 sub-helpers
 *	  (pgstat_get_wait_cluster_ges .. pgstat_get_wait_cluster_adg) that
 *	  return the human-readable name for each event in their owning
 *	  category.  All 46 event names match docs/wait-events-design.md
 *	  §3-§12 verbatim.
 *
 *	  Stage 0.11 only registers names; the call sites that emit these
 *	  events live in the spec for each owning subsystem.
 *
 *	  Related design:
 *	    docs/wait-events-design.md v1.1 §14
 *	    specs/spec-0.11-wait-events-framework.md
 *
 * ----------
 */
#include "postgres.h"

#include "storage/lmgr.h"	/* for GetLockNameFromTagType */
#include "storage/lwlock.h" /* for GetLWLockIdentifier */
#include "utils/wait_event.h"


static const char *pgstat_get_wait_activity(WaitEventActivity w);
static const char *pgstat_get_wait_client(WaitEventClient w);
static const char *pgstat_get_wait_ipc(WaitEventIPC w);
static const char *pgstat_get_wait_timeout(WaitEventTimeout w);
static const char *pgstat_get_wait_io(WaitEventIO w);

/* PGRAC: 10 cluster sub-helpers, each returning name for one category. */
static const char *pgstat_get_wait_cluster_ges(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_pcm(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_buffership(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_scn(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_reconfig(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_recovery(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_sinval(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_interconnect(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_undo(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_adg(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_sharedfs(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_startup_phase(WaitEventCluster w);
static const char *pgstat_get_wait_cluster_bgproc(WaitEventCluster w);


static uint32 local_my_wait_event_info;
uint32 *my_wait_event_info = &local_my_wait_event_info;


/*
 * Configure wait event reporting to report wait events to *wait_event_info.
 * *wait_event_info needs to be valid until pgstat_reset_wait_event_storage()
 * is called.
 *
 * Expected to be called during backend startup, to point my_wait_event_info
 * into shared memory.
 */
void
pgstat_set_wait_event_storage(uint32 *wait_event_info)
{
	my_wait_event_info = wait_event_info;
}

/*
 * Reset wait event storage location.
 *
 * Expected to be called during backend shutdown, before the location set up
 * pgstat_set_wait_event_storage() becomes invalid.
 */
void
pgstat_reset_wait_event_storage(void)
{
	my_wait_event_info = &local_my_wait_event_info;
}

/* ----------
 * pgstat_get_wait_event_type() -
 *
 *	Return a string representing the current wait event type, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event_type(uint32 wait_event_info)
{
	uint32 classId;
	const char *event_type;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;

	switch (classId) {
	case PG_WAIT_LWLOCK:
		event_type = "LWLock";
		break;
	case PG_WAIT_LOCK:
		event_type = "Lock";
		break;
	case PG_WAIT_BUFFER_PIN:
		event_type = "BufferPin";
		break;
	case PG_WAIT_ACTIVITY:
		event_type = "Activity";
		break;
	case PG_WAIT_CLIENT:
		event_type = "Client";
		break;
	case PG_WAIT_EXTENSION:
		event_type = "Extension";
		break;
	case PG_WAIT_IPC:
		event_type = "IPC";
		break;
	case PG_WAIT_TIMEOUT:
		event_type = "Timeout";
		break;
	case PG_WAIT_IO:
		event_type = "IO";
		break;
	/* PGRAC: 10 cluster wait classes (stage 0.11). */
	case PG_WAIT_CLUSTER_GES:
		event_type = "Cluster: GES";
		break;
	case PG_WAIT_CLUSTER_PCM:
		event_type = "Cluster: PCM";
		break;
	case PG_WAIT_CLUSTER_BUFFERSHIP:
		event_type = "Cluster: BufferShip";
		break;
	case PG_WAIT_CLUSTER_SCN:
		event_type = "Cluster: SCN";
		break;
	case PG_WAIT_CLUSTER_RECONFIG:
		event_type = "Cluster: Reconfig";
		break;
	case PG_WAIT_CLUSTER_RECOVERY:
		event_type = "Cluster: Recovery";
		break;
	case PG_WAIT_CLUSTER_SINVAL:
		event_type = "Cluster: Sinval";
		break;
	case PG_WAIT_CLUSTER_INTERCONNECT:
		event_type = "Cluster: Interconnect";
		break;
	case PG_WAIT_CLUSTER_UNDO:
		event_type = "Cluster: Undo";
		break;
	case PG_WAIT_CLUSTER_ADG:
		event_type = "Cluster: ADG";
		break;
	case PG_WAIT_CLUSTER_SHAREDFS:
		event_type = "Cluster: SharedFs";
		break;
	case PG_WAIT_CLUSTER_STARTUP_PHASE:
		event_type = "Cluster: StartupPhase";
		break;
	case PG_WAIT_CLUSTER_BGPROC:
		event_type = "Cluster: BgProc";
		break;
	default:
		event_type = "???";
		break;
	}

	return event_type;
}

/* ----------
 * pgstat_get_wait_event() -
 *
 *	Return a string representing the current wait event, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event(uint32 wait_event_info)
{
	uint32 classId;
	uint16 eventId;
	const char *event_name;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;
	eventId = wait_event_info & 0x0000FFFF;

	switch (classId) {
	case PG_WAIT_LWLOCK:
		event_name = GetLWLockIdentifier(classId, eventId);
		break;
	case PG_WAIT_LOCK:
		event_name = GetLockNameFromTagType(eventId);
		break;
	case PG_WAIT_BUFFER_PIN:
		event_name = "BufferPin";
		break;
	case PG_WAIT_ACTIVITY: {
		WaitEventActivity w = (WaitEventActivity)wait_event_info;

		event_name = pgstat_get_wait_activity(w);
		break;
	}
	case PG_WAIT_CLIENT: {
		WaitEventClient w = (WaitEventClient)wait_event_info;

		event_name = pgstat_get_wait_client(w);
		break;
	}
	case PG_WAIT_EXTENSION:
		event_name = "Extension";
		break;
	case PG_WAIT_IPC: {
		WaitEventIPC w = (WaitEventIPC)wait_event_info;

		event_name = pgstat_get_wait_ipc(w);
		break;
	}
	case PG_WAIT_TIMEOUT: {
		WaitEventTimeout w = (WaitEventTimeout)wait_event_info;

		event_name = pgstat_get_wait_timeout(w);
		break;
	}
	case PG_WAIT_IO: {
		WaitEventIO w = (WaitEventIO)wait_event_info;

		event_name = pgstat_get_wait_io(w);
		break;
	}
	/* PGRAC: 10 cluster wait classes (stage 0.11). */
	case PG_WAIT_CLUSTER_GES:
		event_name = pgstat_get_wait_cluster_ges((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_PCM:
		event_name = pgstat_get_wait_cluster_pcm((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_BUFFERSHIP:
		event_name = pgstat_get_wait_cluster_buffership((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_SCN:
		event_name = pgstat_get_wait_cluster_scn((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_RECONFIG:
		event_name = pgstat_get_wait_cluster_reconfig((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_RECOVERY:
		event_name = pgstat_get_wait_cluster_recovery((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_SINVAL:
		event_name = pgstat_get_wait_cluster_sinval((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_INTERCONNECT:
		event_name = pgstat_get_wait_cluster_interconnect((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_UNDO:
		event_name = pgstat_get_wait_cluster_undo((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_ADG:
		event_name = pgstat_get_wait_cluster_adg((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_SHAREDFS:
		event_name = pgstat_get_wait_cluster_sharedfs((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_STARTUP_PHASE:
		event_name = pgstat_get_wait_cluster_startup_phase((WaitEventCluster)wait_event_info);
		break;
	case PG_WAIT_CLUSTER_BGPROC:
		event_name = pgstat_get_wait_cluster_bgproc((WaitEventCluster)wait_event_info);
		break;
	default:
		event_name = "unknown wait event";
		break;
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_activity() -
 *
 * Convert WaitEventActivity to string.
 * ----------
 */
static const char *
pgstat_get_wait_activity(WaitEventActivity w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_ARCHIVER_MAIN:
		event_name = "ArchiverMain";
		break;
	case WAIT_EVENT_AUTOVACUUM_MAIN:
		event_name = "AutoVacuumMain";
		break;
	case WAIT_EVENT_BGWRITER_HIBERNATE:
		event_name = "BgWriterHibernate";
		break;
	case WAIT_EVENT_BGWRITER_MAIN:
		event_name = "BgWriterMain";
		break;
	case WAIT_EVENT_CHECKPOINTER_MAIN:
		event_name = "CheckpointerMain";
		break;
	case WAIT_EVENT_LOGICAL_APPLY_MAIN:
		event_name = "LogicalApplyMain";
		break;
	case WAIT_EVENT_LOGICAL_LAUNCHER_MAIN:
		event_name = "LogicalLauncherMain";
		break;
	case WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN:
		event_name = "LogicalParallelApplyMain";
		break;
	case WAIT_EVENT_RECOVERY_WAL_STREAM:
		event_name = "RecoveryWalStream";
		break;
	case WAIT_EVENT_SYSLOGGER_MAIN:
		event_name = "SysLoggerMain";
		break;
	case WAIT_EVENT_WAL_RECEIVER_MAIN:
		event_name = "WalReceiverMain";
		break;
	case WAIT_EVENT_WAL_SENDER_MAIN:
		event_name = "WalSenderMain";
		break;
	case WAIT_EVENT_WAL_WRITER_MAIN:
		event_name = "WalWriterMain";
		break;
		/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_client() -
 *
 * Convert WaitEventClient to string.
 * ----------
 */
static const char *
pgstat_get_wait_client(WaitEventClient w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_CLIENT_READ:
		event_name = "ClientRead";
		break;
	case WAIT_EVENT_CLIENT_WRITE:
		event_name = "ClientWrite";
		break;
	case WAIT_EVENT_GSS_OPEN_SERVER:
		event_name = "GSSOpenServer";
		break;
	case WAIT_EVENT_LIBPQWALRECEIVER_CONNECT:
		event_name = "LibPQWalReceiverConnect";
		break;
	case WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE:
		event_name = "LibPQWalReceiverReceive";
		break;
	case WAIT_EVENT_SSL_OPEN_SERVER:
		event_name = "SSLOpenServer";
		break;
	case WAIT_EVENT_WAL_SENDER_WAIT_WAL:
		event_name = "WalSenderWaitForWAL";
		break;
	case WAIT_EVENT_WAL_SENDER_WRITE_DATA:
		event_name = "WalSenderWriteData";
		break;
		/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_ipc() -
 *
 * Convert WaitEventIPC to string.
 * ----------
 */
static const char *
pgstat_get_wait_ipc(WaitEventIPC w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_APPEND_READY:
		event_name = "AppendReady";
		break;
	case WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND:
		event_name = "ArchiveCleanupCommand";
		break;
	case WAIT_EVENT_ARCHIVE_COMMAND:
		event_name = "ArchiveCommand";
		break;
	case WAIT_EVENT_BACKEND_TERMINATION:
		event_name = "BackendTermination";
		break;
	case WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE:
		event_name = "BackupWaitWalArchive";
		break;
	case WAIT_EVENT_BGWORKER_SHUTDOWN:
		event_name = "BgWorkerShutdown";
		break;
	case WAIT_EVENT_BGWORKER_STARTUP:
		event_name = "BgWorkerStartup";
		break;
	case WAIT_EVENT_BTREE_PAGE:
		event_name = "BtreePage";
		break;
	case WAIT_EVENT_BUFFER_IO:
		event_name = "BufferIO";
		break;
	case WAIT_EVENT_CHECKPOINT_DONE:
		event_name = "CheckpointDone";
		break;
	case WAIT_EVENT_CHECKPOINT_START:
		event_name = "CheckpointStart";
		break;
	case WAIT_EVENT_EXECUTE_GATHER:
		event_name = "ExecuteGather";
		break;
	case WAIT_EVENT_HASH_BATCH_ALLOCATE:
		event_name = "HashBatchAllocate";
		break;
	case WAIT_EVENT_HASH_BATCH_ELECT:
		event_name = "HashBatchElect";
		break;
	case WAIT_EVENT_HASH_BATCH_LOAD:
		event_name = "HashBatchLoad";
		break;
	case WAIT_EVENT_HASH_BUILD_ALLOCATE:
		event_name = "HashBuildAllocate";
		break;
	case WAIT_EVENT_HASH_BUILD_ELECT:
		event_name = "HashBuildElect";
		break;
	case WAIT_EVENT_HASH_BUILD_HASH_INNER:
		event_name = "HashBuildHashInner";
		break;
	case WAIT_EVENT_HASH_BUILD_HASH_OUTER:
		event_name = "HashBuildHashOuter";
		break;
	case WAIT_EVENT_HASH_GROW_BATCHES_DECIDE:
		event_name = "HashGrowBatchesDecide";
		break;
	case WAIT_EVENT_HASH_GROW_BATCHES_ELECT:
		event_name = "HashGrowBatchesElect";
		break;
	case WAIT_EVENT_HASH_GROW_BATCHES_FINISH:
		event_name = "HashGrowBatchesFinish";
		break;
	case WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE:
		event_name = "HashGrowBatchesReallocate";
		break;
	case WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION:
		event_name = "HashGrowBatchesRepartition";
		break;
	case WAIT_EVENT_HASH_GROW_BUCKETS_ELECT:
		event_name = "HashGrowBucketsElect";
		break;
	case WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE:
		event_name = "HashGrowBucketsReallocate";
		break;
	case WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT:
		event_name = "HashGrowBucketsReinsert";
		break;
	case WAIT_EVENT_LOGICAL_APPLY_SEND_DATA:
		event_name = "LogicalApplySendData";
		break;
	case WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE:
		event_name = "LogicalParallelApplyStateChange";
		break;
	case WAIT_EVENT_LOGICAL_SYNC_DATA:
		event_name = "LogicalSyncData";
		break;
	case WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE:
		event_name = "LogicalSyncStateChange";
		break;
	case WAIT_EVENT_MQ_INTERNAL:
		event_name = "MessageQueueInternal";
		break;
	case WAIT_EVENT_MQ_PUT_MESSAGE:
		event_name = "MessageQueuePutMessage";
		break;
	case WAIT_EVENT_MQ_RECEIVE:
		event_name = "MessageQueueReceive";
		break;
	case WAIT_EVENT_MQ_SEND:
		event_name = "MessageQueueSend";
		break;
	case WAIT_EVENT_PARALLEL_BITMAP_SCAN:
		event_name = "ParallelBitmapScan";
		break;
	case WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN:
		event_name = "ParallelCreateIndexScan";
		break;
	case WAIT_EVENT_PARALLEL_FINISH:
		event_name = "ParallelFinish";
		break;
	case WAIT_EVENT_PROCARRAY_GROUP_UPDATE:
		event_name = "ProcArrayGroupUpdate";
		break;
	case WAIT_EVENT_PROC_SIGNAL_BARRIER:
		event_name = "ProcSignalBarrier";
		break;
	case WAIT_EVENT_PROMOTE:
		event_name = "Promote";
		break;
	case WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT:
		event_name = "RecoveryConflictSnapshot";
		break;
	case WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE:
		event_name = "RecoveryConflictTablespace";
		break;
	case WAIT_EVENT_RECOVERY_END_COMMAND:
		event_name = "RecoveryEndCommand";
		break;
	case WAIT_EVENT_RECOVERY_PAUSE:
		event_name = "RecoveryPause";
		break;
	case WAIT_EVENT_REPLICATION_ORIGIN_DROP:
		event_name = "ReplicationOriginDrop";
		break;
	case WAIT_EVENT_REPLICATION_SLOT_DROP:
		event_name = "ReplicationSlotDrop";
		break;
	case WAIT_EVENT_RESTORE_COMMAND:
		event_name = "RestoreCommand";
		break;
	case WAIT_EVENT_SAFE_SNAPSHOT:
		event_name = "SafeSnapshot";
		break;
	case WAIT_EVENT_SYNC_REP:
		event_name = "SyncRep";
		break;
	case WAIT_EVENT_WAL_RECEIVER_EXIT:
		event_name = "WalReceiverExit";
		break;
	case WAIT_EVENT_WAL_RECEIVER_WAIT_START:
		event_name = "WalReceiverWaitStart";
		break;
	case WAIT_EVENT_XACT_GROUP_UPDATE:
		event_name = "XactGroupUpdate";
		break;
		/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_timeout() -
 *
 * Convert WaitEventTimeout to string.
 * ----------
 */
static const char *
pgstat_get_wait_timeout(WaitEventTimeout w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_BASE_BACKUP_THROTTLE:
		event_name = "BaseBackupThrottle";
		break;
	case WAIT_EVENT_CHECKPOINT_WRITE_DELAY:
		event_name = "CheckpointWriteDelay";
		break;
	case WAIT_EVENT_PG_SLEEP:
		event_name = "PgSleep";
		break;
	case WAIT_EVENT_RECOVERY_APPLY_DELAY:
		event_name = "RecoveryApplyDelay";
		break;
	case WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL:
		event_name = "RecoveryRetrieveRetryInterval";
		break;
	case WAIT_EVENT_REGISTER_SYNC_REQUEST:
		event_name = "RegisterSyncRequest";
		break;
	case WAIT_EVENT_SPIN_DELAY:
		event_name = "SpinDelay";
		break;
	case WAIT_EVENT_VACUUM_DELAY:
		event_name = "VacuumDelay";
		break;
	case WAIT_EVENT_VACUUM_TRUNCATE:
		event_name = "VacuumTruncate";
		break;
		/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_io() -
 *
 * Convert WaitEventIO to string.
 * ----------
 */
static const char *
pgstat_get_wait_io(WaitEventIO w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_BASEBACKUP_READ:
		event_name = "BaseBackupRead";
		break;
	case WAIT_EVENT_BASEBACKUP_SYNC:
		event_name = "BaseBackupSync";
		break;
	case WAIT_EVENT_BASEBACKUP_WRITE:
		event_name = "BaseBackupWrite";
		break;
	case WAIT_EVENT_BUFFILE_READ:
		event_name = "BufFileRead";
		break;
	case WAIT_EVENT_BUFFILE_WRITE:
		event_name = "BufFileWrite";
		break;
	case WAIT_EVENT_BUFFILE_TRUNCATE:
		event_name = "BufFileTruncate";
		break;
	case WAIT_EVENT_CONTROL_FILE_READ:
		event_name = "ControlFileRead";
		break;
	case WAIT_EVENT_CONTROL_FILE_SYNC:
		event_name = "ControlFileSync";
		break;
	case WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE:
		event_name = "ControlFileSyncUpdate";
		break;
	case WAIT_EVENT_CONTROL_FILE_WRITE:
		event_name = "ControlFileWrite";
		break;
	case WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE:
		event_name = "ControlFileWriteUpdate";
		break;
	case WAIT_EVENT_COPY_FILE_READ:
		event_name = "CopyFileRead";
		break;
	case WAIT_EVENT_COPY_FILE_WRITE:
		event_name = "CopyFileWrite";
		break;
	case WAIT_EVENT_DATA_FILE_EXTEND:
		event_name = "DataFileExtend";
		break;
	case WAIT_EVENT_DATA_FILE_FLUSH:
		event_name = "DataFileFlush";
		break;
	case WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC:
		event_name = "DataFileImmediateSync";
		break;
	case WAIT_EVENT_DATA_FILE_PREFETCH:
		event_name = "DataFilePrefetch";
		break;
	case WAIT_EVENT_DATA_FILE_READ:
		event_name = "DataFileRead";
		break;
	case WAIT_EVENT_DATA_FILE_SYNC:
		event_name = "DataFileSync";
		break;
	case WAIT_EVENT_DATA_FILE_TRUNCATE:
		event_name = "DataFileTruncate";
		break;
	case WAIT_EVENT_DATA_FILE_WRITE:
		event_name = "DataFileWrite";
		break;
	case WAIT_EVENT_DSM_ALLOCATE:
		event_name = "DSMAllocate";
		break;
	case WAIT_EVENT_DSM_FILL_ZERO_WRITE:
		event_name = "DSMFillZeroWrite";
		break;
	case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ:
		event_name = "LockFileAddToDataDirRead";
		break;
	case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC:
		event_name = "LockFileAddToDataDirSync";
		break;
	case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE:
		event_name = "LockFileAddToDataDirWrite";
		break;
	case WAIT_EVENT_LOCK_FILE_CREATE_READ:
		event_name = "LockFileCreateRead";
		break;
	case WAIT_EVENT_LOCK_FILE_CREATE_SYNC:
		event_name = "LockFileCreateSync";
		break;
	case WAIT_EVENT_LOCK_FILE_CREATE_WRITE:
		event_name = "LockFileCreateWrite";
		break;
	case WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ:
		event_name = "LockFileReCheckDataDirRead";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC:
		event_name = "LogicalRewriteCheckpointSync";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC:
		event_name = "LogicalRewriteMappingSync";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE:
		event_name = "LogicalRewriteMappingWrite";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_SYNC:
		event_name = "LogicalRewriteSync";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE:
		event_name = "LogicalRewriteTruncate";
		break;
	case WAIT_EVENT_LOGICAL_REWRITE_WRITE:
		event_name = "LogicalRewriteWrite";
		break;
	case WAIT_EVENT_RELATION_MAP_READ:
		event_name = "RelationMapRead";
		break;
	case WAIT_EVENT_RELATION_MAP_REPLACE:
		event_name = "RelationMapReplace";
		break;
	case WAIT_EVENT_RELATION_MAP_WRITE:
		event_name = "RelationMapWrite";
		break;
	case WAIT_EVENT_REORDER_BUFFER_READ:
		event_name = "ReorderBufferRead";
		break;
	case WAIT_EVENT_REORDER_BUFFER_WRITE:
		event_name = "ReorderBufferWrite";
		break;
	case WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ:
		event_name = "ReorderLogicalMappingRead";
		break;
	case WAIT_EVENT_REPLICATION_SLOT_READ:
		event_name = "ReplicationSlotRead";
		break;
	case WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC:
		event_name = "ReplicationSlotRestoreSync";
		break;
	case WAIT_EVENT_REPLICATION_SLOT_SYNC:
		event_name = "ReplicationSlotSync";
		break;
	case WAIT_EVENT_REPLICATION_SLOT_WRITE:
		event_name = "ReplicationSlotWrite";
		break;
	case WAIT_EVENT_SLRU_FLUSH_SYNC:
		event_name = "SLRUFlushSync";
		break;
	case WAIT_EVENT_SLRU_READ:
		event_name = "SLRURead";
		break;
	case WAIT_EVENT_SLRU_SYNC:
		event_name = "SLRUSync";
		break;
	case WAIT_EVENT_SLRU_WRITE:
		event_name = "SLRUWrite";
		break;
	case WAIT_EVENT_SNAPBUILD_READ:
		event_name = "SnapbuildRead";
		break;
	case WAIT_EVENT_SNAPBUILD_SYNC:
		event_name = "SnapbuildSync";
		break;
	case WAIT_EVENT_SNAPBUILD_WRITE:
		event_name = "SnapbuildWrite";
		break;
	case WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC:
		event_name = "TimelineHistoryFileSync";
		break;
	case WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE:
		event_name = "TimelineHistoryFileWrite";
		break;
	case WAIT_EVENT_TIMELINE_HISTORY_READ:
		event_name = "TimelineHistoryRead";
		break;
	case WAIT_EVENT_TIMELINE_HISTORY_SYNC:
		event_name = "TimelineHistorySync";
		break;
	case WAIT_EVENT_TIMELINE_HISTORY_WRITE:
		event_name = "TimelineHistoryWrite";
		break;
	case WAIT_EVENT_TWOPHASE_FILE_READ:
		event_name = "TwophaseFileRead";
		break;
	case WAIT_EVENT_TWOPHASE_FILE_SYNC:
		event_name = "TwophaseFileSync";
		break;
	case WAIT_EVENT_TWOPHASE_FILE_WRITE:
		event_name = "TwophaseFileWrite";
		break;
	case WAIT_EVENT_VERSION_FILE_SYNC:
		event_name = "VersionFileSync";
		break;
	case WAIT_EVENT_VERSION_FILE_WRITE:
		event_name = "VersionFileWrite";
		break;
	case WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ:
		event_name = "WALSenderTimelineHistoryRead";
		break;
	case WAIT_EVENT_WAL_BOOTSTRAP_SYNC:
		event_name = "WALBootstrapSync";
		break;
	case WAIT_EVENT_WAL_BOOTSTRAP_WRITE:
		event_name = "WALBootstrapWrite";
		break;
	case WAIT_EVENT_WAL_COPY_READ:
		event_name = "WALCopyRead";
		break;
	case WAIT_EVENT_WAL_COPY_SYNC:
		event_name = "WALCopySync";
		break;
	case WAIT_EVENT_WAL_COPY_WRITE:
		event_name = "WALCopyWrite";
		break;
	case WAIT_EVENT_WAL_INIT_SYNC:
		event_name = "WALInitSync";
		break;
	case WAIT_EVENT_WAL_INIT_WRITE:
		event_name = "WALInitWrite";
		break;
	case WAIT_EVENT_WAL_READ:
		event_name = "WALRead";
		break;
	case WAIT_EVENT_WAL_SYNC:
		event_name = "WALSync";
		break;
	case WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN:
		event_name = "WALSyncMethodAssign";
		break;
	case WAIT_EVENT_WAL_WRITE:
		event_name = "WALWrite";
		break;

		/* no default case, so that compiler will warn */
	}

	return event_name;
}


/* ----------
 * PGRAC: 10 cluster sub-helpers (stage 0.11).
 *
 * Each sub-helper covers one of the 10 cluster categories declared in
 * src/include/cluster/cluster_wait_events.h.  Event names match
 * docs/wait-events-design.md §3-§12 verbatim.
 *
 * Stage 0.11 only registers names; pgstat_report_wait_start() call
 * sites for each event live in the spec for its owning subsystem.
 * ----------
 */

static const char *
pgstat_get_wait_cluster_ges(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_GES_ENQUEUE_ACQUIRE:
		event_name = "GesEnqueueAcquire";
		break;
	case WAIT_EVENT_GES_ENQUEUE_CONVERT:
		event_name = "GesEnqueueConvert";
		break;
	case WAIT_EVENT_GES_ENQUEUE_RELEASE_ACK:
		event_name = "GesEnqueueReleaseAck";
		break;
	case WAIT_EVENT_GES_MASTER_QUERY:
		event_name = "GesMasterQuery";
		break;
	case WAIT_EVENT_GES_LOCAL_FAST_PATH:
		event_name = "GesLocalFastPath";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_pcm(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_PCM_BLOCK_READ_N_S:
		event_name = "PcmBlockReadNS";
		break;
	case WAIT_EVENT_PCM_BLOCK_READ_N_X:
		event_name = "PcmBlockReadNX";
		break;
	case WAIT_EVENT_PCM_BLOCK_WRITE_S_X:
		event_name = "PcmBlockWriteSX";
		break;
	case WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT:
		event_name = "PcmBlockConvertWait";
		break;
	case WAIT_EVENT_PCM_BLOCK_DOWNGRADE:
		event_name = "PcmBlockDowngrade";
		break;
	case WAIT_EVENT_PCM_ITL_CLEANOUT:
		event_name = "PcmItlCleanout";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_buffership(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_BUFFER_SHIP_CR_BUILD:
		event_name = "BufferShipCrBuild";
		break;
	case WAIT_EVENT_BUFFER_SHIP_CR_SEND:
		event_name = "BufferShipCrSend";
		break;
	case WAIT_EVENT_BUFFER_SHIP_CR_RECEIVE:
		event_name = "BufferShipCrReceive";
		break;
	case WAIT_EVENT_BUFFER_SHIP_CURRENT_SEND:
		event_name = "BufferShipCurrentSend";
		break;
	case WAIT_EVENT_BUFFER_SHIP_CURRENT_RECEIVE:
		event_name = "BufferShipCurrentReceive";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_scn(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_SCN_BOC_FLUSH_WAIT:
		event_name = "ScnBocFlushWait";
		break;
	case WAIT_EVENT_SCN_PIGGYBACK_MERGE:
		event_name = "ScnPiggybackMerge";
		break;
	case WAIT_EVENT_SCN_CROSS_NODE_COMPARE:
		event_name = "ScnCrossNodeCompare";
		break;
	case WAIT_EVENT_SCN_ADVANCE_BROADCAST:
		event_name = "ScnAdvanceBroadcast";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_reconfig(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_RECONFIG_GRD_REBUILD:
		event_name = "ReconfigGrdRebuild";
		break;
	case WAIT_EVENT_RECONFIG_LOCK_RECOVERY:
		event_name = "ReconfigLockRecovery";
		break;
	case WAIT_EVENT_RECONFIG_FENCE_WAIT:
		event_name = "ReconfigFenceWait";
		break;
	case WAIT_EVENT_RECONFIG_MASTER_SELECTION:
		event_name = "ReconfigMasterSelection";
		break;
	case WAIT_EVENT_RECONFIG_BARRIER_WAIT:
		event_name = "ReconfigBarrierWait";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_recovery(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_RECOVERY_WAL_FETCH:
		event_name = "RecoveryWalFetch";
		break;
	case WAIT_EVENT_RECOVERY_KWAY_MERGE:
		event_name = "RecoveryKwayMerge";
		break;
	case WAIT_EVENT_RECOVERY_APPLY_PER_THREAD:
		event_name = "RecoveryApplyPerThread";
		break;
	case WAIT_EVENT_RECOVERY_UNDO_REPLAY:
		event_name = "RecoveryUndoReplay";
		break;
	case WAIT_EVENT_RECOVERY_PCM_STATE_RESTORE:
		event_name = "RecoveryPcmStateRestore";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_sinval(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_SINVAL_BROADCAST_SEND:
		event_name = "SinvalBroadcastSend";
		break;
	case WAIT_EVENT_SINVAL_BROADCAST_RECEIVE:
		event_name = "SinvalBroadcastReceive";
		break;
	case WAIT_EVENT_SINVAL_INJECT_LOCAL_QUEUE:
		event_name = "SinvalInjectLocalQueue";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_interconnect(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_INTERCONNECT_RDMA_SEND:
		event_name = "InterconnectRdmaSend";
		break;
	case WAIT_EVENT_INTERCONNECT_RDMA_RECV:
		event_name = "InterconnectRdmaRecv";
		break;
	case WAIT_EVENT_INTERCONNECT_TCP_FALLBACK:
		event_name = "InterconnectTcpFallback";
		break;
	case WAIT_EVENT_INTERCONNECT_TIER_SWITCH:
		event_name = "InterconnectTierSwitch";
		break;
	case WAIT_EVENT_INTERCONNECT_CONNECT_RETRY:
		event_name = "InterconnectConnectRetry";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_undo(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_UNDO_REMOTE_READ:
		event_name = "UndoRemoteRead";
		break;
	case WAIT_EVENT_UNDO_TT_LOOKUP_REMOTE:
		event_name = "UndoTtLookupRemote";
		break;
	case WAIT_EVENT_UNDO_SEGMENT_FETCH:
		event_name = "UndoSegmentFetch";
		break;
	case WAIT_EVENT_UNDO_RETENTION_WAIT:
		event_name = "UndoRetentionWait";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_adg(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_ADG_MRP_APPLY_WAIT:
		event_name = "AdgMrpApplyWait";
		break;
	case WAIT_EVENT_ADG_WAL_RECEIVE_LAG:
		event_name = "AdgWalReceiveLag";
		break;
	case WAIT_EVENT_ADG_READ_SNAPSHOT_WAIT:
		event_name = "AdgReadSnapshotWait";
		break;
	case WAIT_EVENT_ADG_SCN_SYNC_WAIT:
		event_name = "AdgScnSyncWait";
		break;
	default:
		break;
	}

	return event_name;
}

static const char *
pgstat_get_wait_cluster_sharedfs(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_CLUSTER_SHARED_FS_READ:
		event_name = "ClusterSharedFsRead";
		break;
	case WAIT_EVENT_CLUSTER_SHARED_FS_WRITE:
		event_name = "ClusterSharedFsWrite";
		break;
	case WAIT_EVENT_CLUSTER_SHARED_FS_EXTEND:
		event_name = "ClusterSharedFsExtend";
		break;
	case WAIT_EVENT_CLUSTER_SHARED_FS_TRUNCATE:
		event_name = "ClusterSharedFsTruncate";
		break;
	case WAIT_EVENT_CLUSTER_SHARED_FS_FSYNC:
		event_name = "ClusterSharedFsFsync";
		break;
	default:
		break;
	}

	return event_name;
}

/* PGRAC: spec-1.10 (2026-05-03) cluster startup phase wait events. */
static const char *
pgstat_get_wait_cluster_startup_phase(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_CLUSTER_STARTUP_PHASE_0:
		event_name = "ClusterStartupPhase0Wait";
		break;
	case WAIT_EVENT_CLUSTER_STARTUP_PHASE_1:
		event_name = "ClusterStartupPhase1Wait";
		break;
	case WAIT_EVENT_CLUSTER_STARTUP_PHASE_2:
		event_name = "ClusterStartupPhase2Wait";
		break;
	case WAIT_EVENT_CLUSTER_STARTUP_PHASE_3:
		event_name = "ClusterStartupPhase3Wait";
		break;
	case WAIT_EVENT_CLUSTER_STARTUP_PHASE_4:
		event_name = "ClusterStartupPhase4Wait";
		break;
	default:
		break;
	}

	return event_name;
}

/* PGRAC: spec-1.11 Sprint B (2026-05-04) cluster background-process wait events. */
static const char *
pgstat_get_wait_cluster_bgproc(WaitEventCluster w)
{
	const char *event_name = "unknown wait event";

	switch (w) {
	case WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP:
		event_name = "ClusterBgProcLmonMainLoop";
		break;
	case WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP:
		event_name = "ClusterBgProcLckMainLoop";
		break;
	case WAIT_EVENT_CLUSTER_BGPROC_DIAG_MAIN_LOOP:
		event_name = "ClusterBgProcDiagMainLoop";
		break;
	default:
		break;
	}

	return event_name;
}
