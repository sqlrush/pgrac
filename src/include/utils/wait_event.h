/*-------------------------------------------------------------------------
 * wait_event.h
 *	  Definitions related to wait event reporting
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * src/include/utils/wait_event.h
 * ----------
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  Stage:        0.11 / 1.1
 *
 *	  Added the WaitEventCluster enum (now 51 entries spread across
 *	  11 class IDs 0x10000000..0x1a000000) and pulled in
 *	  cluster/cluster_wait_events.h for the class-ID macros.  No PG
 *	  native enum is touched; the cluster enum is independent.
 *
 *	  Stage 0.11 registered the original 46 entries across 10 classes
 *	  (GES / PCM / BufferShip / SCN / Reconfig / Recovery / Sinval /
 *	  Interconnect / Undo / ADG).  Stage 1.1 extended with the
 *	  Cluster: SharedFs class and 5 events for cluster_shared_fs
 *	  (read / write / extend / truncate / fsync).
 *
 *	  Identifiers are registered here; the call sites that emit
 *	  these wait events are wired up in the spec for each owning
 *	  subsystem (GES events in spec-1.X-ges, PCM events in
 *	  spec-2.X-pcm, SharedFs events when stage 6+ wires production
 *	  perf instrumentation, ...).
 *
 *	  Related design:
 *	    docs/wait-events-design.md v1.1 §14.1 / §14.2
 *	    specs/spec-0.11-wait-events-framework.md
 *	    specs/spec-1.1-shared-fs-skeleton.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef WAIT_EVENT_H
#define WAIT_EVENT_H

/* PGRAC: pull in cluster wait class IDs (10 PG_WAIT_CLUSTER_* macros). */
#include "cluster/cluster_wait_events.h"


/* ----------
 * Wait Classes
 * ----------
 */
#define PG_WAIT_LWLOCK 0x01000000U
#define PG_WAIT_LOCK 0x03000000U
#define PG_WAIT_BUFFER_PIN 0x04000000U
#define PG_WAIT_ACTIVITY 0x05000000U
#define PG_WAIT_CLIENT 0x06000000U
#define PG_WAIT_EXTENSION 0x07000000U
#define PG_WAIT_IPC 0x08000000U
#define PG_WAIT_TIMEOUT 0x09000000U
#define PG_WAIT_IO 0x0A000000U

/* ----------
 * Wait Events - Activity
 *
 * Use this category when a process is waiting because it has no work to do,
 * unless the "Client" or "Timeout" category describes the situation better.
 * Typically, this should only be used for background processes.
 * ----------
 */
typedef enum {
	WAIT_EVENT_ARCHIVER_MAIN = PG_WAIT_ACTIVITY,
	WAIT_EVENT_AUTOVACUUM_MAIN,
	WAIT_EVENT_BGWRITER_HIBERNATE,
	WAIT_EVENT_BGWRITER_MAIN,
	WAIT_EVENT_CHECKPOINTER_MAIN,
	WAIT_EVENT_LOGICAL_APPLY_MAIN,
	WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
	WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN,
	WAIT_EVENT_RECOVERY_WAL_STREAM,
	WAIT_EVENT_SYSLOGGER_MAIN,
	WAIT_EVENT_WAL_RECEIVER_MAIN,
	WAIT_EVENT_WAL_SENDER_MAIN,
	WAIT_EVENT_WAL_WRITER_MAIN
} WaitEventActivity;

/* ----------
 * Wait Events - Client
 *
 * Use this category when a process is waiting to send data to or receive data
 * from the frontend process to which it is connected.  This is never used for
 * a background process, which has no client connection.
 * ----------
 */
typedef enum {
	WAIT_EVENT_CLIENT_READ = PG_WAIT_CLIENT,
	WAIT_EVENT_CLIENT_WRITE,
	WAIT_EVENT_GSS_OPEN_SERVER,
	WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
	WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
	WAIT_EVENT_SSL_OPEN_SERVER,
	WAIT_EVENT_WAL_SENDER_WAIT_WAL,
	WAIT_EVENT_WAL_SENDER_WRITE_DATA,
} WaitEventClient;

/* ----------
 * Wait Events - IPC
 *
 * Use this category when a process cannot complete the work it is doing because
 * it is waiting for a notification from another process.
 * ----------
 */
typedef enum {
	WAIT_EVENT_APPEND_READY = PG_WAIT_IPC,
	WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
	WAIT_EVENT_ARCHIVE_COMMAND,
	WAIT_EVENT_BACKEND_TERMINATION,
	WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
	WAIT_EVENT_BGWORKER_SHUTDOWN,
	WAIT_EVENT_BGWORKER_STARTUP,
	WAIT_EVENT_BTREE_PAGE,
	WAIT_EVENT_BUFFER_IO,
	WAIT_EVENT_CHECKPOINT_DONE,
	WAIT_EVENT_CHECKPOINT_START,
	WAIT_EVENT_EXECUTE_GATHER,
	WAIT_EVENT_HASH_BATCH_ALLOCATE,
	WAIT_EVENT_HASH_BATCH_ELECT,
	WAIT_EVENT_HASH_BATCH_LOAD,
	WAIT_EVENT_HASH_BUILD_ALLOCATE,
	WAIT_EVENT_HASH_BUILD_ELECT,
	WAIT_EVENT_HASH_BUILD_HASH_INNER,
	WAIT_EVENT_HASH_BUILD_HASH_OUTER,
	WAIT_EVENT_HASH_GROW_BATCHES_DECIDE,
	WAIT_EVENT_HASH_GROW_BATCHES_ELECT,
	WAIT_EVENT_HASH_GROW_BATCHES_FINISH,
	WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE,
	WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION,
	WAIT_EVENT_HASH_GROW_BUCKETS_ELECT,
	WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE,
	WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT,
	WAIT_EVENT_LOGICAL_APPLY_SEND_DATA,
	WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE,
	WAIT_EVENT_LOGICAL_SYNC_DATA,
	WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
	WAIT_EVENT_MQ_INTERNAL,
	WAIT_EVENT_MQ_PUT_MESSAGE,
	WAIT_EVENT_MQ_RECEIVE,
	WAIT_EVENT_MQ_SEND,
	WAIT_EVENT_PARALLEL_BITMAP_SCAN,
	WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
	WAIT_EVENT_PARALLEL_FINISH,
	WAIT_EVENT_PROCARRAY_GROUP_UPDATE,
	WAIT_EVENT_PROC_SIGNAL_BARRIER,
	WAIT_EVENT_PROMOTE,
	WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
	WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
	WAIT_EVENT_RECOVERY_END_COMMAND,
	WAIT_EVENT_RECOVERY_PAUSE,
	WAIT_EVENT_REPLICATION_ORIGIN_DROP,
	WAIT_EVENT_REPLICATION_SLOT_DROP,
	WAIT_EVENT_RESTORE_COMMAND,
	WAIT_EVENT_SAFE_SNAPSHOT,
	WAIT_EVENT_SYNC_REP,
	WAIT_EVENT_WAL_RECEIVER_EXIT,
	WAIT_EVENT_WAL_RECEIVER_WAIT_START,
	WAIT_EVENT_XACT_GROUP_UPDATE
} WaitEventIPC;

/* ----------
 * Wait Events - Timeout
 *
 * Use this category when a process is waiting for a timeout to expire.
 * ----------
 */
typedef enum {
	WAIT_EVENT_BASE_BACKUP_THROTTLE = PG_WAIT_TIMEOUT,
	WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
	WAIT_EVENT_PG_SLEEP,
	WAIT_EVENT_RECOVERY_APPLY_DELAY,
	WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
	WAIT_EVENT_REGISTER_SYNC_REQUEST,
	WAIT_EVENT_SPIN_DELAY,
	WAIT_EVENT_VACUUM_DELAY,
	WAIT_EVENT_VACUUM_TRUNCATE
} WaitEventTimeout;

/* ----------
 * Wait Events - IO
 *
 * Use this category when a process is waiting for a IO.
 * ----------
 */
typedef enum {
	WAIT_EVENT_BASEBACKUP_READ = PG_WAIT_IO,
	WAIT_EVENT_BASEBACKUP_SYNC,
	WAIT_EVENT_BASEBACKUP_WRITE,
	WAIT_EVENT_BUFFILE_READ,
	WAIT_EVENT_BUFFILE_WRITE,
	WAIT_EVENT_BUFFILE_TRUNCATE,
	WAIT_EVENT_CONTROL_FILE_READ,
	WAIT_EVENT_CONTROL_FILE_SYNC,
	WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE,
	WAIT_EVENT_CONTROL_FILE_WRITE,
	WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE,
	WAIT_EVENT_COPY_FILE_READ,
	WAIT_EVENT_COPY_FILE_WRITE,
	WAIT_EVENT_DATA_FILE_EXTEND,
	WAIT_EVENT_DATA_FILE_FLUSH,
	WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC,
	WAIT_EVENT_DATA_FILE_PREFETCH,
	WAIT_EVENT_DATA_FILE_READ,
	WAIT_EVENT_DATA_FILE_SYNC,
	WAIT_EVENT_DATA_FILE_TRUNCATE,
	WAIT_EVENT_DATA_FILE_WRITE,
	WAIT_EVENT_DSM_ALLOCATE,
	WAIT_EVENT_DSM_FILL_ZERO_WRITE,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE,
	WAIT_EVENT_LOCK_FILE_CREATE_READ,
	WAIT_EVENT_LOCK_FILE_CREATE_SYNC,
	WAIT_EVENT_LOCK_FILE_CREATE_WRITE,
	WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ,
	WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE,
	WAIT_EVENT_LOGICAL_REWRITE_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE,
	WAIT_EVENT_LOGICAL_REWRITE_WRITE,
	WAIT_EVENT_RELATION_MAP_READ,
	WAIT_EVENT_RELATION_MAP_REPLACE,
	WAIT_EVENT_RELATION_MAP_WRITE,
	WAIT_EVENT_REORDER_BUFFER_READ,
	WAIT_EVENT_REORDER_BUFFER_WRITE,
	WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ,
	WAIT_EVENT_REPLICATION_SLOT_READ,
	WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_WRITE,
	WAIT_EVENT_SLRU_FLUSH_SYNC,
	WAIT_EVENT_SLRU_READ,
	WAIT_EVENT_SLRU_SYNC,
	WAIT_EVENT_SLRU_WRITE,
	WAIT_EVENT_SNAPBUILD_READ,
	WAIT_EVENT_SNAPBUILD_SYNC,
	WAIT_EVENT_SNAPBUILD_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_READ,
	WAIT_EVENT_TIMELINE_HISTORY_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_WRITE,
	WAIT_EVENT_TWOPHASE_FILE_READ,
	WAIT_EVENT_TWOPHASE_FILE_SYNC,
	WAIT_EVENT_TWOPHASE_FILE_WRITE,
	WAIT_EVENT_VERSION_FILE_WRITE,
	WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ,
	WAIT_EVENT_WAL_BOOTSTRAP_SYNC,
	WAIT_EVENT_WAL_BOOTSTRAP_WRITE,
	WAIT_EVENT_WAL_COPY_READ,
	WAIT_EVENT_WAL_COPY_SYNC,
	WAIT_EVENT_WAL_COPY_WRITE,
	WAIT_EVENT_WAL_INIT_SYNC,
	WAIT_EVENT_WAL_INIT_WRITE,
	WAIT_EVENT_WAL_READ,
	WAIT_EVENT_WAL_SYNC,
	WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN,
	WAIT_EVENT_WAL_WRITE,
	WAIT_EVENT_VERSION_FILE_SYNC
} WaitEventIO;


/* ----------
 * Wait Events - Cluster (PGRAC, stage 0.11)
 *
 * pgrac cluster wait events span 11 categories (10 from stage 0.11 plus
 * Cluster: SharedFs added by stage 1.1), each with its own class
 * ID in the upper byte (PG_WAIT_CLUSTER_* macros from cluster_wait_events.h).
 * Within a category, events are densely packed.  See
 * docs/wait-events-design.md §3-§12 for per-event semantics and
 * specs/spec-0.11-wait-events-framework.md for the registration policy.
 *
 * Stage 0.11 registers identifiers only; pgstat_report_wait_start() call
 * sites land in the spec for each owning subsystem.
 * ----------
 */
typedef enum {
	/* Cluster: GES (5+3 events) -- subsystem #8 */
	WAIT_EVENT_GES_ENQUEUE_ACQUIRE = PG_WAIT_CLUSTER_GES,
	WAIT_EVENT_GES_ENQUEUE_CONVERT,
	WAIT_EVENT_GES_ENQUEUE_RELEASE_ACK,
	WAIT_EVENT_GES_MASTER_QUERY,
	WAIT_EVENT_GES_LOCAL_FAST_PATH,
	/* spec-2.16 D13 (3 NEW per v0.4 Q13 + L2.1):  cross-node grant wait
	 * surfaces.  GRANT_WAIT — backend wait latch for GES reply (S4 wait);
	 * CONVERT_WAIT — caller wait for convert ack;  DRAIN — LMON drain
	 * dirty-list / work queue. */
	WAIT_EVENT_GES_GRANT_WAIT,
	WAIT_EVENT_GES_CONVERT_WAIT,
	WAIT_EVENT_GES_DRAIN,
	/* spec-2.17 D28 NEW 4 wait events. */
	WAIT_EVENT_GES_BAST_WAIT,
	WAIT_EVENT_GES_DEADLOCK_PROBE_WAIT,
	WAIT_EVENT_GES_CANCEL_DRAIN,
	WAIT_EVENT_GES_DEADLOCK_REASSEMBLY_WAIT,

	/* Cluster: PCM (9 events; +2 spec-2.30 D8 + 1 spec-2.31 D6) -- subsystem #6 */
	WAIT_EVENT_PCM_BLOCK_READ_N_S = PG_WAIT_CLUSTER_PCM,
	WAIT_EVENT_PCM_BLOCK_READ_N_X,
	WAIT_EVENT_PCM_BLOCK_WRITE_S_X,
	WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT,
	WAIT_EVENT_PCM_BLOCK_DOWNGRADE,
	WAIT_EVENT_PCM_ITL_CLEANOUT,
	/* PGRAC (spec-2.30 D8): GrdEntry HTAB init at postmaster startup. */
	WAIT_EVENT_PCM_GRD_INIT,
	/* PGRAC (spec-2.30 D8): per-entry entry_lock acquire hot path. */
	WAIT_EVENT_PCM_TRANSITION_APPLY,
	/* PGRAC (spec-2.31 D6 F3 v0.4): wait_cv ConditionVariableSleep when
	 * cluster_pcm_lock_acquire sees incompatible holder state.  Bufmgr
	 * content-lock hook contention path; DBA sees backend waiting on
	 * 'ClusterPcmCompatibleStateWait' in pg_stat_activity. */
	WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT,

	/* Cluster: BufferShip (5 events) -- subsystem #5 */
	WAIT_EVENT_BUFFER_SHIP_CR_BUILD = PG_WAIT_CLUSTER_BUFFERSHIP,
	WAIT_EVENT_BUFFER_SHIP_CR_SEND,
	WAIT_EVENT_BUFFER_SHIP_CR_RECEIVE,
	WAIT_EVENT_BUFFER_SHIP_CURRENT_SEND,
	WAIT_EVENT_BUFFER_SHIP_CURRENT_RECEIVE,

	/* Cluster: SCN (4 events) -- subsystem #7 */
	WAIT_EVENT_SCN_BOC_FLUSH_WAIT = PG_WAIT_CLUSTER_SCN,
	WAIT_EVENT_SCN_PIGGYBACK_MERGE,
	WAIT_EVENT_SCN_CROSS_NODE_COMPARE,
	WAIT_EVENT_SCN_ADVANCE_BROADCAST,

	/* Cluster: Reconfig (5 events) -- #14 / #20 */
	WAIT_EVENT_RECONFIG_GRD_REBUILD = PG_WAIT_CLUSTER_RECONFIG,
	WAIT_EVENT_RECONFIG_LOCK_RECOVERY,
	WAIT_EVENT_RECONFIG_FENCE_WAIT,
	WAIT_EVENT_RECONFIG_MASTER_SELECTION,
	WAIT_EVENT_RECONFIG_BARRIER_WAIT,

	/* Cluster: Recovery (5 events) -- #86 */
	WAIT_EVENT_RECOVERY_WAL_FETCH = PG_WAIT_CLUSTER_RECOVERY,
	WAIT_EVENT_RECOVERY_KWAY_MERGE,
	WAIT_EVENT_RECOVERY_APPLY_PER_THREAD,
	WAIT_EVENT_RECOVERY_UNDO_REPLAY,
	WAIT_EVENT_RECOVERY_PCM_STATE_RESTORE,

	/* Cluster: Sinval (3 events) -- subsystem #9 */
	WAIT_EVENT_SINVAL_BROADCAST_SEND = PG_WAIT_CLUSTER_SINVAL,
	WAIT_EVENT_SINVAL_BROADCAST_RECEIVE,
	WAIT_EVENT_SINVAL_INJECT_LOCAL_QUEUE,

	/* Cluster: Interconnect (5 events + 6 spec-2.2 D8) -- AD-007 */
	WAIT_EVENT_INTERCONNECT_RDMA_SEND = PG_WAIT_CLUSTER_INTERCONNECT,
	WAIT_EVENT_INTERCONNECT_RDMA_RECV,
	WAIT_EVENT_INTERCONNECT_TCP_FALLBACK,
	WAIT_EVENT_INTERCONNECT_TIER_SWITCH,
	WAIT_EVENT_INTERCONNECT_CONNECT_RETRY,
	/*
	 * spec-2.2 D8 (2026-05-07) -- Tier 1 TCP transport wait events.
	 * Per 约束 2 strict semantics:
	 *   ClusterICTcpAccept    : listener fd waiting for incoming connection
	 *   ClusterICTcpConnect   : active edge nonblocking connect waiting for
	 *                           SO_ERROR check via WL_SOCKET_WRITEABLE
	 *   ClusterICTcpRecv      : per-peer socket waiting readable -- the
	 *                           ONLY socket-recv wait event (HeartbeatWait
	 *                           is timer-based, NOT recv)
	 *   ClusterICTcpSend      : per-peer socket waiting writeable on
	 *                           short / nonblocking write
	 *   ClusterICHeartbeatWait: timer-based wait until next heartbeat tick
	 *                           (NOT a socket recv -- distinct from TcpRecv)
	 *   ClusterICReconnect    : reconnect backoff sleep after
	 *                           connect failure / connection lost
	 *                           (NOT a connect-in-progress wait)
	 *
	 * WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP (spec-1.11) is RESERVED
	 * for the LMON IDLE tick path -- per 约束 1 it MUST NOT be reused
	 * for any of the 6 IC socket waits.
	 */
	WAIT_EVENT_CLUSTER_IC_TCP_ACCEPT,
	WAIT_EVENT_CLUSTER_IC_TCP_CONNECT,
	WAIT_EVENT_CLUSTER_IC_TCP_RECV,
	WAIT_EVENT_CLUSTER_IC_TCP_SEND,
	WAIT_EVENT_CLUSTER_IC_HEARTBEAT_WAIT,
	WAIT_EVENT_CLUSTER_IC_RECONNECT,

	/* Cluster: Undo (4 events) -- AD-010 */
	WAIT_EVENT_UNDO_REMOTE_READ = PG_WAIT_CLUSTER_UNDO,
	WAIT_EVENT_UNDO_TT_LOOKUP_REMOTE,
	WAIT_EVENT_UNDO_SEGMENT_FETCH,
	WAIT_EVENT_UNDO_RETENTION_WAIT,

	/* Cluster: ADG (4 events) -- #95 */
	WAIT_EVENT_ADG_MRP_APPLY_WAIT = PG_WAIT_CLUSTER_ADG,
	WAIT_EVENT_ADG_WAL_RECEIVE_LAG,
	WAIT_EVENT_ADG_READ_SNAPSHOT_WAIT,
	WAIT_EVENT_ADG_SCN_SYNC_WAIT,

	/* Cluster: SharedFs (5 events) -- spec-1.1 */
	WAIT_EVENT_CLUSTER_SHARED_FS_READ = PG_WAIT_CLUSTER_SHAREDFS,
	WAIT_EVENT_CLUSTER_SHARED_FS_WRITE,
	WAIT_EVENT_CLUSTER_SHARED_FS_EXTEND,
	WAIT_EVENT_CLUSTER_SHARED_FS_TRUNCATE,
	WAIT_EVENT_CLUSTER_SHARED_FS_FSYNC,

	/* Cluster: StartupPhase (5 events) -- spec-1.10 (2026-05-03) */
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_0 = PG_WAIT_CLUSTER_STARTUP_PHASE,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_1,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_2,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_3,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_4,

	/*
	 * Cluster: BgProc (1 event so far) -- spec-1.11 Sprint B (2026-05-04).
	 * Class scoped to cluster background-process main-loop / lifecycle /
	 * liveness waits (LMON, future LCK / DIAG / Cluster Stats).  Real
	 * reconfig / fence / heartbeat / interconnect / GES wait events go
	 * to their dedicated business class, NOT BgProc.
	 */
	WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP = PG_WAIT_CLUSTER_BGPROC,
	WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP,		   /* spec-1.12 Sprint B D11 */
	WAIT_EVENT_CLUSTER_BGPROC_DIAG_MAIN_LOOP,		   /* spec-1.13 D11 */
	WAIT_EVENT_CLUSTER_BGPROC_CLUSTER_STATS_MAIN_LOOP, /* spec-1.14 D11 */
	WAIT_EVENT_CLUSTER_BGPROC_CSSD_MAIN_LOOP,		   /* spec-2.5 D8 */
	WAIT_EVENT_CLUSTER_BGPROC_QVOTEC_MAIN_LOOP,		   /* spec-2.6 D11 */
	WAIT_EVENT_CLUSTER_VOTING_DISK_READ,			   /* spec-2.6 D11 */
	WAIT_EVENT_CLUSTER_VOTING_DISK_WRITE,			   /* spec-2.6 D11 */
	/* spec-2.28 Sprint A Step 4 D9:  fence-lite backend interrupt check
	 * wait event.  Backend ProcessInterrupts hook (postgres.c D4) sets
	 * this wait event briefly while reading + clearing ClusterFenceFreeze
	 * Pending + ereport(ERROR, 53R50).  Sub-microsecond duration in the
	 * fast path (flag==0 early return);longer when ereport unwinds the
	 * transaction.  Lets pg_stat_activity expose freeze-induced abort
	 * source distinct from generic ClientRead / WAL classes. */
	WAIT_EVENT_CLUSTER_FENCE_BACKEND_INTERRUPT_CHECK,
	/* spec-2.29 Sprint A Step 3 D9:  reconfig coordinator LMON tick.
	 * Wraps cluster_reconfig_lmon_tick body which runs every LMON main
	 * loop iteration (~100ms).  Lets pg_stat_activity attribute LMON
	 * tick CPU to its dedicated wait class — useful for diagnosing
	 * "is LMON spinning on reconfig dedup checks". */
	WAIT_EVENT_CLUSTER_BGPROC_LMON_RECONFIG_TICK,
	/* spec-2.19 Sprint A Step 4 D12: LMD lifecycle / idle wait events. */
	WAIT_EVENT_CLUSTER_LMD_STARTUP,
	WAIT_EVENT_CLUSTER_LMD_SCAN,
	WAIT_EVENT_CLUSTER_LMD_IDLE,
	/*
	 * spec-2.20 D12 (v0.3 frozen): 7-step caller-side S4 cross-node wait.
	 * Backend waits for remote master grant decision after S3 reservation +
	 * GES_REQUEST send;wake on GES_REPLY arrival or timeout (53R70).
	 * Local-master fast path (A1) does NOT enter S4_WAIT (no remote IPC).
	 */
	WAIT_EVENT_CLUSTER_GES_S4_WAIT,
	/*
	 * spec-2.22 D10:LMD coordinator handler processing DEADLOCK_PROBE
	 * + snapshot_copy + REPORT encode.  Production cross-node broadcast
	 * 推 spec-2.23 BAST 配套;本 spec scope 仅 handler dispatch + payload.
	 */
	WAIT_EVENT_CLUSTER_LMD_PROBE,
	/*
	 * spec-2.23 D12:cross-node GES reply 5-tuple wait (HC17).  Backend
	 * sleeps on the per-entry ConditionVariable inserted into the
	 * cluster_ges_reply_wait HTAB before sending a GES_REQUEST/RELEASE
	 * to a remote master.  Wake on GES_REPLY arrival or timeout (53R70).
	 */
	WAIT_EVENT_CLUSTER_GES_REPLY_WAIT,
	/*
	 * spec-2.23 D12:LMD coordinator REPORT collect.  Coordinator scan
	 * tick polls WaitLatch up to cluster.lmd_probe_collect_timeout_ms
	 * for N-1 DEADLOCK_REPORTs after broadcasting PROBE.  Distinct from
	 * WAIT_EVENT_CLUSTER_LMD_PROBE which covers the per-receiver
	 * handler processing path.
	 */
	WAIT_EVENT_CLUSTER_LMD_PROBE_COLLECT,
	/*
	 * spec-2.25 D11:  LMS native-lock probe collector aggregate wait.
	 *	LMS process sleeps on cluster_lms_state->cv while in-flight
	 *	probe slots await N-1 peer replies (HC29 fan-out).  Wake on
	 *	reply arrival (recv_reply broadcast) or retry tick.
	 */
	WAIT_EVENT_CLUSTER_LMS_NATIVE_PROBE_WAIT,
	/*
	 * spec-2.25 D11:  peer node scanning local PG lock state during a
	 *	native-lock probe request handler invocation (HC30 scan of
	 *	LockMethodLockHash + LOCK->waitProcs + relation fast-path).
	 *	Distinct from LMS_NATIVE_PROBE_WAIT which covers the LMS
	 *	collector side.
	 */
	WAIT_EVENT_CLUSTER_NATIVE_PROBE_REPLY_WAIT,
} WaitEventCluster;


extern const char *pgstat_get_wait_event(uint32 wait_event_info);
extern const char *pgstat_get_wait_event_type(uint32 wait_event_info);
static inline void pgstat_report_wait_start(uint32 wait_event_info);
static inline void pgstat_report_wait_end(void);
extern void pgstat_set_wait_event_storage(uint32 *wait_event_info);
extern void pgstat_reset_wait_event_storage(void);

extern PGDLLIMPORT uint32 *my_wait_event_info;


/* ----------
 * pgstat_report_wait_start() -
 *
 *	Called from places where server process needs to wait.  This is called
 *	to report wait event information.  The wait information is stored
 *	as 4-bytes where first byte represents the wait event class (type of
 *	wait, for different types of wait, refer WaitClass) and the next
 *	3-bytes represent the actual wait event.  Currently 2-bytes are used
 *	for wait event which is sufficient for current usage, 1-byte is
 *	reserved for future usage.
 *
 *	Historically we used to make this reporting conditional on
 *	pgstat_track_activities, but the check for that seems to add more cost
 *	than it saves.
 *
 *	my_wait_event_info initially points to local memory, making it safe to
 *	call this before MyProc has been initialized.
 * ----------
 */
static inline void
pgstat_report_wait_start(uint32 wait_event_info)
{
	/*
	 * Since this is a four-byte field which is always read and written as
	 * four-bytes, updates are atomic.
	 */
	*(volatile uint32 *)my_wait_event_info = wait_event_info;
}

/* ----------
 * pgstat_report_wait_end() -
 *
 *	Called to report end of a wait.
 * ----------
 */
static inline void
pgstat_report_wait_end(void)
{
	/* see pgstat_report_wait_start() */
	*(volatile uint32 *)my_wait_event_info = 0;
}


#endif /* WAIT_EVENT_H */
