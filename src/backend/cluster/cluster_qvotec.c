/*-------------------------------------------------------------------------
 *
 * cluster_qvotec.c
 *	  pgrac QVOTEC (Quorum Voting Coordinator) — spec-2.6 Sprint A Step 1.
 *
 *	  6th cluster aux process (LMON / LCK / DIAG / Stats / CSSD / QVOTEC).
 *	  Polls voting disks on shared storage (Step 2 D3 module), decides
 *	  cluster-wide quorum (Step 2 D4 module), publishes ClusterQvotec
 *	  Shmem.quorum_state + Q4 v0.2 lease so xact.c CommitTransaction
 *	  can fail-closed on every backend.  spec-2.28 Fence-lite consumes
 *	  QVOTEC quorum_state from LMON and broadcasts ProcSignal freeze/thaw
 *	  for early-abort of in-flight long-running queries; the QVOTEC lease +
 *	  commit gate remain the authoritative durable-write predicate.
 *
 *	  Step 1 scope (this commit):
 *	    - ClusterQvotecShmem private 128-byte region (Q4 v0.2 lease-based)
 *	    - Lifecycle CAS state machine (STARTING → READY → SHUTTING_DOWN
 *	      → DOWN), mirrors spec-2.5 CSSD pattern
 *	    - 7 lifecycle / dump-key accessors (per F11)
 *	    - cluster_qvotec_in_quorum() lease-aware backend hot-path helper
 *	    - ProcSignal flag helpers (process-local atomic;Q5 v0.2 check
 *	      timing wired in Step 3 D6 postgres.c)
 *	    - ClusterQvotecMain skeleton: WaitLatch loop, lifecycle
 *	      transitions, lease writeback every poll_interval, poll-cycle
 *	      counter increment.  Real disk I/O / quorum decision delegated
 *	      to Step 2 stubs (return-success-no-op until D3+D4 land).
 *	    - shmem_size / shmem_init / shmem_register (registered from
 *	      cluster_shmem.c in Step 4 D9)
 *
 *	  Step 1 explicitly DEFERS:
 *	    - Real voting-disk I/O (cluster_voting_disk_io.c — Step 2 D3)
 *	    - Real majority math + collision detection (cluster_quorum_
 *	      decision.c — Step 2 D4)
 *	    - PROCSIG_CLUSTER_FREEZE_WRITES / _THAW_ multiplexer hook
 *	      (procsignal.c — Step 3 D5)
 *	    - Backend write-intent + commit-boundary check (postgres.c —
 *	      Step 3 D6)
 *	    - postmaster reaper / phase 4 driver wiring (Step 3 D7+D8)
 *	    - 4 GUCs / 4 SQLSTATE / 3 wait events / 5 inject (Step 4)
 *	    - SRF / view (Step 5 D15)
 *
 *	  Until those land, ClusterQvotecMain stays in a degenerate
 *	  WaitLatch loop that bumps poll_cycle_count + lease_expire_at_us
 *	  every iteration but does no real disk I/O or quorum decision.
 *	  The lease still works correctly: the helper reads
 *	  quorum_state == INITIALIZING (the default) → backends fail-
 *	  closed even before Step 2/3 land, so the safety contract holds.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_qvotec.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds;
 *	  see src/backend/cluster/Makefile for OBJS rules (Step 4).
 *
 *	  Spec authority: pgrac:specs/spec-2.6-voting-disk-quorum-lite.md
 *	  (frozen v0.2 2026-05-09 Q1-Q10 user approve).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_qvotec.h"

#ifdef USE_PGRAC_CLUSTER

#include <errno.h>
#include <string.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h" /* init_ps_display */
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "cluster/cluster_elog.h"		 /* CLUSTER_LOG (best-effort logging) */
#include "cluster/cluster_guc.h"		 /* cluster_enabled */
#include "cluster/cluster_pgstat.h"		 /* cluster.qvotec.* counters */
#include "cluster/cluster_shmem.h"		 /* cluster_shmem_register_region */
#include "cluster/cluster_write_fence.h" /* spec-4.12 D2: fence marker scan + token refresh */
#include "utils/memutils.h"				 /* TopMemoryContext */


/* ============================================================
 * ClusterQvotecShmem — private 128-byte (2 cache-line) region.
 *
 *	v0.2 amend per Q4 修订: lease-based quorum_state semantics.  The
 *	backend helper cluster_qvotec_in_quorum() validates BOTH the
 *	state == OK condition AND now < lease_expire_at_us, so a hung
 *	qvotec (disk I/O stuck > 2 × poll_interval) auto-fails-closed
 *	without depending on ProcSignal arrival timing.
 *
 *	Layout (offset / size / field):
 *	   0..3   uint32 state              (ClusterQvotecStatus enum)
 *	   4..7   uint32 quorum_state       (ClusterQvotecQuorumState enum)
 *	   8..11  uint32 disks_ok_count
 *	  12..15  uint32 disks_total_count
 *	  16..23  uint64 current_epoch_at_boot
 *	  24..31  uint64 last_poll_ts_us       (NEW Q4 v0.2)
 *	  32..39  uint64 lease_expire_at_us    (NEW Q4 v0.2)
 *	  40..47  uint64 last_quorum_loss_ts_us
 *	  48..51  uint32 collision_state     (ClusterCollisionDetectionState)
 *	  52..55  uint32 poll_cycle_count
 *	  56..59  uint32 torn_write_detect_count
 *	  60..63  uint32 _pad
 *	  64..127 uint8[64] _reserved          (future expansion)
 * ============================================================ */
typedef struct ClusterQvotecShmem {
	pg_atomic_uint32 state;		   /* ClusterQvotecStatus */
	pg_atomic_uint32 quorum_state; /* ClusterQvotecQuorumState */
	pg_atomic_uint32 disks_ok_count;
	pg_atomic_uint32 disks_total_count;
	pg_atomic_uint64 current_epoch_at_boot;
	pg_atomic_uint64 last_poll_ts_us;
	pg_atomic_uint64 lease_expire_at_us;
	pg_atomic_uint64 last_quorum_loss_ts_us;
	pg_atomic_uint32 collision_state; /* ClusterCollisionDetectionState */
	pg_atomic_uint32 poll_cycle_count;
	pg_atomic_uint32 torn_write_detect_count;
	pg_atomic_uint32 _pad;
	uint8 _reserved[64];
} ClusterQvotecShmem;

StaticAssertDecl(sizeof(ClusterQvotecShmem) == 128,
				 "ClusterQvotecShmem must be exactly 128 bytes (2 cache lines)");


static ClusterQvotecShmem *QvotecShmem = NULL;

/*
 * QvotecPid — process-local mirror of the postmaster-side QvotecPID.
 * Set by ClusterQvotecMain at entry (= MyProcPid);read by
 * cluster_qvotec_get_pid().  Step 3 D7 will additionally surface
 * QvotecPID at the postmaster level via reaper hooks.
 */
static int QvotecPid = 0;

/*
 * Process-local frozen flag — set/cleared by signal handlers (Step 3
 * D5 procsignal.c) on PROCSIG_CLUSTER_FREEZE_WRITES / _THAW_.  Async-
 * signal-safe set requires only an atomic 4-byte write to
 * cluster_writes_frozen below.
 *
 * Backend helpers cluster_writes_currently_frozen() / cluster_qvotec_
 * in_quorum() read this flag in addition to the lease check;both
 * conditions must agree (backend treats EITHER frozen-by-signal OR
 * lease-expired as fail-closed).
 */
static volatile sig_atomic_t cluster_writes_frozen = 0;


/* ============================================================
 * Voting disk fd table (P1.3 step 1).
 *
 *	Per cycle qvotec needs to read all configured voting disks +
 *	write its own slot.  Open the fds once at READY publish, close
 *	at shutdown.  Empty cluster.voting_disks ⇒ qvotec stays alive
 *	but does no I/O (single-node compat — backend fail-closed gate
 *	is also skipped per P1.2 xact.c logic).
 *
 *	CLUSTER_MAX_VOTING_DISKS lives in cluster_qvotec.h (was a
 *	divergent local define = 9 in v0.14.0–v0.14.1; Hardening v0.6
 *	F5 unified to header value = 7 matching documented 1/3/5/7
 *	odd-majority recommendation).
 * ============================================================ */

static int qvotec_fds[CLUSTER_MAX_VOTING_DISKS];
static int qvotec_n_disks = 0;

/*
 * qvotec_self_incarnation — set once at qvotec startup
 * (GetCurrentTimestamp at process start) so different qvotec runs are
 * distinguishable on disk.  Used for Q6 v0.2 collision detection.
 *
 * qvotec_slot_generation — monotonic per-write counter (Q2 v0.2 torn-
 * write detection).  Caller bumps this before every write_slot.
 *
 * qvotec_slot_matrix — palloc'd at startup in TopMemoryContext, sized
 * CLUSTER_MAX_VOTING_DISKS × CLUSTER_MAX_NODES, reused every poll
 * cycle.  Large (~580KB) so heap-allocated rather than stack.
 */
static uint64 qvotec_self_incarnation = 0;
static uint64 qvotec_slot_generation = 0;
static ClusterVotingSlot *qvotec_slot_matrix = NULL;

/*
 * D10 pgstat counter handles, looked up once at startup.  The poll
 * cycle bumps the global cluster.qvotec.* counters surfaced through
 * pg_stat_cluster_counters.
 */
static ClusterPgstatCounter *qvotec_counter_poll_cycle = NULL;
static ClusterPgstatCounter *qvotec_counter_quorum_loss = NULL;
static ClusterPgstatCounter *qvotec_counter_collision = NULL;
static ClusterPgstatCounter *qvotec_counter_disk_io_fail = NULL;

static void
qvotec_pgstat_lookup_all(void)
{
	qvotec_counter_poll_cycle = cluster_pgstat_lookup("cluster.qvotec.poll_cycle_count");
	qvotec_counter_quorum_loss = cluster_pgstat_lookup("cluster.qvotec.quorum_loss_event_count");
	qvotec_counter_collision = cluster_pgstat_lookup("cluster.qvotec.collision_detect_event_count");
	qvotec_counter_disk_io_fail = cluster_pgstat_lookup("cluster.qvotec.disk_io_failure_count");
}


/* ============================================================
 * Default poll interval — read from cluster.quorum_poll_interval_ms
 * GUC once that lands in Step 4 D12.  Until then, hard-code default.
 * ============================================================ */
#define CLUSTER_QVOTEC_DEFAULT_POLL_INTERVAL_MS 2000


/* ============================================================
 * Shmem region — size / init / register.
 *
 *	Mirrors cluster_epoch / cluster_diag / cluster_cssd patterns;
 *	registered from cluster_shmem.c in Step 4 D9 with name
 *	"pgrac cluster qvotec".
 * ============================================================ */

Size
cluster_qvotec_shmem_size(void)
{
	return sizeof(ClusterQvotecShmem);
}

void
cluster_qvotec_shmem_init(void)
{
	bool found;

	QvotecShmem = (ClusterQvotecShmem *)ShmemInitStruct("pgrac cluster qvotec",
														cluster_qvotec_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u32(&QvotecShmem->state, CLUSTER_QVOTEC_STARTING);
		pg_atomic_init_u32(&QvotecShmem->quorum_state, CLUSTER_QVOTEC_QUORUM_INITIALIZING);
		pg_atomic_init_u32(&QvotecShmem->disks_ok_count, 0);
		pg_atomic_init_u32(&QvotecShmem->disks_total_count, 0);
		pg_atomic_init_u64(&QvotecShmem->current_epoch_at_boot, 0);
		pg_atomic_init_u64(&QvotecShmem->last_poll_ts_us, 0);
		pg_atomic_init_u64(&QvotecShmem->lease_expire_at_us, 0);
		pg_atomic_init_u64(&QvotecShmem->last_quorum_loss_ts_us, 0);
		pg_atomic_init_u32(&QvotecShmem->collision_state, CLUSTER_COLLISION_NONE);
		pg_atomic_init_u32(&QvotecShmem->poll_cycle_count, 0);
		pg_atomic_init_u32(&QvotecShmem->torn_write_detect_count, 0);
		pg_atomic_init_u32(&QvotecShmem->_pad, 0);
		memset(QvotecShmem->_reserved, 0, sizeof(QvotecShmem->_reserved));
	}
}

static const ClusterShmemRegion cluster_qvotec_region = {
	.name = "pgrac cluster qvotec",
	.size_fn = cluster_qvotec_shmem_size,
	.init_fn = cluster_qvotec_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_qvotec",
	.reserved_flags = 0,
};

void
cluster_qvotec_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_qvotec_region);
}


/* ============================================================
 * 7 lifecycle / dump-key accessors (per F11 mandatory 7-key dump).
 *
 *	All NULL-safe: return defaults (0 / "unknown") when QvotecShmem
 *	is NULL (cluster_unit harness, --disable-cluster build entry).
 * ============================================================ */

int
cluster_qvotec_get_pid(void)
{
	return QvotecPid;
}

const char *
cluster_qvotec_get_status_name(void)
{
	uint32 s;

	if (QvotecShmem == NULL)
		return "(uninitialised)";

	s = pg_atomic_read_u32(&QvotecShmem->state);
	switch (s) {
	case CLUSTER_QVOTEC_STARTING:
		return "starting";
	case CLUSTER_QVOTEC_READY:
		return "ready";
	case CLUSTER_QVOTEC_SHUTTING_DOWN:
		return "shutting_down";
	case CLUSTER_QVOTEC_DOWN:
		return "down";
	case CLUSTER_QVOTEC_FAILED:
		return "failed";
	default:
		return "unknown";
	}
}

const char *
cluster_qvotec_get_quorum_state_name(void)
{
	uint32 q;

	if (QvotecShmem == NULL)
		return "(uninitialised)";

	q = pg_atomic_read_u32(&QvotecShmem->quorum_state);
	switch (q) {
	case CLUSTER_QVOTEC_QUORUM_INITIALIZING:
		return "initializing";
	case CLUSTER_QVOTEC_QUORUM_OK:
		return "ok";
	case CLUSTER_QVOTEC_QUORUM_UNCERTAIN:
		return "uncertain";
	case CLUSTER_QVOTEC_QUORUM_LOST:
		return "lost";
	default:
		return "unknown";
	}
}

int
cluster_qvotec_get_quorum_state(void)
{
	if (QvotecShmem == NULL)
		return (int)CLUSTER_QVOTEC_QUORUM_INITIALIZING;
	return (int)pg_atomic_read_u32(&QvotecShmem->quorum_state);
}

int
cluster_qvotec_get_disks_ok_count(void)
{
	if (QvotecShmem == NULL)
		return 0;
	return (int)pg_atomic_read_u32(&QvotecShmem->disks_ok_count);
}

int
cluster_qvotec_get_disks_total_count(void)
{
	if (QvotecShmem == NULL)
		return 0;
	return (int)pg_atomic_read_u32(&QvotecShmem->disks_total_count);
}

uint64
cluster_qvotec_get_current_epoch_at_boot(void)
{
	if (QvotecShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&QvotecShmem->current_epoch_at_boot);
}

const char *
cluster_qvotec_get_collision_state_name(void)
{
	uint32 c;

	if (QvotecShmem == NULL)
		return "(uninitialised)";

	c = pg_atomic_read_u32(&QvotecShmem->collision_state);
	switch (c) {
	case CLUSTER_COLLISION_NONE:
		return "none";
	case CLUSTER_COLLISION_OBSERVED_OLDER:
		return "observed_older_slot";
	case CLUSTER_COLLISION_FATAL_NEWER_SELF:
		return "fatal_newer_self";
	default:
		return "unknown";
	}
}


/* ============================================================
 * cluster_qvotec_in_quorum — backend hot-path helper (Q4 v0.2).
 *
 *	True ONLY when:
 *	  (a) shmem live
 *	  (b) quorum_state == OK
 *	  (c) now < lease_expire_at_us  (qvotec polled within
 *	      2 × poll_interval — defends against qvotec hung)
 *
 *	Any other state — INITIALIZING / UNCERTAIN / LOST / lease
 *	expired / shmem absent — returns false → backend fail-closed.
 *
 *	Cost: 3 atomic loads + 1 GetCurrentTimestamp() call (~50ns).
 *	v0.14.0 caller:  CommitTransaction (commit-boundary check; xact.c
 *	D6).  Spec Q5 v0.2 write-intent boundary check at INSERT/UPDATE/
 *	DELETE/DDL entry is deferred to Hardening v0.4+;correctness is
 *	preserved by Q4 lease + commit gate (any write must commit
 *	through the gate, so a lost-quorum decision is enforced before
 *	durability).
 * ============================================================ */
bool
cluster_qvotec_in_quorum(void)
{
	uint64 now_us;
	uint64 lease_expire;
	uint32 q;

	/* Disable-cluster / pre-shmem path: fail-closed. */
	if (QvotecShmem == NULL)
		return false;

	/* Process-local frozen flag set by ProcSignal handler — wins
	 * regardless of lease state (defensive double-gate). */
	if (cluster_writes_frozen)
		return false;

	q = pg_atomic_read_u32(&QvotecShmem->quorum_state);
	if (q != CLUSTER_QVOTEC_QUORUM_OK)
		return false;

	lease_expire = pg_atomic_read_u64(&QvotecShmem->lease_expire_at_us);
	now_us = (uint64)GetCurrentTimestamp();
	if (now_us >= lease_expire)
		return false;

	return true;
}


/* ============================================================
 * ProcSignal flag helpers — set/clear from signal handler
 * (Step 3 D5 procsignal.c) and read from backend hot path.
 *
 *	Async-signal-safe: cluster_writes_frozen is sig_atomic_t,
 *	updates are single-byte / 4-byte writes that POSIX guarantees
 *	atomic.  No palloc / no ereport in handler context (per
 *	CLAUDE.md rule 16).
 * ============================================================ */

void
cluster_freeze_writes_set(void)
{
	cluster_writes_frozen = 1;
}

void
cluster_thaw_writes_set(void)
{
	cluster_writes_frozen = 0;
}

bool
cluster_writes_currently_frozen(void)
{
	return cluster_writes_frozen != 0;
}


/* ============================================================
 * Voting disk fd lifecycle helpers (P1.3 step 1).
 *
 *	qvotec_open_disks parses cluster.voting_disks CSV and opens each
 *	configured path R/W.  qvotec_close_disks closes every open fd.
 *
 *	Failure policy:
 *	  - empty CSV: qvotec_n_disks = 0;ClusterQvotecMain proceeds to
 *	    READY but skips real poll (single-node compat per Q7 v0.2).
 *	    Backend fail-closed is also disabled in xact.c when
 *	    cluster_voting_disks empty (P1.2).
 *	  - any path > CLUSTER_MAX_VOTING_DISKS or open(2) failure:
 *	    qvotec_close_disks then ereport(FATAL).  Phase 4 driver gets
 *	    QVOTEC_NOT_READY and refuses to advance to running.
 * ============================================================ */
#include "cluster/cluster_voting_disk_io.h" /* fd open/close + format */

static void
qvotec_close_disks(void)
{
	int i;

	for (i = 0; i < qvotec_n_disks; i++) {
		cluster_voting_disk_close(qvotec_fds[i]);
		qvotec_fds[i] = -1;
	}
	qvotec_n_disks = 0;
}

/* on_shmem_exit signature wrapper (code + arg unused). */
static void
qvotec_close_disks_atexit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	qvotec_close_disks();
}

/*
 * Hardening v0.6 F2 (companion to startup ghost-detect):
 * Clean-shutdown self-slot ALIVE-flag clear.  Writes one final slot to
 * every disk with flags = 0 (no ALIVE) before close.  Best-effort —
 * write failures are swallowed (we are exiting anyway and the startup
 * ghost-detect path will handle next-restart races).  This is NOT
 * called from the on_shmem_exit crash path (proc_exit on FATAL):
 * crash means we cannot trust postmaster_data_dir / fds; the startup
 * ghost-detect path is the fallback for crash-restart races.
 */
static void
qvotec_clear_self_alive_on_clean_shutdown(void)
{
	ClusterVotingSlot blanked;
	int i;

	if (qvotec_n_disks == 0 || cluster_node_id < 0 || (uint32)cluster_node_id >= CLUSTER_MAX_NODES)
		return;

	memset(&blanked, 0, sizeof(blanked));
	blanked.magic = CLUSTER_VOTING_SLOT_MAGIC;
	blanked.version = CLUSTER_VOTING_SLOT_VERSION;
	blanked.node_id = (uint32)cluster_node_id;
	blanked.incarnation = qvotec_self_incarnation;
	blanked.heartbeat_ts_us = (uint64)GetCurrentTimestamp();
	blanked.current_epoch = pg_atomic_read_u64(&QvotecShmem->current_epoch_at_boot);
	blanked.flags = 0; /* ALIVE bit cleared — that's the whole point */

	for (i = 0; i < qvotec_n_disks; i++) {
		qvotec_slot_generation++;
		blanked.generation = qvotec_slot_generation;
		blanked.disk_index = (uint32)i;
		(void)cluster_voting_disk_write_slot(qvotec_fds[i], &blanked);
	}
}


/* ============================================================
 * qvotec_poll_once — single poll cycle (P1.3 step 2).
 *
 *	One pass:
 *	  1. Build self slot (node_id, incarnation, heartbeat, epoch, alive)
 *	  2. For each open disk: bump generation, write self slot
 *	  3. For each (disk × node): read slot into qvotec_slot_matrix
 *	  4. Call decide_quorum_view → ClusterQuorumDecision
 *	  5. Publish ClusterQvotecShmem (quorum_state / disks_ok /
 *	     collision_state / last_quorum_loss_ts_us / lease)
 *
 *	No-op if qvotec_n_disks == 0 (single-node compat — backend fail-
 *	closed gate already disabled in xact.c when voting_disks empty).
 *	Caller (main loop) bumps poll_cycle_count regardless of the no-op
 *	path so observability works even in single-node mode.
 * ============================================================ */
#include "cluster/cluster_quorum_decision.h"

/*
 * qvotec_best_marker_on_disk (spec-4.12 D2) -- scan every node's slot on one disk
 *	for a durable fence marker (in _reserved1, already CRC-validated by read_slot)
 *	and return the highest-epoch one (fence_generation tie-break).  Returns false
 *	when the disk carries no marker.  The poll feeds these per-disk best markers to
 *	cluster_fence_authority_decide, which requires quorum-majority agreement (P0a).
 */
static bool
qvotec_best_marker_on_disk(int disk, ClusterFenceMarker *out)
{
	bool found = false;
	uint32 node;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		ClusterVotingSlot *cell = &qvotec_slot_matrix[disk * CLUSTER_MAX_NODES + node];
		ClusterFenceMarker m;

		if (!cluster_fence_marker_unpack(cell->_reserved1, &m))
			continue;
		if (!found || m.fence_epoch > out->fence_epoch
			|| (m.fence_epoch == out->fence_epoch && m.fence_generation > out->fence_generation)) {
			*out = m;
			found = true;
		}
	}
	return found;
}

static void
qvotec_poll_once(void)
{
	ClusterVotingSlot self_slot;
	ClusterVotingDiskIoState io_states[CLUSTER_MAX_VOTING_DISKS];
	ClusterQuorumDecision decision;
	uint64 now_us;
	uint64 next_lease_expire;
	uint64 heartbeat_timeout_us;
	int i;
	ClusterFenceMarker submit_marker;
	bool have_submit;

	now_us = (uint64)GetCurrentTimestamp();
	next_lease_expire = now_us + (uint64)cluster_quorum_poll_interval_ms * 2 * 1000ULL;
	heartbeat_timeout_us = (uint64)cluster_quorum_poll_interval_ms * 2 * 1000ULL;

	/* Always update the lease + last_poll_ts so the backend helper
	 * sees recent liveness even on the single-node short-circuit. */
	pg_atomic_write_u64(&QvotecShmem->last_poll_ts_us, now_us);
	pg_atomic_write_u64(&QvotecShmem->lease_expire_at_us, next_lease_expire);

	/*
	 * spec-4.12 D4: pick up a pending fence-marker submit from LMON (latch-woke
	 * us).  We embed it in THIS poll's self-slot write to every disk and ack
	 * majority-durability after the write.  A failure on every early-return path
	 * below still completes the in-flight request (fail-closed) so LMON does not
	 * hang to its timeout.
	 */
	have_submit = cluster_write_fence_qvotec_poll_pending(&submit_marker);

	if (qvotec_n_disks == 0) {
		/* Single-node compat: no disks, no quorum to decide.  Hold
		 * quorum_state at INITIALIZING so any explicit consumer that
		 * does check it stays fail-closed (per Q4 v0.2 default). */
		if (have_submit)
			cluster_write_fence_qvotec_complete(false); /* no disk -> no majority */
		return;
	}

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES) {
		/* Defensive: invalid node_id ⇒ cannot author a self slot.
		 * Leave shmem at last-known state and skip the cycle.  Q7
		 * startup validator (next commit) will reject this config
		 * before we get here in production paths. */
		if (have_submit)
			cluster_write_fence_qvotec_complete(false); /* cannot author self slot */
		return;
	}

	/*
	 * Hardening v0.4 P1.3:  read matrix BEFORE writing self slot so
	 * we observe the OLD slot at our node_id offset.  If a peer is
	 * alive with the same node_id and a different incarnation,
	 * decide_quorum_view returns CLUSTER_COLLISION_FATAL_NEWER_SELF
	 * and we must FATAL before overwriting the peer's slot (per
	 * Q6 v0.2 newer-self-FATAL — the older serving instance keeps
	 * its in-flight transactions / cached buffers).
	 *
	 * On the first poll after qvotec start, our own slot at offset
	 * (cluster_node_id × 512) is whatever the previous incarnation
	 * left behind (or generation==0 from format).  generation==0 is
	 * skipped by decide_quorum_view;previous-incarnation slots with
	 * higher incarnation than self trigger Q6 OBSERVED_OLDER (not
	 * FATAL_NEWER_SELF) and we keep going.
	 */

	/* ---- 1. read full slot matrix BEFORE writing ---- */
	memset(qvotec_slot_matrix, 0,
		   sizeof(ClusterVotingSlot) * CLUSTER_MAX_VOTING_DISKS * CLUSTER_MAX_NODES);
	for (i = 0; i < qvotec_n_disks; i++) {
		uint32 node;

		/* Hardening v0.4 P1.2: io_states starts OK and DOWNGRADES on
		 * either header-read failure (whole-disk unreachable) OR a
		 * write failure later in step 3.  Reset to OK each cycle so
		 * a transient failure recovers, but do NOT ignore write
		 * failures — they must propagate into the decide() input. */
		io_states[i] = CLUSTER_VOTING_DISK_IO_OK;

		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			ClusterVotingSlot *cell = &qvotec_slot_matrix[i * CLUSTER_MAX_NODES + node];
			ClusterVotingDiskIoState rrc;

			rrc = cluster_voting_disk_read_slot(qvotec_fds[i], i, node, cell);
			if (rrc != CLUSTER_VOTING_DISK_IO_OK) {
				/* Per-slot miss is no-data;whole-disk failure only
				 * on FAILED at offset 0 (header read).  TORN on one
				 * peer's slot is not a whole-disk failure. */
				if (node == 0 && rrc == CLUSTER_VOTING_DISK_IO_FAILED)
					io_states[i] = CLUSTER_VOTING_DISK_IO_FAILED;
				memset(cell, 0, sizeof(*cell));
			}
		}
	}

	/*
	 * spec-4.12 D2: scan the matrix we just read for the durable fence marker
	 * and refresh the local write-fence token.  A tuple is authoritative only
	 * when an identical marker appears on >= quorum-majority disks (P0a); order
	 * by fence_epoch (monotonic) not event_id (P0b).  A minority / partial
	 * marker is ignored (counter) and the token is left to age out (fail-closed).
	 * This is independent of the node-quorum decision below.
	 */
	{
		ClusterFenceMarker disk_markers[CLUSTER_MAX_VOTING_DISKS];
		bool disk_has_marker[CLUSTER_MAX_VOTING_DISKS];
		ClusterFenceAuthority authority;

		for (i = 0; i < qvotec_n_disks; i++)
			disk_has_marker[i] = qvotec_best_marker_on_disk(i, &disk_markers[i]);

		authority = cluster_fence_authority_decide(disk_markers, disk_has_marker, qvotec_n_disks);
		if (authority.has_authority) {
			uint64 fence_lease_expire = now_us + (uint64)cluster_write_fence_lease_ms * 1000ULL;

			cluster_write_fence_refresh_from_marker(&authority.marker, fence_lease_expire);
		} else if (authority.minority_seen)
			cluster_write_fence_note_minority_marker();
	}

	/* ---- 2. decide BEFORE writing self slot ---- */
	(void)decide_quorum_view(qvotec_slot_matrix, io_states, (uint32)qvotec_n_disks,
							 CLUSTER_MAX_NODES, (uint32)cluster_node_id, qvotec_self_incarnation,
							 now_us, heartbeat_timeout_us, &decision);

	/*
	 * Hardening v0.4 P1.1:  Q6 v0.2 newer-self-FATAL.  decide_quorum_
	 * view observed an OK-disk fresh slot at our node_id offset with
	 * an incarnation strictly less than ours — a peer was serving
	 * with our node_id when we (the newer comer) booted.  Spec Q6
	 * v0.2 contract:  the newer instance MUST exit so the older
	 * peer's in-flight transactions / cached buffers stay valid.
	 *
	 * Publish the collision_state before FATAL so observability
	 * sees it via pg_cluster_quorum_state on the surviving peer.
	 * The FATAL ereport bypasses the rest of the cycle (no self
	 * slot write); ShutdownRequestPending is tripped by FATAL exit
	 * so the main loop's gate handles cleanup.
	 */
	if (decision.collision_state == CLUSTER_COLLISION_FATAL_NEWER_SELF) {
		pg_atomic_write_u32(&QvotecShmem->collision_state, (uint32)decision.collision_state);
		pg_atomic_write_u32(&QvotecShmem->quorum_state, (uint32)CLUSTER_QVOTEC_QUORUM_LOST);
		cluster_pgstat_inc(qvotec_counter_collision);

		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_NODE_ID_COLLISION),
				 errmsg("cluster.node_id %d collides with a serving peer "
						"(observed incarnation %llu, self incarnation %llu)",
						cluster_node_id, (unsigned long long)decision.collision_other_incarnation,
						(unsigned long long)qvotec_self_incarnation),
				 errdetail("This instance booted with a higher incarnation than "
						   "the peer slot already on disk — per Q6 v0.2 newer-"
						   "self-FATAL the newer comer exits to preserve the "
						   "older serving instance's in-flight state."),
				 errhint("Reconfigure cluster.node_id to a unique value, or "
						 "ensure the peer instance has exited before reusing "
						 "this node_id.")));
	}

	/* ---- 3. build + write self slot to every disk ---- */
	memset(&self_slot, 0, sizeof(self_slot));
	self_slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
	self_slot.version = CLUSTER_VOTING_SLOT_VERSION;
	self_slot.node_id = (uint32)cluster_node_id;
	self_slot.incarnation = qvotec_self_incarnation;
	self_slot.heartbeat_ts_us = now_us;
	self_slot.current_epoch = pg_atomic_read_u64(&QvotecShmem->current_epoch_at_boot);
	self_slot.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;

	for (i = 0; i < qvotec_n_disks; i++) {
		ClusterVotingDiskIoState wrc;
		ClusterVotingSlot *own_prior = &qvotec_slot_matrix[i * CLUSTER_MAX_NODES + cluster_node_id];

		/*
		 * spec-4.12 D2 (R13): the heartbeat rebuilt self_slot with a zeroed
		 * _reserved1, which would erase any fence marker this node previously
		 * wrote.  Carry the marker forward -- but STRICTLY per-disk: re-zero
		 * _reserved1 each iteration (no leak across disks) and preserve ONLY the
		 * marker from THIS node's own prior slot on THIS disk.  Copying disk j's
		 * marker onto disk i would amplify a 1-of-N minority marker into a
		 * quorum-majority = P0a revival; the per-disk input forbids it.
		 *
		 * spec-4.12 D4: a fresh marker submit from LMON overrides the preserve --
		 * the coordinator legitimately writes ITS OWN issued marker to ALL its
		 * disks (that is how the fence reaches quorum-majority; not amplification).
		 */
		memset(self_slot._reserved1, 0, sizeof(self_slot._reserved1));
		if (have_submit)
			cluster_fence_marker_pack(self_slot._reserved1, &submit_marker);
		else
			cluster_fence_marker_preserve_per_disk(self_slot._reserved1, own_prior->_reserved1);

		qvotec_slot_generation++;
		self_slot.generation = qvotec_slot_generation;
		self_slot.disk_index = (uint32)i;

		wrc = cluster_voting_disk_write_slot(qvotec_fds[i], &self_slot);
		if (wrc != CLUSTER_VOTING_DISK_IO_OK) {
			/*
			 * Hardening v0.4 P1.2:  write failure must propagate to
			 * the decide() inputs we just used.  But we ALREADY
			 * decided this cycle before this write — that's the
			 * trade-off of read-then-decide-then-write ordering.
			 * The write failure affects the NEXT cycle's view of
			 * this disk:  the read above will succeed reading our
			 * stale slot (still on disk with old generation), so
			 * the disk reports as OK but our heartbeat_ts_us ages
			 * out.  After heartbeat_timeout_us elapses the freshness
			 * gate (P2.1) drops self from alive_bitmap → quorum
			 * naturally shrinks.  In addition we promote the
			 * io_state for THIS cycle's published count so disks_ok
			 * reflects the write failure immediately, even though
			 * the decide() output already used the old value.
			 */
			io_states[i] = CLUSTER_VOTING_DISK_IO_FAILED;
			cluster_pgstat_inc(qvotec_counter_disk_io_fail);
		}
	}

	/*
	 * Hardening v0.6 F1:  recompute BOTH disks_ok_count AND quorum_state
	 * from possibly-downgraded io_states.  The earlier comment
	 * ("decide()'s output is authoritative for quorum_state, but the
	 * count reflects post-write reality") was wrong — if N=3 disks were
	 * all readable at decide() time but 2 then failed at write step,
	 * decide()'s quorum_state=OK is stale: this node only landed its
	 * heartbeat on 1/3 disks, peers reading the failed disks see an
	 * aging slot, and the cross-cluster majority guarantee breaks.
	 *
	 * Post-write quorum_size is the simple majority of disks that
	 * accepted the write; we mirror decide()'s formula so the two
	 * paths stay in lockstep.  collision_state and alive_bitmap
	 * remain decide()'s output (they reflect the read view, which is
	 * unchanged by write failure).
	 */
	{
		uint32 disks_ok_post_write = 0;
		uint32 quorum_size_post_write;

		for (i = 0; i < qvotec_n_disks; i++) {
			if (io_states[i] == CLUSTER_VOTING_DISK_IO_OK)
				disks_ok_post_write++;
		}
		decision.disks_ok_count = disks_ok_post_write;

		quorum_size_post_write = ((uint32)qvotec_n_disks / 2u) + 1u;
		if (disks_ok_post_write >= quorum_size_post_write)
			decision.quorum_state = CLUSTER_QVOTEC_QUORUM_OK;
		else if (disks_ok_post_write == 0)
			decision.quorum_state = CLUSTER_QVOTEC_QUORUM_LOST;
		else
			decision.quorum_state = CLUSTER_QVOTEC_QUORUM_UNCERTAIN;

		/*
		 * spec-4.12 D4: ack the in-flight marker submit.  The marker rode in the
		 * self-slot write above, so the count of disks that accepted the write
		 * (fdatasync'd) is exactly the marker's durable-disk count.  ACK only on
		 * >= quorum-majority -- otherwise the coordinator fails closed and does
		 * NOT publish the reconfig event (core 8.A order).
		 */
		if (have_submit)
			cluster_write_fence_qvotec_complete(disks_ok_post_write >= quorum_size_post_write);
	}

	/* ---- 4. publish shmem ---- */
	{
		uint32 prev_state = pg_atomic_read_u32(&QvotecShmem->quorum_state);

		pg_atomic_write_u32(&QvotecShmem->quorum_state, (uint32)decision.quorum_state);
		pg_atomic_write_u32(&QvotecShmem->disks_ok_count, decision.disks_ok_count);
		pg_atomic_write_u32(&QvotecShmem->disks_total_count, decision.disks_total_count);
		pg_atomic_write_u32(&QvotecShmem->collision_state, (uint32)decision.collision_state);

		if (prev_state == CLUSTER_QVOTEC_QUORUM_OK
			&& decision.quorum_state != CLUSTER_QVOTEC_QUORUM_OK) {
			pg_atomic_write_u64(&QvotecShmem->last_quorum_loss_ts_us, now_us);
			cluster_pgstat_inc(qvotec_counter_quorum_loss);
		}

		/* OBSERVED_OLDER is also a collision but not FATAL — count
		 * it so observability picks up the peer-incarnation race. */
		if (decision.collision_state == CLUSTER_COLLISION_OBSERVED_OLDER)
			cluster_pgstat_inc(qvotec_counter_collision);
	}
}

static void
qvotec_open_disks(void)
{
	const char *csv = cluster_voting_disks;
	const char *p;
	int i;

	qvotec_n_disks = 0;
	for (i = 0; i < CLUSTER_MAX_VOTING_DISKS; i++)
		qvotec_fds[i] = -1;

	if (csv == NULL || csv[0] == '\0')
		return; /* single-node compat — qvotec stays alive but no I/O */

	p = csv;
	while (*p) {
		const char *start = p;
		const char *end;
		char path[MAXPGPATH];
		size_t len;
		int fd;

		while (*p && *p != ',')
			p++;
		end = p;

		while (start < end && (*start == ' ' || *start == '\t'))
			start++;
		while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
			end--;

		len = (size_t)(end - start);
		if (len == 0) {
			if (*p == ',')
				p++;
			continue;
		}
		if (len >= MAXPGPATH) {
			qvotec_close_disks();
			ereport(FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("cluster.voting_disks path too long (>%d bytes)", MAXPGPATH - 1)));
		}
		if (qvotec_n_disks >= CLUSTER_MAX_VOTING_DISKS) {
			qvotec_close_disks();
			ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("cluster.voting_disks declares more than %d entries",
								   CLUSTER_MAX_VOTING_DISKS),
							errhint("Reduce cluster.voting_disks to an odd-majority list "
									"(1 / 3 / 5 / 7 disks recommended).")));
		}

		memcpy(path, start, len);
		path[len] = '\0';

		fd = cluster_voting_disk_open(path, /*create_if_missing*/ false);
		if (fd < 0) {
			int saved_errno = errno;
			qvotec_close_disks();
			errno = saved_errno;
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("cluster.voting_disks: cannot open \"%s\": %m", path),
							errhint("Voting disk files must exist (run pgrac-init or pre-format) "
									"and be readable/writable by the postgres user.")));
		}

		qvotec_fds[qvotec_n_disks++] = fd;

		if (*p == ',')
			p++;
	}
}


/* ============================================================
 * ClusterQvotecMain — aux process entry.
 *
 *	Step 1 skeleton: WaitLatch loop, lifecycle CAS transitions,
 *	lease writeback every poll_interval, poll_cycle_count
 *	increment.  Real poll cycle (read voting disks → tally →
 *	decide → write self slot → broadcast freeze/thaw on transition)
 *	is delegated to Step 2 D3+D4 modules + Step 3 D5 ProcSignal
 *	multiplexer.
 *
 *	Postmaster reaper invokes this after fork() under AuxProcType
 *	QvotecProcess (wired in Step 3 D7).  Until that wiring lands,
 *	this function is NOT called from production paths;cluster_unit
 *	harness can address-take to verify link symbol.
 * ============================================================ */
void
ClusterQvotecMain(void)
{
	long timeout_ms = CLUSTER_QVOTEC_DEFAULT_POLL_INTERVAL_MS;

	Assert(IsUnderPostmaster);

	QvotecPid = MyProcPid;
	MyBackendType = B_QVOTEC;
	init_ps_display(NULL);

	/* Signal handler setup (mirrors CssdMain).  SIGQUIT is installed by
	 * InitPostmasterChild;the others must be set explicitly so SIGHUP
	 * triggers config reload + SIGTERM triggers graceful shutdown. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (QvotecShmem == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_qvotec shmem region not attached"),
						errhint("cluster_qvotec_shmem_init() must run during "
								"CreateSharedMemoryAndSemaphores().")));

	/*
	 * P1.3 step 1 — open all configured voting disks before publishing
	 * READY so phase 4 driver only sees us ready when fds are valid.
	 * Empty CSV is OK (single-node compat); any other open(2) failure
	 * ereports FATAL inside qvotec_open_disks.  fds are closed
	 * explicitly when the main loop exits + via on_shmem_exit hook
	 * registered below for the crash / proc_exit path.
	 */
	qvotec_open_disks();
	pg_atomic_write_u32(&QvotecShmem->disks_total_count, (uint32)qvotec_n_disks);
	on_shmem_exit(qvotec_close_disks_atexit, (Datum)0);

	/*
	 * spec-4.12 D4: publish MyLatch so the reconfig coordinator (LMON) can wake us
	 * for a synchronous fence-marker submit.  No-op until the write-fence region is
	 * registered (D7); auto-cleared at proc_exit so a stale latch is never signalled.
	 */
	cluster_write_fence_publish_qvotec_latch(MyLatch);

	/*
	 * Hardening v0.4 P1.4: install per-I/O timeout handler for the
	 * voting disk slot R/W syscalls.  qvotec is the sole production
	 * caller of cluster_voting_disk_read_slot / write_slot, so it is
	 * safe to claim SIGALRM here (we previously SIG_IGN'd it).  The
	 * timeout value is read from cluster.voting_disk_io_timeout_ms
	 * each cycle so SIGHUP can adjust without restart.
	 */
	cluster_voting_disk_io_install_timeout_handler();
	cluster_voting_disk_io_set_timeout_ms(cluster_voting_disk_io_timeout_ms);

	/*
	 * P1.3 step 2 — boot incarnation + slot matrix + counter handles.
	 * Incarnation = process start timestamp gives us a unique value
	 * per qvotec run for Q6 collision detection.  Matrix lives in
	 * TopMemoryContext so it survives the per-cycle ResourceOwner
	 * resets and is freed automatically at proc_exit.
	 */
	qvotec_self_incarnation = (uint64)GetCurrentTimestamp();
	qvotec_slot_generation = 0;
	qvotec_slot_matrix = (ClusterVotingSlot *)MemoryContextAllocZero(
		TopMemoryContext, sizeof(ClusterVotingSlot) * CLUSTER_MAX_VOTING_DISKS * CLUSTER_MAX_NODES);
	qvotec_pgstat_lookup_all();

	/*
	 * Hardening v0.6 F2:  prior-incarnation self-slot detection.  If a
	 * previous postmaster of THIS node (same node_id) crashed or was
	 * stopped immediate-mode without zeroing its slot, and we are
	 * restarting within the heartbeat freshness window
	 * (2 × poll_interval_ms = 4s default), the first poll cycle would
	 * read our own ghost slot, see node_id == self_node_id with a
	 * lower (older) incarnation, and trigger Q6 newer-self-FATAL —
	 * killing a healthy restart.
	 *
	 * Mitigation:  scan all open disks once at startup; if any slot at
	 * offset self_node_id has flags & ALIVE and a heartbeat within the
	 * timeout window, ereport(LOG) + sleep one heartbeat_timeout so
	 * the ghost ages out before we enter the main poll loop.  Restart
	 * gap > heartbeat_timeout pays zero cost (the ghost is already
	 * stale; freshness gate would skip it).  Restart gap < timeout
	 * pays heartbeat_timeout_us extra startup latency, which is the
	 * minimum safe wait.
	 *
	 * This mitigation is read-only — we do not zero the ghost slot
	 * here; the next poll cycle will overwrite it with our fresh
	 * incarnation naturally.
	 */
	if (qvotec_n_disks > 0 && cluster_node_id >= 0 && (uint32)cluster_node_id < CLUSTER_MAX_NODES) {
		uint64 heartbeat_timeout_us = (uint64)cluster_quorum_poll_interval_ms * 2 * 1000ULL;
		uint64 now_us = (uint64)GetCurrentTimestamp();
		bool ghost_fresh = false;
		int d;

		for (d = 0; d < qvotec_n_disks && !ghost_fresh; d++) {
			ClusterVotingSlot probe;
			ClusterVotingDiskIoState rrc;

			rrc = cluster_voting_disk_read_slot(qvotec_fds[d], d, (uint32)cluster_node_id, &probe);
			if (rrc != CLUSTER_VOTING_DISK_IO_OK)
				continue;
			if (probe.generation == 0)
				continue; /* never written */
			if (!(probe.flags & CLUSTER_VOTING_SLOT_FLAG_ALIVE))
				continue; /* prior shutdown cleared ALIVE — ok */
			if (probe.incarnation == qvotec_self_incarnation)
				continue; /* same incarnation — impossible but defensive */
			if (probe.heartbeat_ts_us == 0)
				continue;
			if (now_us > probe.heartbeat_ts_us
				&& (now_us - probe.heartbeat_ts_us) > heartbeat_timeout_us)
				continue; /* already stale */
			ghost_fresh = true;
		}

		if (ghost_fresh) {
			ereport(LOG, (errmsg("qvotec: prior-incarnation self-slot still fresh, "
								 "waiting %lu ms for it to age out before first "
								 "poll (avoids fast-restart Q6 newer-self FATAL)",
								 (unsigned long)(heartbeat_timeout_us / 1000ULL))));
			pg_usleep((long)(heartbeat_timeout_us / 1000ULL) * 1000L);
		}
	}

	pg_atomic_write_u32(&QvotecShmem->state, CLUSTER_QVOTEC_READY);

	for (;;) {
		int rc;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			/* Re-publish I/O timeout in case admin tuned it. */
			cluster_voting_disk_io_set_timeout_ms(cluster_voting_disk_io_timeout_ms);
		}

		/*
		 * Two shutdown paths: SIGTERM-driven (postmaster fast/smart
		 * shutdown sets ShutdownRequestPending via SignalHandlerFor
		 * ShutdownRequest) AND shmem-driven (cluster_qvotec_request_
		 * shutdown writes SHUTTING_DOWN — used by 095 TAP / inject
		 * tests).  Mirrors CssdMain dual-gate pattern.  Without the
		 * SIGTERM gate, postmaster fast-shutdown blocks waiting for
		 * QVOTEC to exit and pg_ctl times out (D8 hardening F1).
		 */
		if (ShutdownRequestPending
			|| pg_atomic_read_u32(&QvotecShmem->state) == CLUSTER_QVOTEC_SHUTTING_DOWN)
			break;

		/* P1.3 step 2/3 — real poll cycle: write self slot, read
		 * matrix, decide quorum, publish shmem.  Counter bumps live
		 * inside qvotec_poll_once. */
		qvotec_poll_once();
		cluster_pgstat_inc(qvotec_counter_poll_cycle);

		timeout_ms = cluster_quorum_poll_interval_ms;

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
					   WAIT_EVENT_CLUSTER_BGPROC_QVOTEC_MAIN_LOOP);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/*
	 * Hardening v0.6 F2:  best-effort clear ALIVE flag on self-slot
	 * BEFORE closing disks, so a fast-restart sees our prior slot as
	 * "shutdown clean" rather than "ghost peer alive".  Failure here
	 * is non-fatal — startup ghost-detect path covers the residual
	 * crash / immediate-shutdown gap.
	 */
	qvotec_clear_self_alive_on_clean_shutdown();
	qvotec_close_disks();
	pg_atomic_write_u32(&QvotecShmem->state, CLUSTER_QVOTEC_DOWN);

	proc_exit(0);
}


/* ============================================================
 * cluster_qvotec_wait_for_ready / _request_shutdown — phase 4
 * driver helpers (Step 3 D8 wires these into cluster_startup_phase
 * sequence).
 * ============================================================ */

bool
cluster_qvotec_wait_for_ready(int timeout_ms)
{
	TimestampTz deadline;

	if (QvotecShmem == NULL)
		return false;

	deadline = GetCurrentTimestamp() + (TimestampTz)timeout_ms * 1000;

	for (;;) {
		uint32 s = pg_atomic_read_u32(&QvotecShmem->state);

		if (s == CLUSTER_QVOTEC_READY)
			return true;
		if (s == CLUSTER_QVOTEC_FAILED || s == CLUSTER_QVOTEC_DOWN)
			return false;

		if (GetCurrentTimestamp() >= deadline)
			return false;

		pg_usleep(10 * 1000); /* 10 ms */
	}
}

void
cluster_qvotec_request_shutdown(void)
{
	if (QvotecShmem == NULL)
		return;

	pg_atomic_write_u32(&QvotecShmem->state, CLUSTER_QVOTEC_SHUTTING_DOWN);
}


/*
 * cluster_qvotec_start — forward to postmaster spawn wrapper.
 *
 *	Called from cluster_run_phase4_sequence (Sprint A Step 3 D8) after
 *	CSSD has reached READY.  StartChildProcess is file-static in
 *	postmaster.c, so we use cluster_postmaster_start_qvotec as the
 *	narrow wrapper.
 */
pid_t
cluster_qvotec_start(void)
{
	Assert(!IsUnderPostmaster);
	return cluster_postmaster_start_qvotec();
}


/* ============================================================
 * D15 SRFs — pg_cluster_quorum_state + pg_cluster_voting_disks.
 *
 *	PG_FUNCTION_INFO_V1 macros + disable-cluster stubs live in
 *	cluster_ic.c (always-linked file).  Bodies here only compiled
 *	in --enable-cluster builds.
 *
 *	cluster_get_quorum_state — single row, 7 cols:
 *	  in_quorum bool / quorum_size int / disks_ok int / disks_total
 *	  int / current_epoch_at_boot int8 / last_quorum_loss_at
 *	  timestamptz / collision_state text
 *
 *	cluster_get_voting_disks — per-disk row, 7 cols:
 *	  path text / state text / last_read_at timestamptz /
 *	  last_write_at timestamptz / read_count int8 / write_count int8
 *	  / io_error_count int8
 *
 *	Step 4 scope: SRF skeleton against current shmem.  Per-disk
 *	timestamps + per-disk counters are NULL/0 placeholders until D8
 *	phase 4 driver wires real qvotec poll cycle (deferred per Step 3
 *	hardening).  Aggregate disks_ok / disks_total / global I/O error
 *	count are surfaced where wired.
 * ============================================================ */

#include "cluster/cluster_pgstat.h" /* cluster.qvotec.* counters */
#include "cluster/cluster_qvotec.h" /* public accessors */
#include "funcapi.h"
#include "utils/builtins.h" /* CStringGetTextDatum */

Datum
cluster_get_quorum_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	Datum values[7];
	bool nulls[7];
	int col = 0;
	uint64 ts;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (QvotecShmem == NULL) {
		/* qvotec shmem not initialised (cluster.enabled=off / boot
		 * race) — emit one row with all-NULL state per Q5 v0.2
		 * "fail-closed default" reasoning. */
		Datum n_values[7] = { 0 };
		bool n_nulls[7] = { false, true, true, true, true, true, true };

		n_values[0] = BoolGetDatum(false); /* in_quorum = false */
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, n_values, n_nulls);
		return (Datum)0;
	}

	memset(nulls, false, sizeof(nulls));

	values[col++] = BoolGetDatum(cluster_qvotec_in_quorum());
	values[col++]
		= Int32GetDatum((int32)pg_atomic_read_u32(&QvotecShmem->disks_total_count) / 2 + 1);
	values[col++] = Int32GetDatum((int32)pg_atomic_read_u32(&QvotecShmem->disks_ok_count));
	values[col++] = Int32GetDatum((int32)pg_atomic_read_u32(&QvotecShmem->disks_total_count));
	values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&QvotecShmem->current_epoch_at_boot));

	ts = pg_atomic_read_u64(&QvotecShmem->last_quorum_loss_ts_us);
	if (ts == 0)
		nulls[col] = true;
	else
		values[col] = TimestampTzGetDatum((TimestampTz)ts);
	col++;

	values[col++] = CStringGetTextDatum(cluster_qvotec_get_collision_state_name());

	Assert(col == 7);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum)0;
}


Datum
cluster_get_voting_disks(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	const char *csv;
	const char *p;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	csv = cluster_voting_disks;
	if (csv == NULL || csv[0] == '\0')
		return (Datum)0; /* empty config → 0 rows */

	p = csv;
	while (*p) {
		const char *start = p;
		const char *end;
		char *path_buf;
		size_t len;
		Datum values[7];
		bool nulls[7] = { false, false, true, true, false, false, false };

		while (*p && *p != ',')
			p++;
		end = p;

		/* trim leading whitespace */
		while (start < end && (*start == ' ' || *start == '\t'))
			start++;
		/* trim trailing whitespace */
		while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
			end--;

		len = end - start;
		if (len == 0) {
			if (*p == ',')
				p++;
			continue;
		}

		path_buf = palloc(len + 1);
		memcpy(path_buf, start, len);
		path_buf[len] = '\0';

		values[0] = CStringGetTextDatum(path_buf);
		values[1] = CStringGetTextDatum("unknown"); /* per-disk state
													 * NULL until D8 */
		/* values[2..3] last_read_at / last_write_at: NULL */
		values[4] = Int64GetDatum(0); /* read_count: NULL until D8 */
		values[5] = Int64GetDatum(0); /* write_count: NULL until D8 */
		values[6] = Int64GetDatum(0); /* io_error_count: NULL until D8 */

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		pfree(path_buf);

		if (*p == ',')
			p++;
	}

	return (Datum)0;
}


#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stubs.  Same symbol surface, all return defaults.
 * Required because cluster_qvotec.h is included from non-cluster
 * code paths via cluster_views.c (pg_proc.dat references the SRF
 * unconditionally) and the cluster_unit harness.
 */
Size
cluster_qvotec_shmem_size(void)
{
	return 0;
}
void
cluster_qvotec_shmem_init(void)
{}
void
cluster_qvotec_shmem_register(void)
{}
int
cluster_qvotec_get_pid(void)
{
	return 0;
}
const char *
cluster_qvotec_get_status_name(void)
{
	return "(disable-cluster)";
}
const char *
cluster_qvotec_get_quorum_state_name(void)
{
	return "(disable-cluster)";
}
int
cluster_qvotec_get_disks_ok_count(void)
{
	return 0;
}
int
cluster_qvotec_get_disks_total_count(void)
{
	return 0;
}
uint64
cluster_qvotec_get_current_epoch_at_boot(void)
{
	return 0;
}
const char *
cluster_qvotec_get_collision_state_name(void)
{
	return "(disable-cluster)";
}
bool
cluster_qvotec_in_quorum(void)
{
	return false;
}
void
cluster_freeze_writes_set(void)
{}
void
cluster_thaw_writes_set(void)
{}
bool
cluster_writes_currently_frozen(void)
{
	return false;
}
void
ClusterQvotecMain(void)
{
	proc_exit(0);
}
bool
cluster_qvotec_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
void
cluster_qvotec_request_shutdown(void)
{}

#endif /* USE_PGRAC_CLUSTER */
