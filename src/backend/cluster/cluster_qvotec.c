/*-------------------------------------------------------------------------
 *
 * cluster_qvotec.c
 *	  pgrac QVOTEC (Quorum Voting Coordinator) — spec-2.6 Sprint A Step 1.
 *
 *	  6th cluster aux process (LMON / LCK / DIAG / Stats / CSSD / QVOTEC).
 *	  Polls voting disks on shared storage (Step 2 D3 module), decides
 *	  cluster-wide quorum (Step 2 D4 module), broadcasts cluster_freeze_
 *	  writes / cluster_thaw_writes via PG ProcSignal multiplexer (Step 3
 *	  D5 PG-original mod) on quorum_state transition.
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

#include "cluster/cluster_elog.h"  /* CLUSTER_LOG (best-effort logging) */
#include "cluster/cluster_guc.h"   /* cluster_enabled */
#include "cluster/cluster_shmem.h" /* cluster_shmem_register_region */


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
 *	Called at every write-intent boundary (INSERT/UPDATE/DELETE/DDL
 *	entry — Step 3 D6) AND every commit boundary (Q5 v0.2 safety
 *	net).  Hot path acceptable.
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

	pg_atomic_write_u32(&QvotecShmem->state, CLUSTER_QVOTEC_READY);

	for (;;) {
		uint64 now_us;
		uint64 next_lease_expire;
		int rc;

		CHECK_FOR_INTERRUPTS();

		/* SHUTTING_DOWN external transition (postmaster shutdown
		 * signal handler, wired Step 3 D7) breaks the loop. */
		if (pg_atomic_read_u32(&QvotecShmem->state) == CLUSTER_QVOTEC_SHUTTING_DOWN)
			break;

		/*
		 * Step 1 stub poll cycle — actual disk I/O + quorum decision
		 * delegated to Step 2 D3+D4 modules.  Until those land, the
		 * stub:
		 *   1. bumps poll_cycle_count (observability counter)
		 *   2. writes last_poll_ts_us = now
		 *   3. writes lease_expire_at_us = now + 2 × poll_interval
		 *      (lease semantics live even before real poll;backends
		 *      see quorum_state = INITIALIZING and stay fail-closed)
		 *   4. quorum_state stays INITIALIZING (Step 2 sets OK only
		 *      after a real successful poll cycle)
		 */
		(void)pg_atomic_fetch_add_u32(&QvotecShmem->poll_cycle_count, 1);

		now_us = (uint64)GetCurrentTimestamp();
		next_lease_expire = now_us + (uint64)(timeout_ms * 2 * 1000);

		pg_atomic_write_u64(&QvotecShmem->last_poll_ts_us, now_us);
		pg_atomic_write_u64(&QvotecShmem->lease_expire_at_us, next_lease_expire);

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
					   /* wait_event_info wired Step 4 D11 */ 0);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

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
