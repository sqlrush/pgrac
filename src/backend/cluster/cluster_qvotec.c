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
 *	    - ClusterQvotecShmem private 448-byte region (128-byte Q4 v0.2
 *	      lease prefix plus the 320-byte spec-5.15A local mailbox)
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

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h" /* init_ps_display */
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "cluster/cluster_clean_leave.h" /* spec-5.13 §2.5: leave-marker submit + rebuild */
#include "cluster/cluster_adg.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_node_remove.h" /* spec-5.18 §2.5: removal-marker carry-forward */
#include "cluster/cluster_elog.h"		 /* CLUSTER_LOG (best-effort logging) */
#include "cluster/cluster_epoch.h"		 /* spec-4.12b D2/D5: current-epoch upper-bound Assert */
#include "cluster/cluster_epoch_ballot.h"
#include "cluster/cluster_guc.h"		 /* cluster_enabled */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_replacement_request.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_pgstat.h"			 /* cluster.qvotec.* counters */
#include "cluster/cluster_reconfig.h"		 /* spec-4.12b D2: applied-membership snapshot */
#include "cluster/cluster_xid_stripe_boot.h" /* spec-6.15 D5b: region-5 scan + seed */
#include "cluster/cluster_shmem.h"			 /* cluster_shmem_register_region */
#include "cluster/cluster_write_fence.h"	 /* spec-4.12 D2: fence marker scan + token refresh */
#include "utils/memutils.h"					 /* TopMemoryContext */


/* ============================================================
 * ClusterQvotecShmem — private fixed 448-byte region.
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
 *	  64..71  uint64 self_incarnation      (canonical boot session)
 *	  72..75  uint32 prior_unclean_death   (crash-rejoin barrier)
 *	  76..127 uint8[52] _reserved          (future expansion)
 *	 128..447 ClusterQvotecMailbox          (spec-5.15A §2.1A.4)
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
	pg_atomic_uint64 self_incarnation;
	/*
	 * Crash-rejoin re-declare barrier (Shape A) — set ONCE at qvotec startup
	 * (before the READY publish), read-only thereafter: 1 iff this node's
	 * prior-incarnation self-slot on the voting disk still had the ALIVE flag
	 * set (a clean shutdown clears it via qvotec_clear_self_alive_on_clean_
	 * shutdown; a crash / immediate stop does NOT), i.e. this boot follows an
	 * UNCLEAN death.  The off-path rejoin tick fences self-home blocks +
	 * closes the write gate on this, so a crash-rejoined node never cold-
	 * serves stale ownership even when it restarts faster than the survivor's
	 * dead-deadband (the epoch signal is INITIAL on both sides in that race).
	 */
	pg_atomic_uint32 prior_unclean_death; /* offset 72..75 */
	uint8 _reserved[52];
	ClusterQvotecMailbox mailbox;
} ClusterQvotecShmem;

StaticAssertDecl(sizeof(ClusterQvotecShmem) == CLUSTER_QVOTEC_SHMEM_BYTES,
					 "ClusterQvotecShmem must be exactly 448 bytes");
StaticAssertDecl(offsetof(ClusterQvotecShmem, self_incarnation) == 64,
					 "ClusterQvotecShmem self incarnation offset");
StaticAssertDecl(offsetof(ClusterQvotecShmem, prior_unclean_death) == 72,
					 "prior_unclean_death must sit at offset 72 (queue lane owns 64..71)");
StaticAssertDecl(offsetof(ClusterQvotecShmem, mailbox) == CLUSTER_QVOTEC_SHMEM_PREFIX_BYTES,
					 "ClusterQvotecShmem mailbox must start at absolute offset 128");


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

/*
 * Keep poll starts on the configured cadence.  Waiting a full interval after
 * the disk cycle makes the effective period `cycle work + interval`, which can
 * expire the fixed 2x lease even though the dedicated qvotec is progressing.
 * A cycle that itself consumes the interval runs the successor immediately;
 * an actual cycle lasting 2x the interval still expires the lease and remains
 * fail-closed.
 */
static long
qvotec_poll_wait_timeout_ms(uint64 elapsed_us, int poll_interval_ms)
{
	uint64 interval_us;
	uint64 remaining_us;

	if (poll_interval_ms <= 0)
		return 0;
	interval_us = (uint64)poll_interval_ms * 1000ULL;
	if (elapsed_us >= interval_us)
		return 0;
	remaining_us = interval_us - elapsed_us;
	return (long)((remaining_us + 999ULL) / 1000ULL);
}

/* A test stimulus on the actual poll owner, once per observed arm interval.
 * Registry state remains process-local.  The harness must await this owner's
 * disarm log before rearming; SQL-side hits are not execution evidence. */
static void
qvotec_poll_pre_injection(void)
{
	static bool consumed;
	const char *point = "cluster-qvotec-poll-pre";
	ClusterInjectFaultType type = CLUSTER_FAULT_NONE;
	uint64 delay_us = 0;
	uint64 lease_us;
	instr_time started;
	instr_time ended;
	int i;

	if (MyBackendType != B_QVOTEC)
		return;
	if (!consumed && cluster_injection_armed_count == 0)
		return;
	if (!cluster_injection_is_armed(point)) {
		if (consumed)
			elog(LOG, "qvotec poll-pre injection disarmed node=%d pid=%d poll_cycle=" UINT64_FORMAT,
				 cluster_node_id, MyProcPid, cluster_pgstat_read(qvotec_counter_poll_cycle));
		consumed = false;
		return;
	}
	if (consumed)
		return;
	for (i = 0; i < cluster_injection_get_count(); i++) {
		const char *name;
		uint64 hits;

		if (cluster_injection_get_state_at(i, &name, &type, &hits) && strcmp(name, point) == 0)
			break;
	}
	consumed = true;
	if (type != CLUSTER_FAULT_SLEEP || !cluster_cr_injection_armed(point, &delay_us)
		|| delay_us == 0 || delay_us > 10000000ULL) {
		elog(WARNING, "qvotec poll-pre injection requires one bounded sleep");
		return;
	}
	lease_us = QvotecShmem == NULL ? 0 : pg_atomic_read_u64(&QvotecShmem->lease_expire_at_us);
	elog(LOG,
		 "qvotec poll-pre injection begin node=%d pid=%d delay_us=" UINT64_FORMAT
		 " prior_lease_us=" UINT64_FORMAT " now_us=" INT64_FORMAT " poll_cycle=" UINT64_FORMAT,
		 cluster_node_id, MyProcPid, delay_us, lease_us, GetCurrentTimestamp(),
		 cluster_pgstat_read(qvotec_counter_poll_cycle));
	INSTR_TIME_SET_CURRENT(started);
	pg_usleep((long)delay_us);
	INSTR_TIME_SET_CURRENT(ended);
	INSTR_TIME_SUBTRACT(ended, started);
	elog(LOG, "qvotec poll-pre injection end node=%d pid=%d elapsed_us=" UINT64_FORMAT,
		 cluster_node_id, MyProcPid, (uint64)INSTR_TIME_GET_MICROSEC(ended));
}


/* ============================================================
 * Shmem region — size / init / register.
 *
 *	Mirrors cluster_epoch / cluster_diag / cluster_cssd patterns;
 *	registered from cluster_shmem.c in Step 4 D9 with name
 *	"pgrac cluster qvotec".
 * ============================================================ */

static bool
qvotec_mailbox_bytes_are_zero(const uint8 *bytes, Size nbytes)
{
	Size i;

	for (i = 0; i < nbytes; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static bool
qvotec_mailbox_opcode_valid(uint32 opcode)
{
	return opcode == CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD
		   || opcode == CLUSTER_QVOTEC_MAILBOX_PROPOSE_VALUE;
}

static bool
qvotec_mailbox_completion_result_valid(uint32 result)
{
	return result >= CLUSTER_QVOTEC_MAILBOX_CHOSEN && result <= CLUSTER_QVOTEC_MAILBOX_HOLD;
}

void
cluster_qvotec_mailbox_restart_reset(ClusterQvotecMailbox *mailbox)
{
	if (mailbox == NULL)
		return;

	memset(mailbox, 0, sizeof(*mailbox));
	pg_atomic_init_u64(&mailbox->request_seq, 0);
	pg_atomic_init_u64(&mailbox->completion_seq, 0);
	pg_atomic_init_u32(&mailbox->request_opcode, CLUSTER_QVOTEC_MAILBOX_NONE);
	pg_atomic_init_u32(&mailbox->completion_result, CLUSTER_QVOTEC_MAILBOX_RESULT_NONE);
}

ClusterQvotecMailboxSubmitStatus
cluster_qvotec_mailbox_lmon_submit(
	ClusterQvotecMailbox *mailbox, ClusterQvotecMailboxOpcode opcode,
	const uint8 request_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES], uint64 *request_seq_out)
{
	uint64 request_seq;
	uint64 completion_seq;
	uint64 expected_seq;
	uint64 next_seq;

	if (request_seq_out != NULL)
		*request_seq_out = 0;
	if (mailbox == NULL || request_value == NULL || request_seq_out == NULL
		|| !qvotec_mailbox_opcode_valid((uint32)opcode))
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_INVALID;
	if (opcode == CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD
		&& !qvotec_mailbox_bytes_are_zero(request_value, CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES))
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_INVALID;

	request_seq = pg_atomic_read_u64(&mailbox->request_seq);
	completion_seq = pg_atomic_read_u64(&mailbox->completion_seq);
	if ((request_seq & UINT64_C(1)) != 0 || (completion_seq & UINT64_C(1)) != 0
		|| request_seq != completion_seq)
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_BUSY;
	if (request_seq > UINT64_MAX - 2)
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_HOLD;

	next_seq = request_seq + 2;
	expected_seq = request_seq;
	if (!pg_atomic_compare_exchange_u64(&mailbox->request_seq, &expected_seq, request_seq + 1))
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_BUSY;

	pg_write_barrier();
	memcpy(mailbox->request_value, request_value, sizeof(mailbox->request_value));
	pg_atomic_write_u32(&mailbox->request_opcode, (uint32)opcode);
	pg_write_barrier();
	pg_atomic_write_u64(&mailbox->request_seq, next_seq);
	*request_seq_out = next_seq;
	return CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED;
}

bool
cluster_qvotec_mailbox_qvotec_poll(ClusterQvotecMailbox *mailbox,
									ClusterQvotecMailboxRequest *request_out)
{
	ClusterQvotecMailboxRequest snapshot;
	uint64 request_seq_before;
	uint64 request_seq_after;
	uint64 completion_seq;
	uint32 opcode;

	if (request_out != NULL)
		memset(request_out, 0, sizeof(*request_out));
	if (mailbox == NULL || request_out == NULL)
		return false;

	request_seq_before = pg_atomic_read_u64(&mailbox->request_seq);
	completion_seq = pg_atomic_read_u64(&mailbox->completion_seq);
	if (request_seq_before == 0 || (request_seq_before & UINT64_C(1)) != 0
		|| (completion_seq & UINT64_C(1)) != 0 || completion_seq > UINT64_MAX - 2
		|| request_seq_before != completion_seq + 2)
		return false;

	pg_read_barrier();
	opcode = pg_atomic_read_u32(&mailbox->request_opcode);
	snapshot.request_seq = request_seq_before;
	snapshot.opcode = (ClusterQvotecMailboxOpcode)opcode;
	memcpy(snapshot.request_value, mailbox->request_value, sizeof(snapshot.request_value));
	pg_read_barrier();
	request_seq_after = pg_atomic_read_u64(&mailbox->request_seq);
	if (request_seq_before != request_seq_after || (request_seq_after & UINT64_C(1)) != 0
		|| !qvotec_mailbox_opcode_valid(opcode)
		|| (opcode == CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD
			&& !qvotec_mailbox_bytes_are_zero(snapshot.request_value,
											CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES)))
		return false;

	*request_out = snapshot;
	return true;
}

bool
cluster_qvotec_mailbox_qvotec_complete(
	ClusterQvotecMailbox *mailbox, uint8 configured_disk_bitmap,
	const ClusterQvotecMailboxCompletion *completion)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (mailbox == NULL || completion == NULL
		|| !qvotec_mailbox_completion_result_valid((uint32)completion->result)
		|| completion->actor_phase > CLUSTER_QVOTEC_ACTOR_HOLD
		|| (configured_disk_bitmap & ~CLUSTER_QVOTEC_CONFIGURED_DISK_MASK) != 0
		|| (completion->observed_disk_bitmap & ~configured_disk_bitmap) != 0
		|| !qvotec_mailbox_bytes_are_zero(mailbox->reserved, sizeof(mailbox->reserved)))
		return false;

	request_seq = pg_atomic_read_u64(&mailbox->request_seq);
	completion_seq = pg_atomic_read_u64(&mailbox->completion_seq);
	if (completion->request_seq == 0 || (completion->request_seq & UINT64_C(1)) != 0
		|| request_seq != completion->request_seq || completion_seq > UINT64_MAX - 2
		|| completion_seq + 2 != completion->request_seq)
		return false;

	memcpy(mailbox->completion_value, completion->completion_value,
		   sizeof(mailbox->completion_value));
	memcpy(mailbox->completion_ballot, completion->completion_ballot,
		   sizeof(mailbox->completion_ballot));
	mailbox->observed_disk_bitmap = completion->observed_disk_bitmap;
	mailbox->actor_phase = completion->actor_phase;
	mailbox->detail = completion->detail;
	pg_write_barrier();

	/* A restart reset during actor work invalidates this volatile completion. */
	if (pg_atomic_read_u64(&mailbox->request_seq) != completion->request_seq
		|| pg_atomic_read_u64(&mailbox->completion_seq) != completion_seq)
		return false;
	pg_atomic_write_u32(&mailbox->completion_result, (uint32)completion->result);
	pg_write_barrier();
	pg_atomic_write_u64(&mailbox->completion_seq, completion->request_seq);
	return true;
}

bool
cluster_qvotec_mailbox_lmon_poll_completion(
	ClusterQvotecMailbox *mailbox, uint64 request_seq,
	ClusterQvotecMailboxCompletion *completion_out)
{
	ClusterQvotecMailboxCompletion snapshot;
	uint64 completion_seq_before;
	uint64 completion_seq_after;
	uint32 result;

	if (completion_out != NULL)
		memset(completion_out, 0, sizeof(*completion_out));
	if (mailbox == NULL || completion_out == NULL || request_seq == 0
		|| (request_seq & UINT64_C(1)) != 0)
		return false;

	completion_seq_before = pg_atomic_read_u64(&mailbox->completion_seq);
	if (completion_seq_before != request_seq
		|| pg_atomic_read_u64(&mailbox->request_seq) != request_seq)
		return false;

	pg_read_barrier();
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.request_seq = request_seq;
	result = pg_atomic_read_u32(&mailbox->completion_result);
	snapshot.result = (ClusterQvotecMailboxResult)result;
	memcpy(snapshot.completion_value, mailbox->completion_value,
		   sizeof(snapshot.completion_value));
	memcpy(snapshot.completion_ballot, mailbox->completion_ballot,
		   sizeof(snapshot.completion_ballot));
	snapshot.observed_disk_bitmap = mailbox->observed_disk_bitmap;
	snapshot.actor_phase = mailbox->actor_phase;
	snapshot.detail = mailbox->detail;
	pg_read_barrier();
	completion_seq_after = pg_atomic_read_u64(&mailbox->completion_seq);
	if (completion_seq_before != completion_seq_after
		|| !qvotec_mailbox_completion_result_valid(result)
		|| snapshot.actor_phase > CLUSTER_QVOTEC_ACTOR_HOLD
		|| (snapshot.observed_disk_bitmap & ~CLUSTER_QVOTEC_CONFIGURED_DISK_MASK) != 0
		|| !qvotec_mailbox_bytes_are_zero(mailbox->reserved, sizeof(mailbox->reserved)))
		return false;

	*completion_out = snapshot;
	return true;
}

ClusterQvotecMailboxSubmitStatus
cluster_qvotec_authority_lmon_submit(
	ClusterQvotecMailboxOpcode opcode,
	const uint8 request_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint64 *request_seq_out)
{
	if (QvotecShmem == NULL)
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_HOLD;
	return cluster_qvotec_mailbox_lmon_submit(
		&QvotecShmem->mailbox, opcode, request_value, request_seq_out);
}

bool
cluster_qvotec_authority_lmon_poll_completion(
	uint64 request_seq, ClusterQvotecMailboxCompletion *completion_out)
{
	if (QvotecShmem == NULL)
		return false;
	return cluster_qvotec_mailbox_lmon_poll_completion(
		&QvotecShmem->mailbox, request_seq, completion_out);
}

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
		pg_atomic_init_u64(&QvotecShmem->self_incarnation, 0);
		pg_atomic_init_u32(&QvotecShmem->prior_unclean_death, 0);
		memset(QvotecShmem->_reserved, 0, sizeof(QvotecShmem->_reserved));
		cluster_qvotec_mailbox_restart_reset(&QvotecShmem->mailbox);
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

int
cluster_qvotec_get_status(void)
{
	if (QvotecShmem == NULL)
		return (int)CLUSTER_QVOTEC_STARTING;
	return (int)pg_atomic_read_u32(&QvotecShmem->state);
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

/*
 * cluster_qvotec_prior_unclean_death -- crash-rejoin re-declare barrier
 * (Shape A).  True iff this node's prior-incarnation self-slot on the voting
 * disk still carried the ALIVE flag at startup (an unclean death: a crash /
 * immediate stop that skipped the clean-shutdown ALIVE blank).  Latched once
 * before the READY publish; stable for the incarnation.  False when qvotec is
 * absent (no voting disks) so a diskless / single-node deployment is never
 * fenced by this signal.
 */
bool
cluster_qvotec_prior_unclean_death(void)
{
	if (QvotecShmem == NULL)
		return false;
	return pg_atomic_read_u32(&QvotecShmem->prior_unclean_death) != 0;
}

uint64
cluster_qvotec_get_current_epoch_at_boot(void)
{
	if (QvotecShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&QvotecShmem->current_epoch_at_boot);
}

void
cluster_qvotec_publish_self_incarnation(uint64 incarnation)
{
	if (QvotecShmem != NULL)
		pg_atomic_write_u64(&QvotecShmem->self_incarnation, incarnation);
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	if (QvotecShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&QvotecShmem->self_incarnation);
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
/* False-path evidence only. Do not resample shared state, renew a lease, or
 * log a success. Unsampled values are zero; the reason defines their meaning. */
static bool
qvotec_admission_denied(unsigned int diagnostic_bit, const char *reason, uint32 sampled_state,
						uint64 sampled_expiry, uint64 sampled_now)
{
	static uint32 reported_reasons;

	if ((reported_reasons & (UINT32_C(1) << diagnostic_bit)) == 0) {
		reported_reasons |= UINT32_C(1) << diagnostic_bit;
		ereport(LOG, (errmsg_internal("PGRAC_FAMILY=QUORUM_ADMISSION PGRAC_REASON=%s node=%d "
									  "sampled_state=%u sampled_expiry_us=%llu sampled_now_us=%llu",
									  reason, cluster_node_id, sampled_state,
									  (unsigned long long)sampled_expiry,
									  (unsigned long long)sampled_now)));
	}
	return false;
}

bool
cluster_qvotec_in_quorum(void)
{
	uint64 now_us;
	uint64 lease_expire;
	uint32 q;

	/* Disable-cluster / pre-shmem path: fail-closed. */
	if (QvotecShmem == NULL)
		return qvotec_admission_denied(0, "SHMEM_UNAVAILABLE", 0, 0, 0);

	/* Process-local frozen flag set by ProcSignal handler — wins
	 * regardless of lease state (defensive double-gate). */
	if (cluster_writes_frozen)
		return qvotec_admission_denied(1, "WRITES_FROZEN", 0, 0, 0);

	q = pg_atomic_read_u32(&QvotecShmem->quorum_state);
	if (q != CLUSTER_QVOTEC_QUORUM_OK) {
		switch (q) {
		case CLUSTER_QVOTEC_QUORUM_INITIALIZING:
			return qvotec_admission_denied(2, "STATE_INITIALIZING", q, 0, 0);
		case CLUSTER_QVOTEC_QUORUM_UNCERTAIN:
			return qvotec_admission_denied(3, "STATE_UNCERTAIN", q, 0, 0);
		case CLUSTER_QVOTEC_QUORUM_LOST:
			return qvotec_admission_denied(4, "STATE_LOST", q, 0, 0);
		default:
			return qvotec_admission_denied(5, "STATE_INVALID", q, 0, 0);
		}
	}

	lease_expire = pg_atomic_read_u64(&QvotecShmem->lease_expire_at_us);
	now_us = (uint64)GetCurrentTimestamp();
	if (now_us >= lease_expire)
		return qvotec_admission_denied(6, "LEASE_EXPIRED", q, lease_expire, now_us);

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

StaticAssertDecl(CLUSTER_UNDO_ROOT_DESCRIPTOR_FILE_BYTES_MIN
					 == CLUSTER_VOTING_PGRD_FILE_BYTES_MIN,
				 "PGRD voting capacity bounds must match");

static bool
qvotec_pgsa_bytes_are_zero(const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	int i;

	for (i = 0; i < CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static ClusterUndoRootDescriptorState qvotec_undo_root_descriptor_read_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	uint8 root_kind, int32 owner_node, ClusterUndoRootDescriptorV1 *out,
	uint8 *out_observed_disk_bitmap);

static bool
qvotec_undo_root_descriptor_provision_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	uint8 *out_completed_disk_bitmap)
{
	bool eligible[CLUSTER_MAX_VOTING_DISKS] = { false };
	bool wrote[CLUSTER_MAX_VOTING_DISKS] = { false };
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 observed_bitmap = 0;
	ClusterUndoRootDescriptorV1 committed;
	off_t offset;
	int eligible_count = 0;
	int completed_count = 0;
	int majority;
	int i;

	if (fds == NULL || desired == NULL || out_completed_disk_bitmap == NULL
		|| n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| cluster_undo_root_descriptor_decode(
			   desired, system_identifier, &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.descriptor_incarnation != 1)
		return false;
	if (descriptor.root_kind == CLUSTER_UNDO_ROOT_KIND_SHARED)
		offset = CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET;
	else if (descriptor.root_kind == CLUSTER_UNDO_ROOT_KIND_LOCAL)
		offset = CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_OFFSET(
			descriptor.owner_node);
	else
		return false;

	majority = n_disks / 2 + 1;
	for (i = 0; i < n_disks; i++) {
		ClusterVotingDiskRawReadState read_state;
		ClusterUndoRootDescriptorV1 observed;
		ClusterUndoRootDescriptorState descriptor_state;

		memset(image, 0, sizeof(image));
		read_state = cluster_voting_disk_read_raw_slot_at(
			fds[i], offset, image);
		if (read_state == CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF) {
			eligible[i] = true;
			eligible_count++;
			continue;
		}
		if (read_state == CLUSTER_VOTING_DISK_RAW_READ_SHORT)
			return false;
		if (read_state != CLUSTER_VOTING_DISK_RAW_READ_FULL)
			continue;
		descriptor_state = cluster_undo_root_descriptor_decode(
			image, system_identifier, &observed);
		if (descriptor_state == CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
			|| (descriptor_state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				&& memcmp(image, desired, sizeof(image)) == 0)) {
			eligible[i] = true;
			eligible_count++;
			continue;
		}
		return false;
	}
	if (eligible_count < majority)
		return false;

	for (i = 0; i < n_disks; i++) {
		if (eligible[i]
			&& cluster_voting_disk_write_raw_slot_at(fds[i], offset, desired)
				   == CLUSTER_VOTING_DISK_IO_OK)
			wrote[i] = true;
	}
	for (i = 0; i < n_disks; i++) {
		if (!wrote[i])
			continue;
		memset(image, 0, sizeof(image));
		if (cluster_voting_disk_read_raw_slot_at(fds[i], offset, image)
				== CLUSTER_VOTING_DISK_RAW_READ_FULL
			&& memcmp(image, desired, sizeof(image)) == 0) {
			completed_count++;
		}
	}
	if (completed_count < majority)
		return false;
	if (qvotec_undo_root_descriptor_read_fds(
			fds, n_disks, system_identifier, descriptor.root_kind,
			descriptor.owner_node, &committed, &observed_bitmap)
			!= CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| !cluster_undo_root_descriptor_encode(&committed, image)
		|| memcmp(image, desired, sizeof(image)) != 0)
		return false;
	*out_completed_disk_bitmap = observed_bitmap;
	return true;
}

static ClusterUndoRootDescriptorState
qvotec_undo_root_descriptor_read_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	uint8 root_kind, int32 owner_node, ClusterUndoRootDescriptorV1 *out,
	uint8 *out_observed_disk_bitmap)
{
	uint8 images[CLUSTER_MAX_VOTING_DISKS]
		[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	ClusterUndoRootDescriptorV1 descriptors[CLUSTER_MAX_VOTING_DISKS];
	ClusterUndoRootDescriptorState states[CLUSTER_MAX_VOTING_DISKS];
	bool valid[CLUSTER_MAX_VOTING_DISKS] = { false };
	off_t offset;
	int majority;
	int selected = -1;
	int i;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out_observed_disk_bitmap != NULL)
		*out_observed_disk_bitmap = 0;
	if (fds == NULL || n_disks <= 0
		|| n_disks > CLUSTER_MAX_VOTING_DISKS || system_identifier == 0
		|| out == NULL || out_observed_disk_bitmap == NULL)
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
	if (root_kind == CLUSTER_UNDO_ROOT_KIND_SHARED && owner_node == -1)
		offset = CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET;
	else if (root_kind == CLUSTER_UNDO_ROOT_KIND_LOCAL && owner_node >= 0
			 && owner_node < CLUSTER_MAX_NODES)
		offset = CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_OFFSET(owner_node);
	else
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;

	memset(images, 0, sizeof(images));
	memset(descriptors, 0, sizeof(descriptors));
	memset(states, 0, sizeof(states));
	majority = n_disks / 2 + 1;
	for (i = 0; i < n_disks; i++) {
		ClusterVotingDiskRawReadState read_state;

		read_state = cluster_voting_disk_read_raw_slot_at(
			fds[i], offset, images[i]);
		if (read_state == CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF) {
			states[i] = CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED;
			valid[i] = true;
			continue;
		}
		if (read_state != CLUSTER_VOTING_DISK_RAW_READ_FULL)
			return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
		states[i] = cluster_undo_root_descriptor_decode(
			images[i], system_identifier, &descriptors[i]);
		if (states[i] == CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED)
			valid[i] = true;
		else if (states[i] == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				 && descriptors[i].root_kind == root_kind
				 && descriptors[i].owner_node == owner_node)
			valid[i] = true;
		else
			return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
	}

	for (i = 0; i < n_disks; i++) {
		int j;

		if (states[i] != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID)
			continue;
		for (j = i + 1; j < n_disks; j++) {
			if (states[j] == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				&& descriptors[i].descriptor_incarnation
					   == descriptors[j].descriptor_incarnation
				&& memcmp(images[i], images[j], sizeof(images[i])) != 0)
				return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
		}
	}

	for (i = 0; i < n_disks; i++) {
		int identical = 0;
		int j;

		if (!valid[i])
			continue;
		for (j = 0; j < n_disks; j++) {
			if (valid[j]
				&& memcmp(images[i], images[j], sizeof(images[i])) == 0)
				identical++;
		}
		if (identical >= majority) {
			selected = i;
			break;
		}
	}
	if (selected < 0)
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;

	for (i = 0; i < n_disks; i++) {
		if (valid[i]
			&& memcmp(images[selected], images[i], sizeof(images[i])) == 0)
			*out_observed_disk_bitmap |= (uint8)(UINT8_C(1) << i);
	}
	if (states[selected] == CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED)
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED;
	*out = descriptors[selected];
	return CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID;
}

static bool
qvotec_undo_root_descriptor_formation_attested_fds(
	const int *fds, int n_disks)
{
	int i;

	if (fds == NULL
		|| (n_disks != 1 && n_disks != 3 && n_disks != 5 && n_disks != 7))
		return false;
	for (i = 0; i < n_disks; i++) {
		if (!cluster_voting_disk_pgrd_authority_attest(fds[i]))
			return false;
	}
	return true;
}

static ClusterSemanticActivationResult
qvotec_semantic_activation_record_read_fds(
	const int *fds, int n_disks,
	uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open)
{
	uint8 images[CLUSTER_MAX_VOTING_DISKS]
		[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	ClusterSemanticActivationRecord records[CLUSTER_MAX_VOTING_DISKS];
	bool valid[CLUSTER_MAX_VOTING_DISKS];
	bool nonzero[CLUSTER_MAX_VOTING_DISKS];
	int majority;
	int selected = -1;
	int i;

	if (selected_bytes != NULL)
		memset(selected_bytes, 0,
			   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (fds == NULL || n_disks <= 0
		|| n_disks > CLUSTER_MAX_VOTING_DISKS
		|| selected_bytes == NULL || implicit_open == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	memset(images, 0, sizeof(images));
	memset(records, 0, sizeof(records));
	memset(valid, 0, sizeof(valid));
	memset(nonzero, 0, sizeof(nonzero));
	majority = n_disks / 2 + 1;
	for (i = 0; i < n_disks; i++) {
		ClusterVotingDiskRawReadState state
			= cluster_voting_disk_read_raw_tail_slot(fds[i], images[i]);

		if (state == CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF) {
			valid[i] = true;
			continue;
		}
		if (state != CLUSTER_VOTING_DISK_RAW_READ_FULL)
			continue;
		if (qvotec_pgsa_bytes_are_zero(images[i])) {
			valid[i] = true;
			continue;
		}
		if (cluster_semantic_activation_record_decode(
				images[i], &records[i], NULL)) {
			valid[i] = true;
			nonzero[i] = true;
		}
	}

	for (i = 0; i < n_disks; i++) {
		int identical = 0;
		int j;

		if (!valid[i])
			continue;
		for (j = 0; j < n_disks; j++) {
			if (valid[j]
				&& memcmp(images[i], images[j], sizeof(images[i])) == 0)
				identical++;
		}
		if (identical >= majority) {
			selected = i;
			break;
		}
	}
	if (selected >= 0) {
		memcpy(selected_bytes, images[selected],
			   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
		*implicit_open = !nonzero[selected];
		return CLUSTER_SEMANTIC_ACTIVATION_OK;
	}

	for (i = 0; i < n_disks; i++) {
		int j;

		if (!valid[i] || !nonzero[i])
			continue;
		for (j = i + 1; j < n_disks; j++) {
			if (valid[j] && nonzero[j]
				&& records[i].record_generation
					   == records[j].record_generation
				&& memcmp(images[i], images[j], sizeof(images[i])) != 0)
				return CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT;
		}
	}
	return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
}

/*
 * cluster_qvotec_bootstrap_read_semantic_activation -- RF-ROOT P9 verification
 *	closure (verified implementation): read-only startup interface.  Opens the
 *	configured voting disks itself (the qvotec process owns the long-lived
 *	fds; the startup process / recovery path cannot rely on them), runs the
 *	existing strict-majority selector over the R4 semantic-activation
 *	record tail slots, closes every fd, and writes nothing.  `implicit_open`
 *	reports whether the selected image is the all-zero implicit-OPEN sentinel
 *	(the pre-R4 boot state).
 */
ClusterSemanticActivationResult
cluster_qvotec_bootstrap_read_semantic_activation(
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open)
{
	const char *csv = cluster_voting_disks;
	const char *p;
	int fds[CLUSTER_MAX_VOTING_DISKS];
	int n_disks = 0;
	ClusterSemanticActivationResult result;
	int i;

	if (selected == NULL || implicit_open == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	*implicit_open = false;
	for (i = 0; i < CLUSTER_MAX_VOTING_DISKS; i++)
		fds[i] = -1;
	if (csv == NULL || csv[0] == '\0')
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

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
		if (len >= MAXPGPATH || n_disks >= CLUSTER_MAX_VOTING_DISKS) {
			result = CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
			goto cleanup;
		}
		memcpy(path, start, len);
		path[len] = '\0';
		/* Read-only open: the startup-process restore must not touch the
		 * disks with R/W semantics (observed: O_RDWR opens of the voting
		 * disks from the StartupProcess recovery path perturb the 2-node
		 * formation window in t/243). */
		fd = open(path, O_RDONLY, S_IRUSR | S_IWUSR);
		if (fd < 0) {
			result = CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
			goto cleanup;
		}
		fds[n_disks++] = fd;
	}
	if (n_disks <= 0) {
		result = CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
		goto cleanup;
	}
	result = qvotec_semantic_activation_record_read_fds(
		fds, n_disks, selected, implicit_open);
cleanup:
	for (i = 0; i < n_disks; i++)
		if (fds[i] >= 0)
			cluster_voting_disk_close(fds[i]);
	return result;
}

static ClusterSemanticActivationResult
qvotec_semantic_activation_record_cas_write_fds(
	const int *fds, int n_disks, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	uint8 current_images[CLUSTER_MAX_VOTING_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool current_valid[CLUSTER_MAX_VOTING_DISKS];
	ClusterSemanticActivationRecord desired_record;
	ClusterSemanticActivationRecord current_record;
	uint64 current_source_feature_bitmap;
	int selected = -1;
	int majority;
	int i;

	if (desired_bytes == NULL
		|| !cluster_semantic_activation_record_decode(desired_bytes, &desired_record, NULL)
		|| expected_generation == UINT64_MAX
		|| desired_record.record_generation != expected_generation + 1
		|| desired_record.source_feature_bitmap != expected_source_feature_bitmap)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	if (fds == NULL || n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	memset(current_images, 0, sizeof(current_images));
	memset(current_valid, 0, sizeof(current_valid));
	majority = n_disks / 2 + 1;
	for (i = 0; i < n_disks; i++) {
		ClusterVotingDiskRawReadState read_state
			= cluster_voting_disk_read_raw_tail_slot(fds[i], current_images[i]);

		if (read_state == CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF) {
			memset(current_images[i], 0, sizeof(current_images[i]));
			current_valid[i] = true;
		} else if (read_state == CLUSTER_VOTING_DISK_RAW_READ_FULL
				   && (qvotec_pgsa_bytes_are_zero(current_images[i])
					   || cluster_semantic_activation_record_decode(current_images[i],
															   &current_record, NULL)))
			current_valid[i] = true;
	}

	for (i = 0; i < n_disks; i++) {
		int identical = 0;
		int j;

		if (!current_valid[i])
			continue;
		for (j = 0; j < n_disks; j++) {
			if (current_valid[j]
				&& memcmp(current_images[i], current_images[j],
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   == 0)
				identical++;
		}
		if (identical >= majority) {
			selected = i;
			break;
		}
	}
	if (selected < 0)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	if (memcmp(current_images[selected], desired_bytes,
			   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
		== 0)
		return CLUSTER_SEMANTIC_ACTIVATION_OK;

	memset(&current_record, 0, sizeof(current_record));
	if (!qvotec_pgsa_bytes_are_zero(current_images[selected])
		&& !cluster_semantic_activation_record_decode(current_images[selected], &current_record,
														  NULL))
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	if (qvotec_pgsa_bytes_are_zero(current_images[selected]))
		current_source_feature_bitmap = 0;
	else {
		switch (current_record.phase) {
			case CLUSTER_SEMANTIC_PHASE_PREPARE:
			case CLUSTER_SEMANTIC_PHASE_COMMIT:
				current_source_feature_bitmap = current_record.source_feature_bitmap;
				break;
			case CLUSTER_SEMANTIC_PHASE_OPEN:
			case CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE:
				current_source_feature_bitmap = current_record.target_feature_bitmap;
				break;
			default:
				return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
		}
	}
	if (current_record.record_generation != expected_generation
		|| current_source_feature_bitmap != expected_source_feature_bitmap)
		return CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT;

	for (i = 0; i < n_disks; i++)
		(void) cluster_voting_disk_write_raw_tail_slot(fds[i], desired_bytes);

	{
		int desired_count = 0;

		for (i = 0; i < n_disks; i++) {
			uint8 reread[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
			ClusterVotingDiskRawReadState read_state;

			read_state
				= cluster_voting_disk_read_raw_tail_slot(fds[i], reread);
			if (read_state == CLUSTER_VOTING_DISK_RAW_READ_FULL
				&& memcmp(reread, desired_bytes, sizeof(reread)) == 0)
				desired_count++;
		}
		return desired_count >= majority ? CLUSTER_SEMANTIC_ACTIVATION_OK
										   : CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	}
}

ClusterSemanticActivationResult
cluster_semantic_activation_record_cas_write(
	uint64 expected_generation, uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	return qvotec_semantic_activation_record_cas_write_fds(
		qvotec_fds, qvotec_n_disks, expected_generation, expected_source_feature_bitmap,
		desired_bytes);
}

/* A join-marker disk counts only when this poll both durably wrote that disk
 * and read back the exact 512-byte staged image from the same region-3 slot.
 * Keeping the per-disk intersection here prevents two different majorities
 * (write success on A/B, stale exact bytes on B/C) from forming a false ACK. */
static bool
qvotec_join_marker_ack_proven_fds(
	const int *fds, int n_disks, int32 target_node,
	const uint8 staged_slot[CLUSTER_VOTING_SLOT_BYTES],
	const bool write_succeeded[CLUSTER_MAX_VOTING_DISKS])
{
	uint32 exact = 0;
	uint32 majority;
	int i;

	if (fds == NULL || staged_slot == NULL || write_succeeded == NULL
		|| n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return false;

	majority = ((uint32)n_disks / 2u) + 1u;
	for (i = 0; i < n_disks; i++) {
		uint8 reread[CLUSTER_VOTING_SLOT_BYTES];

		if (!write_succeeded[i])
			continue;
		memset(reread, 0, sizeof(reread));
		if (cluster_voting_disk_read_join_slot(
				fds[i], (uint32)target_node, reread)
				== CLUSTER_VOTING_DISK_IO_OK
			&& memcmp(reread, staged_slot, sizeof(reread)) == 0)
			exact++;
	}
	return exact >= majority;
}

/* VERIFY_COMMITTED_CLOSED is a read-only configured-total quorum scan.  A
 * failed disk remains a zero image in the fixed denominator; only one exact,
 * canonical JCMK v3 COMMITTED_CLOSED image may win. */
static bool
qvotec_join_marker_verify_committed_closed_fds(
	const int *fds, int n_disks, int32 target_node,
	uint8 verified_image96[CLUSTER_JCMK_REPLACEMENT_BYTES])
{
	uint8 images[CLUSTER_MAX_VOTING_DISKS][CLUSTER_JCMK_REPLACEMENT_BYTES];
	ClusterReplacementCommitMarkerV3 winner;
	uint8 canonical[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint32 majority;
	int selected;
	int i;

	if (verified_image96 == NULL)
		return false;
	memset(verified_image96, 0, CLUSTER_JCMK_REPLACEMENT_BYTES);
	if (fds == NULL || n_disks <= 0
		|| n_disks > CLUSTER_MAX_VOTING_DISKS
		|| target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return false;

	memset(images, 0, sizeof(images));
	for (i = 0; i < n_disks; i++) {
		uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
		uint64 incarnation_floor;

		memset(slot, 0, sizeof(slot));
		if (cluster_voting_disk_read_join_slot(
				fds[i], (uint32)target_node, slot)
				== CLUSTER_VOTING_DISK_IO_OK
			&& cluster_replacement_marker_v3_is_committed_closed_basis(
				slot, target_node, &incarnation_floor))
			memcpy(images[i], slot, CLUSTER_JCMK_REPLACEMENT_BYTES);
	}

	majority = ((uint32)n_disks / 2u) + 1u;
	selected = cluster_replacement_marker_v3_select_majority(
		images, n_disks, majority, target_node, &winner, NULL);
	if (selected < 0
		|| !cluster_replacement_marker_v3_encode(&winner, canonical)
		|| memcmp(canonical, images[selected], sizeof(canonical)) != 0)
		return false;
	memcpy(verified_image96, canonical, sizeof(canonical));
	return true;
}

static bool qvotec_epoch_ballot_phase1_promise_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	int32 proposer_node_id, const ClusterEpochBallotId *ballot,
	uint8 *out_completed_disk_bitmap) pg_attribute_unused();

static bool
qvotec_epoch_ballot_component_monotone(
	const ClusterEpochBallotId *older_ballot,
	const ClusterEpochAuthorityValue *older_value,
	const ClusterEpochBallotId *newer_ballot,
	const ClusterEpochAuthorityValue *newer_value)
{
	bool older_zero = qvotec_mailbox_bytes_are_zero(
		(const uint8 *)older_ballot, sizeof(*older_ballot));
	bool newer_zero = qvotec_mailbox_bytes_are_zero(
		(const uint8 *)newer_ballot, sizeof(*newer_ballot));
	int cmp;

	if (older_zero)
		return true;
	if (newer_zero)
		return false;
	cmp = cluster_epoch_ballot_id_compare(older_ballot, newer_ballot);
	return cmp < 0
		   || (cmp == 0
			   && memcmp(older_value, newer_value, sizeof(*older_value)) == 0);
}

static bool
qvotec_epoch_ballot_lane_monotone(
	const ClusterEpochBallotLane *older,
	const ClusterEpochBallotLane *newer)
{
	return older->lane_generation < newer->lane_generation
		   && cluster_epoch_ballot_id_compare(
				  &older->promised_ballot, &newer->promised_ballot) <= 0
		   && qvotec_epoch_ballot_component_monotone(
				  &older->accepted_ballot, &older->accepted_value,
				  &newer->accepted_ballot, &newer->accepted_value)
		   && qvotec_epoch_ballot_component_monotone(
				  &older->settled_ballot, &older->settled_value,
				  &newer->settled_ballot, &newer->settled_value);
}

/* Execute only the durable PHASE-1 primitive from spec-5.15A §2.1A.2.
 * This does not publish a mailbox completion: false may still mean that this
 * ballot reached durable disks before a later all-lane scan observed a higher
 * promise, so the owning actor must RECOVER before minting its retry. */
static bool
qvotec_epoch_ballot_phase1_promise_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	int32 proposer_node_id, const ClusterEpochBallotId *ballot,
	uint8 *out_completed_disk_bitmap)
{
	ClusterEpochBallotLane base;
	ClusterEpochBallotLane promised;
	ClusterEpochBallotId max_observed_promise;
	uint8 promised_image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];
	uint8 completed_disk_bitmap = 0;
	uint32 completed_disks = 0;
	uint32 majority;
	bool have_base = false;
	bool have_max_observed_promise = false;
	int proposer;
	int d;

	if (out_completed_disk_bitmap != NULL)
		*out_completed_disk_bitmap = 0;
	if (fds == NULL || admitted_incarnations == NULL
		|| out_completed_disk_bitmap == NULL || ballot == NULL
		|| n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| (n_disks != 1 && n_disks != 3 && n_disks != 5 && n_disks != 7)
		|| system_identifier == 0 || proposer_node_id < 0
		|| proposer_node_id >= CLUSTER_MAX_NODES
		|| admitted_incarnations[proposer_node_id] == 0
		|| !cluster_epoch_ballot_id_is_valid(ballot)
		|| ballot->proposer_node_id != proposer_node_id
		|| ballot->proposer_admitted_incarnation
			   != admitted_incarnations[proposer_node_id])
		return false;

	memset(&base, 0, sizeof(base));
	memset(&max_observed_promise, 0, sizeof(max_observed_promise));
	majority = ((uint32)n_disks / 2u) + 1u;

	/* The pre-write scan prevents a known higher promise from being hidden by
	 * this proposer.  All-zero sectors are the only legal unwritten lanes. */
	for (proposer = 0; proposer < CLUSTER_MAX_NODES; proposer++) {
		ClusterEpochBallotLane lanes[CLUSTER_MAX_VOTING_DISKS];
		bool valid[CLUSTER_MAX_VOTING_DISKS] = { false };
		int other;

		memset(lanes, 0, sizeof(lanes));
		for (d = 0; d < n_disks; d++) {
			uint8 image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

			if (cluster_voting_disk_read_epoch_ballot_slot(
					fds[d], (uint32)proposer, image)
					!= CLUSTER_VOTING_DISK_IO_OK)
				return false;
			if (qvotec_mailbox_bytes_are_zero(image, sizeof(image)))
				continue;
			if (admitted_incarnations[proposer] == 0
				|| !cluster_epoch_ballot_lane_decode(
					image, proposer, (uint32)n_disks,
					admitted_incarnations[proposer], system_identifier,
					CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
					&lanes[d]))
				return false;
			valid[d] = true;
		}

		for (d = 0; d < n_disks; d++) {
			if (!valid[d])
				continue;
			for (other = d + 1; other < n_disks; other++) {
				const ClusterEpochBallotLane *older;
				const ClusterEpochBallotLane *newer;

				if (!valid[other])
					continue;
				if (lanes[d].lane_generation
						== lanes[other].lane_generation) {
					if (memcmp(&lanes[d], &lanes[other],
							   sizeof(lanes[d])) != 0)
						return false;
					continue;
				}
				older = lanes[d].lane_generation
							 < lanes[other].lane_generation
						 ? &lanes[d] : &lanes[other];
				newer = older == &lanes[d] ? &lanes[other] : &lanes[d];
				if (!qvotec_epoch_ballot_lane_monotone(older, newer))
					return false;
			}
			if (!have_max_observed_promise
				|| cluster_epoch_ballot_id_compare(
					   &lanes[d].promised_ballot,
					   &max_observed_promise) > 0) {
				max_observed_promise = lanes[d].promised_ballot;
				have_max_observed_promise = true;
			}
			if (proposer == proposer_node_id
				&& (!have_base
					|| lanes[d].lane_generation > base.lane_generation)) {
				base = lanes[d];
				have_base = true;
			}
		}
	}

	if (have_max_observed_promise
		&& cluster_epoch_ballot_id_compare(
			   ballot, &max_observed_promise) <= 0)
		return false;

	if (have_base) {
		if (base.lane_generation == UINT64_MAX)
			return false;
		promised = base;
		promised.lane_generation++;
	} else {
		memset(&promised, 0, sizeof(promised));
		promised.magic = CLUSTER_EPOCH_BALLOT_MAGIC;
		promised.version = CLUSTER_EPOCH_BALLOT_VERSION;
		promised.proposer_node_id = proposer_node_id;
		promised.configured_disk_count = (uint32)n_disks;
		promised.proposer_admitted_incarnation
			= admitted_incarnations[proposer_node_id];
		promised.lane_generation = 1;
		promised.system_identifier = system_identifier;
		promised.grammar_fingerprint
			= CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	}
	promised.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_PROMISED;
	promised.promised_ballot = *ballot;
	promised.crc32c = 0;
	if (!cluster_epoch_ballot_lane_encode(
			&promised, proposer_node_id, (uint32)n_disks,
			admitted_incarnations[proposer_node_id], system_identifier,
			CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, promised_image))
		return false;

	for (d = 0; d < n_disks; d++) {
		uint8 reread[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

		if (cluster_voting_disk_write_epoch_ballot_slot(
				fds[d], (uint32)proposer_node_id, promised_image)
				== CLUSTER_VOTING_DISK_IO_OK
			&& cluster_voting_disk_read_epoch_ballot_slot(
				fds[d], (uint32)proposer_node_id, reread)
				== CLUSTER_VOTING_DISK_IO_OK
			&& memcmp(reread, promised_image, sizeof(reread)) == 0) {
			completed_disk_bitmap |= (uint8)(1u << d);
			completed_disks++;
		}
	}
	if (completed_disks < majority)
		return false;

	/* A phase-1 quorum is usable only after every proposer lane is rescanned
	 * on those exact completed disks and none contains a higher promise. */
	for (d = 0; d < n_disks; d++) {
		if ((completed_disk_bitmap & (uint8)(1u << d)) == 0)
			continue;
		for (proposer = 0; proposer < CLUSTER_MAX_NODES; proposer++) {
			ClusterEpochBallotLane lane;
			uint8 image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

			if (cluster_voting_disk_read_epoch_ballot_slot(
					fds[d], (uint32)proposer, image)
					!= CLUSTER_VOTING_DISK_IO_OK)
				return false;
			if (qvotec_mailbox_bytes_are_zero(image, sizeof(image)))
				continue;
			if (admitted_incarnations[proposer] == 0
				|| !cluster_epoch_ballot_lane_decode(
					image, proposer, (uint32)n_disks,
					admitted_incarnations[proposer], system_identifier,
					CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &lane)
				|| cluster_epoch_ballot_id_compare(
					   &lane.promised_ballot, ballot) > 0)
				return false;
		}
	}

	*out_completed_disk_bitmap = completed_disk_bitmap;
	return true;
}

/* Recover only strict-majority ballot evidence.  A lone or split sector is
 * never promoted to a settled head or accepted invalidator.  The second scan
 * is deliberate: the settled head must be known before accepted generations
 * can be classified as historical, next, or corruptly skipped. */
static ClusterQvotecMailboxResult
qvotec_epoch_ballot_recover_head_fds(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	uint8 out_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint8 out_ballot[CLUSTER_QVOTEC_BALLOT_BYTES],
	uint8 *out_observed_disk_bitmap)
{
	ClusterEpochAuthorityValue settled_value;
	ClusterEpochBallotId settled_ballot;
	ClusterEpochAuthorityValue accepted_value;
	ClusterEpochBallotId accepted_ballot;
	uint8 settled_disk_bitmap = 0;
	uint8 accepted_disk_bitmap = 0;
	uint32 majority;
	bool have_settled = false;
	bool have_accepted = false;
	int proposer;

	if (out_value != NULL)
		memset(out_value, 0, CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES);
	if (out_ballot != NULL)
		memset(out_ballot, 0, CLUSTER_QVOTEC_BALLOT_BYTES);
	if (out_observed_disk_bitmap != NULL)
		*out_observed_disk_bitmap = 0;
	if (fds == NULL || admitted_incarnations == NULL || out_value == NULL
		|| out_ballot == NULL || out_observed_disk_bitmap == NULL
		|| n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| system_identifier == 0)
		return CLUSTER_QVOTEC_MAILBOX_HOLD;

	memset(&settled_value, 0, sizeof(settled_value));
	memset(&settled_ballot, 0, sizeof(settled_ballot));
	memset(&accepted_value, 0, sizeof(accepted_value));
	memset(&accepted_ballot, 0, sizeof(accepted_ballot));
	majority = ((uint32)n_disks / 2u) + 1u;

	for (proposer = 0; proposer < CLUSTER_MAX_NODES; proposer++) {
		ClusterEpochBallotLane lanes[CLUSTER_MAX_VOTING_DISKS];
		bool valid[CLUSTER_MAX_VOTING_DISKS] = { false };
		int d;

		if (admitted_incarnations[proposer] == 0)
			continue;
		memset(lanes, 0, sizeof(lanes));
		for (d = 0; d < n_disks; d++) {
			uint8 image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

			if (cluster_voting_disk_read_epoch_ballot_slot(
					fds[d], (uint32)proposer, image)
					== CLUSTER_VOTING_DISK_IO_OK
				&& cluster_epoch_ballot_lane_decode(
					image, proposer, (uint32)n_disks,
					admitted_incarnations[proposer], system_identifier,
					CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
					&lanes[d]))
				valid[d] = true;
		}

		for (d = 0; d < n_disks; d++) {
			uint8 agreeing = 0;
			uint32 count = 0;
			int other;

			if (!valid[d] || lanes[d].settled_ballot.counter == 0)
				continue;
			for (other = 0; other < n_disks; other++) {
				if (valid[other]
					&& memcmp(&lanes[d].settled_ballot,
							  &lanes[other].settled_ballot,
							  sizeof(lanes[d].settled_ballot))
						   == 0
					&& memcmp(&lanes[d].settled_value,
							  &lanes[other].settled_value,
							  sizeof(lanes[d].settled_value))
						   == 0) {
					count++;
					agreeing |= (uint8)(1u << other);
				}
			}
			if (count < majority)
				continue;
			if (!have_settled
				|| lanes[d].settled_value.authority_generation
					   > settled_value.authority_generation) {
				settled_value = lanes[d].settled_value;
				settled_ballot = lanes[d].settled_ballot;
				settled_disk_bitmap = agreeing;
				have_settled = true;
			} else if (lanes[d].settled_value.authority_generation
					   == settled_value.authority_generation) {
				if (memcmp(&lanes[d].settled_value, &settled_value,
						   sizeof(settled_value)) != 0)
					return CLUSTER_QVOTEC_MAILBOX_HOLD;
				if (cluster_epoch_ballot_id_compare(
						&lanes[d].settled_ballot, &settled_ballot) > 0) {
					settled_ballot = lanes[d].settled_ballot;
					settled_disk_bitmap = agreeing;
				}
			}
		}
	}

	if (!have_settled || settled_value.authority_generation == UINT64_MAX)
		return CLUSTER_QVOTEC_MAILBOX_HOLD;

	for (proposer = 0; proposer < CLUSTER_MAX_NODES; proposer++) {
		ClusterEpochBallotLane lanes[CLUSTER_MAX_VOTING_DISKS];
		bool valid[CLUSTER_MAX_VOTING_DISKS] = { false };
		int d;

		if (admitted_incarnations[proposer] == 0)
			continue;
		memset(lanes, 0, sizeof(lanes));
		for (d = 0; d < n_disks; d++) {
			uint8 image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

			if (cluster_voting_disk_read_epoch_ballot_slot(
					fds[d], (uint32)proposer, image)
					== CLUSTER_VOTING_DISK_IO_OK
				&& cluster_epoch_ballot_lane_decode(
					image, proposer, (uint32)n_disks,
					admitted_incarnations[proposer], system_identifier,
					CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
					&lanes[d]))
				valid[d] = true;
		}
		for (d = 0; d < n_disks; d++) {
			uint8 agreeing = 0;
			uint32 count = 0;
			int other;

			if (!valid[d] || lanes[d].accepted_ballot.counter == 0)
				continue;
			for (other = 0; other < n_disks; other++) {
				if (valid[other]
					&& memcmp(&lanes[d].accepted_ballot,
							  &lanes[other].accepted_ballot,
							  sizeof(lanes[d].accepted_ballot))
						   == 0
					&& memcmp(&lanes[d].accepted_value,
							  &lanes[other].accepted_value,
							  sizeof(lanes[d].accepted_value))
						   == 0) {
					count++;
					agreeing |= (uint8)(1u << other);
				}
			}
			if (count < majority)
				continue;
			if (lanes[d].accepted_value.authority_generation
					> settled_value.authority_generation + 1)
				return CLUSTER_QVOTEC_MAILBOX_HOLD;
			if (lanes[d].accepted_value.authority_generation
					== settled_value.authority_generation
				&& memcmp(&lanes[d].accepted_value, &settled_value,
						  sizeof(settled_value)) != 0)
				return CLUSTER_QVOTEC_MAILBOX_HOLD;
			if (lanes[d].accepted_value.authority_generation
					!= settled_value.authority_generation + 1)
				continue;
			if (!have_accepted
				|| cluster_epoch_ballot_id_compare(
					   &lanes[d].accepted_ballot, &accepted_ballot) > 0) {
				accepted_value = lanes[d].accepted_value;
				accepted_ballot = lanes[d].accepted_ballot;
				accepted_disk_bitmap = agreeing;
				have_accepted = true;
			} else if (cluster_epoch_ballot_id_compare(
						   &lanes[d].accepted_ballot, &accepted_ballot) == 0
					   && memcmp(&lanes[d].accepted_value, &accepted_value,
							 sizeof(accepted_value)) != 0)
				return CLUSTER_QVOTEC_MAILBOX_HOLD;
		}
	}

	if (have_accepted) {
		if (!cluster_epoch_authority_value_encode(
				&accepted_value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
				out_value)
			|| !cluster_epoch_ballot_id_encode(&accepted_ballot, out_ballot))
			return CLUSTER_QVOTEC_MAILBOX_HOLD;
		*out_observed_disk_bitmap = accepted_disk_bitmap;
		return CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER;
	}

	if (!cluster_epoch_authority_value_encode(
			&settled_value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
			out_value)
		|| !cluster_epoch_ballot_id_encode(&settled_ballot, out_ballot))
		return CLUSTER_QVOTEC_MAILBOX_HOLD;
	*out_observed_disk_bitmap = settled_disk_bitmap;
	return CLUSTER_QVOTEC_MAILBOX_CHOSEN;
}

/*
 * spec-5.15A §2.1A.3: a common epoch-ballot formation is authoritative only
 * when every configured member of the fixed odd disk set is a sufficiently
 * large sector-aligned Linux block device opened with effective O_DIRECT.
 * Regular files remain usable by the codec/unit-test helpers below, but can
 * never authorize the live mailbox actor.
 */
static bool
qvotec_epoch_ballot_formation_attested_fds(const int *fds, int n_disks)
{
	int i;

	if (fds == NULL
		|| (n_disks != 1 && n_disks != 3 && n_disks != 5 && n_disks != 7))
		return false;
	for (i = 0; i < n_disks; i++) {
		if (!cluster_voting_disk_epoch_ballot_authority_attest(fds[i]))
			return false;
	}
	return true;
}

static void
qvotec_epoch_ballot_mailbox_tick(void)
{
	ClusterQvotecMailboxRequest request;
	ClusterQvotecMailboxCompletion completion;
	uint64 admitted_incarnations[CLUSTER_MAX_NODES];
	uint8 configured_disk_bitmap;
	int i;

	if (QvotecShmem == NULL
		|| !cluster_qvotec_mailbox_qvotec_poll(
			&QvotecShmem->mailbox, &request))
		return;
	memset(&completion, 0, sizeof(completion));
	completion.request_seq = request.request_seq;
	completion.actor_phase = CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_A;
	if (!qvotec_epoch_ballot_formation_attested_fds(
			qvotec_fds, qvotec_n_disks)
		|| request.opcode != CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD) {
		completion.result = CLUSTER_QVOTEC_MAILBOX_HOLD;
		completion.actor_phase = CLUSTER_QVOTEC_ACTOR_HOLD;
	} else {
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			admitted_incarnations[i]
				= cluster_membership_get_last_admitted_incarnation(i);
		completion.result = qvotec_epoch_ballot_recover_head_fds(
			qvotec_fds, qvotec_n_disks, GetSystemIdentifier(),
			admitted_incarnations, completion.completion_value,
			completion.completion_ballot,
			&completion.observed_disk_bitmap);
		completion.actor_phase
			= completion.result == CLUSTER_QVOTEC_MAILBOX_HOLD
				  ? CLUSTER_QVOTEC_ACTOR_HOLD
				  : CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;
	}
	configured_disk_bitmap
		= (uint8)((UINT32_C(1) << qvotec_n_disks) - UINT32_C(1));
	(void)cluster_qvotec_mailbox_qvotec_complete(
		&QvotecShmem->mailbox, configured_disk_bitmap, &completion);
}

#ifdef CLUSTER_QVOTEC_PGSA_UNIT_TEST
extern ClusterSemanticActivationResult cluster_qvotec_test_semantic_activation_record_cas_write(
	const int *fds, int n_disks, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES]);
extern ClusterSemanticActivationResult cluster_qvotec_test_semantic_activation_record_read(
	const int *fds, int n_disks,
	uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open);
extern bool cluster_qvotec_test_join_marker_ack_proven(
	const int *fds, int n_disks, int32 target_node,
	const uint8 *staged_slot, uint32 writes_ok);
extern bool cluster_qvotec_test_join_marker_verify_committed_closed(
	const int *fds, int n_disks, int32 target_node,
	uint8 verified_image96[CLUSTER_JCMK_REPLACEMENT_BYTES]);
extern ClusterQvotecMailboxResult cluster_qvotec_test_epoch_ballot_recover_head(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	uint8 out_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint8 out_ballot[CLUSTER_QVOTEC_BALLOT_BYTES],
	uint8 *out_observed_disk_bitmap);
extern bool cluster_qvotec_test_epoch_ballot_phase1_promise(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	int32 proposer_node_id, const ClusterEpochBallotId *ballot,
	uint8 *out_completed_disk_bitmap);
extern bool cluster_qvotec_test_epoch_ballot_formation_attested(
	const int *fds, int n_disks);
extern bool cluster_qvotec_test_undo_root_descriptor_formation_attested(
	const int *fds, int n_disks);
extern bool cluster_qvotec_test_undo_root_descriptor_provision(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	uint8 *out_completed_disk_bitmap);
extern ClusterUndoRootDescriptorState
cluster_qvotec_test_undo_root_descriptor_read(
	const int *fds, int n_disks, uint64 system_identifier,
	uint8 root_kind, int32 owner_node, ClusterUndoRootDescriptorV1 *out,
	uint8 *out_observed_disk_bitmap);
extern ClusterReplacementRequestSlotState
cluster_qvotec_test_replacement_request_preserve(
	ClusterVotingSlot *next, const ClusterVotingSlot *prior);
extern long cluster_qvotec_test_poll_wait_timeout_ms(
	uint64 elapsed_us, int poll_interval_ms);

ClusterSemanticActivationResult
cluster_qvotec_test_semantic_activation_record_cas_write(
	const int *fds, int n_disks, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	return qvotec_semantic_activation_record_cas_write_fds(
		fds, n_disks, expected_generation, expected_source_feature_bitmap, desired_bytes);
}

ClusterSemanticActivationResult
cluster_qvotec_test_semantic_activation_record_read(
	const int *fds, int n_disks,
	uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open)
{
	return qvotec_semantic_activation_record_read_fds(
		fds, n_disks, selected_bytes, implicit_open);
}

bool
cluster_qvotec_test_join_marker_ack_proven(
	const int *fds, int n_disks, int32 target_node,
	const uint8 *staged_slot, uint32 writes_ok)
{
	bool write_succeeded[CLUSTER_MAX_VOTING_DISKS] = { false };
	uint32 bounded_writes;
	uint32 i;

	if (n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS)
		return false;
	bounded_writes = Min(writes_ok, (uint32)n_disks);
	for (i = 0; i < bounded_writes; i++)
		write_succeeded[i] = true;
	return qvotec_join_marker_ack_proven_fds(
		fds, n_disks, target_node, staged_slot, write_succeeded);
}

bool
cluster_qvotec_test_join_marker_verify_committed_closed(
	const int *fds, int n_disks, int32 target_node,
	uint8 verified_image96[CLUSTER_JCMK_REPLACEMENT_BYTES])
{
	return qvotec_join_marker_verify_committed_closed_fds(
		fds, n_disks, target_node, verified_image96);
}

ClusterQvotecMailboxResult
cluster_qvotec_test_epoch_ballot_recover_head(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	uint8 out_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint8 out_ballot[CLUSTER_QVOTEC_BALLOT_BYTES],
	uint8 *out_observed_disk_bitmap)
{
	return qvotec_epoch_ballot_recover_head_fds(
		fds, n_disks, system_identifier, admitted_incarnations,
		out_value, out_ballot, out_observed_disk_bitmap);
}

bool
cluster_qvotec_test_epoch_ballot_phase1_promise(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	int32 proposer_node_id, const ClusterEpochBallotId *ballot,
	uint8 *out_completed_disk_bitmap)
{
	return qvotec_epoch_ballot_phase1_promise_fds(
		fds, n_disks, system_identifier, admitted_incarnations,
		proposer_node_id, ballot, out_completed_disk_bitmap);
}

bool
cluster_qvotec_test_epoch_ballot_formation_attested(
	const int *fds, int n_disks)
{
	return qvotec_epoch_ballot_formation_attested_fds(fds, n_disks);
}

bool
cluster_qvotec_test_undo_root_descriptor_formation_attested(
	const int *fds, int n_disks)
{
	return qvotec_undo_root_descriptor_formation_attested_fds(
		fds, n_disks);
}

bool
cluster_qvotec_test_undo_root_descriptor_provision(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	uint8 *out_completed_disk_bitmap)
{
	return qvotec_undo_root_descriptor_provision_fds(
		fds, n_disks, system_identifier, desired,
		out_completed_disk_bitmap);
}

ClusterUndoRootDescriptorState
cluster_qvotec_test_undo_root_descriptor_read(
	const int *fds, int n_disks, uint64 system_identifier,
	uint8 root_kind, int32 owner_node, ClusterUndoRootDescriptorV1 *out,
	uint8 *out_observed_disk_bitmap)
{
	return qvotec_undo_root_descriptor_read_fds(
		fds, n_disks, system_identifier, root_kind, owner_node, out,
		out_observed_disk_bitmap);
}
#endif

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
 * Preserve a replacement request only from this disk's prior self-slot.  The
 * marker identity is inseparable from the outer slot node/incarnation and bit
 * 2; any partial or stale combination is HOLD and the caller must not write
 * over that disk.
 */
static ClusterReplacementRequestSlotState
qvotec_replacement_request_preserve(ClusterVotingSlot *next,
									const ClusterVotingSlot *prior)
{
	ClusterReplacementRequestSlotState state;

	if (next == NULL || prior == NULL)
		return CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	state = cluster_replacement_request_slot_state(
		prior->flags, prior->_reserved1, (int32)next->node_id,
		next->incarnation, NULL);
	if (state == CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID
		&& (prior->node_id != next->node_id
			|| prior->incarnation != next->incarnation))
		return CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	if (state != CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID)
		return state;
	return cluster_replacement_request_preserve_per_disk(
		prior->flags, prior->_reserved1, (int32)next->node_id,
		next->incarnation, &next->flags, next->_reserved1);
}

#ifdef CLUSTER_QVOTEC_PGSA_UNIT_TEST
void cluster_qvotec_test_poll_pre_injection(void);

void
cluster_qvotec_test_poll_pre_injection(void)
{
	qvotec_poll_pre_injection();
}

ClusterReplacementRequestSlotState
cluster_qvotec_test_replacement_request_preserve(
	ClusterVotingSlot *next, const ClusterVotingSlot *prior)
{
	return qvotec_replacement_request_preserve(next, prior);
}

long
cluster_qvotec_test_poll_wait_timeout_ms(uint64 elapsed_us,
									 int poll_interval_ms)
{
	return qvotec_poll_wait_timeout_ms(elapsed_us, poll_interval_ms);
}
#endif

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
		ClusterVotingSlot existing;
		ClusterVotingDiskIoState rrc;
		ClusterReplacementRequestSlotState rplm_state;

		/*
		 * spec-5.18 R12: the durable removal marker (§2.5) rides THIS slot's
		 * _reserved1[64..].  Clearing ALIVE is a LIVENESS signal — it must NOT
		 * erase the durable removal record, or a clean coordinator restart would
		 * lose the SHRUNK/REMOVED marker and fail the INV-LF7 crash-recovery.
		 * Carry the removal marker forward per-disk from this disk's current slot.
		 * (The 4.12 fence marker [0..64) keeps its existing clean-shutdown behaviour
		 * — cleared here, re-established by the cluster-wide baseline republish;
		 * the removal-marker recovery re-fences via the seeded removed_bitmap.)
		 */
		blanked.flags = 0;
		memset(blanked._reserved1, 0, sizeof(blanked._reserved1));
		rrc = cluster_voting_disk_read_slot(
			qvotec_fds[i], i, (uint32)cluster_node_id, &existing);
		if (rrc != CLUSTER_VOTING_DISK_IO_OK)
			continue;
		cluster_removal_marker_preserve_per_disk(
			blanked._reserved1, existing._reserved1);
		rplm_state = qvotec_replacement_request_preserve(&blanked, &existing);
		if (rplm_state == CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD)
			continue;

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

/*
 * qvotec_self_is_membership_leader (spec-4.12b D2, Q1=A) -- is THIS node the
 *	deterministic steady-state baseline author?  The author is the lowest live
 *	node_id in the current quorum view's alive_bitmap (no election); a leader
 *	change (a lower-id node joining, or this leader dying so the next-lowest takes
 *	over) is implicit in the alive_bitmap the caller passes.  A node that does not
 *	even see itself alive (its own bit unset) is never the leader -- the lowest
 *	live id then differs from cluster_node_id and this returns false (R4: a
 *	non-leader never authors its own per-disk minority baseline).
 */
static bool
qvotec_self_is_membership_leader(const uint8 *alive_bitmap)
{
	return cluster_write_fence_lowest_live_node(alive_bitmap) == (int32)cluster_node_id;
}

/*
 * qvotec_build_baseline_marker (spec-4.12b D2/D5(a), P0-3) -- build the
 *	steady-state baseline marker the leader republishes every poll.  The
 *	membership tuple (epoch, dead set, generation, event_id) is sourced ATOMICALLY
 *	from the locally-APPLIED ReconfigEvent (cluster_reconfig_get_last_event() is a
 *	shared-lock memcpy snapshot), NEVER from raw cluster_epoch_get_current() + a
 *	separately-read dead set: the reconfig coordinator bumps the epoch BEFORE it
 *	submits/publishes the fence marker (cluster_reconfig.c bump-before-publish
 *	window), so current_epoch can be AHEAD of the applied membership.  A leader
 *	that has not yet applied the newest reconfig therefore authors at its older
 *	applied epoch, which loses to the coordinator's higher-epoch fence marker in
 *	cluster_fence_authority_decide -- it can never MASK a real fence (8.A).
 *
 *	issuer = last_applied.coordinator_node_id (P1: the STABLE membership issuer,
 *	the SAME id the fence marker carried), so the baseline republish is
 *	tuple-identical to the fence marker for that membership -- one authoritative
 *	tuple per epoch, never a competing per-author tuple.  A pristine
 *	(never-reconfigured) membership uses the fixed sentinel issuer so every node's
 *	pristine baseline is identical.
 *
 *	cluster_epoch_get_current() is used ONLY as an upper-bound Assert (the applied
 *	epoch must never exceed the live epoch); it is never the source value.
 */
static void
qvotec_build_baseline_marker(ClusterFenceMarker *out)
{
	ReconfigEvent applied;
	uint8 fenced[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	bool any_removed;

	cluster_reconfig_get_last_event(&applied);

	/*
	 * spec-5.18 INV-LF10 (R13): when there ARE permanently-removed nodes, the
	 * steady-state fence baseline MUST keep every removed node in the fenced set,
	 * not just at the arm-instant — the fenced set is `applied.dead | removed`, so
	 * a removed node stays fenced across every later reconfig/baseline republish
	 * (a fresh/restart node's first-authority baseline never re-releases it,
	 * INV-LF8).  removed_bitmap only grows, so the superset guard is satisfied.
	 * Guarded on removed_count so a cluster that never removed a node builds a
	 * fence baseline byte-identical to pre-5.18 (zero extra lock/work).
	 */
	any_removed = (cluster_reconfig_get_removed_count() > 0);

	if (applied.event_id == 0) {
		/* pristine membership: initial epoch, removed-only fenced set, sentinel issuer. */
		memset(fenced, 0, sizeof(fenced));
		if (any_removed)
			cluster_reconfig_snapshot_removed_bitmap(fenced);
		cluster_fence_marker_build_baseline(out, CLUSTER_EPOCH_INITIAL, fenced, 0 /* generation */,
											0 /* event_id */,
											CLUSTER_FENCE_BASELINE_INITIAL_ISSUER);
	} else {
		/* applied reconfig: republish its membership tuple, unioned with removed. */
		memcpy(fenced, applied.dead_bitmap, sizeof(fenced));
		if (any_removed) {
			uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
			int b;

			cluster_reconfig_snapshot_removed_bitmap(removed_bitmap);
			for (b = 0; b < CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES; b++)
				fenced[b] |= removed_bitmap[b];
		}
		cluster_fence_marker_build_baseline(out, applied.new_epoch, fenced,
											applied.cssd_dead_generation, applied.event_id,
											applied.coordinator_node_id);
	}

	/* RF-ROOT P6 (STOP-01 contract): the clean-departed epoch
	 * floor is a durable membership fact (leave-slot COMMITTED markers) that
	 * survives restart, while the applied ReconfigEvent it names does not
	 * (volatile last_applied).  spec-5.13 fences nothing on a clean leave,
	 * so raising the baseline's fence_epoch to the floor with the applied
	 * dead set unchanged is the correct fence tuple;  without it the token
	 * stays at the pristine/applied epoch below the floor and every
	 * fence-gated write PANICs against the live epoch (observed: cast-leg
	 * second-boot W2 anchor PANIC, epoch_cur=1 authorized=0).  Monotone:
	 * the floor only rises via leave commits, which never fence a node;
	 * the durable-authority guard above still vetoes any regression. */
	{
		uint64 floor_epoch = out->fence_epoch;
		int f;

		for (f = 0; f < CLUSTER_MAX_NODES; f++) {
			uint64 departed_epoch
				= cluster_reconfig_get_clean_departed_epoch(f);

			if (departed_epoch > floor_epoch)
				floor_epoch = departed_epoch;
		}
		if (floor_epoch > out->fence_epoch)
			out->fence_epoch = floor_epoch;
	}

	/* P0-3 upper-bound: the applied epoch can never exceed the live epoch. */
	Assert(out->fence_epoch <= cluster_epoch_get_current());
}

static bool
qvotec_apply_lease_scan(ClusterAdgApplyMasterLeaseQuorum *out, int *disks_ok_out)
{
	ClusterAdgApplyMasterLease leases[CLUSTER_MAX_VOTING_DISKS];
	bool valid[CLUSTER_MAX_VOTING_DISKS];
	int disks_ok = 0;
	int quorum;

	if (out == NULL || qvotec_n_disks <= 0)
		return false;

	memset(leases, 0, sizeof(leases));
	memset(valid, 0, sizeof(valid));
	memset(out, 0, sizeof(*out));
	out->owner_node_id = -1;
	quorum = qvotec_n_disks / 2 + 1;

	for (int disk = 0; disk < qvotec_n_disks; disk++) {
		uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

		if (cluster_voting_disk_read_apply_lease_global_slot(qvotec_fds[disk], slot)
			!= CLUSTER_VOTING_DISK_IO_OK)
			continue;
		disks_ok++;
		valid[disk] = cluster_adg_apply_master_lease_unpack(slot, &leases[disk]);
	}

	if (disks_ok_out != NULL)
		*disks_ok_out = disks_ok;
	if (disks_ok < quorum)
		return false;
	return cluster_adg_apply_master_lease_quorum(leases, valid, qvotec_n_disks, quorum, out);
}

static bool
qvotec_apply_lease_same_winner(const ClusterAdgApplyMasterLeaseQuorum *winner,
							   const ClusterAdgApplyMasterLease *desired)
{
	return winner != NULL && desired != NULL && winner->attached
		   && winner->durable_term == desired->term
		   && winner->owner_node_id == desired->owner_node_id
		   && winner->generation == desired->generation
		   && winner->lease_epoch == desired->lease_epoch
		   && winner->owner_incarnation == desired->owner_incarnation
		   && winner->receive_lsn == desired->receive_lsn && winner->apply_lsn == desired->apply_lsn
		   && winner->standby_consistent_scn == desired->standby_consistent_scn;
}

static ClusterMrpApplyLeaseSubmitResult
qvotec_apply_lease_cas(const ClusterAdgApplyMasterLease *desired, const uint8 *alive_bitmap,
					   int alive_bitmap_bytes, ClusterAdgApplyMasterLeaseQuorum *winner)
{
	ClusterAdgApplyMasterLeaseQuorum current;
	ClusterAdgApplyMasterLeaseCasVerdict verdict;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int disks_ok = 0;
	int writes_ok = 0;
	int quorum;

	if (winner != NULL) {
		memset(winner, 0, sizeof(*winner));
		winner->owner_node_id = -1;
	}
	if (qvotec_n_disks <= 0)
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM;
	if (desired == NULL || !cluster_adg_apply_master_lease_valid(desired))
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID;
	if (cluster_adg_apply_master_candidate_node(alive_bitmap, alive_bitmap_bytes) < 0)
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM;
	if (!cluster_adg_apply_master_candidate_allows_owner(alive_bitmap, alive_bitmap_bytes,
														 desired->owner_node_id))
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_STALE;
	quorum = qvotec_n_disks / 2 + 1;

	if (!qvotec_apply_lease_scan(&current, &disks_ok) && disks_ok < quorum)
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM;

	verdict = cluster_adg_apply_master_lease_cas_verdict(&current, desired,
														 (int64)(GetCurrentTimestamp() / 1000),
														 cluster_adg_lease_takeover_grace_ms);
	switch (verdict) {
	case CLUSTER_ADG_APPLY_LEASE_CAS_RENEW:
	case CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EMPTY:
	case CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EXPIRED:
		break;
	case CLUSTER_ADG_APPLY_LEASE_CAS_INVALID:
		if (winner != NULL)
			*winner = current;
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID;
	case CLUSTER_ADG_APPLY_LEASE_CAS_STALE:
	default:
		if (winner != NULL)
			*winner = current;
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_STALE;
	}

	if (!cluster_adg_apply_master_lease_pack(slot, desired))
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID;

	for (int disk = 0; disk < qvotec_n_disks; disk++) {
		if (cluster_voting_disk_write_apply_lease_global_slot(qvotec_fds[disk], slot)
			== CLUSTER_VOTING_DISK_IO_OK)
			writes_ok++;
	}
	if (writes_ok < quorum)
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED;

	if (!qvotec_apply_lease_scan(winner, &disks_ok))
		return disks_ok < quorum ? CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM
								 : CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED;
	if (!qvotec_apply_lease_same_winner(winner, desired))
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED;
	return CLUSTER_MRP_APPLY_LEASE_SUBMIT_ACK;
}

static void
qvotec_poll_once(void)
{
	ClusterSemanticActivationCasRequest semantic_record_cas_request;
	ClusterSemanticActivationReadRequest semantic_record_read_request;
	ClusterUndoRootDescriptorReadRequest undo_root_descriptor_read_request;
	ClusterUndoRootDescriptorRequest undo_root_descriptor_request;
	ClusterVotingSlot self_slot;
	ClusterVotingDiskIoState io_states[CLUSTER_MAX_VOTING_DISKS];
	bool own_prior_read_ok[CLUSTER_MAX_VOTING_DISKS] = { false };
	ClusterQuorumDecision decision;
	uint64 now_us;
	uint64 next_lease_expire;
	uint64 heartbeat_timeout_us;
	int i;
	ClusterFenceMarker submit_marker;
	bool have_submit;
	uint8 leave_marker_slot[CLUSTER_VOTING_SLOT_BYTES]; /* spec-5.13 §2.5 staged marker */
	bool have_leave_submit;
	uint32 leave_disks_ok = 0;
	uint8 join_marker_slot[CLUSTER_VOTING_SLOT_BYTES]; /* spec-5.15 §2.6 staged marker */
	bool have_join_submit;
	ClusterJoinMarkerMailboxOperationV1 join_marker_operation
		= CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	int32 join_target_node = -1; /* region-3 slot to write (the joiner N) */
	bool join_disk_write_succeeded[CLUSTER_MAX_VOTING_DISKS] = { false };
	ClusterRemovalMarker removal_submit_marker; /* spec-5.18 §2.5 staged removal marker */
	bool have_removal_submit;
	/* RF-ROOT P9 verification (cold-formation cold-formation ruling):
	 * staged cold-formation marker submit (region 7) + self-slot observe. */
	ClusterFormationMarkerSubmitRequest formation_marker_request;
	bool have_formation_marker_submit;
	ClusterAdgApplyMasterLease apply_lease_request;
	ClusterAdgApplyMasterLeaseQuorum apply_lease_winner;
	bool have_apply_lease_request;
	ClusterFenceMarker baseline_marker; /* spec-4.12b D2 */
	bool author_baseline = false;		/* spec-4.12b D2: wrote a baseline this cycle */
	bool is_leader = false;				/* spec-4.12b D6: lowest-live baseline leader */
	bool durable_has_authority = false; /* exact authority observed on disk THIS poll */
	ClusterFenceMarker durable_authority_marker; /* exact tuple observed this poll */
	bool fence_majority_written = false; /* RF-ROOT P6: this poll's marker tuple
										 * reached quorum-majority durability */

	if (cluster_semantic_activation_qvotec_poll_record_read(
			&semantic_record_read_request)) {
		uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
		bool implicit_open = false;
		ClusterSemanticActivationResult result
			= qvotec_semantic_activation_record_read_fds(
				qvotec_fds, qvotec_n_disks, selected, &implicit_open);

		(void)cluster_semantic_activation_qvotec_complete_record_read(
			semantic_record_read_request.request_seq, result,
			implicit_open, selected);
	} else if (cluster_semantic_activation_qvotec_poll_record_cas(
				   &semantic_record_cas_request)) {
		ClusterSemanticActivationResult semantic_record_cas_result
			= cluster_semantic_activation_record_cas_write(
				semantic_record_cas_request.expected_generation,
				semantic_record_cas_request.expected_source_feature_bitmap,
				semantic_record_cas_request.desired_bytes);

		(void) cluster_semantic_activation_qvotec_complete_record_cas(
				semantic_record_cas_request.request_seq,
				semantic_record_cas_result);
	} else if (cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
				   &undo_root_descriptor_read_request)) {
		ClusterUndoRootDescriptorV1 descriptor;
		uint8 selected[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
		uint8 observed_disk_bitmap = 0;
		ClusterUndoRootDescriptorState state
			= CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;

		if (qvotec_undo_root_descriptor_formation_attested_fds(
				qvotec_fds, qvotec_n_disks)) {
			state = qvotec_undo_root_descriptor_read_fds(
				qvotec_fds, qvotec_n_disks,
				undo_root_descriptor_read_request.system_identifier,
				CLUSTER_UNDO_ROOT_KIND_SHARED, -1, &descriptor,
				&observed_disk_bitmap);
			if (state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				&& !cluster_undo_root_descriptor_encode(
					&descriptor, selected))
				state = CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
		}
		(void)observed_disk_bitmap;
		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
			undo_root_descriptor_read_request.request_seq, state, selected);
	} else if (cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
				   &undo_root_descriptor_request)) {
		uint8 completed_disk_bitmap;
		ClusterSemanticActivationResult result
			= qvotec_undo_root_descriptor_formation_attested_fds(
				  qvotec_fds, qvotec_n_disks)
				  && qvotec_undo_root_descriptor_provision_fds(
				  qvotec_fds, qvotec_n_disks,
				  undo_root_descriptor_request.system_identifier,
				  undo_root_descriptor_request.desired_bytes,
				  &completed_disk_bitmap)
				  ? CLUSTER_SEMANTIC_ACTIVATION_OK
				  : CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
			undo_root_descriptor_request.request_seq, result);
	}

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

	/* spec-5.13 §2.5: pick up a pending clean-leave marker submit too.  It is
	 * written into THIS node's own leave-slot on every disk in the same write
	 * loop below and acked majority-durable, exactly like the fence marker. */
	have_leave_submit = cluster_clean_leave_qvotec_poll_pending(leave_marker_slot);

	/* spec-5.15 §2.6: pick up a pending join-commit marker submit too.  Unlike the
	 * leave marker (this node's own slot) it is written to the JOINER's region-3
	 * slot (join_target_node) on every disk in the same write loop below and acked
	 * majority-durable. */
	have_join_submit
		= cluster_reconfig_join_qvotec_poll_pending(
			&join_marker_operation, &join_target_node, join_marker_slot);

	/* RF-ROOT P9 verification (cold-formation cold-formation ruling): pick
	 * up a pending cold-formation marker submit.  The arbiter LMON staged a
	 * COMMITTED marker + the target co-boot member set; qvotec writes it to
	 * every target member's region-7 slot on every disk and ACKs only on a
	 * majority write + majority exact readback. */
	have_formation_marker_submit
		= cluster_reconfig_formation_qvotec_poll_pending(
			&formation_marker_request);

	/* spec-5.18 §2.5: pick up a pending removal-marker submit too.  It rides THIS
	 * node's own self-slot _reserved1[64..] (right after the 4.12 fence marker),
	 * carried forward every poll like the fence marker (R12), and acked majority-
	 * durable from the self-slot write tally. */
	have_removal_submit = cluster_node_remove_qvotec_poll_pending(&removal_submit_marker);
	if ((have_submit || have_leave_submit || have_join_submit || have_removal_submit)
		&& cluster_cr_injection_armed("cluster-qvotec-marker-service-hold", NULL))
		return;

	memset(&apply_lease_request, 0, sizeof(apply_lease_request));
	memset(&apply_lease_winner, 0, sizeof(apply_lease_winner));
	apply_lease_winner.owner_node_id = -1;
	have_apply_lease_request = cluster_mrp_qvotec_poll_apply_lease_request(&apply_lease_request);

	if (qvotec_n_disks == 0) {
		/* Single-node compat: no disks, no quorum to decide.  Hold
		 * quorum_state at INITIALIZING so any explicit consumer that
		 * does check it stays fail-closed (per Q4 v0.2 default). */
		if (have_submit)
			cluster_write_fence_qvotec_complete(false); /* no disk -> no majority */
		if (have_leave_submit)
			cluster_clean_leave_qvotec_complete(false); /* no disk -> no majority */
		if (have_join_submit)
			cluster_reconfig_join_qvotec_complete(
				join_marker_operation, false, NULL); /* no disk -> no majority */
		if (have_formation_marker_submit)
			cluster_reconfig_formation_qvotec_complete(false); /* no disk */
		if (have_removal_submit)
			cluster_node_remove_qvotec_complete(false); /* no disk -> no majority */
		if (have_apply_lease_request)
			cluster_mrp_qvotec_complete_apply_lease_request(
				CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM, NULL);
		return;
	}

	if (have_join_submit
		&& join_marker_operation
			   == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED) {
		uint8 verified_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
		bool verified;

		verified = qvotec_join_marker_verify_committed_closed_fds(
			qvotec_fds, qvotec_n_disks, join_target_node, verified_image);
		cluster_reconfig_join_qvotec_complete(
			join_marker_operation, verified,
			verified ? verified_image : NULL);
		have_join_submit = false;
	}

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES) {
		/* Defensive: invalid node_id ⇒ cannot author a self slot.
		 * Leave shmem at last-known state and skip the cycle.  Q7
		 * startup validator (next commit) will reject this config
		 * before we get here in production paths. */
		if (have_submit)
			cluster_write_fence_qvotec_complete(false); /* cannot author self slot */
		if (have_leave_submit)
			cluster_clean_leave_qvotec_complete(false); /* cannot author self slot */
		if (have_join_submit)
			cluster_reconfig_join_qvotec_complete(
				join_marker_operation, false, NULL); /* cannot author self slot */
		if (have_removal_submit)
			cluster_node_remove_qvotec_complete(false); /* cannot author self slot */
		if (have_apply_lease_request)
			cluster_mrp_qvotec_complete_apply_lease_request(CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID,
															NULL);
		return;
	}

	/* Common-epoch authority is serviced only by QVOTEC after its disk set and
	 * local node identity are valid.  RECOVER_HEAD is read-only; PROPOSE remains
	 * HOLD until the cooperative P1/P2/SETTLE phases are installed. */
	qvotec_epoch_ballot_mailbox_tick();

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
			if (node == (uint32)cluster_node_id
				&& rrc == CLUSTER_VOTING_DISK_IO_OK)
				own_prior_read_ok[i] = true;
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
	 * RF-ROOT P9 verification (cold-formation): the per-node observed-slot
	 * publication (incarnation/generation/epoch) MOVED to the end of the
	 * poll cycle, where it is published together with the fresh-alive view
	 * and the same-round in-quorum snapshot inside ONE seqlock window
	 * (see the publish-shmem block below).  Publishing it here — BEFORE
	 * decide_quorum_view computed the fresh-alive view — let an ABSENT
	 * admission combine a new incarnation with a stale fresh bit.
	 */

	/*
	 * spec-5.16 (3-node join participation) — observe each PEER's durable COMMITTED
	 * join marker (region-3) and publish its admitted incarnation + epoch to shmem.
	 * This is the symmetric observer half of the LEAVE detection: a SURVIVOR's LMON
	 * membership tick uses it to recognize a rejoined peer as MEMBER so its GRD FSM
	 * joins the re-declare barrier (without it the coordinator's barrier waits
	 * forever for a non-participating survivor in >=3-node).  Mirrors the self-
	 * admission read below; one extra region-3 slot read per (disk × peer) per poll
	 * (qvotec already has the fds).  Same-commit majority + COMMITTED-basis
	 * validated (INV-J13); a never-joined / undeclared slot fails the basis check
	 * and publishes nothing.
	 *
	 * Hardening v1.4 (reviewer P1 #2) — this used to count ANY committed-basis
	 * marker toward the majority and take the max incarnation/epoch, UNLIKE the
	 * self-admit and startup-seed paths.  Two distinct join attempts (different
	 * coordinator / epoch / nonce), each on a minority of disks, would then
	 * aggregate into a false majority -> the survivor readmits a peer whose join
	 * never durably committed (DEAD->MEMBER + observer JOIN event) -> 8.A.  Now it
	 * mirrors them exactly: collect committed-basis markers, then require a
	 * same-commit majority via the shared cluster_join_marker_select_majority.
	 */
	{
		uint32 node;
		uint32 majority = ((uint32)qvotec_n_disks / 2u) + 1u;

		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			ClusterJoinCommitMarker committed[CLUSTER_MAX_VOTING_DISKS];
			int n_committed = 0;
			int win;
			int d;

			if ((int32)node == cluster_node_id)
				continue; /* self handled below */

			for (d = 0; d < qvotec_n_disks; d++) {
				union {
					uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
					uint64 _align;
				} jslot;
				ClusterJoinCommitMarker m;

				if (cluster_voting_disk_read_join_slot(qvotec_fds[d], node, jslot.bytes)
					!= CLUSTER_VOTING_DISK_IO_OK)
					continue;
				memcpy(&m, jslot.bytes, sizeof(m));
				if (!cluster_join_marker_is_committed_basis(&m, (int32)node))
					continue;
				committed[n_committed++] = m;
			}
			win = cluster_join_marker_select_majority(committed, n_committed, majority, NULL);
			if (win >= 0 && committed[win].admitted_incarnation > 0)
				cluster_reconfig_record_observed_committed_join((int32)node,
																committed[win].admitted_incarnation,
																committed[win].admitted_epoch);
		}
	}

	/*
	 * spec-5.15 D5: detect THIS node's own admission — a §2.6 COMMITTED join
	 * marker in region-3 slot self, with admitted_incarnation == our incarnation,
	 * on a quorum-majority of disks (one extra slot read per disk; qvotec already
	 * has the fds open).
	 *
	 * Hardening v1.1:
	 *   HF-3 (INV-J13): require a majority of the SAME commit (identical nonce),
	 *     not "any COMMITTED marker" — two minority writes from different commit
	 *     attempts (different coordinator / epoch) must not aggregate (P1-3).
	 *   HF-1 (INV-J9): open the gate only after the publish-proof also holds (a
	 *     member quorum reached admitted_epoch).  marker-durable-but-coordinator-
	 *     crashed-before-publish keeps the gate CLOSED -> the joiner times out ->
	 *     53R61 -> restarts (P1-1 half-publish window).  Until then the lmon
	 *     epoch catch-up keeps transport alive and the next poll re-checks.
	 */
	if (cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES) {
		ClusterJoinCommitMarker self_markers[CLUSTER_MAX_VOTING_DISKS];
		int n_self = 0;
		uint32 majority = ((uint32)qvotec_n_disks / 2u) + 1u;
		int d;
		int win;

		(void)cluster_reconfig_qvotec_observe_replacement_admitted(
			qvotec_fds, qvotec_n_disks, qvotec_self_incarnation);

		for (d = 0; d < qvotec_n_disks; d++) {
			union {
				uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
				uint64 _align;
			} jslot;
			ClusterJoinCommitMarker m;

			if (cluster_voting_disk_read_join_slot(qvotec_fds[d], (uint32)cluster_node_id,
												   jslot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&m, jslot.bytes, sizeof(m));
			if (!cluster_join_marker_is_committed_basis(&m, cluster_node_id))
				continue;
			if (m.admitted_incarnation != qvotec_self_incarnation)
				continue; /* a stale prior-incarnation admission — not us */
			self_markers[n_self++] = m;
		}
		/* HF-3 / INV-J13: find a single commit (nonce) present on >= majority disks
		 * (shared selector — same logic as startup-seed and peer-observe). */
		win = cluster_join_marker_select_majority(self_markers, n_self, majority, NULL);

		/* HF-1: gate-open requires the publish-proof too, not the marker alone. */
		if (win >= 0 && cluster_reconfig_join_publish_proven(self_markers[win].admitted_epoch)) {
			cluster_reconfig_note_self_admitted(self_markers[win].admitted_epoch);
			/*
			 * spec-5.16 (3-node rejoin) — the same durable COMMITTED join marker
			 * that admits self also supersedes any fail-stop write-fence still
			 * listing self as dead (RC-5 for the write-fence): if its admitted
			 * epoch is newer than the fence, self un-fences and may serve again,
			 * breaking the 3-node rejoin convergence deadlock.
			 */
			cluster_write_fence_supersede_by_admit(self_markers[win].admitted_epoch);
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
			/* spec-4.12b D5/P1-1: retain the exact durable tuple so the
			 * baseline author below cannot regress order/dead membership or
			 * publish a competing identity at the same order. */
			durable_authority_marker = authority.marker;
			durable_has_authority = true;
		} else if (authority.minority_seen)
			cluster_write_fence_note_minority_marker();
	}

	/* ---- 2. decide BEFORE writing self slot ---- */
	(void)decide_quorum_view(qvotec_slot_matrix, io_states, (uint32)qvotec_n_disks,
							 CLUSTER_MAX_NODES, (uint32)cluster_node_id, qvotec_self_incarnation,
							 now_us, heartbeat_timeout_us, &decision);

	/*
	 * RF-ROOT P9 verification (cold-formation): the per-node FRESH-ALIVE
	 * publication moved to the end of the poll cycle — it is now published
	 * with the observed slots and the in-quorum snapshot inside ONE seqlock
	 * window (see the publish-shmem block below).  Publishing it right after
	 * decide_quorum_view left a window where a NEW incarnation (published
	 * above it) could pair with a STALE fresh bit.
	 */

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
		if (have_apply_lease_request)
			cluster_mrp_qvotec_complete_apply_lease_request(CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID,
															NULL);

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

	if (have_apply_lease_request) {
		ClusterMrpApplyLeaseSubmitResult apply_lease_result;

		apply_lease_result
			= qvotec_apply_lease_cas(&apply_lease_request, decision.alive_bitmap,
									 (int)sizeof(decision.alive_bitmap), &apply_lease_winner);
		cluster_mrp_qvotec_complete_apply_lease_request(apply_lease_result, &apply_lease_winner);
	}

	/* ---- 3. build + write self slot to every disk ---- */
	memset(&self_slot, 0, sizeof(self_slot));
	self_slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
	self_slot.version = CLUSTER_VOTING_SLOT_VERSION;
	self_slot.node_id = (uint32)cluster_node_id;
	self_slot.incarnation = qvotec_self_incarnation;
	self_slot.heartbeat_ts_us = now_us;
	/*
	 * spec-5.15: publish the LIVE membership epoch (was current_epoch_at_boot,
	 * which is initialized to 0 and never updated -> every slot carried epoch 0,
	 * leaving the spec-2.0 R10 boot-epoch recovery dormant and an online rejoiner
	 * unable to learn the cluster epoch from the durable disk).  A rejoiner reads
	 * this to catch up (joiner_self_tick) so its IC frames are not stale-dropped
	 * (the anti-stale envelope guard, spec-2.4) before it can be detected ALIVE
	 * and admitted.  The incarnation vet + COMMITTED marker still gate MEMBER.
	 *
	 * RF-ROOT P9 verification / cold-formation cold-formation ruling
	 * (2026-08-19): an UN-FORMED node publishes CLUSTER_EPOCH_INITIAL in its
	 * slot — NOT the local (possibly recovered) epoch.  A formed survivor
	 * publishes the live epoch, so a co-booting peer's observation window can
	 * distinguish "old formation live survivor" (fresh slot past INITIAL)
	 * from "fresh cold co-boot" (fresh slot at INITIAL, still unformed) —
	 * the 5.22 no-survivor observation window.  The node flips to the live
	 * epoch the moment its membership becomes MEMBER (cold-formation marker
	 * admission or any join path).  This is a slot-epoch SEMANTIC only: the
	 * membership epoch itself is untouched.
	 */
	if (cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER)
		self_slot.current_epoch = cluster_epoch_get_current();
	else
		self_slot.current_epoch = CLUSTER_EPOCH_INITIAL;
	self_slot.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;

	/*
	 * spec-4.12b D2: steady-state baseline author.  When there is no fresh LMON
	 * submit and THIS node is the deterministic membership leader (lowest live
	 * node_id in decide()'s alive_bitmap, computed above before any self-slot
	 * write), author a BASELINE marker reflecting the locally-applied membership
	 * and pack it to EVERY disk below -- exactly like the fence path, so the
	 * leader's tuple reaches quorum-majority and refreshes every node's token next
	 * poll.  A non-leader never authors a baseline (it would only ever land a
	 * per-disk minority marker = R4); it keeps preserve_per_disk.
	 *
	 * Gated on enforcement == ON (mirrors the reconfig fence-submit gate): when
	 * OFF/DEV the hot gate is a no-op, so authoring a baseline buys nothing and we
	 * keep the pre-4.12b steady-state behaviour (zero regression, no marker write).
	 *
	 * spec-4.12b D5/P1-1 (8.A): the membership tuple is built from last_applied
	 * (cluster_reconfig.c publishes the event -- and thus advances last_applied --
	 * only AFTER the coordinator's fence-marker submit ACKs).  In that
	 * bump-before-publish window a leader whose last_applied still lags the just-
	 * submitted fence would author a LOWER-epoch baseline and pack it over the SAME
	 * self-slot that just received the (higher-epoch) fence marker -- erasing the
	 * fence from disk before peers read it.  Guard fail-closed: if the would-be
	 * baseline is below the durable authority this very poll observed, shrinks its
	 * dead set, or names a competing identity at the same ordering key, do NOT
	 * author it -- preserve the durable marker instead and count it.  (The leader
	 * catches up once last_applied advances, authoring a baseline tuple-identical
	 * to the fence.)
	 */
	is_leader = qvotec_self_is_membership_leader(decision.alive_bitmap);
	if (!have_submit && cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON
		&& is_leader) {
		qvotec_build_baseline_marker(&baseline_marker);
		if (!cluster_fence_baseline_author_permitted_v1(
				&baseline_marker, durable_has_authority,
				durable_has_authority ? &durable_authority_marker : NULL))
			cluster_write_fence_note_baseline_stale(); /* non-monotonic or same-order conflict */
		else
			author_baseline = true;
	}

	for (i = 0; i < qvotec_n_disks; i++) {
		ClusterVotingDiskIoState wrc;
		ClusterVotingSlot *own_prior = &qvotec_slot_matrix[i * CLUSTER_MAX_NODES + cluster_node_id];
		ClusterReplacementRequestSlotState rplm_state;

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
		 *
		 * spec-4.12b D2: the steady-state baseline (leader, no submit) is likewise
		 * written to ALL disks -- it is this leader's own authoritative membership
		 * tuple, not a cross-disk copy, so it reaches quorum-majority the same way.
		 */
		self_slot.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
		memset(self_slot._reserved1, 0, sizeof(self_slot._reserved1));
		if (have_submit)
			cluster_fence_marker_pack(self_slot._reserved1, &submit_marker);
		else if (author_baseline)
			cluster_fence_marker_pack(self_slot._reserved1, &baseline_marker);
		else
			cluster_fence_marker_preserve_per_disk(self_slot._reserved1, own_prior->_reserved1);

		/*
		 * spec-5.18 §2.5 (R12): the removal marker rides _reserved1[64..] (after
		 * the fence marker).  The memset above zeroed it, so it MUST be re-packed
		 * (a fresh submit) or carried forward from THIS disk's own prior slot every
		 * poll — exactly like the fence marker's R13 carry-forward — else the next
		 * heartbeat erases the durable SHRUNK/REMOVED record (crash-recovery would
		 * then mis-read the removal phase).  This block runs UNCONDITIONALLY: it is
		 * independent of the fence-marker path above (different _reserved1 region).
		 */
		if (have_removal_submit)
			cluster_removal_marker_pack(self_slot._reserved1, &removal_submit_marker);
		else
			cluster_removal_marker_preserve_per_disk(self_slot._reserved1, own_prior->_reserved1);

		/*
		 * Spec-5.15A §2.1/A1-I2: RPLM is another independent reserved1
		 * region.  Preserve only this disk's exact flag+marker+slot identity.
		 * An unreadable or conflicting prior self-slot is not permission to
		 * erase durable replacement intent, so this disk's heartbeat write is
		 * withheld and counted failed below.
		 */
		if (own_prior_read_ok[i])
			rplm_state
				= qvotec_replacement_request_preserve(&self_slot, own_prior);
		else
			rplm_state = CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;

		qvotec_slot_generation++;
		self_slot.generation = qvotec_slot_generation;
		self_slot.disk_index = (uint32)i;


		if (rplm_state == CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD)
			wrc = CLUSTER_VOTING_DISK_IO_FAILED;
		else
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

		/*
		 * spec-5.13 §2.5: write the staged clean-leave marker to THIS node's
		 * own leave-slot (region 2) on this disk, in the same loop.  Count the
		 * disks that accepted it for the majority-durable ack below.  This is
		 * independent of the voting-slot write above (a separate offset); a
		 * voting-slot write failure does not by itself fail the marker write.
		 */
		if (have_leave_submit
			&& cluster_voting_disk_write_leave_slot(qvotec_fds[i], (uint32)cluster_node_id,
													leave_marker_slot)
				   == CLUSTER_VOTING_DISK_IO_OK)
			leave_disks_ok++;

		/*
		 * spec-5.15 §2.6: write the staged join-commit marker to the JOINER's
		 * region-3 slot (join_target_node, NOT this node's own slot) on this disk,
		 * in the same loop.  Count the disks that accepted it for the majority-
		 * durable ack below; independent of the voting-slot / leave-slot writes.
		 */
		if (have_join_submit && join_target_node >= 0
			&& cluster_voting_disk_write_join_slot(qvotec_fds[i], (uint32)join_target_node,
											   join_marker_slot)
				   == CLUSTER_VOTING_DISK_IO_OK)
			join_disk_write_succeeded[i] = true;

		/*
		 * RF-ROOT P9 verification (cold-formation cold-formation ruling):
		 * write the staged cold-formation marker into EVERY target member's
		 * region-7 slot on this disk (the JCMK coordinator-write pattern).
		 */
		if (have_formation_marker_submit)
		{
			int			tm;

			for (tm = 0; tm < CLUSTER_MAX_NODES; tm++)
			{
				if ((formation_marker_request.target_members[tm / 8]
					 & (uint8) (1u << (tm % 8))) == 0)
					continue;
				(void) cluster_voting_disk_write_formation_slot(
					qvotec_fds[i], (uint32) tm,
					formation_marker_request.marker_bytes);
			}
		}
	}

	/*
	 * spec-4.12b D6: record this poll's baseline-author observability -- the
	 * current leadership (is_leader -> baseline_author_is_self) and whether this
	 * cycle authored a baseline (author_baseline -> bumps baseline_published).
	 */
	cluster_write_fence_note_baseline_published(is_leader, author_baseline);

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
		fence_majority_written = (disks_ok_post_write >= quorum_size_post_write);
		if (have_submit)
			cluster_write_fence_qvotec_complete(fence_majority_written);

		/*
		 * spec-5.13 §2.5: ack the clean-leave marker submit.  Uses the marker's
		 * own per-disk write tally (leave_disks_ok), not the voting-slot tally,
		 * since the two writes are at independent offsets.  ACK only on >=
		 * quorum-majority; otherwise the driver fails closed (no false durable).
		 */
		if (have_leave_submit)
			cluster_clean_leave_qvotec_complete(leave_disks_ok >= quorum_size_post_write);

		/* spec-5.15 §2.6: ACK only after a strict majority of the same
		 * disks complete write + durability + exact region-3 readback. */
	if (have_join_submit)
	{
		bool proven;

		proven = qvotec_join_marker_ack_proven_fds(
			qvotec_fds, qvotec_n_disks, join_target_node,
			join_marker_slot, join_disk_write_succeeded);
		cluster_reconfig_join_qvotec_complete(
			join_marker_operation, proven, NULL);
	}

		/* RF-ROOT P9 verification (cold-formation cold-formation ruling):
		 * ACK the cold-formation marker submit only when EVERY target
		 * member's region-7 slot carries the EXACT image on a strict
		 * majority of disks (write + readback). */
		if (have_formation_marker_submit)
		{
			bool		proven = true;
			int			tm;

			for (tm = 0; tm < CLUSTER_MAX_NODES && proven; tm++)
			{
				uint32		exact = 0;
				int			d;

				if ((formation_marker_request.target_members[tm / 8]
					 & (uint8) (1u << (tm % 8))) == 0)
					continue;
				for (d = 0; d < qvotec_n_disks; d++)
				{
					uint8		reread[CLUSTER_VOTING_SLOT_BYTES];

					if (cluster_voting_disk_read_formation_slot(
							qvotec_fds[d], (uint32) tm, reread)
							== CLUSTER_VOTING_DISK_IO_OK
						&& memcmp(reread,
								  formation_marker_request.marker_bytes,
								  CLUSTER_VOTING_SLOT_BYTES) == 0)
						exact++;
				}
				if (exact < (uint32) qvotec_n_disks / 2u + 1u)
					proven = false;
			}
			cluster_reconfig_formation_qvotec_complete(proven);
		}

		/*
		 * spec-5.18 §2.5: ack the removal-marker submit.  It rode in the self-slot
		 * write (same _reserved1 as the fence marker), so the count of disks that
		 * accepted the write is exactly its durable-disk count.  ACK only on >=
		 * quorum-majority — otherwise the driver fails closed (no false durable
		 * REMOVING/SHRUNK/REMOVED).
		 */
		if (have_removal_submit)
			cluster_node_remove_qvotec_complete(disks_ok_post_write >= quorum_size_post_write);
	}

	/*
	 * RF-ROOT P6 (contract 1 survivor side; spec-4.12b D2 same-poll
	 * refinement):  the D2 token refresh near the top of this poll read the
	 * PRE-write matrix, so an authority this very poll just made durable
	 * (a submitted fence marker, or the leader's steady-state baseline at
	 * the new applied epoch) would otherwise leave the LOCAL hot gate stale
	 * for one more poll.  A clean-leave commit publishes its epoch advance
	 * WITHOUT a fence marker (spec-5.13: nothing to fence), so the first
	 * fence-gated write on the survivor right after the commit — e.g. the
	 * CHECKPOINT / shutdown-checkpoint recovery-anchor publication — would
	 * PANIC on the exact-epoch judge before a later poll latched the new
	 * baseline.  Once the tuple this poll wrote is majority-durable (the
	 * same tally that ACKs the submit), refresh the local token from it
	 * directly:  same pure judge + monotonic guard, qvotec remains the sole
	 * token writer, no gate loosening — it only shrinks the healthy-side
	 * stale window (the R4 window's reverse) from two polls to one.
	 */
	if (fence_majority_written && (have_submit || author_baseline))
		cluster_write_fence_refresh_from_marker(
			have_submit ? &submit_marker : &baseline_marker,
			now_us + (uint64)cluster_write_fence_lease_ms * 1000ULL);

	/*
	 * spec-6.15 D5b: xid-stripe face.  Keep re-scanning region 5 until a
	 * record is PUBLISHED — an ABSENT verdict is not final (the seed
	 * candidate on ANOTHER node writes the record after our startup scan,
	 * and a co-booting joiner must observe it to leave the stripe HOLD),
	 * and a CORRUPT verdict may be repaired in place.  One 512-byte pread
	 * per disk per poll, and none once PUBLISHED (the steady state).
	 * Also service a pending activation-seed request (write to every
	 * disk, majority-durable, adopt-not-overwrite — see
	 * cluster_xid_stripe_boot.c).
	 */
	if (cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_PUBLISHED)
		cluster_xid_stripe_scan_disks(qvotec_fds, qvotec_n_disks);
	cluster_xid_stripe_service_seed(qvotec_fds, qvotec_n_disks);

	/* spec-6.15 D3: counter herding — sweep peer hwm, publish min/max,
	 * durably advance this node's hwm promise (no-op until the stripe
	 * face is PUBLISHED + MINE). */
	cluster_xid_stripe_herding_tick(qvotec_fds, qvotec_n_disks);

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

	/* RF-ROOT P9 verification (cold-formation cold-formation ruling):
	 * observe THIS node's own region-7 formation-marker slot.  A COMMITTED
	 * marker standing on a strict majority of disks (exact same image) is
	 * published for the cold-formation admission; otherwise the observation
	 * is cleared (fail-closed: no marker, no admission). */
	{
		uint8		images[CLUSTER_MAX_VOTING_DISKS][CLUSTER_VOTING_SLOT_BYTES];
		bool		valid[CLUSTER_MAX_VOTING_DISKS];
		int			selected = -1;
		uint32		majority = (uint32) qvotec_n_disks / 2u + 1u;
		int			d;

		memset(valid, 0, sizeof(valid));
		for (d = 0; d < qvotec_n_disks; d++)
		{
			if (cluster_voting_disk_read_formation_slot(
					qvotec_fds[d], (uint32) cluster_node_id, images[d])
					== CLUSTER_VOTING_DISK_IO_OK)
			{
				ClusterFormationCommitMarker dec;

				if (cluster_formation_marker_validate(images[d], &dec, NULL))
					valid[d] = true;
			}
		}
		for (d = 0; d < qvotec_n_disks && selected < 0; d++)
		{
			uint32		same = 0;
			int			e;

			if (!valid[d])
				continue;
			for (e = 0; e < qvotec_n_disks; e++)
				if (valid[e]
					&& memcmp(images[d], images[e], CLUSTER_VOTING_SLOT_BYTES) == 0)
					same++;
			if (same >= majority)
				selected = d;
		}
		if (selected >= 0)
		{
			ClusterFormationCommitMarker dec;
			uint64		incarnation_by_node[CLUSTER_MAX_NODES];

			if (cluster_formation_marker_decode(
					images[selected], &dec, incarnation_by_node))
				cluster_reconfig_formation_qvotec_publish_observed(
					&dec, incarnation_by_node);
		}
		else
			cluster_reconfig_formation_qvotec_clear_observed();
	}

	/*
	 * RF-ROOT P9 verification / cold-formation ruling (2026-08-19): publish
	 * the bootstrap-observation window — per-node observed incarnation /
	 * generation / epoch + fresh-alive + the same-round in-quorum snapshot —
	 * inside ONE seqlock window (observed_bootstrap_seq odd = writer in
	 * progress).  The founding-formation ABSENT admission reads this window
	 * through cluster_reconfig_bootstrap_proof_node, which retries until it
	 * samples a stable EVEN seq: no "new incarnation + stale fresh-alive"
	 * cross-window combination can form an admission proof.  The observed
	 * slot/epoch data comes from this poll's matrix (decide_quorum_view's
	 * input); the fresh view is decide's alive_bitmap (P2.1 freshness); the
	 * in-quorum snapshot is the post-write quorum decision (same cycle as
	 * the quorum_state published above).  JCMK observations (region-3) are
	 * NOT part of the founding proof and stay outside the window.
	 */
	{
		uint32 node;

		cluster_reconfig_bootstrap_publish_begin();
		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			uint64 best_gen = 0;
			uint64 best_incarnation = 0;
			uint64 best_epoch = 0;
			bool	fresh;

			for (i = 0; i < qvotec_n_disks; i++) {
				ClusterVotingSlot *cell = &qvotec_slot_matrix[i * CLUSTER_MAX_NODES + node];

				if (cell->generation > best_gen && cell->node_id == node) {
					best_gen = cell->generation;
					best_incarnation = cell->incarnation;
					best_epoch = cell->current_epoch;
				}
			}
			cluster_reconfig_record_observed_slot((int32)node, best_incarnation,
												  best_gen, best_epoch);
			fresh = (decision.alive_bitmap[node / 8]
					 & (uint8) (1u << (node % 8))) != 0;
			cluster_reconfig_record_observed_fresh_alive((int32)node, fresh);
		}
		cluster_reconfig_bootstrap_publish_in_quorum(
			decision.quorum_state == CLUSTER_QVOTEC_QUORUM_OK);
		cluster_reconfig_bootstrap_publish_end();
	}
}

/*
 * spec-6.15 D5c: expose this boot's self-incarnation (the canonical
 * durable identity seed, also written into the voting self-slot every
 * poll).  Consumed by the stripe slot first-claim, which runs in this
 * process (qvotec) via the stripe mailbox service.
 */
uint64
cluster_qvotec_self_incarnation_value(void)
{
	return qvotec_self_incarnation;
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

	/* spec-5.15A §2.4.1: start-current invalidates both volatile
	 * completions under the reconfig lock before any latch is published. */
	if (!cluster_reconfig_qvotec_lifecycle_transition(
			&QvotecShmem->mailbox, &QvotecShmem->state,
			CLUSTER_QVOTEC_STARTING))
		ereport(FATAL, (errmsg("qvotec could not establish STARTING lifecycle cut")));

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
	 * spec-4.12b D4: with enforcement default ON, a single node / no-voting-disk
	 * deployment cannot fence (no shared storage, no quorum authority) and would
	 * otherwise wrongly block its own recovery; cluster_write_fence_enforcing()
	 * auto-degrades it to a no-op at runtime.  Emit one LOG line here (qvotec is
	 * postmaster-spawned exactly once) so the operator knows the fence is inactive
	 * despite the on setting -- they must configure cluster.voting_disks to make it
	 * effective.
	 */
	if (qvotec_n_disks == 0 && cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON)
		ereport(LOG, (errmsg("cluster write-fence: enforcement is on but no voting disks are "
							 "configured; the write fence is inactive (single-node degraded mode)"),
					  errhint("Set cluster.voting_disks to a shared-storage majority to make "
							  "write-fence enforcement effective, or set "
							  "cluster.write_fence_enforcement=dev to silence this notice.")));

	/*
	 * spec-4.12 D4: publish MyLatch so the reconfig coordinator (LMON) can wake us
	 * for a synchronous fence-marker submit.  No-op until the write-fence region is
	 * registered (D7); auto-cleared at proc_exit so a stale latch is never signalled.
	 */
	cluster_write_fence_publish_qvotec_latch(MyLatch);

	/*
	 * spec-5.13 D2/§2.5: publish MyLatch for the clean-leave marker submit
	 * handshake too (the leaving node's REQUESTED + the coordinator's
	 * COMMITTING/COMMITTED writes ride qvotec the same way the fence marker
	 * does), then rebuild clean-departed state from any durable COMMITTED
	 * leave-marker on disk — a node that left cleanly before this postmaster
	 * started must not be re-treated as a crash (CL-I13 / P1-V0.7 restart leg).
	 * Runs once, postmaster-spawned, after the disks are open.
	 */
	cluster_clean_leave_publish_qvotec_latch(MyLatch);
	cluster_clean_leave_rebuild_from_disks(qvotec_fds, qvotec_n_disks);

	/* spec-6.4: qvotec is the sole writer for the ADG apply-master lease CAS. */
	cluster_mrp_publish_qvotec_latch(MyLatch);

	/* spec-5.18 D8: publish the removal-marker latch + rebuild the permanently-
	 * removed set from durable §2.5 SHRUNK/REMOVED markers (INV-LF7 restart leg). */
	cluster_node_remove_publish_qvotec_latch(MyLatch);
	cluster_node_remove_rebuild_from_disks(qvotec_fds, qvotec_n_disks);

	/*
	 * spec-5.15 D4: publish the join-marker latch + seed last_admitted from the
	 * durable COMMITTED region-3 join markers (INV-J7 — a restart must not zero
	 * the floor and re-open the gate to a stale incarnation).  The seed MUST run
	 * AFTER cluster_clean_leave_rebuild_from_disks so its RC-5 supersede
	 * correction can clear clean_departed[N] for a node that rejoined after a
	 * clean leave (else the rebuild's re-set would mask N's later fail-stop).
	 */
	cluster_reconfig_publish_join_qvotec_latch(MyLatch);
	cluster_reconfig_publish_formation_qvotec_latch(MyLatch);
	cluster_membership_seed_last_admitted_from_voting_disk(qvotec_fds, qvotec_n_disks);

	/* RF-ROOT P9 verification (cold-formation cold-formation ruling):
	 * seed the highest COMMITTED cold-formation generation found across
	 * region 7, so a takeover arbiter writes max+1 (monotonic).  The
	 * marker's per-member INCARNATIONS are never inherited — each cold
	 * formation commits its CURRENT boot incarnations (a new postmaster
	 * never resumes a previous marker's incarnation). */
	{
		uint64		max_generation = 0;
		int			n;

		for (n = 0; n < CLUSTER_MAX_NODES; n++)
		{
			int			d;

			for (d = 0; d < qvotec_n_disks; d++)
			{
				uint8		slot_bytes[CLUSTER_VOTING_SLOT_BYTES];
				ClusterFormationCommitMarker dec;

				if (cluster_voting_disk_read_formation_slot(
						qvotec_fds[d], (uint32) n, slot_bytes)
						== CLUSTER_VOTING_DISK_IO_OK
					&& cluster_formation_marker_validate(
						slot_bytes, &dec, NULL)
					&& dec.formation_generation > max_generation)
					max_generation = dec.formation_generation;
			}
		}
		cluster_reconfig_formation_qvotec_note_max_generation(max_generation);
	}

	/*
	 * spec-6.15 D5b: publish the durable xid-stripe activation state
	 * (voting-disk region 5) before the joiner gate can consult it.
	 * Idempotent adopt-only read; any later activation seed goes
	 * through the poll-loop mailbox below.
	 */
	cluster_xid_stripe_scan_disks(qvotec_fds, qvotec_n_disks);

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
	cluster_qvotec_publish_self_incarnation(qvotec_self_incarnation);
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

		for (d = 0; d < qvotec_n_disks; d++) {
			ClusterVotingSlot probe;
			ClusterVotingDiskIoState rrc;

			rrc = cluster_voting_disk_read_slot(qvotec_fds[d], d, (uint32)cluster_node_id, &probe);
			if (rrc != CLUSTER_VOTING_DISK_IO_OK)
				continue;
			if (probe.generation == 0)
				continue; /* never written */
			if (!(probe.flags & CLUSTER_VOTING_SLOT_FLAG_ALIVE))
				continue; /* prior shutdown cleared ALIVE — clean death, ok */
			if (probe.incarnation == qvotec_self_incarnation)
				continue; /* same incarnation — impossible but defensive */

			/*
			 * Crash-rejoin re-declare barrier (Shape A) — a prior-incarnation
			 * self-slot that still carries ALIVE means the previous postmaster
			 * of THIS node died WITHOUT running the clean-shutdown blank
			 * (qvotec_clear_self_alive_on_clean_shutdown), i.e. an UNCLEAN
			 * death.  Latch it REGARDLESS of freshness: a stale ALIVE ghost is
			 * still proof we crashed (we just crashed longer ago), and the
			 * fence must engage on a fast rejoin where the survivor has not yet
			 * advanced its epoch (the epoch signal is INITIAL on both sides).
			 * Single writer, before the READY publish; read-only afterwards.
			 */
			if (QvotecShmem != NULL)
				pg_atomic_write_u32(&QvotecShmem->prior_unclean_death, 1);

			if (probe.heartbeat_ts_us == 0)
				continue;
			if (now_us > probe.heartbeat_ts_us
				&& (now_us - probe.heartbeat_ts_us) > heartbeat_timeout_us)
				continue; /* already stale — no fast-restart Q6 sleep needed */
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

	if (!cluster_reconfig_qvotec_lifecycle_transition(
			&QvotecShmem->mailbox, &QvotecShmem->state,
			CLUSTER_QVOTEC_READY))
		ereport(FATAL, (errmsg("qvotec could not publish READY lifecycle cut")));

	for (;;) {
		int rc;
		instr_time cycle_started;
		instr_time cycle_finished;

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
			|| pg_atomic_read_u32(&QvotecShmem->state) == CLUSTER_QVOTEC_SHUTTING_DOWN) {
			if (!cluster_reconfig_qvotec_lifecycle_transition(
					&QvotecShmem->mailbox, &QvotecShmem->state,
					CLUSTER_QVOTEC_SHUTTING_DOWN))
				ereport(FATAL,
						(errmsg("qvotec could not publish SHUTTING_DOWN lifecycle cut")));
			break;
		}

		/* P1.3 step 2/3 — real poll cycle: write self slot, read
		 * matrix, decide quorum, publish shmem.  Counter bumps live
		 * inside qvotec_poll_once. */
		INSTR_TIME_SET_CURRENT(cycle_started);
		qvotec_poll_pre_injection();
		qvotec_poll_once();
		INSTR_TIME_SET_CURRENT(cycle_finished);
		INSTR_TIME_SUBTRACT(cycle_finished, cycle_started);
		cluster_pgstat_inc(qvotec_counter_poll_cycle);

		timeout_ms = qvotec_poll_wait_timeout_ms(
			INSTR_TIME_GET_MICROSEC(cycle_finished),
			cluster_quorum_poll_interval_ms);

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
	if (!cluster_reconfig_qvotec_lifecycle_transition(
			&QvotecShmem->mailbox, &QvotecShmem->state,
			CLUSTER_QVOTEC_DOWN))
		ereport(FATAL, (errmsg("qvotec could not publish DOWN lifecycle cut")));

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

	(void)cluster_reconfig_qvotec_lifecycle_transition(
		&QvotecShmem->mailbox, &QvotecShmem->state,
		CLUSTER_QVOTEC_SHUTTING_DOWN);
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
int
cluster_qvotec_get_status(void)
{
	return (int)CLUSTER_QVOTEC_DOWN;
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
void
cluster_qvotec_publish_self_incarnation(uint64 incarnation pg_attribute_unused())
{}
uint64
cluster_qvotec_get_self_incarnation(void)
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
