/*-------------------------------------------------------------------------
 *
 * cluster_qvotec.h
 *	  pgrac QVOTEC (Quorum Voting Coordinator) — spec-2.6.
 *
 *	  QVOTEC is the 6th cluster aux process (continuing LMON/LCK/DIAG/
 *	  Stats/CSSD).  Polls voting disks on shared storage to derive a
 *	  cluster-wide quorum view; publishes ClusterQvotecShmem.quorum_
 *	  state + Q4 v0.2 lease so the xact.c commit-boundary fail-closed
 *	  gate can enforce on every backend without a per-cycle broadcast.
 *	  spec-2.28 Fence-lite uses those quorum_state transitions from LMON to
 *	  broadcast ProcSignal freeze/thaw for early in-flight transaction aborts.
 *	  The QVOTEC lease + commit gate remain the authoritative durable-write
 *	  predicate.
 *	  Implements spec-2.0 §3 Invariant 1 (no-quorum no dual-write
 *	  fail-closed) + Invariant 3 (uncertainty → fail-closed).
 *
 *	  spec-2.6 v0.2 architecture (key Q decisions):
 *
 *	    Q1 6th aux process — cluster_qvotec is its own aux process, not
 *	    piggybacking on CSSD/LMON, because voting disk I/O is latency-
 *	    sensitive (sync write to shared storage) and would otherwise
 *	    interfere with CSSD heartbeat tick.
 *
 *	    Q2 v0.2 O_DIRECT best-effort + correctness via generation +
 *	    CRC32C + torn-write detection — the slot does NOT rely on
 *	    sector-atomic write guarantee (POSIX does not guarantee that
 *	    on NFS / iSCSI / cloud block).  Each write increments
 *	    generation;readers verify CRC and treat any mismatch as a
 *	    torn or corrupt slot for this cycle (multi-disk redundancy +
 *	    multi-cycle convergence absorb the brief uncertainty).
 *
 *	    Q4 v0.2 lease-based in_quorum — the public helper
 *	    cluster_qvotec_in_quorum() returns true ONLY when
 *	    quorum_state == OK *and* now < lease_expire_at_us.  This
 *	    defends against qvotec hung / I/O stuck for > 2 × poll
 *	    interval — backends fail-closed without depending on
 *	    ProcSignal arriving promptly.
 *
 *	    Q5 v0.2 fail-closed boundary check + independent SQLSTATE —
 *	    v0.14.0 implements the COMMIT-boundary check only(xact.c
 *	    CommitTransaction).  The write-intent early-reject hook at
 *	    INSERT/UPDATE/DELETE/DDL entry is part of the spec Q5 v0.2
 *	    contract but is deferred to Hardening v0.4+ follow-up;
 *	    correctness is preserved by the Q4 lease + commit-boundary
 *	    check(no committed write can outlive a lost-quorum decision).
 *	    The error is ERRCODE_CLUSTER_QUORUM_LOST(53R40),NOT aliased
 *	    to ERRCODE_T_R_SERIALIZATION_FAILURE(40001).
 *
 *	    Q6 v0.2 newer-self-FATAL collision detection — when
 *	    self.incarnation > slot.incarnation we detected an older
 *	    serving instance with the same node_id;self FATALs to let
 *	    the older instance's in-flight transactions / cached buffers
 *	    keep serving.
 *
 *	    Q7 v0.2 multi-node + voting_disks="" → postmaster startup
 *	    FATAL (production cannot silently fail-open;cluster.allow_
 *	    single_node=on is the dev/single-instance escape).
 *
 *	    Q10 failure domain validation — postmaster startup WARNING
 *	    when voting disks share a parent dirname (best-effort
 *	    heuristic + errhint pointing to documentation requirement).
 *
 *	  Surface (Sprint A Step 1 — D1):
 *	    - 4 enums (QvotecStatus / QuorumState / VotingDiskIoState /
 *	      CollisionDetectionState)
 *	    - 512-byte ClusterVotingSlot disk-resident layout (with
 *	      generation counter + CRC32C for torn-write detection)
 *	    - 128-byte ClusterQvotecShmem (lease-protected quorum state)
 *	    - 7 lifecycle / dump key accessors (per F11)
 *	    - cluster_qvotec_in_quorum() backend hot-path helper
 *	    - cluster_freeze_writes_set / _thaw_writes_set / _currently_frozen
 *	    - ClusterQvotecMain entry
 *	    - shmem_size / shmem_init / shmem_register
 *
 *	  Following step 1 deliverables (Step 2+):
 *	    Step 2: D3+D4 voting_disk_io + quorum_decision modules
 *	    Step 3: D5+D6+D7+D8 PG-original mods (procsignal +
 *	            postgres + postmaster + phase 4 driver)
 *	    Step 4: D9-D14 shmem register + counters + wait events +
 *	            GUCs + SQLSTATE + inject + view
 *	    Step 5: D15 (SRF/view) + D17+D18 cluster_unit + D19 095 TAP +
 *	            D20 096 TAP 2-node + D21 cluster_regress + D22-D24
 *	            linkdb manual
 *	    Step 6: ship — linkdb tag v0.14.0-stage2.6 + nightly verify
 *	    Step 7: post-ship 24-48h review window (per Q9 v0.2)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_qvotec.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER);declarations are gated.
 *
 *	  Spec authority: pgrac:specs/spec-2.6-voting-disk-quorum-lite.md
 *	  (frozen v0.2 2026-05-09 Q1-Q10 user approve).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_QVOTEC_H
#define CLUSTER_QVOTEC_H

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */


/*
 * CLUSTER_VOTING_SLOT_MAGIC — 'QVOT' little-endian sentinel in slot
 * byte 0..3.  Used by readers to reject random-data sectors / wrong-
 * file reads / pre-init slots.
 */
#define CLUSTER_VOTING_SLOT_MAGIC 0x51564f54 /* 'QVOT' */
#define CLUSTER_VOTING_SLOT_VERSION 1

/* slot.flags bit definitions */
#define CLUSTER_VOTING_SLOT_FLAG_ALIVE (1ULL << 0)
#define CLUSTER_VOTING_SLOT_FLAG_WRITE_FROZEN (1ULL << 1)

/*
 * CLUSTER_MAX_VOTING_DISKS — compile-time upper bound on cluster.
 * voting_disks count.  Stage 2.6 supports 1..7 (Oracle-aligned
 * default 3; recommended odd-majority cardinalities are 1 / 3 / 5 / 7
 * — see qvotec_open_disks errhint and cluster_startup_phase Q10
 * validator wording).  Larger values raise quorum_size linearly with
 * no proven HA benefit at this stage and are bounded by this define.
 *
 * Hardening v0.6 F5:  prior versions had two divergent constants
 * (CLUSTER_VOTING_DISKS_MAX = 5 in this header, CLUSTER_MAX_VOTING_
 * DISKS = 9 in cluster_qvotec.c).  Unified to one name + one value
 * matching the documented 1/3/5/7 recommendation.
 */
#define CLUSTER_MAX_VOTING_DISKS 7


/* ----------
 * QvotecStatus — qvotec aux process lifecycle state (Sprint A Step 1).
 *
 *	Mirrors LMON / LCK / DIAG / Stats / CSSD 5-state pattern (per
 *	spec-1.11 HC2).  Numeric values are observable via SQL views
 *	(Step 4+);preserve the 0..4 mapping when amending.
 * ---------- */
typedef enum ClusterQvotecStatus {
	CLUSTER_QVOTEC_STARTING = 0,	  /* postmaster spawned;main not yet active */
	CLUSTER_QVOTEC_READY = 1,		  /* main loop active;phase 4 driver may advance */
	CLUSTER_QVOTEC_SHUTTING_DOWN = 2, /* shutdown_requested set;qvotec exiting */
	CLUSTER_QVOTEC_DOWN = 3,		  /* clean exit complete */
	CLUSTER_QVOTEC_FAILED = 4		  /* unexpected exit;reaper will respawn */
} ClusterQvotecStatus;


/* ----------
 * QuorumState — cluster-wide quorum decision (lease-protected;Q4 v0.2).
 *
 *	Backend helper cluster_qvotec_in_quorum() returns true ONLY for
 *	OK + not-expired-lease.  The other states all represent fail-
 *	closed conditions from the backend's POV.
 * ---------- */
typedef enum ClusterQvotecQuorumState {
	CLUSTER_QVOTEC_QUORUM_INITIALIZING = 0, /* qvotec just spawned, no poll yet */
	CLUSTER_QVOTEC_QUORUM_OK = 1,			/* majority reached, in_quorum + lease live */
	CLUSTER_QVOTEC_QUORUM_UNCERTAIN = 2,	/* < majority disks ok, OR poll inflight */
	CLUSTER_QVOTEC_QUORUM_LOST = 3			/* 0 disks ok / explicit quorum loss */
} ClusterQvotecQuorumState;


/* ----------
 * VotingDiskIoState — per-disk health in shmem.
 * ---------- */
typedef enum ClusterVotingDiskIoState {
	CLUSTER_VOTING_DISK_IO_OK = 0,
	CLUSTER_VOTING_DISK_IO_TORN = 1,	 /* CRC mismatch detected this cycle */
	CLUSTER_VOTING_DISK_IO_FAILED = 2,	 /* persistent EIO / EOF */
	CLUSTER_VOTING_DISK_IO_NOT_TRIED = 3 /* qvotec hasn't polled yet */
} ClusterVotingDiskIoState;


/* ----------
 * CollisionDetectionState — qvotec's view of node_id collision.
 * ---------- */
typedef enum ClusterCollisionDetectionState {
	CLUSTER_COLLISION_NONE = 0,			   /* default */
	CLUSTER_COLLISION_OBSERVED_OLDER = 1,  /* slot has lower incarnation;self continues */
	CLUSTER_COLLISION_FATAL_NEWER_SELF = 2 /* about to FATAL self;Q6 v0.2 */
} ClusterCollisionDetectionState;


/* ----------
 * ClusterVotingSlot — 512-byte disk-resident per-instance slot.
 *
 *	Layout matrix (offset / size / field):
 *	   0..3   uint32  magic            = 0x51564f54 ("QVOT")
 *	   4..7   uint32  version          = CLUSTER_VOTING_SLOT_VERSION
 *	   8..11  uint32  node_id          (instance node_id from cluster_guc)
 *	  12..15  uint32  _pad0            = 0
 *	  16..23  uint64  incarnation      (boot session id;reset each postmaster start)
 *	  24..31  uint64  heartbeat_ts_us  (CLOCK_REALTIME microseconds at last write)
 *	  32..39  uint64  current_epoch    (membership epoch from cluster_epoch shmem)
 *	  40..47  uint64  flags            (bit0=alive bit1=write_frozen bit2-63=reserved)
 *	  48..51  uint32  disk_index       (which voting disk this slot is on;0..N-1)
 *	  52..55  uint32  _pad1            = 0
 *	  56..63  uint64  generation       (Q2 v0.2 — torn-write detect counter)
 *	  64..127 uint8[64] _alive_bitmap  (per-instance alive view from this writer's POV)
 *	 128..495 uint8[368] _reserved1    = 0  (future expansion)
 *	 496..507 uint8[12]  _reserved2    = 0
 *	 508..511 uint32  crc32c           (covers offsets 0..507)
 *
 *	StaticAssertDecl(sizeof(ClusterVotingSlot) == 512).
 *
 *	Q2 v0.2 correctness protocol:
 *	  Write side:  read current generation;write slot with generation
 *	    + 1, fields, CRC32C of byte 0..507;fdatasync.
 *	  Read side:   pread 512;verify CRC32C;mismatch ⇒ mark
 *	    disk_io_failure_inflight + retry next cycle (treat as torn or
 *	    corrupt — multi-disk redundancy absorbs this).
 *
 *	The slot is sized exactly one disk sector (512 byte) so well-behaved
 *	storage backends (SCSI, NVMe, SAS) tend toward sector-atomic writes,
 *	but we do NOT rely on that property — generation + CRC + retry is
 *	the correctness mechanism.
 * ---------- */
typedef struct ClusterVotingSlot {
	uint32 magic;
	uint32 version;
	uint32 node_id;
	uint32 _pad0;
	uint64 incarnation;
	uint64 heartbeat_ts_us;
	uint64 current_epoch;
	uint64 flags;
	uint32 disk_index;
	uint32 _pad1;
	uint64 generation;
	uint8 _alive_bitmap[64];
	uint8 _reserved1[368];
	uint8 _reserved2[12];
	uint32 crc32c;
} ClusterVotingSlot;

#ifdef USE_PGRAC_CLUSTER
StaticAssertDecl(sizeof(ClusterVotingSlot) == 512, "ClusterVotingSlot must be exactly 512 bytes");
StaticAssertDecl(offsetof(ClusterVotingSlot, magic) == 0,
				 "ClusterVotingSlot.magic must be at offset 0");
StaticAssertDecl(offsetof(ClusterVotingSlot, node_id) == 8,
				 "ClusterVotingSlot.node_id must be at offset 8");
StaticAssertDecl(offsetof(ClusterVotingSlot, incarnation) == 16,
				 "ClusterVotingSlot.incarnation must be at offset 16");
StaticAssertDecl(offsetof(ClusterVotingSlot, heartbeat_ts_us) == 24,
				 "ClusterVotingSlot.heartbeat_ts_us must be at offset 24");
StaticAssertDecl(offsetof(ClusterVotingSlot, current_epoch) == 32,
				 "ClusterVotingSlot.current_epoch must be at offset 32");
StaticAssertDecl(offsetof(ClusterVotingSlot, flags) == 40,
				 "ClusterVotingSlot.flags must be at offset 40");
StaticAssertDecl(offsetof(ClusterVotingSlot, generation) == 56,
				 "ClusterVotingSlot.generation must be at offset 56");
StaticAssertDecl(offsetof(ClusterVotingSlot, _alive_bitmap) == 64,
				 "ClusterVotingSlot._alive_bitmap must be at offset 64");
StaticAssertDecl(offsetof(ClusterVotingSlot, crc32c) == 508,
				 "ClusterVotingSlot.crc32c must be at offset 508");
#endif


#ifdef USE_PGRAC_CLUSTER

/* ----------
 * cluster_qvotec_shmem_size / _init / _register — shmem region API.
 *
 *	Mirrors cluster_epoch / cluster_diag / cluster_cssd pattern;
 *	registered from cluster_shmem.c (D9).  ClusterQvotecShmem layout
 *	is private to cluster_qvotec.c (128-byte cache-line aligned).
 * ---------- */
extern Size cluster_qvotec_shmem_size(void);
extern void cluster_qvotec_shmem_init(void);
extern void cluster_qvotec_shmem_register(void);


/* ----------
 * 7 lifecycle / dump-key accessors (per F11 mandatory 7-key dump).
 *
 *	These back the pg_cluster_state.qvotec.* keys (D15 — Step 5).  All
 *	are NULL-safe pre-shmem (return defaults) so cluster_unit harness
 *	links cleanly without driving full shmem init.
 * ---------- */
extern int cluster_qvotec_get_pid(void);
extern const char *cluster_qvotec_get_status_name(void);
extern const char *cluster_qvotec_get_quorum_state_name(void);

/*
 * cluster_qvotec_get_quorum_state — raw enum read for Step 3 LMON
 *	consumer (spec-2.28 §3.0 I1 freeze-immediate / §3.7 dual-set).
 *	LMON tick calls this each iteration to detect OK→LOST transition
 *	and trigger cluster_fence_broadcast_freeze.  Returns the raw
 *	ClusterQvotecQuorumState enum value (cast to int for header
 *	cleanliness), or CLUSTER_QVOTEC_QUORUM_INITIALIZING (0) when
 *	shmem not yet initialised.  NOT lease-aware (in_quorum() is the
 *	commit-gate path); this is for state-transition observation.
 */
extern int cluster_qvotec_get_quorum_state(void);

extern int cluster_qvotec_get_disks_ok_count(void);
extern int cluster_qvotec_get_disks_total_count(void);
extern uint64 cluster_qvotec_get_current_epoch_at_boot(void);
/* Canonical boot session published by QVOTEC into its existing shared-memory
 * region.  QVOTEC is the sole writer; LMS/backends are read-only consumers. */
extern void cluster_qvotec_publish_self_incarnation(uint64 incarnation);
extern uint64 cluster_qvotec_get_self_incarnation(void);
extern const char *cluster_qvotec_get_collision_state_name(void);


/* ----------
 * cluster_qvotec_in_quorum — backend hot-path helper (Q4 v0.2 lease-aware).
 *
 *	Returns true ONLY when (a) shmem live, (b) quorum_state == OK,
 *	(c) now < lease_expire_at_us (qvotec polled within 2 × poll_interval).
 *	Any other state — INITIALIZING / UNCERTAIN / LOST / lease expired
 *	/ shmem absent — returns false → backend fail-closed.
 *
 *	Cost: 3 atomic loads + 1 GetCurrentTimestamp() call (~50ns).
 *	v0.14.0 callers:  CommitTransaction (commit-boundary check;xact.c
 *	D6).  Spec Q5 v0.2 also calls for a write-intent boundary check at
 *	INSERT/UPDATE/DELETE/DDL entry, but that path is deferred to
 *	Hardening v0.4+ — correctness is preserved because no write that
 *	survives Q4 lease expiry can pass the commit gate.
 * ---------- */
extern bool cluster_qvotec_in_quorum(void);
/* Shape A (crash-rejoin re-declare barrier): prior-incarnation self-slot
 * carried ALIVE at startup => this boot follows an UNCLEAN death. */
extern bool cluster_qvotec_prior_unclean_death(void);


/* ----------
 * ProcSignal hook helpers — process-local atomic flag setters.
 *
 *	Set by PROCSIG_CLUSTER_FREEZE_WRITES / _THAW_WRITES handler in
 *	procsignal.c (D5 wired);read by cluster_qvotec_in_quorum() and
 *	cluster_writes_currently_frozen().
 *
 *	Sender side is LMON-mediated as of spec-2.28: LMON observes QVOTEC
 *	quorum_state transitions and broadcasts PROCSIG_CLUSTER_FREEZE_WRITES /
 *	_THAW_WRITES.  The Q4 v0.2 lease + commit-boundary check remain the
 *	authoritative durable-write predicate.
 * ---------- */
extern void cluster_freeze_writes_set(void);
extern void cluster_thaw_writes_set(void);
extern bool cluster_writes_currently_frozen(void);


/* ----------
 * Lifecycle entry point — postmaster reaper invokes this after
 * fork() under AuxProcType QvotecProcess (D7 — Step 3).
 * ---------- */
extern void ClusterQvotecMain(void) pg_attribute_noreturn();


/* ----------
 * Standalone control helpers (used by phase 4 driver — D8 Step 3).
 *
 *	Wait for qvotec to reach READY / SHUTTING_DOWN per phase 4
 *	advance gates;wait_for_ready returns true on success, false on
 *	timeout.
 * ---------- */
extern bool cluster_qvotec_wait_for_ready(int timeout_ms);
extern void cluster_qvotec_request_shutdown(void);

/* Postmaster spawn wrapper (defined in postmaster/postmaster.c — Step 3 D7).
 * cluster_qvotec_start() (defined in cluster_qvotec.c) forwards here. */
extern pid_t cluster_qvotec_start(void);
extern pid_t cluster_postmaster_start_qvotec(void);

#endif /* USE_PGRAC_CLUSTER */


#endif /* CLUSTER_QVOTEC_H */
