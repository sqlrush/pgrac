/*-------------------------------------------------------------------------
 *
 * cluster_cssd.h
 *	  pgrac CSSD (Cluster Synchronization Service Daemon) — spec-2.5.
 *
 *	  CSSD is the 5th cluster aux process (continuing LMON/LCK/DIAG/Stats);
 *	  application-level peer dead detection via heartbeat broadcast.  Two-
 *	  layer dead detection paired with spec-2.4 D8 socket-level TCP
 *	  KeepAlive: TCP-keepalive catches "socket closed but undetected"
 *	  failure mode (~120s); CSSD heartbeat catches "peer process stuck
 *	  but socket alive" mode (~3s default).
 *
 *	  spec-2.5 v0.2 architecture (3 hard prerequisites):
 *
 *	    Q1 (L61 process-resource-vs-shmem):  CSSD aux process does NOT
 *	    hold tier1 TCP fd directly (LMON-only kernel resource).  CSSD
 *	    writes heartbeat into ClusterCssdOutboundSlot[] shmem; LMON tick
 *	    drains and dispatches via single-peer send.
 *
 *	    Q3 (L71 metadata-symmetric-enforce): broadcast goes through
 *	    the explicit cluster_ic_send_envelope_fanout() API (spec-2.5
 *	    D2.5; first deliverable in Sprint A).  CSSD logically requests
 *	    "send to all declared peers" via N writes to outbound queue;
 *	    LMON drain picks one peer per slot and calls single-peer send
 *	    (not the fanout API itself, since LMON is already in per-peer
 *	    iteration).
 *
 *	    Q6 (first-tick nonfatal + grace period):  phase 4 spawn does
 *	    not guarantee peer connect / HELLO handshake complete.  CSSD
 *	    first-tick send PEER_DOWN/WOULD_BLOCK is nonfatal; deadband
 *	    scan during first-tick grace period (= 1 deadband interval,
 *	    default 3s) skips SUSPECTED/DEAD transitions to avoid spawn-
 *	    immediate-reconfig storm.
 *
 *	  Surface (Sprint A Step 2 — D1):
 *	    - 5-value ClusterCssdStatus enum (STARTING / READY /
 *	      SHUTTING_DOWN / DOWN / FAILED)
 *	    - 3-value ClusterCssdPeerState enum (ALIVE / SUSPECTED / DEAD)
 *	    - 12-byte ClusterCssdHeartbeatPayload (cssd_seq + sender_local_clock)
 *	    - 64-byte ClusterCssdOutboundSlot (CAS lifecycle 0→1→2→3→0)
 *	    - per-peer state shmem (state + counters + last_recv_at)
 *	    - 9 shmem accessors (mirrors spec-1.11.1 F11 7-key + 2 cssd-specific)
 *	    - per-peer state read accessor
 *	    - status / state name lookup helpers
 *	    - CssdMain entry + dispatch_heartbeat handler
 *	    - shmem_size / shmem_init / shmem_register
 *	    - lifecycle (start / wait_for_ready / request_shutdown)
 *
 *	  Following step 2 deliverables (Step 3+):
 *	    Step 3: D2.6 LMON tick drain CSSD outbound queue
 *	    Step 4: D3+D4+D5 AuxProcType + postmaster spawn + phase 4
 *	    Step 5: D6+D7+D8+D9+D10+D11+D12+D13+D14
 *	      D6 cluster_shmem.c register
 *	      D7 LWTRANCHE_CLUSTER_CSSD
 *	      D8 wait event (WAIT_EVENT_CLUSTER_BGPROC_CSSD_MAIN_LOOP)
 *	      D9 3 GUC (cssd_main_loop_interval_ms / heartbeat_interval_ms /
 *	         dead_deadband_factor)
 *	      D10 4 SQLSTATE (53R30 spawn / 53R31 not_ready / 53R32 suspected /
 *	          53R33 dead)
 *	      D11 6 inject points
 *	      D12 phase 1 register msg_type=11 PGRAC_IC_MSG_CSSD_HEARTBEAT
 *	      D13 CLUSTER_IC_PRODUCER_CSSD producer mask
 *	      D14 4 per-peer counter (cssd_*)
 *	    Step 6: D15 SRF/view + D16 catversion + D20 cssd_smoke
 *	    Step 7: D18 065 + D19 085 TAP
 *	    Step 8: ship — linkdb tag v0.12.0-stage2.5
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cssd.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); declarations are gated.
 *
 *	  Spec authority: pgrac:specs/spec-2.5-cssd-heartbeat-skeleton.md
 *	  (frozen v0.2 2026-05-08).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CSSD_H
#define CLUSTER_CSSD_H

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

#include "cluster/cluster_conf.h"		 /* CLUSTER_MAX_NODES */
#include "cluster/cluster_ic_envelope.h" /* ClusterICEnvelope (handler arg) */


/*
 * PGRAC_IC_MSG_CSSD_HEARTBEAT -- cluster IC msg_type slot reserved for
 * CSSD application-level heartbeat broadcast (spec-2.5 D12).
 *
 * Registered in postmaster phase 1 (alongside spec-2.2 HEARTBEAT msg_type
 * = 1) via cluster_ic_register_msg_type with broadcast_ok=true and
 * allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON (LMON drains CSSD
 * outbound queue and is the actual sender).
 */
#define PGRAC_IC_MSG_CSSD_HEARTBEAT ((uint8)11)


/*
 * ClusterCssdStatus -- HC2 SSOT for CSSD aux process lifecycle state.
 *
 *	Mirrors LMON / LCK / DIAG / Stats 5-state pattern (per spec-1.11
 *	HC2).  Numeric values are observable via SQL views (Step 5+);
 *	preserve the existing 0..4 mapping when amending.
 */
typedef enum ClusterCssdStatus {
	CLUSTER_CSSD_STARTING = 0,		/* postmaster spawned; CssdMain not yet active */
	CLUSTER_CSSD_READY = 1,			/* main loop active; phase 4 driver may advance */
	CLUSTER_CSSD_SHUTTING_DOWN = 2, /* shutdown_requested set; CSSD exiting */
	CLUSTER_CSSD_DOWN = 3,			/* proc_exit complete; postmaster reaper to harvest */
	CLUSTER_CSSD_FAILED = 4			/* HandleChildCrash hit; restart_after_crash decides */
} ClusterCssdStatus;

#define CLUSTER_CSSD_STATUS_LAST CLUSTER_CSSD_FAILED


/*
 * ClusterCssdPeerState -- per-peer dead-detection state (3-stage
 * hysteresis per spec-2.5 Q5 ★ B).
 *
 *	ALIVE → SUSPECTED at 2× heartbeat_interval (LOG 53R32 advisory)
 *	SUSPECTED → DEAD at 3× heartbeat_interval (WARNING 53R33;
 *	                                            NO reconfig trigger
 *	                                            in spec-2.5)
 *	DEAD / SUSPECTED → ALIVE on heartbeat recv (hysteresis recovery)
 *
 *	Numeric values exposed via pg_cluster_cssd_peers view (Step 6).
 */
typedef enum ClusterCssdPeerState {
	CLUSTER_CSSD_PEER_ALIVE = 0,
	CLUSTER_CSSD_PEER_SUSPECTED = 1,
	CLUSTER_CSSD_PEER_DEAD = 2
} ClusterCssdPeerState;

#define CLUSTER_CSSD_PEER_STATE_LAST CLUSTER_CSSD_PEER_DEAD


/*
 * ClusterCssdHeartbeatPayload -- 12-byte heartbeat wire format.
 *
 *	Wire format (packed) — 12 bytes total.  Without pg_attribute_packed
 *	the compiler would round struct size up to 16 (uint64 forces 8-byte
 *	struct alignment).  Per L67 cppcheck-cannot-expand-PG-attribute-
 *	macros: cluster_cssd.h needs a syntaxError suppression entry in
 *	scripts/ci/cppcheck-suppressions.txt.  StaticAssertDecl in
 *	implementation locks size at compile-time.
 *
 *	sender_local_clock : GetCurrentTimestamp at send (TimestampTz
 *	                     = int64 microseconds since 2000-01-01).
 *	                     Diagnostic only; receivers use their own
 *	                     local clock for deadband scan, NOT this
 *	                     value (clock skew safe).
 *	cssd_seq           : per-sender monotonic counter (detect dup /
 *	                     reorder on receiver side; diagnostic).
 */
typedef struct pg_attribute_packed() ClusterCssdHeartbeatPayload
{
	uint64 sender_local_clock;
	uint32 cssd_seq;
}
ClusterCssdHeartbeatPayload;


/*
 * ClusterCssdOutboundSlot -- per-peer outbound request slot for LMON-
 * mediated send (spec-2.5 §2.2.1 + Q1 修订).
 *
 *	State machine (pending field; pg_atomic_uint32):
 *	    0 → idle (CSSD may write next)
 *	    1 → CSSD writing (payload + request_seq); CAS publish to 2
 *	    2 → ready for LMON drain
 *	    3 → LMON draining (write result_state + result_seq +
 *	         result_at_us); CAS publish to 0
 *
 *	Single producer (CSSD MainLoop) + single consumer (LMON tick) =
 *	no LWLock needed.  64-byte cache-line alignment defends against
 *	false sharing across peers.
 */
typedef struct ClusterCssdOutboundSlot {
	/* Layout chosen to give the uint64 result_at_us natural 8-byte
	 * alignment without compiler-inserted padding;total exactly 64
	 * bytes (cache-line aligned). */
	pg_atomic_uint32 pending;			 /* state machine 0/1/2/3 (offset 0) */
	pg_atomic_uint32 result_state;		 /* ClusterICFanoutResult enum (offset 4) */
	uint32 request_seq;					 /* CSSD-monotonic (offset 8) */
	uint32 result_seq;					 /* LMON echo (offset 12) */
	uint64 result_at_us;				 /* GetCurrentTimestamp (offset 16, 8B aligned) */
	ClusterCssdHeartbeatPayload payload; /* 12 B packed (offset 24) */
	char _pad[28];						 /* 64 - 24 - 12 = 28 (offset 36..63) */
} ClusterCssdOutboundSlot;


/*
 * ClusterCssdPeerStateShmem -- per-peer dead-detection state.
 *
 *	state                       : ClusterCssdPeerState 3-stage hysteresis
 *	last_heartbeat_recv_at_us   : TimestampTz of last accepted recv (atomic
 *	                              uint64 for handler 4-硬约束 — handler
 *	                              writes here without LWLock)
 *	last_heartbeat_send_at_us   : TimestampTz of last DONE send (LMON-
 *	                              drained;CSSD updates after reading
 *	                              result_state)
 *	heartbeat_send_count        : per-peer DONE count (atomic)
 *	heartbeat_recv_count        : per-peer accepted recv count (atomic)
 *	suspected_transitions       : ALIVE → SUSPECTED counter (atomic)
 *	dead_transitions            : SUSPECTED → DEAD counter (atomic)
 *	suspected_since_us          : TimestampTz of last ALIVE → SUSPECTED
 *	                              (NULL semantics: 0)
 *	dead_since_us               : TimestampTz of last SUSPECTED → DEAD
 *	                              (NULL semantics: 0)
 */
typedef struct ClusterCssdPeerStateShmem {
	pg_atomic_uint32 state;
	pg_atomic_uint64 last_heartbeat_recv_at_us;
	pg_atomic_uint64 last_heartbeat_send_at_us;
	pg_atomic_uint64 heartbeat_send_count;
	pg_atomic_uint64 heartbeat_recv_count;
	pg_atomic_uint64 suspected_transitions;
	pg_atomic_uint64 dead_transitions;
	pg_atomic_uint64 suspected_since_us;
	pg_atomic_uint64 dead_since_us;
} ClusterCssdPeerStateShmem;


/*
 * ClusterCssdShmem -- top-level CSSD shmem region.
 *
 *	Sub-fields are atomic-only (single-writer / single-reader patterns
 *	per field) OR LWLock-guarded (lwlock field for status + lifecycle
 *	timestamps).  Per-peer state + outbound queue are atomic without
 *	LWLock (single producer / single consumer per peer).
 */
typedef struct ClusterCssdShmem {
	LWLock lwlock; /* LWTRANCHE_CLUSTER_CSSD; guards lifecycle below */
	ClusterCssdStatus status;
	pid_t pid;
	TimestampTz spawned_at;
	TimestampTz ready_at;
	TimestampTz last_liveness_tick_at;
	int64 main_loop_iters;
	bool shutdown_requested;

	/* spec-2.5 Q6 first-tick grace period: deadband-scan does not
	 * trigger SUSPECTED/DEAD transitions until now > grace_until_us.
	 * = ready_at + (cssd_dead_deadband_factor × cssd_heartbeat_interval_ms × 1000) */
	uint64 first_tick_grace_until_us;

	/* Aggregate counters (across all peers). */
	pg_atomic_uint64 total_heartbeat_send_count;
	pg_atomic_uint64 total_heartbeat_recv_count;

	/* spec-2.29 D19: per-instance monotonic dead_generation counter,
	 * bumped on every ALIVE↔SUSPECTED↔DEAD peer transition.  Used by
	 * cluster_reconfig event_id hash dedup (P1.2 fix) to disambiguate
	 * same-bitmap re-deaths after rejoin. */
	pg_atomic_uint64 dead_generation;

	/* Per-peer state (CLUSTER_MAX_NODES = 128). */
	ClusterCssdPeerStateShmem peers[CLUSTER_MAX_NODES];

	/* Outbound queue (single producer CSSD; single consumer LMON). */
	ClusterCssdOutboundSlot outbound[CLUSTER_MAX_NODES];
} ClusterCssdShmem;


/*
 * Status / state name lookup helpers.
 *	Out-of-range returns "(unknown)".
 */
extern const char *cluster_cssd_status_to_string(ClusterCssdStatus s);
extern const char *cluster_cssd_peer_state_to_string(ClusterCssdPeerState s);


/*
 * Shmem region helpers — registered by cluster_init_shmem_module() via
 * the spec-1.3 region registry (Step 5 D6).
 */
extern Size cluster_cssd_shmem_size(void);
extern void cluster_cssd_shmem_init(void);
extern void cluster_cssd_shmem_register(void);


/*
 * Lifecycle accessors (LW_SHARED).
 */
extern pid_t cluster_cssd_get_pid(void);
extern TimestampTz cluster_cssd_get_spawned_at(void);
extern TimestampTz cluster_cssd_get_ready_at(void);
extern TimestampTz cluster_cssd_get_last_liveness_tick_at(void);
extern uint64 cluster_cssd_get_main_loop_iters(void);
extern ClusterCssdStatus cluster_cssd_get_status(void);
extern uint64 cluster_cssd_get_total_heartbeat_send_count(void);
extern uint64 cluster_cssd_get_total_heartbeat_recv_count(void);
extern int cluster_cssd_get_alive_peer_count(void);

/*
 * spec-2.29 D19: cluster_cssd_get_dead_generation
 *
 *	  Monotonic counter incremented on every peer state transition
 *	  (ALIVE↔SUSPECTED↔DEAD) detected by deadband_scan_tick.  Used
 *	  by cluster_reconfig event_id hash dedup (per spec-2.29 §3.2
 *	  + I3 event-loss-tolerant + P1.2 fix) to distinguish:
 *
 *	    - same dead_bitmap within one continuous DEAD episode →
 *	      same dead_generation → same event_id → dedup skip
 *	    - same dead_bitmap across rejoin-then-redeath →
 *	      different dead_generation → different event_id → re-fire
 *
 *	  Counter is per-instance (shmem-local;each instance's CSSD
 *	  observer maintains its own view).  Cross-instance convergence
 *	  is by Lamport piggyback of (event_id, dead_bitmap) — not by
 *	  cross-instance shared dead_generation.
 */
extern uint64 cluster_cssd_get_dead_generation(void);


/*
 * Per-peer state accessors (used by SRF + diagnostic;
 * range-checked + shmem-NULL-safe).
 */
extern ClusterCssdPeerState cluster_cssd_get_peer_state(int32 peer_id);
extern TimestampTz cluster_cssd_get_peer_last_recv_at(int32 peer_id);


/*
 * Outbound queue accessor (used by LMON tick drain in Step 3 D2.6).
 *	Returns NULL if shmem not yet initialized.
 */
extern ClusterCssdOutboundSlot *cluster_cssd_outbound_slots(void);


/*
 * Process entry point (called from auxprocess.c switch in Step 4 D3).
 */
extern void CssdMain(void) pg_attribute_noreturn();


/*
 * msg_type=11 dispatch handler (registered phase 1 in Step 5 D12).
 *	Handler 4 hard constraints (per spec-2.3 §3.5):
 *	  - nonblocking (no LWLockAcquire wait)
 *	  - no catalog SQL
 *	  - no ereport ERROR/FATAL/PANIC
 *	  - bounded work (atomic update only)
 */
extern void cluster_cssd_dispatch_heartbeat(const ClusterICEnvelope *env, const void *payload);


/*
 * Lifecycle (Step 4 D4).
 */
extern int cluster_cssd_start(void);
extern bool cluster_cssd_wait_for_ready(int timeout_ms);
extern void cluster_cssd_request_shutdown(void);


/*
 * spec-2.5 D14 stubs (real impl in Step 5).  Counter bumpers used by
 * dispatch_heartbeat handler + LMON drain;keep here as forward-decls
 * so D2 + cluster_unit can link.
 */
extern void cluster_cssd_bump_send_count(int32 peer_id);
extern void cluster_cssd_bump_recv_count(int32 peer_id);
extern void cluster_cssd_bump_suspected_transition(int32 peer_id);
extern void cluster_cssd_bump_dead_transition(int32 peer_id);


#endif /* CLUSTER_CSSD_H */
