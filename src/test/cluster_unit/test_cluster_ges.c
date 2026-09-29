/*-------------------------------------------------------------------------
 *
 * test_cluster_ges.c
 *	  Standalone unit tests for spec-2.13 GES protocol skeleton.
 *
 *	  T-ges-1 (5 tests, spec-2.13 D6):
 *	    a) cluster_ges_request_handler / cluster_ges_reply_handler symbol
 *	       linkable (UT_ASSERT_NOT_NULL fn ptr).
 *	    b) cluster_ges_request_defer_count / cluster_ges_reply_defer_count
 *	       accessors linkable + initial read returns 0.
 *	    c) **真行为 — request stub**:  pre = cluster_ges_request_defer_count()
 *	       → invoke cluster_ges_request_handler(envelope_sentinel, NULL) →
 *	       assert (1) handler 静默返回 (no ERROR/FATAL abort), (2)
 *	       cluster_ges_request_defer_count() == pre + 1, (3)
 *	       cluster_ges_reply_defer_count() == pre_reply (unchanged).
 *	    d) **真行为 — reply stub**:  symmetric with (c) but reply path;
 *	       assert reply_defer_count +1 + request_defer_count unchanged.
 *	    e) handler 跨 multiple invocations 真测 monotonic non-decrease +
 *	       counter accuracy (handler 调用 N 次 → counter 真递增 N).
 *
 *	  Stubs:
 *	    - ShmemInitStruct returns a union force-aligned buffer per L105
 *	      (Apple silicon tolerates misaligned atomic but strict-alignment
 *	      platform ARM Linux / SPARC SIGBUS without union force-align).
 *	    - cluster_shmem_register_region: no-op (region 注册不真测).
 *	    - elog / ereport: stubbed pass-through (DEBUG2 from handler
 *	      should be silent in test runner).
 *
 *	  Spec: spec-2.13 D6 + Q5.2 + Q9 (L105 union force-align).
 *	  Cross-spec lesson inheritance: L94 / L105 / L106 / L107.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ges.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_ges.o only; all PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <stdlib.h> /* spec-5.8 D8 — malloc/free for the palloc/pfree stubs */
#include <string.h>

#include "access/transam.h" /* spec-5.8 D1c — InvalidTransactionId for the GetTopTransactionIdIfAny stub */
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_control_retire.h"
#include "miscadmin.h"
#include "cluster/cluster_touched_peers.h" /* spec-5.14 D2 stamp stub */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic.h" /* spec-5.8 D8 — ClusterICSendResult for the send-envelope stub */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_replacement_wire.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_cssd.h"		   /* spec-5.7 Direction B stub — peer state */
#include "cluster/cluster_extend_gate.h"   /* spec-5.7 Direction B stub — sole-native */
#include "cluster/cluster_inject.h"		   /* S3 forensics step 1a stub prototypes */
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D2 stub — profiling gate */
#include "port/atomics.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

/* PGRAC: these legacy protocol cases do not enter PRE2 retirement.
 * Its real service/master combinations have dedicated tests.
 * Author: SqlRush <sqlrush@gmail.com> */
BackendType MyBackendType = B_BACKEND;
bool cluster_shared_config;
static bool stub_retire_driver_enabled;
static ClusterControlRequestCut stub_retire_cut;
static int stub_retire_drain_budget;
static ClusterGrdGrantIdentity stub_drained_grant;
bool
cluster_control_retire_is_frame(const void *bytes pg_attribute_unused(),
								Size len pg_attribute_unused())
{
	return false;
}
bool
cluster_control_retire_cut(const ClusterResId *resid pg_attribute_unused(),
						   ClusterControlRequestCut *cut)
{
	if (stub_retire_driver_enabled) {
		*cut = stub_retire_cut;
		return true;
	}
	abort();
}
void
cluster_control_retire_drain(const ClusterGrdWorkItem *item pg_attribute_unused())
{
	abort();
}
void
cluster_control_retire_ingress(const ClusterICEnvelope *env pg_attribute_unused(),
							   const void *bytes pg_attribute_unused())
{
	abort();
}
bool
cluster_ges_dedup_retire_control_request(uint32 node pg_attribute_unused(),
										 uint32 proc pg_attribute_unused(),
										 uint64 epoch pg_attribute_unused(),
										 uint64 req pg_attribute_unused())
{
	if (stub_retire_driver_enabled)
		return true;
	abort();
}
bool
cluster_grd_control_acquire_allowed(const ClusterResId *resid pg_attribute_unused(),
									LOCKMODE mode pg_attribute_unused())
{
	return true;
}
bool
cluster_grd_control_recovery_ready(const ClusterResId *resid pg_attribute_unused(),
								   LOCKMODE mode pg_attribute_unused())
{
	return false;
}
int
cluster_grd_retire_request_and_drain(const ClusterResId *resid pg_attribute_unused(),
									 const ClusterGrdHolderId *holder pg_attribute_unused(),
									 uint64 previous pg_attribute_unused(),
									 LOCKMODE mode pg_attribute_unused(),
									 ClusterGrdGrantIdentity *granted, int max)
{
	if (stub_retire_driver_enabled) {
		stub_retire_drain_budget = max;
		if (max > 0) {
			granted[0] = stub_drained_grant;
			return 1;
		}
		return 0;
	}
	abort();
}


/* ============================================================
 * Stubs needed to link cluster_ges.o standalone.
 *
 *	ShmemInitStruct uses L105 union force-align pattern (mirror
 *	test_cluster_scn.c spec-2.11 P1.2 fix) — pg_atomic_uint64 must
 *	be 8-byte aligned on strict-alignment platforms.
 * ============================================================ */

bool IsUnderPostmaster = false;
static bool stub_lmd_ready;
static bool stub_stop_read_allowed = true;
static int stub_stop_read_calls, stub_report_sends;
bool cluster_normal_stop_service_new_work(bool modifies_data);
bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	if (modifies_data)
		abort();
	stub_stop_read_calls++;
	return stub_stop_read_allowed;
}
AuxProcType MyAuxProcType = NotAnAuxProcess;

/* S3 forensics step 1a stubs — cluster_ges.c now carries the
 * cluster-ges-master-work-queue-full injection point; this standalone
 * binary links no cluster_inject.o.  armed_count 0 keeps the macro's
 * fast-path gate closed; the helpers are link-only. */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}

int
errcode(int s pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}

void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

/*
 * spec-2.22 D6 — cluster_ges.c handler calls pgstat_report_wait_start/_end
 * for WAIT_EVENT_CLUSTER_LMD_PROBE.  The inline helpers reference
 * my_wait_event_info which lives in pgstat backend code.  Provide a
 * file-static fake so the standalone link resolves cleanly.
 */
#include "pgstat.h"
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* cluster_lmd_graph_snapshot_copy stub — cluster_ges DEADLOCK_PROBE
 * handler calls into LMD graph;  test_cluster_ges binary links
 * cluster_ges.o standalone (no cluster_lmd_graph.o), so stub it. */
#include "cluster/cluster_lmd.h"

int
cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out_buf pg_attribute_unused(),
								int max_edges pg_attribute_unused(), uint64 *out_gen_at_snapshot)
{
	if (out_gen_at_snapshot)
		*out_gen_at_snapshot = 0;
	return 0;
}
/* cluster_lmd_is_ready stub already provided below (line ~275). */

/*
 * spec-2.13 Q9 (L105 inherit):  ShmemInitStruct stub uses union
 * force-align to guarantee 8-byte alignment for pg_atomic_uint64
 * fields inside ClusterGesSharedState.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	static union {
		uint64 force_align;
		char data[1024]; /* generous; cluster_ges_shmem_size() << 1KB */
	} ges_buf;
	static bool ges_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster ges") == 0) {
		Assert(size <= sizeof(ges_buf.data)); /* catch shmem layout growth */
		*foundPtr = ges_initialized;
		ges_initialized = true;
		return ges_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * spec-2.16 Step 6 L104 stubs — cross-module deps activated by
 *   Step 3 D6 handler (5-item validation + work_queue enqueue +
 *   REJECT_BUSY fallback).  Stubs default to "pass-through" so
 *   既有 T-ges-1 tests still PASS with handler 真激活 behavior.
 * ============================================================ */

int cluster_node_id = 0;
static uint64 stub_current_epoch = 0;
static bool stub_authority_managed = false;
static bool stub_serving_ready = false;
static bool stub_recovery_ready = false;
static bool stub_recovery_transport_ready = false;
static bool stub_survivor_protocol_ready = false;

static uint64 stub_replacement_capability_sample_count = 0;
static uint32 stub_replacement_required_capabilities = 0;

bool
cluster_sf_peer_capability_family_sample(int32 peer_id pg_attribute_unused(),
										 uint32 required_capabilities,
										 uint32 optional_capabilities pg_attribute_unused(),
										 bool *optional_supported_out, uint32 *generation_out)
{
	stub_replacement_capability_sample_count++;
	stub_replacement_required_capabilities = required_capabilities;
	if (optional_supported_out != NULL)
		*optional_supported_out = false;
	if (generation_out != NULL)
		*generation_out = 7;
	return required_capabilities == UINT32_C(0x00100000);
}

bool
cluster_qvotec_in_quorum(void)
{
	return true; /* default in-quorum so validation step 4 passes */
}

bool
cluster_authority_readiness_managed(void)
{
	return stub_authority_managed;
}

bool
cluster_serving_ready_is_current(void)
{
	return stub_serving_ready;
}

bool
cluster_recovery_authority_is_current(void)
{
	return stub_recovery_ready;
}

bool
cluster_configuration_read_transport_is_current(const ClusterResId *resid pg_attribute_unused(),
												LOCKMODE mode pg_attribute_unused())
{
	/* Startup-phase tests exercise the actual positive predicate. These
	 * existing protocol cases must not gain a configuration-only permission. */
	return false;
}

bool
cluster_recovery_transport_is_current(void)
{
	return stub_recovery_transport_ready || stub_recovery_ready || stub_survivor_protocol_ready;
}

ClusterAuthorityReadiness
cluster_authority_readiness_get(void)
{
	return stub_survivor_protocol_ready ? CLUSTER_AUTHORITY_SERVING_READY
										: CLUSTER_AUTHORITY_STARTING;
}

/* RF-ROOT P6 (crash-rejoin): the REDECLARE_DONE ingress gate consults the
 * components-only transport proof; the pure unit pins it to the same stub as
 * the strict transport, and the remaining diag/self samples are inert. */
bool
cluster_recovery_transport_components_current(void)
{
	return stub_recovery_transport_ready || stub_recovery_ready;
}

bool
cluster_lms_is_recovery_ready(void)
{
	return true;
}

bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return true;
}

ClusterStartupPhase
cluster_current_phase(void)
{
	return CLUSTER_PHASE_PRE_INIT;
}

int MyProcPid = 0;

bool
cluster_recovery_authority_request_allowed(const ClusterResId *resid, LOCKMODE mode,
										   bool startup_process)
{
	return startup_process && stub_recovery_ready && resid != NULL
		   && ((resid->type == CLUSTER_CF_RESID_TYPE && mode == ShareLock)
			   || (resid->type == CLUSTER_WAL_RETENTION_RESID_TYPE && mode == ExclusiveLock));
}

/* PGRAC: use the actual resource/mode contract; readiness observations above
 * remain the explicit fixture boundary. Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_ges_recovery_modes.inc"

/*
 * spec-5.7 Direction B link stubs — the REQUEST wait loop now consults CSSD
 * peer state + the liveness reclassify.  Default to "master alive / not
 * sole-native" so the dead-master native-safe abort is never taken in these
 * standalone GES tests (which exercise the steady-state grant/reject/retransmit
 * paths); the abort branch itself is covered by the extend-gate + TAP suites.
 */
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}

bool
cluster_extend_liveness_is_sole_native(void)
{
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	return stub_current_epoch;
}

/* cluster_conf — return non-NULL so validation step 4 declared check
 * passes.  Signature must match cluster_conf.h (pulled in via
 * cluster_grd.h since spec-4.6). */
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	static ClusterNodeInfo dummy;
	return &dummy;
}

/* spec-4.6 D3/D4 stubs:  the drain path now reads the shard recovery
 * phase (master-side freeze gate) and handles GES_REDECLARE via the
 * insert-or-rebind entry API.  Standalone fixture:  NORMAL phase +
 * always-OK rebind. */
uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

/* spec-5.8 D1c stub:  cluster_ges.c REQUEST/CONVERT send fills the wire
 * waiter_xid from the backend's xid; the standalone fixture has no xact. */
TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}

/* spec-5.9 D3 stub:  the GES wait loops consume a per-proc cancel token; the
 * token match logic is covered by test_cluster_cancel_token.  Here the fixture
 * never installs one, so consume always reports "no cancel". */
bool
cluster_cancel_token_consume(void)
{
	return false;
}

/* Observe the real handler's boundary; GRD convergence is tested separately. */
static int stub_peer_done_count;
static int32 stub_peer_done_node;
static uint64 stub_peer_done_epoch;
static uint64 stub_peer_done_hash;
void
cluster_grd_recovery_mark_peer_done(int32 node, uint64 epoch, uint64 event_id)
{
	stub_peer_done_count++;
	stub_peer_done_node = node;
	stub_peer_done_epoch = epoch;
	stub_peer_done_hash = event_id;
}

static int stub_rebind_calls;
static ClusterGrdHolderId stub_rebind_holder;
static int stub_rebind_mode;
static ClusterGrdEntryResult stub_rebind_result = CLUSTER_GRD_ENTRY_OK;

ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid pg_attribute_unused(),
										  const struct ClusterGrdHolderId *nh,
										  int32 src pg_attribute_unused(), int lockmode)
{
	stub_rebind_calls++;
	stub_rebind_holder = *nh;
	stub_rebind_mode = lockmode;
	return stub_rebind_result;
}

static int32 stub_remote_master = -1;
static bool stub_master_unknown = false;
static uint64 stub_master_generation = 1;
static bool stub_remaster_on_second_lookup = false;
static int stub_master_gen_lookup_calls = 0;
static int stub_release_and_drain_result = 0;
static int stub_release_and_drain_calls;
static ClusterGrdEntryResult stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
static int stub_exact_release_calls;
static bool stub_holder_absent;
static LOCKMODE stub_holder_mode_override = NoLock;
static bool stub_readiness_lost_on_release;
static ClusterGrdHolderId stub_exact_release_holder;

int32
cluster_grd_lookup_master(const struct ClusterResId *resid pg_attribute_unused())
{
	if (stub_master_unknown)
		return -1;
	return stub_remote_master >= 0 ? stub_remote_master : cluster_node_id;
}

int32
cluster_grd_lookup_master_gen(const struct ClusterResId *resid, uint64 *out_routing_generation)
{
	if (out_routing_generation != NULL)
		*out_routing_generation
			= stub_master_generation
			  + (stub_remaster_on_second_lookup && stub_master_gen_lookup_calls > 0 ? 1 : 0);
	stub_master_gen_lookup_calls++;
	return cluster_grd_lookup_master(resid);
}

void
cluster_grd_resid_encode(const LOCKTAG *src, struct ClusterResId *dst)
{
	memset(dst, 0, sizeof(*dst));
	if (src != NULL) {
		dst->type = src->locktag_type;
		dst->lockmethodid = src->locktag_lockmethodid;
	}
}

/* GRD inc helpers — stub bump local counters (test verifies via accessor) */
static uint64 stub_work_queue_full = 0;
static uint64 stub_inbound_validation_fail = 0;
static uint64 stub_cleanup_deferred = 0;
static uint64 stub_reply_deferred = 0;
static uint64 stub_reply_dropped = 0;
static uint64 stub_work_queue_enqueue_count = 0;
static uint64 stub_lmon_reply_enqueue_count = 0;
static GesReplyPayload stub_lmon_reply_last;
static bool stub_work_queue_dequeue_pending = false;
static ClusterGrdWorkItem stub_work_queue_dequeue_item;
static uint64 stub_master_grant_mutation_count = 0;
static uint64 stub_bast_received = 0;
static uint64 stub_lmd_cancel_enqueue_count = 0;
static uint32 stub_lmd_cancel_last_source = 0;
static uint64 stub_bast_ack = 0;
static uint64 stub_deadlock_probe_drop = 0;
static uint64 stub_backend_request_enqueue_count = 0;
static GesRequestPayload stub_backend_request_last;
static GesReplyWaitEntry stub_reply_wait_entry;
static uint64 stub_backend_request_ready_after = 0;
static bool stub_cooperative_reply_table;
static bool stub_cooperative_outbound_full;
static bool stub_cooperative_reply_present;
static bool stub_cooperative_change_cut_on_poll;
static int stub_cooperative_cv_calls;
static int stub_cooperative_inserts;
static GesReplyWaitKey stub_cooperative_key;
static uint64 stub_cancel_wait_enqueue_count = 0;
static uint32 stub_cancel_wait_last_dest = 0;
static GesCancelWaitPayload stub_cancel_wait_last;
static uint64 stub_dedup_lookup_count = 0;
static uint64 stub_dedup_record_count = 0;
static ClusterGesDedupKey stub_dedup_record_key;
static GesReplyPayload stub_dedup_record_reply;
static bool stub_cancel_match;
static uint64 stub_dedup_remove_completed_count = 0;
static ClusterGesDedupKey stub_dedup_remove_completed_keys[8];

void
cluster_grd_inc_ges_work_queue_full(void)
{
	stub_work_queue_full++;
}

/* spec-2.18 Sprint A Step 3 D9 L104 stub:  cluster_lms_wake_drain
 * broadcasts CV after successful work_queue enqueue. */
void cluster_lms_wake_drain(void);
void
cluster_lms_wake_drain(void)
{}

/*
 * spec-5.1c D1 stubs — cluster_ges_send_bast_targeted's local-delivery branch
 * references these backend primitives.  This test never drives a live local
 * holder (it does not call send_bast_targeted), so the stubs are inert and
 * link-only: ProcGlobal is never dereferenced, and SendProcSignal /
 * cluster_grd_bast_local_deliver_ok are never invoked.
 */
void *ProcGlobal = NULL;

int SendProcSignal(int pid, int reason, int backendid);
int
SendProcSignal(int pid pg_attribute_unused(), int reason pg_attribute_unused(),
			   int backendid pg_attribute_unused())
{
	return -1;
}

bool
cluster_grd_bast_local_deliver_ok(uint32 procno pg_attribute_unused(),
								  int proc_count pg_attribute_unused(),
								  uint64 holder_epoch pg_attribute_unused(),
								  uint64 current_epoch pg_attribute_unused(),
								  int target_pid pg_attribute_unused(),
								  int target_backendid pg_attribute_unused())
{
	return false;
}

/* spec-2.19 Sprint A Step 3 D8 L104 stubs:  cluster_lmd_is_ready (HC4
 * exact predicate) + cluster_lmd_submit_wait_edge (HC3 producer wake +
 * HC6 skeleton "no graph maintenance").  Called from cluster_ges.c
 * GES_REQ_OPCODE_DEADLOCK_PROBE handler. */
bool cluster_lmd_is_ready(void);
bool
cluster_lmd_is_ready(void)
{
	return stub_lmd_ready;
}
void cluster_lmd_submit_wait_edge(void);
void
cluster_lmd_submit_wait_edge(void)
{}
void
cluster_grd_inc_ges_inbound_validation_fail(void)
{
	stub_inbound_validation_fail++;
}
void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	stub_cleanup_deferred++;
}
void
cluster_grd_inc_ges_reply_deferred(void)
{
	stub_reply_deferred++;
}
void
cluster_grd_inc_ges_reply_dropped(void)
{
	stub_reply_dropped++;
}
void
cluster_grd_inc_bast_received(void)
{
	stub_bast_received++;
}
void
cluster_grd_inc_bast_ack(void)
{
	stub_bast_ack++;
}
void
cluster_grd_inc_deadlock_probe_drop(void)
{
	stub_deadlock_probe_drop++;
}

/* work_queue + outbound enqueue stubs — accept always (no overflow path tested
 * at unit layer; TAP exercises overflow with real shmem). */
bool
cluster_grd_work_queue_enqueue(uint32 src pg_attribute_unused(),
							   const void *p pg_attribute_unused(), uint16 l pg_attribute_unused())
{
	stub_work_queue_enqueue_count++;
	return true;
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out pg_attribute_unused())
{
	if (!stub_work_queue_dequeue_pending)
		return false;
	if (out != NULL)
		*out = stub_work_queue_dequeue_item;
	stub_work_queue_dequeue_pending = false;
	return true;
}

void
cluster_grd_outbound_enqueue_lmon_reply(uint32 d pg_attribute_unused(),
										const void *p pg_attribute_unused(),
										uint16 l pg_attribute_unused())
{
	stub_lmon_reply_enqueue_count++;
	if (p != NULL && l == sizeof(GesReplyPayload))
		memcpy(&stub_lmon_reply_last, p, sizeof(stub_lmon_reply_last));
}
/* spec-5.16 orphan-grant auto-release uses the cleanup-release producer. */
void
cluster_grd_outbound_enqueue_cleanup_release(uint32 d pg_attribute_unused(),
											 const void *p pg_attribute_unused(),
											 uint16 l pg_attribute_unused())
{}

/* spec-2.23 D14 R13 stub audit — new symbol surface introduced by
 * Steps 1-9 needs file-local stubs so cluster_ges.o links standalone
 * in this test binary.  All stubs are minimal inert bodies; behavior
 * coverage lives in TAP 108/109 where real backend wiring runs. */

bool
cluster_grd_outbound_enqueue_backend_request(uint32 d pg_attribute_unused(), const void *p,
											 uint16 l)
{
	stub_backend_request_enqueue_count++;
	if (p != NULL && l == sizeof(GesRequestPayload))
		memcpy(&stub_backend_request_last, p, sizeof(stub_backend_request_last));
	if (stub_cooperative_outbound_full)
		return false;
	if (stub_backend_request_ready_after > 0
		&& stub_backend_request_enqueue_count >= stub_backend_request_ready_after) {
		stub_reply_wait_entry.reply_opcode = GES_REPLY_OPCODE_GRANT;
		stub_reply_wait_entry.reject_reason = GES_REJECT_REASON_NONE;
		stub_reply_wait_entry.ready = true;
	}
	return true;
}

/* spec-5.8 D8 — REPORT send-back deps newly referenced by cluster_ges.o.  The
 * standalone harness does not exercise the deadlock probe path (TAP 109 / the
 * 2-node TAP do); these only resolve at link time. */
int cluster_lmd_max_wait_edges = 1024;

/* spec-5.8 D8 — victim cancel flag referenced by the GES wait loops. */
volatile sig_atomic_t cluster_ges_cancel_pending = false;

ClusterICSendResult
cluster_ic_send_envelope(uint8 mt pg_attribute_unused(), int32 dest pg_attribute_unused(),
						 const void *p pg_attribute_unused(), uint32 len pg_attribute_unused())
{
	stub_report_sends++;
	return CLUSTER_IC_SEND_DONE;
}

void *
palloc(Size sz)
{
	return malloc(sz);
}

void
pfree(void *p)
{
	free(p);
}

/* spec-2.24 D14 stub audit. */
void
cluster_grd_outbound_enqueue_lmd_cancel(uint32 d, const void *p, uint16 l)
{
	if (p != NULL && l == sizeof(GesCancelWaitPayload)
		&& ((const GesCancelWaitPayload *)p)->opcode == GES_REQ_OPCODE_CANCEL_WAIT) {
		stub_cancel_wait_enqueue_count++;
		stub_cancel_wait_last_dest = d;
		memcpy(&stub_cancel_wait_last, p, sizeof(stub_cancel_wait_last));
	}
}
bool
cluster_lmd_cancel_queue_enqueue(uint32 s, const void *p pg_attribute_unused(),
								 uint16 l pg_attribute_unused())
{
	stub_lmd_cancel_enqueue_count++;
	stub_lmd_cancel_last_source = s;
	return true;
}
void
cluster_lmd_cross_node_cancel_queue_full_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_cancel_received_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_victim_cancel_sent_count_inc(uint64 d pg_attribute_unused())
{}

void
cluster_grd_inc_bast_sent(void)
{}

/* spec-5.1c D1 — local BAST delivery bumps stale_drop on a guard miss. */
void
cluster_grd_inc_bast_stale_drop(void)
{}

/* spec-2.25 D14 R10 stub audit — native-lock probe 3 NEW symbols.
 * Real behavior tested in test_cluster_native_lock_probe.c (Step 11
 * forward-link).  Here we only need link-surface satisfaction +
 * call counters for T-ges-14..16 dispatch assertions.
 *
 * NOTE:  cannot #include cluster_native_lock_probe.h here because it
 * pulls in cluster_grd.h which conflicts with the local opaque stubs
 * for cluster_grd_entry_enqueue_or_grant et al.  Forward-declare the
 * probe enum + use struct ClusterGrdHolderId opaque (typedef'd in
 * cluster_grd.h but compatible with opaque struct here per C tag/
 * typedef rules — function pointer compares correctly at link time
 * because all callers see the same struct tag). */
static int stub_native_probe_local_calls;
static int stub_native_probe_recv_calls;
static int stub_native_probe_outbound_calls;
static uint32 stub_native_probe_outbound_last_dest;
static uint16 stub_native_probe_outbound_last_len;

/* Forward decls matching real prototypes in
 * src/include/cluster/cluster_native_lock_probe.h. */
typedef enum ClusterNativeLockProbeReply {
	CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL = 0,
} ClusterNativeLockProbeReplyLocal;

int
cluster_native_lock_probe_local(const void *locktag pg_attribute_unused(),
								int lockmode pg_attribute_unused(),
								const struct ClusterGrdHolderId *eh pg_attribute_unused())
{
	stub_native_probe_local_calls++;
	return CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL;
}

void
cluster_lms_native_probe_recv_reply(uint64 probe_id pg_attribute_unused(),
									int32 sender pg_attribute_unused(),
									int status pg_attribute_unused())
{
	stub_native_probe_recv_calls++;
}

/* spec-2.27 D2 / D7 R10 stub audit. */
int cluster_ges_retransmit_max_attempts = 5;
int cluster_ges_dedup_max_entries = 8192;

uint64
cluster_lms_get_shard_master_generation(void)
{
	return 0;
}

void
cluster_lms_inc_priority_starvation_observed(void)
{}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key pg_attribute_unused(),
									 uint8 *reply_out pg_attribute_unused(),
									 uint16 reply_buf_len pg_attribute_unused(),
									 uint16 *reply_len_out pg_attribute_unused())
{
	stub_dedup_lookup_count++;
	if (reply_len_out)
		*reply_len_out = 0;
	return CLUSTER_GES_DEDUP_MISS_REGISTERED;
}

void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 reply_len)
{
	stub_dedup_record_count++;
	stub_dedup_record_key = *key;
	if (reply_len == sizeof(stub_dedup_record_reply))
		memcpy(&stub_dedup_record_reply, reply, reply_len);
}

bool
cluster_ges_dedup_remove_completed(const ClusterGesDedupKey *key)
{
	if (stub_dedup_remove_completed_count < lengthof(stub_dedup_remove_completed_keys))
		stub_dedup_remove_completed_keys[stub_dedup_remove_completed_count] = *key;
	stub_dedup_remove_completed_count++;
	return true;
}

bool
cluster_lms_native_probe_required(const struct ClusterResId *resid pg_attribute_unused(),
								  int lockmode pg_attribute_unused())
{
	return false;
}

bool
cluster_lms_native_probe_schedule_grant(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int32 source_node_id pg_attribute_unused(), uint32 request_opcode pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	uint64 receiver_generation pg_attribute_unused(),
	int convert_current_mode pg_attribute_unused())
{
	return false;
}

/* spec-5.3 — stubs for the convert wrappers + sync native probe (cluster_ges.c
 * references them; this harness exercises the dispatch/dedup paths, not the
 * convert decision, which is covered by test_cluster_grd). */
bool
cluster_lms_native_probe_wait_clear(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int timeout_ms pg_attribute_unused())
{
	return true;
}

ClusterGrdConvertResult
cluster_grd_convert_or_enqueue(
	const struct ClusterResId *resid pg_attribute_unused(), int32 node_id pg_attribute_unused(),
	uint32 procno pg_attribute_unused(), uint64 cluster_epoch pg_attribute_unused(),
	int current_mode pg_attribute_unused(), int requested_mode pg_attribute_unused(),
	uint64 convert_request_id pg_attribute_unused(), int32 source_node_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
	int *n_conflict_out pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

/* spec-5.8 D1c/D1e — waiter-metadata variant stub. */
ClusterGrdConvertResult
cluster_grd_convert_or_enqueue_meta(
	const struct ClusterResId *resid pg_attribute_unused(), int32 node_id pg_attribute_unused(),
	uint32 procno pg_attribute_unused(), uint64 cluster_epoch pg_attribute_unused(),
	int current_mode pg_attribute_unused(), int requested_mode pg_attribute_unused(),
	uint64 convert_request_id pg_attribute_unused(), int32 source_node_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	ClusterGrdWaiterMeta meta pg_attribute_unused(),
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
	int *n_conflict_out pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

ClusterGrdConvertResult
cluster_grd_convert_nowait(const struct ClusterResId *resid pg_attribute_unused(),
						   int32 node_id pg_attribute_unused(), uint32 procno pg_attribute_unused(),
						   uint64 cluster_epoch pg_attribute_unused(),
						   int current_mode pg_attribute_unused(),
						   int requested_mode pg_attribute_unused(),
						   uint64 convert_request_id pg_attribute_unused(),
						   uint64 old_request_id pg_attribute_unused(),
						   int32 source_node_id pg_attribute_unused(),
						   uint64 shard_master_generation pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

int
cluster_grd_release_and_drain(const struct ClusterResId *resid pg_attribute_unused(),
							  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
							  ClusterGrdGrantIdentity *granted_out, int max_out)
{
	stub_release_and_drain_calls++;
	if (stub_release_and_drain_result == 1) {
		Assert(granted_out != NULL && max_out > 0);
		granted_out[0] = stub_drained_grant;
	}
	return stub_release_and_drain_result;
}

ClusterGrdEntryResult
cluster_grd_rollback_convert(const struct ClusterResId *resid pg_attribute_unused(),
							 int32 node_id pg_attribute_unused(),
							 uint32 procno pg_attribute_unused(),
							 int upgraded_mode pg_attribute_unused(),
							 int old_mode pg_attribute_unused(),
							 uint64 old_request_id pg_attribute_unused(),
							 uint64 convert_request_id pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

int cluster_ges_convert_timeout_ms = 30000;

void
cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest, const void *p pg_attribute_unused(),
											  uint16 len)
{
	stub_native_probe_outbound_calls++;
	stub_native_probe_outbound_last_dest = dest;
	stub_native_probe_outbound_last_len = len;
}

/* cluster_ges_reply_wait API stubs (spec-2.23 D1). */
static bool stub_reply_wait_insert_enabled = false;

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *k, TimestampTz deadline)
{
	if (!stub_reply_wait_insert_enabled)
		return NULL;
	if (stub_cooperative_reply_table) {
		if (stub_cooperative_reply_present)
			abort();
		stub_cooperative_key = *k;
		stub_cooperative_reply_present = true;
		stub_cooperative_inserts++;
	}
	memset(&stub_reply_wait_entry, 0, sizeof(stub_reply_wait_entry));
	stub_reply_wait_entry.key = *k;
	stub_reply_wait_entry.deadline = deadline;
	return &stub_reply_wait_entry;
}
GesReplyWaitEntry *
cluster_ges_reply_wait_lookup(const GesReplyWaitKey *k pg_attribute_unused())
{
	return NULL;
}
void
cluster_ges_reply_wait_wake(GesReplyWaitEntry *e pg_attribute_unused(),
							uint32 opcode pg_attribute_unused(),
							uint32 reason pg_attribute_unused())
{}
void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *k pg_attribute_unused())
{}
/* spec-5.16 orphan-grant stubs (deliver / mark_abandoned). */
GesReplyDeliverResult
cluster_ges_reply_wait_deliver(const GesReplyWaitKey *k pg_attribute_unused(),
							   uint32 opcode pg_attribute_unused(),
							   uint32 reason pg_attribute_unused())
{
	return GES_REPLY_DELIVER_NO_WAITER;
}
bool
cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *k pg_attribute_unused(),
									  TimestampTz tombstone_deadline pg_attribute_unused())
{
	return false;
}
void
cluster_ges_inc_release_ack(void)
{}
void
cluster_ges_inc_reply_late_drop(void)
{}

/* cluster_lmd probe collector receive (spec-2.23 D8). */
struct GesDeadlockReportHeader;
bool
cluster_lmd_probe_collect_receive(const struct GesDeadlockReportHeader *r pg_attribute_unused(),
								  Size len pg_attribute_unused())
{
	return false;
}

/* cluster_grd GRD-owned waiter API (spec-2.23 D6). */
struct ClusterGrdConflictHolder;
struct ClusterGrdWaiterIdentity;
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const struct ClusterResId *r pg_attribute_unused(),
								   const struct ClusterGrdHolderId *h pg_attribute_unused(),
								   int32 src pg_attribute_unused(),
								   uint64 req_id pg_attribute_unused(),
								   uint64 shard_master_generation pg_attribute_unused(),
								   uint32 op pg_attribute_unused(), int mode pg_attribute_unused(),
								   struct ClusterGrdConflictHolder *out pg_attribute_unused(),
								   int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
/* spec-5.5 D5 — conditional (NOWAIT) variant stub (mirror: grant, no conflict). */
ClusterGrdGrantAction
cluster_grd_entry_grant_conditional(const struct ClusterResId *r pg_attribute_unused(),
									const struct ClusterGrdHolderId *h pg_attribute_unused(),
									int32 src pg_attribute_unused(),
									uint64 req_id pg_attribute_unused(),
									uint64 shard_master_generation pg_attribute_unused(),
									uint32 op pg_attribute_unused(), int mode pg_attribute_unused(),
									struct ClusterGrdConflictHolder *out pg_attribute_unused(),
									int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
/* spec-5.8 D1c/D1e — waiter-metadata variant stubs. */
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant_meta(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(), int32 src pg_attribute_unused(),
	uint64 req_id pg_attribute_unused(), ClusterGrdWaiterMeta meta pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(), uint32 op pg_attribute_unused(),
	int mode pg_attribute_unused(), struct ClusterGrdConflictHolder *out pg_attribute_unused(),
	int *nout pg_attribute_unused())
{
	stub_master_grant_mutation_count++;
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
ClusterGrdGrantAction
cluster_grd_entry_grant_conditional_meta(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(), int32 src pg_attribute_unused(),
	uint64 req_id pg_attribute_unused(), ClusterGrdWaiterMeta meta pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(), uint32 op pg_attribute_unused(),
	int mode pg_attribute_unused(), struct ClusterGrdConflictHolder *out pg_attribute_unused(),
	int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
int
cluster_grd_entry_release_and_pop_compatible_waiter(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(),
	struct ClusterGrdWaiterIdentity *out pg_attribute_unused(), int max_out pg_attribute_unused())
{
	return 0;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const struct ClusterResId *r pg_attribute_unused(),
								 const struct ClusterGrdHolderId *h)
{
	stub_exact_release_calls++;
	stub_exact_release_holder = *h;
	if (stub_readiness_lost_on_release)
		stub_recovery_ready = false;
	return stub_exact_release_result;
}

bool
cluster_grd_holder_mode_by_id(const struct ClusterResId *r,
							  const struct ClusterGrdHolderId *h pg_attribute_unused(),
							  LOCKMODE *out_mode)
{
	if (r == NULL || stub_holder_absent)
		return false;
	if (out_mode != NULL)
		*out_mode = stub_holder_mode_override != NoLock
						? stub_holder_mode_override
						: (r->type == CLUSTER_CF_RESID_TYPE ? ShareLock : ExclusiveLock);
	return r->type == CLUSTER_CF_RESID_TYPE || r->type == CLUSTER_WAL_RETENTION_RESID_TYPE;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id(const struct ClusterResId *r pg_attribute_unused(),
								const struct ClusterGrdHolderId *h pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

/* spec-5.9 D4 stubs — the CANCEL_WAIT handler dequeues via these; the real
 * dequeue is covered by test_cluster_grd / the 2-node TAP. */
ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq(const struct ClusterResId *r pg_attribute_unused(),
									const struct ClusterGrdHolderId *h pg_attribute_unused(),
									uint64 ws pg_attribute_unused())
{
	return stub_cancel_match ? CLUSTER_GRD_ENTRY_OK : CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_cancel_convert_by_id(const struct ClusterResId *r pg_attribute_unused(),
								 const struct ClusterGrdHolderId *h pg_attribute_unused(),
								 uint64 ws pg_attribute_unused())
{
	return stub_cancel_match ? CLUSTER_GRD_ENTRY_OK : CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_exact(const ClusterResId *r, const ClusterGrdHolderId *h, uint64 ws,
								ClusterGrdGrantIdentity *out)
{
	ClusterGrdEntryResult result = cluster_grd_cancel_waiter_by_id_seq(r, h, ws);
	memset(out, 0, sizeof(*out));
	if (result == CLUSTER_GRD_ENTRY_OK) {
		out->holder = *h;
		out->source_node_id = h->node_id;
		out->request_opcode = GES_REQ_OPCODE_REQUEST;
		out->shard_master_generation = 71;
	}
	return result;
}

ClusterGrdEntryResult
cluster_grd_cancel_convert_exact(const ClusterResId *r, const ClusterGrdHolderId *h, uint64 ws,
								 ClusterGrdGrantIdentity *out)
{
	ClusterGrdEntryResult result = cluster_grd_cancel_waiter_exact(r, h, ws, out);
	if (result == CLUSTER_GRD_ENTRY_OK)
		out->request_opcode = GES_REQ_OPCODE_CONVERT;
	return result;
}

void
cluster_lmd_cancel_wait_stale_rejected_count_inc(uint64 d pg_attribute_unused())
{}

void
cluster_lmd_cancel_ack_received_count_inc(uint64 d pg_attribute_unused())
{}

/* GUC + PG runtime stubs. */
int cluster_ges_request_timeout_ms = 60000;

/* spec-5.59 D2 stubs: cluster_ges.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

/* CHECK_FOR_INTERRUPTS() in the local-master wait loop. */
volatile sig_atomic_t InterruptPending = false;

void
ProcessInterrupts(void)
{}

/* PG_TRY/PG_CATCH machinery referenced by the local-master conflict wait loop.
 * The wait path is never exercised by these tests (no cross-node enqueue), so
 * these only need to satisfy the standalone link. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

bool
DoLockModesConflict(int a pg_attribute_unused(), int b pg_attribute_unused())
{
	return false;
}

static bool stub_clock_advances = false;
static TimestampTz stub_now = 0;
static bool stub_cv_timeout_expires = true;
static TimestampTz stub_cv_now_after_sleep = 0;
static int stub_cv_waits, stub_cv_grant_on_wait;

TimestampTz
GetCurrentTimestamp(void)
{
	if (stub_clock_advances)
		stub_now += 1000;
	return stub_now;
}

void *MyProc;

#include "storage/condition_variable.h"
void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}
/* HW consumes the real reply table in test_cluster_hw_handoff. */
GesReplyWaitPollResult
cluster_ges_reply_wait_poll_consume(const GesReplyWaitKey *key, GesReplyWaitVerdict *verdict)
{
	if (!stub_cooperative_reply_table)
		abort();
	if (!stub_cooperative_reply_present || memcmp(key, &stub_cooperative_key, sizeof(*key)) != 0)
		return GES_REPLY_WAIT_POLL_MISSING;
	if (stub_reply_wait_entry.abandoned)
		return GES_REPLY_WAIT_POLL_ABANDONED;
	if (!stub_reply_wait_entry.ready)
		return GES_REPLY_WAIT_POLL_PENDING;
	verdict->reply_opcode = stub_reply_wait_entry.reply_opcode;
	verdict->reject_reason = stub_reply_wait_entry.reject_reason;
	stub_cooperative_reply_present = false;
	if (stub_cooperative_change_cut_on_poll)
		stub_remote_master = 3;
	return GES_REPLY_WAIT_POLL_DELIVERED;
}

bool
ConditionVariableCancelSleep(void)
{
	return false;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
							long timeout pg_attribute_unused(),
							uint32 wait_event pg_attribute_unused())
{
	stub_cooperative_cv_calls++;
	if (stub_cv_now_after_sleep > 0)
		stub_now = stub_cv_now_after_sleep;
	if (stub_cv_grant_on_wait > 0 && ++stub_cv_waits >= stub_cv_grant_on_wait) {
		stub_reply_wait_entry.ready = true;
		stub_reply_wait_entry.reject_reason = GES_REJECT_REASON_NONE;
	}
	return stub_cv_timeout_expires;
}


/* ============================================================
 * T-ges-1 a/b/c/d/e (spec-2.13 D6 Q5.2).
 * ============================================================ */

UT_TEST(test_ges_request_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_handler);
}

UT_TEST(test_ges_reply_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_handler);
}

UT_TEST(test_ges_accessors_linkable_and_initial_zero)
{
	cluster_ges_shmem_init();

	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_defer_count);
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_defer_count);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), (uint64)0);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), (uint64)0);
}

UT_TEST(test_ges_request_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 7; /* sentinel non-zero peer id */

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	/* Invoke stub — must return silently without ERROR/FATAL. */
	cluster_ges_request_handler(&env_sentinel, NULL);

	/* (1) handler returned (would not get here on FATAL abort).
	 * (2) request counter +1.
	 * (3) reply counter unchanged. */
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + 1);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply);
}

UT_TEST(test_ges_reply_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 11;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request);
}

UT_TEST(test_ges_handler_counter_monotonic_n_invocations)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;
	const int N = 7;
	int i;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 3;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	for (i = 0; i < N; i++)
		cluster_ges_request_handler(&env_sentinel, NULL);

	for (i = 0; i < N; i++)
		cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + (uint64)N);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + (uint64)N);
}

UT_TEST(test_ges_request_valid_payload_enqueues_work)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.holder_node_id = 1;

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue + 1);
}

static void
init_valid_ges_request(ClusterICEnvelope *env, GesRequestPayload *req, GesRequestOpcode opcode,
					   const ClusterResId *resid, LOCKMODE mode)
{
	memset(env, 0, sizeof(*env));
	env->source_node_id = 1;
	env->epoch = stub_current_epoch;
	env->payload_length = sizeof(*req);
	memset(req, 0, sizeof(*req));
	req->opcode = opcode;
	req->lockmode = mode;
	req->holder_node_id = 1;
	if (resid != NULL)
		memcpy(req->resid, resid, sizeof(*resid));
}

/*
 * A RELEASE is the holder-lifecycle completion message, not another durable
 * retry receipt.  The exact-holder GRD primitive makes duplicate RELEASE
 * processing idempotent; retaining one dedup row for every completed release
 * would otherwise make the fixed HTAB grow monotonically.  The release must
 * therefore bypass receiver dedup and retire the completed acquisition-family
 * receipts after the exact release decision.
 */
UT_TEST(test_ges_release_bypasses_dedup_and_reclaims_acquire_receipts)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	ClusterResId resid;
	uint64 lookup_before = stub_dedup_lookup_count;
	uint64 record_before = stub_dedup_record_count;
	uint64 remove_before = stub_dedup_remove_completed_count;
	uint64 enqueue_before = stub_work_queue_enqueue_count;
	uint64 reply_before = stub_lmon_reply_enqueue_count;
	static const uint32 expected_opcodes[] = {
		GES_REQ_OPCODE_REQUEST,
		GES_REQ_OPCODE_REQUEST_NOWAIT,
		GES_REQ_OPCODE_CONVERT,
		GES_REQ_OPCODE_REDECLARE,
	};

	memset(&resid, 0, sizeof(resid));
	resid.field1 = 77;
	resid.type = CLUSTER_IR_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	init_valid_ges_request(&env, &req, GES_REQ_OPCODE_RELEASE, &resid, ExclusiveLock);
	req.holder_procno = 23;
	req.holder_request_id_lo = UINT32_C(0x55667788);
	req.holder_request_id_hi = UINT32_C(0x11223344);
	req.shard_master_generation_lo = 19;

	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_dedup_lookup_count, lookup_before);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueue_before + 1);

	memset(&stub_work_queue_dequeue_item, 0, sizeof(stub_work_queue_dequeue_item));
	stub_work_queue_dequeue_item.routing_generation = stub_master_generation;
	stub_work_queue_dequeue_item.source_node_id = env.source_node_id;
	stub_work_queue_dequeue_item.payload_len = sizeof(req);
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	stub_release_and_drain_result = 0;

	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_dedup_record_count, record_before);
	UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, reply_before + 1);
	UT_ASSERT_EQ(stub_dedup_remove_completed_count, remove_before + lengthof(expected_opcodes));
	for (int i = 0; i < lengthof(expected_opcodes); i++) {
		const ClusterGesDedupKey *key = &stub_dedup_remove_completed_keys[remove_before + i];

		UT_ASSERT_EQ(key->origin_node_id, env.source_node_id);
		UT_ASSERT_EQ(key->opcode, expected_opcodes[i]);
		UT_ASSERT_EQ(key->request_id, UINT64_C(0x1122334455667788));
		UT_ASSERT_EQ(key->cluster_epoch, stub_current_epoch);
		UT_ASSERT_EQ(key->shard_master_generation, (uint64)19);
		UT_ASSERT_EQ(key->holder_procno, (uint32)23);
	}
}

UT_TEST(test_ges_recovery_ingress_exact_allowlist)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	ClusterResId resid;
	uint64 enqueued;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	stub_authority_managed = true;
	stub_recovery_ready = true;
	stub_serving_ready = false;
	enqueued = stub_work_queue_enqueue_count;

	memset(&resid, 0, sizeof(resid));
	resid.type = CLUSTER_CF_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	init_valid_ges_request(&env, &req, GES_REQ_OPCODE_REQUEST, &resid, ShareLock);
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, ++enqueued);

	req.lockmode = ExclusiveLock;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued);

	memset(&resid, 0, sizeof(resid));
	resid.field1 = 1;
	resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	init_valid_ges_request(&env, &req, GES_REQ_OPCODE_REQUEST, &resid, ExclusiveLock);
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, ++enqueued);

	resid.field1 = 0; /* not a canonical WAL-thread resource */
	memcpy(req.resid, &resid, sizeof(resid));
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued);

	memset(&resid, 0, sizeof(resid));
	resid.type = CLUSTER_IR_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	init_valid_ges_request(&env, &req, GES_REQ_OPCODE_REQUEST, &resid, ExclusiveLock);
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued);

	resid.type = CLUSTER_CF_RESID_TYPE;
	memcpy(req.resid, &resid, sizeof(resid));
	req.opcode = GES_REQ_OPCODE_CONVERT;
	req.lockmode = ShareLock;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued);

	stub_serving_ready = true;
	resid.type = CLUSTER_IR_RESID_TYPE;
	memcpy(req.resid, &resid, sizeof(resid));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.lockmode = ExclusiveLock;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, ++enqueued);

	stub_authority_managed = false;
	stub_recovery_ready = false;
	stub_serving_ready = false;
}

/* PGRAC: actual ingress and executor retain the CF-X allowlist on both
 * admission and exact-holder removal. No serving grant is implied.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_pre2_startup_cf_x_protocol_owner)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	ClusterResId cf = { 0 };
	uint64 enqueued = stub_work_queue_enqueue_count;
	uint64 mutations = stub_master_grant_mutation_count;
	uint64 drains = stub_release_and_drain_calls;

	cluster_shared_config = true;
	stub_authority_managed = true;
	stub_recovery_ready = true;
	stub_serving_ready = false;
	cf.type = CLUSTER_CF_RESID_TYPE;
	cf.lockmethodid = DEFAULT_LOCKMETHOD;
	init_valid_ges_request(&env, &req, GES_REQ_OPCODE_REQUEST, &cf, ExclusiveLock);
	req.holder_request_id_lo = 210;
	req.holder_cluster_epoch_lo = (uint32)stub_current_epoch;
	req.holder_cluster_epoch_hi = (uint32)(stub_current_epoch >> 32);
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued + 1);
	memset(&stub_work_queue_dequeue_item, 0, sizeof(stub_work_queue_dequeue_item));
	stub_work_queue_dequeue_item.routing_generation = stub_master_generation;
	stub_work_queue_dequeue_item.source_node_id = req.holder_node_id;
	stub_work_queue_dequeue_item.payload_len = sizeof(req);
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, mutations + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, (uint32)GES_REPLY_OPCODE_GRANT);

	stub_holder_mode_override = ExclusiveLock;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	memset(&stub_drained_grant, 0, sizeof(stub_drained_grant));
	stub_drained_grant.holder.node_id = 1;
	stub_drained_grant.holder.procno = 24;
	stub_drained_grant.holder.cluster_epoch = stub_current_epoch;
	stub_drained_grant.holder.request_id = 211;
	stub_drained_grant.source_node_id = 1;
	stub_drained_grant.shard_master_generation = stub_master_generation;
	stub_drained_grant.request_opcode = GES_REQ_OPCODE_REQUEST;
	stub_drained_grant.mode = ExclusiveLock;
	stub_release_and_drain_result = 1;
	req.opcode = GES_REQ_OPCODE_RELEASE;
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_release_and_drain_calls, drains + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, (uint32)GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(stub_lmon_reply_last.reply_for_opcode, (uint32)GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_lo, 211);
	stub_release_and_drain_result = 0;

	/* Queued requests must be rechecked; the transport preseal alone cannot
	 * manufacture the exclusive holder after losing the recovery seal. */
	stub_recovery_ready = false;
	stub_recovery_transport_ready = true;
	req.opcode = GES_REQ_OPCODE_REQUEST;
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, mutations + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, (uint32)GES_REPLY_OPCODE_REJECT);
	stub_holder_mode_override = NoLock;
	stub_authority_managed = false;
	stub_recovery_transport_ready = false;
	cluster_shared_config = false;
}

UT_TEST(test_ges_recovery_master_rechecks_before_mutation)
{
	GesRequestPayload req;
	ClusterResId resid;
	uint64 mutations;
	uint64 replies;

	stub_authority_managed = true;
	stub_recovery_ready = true;
	stub_serving_ready = false;
	memset(&resid, 0, sizeof(resid));
	resid.type = CLUSTER_IR_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.lockmode = AccessExclusiveLock;
	req.holder_node_id = 1;
	req.holder_request_id_lo = 201;
	req.holder_cluster_epoch_lo = (uint32)stub_current_epoch;
	req.holder_cluster_epoch_hi = (uint32)(stub_current_epoch >> 32);
	memcpy(req.resid, &resid, sizeof(resid));
	memset(&stub_work_queue_dequeue_item, 0, sizeof(stub_work_queue_dequeue_item));
	stub_work_queue_dequeue_item.routing_generation = stub_master_generation;
	stub_work_queue_dequeue_item.source_node_id = 1;
	stub_work_queue_dequeue_item.payload_len = sizeof(req);
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	mutations = stub_master_grant_mutation_count;
	replies = stub_lmon_reply_enqueue_count;

	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, mutations);
	UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, replies + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, (uint32)GES_REPLY_OPCODE_REJECT);

	memset(&resid, 0, sizeof(resid));
	resid.type = CLUSTER_CF_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	req.lockmode = ShareLock;
	memcpy(req.resid, &resid, sizeof(resid));
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, mutations + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, (uint32)GES_REPLY_OPCODE_GRANT);

	stub_authority_managed = false;
	stub_recovery_ready = false;
}

UT_TEST(test_startup_cf_local_release_and_cancel_handoff)
{
	ClusterResId cf = { 0 };
	ClusterGrdHolderId holder = { 0 };
	ClusterControlRetireMessage message = { 0 };
	uint64 before = stub_release_and_drain_calls;

	cluster_shared_config = true;
	stub_authority_managed = stub_recovery_ready = true;
	stub_serving_ready = false;
	MyAuxProcType = StartupProcess;
	cf.type = CLUSTER_CF_RESID_TYPE;
	cf.lockmethodid = DEFAULT_LOCKMETHOD;
	holder.node_id = cluster_node_id;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = 311;
	/* Exact GRD queue mutation is tested by the real GRD program. Here the
	 * returned successor verifies actual local dispatch and retirement gates. */
	memset(&stub_drained_grant, 0, sizeof(stub_drained_grant));
	stub_drained_grant.holder = holder;
	stub_drained_grant.holder.node_id = 1;
	stub_drained_grant.holder.request_id = 312;
	stub_drained_grant.source_node_id = 1;
	stub_drained_grant.shard_master_generation = stub_master_generation;
	stub_drained_grant.request_opcode = GES_REQ_OPCODE_REQUEST;
	stub_drained_grant.mode = ExclusiveLock;
	stub_release_and_drain_result = 1;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&cf, &holder), GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_release_and_drain_calls, before + 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_lo, 312);
	stub_release_and_drain_result = 0;
	MyAuxProcType = NotAnAuxProcess;

	stub_retire_driver_enabled = true;
	memset(&stub_retire_cut, 0, sizeof(stub_retire_cut));
	stub_retire_cut.master = cluster_node_id;
	stub_retire_cut.epoch = stub_current_epoch;
	stub_retire_cut.generation = stub_master_generation;
	message.key.resid = cf;
	message.key.holder = holder;
	message.cleanup_epoch = stub_current_epoch;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &stub_retire_cut),
				 CLUSTER_CONTROL_RETIRED);
	UT_ASSERT(stub_retire_drain_budget > 0);
	UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_lo, 312);
	stub_recovery_ready = false;
	stub_recovery_transport_ready = true;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &stub_retire_cut),
				 CLUSTER_CONTROL_RETIRED);
	UT_ASSERT_EQ(stub_retire_drain_budget, 0);
	stub_recovery_ready = true;
	message.key.resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	message.key.resid.field1 = 1;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &stub_retire_cut),
				 CLUSTER_CONTROL_RETIRED);
	UT_ASSERT_EQ(stub_retire_drain_budget, 0);
	MyBackendType = B_BACKEND;
	stub_retire_driver_enabled = false;
	stub_authority_managed = stub_recovery_ready = stub_recovery_transport_ready = false;
	cluster_shared_config = false;
}

UT_TEST(test_ges_starting_redeclare_uses_preseal_transport_only)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;

	memset(&resid, 0, sizeof(resid));
	resid.type = CLUSTER_CF_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 41;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = UINT64_C(0x4411);
	stub_authority_managed = true;
	stub_serving_ready = false;
	stub_recovery_ready = false;
	stub_recovery_transport_ready = true;
	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 1;
	MyAuxProcType = NotAnAuxProcess;

	result = cluster_ges_send_redeclare_and_wait(&resid, ShareLock, &holder, holder.request_id);
	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)1);

	stub_backend_request_enqueue_count = 0;
	MyAuxProcType = StartupProcess;
	result = cluster_ges_send_request_and_wait(&resid, ShareLock, &holder, holder.request_id, 1, 0);
	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_SHARD_FROZEN);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)0);

	stub_authority_managed = false;
	stub_recovery_transport_ready = false;
	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_backend_request_ready_after = 0;
	MyAuxProcType = NotAnAuxProcess;
}


/* Break caught: a retained survivor must rebuild ordinary existing grants,
 * not only the startup CF(S)/WALR(X) allowlist. New acquisitions stay frozen. */
UT_TEST(test_pre2_survivor_redeclare_send_does_not_open_requests)
{
	ClusterResId resid = { 0 };
	ClusterGrdHolderId holder = { 0 };

	resid.type = LOCKTAG_RELATION;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	resid.field1 = 5;
	resid.field2 = 16386;
	holder.node_id = 0;
	holder.procno = 41;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = UINT64_C(0x5522);
	stub_authority_managed = true;
	stub_serving_ready = false;
	stub_survivor_protocol_ready = true;
	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 1;
	UT_ASSERT_EQ(cluster_ges_send_redeclare_and_wait(&resid, ShareLock, &holder, holder.request_id),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 1);
	stub_backend_request_enqueue_count = 0;
	UT_ASSERT_EQ(
		cluster_ges_send_request_and_wait(&resid, ShareLock, &holder, holder.request_id, 1, 0),
		GES_REJECT_REASON_SHARD_FROZEN);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 0);
	stub_survivor_protocol_ready = false;
	UT_ASSERT_EQ(cluster_ges_send_redeclare_and_wait(&resid, ShareLock, &holder, holder.request_id),
				 GES_REJECT_REASON_SHARD_FROZEN);
	stub_authority_managed = false;
	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_backend_request_ready_after = 0;
}

UT_TEST(test_pre2_survivor_redeclare_master_rechecks_before_rebind)
{
	ClusterResId resid = { 0 };
	ClusterICEnvelope env = { 0 };
	GesRequestPayload req = { 0 };
	uint64 enqueued;
	uint64 ordinary_mutations;

	resid.type = LOCKTAG_RELATION;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	resid.field1 = 5;
	resid.field2 = 16386;
	req.opcode = GES_REQ_OPCODE_REDECLARE;
	req.lockmode = ShareLock;
	req.holder_node_id = 1;
	req.holder_procno = 41;
	req.holder_request_id_lo = 99;
	req.holder_cluster_epoch_lo = (uint32)stub_current_epoch;
	req.shard_master_generation_lo = 1;
	memcpy(req.resid, &resid, sizeof(resid));
	env.source_node_id = 1;
	env.epoch = stub_current_epoch;
	env.payload_length = sizeof(req);
	stub_authority_managed = true;
	stub_serving_ready = false;
	stub_survivor_protocol_ready = true;
	enqueued = stub_work_queue_enqueue_count;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued + 1);
	memset(&stub_work_queue_dequeue_item, 0, sizeof(stub_work_queue_dequeue_item));
	stub_work_queue_dequeue_item.routing_generation = stub_master_generation;
	stub_work_queue_dequeue_item.source_node_id = 1;
	stub_work_queue_dequeue_item.payload_len = sizeof(req);
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	ordinary_mutations = stub_master_grant_mutation_count;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(stub_lmon_reply_last.reply_for_opcode, GES_REQ_OPCODE_REDECLARE);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, ordinary_mutations);

	stub_survivor_protocol_ready = false;
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, GES_REPLY_OPCODE_REJECT);
	stub_survivor_protocol_ready = true;
	req.opcode = GES_REQ_OPCODE_REQUEST;
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(stub_lmon_reply_last.opcode, GES_REPLY_OPCODE_REJECT);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, ordinary_mutations);
	stub_survivor_protocol_ready = false;
	stub_authority_managed = false;
}

UT_TEST(test_pre2_survivor_done_retains_envelope_checks)
{
	ClusterICEnvelope env = { 0 };
	GesRequestPayload req = { 0 };
	int before = stub_peer_done_count;
	uint64 enqueued = stub_work_queue_enqueue_count;

	stub_authority_managed = true;
	stub_survivor_protocol_ready = true;
	req.opcode = GES_REQ_OPCODE_REDECLARE_DONE;
	req.holder_node_id = 1;
	req.holder_cluster_epoch_lo = (uint32)stub_current_epoch;
	req.holder_request_id_lo = UINT32_C(0x5511);
	env.source_node_id = 1;
	env.epoch = stub_current_epoch;
	env.payload_length = sizeof(req);
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_peer_done_count, before + 1);
	UT_ASSERT_EQ(stub_peer_done_node, 1);
	UT_ASSERT_EQ(stub_peer_done_epoch, stub_current_epoch);
	UT_ASSERT_EQ(stub_peer_done_hash, UINT64_C(0x5511));
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, enqueued);
	env.source_node_id = 2;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_peer_done_count, before + 1);
	env.source_node_id = 1;
	req.holder_cluster_epoch_lo++;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_peer_done_count, before + 1);
	req.holder_cluster_epoch_lo--;
	stub_survivor_protocol_ready = false;
	cluster_ges_request_handler(&env, &req);
	UT_ASSERT_EQ(stub_peer_done_count, before + 1);
	stub_authority_managed = false;
}

static ClusterReplacementWireMessage
make_ges_phase3_message(uint32 phase)
{
	ClusterReplacementWireMessage message;

	memset(&message, 0, sizeof(message));
	message.phase = phase;
	message.target_node_id = 3;
	message.epoch = 0;
	message.request_nonce = UINT64_C(101);
	message.identity0 = UINT64_C(202);
	message.identity1 = UINT64_C(303);
	message.grammar_fingerprint = CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	if (phase == CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY) {
		message.body.phase3.jcmk_generation = UINT64_C(404);
		message.body.phase3.episode_state_generation = UINT32_C(505);
	}
	return message;
}


static void
drain_ges_phase3_handoff(void)
{
	ClusterReplacementPhase3HandoffItem ignored;

	while (cluster_replacement_phase3_handoff_poll_local(&ignored))
		;
}


/* Break caught: opcode-18 phase 3 must fork before the legacy generic GES
 * classifier and enqueue only an authenticated formation-LMON observation. */
UT_TEST(test_ges_phase3_early_dispatch_enqueues_formation_handoff)
{
	ClusterReplacementPhase3HandoffItem item;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope env;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint64 pre_fail;
	uint64 pre_enqueue;
	uint64 pre_capability;

	cluster_ges_shmem_init();
	cluster_node_id = 1;
	drain_ges_phase3_handoff();
	message = make_ges_phase3_message(CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	memset(&env, 0, sizeof(env));
	env.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	env.source_node_id = 3;
	env.dest_node_id = 1;
	stub_current_epoch = 1;
	env.epoch = stub_current_epoch;
	env.payload_length = sizeof(bytes);
	pre_fail = stub_inbound_validation_fail;
	pre_enqueue = stub_work_queue_enqueue_count;
	pre_capability = stub_replacement_capability_sample_count;

	cluster_ges_request_handler(&env, bytes);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
	UT_ASSERT_EQ(stub_replacement_capability_sample_count, pre_capability + 1);
	UT_ASSERT_EQ(stub_replacement_required_capabilities, (uint32)0x00100000U);
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending_local(), 1);
	UT_ASSERT(cluster_replacement_phase3_handoff_poll_local(&item));
	UT_ASSERT_EQ(memcmp(&item.message, &message, sizeof(message)), 0);
	UT_ASSERT_EQ(item.authenticated_source_node_id, 3);
	UT_ASSERT_EQ(item.local_receiver_node_id, 1);
	UT_ASSERT_EQ((int)item.control_connection_generation, 7);
	stub_current_epoch = 0;
}


/* Break caught: another valid opcode-18 phase must be withheld, not cast as
 * GesRequestPayload or submitted to the legacy work queue. */
UT_TEST(test_ges_nonphase3_opcode18_never_falls_into_legacy_classifier)
{
	ClusterReplacementWireMessage message;
	ClusterICEnvelope env;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint64 pre_fail;
	uint64 pre_enqueue;

	cluster_ges_shmem_init();
	cluster_node_id = 1;
	drain_ges_phase3_handoff();
	message = make_ges_phase3_message(CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	memset(&env, 0, sizeof(env));
	env.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	env.source_node_id = 3;
	env.dest_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(bytes);
	pre_fail = stub_inbound_validation_fail;
	pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_request_handler(&env, bytes);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending_local(), 0);
}

UT_TEST(test_ges_reply_valid_payload_echoes_local_holder)
{
	ClusterICEnvelope env;
	GesReplyPayload rep;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;

	memset(&rep, 0, sizeof(rep));
	rep.opcode = GES_REPLY_OPCODE_REJECT;
	rep.reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
	rep.holder_node_id = 0;

	cluster_ges_reply_handler(&env, &rep);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
}

UT_TEST(test_ges_lmon_drain_work_queue_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_lmon_drain_work_queue);
}


UT_DEFINE_GLOBALS();

/* spec-5.14 D2: link-only stub.  This test exercises the GES enqueue logic,
 * not the touched_peers bitmap (covered by test_cluster_touched_peers + TAP). */
bool
cluster_touched_peers_stamp(int32 node_id pg_attribute_unused(),
							ClusterTouchKind kind pg_attribute_unused())
{
	return false;
}


/* spec-2.17 Step 2 — NEW T-ges-3 opcode + payload size invariant. */
static void
test_ges_opcode_enum_spec_2_17_extension(void)
{
	/* spec-2.17 Q5 v0.6:  7 opcode 全集 — BAST=4 / BAST_ACK=5 /
	 * DEADLOCK_PROBE=6 / CANCEL_PENDING=7. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_REQUEST, 1);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CONVERT, 2);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_RELEASE, 3);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST, 4);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST_ACK, 5);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_DEADLOCK_PROBE, 6);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CANCEL_PENDING, 7);
}

static void
test_ges_bast_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_bast = stub_bast_received;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* master */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST;
	req.holder_node_id = 0; /* local target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_received, pre_bast + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

static void
test_ges_cancel_pending_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_cancel = stub_lmd_cancel_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* coordinator */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_CANCEL_PENDING;
	req.holder_node_id = 0; /* local victim target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_lmd_cancel_enqueue_count, pre_cancel + 1);
	UT_ASSERT_EQ(stub_lmd_cancel_last_source, (uint32)1);
}

static void
test_ges_bast_ack_opcode_validates_as_source_holder(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_ack = stub_bast_ack;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* holder */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST_ACK;
	req.holder_node_id = 1; /* holder must match envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_ack, pre_ack + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

/* ============================================================
 * spec-2.25 T-ges-14..16 — NATIVE_LOCK_PROBE opcode dispatch tests.
 *
 *	T-ges-14:  opcode enum values 9 / 10 + opcode_max = 10 boundary.
 *	T-ges-15:  request handler dispatches to local probe + outbound reply
 *	           (HC33 source/target/sender validation prefix passes).
 *	T-ges-16:  reply handler routes status to LMS collector (recv_reply
 *	           counter increments;  HC36 stale drop deferred to TAP).
 * ============================================================ */

UT_TEST(test_ges_native_lock_probe_opcode_enum_extension)
{
	/* spec-2.25 D6:  opcode 9 + 10 ABI lock. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE, 9);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY, 10);
	/* Payload size lock.  spec-5.3 grew the probe REQUEST to 40B (+requester
	 * node/procno identity, so the peer can exclude the original requester's own
	 * sub-SUEX holds from the conflict scan);  the REPLY stays 32B. */
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbePayload), 40);
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbeReplyPayload), 32);
}

UT_TEST(test_ges_native_lock_probe_request_dispatch)
{
	/* spec-2.25 D5:  request handler probes local + emits reply. */
	GesNativeLockProbePayload probe;
	ClusterICEnvelope env;
	int pre_local = stub_native_probe_local_calls;
	int pre_outbound = stub_native_probe_outbound_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 7;
	env.epoch = 0;
	env.payload_length = sizeof(probe);

	memset(&probe, 0, sizeof(probe));
	probe.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE;
	probe.lockmode = 5;
	probe.probe_id = 0xDEADBEEFCAFEBABEull;

	cluster_ges_handle_native_lock_probe_request(&env, &probe);

	UT_ASSERT_EQ(stub_native_probe_local_calls, pre_local + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_calls, pre_outbound + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_last_dest, 7u); /* echo source */
	UT_ASSERT_EQ((int)stub_native_probe_outbound_last_len, 32);
}

UT_TEST(test_ges_native_lock_probe_reply_dispatch)
{
	/* spec-2.25 D5:  reply handler routes to LMS collector. */
	GesNativeLockProbeReplyPayload reply;
	ClusterICEnvelope env;
	int pre_recv = stub_native_probe_recv_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 3;
	env.epoch = 0;
	env.payload_length = sizeof(reply);

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY;
	reply.status = 0; /* CLUSTER_NATIVE_LOCK_PROBE_CLEAR — value 0 */
	reply.probe_id = 0xAAAAAAAA00000001ull;
	reply.sender_node_id = 3; /* HC33 dual-source: == env.source_node_id */

	cluster_ges_handle_native_lock_probe_reply(&env, &reply);

	UT_ASSERT_EQ(stub_native_probe_recv_calls, pre_recv + 1);
}

/*
 * P0#3 regression: a finite remote REQUEST timeout must remove the exact
 * master-side waiter.  The deadlock-victim path already sends CANCEL_WAIT;
 * the ordinary timeout path historically left the waiter + WFG edge behind.
 */
UT_TEST(test_ges_request_timeout_sends_wait_seq_exact_cancel_wait)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;

	memset(&resid, 0x5A, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 0;
	holder.request_id = UINT64CONST(0x1122334455667788);

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_now = 0;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;
	memset(&stub_backend_request_last, 0, sizeof(stub_backend_request_last));
	memset(&stub_cancel_wait_last, 0, sizeof(stub_cancel_wait_last));

	result = cluster_ges_send_request_and_wait(&resid, AccessExclusiveLock, &holder,
											   holder.request_id, 1, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_TIMEOUT);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cancel_wait_last_dest, (uint32)7);
	UT_ASSERT_EQ(stub_cancel_wait_last.opcode, (uint32)GES_REQ_OPCODE_CANCEL_WAIT);
	UT_ASSERT_EQ(stub_cancel_wait_last.kind, (uint32)GES_CANCEL_WAIT_KIND_REQUEST);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_node_id, holder.node_id);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_procno, holder.procno);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_cluster_epoch, holder.cluster_epoch);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_request_id, holder.request_id);
	UT_ASSERT_EQ(stub_cancel_wait_last.wait_seq, stub_backend_request_last.wait_seq);
	UT_ASSERT(memcmp(stub_cancel_wait_last.resid, &resid, sizeof(resid)) == 0);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_now = 0;
}

/*
 * PostgreSQL's ConditionVariableTimedSleep() returns true when the timeout
 * expires and false when the CV is signaled.  A timed-out remote GES wait must
 * therefore enter the retransmit leg before the absolute deadline expires.
 * The second enqueue below stands in for the retry reaching a now-ready
 * master and delivering the grant.
 */
UT_TEST(test_ges_request_cv_timeout_retransmits)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;

	memset(&resid, 0x4C, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 21;
	holder.cluster_epoch = 0;
	holder.request_id = UINT64CONST(0x0102030405060708);

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = false;
	stub_now = 0;
	stub_cv_timeout_expires = true;
	stub_cv_now_after_sleep = INT64CONST(2000000);
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 2;

	result = cluster_ges_send_request_and_wait(&resid, AccessExclusiveLock, &holder,
											   holder.request_id, 1000, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)2);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_cv_now_after_sleep = 0;
	stub_backend_request_ready_after = 0;
}

UT_TEST(test_retry_allowance_does_not_preempt_original_wait_deadline)
{
	ClusterResId resid = { 0 };
	ClusterGrdHolderId holder = { 0 };
	uint32 result;

	holder.node_id = 0;
	holder.procno = 21;
	holder.request_id = 1001;
	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_now = 0;
	stub_clock_advances = false;
	stub_cv_timeout_expires = true;
	stub_cv_waits = 0;
	stub_cv_grant_on_wait = cluster_ges_retransmit_max_attempts + 3;
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 0;
	result = cluster_ges_send_request_and_wait(&resid, AccessExclusiveLock, &holder,
											   holder.request_id, 60000, 0);
	UT_ASSERT_EQ(result, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 1 + cluster_ges_retransmit_max_attempts);
	UT_ASSERT_EQ(stub_cv_waits, stub_cv_grant_on_wait);
	stub_cv_grant_on_wait = 0;
	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
}

UT_TEST(test_exact_cancel_completes_original_dedup_identity)
{
	ClusterICEnvelope env = { 0 };
	GesCancelWaitPayload cancel = { 0 };
	uint64 before;

	cluster_node_id = 0;
	env.source_node_id = 2; /* coordinator is not the original requester */
	env.payload_length = sizeof(cancel);
	cancel.opcode = GES_REQ_OPCODE_CANCEL_WAIT;
	cancel.waiter_node_id = 6;
	cancel.waiter_procno = 27;
	cancel.waiter_request_id = 1005;
	cancel.waiter_cluster_epoch = 19;
	cancel.wait_seq = 31;
	for (int leg = 0; leg < 2; leg++) {
		cancel.kind = leg == 0 ? GES_CANCEL_WAIT_KIND_REQUEST : GES_CANCEL_WAIT_KIND_CONVERT;
		before = stub_dedup_record_count;
		stub_cancel_match = true;
		cluster_ges_request_handler(&env, &cancel);
		UT_ASSERT_EQ(stub_dedup_record_count, before + 1);
		UT_ASSERT_EQ(stub_dedup_record_key.origin_node_id, 6);
		UT_ASSERT_EQ(stub_dedup_record_key.request_id, 1005);
		UT_ASSERT_EQ(stub_dedup_record_key.holder_procno, 27);
		UT_ASSERT_EQ(stub_dedup_record_key.cluster_epoch, 19);
		UT_ASSERT_EQ(stub_dedup_record_key.shard_master_generation, 71);
		UT_ASSERT_EQ(stub_dedup_record_key.opcode,
					 leg == 0 ? GES_REQ_OPCODE_REQUEST : GES_REQ_OPCODE_CONVERT);
		UT_ASSERT_EQ(stub_dedup_record_reply.opcode, GES_REPLY_OPCODE_REJECT);
		UT_ASSERT_EQ(stub_dedup_record_reply.reject_reason, GES_REJECT_REASON_TIMEOUT);
		stub_cancel_match = false; /* absent / stale / grant-won: do not rewrite */
		cluster_ges_request_handler(&env, &cancel);
		UT_ASSERT_EQ(stub_dedup_record_count, before + 1);
	}
}

UT_TEST(test_ges_release_cv_timeout_retransmits)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;

	memset(&resid, 0x6D, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 22;
	holder.cluster_epoch = 0;
	holder.request_id = UINT64CONST(0x1112131415161718);

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = false;
	stub_now = 0;
	stub_cv_timeout_expires = true;
	stub_cv_now_after_sleep = INT64CONST(61000000);
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 2;

	result = cluster_ges_send_release_and_wait(&resid, &holder, holder.request_id, 0, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)2);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_cv_now_after_sleep = 0;
	stub_backend_request_ready_after = 0;
}

UT_TEST(test_ges_local_release_requires_available_authority_and_stable_master)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	int32 saved_node = cluster_node_id;

	memset(&resid, 0, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	cluster_node_id = 0;
	stub_remote_master = -1;
	stub_master_generation = 7;
	stub_master_gen_lookup_calls = 0;
	stub_remaster_on_second_lookup = false;
	stub_release_and_drain_result = CLUSTER_GRD_RELEASE_NOT_READY;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder), GES_REJECT_REASON_TIMEOUT);

	stub_master_gen_lookup_calls = 0;
	stub_release_and_drain_result = 0;
	stub_remaster_on_second_lookup = true;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder),
				 GES_REJECT_REASON_MASTER_DEAD_NATIVE);

	stub_master_gen_lookup_calls = 0;
	stub_remaster_on_second_lookup = false;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder), GES_REJECT_REASON_NONE);
	cluster_node_id = saved_node;
}

/* Break caught: local RELEASE must confirm an exact absent holder just as
 * the remote RELEASE owner does, but never turn unavailable authority into
 * absence. The real GRD exact-removal tests cover the substituted boundary. */
UT_TEST(test_ges_local_release_confirms_absence_only_at_stable_master)
{
	ClusterResId resid = { 0 };
	ClusterGrdHolderId holder = { 0 };
	uint64 replies = stub_lmon_reply_enqueue_count;

	holder.node_id = cluster_node_id;
	holder.procno = 41;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = 987;
	stub_release_and_drain_result = CLUSTER_GRD_RELEASE_NOT_FOUND;
	stub_master_gen_lookup_calls = stub_release_and_drain_calls = 0;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder), GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(stub_release_and_drain_calls, 1);
	UT_ASSERT_EQ(stub_master_gen_lookup_calls, 2);
	UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, replies);

	stub_master_gen_lookup_calls = 0;
	stub_remaster_on_second_lookup = true;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder),
				 GES_REJECT_REASON_MASTER_DEAD_NATIVE);
	stub_remaster_on_second_lookup = false;
	stub_master_unknown = true;
	stub_release_and_drain_calls = 0;
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder),
				 GES_REJECT_REASON_MASTER_DEAD_NATIVE);
	UT_ASSERT_EQ(stub_release_and_drain_calls, 0);
	stub_master_unknown = false;
	stub_release_and_drain_result = 0;
}

UT_TEST(test_ges_recovery_local_retirement_never_drains_ordinary_waiters)
{
	ClusterResId resid = { 0 };
	ClusterGrdHolderId holder = { 0 };
	unsigned int kind;

	holder.node_id = cluster_node_id;
	holder.procno = 41;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = 988;
	stub_authority_managed = true;
	stub_recovery_ready = true;
	stub_serving_ready = false;
	MyAuxProcType = StartupProcess;
	stub_holder_absent = true;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	stub_release_and_drain_calls = 0;
	for (kind = 0; kind < 2; kind++) {
		memset(&resid, 0, sizeof(resid));
		resid.type = kind == 0 ? CLUSTER_CF_RESID_TYPE : CLUSTER_WAL_RETENTION_RESID_TYPE;
		resid.field1 = kind == 0 ? 0 : 2;
		resid.lockmethodid = DEFAULT_LOCKMETHOD;
		stub_exact_release_calls = stub_master_gen_lookup_calls = 0;
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder), GES_REJECT_REASON_NONE);
		UT_ASSERT_EQ(stub_exact_release_calls, 1);
		UT_ASSERT_EQ(stub_exact_release_holder.request_id, (uint64)988);
		UT_ASSERT_EQ(stub_master_gen_lookup_calls, 2);
		UT_ASSERT_EQ(stub_release_and_drain_calls, 0);
	}
	stub_holder_absent = false;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	stub_authority_managed = stub_recovery_ready = false;
	MyAuxProcType = NotAnAuxProcess;
}

UT_TEST(test_ges_recovery_local_retirement_retains_authority_guards)
{
	ClusterResId resid = { 0 };
	ClusterGrdHolderId holder = { 0 };
	unsigned int scenario;
	const uint32 expected[] = { GES_REJECT_REASON_MASTER_DEAD_NATIVE,
								GES_REJECT_REASON_MASTER_DEAD_NATIVE,
								GES_REJECT_REASON_MASTER_DEAD_NATIVE,
								GES_REJECT_REASON_SHARD_FROZEN,
								GES_REJECT_REASON_TIMEOUT,
								GES_REJECT_REASON_SHARD_FROZEN,
								GES_REJECT_REASON_SHARD_FROZEN };

	resid.type = CLUSTER_CF_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	holder.node_id = cluster_node_id;
	holder.procno = 41;
	holder.cluster_epoch = stub_current_epoch;
	holder.request_id = 989;
	stub_authority_managed = true;
	stub_serving_ready = false;
	MyAuxProcType = StartupProcess;
	for (scenario = 0; scenario < lengthof(expected); scenario++) {
		stub_recovery_ready = true;
		stub_master_gen_lookup_calls = stub_exact_release_calls = stub_release_and_drain_calls = 0;
		stub_master_unknown = scenario == 0;
		stub_remote_master = scenario == 1 ? 7 : -1;
		stub_remaster_on_second_lookup = scenario == 2;
		stub_holder_mode_override = scenario == 3 ? ExclusiveLock : NoLock;
		stub_exact_release_result = scenario == 4 ? CLUSTER_GRD_ENTRY_ERROR : CLUSTER_GRD_ENTRY_OK;
		stub_readiness_lost_on_release = scenario == 5;
		resid.field1 = scenario == 6 ? 1 : 0; /* malformed CF, not a new resource */
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&resid, &holder), expected[scenario]);
		UT_ASSERT_EQ(stub_release_and_drain_calls, 0);
		UT_ASSERT_EQ(stub_exact_release_calls,
					 scenario == 2 || scenario == 4 || scenario == 5 ? 1 : 0);
	}
	stub_master_unknown = stub_remaster_on_second_lookup = false;
	stub_remote_master = -1;
	stub_holder_mode_override = NoLock;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	stub_readiness_lost_on_release = false;
	stub_authority_managed = stub_recovery_ready = false;
	MyAuxProcType = NotAnAuxProcess;
}

/* Drive the real remote mutation owner; only the GRD/storage and transport
 * boundaries are substituted. The emitted RELEASE reply is the verdict.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
queue_exact_recovery_release(const ClusterResId *resid)
{
	GesRequestPayload req = { 0 };

	req.opcode = GES_REQ_OPCODE_RELEASE;
	req.holder_node_id = 1;
	req.holder_procno = 41;
	req.holder_cluster_epoch_lo = (uint32)stub_current_epoch;
	req.holder_cluster_epoch_hi = (uint32)(stub_current_epoch >> 32);
	req.holder_request_id_lo = UINT32_C(0x15161718);
	req.holder_request_id_hi = UINT32_C(0x11121314);
	req.shard_master_generation_lo = (uint32)stub_master_generation;
	memcpy(req.resid, resid, sizeof(*resid));
	memset(&stub_work_queue_dequeue_item, 0, sizeof(stub_work_queue_dequeue_item));
	stub_work_queue_dequeue_item.routing_generation = stub_master_generation;
	stub_work_queue_dequeue_item.source_node_id = 1;
	stub_work_queue_dequeue_item.payload_len = sizeof(req);
	memcpy(stub_work_queue_dequeue_item.payload, &req, sizeof(req));
	stub_work_queue_dequeue_pending = true;
}

/* Break caught: a duplicate release must reach exact removal, not be rejected
 * merely because the mode lookup no longer finds the already-retired holder. */
UT_TEST(test_ges_recovery_remote_retirement_confirms_exact_absence)
{
	ClusterResId resid = { 0 };
	unsigned int scenario;
	uint64 grants = stub_master_grant_mutation_count;

	stub_authority_managed = stub_recovery_ready = true;
	stub_serving_ready = false;
	for (scenario = 0; scenario < 4; scenario++) {
		uint64 replies = stub_lmon_reply_enqueue_count;

		resid.type = scenario < 2 ? CLUSTER_CF_RESID_TYPE : CLUSTER_WAL_RETENTION_RESID_TYPE;
		resid.field1 = scenario < 2 ? 0 : 2;
		resid.lockmethodid = DEFAULT_LOCKMETHOD;
		stub_holder_absent = (scenario % 2) != 0;
		stub_exact_release_result
			= stub_holder_absent ? CLUSTER_GRD_ENTRY_NOT_FOUND : CLUSTER_GRD_ENTRY_OK;
		stub_exact_release_calls = stub_master_gen_lookup_calls = stub_release_and_drain_calls = 0;
		queue_exact_recovery_release(&resid);
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, replies + 1);
		UT_ASSERT_EQ(stub_lmon_reply_last.opcode, GES_REPLY_OPCODE_GRANT);
		UT_ASSERT_EQ(stub_lmon_reply_last.reply_for_opcode, GES_REQ_OPCODE_RELEASE);
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_node_id, 1);
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_procno, 41);
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_lo, UINT32_C(0x15161718));
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_hi, UINT32_C(0x11121314));
		UT_ASSERT_EQ(stub_exact_release_calls, 1);
		UT_ASSERT_EQ(stub_release_and_drain_calls, 0);
		UT_ASSERT_EQ(stub_master_grant_mutation_count, grants);
	}
	stub_holder_absent = false;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	stub_authority_managed = stub_recovery_ready = false;
}

/* Break caught: neither an absent local copy at the wrong master nor a
 * readiness/map change across removal certifies remote retirement. */
UT_TEST(test_ges_recovery_remote_retirement_refuses_unproven_absence)
{
	ClusterResId resid = { 0 };
	unsigned int scenario;
	const uint32 reasons[] = {
		GES_REJECT_REASON_MASTER_DEAD_NATIVE, /* unknown current master */
		GES_REJECT_REASON_MASTER_DEAD_NATIVE, /* another master */
		GES_REJECT_REASON_MASTER_DEAD_NATIVE, /* routing changed at removal */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* present disallowed mode */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* lookup false, GRD not ready */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* readiness lost at removal */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* malformed CF identity */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* readiness lost before removal */
		GES_REJECT_REASON_WORK_QUEUE_FULL,	  /* real GRD error */
	};
	uint64 grants = stub_master_grant_mutation_count;

	resid.type = CLUSTER_CF_RESID_TYPE;
	resid.lockmethodid = DEFAULT_LOCKMETHOD;
	stub_authority_managed = true;
	stub_serving_ready = false;
	for (scenario = 0; scenario < lengthof(reasons); scenario++) {
		uint64 replies = stub_lmon_reply_enqueue_count;
		uint64 forgotten = stub_dedup_remove_completed_count;

		stub_recovery_ready = scenario != 7;
		stub_master_unknown = scenario == 0;
		stub_remote_master = scenario == 1 ? 7 : -1;
		stub_remaster_on_second_lookup = scenario == 2;
		stub_holder_mode_override = scenario == 3 ? ExclusiveLock : NoLock;
		stub_holder_absent = scenario == 4;
		stub_exact_release_result
			= scenario == 4 ? CLUSTER_GRD_ENTRY_NOT_READY
							: (scenario == 8 ? CLUSTER_GRD_ENTRY_ERROR : CLUSTER_GRD_ENTRY_OK);
		stub_readiness_lost_on_release = scenario == 5;
		resid.field1 = scenario == 6 ? 1 : 0;
		stub_exact_release_calls = stub_master_gen_lookup_calls = stub_release_and_drain_calls = 0;
		queue_exact_recovery_release(&resid);
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, replies + 1);
		UT_ASSERT_EQ(stub_lmon_reply_last.opcode, GES_REPLY_OPCODE_REJECT);
		UT_ASSERT_EQ(stub_lmon_reply_last.reply_for_opcode, GES_REQ_OPCODE_RELEASE);
		UT_ASSERT_EQ(stub_lmon_reply_last.reject_reason, reasons[scenario]);
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_lo, UINT32_C(0x15161718));
		UT_ASSERT_EQ(stub_lmon_reply_last.holder_request_id_hi, UINT32_C(0x11121314));
		UT_ASSERT_EQ(stub_dedup_remove_completed_count, forgotten);
		UT_ASSERT_EQ(stub_exact_release_calls,
					 scenario == 2 || scenario == 4 || scenario == 5 || scenario == 8 ? 1 : 0);
		UT_ASSERT_EQ(stub_release_and_drain_calls, 0);
		UT_ASSERT_EQ(stub_master_grant_mutation_count, grants);
	}
	stub_master_unknown = stub_remaster_on_second_lookup = false;
	stub_remote_master = -1;
	stub_holder_mode_override = NoLock;
	stub_holder_absent = stub_readiness_lost_on_release = false;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	stub_authority_managed = stub_recovery_ready = false;
}

UT_TEST(test_block0_protected_failure_detail_is_not_elapsed_timeout)
{
	const ClusterGesTimeoutSrc sources[]
		= { CLUSTER_GES_TSRC_BLOCK0_REPLY_MISSING, CLUSTER_GES_TSRC_BLOCK0_REPLY_ABANDONED,
			CLUSTER_GES_TSRC_BLOCK0_GUARD_INCONSISTENT, CLUSTER_GES_TSRC_BLOCK0_MASTER_REJECT };
	const char *names[] = { "block0-reply-missing", "block0-reply-abandoned",
							"block0-guard-inconsistent", "block0-master-reject" };
	unsigned int i;

	for (i = 0; i < lengthof(sources); i++) {
		const ClusterGesTimeoutDetail *detail;

		cluster_ges_timeout_detail_set(sources[i], 2, 4700, 5, -1, 60000);
		detail = cluster_ges_timeout_detail_get();
		UT_ASSERT_EQ(detail->source, sources[i]);
		UT_ASSERT_EQ(detail->master_node, 2);
		UT_ASSERT_EQ(detail->attempts, 5);
		UT_ASSERT_EQ(detail->elapsed_ms, 4700);
		UT_ASSERT_EQ(strcmp(cluster_ges_timeout_src_text(detail->source), names[i]), 0);
	}
	cluster_ges_timeout_detail_reset();
	UT_ASSERT_EQ(cluster_ges_timeout_detail_get()->source, CLUSTER_GES_TSRC_NONE);
}

UT_TEST(test_ges_probe_validates_identity_then_obeys_final_stop_seal)
{
	ClusterICEnvelope env = { 0 };
	GesDeadlockProbePayload probe = { 0 };
	cluster_ges_shmem_init();
	cluster_node_id = 0;
	env.source_node_id = 1;
	env.epoch = stub_current_epoch;
	env.payload_length = sizeof(probe);
	probe.opcode = GES_REQ_OPCODE_DEADLOCK_PROBE;
	probe.coordinator_node_id = 1;
	probe.probe_id = 17;
	stub_lmd_ready = true;
	stub_authority_managed = false;
	stub_stop_read_allowed = true;
	stub_stop_read_calls = stub_report_sends = 0;
	cluster_ges_request_handler(&env, &probe);
	UT_ASSERT_EQ(stub_report_sends, 1);
	UT_ASSERT_EQ(stub_stop_read_calls, 1);
	stub_stop_read_allowed = false;
	cluster_ges_request_handler(&env, &probe);
	UT_ASSERT_EQ(stub_report_sends, 1);
	UT_ASSERT_EQ(stub_stop_read_calls, 2);
	probe.coordinator_node_id = 2;
	cluster_ges_request_handler(&env, &probe);
	UT_ASSERT_EQ(stub_report_sends, 1);
	UT_ASSERT_EQ(stub_stop_read_calls, 2);
	stub_lmd_ready = false;
	stub_stop_read_allowed = true;
}

/* Real GES scheduling with a controlled exact-key transport boundary. The
 * breaks caught are false ACK, replacement of an in-flight identity, and
 * blocking the service process on its own reply. Author: SqlRush */
static void
cooperative_fixture(ClusterResId *resid, ClusterGrdHolderId *holder)
{
	memset(resid, 0, sizeof(*resid));
	resid->type = CLUSTER_CF_RESID_TYPE;
	resid->lockmethodid = DEFAULT_LOCKMETHOD;
	memset(holder, 0, sizeof(*holder));
	holder->node_id = 0;
	holder->procno = 41;
	holder->cluster_epoch = 81;
	holder->request_id = 5522;
	cluster_node_id = 0;
	stub_current_epoch = 81;
	stub_master_generation = 71;
	stub_remote_master = 7;
	stub_master_unknown = false;
	stub_remaster_on_second_lookup = false;
	stub_master_gen_lookup_calls = 0;
	stub_authority_managed = true;
	stub_serving_ready = false;
	stub_survivor_protocol_ready = true;
	stub_recovery_ready = stub_recovery_transport_ready = false;
	stub_cooperative_reply_table = true;
	stub_reply_wait_insert_enabled = true;
	stub_cooperative_reply_present = false;
	stub_cooperative_change_cut_on_poll = false;
	stub_cooperative_outbound_full = false;
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_ready_after = 0;
	stub_cooperative_inserts = stub_cooperative_cv_calls = 0;
	stub_rebind_calls = 0;
	stub_rebind_result = CLUSTER_GRD_ENTRY_OK;
	stub_clock_advances = false;
	stub_now = 1000000;
	memset(&stub_reply_wait_entry, 0, sizeof(stub_reply_wait_entry));
}

static void
cooperative_reply(uint32 opcode, uint32 reason)
{
	stub_reply_wait_entry.ready = true;
	stub_reply_wait_entry.reply_opcode = opcode;
	stub_reply_wait_entry.reject_reason = reason;
}

UT_TEST(test_redeclare_poll_pending_keeps_one_identity_without_sleep)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;
	GesRequestPayload first;

	cooperative_fixture(&resid, &holder);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(attempt.initialized && attempt.wait_registered && attempt.sent);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 1);
	UT_ASSERT_EQ(stub_cooperative_inserts, 1);
	UT_ASSERT_EQ(stub_reply_wait_entry.deadline, 0);
	first = stub_backend_request_last;
	UT_ASSERT_EQ(first.opcode, GES_REQ_OPCODE_REDECLARE);
	UT_ASSERT_EQ(first.holder_request_id_lo, 5522);
	UT_ASSERT_EQ(first.holder_cluster_epoch_lo, 81);
	UT_ASSERT_EQ(first.shard_master_generation_lo, 71);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 1);
	stub_now += 100000;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 2);
	UT_ASSERT_EQ(memcmp(&first, &stub_backend_request_last, sizeof(first)), 0);
	UT_ASSERT_EQ(stub_cooperative_inserts, 1);
	UT_ASSERT_EQ(stub_cooperative_cv_calls, 0);
}

UT_TEST(test_redeclare_poll_late_grant_confirms_only_exact_attempt)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;

	cooperative_fixture(&resid, &holder);
	(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
	stub_now += INT64CONST(120000000);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT(attempt.confirmed && !attempt.wait_registered);
	UT_ASSERT(!stub_cooperative_reply_present);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 1);
	UT_ASSERT_EQ(stub_cooperative_cv_calls, 0);
}

UT_TEST(test_redeclare_poll_mismatched_inputs_never_replace_pending)
{
	ClusterGesRedeclareAttempt attempt = { 0 }, saved;
	ClusterResId resid, other;
	ClusterGrdHolderId holder, changed;

	cooperative_fixture(&resid, &holder);
	(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
	saved = attempt;
	other = resid;
	other.field1 = 999;
	changed = holder;
	changed.request_id++;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &other, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ExclusiveLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &changed),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT_EQ(memcmp(&attempt, &saved, sizeof(saved)), 0);
	UT_ASSERT(stub_cooperative_reply_present);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
}

UT_TEST(test_redeclare_poll_capacity_refusal_retains_reply_and_retry_key)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;
	GesRequestPayload first;

	cooperative_fixture(&resid, &holder);
	stub_cooperative_outbound_full = true;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(attempt.wait_registered);
	UT_ASSERT(!attempt.sent && !attempt.confirmed);
	first = stub_backend_request_last;
	stub_cooperative_outbound_full = false;
	stub_now += 100000;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(attempt.sent);
	UT_ASSERT_EQ(stub_cooperative_inserts, 1);
	UT_ASSERT_EQ(memcmp(&first, &stub_backend_request_last, sizeof(first)), 0);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
}

UT_TEST(test_redeclare_poll_cut_changes_never_ack_or_forget)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		ClusterGesRedeclareAttempt attempt = { 0 };
		ClusterResId resid;
		ClusterGrdHolderId holder;

		cooperative_fixture(&resid, &holder);
		(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
		if (scenario == 0)
			stub_current_epoch++;
		else if (scenario == 1)
			stub_remote_master = 0;
		else if (scenario == 2)
			stub_remote_master = 3;
		else
			stub_master_unknown = true;
		cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
		UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
					 CLUSTER_GES_REDECLARE_CUT_CHANGED);
		UT_ASSERT(!attempt.confirmed && attempt.wait_registered);
		UT_ASSERT(stub_cooperative_reply_present);
		UT_ASSERT_EQ(attempt.key.request_id, 5522);
	}
}

UT_TEST(test_redeclare_poll_readiness_loss_preserves_late_ack)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;

	cooperative_fixture(&resid, &holder);
	(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
	stub_survivor_protocol_ready = false;
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(stub_cooperative_reply_present && !attempt.confirmed);
	stub_survivor_protocol_ready = true;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	stub_survivor_protocol_ready = false;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
}

UT_TEST(test_redeclare_poll_local_rebind_is_not_fresh_acquisition)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 mutations;

	cooperative_fixture(&resid, &holder);
	stub_remote_master = 0;
	mutations = stub_master_grant_mutation_count;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(stub_rebind_calls, 1);
	UT_ASSERT_EQ(stub_rebind_holder.request_id, 5522);
	UT_ASSERT_EQ(stub_rebind_mode, ShareLock);
	UT_ASSERT_EQ(stub_master_grant_mutation_count, mutations);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 0);
	UT_ASSERT_EQ(stub_cooperative_inserts, 0);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(stub_rebind_calls, 1);
	/* Here this process is the master, so its changed generation really
	 * does invalidate the retained local authority. It is not the remote
	 * exchange's unrelated sender counter. */
	stub_master_generation++;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CUT_CHANGED);
	UT_ASSERT_EQ(stub_rebind_calls, 1);
}

UT_TEST(test_redeclare_poll_invalid_or_unknown_route_never_grants)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;

	cooperative_fixture(&resid, &holder);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(NULL, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, NULL, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, NoLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	holder.request_id = 0;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	holder.request_id = 5522;
	stub_master_unknown = true;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(!attempt.initialized);
	UT_ASSERT_EQ(stub_rebind_calls, 0);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, 0);
}

UT_TEST(test_redeclare_poll_reject_and_post_reply_cut_are_not_ack)
{
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;

	cooperative_fixture(&resid, &holder);
	(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
	cooperative_reply(GES_REPLY_OPCODE_REJECT, GES_REJECT_REASON_LOCK_CONFLICT);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_REJECTED);
	UT_ASSERT_EQ(attempt.reject_reason, GES_REJECT_REASON_LOCK_CONFLICT);
	UT_ASSERT(!attempt.confirmed && !attempt.wait_registered);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_REJECTED);
	memset(&attempt, 0, sizeof(attempt));
	cooperative_fixture(&resid, &holder);
	(void)cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	stub_cooperative_change_cut_on_poll = true;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &resid, ShareLock, &holder),
				 CLUSTER_GES_REDECLARE_CUT_CHANGED);
	UT_ASSERT_EQ(attempt.key.request_id, 5522);
	UT_ASSERT(!attempt.wait_registered);
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(49);
	UT_RUN(test_block0_protected_failure_detail_is_not_elapsed_timeout);

	UT_RUN(test_ges_request_handler_linkable);
	UT_RUN(test_ges_reply_handler_linkable);
	UT_RUN(test_ges_accessors_linkable_and_initial_zero);
	UT_RUN(test_ges_request_handler_real_behavior);
	UT_RUN(test_ges_reply_handler_real_behavior);
	UT_RUN(test_ges_handler_counter_monotonic_n_invocations);
	UT_RUN(test_ges_request_valid_payload_enqueues_work);
	UT_RUN(test_ges_release_bypasses_dedup_and_reclaims_acquire_receipts);
	UT_RUN(test_ges_recovery_ingress_exact_allowlist);
	UT_RUN(test_pre2_startup_cf_x_protocol_owner);
	UT_RUN(test_startup_cf_local_release_and_cancel_handoff);
	UT_RUN(test_ges_recovery_master_rechecks_before_mutation);
	UT_RUN(test_ges_starting_redeclare_uses_preseal_transport_only);
	UT_RUN(test_pre2_survivor_redeclare_send_does_not_open_requests);
	UT_RUN(test_pre2_survivor_redeclare_master_rechecks_before_rebind);
	UT_RUN(test_pre2_survivor_done_retains_envelope_checks);
	UT_RUN(test_ges_phase3_early_dispatch_enqueues_formation_handoff);
	UT_RUN(test_ges_nonphase3_opcode18_never_falls_into_legacy_classifier);
	UT_RUN(test_ges_reply_valid_payload_echoes_local_holder);
	UT_RUN(test_ges_lmon_drain_work_queue_symbol_linkable);
	UT_RUN(test_ges_opcode_enum_spec_2_17_extension);
	UT_RUN(test_ges_bast_opcode_validates_as_target_local);
	UT_RUN(test_ges_cancel_pending_opcode_validates_as_target_local);
	UT_RUN(test_ges_bast_ack_opcode_validates_as_source_holder);
	UT_RUN(test_ges_native_lock_probe_opcode_enum_extension);
	UT_RUN(test_ges_native_lock_probe_request_dispatch);
	UT_RUN(test_ges_native_lock_probe_reply_dispatch);
	UT_RUN(test_ges_request_timeout_sends_wait_seq_exact_cancel_wait);
	UT_RUN(test_ges_request_cv_timeout_retransmits);
	UT_RUN(test_ges_release_cv_timeout_retransmits);
	UT_RUN(test_ges_local_release_requires_available_authority_and_stable_master);
	UT_RUN(test_ges_local_release_confirms_absence_only_at_stable_master);
	UT_RUN(test_ges_recovery_local_retirement_never_drains_ordinary_waiters);
	UT_RUN(test_ges_recovery_local_retirement_retains_authority_guards);
	UT_RUN(test_ges_recovery_remote_retirement_confirms_exact_absence);
	UT_RUN(test_ges_recovery_remote_retirement_refuses_unproven_absence);
	UT_RUN(test_ges_probe_validates_identity_then_obeys_final_stop_seal);
	UT_RUN(test_retry_allowance_does_not_preempt_original_wait_deadline);
	UT_RUN(test_exact_cancel_completes_original_dedup_identity);
	UT_RUN(test_redeclare_poll_pending_keeps_one_identity_without_sleep);
	UT_RUN(test_redeclare_poll_late_grant_confirms_only_exact_attempt);
	UT_RUN(test_redeclare_poll_mismatched_inputs_never_replace_pending);
	UT_RUN(test_redeclare_poll_capacity_refusal_retains_reply_and_retry_key);
	UT_RUN(test_redeclare_poll_cut_changes_never_ack_or_forget);
	UT_RUN(test_redeclare_poll_readiness_loss_preserves_late_ack);
	UT_RUN(test_redeclare_poll_local_rebind_is_not_fresh_acquisition);
	UT_RUN(test_redeclare_poll_invalid_or_unknown_route_never_grants);
	UT_RUN(test_redeclare_poll_reject_and_post_reply_cut_are_not_ack);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
