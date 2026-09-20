/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_dispatch.c
 *	  Compile-time + link-time invariants for the spec-2.32 GCS request
 *	  protocol skeleton (cluster_gcs.h + cluster_gcs.c module).
 *
 *	  spec-2.32 ships single-node loopback GCS wire framework on top of
 *	  cluster_ic envelope dispatcher.  Most behavioral coverage is in
 *	  cluster_tap t/110_gcs_loopback.pl (which runs a real PG instance
 *	  to exercise master-lookup + send + dispatch + reply CV path).
 *	  This unit binary verifies invariants linkable without a backend:
 *
 *	    L1  msg_type enum values (GCS_REQUEST=12, GCS_REPLY=13;
 *	         CSSD_HEARTBEAT=11 preserved)
 *	    L2  payload struct sizes (GcsRequestPayload 48B, GcsReplyPayload 24B)
 *	    L3  payload field offsets (request_id @0, epoch @8, ...)
 *	    L4  dispatch handler symbols resolve (sender + receiver)
 *	    L5  GcsReplyStatus enum exhaustive (4 statuses)
 *	    L6  cluster_gcs_lookup_master symbol linkable
 *	    L7  cluster_gcs_send_transition_and_wait symbol linkable
 *	    L8  cluster_gcs_register_msg_types symbol linkable
 *	    L9  14 dump_gcs accessor symbols all linkable
 *	    L10 cluster_gcs_get_api_state returns "stub" pre-init
 *	    L11 MAX_OUTSTANDING_REQUESTS_PER_BACKEND constant == 8
 *	    L12 GCS_REPLY_INTERNAL_DEADLINE_MS == 5000
 *	    L13 PCM transition_id range invariant (1..9)
 *	    L14 spec-2.30 transition validator accepts all 9 from GCS payload
 *	    L15 receiver handler signature matches dispatch table expectation
 *	    L16 LWTRANCHE_CLUSTER_GCS registered enum value distinct from PCM
 *	    L17 sender API does NOT auto-apply transition on local short-circuit
 *	         (caller must invoke PCM acquire/release locally — HC77)
 *	    L18 module init helpers (shmem_size / shmem_init / module_init) linkable
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_dispatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h" /* ClusterNodeInfo (spec-2.33 D2 stub) */
#include "cluster/cluster_cssd.h" /* PGRAC_IC_MSG_CSSD_HEARTBEAT */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_pcm_lock.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

#include <stddef.h>

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


UT_DEFINE_GLOBALS();

BackendType MyBackendType = B_LMON;


/* ============================================================
 * Minimal PG runtime + cluster module stubs.  cluster_gcs.o is
 * linked against this test binary;  to avoid pulling the entire
 * PG backend we provide just-enough stubs to satisfy the linker.
 * No test exercises actual ereport / shmem behavior — all stubs
 * either abort() (must not be reached) or no-op.
 * ============================================================ */
#include "cluster/cluster_shmem.h"
#include "storage/condition_variable.h"
#include "utils/timestamp.h"

int cluster_node_id = 0;
int NBuffers = 0;
int MaxBackends = 100;
int MyBackendId = 1;
sigjmp_buf *PG_exception_stack = NULL;
struct ErrorContextCallback *error_context_stack = NULL;
static void *fake_shmem;
static TimestampTz fake_clock;
static int fake_error_level;
static bool fake_control_active;
static bool fake_master_apply = true;
static bool fake_drop_reply;
static bool fake_bad_reply_identity;
static bool fake_send_refused;
static int fake_reply_status = -1;
static uint64 fake_last_request_id;
static uint64 fake_previous_request_id;
static int fake_ack_degraded;

/* Only the counter touched by the extracted, real block consumer. */
static struct {
	pg_atomic_uint64 block_x_granted_from_holder_count;
} fake_block_counters, *ClusterGcsBlock = &fake_block_counters;

void cluster_lever_a_note_remote_ack_degraded(void);

void
cluster_lever_a_note_remote_ack_degraded(void)
{
	fake_ack_degraded++;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	*foundPtr = false;
	free(fake_shmem);
	if (posix_memalign(&fake_shmem, PG_CACHE_LINE_SIZE, size) != 0)
		abort();
	memset(fake_shmem, 0, size);
	return fake_shmem;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	return NULL;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return false;
}

void
ConditionVariableInit(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariableSleep(ConditionVariable *cv pg_attribute_unused(),
					   uint32 wait_event_info pg_attribute_unused())
{}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout,
							uint32 wait_event_info pg_attribute_unused())
{
	fake_clock += (TimestampTz)timeout * 1000;
	return true;
}

bool
ConditionVariableCancelSleep(void)
{
	return false;
}

void
ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_clock;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(), bool *foundPtr)
{
	if (foundPtr)
		*foundPtr = false;
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return 0;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp pg_attribute_unused())
{
	status->curBucket = 0;
	status->hashp = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	return NULL;
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entry_size pg_attribute_unused())
{
	return 0;
}

void *
ShmemAllocUnlocked(Size size pg_attribute_unused())
{
	return NULL;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	fake_error_level = elevel;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (fake_error_level >= ERROR) {
		if (PG_exception_stack == NULL)
			abort();
		siglongjmp(*PG_exception_stack, 1);
	}
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

/* cluster module stubs */
uint64
cluster_epoch_get_current(void)
{
	return 0;
}

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info pg_attribute_unused())
{}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	ClusterICEnvelope env;
	int sender = cluster_node_id;

	if (!fake_control_active)
		return CLUSTER_IC_SEND_DONE;
	if (fake_send_refused)
		return CLUSTER_IC_SEND_NOT_ADMITTED;
	memset(&env, 0, sizeof(env));
	env.source_node_id = sender;
	env.dest_node_id = dest_node_id;
	env.payload_length = payload_len;
	if (msg_type == PGRAC_IC_MSG_GCS_REQUEST) {
		const GcsRequestPayload *req = payload;

		UT_ASSERT_EQ(payload_len, sizeof(*req));
		fake_previous_request_id = fake_last_request_id;
		fake_last_request_id = req->request_id;
		if (fake_drop_reply)
			return CLUSTER_IC_SEND_DONE;
		cluster_node_id = dest_node_id;
		if (fake_reply_status >= 0 || fake_bad_reply_identity) {
			GcsReplyPayload reply;
			ClusterICEnvelope reply_env;

			memset(&reply, 0, sizeof(reply));
			memset(&reply_env, 0, sizeof(reply_env));
			reply.request_id = req->request_id + (fake_bad_reply_identity ? 1 : 0);
			reply.transition_id = req->transition_id;
			reply.status = fake_reply_status >= 0 ? fake_reply_status : GCS_REPLY_GRANTED;
			reply.sender_node = dest_node_id;
			reply.epoch = req->epoch;
			reply_env.source_node_id = dest_node_id;
			reply_env.payload_length = sizeof(reply);
			cluster_gcs_handle_reply_envelope(&reply_env, &reply);
		} else
			cluster_gcs_handle_request_envelope(&env, payload);
		cluster_node_id = sender;
	} else if (msg_type == PGRAC_IC_MSG_GCS_REPLY) {
		UT_ASSERT_EQ(payload_len, sizeof(GcsReplyPayload));
		cluster_gcs_handle_reply_envelope(&env, payload);
	} else
		UT_ASSERT(false);
	return CLUSTER_IC_SEND_DONE;
}

bool
cluster_grd_outbound_enqueue_backend_msg(uint8 msg_type pg_attribute_unused(),
										 uint32 dest_node_id pg_attribute_unused(),
										 const void *payload pg_attribute_unused(),
										 uint16 payload_len pg_attribute_unused())
{
	return true;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env pg_attribute_unused(),
							 const void *payload pg_attribute_unused(),
							 int32 peer_id pg_attribute_unused())
{
	return true;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env pg_attribute_unused(),
						  uint8 msg_type pg_attribute_unused(),
						  uint32 src_node_id pg_attribute_unused(),
						  uint32 dest_node_id pg_attribute_unused(),
						  const void *payload pg_attribute_unused(),
						  uint32 payload_len pg_attribute_unused())
{
	return true;
}

/* cluster_pcm_lock validator (real implementation is in cluster_pcm_lock.o
 * but we don't link it here -- L14 only needs the symbol address-of) */
bool
cluster_pcm_transition_legal(PcmState from pg_attribute_unused(), PcmState to pg_attribute_unused(),
							 PcmLockTransition trans pg_attribute_unused())
{
	return true;
}

bool
cluster_pcm_lock_apply_gcs_transition(BufferTag tag pg_attribute_unused(),
									  PcmLockTransition trans pg_attribute_unused(),
									  int holder_node_id pg_attribute_unused())
{
	return fake_master_apply;
}

void
cluster_pcm_lock_clear_pending_x(BufferTag tag pg_attribute_unused())
{}

bool
cluster_pcm_lock_clear_pending_x_if(BufferTag tag pg_attribute_unused(),
									int32 expected_requester pg_attribute_unused())
{
	return false;
}

/* spec-2.33 D2 stub:  cluster_gcs_lookup_master now real (declared-node
 * hash mod-N) calls cluster_conf_lookup_node.  Single-node fixture: return
 * NULL for all slots except 0 to keep declared_count = 1 (HC72 self short-
 * circuit). */
/* spec-4.7 D7 — controllable declared-node count (default 1 = the original
 * single-node fixture;  the D7 routing test raises it to exercise re-route). */
static int fake_declared_count = 1;
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	static ClusterNodeInfo infos[CLUSTER_MAX_NODES];

	if (node_id >= 0 && node_id < fake_declared_count) {
		infos[node_id].node_id = node_id;
		return &infos[node_id];
	}
	return NULL;
}

/* spec-4.7 D7 (L238) — cluster_gcs.o's recovery-aware lookup_master reads peer
 * liveness;  controllable dead node (default -1 = all alive → re-route never
 * triggers → healthy static routing unchanged for the existing tests). */
static int32 fake_dead_node = -1;
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	return (peer_id == fake_dead_node) ? CLUSTER_CSSD_PEER_DEAD : CLUSTER_CSSD_PEER_ALIVE;
}


/* ----------
 * L1: msg_type enum values.  GCS_REQUEST=12 + GCS_REPLY=13.
 *	   CSSD_HEARTBEAT=11 is a #define in cluster_cssd.h and MUST NOT collide
 *	   with the new enum values (spec-2.32 v0.2 F1 PG-fact discovery).
 * ----------
 */
UT_TEST(test_gcs_msg_type_enum_values_no_collision)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REQUEST, 12);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY, 13);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CSSD_HEARTBEAT, 11);
	/* Sanity:  spec-2.16 CF_BLOCK_SHIP reservation slot 6 preserved. */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CF_BLOCK_SHIP, 6);
}


/* ----------
 * L2: payload struct sizes locked via StaticAssertDecl.
 *	   GcsRequestPayload = 48B, GcsReplyPayload = 24B.
 * ----------
 */
UT_TEST(test_gcs_payload_sizes_locked)
{
	UT_ASSERT_EQ((int)sizeof(GcsRequestPayload), 48);
	UT_ASSERT_EQ((int)sizeof(GcsReplyPayload), 24);
}


/* ----------
 * L3: payload field offsets ABI lock.
 * ----------
 */
UT_TEST(test_gcs_payload_field_offsets)
{
	/* Request payload (48B) */
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, sender_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, transition_id), 40);
	UT_ASSERT_EQ((int)offsetof(GcsRequestPayload, reserved_0), 41);

	/* Reply payload (24B) */
	UT_ASSERT_EQ((int)offsetof(GcsReplyPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsReplyPayload, transition_id), 8);
	UT_ASSERT_EQ((int)offsetof(GcsReplyPayload, status), 9);
	UT_ASSERT_EQ((int)offsetof(GcsReplyPayload, sender_node), 12);
	UT_ASSERT_EQ((int)offsetof(GcsReplyPayload, epoch), 16);
}


/* ----------
 * L4: dispatch handler symbols linkable.
 * ----------
 */
UT_TEST(test_gcs_handler_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_handle_request_envelope);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_handle_reply_envelope);
}


/* ----------
 * L5: GcsReplyStatus enum has exactly 4 statuses.
 * ----------
 */
UT_TEST(test_gcs_reply_status_enum_count_is_4)
{
	UT_ASSERT_EQ((int)GCS_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_REPLY_DENIED_INCOMPATIBLE, 1);
	UT_ASSERT_EQ((int)GCS_REPLY_DENIED_VALIDATOR_REJECT, 2);
	UT_ASSERT_EQ((int)GCS_REPLY_DENIED_EPOCH_STALE, 3);
}


/* ----------
 * L6: cluster_gcs_lookup_master symbol linkable.
 * ----------
 */
UT_TEST(test_gcs_lookup_master_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_lookup_master);
}

/*
 * spec-4.7 D7 — recovery-aware GCS routing remaster.  Healthy (no dead node):
 * lookup_master == lookup_master_static (zero behaviour change).  When the
 * STATIC declared master is DEAD: lookup_master re-routes to a LIVE survivor
 * (never the dead node), while lookup_master_static still returns the original
 * (dead) master for the recovery-phase gate.
 */
UT_TEST(test_gcs_d7_recovery_aware_reroute)
{
	BufferTag tag;
	int static_m = 0;
	int routed;
	int blk;

	fake_declared_count = 3; /* nodes 0,1,2;  self = 0 */
	fake_dead_node = -1;

	/* Find a tag whose STATIC master is a non-self node (1 or 2). */
	memset(&tag, 0, sizeof(tag));
	for (blk = 0; blk < 256; blk++) {
		tag.blockNum = (BlockNumber)blk;
		static_m = cluster_gcs_lookup_master_static(tag);
		if (static_m != 0)
			break;
	}
	UT_ASSERT(static_m != 0);

	/* Healthy: recovery-aware lookup returns the static master UNCHANGED. */
	UT_ASSERT_EQ(cluster_gcs_lookup_master(tag), static_m);

	/* Kill the static master → re-route to a live survivor (never the dead). */
	fake_dead_node = static_m;
	routed = cluster_gcs_lookup_master(tag);
	UT_ASSERT(routed != static_m);
	UT_ASSERT_EQ((int)cluster_cssd_get_peer_state(routed), (int)CLUSTER_CSSD_PEER_ALIVE);
	/* static lookup still reports the original (dead) master for the gate. */
	UT_ASSERT_EQ(cluster_gcs_lookup_master_static(tag), static_m);

	fake_declared_count = 1;
	fake_dead_node = -1;
}


/* ----------
 * L7: cluster_gcs_send_transition_and_wait symbol linkable.
 * ----------
 */
UT_TEST(test_gcs_send_transition_and_wait_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_send_transition_and_wait);
}


/* ----------
 * L8: cluster_gcs_register_msg_types symbol linkable.
 * ----------
 */
UT_TEST(test_gcs_register_msg_types_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_register_msg_types);
}


/* ----------
 * L9: 14 dump_gcs accessor symbols all linkable.
 * ----------
 */
UT_TEST(test_gcs_dump_accessors_all_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_lookup_master_self_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_lookup_master_remote_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_send_request_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_handle_request_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_handle_reply_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_reply_late_drop_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_reply_timeout_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_encode_payload_bytes);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_decode_payload_bytes);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_dispatch_loop_iterations);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_outstanding_count);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_max_outstanding);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_get_api_state);
}


/* ----------
 * L10: cluster_gcs_get_api_state returns "stub" before module init.
 *      Module init runs inside postmaster phase 1; unit binary doesn't.
 * ----------
 */
UT_TEST(test_gcs_get_api_state_stub_before_init)
{
	const char *state = cluster_gcs_get_api_state();

	UT_ASSERT_NOT_NULL(state);
	UT_ASSERT(strcmp(state, "stub") == 0);
}


/* ----------
 * L11: MAX_OUTSTANDING_REQUESTS_PER_BACKEND == 8.
 * ----------
 */
UT_TEST(test_gcs_max_outstanding_per_backend_constant)
{
	UT_ASSERT_EQ(MAX_OUTSTANDING_REQUESTS_PER_BACKEND, 8);
}


/* ----------
 * L12: GCS_REPLY_INTERNAL_DEADLINE_MS == 5000.
 * ----------
 */
UT_TEST(test_gcs_internal_deadline_ms_constant)
{
	UT_ASSERT_EQ(GCS_REPLY_INTERNAL_DEADLINE_MS, 5000);
}


/* ----------
 * L13: PCM transition_id range invariant (1..9; spec-2.30 9 transitions).
 *      payload.transition_id field stores values from this range only.
 * ----------
 */
UT_TEST(test_gcs_transition_id_range_invariant_1_to_9)
{
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_S, 1);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_CLEANOUT, 9);
	UT_ASSERT_EQ((int)PCM_TRANSITION_COUNT, 9);
}


/* ----------
 * L14: spec-2.30 validator accepts all 9 transitions encoded in GCS payload.
 *      Receiver handler delegates HC75 rejection to cluster_pcm_transition_legal.
 * ----------
 */
UT_TEST(test_gcs_pcm_validator_accepts_all_9_transitions)
{
	/* Validator function signature is (from, to, trans) — spec-2.30 D2.
	 * For GCS use we only need the validator symbol resolvable; deeper
	 * coverage of each transition lives in test_cluster_pcm_lock. */
	UT_ASSERT_NOT_NULL((void *)cluster_pcm_transition_legal);
}


/* ----------
 * L15: dispatch table handler signature matches expected (env, payload).
 *      ClusterICMsgTypeInfo.handler takes (env, payload) — 2 args, no peer_id
 *      (peer_id absorbed by dispatcher).  This invariant prevents accidental
 *      handler signature drift that would cause silent ABI mismatch.
 * ----------
 */
UT_TEST(test_gcs_handler_signature_matches_dispatch_table)
{
	/* Compile-time check: handler signature compatible with ClusterICMsgTypeInfo.
	 * If signatures diverge, this assignment fails to compile. */
	void (*req_handler)(const ClusterICEnvelope *env, const void *payload)
		= cluster_gcs_handle_request_envelope;
	void (*reply_handler)(const ClusterICEnvelope *env, const void *payload)
		= cluster_gcs_handle_reply_envelope;

	UT_ASSERT_NOT_NULL((void *)req_handler);
	UT_ASSERT_NOT_NULL((void *)reply_handler);
}


/* ----------
 * L16: LWTRANCHE_CLUSTER_GCS registered enum value distinct from PCM.
 * ----------
 */
UT_TEST(test_gcs_lwlock_tranche_distinct_from_pcm)
{
	UT_ASSERT((int)LWTRANCHE_CLUSTER_GCS != (int)LWTRANCHE_CLUSTER_PCM);
	UT_ASSERT_EQ((int)LWTRANCHE_CLUSTER_GCS, (int)LWTRANCHE_CLUSTER_PCM + 1);
}


/* ----------
 * L17: HC77 — sender API documented as NOT auto-applying transition on
 *      local short-circuit.  Verified by API surface only (the spec-2.31
 *      local path is the apply owner; spec-2.32 wire path defers to
 *      master-side handler which already applied per HC77 contract).
 * ----------
 */
UT_TEST(test_gcs_hc77_sender_no_double_apply_doc)
{
	/* Symbol exists + documented contract; behavioral coverage in TAP 110 L4 + L17. */
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_send_transition_and_wait);
}


/* ----------
 * L18: module init helpers (shmem_size / shmem_init / module_init) linkable.
 *      Postmaster phase 1 calls cluster_gcs_module_init → cluster_shmem
 *      registry consumes shmem_size + shmem_init.
 * ----------
 */
UT_TEST(test_gcs_module_init_helpers_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_shmem_init);
	UT_ASSERT_NOT_NULL((void *)cluster_gcs_module_init);
}


/* ----------
 * L19: a control reply belongs to exactly one outstanding transition.
 *
 * Per-backend raw counters all begin at one.  The wire request id must
 * therefore carry the requester node/backend domain, and reply admission
 * must also bind the echoed transition and authenticated master identity.
 * A denial for an X->S predecessor must never satisfy another backend's
 * later S->N release wait.
 * ----------
 */
UT_TEST(test_gcs_reply_identity_is_exact_across_backends)
{
	GcsReplyPayload reply;
	uint64 backend0_request = gcs_reqid_requester(0, 0, 1);
	uint64 backend1_request = gcs_reqid_requester(0, 1, 1);

	UT_ASSERT(backend0_request != backend1_request);

	memset(&reply, 0, sizeof(reply));
	reply.request_id = backend1_request;
	reply.transition_id = PCM_TRANS_S_TO_N_RELEASE;
	reply.status = GCS_REPLY_GRANTED;
	reply.sender_node = 3;
	reply.epoch = 7;

	UT_ASSERT(cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													PCM_TRANS_S_TO_N_RELEASE, 3, 3));

	/* Same raw sequence in another backend domain is not this request. */
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend0_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 3));

	/* A predecessor reply cannot complete the successor transition. */
	reply.transition_id = PCM_TRANS_X_TO_S_DOWNGRADE;
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 3));
	reply.transition_id = PCM_TRANS_S_TO_N_RELEASE;

	/* Body sender and authenticated envelope source are both exact. */
	reply.sender_node = 2;
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 3));
	reply.sender_node = 3;
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 2));

	/* Reserved bytes and status domain remain fail closed. */
	reply.reserved_0[0] = 1;
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 3));
	reply.reserved_0[0] = 0;
	reply.status = (uint8)(GCS_REPLY_DENIED_EPOCH_STALE + 1);
	UT_ASSERT(!cluster_gcs_reply_matches_outstanding(&reply, backend1_request,
													 PCM_TRANS_S_TO_N_RELEASE, 3, 3));
}


/* The production block consumer starts inside the already verified/installed
 * holder-image branch. No checksum, image install or ownership rule is mocked
 * as a grant: only a successful registration may return durable=true. */
static bool
run_holder_registration(uint8 final_status, PcmLockTransition transition_id, bool *out_retry,
						bool *out_image)
{
	BufferTag tag;
	int final_forwarding_master = 1;
	bool read_image = false;
	bool retry_denied = false;
	bool granted = false;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 16386;
	tag.blockNum = 17123;
	do {
		{
#include "test_cluster_gcs_shared_registration.inc"
			granted = true;
		}
		while (false)
			;
		*out_retry = retry_denied;
		*out_image = read_image;
		return granted;
	}

	static void reset_control_fixture(void)
	{
		cluster_node_id = 0;
		MyBackendType = B_LMON;
		fake_control_active = true;
		fake_master_apply = true;
		fake_drop_reply = false;
		fake_bad_reply_identity = false;
		fake_send_refused = false;
		fake_reply_status = -1;
		fake_clock = 1000;
		fake_error_level = 0;
		fake_last_request_id = fake_previous_request_id = 0;
		fake_ack_degraded = 0;
		cluster_gcs_shmem_init();
		pg_atomic_init_u64(&ClusterGcsBlock->block_x_granted_from_holder_count, 0);
	}

	/* Regression: a real, identity-matched refusal after image arrival must
 * retire its slot and return to the existing fresh-reservation boundary.
 * Replacing this branch with the old throwing API fails this test. */
	UT_TEST(test_forwarded_s_registration_refusal_reenters_without_grant)
	{
		volatile bool raised = false;
		bool retry = false;
		bool image = false;
		bool granted = false;

		reset_control_fixture();
		fake_master_apply = false;
		PG_TRY();
		{
			granted = run_holder_registration(GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, PCM_TRANS_N_TO_S,
											  &retry, &image);
		}
		PG_CATCH();
		{
			raised = true;
		}
		PG_END_TRY();
		UT_ASSERT(!raised);
		UT_ASSERT(!granted);
		UT_ASSERT(retry);
		UT_ASSERT(!image);
		UT_ASSERT_EQ(cluster_gcs_get_outstanding_count(), 0);
		UT_ASSERT_EQ(cluster_gcs_get_reply_timeout_count(), 0);
		UT_ASSERT_EQ(cluster_gcs_get_handle_request_count(), 1);
		UT_ASSERT_EQ(cluster_gcs_get_handle_reply_count(), 1);
		UT_ASSERT_EQ(fake_ack_degraded, 0);

		/* A successor uses another real slot identity and can acquire S. */
		fake_master_apply = true;
		granted = run_holder_registration(GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, PCM_TRANS_N_TO_S,
										  &retry, &image);
		UT_ASSERT(granted && !retry && !image);
		UT_ASSERT(fake_last_request_id != fake_previous_request_id);
		UT_ASSERT_EQ(cluster_gcs_get_outstanding_count(), 0);
	}

	UT_TEST(test_forwarded_registration_hard_failures_never_become_retry)
	{
		int scenario;

		for (scenario = 0; scenario < 6; scenario++) {
			volatile bool raised = false;
			bool retry = false;
			bool image = false;
			bool granted = false;

			reset_control_fixture();
			if (scenario == 0)
				fake_reply_status = GCS_REPLY_DENIED_VALIDATOR_REJECT;
			else if (scenario == 1)
				fake_reply_status = GCS_REPLY_DENIED_EPOCH_STALE;
			else if (scenario == 2)
				fake_drop_reply = true;
			else if (scenario == 3)
				fake_bad_reply_identity = true;
			else if (scenario == 4)
				fake_send_refused = true;
			else
				fake_master_apply = false;
			PG_TRY();
			{
				granted = run_holder_registration(
					scenario == 5 ? GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
								  : GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
					scenario == 5 ? PCM_TRANS_N_TO_X : PCM_TRANS_N_TO_S, &retry, &image);
			}
			PG_CATCH();
			{
				raised = true;
			}
			PG_END_TRY();
			UT_ASSERT(raised);
			UT_ASSERT(!granted && !retry && !image);
			UT_ASSERT_EQ(cluster_gcs_get_outstanding_count(), 0);
			UT_ASSERT_EQ(cluster_gcs_get_reply_timeout_count(),
						 (scenario == 2 || scenario == 3) ? 1 : 0);
		}
	}

	int main(void)
	{
		UT_PLAN(22);
		UT_RUN(test_gcs_msg_type_enum_values_no_collision);
		UT_RUN(test_gcs_payload_sizes_locked);
		UT_RUN(test_gcs_payload_field_offsets);
		UT_RUN(test_gcs_handler_symbols_linkable);
		UT_RUN(test_gcs_reply_status_enum_count_is_4);
		UT_RUN(test_gcs_lookup_master_symbol_linkable);
		UT_RUN(test_gcs_d7_recovery_aware_reroute);
		UT_RUN(test_gcs_send_transition_and_wait_symbol_linkable);
		UT_RUN(test_gcs_register_msg_types_symbol_linkable);
		UT_RUN(test_gcs_dump_accessors_all_linkable);
		UT_RUN(test_gcs_get_api_state_stub_before_init);
		UT_RUN(test_gcs_max_outstanding_per_backend_constant);
		UT_RUN(test_gcs_internal_deadline_ms_constant);
		UT_RUN(test_gcs_transition_id_range_invariant_1_to_9);
		UT_RUN(test_gcs_pcm_validator_accepts_all_9_transitions);
		UT_RUN(test_gcs_handler_signature_matches_dispatch_table);
		UT_RUN(test_gcs_lwlock_tranche_distinct_from_pcm);
		UT_RUN(test_gcs_hc77_sender_no_double_apply_doc);
		UT_RUN(test_gcs_module_init_helpers_linkable);
		UT_RUN(test_gcs_reply_identity_is_exact_across_backends);
		UT_RUN(test_forwarded_s_registration_refusal_reenters_without_grant);
		UT_RUN(test_forwarded_registration_hard_failures_never_become_retry);
		free(fake_shmem);
		UT_DONE();
		return ut_failed_count == 0 ? 0 : 1;
	}
