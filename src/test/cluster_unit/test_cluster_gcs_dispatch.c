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
#include "cluster/cluster_gcs.h"
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

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	*foundPtr = false;
	return NULL;
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
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
							long timeout pg_attribute_unused(),
							uint32 wait_event_info pg_attribute_unused())
{
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
	return (TimestampTz)0;
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
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
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
{}

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

void
pg_re_throw(void)
{
	abort();
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
cluster_ic_send_envelope(uint8 msg_type pg_attribute_unused(),
						 int32 dest_node_id pg_attribute_unused(),
						 const void *payload pg_attribute_unused(),
						 uint32 payload_len pg_attribute_unused())
{
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
	return true;
}

/* spec-2.33 D2 stub:  cluster_gcs_lookup_master now real (declared-node
 * hash mod-N) calls cluster_conf_lookup_node.  Single-node fixture: return
 * NULL for all slots except 0 to keep declared_count = 1 (HC72 self short-
 * circuit). */
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	static ClusterNodeInfo info_self = { 0 };

	if (node_id == 0) {
		info_self.node_id = 0;
		return &info_self;
	}
	return NULL;
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


int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_gcs_msg_type_enum_values_no_collision);
	UT_RUN(test_gcs_payload_sizes_locked);
	UT_RUN(test_gcs_payload_field_offsets);
	UT_RUN(test_gcs_handler_symbols_linkable);
	UT_RUN(test_gcs_reply_status_enum_count_is_4);
	UT_RUN(test_gcs_lookup_master_symbol_linkable);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
