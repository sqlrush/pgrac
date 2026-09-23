/*-------------------------------------------------------------------------
 *
 * test_cluster_lms_outbound.c
 *	  Reliable-handoff contract of the DATA-plane outbound ring drain
 *	  (GCS serve-stall round-5).
 *
 *	  cluster_lms_outbound_drain_send() dequeues staged frames and hands
 *	  them to cluster_ic_send_envelope.  The send result is a four-state
 *	  ownership contract, and the drain must honor it exactly:
 *
 *	    DONE         frame is on the wire            -> ring slot consumed
 *	    WOULD_BLOCK  transport ADMITTED the frame
 *	                 (owns a private copy; drains on
 *	                 WL_SOCKET_WRITEABLE)            -> ring slot consumed;
 *	                                                    NEVER resubmit
 *	    NOT_ADMITTED transport refused the frame
 *	                 (peer mid-HELLO / FIFO full)    -> retain in ring, in
 *	                                                    per-peer order
 *	    HARD_ERROR   peer dead                       -> drop; requesters
 *	                                                    self-heal by retry
 *
 *	  Pre-fix drain treated WOULD_BLOCK as "not sent": it head-requeued
 *	  the whole frame and broke the batch.  Both halves were defects:
 *
 *	    U1  the requeued frame was ALSO admitted by tier1 (partial write
 *	        or tail queue), so the next drain put a duplicate frame on
 *	        the per-peer byte stream;
 *	    U2  the batch break parked every frame behind the blocked peer,
 *	        so one backpressured peer head-of-line blocked all others
 *	        sharing the worker ring.
 *
 *	  This binary links cluster_lms_outbound.o standalone and mocks
 *	  cluster_ic_send_envelope with a scripted per-peer result + a call
 *	  log, so the ownership contract is pinned deterministically.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lms_outbound.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope prototype */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* Desired R10 C-intent boundary.  The standalone ring test supplies the
 * semantic-owner callbacks below and exercises the real ring/drain object. */
extern bool cluster_lms_outbound_enqueue_resource_x_intent(int worker_id,
														   const ResourceXIntentSlot *intent,
														   uint32 connection_generation,
														   uint64 deadline_us);
extern int cluster_lms_outbound_resource_x_intent_pump(void);
extern ClusterPcmOwnResult cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
	int worker_id, uint32 dest_node_id, const void *payload, uint16 payload_len,
	const ClusterPcmOwnSnapshot *expected_revoking, ClusterLmsRemoteSStatusHandle *handle_out);
extern ClusterPcmOwnResult cluster_lms_outbound_publish_resource_x_remote_s_status_exact(
	const ClusterLmsRemoteSStatusHandle *handle, const ClusterPcmOwnSnapshot *released_n);
extern ClusterPcmOwnResult cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(
	const ClusterLmsRemoteSStatusHandle *handle);
extern bool
cluster_lms_outbound_resource_x_transport_snapshot(ClusterLmsResourceXTransportSnapshot *out);

/* ============================================================
 * PG-runtime stubs.
 * ============================================================ */

ProcessingMode Mode = NormalProcessing;
BackendType MyBackendType = B_LMS;
int cluster_node_id = 0;
int cluster_lms_workers = 2;
int cluster_gcs_reply_timeout_ms = 5000;
static uint32 ut_peer_capabilities[CLUSTER_MAX_NODES];
static uint32 ut_peer_cap_generation[CLUSTER_MAX_NODES];
static ResourceXIntentSlot ut_resource_x_owner_slot;
static uint8 ut_resource_x_owner_payload[RESOURCE_X_IMAGE_V1_BYTES];
static int ut_resource_x_stage_count = 0;
static int ut_resource_x_rearm_count = 0;
static int ut_resource_x_complete_count = 0;
static ResourceXIntentProbeResult ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_IDLE;
static int ut_resource_x_probe_call_count = 0;
static int ut_resource_x_delivery_tick_count = 0;
static int ut_resource_x_source_finish_tick_count = 0;
static uint32 ut_resource_x_probe_max_budget = 0;
static int ut_resource_x_rebind_count = 0;
static uint32 ut_resource_x_rebind_generation = 0;
static int ut_resource_x_decode_count = 0;
static uint32 ut_resource_x_decode_sender_generation = 0;

void
cluster_pcm_lock_resource_x_trace_wire(uint8 type, int32 peer, const void *payload, uint32 length,
									   int32 result)
{
	(void)type;
	(void)peer;
	(void)payload;
	(void)length;
	(void)result;
}

bool
cluster_resource_x_wire_decode(uint8 msg_type, const void *payload, uint16 payload_len,
							   ResourceXDecodedFrame *out, ResourceXWireReject *reject)
{
	if (msg_type != RESOURCE_X_MSG_IMAGE_OR_GRANT || payload == NULL
		|| payload_len != RESOURCE_X_IMAGE_V1_BYTES || out == NULL
		|| ut_resource_x_decode_sender_generation == 0)
		return false;
	memset(out, 0, sizeof(*out));
	out->kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	out->common.sender_connection_generation = ut_resource_x_decode_sender_generation;
	ut_resource_x_decode_count++;
	if (reject != NULL)
		*reject = RESOURCE_X_WIRE_REJECT_NONE;
	return true;
}

bool
cluster_resource_x_wire_rebind_sender_generation(uint8 msg_type pg_attribute_unused(),
												 void *payload, uint16 payload_len,
												 uint32 sender_connection_generation,
												 ResourceXWireReject *reject)
{
	if (payload == NULL || payload_len < RESOURCE_X_CONTROL_V1_BYTES
		|| sender_connection_generation == 0)
		return false;
	ut_resource_x_rebind_count++;
	ut_resource_x_rebind_generation = sender_connection_generation;
	if (reject != NULL)
		*reject = RESOURCE_X_WIRE_REJECT_NONE;
	return true;
}

static bool
ut_resource_x_intent_identity_equal(const ResourceXIntentSlot *left,
									const ResourceXIntentSlot *right)
{
	return left != NULL && right != NULL && left->logical_generation == right->logical_generation
		   && left->authority_generation == right->authority_generation
		   && left->first_armed_us == right->first_armed_us
		   && left->destination_node == right->destination_node
		   && left->payload_bytes == right->payload_bytes && left->kind == right->kind
		   && memcmp(&left->body, &right->body, sizeof(left->body)) == 0;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(const ResourceXAssertion *assertion,
														ResourceXIntentSlot *slot_out,
														void *payload_out, uint16 payload_capacity)
{
	if (assertion == NULL || slot_out == NULL || payload_out == NULL
		|| payload_capacity < ut_resource_x_owner_slot.payload_bytes
		|| ut_resource_x_owner_slot.state == RESOURCE_X_INTENT_SLOT_EMPTY)
		return RESOURCE_X_APPLY_NOT_FOUND;
	if (memcmp(assertion, &ut_resource_x_owner_slot.body.assertion, sizeof(*assertion)) != 0)
		return RESOURCE_X_APPLY_STALE;
	*slot_out = ut_resource_x_owner_slot;
	memcpy(payload_out, ut_resource_x_owner_payload, ut_resource_x_owner_slot.payload_bytes);
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_stage_exact(const ResourceXIntentSlot *expected,
													 uint64 now_us)
{
	if (now_us == 0 || !ut_resource_x_intent_identity_equal(expected, &ut_resource_x_owner_slot)
		|| ut_resource_x_owner_slot.state != RESOURCE_X_INTENT_SLOT_ARMED)
		return RESOURCE_X_INTENT_STALE;
	ut_resource_x_owner_slot.state = RESOURCE_X_INTENT_SLOT_STAGED;
	ut_resource_x_owner_slot.last_attempt_us = now_us;
	ut_resource_x_stage_count++;
	return RESOURCE_X_INTENT_STAGED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(const ResourceXIntentSlot *expected,
															uint64 now_us)
{
	if (now_us == 0 || !ut_resource_x_intent_identity_equal(expected, &ut_resource_x_owner_slot)
		|| ut_resource_x_owner_slot.state != RESOURCE_X_INTENT_SLOT_ARMED)
		return RESOURCE_X_INTENT_STALE;
	ut_resource_x_owner_slot.last_attempt_us = now_us;
	return RESOURCE_X_INTENT_NOT_ADMITTED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(const ResourceXIntentSlot *expected,
														  uint64 now_us)
{
	if (now_us == 0 || !ut_resource_x_intent_identity_equal(expected, &ut_resource_x_owner_slot)
		|| ut_resource_x_owner_slot.state != RESOURCE_X_INTENT_SLOT_STAGED)
		return RESOURCE_X_INTENT_STALE;
	ut_resource_x_owner_slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
	ut_resource_x_owner_slot.last_attempt_us = now_us;
	ut_resource_x_rearm_count++;
	return RESOURCE_X_INTENT_HARD_REARMED;
}

bool
cluster_pcm_lock_resource_x_grant_intent_complete_exact(const ResourceXIntentSlot *expected)
{
	if (!ut_resource_x_intent_identity_equal(expected, &ut_resource_x_owner_slot)
		|| ut_resource_x_owner_slot.state != RESOURCE_X_INTENT_SLOT_STAGED)
		return false;
	memset(&ut_resource_x_owner_slot, 0, sizeof(ut_resource_x_owner_slot));
	ut_resource_x_complete_count++;
	return true;
}

ResourceXIntentProbeResult
cluster_pcm_lock_resource_x_outbound_intent_probe_exact(uint32 probe_budget,
														ResourceXIntentSlot *slot_out,
														void *payload_out, uint16 payload_capacity,
														uint32 *examined_out)
{
	ut_resource_x_probe_call_count++;
	if (probe_budget > ut_resource_x_probe_max_budget)
		ut_resource_x_probe_max_budget = probe_budget;
	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (examined_out != NULL)
		*examined_out = 0;
	if (slot_out == NULL || payload_out == NULL || examined_out == NULL
		|| payload_capacity < ut_resource_x_owner_slot.payload_bytes)
		return RESOURCE_X_INTENT_PROBE_CORRUPT;
	if (ut_resource_x_probe_mode == RESOURCE_X_INTENT_PROBE_FOUND) {
		*slot_out = ut_resource_x_owner_slot;
		memcpy(payload_out, ut_resource_x_owner_payload, ut_resource_x_owner_slot.payload_bytes);
		*examined_out = 1;
		ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_COMPLETE;
		return RESOURCE_X_INTENT_PROBE_FOUND;
	}
	return ut_resource_x_probe_mode;
}

ResourceXApplyResult
cluster_gcs_block_resource_x_delivery_tick(const ResourceXAcquisitionRef *ref)
{
	UT_ASSERT_EQ(ref->formation, UINT64_C(17));
	UT_ASSERT_EQ(ref->acquisition_generation, UINT64_C(41));
	ut_resource_x_delivery_tick_count++;
	return RESOURCE_X_APPLY_BAD_STATE; /* BUSY does not self-wake or fake send. */
}

ResourceXIntentProbeResult
cluster_pcm_lock_resource_x_outbound_work_probe_exact(uint32 probe_budget,
													  ResourceXIntentSlot *slot_out,
													  void *payload_out, uint16 payload_capacity,
													  uint32 *examined_out,
													  ResourceXAcquisitionRef *delivery_out)
{
	ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
		probe_budget, slot_out, payload_out, payload_capacity, examined_out);

	memset(delivery_out, 0, sizeof(*delivery_out));
	if (result == RESOURCE_X_INTENT_PROBE_DELIVERY
		|| result == RESOURCE_X_INTENT_PROBE_SOURCE_FINISH) {
		delivery_out->formation = 17;
		delivery_out->acquisition_generation = 41;
		*examined_out = 1;
		ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_COMPLETE;
	}
	return result;
}

ResourceXApplyResult
cluster_gcs_block_resource_x_source_finish_tick(const ResourceXAcquisitionRef *ref)
{
	UT_ASSERT_EQ(ref->formation, UINT64_C(17));
	UT_ASSERT_EQ(ref->acquisition_generation, UINT64_C(41));
	ut_resource_x_source_finish_tick_count++;
	return RESOURCE_X_APPLY_BAD_STATE;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(const ResourceXIntentSlot *expected,
														   ResourceXIntentSlot *slot_out,
														   void *payload_out,
														   uint16 payload_capacity)
{
	if (expected == NULL
		|| !ut_resource_x_intent_identity_equal(expected, &ut_resource_x_owner_slot))
		return RESOURCE_X_APPLY_STALE;
	return cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&expected->body.assertion, slot_out, payload_out, payload_capacity);
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_stage_exact(const ResourceXIntentSlot *expected,
														uint64 now_us)
{
	return cluster_pcm_lock_resource_x_grant_intent_stage_exact(expected, now_us);
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(const ResourceXIntentSlot *expected,
															   uint64 now_us)
{
	return cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(expected, now_us);
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(const ResourceXIntentSlot *expected,
															 uint64 now_us)
{
	return cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(expected, now_us);
}

bool
cluster_pcm_lock_resource_x_outbound_intent_complete_exact(const ResourceXIntentSlot *expected)
{
	return cluster_pcm_lock_resource_x_grant_intent_complete_exact(expected);
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
											  uint32 expected_generation)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || required_capabilities == 0)
		return false;
	return (ut_peer_capabilities[peer_id] & required_capabilities) == required_capabilities
		   && ut_peer_cap_generation[peer_id] == expected_generation;
}

bool
cluster_sf_peer_capability_word_sample(int32 peer_id, uint32 required_capabilities,
									   uint32 *capability_word_out, uint32 *generation_out)
{
	if (capability_word_out != NULL)
		*capability_word_out = 0;
	if (generation_out != NULL)
		*generation_out = 0;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || required_capabilities == 0
		|| (ut_peer_capabilities[peer_id] & required_capabilities) != required_capabilities)
		return false;
	if (capability_word_out != NULL)
		*capability_word_out = ut_peer_capabilities[peer_id];
	if (generation_out != NULL)
		*generation_out = ut_peer_cap_generation[peer_id];
	return true;
}

int
cluster_lms_shard_for_tag(const BufferTag *tag, int n_workers)
{
	UT_ASSERT(tag != NULL);
	UT_ASSERT(n_workers > 0 && n_workers <= CLUSTER_LMS_MAX_WORKERS);
	return (int)(tag->blockNum % (BlockNumber)n_workers);
}

uint32
cluster_ic_local_capability_word(void)
{
	return UINT32_MAX;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	void *p = malloc(size);

	(void)name;
	UT_ASSERT(p != NULL);
	memset(p, 0, size);
	*foundPtr = false;
	return p;
}

static const ClusterShmemRegion *ut_captured_region = NULL;

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_captured_region = region;
}

/* Named-tranche plumbing: hand back a static lock array. */
static LWLockPadded ut_locks[CLUSTER_LMS_MAX_WORKERS];
static LWLock *ut_held_lock;
static LWLock *ut_lock_stack[CLUSTER_LMS_MAX_WORKERS];
static unsigned ut_lock_depth;
static int ut_lock_reads;
extern void cluster_lms_outbound_test_geometry(int worker, uint32 head, uint32 tail, uint32 count);

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
	(void)tranche_name;
	return ut_locks;
}

void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
	(void)tranche_name;
	(void)num_lwlocks;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (ut_lock_depth == CLUSTER_LMS_MAX_WORKERS
		|| (ut_held_lock != NULL && (uintptr_t)lock <= (uintptr_t)ut_held_lock))
		abort();
	ut_lock_stack[ut_lock_depth++] = lock;
	ut_held_lock = lock;
	if (mode == LW_SHARED)
		ut_lock_reads++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (ut_held_lock != lock)
		abort();
	ut_lock_depth--;
	ut_held_lock = ut_lock_depth == 0 ? NULL : ut_lock_stack[ut_lock_depth - 1];
}

/* LMS wakeup + GCS pre-send hook: count-only stubs. */
static int ut_wakeup_count = 0;

void
cluster_lms_wakeup(int worker_id)
{
	(void)worker_id;
	ut_wakeup_count++;
}

/* Drain honesty counters (shmem-backed in production): count-only stubs. */
static int ut_not_admitted_count = 0;
static int ut_requeue_drop_count = 0;
static int ut_cap_guard_drop_count = 0;

void
cluster_lms_obs_note_outbound_not_admitted(int worker_id)
{
	(void)worker_id;
	ut_not_admitted_count++;
}

void
cluster_lms_obs_note_outbound_requeue_drop(int worker_id)
{
	(void)worker_id;
	ut_requeue_drop_count++;
}

void
cluster_lms_obs_note_outbound_cap_guard_drop(int worker_id)
{
	(void)worker_id;
	ut_cap_guard_drop_count++;
}

void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc)
{
	(void)family;
	(void)rc;
}

static int ut_prepare_hook_count = 0;

void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req, int32 dest_node)
{
	(void)req;
	(void)dest_node;
	ut_prepare_hook_count++;
}

/* ============================================================
 * cluster_ic_send_envelope mock: scripted per-peer result + call log.
 * ============================================================ */

#define UT_PEER_X 3
#define UT_PEER_Y 5
#define UT_MSG_TYPE 42

typedef struct UtSentRec {
	uint8 msg_type;
	int32 dest;
	uint8 marker; /* first payload byte identifies the frame */
	uint32 payload_len;
	GcsBlockReplyHeader reply_header;
	bool reply_block_zero;
} UtSentRec;

/* Include a complete worker ring and the next frame admitted after draining. */
static UtSentRec ut_sent_log[1024];
static int ut_sent_n = 0;
static ClusterICSendResult ut_peer_rc[CLUSTER_MAX_NODES];
static int ut_local_dispatch_count = 0;
static uint8 ut_local_dispatch_marker = 0;
static int ut_direct_zero_reply_count = 0;
static GcsBlockReplyHeader ut_direct_zero_reply_header;
static int ut_checksum_call_count = 0;
static bool ut_r4_real_checksum = false;
static char ut_r4_reply_payload[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];

/* Production bodies, including their capability mask and CRC, extracted by
 * the Makefile. Only observation and the final transport are fixture seams. */
#include "test_cluster_r4_refusal_handoff.inc"

void
cluster_r4_observe_refusal(ClusterR4RefusalStage stage, ClusterCrBuildReason reason,
						   const BufferTag *tag, uint64 request_id, uint64 epoch, int32 requester,
						   int32 master, SCN read_scn)
{
	(void)stage;
	(void)reason;
	(void)tag;
	(void)request_id;
	(void)epoch;
	(void)requester;
	(void)master;
	(void)read_scn;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type, uint32 source_node_id,
						  uint32 dest_node_id, const void *payload, uint32 payload_length)
{
	memset(out_env, 0, sizeof(*out_env));
	out_env->msg_type = msg_type;
	out_env->source_node_id = source_node_id;
	out_env->dest_node_id = dest_node_id;
	out_env->payload_length = payload_length;
	(void)payload;
	return true;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	UT_ASSERT(env != NULL);
	UT_ASSERT_EQ((int32)env->source_node_id, cluster_node_id);
	UT_ASSERT_EQ((int32)env->dest_node_id, cluster_node_id);
	UT_ASSERT_EQ(peer_id, cluster_node_id);
	ut_local_dispatch_count++;
	ut_local_dispatch_marker = env->payload_length > 0 ? *(const uint8 *)payload : 0;
	return true;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	if (ut_sent_n < (int)lengthof(ut_sent_log)) {
		ut_sent_log[ut_sent_n].msg_type = msg_type;
		ut_sent_log[ut_sent_n].dest = dest_node_id;
		ut_sent_log[ut_sent_n].marker = payload_len > 0 ? *(const uint8 *)payload : 0;
		ut_sent_log[ut_sent_n].payload_len = payload_len;
		if (msg_type == PGRAC_IC_MSG_GCS_BLOCK_REPLY
			&& payload_len >= sizeof(GcsBlockReplyHeader)) {
			const uint8 *block_data = ((const uint8 *)payload) + sizeof(GcsBlockReplyHeader);
			uint32 i;

			memcpy(&ut_sent_log[ut_sent_n].reply_header, payload, sizeof(GcsBlockReplyHeader));
			if (payload_len == sizeof(ut_r4_reply_payload))
				memcpy(ut_r4_reply_payload, payload, payload_len);
			ut_sent_log[ut_sent_n].reply_block_zero
				= payload_len == GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE;
			for (i = 0; ut_sent_log[ut_sent_n].reply_block_zero && i < GCS_BLOCK_DATA_SIZE; i++)
				if (block_data[i] != 0)
					ut_sent_log[ut_sent_n].reply_block_zero = false;
		}
	}
	ut_sent_n++;
	UT_ASSERT(dest_node_id >= 0 && dest_node_id < CLUSTER_MAX_NODES);
	return ut_peer_rc[dest_node_id];
}

uint32
cluster_gcs_block_compute_checksum(const char *block_data)
{
	ut_checksum_call_count++;
	if (ut_r4_real_checksum)
		return gcs_block_compute_checksum(block_data);
	return UINT32_C(0xA55A7E11);
}

ClusterICSendResult
cluster_gcs_block_send_direct_zero_reply(int32 dest_node, const GcsBlockReplyHeader *header)
{
	ut_direct_zero_reply_count++;
	ut_direct_zero_reply_header = *header;
	return ut_peer_rc[dest_node];
}

static int
ut_count_marker(uint8 marker)
{
	int n = 0;
	int i;

	for (i = 0; i < ut_sent_n && i < (int)lengthof(ut_sent_log); i++)
		if (ut_sent_log[i].marker == marker)
			n++;
	return n;
}

static void
ut_reset_log(void)
{
	ut_sent_n = 0;
	ut_local_dispatch_count = 0;
	ut_local_dispatch_marker = 0;
	ut_direct_zero_reply_count = 0;
	ut_checksum_call_count = 0;
	ut_r4_real_checksum = false;
	memset(ut_r4_reply_payload, 0, sizeof(ut_r4_reply_payload));
	memset(&ut_direct_zero_reply_header, 0, sizeof(ut_direct_zero_reply_header));
	ut_cap_guard_drop_count = 0;
	memset(ut_peer_capabilities, 0, sizeof(ut_peer_capabilities));
	memset(ut_peer_cap_generation, 0, sizeof(ut_peer_cap_generation));
	memset(ut_sent_log, 0, sizeof(ut_sent_log));
	memset(&ut_resource_x_owner_slot, 0, sizeof(ut_resource_x_owner_slot));
	memset(ut_resource_x_owner_payload, 0, sizeof(ut_resource_x_owner_payload));
	ut_resource_x_stage_count = 0;
	ut_resource_x_rearm_count = 0;
	ut_resource_x_complete_count = 0;
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_IDLE;
	ut_resource_x_probe_call_count = 0;
	ut_resource_x_probe_max_budget = 0;
	ut_resource_x_rebind_count = 0;
	ut_resource_x_rebind_generation = 0;
	ut_resource_x_decode_count = 0;
	ut_resource_x_decode_sender_generation = 0;
}

static bool
ut_enqueue_typed_marker(int worker_id, uint8 msg_type, int32 dest, uint8 marker)
{
	return cluster_lms_outbound_enqueue(worker_id, msg_type, (uint32)dest, &marker, 1);
}

static bool
ut_enqueue_marker(int worker_id, int32 dest, uint8 marker)
{
	return ut_enqueue_typed_marker(worker_id, UT_MSG_TYPE, dest, marker);
}

static GcsBlockReplyHeader
ut_r4_refusal_header(GcsBlockReplyStatus status, uint64 page_lsn)
{
	GcsBlockReplyHeader header;

	memset(&header, 0, sizeof(header));
	header.request_id = UINT64_C(0x1020304050607080);
	header.page_lsn = page_lsn;
	header.epoch = UINT64_C(9);
	header.sender_node = cluster_node_id;
	header.requester_backend_id = 17;
	header.transition_id = PCM_TRANS_N_TO_S;
	header.status = (uint8)status;
	GcsBlockReplyHeaderSetForwardingMasterNode(&header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return header;
}

static ResourceXIntentSlot
ut_resource_x_grant_intent(int32 destination_node)
{
	ResourceXIntentSlot intent;
	BufferTag tag;

	memset(&intent, 0, sizeof(intent));
	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 11;
	tag.dbOid = 12;
	tag.relNumber = 13;
	tag.blockNum = 15;
	intent.body.assertion.resource = tag;
	intent.body.assertion.requester_node = destination_node;
	intent.logical_generation = 41;
	intent.authority_generation = 42;
	intent.first_armed_us = 43;
	intent.destination_node = (uint32)destination_node;
	intent.payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	intent.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	intent.state = RESOURCE_X_INTENT_SLOT_ARMED;
	intent.body.owner_generation = 42;
	intent.body.owner_node = 0;
	intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;
	return intent;
}

static ResourceXIntentSlot
ut_resource_x_block_intent(int32 destination_node)
{
	ResourceXIntentSlot intent = ut_resource_x_grant_intent(2);

	intent.destination_node = (uint32)destination_node;
	intent.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	intent.kind = RESOURCE_X_WIRE_BLOCK_TO_N;
	intent.body.owner_generation = intent.logical_generation;
	intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_BLOCK;
	intent.body.owner_index = (uint8)destination_node;
	return intent;
}

static ResourceXIntentSlot
ut_resource_x_image_intent(int32 destination_node)
{
	ResourceXIntentSlot intent = ut_resource_x_grant_intent(destination_node);

	intent.payload_bytes = RESOURCE_X_IMAGE_V1_BYTES;
	intent.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	intent.body.owner_generation = intent.logical_generation;
	intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE;
	return intent;
}

static ResourceXIntentSlot
ut_resource_x_settlement_intent(int32 destination_node)
{
	ResourceXIntentSlot intent = ut_resource_x_grant_intent(destination_node);

	intent.payload_bytes = RESOURCE_X_SHORT_V1_BYTES;
	intent.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	intent.body.assertion.requester_node = cluster_node_id;
	intent.body.owner_generation = intent.logical_generation;
	intent.body.owner_node = (uint32)cluster_node_id;
	intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT;
	return intent;
}

static ResourceXIntentSlot
ut_resource_x_holder_release_intent(int32 destination_node)
{
	ResourceXIntentSlot intent = ut_resource_x_grant_intent(2);

	intent.destination_node = (uint32)destination_node;
	intent.payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	intent.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	intent.body.owner_generation = intent.logical_generation;
	intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE;
	return intent;
}

/* ============================================================
 * Tests.
 * ============================================================ */

/* U0: ring shmem up through the production region hooks. */
UT_TEST(test_ring_shmem_init)
{
	int i;

	cluster_lms_outbound_shmem_register();
	UT_ASSERT(ut_captured_region != NULL);
	ut_captured_region->init_fn();

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		ut_peer_rc[i] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, 0x01));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_count_marker(0x01), 1);
}

/*
 * U1 (RED): a frame the transport ADMITTED (WOULD_BLOCK) must never be
 * resubmitted.  Pre-fix drain head-requeued it, and the next drain sent
 * a second copy onto the per-peer stream.
 */
UT_TEST(test_admitted_frame_is_never_resubmitted)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(1, UT_PEER_X, 0xA1));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK;
	(void)cluster_lms_outbound_drain_send(1);

	/* The transport owns the frame now; a later drain must not resend. */
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(1);

	UT_ASSERT_EQ(ut_count_marker(0xA1), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(1), 0);
}

/*
 * U2 (RED): a backpressured peer must not head-of-line block other peers
 * sharing the worker ring.  Pre-fix drain broke the batch on the first
 * WOULD_BLOCK, so Y's frame sat parked behind X's.
 */
UT_TEST(test_blocked_peer_does_not_starve_other_peer)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(2, UT_PEER_X, 0xB1));
	UT_ASSERT(ut_enqueue_marker(2, UT_PEER_Y, 0xB2));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK; /* admitted */
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(2);

	UT_ASSERT_EQ(ut_count_marker(0xB1), 1);
	UT_ASSERT_EQ(ut_count_marker(0xB2), 1); /* Y sent in the SAME batch */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
}

/*
 * U3: a REFUSED frame (NOT_ADMITTED) is retained — never dropped — and a
 * later drain delivers it once the transport admits it.
 */
UT_TEST(test_refused_frame_retained_and_delivered)
{
	int refused0 = ut_not_admitted_count;

	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(3, UT_PEER_X, 0xC1));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	(void)cluster_lms_outbound_drain_send(3);
	UT_ASSERT_EQ(ut_count_marker(0xC1), 1);			   /* attempted once */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(3), 1);	   /* retained */
	UT_ASSERT_EQ(ut_not_admitted_count - refused0, 1); /* counted */

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(3);
	UT_ASSERT_EQ(ut_count_marker(0xC1), 2); /* re-attempted exactly once */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(3), 0);
}

/*
 * U4: after a peer refuses a frame, its LATER frames in the same batch
 * must not be attempted (they queue in order BEHIND the refused one) while
 * other peers keep flowing;  the next drain then delivers the retained
 * frames in original submission order.
 */
UT_TEST(test_blocked_peer_batch_keeps_per_peer_order)
{
	int i;
	int d1_idx = -1;
	int d2_idx = -1;

	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_X, 0xD1));
	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_X, 0xD2));
	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_Y, 0xD3));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(4);

	UT_ASSERT_EQ(ut_count_marker(0xD1), 1); /* attempted + refused */
	UT_ASSERT_EQ(ut_count_marker(0xD2), 0); /* never attempted past D1 */
	UT_ASSERT_EQ(ut_count_marker(0xD3), 1); /* Y flowed in the same batch */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(4), 2);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(4);
	UT_ASSERT_EQ(ut_count_marker(0xD1), 2);
	UT_ASSERT_EQ(ut_count_marker(0xD2), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(4), 0);

	/* Submission order preserved: D1's redelivery precedes D2's. */
	for (i = 0; i < ut_sent_n && i < (int)lengthof(ut_sent_log); i++) {
		if (ut_sent_log[i].marker == 0xD2 && d2_idx < 0)
			d2_idx = i;
		if (ut_sent_log[i].marker == 0xD1)
			d1_idx = i; /* last D1 attempt (the delivery) */
	}
	UT_ASSERT(d1_idx >= 0 && d2_idx >= 0);
	UT_ASSERT(d1_idx < d2_idx);
}

/*
 * PCM-X can hash a tag to the local node's master.  A DATA frame staged by a
 * backend must still execute on that tag's LMS worker: the generic IC send
 * self-shortcut reports DONE without dispatching, which would otherwise turn
 * ENQUEUE/ACK into a silent no-op.  The worker therefore loopback-dispatches
 * self frames and never hands them to the transport.
 */
UT_TEST(test_self_frame_dispatches_on_owning_worker)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(5, cluster_node_id, 0xE1));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(5), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(5), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_local_dispatch_count, 1);
	UT_ASSERT_EQ(ut_local_dispatch_marker, 0xE1);
}


/*
 * Shape-B denial replay is driven by LMON, which owns only plane 0.  The
 * reply is an ABI-sized header + zero block, so LMON stages its compact
 * header on the tag's DATA ring and the owning LMS worker expands and sends
 * it.  A direct LMON send is a production FATAL under the plane guard.
 */
UT_TEST(test_zero_block_reply_is_expanded_by_data_owner)
{
	GcsBlockReplyHeader hdr;

	ut_reset_log();
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = UINT64_C(0x1122334455667788);
	hdr.epoch = UINT64_C(41);
	hdr.sender_node = 1;
	hdr.requester_backend_id = 17;
	hdr.transition_id = PCM_TRANS_N_TO_X;
	hdr.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply(2, UT_PEER_X, &hdr, false));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ((int)ut_sent_log[0].payload_len, (int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ((int)ut_sent_log[1].msg_type, (int)PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ((int)ut_sent_log[1].payload_len, (int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(ut_sent_log[1].reply_header.request_id, hdr.request_id);
	UT_ASSERT_EQ((int)ut_sent_log[1].reply_header.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ(ut_sent_log[1].reply_header.checksum, UINT32_C(0xA55A7E11));
}

UT_TEST(test_direct_zero_block_reply_uses_data_owner_direct_lane)
{
	GcsBlockReplyHeader hdr;

	ut_reset_log();
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = UINT64_C(0x8877665544332211);
	hdr.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;

	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply(3, UT_PEER_Y, &hdr, true));
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(3), 1);
	UT_ASSERT_EQ(ut_direct_zero_reply_count, 1);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_direct_zero_reply_header.request_id, hdr.request_id);
	UT_ASSERT_EQ(ut_direct_zero_reply_header.checksum, UINT32_C(0xA55A7E11));
}

UT_TEST(test_r4_cap_bound_zero_reply_sends_only_on_exact_generation)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1;
	GcsBlockReplyHeader hdr = ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 1);

	ut_reset_log();
	ut_peer_capabilities[UT_PEER_X] = cap;
	ut_peer_cap_generation[UT_PEER_X] = 42;
	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(2, UT_PEER_X, &hdr, cap, 42));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_checksum_call_count, 1);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT(ut_sent_log[0].reply_block_zero);
	UT_ASSERT_EQ(ut_sent_log[0].reply_header.status, GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED);
	UT_ASSERT_EQ(ut_sent_log[0].reply_header.page_lsn, 1);
	UT_ASSERT_EQ(ut_sent_log[0].reply_header.checksum, UINT32_C(0xA55A7E11));
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&ut_sent_log[0].reply_header),
				 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
}

UT_TEST(test_r4_cap_bound_zero_reply_drops_drift_before_zero_expansion)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1;
	GcsBlockReplyHeader hdr = ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_DENIED, 0);

	ut_reset_log();
	ut_peer_capabilities[UT_PEER_X] = cap;
	ut_peer_cap_generation[UT_PEER_X] = 43;
	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(2, UT_PEER_X, &hdr, cap, 42));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_checksum_call_count, 0);
	UT_ASSERT_EQ(ut_cap_guard_drop_count, 1);
}

/* Cover the missing handoff: a real holder refusal carries the master identity,
 * unlike a master refusal. Both enqueue and drain must accept it, and the real
 * requester decoder must still bind it to that exact master/request/epoch. */
UT_TEST(test_r4_real_refusal_producers_cross_outbound_and_requester_boundary)
{
	const int masters[] = { 0, 1, UT_PEER_X, CLUSTER_MAX_NODES - 1 };
	const ClusterCrBuildReason reasons[]
		= { CLUSTER_CR_BUILD_CAPACITY, CLUSTER_CR_BUILD_HOLDER_MOVED, CLUSTER_CR_BUILD_PROTOCOL };
	int m;
	int r;

	for (m = 0; m < lengthof(masters); m++) {
		for (r = 0; r < lengthof(reasons); r++) {
			ClusterR4CrForwardPayload forward = { 0 };
			GcsBlockR4ReplyExpectation expected = { 0 };
			ClusterICEnvelope env = { 0 };
			ClusterCrBuildResult result
				= r == 2 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_RETRYABLE;
			bool accepted;
			bool wrong_master;
			bool wrong_request;
			bool wrong_epoch;

			ut_reset_log();
			ut_r4_real_checksum = true;
			ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
			ut_peer_capabilities[UT_PEER_X] = R4_CR_REQUIRED_HELLO_CAPS;
			ut_peer_cap_generation[UT_PEER_X] = 42;
			forward.base.request_id = 123;
			forward.base.epoch = 9;
			forward.base.master_node = masters[m];
			forward.base.original_requester_node = UT_PEER_X;
			forward.base.requester_backend_id = 17;
			forward.base.transition_id = PCM_TRANS_N_TO_S;
			UT_ASSERT(gcs_block_r4_publish_holder_refusal(2, &forward, 42, result, reasons[r]));
			UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
			UT_ASSERT_EQ(ut_sent_n, 0);
			UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
			UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
			UT_ASSERT_EQ(ut_sent_n, 1);
			UT_ASSERT_EQ(ut_sent_log[0].dest, UT_PEER_X);
			UT_ASSERT(ut_sent_log[0].reply_block_zero);
			UT_ASSERT_EQ(ut_sent_log[0].reply_header.status,
						 r == 2 ? GCS_BLOCK_REPLY_R4_DENIED
								: GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED);
			UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&ut_sent_log[0].reply_header),
						 masters[m]);
			expected.request_id = forward.base.request_id;
			expected.epoch = forward.base.epoch;
			expected.sender_node = cluster_node_id;
			expected.forwarding_master_node = masters[m];
			expected.requester_backend_id = forward.base.requester_backend_id;
			expected.transition_id = PCM_TRANS_N_TO_S;
			expected.reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
			env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
			env.source_node_id = cluster_node_id;
			env.dest_node_id = UT_PEER_X;
			env.payload_length = sizeof(ut_r4_reply_payload);
			cluster_node_id = UT_PEER_X;
			accepted = gcs_block_decode_r4_reply_payload(&env, ut_r4_reply_payload, &expected);
			expected.forwarding_master_node = GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
			wrong_master = gcs_block_decode_r4_reply_payload(&env, ut_r4_reply_payload, &expected);
			expected.forwarding_master_node = masters[m];
			expected.request_id++;
			wrong_request = gcs_block_decode_r4_reply_payload(&env, ut_r4_reply_payload, &expected);
			expected.request_id--;
			expected.epoch++;
			wrong_epoch = gcs_block_decode_r4_reply_payload(&env, ut_r4_reply_payload, &expected);
			cluster_node_id = 0;
			UT_ASSERT(accepted);
			UT_ASSERT(!wrong_master && !wrong_request && !wrong_epoch);
		}
	}
}

UT_TEST(test_r4_real_master_refusal_preserves_redirect)
{
	ClusterR4CrRequestPayload request = { 0 };
	ClusterICEnvelope env = { 0 };

	ut_reset_log();
	ut_peer_capabilities[UT_PEER_X] = R4_CR_REQUIRED_HELLO_CAPS;
	ut_peer_cap_generation[UT_PEER_X] = 42;
	request.base.request_id = 321;
	request.base.epoch = 9;
	request.base.requester_backend_id = 17;
	request.base.transition_id = PCM_TRANS_N_TO_S;
	env.source_node_id = UT_PEER_X;
	UT_ASSERT(gcs_block_r4_publish_refusal(2, &env, &request, 42, CLUSTER_CR_BUILD_RETRYABLE,
										   CLUSTER_CR_BUILD_WRONG_MASTER, false,
										   CLUSTER_MAX_NODES - 1));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].reply_header.page_lsn, CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&ut_sent_log[0].reply_header),
				 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
}

UT_TEST(test_r4_holder_refusal_retains_backpressure_and_rejects_reconnect)
{
	GcsBlockReplyHeader hdr = ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_DENIED, 0);

	ut_reset_log();
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 1);
	ut_peer_capabilities[UT_PEER_X] = R4_CR_REQUIRED_HELLO_CAPS;
	ut_peer_cap_generation[UT_PEER_X] = 42;
	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
		2, UT_PEER_X, &hdr, R4_CR_REQUIRED_HELLO_CAPS, 42));
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ(memcmp(&ut_sent_log[0].reply_header, &ut_sent_log[1].reply_header, sizeof(hdr)),
				 0);
	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
		2, UT_PEER_X, &hdr, R4_CR_REQUIRED_HELLO_CAPS, 42));
	ut_peer_cap_generation[UT_PEER_X] = 43;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ(ut_cap_guard_drop_count, 1);
}

UT_TEST(test_r4_holder_refusal_rejects_malformed_identity)
{
	int mutation;

	ut_reset_log();
	for (mutation = 0; mutation < 12; mutation++) {
		GcsBlockReplyHeader hdr
			= ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);

		GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 1);
		switch (mutation) {
		case 0:
			GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, -2);
			break;
		case 1:
			GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, CLUSTER_MAX_NODES);
			break;
		case 2:
			hdr.page_lsn = 1;
			break;
		case 3:
			hdr.status = GCS_BLOCK_REPLY_R4_DENIED;
			hdr.page_lsn = 1;
			break;
		case 4:
			hdr.request_id = 0;
			break;
		case 5:
			hdr.sender_node = -1;
			break;
		case 6:
			hdr.sender_node = CLUSTER_MAX_NODES;
			break;
		case 7:
			hdr.requester_backend_id = 0;
			break;
		case 8:
			hdr.transition_id = PCM_TRANS_N_TO_X;
			break;
		case 9:
			hdr.checksum = 1;
			break;
		case 10:
			hdr.reserved_0[0] = 1;
			break;
		case 11:
			hdr.status = GCS_BLOCK_REPLY_R4_CR_FULL;
			break;
		}
		UT_ASSERT(!cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
			2, UT_PEER_X, &hdr, R4_CR_REQUIRED_HELLO_CAPS, 42));
	}
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
}

UT_TEST(test_zero_reply_wrappers_reject_the_other_status_domain)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1;
	GcsBlockReplyHeader legacy = ut_r4_refusal_header(GCS_BLOCK_REPLY_DENIED_PENDING_X, 0);
	GcsBlockReplyHeader retryable
		= ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	GcsBlockReplyHeader denied = ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_DENIED, 1);

	ut_reset_log();
	UT_ASSERT(!cluster_lms_outbound_enqueue_zero_block_reply(0, UT_PEER_X, &retryable, false));
	UT_ASSERT(
		!cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(0, UT_PEER_X, &legacy, cap, 42));
	UT_ASSERT(
		!cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(0, UT_PEER_X, &denied, cap, 42));
	retryable.reserved_0[0] = 1;
	UT_ASSERT(!cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(0, UT_PEER_X, &retryable,
																	   cap, 42));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
}

/* A producer must receive false when the selected worker ring is full.  The
 * PI durable-note drain couples this real return contract with its structural
 * false->break-before-seq-advance unit, so a full shard retains the source
 * note for the next tick instead of losing it. */
UT_TEST(test_full_worker_ring_refuses_without_overwrite)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1;
	GcsBlockReplyHeader refusal = ut_r4_refusal_header(GCS_BLOCK_REPLY_R4_DENIED, 0);
	int accepted = 0;
	int sent = 0;

	ut_reset_log();
	while (accepted < 1024 && ut_enqueue_marker(1, UT_PEER_X, 0xE2))
		accepted++;
	UT_ASSERT(accepted > 0);
	UT_ASSERT(accepted < 1024);
	UT_ASSERT_EQ((int)cluster_lms_outbound_depth(1), accepted);
	UT_ASSERT(!ut_enqueue_marker(1, UT_PEER_X, 0xE3));
	UT_ASSERT(
		!cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(1, UT_PEER_X, &refusal, cap, 42));
	UT_ASSERT_EQ((int)cluster_lms_outbound_depth(1), accepted);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	while (cluster_lms_outbound_depth(1) > 0)
		sent += cluster_lms_outbound_drain_send(1);
	UT_ASSERT_EQ(sent, accepted);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(1), 0);
}

/* The real ring, not a post-refusal depth guess, distinguishes capacity from
 * an invalid call. A FULL try must own no copy; after one drain only one copy
 * of the pending frame can become admitted. */
UT_TEST(test_typed_admission_full_then_drain_never_duplicates)
{
	uint8 marker = 0xa6;
	int accepted = 0;

	ut_reset_log();
	UT_ASSERT_EQ(cluster_lms_outbound_try_enqueue(-1, UT_MSG_TYPE, UT_PEER_X, &marker, 1),
				 CLUSTER_LMS_ENQUEUE_INVALID);
	UT_ASSERT_EQ(cluster_lms_outbound_try_enqueue(1, UT_MSG_TYPE, UT_PEER_X, &marker, UINT16_MAX),
				 CLUSTER_LMS_ENQUEUE_INVALID);
	while (accepted < 1024 && ut_enqueue_marker(1, UT_PEER_X, 0xe2))
		accepted++;
	UT_ASSERT(accepted > 0 && accepted < 1024);
	UT_ASSERT_EQ(cluster_lms_outbound_try_enqueue(1, UT_MSG_TYPE, UT_PEER_X, &marker, 1),
				 CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(1), accepted);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(cluster_lms_outbound_drain_send(1) > 0);
	UT_ASSERT_EQ(cluster_lms_outbound_try_enqueue(1, UT_MSG_TYPE, UT_PEER_X, &marker, 1),
				 CLUSTER_LMS_ENQUEUE_ADMITTED);
	while (cluster_lms_outbound_depth(1) > 0)
		(void)cluster_lms_outbound_drain_send(1);
	UT_ASSERT(ut_sent_n <= (int)lengthof(ut_sent_log));
	UT_ASSERT_EQ(ut_count_marker(marker), 1);
	UT_ASSERT_EQ(ut_sent_n, accepted + 1);
}

/* A V2 wire frame is legal only on the exact HELLO-authenticated connection
 * generation sampled by its producer.  A reconnect or capability downgrade
 * consumes the stale ring copy without transport admission; the reliable
 * protocol leg remains armed and the periodic master drive reconstructs it. */
UT_TEST(test_cap_bound_frame_drops_on_connection_generation_drift)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1;
	uint8 marker = 0x91;

	ut_reset_log();
	ut_peer_capabilities[UT_PEER_X] = cap;
	ut_peer_cap_generation[UT_PEER_X] = 18;
	UT_ASSERT(cluster_lms_outbound_enqueue_cap_bound(0, PGRAC_IC_MSG_PCM_X_REVOKE, UT_PEER_X,
													 &marker, sizeof(marker), cap, 17));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_count_marker(marker), 0);
	UT_ASSERT_EQ(ut_cap_guard_drop_count, 1);
}

UT_TEST(test_cap_bound_frame_drops_on_capability_downgrade)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1;
	uint8 marker = 0x92;

	ut_reset_log();
	ut_peer_cap_generation[UT_PEER_X] = 21;
	UT_ASSERT(cluster_lms_outbound_enqueue_cap_bound(0, PGRAC_IC_MSG_PCM_X_REVOKE, UT_PEER_X,
													 &marker, sizeof(marker), cap, 21));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_count_marker(marker), 0);
	UT_ASSERT_EQ(ut_cap_guard_drop_count, 1);
}

UT_TEST(test_cap_bound_frame_sends_on_exact_connection_capability)
{
	const uint32 cap = PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1;
	uint8 marker = 0x93;

	ut_reset_log();
	ut_peer_capabilities[UT_PEER_X] = cap;
	ut_peer_cap_generation[UT_PEER_X] = 34;
	UT_ASSERT(cluster_lms_outbound_enqueue_cap_bound(0, PGRAC_IC_MSG_PCM_X_REVOKE, UT_PEER_X,
													 &marker, sizeof(marker), cap, 34));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_count_marker(marker), 1);
	UT_ASSERT_EQ(ut_cap_guard_drop_count, 0);
}

UT_TEST(test_resource_x_intent_admission_stages_and_completion_clears_owner)
{
	ClusterLmsResourceXTransportSnapshot after;
	ClusterLmsResourceXTransportSnapshot before;
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xA6;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 77;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&before));
	UT_ASSERT_EQ(before.staged_count, 0);
	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 77, UINT64_MAX));
	UT_ASSERT_EQ(ut_resource_x_stage_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 1);
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&after));
	UT_ASSERT_EQ(after.staged_count, 1);
	UT_ASSERT(after.mutation_sequence > before.mutation_sequence);
	before = after;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&after));
	UT_ASSERT_EQ(after.staged_count, 0);
	UT_ASSERT(after.mutation_sequence > before.mutation_sequence);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_IMAGE_OR_GRANT);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xA6);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 77);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_resource_x_block_intent_uses_type17_and_control_payload)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_block_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xA9;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 83;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 83, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_BLOCK_TO_N);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xA9);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 83);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
}

UT_TEST(test_resource_x_settlement_intent_uses_type38_and_short_payload)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_settlement_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xAB;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 86;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 86, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_SHORT_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xAB);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 86);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
}

UT_TEST(test_resource_x_holder_release_transport_rearms_until_typed_ack)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_holder_release_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xAC;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 87;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 87, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_BLOCK_TO_N);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xAC);
	UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}

UT_TEST(test_resource_x_source_settlement_ack_fits_ordinary_data_ring)
{
	uint8 ack[RESOURCE_X_PROOF_V1_BYTES] = { 0xAD };

	ut_reset_log();
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(
		cluster_lms_outbound_enqueue(0, RESOURCE_X_MSG_BLOCKED_TO_N, UT_PEER_X, ack, sizeof(ack)));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_BLOCKED_TO_N);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xAD);
}

UT_TEST(test_resource_x_image_intent_rebinds_transport_generation)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_image_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xAA;
	ut_resource_x_decode_sender_generation = 84;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 84;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 84, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].msg_type, RESOURCE_X_MSG_IMAGE_OR_GRANT);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xAA);
	UT_ASSERT_EQ(ut_resource_x_decode_count, 0);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 84);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);

	ut_reset_log();
	intent = ut_resource_x_image_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_decode_sender_generation = 84;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 85;
	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 85, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_resource_x_decode_count, 0);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 85);
	UT_ASSERT_EQ(ut_resource_x_rearm_count, 0);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_resource_x_intent_transport_refusal_rearms_without_ring_copy)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xA7;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 78;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;

	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 78, UINT64_MAX));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
}

UT_TEST(test_resource_x_intent_capability_drift_rearms_before_send)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 79;
	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 79, UINT64_MAX));
	ut_peer_cap_generation[UT_PEER_X] = 80;

	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}

UT_TEST(test_resource_x_intent_physical_deadline_rearms_before_send)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 81;
	UT_ASSERT(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 81, 1));

	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}

UT_TEST(test_resource_x_intent_pump_stages_found_owner_on_tag_shard)
{
	ResourceXIntentSlot intent;
	int worker_id;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_owner_payload[0] = 0xA8;
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_FOUND;
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 82;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	worker_id = cluster_lms_shard_for_tag(&intent.body.assertion.resource, cluster_lms_workers);

	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 1);
	UT_ASSERT_EQ(ut_resource_x_probe_call_count, 2);
	UT_ASSERT_EQ(ut_resource_x_probe_max_budget, 4);
	UT_ASSERT_EQ(ut_resource_x_stage_count, 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xA8);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
}

UT_TEST(test_resource_x_intent_pump_not_admitted_preserves_owner)
{
	ResourceXIntentSlot intent;

	ut_reset_log();
	intent = ut_resource_x_grant_intent(UT_PEER_X);
	ut_resource_x_owner_slot = intent;
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_FOUND;

	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 0);
	UT_ASSERT_EQ(ut_resource_x_probe_call_count, 2);
	UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT(ut_resource_x_owner_slot.last_attempt_us != 0);
	UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
}

UT_TEST(test_resource_x_intent_pump_is_bounded_to_sixteen_four_probes)
{
	ut_reset_log();
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_MORE;
	ut_wakeup_count = 0;

	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 0);
	UT_ASSERT_EQ(ut_resource_x_probe_call_count, 16);
	UT_ASSERT_EQ(ut_resource_x_probe_max_budget, 4);
	UT_ASSERT_EQ(ut_wakeup_count, 1);
}

UT_TEST(test_resource_x_intent_pump_drives_local_delivery_without_wire_or_busy_spin)
{
	ut_reset_log();
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_DELIVERY;
	ut_resource_x_delivery_tick_count = 0;
	ut_wakeup_count = 0;
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 0);
	UT_ASSERT_EQ(ut_resource_x_delivery_tick_count, 1);
	UT_ASSERT_EQ(ut_resource_x_probe_call_count, 2);
	UT_ASSERT_EQ(ut_resource_x_probe_max_budget, 4);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
	UT_ASSERT_EQ(ut_wakeup_count, 0);
	ut_reset_log();
	ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_SOURCE_FINISH;
	ut_resource_x_source_finish_tick_count = 0;
	ut_wakeup_count = 0;
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 0);
	UT_ASSERT_EQ(ut_resource_x_source_finish_tick_count, 1);
	UT_ASSERT_EQ(ut_resource_x_probe_call_count, 2);
	UT_ASSERT_EQ(ut_resource_x_probe_max_budget, 4);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
	UT_ASSERT_EQ(ut_wakeup_count, 0);
}

UT_TEST(test_resource_x_type14_assert_and_local_proof_share_one_data_fifo)
{
	uint8 assertion[RESOURCE_X_CONTROL_V1_BYTES] = { 0xA1 };
	uint8 local_proof[RESOURCE_X_SHORT_V1_BYTES] = { 0xA2 };
	int worker_id = 1;

	ut_reset_log();
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(cluster_lms_outbound_enqueue(worker_id, RESOURCE_X_MSG_ASSERT_X, UT_PEER_X, assertion,
										   sizeof(assertion)));
	UT_ASSERT(cluster_lms_outbound_enqueue(worker_id, RESOURCE_X_MSG_ASSERT_X, UT_PEER_X,
										   local_proof, sizeof(local_proof)));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 2);
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 2);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ(ut_sent_log[0].marker, 0xA1);
	UT_ASSERT_EQ(ut_sent_log[0].payload_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[1].marker, 0xA2);
	UT_ASSERT_EQ(ut_sent_log[1].payload_len, RESOURCE_X_SHORT_V1_BYTES);
}

/* The approved remote-S adaptation first retains an unsendable type-18 in
 * the existing DATA ring.  Only the exact post-revoke N tuple may make it
 * READY; transport/capability failures retain that READY proof. */
UT_TEST(test_resource_x_remote_s_status_is_pending_then_exact_ready)
{
	ClusterLmsRemoteSStatusHandle handle;
	ClusterLmsResourceXTransportSnapshot snapshot;
	uint64 transport_sequence;
	ClusterPcmOwnSnapshot released;
	ClusterPcmOwnSnapshot revoking;
	uint8 status[RESOURCE_X_CONTROL_V1_BYTES] = { 0xB1 };
	int worker_id = 1;

	ut_reset_log();
	memset(&handle, 0, sizeof(handle));
	memset(&revoking, 0, sizeof(revoking));
	revoking.tag.spcOid = 11;
	revoking.tag.dbOid = 12;
	revoking.tag.relNumber = 13;
	revoking.tag.forkNum = MAIN_FORKNUM;
	revoking.tag.blockNum = 14;
	revoking.generation = 17;
	revoking.reservation_token = 9;
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&snapshot));
	transport_sequence = snapshot.mutation_sequence;

	UT_ASSERT_EQ(cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
					 worker_id, UT_PEER_X, status, sizeof(status), &revoking, &handle),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(handle.slot_cookie != 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 1);
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&snapshot));
	UT_ASSERT(snapshot.mutation_sequence > transport_sequence);
	transport_sequence = snapshot.mutation_sequence;

	/* PENDING is retained but never transport-visible. */
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(ut_sent_n, 0);

	released = revoking;
	released.generation += 2;
	released.flags = 0;
	released.pcm_state = (uint8)PCM_STATE_N;
	UT_ASSERT_EQ(cluster_lms_outbound_publish_resource_x_remote_s_status_exact(&handle, &released),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(ut_sent_n, 0);

	released.generation = revoking.generation + 1;
	UT_ASSERT_EQ(cluster_lms_outbound_publish_resource_x_remote_s_status_exact(&handle, &released),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&snapshot));
	UT_ASSERT(snapshot.mutation_sequence > transport_sequence);
	transport_sequence = snapshot.mutation_sequence;
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);

	/* READY without an exact capability is retained, not dropped. */
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(ut_sent_n, 0);

	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 88;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_HARD_ERROR;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_count, 1);
	UT_ASSERT_EQ(ut_resource_x_rebind_generation, 88);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
	UT_ASSERT(cluster_lms_outbound_resource_x_transport_snapshot(&snapshot));
	UT_ASSERT(snapshot.mutation_sequence > transport_sequence);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ(ut_sent_log[1].msg_type, RESOURCE_X_MSG_BLOCKED_TO_N);
	UT_ASSERT_EQ(ut_sent_log[1].payload_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(ut_sent_log[1].marker, 0xB1);
}

UT_TEST(test_resource_x_remote_s_pending_can_cancel_without_send)
{
	ClusterLmsRemoteSStatusHandle handle;
	ClusterPcmOwnSnapshot revoking;
	uint8 status[RESOURCE_X_CONTROL_V1_BYTES] = { 0xB2 };
	int worker_id = 1;

	ut_reset_log();
	memset(&handle, 0, sizeof(handle));
	memset(&revoking, 0, sizeof(revoking));
	revoking.tag.relNumber = 21;
	revoking.tag.forkNum = MAIN_FORKNUM;
	revoking.tag.blockNum = 22;
	revoking.generation = 23;
	revoking.reservation_token = 24;
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_S;

	UT_ASSERT_EQ(cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
					 worker_id, UT_PEER_X, status, sizeof(status), &revoking, &handle),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(&handle),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(&handle),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
}

UT_TEST(test_resource_x_nonrequester_s_status_self_master_loopback_is_retained)
{
	ClusterLmsRemoteSStatusHandle handle;
	ClusterPcmOwnSnapshot released;
	ClusterPcmOwnSnapshot revoking;
	uint8 status[RESOURCE_X_CONTROL_V1_BYTES] = { 0xB3 };
	int worker_id = 1;

	ut_reset_log();
	memset(&handle, 0, sizeof(handle));
	memset(&revoking, 0, sizeof(revoking));
	revoking.tag.relNumber = 31;
	revoking.tag.forkNum = MAIN_FORKNUM;
	revoking.tag.blockNum = 32;
	revoking.generation = 33;
	revoking.reservation_token = 34;
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_S;

	UT_ASSERT_EQ(
		cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
			worker_id, (uint32)cluster_node_id, status, sizeof(status), &revoking, &handle),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 1);
	UT_ASSERT_EQ(ut_local_dispatch_count, 0);

	released = revoking;
	released.generation++;
	released.flags = 0;
	released.pcm_state = (uint8)PCM_STATE_N;
	UT_ASSERT_EQ(cluster_lms_outbound_publish_resource_x_remote_s_status_exact(&handle, &released),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker_id), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(worker_id), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_local_dispatch_count, 1);
	UT_ASSERT_EQ(ut_local_dispatch_marker, 0xB3);
}

UT_TEST(test_normal_stop_missing_outbound_is_not_empty)
{
	int worker;
	uint32 slot;
	const char *reason;

	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(strcmp(reason, "OUTBOUND_UNINITIALIZED"), 0);
	UT_ASSERT_EQ(worker, -1);
}

UT_TEST(test_normal_stop_queue_observation_follows_real_handoff)
{
	int worker;
	uint32 slot;
	const char *reason;

	ut_captured_region->init_fn();
	ut_reset_log();
	ut_lock_reads = 0;
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_lock_reads, CLUSTER_LMS_MAX_WORKERS);
	UT_ASSERT(ut_enqueue_marker(1, UT_PEER_X, 0x81));
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(worker, 1);
	UT_ASSERT_EQ(slot, 0);
	UT_ASSERT_EQ(strcmp(reason, "OUTBOUND_FRAME_PENDING"), 0);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1);
	/* Ring no longer owns it. This READY does NOT certify the IC FIFO. */
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_NULL(ut_held_lock);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, 0x82));
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_normal_stop_inactive_and_malformed_ring_are_not_hidden)
{
	int worker;
	uint32 slot;
	const char *reason;

	ut_captured_region->init_fn();
	UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, 0x83));
	UT_ASSERT(ut_enqueue_marker(7, UT_PEER_X, 0x84));
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(worker, 7); /* INVALID overrides the earlier ring's PENDING */
	UT_ASSERT_EQ(strcmp(reason, "OUTBOUND_INACTIVE_RING"), 0);
	ut_captured_region->init_fn();
	cluster_lms_outbound_test_geometry(0, 4, 4, 0); /* retained cursor is not debt */
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	cluster_lms_outbound_test_geometry(0, 5, 4, 0);
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(strcmp(reason, "OUTBOUND_RING_GEOMETRY"), 0);
	cluster_lms_outbound_test_geometry(0, 256, 0, 0);
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	cluster_lms_outbound_test_geometry(0, 0, 0, 257);
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_NULL(ut_held_lock);
}

UT_TEST(test_normal_stop_full_and_bad_frames_remain_debt)
{
	int worker;
	uint32 slot;
	const char *reason;

	ut_captured_region->init_fn();
	for (int i = 0; i < 256; i++)
		UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, (uint8)i));
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 256);
	ut_captured_region->init_fn();
	UT_ASSERT(ut_enqueue_marker(0, CLUSTER_MAX_NODES, 0x85));
	UT_ASSERT_EQ(cluster_lms_outbound_normal_stop_poll(&worker, &slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(strcmp(reason, "OUTBOUND_FRAME_INVALID"), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1); /* poll never discards */
}

int
main(void)
{
	UT_PLAN(40);

	UT_RUN(test_normal_stop_missing_outbound_is_not_empty);
	UT_RUN(test_ring_shmem_init);
	UT_RUN(test_admitted_frame_is_never_resubmitted);
	UT_RUN(test_blocked_peer_does_not_starve_other_peer);
	UT_RUN(test_refused_frame_retained_and_delivered);
	UT_RUN(test_blocked_peer_batch_keeps_per_peer_order);
	UT_RUN(test_self_frame_dispatches_on_owning_worker);
	UT_RUN(test_zero_block_reply_is_expanded_by_data_owner);
	UT_RUN(test_direct_zero_block_reply_uses_data_owner_direct_lane);
	UT_RUN(test_r4_cap_bound_zero_reply_sends_only_on_exact_generation);
	UT_RUN(test_r4_cap_bound_zero_reply_drops_drift_before_zero_expansion);
	UT_RUN(test_r4_real_refusal_producers_cross_outbound_and_requester_boundary);
	UT_RUN(test_r4_real_master_refusal_preserves_redirect);
	UT_RUN(test_r4_holder_refusal_retains_backpressure_and_rejects_reconnect);
	UT_RUN(test_r4_holder_refusal_rejects_malformed_identity);
	UT_RUN(test_zero_reply_wrappers_reject_the_other_status_domain);
	UT_RUN(test_full_worker_ring_refuses_without_overwrite);
	UT_RUN(test_typed_admission_full_then_drain_never_duplicates);
	UT_RUN(test_cap_bound_frame_drops_on_connection_generation_drift);
	UT_RUN(test_cap_bound_frame_drops_on_capability_downgrade);
	UT_RUN(test_cap_bound_frame_sends_on_exact_connection_capability);
	UT_RUN(test_resource_x_intent_admission_stages_and_completion_clears_owner);
	UT_RUN(test_resource_x_block_intent_uses_type17_and_control_payload);
	UT_RUN(test_resource_x_settlement_intent_uses_type38_and_short_payload);
	UT_RUN(test_resource_x_holder_release_transport_rearms_until_typed_ack);
	UT_RUN(test_resource_x_source_settlement_ack_fits_ordinary_data_ring);
	UT_RUN(test_resource_x_image_intent_rebinds_transport_generation);
	UT_RUN(test_resource_x_intent_transport_refusal_rearms_without_ring_copy);
	UT_RUN(test_resource_x_intent_capability_drift_rearms_before_send);
	UT_RUN(test_resource_x_intent_physical_deadline_rearms_before_send);
	UT_RUN(test_resource_x_intent_pump_stages_found_owner_on_tag_shard);
	UT_RUN(test_resource_x_intent_pump_not_admitted_preserves_owner);
	UT_RUN(test_resource_x_intent_pump_is_bounded_to_sixteen_four_probes);
	UT_RUN(test_resource_x_intent_pump_drives_local_delivery_without_wire_or_busy_spin);
	UT_RUN(test_resource_x_type14_assert_and_local_proof_share_one_data_fifo);
	UT_RUN(test_resource_x_remote_s_status_is_pending_then_exact_ready);
	UT_RUN(test_resource_x_remote_s_pending_can_cancel_without_send);
	UT_RUN(test_resource_x_nonrequester_s_status_self_master_loopback_is_retained);
	UT_RUN(test_normal_stop_queue_observation_follows_real_handoff);
	UT_RUN(test_normal_stop_inactive_and_malformed_ring_are_not_hidden);
	UT_RUN(test_normal_stop_full_and_bad_frames_remain_debt);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
