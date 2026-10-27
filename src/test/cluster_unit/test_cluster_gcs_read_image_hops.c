/* Exact R4 requester lifecycle and reply RX with controlled dispatch boundaries.
 * Slot storage is a private fixture projection, not a wire/authority model. */
#include "postgres.h"

#include <setjmp.h>
#include <unistd.h>
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_resource_x_identity.h"
#include "cluster/cluster_sf_dep.h"
#include "port/pg_crc32c.h"
#include "storage/backendid.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND 2
#define CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE 0
#define CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR 1
typedef struct ClusterGcsBlockOutstandingSlot {
	bool in_use, reply_received, stale, reply_sf_dep_valid, reply_undo_trailer_valid;
	bool first_fill_revoked, direct_target_prepared;
	uint8 reply_domain, transition_id, reply_sf_flags;
	uint8 expected_reply_status;
	uint64 request_id, request_epoch;
	uint32 direct_generation, direct_arm_id, direct_target_lkey;
	int32 expected_master_node, master_node, direct_expected_peer;
	ClusterGcsBlockDirectState direct_state;
	ClusterGcsBlockDirectTargetKind direct_target_kind;
	ClusterGcsBlockDirectAbortReason direct_abort_reason;
	BufferDesc *direct_target_buf;
	void *direct_target_addr;
	bool expected_current_mx_key_valid, expected_current_mx_proof_valid;
	ClusterCurrentMxKey expected_current_mx_key;
	ClusterCurrentMxProofForwardV2 expected_current_mx_proof;
	BufferTag tag;
	GcsBlockReplyHeader reply_header;
	char reply_block_data[GCS_BLOCK_DATA_SIZE];
	ClusterSfDepVec reply_sf_dep_vec;
	uint64 reply_undo_tt_generation, reply_undo_authority_scn;
	ConditionVariable reply_cv;
	struct {
		bool armed, delivery_closed;
	} first_fill;
} ClusterGcsBlockOutstandingSlot;
typedef struct ClusterGcsBlockBackendBlock {
	LWLockPadded lock;
	ClusterGcsBlockOutstandingSlot slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND];
	uint64 next_request_id;
	ConditionVariable capacity_cv;
} ClusterGcsBlockBackendBlock;

static ClusterGcsBlockBackendBlock blocks[4], *gcs_block_backend_blocks = blocks;
static struct { pg_atomic_uint64 stale_reply_drop_count; } counts, *ClusterGcsBlock = &counts;
int cluster_node_id = 2, MaxBackends = 4;
bool cluster_shared_config = false, cluster_trace_normal_stop = false;
bool cluster_smart_fusion = false;
volatile uint32 CritSectionCount = 0;
BackendId MyBackendId = 0;
BackendType MyBackendType = B_LMS_WORKER;
PGPROC *MyProc = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
static int locks, signals, broadcasts, event_count, injected_log_errors;
static bool fail_log;
static int lost_failclosed;
void cluster_sf_dep_note_lost_failclosed(void) { lost_failclosed++; }
static char logs[16384], message[4096];

bool errstart(int level, const char *domain pg_attribute_unused()) { return level == LOG; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int
errmsg_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);
	return 0;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	if (locks != 0)
		abort();
	if (fail_log && injected_log_errors++ == 0)
		siglongjmp(*PG_exception_stack, 1);
	event_count++;
	strncat(logs, message, sizeof(logs) - strlen(logs) - 2);
	strncat(logs, "\n", sizeof(logs) - strlen(logs) - 1);
}
void ExceptionalCondition(const char *c pg_attribute_unused(), const char *f pg_attribute_unused(),
						  int line pg_attribute_unused()) { abort(); }
TimestampTz GetCurrentTimestamp(void) { return 12500; }
static uint64 gcs_block_pcm_x_monotonic_us(void) pg_attribute_unused();
static uint64 gcs_block_pcm_x_monotonic_us(void) { return 22200; }
bool LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{ locks++; return true; }
void LWLockRelease(LWLock *lock pg_attribute_unused()) { locks--; }
void ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused()) { signals++; }
void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	broadcasts++;
}

/* Private storage mapping; no PGPROC allocation or incarnation is simulated. */
static ClusterGcsBlockBackendBlock *
gcs_block_my_block(void)
{
	if (MyBackendId <= 0 || MyBackendId > MaxBackends)
		abort();
	return &blocks[MyBackendId - 1];
}

static bool gcs_block_try_resource_x_frame(const ClusterICEnvelope *e pg_attribute_unused(),
										 const void *p pg_attribute_unused()) { return false; }
static bool gcs_block_try_land_ctrc_reply(const ClusterICEnvelope *e pg_attribute_unused(),
										const void *p pg_attribute_unused()) { return false; }
static bool gcs_block_try_land_current_mx_reply(const ClusterICEnvelope *e pg_attribute_unused(),
											  const void *p pg_attribute_unused()) { return false; }
bool
cluster_cr_server_r4_land_foreign_undo(const ClusterICEnvelope *e pg_attribute_unused(),
									 const GcsBlockReplyHeader *h pg_attribute_unused(),
									 const char data[BLCKSZ] pg_attribute_unused(),
									 const ClusterGcsUndoAuthTrailer *t pg_attribute_unused())
{ return false; }

#include "test_cluster_gcs_read_image_hops.inc"

#ifndef GCS_R4_REQUESTER_LIFECYCLE_PRESENT
#error "R4 requester lifecycle extraction is required"
#endif

static struct { GcsBlockReplyHeader hdr; char data[GCS_BLOCK_DATA_SIZE]; } payload;
static ClusterICEnvelope envelope;

static void
reset_fixture(bool trace)
{
	ClusterGcsBlockOutstandingSlot *slot;
	memset(blocks, 0, sizeof(blocks));
	memset(&envelope, 0, sizeof(envelope));
	memset(&payload, 0, sizeof(payload));
	memset(&counts, 0, sizeof(counts));
	slot = &blocks[1].slots[0];
	slot->in_use = true;
	slot->request_id = 51;
	slot->request_epoch = 70;
	slot->expected_master_node = 0;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
	slot->transition_id = PCM_TRANS_N_TO_S;
	slot->tag.spcOid = 1663; slot->tag.dbOid = 5; slot->tag.relNumber = 16388;
	slot->tag.forkNum = MAIN_FORKNUM; slot->tag.blockNum = 137;
	payload.hdr.request_id = 51; payload.hdr.epoch = 71;
	payload.hdr.sender_node = 1; payload.hdr.requester_backend_id = 2;
	payload.hdr.transition_id = PCM_TRANS_N_TO_S;
	payload.hdr.status = GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	payload.hdr.page_lsn = 123456; payload.hdr.checksum = 321;
	GcsBlockReplyHeaderSetForwardingMasterNode(&payload.hdr, 0);
	payload.data[0] = 42;
	envelope.source_node_id = 1; envelope.dest_node_id = 2; envelope.epoch = 71;
	locks = signals = broadcasts = event_count = injected_log_errors = 0;
	fail_log = false;
	cluster_smart_fusion = false;
	lost_failclosed = 0;
	envelope.payload_length = sizeof(payload);
	CritSectionCount = 0;
	cluster_trace_normal_stop = trace;
	logs[0] = '\0';
}
static void receive(void) { cluster_gcs_handle_block_reply_envelope(&envelope, &payload); }

static void
r4_tx_fixture(void)
{
	ClusterTxResolution resolution = { 0 };

	reset_fixture(false);
	blocks[1].slots[0].reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
	blocks[1].slots[0].expected_master_node = 1;
	payload.hdr.epoch = 70;
	payload.hdr.status = GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(&payload.hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	resolution.locator_echo.uba.raw[0] = UINT64CONST(0x0102030405060708);
	resolution.locator_echo.uba.raw[1] = UINT64CONST(0x1112131415161718);
	resolution.locator_echo.xid = 798;
	resolution.locator_echo.tt_wrap = 7;
	resolution.locator_echo.itl_kind = ITL_FLAG_ACTIVE;
	resolution.locator_echo.itl_slot_index = 3;
	resolution.top_xid = 700;
	resolution.outcome = CLUSTER_TX_COMMITTED;
	resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	resolution.commit_scn = 9001;
	resolution.authority.origin_epoch = 70;
	resolution.authority.tt_generation = 77;
	resolution.authority.authority_scn = 9002;
	UT_ASSERT(ClusterR4TxVerdictPageEncode((uint8 *)payload.data, &resolution));
	/* Independent CRC32C vector for this entire canonical page. */
	payload.hdr.checksum = UINT32_C(0xd3c893eb);
	envelope.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	envelope.epoch = 70;
}

UT_TEST(r4_tx_exact_reply_publishes_once_without_rewriting_a_consumer)
{
	ClusterGcsBlockBackendBlock before[4];
	ClusterGcsBlockOutstandingSlot *slot;

	r4_tx_fixture();
	slot = &blocks[1].slots[0];
	slot->reply_sf_dep_valid = slot->reply_undo_trailer_valid = true;
	slot->reply_sf_flags = 255;
	slot->reply_sf_dep_vec.required[1] = 55;
	slot->reply_undo_tt_generation = 66;
	slot->reply_undo_authority_scn = 77;
	receive();
	UT_ASSERT(slot->reply_received && locks == 0 && signals == 1);
	UT_ASSERT_EQ(memcmp(&slot->reply_header, &payload.hdr, sizeof(payload.hdr)), 0);
	UT_ASSERT_EQ(memcmp(slot->reply_block_data, payload.data, BLCKSZ), 0);
	UT_ASSERT(!slot->reply_sf_dep_valid && !slot->reply_undo_trailer_valid);
	UT_ASSERT_EQ(slot->reply_sf_flags, 0);
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&slot->reply_sf_dep_vec));
	UT_ASSERT_EQ(slot->reply_undo_tt_generation + slot->reply_undo_authority_scn, 0);
	memcpy(before, blocks, sizeof(blocks));
	/* A second checksum-valid terminal result cannot tear the first reply. */
	payload.data[8] = CLUSTER_TX_ABORTED;
	memset(payload.data + 40, 0, 8);
	payload.hdr.checksum = UINT32_C(0x35d8e802);
	receive();
	UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
	UT_ASSERT_EQ(signals, 1);
	UT_ASSERT_EQ(locks, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&counts.stale_reply_drop_count), 1);
}

UT_TEST(r4_tx_identity_and_shape_refuse_before_any_slot_mutation)
{
	const int drops[] = { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0 };
	ClusterGcsBlockBackendBlock before[4];

	for (int fault = 0; fault < lengthof(drops); fault++) {
		r4_tx_fixture();
		switch (fault) {
		case 0:
			payload.hdr.epoch = 69;
			break;
		case 1:
			payload.hdr.epoch = 71;
			break;
		case 2:
			envelope.source_node_id = 3;
			break;
		case 3:
			envelope.dest_node_id = 3;
			break;
		case 4:
			envelope.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
			break;
		case 5:
			payload.hdr.sender_node = 0;
			break;
		case 6:
			payload.hdr.transition_id = PCM_TRANS_N_TO_X;
			break;
		case 7:
			payload.hdr.checksum ^= 1;
			break;
		case 8:
			envelope.payload_length--;
			break;
		case 9:
			payload.hdr.requester_backend_id = 0;
			break;
		case 10:
			payload.hdr.reserved_0[0] = 1;
			break;
		case 11:
			GcsBlockReplyHeaderSetForwardingMasterNode(&payload.hdr, 0);
			break;
		case 12:
			payload.hdr.page_lsn = 0;
			break;
		case 13:
			payload.hdr.request_id = 999;
			break;
		}
		memcpy(before, blocks, sizeof(blocks));
		receive();
		UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
		UT_ASSERT_EQ(signals + locks, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&counts.stale_reply_drop_count), drops[fault]);
	}
	r4_tx_fixture();
	receive();
	UT_ASSERT(blocks[1].slots[0].reply_received && signals == 1 && locks == 0);
}

UT_TEST(r4_tx_late_reply_cannot_populate_a_reused_owner_slot)
{
	for (int index = 0; index < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; index++) {
		ClusterGcsBlockBackendBlock before[4];
		ClusterGcsBlockOutstandingSlot seed;
		ClusterGcsBlockOutstandingSlot *slot;

		r4_tx_fixture();
		seed = blocks[1].slots[0];
		/* Retirement/reuse is a fixture input, not the real slot allocator. */
		blocks[1].slots[0].in_use = false;
		blocks[1].slots[index] = seed;
		slot = &blocks[1].slots[index];
		slot->request_id = 52;
		blocks[1].slots[1 - index] = seed;
		blocks[1].slots[1 - index].request_id = 90;
		blocks[2].slots[0] = seed;
		memcpy(before, blocks, sizeof(blocks));
		receive(); /* The old request51 belongs to neither live owner here. */
		UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
		UT_ASSERT_EQ(signals + locks, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&counts.stale_reply_drop_count), 0);
		payload.hdr.request_id = 52;
		receive();
		UT_ASSERT(slot->reply_received && signals == 1 && locks == 0);
		UT_ASSERT_EQ(slot->reply_header.request_id, 52);
		UT_ASSERT_EQ(memcmp(slot->reply_block_data, payload.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(&blocks[1].slots[1 - index], &before[1].slots[1 - index], sizeof(seed)),
					 0);
		UT_ASSERT_EQ(memcmp(&blocks[2], &before[2], sizeof(blocks[2])), 0);
	}
}

UT_TEST(r4_tx_reply_preserves_foreign_domains_and_direct_owners)
{
	ClusterGcsBlockBackendBlock before[4];

	for (int owner = 0; owner < 5; owner++) {
		ClusterGcsBlockOutstandingSlot *slot;

		r4_tx_fixture();
		slot = &blocks[1].slots[0];
		if (owner == 0)
			slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
		else if (owner == 1)
			slot->reply_received = true;
		else
			slot->direct_state = owner == 2	  ? GCS_BLOCK_DIRECT_ARMED
								 : owner == 3 ? GCS_BLOCK_DIRECT_LANDED
											  : GCS_BLOCK_DIRECT_ABORTING;
		slot->reply_block_data[0] = 99;
		memcpy(before, blocks, sizeof(blocks));
		receive();
		UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
		UT_ASSERT_EQ(signals + locks, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&counts.stale_reply_drop_count), 1);
	}
	r4_tx_fixture();
	receive();
	UT_ASSERT(blocks[1].slots[0].reply_received && signals == 1 && locks == 0);
}

UT_TEST(default_off_preserves_original_publish_and_signal)
{
	reset_fixture(false); receive();
	UT_ASSERT(blocks[1].slots[0].reply_received && signals == 1 && locks == 0);
	UT_ASSERT_EQ(blocks[1].slots[0].reply_block_data[0], 42);
	UT_ASSERT_EQ(event_count, 0);
}
UT_TEST(accepted_reply_logs_original_identity_after_unlock)
{
	reset_fixture(true); receive();
	UT_ASSERT(blocks[1].slots[0].reply_received && signals == 1 && locks == 0);
	UT_ASSERT(strstr(logs, "event=reply.published") != NULL);
	UT_ASSERT(strstr(logs, "request_id=51") != NULL);
	UT_ASSERT(strstr(logs, "spc=1663 db=5 rel=16388 fork=0 block=137") != NULL);
	UT_ASSERT(strstr(logs, "slot_known=1") != NULL);
	UT_ASSERT(strstr(logs, "signal_called=1") != NULL);
	UT_ASSERT(strstr(logs, "authoritative=0") != NULL);
	UT_ASSERT(strstr(logs, "process_incarnation_known=0") != NULL);
}
UT_TEST(epoch_refusal_keeps_result_bytes_unchanged)
{
	reset_fixture(true); payload.hdr.epoch = 69; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "predicate=reply.epoch_minimum") != NULL);
	UT_ASSERT(strstr(logs, "expected_epoch=70 reply_epoch=69") != NULL);
}
UT_TEST(source_chain_refusal_precedes_epoch_and_transition)
{
	reset_fixture(true); GcsBlockReplyHeaderSetForwardingMasterNode(&payload.hdr, 3);
	payload.hdr.epoch = 69; payload.hdr.transition_id = PCM_TRANS_N_TO_X; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "predicate=reply.authorized_chain") != NULL);
}
UT_TEST(transition_refusal_is_diagnosed_without_accepting_reply)
{
	reset_fixture(true); payload.hdr.transition_id = PCM_TRANS_N_TO_X; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "predicate=reply.transition_exact") != NULL);
}
UT_TEST(domain_refusal_does_not_mutate_foreign_reply_domain)
{
	reset_fixture(true); blocks[1].slots[0].reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
	receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "event=reply.domain_refused") != NULL);
	UT_ASSERT(strstr(logs, "slot_domain=1 incoming_domain_known=1 incoming_domain=0") != NULL);
}
UT_TEST(duplicate_reply_remains_first_reply_wins)
{
	reset_fixture(true); blocks[1].slots[0].reply_received = true;
	blocks[1].slots[0].reply_block_data[0] = 9; receive();
	UT_ASSERT_EQ(blocks[1].slots[0].reply_block_data[0], 9);
	UT_ASSERT_EQ(signals, 0);
	UT_ASSERT(strstr(logs, "event=reply.duplicate") != NULL);
}
UT_TEST(direct_active_keeps_original_refusal)
{
	reset_fixture(true); blocks[1].slots[0].direct_state = GCS_BLOCK_DIRECT_ARMED; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "event=reply.direct_active") != NULL);
}
UT_TEST(authenticated_envelope_mismatch_does_not_touch_slot)
{
	reset_fixture(true); envelope.source_node_id = 3; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "event=reply.envelope_refused") != NULL);
}
UT_TEST(bad_backend_never_indexes_or_guesses_tag)
{
	reset_fixture(true); payload.hdr.requester_backend_id = 0; receive();
	UT_ASSERT_EQ(signals, 0);
	UT_ASSERT(strstr(logs, "event=reply.backend_refused") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=0") != NULL);
}
UT_TEST(late_reply_does_not_borrow_a_nearby_slot_tag)
{
	reset_fixture(true); payload.hdr.request_id = 52; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "event=reply.no_slot") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=0") != NULL);
}
UT_TEST(undecodable_payload_reports_original_decoder_refusal)
{
	reset_fixture(true); payload.hdr.status = 255; receive();
	UT_ASSERT(!blocks[1].slots[0].reply_received && signals == 0);
	UT_ASSERT(strstr(logs, "event=reply.decode_refused predicate=reply.v1.status_legacy") != NULL);
	UT_ASSERT(strstr(logs, "header_validated=0") != NULL);
}
UT_TEST(log_error_occurs_after_unlock_and_original_signal)
{
	sigjmp_buf trap;
	bool caught = false;
	reset_fixture(true); fail_log = true;
	PG_exception_stack = &trap;
	if (sigsetjmp(trap, 0) == 0) receive(); else caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught && locks == 0 && signals == 1 && blocks[1].slots[0].reply_received);
}
UT_TEST(critical_gap_is_disclosed_without_changing_reply_publish)
{
	reset_fixture(true); CritSectionCount = 1; receive();
	UT_ASSERT(blocks[1].slots[0].reply_received && signals == 1 && event_count == 0);
	reset_fixture(true); receive();
	UT_ASSERT(strstr(logs, "trace_critical_skipped=1") != NULL);
}
UT_TEST(holder_view_logs_only_copied_values_not_a_transport_receipt)
{
#ifdef GCS_READ_IMAGE_HOP_OBSERVER_PRESENT
	GcsReadImageHopTrace view;
	reset_fixture(true); memset(&view, 0, sizeof(view));
	view.forward_known = view.reply_known = view.tag_known = true;
	view.forward.request_id = 51; view.forward.epoch = 71;
	view.forward.tag = blocks[1].slots[0].tag; view.tag = view.forward.tag;
	view.forward.original_requester_node = 2; view.forward.requester_backend_id = 2;
	view.forward.master_node = 0; view.reply = payload.hdr;
	view.copy_known = true; view.copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	view.ship_ok = true; view.page_scn_known = true; view.page_scn = 900;
	view.send_known = true; view.send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	gcs_block_read_image_hop_trace(&view, "holder.reply.dispatched", "none");
	UT_ASSERT(strstr(logs, "forward_known=1") != NULL);
	UT_ASSERT(strstr(logs, "page_scn_known=1 page_scn=900") != NULL);
	UT_ASSERT(strstr(logs, "send_result_known=1") != NULL);
	UT_ASSERT(strstr(logs, "transport_receipt=0") != NULL);
	UT_ASSERT(strstr(logs, "reply_header_scope=prepared") != NULL);
	UT_ASSERT(strstr(logs, "direct_wire_reply_known=0") != NULL);
#else
	UT_ASSERT(false);
#endif
}

/* These two safety contracts inspect the production holder boundary.  They
 * do not emulate holder service or claim a live pin/transport proof. */
static char *
read_holder_source(void)
{
	FILE *file = fopen(GCS_BLOCK_SOURCE_PATH, "rb");
	long length;
	char *text;
	if (file == NULL) return NULL;
	if (fseek(file, 0, SEEK_END) != 0) { fclose(file); return NULL; }
	length = ftell(file);
	if (length < 0 || fseek(file, 0, SEEK_SET) != 0) { fclose(file); return NULL; }
	text = malloc((size_t)length + 1);
	if (text == NULL) { fclose(file); return NULL; }
	if (fread(text, 1, (size_t)length, file) != (size_t)length) { free(text); fclose(file); return NULL; }
	text[length] = '\0'; fclose(file); return text;
}
UT_TEST(holder_observation_waits_for_image_resource_handoff)
{
	char *source = read_holder_source();
	const char *holder, *copy, *first_log, *direct_send, *free_buf;
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) return;
	holder = strstr(source, "\ncluster_gcs_handle_block_forward_envelope(");
	copy = holder != NULL ? strstr(holder, "holder_ship_ok = gcs_block_get_ship_image(") : NULL;
	first_log = copy != NULL ? strstr(copy, "gcs_block_read_image_hop_trace(") : NULL;
	direct_send = copy != NULL ? strstr(copy, "gcs_block_try_send_direct_reply(") : NULL;
	free_buf = direct_send != NULL ? strstr(direct_send, "pfree(buf);") : NULL;
	UT_ASSERT(copy && first_log && direct_send && free_buf);
	if (copy && first_log && direct_send && free_buf)
		UT_ASSERT(copy < direct_send && direct_send < free_buf && free_buf < first_log);
	free(source);
}
UT_TEST(holder_final_event_follows_all_original_send_cleanup)
{
	char *source = read_holder_source();
	const char *holder, *ordinary_send, *free_buf, *event;
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) return;
	holder = strstr(source, "\ncluster_gcs_handle_block_forward_envelope(");
	ordinary_send = holder != NULL ? strstr(holder, "cluster_ic_rdma_send_envelope_sge(") : NULL;
	free_buf = ordinary_send != NULL ? strstr(ordinary_send, "pfree(buf);") : NULL;
	event = free_buf != NULL ? strstr(free_buf, "gcs_block_read_image_hop_trace(") : NULL;
	UT_ASSERT(ordinary_send && free_buf && event);
	if (ordinary_send && free_buf && event)
		UT_ASSERT(ordinary_send < free_buf && free_buf < event);
	free(source);
}

static const GcsBlockReplyHeader *decoded_header;
static const char *decoded_data;
static const ClusterGcsUndoAuthTrailer *decoded_undo;
static bool decoded_sf_valid;
static uint8 decoded_sf_flags;
static ClusterSfDepVec decoded_sf_vec;
static union {
	GcsBlockReplyHeaderV2 header;
	char bytes[sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE
			   + sizeof(ClusterGcsUndoAuthTrailer)];
} v2_payload;

static bool
decode(const ClusterICEnvelope *env, const void *data)
{
	decoded_header = (const GcsBlockReplyHeader *)1;
	decoded_data = (const char *)1;
	decoded_undo = (const ClusterGcsUndoAuthTrailer *)1;
	decoded_sf_valid = true;
	decoded_sf_flags = 255;
	memset(&decoded_sf_vec, 1, sizeof(decoded_sf_vec));
	return gcs_block_decode_reply_payload(env, data, &decoded_header, &decoded_data,
										 &decoded_sf_valid, &decoded_sf_flags,
										 &decoded_sf_vec, &decoded_undo);
}
static bool
outputs_reset(void)
{
	return decoded_header == NULL && decoded_data == NULL && decoded_undo == NULL
		&& !decoded_sf_valid && decoded_sf_flags == 0
		&& cluster_sf_dep_vec_is_empty(&decoded_sf_vec);
}
static void
v2_fixture(void)
{
	reset_fixture(true);
	memset(&v2_payload, 0, sizeof(v2_payload));
	v2_payload.header.v1 = payload.hdr;
	envelope.payload_length = sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE;
	cluster_smart_fusion = true;
}

UT_TEST(null_envelope_resets_outputs_before_diagnostic)
{
	reset_fixture(true);
	UT_ASSERT(!decode(NULL, &payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.envelope_present") != NULL);
	UT_ASSERT(strstr(logs, "envelope_known=0") != NULL);
	UT_ASSERT(strstr(logs, "payload_header_known=0") != NULL);
}
UT_TEST(null_payload_does_not_borrow_header_or_tag)
{
	reset_fixture(true);
	UT_ASSERT(!decode(&envelope, NULL) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.payload_present") != NULL);
	UT_ASSERT(strstr(logs, "payload_header_known=0") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=0") != NULL);
}
UT_TEST(short_declared_payload_does_not_read_header)
{
	char one_byte = 0;
	reset_fixture(true); envelope.payload_length = sizeof(one_byte);
	UT_ASSERT(!decode(&envelope, &one_byte) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.payload_size") != NULL);
	UT_ASSERT(strstr(logs, "payload_length=1") != NULL);
	UT_ASSERT(strstr(logs, "payload_header_known=0") != NULL);
}
UT_TEST(unsupported_size_header_is_unvalidated_only)
{
	reset_fixture(true); envelope.payload_length = sizeof(payload) - 1;
	UT_ASSERT(!decode(&envelope, &payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.payload_size") != NULL);
	UT_ASSERT(strstr(logs, "payload_header_known=1 header_validated=0") != NULL);
	UT_ASSERT(strstr(logs, "request_id=51") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=0") != NULL);
	UT_ASSERT(strstr(logs, "authoritative=0 transport_receipt=0") != NULL);
}
UT_TEST(undo_shape_requires_undo_status)
{
	v2_fixture();
	envelope.payload_length = sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE
		+ sizeof(ClusterGcsUndoAuthTrailer);
	UT_ASSERT(!decode(&envelope, &v2_payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.undo.status_trailer") != NULL);
	UT_ASSERT_EQ(lost_failclosed, 0);
}
UT_TEST(v2_status_refusal_precedes_disabled_fusion)
{
	v2_fixture(); v2_payload.header.v1.status = 255; cluster_smart_fusion = false;
	UT_ASSERT(!decode(&envelope, &v2_payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.v2.status_legacy") != NULL);
	UT_ASSERT_EQ(lost_failclosed, 1);
}
UT_TEST(v2_disabled_fusion_precedes_vector_validation)
{
	v2_fixture(); cluster_smart_fusion = false; v2_payload.header.reserved_0[0] = 1;
	UT_ASSERT(!decode(&envelope, &v2_payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.v2.smart_fusion_enabled") != NULL);
	UT_ASSERT_EQ(lost_failclosed, 1);
}
UT_TEST(v2_invalid_vector_keeps_original_counter_and_refusal)
{
	v2_fixture(); v2_payload.header.reserved_0[0] = 1;
	UT_ASSERT(!decode(&envelope, &v2_payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "predicate=reply.v2.dependency_vector") != NULL);
	UT_ASSERT_EQ(lost_failclosed, 1);
}
UT_TEST(valid_v2_keeps_real_vector_and_is_quiet)
{
	v2_fixture();
	v2_payload.header.sf_flags = GCS_BLOCK_REPLY_SF_HAS_DEP_VEC
		| GCS_BLOCK_REPLY_SF_EARLY_TRANSFER;
	v2_payload.header.sf_dep_count = 1;
	v2_payload.header.sf_dep[0].origin_node = 1;
	v2_payload.header.sf_dep[0].required_redo_lsn = 1234;
	UT_ASSERT(decode(&envelope, &v2_payload));
	UT_ASSERT(decoded_header == &v2_payload.header.v1);
	UT_ASSERT(decoded_data == v2_payload.bytes + sizeof(GcsBlockReplyHeaderV2));
	UT_ASSERT(decoded_sf_valid && decoded_sf_vec.required[1] == 1234);
	UT_ASSERT_EQ(event_count, 0);
	UT_ASSERT_EQ(lost_failclosed, 0);
}
UT_TEST(valid_undo_trailer_keeps_exact_original_offsets)
{
	v2_fixture();
	v2_payload.header.v1.status = GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT;
	envelope.payload_length = sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE
		+ sizeof(ClusterGcsUndoAuthTrailer);
	UT_ASSERT(decode(&envelope, &v2_payload));
	UT_ASSERT(decoded_data == v2_payload.bytes + sizeof(GcsBlockReplyHeader));
	UT_ASSERT((const char *)decoded_undo == decoded_data + GCS_BLOCK_DATA_SIZE);
	UT_ASSERT_EQ(event_count, 0);
}
UT_TEST(default_off_invalid_decoder_has_no_diagnostic)
{
	reset_fixture(false); payload.hdr.status = 255;
	UT_ASSERT(!decode(&envelope, &payload) && outputs_reset());
	UT_ASSERT_EQ(event_count, 0);
}
UT_TEST(decoder_critical_gap_is_disclosed_by_next_safe_refusal)
{
	reset_fixture(true); payload.hdr.status = 255; CritSectionCount = 1;
	UT_ASSERT(!decode(&envelope, &payload) && outputs_reset());
	UT_ASSERT_EQ(event_count, 0);
	CritSectionCount = 0;
	UT_ASSERT(!decode(&envelope, &payload) && outputs_reset());
	UT_ASSERT(strstr(logs, "trace_critical_skipped=1") != NULL);
}
UT_TEST(decoder_log_error_follows_original_reset_and_counter)
{
	sigjmp_buf trap;
	bool caught = false;
	v2_fixture(); v2_payload.header.reserved_0[0] = 1; fail_log = true;
	PG_exception_stack = &trap;
	if (sigsetjmp(trap, 0) == 0)
		(void)decode(&envelope, &v2_payload);
	else
		caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught && outputs_reset() && lost_failclosed == 1 && locks == 0);
}
UT_TEST(forward_rejection_capture_is_bounded_and_non_authoritative)
{
#ifdef GCS_READ_IMAGE_INGRESS_REJECT_PRESENT
	GcsBlockForwardPayload forward;
	reset_fixture(true); memset(&forward, 0, sizeof(forward));
	forward.request_id = 51; forward.epoch = 71; forward.tag = blocks[1].slots[0].tag;
	forward.master_node = 0; forward.original_requester_node = 2;
	forward.requester_backend_id = 2; forward.transition_id = 255;
	envelope.payload_length = sizeof(forward);
	gcs_block_read_image_ingress_reject_trace(&envelope, &forward, true,
											"holder.transition_refused", "forward.transition_range");
	UT_ASSERT(strstr(logs, "event=holder.transition_refused") != NULL);
	UT_ASSERT(strstr(logs, "request_id=51") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=1") != NULL);
	UT_ASSERT(strstr(logs, "header_validated=0") != NULL);
	UT_ASSERT(strstr(logs, "authoritative=0 transport_receipt=0") != NULL);
	logs[0] = '\0'; envelope.payload_length = 1;
	gcs_block_read_image_ingress_reject_trace(&envelope, (const void *)1, true,
											"holder.shape_refused", "forward.payload_size");
	UT_ASSERT(strstr(logs, "payload_header_known=0") != NULL);
	UT_ASSERT(strstr(logs, "tag_known=0") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(forward_refusal_logging_stays_inside_original_refusal_branches)
{
	char *source = read_holder_source();
	const char *holder, *shape, *transition, *shape_event, *transition_event, *dispatch;
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) return;
	holder = strstr(source, "\ncluster_gcs_handle_block_forward_envelope(");
	shape = holder ? strstr(holder, "if (env == NULL || payload == NULL") : NULL;
	transition = shape ? strstr(shape, "if (fwd->transition_id < PCM_TRANS_N_TO_S") : NULL;
	shape_event = shape ? strstr(shape, "\"holder.shape_refused\"") : NULL;
	transition_event = transition ? strstr(transition, "\"holder.transition_refused\"") : NULL;
	dispatch = transition ? strstr(transition, "if (GcsBlockForwardPayloadIsCrRequest(fwd))") : NULL;
	UT_ASSERT(shape && transition && shape_event && transition_event && dispatch);
	if (shape && transition && shape_event && transition_event && dispatch)
		UT_ASSERT(shape < shape_event && shape_event < transition
				  && transition < transition_event && transition_event < dispatch);
	free(source);
}

/* Keep the canonical RX input, but allocate every owner through the real body. */
static BufferTag
r4_requester_fixture(void)
{
	BufferTag tag;
	int i;

	r4_tx_fixture();
	tag = blocks[1].slots[0].tag;
	memset(blocks, 0, sizeof(blocks));
	for (i = 0; i < MaxBackends; i++)
		blocks[i].next_request_id = 51;
	MyBackendId = 2;
	return tag;
}

UT_TEST(r4_requester_reserve_land_release_and_reuse_clear_stale_state)
{
	BufferTag tag = r4_requester_fixture();
	ClusterGcsBlockOutstandingSlot *slot = &blocks[1].slots[0], *next;
	const ClusterGcsBlockOutstandingSlot zero = { 0 };
	uint64 request_id = 0;

	/* Dirty a free slot only; no live direct or first-fill owner is simulated. */
	slot->reply_received = slot->stale = slot->first_fill_revoked = true;
	slot->reply_sf_dep_valid = slot->reply_undo_trailer_valid = true;
	slot->reply_sf_flags = 255;
	slot->reply_sf_dep_vec.required[1] = 55;
	slot->reply_undo_tt_generation = 66;
	slot->reply_undo_authority_scn = 77;
	slot->expected_reply_status = GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT;
	slot->expected_current_mx_key_valid = slot->expected_current_mx_proof_valid = true;
	memset(&slot->expected_current_mx_key, 0x55, sizeof(slot->expected_current_mx_key));
	memset(&slot->expected_current_mx_proof, 0x66, sizeof(slot->expected_current_mx_proof));
	memset(&slot->reply_header, 0xa5, sizeof(slot->reply_header));
	memset(slot->reply_block_data, 0x5a, sizeof(slot->reply_block_data));
	slot->direct_generation = 9;
	slot = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	UT_ASSERT_NOT_NULL(slot);
	if (slot == NULL)
		return;
	UT_ASSERT(slot == &blocks[1].slots[0] && slot->in_use);
	UT_ASSERT_EQ(request_id, UINT64CONST(0x0200010000000033));
	UT_ASSERT_EQ(slot->request_id, request_id);
	UT_ASSERT_EQ(blocks[1].next_request_id, 52);
	UT_ASSERT_EQ(slot->request_epoch, 70);
	UT_ASSERT_EQ(slot->master_node, 1);
	UT_ASSERT_EQ(slot->expected_master_node, 1);
	UT_ASSERT_EQ(slot->transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(slot->reply_domain, CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(memcmp(&slot->tag, &tag, sizeof(tag)), 0);
	UT_ASSERT(!slot->reply_received && !slot->stale && !slot->first_fill_revoked);
	UT_ASSERT(!slot->reply_sf_dep_valid && !slot->reply_undo_trailer_valid);
	UT_ASSERT_EQ(slot->reply_sf_flags, 0);
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&slot->reply_sf_dep_vec));
	UT_ASSERT_EQ(slot->reply_undo_tt_generation | slot->reply_undo_authority_scn, 0);
	UT_ASSERT_EQ(slot->expected_reply_status, 0);
	UT_ASSERT(!slot->expected_current_mx_key_valid && !slot->expected_current_mx_proof_valid);
	UT_ASSERT_EQ(memcmp(&slot->expected_current_mx_key, &zero.expected_current_mx_key,
						sizeof(slot->expected_current_mx_key)),
				 0);
	UT_ASSERT_EQ(memcmp(&slot->expected_current_mx_proof, &zero.expected_current_mx_proof,
						sizeof(slot->expected_current_mx_proof)),
				 0);
	UT_ASSERT_EQ(memcmp(&slot->reply_header, &zero.reply_header, sizeof(slot->reply_header)), 0);
	UT_ASSERT_EQ(memcmp(slot->reply_block_data, zero.reply_block_data, BLCKSZ), 0);
	UT_ASSERT_EQ(slot->direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT_EQ(slot->direct_generation, 9);
	UT_ASSERT_EQ(locks | signals | broadcasts, 0);

	payload.hdr.request_id = request_id;
	receive();
	UT_ASSERT(slot->reply_received && signals == 1 && locks == 0);
	UT_ASSERT_EQ(memcmp(slot->reply_block_data, payload.data, BLCKSZ), 0);
	gcs_block_release_slot(slot);
	UT_ASSERT(!slot->in_use && !slot->reply_received && locks == 0 && broadcasts == 1);
	UT_ASSERT_EQ(slot->request_id | slot->request_epoch, 0);
	UT_ASSERT_EQ(slot->reply_domain, CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(slot->transition_id, 0);
	UT_ASSERT_EQ(slot->master_node, -1);
	UT_ASSERT_EQ(slot->expected_master_node, -1);
	UT_ASSERT(!slot->first_fill.armed && !slot->first_fill.delivery_closed);
	/* Retired bytes have no owner; the next reservation erases them. */
	UT_ASSERT_EQ(memcmp(slot->reply_block_data, payload.data, BLCKSZ), 0);
	next = gcs_block_try_reserve_r4_slot(tag, 71, 3, &request_id);
	UT_ASSERT(next == slot);
	if (next == NULL)
		return;
	UT_ASSERT_EQ(request_id, UINT64CONST(0x0200010000000034));
	UT_ASSERT_EQ(next->request_epoch, 71);
	UT_ASSERT_EQ(next->expected_master_node, 3);
	UT_ASSERT(!next->reply_received);
	UT_ASSERT_EQ(memcmp(&next->reply_header, &zero.reply_header, sizeof(next->reply_header)), 0);
	UT_ASSERT_EQ(memcmp(next->reply_block_data, zero.reply_block_data, BLCKSZ), 0);
	UT_ASSERT_EQ(next->direct_generation, 9);
	gcs_block_release_slot(next);
	UT_ASSERT_EQ(broadcasts, 2);
}

UT_TEST(r4_requester_real_reuse_rejects_old_reply_and_preserves_both_peers)
{
	BufferTag tag = r4_requester_fixture();
	ClusterGcsBlockOutstandingSlot *slot, *same_backend, *other_backend, *reused;
	ClusterGcsBlockBackendBlock before[4];
	uint64 request_id, old_request_id;

	slot = gcs_block_try_reserve_r4_slot(tag, 70, 1, &old_request_id);
	same_backend = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	MyBackendId = 3;
	blocks[2].next_request_id = 151;
	other_backend = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	MyBackendId = 2;
	UT_ASSERT(slot != NULL && same_backend != NULL && other_backend != NULL);
	if (slot == NULL || same_backend == NULL || other_backend == NULL)
		return;
	UT_ASSERT_EQ(old_request_id, UINT64CONST(0x0200010000000033));
	UT_ASSERT_EQ(same_backend->request_id, UINT64CONST(0x0200010000000034));
	UT_ASSERT_EQ(other_backend->request_id, UINT64CONST(0x0200020000000097));
	gcs_block_release_slot(slot);
	reused = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	UT_ASSERT(reused == slot);
	if (reused == NULL)
		return;
	UT_ASSERT_EQ(request_id, UINT64CONST(0x0200010000000035));
	memcpy(before, blocks, sizeof(blocks));
	payload.hdr.request_id = old_request_id;
	receive();
	UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
	UT_ASSERT_EQ(signals | locks, 0);
	/* No owner matches: preserve the original silent-drop counter boundary. */
	UT_ASSERT_EQ(pg_atomic_read_u64(&counts.stale_reply_drop_count), 0);
	payload.hdr.request_id = request_id;
	receive();
	UT_ASSERT(reused->reply_received && signals == 1 && locks == 0);
	UT_ASSERT_EQ(memcmp(&reused->reply_header, &payload.hdr, sizeof(payload.hdr)), 0);
	UT_ASSERT_EQ(memcmp(reused->reply_block_data, payload.data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(same_backend, &before[1].slots[1], sizeof(*same_backend)), 0);
	UT_ASSERT_EQ(memcmp(other_backend, &before[2].slots[0], sizeof(*other_backend)), 0);
	gcs_block_release_slot(reused);
	gcs_block_release_slot(same_backend);
	MyBackendId = 3;
	gcs_block_release_slot(other_backend);
	MyBackendId = 2;
}

UT_TEST(r4_requester_full_capacity_does_not_consume_a_sequence)
{
	BufferTag tag = r4_requester_fixture();
	ClusterGcsBlockOutstandingSlot *first, *second, *next;
	ClusterGcsBlockBackendBlock before[4];
	uint64 request_id;

	first = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	second = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	UT_ASSERT(first != NULL && second != NULL);
	if (first == NULL || second == NULL)
		return;
	memcpy(before, blocks, sizeof(blocks));
	request_id = 999;
	UT_ASSERT(gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id) == NULL);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
	UT_ASSERT_EQ(blocks[1].next_request_id, 53);
	UT_ASSERT_EQ(locks | signals | broadcasts, 0);
	gcs_block_release_slot(first);
	UT_ASSERT_EQ(broadcasts, 1);
	next = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	UT_ASSERT(next == first);
	if (next == NULL)
		return;
	UT_ASSERT_EQ(request_id, UINT64CONST(0x0200010000000035));
	UT_ASSERT_EQ(blocks[1].next_request_id, 54);
	UT_ASSERT_EQ(memcmp(second, &before[1].slots[1], sizeof(*second)), 0);
	gcs_block_release_slot(next);
	gcs_block_release_slot(second);
	UT_ASSERT_EQ(broadcasts, 3);
	UT_ASSERT_EQ(locks, 0);
}

UT_TEST(r4_requester_exhausted_sequence_and_invalid_callers_never_alias)
{
	static const struct {
		BackendId backend;
		int32 master;
		bool null_output;
	} invalid[] = { { -1, 1, false },
					{ 0, 1, false },
					{ 5, 1, false },
					{ 2, -1, false },
					{ 2, RESOURCE_X_PROTOCOL_NODE_LIMIT, false },
					{ 2, 1, true } };
	BufferTag tag = r4_requester_fixture();
	ClusterGcsBlockOutstandingSlot *slot;
	ClusterGcsBlockBackendBlock before[4];
	uint64 request_id;
	size_t i;

	blocks[1].next_request_id = GCS_REQID_REQUESTER_SEQ_MASK;
	slot = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
	UT_ASSERT_NOT_NULL(slot);
	if (slot == NULL)
		return;
	UT_ASSERT_EQ(request_id, UINT64CONST(0x020001ffffffffff));
	UT_ASSERT_EQ(blocks[1].next_request_id, GCS_REQID_REQUESTER_SEQ_MASK + 1);
	gcs_block_release_slot(slot);
	memcpy(before, blocks, sizeof(blocks));
	request_id = 999;
	UT_ASSERT(gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id) == NULL);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);

	tag = r4_requester_fixture();
	blocks[1].next_request_id = 0;
	memcpy(before, blocks, sizeof(blocks));
	request_id = 999;
	UT_ASSERT(gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id) == NULL);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);

	tag = r4_requester_fixture();
	memcpy(before, blocks, sizeof(blocks));
	for (i = 0; i < lengthof(invalid); i++) {
		MyBackendId = invalid[i].backend;
		request_id = 999;
		UT_ASSERT(gcs_block_try_reserve_r4_slot(tag, 70, invalid[i].master,
												invalid[i].null_output ? NULL : &request_id)
				  == NULL);
		UT_ASSERT_EQ(request_id, invalid[i].null_output ? 999 : 0);
		UT_ASSERT_EQ(memcmp(before, blocks, sizeof(blocks)), 0);
		UT_ASSERT_EQ(locks | signals | broadcasts, 0);
	}
	MyBackendId = 2;
}

int
main(void)
{
	UT_PLAN(40);
	UT_RUN(default_off_preserves_original_publish_and_signal);
	UT_RUN(accepted_reply_logs_original_identity_after_unlock);
	UT_RUN(epoch_refusal_keeps_result_bytes_unchanged);
	UT_RUN(source_chain_refusal_precedes_epoch_and_transition);
	UT_RUN(transition_refusal_is_diagnosed_without_accepting_reply);
	UT_RUN(domain_refusal_does_not_mutate_foreign_reply_domain);
	UT_RUN(duplicate_reply_remains_first_reply_wins);
	UT_RUN(direct_active_keeps_original_refusal);
	UT_RUN(authenticated_envelope_mismatch_does_not_touch_slot);
	UT_RUN(bad_backend_never_indexes_or_guesses_tag);
	UT_RUN(late_reply_does_not_borrow_a_nearby_slot_tag);
	UT_RUN(undecodable_payload_reports_original_decoder_refusal);
	UT_RUN(log_error_occurs_after_unlock_and_original_signal);
	UT_RUN(critical_gap_is_disclosed_without_changing_reply_publish);
	UT_RUN(holder_view_logs_only_copied_values_not_a_transport_receipt);
	UT_RUN(holder_observation_waits_for_image_resource_handoff);
	UT_RUN(holder_final_event_follows_all_original_send_cleanup);
	UT_RUN(null_envelope_resets_outputs_before_diagnostic);
	UT_RUN(null_payload_does_not_borrow_header_or_tag);
	UT_RUN(short_declared_payload_does_not_read_header);
	UT_RUN(unsupported_size_header_is_unvalidated_only);
	UT_RUN(undo_shape_requires_undo_status);
	UT_RUN(v2_status_refusal_precedes_disabled_fusion);
	UT_RUN(v2_disabled_fusion_precedes_vector_validation);
	UT_RUN(v2_invalid_vector_keeps_original_counter_and_refusal);
	UT_RUN(valid_v2_keeps_real_vector_and_is_quiet);
	UT_RUN(valid_undo_trailer_keeps_exact_original_offsets);
	UT_RUN(default_off_invalid_decoder_has_no_diagnostic);
	UT_RUN(decoder_critical_gap_is_disclosed_by_next_safe_refusal);
	UT_RUN(decoder_log_error_follows_original_reset_and_counter);
	UT_RUN(forward_rejection_capture_is_bounded_and_non_authoritative);
	UT_RUN(forward_refusal_logging_stays_inside_original_refusal_branches);
	UT_RUN(r4_tx_exact_reply_publishes_once_without_rewriting_a_consumer);
	UT_RUN(r4_tx_identity_and_shape_refuse_before_any_slot_mutation);
	UT_RUN(r4_tx_late_reply_cannot_populate_a_reused_owner_slot);
	UT_RUN(r4_tx_reply_preserves_foreign_domains_and_direct_owners);
	UT_RUN(r4_requester_reserve_land_release_and_reuse_clear_stale_state);
	UT_RUN(r4_requester_real_reuse_rejects_old_reply_and_preserves_both_peers);
	UT_RUN(r4_requester_full_capacity_does_not_consume_a_sequence);
	UT_RUN(r4_requester_exhausted_sequence_and_invalid_callers_never_alias);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
