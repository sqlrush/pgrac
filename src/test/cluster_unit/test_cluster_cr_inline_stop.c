/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual inline entry and queued/inline reply construction and handoff.
 * Constructor, transport, checksum, service-seal and scratch allocation are
 * boundaries; this does not prove remote CR correctness or native shutdown. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_write_fence.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 1, cluster_lms_workers = 4, MaxBackends = 128;
int cluster_injection_armed_count;
MemoryContext CurrentMemoryContext, TopMemoryContext;
static MemoryContext CrServeScratchCtx;
static char caller_context, scratch_context;
static bool new_work_allowed, fence_allowed, seal_during_serve;
static int gate_calls, scratch_calls, serve_calls, reply_calls, reset_calls;
static int direct_replies, serve_observations, fence_refusals;
static ClusterLmsCrSlot last_served, last_reply;
static ClusterICSendResult send_result;
static unsigned send_calls, note_calls, checksum_calls, alloc_calls, free_calls;
static uint8 serve_result;
static char reply_bytes[sizeof(GcsBlockReplyHeader) + BLCKSZ + sizeof(ClusterGcsUndoAuthTrailer)];
static uint32 reply_length;
static ClusterICSendResult noted_result;
static bool diag_enabled, diag_throw, send_throw;
static unsigned diag_n, diag_attempts;
static bool expect_queued_cleanup, reuse_slots;
static bool queued_cleanup_checked;
static unsigned queued_ready_mask;
static void *outstanding_buf;
static void check_diagnostic_cleanup(void);
static char diag_message[2048], diag_log[32768];
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
bool
errstart(int level, const char *domain)
{
	if (level == LOG)
		diag_attempts++;
	return diag_enabled && level == LOG;
}
bool
errstart_cold(int level, const char *domain)
{
	return false;
}
int
errmsg_internal(const char *fmt, ...)
{
	va_list args;
	check_diagnostic_cleanup();
	va_start(args, fmt);
	vsnprintf(diag_message, sizeof(diag_message), fmt, args);
	va_end(args);
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	diag_n++;
	if (diag_throw)
		pg_re_throw();
	strncat(diag_log, diag_message, sizeof(diag_log) - strlen(diag_log) - 2);
	strncat(diag_log, "\n", sizeof(diag_log) - strlen(diag_log) - 1);
}

void *
palloc0(Size size)
{
	alloc_calls++;
	outstanding_buf = calloc(1, size);
	return outstanding_buf;
}
void
pfree(void *ptr)
{
	free_calls++;
	free(ptr);
	outstanding_buf = NULL;
}
uint32
cluster_gcs_block_compute_checksum(const char *page)
{
	checksum_calls++;
	return 1234;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 length)
{
	const GcsBlockReplyHeader *hdr = payload;
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(dest, 2);
	UT_ASSERT(length <= sizeof(reply_bytes));
	UT_ASSERT_EQ(alloc_calls, free_calls + 1);
	send_calls++;
	if (send_throw)
		pg_re_throw();
	memcpy(reply_bytes, payload, length);
	reply_length = length;
	reply_calls++;
	last_reply.result_status = hdr->status;
	return send_result;
}
void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult result)
{
	UT_ASSERT_EQ(family, GCS_BLOCK_SEND_FAMILY_REPLY);
	note_calls++;
	noted_result = result;
}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data);
	gate_calls++;
	return new_work_allowed;
}
int
cluster_ic_tier1_my_data_channel(void)
{
	return 3;
}
int
cluster_lms_shard_for_tag(const BufferTag *tag, int count)
{
	return 3;
}
void
cluster_gcs_block_dedup_note_misroute(void)
{
	abort();
}
bool
cluster_write_fence_enforcing(void)
{
	return true;
}
bool
cluster_write_fence_allowed(void)
{
	return fence_allowed;
}
void
cluster_injection_run(const char *name)
{
	abort();
}
bool
cluster_injection_should_skip(const char *name)
{
	return false;
}
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
TimestampTz
GetCurrentTimestamp(void)
{
	static TimestampTz now;
	return ++now;
}
void
cluster_lms_obs_note_direct_reply(void)
{
	direct_replies++;
}
void
cluster_lms_obs_note_inline_serve(uint64 elapsed)
{
	serve_observations++;
}
void
cluster_cr_server_stat_bump(ClusterCrServerStat which)
{
	Assert(which == CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
	fence_refusals++;
}
bool
cluster_cr_server_freshref_c1b_pair_request_decode(const GcsBlockForwardPayload *fwd, int32 source,
												   int32 local, uint64 epoch, int backends,
												   uint32 *segment, TransactionId *xid,
												   uint32 *slot, SCN *scn)
{
	abort(); /* Freshref decode is outside these ordinary inline cases. */
	return false;
}
static MemoryContext
cr_serve_scratch_context(void)
{
	scratch_calls++;
	CrServeScratchCtx = (MemoryContext)&scratch_context;
	return CrServeScratchCtx;
}
void
MemoryContextReset(MemoryContext context)
{
	Assert(context == CrServeScratchCtx);
	reset_calls++;
}
static void
cr_serve_slot(ClusterLmsCrSlot *slot)
{
	Assert(CurrentMemoryContext == CrServeScratchCtx);
	serve_calls++;
	last_served = *slot;
	slot->result_status = serve_result;
	memset(slot->result_page, 0x5a, sizeof(slot->result_page));
	slot->undo_auth.origin_epoch = 7;
	slot->undo_auth.live_hwm_lsn = 555;
	slot->undo_auth.tt_generation = 8;
	slot->undo_auth.authority_scn = 9001;
	if (seal_during_serve)
		new_work_allowed = false;
}
#include "test_cluster_cr_inline_stop.inc"

static void
check_diagnostic_cleanup(void)
{
	UT_ASSERT_EQ(alloc_calls, free_calls);
	if (expect_queued_cleanup) {
		if (queued_cleanup_checked)
			return;
		for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
			if (queued_ready_mask & (1u << i))
				UT_ASSERT_EQ(pg_atomic_read_u32(&CrServerShared->slots[i].state),
							 CLUSTER_LMS_CR_FREE);
		queued_cleanup_checked = true;
		if (reuse_slots) {
			for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
				CrServerShared->slots[i].request_id = 999999;
				pg_atomic_write_u32(&CrServerShared->slots[i].state, CLUSTER_LMS_CR_PENDING);
			}
			reuse_slots = false;
		}
	} else {
		UT_ASSERT_EQ(reset_calls, 1);
		UT_ASSERT_EQ(direct_replies, 1);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}

static GcsBlockForwardPayload
reset_test(void)
{
	GcsBlockForwardPayload fwd = { 0 };
	new_work_allowed = fence_allowed = true;
	seal_during_serve = false;
	gate_calls = scratch_calls = serve_calls = reply_calls = reset_calls = 0;
	direct_replies = serve_observations = fence_refusals = 0;
	send_calls = note_calls = checksum_calls = alloc_calls = free_calls = 0;
	send_result = CLUSTER_IC_SEND_DONE;
	serve_result = GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	diag_enabled = diag_throw = send_throw = false;
	diag_n = 0;
	diag_log[0] = diag_message[0] = '\0';
	expect_queued_cleanup = reuse_slots = false;
	queued_cleanup_checked = false;
	queued_ready_mask = 0;
	outstanding_buf = NULL;
	CrServerShared = NULL;
	reply_length = 0;
	memset(&last_served, 0, sizeof(last_served));
	memset(&last_reply, 0, sizeof(last_reply));
	CurrentMemoryContext = (MemoryContext)&caller_context;
	CrServeScratchCtx = NULL;
	fwd.request_id = 19;
	fwd.epoch = 7;
	fwd.original_requester_node = 2;
	fwd.master_node = 0;
	fwd.requester_backend_id = 5;
	fwd.tag.spcOid = 1663;
	fwd.tag.dbOid = 5;
	fwd.tag.relNumber = 0;
	return fwd;
}
UT_TEST(test_seal_two_forbids_all_inline_read_contexts)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_CR; kind <= CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT; kind++) {
		GcsBlockForwardPayload fwd = reset_test(), before = fwd;
		new_work_allowed = false;
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(gate_calls, 1);
		UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls + reset_calls, 0);
		UT_ASSERT_EQ(direct_replies + serve_observations, 0);
		UT_ASSERT_EQ(memcmp(&fwd, &before, sizeof(fwd)), 0);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}
UT_TEST(test_seal_one_allows_original_read_and_reply)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_CR; kind <= CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT; kind++) {
		GcsBlockForwardPayload fwd = reset_test();
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(gate_calls, 1);
		UT_ASSERT_EQ(serve_calls, 1);
		UT_ASSERT_EQ(reply_calls, 1);
		UT_ASSERT_EQ(last_served.request_id, fwd.request_id);
		UT_ASSERT_EQ(last_served.req_kind, kind);
		UT_ASSERT_EQ(last_served.undo_owner, -1);
		UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER);
		UT_ASSERT_EQ(reset_calls, 1);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}
UT_TEST(test_existing_admission_keeps_its_original_completion)
{
	GcsBlockForwardPayload fwd = reset_test();
	seal_during_serve = true;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT(!new_work_allowed);
	UT_ASSERT_EQ(serve_calls, 1);
	UT_ASSERT_EQ(reply_calls, 1);
	UT_ASSERT_EQ(reset_calls, 1);
}
UT_TEST(test_fence_denial_is_preserved_but_does_not_bypass_final_seal)
{
	GcsBlockForwardPayload fwd = reset_test();
	fence_allowed = false;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(serve_calls, 0);
	UT_ASSERT_EQ(reply_calls, 1);
	UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	UT_ASSERT_EQ(fence_refusals, 1);
	fwd = reset_test();
	new_work_allowed = fence_allowed = false;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls, 0);
}
UT_TEST(test_park_only_kinds_never_enter_generic_serve)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
		 kind <= CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD; kind++) {
		GcsBlockForwardPayload fwd = reset_test();
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(serve_calls, 0);
		UT_ASSERT_EQ(reply_calls, 1);
		UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
		fwd = reset_test();
		new_work_allowed = false;
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls, 0);
	}
}
UT_TEST(test_null_does_not_allocate_or_use_service_gate)
{
	reset_test();
	cluster_gcs_block_forward_serve_inline(NULL, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls + scratch_calls + serve_calls + reply_calls, 0);
}

/* Keep the original six admission cases above. These additional cases execute
 * the original builder and both shipping callers; only proof/I/O are leaves. */
static GcsBlockForwardPayload
ordinary_request(void)
{
	GcsBlockForwardPayload fwd = reset_test();
	fwd.request_id = UINT64CONST(4294971923);
	fwd.tag = GcsBlockUndoFetchTagMake(3, 7);
	fwd.transition_id = 1;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)3497490);
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, false);
	diag_enabled = true;
	return fwd;
}

static void
check_reply(const GcsBlockForwardPayload *fwd, uint8 status)
{
	GcsBlockReplyHeader hdr;
	bool served = GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)status);
	memcpy(&hdr, reply_bytes, sizeof(hdr));
	UT_ASSERT_EQ(hdr.request_id, fwd->request_id);
	UT_ASSERT_EQ(hdr.requester_backend_id, fwd->requester_backend_id);
	UT_ASSERT_EQ(hdr.sender_node, cluster_node_id);
	UT_ASSERT_EQ(hdr.epoch, 7);
	UT_ASSERT_EQ(hdr.transition_id, fwd->transition_id);
	UT_ASSERT_EQ(hdr.status, status);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&hdr), fwd->master_node);
	UT_ASSERT_EQ(hdr.checksum, 1234);
	UT_ASSERT_EQ(reply_length,
				 sizeof(hdr) + BLCKSZ + (served ? sizeof(ClusterGcsUndoAuthTrailer) : 0));
	for (unsigned i = 0; i < BLCKSZ; i++)
		UT_ASSERT_EQ((uint8)reply_bytes[sizeof(hdr) + i], served ? 0x5a : 0);
	if (served) {
		ClusterGcsUndoAuthTrailer trailer;
		memcpy(&trailer, reply_bytes + sizeof(hdr) + BLCKSZ, sizeof(trailer));
		UT_ASSERT_EQ(hdr.page_lsn, 555);
		UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetTtGeneration(&trailer), 8);
		UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetAuthorityScn(&trailer), 9001);
	} else
		UT_ASSERT_EQ(hdr.page_lsn, 0);
	UT_ASSERT_EQ(send_calls, note_calls);
	UT_ASSERT_EQ(send_calls, checksum_calls);
	UT_ASSERT_EQ(send_calls, alloc_calls);
	UT_ASSERT_EQ(alloc_calls, free_calls);
	UT_ASSERT_EQ(noted_result, send_result);
}

static void
check_reply_diagnostic(const GcsBlockForwardPayload *fwd, const char *path, TransactionId xid)
{
	char expected[256];
	UT_ASSERT(strstr(diag_log, "PGRAC ordinary TT reply:") != NULL);
	snprintf(expected, sizeof(expected), "path=%s ", path);
	UT_ASSERT(strstr(diag_log, expected) != NULL);
	UT_ASSERT(strstr(diag_log, "node=1 requester=2 backend=5 request=4294971923") != NULL);
	snprintf(expected, sizeof(expected), "tag=%u/%u/%u/%u/%u", fwd->tag.spcOid, fwd->tag.dbOid,
			 fwd->tag.relNumber, (unsigned)fwd->tag.forkNum, fwd->tag.blockNum);
	UT_ASSERT(strstr(diag_log, expected) != NULL);
	snprintf(expected, sizeof(expected), "xid=%u", xid);
	UT_ASSERT(strstr(diag_log, expected) != NULL);
	UT_ASSERT(strstr(diag_log, "decoded_xid_valid=1 request_segment=3 request_block=7") != NULL);
	UT_ASSERT(strstr(diag_log, "request_epoch=7 master=0 transition=1") != NULL);
	snprintf(expected, sizeof(expected), "reply_status=%u", (unsigned)last_reply.result_status);
	UT_ASSERT(strstr(diag_log, expected) != NULL);
	snprintf(expected, sizeof(expected), "send_result=%d", (int)send_result);
	UT_ASSERT(strstr(diag_log, expected) != NULL);
}

static ClusterCrServerShared queued;
static void
prepare_queued(const GcsBlockForwardPayload *fwd, unsigned mask, uint8 status)
{
	memset(&queued, 0, sizeof(queued));
	CrServerShared = &queued;
	expect_queued_cleanup = true;
	queued_ready_mask = mask;
	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &queued.slots[i];
		slot->tag = fwd->tag;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->request_id = fwd->request_id + i;
		slot->epoch = fwd->epoch;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
		slot->undo_owner = -1;
		slot->undo_xid = 3497490;
		slot->undo_segment_id = 3;
		slot->undo_block_no = 7;
		slot->result_status = status;
		memset(slot->result_page, 0x5a, BLCKSZ);
		slot->undo_auth.origin_epoch = 7;
		slot->undo_auth.live_hwm_lsn = 555;
		slot->undo_auth.tt_generation = 8;
		slot->undo_auth.authority_scn = 9001;
		pg_atomic_init_u32(&slot->state,
						   (mask & (1u << i)) ? CLUSTER_LMS_CR_READY : CLUSTER_LMS_CR_FREE);
	}
}

UT_TEST(test_ordinary_inline_reply_preserves_each_raw_handoff)
{
	for (int status = 0; status < 2; status++) {
		for (int result = 0; result < 4; result++) {
			GcsBlockForwardPayload fwd = ordinary_request();
			serve_result = status ? GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
								  : GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
			send_result = (ClusterICSendResult)result;
			cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT);
			check_reply(&fwd, serve_result);
			UT_ASSERT_EQ(send_calls, 1);
			UT_ASSERT_EQ(serve_calls, 1);
			UT_ASSERT_EQ(reset_calls, 1);
			UT_ASSERT_EQ(diag_n, !status || result >= 2);
			if (diag_n)
				check_reply_diagnostic(&fwd, "INLINE", 3497490);
		}
	}
}
UT_TEST(test_ordinary_queued_reply_preserves_each_raw_handoff)
{
	for (int status = 0; status < 2; status++) {
		for (int result = 0; result < 4; result++) {
			GcsBlockForwardPayload fwd = ordinary_request();
			uint8 reply_status = status ? GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
										: GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
			prepare_queued(&fwd, 1, reply_status);
			send_result = (ClusterICSendResult)result;
			cluster_lms_cr_ship_ready();
			check_reply(&fwd, reply_status);
			UT_ASSERT_EQ(send_calls, 1);
			UT_ASSERT_EQ(serve_calls + reset_calls + gate_calls, 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&queued.slots[0].state), CLUSTER_LMS_CR_FREE);
			UT_ASSERT_EQ(diag_n, !status || result >= 2);
			if (diag_n)
				check_reply_diagnostic(&fwd, "QUEUED", 3497490);
		}
	}
}
UT_TEST(test_queued_observations_survive_original_slot_reuse)
{
	GcsBlockForwardPayload fwd = ordinary_request();
	prepare_queued(&fwd, 15, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	reuse_slots = true; /* Simulated new publisher, after all original FREE stores. */
	cluster_lms_cr_ship_ready();
	UT_ASSERT_EQ(send_calls, 4);
	UT_ASSERT_EQ(diag_n, 4);
	UT_ASSERT(queued_cleanup_checked);
	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		char expected[64];
		snprintf(expected, sizeof(expected), "request=" UINT64_FORMAT " ", fwd.request_id + i);
		UT_ASSERT(strstr(diag_log, expected) != NULL);
		UT_ASSERT_EQ(queued.slots[i].request_id, 999999);
		UT_ASSERT_EQ(pg_atomic_read_u32(&queued.slots[i].state), CLUSTER_LMS_CR_PENDING);
	}
	UT_ASSERT(strstr(diag_log, "request=999999") == NULL);
}
UT_TEST(test_inline_log_exception_follows_original_cleanup_even_before_decode)
{
	for (int fence = 0; fence < 2; fence++) {
		GcsBlockForwardPayload fwd = ordinary_request();
		volatile bool caught = false;
		serve_result = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		fence_allowed = !fence;
		diag_throw = true;
		PG_TRY();
		{
			cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(note_calls, 1);
		UT_ASSERT_EQ(alloc_calls, free_calls);
		UT_ASSERT_EQ(reset_calls, 1);
		UT_ASSERT_EQ(direct_replies, 1);
		UT_ASSERT_EQ(serve_calls, !fence);
		UT_ASSERT(strstr(diag_message, fence ? "path=INLINE_FENCE" : "path=INLINE") != NULL);
		UT_ASSERT(strstr(diag_message,
						 fence
							 ? "xid=0 decoded_xid_valid=0 request_segment=0 request_block=0"
							 : "xid=3497490 decoded_xid_valid=1 request_segment=3 request_block=7")
				  != NULL);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}
UT_TEST(test_queued_log_exception_cannot_hold_any_original_ready_slot)
{
	GcsBlockForwardPayload fwd = ordinary_request();
	volatile bool caught = false;
	prepare_queued(&fwd, 15, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	diag_throw = true;
	PG_TRY();
	{
		cluster_lms_cr_ship_ready();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(send_calls, 4);
	UT_ASSERT_EQ(note_calls, 4);
	UT_ASSERT_EQ(alloc_calls, free_calls);
	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
		UT_ASSERT_EQ(pg_atomic_read_u32(&queued.slots[i].state), CLUSTER_LMS_CR_FREE);

	/* An original transport exception remains original: no synthetic outcome,
	 * extra send, diagnostic or new catch/slot retirement is introduced. */
	fwd = ordinary_request();
	prepare_queued(&fwd, 1, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	caught = false;
	send_throw = true;
	PG_TRY();
	{
		cluster_lms_cr_ship_ready();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(note_calls + diag_n, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&queued.slots[0].state), CLUSTER_LMS_CR_READY);
	free(outstanding_buf); /* Fixture allocator teardown, not a product release. */
	outstanding_buf = NULL;
}
UT_TEST(test_reply_filter_keeps_other_families_and_nonready_slots_silent)
{
	for (int variant = 0; variant < 6; variant++) {
		GcsBlockForwardPayload fwd = ordinary_request();
		ClusterLmsCrSlot untouched;
		prepare_queued(&fwd, 1, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
		switch (variant) {
		case 0:
			queued.slots[0].req_kind = CLUSTER_LMS_SLOT_KIND_CR;
			break;
		case 1:
			queued.slots[0].req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_FETCH;
			break;
		case 2:
			queued.slots[0].req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
			break;
		case 3:
			queued.slots[0].undo_authoritative = true;
			break;
		case 4:
			queued.slots[0].undo_owner = 0;
			break;
		case 5:
			queued.slots[0].read_scn = 9001;
			break;
		}
		pg_atomic_write_u32(&queued.slots[1].state, CLUSTER_LMS_CR_PENDING);
		untouched = queued.slots[1];
		cluster_lms_cr_ship_ready();
		UT_ASSERT_EQ(diag_n, 0);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(memcmp(&untouched, &queued.slots[1], sizeof(untouched)), 0);
	}
}
UT_TEST(test_reply_detail_budget_does_not_limit_original_handoffs)
{
	unsigned before = diag_attempts, observed = 0;
	for (int i = 0; i < 80; i++) {
		GcsBlockForwardPayload fwd = ordinary_request();
		prepare_queued(&fwd, 1, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
		cluster_lms_cr_ship_ready();
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(note_calls, 1);
		UT_ASSERT_EQ(alloc_calls, free_calls);
		UT_ASSERT_EQ(pg_atomic_read_u32(&queued.slots[0].state), CLUSTER_LMS_CR_FREE);
		observed += diag_n;
	}
	UT_ASSERT(before < 64);
	UT_ASSERT_EQ(observed, 64 - before);
	UT_ASSERT_EQ(diag_attempts, 64);
}
UT_TEST(test_inline_fence_does_not_reclassify_undecoded_families)
{
	const uint8 families[] = { 0, 1, 3, 4, 5, GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR, 255 };
	for (unsigned family = 0; family < lengthof(families); family++) {
		for (int result = 0; result < 4; result++) {
			GcsBlockForwardPayload fwd = ordinary_request();
			unsigned before = diag_attempts;
			fwd.reserved_0[6] = families[family];
			fence_allowed = false;
			send_result = (ClusterICSendResult)result;
			cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT);
			check_reply(&fwd, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
			UT_ASSERT_EQ(serve_calls, 0);
			UT_ASSERT_EQ(send_calls, 1);
			UT_ASSERT_EQ(reset_calls, 1);
			UT_ASSERT_EQ(fence_refusals, 1);
			UT_ASSERT_EQ(diag_n, 0);
			UT_ASSERT_EQ(diag_attempts, before);
		}
	}
}
int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_seal_two_forbids_all_inline_read_contexts);
	UT_RUN(test_seal_one_allows_original_read_and_reply);
	UT_RUN(test_existing_admission_keeps_its_original_completion);
	UT_RUN(test_fence_denial_is_preserved_but_does_not_bypass_final_seal);
	UT_RUN(test_park_only_kinds_never_enter_generic_serve);
	UT_RUN(test_null_does_not_allocate_or_use_service_gate);
	UT_RUN(test_ordinary_inline_reply_preserves_each_raw_handoff);
	UT_RUN(test_ordinary_queued_reply_preserves_each_raw_handoff);
	UT_RUN(test_queued_observations_survive_original_slot_reuse);
	UT_RUN(test_inline_log_exception_follows_original_cleanup_even_before_decode);
	UT_RUN(test_queued_log_exception_cannot_hold_any_original_ready_slot);
	UT_RUN(test_reply_filter_keeps_other_families_and_nonready_slots_silent);
	UT_RUN(test_inline_fence_does_not_reclassify_undecoded_families);
	UT_RUN(test_reply_detail_budget_does_not_limit_original_handoffs);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
