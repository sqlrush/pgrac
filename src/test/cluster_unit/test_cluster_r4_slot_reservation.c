/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_slot_reservation.c
 *	Stage 8 R4 D4-B common legacy slot-reservation proof window.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include "access/htup_details.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#undef printf
#undef snprintf

#include "unit_test.h"

#ifndef CR_SERVER_SOURCE_PATH
#error "CR_SERVER_SOURCE_PATH must identify cluster_cr_server.c"
#endif

UT_DEFINE_GLOBALS();

int cluster_gcs_reply_timeout_ms = 3000;
static TimestampTz ut_now = INT64CONST(1000000);

TimestampTz
GetCurrentTimestamp(void)
{
	return ut_now;
}

extern bool cluster_cr_server_test_reserve_legacy_slot(ClusterLmsCrSlot *slot,
													   uint32 reserved_state);
/* Expected USE_CLUSTER_UNIT claim-only seams.  They expose no builder step. */
extern bool cluster_cr_server_test_r4_claim_queued(uint32 slot_index);
extern bool cluster_cr_server_test_r4_build_step(uint32 slot_index);
extern bool cluster_cr_server_test_r4_send_foreign_undo(uint32 slot_index);
extern bool cluster_cr_server_r4_land_foreign_undo(const ClusterICEnvelope *env,
												   const GcsBlockReplyHeader *header,
												   const char undo_page[BLCKSZ],
												   const ClusterGcsUndoAuthTrailer *undo_auth);
extern bool cluster_cr_server_test_r4_freeze_foreign_generation(uint32 slot_index,
																uint32 physical_generation);
extern bool cluster_cr_server_test_r4_ship_terminal(uint32 slot_index);
extern void cluster_cr_server_test_r4_reset_contexts(void);
extern bool
cluster_cr_server_test_r4_context_matches(uint32 slot_index, bool expect_present,
										  uint64 slot_generation, uint64 builder_incarnation,
										  const ClusterSemanticAdmissionToken *admission);
extern bool cluster_cr_server_r4_worker0_drained(void);
extern bool cluster_cr_server_r4_lmon_reclaim_closed(uint64 worker_incarnation, uint64 generation);
extern void cluster_lms_data_plane_close_peer_now(int32 peer_id);
extern void cluster_lms_data_plane_test_seed_peer(int32 peer_id, int fd, bool connected,
												  bool enabled, bool wes_dirty);
extern bool cluster_lms_data_plane_test_peer_snapshot(int32 peer_id, int *fd_out, bool *down_out,
													  bool *wes_dirty_out);

#define UT_FORMATION_EPOCH UINT64_C(9)
#define UT_ACTIVATION_GENERATION UINT64_C(12)
#define UT_LOCAL_OPEN_GENERATION UINT64_C(0xffffffff)
#define UT_REQUESTER_CAPABILITY_GENERATION UINT32_C(42)
#define UT_MASTER_CAPABILITY_GENERATION UINT32_C(43)
#define UT_REQUESTER_NODE 2
#define UT_MASTER_NODE 1
#define UT_HOLDER_NODE 3
#define UT_WORKER_ID 2
#define UT_WORKER_INCARNATION UINT64_C(0x1122334455667788)
#define UT_WORKER0_INCARNATION UINT64_C(0x8877665544332211)
#define UT_PRODUCER_PID 3131
#define UT_REQUESTER_BACKEND 7
#define UT_REQUEST_ID UINT64_C(0x0102030405060708)
#define UT_READ_SCN ((SCN)UINT64_C(0x1234))
#define UT_EXPECTED_PAGE_SCN ((SCN)UINT64_C(0x2222))
#define UT_COPIED_PAGE_SCN ((SCN)UINT64_C(0x3333))
#define UT_COPIED_PAGE_LSN ((XLogRecPtr)UINT64_C(0x445566778899aabb))
#define UT_MASTER_GENERATION ((UT_FORMATION_EPOCH << 32) | UINT64_C(4))
#define UT_MASTER_TRANSITION UINT64_C(7)
#define UT_FOREIGN_PHYSICAL_GENERATION UINT32_C(0x01020304)
#define UT_FOREIGN_LIVE_HWM ((XLogRecPtr)UINT64_C(0x4000))
#define UT_FOREIGN_TT_GENERATION UINT64_C(3)
#define UT_FOREIGN_AUTHORITY_SCN ((SCN)(UT_READ_SCN + 1))
#define UT_R4_REQUIRED_CAPABILITIES                                                                \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1                  \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)

typedef struct UtClusterCrServerShared {
	pg_atomic_uint64 lms_latch_ptr;
	ClusterLmsCrSlot slots[CLUSTER_LMS_CR_SLOTS];
} UtClusterCrServerShared;

int MyProcPid = 4242;
bool IsUnderPostmaster = true;
static bool ut_stop_poll_during_forget;
static bool ut_stop_new_work_allowed = true;
static int ut_stop_new_work_calls;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	UT_ASSERT(!modifies_data);
	ut_stop_new_work_calls++;
	return ut_stop_new_work_allowed;
}

int cluster_node_id = UT_HOLDER_NODE;
BackendType MyBackendType = B_LMS;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
static bool ut_builder_throw_query_cancel;
static int ut_builder_error_code;
static int ut_flush_error_calls;
static char ut_initial_memory_context_storage;
static char ut_error_memory_context_storage;
MemoryContext CurrentMemoryContext = (MemoryContext)&ut_initial_memory_context_storage;
static MemoryContext ut_context_at_flush;
static int ut_refusal_observe_calls;
static ClusterCrBuildReason ut_refusal_observed_reason;
static int ut_log_level;
static int ut_protocol_logs;
static int ut_protocol_total_logs;
static char ut_last_log[1024];

void
cluster_r4_observe_refusal(ClusterR4RefusalStage stage, ClusterCrBuildReason reason,
						   const BufferTag *tag, uint64 request, uint64 epoch, int32 requester,
						   int32 master, SCN read_scn)
{
	UT_ASSERT_EQ(stage, CLUSTER_R4_REFUSAL_HOLDER_SHIP);
	UT_ASSERT_NOT_NULL(tag);
	UT_ASSERT_EQ(request, UT_REQUEST_ID);
	UT_ASSERT_EQ(epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(requester, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(master, UT_MASTER_NODE);
	UT_ASSERT_EQ(read_scn, UT_READ_SCN);
	ut_refusal_observe_calls++;
	ut_refusal_observed_reason = reason;
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

int
geterrcode(void)
{
	return ut_builder_error_code;
}

void
FlushErrorState(void)
{
	ut_flush_error_calls++;
	ut_context_at_flush = CurrentMemoryContext;
}

static ClusterLmsSharedState *ut_lms_state;
static int ut_lock_acquire_count;
static int ut_lock_release_count;
static LWLock *ut_lock;
static LWLockMode ut_lock_mode;
static UtClusterCrServerShared ut_cr_server_shared;
static const ClusterShmemRegion *ut_cr_server_region;
static bool ut_shmem_found;
static Latch ut_lms_latch;
static int ut_data_worker_id;
static int ut_sequence;
static int ut_lock_release_sequence;
static int ut_copy_sequence;
static int ut_enter_sequence;
static int ut_recheck_sequence;
static int ut_peer_open_sequence[2];
static int ut_leave_sequence;
static int ut_wake_sequence;
static bool ut_watch_submit_lock;
static ClusterLmsCrSlot *ut_submit_slot;
static uint32 ut_state_at_lock_release;
static uint64 ut_generation_at_lock_release;
static ClusterR4CrOwnerStamp ut_owner_at_lock_release;
static bool ut_copy_ok;
static ClusterBufmgrGcsCopyRefusal ut_copy_refusal;
static int ut_copy_calls;
static BufferTag ut_copy_tag;
static SCN ut_copy_expected_page_scn;
static int ut_enter_calls;
static uint64 ut_enter_feature_bit;
static ClusterSemanticAdmissionSide ut_enter_side;
static bool ut_recheck_ok;
static int ut_recheck_calls;
static uint32 ut_state_at_enter;
static uint32 ut_state_at_recheck;
static uint32 ut_state_at_peer_open[2];
static bool ut_peer_open_ok;
static int ut_peer_open_calls;
static int32 ut_peer_open_peers[2];
static uint32 ut_peer_open_required[2];
static uint32 ut_peer_open_generations[2];
static int ut_leave_calls;
static ClusterSemanticAdmissionToken ut_left_admission;
static int ut_wake_calls;
static ClusterSemanticAdmissionToken ut_expected_admission;
static int ut_builder_step_calls;
static uint32 ut_builder_slot_index;
static uint64 ut_builder_slot_generation;
static bool ut_builder_foreign_ready;
static ClusterR4CrSlotExtension *ut_builder_extension;
static char *ut_builder_result_page;
static const char *ut_builder_foreign_page;
static ClusterR4CrBuildStepResult ut_builder_step_result;
static ClusterCrBuildReason ut_builder_step_reason;
static bool ut_builder_publish_foreign_pause;
static uint32 ut_state_at_builder_step;
static bool ut_pending_locator_ok;
static int ut_pending_locator_calls;
static uint32 ut_pending_locator_slot_index;
static uint64 ut_pending_locator_slot_generation;
static uint32 ut_state_at_pending_locator;
static ClusterTxLocator ut_pending_locator;
static bool ut_pgrd_resolve_ok;
static int ut_pgrd_resolve_calls;
static ClusterSemanticAdmissionToken ut_pgrd_resolve_admission;
static ClusterUndoPathIntent ut_pgrd_resolve_intent;
static uint32 ut_pgrd_resolve_owner_instance;
static uint32 ut_pgrd_resolve_segment_id;
static ClusterUndoBlock0ResolvedRoot ut_pgrd_resolved_root;
static ClusterUndoBlock0CurrentStep ut_current_acquire_step;
static ClusterUndoBlock0CurrentStep ut_current_release_step;
static ClusterUndoBlock0Result ut_current_sample_result;
static int ut_current_acquire_calls;
static int ut_current_acquire_poll_calls;
static int ut_current_sample_calls;
static int ut_current_release_calls;
static int ut_current_release_poll_calls;
static int ut_current_cancel_calls;
static ClusterUndoBlock0LogicalKey ut_current_logical;
static ClusterUndoBlock0ResolvedRoot ut_current_sample_root;
static uint32 ut_current_sample_generation;
static ClusterICSendResult ut_send_result;
static int ut_send_calls;
static uint8 ut_send_msg_type;
static int32 ut_send_dest;
static uint32 ut_send_length;
static uint32 ut_state_at_send;
static char ut_send_payload[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
static int ut_note_send_calls;
static GcsBlockSendFamily ut_note_send_family;
static ClusterICSendResult ut_note_send_result;
static bool ut_envelope_build_ok;
static int ut_envelope_build_calls;
static int ut_envelope_build_sequence;
static uint8 ut_envelope_msg_type;
static uint32 ut_envelope_source;
static uint32 ut_envelope_dest;
static uint32 ut_envelope_payload_length;
static bool ut_local_dispatch_ok;
static int ut_local_dispatch_calls;
static int ut_local_dispatch_sequence;
static int32 ut_local_dispatch_peer;
static uint32 ut_state_at_local_dispatch;
static char ut_local_dispatch_payload[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
static bool ut_write_fence_enforcing;
static bool ut_write_fence_allowed;
static int ut_checksum_calls;
static uint32 ut_checksum_value;
static int ut_forget_calls;
static uint32 ut_forget_slot_index;
static uint64 ut_forget_slot_generation;
static int ut_close_peer_calls;
static int ut_tier1_close_calls;
static int32 ut_tier1_close_peer;
static char ut_tier1_close_reason[96];
static uint32 ut_state_at_tier1_close;
static int ut_forget_calls_at_tier1_close;
static int ut_leave_calls_at_tier1_close;
static bool ut_extract_ok;
static uint16 ut_extract_canonical_wrap;
static int ut_extract_calls;
static uint32 ut_state_at_extract;
static ClusterTxLocator ut_extract_locator;
static bool ut_reclaim_race_on_proof;
static bool ut_reclaim_race_injected;
static bool ut_real_builder;
int cluster_cr_chain_walk_max_steps = 4096;
MemoryContext TopMemoryContext = (MemoryContext)0x1;
extern ClusterR4CrBuildStepResult continuation_real_step(uint32, uint64, bool,
														 ClusterR4CrSlotExtension *, char *,
														 const char *, ClusterCrBuildReason *);
extern void continuation_real_forget(uint32, uint64);
extern bool continuation_real_pending(uint32, uint64, ClusterTxLocator *);
extern bool continuation_real_extract(const char *, const ClusterTxLocator *, char *, size_t *,
									  ClusterTxLocator *);

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

size_t
cluster_undo_get_record(UBA uba pg_attribute_unused(), void *out pg_attribute_unused(),
						size_t size pg_attribute_unused())
{
	/* The integrated fixture is entirely foreign. A local read is a bug. */
	UT_ASSERT(false);
	return 0;
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	ut_log_level = level;
	return level == LOG || level >= ERROR;
}

int
errcode(int sqlerrcode)
{
	ut_builder_error_code = sqlerrcode;
	return 0;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *format pg_attribute_unused(), ...)
{
	va_list args;

	va_start(args, format);
	vsnprintf(ut_last_log, sizeof(ut_last_log), format, args);
	va_end(args);
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	if (ut_log_level < ERROR) {
		if (strstr(ut_last_log, "PGRAC_FAMILY=R4_PROTOCOL") != NULL) {
			ut_protocol_logs++;
			ut_protocol_total_logs++;
		}
		return;
	}
	UT_ASSERT(false); /* A real page operation must not throw in this fixture. */
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_cr_r4_extract_resident_record(const char resident_undo_page[BLCKSZ],
									  const ClusterTxLocator *request_locator,
									  char record_out[BLCKSZ], size_t *record_length_out,
									  ClusterTxLocator *canonical_locator_out)
{
	ut_extract_calls++;
	if (ut_real_builder)
		return continuation_real_extract(resident_undo_page, request_locator, record_out,
										 record_length_out, canonical_locator_out);
	ut_state_at_extract = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	memset(&ut_extract_locator, 0, sizeof(ut_extract_locator));
	if (request_locator != NULL)
		ut_extract_locator = *request_locator;
	if (!ut_extract_ok || resident_undo_page == NULL || resident_undo_page[0] != (char)0xa5
		|| request_locator == NULL || record_out == NULL || record_length_out == NULL
		|| canonical_locator_out == NULL)
		return false;
	memset(record_out, 0x6b, sizeof(UndoRecordHeader));
	*record_length_out = sizeof(UndoRecordHeader);
	*canonical_locator_out = *request_locator;
	canonical_locator_out->tt_wrap = ut_extract_canonical_wrap;
	return true;
}

ClusterR4CrBuildStepResult
cluster_cr_build_on_holder_step(uint32 slot_index, uint64 slot_generation, bool foreign_undo_ready,
								ClusterR4CrSlotExtension *extension, char result_page[BLCKSZ],
								const char foreign_undo_page[BLCKSZ],
								ClusterCrBuildReason *reason_out)
{
	UBA foreign_uba;

	ut_builder_step_calls++;
	ut_builder_slot_index = slot_index;
	ut_builder_slot_generation = slot_generation;
	ut_builder_foreign_ready = foreign_undo_ready;
	ut_builder_extension = extension;
	ut_builder_result_page = result_page;
	ut_builder_foreign_page = foreign_undo_page;
	ut_state_at_builder_step = pg_atomic_read_u32(&ut_cr_server_shared.slots[slot_index].state);
	if (ut_real_builder)
		return continuation_real_step(slot_index, slot_generation, foreign_undo_ready, extension,
									  result_page, foreign_undo_page, reason_out);
	if (ut_builder_throw_query_cancel) {
		ut_builder_error_code = ERRCODE_QUERY_CANCELED;
		CurrentMemoryContext = (MemoryContext)&ut_error_memory_context_storage;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	if (ut_builder_publish_foreign_pause && extension != NULL) {
		foreign_uba.raw[0] = UINT64CONST(0x0000000900000101);
		foreign_uba.raw[1] = UINT64CONST(0x0000000000060003);
		extension->foreign_request_id = UINT64CONST(262144);
		extension->foreign_uba = foreign_uba;
		extension->origin_formation_epoch = extension->route_proof.formation_epoch;
		extension->foreign_origin_node = 1;
		extension->foreign_segment_id = 257;
		extension->foreign_block_no = 9;
		extension->foreign_xid = 797;
		extension->foreign_wrap = 7;
		extension->build_steps = 1;
		extension->foreign_tt_slot_offset = 3;
		extension->foreign_row_offset = 6;
	}
	if (reason_out != NULL)
		*reason_out = ut_builder_step_reason;
	return ut_builder_step_result;
}

void
cluster_cr_build_on_holder_forget(uint32 slot_index, uint64 slot_generation)
{
	if (ut_stop_poll_during_forget) {
		int observed_slot;
		const char *reason;

		UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&observed_slot, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(observed_slot, slot_index);
		UT_ASSERT_EQ(strcmp(reason, "CR_CONTEXT_OWNED"), 0);
	}
	ut_forget_calls++;
	ut_forget_slot_index = slot_index;
	ut_forget_slot_generation = slot_generation;
	if (ut_real_builder)
		continuation_real_forget(slot_index, slot_generation);
}

bool
cluster_cr_build_on_holder_pending_locator(uint32 slot_index, uint64 slot_generation,
										   ClusterTxLocator *locator_out)
{
	if (ut_real_builder)
		return continuation_real_pending(slot_index, slot_generation, locator_out);
	ut_pending_locator_calls++;
	ut_pending_locator_slot_index = slot_index;
	ut_pending_locator_slot_generation = slot_generation;
	ut_state_at_pending_locator = pg_atomic_read_u32(&ut_cr_server_shared.slots[slot_index].state);
	if (locator_out != NULL)
		memset(locator_out, 0, sizeof(*locator_out));
	if (!ut_pending_locator_ok || locator_out == NULL)
		return false;
	*locator_out = ut_pending_locator;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
													 ClusterUndoPathIntent intent,
													 uint32 owner_instance, uint32 segment_id,
													 ClusterUndoBlock0ResolvedRoot *out)
{
	ut_pgrd_resolve_calls++;
	memset(&ut_pgrd_resolve_admission, 0, sizeof(ut_pgrd_resolve_admission));
	if (token != NULL)
		ut_pgrd_resolve_admission = *token;
	ut_pgrd_resolve_intent = intent;
	ut_pgrd_resolve_owner_instance = owner_instance;
	ut_pgrd_resolve_segment_id = segment_id;
	if (!ut_pgrd_resolve_ok || token == NULL || out == NULL)
		return false;
	*out = ut_pgrd_resolved_root;
	return true;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin(const ClusterUndoBlock0LogicalKey *key,
										  ClusterUndoBlock0CurrentMode mode, int timeout_ms,
										  ClusterUndoBlock0CurrentGuard *guard,
										  ClusterUndoBlock0Result *failure)
{
	ut_current_acquire_calls++;
	memset(&ut_current_logical, 0, sizeof(ut_current_logical));
	if (key != NULL)
		ut_current_logical = *key;
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BLOCK0_SCUR);
	UT_ASSERT_EQ(timeout_ms, 0);
	UT_ASSERT_NOT_NULL(guard);
	if (failure != NULL)
		*failure = ut_current_acquire_step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED
					   ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
					   : CLUSTER_UNDO_BLOCK0_OK;
	return ut_current_acquire_step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	ut_current_acquire_poll_calls++;
	UT_ASSERT_NOT_NULL(guard);
	if (failure != NULL)
		*failure = ut_current_acquire_step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED
					   ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
					   : CLUSTER_UNDO_BLOCK0_OK;
	return ut_current_acquire_step;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(ClusterUndoBlock0CurrentGuard *guard,
											  const ClusterUndoBlock0ResolvedRoot *root,
											  ClusterUndoBlock0Generation *observed)
{
	ut_current_sample_calls++;
	UT_ASSERT_NOT_NULL(guard);
	memset(&ut_current_sample_root, 0, sizeof(ut_current_sample_root));
	if (root != NULL)
		ut_current_sample_root = *root;
	if (ut_current_sample_result == CLUSTER_UNDO_BLOCK0_OK && observed != NULL) {
		observed->known = true;
		observed->value = ut_current_sample_generation;
	}
	return ut_current_sample_result;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard,
										  ClusterUndoBlock0Result *failure)
{
	ut_current_release_calls++;
	UT_ASSERT_NOT_NULL(guard);
	if (failure != NULL)
		*failure = ut_current_release_step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED
					   ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
					   : CLUSTER_UNDO_BLOCK0_OK;
	return ut_current_release_step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	ut_current_release_poll_calls++;
	UT_ASSERT_NOT_NULL(guard);
	if (failure != NULL)
		*failure = ut_current_release_step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED
					   ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
					   : CLUSTER_UNDO_BLOCK0_OK;
	return ut_current_release_step;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard)
{
	ut_current_cancel_calls++;
	UT_ASSERT_NOT_NULL(guard);
}

void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc)
{
	ut_note_send_calls++;
	ut_note_send_family = family;
	ut_note_send_result = rc;
}

bool
cluster_gcs_block_family_on_data_plane(void)
{
	return true;
}

bool
cluster_write_fence_enforcing(void)
{
	return ut_write_fence_enforcing;
}

bool
cluster_write_fence_allowed(void)
{
	return ut_write_fence_allowed;
}

uint32
cluster_gcs_block_compute_checksum(const char *block_data)
{
	ut_checksum_calls++;
	UT_ASSERT_NOT_NULL(block_data);
	return ut_checksum_value;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	ut_send_calls++;
	ut_state_at_send = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	ut_send_msg_type = msg_type;
	ut_send_dest = dest_node_id;
	ut_send_length = payload_len;
	UT_ASSERT(payload_len <= sizeof(ut_send_payload));
	if (payload != NULL && payload_len <= sizeof(ut_send_payload))
		memcpy(ut_send_payload, payload, payload_len);
	return ut_send_result;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type, uint32 source_node_id,
						  uint32 dest_node_id, const void *payload, uint32 payload_length)
{
	ut_envelope_build_calls++;
	ut_envelope_build_sequence = ++ut_sequence;
	ut_envelope_msg_type = msg_type;
	ut_envelope_source = source_node_id;
	ut_envelope_dest = dest_node_id;
	ut_envelope_payload_length = payload_length;
	if (!ut_envelope_build_ok || out_env == NULL || payload == NULL)
		return false;
	memset(out_env, 0, sizeof(*out_env));
	out_env->msg_type = msg_type;
	out_env->source_node_id = source_node_id;
	out_env->dest_node_id = dest_node_id;
	out_env->payload_length = payload_length;
	return true;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	ut_local_dispatch_calls++;
	ut_local_dispatch_sequence = ++ut_sequence;
	ut_local_dispatch_peer = peer_id;
	ut_state_at_local_dispatch = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	UT_ASSERT_NOT_NULL(env);
	UT_ASSERT_NOT_NULL(payload);
	UT_ASSERT(env == NULL || env->payload_length <= sizeof(ut_local_dispatch_payload));
	if (env != NULL && payload != NULL && env->payload_length <= sizeof(ut_local_dispatch_payload))
		memcpy(ut_local_dispatch_payload, payload, env->payload_length);
	return ut_local_dispatch_ok;
}

void
cluster_ic_tier1_request_close_peer(int32 peer_id pg_attribute_unused(),
									const char *reason pg_attribute_unused())
{
	ut_close_peer_calls++;
}

void
cluster_ic_tier1_close_peer(int32 peer_id, const char *reason)
{
	ut_tier1_close_calls++;
	ut_tier1_close_peer = peer_id;
	ut_state_at_tier1_close = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	ut_forget_calls_at_tier1_close = ut_forget_calls;
	ut_leave_calls_at_tier1_close = ut_leave_calls;
	snprintf(ut_tier1_close_reason, sizeof(ut_tier1_close_reason), "%s",
			 reason != NULL ? reason : "");
}

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return ut_lms_state;
}

int
cluster_ic_tier1_my_data_channel(void)
{
	return ut_data_worker_id;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found_ptr)
{
	UT_ASSERT_EQ(size, MAXALIGN(sizeof(ut_cr_server_shared)));
	if (found_ptr != NULL)
		*found_ptr = ut_shmem_found;
	ut_shmem_found = true;
	return &ut_cr_server_shared;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_cr_server_region = region;
}

void
SetLatch(Latch *latch)
{
	ut_wake_calls++;
	ut_wake_sequence = ++ut_sequence;
	UT_ASSERT(latch == &ut_lms_latch);
}

bool
cluster_bufmgr_copy_block_for_r4_cr(BufferTag tag, SCN expected_page_scn, XLogRecPtr *page_lsn_out,
									SCN *page_scn_out, char *dst,
									ClusterBufmgrGcsCopyRefusal *refusal_out)
{
	ut_copy_calls++;
	ut_copy_sequence = ++ut_sequence;
	ut_copy_tag = tag;
	ut_copy_expected_page_scn = expected_page_scn;
	if (page_lsn_out != NULL)
		*page_lsn_out = InvalidXLogRecPtr;
	if (page_scn_out != NULL)
		*page_scn_out = InvalidScn;
	if (dst != NULL)
		memset(dst, 0, BLCKSZ);
	if (refusal_out != NULL)
		*refusal_out = ut_copy_refusal;
	if (!ut_copy_ok || page_lsn_out == NULL || page_scn_out == NULL || dst == NULL
		|| refusal_out == NULL)
		return false;
	memset(dst, 0x5a, BLCKSZ);
	*page_lsn_out = UT_COPIED_PAGE_LSN;
	*page_scn_out = UT_COPIED_PAGE_SCN;
	*refusal_out = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	return true;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	ut_enter_calls++;
	ut_enter_sequence = ++ut_sequence;
	ut_enter_feature_bit = feature_bit;
	ut_enter_side = side;
	ut_state_at_enter = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| side != CLUSTER_SEMANTIC_TARGET_SIDE || token == NULL)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	*token = ut_expected_admission;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	ut_recheck_calls++;
	ut_recheck_sequence = ++ut_sequence;
	ut_state_at_recheck = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	return ut_recheck_ok && token != NULL
		   && memcmp(token, &ut_expected_admission, sizeof(*token)) == 0;
}

bool
cluster_semantic_activation_peer_open_matches(const ClusterSemanticAdmissionToken *token,
											  int32 authenticated_peer_node_id,
											  uint32 required_hello_caps,
											  uint32 sampled_capability_generation)
{
	int slot = ut_peer_open_calls++;

	if (slot < lengthof(ut_peer_open_peers)) {
		ut_peer_open_peers[slot] = authenticated_peer_node_id;
		ut_peer_open_required[slot] = required_hello_caps;
		ut_peer_open_generations[slot] = sampled_capability_generation;
		ut_peer_open_sequence[slot] = ++ut_sequence;
		ut_state_at_peer_open[slot] = pg_atomic_read_u32(&ut_cr_server_shared.slots[0].state);
	}
	return ut_peer_open_ok && token != NULL
		   && memcmp(token, &ut_expected_admission, sizeof(*token)) == 0
		   && required_hello_caps == UT_R4_REQUIRED_CAPABILITIES;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	ut_leave_calls++;
	ut_leave_sequence = ++ut_sequence;
	memset(&ut_left_admission, 0, sizeof(ut_left_admission));
	if (token != NULL) {
		ut_left_admission = *token;
		token->entered = false;
	}
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (ut_reclaim_race_on_proof && !ut_reclaim_race_injected && ut_lms_state != NULL
		&& lock == &ut_lms_state->lwlock && mode == LW_EXCLUSIVE) {
		ut_lms_state->r4_controls.data_worker_incarnation[0]++;
		ut_reclaim_race_injected = true;
	}
	ut_lock_acquire_count++;
	ut_lock = lock;
	ut_lock_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	ut_lock_release_count++;
	UT_ASSERT(lock == ut_lock);
	if (ut_watch_submit_lock && lock == &ut_lms_state->lwlock && ut_submit_slot != NULL) {
		ut_state_at_lock_release = pg_atomic_read_u32(&ut_submit_slot->state);
		ut_generation_at_lock_release = ut_submit_slot->r4.slot_generation;
		ut_owner_at_lock_release = ut_submit_slot->r4.owner;
		ut_lock_release_sequence = ++ut_sequence;
	}
}

static void
reset_fixture(ClusterLmsSharedState *state, ClusterLmsCrSlot *slot)
{
	ut_stop_new_work_allowed = true;
	ut_stop_new_work_calls = 0;
	memset(state, 0, sizeof(*state));
	memset(slot, 0, sizeof(*slot));
	pg_atomic_init_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	ut_lms_state = state;
	ut_lock_acquire_count = 0;
	ut_lock_release_count = 0;
	ut_lock = NULL;
	ut_lock_mode = LW_SHARED;
	ut_watch_submit_lock = false;
	ut_submit_slot = NULL;
}

static void
dirty_owner_stamp(ClusterLmsCrSlot *slot)
{
	memset(&slot->r4.owner, 0xa5, sizeof(slot->r4.owner));
}

static bool
owner_stamp_is_zero(const ClusterLmsCrSlot *slot)
{
	ClusterR4CrOwnerStamp zero;

	memset(&zero, 0, sizeof(zero));
	return memcmp(&slot->r4.owner, &zero, sizeof(zero)) == 0;
}

static BufferTag
submit_test_tag(void)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 20000;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 37;
	return tag;
}

/* Literal FORWARD96 fixture; no production encoder computes the expected bytes. */
static ClusterR4CrForwardPayload
submit_test_forward96(void)
{
	static const uint8 extension_bytes[32]
		= { 0x01, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
			0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x22,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
	ClusterR4CrForwardPayload forward;

	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = UT_REQUEST_ID;
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = submit_test_tag();
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
	forward.base.master_node = UT_MASTER_NODE;
	forward.base.transition_id = PCM_TRANS_N_TO_S;
	forward.base.expected_pi_watermark_scn_bytes[0] = 0x34;
	forward.base.expected_pi_watermark_scn_bytes[1] = 0x12;
	forward.base.reserved_0[4] = 1;
	memcpy(&forward.extension, extension_bytes, sizeof(extension_bytes));
	return forward;
}

static ClusterR4CrForwardPayload
submit_test_all_local_forward96(void)
{
	ClusterR4CrForwardPayload forward = submit_test_forward96();

	forward.base.original_requester_node = UT_HOLDER_NODE;
	forward.base.master_node = UT_HOLDER_NODE;
	return forward;
}

static ClusterSemanticAdmissionToken
submit_test_admission(void)
{
	ClusterSemanticAdmissionToken admission;

	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = UT_ACTIVATION_GENERATION;
	admission.formation_epoch = UT_FORMATION_EPOCH;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	return admission;
}

static void
reset_submit_fixture(ClusterLmsSharedState *state)
{
	ut_now = INT64CONST(1000000);
	ut_stop_new_work_allowed = true;
	ut_stop_new_work_calls = 0;
	ut_stop_poll_during_forget = false;
	memset(state, 0, sizeof(*state));
	memset(&ut_cr_server_shared, 0, sizeof(ut_cr_server_shared));
	memset(&ut_lms_latch, 0, sizeof(ut_lms_latch));
	ut_lms_state = state;
	ut_cr_server_region = NULL;
	ut_shmem_found = false;
	ut_data_worker_id = UT_WORKER_ID;
	ut_sequence = 0;
	ut_lock_acquire_count = 0;
	ut_lock_release_count = 0;
	ut_lock = NULL;
	ut_lock_mode = LW_SHARED;
	ut_lock_release_sequence = 0;
	ut_copy_sequence = 0;
	ut_enter_sequence = 0;
	ut_recheck_sequence = 0;
	memset(ut_peer_open_sequence, 0, sizeof(ut_peer_open_sequence));
	ut_leave_sequence = 0;
	ut_wake_sequence = 0;
	ut_watch_submit_lock = false;
	ut_submit_slot = NULL;
	ut_state_at_lock_release = UINT32_MAX;
	ut_generation_at_lock_release = 0;
	memset(&ut_owner_at_lock_release, 0, sizeof(ut_owner_at_lock_release));
	ut_copy_ok = true;
	ut_copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	ut_copy_calls = 0;
	memset(&ut_copy_tag, 0, sizeof(ut_copy_tag));
	ut_copy_expected_page_scn = InvalidScn;
	ut_enter_calls = 0;
	ut_enter_feature_bit = 0;
	ut_enter_side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	ut_recheck_ok = true;
	ut_recheck_calls = 0;
	ut_state_at_enter = UINT32_MAX;
	ut_state_at_recheck = UINT32_MAX;
	memset(ut_state_at_peer_open, 0xff, sizeof(ut_state_at_peer_open));
	ut_peer_open_ok = true;
	ut_peer_open_calls = 0;
	memset(ut_peer_open_peers, 0, sizeof(ut_peer_open_peers));
	memset(ut_peer_open_required, 0, sizeof(ut_peer_open_required));
	memset(ut_peer_open_generations, 0, sizeof(ut_peer_open_generations));
	ut_leave_calls = 0;
	memset(&ut_left_admission, 0, sizeof(ut_left_admission));
	ut_wake_calls = 0;
	ut_expected_admission = submit_test_admission();
	ut_builder_step_calls = 0;
	ut_builder_slot_index = UINT32_MAX;
	ut_builder_slot_generation = 0;
	ut_builder_foreign_ready = true;
	ut_builder_extension = NULL;
	ut_builder_result_page = NULL;
	ut_builder_foreign_page = NULL;
	ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
	ut_builder_step_reason = CLUSTER_CR_BUILD_NONE;
	ut_builder_publish_foreign_pause = false;
	ut_builder_throw_query_cancel = false;
	ut_builder_error_code = 0;
	ut_flush_error_calls = 0;
	CurrentMemoryContext = (MemoryContext)&ut_initial_memory_context_storage;
	ut_context_at_flush = NULL;
	ut_state_at_builder_step = UINT32_MAX;
	ut_pending_locator_ok = true;
	ut_pending_locator_calls = 0;
	ut_pending_locator_slot_index = UINT32_MAX;
	ut_pending_locator_slot_generation = 0;
	ut_state_at_pending_locator = UINT32_MAX;
	memset(&ut_pending_locator, 0, sizeof(ut_pending_locator));
	ut_pending_locator.uba.raw[0] = UINT64CONST(0x0000000900000101);
	ut_pending_locator.uba.raw[1] = UINT64CONST(0x0000000000060003);
	ut_pending_locator.xid = 797;
	ut_pending_locator.tt_wrap = 7;
	ut_pending_locator.itl_kind = ITL_FLAG_ACTIVE;
	ut_pending_locator.itl_slot_index = 0;
	ut_pgrd_resolve_ok = true;
	ut_pgrd_resolve_calls = 0;
	memset(&ut_pgrd_resolve_admission, 0, sizeof(ut_pgrd_resolve_admission));
	ut_pgrd_resolve_intent = CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL;
	ut_pgrd_resolve_owner_instance = 0;
	ut_pgrd_resolve_segment_id = 0;
	ut_pgrd_resolved_root = (ClusterUndoBlock0ResolvedRoot){
		.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED,
		.root_id = UINT64_C(33024),
		.root_generation = UINT64_C(1),
	};
	ut_current_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	ut_current_release_step = CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
	ut_current_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	ut_current_acquire_calls = 0;
	ut_current_acquire_poll_calls = 0;
	ut_current_sample_calls = 0;
	ut_current_release_calls = 0;
	ut_current_release_poll_calls = 0;
	ut_current_cancel_calls = 0;
	memset(&ut_current_logical, 0, sizeof(ut_current_logical));
	memset(&ut_current_sample_root, 0, sizeof(ut_current_sample_root));
	ut_current_sample_generation = UT_FOREIGN_PHYSICAL_GENERATION;
	ut_protocol_logs = 0;
	memset(ut_last_log, 0, sizeof(ut_last_log));
	ut_send_result = CLUSTER_IC_SEND_DONE;
	ut_send_calls = 0;
	ut_refusal_observe_calls = 0;
	ut_send_msg_type = 0;
	ut_send_dest = -1;
	ut_send_length = 0;
	ut_state_at_send = UINT32_MAX;
	memset(ut_send_payload, 0, sizeof(ut_send_payload));
	ut_note_send_calls = 0;
	ut_note_send_family = GCS_BLOCK_SEND_FAMILY_INVALIDATE;
	ut_note_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	ut_envelope_build_ok = true;
	ut_envelope_build_calls = 0;
	ut_envelope_build_sequence = 0;
	ut_envelope_msg_type = 0;
	ut_envelope_source = UINT32_MAX;
	ut_envelope_dest = UINT32_MAX;
	ut_envelope_payload_length = 0;
	ut_local_dispatch_ok = true;
	ut_local_dispatch_calls = 0;
	ut_local_dispatch_sequence = 0;
	ut_local_dispatch_peer = -1;
	ut_state_at_local_dispatch = UINT32_MAX;
	memset(ut_local_dispatch_payload, 0, sizeof(ut_local_dispatch_payload));
	ut_write_fence_enforcing = false;
	ut_write_fence_allowed = true;
	ut_checksum_calls = 0;
	ut_checksum_value = UINT32_C(0x1122aabb);
	ut_forget_calls = 0;
	ut_forget_slot_index = UINT32_MAX;
	ut_forget_slot_generation = 0;
	ut_close_peer_calls = 0;
	ut_tier1_close_calls = 0;
	ut_tier1_close_peer = -1;
	memset(ut_tier1_close_reason, 0, sizeof(ut_tier1_close_reason));
	ut_state_at_tier1_close = UINT32_MAX;
	ut_forget_calls_at_tier1_close = -1;
	ut_leave_calls_at_tier1_close = -1;
	ut_extract_ok = true;
	ut_extract_canonical_wrap = ut_pending_locator.tt_wrap;
	ut_extract_calls = 0;
	ut_state_at_extract = UINT32_MAX;
	memset(&ut_extract_locator, 0, sizeof(ut_extract_locator));
	ut_reclaim_race_on_proof = false;
	ut_reclaim_race_injected = false;
	ut_real_builder = false;
	cluster_cr_server_test_r4_reset_contexts();

	cluster_cr_server_shmem_register();
	UT_ASSERT_NOT_NULL(ut_cr_server_region);
	if (ut_cr_server_region == NULL)
		return;
	UT_ASSERT_EQ(ut_cr_server_region->size_fn(), MAXALIGN(sizeof(ut_cr_server_shared)));
	ut_cr_server_region->init_fn();
	state->r4_controls.data_worker_incarnation[UT_WORKER_ID] = UT_WORKER_INCARNATION;
	cluster_cr_server_publish_lms_latch(&ut_lms_latch);
	ut_submit_slot = &ut_cr_server_shared.slots[0];
	ut_watch_submit_lock = true;
}

static bool
bytes_are(const void *ptr, Size size, uint8 expected)
{
	const uint8 *bytes = (const uint8 *)ptr;
	Size i;

	for (i = 0; i < size; i++) {
		if (bytes[i] != expected)
			return false;
	}
	return true;
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 left = scn_local(a);
	uint64 right = scn_local(b);
	return (left > right) - (left < right);
}

int
cluster_conf_node_count(void)
{
	return 4;
}

static bool
peer_recheck_seen(int32 peer, uint32 generation)
{
	int i;

	for (i = 0; i < ut_peer_open_calls && i < lengthof(ut_peer_open_peers); i++) {
		if (ut_peer_open_peers[i] == peer && ut_peer_open_generations[i] == generation
			&& ut_peer_open_required[i] == UT_R4_REQUIRED_CAPABILITIES)
			return true;
	}
	return false;
}

static bool
slot_is_canonical_free_with_generation(const ClusterLmsCrSlot *slot, uint64 generation)
{
	ClusterLmsCrSlot expected;

	memset(&expected, 0, sizeof(expected));
	pg_atomic_init_u32(&expected.state, CLUSTER_LMS_CR_FREE);
	expected.r4.slot_generation = generation;
	return memcmp(slot, &expected, sizeof(expected)) == 0;
}

static void
prepare_reclaimable_r4_slot(ClusterLmsCrSlot *slot, uint32 state, uint64 generation, int worker_id)
{
	memset(slot, 0, sizeof(*slot));
	pg_atomic_init_u32(&slot->state, state);
	slot->r4.slot_generation = generation;
	slot->r4.owner.edge_owner_incarnation = UINT64_C(41) + (uint64)worker_id;
	slot->r4.owner.edge_owner_pid = 7000 + worker_id;
	slot->r4.owner.edge_owner_worker_id = (uint8)worker_id;
	slot->r4.owner.edge_owner_role = (uint8)(worker_id == 0 ? B_LMS : B_LMS_WORKER);
	if (state != CLUSTER_LMS_CR_FILLING)
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD;
}

static void
prepare_exact_drain_ack(ClusterLmsSharedState *state, uint64 worker_incarnation, uint64 generation)
{
	state->r4_controls.data_worker_incarnation[0] = worker_incarnation;
	state->r4_controls.drain_request_generation = generation;
	state->r4_controls.drain_ack_generation = generation;
}

static ClusterLmsCrSlot *
prepare_worker0_claim(ClusterLmsSharedState *state)
{
	ClusterR4CrForwardPayload forward = submit_test_forward96();
	ClusterSemanticAdmissionToken admission = submit_test_admission();
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;

	reset_submit_fixture(state);
	result = cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
									  UT_MASTER_CAPABILITY_GENERATION, &reason);
	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);

	state->r4_controls.data_worker_incarnation[0] = UT_WORKER0_INCARNATION;
	ut_data_worker_id = 0;
	ut_sequence = 0;
	ut_enter_sequence = 0;
	ut_recheck_sequence = 0;
	memset(ut_peer_open_sequence, 0, sizeof(ut_peer_open_sequence));
	ut_leave_sequence = 0;
	ut_wake_sequence = 0;
	ut_enter_calls = 0;
	ut_enter_feature_bit = 0;
	ut_enter_side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	ut_recheck_calls = 0;
	ut_state_at_enter = UINT32_MAX;
	ut_state_at_recheck = UINT32_MAX;
	memset(ut_state_at_peer_open, 0xff, sizeof(ut_state_at_peer_open));
	ut_peer_open_ok = true;
	ut_peer_open_calls = 0;
	memset(ut_peer_open_peers, 0, sizeof(ut_peer_open_peers));
	memset(ut_peer_open_required, 0, sizeof(ut_peer_open_required));
	memset(ut_peer_open_generations, 0, sizeof(ut_peer_open_generations));
	ut_leave_calls = 0;
	memset(&ut_left_admission, 0, sizeof(ut_left_admission));
	ut_wake_calls = 0;
	ut_watch_submit_lock = false;
	return &ut_cr_server_shared.slots[0];
}

static ClusterLmsCrSlot *
prepare_worker0_need_undo(ClusterLmsSharedState *state)
{
	ClusterLmsCrSlot *slot = prepare_worker0_claim(state);

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	ut_builder_step_result = CLUSTER_R4_CR_STEP_NEED_UNDO;
	ut_builder_step_reason = CLUSTER_CR_BUILD_NONE;
	ut_builder_publish_foreign_pause = true;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_NEED_UNDO);
	return slot;
}

static ClusterLmsCrSlot *
prepare_worker0_need_undo_frozen(ClusterLmsSharedState *state, uint32 physical_generation)
{
	ClusterLmsCrSlot *slot = prepare_worker0_need_undo(state);

	UT_ASSERT(cluster_cr_server_test_r4_freeze_foreign_generation(0, physical_generation));
	return slot;
}

static ClusterLmsCrSlot *
prepare_worker0_undo_inflight(ClusterLmsSharedState *state)
{
	ClusterLmsCrSlot *slot
		= prepare_worker0_need_undo_frozen(state, UT_FOREIGN_PHYSICAL_GENERATION);

	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);

	/* Measure the receive-side short TARGET episode independently from the
	 * retained builder episode that prepare_worker0_need_undo() entered. */
	ut_enter_calls = 0;
	ut_recheck_calls = 0;
	ut_leave_calls = 0;
	ut_enter_sequence = 0;
	ut_recheck_sequence = 0;
	ut_leave_sequence = 0;
	ut_wake_calls = 0;
	ut_wake_sequence = 0;
	ut_extract_calls = 0;
	ut_state_at_extract = UINT32_MAX;
	return slot;
}

static void
make_foreign_undo_reply(GcsBlockReplyHeader *header, ClusterGcsUndoAuthTrailer *auth,
						ClusterICEnvelope *env, char page[BLCKSZ])
{
	memset(header, 0, sizeof(*header));
	header->request_id = UINT64CONST(262144);
	header->page_lsn = UT_FOREIGN_LIVE_HWM;
	header->epoch = UT_FORMATION_EPOCH;
	header->checksum = ut_checksum_value;
	header->sender_node = 1;
	header->requester_backend_id = CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT;
	header->transition_id = (uint8)PCM_TRANS_N_TO_S;
	header->status = (uint8)GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(header, UT_FOREIGN_PHYSICAL_GENERATION));

	memset(page, 0, BLCKSZ);
	page[0] = (char)0xa5;
	memset(auth, 0, sizeof(*auth));
	ClusterGcsUndoAuthTrailerSetTtGeneration(auth, UT_FOREIGN_TT_GENERATION);
	ClusterGcsUndoAuthTrailerSetAuthorityScn(auth, (uint64)UT_FOREIGN_AUTHORITY_SCN);

	memset(env, 0, sizeof(*env));
	env->msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	env->source_node_id = 1;
	env->dest_node_id = UT_HOLDER_NODE;
	env->epoch = UT_FORMATION_EPOCH;
	env->payload_length = sizeof(*header) + BLCKSZ + sizeof(*auth);
}

static void
assert_exact_foreign_undo_forward96(void)
{
	static const uint8 extension_bytes[32]
		= { 0x01, 0x04, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x09, 0x00, 0x00,
			0x00, 0x03, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1d, 0x03,
			0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x04, 0x03, 0x02, 0x01 };
	const ClusterR4CrForwardPayload *forward = (const ClusterR4CrForwardPayload *)ut_send_payload;

	UT_ASSERT_EQ(ut_send_msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(ut_send_dest, 1);
	UT_ASSERT_EQ(ut_send_length, sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT_EQ(forward->base.request_id, UINT64CONST(262144));
	UT_ASSERT_EQ(forward->base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(forward->base.tag.spcOid, GCS_BLOCK_UNDO_FETCH_TAG_MAGIC);
	UT_ASSERT_EQ(forward->base.tag.dbOid, 257);
	UT_ASSERT_EQ(forward->base.tag.relNumber, 0);
	UT_ASSERT_EQ(forward->base.tag.forkNum, MAIN_FORKNUM);
	UT_ASSERT_EQ(forward->base.tag.blockNum, 9);
	UT_ASSERT_EQ(forward->base.original_requester_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(forward->base.requester_backend_id, -2);
	UT_ASSERT_EQ(forward->base.master_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(forward->base.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&forward->base), UT_READ_SCN);
	UT_ASSERT(bytes_are(forward->base.reserved_0, 6, 0));
	UT_ASSERT_EQ(forward->base.reserved_0[6], CLUSTER_R4_FORWARD_EXTENDED);
	UT_ASSERT_EQ(memcmp(&forward->extension, extension_bytes, sizeof(extension_bytes)), 0);
}

UT_TEST(test_free_to_pending_canonicalizes_owner_under_lms_lock)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);

	UT_ASSERT(cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_PENDING);
	UT_ASSERT(owner_stamp_is_zero(&slot));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
}

UT_TEST(test_free_to_filling_uses_same_proof_window)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);

	UT_ASSERT(cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_FILLING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_FILLING);
	UT_ASSERT(owner_stamp_is_zero(&slot));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_busy_slot_preserves_winner_owner_stamp)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;
	ClusterR4CrOwnerStamp before;

	reset_fixture(&state, &slot);
	pg_atomic_write_u32(&slot.state, CLUSTER_LMS_CR_R4_QUEUED);
	dirty_owner_stamp(&slot);
	before = slot.r4.owner;

	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT(memcmp(&slot.r4.owner, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_missing_lms_state_refuses_before_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;
	ClusterLmsCrSlot before;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);
	before = slot;
	ut_lms_state = NULL;

	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT(memcmp(&slot, &before, sizeof(slot)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);
}

/* Dropping the common proof lock, claiming before the complete producer
 * stamp, omitting either peer generation, copying the admission token into
 * shared state, or publishing before the stable copy/rechecks breaks this
 * real cluster_cr_server.c behavior. */
UT_TEST(test_r4_submit_publishes_complete_stable_copy_once)
{
	ClusterLmsSharedState state;
	ClusterR4CrForwardPayload forward = submit_test_forward96();
	ClusterSemanticAdmissionToken admission = submit_test_admission();
	ClusterSemanticAdmissionToken admission_before = admission;
	ClusterLmsCrSlot *slot;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;
	int i;

	reset_submit_fixture(&state);
	slot = &ut_cr_server_shared.slots[0];
	result = cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
									  UT_MASTER_CAPABILITY_GENERATION, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(memcmp(&admission, &admission_before, sizeof(admission)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(ut_state_at_lock_release, CLUSTER_LMS_CR_FILLING);
	UT_ASSERT_EQ(ut_generation_at_lock_release, 1);
	UT_ASSERT_EQ(ut_owner_at_lock_release.edge_owner_incarnation, UT_WORKER_INCARNATION);
	UT_ASSERT_EQ(ut_owner_at_lock_release.edge_owner_pid, MyProcPid);
	UT_ASSERT_EQ(ut_owner_at_lock_release.edge_owner_worker_id, UT_WORKER_ID);
	UT_ASSERT(ut_owner_at_lock_release.edge_owner_role != 0);
	UT_ASSERT_EQ(ut_owner_at_lock_release.builder_incarnation, 0);
	UT_ASSERT_EQ(ut_owner_at_lock_release.builder_pid, 0);
	UT_ASSERT_EQ(ut_owner_at_lock_release.builder_worker_id, 0);
	UT_ASSERT(
		bytes_are(ut_owner_at_lock_release.reserved, sizeof(ut_owner_at_lock_release.reserved), 0));

	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(slot->r4.slot_generation, 1);
	UT_ASSERT_EQ(slot->req_kind, CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD);
	UT_ASSERT_EQ(memcmp(&slot->tag, &forward.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT_EQ(slot->read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(slot->request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(slot->epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(slot->requester_node, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(slot->requester_backend, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(slot->reply_master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(slot->transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(memcmp(&slot->r4.route_proof.tag, &forward.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT_EQ(slot->r4.route_proof.read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(slot->r4.route_proof.formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(slot->r4.route_proof.activation_generation, UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(slot->r4.route_proof.master_authority_generation, UT_MASTER_GENERATION);
	UT_ASSERT_EQ(slot->r4.route_proof.master_resource_transition_count, UT_MASTER_TRANSITION);
	UT_ASSERT_EQ(slot->r4.route_proof.expected_page_scn, UT_EXPECTED_PAGE_SCN);
	UT_ASSERT_EQ(slot->r4.route_proof.real_master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(slot->r4.route_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->r4.requester_capability_generation, UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(slot->r4.master_capability_generation, UT_MASTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(slot->r4.foreign_request_id, 0);
	UT_ASSERT_EQ(slot->r4.origin_formation_epoch, 0);
	UT_ASSERT_EQ(slot->r4.origin_live_hwm_lsn, 0);
	UT_ASSERT_EQ(slot->r4.origin_tt_generation, 0);
	UT_ASSERT_EQ(slot->r4.origin_authority_scn, InvalidScn);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(slot->r4.flags, 0);
	UT_ASSERT(bytes_are(slot->r4.reserved, sizeof(slot->r4.reserved), 0));
	UT_ASSERT_EQ(slot->r4.owner.builder_incarnation, 0);
	UT_ASSERT_EQ(slot->r4.owner.builder_pid, 0);
	UT_ASSERT_EQ(slot->r4.owner.builder_worker_id, 0);
	UT_ASSERT(bytes_are(slot->r4.owner.reserved, sizeof(slot->r4.owner.reserved), 0));

	UT_ASSERT_EQ(ut_copy_calls, 1);
	UT_ASSERT_EQ(memcmp(&ut_copy_tag, &forward.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT_EQ(ut_copy_expected_page_scn, UT_EXPECTED_PAGE_SCN);
	UT_ASSERT_EQ(slot->r4.copied_page_lsn, UT_COPIED_PAGE_LSN);
	UT_ASSERT_EQ(slot->r4.copied_page_scn, UT_COPIED_PAGE_SCN);
	UT_ASSERT(bytes_are(slot->result_page, sizeof(slot->result_page), 0x5a));
	UT_ASSERT(bytes_are(slot->foreign_undo_page, sizeof(slot->foreign_undo_page), 0));
	UT_ASSERT_EQ(ut_recheck_calls, 1);
	UT_ASSERT_EQ(ut_peer_open_calls, 2);
	UT_ASSERT(peer_recheck_seen(UT_MASTER_NODE, UT_MASTER_CAPABILITY_GENERATION));
	UT_ASSERT(peer_recheck_seen(UT_REQUESTER_NODE, UT_REQUESTER_CAPABILITY_GENERATION));
	UT_ASSERT_EQ(ut_wake_calls, 1);
	UT_ASSERT(ut_lock_release_sequence < ut_copy_sequence);
	UT_ASSERT(ut_copy_sequence < ut_recheck_sequence);
	UT_ASSERT(ut_recheck_sequence < ut_wake_sequence);
	for (i = 0; i < ut_peer_open_calls && i < lengthof(ut_peer_open_sequence); i++) {
		UT_ASSERT(ut_copy_sequence < ut_peer_open_sequence[i]);
		UT_ASSERT(ut_peer_open_sequence[i] < ut_wake_sequence);
	}
	for (i = 1; i < CLUSTER_LMS_CR_SLOTS; i++) {
		UT_ASSERT_EQ(pg_atomic_read_u32(&ut_cr_server_shared.slots[i].state), CLUSTER_LMS_CR_FREE);
		UT_ASSERT_EQ(ut_cr_server_shared.slots[i].r4.slot_generation, 0);
	}
}

/* Local master/requester identities have no self HELLO record.  The real
 * submit must prove the compiled R4 family under this already-entered TARGET
 * token, preserve the exact widened OPEN generation in both uint32 carriers,
 * and publish only after the stable copy plus final token recheck. */
UT_TEST(test_r4_all_local_submit_uses_exact_token_generation_without_peer_matcher)
{
	ClusterLmsSharedState state;
	ClusterR4CrForwardPayload forward = submit_test_all_local_forward96();
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionToken admission_before;
	ClusterLmsCrSlot *slot;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;

	reset_submit_fixture(&state);
	ut_expected_admission.record_generation = UT_LOCAL_OPEN_GENERATION;
	admission = ut_expected_admission;
	admission_before = admission;
	slot = &ut_cr_server_shared.slots[0];

	result = cluster_lms_cr_submit_r4(&forward, &admission, (uint32)admission.record_generation,
									  (uint32)admission.record_generation, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(memcmp(&admission, &admission_before, sizeof(admission)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT_EQ(ut_state_at_lock_release, CLUSTER_LMS_CR_FILLING);
	UT_ASSERT_EQ(ut_generation_at_lock_release, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(slot->requester_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->reply_master_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->r4.route_proof.real_master_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->r4.route_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->r4.route_proof.activation_generation, admission.record_generation);
	UT_ASSERT_EQ((uint64)slot->r4.requester_capability_generation, admission.record_generation);
	UT_ASSERT_EQ((uint64)slot->r4.master_capability_generation, admission.record_generation);
	UT_ASSERT_EQ(ut_copy_calls, 1);
	UT_ASSERT_EQ(ut_recheck_calls, 1);
	UT_ASSERT_EQ(ut_state_at_recheck, CLUSTER_LMS_CR_FILLING);
	UT_ASSERT_EQ(ut_peer_open_calls, 0);
	UT_ASSERT_EQ(ut_enter_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT_EQ(ut_wake_calls, 1);
	UT_ASSERT(ut_lock_release_sequence < ut_copy_sequence);
	UT_ASSERT(ut_copy_sequence < ut_recheck_sequence);
	UT_ASSERT(ut_recheck_sequence < ut_wake_sequence);
}

/* Both local uint32 carriers are checked views of one entered uint64 OPEN
 * generation.  A nonzero but unequal value is protocol corruption and must
 * fail before the reservation proof window or any slot byte is touched. */
UT_TEST(test_r4_all_local_submit_refuses_generation_mismatch_before_slot_mutation)
{
	ClusterLmsSharedState state;
	ClusterR4CrForwardPayload forward = submit_test_all_local_forward96();
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionToken admission_before;
	ClusterLmsCrSlot before;
	ClusterLmsCrSlot *slot;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
	ClusterCrBuildResult result;

	reset_submit_fixture(&state);
	ut_expected_admission.record_generation = UT_LOCAL_OPEN_GENERATION;
	admission = ut_expected_admission;
	admission_before = admission;
	slot = &ut_cr_server_shared.slots[0];
	before = *slot;

	result = cluster_lms_cr_submit_r4(&forward, &admission, (uint32)admission.record_generation - 1,
									  (uint32)admission.record_generation, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(memcmp(&admission, &admission_before, sizeof(admission)), 0);
	UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);
	UT_ASSERT_EQ(ut_copy_calls, 0);
	UT_ASSERT_EQ(ut_recheck_calls, 0);
	UT_ASSERT_EQ(ut_peer_open_calls, 0);
	UT_ASSERT_EQ(ut_enter_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT_EQ(ut_wake_calls, 0);
}

/* The no-fetch copy ABI has a closed refusal domain.  Every failure consumes
 * exactly one reservation generation, but only the five transient holder-loss
 * reasons may become retryable; legacy/unknown values fail closed. */
UT_TEST(test_r4_submit_copy_refusal_mapping_is_closed)
{
	static const struct {
		ClusterBufmgrGcsCopyRefusal refusal;
		ClusterCrBuildResult result;
		ClusterCrBuildReason reason;
	} cases[]
		= { { CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INVALID_ARGUMENT, CLUSTER_CR_BUILD_FAIL_CLOSED,
			  CLUSTER_CR_BUILD_PROTOCOL },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE, CLUSTER_CR_BUILD_FAIL_CLOSED,
			  CLUSTER_CR_BUILD_PROTOCOL },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT, CLUSTER_CR_BUILD_FAIL_CLOSED,
			  CLUSTER_CR_BUILD_PROTOCOL },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED,
			  CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_PROTOCOL },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT, CLUSTER_CR_BUILD_FAIL_CLOSED,
			  CLUSTER_CR_BUILD_PROTOCOL },
			{ (ClusterBufmgrGcsCopyRefusal)(CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT + 1),
			  CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_PROTOCOL },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT, CLUSTER_CR_BUILD_RETRYABLE,
			  CLUSTER_CR_BUILD_HOLDER_MOVED },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID, CLUSTER_CR_BUILD_RETRYABLE,
			  CLUSTER_CR_BUILD_HOLDER_MOVED },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST, CLUSTER_CR_BUILD_RETRYABLE,
			  CLUSTER_CR_BUILD_HOLDER_MOVED },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND, CLUSTER_CR_BUILD_RETRYABLE,
			  CLUSTER_CR_BUILD_HOLDER_MOVED },
			{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_OWNERSHIP_REVOKE_BUSY, CLUSTER_CR_BUILD_RETRYABLE,
			  CLUSTER_CR_BUILD_HOLDER_MOVED } };
	ClusterR4CrForwardPayload forward = submit_test_forward96();
	ClusterSemanticAdmissionToken admission = submit_test_admission();
	ClusterSemanticAdmissionToken admission_before = admission;
	int i;

	for (i = 0; i < lengthof(cases); i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot;
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
		ClusterCrBuildResult result;

		reset_submit_fixture(&state);
		ut_copy_ok = false;
		ut_copy_refusal = cases[i].refusal;
		slot = &ut_cr_server_shared.slots[0];

		result = cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
										  UT_MASTER_CAPABILITY_GENERATION, &reason);

		UT_ASSERT_EQ(result, cases[i].result);
		UT_ASSERT_EQ(reason, cases[i].reason);
		UT_ASSERT_EQ(memcmp(&admission, &admission_before, sizeof(admission)), 0);
		UT_ASSERT_EQ(ut_lock_acquire_count, 1);
		UT_ASSERT_EQ(ut_lock_release_count, 1);
		UT_ASSERT_EQ(ut_state_at_lock_release, CLUSTER_LMS_CR_FILLING);
		UT_ASSERT_EQ(ut_generation_at_lock_release, 1);
		UT_ASSERT_EQ(ut_copy_calls, 1);
		UT_ASSERT_EQ(ut_recheck_calls, 0);
		UT_ASSERT_EQ(ut_peer_open_calls, 0);
		UT_ASSERT_EQ(ut_wake_calls, 0);
		UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	}
}

/* A final same-token failure happens after the stable image exists.  It must
 * consume the generation but make every unpublished byte reusable-zero,
 * publish FREE and never wake worker 0. */
UT_TEST(test_r4_submit_final_recheck_failure_canonicalizes_before_free)
{
	ClusterLmsSharedState state;
	ClusterR4CrForwardPayload forward = submit_test_forward96();
	ClusterSemanticAdmissionToken admission = submit_test_admission();
	ClusterSemanticAdmissionToken admission_before = admission;
	ClusterLmsCrSlot *slot;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
	ClusterCrBuildResult result;

	reset_submit_fixture(&state);
	ut_recheck_ok = false;
	slot = &ut_cr_server_shared.slots[0];
	result = cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
									  UT_MASTER_CAPABILITY_GENERATION, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_RF_DEFERRED);
	UT_ASSERT_EQ(memcmp(&admission, &admission_before, sizeof(admission)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT_EQ(ut_state_at_lock_release, CLUSTER_LMS_CR_FILLING);
	UT_ASSERT_EQ(ut_generation_at_lock_release, 1);
	UT_ASSERT_EQ(ut_copy_calls, 1);
	UT_ASSERT_EQ(ut_recheck_calls, 1);
	UT_ASSERT_EQ(ut_wake_calls, 0);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
}

/* A worker-0 admission that cannot re-prove either peer OPEN owns no slot
 * edge and leaves its just-entered process-local token exactly once. */
UT_TEST(test_r4_worker0_claim_peer_refusal_preserves_queued_slot)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot before;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	before = *slot;
	ut_peer_open_ok = false;
	UT_ASSERT(!cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(ut_enter_calls, 1);
	UT_ASSERT_EQ(ut_enter_feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(ut_enter_side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(ut_state_at_enter, CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(ut_peer_open_calls, 1);
	UT_ASSERT_EQ(ut_state_at_peer_open[0], CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT_EQ(memcmp(&ut_left_admission, &ut_expected_admission, sizeof(ut_expected_admission)),
				 0);
	UT_ASSERT(ut_enter_sequence < ut_peer_open_sequence[0]);
	UT_ASSERT(ut_peer_open_sequence[0] < ut_leave_sequence);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
	UT_ASSERT_EQ(ut_wake_calls, 0);
}

/* Claiming queued work is a fresh worker-0 episode: both owner halves and
 * the retained context bind the current incarnation before BUILDING is
 * published.  No builder step or terminal transport belongs to this unit. */
UT_TEST(test_r4_worker0_claim_rebinds_owner_and_retains_context)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_BUILDING);
	UT_ASSERT_EQ(slot->r4.slot_generation, 1);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_incarnation, UT_WORKER0_INCARNATION);
	UT_ASSERT_EQ(slot->r4.owner.builder_incarnation, UT_WORKER0_INCARNATION);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_pid, MyProcPid);
	UT_ASSERT_EQ(slot->r4.owner.builder_pid, MyProcPid);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_worker_id, 0);
	UT_ASSERT_EQ(slot->r4.owner.builder_worker_id, 0);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_role, B_LMS);
	UT_ASSERT(bytes_are(slot->r4.owner.reserved, sizeof(slot->r4.owner.reserved), 0));
	UT_ASSERT_EQ(ut_enter_calls, 1);
	UT_ASSERT_EQ(ut_peer_open_calls, 2);
	UT_ASSERT_EQ(ut_peer_open_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(ut_peer_open_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(ut_peer_open_required[0], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(ut_peer_open_required[1], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(ut_peer_open_generations[0], UT_MASTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(ut_peer_open_generations[1], UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(ut_state_at_peer_open[0], CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(ut_state_at_peer_open[1], CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
	UT_ASSERT(ut_enter_sequence < ut_peer_open_sequence[0]);
	UT_ASSERT(ut_peer_open_sequence[0] < ut_peer_open_sequence[1]);
	UT_ASSERT_EQ(ut_wake_calls, 0);
}

UT_TEST(test_r4_worker0_drain_requires_exact_empty_contexts)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	UT_ASSERT(cluster_cr_server_r4_worker0_drained());
	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_BUILDING);
	UT_ASSERT(!cluster_cr_server_r4_worker0_drained());

	/* A replacement worker owns no prior process's local context.  The stale
	 * shared slot remains LMON recovery work after the live-worker ACK. */
	cluster_cr_server_test_r4_reset_contexts();
	UT_ASSERT(cluster_cr_server_r4_worker0_drained());
}

UT_TEST(test_r4_worker0_drain_rejects_every_terminal_and_shipping_state)
{
	static const uint32 blocked_states[]
		= { CLUSTER_LMS_CR_R4_READY_FULL, CLUSTER_LMS_CR_R4_READY_RETRY,
			CLUSTER_LMS_CR_R4_READY_FAIL, CLUSTER_LMS_CR_R4_CANCELLED, CLUSTER_LMS_CR_R4_SHIPPING };
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot;
	int i;

	reset_submit_fixture(&state);
	slot = &ut_cr_server_shared.slots[0];
	UT_ASSERT(cluster_cr_server_r4_worker0_drained());
	for (i = 0; i < lengthof(blocked_states); i++) {
		pg_atomic_write_u32(&slot->state, blocked_states[i]);
		UT_ASSERT(!cluster_cr_server_r4_worker0_drained());
	}
	pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_R4_RECLAIMING);
	UT_ASSERT(cluster_cr_server_r4_worker0_drained());
}

UT_TEST(test_r4_lmon_reclaim_requires_exact_ack_then_canonicalizes_all_slots)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot0;
	ClusterLmsCrSlot *slot1;
	ClusterLmsCrSlot *slot2;
	ClusterR4CrOwnerStamp slot0_owner;

	reset_submit_fixture(&state);
	slot0 = &ut_cr_server_shared.slots[0];
	slot1 = &ut_cr_server_shared.slots[1];
	slot2 = &ut_cr_server_shared.slots[2];
	prepare_reclaimable_r4_slot(slot0, CLUSTER_LMS_CR_FILLING, 1, 2);
	slot0_owner = slot0->r4.owner;
	prepare_reclaimable_r4_slot(slot1, CLUSTER_LMS_CR_R4_QUEUED, 2, 3);
	memset(slot2, 0xa5, sizeof(*slot2));
	pg_atomic_init_u32(&slot2->state, CLUSTER_LMS_CR_R4_RECLAIMING);
	slot2->r4.slot_generation = 3;
	prepare_exact_drain_ack(&state, UINT64_C(8), UINT64_C(17));

	UT_ASSERT(cluster_cr_server_r4_lmon_reclaim_closed(UINT64_C(8), UINT64_C(17)));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(ut_state_at_lock_release, CLUSTER_LMS_CR_R4_RECLAIMING);
	UT_ASSERT_EQ(ut_generation_at_lock_release, UINT64_C(1));
	UT_ASSERT_EQ(memcmp(&ut_owner_at_lock_release, &slot0_owner, sizeof(slot0_owner)), 0);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot0, 1));
	UT_ASSERT(slot_is_canonical_free_with_generation(slot1, 2));
	UT_ASSERT(slot_is_canonical_free_with_generation(slot2, 3));
	UT_ASSERT(slot_is_canonical_free_with_generation(&ut_cr_server_shared.slots[3], 0));
}

UT_TEST(test_r4_lmon_reclaim_rechecks_ack_inside_exclusive_claim_window)
{
	ClusterLmsSharedState state;
	UtClusterCrServerShared before;
	ClusterLmsCrSlot *slot;

	reset_submit_fixture(&state);
	slot = &ut_cr_server_shared.slots[0];
	prepare_reclaimable_r4_slot(slot, CLUSTER_LMS_CR_R4_QUEUED, 1, 2);
	before = ut_cr_server_shared;
	prepare_exact_drain_ack(&state, UINT64_C(8), UINT64_C(17));
	ut_reclaim_race_on_proof = true;

	UT_ASSERT(!cluster_cr_server_r4_lmon_reclaim_closed(UINT64_C(8), UINT64_C(17)));
	UT_ASSERT(ut_reclaim_race_injected);
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_C(9));
	UT_ASSERT_EQ(memcmp(&ut_cr_server_shared, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
}

UT_TEST(test_r4_lmon_reclaim_refuses_unproved_or_legacy_slot_without_mutation)
{
	ClusterLmsSharedState state;
	UtClusterCrServerShared before;
	ClusterLmsCrSlot *slot;

	reset_submit_fixture(&state);
	slot = &ut_cr_server_shared.slots[0];
	prepare_reclaimable_r4_slot(slot, CLUSTER_LMS_CR_R4_QUEUED, 1, 2);
	before = ut_cr_server_shared;
	UT_ASSERT(!cluster_cr_server_r4_lmon_reclaim_closed(UINT64_C(8), UINT64_C(17)));
	UT_ASSERT(memcmp(&ut_cr_server_shared, &before, sizeof(before)) == 0);

	reset_submit_fixture(&state);
	slot = &ut_cr_server_shared.slots[0];
	pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_FILLING);
	before = ut_cr_server_shared;
	prepare_exact_drain_ack(&state, UINT64_C(8), UINT64_C(17));
	UT_ASSERT(!cluster_cr_server_r4_lmon_reclaim_closed(UINT64_C(8), UINT64_C(17)));
	UT_ASSERT(memcmp(&ut_cr_server_shared, &before, sizeof(before)) == 0);
}

/* A queued all-local identity is re-proved from worker 0's freshly entered
 * TARGET token, not from self HELLO.  Admission still precedes the owner/state
 * edge, and the exact token remains retained in the keyed builder context. */
UT_TEST(test_r4_worker0_claim_all_local_uses_token_without_peer_matcher)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	ut_expected_admission.record_generation = UT_LOCAL_OPEN_GENERATION;
	slot->requester_node = UT_HOLDER_NODE;
	slot->reply_master_node = UT_HOLDER_NODE;
	slot->r4.route_proof.real_master_node = UT_HOLDER_NODE;
	slot->r4.route_proof.activation_generation = UT_LOCAL_OPEN_GENERATION;
	slot->r4.requester_capability_generation = (uint32)UT_LOCAL_OPEN_GENERATION;
	slot->r4.master_capability_generation = (uint32)UT_LOCAL_OPEN_GENERATION;

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_BUILDING);
	UT_ASSERT_EQ(slot->requester_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(slot->reply_master_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ((uint64)slot->r4.requester_capability_generation,
				 ut_expected_admission.record_generation);
	UT_ASSERT_EQ((uint64)slot->r4.master_capability_generation,
				 ut_expected_admission.record_generation);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_incarnation, UT_WORKER0_INCARNATION);
	UT_ASSERT_EQ(slot->r4.owner.builder_incarnation, UT_WORKER0_INCARNATION);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_pid, MyProcPid);
	UT_ASSERT_EQ(slot->r4.owner.builder_pid, MyProcPid);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_worker_id, 0);
	UT_ASSERT_EQ(slot->r4.owner.builder_worker_id, 0);
	UT_ASSERT_EQ(slot->r4.owner.edge_owner_role, B_LMS);
	UT_ASSERT_EQ(ut_enter_calls, 1);
	UT_ASSERT_EQ(ut_enter_feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(ut_enter_side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(ut_state_at_enter, CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT_EQ(ut_peer_open_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
	UT_ASSERT_EQ(ut_wake_calls, 0);
}

UT_TEST(test_r4_worker0_build_step_publishes_ready_full)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(ut_builder_step_calls, 1);
	UT_ASSERT_EQ(ut_builder_slot_index, 0);
	UT_ASSERT_EQ(ut_builder_slot_generation, 1);
	UT_ASSERT(!ut_builder_foreign_ready);
	UT_ASSERT(ut_builder_extension == &slot->r4);
	UT_ASSERT(ut_builder_result_page == slot->result_page);
	UT_ASSERT(ut_builder_foreign_page == slot->foreign_undo_page);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
	UT_ASSERT_EQ(ut_leave_calls, 0);
}

/* A complete builder-owned foreign tuple must become visible before worker 0
 * release-publishes BUILDING->NEED_UNDO.  This step only yields the slot; it
 * does not send, forget, wake, or leave the retained TARGET episode. */
UT_TEST(test_r4_worker0_build_step_publishes_one_need_undo)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	ClusterR4CrSlotExtension expected;

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	expected = slot->r4;
	expected.foreign_request_id = UINT64CONST(262144);
	expected.foreign_uba.raw[0] = UINT64CONST(0x0000000900000101);
	expected.foreign_uba.raw[1] = UINT64CONST(0x0000000000060003);
	expected.origin_formation_epoch = UT_FORMATION_EPOCH;
	expected.foreign_origin_node = 1;
	expected.foreign_segment_id = 257;
	expected.foreign_block_no = 9;
	expected.foreign_xid = 797;
	expected.foreign_wrap = 7;
	expected.build_steps = 1;
	expected.foreign_tt_slot_offset = 3;
	expected.foreign_row_offset = 6;
	ut_builder_step_result = CLUSTER_R4_CR_STEP_NEED_UNDO;
	ut_builder_step_reason = CLUSTER_CR_BUILD_NONE;
	ut_builder_publish_foreign_pause = true;

	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(ut_builder_step_calls, 1);
	UT_ASSERT_EQ(ut_builder_slot_index, 0);
	UT_ASSERT_EQ(ut_builder_slot_generation, 1);
	UT_ASSERT(!ut_builder_foreign_ready);
	UT_ASSERT_EQ(ut_state_at_builder_step, CLUSTER_LMS_CR_R4_BUILDING);
	UT_ASSERT_EQ(memcmp(&slot->r4, &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_NEED_UNDO);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(bytes_are(slot->foreign_undo_page, BLCKSZ, 0));
	UT_ASSERT_EQ(ut_send_calls, 0);
	UT_ASSERT_EQ(ut_forget_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT_EQ(ut_wake_calls, 0);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
}

/* A query cancel raised by the immutable-page builder belongs to the exact
 * live worker-0 key.  The catch must publish only a zero-body CANCELLED
 * terminal; it retains this process's TARGET token until normal terminal
 * shipping performs the one leave and FREE transition. */
UT_TEST(test_r4_worker0_query_cancel_terminalizes_exact_building_slot)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	ClusterLmsCrSlot expected;
	GcsBlockReplyHeader *header;
	volatile bool caught = false;
	volatile bool returned = false;

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_BUILDING);
	memset(slot->result_page, 0xa5, sizeof(slot->result_page));
	memset(slot->foreign_undo_page, 0x5c, sizeof(slot->foreign_undo_page));
	expected = *slot;
	pg_atomic_write_u32(&expected.state, CLUSTER_LMS_CR_R4_CANCELLED);
	memset(expected.result_page, 0, sizeof(expected.result_page));
	memset(expected.foreign_undo_page, 0, sizeof(expected.foreign_undo_page));
	expected.r4.terminal_reason = (uint8)CLUSTER_CR_BUILD_CANCELLED;

	ut_builder_throw_query_cancel = true;
	PG_TRY();
	{
		returned = cluster_cr_server_test_r4_build_step(0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	ut_builder_throw_query_cancel = false;

	UT_ASSERT(!caught);
	UT_ASSERT(returned);
	UT_ASSERT(ut_context_at_flush == (MemoryContext)&ut_initial_memory_context_storage);
	UT_ASSERT(CurrentMemoryContext == (MemoryContext)&ut_initial_memory_context_storage);
	UT_ASSERT_EQ(ut_flush_error_calls, 1);
	UT_ASSERT_EQ(memcmp(slot, &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(ut_builder_step_calls, 1);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_forget_slot_index, 0);
	UT_ASSERT_EQ(ut_forget_slot_generation, 1);
	UT_ASSERT_EQ(ut_wake_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));

	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(ut_send_length, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	header = (GcsBlockReplyHeader *)ut_send_payload;
	UT_ASSERT_EQ(header->status, GCS_BLOCK_REPLY_R4_DENIED);
	UT_ASSERT_EQ(header->page_lsn, 0);
	UT_ASSERT(bytes_are(ut_send_payload + sizeof(*header), BLCKSZ, 0));
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
}

/* A cold holder sends the exact saved dependency without any foreign-header
 * pin or SCUR. The original origin owns selection of its physical generation. */
UT_TEST(test_r4_worker0_foreign_undo_requests_origin_selection_without_local_scur)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);
	const ClusterR4CrForwardPayload *forward;
	ClusterTxLocator locator;
	uint32 physical_generation = 0;

	ut_current_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	ut_current_release_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_pending_locator_calls, 1);
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(ut_note_send_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	forward = (const ClusterR4CrForwardPayload *)ut_send_payload;
	UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
		&forward->extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, &physical_generation));
	UT_ASSERT_EQ(physical_generation, UINT32_MAX);
	UT_ASSERT_EQ(memcmp(&locator, &ut_pending_locator, sizeof(locator)), 0);
	UT_ASSERT_EQ(ut_pgrd_resolve_calls, 0);
	UT_ASSERT_EQ(ut_current_acquire_calls, 0);
	UT_ASSERT_EQ(ut_current_acquire_poll_calls, 0);
	UT_ASSERT_EQ(ut_current_sample_calls, 0);
	UT_ASSERT_EQ(ut_current_release_calls, 0);
	UT_ASSERT_EQ(ut_current_release_poll_calls, 0);
	UT_ASSERT_EQ(ut_current_cancel_calls, 0);
	UT_ASSERT_EQ(ut_forget_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT_EQ(ut_protocol_logs, 0);
}

/* Generation zero is a real first physical generation, not the sentinel for
 * an unsampled request.  The process-local frozen bit distinguishes those
 * states and the existing subject_id carrier must preserve zero exactly. */
UT_TEST(test_r4_worker0_foreign_undo_sends_frozen_zero_generation)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_need_undo_frozen(&state, 0);
	const ClusterR4CrForwardPayload *forward;
	ClusterTxLocator locator;
	uint32 physical_generation = UINT32_MAX;

	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	forward = (const ClusterR4CrForwardPayload *)ut_send_payload;
	UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
		&forward->extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, &physical_generation));
	UT_ASSERT_EQ(physical_generation, 0);
	UT_ASSERT_EQ(memcmp(&locator, &ut_pending_locator, sizeof(locator)), 0);
}

UT_TEST(test_r4_worker0_foreign_undo_admitted_send_is_exact_and_one_shot)
{
	const ClusterICSendResult admitted_results[]
		= { CLUSTER_IC_SEND_DONE, CLUSTER_IC_SEND_WOULD_BLOCK };
	int i;

	for (i = 0; i < lengthof(admitted_results); i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot
			= prepare_worker0_need_undo_frozen(&state, UT_FOREIGN_PHYSICAL_GENERATION);

		ut_send_result = admitted_results[i];
		UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(ut_pending_locator_calls, 1);
		UT_ASSERT_EQ(ut_pending_locator_slot_index, 0);
		UT_ASSERT_EQ(ut_pending_locator_slot_generation, 1);
		UT_ASSERT_EQ(ut_state_at_pending_locator, CLUSTER_LMS_CR_R4_NEED_UNDO);
		UT_ASSERT_EQ(ut_send_calls, 1);
		UT_ASSERT_EQ(ut_state_at_send, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
		assert_exact_foreign_undo_forward96();
		UT_ASSERT_EQ(ut_note_send_calls, 1);
		UT_ASSERT_EQ(ut_note_send_family, GCS_BLOCK_SEND_FAMILY_FORWARD);
		UT_ASSERT_EQ(ut_note_send_result, admitted_results[i]);
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
		UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT_EQ(ut_tier1_close_calls, 0);
		UT_ASSERT_EQ(ut_forget_calls, 0);
		UT_ASSERT_EQ(ut_leave_calls, 0);
		UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
															&ut_expected_admission));
		UT_ASSERT(!cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(ut_pending_locator_calls, 1);
		UT_ASSERT_EQ(ut_send_calls, 1);
	}
}

/* A fresh TT slot uses wrap zero.  The holder must preserve that first-
 * generation identity in the exact kind-4 locator instead of classifying it
 * as an absent or malformed foreign request. */
UT_TEST(test_r4_worker0_foreign_undo_accepts_initial_tt_wrap)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot
		= prepare_worker0_need_undo_frozen(&state, UT_FOREIGN_PHYSICAL_GENERATION);
	const ClusterR4CrForwardPayload *forward;

	slot->r4.foreign_wrap = TT_WRAP_INITIAL;
	ut_pending_locator.tt_wrap = TT_WRAP_INITIAL;
	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_pending_locator_calls, 1);
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(ut_state_at_send, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
	forward = (const ClusterR4CrForwardPayload *)ut_send_payload;
	UT_ASSERT_EQ(forward->extension.kind.locator_bytes[20], 0);
	UT_ASSERT_EQ(forward->extension.kind.locator_bytes[21], 0);
}

/* TT_WRAP_INVALID is the one legal pre-origin sentinel.  It is transported
 * once with the physical segment generation; the origin must replace it with
 * the record's canonical durable wrap before status24 can publish READY. */
UT_TEST(test_r4_worker0_foreign_undo_sends_unresolved_wrap_for_origin_upgrade)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot
		= prepare_worker0_need_undo_frozen(&state, UT_FOREIGN_PHYSICAL_GENERATION);
	const ClusterR4CrForwardPayload *forward;

	slot->r4.foreign_wrap = TT_WRAP_INVALID;
	ut_pending_locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(ut_note_send_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_NONE);
	forward = (const ClusterR4CrForwardPayload *)ut_send_payload;
	UT_ASSERT_EQ(forward->extension.kind.locator_bytes[20], 0xff);
	UT_ASSERT_EQ(forward->extension.kind.locator_bytes[21], 0xff);
	UT_ASSERT_EQ(ut_tier1_close_calls, 0);
}

/* Transport refusal has a closed state mapping.  Capacity leaves the peer
 * live and publishes retry; a hard error synchronously closes the same DATA
 * peer while the slot is still INFLIGHT, then publishes fail-closed. */
UT_TEST(test_r4_worker0_foreign_undo_refusal_mapping_is_close_first)
{
	static const struct {
		ClusterICSendResult send_result;
		uint32 terminal_state;
		ClusterCrBuildReason reason;
		bool closes_peer;
	} cases[] = { { CLUSTER_IC_SEND_NOT_ADMITTED, CLUSTER_LMS_CR_R4_READY_RETRY,
					CLUSTER_CR_BUILD_CAPACITY, false },
				  { CLUSTER_IC_SEND_HARD_ERROR, CLUSTER_LMS_CR_R4_READY_FAIL,
					CLUSTER_CR_BUILD_PROTOCOL, true } };
	int i;

	for (i = 0; i < lengthof(cases); i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot
			= prepare_worker0_need_undo_frozen(&state, UT_FOREIGN_PHYSICAL_GENERATION);
		int fd = -1;
		bool down = false;
		bool wes_dirty = false;

		cluster_lms_data_plane_test_seed_peer(1, 81 + i, true, true, false);
		ut_send_result = cases[i].send_result;
		UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(ut_send_calls, 1);
		UT_ASSERT_EQ(ut_state_at_send, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
		assert_exact_foreign_undo_forward96();
		UT_ASSERT_EQ(ut_note_send_calls, 1);
		UT_ASSERT_EQ(ut_note_send_family, GCS_BLOCK_SEND_FAMILY_FORWARD);
		UT_ASSERT_EQ(ut_note_send_result, cases[i].send_result);
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), cases[i].terminal_state);
		UT_ASSERT_EQ(slot->r4.terminal_reason, cases[i].reason);
		UT_ASSERT_EQ(ut_tier1_close_calls, cases[i].closes_peer ? 1 : 0);
		UT_ASSERT_EQ(ut_close_peer_calls, 0);
		UT_ASSERT(cluster_lms_data_plane_test_peer_snapshot(1, &fd, &down, &wes_dirty));
		if (cases[i].closes_peer) {
			UT_ASSERT_EQ(ut_tier1_close_peer, 1);
			UT_ASSERT_EQ(ut_state_at_tier1_close, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
			UT_ASSERT_EQ(fd, -1);
			UT_ASSERT(down);
			UT_ASSERT(wes_dirty);
		} else {
			UT_ASSERT_EQ(fd, 81 + i);
			UT_ASSERT(!down);
			UT_ASSERT(!wes_dirty);
		}
		UT_ASSERT_EQ(ut_forget_calls, 0);
		UT_ASSERT_EQ(ut_leave_calls, 0);
		UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
															&ut_expected_admission));
		UT_ASSERT(!cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(ut_send_calls, 1);
	}
}

/* The process-local full locator is the non-recomputed carrier.  Any mismatch
 * with the shared frozen tuple fails before INFLIGHT/send and publishes the
 * existing PROTOCOL terminal without abandoning the builder context. */
UT_TEST(test_r4_worker0_foreign_undo_locator_mismatch_fails_before_send)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot
		= prepare_worker0_need_undo_frozen(&state, UT_FOREIGN_PHYSICAL_GENERATION);

	ut_pending_locator.xid = 798;
	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_pending_locator_calls, 1);
	UT_ASSERT_EQ(ut_state_at_pending_locator, CLUSTER_LMS_CR_R4_NEED_UNDO);
	UT_ASSERT_EQ(ut_send_calls, 0);
	UT_ASSERT_EQ(ut_note_send_calls, 0);
	UT_ASSERT_EQ(ut_tier1_close_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FAIL);
	UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(ut_forget_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
	UT_ASSERT(!cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(ut_pending_locator_calls, 1);
	UT_ASSERT_EQ(ut_send_calls, 0);
}

/* The authenticated status-24 reply is a worker-0 process-local correlation,
 * not a backend-table reply.  Removing any exact key/authority/record gate,
 * publishing READY before the copy, or leaving the retained builder token
 * makes this observable landing contract fail. */
UT_TEST(test_r4_worker0_status24_lands_exact_foreign_undo_and_short_admission)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	make_foreign_undo_reply(&header, &auth, &env, page);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));

	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_READY);
	UT_ASSERT_EQ(memcmp(slot->foreign_undo_page, page, BLCKSZ), 0);
	UT_ASSERT_EQ(slot->r4.origin_formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(slot->r4.origin_live_hwm_lsn, UT_FOREIGN_LIVE_HWM);
	UT_ASSERT_EQ(slot->r4.origin_tt_generation, UT_FOREIGN_TT_GENERATION);
	UT_ASSERT_EQ(slot->r4.origin_authority_scn, UT_FOREIGN_AUTHORITY_SCN);
	UT_ASSERT_EQ(slot->r4.foreign_wrap, ut_pending_locator.tt_wrap);
	UT_ASSERT_EQ(ut_extract_calls, 1);
	UT_ASSERT_EQ(ut_state_at_extract, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(memcmp(&ut_extract_locator, &ut_pending_locator, sizeof(ut_pending_locator)), 0);
	UT_ASSERT_EQ(ut_checksum_calls, 1);
	UT_ASSERT_EQ(ut_enter_calls, 1);
	UT_ASSERT_EQ(ut_enter_feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(ut_enter_side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(ut_state_at_enter, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(ut_recheck_calls, 1);
	UT_ASSERT_EQ(ut_state_at_recheck, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(ut_enter_sequence < ut_recheck_sequence);
	UT_ASSERT(ut_recheck_sequence < ut_leave_sequence);
	UT_ASSERT_EQ(ut_wake_calls, 1);
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
														&ut_expected_admission));
}

/* TT_WRAP_INVALID is the one legal unresolved request generation.  A valid
 * resident record upgrades it exactly once before READY becomes visible;
 * keeping the sentinel or rejecting this landing loses the approved D4
 * bootstrap path. */
UT_TEST(test_r4_worker0_status24_canonicalizes_invalid_wrap_before_ready)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	slot->r4.foreign_wrap = TT_WRAP_INVALID;
	ut_pending_locator.tt_wrap = TT_WRAP_INVALID;
	ut_extract_canonical_wrap = 7;
	UT_ASSERT(
		cluster_cr_server_test_r4_freeze_foreign_generation(0, UT_FOREIGN_PHYSICAL_GENERATION));
	pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	ut_enter_calls = 0;
	ut_recheck_calls = 0;
	ut_leave_calls = 0;
	ut_wake_calls = 0;
	ut_extract_calls = 0;

	make_foreign_undo_reply(&header, &auth, &env, page);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_READY);
	UT_ASSERT_EQ(slot->r4.foreign_wrap, 7);
	UT_ASSERT_EQ(ut_extract_calls, 1);
	UT_ASSERT_EQ(ut_extract_locator.tt_wrap, TT_WRAP_INVALID);
	UT_ASSERT_EQ(ut_enter_calls, 1);
	UT_ASSERT_EQ(ut_recheck_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT_EQ(ut_wake_calls, 1);
}

/* Every authenticated-wire, authority, record, process-local-key and state
 * mismatch is a pure drop.  In particular, no negative path may partially
 * stamp the page/authority tuple that UNDO_READY would publish. */
UT_TEST(test_r4_worker0_status24_mismatches_leave_shared_slot_unchanged)
{
	typedef enum RejectCase {
		REJECT_ENV_SOURCE,
		REJECT_ENV_DEST,
		REJECT_ENV_LENGTH,
		REJECT_STATUS,
		REJECT_ENDPOINT,
		REJECT_TRANSITION,
		REJECT_FORWARDING,
		REJECT_REQUEST_ID,
		REJECT_EPOCH,
		REJECT_ORIGIN,
		REJECT_PHYSICAL_GENERATION,
		REJECT_LIVE_HWM,
		REJECT_ENV_EPOCH,
		REJECT_AUTHORITY_ZERO,
		REJECT_AUTHORITY_BELOW_DEMAND,
		REJECT_CHECKSUM,
		REJECT_RECORD,
		REJECT_CANONICAL_WRAP,
		REJECT_SLOT_CONTEXT_KEY,
		REJECT_DIRTY_FOREIGN_PAGE,
		REJECT_FINAL_RECHECK,
		REJECT_WRONG_WORKER,
		REJECT_WORKER_INCARCATION,
		REJECT_WRONG_STATE
	} RejectCase;
	int i;

	for (i = REJECT_ENV_SOURCE; i <= REJECT_WRONG_STATE; i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
		ClusterLmsCrSlot before;
		ClusterSemanticAdmissionToken retained = ut_expected_admission;
		GcsBlockReplyHeader header;
		ClusterGcsUndoAuthTrailer auth;
		ClusterICEnvelope env;
		char page[BLCKSZ];

		make_foreign_undo_reply(&header, &auth, &env, page);
		switch ((RejectCase)i) {
		case REJECT_ENV_SOURCE:
			env.source_node_id = 2;
			break;
		case REJECT_ENV_DEST:
			env.dest_node_id = 2;
			break;
		case REJECT_ENV_LENGTH:
			env.payload_length--;
			break;
		case REJECT_STATUS:
			header.status = GCS_BLOCK_REPLY_R4_CR_FULL;
			break;
		case REJECT_ENDPOINT:
			header.requester_backend_id = UT_REQUESTER_BACKEND;
			break;
		case REJECT_TRANSITION:
			header.transition_id = PCM_TRANS_N_TO_X;
			break;
		case REJECT_FORWARDING:
			GcsBlockReplyHeaderSetForwardingMasterNode(&header, UT_MASTER_NODE);
			break;
		case REJECT_REQUEST_ID:
			header.request_id = UINT64CONST(8);
			break;
		case REJECT_EPOCH:
			header.epoch++;
			break;
		case REJECT_ORIGIN:
			header.sender_node = 2;
			env.source_node_id = 2;
			break;
		case REJECT_PHYSICAL_GENERATION:
			UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(&header,
															 UT_FOREIGN_PHYSICAL_GENERATION + 1));
			break;
		case REJECT_LIVE_HWM:
			header.page_lsn = InvalidXLogRecPtr;
			break;
		case REJECT_ENV_EPOCH:
			env.epoch++;
			break;
		case REJECT_AUTHORITY_ZERO:
			ClusterGcsUndoAuthTrailerSetAuthorityScn(&auth, InvalidScn);
			break;
		case REJECT_AUTHORITY_BELOW_DEMAND:
			ClusterGcsUndoAuthTrailerSetAuthorityScn(&auth, (uint64)(UT_READ_SCN - 1));
			break;
		case REJECT_CHECKSUM:
			header.checksum ^= UINT32_C(1);
			break;
		case REJECT_RECORD:
			ut_extract_ok = false;
			break;
		case REJECT_CANONICAL_WRAP:
			ut_extract_canonical_wrap++;
			break;
		case REJECT_SLOT_CONTEXT_KEY:
			slot->requester_backend++;
			break;
		case REJECT_DIRTY_FOREIGN_PAGE:
			slot->foreign_undo_page[BLCKSZ - 1] = 1;
			break;
		case REJECT_FINAL_RECHECK:
			ut_recheck_ok = false;
			break;
		case REJECT_WRONG_WORKER:
			ut_data_worker_id = 1;
			break;
		case REJECT_WORKER_INCARCATION:
			state.r4_controls.data_worker_incarnation[0]++;
			break;
		case REJECT_WRONG_STATE:
			pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_R4_UNDO_READY);
			break;
		}
		ut_checksum_calls = 0;
		before = *slot;
		UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
		UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(ut_wake_calls, 0);
		UT_ASSERT_EQ(ut_leave_calls, ut_enter_calls);
		if ((RejectCase)i != REJECT_SLOT_CONTEXT_KEY)
			UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, true, 1, UT_WORKER0_INCARNATION,
																&retained));
	}
}

UT_TEST(test_r4_worker0_remote_full_ship_transfers_frame_and_cleans_context)
{
	const ClusterICSendResult admitted_results[]
		= { CLUSTER_IC_SEND_DONE, CLUSTER_IC_SEND_WOULD_BLOCK };
	int i;

	for (i = 0; i < lengthof(admitted_results); i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
		GcsBlockReplyHeader *header;

		ut_send_result = admitted_results[i];
		UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
		UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
		UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));

		UT_ASSERT_EQ(ut_send_calls, 1);
		UT_ASSERT_EQ(ut_send_msg_type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
		UT_ASSERT_EQ(ut_send_dest, UT_REQUESTER_NODE);
		UT_ASSERT_EQ(ut_send_length, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
		header = (GcsBlockReplyHeader *)ut_send_payload;
		UT_ASSERT_EQ(header->request_id, UT_REQUEST_ID);
		UT_ASSERT_EQ(header->page_lsn, UT_COPIED_PAGE_LSN);
		UT_ASSERT_EQ(header->epoch, UT_FORMATION_EPOCH);
		UT_ASSERT_EQ(header->checksum, ut_checksum_value);
		UT_ASSERT_EQ(header->sender_node, UT_HOLDER_NODE);
		UT_ASSERT_EQ(header->requester_backend_id, UT_REQUESTER_BACKEND);
		UT_ASSERT_EQ(header->transition_id, PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ(header->status, GCS_BLOCK_REPLY_R4_CR_FULL);
		UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(header), UT_MASTER_NODE);
		UT_ASSERT(bytes_are(header->reserved_0, sizeof(header->reserved_0), 0));
		UT_ASSERT(bytes_are(ut_send_payload + sizeof(*header), BLCKSZ, 0x5a));
		UT_ASSERT_EQ(ut_checksum_calls, 1);
		UT_ASSERT_EQ(ut_forget_calls, 1);
		UT_ASSERT_EQ(ut_forget_slot_index, 0);
		UT_ASSERT_EQ(ut_forget_slot_generation, 1);
		UT_ASSERT_EQ(ut_leave_calls, 1);
		UT_ASSERT_EQ(
			memcmp(&ut_left_admission, &ut_expected_admission, sizeof(ut_expected_admission)), 0);
		UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
		UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
	}
}

/* A same-node requester is completed through the registered R4 reply
 * dispatcher.  It has no self HELLO and the generic IC sender's self no-op is
 * never used as a completion signal. */
UT_TEST(test_r4_worker0_local_full_uses_registered_dispatch_and_cleans_context)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	GcsBlockReplyHeader *header;

	ut_expected_admission.record_generation = UT_LOCAL_OPEN_GENERATION;
	slot->requester_node = UT_HOLDER_NODE;
	slot->reply_master_node = UT_HOLDER_NODE;
	slot->r4.route_proof.real_master_node = UT_HOLDER_NODE;
	slot->r4.route_proof.activation_generation = UT_LOCAL_OPEN_GENERATION;
	slot->r4.requester_capability_generation = (uint32)UT_LOCAL_OPEN_GENERATION;
	slot->r4.master_capability_generation = (uint32)UT_LOCAL_OPEN_GENERATION;

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));

	UT_ASSERT_EQ(ut_peer_open_calls, 0);
	UT_ASSERT_EQ(ut_send_calls, 0);
	UT_ASSERT_EQ(ut_envelope_build_calls, 1);
	UT_ASSERT_EQ(ut_envelope_msg_type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(ut_envelope_source, UT_HOLDER_NODE);
	UT_ASSERT_EQ(ut_envelope_dest, UT_HOLDER_NODE);
	UT_ASSERT_EQ(ut_envelope_payload_length, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(ut_local_dispatch_calls, 1);
	UT_ASSERT_EQ(ut_local_dispatch_peer, UT_HOLDER_NODE);
	UT_ASSERT_EQ(ut_state_at_local_dispatch, CLUSTER_LMS_CR_R4_SHIPPING);
	UT_ASSERT(ut_recheck_sequence < ut_envelope_build_sequence);
	UT_ASSERT(ut_envelope_build_sequence < ut_local_dispatch_sequence);
	UT_ASSERT(ut_local_dispatch_sequence < ut_leave_sequence);
	header = (GcsBlockReplyHeader *)ut_local_dispatch_payload;
	UT_ASSERT_EQ(header->request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(header->page_lsn, UT_COPIED_PAGE_LSN);
	UT_ASSERT_EQ(header->epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(header->checksum, ut_checksum_value);
	UT_ASSERT_EQ(header->sender_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(header->requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(header->transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(header->status, GCS_BLOCK_REPLY_R4_CR_FULL);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(header), UT_HOLDER_NODE);
	UT_ASSERT(bytes_are(header->reserved_0, sizeof(header->reserved_0), 0));
	UT_ASSERT(bytes_are(ut_local_dispatch_payload + sizeof(*header), BLCKSZ, 0x5a));
	UT_ASSERT_EQ(ut_checksum_calls, 1);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_forget_slot_index, 0);
	UT_ASSERT_EQ(ut_forget_slot_generation, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
}

/* Retry/fail terminals carry no page image and therefore remain deliverable
 * after the image fence closes.  Their exact build-reason polarity selects
 * status 25/26; neither may leak the stable-current result_page bytes. */
UT_TEST(test_r4_worker0_retry_and_fail_ship_zero_body_without_image_fence)
{
	static const struct {
		ClusterR4CrBuildStepResult step_result;
		ClusterCrBuildReason reason;
		uint32 terminal_state;
		GcsBlockReplyStatus status;
	} cases[] = { { CLUSTER_R4_CR_STEP_RETRY, CLUSTER_CR_BUILD_CAPACITY,
					CLUSTER_LMS_CR_R4_READY_RETRY, GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED },
				  { CLUSTER_R4_CR_STEP_FAIL, CLUSTER_CR_BUILD_PROTOCOL,
					CLUSTER_LMS_CR_R4_READY_FAIL, GCS_BLOCK_REPLY_R4_DENIED } };
	int i;

	for (i = 0; i < lengthof(cases); i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
		GcsBlockReplyHeader *header;

		ut_builder_step_result = cases[i].step_result;
		ut_builder_step_reason = cases[i].reason;
		ut_write_fence_enforcing = true;
		ut_write_fence_allowed = false;
		UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
		UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), cases[i].terminal_state);
		UT_ASSERT_EQ(slot->r4.terminal_reason, cases[i].reason);
		UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
		UT_ASSERT_EQ(ut_refusal_observe_calls, 1);
		UT_ASSERT_EQ(ut_refusal_observed_reason, cases[i].reason);

		UT_ASSERT_EQ(ut_send_calls, 1);
		UT_ASSERT_EQ(ut_send_msg_type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
		UT_ASSERT_EQ(ut_send_dest, UT_REQUESTER_NODE);
		UT_ASSERT_EQ(ut_send_length, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
		header = (GcsBlockReplyHeader *)ut_send_payload;
		UT_ASSERT_EQ(header->request_id, UT_REQUEST_ID);
		UT_ASSERT_EQ(header->page_lsn, 0);
		UT_ASSERT_EQ(header->epoch, UT_FORMATION_EPOCH);
		UT_ASSERT_EQ(header->checksum, ut_checksum_value);
		UT_ASSERT_EQ(header->sender_node, UT_HOLDER_NODE);
		UT_ASSERT_EQ(header->requester_backend_id, UT_REQUESTER_BACKEND);
		UT_ASSERT_EQ(header->transition_id, PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ(header->status, cases[i].status);
		UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(header), UT_MASTER_NODE);
		UT_ASSERT(bytes_are(header->reserved_0, sizeof(header->reserved_0), 0));
		UT_ASSERT(bytes_are(ut_send_payload + sizeof(*header), BLCKSZ, 0));
		UT_ASSERT_EQ(ut_checksum_calls, 1);
		UT_ASSERT_EQ(ut_forget_calls, 1);
		UT_ASSERT_EQ(ut_forget_slot_index, 0);
		UT_ASSERT_EQ(ut_forget_slot_generation, 1);
		UT_ASSERT_EQ(ut_leave_calls, 1);
		UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
		UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
	}
}

UT_TEST(test_data_plane_close_peer_now_is_same_process_and_idempotent)
{
	int fd = -1;
	bool down = false;
	bool wes_dirty = false;

	ut_tier1_close_calls = 0;
	ut_tier1_close_peer = -1;
	memset(ut_tier1_close_reason, 0, sizeof(ut_tier1_close_reason));
	cluster_lms_data_plane_test_seed_peer(UT_REQUESTER_NODE, 77, true, true, false);
	cluster_lms_data_plane_close_peer_now(UT_REQUESTER_NODE);
	UT_ASSERT_EQ(ut_tier1_close_calls, 1);
	UT_ASSERT_EQ(ut_tier1_close_peer, UT_REQUESTER_NODE);
	UT_ASSERT(strstr(ut_tier1_close_reason, "R4 undo data send hard error") != NULL);
	UT_ASSERT(cluster_lms_data_plane_test_peer_snapshot(UT_REQUESTER_NODE, &fd, &down, &wes_dirty));
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT(down);
	UT_ASSERT(wes_dirty);

	/* A defensive repeat must not call the non-idempotent tier1 close again. */
	cluster_lms_data_plane_close_peer_now(UT_REQUESTER_NODE);
	UT_ASSERT_EQ(ut_tier1_close_calls, 1);

	/* The thin API is legal only for a live remote peer on this DATA plane. */
	cluster_lms_data_plane_test_seed_peer(UT_REQUESTER_NODE, 78, true, false, false);
	cluster_lms_data_plane_close_peer_now(UT_REQUESTER_NODE);
	UT_ASSERT(cluster_lms_data_plane_test_peer_snapshot(UT_REQUESTER_NODE, &fd, &down, &wes_dirty));
	UT_ASSERT_EQ(fd, 78);
	UT_ASSERT(!down);
	UT_ASSERT(!wes_dirty);
	cluster_lms_data_plane_close_peer_now(cluster_node_id);
	cluster_lms_data_plane_close_peer_now(-1);
	UT_ASSERT_EQ(ut_tier1_close_calls, 1);
}

UT_TEST(test_r4_worker0_terminal_hard_error_closes_data_peer_before_cleanup)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	int fd = -1;
	bool down = false;
	bool wes_dirty = false;

	cluster_lms_data_plane_test_seed_peer(UT_REQUESTER_NODE, 79, true, true, false);
	ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
	ut_builder_step_reason = CLUSTER_CR_BUILD_NONE;
	ut_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));

	UT_ASSERT_EQ(ut_tier1_close_calls, 1);
	UT_ASSERT_EQ(ut_tier1_close_peer, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(ut_state_at_tier1_close, CLUSTER_LMS_CR_R4_SHIPPING);
	UT_ASSERT_EQ(ut_forget_calls_at_tier1_close, 0);
	UT_ASSERT_EQ(ut_leave_calls_at_tier1_close, 0);
	UT_ASSERT_EQ(ut_close_peer_calls, 0);
	UT_ASSERT(cluster_lms_data_plane_test_peer_snapshot(UT_REQUESTER_NODE, &fd, &down, &wes_dirty));
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT(down);
	UT_ASSERT(wes_dirty);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	UT_ASSERT(cluster_cr_server_test_r4_context_matches(0, false, 0, 0, NULL));
}

static char *
read_source(void)
{
	FILE *fp;
	char *contents;
	long length;

	fp = fopen(CR_SERVER_SOURCE_PATH, "rb");
	if (fp == NULL || fseek(fp, 0, SEEK_END) != 0)
		return NULL;
	length = ftell(fp);
	if (length <= 0 || fseek(fp, 0, SEEK_SET) != 0) {
		fclose(fp);
		return NULL;
	}
	contents = malloc((size_t)length + 1);
	if (contents == NULL || fread(contents, 1, (size_t)length, fp) != (size_t)length) {
		free(contents);
		fclose(fp);
		return NULL;
	}
	fclose(fp);
	contents[length] = '\0';
	return contents;
}

static bool
function_uses_common_reserver(const char *source, const char *symbol, const char *next_symbol)
{
	char start_marker[128];
	char end_marker[128];
	const char *start;
	const char *end;
	const char *call;

	if (source == NULL)
		return false;
	snprintf(start_marker, sizeof(start_marker), "\n%s(", symbol);
	snprintf(end_marker, sizeof(end_marker), "\n%s(", next_symbol);
	start = strstr(source, start_marker);
	end = start != NULL ? strstr(start + 1, end_marker) : NULL;
	call = start != NULL ? strstr(start, "cr_server_reserve_legacy_slot(slot,") : NULL;
	return start != NULL && end != NULL && call != NULL && call < end;
}

UT_TEST(test_all_four_legacy_submitters_use_common_reserver)
{
	char *source = read_source();

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_cr_submit",
												"cluster_lms_cr_submit_r4"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_fetch_submit",
												"cluster_lms_undo_verdict_submit"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_verdict_submit",
												"cluster_lms_undo_multi_verdict_submit"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_multi_verdict_submit",
												"lms_undo_fetch_serve"));
	}
	free(source);
}

UT_TEST(test_r4_worker0_ready_undo_resumes_and_releases_original_owner)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];
	int enters;

	make_foreign_undo_reply(&header, &auth, &env, page);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	enters = ut_enter_calls;
	ut_builder_publish_foreign_pause = false;
	ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT(ut_builder_foreign_ready);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT_EQ(ut_state_at_builder_step, CLUSTER_LMS_CR_R4_BUILDING);
	UT_ASSERT_EQ(ut_enter_calls, enters);
	UT_ASSERT(bytes_are(slot->foreign_undo_page, BLCKSZ, 0));
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 2); /* short receive + original builder */
	UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
}

UT_TEST(test_r4_worker0_ready_undo_error_uses_original_terminal_cleanup)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	make_foreign_undo_reply(&header, &auth, &env, page);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	ut_builder_throw_query_cancel = true;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT(ut_builder_foreign_ready);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_CANCELLED);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT(bytes_are(slot->foreign_undo_page, BLCKSZ, 0));
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 2);
}

UT_TEST(test_r4_worker0_origin_refusal_uses_exact_original_terminal_owner)
{
	int variant;
	for (variant = 0; variant < 2; variant++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
		GcsBlockReplyHeader header;
		ClusterGcsUndoAuthTrailer auth;
		ClusterICEnvelope env;
		char page[BLCKSZ];

		make_foreign_undo_reply(&header, &auth, &env, page);
		header.status
			= variant ? GCS_BLOCK_REPLY_R4_DENIED : GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
		header.page_lsn = 0;
		memset(header.reserved_0, 0, sizeof(header.reserved_0));
		memset(page, 0, sizeof(page));
		env.payload_length = GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE;
		UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, NULL));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state),
					 variant ? CLUSTER_LMS_CR_R4_READY_FAIL : CLUSTER_LMS_CR_R4_READY_RETRY);
		UT_ASSERT_EQ(ut_extract_calls, 0);
		UT_ASSERT(bytes_are(slot->foreign_undo_page, BLCKSZ, 0));
		UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
		UT_ASSERT_EQ(ut_forget_calls, 1);
		UT_ASSERT_EQ(ut_leave_calls, 2);
		UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, NULL));
	}
}

UT_TEST(test_r4_worker0_initial_tt_rollover_counter_is_proven_zero)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	make_foreign_undo_reply(&header, &auth, &env, page);
	ClusterGcsUndoAuthTrailerSetTtGeneration(&auth, 0);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	UT_ASSERT_EQ(slot->r4.origin_tt_generation, 0);
	ut_builder_publish_foreign_pause = false;
	ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
}

UT_TEST(test_r4_worker0_authority_cover_uses_logical_cross_origin_scn)
{
	int variant;
	for (variant = 0; variant < 2; variant++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
		ClusterLmsCrSlot before;
		GcsBlockReplyHeader header;
		ClusterGcsUndoAuthTrailer auth;
		ClusterICEnvelope env;
		char page[BLCKSZ];

		make_foreign_undo_reply(&header, &auth, &env, page);
		slot->read_scn = slot->r4.route_proof.read_scn = ((SCN)1 << 56) | (SCN)100;
		ClusterGcsUndoAuthTrailerSetAuthorityScn(&auth,
												 variant ? (((SCN)3 << 56) | (SCN)99) : (SCN)101);
		before = *slot;
		if (variant) {
			UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
			UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
		} else {
			UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
			ut_builder_publish_foreign_pause = false;
			ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
			UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
			UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
			UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
		}
	}
}

UT_TEST(test_r4_worker0_foreign_dependency_preserves_current_zero_epoch_open)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	/* Establish the original worker0 token in the ordinary current OPEN
	 * formation; changing the token after claim would be a stale test. */
	ut_expected_admission.formation_epoch = 0;
	slot->epoch = slot->r4.route_proof.formation_epoch = 0;
	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	ut_builder_step_result = CLUSTER_R4_CR_STEP_NEED_UNDO;
	ut_builder_step_reason = CLUSTER_CR_BUILD_NONE;
	ut_builder_publish_foreign_pause = true;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT(
		cluster_cr_server_test_r4_freeze_foreign_generation(0, UT_FOREIGN_PHYSICAL_GENERATION));
	UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	make_foreign_undo_reply(&header, &auth, &env, page);
	header.epoch = env.epoch = 0;
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	ut_builder_publish_foreign_pause = false;
	ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
}

UT_TEST(test_worker0_real_builder_two_foreign_updates_complete_same_owned_continuation)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	Page page = slot->result_page;
	PageHeader phdr = (PageHeader)page;
	char old_images[2][32];
	GcsBlockReplyHeader previous_header;
	ClusterGcsUndoAuthTrailer previous_auth;
	ClusterICEnvelope previous_env;
	PGAlignedBlock previous_page;
	int i;

	ut_real_builder = true;
	memset(page, 0, BLCKSZ);
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	phdr->pd_flags = PD_HAS_ITL;
	phdr->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	phdr->pd_lower = SizeOfPageHeaderData + 4 * sizeof(ItemIdData);
	phdr->pd_upper = phdr->pd_special - 4 * 32;
	for (i = 0; i < 2; i++) {
		OffsetNumber old_off = (OffsetNumber)(1 + i * 2), new_off = old_off + 1;
		TransactionId xid = 797 + i;
		HeapTupleHeader old_image = (HeapTupleHeader)old_images[i];
		HeapTupleHeader current, replacement;
		ClusterItlSlotData *itl = &ClusterPageGetItlSlots(page)[i];
		uint16 old_pos = phdr->pd_upper + i * 64, new_pos = old_pos + 32;
		ItemIdSetNormal(PageGetItemId(page, old_off), old_pos, 32);
		ItemIdSetNormal(PageGetItemId(page, new_off), new_pos, 32);
		memset(old_image, 0, 32);
		HeapTupleHeaderSetXmin(old_image, 796);
		HeapTupleHeaderSetXmax(old_image, InvalidTransactionId);
		old_image->t_infomask = HEAP_XMAX_INVALID;
		old_image->t_hoff = SizeofHeapTupleHeader;
		old_image->t_itl_slot_idx = (uint8)i;
		ItemPointerSet(&old_image->t_ctid, slot->tag.blockNum, old_off);
		current = (HeapTupleHeader)(page + old_pos);
		memcpy(current, old_image, 32);
		HeapTupleHeaderSetXmax(current, xid);
		current->t_infomask &= ~HEAP_XMAX_INVALID;
		ItemPointerSet(&current->t_ctid, slot->tag.blockNum, new_off);
		replacement = (HeapTupleHeader)(page + new_pos);
		memcpy(replacement, old_image, 32);
		HeapTupleHeaderSetXmin(replacement, xid);
		ItemPointerSet(&replacement->t_ctid, slot->tag.blockNum, new_off);
		itl->xid = xid;
		itl->wrap = 1; /* Page ABA is deliberately not canonical TT wrap7. */
		itl->flags = ITL_FLAG_ACTIVE;
		itl->write_scn = UT_READ_SCN + 10 + i;
		itl->undo_segment_head = uba_encode(257, 9 - i, 3, 6);
	}
	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	for (i = 0; i < 2; i++) {
		PGAlignedBlock undo_page;
		UndoBlockHeader *undo_header = (UndoBlockHeader *)undo_page.data;
		UndoRecordHeader *record = (UndoRecordHeader *)(undo_page.data + sizeof(*undo_header));
		UndoUpdatePayload *payload = (UndoUpdatePayload *)(record + 1);
		UndoSlotDirEntry *directory;
		GcsBlockReplyHeader header;
		ClusterGcsUndoAuthTrailer auth;
		ClusterICEnvelope env;
		ClusterLmsCrSlot before;
		int tuple_index = 1 - i;
		int phase;

		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_NEED_UNDO);
		UT_ASSERT_EQ(slot->r4.foreign_request_id, UINT64_C(262144) + i * 4);
		ut_current_sample_generation = UT_FOREIGN_PHYSICAL_GENERATION + i;
		for (phase = 0;
			 phase < 4 && pg_atomic_read_u32(&slot->state) == CLUSTER_LMS_CR_R4_NEED_UNDO; phase++)
			(void)cluster_cr_server_test_r4_send_foreign_undo(0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
		if (i != 0) {
			before = *slot;
			UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&previous_env, &previous_header,
															  previous_page.data, &previous_auth));
			UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
		}
		make_foreign_undo_reply(&header, &auth, &env, undo_page.data);
		memset(undo_page.data, 0, BLCKSZ);
		undo_header->magic = PGRAC_UNDO_BLOCK_MAGIC;
		undo_header->block_version = UNDO_BLOCK_VERSION_1;
		undo_header->slot_count = 7;
		undo_header->free_offset = sizeof(*undo_header) + sizeof(*record) + sizeof(*payload) + 32;
		record->record_type = UNDO_RECORD_UPDATE;
		record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
		record->payload_length = sizeof(*payload) + 32;
		record->xid = 797 + tuple_index;
		record->origin_node_id = 1;
		record->tt_slot_segment_id = 257;
		record->tt_slot_id = 4;
		record->tt_wrap_plus1 = 8;
		record->write_scn = UT_READ_SCN + 10 + tuple_index;
		record->target_locator.spcOid = slot->tag.spcOid;
		record->target_locator.dbOid = slot->tag.dbOid;
		record->target_locator.relNumber = slot->tag.relNumber;
		record->target_fork = slot->tag.forkNum;
		record->target_block = slot->tag.blockNum;
		record->target_offset = 1 + tuple_index * 2;
		payload->new_block = slot->tag.blockNum;
		payload->new_offset = record->target_offset + 1;
		payload->old_tuple_length = 32;
		payload->old_tuple_offset = sizeof(*payload);
		memcpy(payload + 1, old_images[tuple_index], 32);
		directory = UNDO_SLOT_DIR_PTR(undo_page.data, 6);
		directory->record_offset = sizeof(*undo_header);
		directory->record_length = sizeof(*record) + record->payload_length;
		directory->record_type = record->record_type;
		directory->flags = record->flags;
		header.request_id = slot->r4.foreign_request_id;
		UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(&header, ut_current_sample_generation));
		UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, undo_page.data, &auth));
		previous_header = header;
		previous_auth = auth;
		previous_env = env;
		previous_page = undo_page;
		UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
		UT_ASSERT(ut_builder_foreign_ready);
		UT_ASSERT(bytes_are(slot->foreign_undo_page, BLCKSZ, 0));
	}
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
	UT_ASSERT_EQ(ut_current_sample_calls, 0);
	for (i = 0; i < 2; i++) {
		ItemId old_item = PageGetItemId(page, 1 + i * 2);
		UT_ASSERT(ItemIdIsNormal(old_item));
		UT_ASSERT_EQ(memcmp(PageGetItem(page, old_item), old_images[i], 32), 0);
		UT_ASSERT(!ItemIdIsNormal(PageGetItemId(page, 2 + i * 2)));
		UT_ASSERT_EQ(ClusterPageGetItlSlots(page)[i].wrap, 1);
	}
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
	UT_ASSERT(!continuation_real_pending(0, 1, &ut_pending_locator));
}

UT_TEST(test_foreign_undo_cold_holder_reaches_origin_without_resident_header)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);
	int phase;

	/* A lock grant does not populate a foreign resident header. The actual
	 * sender must reach its origin even when that local sampler has no page. */
	ut_current_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	for (phase = 0; phase < 4 && pg_atomic_read_u32(&slot->state) == CLUSTER_LMS_CR_R4_NEED_UNDO;
		 phase++)
		(void)cluster_cr_server_test_r4_send_foreign_undo(0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(ut_current_acquire_calls, 0);
	UT_ASSERT_EQ(ut_current_sample_calls, 0);
}

UT_TEST(test_cold_foreign_landing_freezes_only_the_validated_dependency)
{
	for (int variant = 0; variant < 5; variant++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);
		ClusterLmsCrSlot before;
		GcsBlockReplyHeader header;
		GcsBlockReplyHeader rejected;
		ClusterGcsUndoAuthTrailer auth;
		ClusterICEnvelope env;
		char page[BLCKSZ];

		ut_current_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
		UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
		make_foreign_undo_reply(&header, &auth, &env, page);
		UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(&header, variant == 0 ? 0 : 17));
		rejected = header;
		if (variant != 0) {
			if (variant == 1)
				memset(rejected.reserved_0, 0xff, 4);
			else if (variant == 2)
				rejected.checksum ^= 1;
			else if (variant == 3)
				ut_extract_ok = false;
			else
				rejected.request_id += 4;
			before = *slot;
			UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &rejected, page, &auth));
			UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
			ut_extract_ok = true;
		}
		UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
		before = *slot;
		UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
		UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
		ut_builder_publish_foreign_pause = false;
		ut_builder_step_result = CLUSTER_R4_CR_STEP_FULL;
		UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FULL);
		UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
		UT_ASSERT_EQ(ut_current_acquire_calls, 0);
		UT_ASSERT_EQ(ut_current_sample_calls, 0);
	}
}

UT_TEST(test_r4_protocol_detail_distinguishes_actual_request_and_send_failures)
{
	const char *sites[] = { "PGRAC_SITE=FROZEN_REQUEST", "PGRAC_SITE=FORWARD_SEND" };

	for (int leg = 0; leg < lengthof(sites); leg++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);

		if (leg == 0)
			ut_pending_locator.xid++;
		else
			ut_send_result = CLUSTER_IC_SEND_HARD_ERROR;
		UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FAIL);
		UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_PROTOCOL);
		UT_ASSERT_EQ(ut_send_calls, leg == 0 ? 0 : 1);
		UT_ASSERT_EQ(ut_current_cancel_calls, 0);
		UT_ASSERT_EQ(ut_protocol_logs, 1);
		UT_ASSERT(strstr(ut_last_log, sites[leg]) != NULL);
		UT_ASSERT(strstr(ut_last_log, "PGRAC_FAMILY=R4_PROTOCOL") != NULL);
		UT_ASSERT_EQ(ut_current_acquire_calls, 0);
		UT_ASSERT_EQ(ut_current_sample_calls, 0);
		UT_ASSERT_EQ(ut_current_release_calls, 0);
	}
}

UT_TEST(test_r4_protocol_detail_budget_never_changes_terminal)
{
	for (int i = 0; i < 70; i++) {
		ClusterLmsSharedState state;
		ClusterLmsCrSlot *slot = prepare_worker0_need_undo(&state);

		ut_pending_locator.xid++;
		UT_ASSERT(cluster_cr_server_test_r4_send_foreign_undo(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_FAIL);
		UT_ASSERT_EQ(slot->r4.terminal_reason, CLUSTER_CR_BUILD_PROTOCOL);
		UT_ASSERT_EQ(ut_send_calls, 0);
		UT_ASSERT_EQ(ut_current_cancel_calls, 0);
	}
	UT_ASSERT_EQ(ut_protocol_total_logs, 64);
	UT_ASSERT_EQ(ut_protocol_logs, 0);
}

UT_TEST(test_normal_stop_cr_requires_initialized_actual_owner)
{
	ClusterLmsSharedState state;
	const char *reason;
	int slot;

	/* This test runs before any fixture attaches the production pointer. */
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(strcmp(reason, "CR_UNINITIALIZED"), 0);
	reset_submit_fixture(&state);
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_LMS;
}

UT_TEST(test_normal_stop_cr_all_owned_states_not_recovery_drain)
{
	ClusterLmsSharedState state;
	UtClusterCrServerShared before;
	const char *reason;
	int slot;

	reset_submit_fixture(&state);
	for (uint32 phase = CLUSTER_LMS_CR_PENDING; phase <= CLUSTER_LMS_CR_R4_RECLAIMING; phase++) {
		pg_atomic_write_u32(&ut_cr_server_shared.slots[0].state, phase);
		memcpy(&before, &ut_cr_server_shared, sizeof(before));
		UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(slot, 0);
		UT_ASSERT_EQ(strcmp(reason, "CR_SLOT_OWNED"), 0);
		UT_ASSERT_EQ(memcmp(&before, &ut_cr_server_shared, sizeof(before)), 0);
	}
	/* The old recovery predicate explicitly accepts RECLAIMING. */
	UT_ASSERT(cluster_cr_server_r4_worker0_drained());
	pg_atomic_write_u32(&ut_cr_server_shared.slots[3].state, UINT32_MAX);
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(slot, 3);
	UT_ASSERT_EQ(strcmp(reason, "CR_SLOT_STATE"), 0);
}

UT_TEST(test_normal_stop_cr_original_terminal_must_release_local_context)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);
	const char *reason;
	int index;

	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&index, &reason), CLUSTER_NORMAL_STOP_PENDING);
	/* A final seal forbids a new context, not this admitted completion. */
	ut_stop_new_work_allowed = false;
	ut_stop_new_work_calls = 0;
	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&index, &reason), CLUSTER_NORMAL_STOP_PENDING);
	ut_stop_poll_during_forget = true;
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	ut_stop_poll_during_forget = false;
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&index, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_stop_new_work_calls, 0);
	/* A lawful new reservation reopens debt; no cached READY. */
	ut_stop_new_work_allowed = true;
	ut_watch_submit_lock = false;
	UT_ASSERT(cluster_cr_server_test_reserve_legacy_slot(slot, CLUSTER_LMS_CR_FILLING));
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&index, &reason), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_normal_stop_cr_legacy_seal_precedes_owner_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot, before;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);
	slot.r4.slot_generation = 19;
	memcpy(&before, &slot, sizeof(before));
	ut_stop_new_work_allowed = false;
	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(ut_stop_new_work_calls, 1);
	UT_ASSERT_EQ(memcmp(&slot, &before, sizeof(slot)), 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, ut_lock_release_count);

	/* A busy slot remains its original owner's responsibility, not a new one. */
	pg_atomic_write_u32(&slot.state, CLUSTER_LMS_CR_FILLING);
	memcpy(&before, &slot, sizeof(before));
	ut_stop_new_work_calls = 0;
	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(ut_stop_new_work_calls, 0);
	UT_ASSERT_EQ(memcmp(&slot, &before, sizeof(slot)), 0);
}

UT_TEST(test_normal_stop_cr_r4_seal_precedes_reservation_and_copy)
{
	ClusterLmsSharedState state;
	ClusterR4CrForwardPayload forward = submit_test_forward96();
	ClusterSemanticAdmissionToken admission = submit_test_admission();
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
	UtClusterCrServerShared before;

	reset_submit_fixture(&state);
	memcpy(&before, &ut_cr_server_shared, sizeof(before));
	ut_stop_new_work_allowed = false;
	UT_ASSERT_EQ(cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
										  UT_MASTER_CAPABILITY_GENERATION, &reason),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(ut_stop_new_work_calls, 1);
	UT_ASSERT_EQ(ut_copy_calls, 0);
	UT_ASSERT_EQ(ut_wake_calls, 0);
	UT_ASSERT_EQ(memcmp(&ut_cr_server_shared, &before, sizeof(before)), 0);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(ut_lock_acquire_count, ut_lock_release_count);

	/* Invalid identity still refuses at its original boundary, before the seal. */
	ut_stop_new_work_calls = 0;
	forward.base.request_id = 0;
	UT_ASSERT_EQ(cluster_lms_cr_submit_r4(&forward, &admission, UT_REQUESTER_CAPABILITY_GENERATION,
										  UT_MASTER_CAPABILITY_GENERATION, &reason),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(ut_stop_new_work_calls, 0);
	UT_ASSERT_EQ(memcmp(&ut_cr_server_shared, &before, sizeof(before)), 0);
}

int
main(void)
{
	UT_PLAN(52);
	UT_RUN(test_normal_stop_cr_requires_initialized_actual_owner);
	UT_RUN(test_normal_stop_cr_all_owned_states_not_recovery_drain);
	UT_RUN(test_normal_stop_cr_original_terminal_must_release_local_context);
	UT_RUN(test_normal_stop_cr_legacy_seal_precedes_owner_mutation);
	UT_RUN(test_normal_stop_cr_r4_seal_precedes_reservation_and_copy);
	UT_RUN(test_foreign_undo_cold_holder_reaches_origin_without_resident_header);
	UT_RUN(test_cold_foreign_landing_freezes_only_the_validated_dependency);
	UT_RUN(test_r4_protocol_detail_distinguishes_actual_request_and_send_failures);
	UT_RUN(test_free_to_pending_canonicalizes_owner_under_lms_lock);
	UT_RUN(test_free_to_filling_uses_same_proof_window);
	UT_RUN(test_busy_slot_preserves_winner_owner_stamp);
	UT_RUN(test_missing_lms_state_refuses_before_mutation);
	UT_RUN(test_r4_submit_publishes_complete_stable_copy_once);
	UT_RUN(test_r4_all_local_submit_uses_exact_token_generation_without_peer_matcher);
	UT_RUN(test_r4_all_local_submit_refuses_generation_mismatch_before_slot_mutation);
	UT_RUN(test_r4_submit_copy_refusal_mapping_is_closed);
	UT_RUN(test_r4_submit_final_recheck_failure_canonicalizes_before_free);
	UT_RUN(test_r4_worker0_claim_peer_refusal_preserves_queued_slot);
	UT_RUN(test_r4_worker0_claim_rebinds_owner_and_retains_context);
	UT_RUN(test_r4_worker0_drain_requires_exact_empty_contexts);
	UT_RUN(test_r4_worker0_drain_rejects_every_terminal_and_shipping_state);
	UT_RUN(test_r4_lmon_reclaim_requires_exact_ack_then_canonicalizes_all_slots);
	UT_RUN(test_r4_lmon_reclaim_rechecks_ack_inside_exclusive_claim_window);
	UT_RUN(test_r4_lmon_reclaim_refuses_unproved_or_legacy_slot_without_mutation);
	UT_RUN(test_r4_worker0_claim_all_local_uses_token_without_peer_matcher);
	UT_RUN(test_r4_worker0_build_step_publishes_ready_full);
	UT_RUN(test_r4_worker0_build_step_publishes_one_need_undo);
	UT_RUN(test_r4_worker0_query_cancel_terminalizes_exact_building_slot);
	UT_RUN(test_r4_worker0_foreign_undo_requests_origin_selection_without_local_scur);
	UT_RUN(test_r4_worker0_foreign_undo_sends_frozen_zero_generation);
	UT_RUN(test_r4_worker0_foreign_undo_admitted_send_is_exact_and_one_shot);
	UT_RUN(test_r4_worker0_foreign_undo_accepts_initial_tt_wrap);
	UT_RUN(test_r4_worker0_foreign_undo_sends_unresolved_wrap_for_origin_upgrade);
	UT_RUN(test_r4_worker0_foreign_undo_refusal_mapping_is_close_first);
	UT_RUN(test_r4_worker0_foreign_undo_locator_mismatch_fails_before_send);
	UT_RUN(test_r4_worker0_status24_lands_exact_foreign_undo_and_short_admission);
	UT_RUN(test_r4_worker0_ready_undo_resumes_and_releases_original_owner);
	UT_RUN(test_r4_worker0_ready_undo_error_uses_original_terminal_cleanup);
	UT_RUN(test_r4_worker0_origin_refusal_uses_exact_original_terminal_owner);
	UT_RUN(test_r4_worker0_initial_tt_rollover_counter_is_proven_zero);
	UT_RUN(test_r4_worker0_authority_cover_uses_logical_cross_origin_scn);
	UT_RUN(test_r4_worker0_foreign_dependency_preserves_current_zero_epoch_open);
	UT_RUN(test_worker0_real_builder_two_foreign_updates_complete_same_owned_continuation);
	UT_RUN(test_r4_worker0_status24_canonicalizes_invalid_wrap_before_ready);
	UT_RUN(test_r4_worker0_status24_mismatches_leave_shared_slot_unchanged);
	UT_RUN(test_r4_worker0_remote_full_ship_transfers_frame_and_cleans_context);
	UT_RUN(test_r4_worker0_local_full_uses_registered_dispatch_and_cleans_context);
	UT_RUN(test_r4_worker0_retry_and_fail_ship_zero_body_without_image_fence);
	UT_RUN(test_data_plane_close_peer_now_is_same_process_and_idempotent);
	UT_RUN(test_r4_worker0_terminal_hard_error_closes_data_peer_before_cleanup);
	UT_RUN(test_all_four_legacy_submitters_use_common_reserver);
	UT_RUN(test_r4_protocol_detail_budget_never_changes_terminal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
