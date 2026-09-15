/* Author: SqlRush <sqlrush@gmail.com> */
/* Normal-stop GCS observations execute the production module, its real slot
 * reserve/release and init. Runtime locks, guard cancellation and admission
 * leave are boundary fixtures; no remote producer or disk sync is claimed. */
#include "postgres.h"
#include "../../backend/cluster/cluster_gcs_block.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
ProcessingMode Mode = NormalProcessing;
int MaxBackends = 4;
BackendId MyBackendId = 1;
int cluster_node_id = 0;
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = LmsProcess;
bool cluster_past_image = true;
int cluster_gcs_block_invalidate_ack_timeout_ms = 5000;
static void *shared_allocation;
static LWLock *held_lock;
static int held_count;
static int guard_cancelled;
static int admission_left;
int cluster_lms_workers = 1;
int cluster_injection_armed_count = 0;
static bool admit_new_invalidate = true, invalidate_finishes = true, rx_frame_consumed;
static unsigned invalidate_admissions, invalidate_attempts, discard_calls, misroutes;
static bool pi_global_cut;
static unsigned pi_hint_kept, pi_hint_durable;
static void stop_fixture_invalidate_tick(void);

void
cluster_injection_run(const char *name)
{
	(void)name;
	Assert(false);
}
bool
cluster_injection_should_skip(const char *name)
{
	(void)name;
	return false;
}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data); /* existing revoke/durability work remains allowed at seal1 */
	invalidate_admissions++;
	return admit_new_invalidate;
}
uint64
cluster_epoch_get_current(void)
{
	return 77;
}
int
cluster_gcs_lookup_master(BufferTag tag)
{
	(void)tag;
	return 1;
}
int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}
int
cluster_lms_shard_for_tag(const BufferTag *tag, int workers)
{
	(void)tag;
	Assert(workers == 1);
	return 0;
}
void
cluster_gcs_block_dedup_note_misroute(void)
{
	misroutes++;
}
bool
cluster_bufmgr_discard_pi_block(BufferTag tag)
{
	(void)tag;
	discard_calls++;
	return true; /* original strict PI proof/drop remains a boundary */
}
void
cluster_lever_h_note_discard_result(bool discarded)
{
	Assert(discarded);
}
static bool
stop_fixture_resource_x_frame(const ClusterICEnvelope *env, const void *payload)
{
	(void)env;
	(void)payload;
	return rx_frame_consumed;
}
static bool
stop_fixture_authenticated_session(int node, uint64 epoch, uint64 *session)
{
	Assert(node == 1 && epoch == 77);
	*session = 99;
	return true;
}
static bool
stop_fixture_invalidate_execute(const GcsBlockInvalidatePayload *inv)
{
	Assert(inv->master_node == 1 && inv->epoch == 77);
	invalidate_attempts++;
	return invalidate_finishes;
}

bool
cluster_normal_stop_pi_retirement_allowed(void)
{
	Assert(held_count == 0);
	return pi_global_cut;
}

static void
stop_fixture_pi_kept(BufferTag tag, int32 sender)
{
	Assert(tag.relNumber == 99 && sender == 1);
	pi_hint_kept++;
}

static void
stop_fixture_pi_durable(BufferTag tag, SCN written)
{
	Assert(tag.relNumber == 99 && written == (SCN)0x5500);
	pi_hint_durable++;
}

/* Verbatim actual handler and original park tick; only frame routing,
 * authenticated connection and holder/PI execution are boundary fixtures. */
#define cluster_gcs_handle_block_invalidate_envelope stop_fixture_invalidate_handler
#define cluster_gcs_block_invalidate_park_tick stop_fixture_invalidate_tick
#define gcs_block_try_resource_x_frame stop_fixture_resource_x_frame
#define gcs_block_pcm_x_authenticated_session stop_fixture_authenticated_session
#define gcs_block_invalidate_execute stop_fixture_invalidate_execute
#include "test_cluster_gcs_stop_invalidate.inc"
#undef cluster_gcs_handle_block_invalidate_envelope
#undef cluster_gcs_block_invalidate_park_tick
#undef gcs_block_try_resource_x_frame
#undef gcs_block_pcm_x_authenticated_session
#undef gcs_block_invalidate_execute

/* Original checksum/identity/epoch and both unsolicited PI branches.
 * PCM application is the boundary (its real owner is tested separately). */
#define cluster_gcs_handle_block_invalidate_ack_envelope stop_fixture_pi_ack_handler
#define gcs_block_try_resource_x_frame stop_fixture_resource_x_frame
#define gcs_block_pi_discard_master_apply stop_fixture_pi_durable
#define cluster_pcm_lock_pi_holder_note stop_fixture_pi_kept
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#include "test_cluster_gcs_stop_pi_ack.inc"
#pragma GCC diagnostic pop
#undef cluster_gcs_handle_block_invalidate_ack_envelope
#undef gcs_block_try_resource_x_frame
#undef gcs_block_pi_discard_master_apply
#undef cluster_pcm_lock_pi_holder_note

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}

Size
add_size(Size a, Size b)
{
	Assert(a <= SIZE_MAX - b);
	return a + b;
}
Size
mul_size(Size a, Size b)
{
	Assert(b == 0 || a <= SIZE_MAX / b);
	return a * b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(strcmp(name, "pgrac cluster gcs block") == 0);
	Assert(shared_allocation == NULL);
	shared_allocation = calloc(1, size);
	Assert(shared_allocation != NULL);
	*found = false;
	return shared_allocation;
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	memset(lock, 0, sizeof(*lock));
}
void
ConditionVariableInit(ConditionVariable *cv)
{
	memset(cv, 0, sizeof(*cv));
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(held_count == 0);
	held_lock = lock;
	held_count++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	Assert(held_lock == lock);
	held_lock = NULL;
	held_count--;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return held_lock == lock;
}
int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	abort();
}
bool
errstart(int level, const char *domain)
{
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return false;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *format, ...)
{
	return 0;
}
int
errmsg_internal(const char *format, ...)
{
	return 0;
}
int
errdetail(const char *format, ...)
{
	return 0;
}
int
errhint(const char *format, ...)
{
	return 0;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
void
cluster_lever_h_note_write_note(bool overflowed)
{
	Assert(!overflowed);
}
void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard)
{
	Assert(held_count == 0);
	guard_cancelled++;
}
void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	Assert(held_count == 0 && token->entered);
	admission_left++;
	token->entered = false;
}

static ClusterNormalStopPollResult
shared_poll(bool post)
{
	int backend, slot;
	const char *reason;
	return cluster_gcs_block_normal_stop_poll(post, &backend, &slot, &reason);
}
static ClusterNormalStopPollResult
local_poll(void)
{
	int slot;
	const char *reason;
	return cluster_gcs_block_normal_stop_local_poll(&slot, &reason);
}
static void
reset_test(void)
{
	Assert(held_count == 0);
	free(shared_allocation);
	shared_allocation = NULL;
	cluster_gcs_block_shmem_init();
	memset(gcs_block_r4_tx_origin_contexts, 0, sizeof(gcs_block_r4_tx_origin_contexts));
	memset(gcs_block_invalidate_park, 0, sizeof(gcs_block_invalidate_park));
	MyBackendId = 1;
	MyAuxProcType = LmsProcess;
	IsUnderPostmaster = true;
	guard_cancelled = admission_left = 0;
	admit_new_invalidate = invalidate_finishes = true;
	rx_frame_consumed = false;
	invalidate_admissions = invalidate_attempts = discard_calls = misroutes = 0;
}

UT_TEST(test_uninitialized_is_not_empty)
{
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_INVALID);
	reset_test();
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_all_three_real_requester_domains_need_original_release)
{
	ClusterGcsBlockOutstandingSlot *slots[3];
	ClusterCurrentMxKey mx = { 0 };
	BufferTag tag = { 0 };
	uint64 request;
	reset_test();
	tag.relNumber = 99;
	slots[0] = gcs_block_reserve_slot(tag, PCM_TRANS_N_TO_X, 1, &request);
	slots[1] = gcs_block_try_reserve_r4_slot(tag, 8, 1, &request);
	slots[2] = gcs_block_try_reserve_current_mx_slot(
		&mx, 8, 1, GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT, &request);
	for (int i = 0; i < 3; i++) {
		UT_ASSERT(slots[i] != NULL);
		UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_PENDING);
		/* Reply received is not slot release. */
		slots[i]->reply_received = true;
		UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_PENDING);
		gcs_block_release_slot(slots[i]);
	}
	UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(gcs_block_backend_blocks[0].next_request_id > 1);
}

UT_TEST(test_later_orphan_direct_target_is_invalid_not_empty)
{
	ClusterGcsBlockOutstandingSlot *slot;
	ClusterGcsBlockOutstandingSlot *orphan;
	BufferTag tag = { 0 };
	uint64 request;
	int backend, index;
	const char *reason;
	reset_test();
	slot = gcs_block_try_reserve_r4_slot(tag, 8, 1, &request);
	orphan = &gcs_block_backend_blocks[3].slots[1];
	orphan->direct_target_prepared = true;
	UT_ASSERT_EQ(cluster_gcs_block_normal_stop_poll(false, &backend, &index, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(backend, 3);
	UT_ASSERT_EQ(index, 1);
	UT_ASSERT(orphan->direct_target_prepared);
	orphan->direct_target_prepared = false;
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	slot->reply_domain = 255;
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
	gcs_block_release_slot(slot);
	orphan->direct_state = GCS_BLOCK_DIRECT_ABORTING;
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	orphan->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_broadcast_all_acked_is_not_released)
{
	BufferTag zero = { 0 };
	reset_test();
	/* Inject an original-lock protected in-flight broadcast observation.
	 * This tests the observer, not the remote broadcast protocol. */
	LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 37);
	ClusterGcsBlock->invalidate_broadcast_epoch = 8;
	ClusterGcsBlock->invalidate_broadcast_tag.relNumber = 99;
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 6);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 6);
	LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 7);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 6);
	pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 0);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	ClusterGcsBlock->invalidate_broadcast_epoch = 0;
	ClusterGcsBlock->invalidate_broadcast_tag = zero;
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 0);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_pi_note_keeps_pre_and_post_checkpoint_cuts_distinct)
{
	BufferTag tag = { 0 };
	uint64 before;
	reset_test();
	tag.relNumber = 99;
	cluster_gcs_block_pi_write_note(tag, 100);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_PENDING);
	before = cluster_gcs_block_pi_note_presync_snapshot();
	cluster_gcs_block_pi_note_confirm(before);
	UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ClusterGcsBlock->pi_note_drain_seq, 0);
	/* A fixture for the original drain completion, not a disk/PI proof. */
	SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
	ClusterGcsBlock->pi_note_drain_seq = before;
	SpinLockRelease(&ClusterGcsBlock->pi_note_lock);
	UT_ASSERT_EQ(shared_poll(true), CLUSTER_NORMAL_STOP_READY);
	cluster_gcs_block_pi_note_confirm(before + 1);
	UT_ASSERT_EQ(shared_poll(false), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_local_context_guard_and_admission_have_own_terminator)
{
	GcsBlockR4TxOriginContext *context;
	reset_test();
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = LmsWorker7Process;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
	context = &gcs_block_r4_tx_origin_contexts[3];
	/* Origin acceptance is a fixture; clear is the actual production owner. */
	context->in_use = true;
	context->domain = GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE;
	context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE;
	context->guard_active = true;
	context->admission.entered = true;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(guard_cancelled, 0);
	gcs_block_r4_tx_origin_context_clear(context, true);
	UT_ASSERT_EQ(guard_cancelled, 1);
	UT_ASSERT_EQ(admission_left, 1);
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
	context->guard_active = true;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_INVALID);
	context->guard_active = false;
	context->admission.entered = true;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_INVALID);
	context->admission.entered = false;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_parked_invalidate_owned_and_later_invalid_context_wins)
{
	GcsBlockInvalidatePayload inv = { 0 };
	int slot;
	const char *reason;
	reset_test();
	inv.tag.relNumber = 99;
	inv.request_id = 17;
	inv.master_node = 1;
	gcs_block_invalidate_park_add(&inv);
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(gcs_block_invalidate_park[0].in_use);
	/* Raw buffered state without its live owner cannot be excused by an
	 * earlier legitimate pending entry. */
	gcs_block_r4_tx_origin_contexts[7].guard_active = true;
	UT_ASSERT_EQ(cluster_gcs_block_normal_stop_local_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(slot, 7);
	gcs_block_r4_tx_origin_contexts[7].guard_active = false;
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_sealed_invalidate_new_work_vs_exact_parked_completion)
{
	GcsBlockInvalidatePayload inv = { .request_id = 31, .epoch = 77, .master_node = 1 };
	ClusterICEnvelope env = { .source_node_id = 1, .payload_length = sizeof(inv) };
	GcsBlockParkedInvalidate before;
	reset_test();
	inv.tag.relNumber = 99;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);
	admit_new_invalidate = false;
	invalidate_finishes = false;
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(invalidate_admissions, 1);
	UT_ASSERT_EQ(invalidate_attempts, 0);
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
	admit_new_invalidate = true;
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(invalidate_admissions, 2);
	UT_ASSERT(gcs_block_invalidate_park[0].in_use);
	before = gcs_block_invalidate_park[0];
	admit_new_invalidate = false;
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(invalidate_admissions, 2);
	UT_ASSERT(memcmp(&gcs_block_invalidate_park[0], &before, sizeof(before)) == 0);
	/* Same tag is not enough to identify the retained directive. */
	inv.request_id++;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(invalidate_admissions, 3);
	UT_ASSERT(memcmp(&gcs_block_invalidate_park[0], &before, sizeof(before)) == 0);
	invalidate_finishes = true;
	stop_fixture_invalidate_tick();
	UT_ASSERT_EQ(local_poll(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(invalidate_admissions, 3);
	UT_ASSERT_EQ(invalidate_attempts, 3);
}

UT_TEST(test_invalidate_validation_and_pi_suffix_precede_new_work_seal)
{
	GcsBlockInvalidatePayload inv = { .request_id = 32, .epoch = 77, .master_node = 1 };
	ClusterICEnvelope env = { .source_node_id = 1, .payload_length = sizeof(inv) };
	reset_test();
	admit_new_invalidate = false;
	inv.tag.relNumber = 99;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);
	env.source_node_id = 2;
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(misroutes, 1);
	UT_ASSERT_EQ(invalidate_admissions, 0);
	env.source_node_id = 1;
	inv.reserved_0[0] = GCS_BLOCK_INVALIDATE_KIND_PI_DISCARD;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(discard_calls, 1);
	UT_ASSERT_EQ(invalidate_admissions, 0);
	UT_ASSERT_EQ(invalidate_attempts, 0);
	rx_frame_consumed = true;
	stop_fixture_invalidate_handler(&env, &inv);
	UT_ASSERT_EQ(discard_calls, 1);
	UT_ASSERT_EQ(invalidate_admissions, 0);
}

UT_TEST(test_pi_hints_cannot_recreate_debt_after_global_checkpoint_cut)
{
	for (int kind = 0; kind < 2; kind++) {
		GcsBlockInvalidateAckPayload ack = { .epoch = 77, .sender_node = 1 };
		ClusterICEnvelope env = { .source_node_id = 1, .epoch = 77, .payload_length = sizeof(ack) };
		reset_test();
		pi_hint_kept = pi_hint_durable = 0;
		pi_global_cut = false;
		ack.tag.relNumber = 99;
		ack.ack_status = kind ? GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE
							  : GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_KEPT_NOTE;
		GcsBlockInvalidateAckPayloadSetPageScn(&ack, (SCN)0x5500);
		ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);
		stop_fixture_pi_ack_handler(&env, &ack);
		UT_ASSERT_EQ(pi_hint_kept + pi_hint_durable, 1);
		pi_global_cut = true;
		stop_fixture_pi_ack_handler(&env, &ack);
		UT_ASSERT_EQ(pi_hint_kept + pi_hint_durable, 1);
		/* No full proof: retain ordinary behavior, but never accept a bad
		 * frame merely because the cut is absent. */
		pi_global_cut = false;
		ack.checksum++;
		stop_fixture_pi_ack_handler(&env, &ack);
		ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);
		env.source_node_id = 2;
		stop_fixture_pi_ack_handler(&env, &ack);
		env.source_node_id = 1;
		ack.epoch = 76;
		ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);
		stop_fixture_pi_ack_handler(&env, &ack);
		UT_ASSERT_EQ(pi_hint_kept + pi_hint_durable, 1);
		ack.epoch = 77;
		ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);
		stop_fixture_pi_ack_handler(&env, &ack);
		UT_ASSERT_EQ(pi_hint_kept + pi_hint_durable, 2);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_uninitialized_is_not_empty);
	UT_RUN(test_all_three_real_requester_domains_need_original_release);
	UT_RUN(test_later_orphan_direct_target_is_invalid_not_empty);
	UT_RUN(test_broadcast_all_acked_is_not_released);
	UT_RUN(test_pi_note_keeps_pre_and_post_checkpoint_cuts_distinct);
	UT_RUN(test_local_context_guard_and_admission_have_own_terminator);
	UT_RUN(test_parked_invalidate_owned_and_later_invalid_context_wins);
	UT_RUN(test_sealed_invalidate_new_work_vs_exact_parked_completion);
	UT_RUN(test_invalidate_validation_and_pi_suffix_precede_new_work_seal);
	UT_RUN(test_pi_hints_cannot_recreate_debt_after_global_checkpoint_cut);
	UT_DONE();
	return ut_failed_count != 0;
}
