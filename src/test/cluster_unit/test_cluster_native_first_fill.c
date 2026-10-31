/* Ordinary first-fill: actual PCM/requester/native bodies; controlled peers,
 * process interrupts and OS wakeups are explicit fixture inputs. */
#define NATIVE_FIRST_FILL_COMPOSITION
#include "test_cluster_native_wait_fixture.inc"
#undef NATIVE_FIRST_FILL_COMPOSITION
#include "cluster/cluster_space_reservation.h"
#include "pgstat.h"

/* Retain the complete real preparation, disk/image execution and cleanup. */
static void ordinary_report(int level);
static void cluster_bufmgr_acquire_resident_scope_cleanup(BufferDesc *buf,
	const ClusterPcmOwnSnapshot *base, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXDeliveryClaim *prepared);
#ifndef NATIVE_RESIDENT_FILL_COMPOSITION
/* This first-fill-only composition never sets resident. Keep accidental
 * dispatch a hard failure, not an implementation of resident cleanup. */
static void
cluster_bufmgr_acquire_resident_scope_cleanup(BufferDesc *buf,
	const ClusterPcmOwnSnapshot *base, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXDeliveryClaim *prepared)
{
	(void)buf; (void)base; (void)episode; (void)prepared;
	ordinary_report(PANIC);
}
#endif
BufferDescPadded *BufferDescriptors = (BufferDescPadded *)&request_buf;
uint32 LockBufHdr(BufferDesc *buf) { return request_header_lock(buf); }
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#undef ereport
#define ereport(level, rest) ordinary_report(level)
#include "test_cluster_native_disk_guard.inc"
#include "test_cluster_native_disk_episode.inc"
#ifdef NATIVE_RESIDENT_FILL_COMPOSITION
/* Retain the actual image body while exposing the same content-lock
 * boundary leaves as the original disk staging component. */
#define LWLockAcquire stage_content_acquire
#define LWLockRelease stage_content_release
#endif
#include "test_cluster_native_image_episode.inc"
#ifdef NATIVE_RESIDENT_FILL_COMPOSITION
#undef LWLockRelease
#undef LWLockAcquire
#endif
#include "test_cluster_native_first_fill_cleanup.inc"
#undef GetPrivateRefCount
#undef LockBufHdr

static unsigned ordinary_pauses, ordinary_sends, ordinary_leaves;
static ResourceXApplyResult ordinary_wait(void);
static unsigned ordinary_panics;
static void ordinary_report(int level)
{
	if (level < ERROR) return;
	if (level >= PANIC) ordinary_panics++;
	pg_re_throw();
}
static unsigned ordinary_peer_samples, ordinary_current_calls;
static unsigned ordinary_unavailable_after_send, ordinary_unavailable;
int cluster_gcs_block_retransmit_initial_backoff_ms = 1;
static unsigned ordinary_pause_limit, ordinary_cancel_at;
static bool ordinary_send_error;
static int ordinary_profile;
static uint64 ordinary_master_session = 31;
static uint32 ordinary_connection = 61;
static ClusterGcsFirstFill *ordinary_active;

/* Explicit authenticated membership/session/peer provider inputs. Actual
 * PCM gates, E/A/I, ring handoff, native installation and IO owners remain. */
static PcmXSessionAuthResult
ordinary_session(const BufferTag *tag, ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(BufferTagsEqual(tag, &request_base.tag));
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate) || gate->phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	*master = 0; *session = ordinary_master_session;
	return PCM_X_SESSION_AUTH_OK;
}
static ResourceXApplyResult
ordinary_current(const BufferTag *tag, const ResourceXGateSnapshot *expected, int32 master, uint64 session)
{
	ResourceXGateSnapshot gate; int32 node; uint64 boot;
	if (++ordinary_current_calls > 32) { UT_ASSERT(false); pg_re_throw(); }
	if (ordinary_unavailable != 0) { ordinary_unavailable--; return RESOURCE_X_APPLY_BAD_STATE; }
	if (ordinary_session(tag, &gate, &node, &boot) != PCM_X_SESSION_AUTH_OK)
		return RESOURCE_X_APPLY_STALE;
	return memcmp(&gate, expected, sizeof(gate)) == 0 && master == node && session == boot
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}
static bool
ordinary_peer(int32 node, uint32 *generation)
{
	UT_ASSERT_EQ(node, 0);
	*generation = ++ordinary_peer_samples % 2 == 1 ? 51 : ordinary_connection;
	return true;
}
static ResourceXApplyResult
ordinary_peer_result(const ClusterSemanticAdmissionToken *admission, int32 node, uint32 generation)
{
	return admission->entered && node == 0 && generation == ordinary_connection
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}
static bool
ordinary_peer_matches(const ClusterSemanticAdmissionToken *admission, int32 node, uint32 generation)
{
	return ordinary_peer_result(admission, node, generation) == RESOURCE_X_APPLY_APPLIED;
}
static ClusterSemanticAdmissionResult
ordinary_enter(uint64 feature, ClusterSemanticAdmissionSide side, ClusterSemanticAdmissionToken *admission)
{
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	*admission = request_admission; return CLUSTER_SEMANTIC_ADMISSION_OK;
}
static void ordinary_leave(ClusterSemanticAdmissionToken *admission)
{
	UT_ASSERT(admission->entered); admission->entered = false; ordinary_leaves++;
}
static uint64 ordinary_now(void) { return ++fake_pcm_clock_us; }
static uint64 ordinary_budget(void) { return 9000000; }
static void ordinary_deliver(void);
static void ordinary_pause(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (++ordinary_pauses >= ordinary_pause_limit) pg_re_throw();
}
static ClusterLmsEnqueueResult
ordinary_send(int32 master, const ResourceXDecodedFrame *frame, BufferDesc *buf,
	const ClusterPcmOwnSnapshot *base, uint64 token, uint8 path, uint8 mode,
	const ClusterSemanticAdmissionToken *admission)
{
	ClusterLmsEnqueueResult result;
	ordinary_sends++;
	request_unsent = request_entry()->resource_x_bootstrap_round;
	result = cluster_lms_outbound_enqueue_resource_x_acquire(master, frame, buf, base, token,
		path, mode, admission);
	if (ordinary_send_error) pg_re_throw();
	ordinary_unavailable = ordinary_unavailable_after_send;
	if (result == CLUSTER_LMS_ENQUEUE_ADMITTED) fake_cv_prepare_hook = ordinary_deliver;
	return result;
}
#define gcs_block_resource_x_gate_session_snapshot_result ordinary_session
#define gcs_block_resource_x_gate_session_recheck_result ordinary_current
#define gcs_block_pcm_x_resource_x_peer_ready_exact ordinary_peer
#define gcs_block_resource_x_target_peer_matches_exact ordinary_peer_matches
#define gcs_block_resource_x_target_peer_result_exact ordinary_peer_result
#define cluster_semantic_activation_enter ordinary_enter
#define cluster_semantic_activation_leave ordinary_leave
#define gcs_block_pcm_x_monotonic_us ordinary_now
#define gcs_block_pcm_x_retry_timeout_us ordinary_budget
#define gcs_block_resource_x_observation_pause ordinary_pause
#define cluster_lms_outbound_enqueue_resource_x_acquire ordinary_send
#include "test_cluster_native_first_fill_gcs.inc"
#undef cluster_lms_outbound_enqueue_resource_x_acquire
#undef gcs_block_resource_x_observation_pause
#undef gcs_block_pcm_x_retry_timeout_us
#undef gcs_block_pcm_x_monotonic_us
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_enter
#undef gcs_block_resource_x_target_peer_result_exact
#undef gcs_block_resource_x_target_peer_matches_exact
#undef gcs_block_pcm_x_resource_x_peer_ready_exact
#undef gcs_block_resource_x_gate_session_recheck_result
#undef gcs_block_resource_x_gate_session_snapshot_result

static ClusterPcmOwnResult
ordinary_reserve(BufferDesc *buf, PcmLockMode mode, ClusterPcmOwnSnapshot *base,
	uint64 *token, bool *covered, bool *unused)
{
	(void)unused; UT_ASSERT(buf == &request_buf); UT_ASSERT_EQ(mode, PCM_LOCK_MODE_S);
	*base = request_base; *token = request_token; *covered = false;
	return CLUSTER_PCM_OWN_OK;
}
static void ordinary_count(IOObject object, IOContext context, IOOp op, instr_time start, uint32 count)
{
	(void)object; (void)context; (void)start; UT_ASSERT_EQ(op, IOOP_READ); UT_ASSERT_EQ(count, 1);
}
#define LockBufHdr stage_header_lock
#define TerminateBufferIO ordinary_actual_terminate
#include "test_cluster_native_disk_terminate.inc"
#undef TerminateBufferIO
#undef LockBufHdr
#define GetBufferDescriptor(id) quiet_descriptor(id)
#define relpathperm(locator, forknum) quiet_wrong_output()
#define pfree(pointer) quiet_wrong_free(pointer)
#define LockBufHdr stage_header_lock
#define TerminateBufferIO ordinary_actual_terminate
#define AbortBufferIO ordinary_actual_abort
#include "test_cluster_native_disk_abort.inc"
#undef AbortBufferIO
#undef TerminateBufferIO
#undef LockBufHdr
#undef pfree
#undef relpathperm
#undef GetBufferDescriptor
#define cluster_bufmgr_pcm_begin_grant_reservation_wait ordinary_reserve
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define AbortBufferIO ordinary_actual_abort
#define pgstat_prepare_io_time() ((instr_time){0})
#define pgstat_count_io_op_time ordinary_count
#ifndef NATIVE_RESIDENT_REQUEST_COMPOSITION
#define cluster_bufmgr_pcm_own_acquire_cancel_resident_wait_exact(...) \
	(ordinary_report(PANIC), RESOURCE_X_APPLY_INVALID)
#endif
#include "test_cluster_native_first_fill_native.inc"
#ifndef NATIVE_RESIDENT_REQUEST_COMPOSITION
#undef cluster_bufmgr_pcm_own_acquire_cancel_resident_wait_exact
#endif
#undef ereport
#undef pgstat_count_io_op_time
#undef pgstat_prepare_io_time
#undef AbortBufferIO
#undef GetPrivateRefCount
#undef LockBufHdr

static bool
ordinary_episode_input(ResourceXDecodedFrame *request, ResourceXDecodedFrame *legacy,
	ResourceXIntentSlot *ticket, PcmState observed, PcmState target, uint8 path)
{
	(void)observed; (void)target; (void)path;
	*ticket = request_ticket(request); *legacy = ordinary_active->prepared;
	return true;
}
#define episode_begin ordinary_episode_input
#define evidence_begin ordinary_evidence_begin
#include "test_cluster_native_first_fill_evidence.inc"
#undef evidence_begin
#undef episode_begin

static EvidenceFixture ordinary_material;
static void
ordinary_deliver(void)
{
	ResourceXDecodedImageEnvelope *image;
	ResourceXDecodedBlockedToN *proof;
	ResourceXWireReject reject; uint32 component;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (ordinary_cancel_at == 1) pg_re_throw();
	evidence_disk_watermark_scn = 90;
	UT_ASSERT(ordinary_evidence_begin(&ordinary_material, ordinary_profile));
	evidence_disk_watermark_scn = 0;
	prepared_episode = ordinary_material.request.body.acquire_intent;
	input_observation = ordinary_active->observation;
	input_grant = ordinary_material.decision;
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	UT_ASSERT(evidence_wire(&input_grant));
	if (ordinary_profile >= 2) {
		image = &ordinary_material.image.body.acquire_image.original_image;
		proof = &ordinary_material.image.body.acquire_image.source.original_proof;
		image->image_flags = 0;
		memset(&image->page_wal, 0, sizeof(image->page_wal));
		memcpy(image->page_bytes, disk_bytes.data, BLCKSZ);
		image->page_scn_lsn = proof->page_scn_lsn = ((PageHeader)disk_bytes.data)->pd_block_scn;
		image->page_checksum = proof->page_checksum = cluster_gcs_block_compute_checksum(disk_bytes.data);
		UT_ASSERT(cluster_resource_x_wire_acquire_image_crc(&ordinary_material.image.body.acquire_image,
			&component, &reject));
		proof->source_proof_crc32c = component;
		UT_ASSERT(evidence_wire(&ordinary_material.image));
	}
	input_complete(&ordinary_material, ordinary_profile);
	auth_peer_boot = 31;
	if (ordinary_cancel_at == 2) ordinary_master_session++;
	if (ordinary_cancel_at == 3) ordinary_connection++;
}

static void
ordinary_start(ClusterBufmgrFirstFill *read, int profile)
{
	EvidenceFixture unused;
	/* Initialize the existing disk/process leaves without using their
	 * prepared executor as the subject. Reset only this fresh test fixture. */
	UT_ASSERT(wait_start(&unused, 0));
	reset_fake_pcm_runtime(4); cluster_node_id = 1; ut_master_session = 101;
	master_durable_admission = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	fake_gcs_master_node = 0;
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	memset(read, 0, sizeof(*read)); read->buf = &request_buf;
	ordinary_active = &read->request;
	ordinary_profile = profile; ordinary_sends = ordinary_pauses = ordinary_leaves = 0;
	ordinary_panics = 0;
	ordinary_master_session = 31; ordinary_connection = 61;
	ordinary_peer_samples = ordinary_current_calls = 0;
	ordinary_unavailable = ordinary_unavailable_after_send = 0;
	ordinary_pause_limit = 8; ordinary_cancel_at = 0; ordinary_send_error = false;
	MyBackendType = B_BACKEND;
	disk_online_join = false; disk_boot_decided = true; disk_extension = false;
	disk_on_extension = disk_on_enter = disk_on_open = disk_on_read = disk_on_end = NULL;
	disk_enter_ok = disk_end_ok = true; disk_open_error = disk_read_error = false;
	disk_enters = disk_ends = disk_opens = disk_reads = disk_bad_pages = disk_traces = 0;
	memset(&disk_bytes, 0x53, sizeof(disk_bytes));
	memset(disk_bytes.data, 0, SizeOfPageHeaderData);
	((PageHeader)disk_bytes.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_bytes.data)->pd_upper = ((PageHeader)disk_bytes.data)->pd_special = BLCKSZ;
	((PageHeader)disk_bytes.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	((PageHeader)disk_bytes.data)->pd_block_scn = 93;
	PageSetLSNPreserveOrigin(disk_bytes.data, 12345);
	((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	cluster_enabled = cluster_shared_config = true;
	cluster_page_wal_shmem_init();
	stage_on_content = stage_after_content = NULL;
	complete_before_io_error = complete_after_io_error = complete_publish_error = false;
	complete_io_calls = complete_broadcasts = complete_commits = 0;
	complete_on_broadcast = NULL; stage_header_hook = NULL;
}

static bool
ordinary_run(ClusterBufmgrFirstFill *read)
{
	volatile bool caught = false;
	PG_TRY();
	{
		PG_ENSURE_ERROR_CLEANUP(cluster_bufmgr_first_fill_error, PointerGetDatum(read));
		{ cluster_bufmgr_first_fill_begin(&request_buf, read, IOOBJECT_RELATION, IOCONTEXT_NORMAL); }
		PG_END_ENSURE_ERROR_CLEANUP(cluster_bufmgr_first_fill_error, PointerGetDatum(read));
	}
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT_EQ(ordinary_panics, 0);
	return !caught;
}

UT_TEST(test_ordinary_disk_path_completes_one_original_io_and_request)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0);
	UT_ASSERT(ordinary_run(&read));
	UT_ASSERT(!read.pending); UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(ordinary_leaves, 1);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	complete_idle();
}

UT_TEST(test_ordinary_remote_current_s_and_x_never_read_disk)
{
	for (int profile = 2; profile <= 3; profile++) {
		ClusterBufmgrFirstFill read;
		ordinary_start(&read, profile);
		/* In-memory current legitimately carries its old on-disk checksum. */
		((PageHeader)disk_bytes.data)->pd_checksum ^= UINT16_MAX;
		UT_ASSERT(ordinary_run(&read));
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
		UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_commits, 1);
		UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
		complete_idle();
	}
}

static void ordinary_capacity_cancel(void) { pg_re_throw(); }
UT_TEST(test_ordinary_capacity_cancel_retires_only_unsent_round_then_native_io)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0);
	request_shared.rings[1].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	request_on_sleep = ordinary_capacity_cancel;
	UT_ASSERT(!ordinary_run(&read));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.phase, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(ordinary_leaves, 1);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	complete_idle();
}

UT_TEST(test_ordinary_error_inside_physical_handoff_keeps_the_exact_request_owner)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0); ordinary_send_error = true;
	UT_ASSERT(!ordinary_run(&read));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(ordinary_leaves, 1); UT_ASSERT(!read.pending);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	complete_idle();
}

UT_TEST(test_ordinary_cancel_while_waiting_never_creates_a_second_episode)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0); ordinary_cancel_at = 1;
	UT_ASSERT(!ordinary_run(&read));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(ordinary_leaves, 1); UT_ASSERT(!read.pending);
	UT_ASSERT(!fake_cv_prepared); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	complete_idle();
}

UT_TEST(test_ordinary_storage_error_uses_existing_quiet_executor_cleanup)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0); disk_read_error = true;
	UT_ASSERT(!ordinary_run(&read));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 14);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT_EQ(ordinary_leaves, 1); UT_ASSERT(!read.pending);
	complete_idle();
}

UT_TEST(test_ordinary_error_after_C_finishes_handoff_without_second_io_completion)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0); complete_after_io_error = true;
	UT_ASSERT(!ordinary_run(&read));
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_VALID);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(ordinary_leaves, 1); UT_ASSERT(!read.pending);
	complete_idle();
}

UT_TEST(test_ordinary_resource_formation_is_independent_of_membership_epoch)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0);
	reset_fake_pcm_runtime(4); cluster_node_id = 1; ut_master_session = 101;
	master_durable_admission = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(29), RESOURCE_X_APPLY_APPLIED);
	fake_gcs_master_node = 0;
	UT_ASSERT(ordinary_run(&read));
	UT_ASSERT_EQ(read.request.gate.formation, 29);
	UT_ASSERT_EQ(read.request.episode.cluster_epoch, 17);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	complete_idle();
}

UT_TEST(test_ordinary_completion_rechecks_master_and_transport_before_any_bytes)
{
	for (unsigned cut = 2; cut <= 3; cut++) {
		ClusterBufmgrFirstFill read;
		ordinary_start(&read, 0); ordinary_cancel_at = cut;
		UT_ASSERT(!ordinary_run(&read));
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
		UT_ASSERT_EQ(complete_commits, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
		UT_ASSERT_EQ(ordinary_leaves, 1); UT_ASSERT(!read.pending);
		complete_idle();
	}
}

UT_TEST(test_ordinary_unavailable_observations_do_not_fail_or_repeat_request)
{
	ClusterBufmgrFirstFill read;
	ordinary_start(&read, 0); ordinary_unavailable_after_send = 3;
	UT_ASSERT(ordinary_run(&read));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(ordinary_pauses, 3);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_commits, 1);
	complete_idle();
}

UT_TEST(test_ordinary_passive_wait_rejects_lost_binding_and_closed_gate)
{
	EvidenceFixture f; ResourceXGateSnapshot gate;
	UT_ASSERT(wait_start(&f, 0));
	input_observation.entry_binding_generation++;
	UT_ASSERT(ordinary_wait() != RESOURCE_X_APPLY_NOT_FOUND);
	input_observation.entry_binding_generation--;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ordinary_wait() != RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(!fake_cv_prepared); complete_idle();
}

extern ResourceXApplyResult cluster_pcm_lock_resource_x_first_fill_retire_unsent_exact(
	const ResourceXInstallClaimJoinObservation *, const ResourceXDecodedAcquireIntent *,
	ResourceXInstallClaimJoinObservation *);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_acquire_wait_exact(
	const ResourceXInstallClaimJoinObservation *, const ResourceXDecodedAcquireIntent *);

static ResourceXApplyResult
ordinary_unsent(const ResourceXInstallClaimJoinObservation *observation,
	const ResourceXDecodedAcquireIntent *episode, ResourceXInstallClaimJoinObservation *out)
{
#ifdef FIRST_FILL_PARENT_RED
	/* Parent has no exact unsent-retirement API. Retention is its only
	 * fail-closed choice; generic round clear is not an owner proof. */
	return RESOURCE_X_APPLY_BAD_STATE;
#else
	return cluster_pcm_lock_resource_x_first_fill_retire_unsent_exact(observation, episode, out);
#endif
}

static ResourceXApplyResult
ordinary_wait(void)
{
#ifdef FIRST_FILL_PARENT_RED
	return RESOURCE_X_APPLY_BAD_STATE;
#else
	return cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&input_observation, &prepared_episode);
#endif
}

static EvidenceFixture *ordinary_wake_fixture;
static void
ordinary_material_on_enroll(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(wait_material(ordinary_wake_fixture, 0));
}

UT_TEST(test_ordinary_passive_wait_keeps_one_request_across_response_periods)
{
	EvidenceFixture f;
	ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(wait_start(&f, 0)); round = request_entry()->resource_x_bootstrap_round;
	for (unsigned n = 0; n < 4; n++) {
		fake_pcm_clock_us += UINT64_C(60000000);
		UT_ASSERT_EQ(ordinary_wait(), RESOURCE_X_APPLY_NOT_FOUND);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		UT_ASSERT_EQ(fake_cv_timed_sleep_timeout, 1000);
		UT_ASSERT_EQ(fake_cv_sleep_wait_event, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		UT_ASSERT(!fake_cv_prepared);
	}
	UT_ASSERT(wait_material(&f, 0));
	UT_ASSERT_EQ(ordinary_wait(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	complete_idle();
}

UT_TEST(test_ordinary_enroll_before_predicate_cannot_lose_completion)
{
	EvidenceFixture f; int before;
	UT_ASSERT(wait_start(&f, 0));
	ordinary_wake_fixture = &f; fake_cv_prepare_hook = ordinary_material_on_enroll;
	before = fake_cv_sleep_count;
	UT_ASSERT_EQ(ordinary_wait(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_sleep_count, before);
	UT_ASSERT(!fake_cv_prepared); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	complete_idle();
}

UT_TEST(test_ordinary_incomplete_image_waits_but_wrong_stage_does_not)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(wait_start(&f, 2));
	UT_ASSERT_EQ(evidence_offer(&f.decision, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ordinary_wait(), RESOURCE_X_APPLY_NOT_FOUND);
	request_entry()->resource_x_bootstrap_round.delivery_executor_purpose = 99;
	UT_ASSERT(ordinary_wait() != RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(!fake_cv_prepared); complete_idle();
}

UT_TEST(test_ordinary_unsent_round_retires_without_touching_native_io)
{
	ResourceXInstallClaimJoinObservation observation, out;
	ResourceXDecodedAcquireIntent episode;
	ClusterPcmOwnEntry own;
	uint64 attempt;
	request_start(0, false);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_intent_exact(&request_buf, &request_base,
		request_token, RESOURCE_X_ACQUIRE_FIRST_FILL, &request_admission, NULL, &episode), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
		&request_prepared.common.logical_assertion, 0, 17, 31, 77, 51, 61, &observation), RESOURCE_X_APPLY_APPLIED);
	own = request_own; attempt = observation.request.assertion_sequence;
	UT_ASSERT_EQ(ordinary_unsent(&observation, &episode, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.phase, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, attempt);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, attempt);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
}

UT_TEST(test_ordinary_handoff_requires_the_original_full_e_canceler)
{
	EvidenceFixture f;
	ResourceXInstallClaimJoinObservation before, out;
	ClusterPcmResourceXBootstrapRound round;
	request_start(0, false);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
		&request_prepared.common.logical_assertion, 0, 17, 31, 77, 51, 61, &before), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_start(&f, 0));
	round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(ordinary_unsent(&before, &prepared_episode, &out), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(out.entry_binding_generation, input_observation.entry_binding_generation);
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	wait_done();
}

UT_TEST(test_ordinary_unsent_retirement_rejects_send_or_authority_debt)
{
	for (unsigned n = 0; n < 6; n++) {
		ResourceXInstallClaimJoinObservation observation, out;
		ResourceXDecodedAcquireIntent episode;
		ClusterPcmResourceXBootstrapRound round;
		request_start(0, false);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_intent_exact(&request_buf, &request_base,
			request_token, RESOURCE_X_ACQUIRE_FIRST_FILL, &request_admission, NULL, &episode), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
			&request_prepared.common.logical_assertion, 0, 17, 31, 77, 51, 61, &observation), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) request_entry()->resource_x_bootstrap_round.send.pending = 1;
		if (n == 1) request_entry()->resource_x_bootstrap_round.accepted_base = 1;
		if (n == 2) request_entry()->resource_x_bootstrap_round.delivery_executor_sequence = 1;
		if (n == 3) observation.entry_binding_generation++;
		if (n == 4) observation.request.assertion_sequence++;
		if (n == 5) request_entry()->resource_x_bootstrap_round.has_acquire_intent = 2;
		round = request_entry()->resource_x_bootstrap_round;
		UT_ASSERT(ordinary_unsent(&observation, &episode, &out) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(17);
	UT_RUN(test_ordinary_disk_path_completes_one_original_io_and_request);
	UT_RUN(test_ordinary_remote_current_s_and_x_never_read_disk);
	UT_RUN(test_ordinary_capacity_cancel_retires_only_unsent_round_then_native_io);
	UT_RUN(test_ordinary_error_inside_physical_handoff_keeps_the_exact_request_owner);
	UT_RUN(test_ordinary_cancel_while_waiting_never_creates_a_second_episode);
	UT_RUN(test_ordinary_storage_error_uses_existing_quiet_executor_cleanup);
	UT_RUN(test_ordinary_error_after_C_finishes_handoff_without_second_io_completion);
	UT_RUN(test_ordinary_resource_formation_is_independent_of_membership_epoch);
	UT_RUN(test_ordinary_completion_rechecks_master_and_transport_before_any_bytes);
	UT_RUN(test_ordinary_unavailable_observations_do_not_fail_or_repeat_request);
	UT_RUN(test_ordinary_passive_wait_rejects_lost_binding_and_closed_gate);
	UT_RUN(test_ordinary_unsent_round_retires_without_touching_native_io);
	UT_RUN(test_ordinary_handoff_requires_the_original_full_e_canceler);
	UT_RUN(test_ordinary_unsent_retirement_rejects_send_or_authority_debt);
	UT_RUN(test_ordinary_passive_wait_keeps_one_request_across_response_periods);
	UT_RUN(test_ordinary_enroll_before_predicate_cannot_lose_completion);
	UT_RUN(test_ordinary_incomplete_image_waits_but_wrong_stage_does_not);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
