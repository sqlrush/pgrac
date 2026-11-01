/* Actual ordinary first-fill and an independently owned master. Only the
 * socket transport, peer authentication and OS lock/storage leaves are
 * controlled; no master decision or native completion is manufactured.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifdef NATIVE_MASTER_HOLDER_COMPOSITION
#include "test_cluster_native_holder.c"
#else
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#if defined(NATIVE_MASTER_RESIDENT_READ_COMPOSITION)
#include "test_cluster_native_resident_lock_fixture.inc"
#elif defined(NATIVE_MASTER_RESIDENT_WRITE_COMPOSITION)
#include "test_cluster_native_resident_x_fixture.inc"
#else
#include "test_cluster_native_first_fill_fixture.inc"
#endif
#undef cluster_test_pcm_clock_gettime
#define ereport(level, rest) ordinary_report(level)
#define gcs_block_resource_x_payload_candidate native_master_payload_candidate
#define RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#include "test_cluster_resource_x_master_ack_fixture.inc"
#undef RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#undef gcs_block_resource_x_payload_candidate
#endif
#include "test_cluster_resource_x_master_done_ingress.inc"
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <poll.h>
#include <signal.h>

/* Frame bytes, not a copy of a different role's in-memory state, cross this
 * boundary. Every wait is bounded so a failed child cannot hang the suite. */
static bool nm_transfer(int fd, void *data, size_t size, bool output)
{
	unsigned char *cursor = data;
	while (size != 0) {
		struct pollfd event = { .fd = fd, .events = output ? POLLOUT : POLLIN };
		ssize_t n;
		int ready = poll(&event, 1, 5000);
		if (ready < 0 && errno == EINTR) continue;
		if (ready <= 0) return false;
#ifdef MSG_NOSIGNAL
		n = output ? send(fd, cursor, size, MSG_NOSIGNAL) : read(fd, cursor, size);
#else
		n = output ? write(fd, cursor, size) : read(fd, cursor, size);
#endif
		if (n < 0 && errno == EINTR) continue;
		if (n <= 0) return false;
		cursor += n; size -= n;
	}
	return true;
}
static bool nm_send(int fd, uint16 type, const ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 header[2] = {(uint16)type, 0}; ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(type, frame, bytes, sizeof(bytes), &header[1], &reject)
		&& nm_transfer(fd, header, sizeof(header), true)
		&& nm_transfer(fd, bytes, header[1], true);
}
static bool nm_receive(int fd, uint16 type, ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 header[2]; ResourceXWireReject reject;
	return nm_transfer(fd, header, sizeof(header), false)
		&& header[0] == (uint16)type && header[1] <= sizeof(bytes)
		&& nm_transfer(fd, bytes, header[1], false)
		&& cluster_resource_x_wire_decode(type, bytes, header[1], frame, &reject);
}
#define NM_REQUIRE(condition) do { bool ok_ = (condition); UT_ASSERT(ok_); if (!ok_) { printf("# Required: %s\n", #condition); return false; } } while (0)
static bool nm_master_enqueue(PublishFixture *f, const ResourceXDecodedFrame *request)
{
	ClusterPcmResourceXMasterRequest canonical = {0};
	unsigned matches = 0; Size offset;
	int requester = request->common.logical_assertion.requester_node;
	unsigned char *memory;
	NM_REQUIRE(requester >= 0 && requester < RESOURCE_X_PROTOCOL_NODE_LIMIT);
	memset(f, 0, sizeof(*f)); f->request = *request;
	NM_REQUIRE(acquire_canonical_start(&f->request, &f->work));
	f->plan = decision_plan_read(&f->work);
	canonical.base_authority_generation = f->plan.input.canonical_base_generation;
	canonical.resource_formation = f->work.request.resource_formation;
	canonical.master_session_incarnation = f->work.request.master_session_incarnation;
	canonical.assertion_sequence = f->work.request.assertion_sequence;
	canonical.enqueue_order = f->work.enqueue_order;
	canonical.sender_connection_generation = f->work.request.sender_connection_generation;
	canonical.source_node = -1; canonical.phase = RESOURCE_X_MASTER_QUEUED;
	memory = (unsigned char *)fake_pcm_header.data;
	offset = offsetof(ClusterPcmResourceXMasterState, requests) + requester * sizeof(canonical);
	for (Size n = offset; n + sizeof(canonical) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &canonical, sizeof(canonical)) == 0) {
			f->state = (void *)(memory + n - offset); matches++;
		}
	NM_REQUIRE(matches == 1);
	return true;
}
static bool nm_master_admit(PublishFixture *f, const ResourceXDecodedFrame *request)
{
	NM_REQUIRE(nm_master_enqueue(f, request));
	publish_apply(f);
	return true;
}
static bool nm_master_begin(PublishFixture *f, const ResourceXDecodedFrame *request)
{
	acquire_reset(); return nm_master_admit(f, request);
}
static bool nm_master_callback(ResourceXIntentSlot *slot, bool sent)
{
	ResourceXIntentSlot staged;
	if (slot->state == RESOURCE_X_INTENT_SLOT_STAGED) staged = *slot;
	else NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		slot, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&staged, sent, ++fake_pcm_clock_us));
	return true;
}
static ResourceXApplyResult nm_master_done(const ResourceXDecodedFrame *done)
{
	ClusterICEnvelope env = {0};
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = done->common.logical_assertion.requester_node; env.dest_node_id = 0; env.epoch = 17;
	env.payload_length = done->payload_bytes;
	return gcs_block_resource_x_acquire_done_ingress(&env, done, 61);
}
/* Cases vary the ordering of existing physical callbacks, never the encoded
 * decision or C. Mode3 fails a notification's physical send once. */
static bool nm_master(int fd, int mode)
{
	PublishFixture f; ResourceXDecodedFrame request, grant, done, notice;
	ClusterPcmResourceXMasterRequest empty = {0}; int requester;
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request));
	requester = request.common.logical_assertion.requester_node;
	NM_REQUIRE(nm_master_begin(&f, &request));
	grant = publish_decode(f.state->grant_intent.payload, false);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT_EQ(grant.body.acquire_decision.required_holders_bitmap, 0);
	if (mode != 1) NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	UT_ASSERT(memcmp(&done.body.acquire_completion.decision, &grant.body.acquire_decision,
		sizeof(grant.body.acquire_decision)) == 0);
	NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 1) {
		UT_ASSERT(f.state->deferred_settlement.present != 0);
		UT_ASSERT(f.state->requests[requester].phase != RESOURCE_X_MASTER_NONE);
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_APPLIED);
	}
	NM_REQUIRE(f.state->deferred_settlement.present == 3);
	if (mode == 2) NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		f.state->grant_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &notice, &(ResourceXWireReject){0}));
	if (mode == 3) {
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, false));
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
		UT_ASSERT_EQ(f.state->requests[requester].phase, RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(f.state->deferred_settlement.authenticated_sender, requester);
		fake_pcm_clock_us += 1000000;
	}
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	UT_ASSERT(memcmp(&f.state->requests[requester], &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.state->bootstrap_receipts[requester].highest_attempt_floor, request.common.assertion_sequence);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return true;
}

static int nm_fd, nm_mode;
static ResourceXDecodedFrame nm_grant;
static void nm_deliver(void)
{
	ResourceXDecodedFrame request; ResourceXRequesterJoinSnapshot joined;
	ClusterICEnvelope env = {0};
	ResourceXApplyResult result;
	ResourceXIntentSlot ticket = request_ticket(&request);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	if (!nm_send(nm_fd, RESOURCE_X_MSG_ASSERT_X, &request)) { UT_ASSERT(false); pg_re_throw(); }
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	if (!nm_receive(nm_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant)) {
		UT_ASSERT(false); pg_re_throw();
	}
	prepared_episode = request.body.acquire_intent;
	input_observation = ordinary_active->observation;
	input_grant = nm_grant;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0;
	env.dest_node_id = cluster_node_id; env.epoch = 17; env.payload_length = nm_grant.payload_bytes;
	auth_peer_boot = 31;
	result = gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED) pg_re_throw();
	if (nm_mode == 2) UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(
		&env, &nm_grant, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}
static bool nm_requester_notice(ClusterPcmResourceXMasterState **state)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 size = 0;
	ResourceXWireReject reject; unsigned matches = 0;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload);
	/* The real retained copy deliberately neutralizes the wire generation. */
	ResourceXDecodedFrame retained = nm_grant;
	retained.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	NM_REQUIRE(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &retained,
		bytes, sizeof(bytes), &size, &reject));
	for (Size n = offset % 8; n + size <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, bytes, size) == 0 && n >= offset) {
			*state = (void *)(memory + n - offset); matches++;
		}
	NM_REQUIRE(matches == 1);
	return true;
}
/* Optional read-only observation of the actual retired transport/receipt.
 * Compositions use it to replay old bytes against a real successor. */
static void (*nm_settled_observer)(const ResourceXIntentSlot *, const ResourceXDecodedFrame *);
static bool nm_parent_as(int fd, int mode, int requester)
{
	ClusterBufmgrFirstFill read;
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot staged; ResourceXDecodedFrame done, notice;
	ResourceXWireReject reject; ClusterICEnvelope env = {0};
	ResourceXRequesterJoinSnapshot joined;
	ordinary_start(&read, 0); cluster_node_id = requester; nm_fd = fd; nm_mode = mode;
	ordinary_delivery_transport = nm_deliver;
	NM_REQUIRE(ordinary_run(&read));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	NM_REQUIRE(nm_requester_notice(&state));
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->requester_settlement_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &done, &reject));
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&state->requester_settlement_intent.slot, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0;
	env.dest_node_id = cluster_node_id; env.epoch = 17; env.payload_length = notice.payload_bytes;
	auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &notice, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 2) NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(
		&env, &notice, 61, &joined) == RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 9);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	if (nm_settled_observer != NULL) nm_settled_observer(&staged, &notice);
	complete_idle(); return true;
}
static bool nm_parent(int fd, int mode) { return nm_parent_as(fd, mode, 1); }
static void nm_wait_child(pid_t child)
{
	/* IPC failure closes the peer and releases a blocked role. Only this
	 * test's own child is reaped; no other process or test environment. */
	int status = 0; pid_t reaped = 0;
	for (int n = 0; n < 100 && reaped == 0; n++) {
		reaped = waitpid(child, &status, WNOHANG);
		if (reaped == 0) poll(NULL, 0, 50);
	}
	if (reaped == 0) { kill(child, SIGKILL); reaped = waitpid(child, &status, 0); UT_ASSERT(false); }
	UT_ASSERT_EQ(reaped, child);
	UT_ASSERT(WIFEXITED(status)); if (WIFEXITED(status)) UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}
static void nm_run_roles(int mode, bool (*master)(int, int), bool (*parent)(int, int))
{
	int sockets[2]; pid_t child; bool ok;
	int created = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
	UT_ASSERT_EQ(created, 0); if (created != 0) return;
#ifdef SO_NOSIGPIPE
	for (int n = 0; n < 2; n++) { int one = 1; setsockopt(sockets[n], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)); }
#endif
	fflush(NULL); child = fork(); UT_ASSERT(child >= 0);
	if (child < 0) { close(sockets[0]); close(sockets[1]); return; }
	if (child == 0) {
		close(sockets[0]); ok = master(sockets[1], mode); close(sockets[1]);
		fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	close(sockets[1]); ok = parent(sockets[0], mode); close(sockets[0]);
	UT_ASSERT(ok);
	nm_wait_child(child);
}
static void nm_run(int mode) { nm_run_roles(mode, nm_master, nm_parent); }
UT_TEST(test_native_master_actual_disk_episode) { nm_run(0); }
UT_TEST(test_native_master_done_before_grant_callback) { nm_run(1); }
UT_TEST(test_native_master_duplicate_frames_complete_once) { nm_run(2); }
UT_TEST(test_native_master_failed_notice_send_keeps_responsibility) { nm_run(3); }
int main(void)
{
	UT_PLAN(4);
	UT_RUN(test_native_master_actual_disk_episode);
	UT_RUN(test_native_master_done_before_grant_callback);
	UT_RUN(test_native_master_duplicate_frames_complete_once);
	UT_RUN(test_native_master_failed_notice_send_keeps_responsibility);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
