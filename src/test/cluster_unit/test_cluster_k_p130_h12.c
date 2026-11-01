/* Native output IO precedes an ordinary current request; local service
 * attempts keep their original identity through release events.
 * Actor locks, authentication, pins and storage handles are host leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
int holder_clean_fixture_main(void);
#define NATIVE_MASTER_HOLDER_COMPOSITION
#define NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_master_fixture.inc"
#undef NATIVE_MASTER_HOLDER_COMPOSITION
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H12 assertion-OFF requires an assertion-disabled configuration"
#endif
static int h12_kind;
static bool h12_race;
static unsigned h12_captures, h12_parks, h12_releases, h12_images, h12_requests;
static ResourceXDecodedFrame h12_request;
static PGIOAlignedBlock h12_source;
static uint64 h12_base, h12_E, h12_A;
static unsigned h12_native_calls;
static void h12_keep_native(PcmState mode);
static ResourceXApplyResult h12_capture(HolderFixture *f);
static void h12_census(void);
static bool h12_run(ClusterBufmgrFirstFill *read);
static bool h12_send_frame(int fd, uint16 type, const ResourceXDecodedFrame *frame);
#define holder_native_start h12_keep_native
#define holder_capture h12_capture
#define holder_no_debt h12_census
#define ordinary_run h12_run
#define nm_send h12_send_frame
#include "test_cluster_k_p130_h12_current.inc"
#undef nm_send
#undef ordinary_run
#undef holder_no_debt
#undef holder_capture
#undef holder_native_start
#include "test_cluster_k_p130_h12_output.inc"
#include "test_cluster_k_p130_h02_leaves.inc"

static ClusterICSendResult h12_send(uint8 type, int32 node, const void *bytes,
	uint32 length, const ResourceXIntentSlot *intent)
{
	uint16 header[2] = {type, (uint16)length}; ResourceXWireReject reject;
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (!cluster_resource_x_wire_decode(type, bytes, length, &h12_request, &reject)) return CLUSTER_IC_SEND_HARD_ERROR;
	h12_requests++;
	return nm_transfer(nc_fd, header, sizeof(header), true)
		&& nm_transfer(nc_fd, (void *)bytes, length, true) ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}
static bool h12_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	return node == 0 && generation == 51;
}
#define cluster_ic_tier1_send_resource_x_intent h12_send
#define cluster_sf_peer_capability_generation_matches h12_peer
#include "test_cluster_k_p130_h01_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent
static void h12_deliver(void)
{
	BackendType previous = MyBackendType;
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	MyBackendType = B_LMS; UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1); MyBackendType = previous;
	UT_ASSERT_EQ(h12_requests, 1); nc_request = h12_request;
	if (!nm_receive(nc_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image)) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(nc_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT_EQ(nc_image.body.acquire_image.source.decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	prepared_episode = h12_request.body.acquire_intent;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = nc_image.payload_bytes; auth_peer_boot = 31;
	if (gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); pg_re_throw(); }
}
static bool h12_run(ClusterBufmgrFirstFill *read)
{
	bool ok; ordinary_delivery_transport = h12_deliver; ok = ordinary_run(read);
	UT_ASSERT(ok); UT_ASSERT_EQ(h12_requests, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	printf("# H12 R kind=%d race=%d E=" UINT64_FORMAT " A=" UINT64_FORMAT " gen=" UINT64_FORMAT
		" crc=%u REQUEST=%u C=%u IO=%u reads=%u\n", h12_kind, h12_race,
		h12_request.common.assertion_sequence, nc_image.common.authority_generation,
		cluster_pcm_own_gen_get(0), cluster_gcs_block_compute_checksum(disk_shared.data),
		h12_requests, complete_commits, complete_io_calls, disk_reads);
	return ok;
}
static bool h12_send_frame(int fd, uint16 type, const ResourceXDecodedFrame *frame)
{
	if (type == RESOURCE_X_MSG_IMAGE_OR_GRANT && frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		h12_images++; UT_ASSERT_EQ(h12_images, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
		UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_output_wakes, 1);
		UT_ASSERT(memcmp(h12_source.data, frame->body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	}
	return nm_send(fd, type, frame);
}
static void h12_keep_native(PcmState mode)
{
	UT_ASSERT_EQ(mode, PCM_STATE_X); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 1);
	UT_ASSERT(ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
}
static bool h12_park(HolderFixture *f, ResourceXApplyResult result, bool expect_idle)
{
	ResourceXSourceDrainWork next; bool found;
	NM_REQUIRE(result == RESOURCE_X_APPLY_BAD_STATE);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f->work, result) == RESOURCE_X_APPLY_APPLIED);
	h12_parks++;
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h12_base);
	UT_ASSERT_EQ(h12_images, 0); UT_ASSERT_EQ(f->master.state->holder_image.valid, 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(holder_pins, 0);
	UT_ASSERT(memcmp(h12_source.data, disk_shared.data, BLCKSZ) == 0);
	found = holder_probe(&next);
	printf("# H12 park kind=%d race=%d stage=%u attempt=" UINT64_FORMAT " unexpected_work=%d IOgen=" UINT64_FORMAT " image=0\n",
		h12_kind, h12_race, h12_parks, f->work.attempt_generation, expect_idle && found,
		pg_atomic_read_u64(&request_own.write_io_generation));
	if (expect_idle) { UT_ASSERT(!found); return !found; }
	NM_REQUIRE(found);
	UT_ASSERT(memcmp(&next.request, &f->work.request, sizeof(next.request)) == 0);
	UT_ASSERT_EQ(next.attempt_generation, f->work.attempt_generation + 2); f->work = next;
	return true;
}
static bool h12_resume(HolderFixture *f)
{
	ResourceXSourceDrainWork next; NM_REQUIRE(holder_probe(&next));
	UT_ASSERT(memcmp(&next.request, &f->work.request, sizeof(next.request)) == 0);
	UT_ASSERT_EQ(next.attempt_generation, f->work.attempt_generation + 2); f->work = next; return true;
}
static ResourceXApplyResult h12_capture(HolderFixture *f)
{
	ResourceXApplyResult result;
	h12_captures++; h12_base = cluster_pcm_own_gen_get(0);
	h12_E = f->block.common.assertion_sequence; h12_A = f->block.common.authority_generation;
	UT_ASSERT_EQ(h12_captures, 1); UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 1);
	UT_ASSERT_EQ(f->block.body.acquire_decision.directory_mode, PCM_STATE_X);
	UT_ASSERT_EQ(f->block.body.acquire_decision.source_node, 0);
	memcpy(h12_source.data, disk_shared.data, BLCKSZ);
	h12_native_calls++; result = holder_capture(f);
	if (h12_race) { writeback_existing_output_end(false); h12_releases++; }
	if (!h12_park(f, result, !h12_race)) return RESOURCE_X_APPLY_BAD_STATE;
	if (!h12_race) { writeback_existing_output_end(false); h12_releases++; if (!h12_resume(f)) return RESOURCE_X_APPLY_BAD_STATE; }
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_output_wakes, 1);
	if (h12_kind != 0) {
		if (h12_kind == 1) holder_content_busy = true;
		h12_native_calls++; result = holder_capture(f);
		if (!h12_park(f, result, true)) return RESOURCE_X_APPLY_BAD_STATE;
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		if (h12_kind == 1) holder_content_busy = false;
		else {
			UT_ASSERT(cluster_itl_page_has_active_slot((Page)disk_shared.data));
			ClusterPageGetItlSlots(disk_shared.data)[0].flags = ITL_FLAG_FREE;
			memcpy(h12_source.data, disk_shared.data, BLCKSZ);
		}
		holder_lock_release(BufferDescriptorGetContentLock(&request_buf)); h12_releases++;
		if (!h12_resume(f)) return RESOURCE_X_APPLY_BAD_STATE;
	}
	h12_native_calls++; result = holder_capture(f);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h12_base);
	return result;
}
static void h12_census(void)
{
	UT_ASSERT_EQ(h12_captures, 1); UT_ASSERT_EQ(h12_images, 1);
	UT_ASSERT_EQ(h12_parks, h12_kind == 0 ? 1 : 2); UT_ASSERT_EQ(h12_releases, h12_parks);
	UT_ASSERT_EQ(holder_pins, 0); UT_ASSERT_EQ(holder_fuses, 0);
	UT_ASSERT_EQ(holder_io_enters, holder_io_ends); UT_ASSERT_EQ(holder_interrupt_repairs, holder_interrupt_releases);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1)); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	printf("# H12 H kind=%d race=%d E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" base=" UINT64_FORMAT " gen=" UINT64_FORMAT " service=1 parks=%u events=%u SHIP=%u writes=%u IOgen=2 native_attempts=%u\n",
		h12_kind, h12_race, h12_E, h12_A,
		h12_base, cluster_pcm_own_gen_get(0), h12_parks, h12_releases, h12_images, holder_writes, h12_native_calls);
	holder_no_debt();
}
static bool h12_master(int fd, int mode)
{
	uint64 ready = 1; LWLock *lock; BufferTag tag;
	(void)mode; holder_native_start(PCM_STATE_X);
	tag = request_base.tag; request_buf.tag = tag;
	BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
	holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = holder_written_wal_flushes = 0;
	holder_redirty = false; local_pi_writer_ready = local_pi_covered = false;
	if (h12_kind == 2) {
		PageHeader page = (PageHeader)disk_bytes.data;
		page->pd_flags |= PD_HAS_ITL; page->pd_special = BLCKSZ - CLUSTER_ITL_ARRAY_SIZE;
		memset(ClusterPageGetItlSlots(disk_bytes.data), 0, CLUSTER_ITL_ARRAY_SIZE);
		ClusterPageGetItlSlots(disk_bytes.data)[0].flags = ITL_FLAG_ACTIVE;
	}
	disk_bytes.data[BLCKSZ - 16] = 0x7a;
	((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, tag.blockNum);
	disk_store(); memcpy(disk_shared.data, disk_bytes.data, BLCKSZ);
	pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
	lock = BufferDescriptorGetContentLock(&request_buf); LWLockAcquire(lock, LW_SHARED);
	NM_REQUIRE(holder_output_start(&request_buf, false)); holder_lock_release(lock);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 1);
	printf("# H12 IO-before-REQUEST kind=%d race=%d tag=%u IOgen=1\n", h12_kind, h12_race, tag.blockNum);
	NM_REQUIRE(nm_transfer(fd, &ready, sizeof(ready), true));
	return nc_master(fd, 1);
}
static bool h12_parent(int fd, int mode)
{
	uint64 ready = 0; (void)mode; NM_REQUIRE(nm_transfer(fd, &ready, sizeof(ready), false)); NM_REQUIRE(ready == 1);
	return nc_parent(fd, 1);
}
static void h12_case(int kind, bool race)
{
	h12_kind = kind; h12_race = race;
	h12_captures = h12_parks = h12_releases = h12_images = h12_requests = h12_native_calls = 0;
	h12_E = h12_A = 0; memset(&nc_image, 0, sizeof(nc_image)); memset(&nc_request, 0, sizeof(nc_request));
	memset(&h12_request, 0, sizeof(h12_request));
	nm_run_roles(1, h12_master, h12_parent);
}
UT_TEST(test_k_h12_output_then_request_then_release) { h12_case(0, false); }
UT_TEST(test_k_h12_output_release_before_BUSY_completion) { h12_case(0, true); }
UT_TEST(test_k_h12_output_then_content_release_same_service) { h12_case(1, false); }
UT_TEST(test_k_h12_early_output_then_content_release_same_service) { h12_case(1, true); }
UT_TEST(test_k_h12_output_then_active_ITL_parks_until_release) { h12_case(2, false); }
UT_TEST(test_k_h12_early_output_then_active_ITL_parks_until_release) { h12_case(2, true); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H12 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H12 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(6);
	UT_RUN(test_k_h12_output_then_request_then_release);
	UT_RUN(test_k_h12_output_release_before_BUSY_completion);
	UT_RUN(test_k_h12_output_then_content_release_same_service);
	UT_RUN(test_k_h12_early_output_then_content_release_same_service);
	UT_RUN(test_k_h12_output_then_active_ITL_parks_until_release);
	UT_RUN(test_k_h12_early_output_then_active_ITL_parks_until_release);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
