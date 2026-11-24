/* Actual native writeback joined to the original holder service.
 * Storage handles and event scheduling are controlled host leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
int holder_clean_fixture_main(void);
#define NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_holder.c"

static bool writeback_start(HolderFixture *f, PcmState target)
{
	if (!holder_start_target(f, PCM_STATE_X, target)) return false;
	holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = 0;
	holder_written_wal_flushes = 0;
	holder_redirty = false;
	local_pi_writer_ready = local_pi_covered = false;
	BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
	return true;
}
#define WRITEBACK_START(f, target) do { bool ok = writeback_start(f, target); \
	UT_ASSERT(ok); if (!ok) return; } while (0)
#define WRITEBACK_PROBE(next) do { bool found = holder_probe(next); \
	UT_ASSERT(found); if (!found) return; } while (0)
static void writeback_dirty(void)
{
	pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
}
static void writeback_idle(void)
{
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS) == 0);
	UT_ASSERT((pg_atomic_read_u64(&request_own.write_io_generation) & 1) == 0);
	holder_no_debt();
}
static void writeback_file_matches(void)
{
	PGAlignedBlock actual, shared;
	UT_ASSERT_EQ(pread(fileno(disk_file), actual.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT(memcmp(actual.data, holder_written.data, BLCKSZ) == 0);
	UT_ASSERT(PageIsVerifiedExtended(actual.data, request_buf.tag.blockNum, 0));
	memcpy(shared.data, disk_shared.data, BLCKSZ);
	/* Only the real disk checksum copy may change this field. */
	((PageHeader)shared.data)->pd_checksum = ((PageHeader)actual.data)->pd_checksum;
	UT_ASSERT(memcmp(actual.data, shared.data, BLCKSZ) == 0);
}

UT_TEST(dirty_current_write_completes_before_original_holder_publication)
{
	HolderFixture f;
	volatile bool caught = false;
	volatile ResourceXApplyResult result = RESOURCE_X_APPLY_BAD_STATE;
	WRITEBACK_START(&f, PCM_STATE_S);
	writeback_dirty();
	holder_write_error = false;
	PG_TRY(); { result = holder_capture(&f); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (caught || result != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_write_notes, 1);
	UT_ASSERT_EQ(holder_output_wakes, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_IO_IN_PROGRESS)) == 0);
	writeback_file_matches();
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	writeback_idle();
}

UT_TEST(permanent_current_flushes_WAL_before_DATA_without_changing_shared_checksum)
{
	HolderFixture f; uint16 checksum;
	WRITEBACK_START(&f, PCM_STATE_S);
	pg_atomic_fetch_or_u32(&request_buf.state, BM_PERMANENT);
	writeback_dirty(); checksum = ((PageHeader)disk_shared.data)->pd_checksum;
	/* Hint-class bytes change in memory; only the disk copy is checksummed. */
	disk_shared.data[BLCKSZ - 1] ^= 0x5a;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(holder_wal_flushes, 1); UT_ASSERT_EQ(holder_writes, 1);
	UT_ASSERT_EQ(((PageHeader)disk_shared.data)->pd_checksum, checksum);
	UT_ASSERT_EQ(holder_written_wal_flushes, 1);
	writeback_file_matches();
	{
		ResourceXDecodedFrame image; ResourceXAcquireSSourceState phase; ClusterPcmOwnSnapshot native; int32 id;
		HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0,
			&id, &native, &image, &phase), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, disk_shared.data, BLCKSZ) == 0);
	}
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED); writeback_idle();
}

/* Split an existing writer at the real IO boundary; no success or bytes are
 * supplied by this event scheduler. Both native IO owners remain real. */
static void writeback_existing_output_end(bool error)
{
	LWLock *lock = BufferDescriptorGetContentLock(&request_buf);
	LWLockAcquire(lock, LW_SHARED);
	if (!error) {
		char *bytes = PageSetChecksumCopy(disk_shared.data, request_buf.tag.blockNum);
		pg_atomic_fetch_and_u32(&request_buf.state, ~BM_JUST_DIRTIED);
		holder_output_write(holder_output_open(BufTagGetRelFileLocator(&request_buf.tag), InvalidBackendId),
			request_buf.tag.forkNum, request_buf.tag.blockNum, bytes, false);
		holder_output_end(&request_buf, true, 0);
	} else holder_output_abort(1);
	holder_lock_release(lock);
}
UT_TEST(existing_output_event_releases_same_parked_service_without_new_request)
{
	for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		HolderFixture f; ResourceXSourceDrainWork next; uint64 attempt;
		WRITEBACK_START(&f, target); writeback_dirty();
		UT_ASSERT(holder_output_start(&request_buf, false));
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 1);
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
			RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next)); attempt = f.work.attempt_generation;
		writeback_existing_output_end(false);
		WRITEBACK_PROBE(&next);
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0);
		UT_ASSERT_EQ(next.attempt_generation, attempt + 2); f.work = next;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
			RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(ordinary_sends, 0);
		UT_ASSERT(!holder_probe(&next)); writeback_file_matches(); writeback_idle();
	}
}
UT_TEST(output_release_before_BUSY_return_is_not_lost)
{
	HolderFixture f; ResourceXSourceDrainWork next;
	WRITEBACK_START(&f, PCM_STATE_S); writeback_dirty();
	UT_ASSERT(holder_output_start(&request_buf, false));
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
	/* IO/content release races the old odd attempt's completion. */
	writeback_existing_output_end(false); UT_ASSERT(!holder_probe(&next));
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	WRITEBACK_PROBE(&next);
	UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0); f.work = next;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED); writeback_idle();
}

/* An active ITL is an input to the probe, not progress made by the probe.
 * Schedule an external content release on either side of BUSY completion;
 * even an unrelated release permits just one new observation, not a loop. */
static void writeback_itl_progress(bool release_before_complete)
{
	for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		HolderFixture f; ResourceXSourceDrainWork next; PageHeader page;
		WRITEBACK_START(&f, target);
		page = (PageHeader)disk_shared.data;
		page->pd_flags |= PD_HAS_ITL; page->pd_special = BLCKSZ - CLUSTER_ITL_ARRAY_SIZE;
		page->pd_upper = page->pd_special;
		memset(ClusterPageGetItlSlots(disk_shared.data), 0, CLUSTER_ITL_ARRAY_SIZE);
		ClusterPageGetItlSlots(disk_shared.data)[0].flags = ITL_FLAG_ACTIVE;
		for (int event = 0; event < 2; event++) {
			HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
			UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
			UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
			if (release_before_complete) {
				LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
				holder_lock_release(BufferDescriptorGetContentLock(&request_buf));
			}
			HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
				RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
			if (!release_before_complete) {
				UT_ASSERT(!holder_probe(&next));
				LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
				holder_lock_release(BufferDescriptorGetContentLock(&request_buf));
			}
			WRITEBACK_PROBE(&next);
			UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0);
			UT_ASSERT_EQ(next.attempt_generation, f.work.attempt_generation + 2); f.work = next;
		}
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
			RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next));
		/* Controlled page-owner input; this is not an undo/commit proof. */
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		ClusterPageGetItlSlots(disk_shared.data)[0].flags = ITL_FLAG_FREE;
		holder_lock_release(BufferDescriptorGetContentLock(&request_buf));
		WRITEBACK_PROBE(&next); f.work = next;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
			RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next)); UT_ASSERT_EQ(ordinary_sends, 0);
		UT_ASSERT_EQ(holder_writes, 0); UT_ASSERT_EQ(disk_reads, 0); writeback_idle();
	}
}
UT_TEST(active_ITL_probe_waits_for_later_external_progress)
{
	writeback_itl_progress(false);
}
UT_TEST(active_ITL_probe_preserves_external_progress_before_BUSY_completion)
{
	writeback_itl_progress(true);
}
UT_TEST(redirty_during_write_retains_same_source_and_requires_its_next_write)
{
	HolderFixture f; ResourceXSourceDrainWork next;
	WRITEBACK_START(&f, PCM_STATE_S); writeback_dirty(); holder_redirty = true;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_JUST_DIRTIED))
		== (BM_DIRTY | BM_JUST_DIRTIED));
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	/* The actual content release already occurred during the running claim. */
	WRITEBACK_PROBE(&next);
	UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0); f.work = next;
	holder_redirty = false;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(holder_writes, 2); UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 4);
	writeback_file_matches(); writeback_idle();
}
UT_TEST(write_ERROR_aborts_only_original_output_and_preserves_failed_dirty_source)
{
	HolderFixture f; volatile bool caught = false;
	WRITEBACK_START(&f, PCM_STATE_S); writeback_dirty(); holder_write_error = true;
	PG_TRY(); { (void)holder_capture(&f); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_write_notes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_IO_ERROR | BM_IO_IN_PROGRESS))
		== (BM_DIRTY | BM_IO_ERROR));
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0); writeback_idle();
}

/* Certified PAGE/WAL input is a provider boundary, not a fabricated DATA
 * receipt. Actual install, source flush gates, local PI and carrier run below. */
static ClusterPageWalBindingV1 writeback_install_wal(void)
{
	ClusterPageWalBindingV1 wal = {0}; ClusterPageWalInstallV1 install = {0};
	wal.source.claim.identity.system_identifier = 123;
	wal.source.claim.identity.origin_node_id = 0; wal.source.claim.identity.origin_thread_id = 1;
	wal.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	wal.source.claim.identity.thread_claim_created_at = 8; wal.source.claim.identity.root_lineage_seq = 9;
	wal.source.claim.identity.storage_uuid[0] = 10; wal.source.claim.identity.authority_uuid[0] = 11;
	wal.source.claim.database_incarnation = 12; wal.source.claim.max_config_generation = 13;
	wal.source.claim.claim_sha256[0] = 14; wal.source.timeline = 1;
	wal.identity.system_identifier = 123;
	memcpy(wal.identity.storage_uuid, wal.source.claim.identity.storage_uuid, 16);
	wal.identity.locator = BufTagGetRelFileLocator(&request_buf.tag);
	wal.identity.forknum = request_buf.tag.forkNum; wal.identity.blockno = request_buf.tag.blockNum;
	wal.version.segment_incarnation[0] = 15; wal.version.mutation_token = 93;
	wal.record_start = 100; wal.record_end = 12345; wal.record_crc = 30; wal.rmid = RM_HEAP_ID;
	wal.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	local_pi_writer = wal.source; local_pi_writer_ready = true;
	pg_atomic_fetch_or_u32(&request_buf.state, BM_PERMANENT);
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT(PageSetLSNOrigin(disk_shared.data, 0));
	UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &wal, disk_shared.data, &install));
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &install));
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	return wal;
}
UT_TEST(written_PAGE_WAL_and_PI_survive_holder_delivery_without_DATA_receipt)
{
	for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		HolderFixture f; ClusterPageWalBindingV1 wal; ClusterPcmLocalPiSnapshotV1 before = {0}, after = {0};
		ClusterPcmLocalPiFloorV1 floor = {0};
		WRITEBACK_START(&f, target); wal = writeback_install_wal(); writeback_dirty();
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_write_notes, 1);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &before));
		UT_ASSERT(cluster_page_wal_same_mutation_v1(&before.last, &wal));
		UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &floor));
		UT_ASSERT_EQ(floor.bounded, 1); UT_ASSERT_EQ(floor.floor, wal.record_start);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		if (target == PCM_STATE_X) {
			UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 1);
			UT_ASSERT(f.master.state->pi_carrier.last.source_flags != 0);
		}
		for (int half = 0; half < 2; half++) holder_send_control(&f, half, true);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &floor));
		/* Publication adds the independent immutable carrier lower. Both
		 * it and the original logical PI remain after physical delivery. */
		UT_ASSERT_EQ(floor.bounded, 2); UT_ASSERT_EQ(floor.floor, wal.record_start);
		/* A real host fsync is still not a typed DATA certificate. The
		 * existing retirement owner must reject the missing receipt. */
		UT_ASSERT_EQ(fsync(fileno(disk_file)), 0);
		UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&after, NULL, NULL, NULL, 0, NULL));
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		UT_ASSERT(!local_pi_covered); writeback_file_matches(); writeback_idle();
	}
}
UT_TEST(changed_WAL_source_refuses_without_writing_or_publishing_image)
{
	HolderFixture f; ClusterPageWalBindingV1 wal; volatile bool caught = false;
	WRITEBACK_START(&f, PCM_STATE_S); wal = writeback_install_wal(); writeback_dirty();
	wal.flags = 0;
	{
		ClusterPageWalInstallV1 install = {0};
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &wal, disk_shared.data, &install));
		UT_ASSERT(cluster_page_wal_publish_install_v1(1, &install));
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	}
	local_pi_writer.claim.identity.origin_owner_incarnation++;
	PG_TRY(); { (void)holder_capture(&f); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(holder_writes, 0); UT_ASSERT_EQ(holder_write_notes, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_IO_ERROR)) == (BM_DIRTY | BM_IO_ERROR));
	writeback_idle();
}
UT_TEST(retained_PI_is_never_an_output_writer_even_if_marked_dirty)
{
	HolderFixture f; uint64 generation;
	WRITEBACK_START(&f, PCM_STATE_X);
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI); generation = cluster_pcm_own_gen_get(0);
	writeback_dirty();
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_SHARED);
	holder_flush(&request_buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(holder_writes, 0); UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_DIRTY) != 0); writeback_idle();
}
UT_TEST(dirty_activation_fence_refuses_before_output_IO_or_file_write)
{
	HolderFixture f; volatile bool caught = false;
	WRITEBACK_START(&f, PCM_STATE_S); writeback_dirty();
	pg_atomic_write_u64(&request_own.writer_activation_token, 77);
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_SHARED);
	PG_TRY(); { holder_flush(&request_buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT(caught); UT_ASSERT_EQ(holder_writes, 0); UT_ASSERT_EQ(holder_output_wakes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 0);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_token_get(0), 77); writeback_idle();
}

int main(void)
{
	UT_PLAN(12);
	UT_RUN(dirty_current_write_completes_before_original_holder_publication);
	UT_RUN(permanent_current_flushes_WAL_before_DATA_without_changing_shared_checksum);
	UT_RUN(existing_output_event_releases_same_parked_service_without_new_request);
	UT_RUN(output_release_before_BUSY_return_is_not_lost);
	UT_RUN(redirty_during_write_retains_same_source_and_requires_its_next_write);
	UT_RUN(write_ERROR_aborts_only_original_output_and_preserves_failed_dirty_source);
	UT_RUN(written_PAGE_WAL_and_PI_survive_holder_delivery_without_DATA_receipt);
	UT_RUN(changed_WAL_source_refuses_without_writing_or_publishing_image);
	UT_RUN(retained_PI_is_never_an_output_writer_even_if_marked_dirty);
	UT_RUN(dirty_activation_fence_refuses_before_output_IO_or_file_write);
	UT_RUN(active_ITL_probe_waits_for_later_external_progress);
	UT_RUN(active_ITL_probe_preserves_external_progress_before_BUSY_completion);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
