/*-------------------------------------------------------------------------
 * test_cluster_ko_drop_io.c
 *    Original peer COMMIT and raw-I/O exclusion lifecycle.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ko_drop_io.c
 *
 * GES, physical READY prerequisites and transport are explicit fixtures.
 * The operation, marker, exclusion and reclamation consumers are real.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_gcs_block.h"

static ClusterKoSharedMessageV3
ready_operation(ClusterKoSharedMessageV3 request)
{
	ClusterKoPeerFenceHandleV3 handle;
	ClusterKoPeerFenceStateV3 state;
	local_scan_rel_number = request.base.key.locator.relNumber;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&request, &handle, &state));
	UT_ASSERT(fixture_completed_fence_ready(&request, &handle));
	fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
	return request;
}

static bool
commit_operation(const ClusterKoSharedMessageV3 *request)
{
	return cluster_ko_peer_commit_v3(request->base.origin_node, request->base.epoch,
									 request->base.origin_boot, request->base.peer_boot,
									 request->base.batch_id);
}

UT_TEST(test_drop_identity_survives_two_full_active_slot_reclamations)
{
	ClusterKoSharedMessageV3 first = peer_fence_fixture(20000);
	ClusterKoPeerAdmissionV3 sample;
	uint64 revision;

	for (unsigned round = 0; round < 2; round++) {
		for (unsigned i = 0; i < 64; i++) {
			ClusterKoSharedMessageV3 request = first;
			request.base.batch_id += round * 64 + i;
			request.base.key.locator.relNumber += round * 64 + i;
			request = ready_operation(request);
			UT_ASSERT(commit_operation(&request));
		}
		UT_ASSERT(cluster_ko_peer_fence_gc_tick_v3());
		for (unsigned i = 0; i < 64; i++)
			UT_ASSERT_EQ(storage.peer_fences[i].state, CLUSTER_KO_PEER_FENCE_EMPTY);
	}
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(first.base.key.locator, &sample));
	UT_ASSERT(!cluster_ko_peer_delivery_begin_v3(first.base.key.locator, &revision));
	UT_ASSERT(ko_peer_request_ended_or_conflicts_v3(&first));
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(first.base.key.locator));
}

UT_TEST(test_truncate_keeps_original_reclamation_semantics)
{
	ClusterKoSharedMessageV3 first = peer_fence_fixture(21000);
	ClusterKoPeerAdmissionV3 sample;
	first.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
	for (unsigned i = 0; i < 64; i++) {
		ClusterKoSharedMessageV3 request = first;
		request.base.batch_id += i;
		request.base.key.locator.relNumber += i;
		request = ready_operation(request);
		UT_ASSERT(commit_operation(&request));
	}
	UT_ASSERT(cluster_ko_peer_fence_gc_tick_v3());
	UT_ASSERT(cluster_ko_peer_admission_begin_v3(first.base.key.locator, &sample));
}

static ClusterKoSharedMessageV3 io_request;
static bool marker_during_read, raw_read_error;
static bool raw_open_error, raw_verify_ok = true;
static unsigned raw_reads, raw_opens;

static SMgrRelation
raw_io_open(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, io_request.base.key.locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	raw_opens++;
	if (raw_open_error) {
		marker_during_read = commit_operation(&io_request);
		pg_re_throw();
	}
	return (SMgrRelation)1;
}

static void
raw_io_read(SMgrRelation rel, ForkNumber fork, BlockNumber block, void *page)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT_EQ(block, 4);
	raw_reads++;
	marker_during_read = commit_operation(&io_request);
	if (raw_read_error)
		pg_re_throw();
	memset(page, 0, BLCKSZ);
	((PageHeader)page)->pd_block_scn = 93;
}

static bool
raw_io_verified(Page page, ForkNumber fork, BlockNumber block, int flags)
{
	return raw_verify_ok; /* Checksum implementation is an explicit boundary. */
}

#define smgropen raw_io_open
#define smgrread raw_io_read
#define PageIsVerifiedForFork raw_io_verified
#include "test_cluster_ko_raw_io.inc"
#undef smgropen
#undef smgrread
#undef PageIsVerifiedForFork

/* Only storage and descriptor primitives are fixtures; the GCS consumers
 * and original KO entry/marker/exit implementation below are unmodified. */
static BufferDesc refresh_buffer;
static PGIOAlignedBlock refresh_page;
static LWLock refresh_lock;
static bool refresh_locked, refresh_write_allowed, marker_during_copy;
static uint32 refresh_state;
static unsigned metadata_reads;

static bool
refresh_acquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &refresh_lock && mode == LW_EXCLUSIVE && !refresh_locked);
	refresh_locked = true;
	return true;
}

static void
refresh_release(LWLock *lock)
{
	UT_ASSERT(lock == &refresh_lock && refresh_locked);
	refresh_locked = false;
}

static bool
refresh_write_permitted(BufferDesc *buf)
{
	UT_ASSERT(buf == &refresh_buffer && refresh_locked);
	marker_during_copy = commit_operation(&io_request);
	return refresh_write_allowed;
}

static void
raw_release(SMgrRelation rel)
{
	UT_ASSERT(rel == (SMgrRelation)1);
}

static BlockNumber
raw_nblocks(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(rel == (SMgrRelation)1 && fork == MAIN_FORKNUM);
	metadata_reads++;
	marker_during_read = commit_operation(&io_request);
	if (raw_read_error)
		pg_re_throw();
	return 4;
}

#define smgropen raw_io_open
#define smgrread raw_io_read
#define smgrrelease raw_release
#define smgrnblocks raw_nblocks
#define PageIsVerifiedForFork raw_io_verified
#define BufferDescriptorGetContentLock(buf) (&refresh_lock)
#define LWLockAcquire refresh_acquire
#define LWLockRelease refresh_release
#define cluster_bufmgr_pcm_x_content_write_permitted refresh_write_permitted
#define LockBufHdr(buf) refresh_state
#define UnlockBufHdr(buf, state) ((void)0)
#undef BufHdrGetBlock
#define BufHdrGetBlock(buf) ((Block)refresh_page.data)
#include "test_cluster_ko_raw_consumers.inc"
#undef smgropen
#undef smgrread
#undef smgrrelease
#undef smgrnblocks
#undef PageIsVerifiedForFork
#undef BufferDescriptorGetContentLock
#undef LWLockAcquire
#undef LWLockRelease
#undef cluster_bufmgr_pcm_x_content_write_permitted
#undef LockBufHdr
#undef UnlockBufHdr
#undef BufHdrGetBlock

static BufferTag
raw_consumer_fixture(unsigned number)
{
	BufferTag tag;
	io_request = ready_operation(peer_fence_fixture(40000 + number));
	InitBufferTag(&tag, &io_request.base.key.locator, MAIN_FORKNUM, 4);
	raw_reads = raw_opens = metadata_reads = 0;
	marker_during_read = marker_during_copy = false;
	raw_open_error = raw_read_error = false;
	raw_verify_ok = refresh_write_allowed = true;
	refresh_locked = false;
	refresh_state = 0;
	memset(&refresh_buffer, 0, sizeof(refresh_buffer));
	refresh_buffer.tag = tag;
	memset(refresh_page.data, 0x5a, BLCKSZ);
	return tag;
}

UT_TEST(test_scn_probe_marker_waits_for_normal_error_and_refusal_exit)
{
	for (unsigned failure = 0; failure < 4; failure++) {
		BufferTag tag = raw_consumer_fixture(failure);
		SCN scn = 777;
		volatile bool caught = false;
		volatile bool result = false;
		unsigned calls;
		raw_open_error = failure == 1;
		raw_read_error = failure == 2;
		raw_verify_ok = failure != 3;
		PG_TRY();
		{
			result = cluster_bufmgr_read_storage_scn_for_gcs(tag, &scn);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, failure == 1 || failure == 2);
		UT_ASSERT_EQ(result, failure == 0);
		if (!caught)
			UT_ASSERT_EQ(scn, failure == 0 ? 93 : InvalidScn);
		UT_ASSERT(!marker_during_read);
		UT_ASSERT(commit_operation(&io_request));
		calls = raw_opens + raw_reads;
		raw_open_error = raw_read_error = false;
		raw_verify_ok = true;
		UT_ASSERT(!cluster_bufmgr_read_storage_scn_for_gcs(tag, &scn));
		UT_ASSERT_EQ(scn, InvalidScn);
		UT_ASSERT_EQ(raw_opens + raw_reads, calls);
	}
}

UT_TEST(test_refresh_call_covers_storage_and_original_content_copy)
{
	for (unsigned failure = 0; failure < 6; failure++) {
		SCN scn = 777;
		volatile bool caught = false;
		volatile bool result = false;
		unsigned calls;
		(void)raw_consumer_fixture(10 + failure);
		raw_open_error = failure == 1;
		raw_read_error = failure == 2;
		raw_verify_ok = failure != 3;
		refresh_state = failure == 4 ? BM_DIRTY : 0;
		refresh_write_allowed = failure != 5;
		expecting_error = failure == 3;
		PG_TRY();
		{
			result = cluster_bufmgr_refresh_block_from_storage_for_gcs(&refresh_buffer, &scn);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT_EQ(caught, failure >= 1 && failure <= 3);
		UT_ASSERT_EQ(result, failure == 0);
		UT_ASSERT(!marker_during_read && !marker_during_copy && !refresh_locked);
		if (!caught)
			UT_ASSERT_EQ(scn, failure == 0 ? 93 : InvalidScn);
		UT_ASSERT_EQ((unsigned char)refresh_page.data[BLCKSZ - 1], failure == 0 ? 0 : 0x5a);
		UT_ASSERT(commit_operation(&io_request));
		calls = raw_opens + raw_reads;
		raw_open_error = raw_read_error = false;
		raw_verify_ok = true;
		UT_ASSERT(!cluster_bufmgr_refresh_block_from_storage_for_gcs(&refresh_buffer, &scn));
		UT_ASSERT_EQ(raw_opens + raw_reads, calls);
		UT_ASSERT_EQ(scn, InvalidScn);
	}
}

UT_TEST(test_extension_metadata_probe_cannot_outlive_committed_drop)
{
	for (unsigned failure = 0; failure < 3; failure++) {
		BufferTag tag = raw_consumer_fixture(20 + failure);
		volatile bool caught = false;
		volatile bool result = false;
		unsigned calls;
		raw_open_error = failure == 1;
		raw_read_error = failure == 2;
		PG_TRY();
		{
			result = cluster_bufmgr_block_is_extension_for_gcs(tag);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, failure != 0);
		UT_ASSERT_EQ(result, failure == 0);
		UT_ASSERT(!marker_during_read);
		UT_ASSERT(commit_operation(&io_request));
		calls = raw_opens + metadata_reads;
		raw_open_error = raw_read_error = false;
		UT_ASSERT(!cluster_bufmgr_block_is_extension_for_gcs(tag));
		UT_ASSERT_EQ(raw_opens + metadata_reads, calls);
	}
}

UT_TEST(test_entered_raw_read_delays_marker_and_error_releases_only_call)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		BufferTag tag;
		char page[BLCKSZ];
		XLogRecPtr lsn;
		uint64 scn;
		volatile bool caught = false;
		io_request = ready_operation(peer_fence_fixture(22000 + fault));
		InitBufferTag(&tag, &io_request.base.key.locator, MAIN_FORKNUM, 4);
		raw_read_error = fault != 0;
		raw_reads = 0;
		marker_during_read = false;
		PG_TRY();
		{
			UT_ASSERT(cluster_bufmgr_read_storage_image_for_resource_x(tag, page, &lsn, &scn));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault != 0);
		UT_ASSERT_EQ(raw_reads, 1);
		UT_ASSERT(!marker_during_read);
		UT_ASSERT(commit_operation(&io_request));
		raw_read_error = false;
		UT_ASSERT(!cluster_bufmgr_read_storage_image_for_resource_x(tag, page, &lsn, &scn));
		UT_ASSERT_EQ(raw_reads, 1);
	}
}

UT_TEST(test_scope_capacity_identity_and_stop_do_not_invent_completion)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(23000);
	ClusterKoDirectoryIoV3 calls[64], extra, stale;

	for (unsigned i = 0; i < 64; i++)
		UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &calls[i]));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(request.base.key.locator, &extra));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(NULL), CLUSTER_NORMAL_STOP_PENDING);
	stale = calls[0];
	stale.serial++;
	UT_ASSERT(!cluster_ko_directory_io_end_v3(&stale));
	stale = calls[0];
	MyProcPid++;
	UT_ASSERT(!cluster_ko_directory_io_end_v3(&stale));
	MyProcPid--;
	UT_ASSERT(cluster_ko_directory_io_end_v3(&calls[0]));
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &extra));
	UT_ASSERT(!cluster_ko_directory_io_end_v3(&calls[0]));
	for (unsigned i = 1; i < 64; i++)
		UT_ASSERT(cluster_ko_directory_io_end_v3(&calls[i]));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&extra));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_marker_blocks_new_calls_and_cannot_gc_entered_call)
{
	ClusterKoSharedMessageV3 request = ready_operation(peer_fence_fixture(24000));
	ClusterKoDirectoryIoV3 call, other;
	ClusterKoSharedMessageV3 wrong = request;
	ClusterKoPeerFenceV3 *entry = &storage.peer_fences[0];
	RelFileLocator different = request.base.key.locator;

	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &call));
	wrong.base.peer_boot++;
	UT_ASSERT(!commit_operation(&wrong));
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &other));
	UT_ASSERT(cluster_ko_directory_io_end_v3(&other));
	UT_ASSERT(!commit_operation(&request));
	UT_ASSERT_EQ(entry->state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(request.base.key.locator, &other));
	SpinLockAcquire(&storage.shared_lock);
	UT_ASSERT(!ko_peer_fence_clear_locked(entry));
	SpinLockRelease(&storage.shared_lock);
	different.relNumber++;
	UT_ASSERT(cluster_ko_directory_io_begin_v3(different, &other));
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	UT_ASSERT(commit_operation(&request));
	UT_ASSERT_EQ(entry->state, CLUSTER_KO_PEER_FENCE_COMMITTED);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&other));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_fresh_grant_cannot_erase_committed_drop_exclusion)
{
	ClusterKoSharedMessageV3 request = ready_operation(peer_fence_fixture(25000));
	ClusterLockAcquireRequest grant = fresh_grant_fixture(&request);
	ClusterKoDirectoryIoV3 call;
	ClusterKoPeerAdmissionV3 sample;

	UT_ASSERT(commit_operation(&request));
	UT_ASSERT(cluster_ko_peer_fence_fresh_grant_v3(&grant));
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_EMPTY);
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(request.base.key.locator, &call));
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(request.base.key.locator, &sample));
}

UT_TEST(test_rollback_releases_identity_reservation_without_retiring_locator)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(26000);
	ClusterKoDirectoryIoV3 call;
	ClusterLockAcquireRequest grant;

	/* The original exact fresh-grant consumer, not a manual slot erase. */
	for (unsigned i = 0; i < KO_DROP_IDENTITY_CAPACITY + 1; i++) {
		request.base.batch_id++;
		request = ready_operation(request);
		grant = fresh_grant_fixture(&request);
		UT_ASSERT(cluster_ko_peer_fence_fresh_grant_v3(&grant));
	}
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &call));
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
}

UT_TEST(test_terminal_capacity_refuses_before_physical_work_not_during_commit)
{
	ClusterKoSharedMessageV3 first = peer_fence_fixture(30000), request;
	volatile bool caught = false;
	unsigned flushes = flush_count, drops = drop_count, syncs = sync_count;

	for (unsigned i = 0; i < KO_DROP_IDENTITY_CAPACITY; i++) {
		request = first;
		request.base.batch_id += i;
		request.base.key.locator.relNumber += i;
		request = ready_operation(request);
		UT_ASSERT(commit_operation(&request));
		if ((i + 1) % 64 == 0)
			UT_ASSERT(cluster_ko_peer_fence_gc_tick_v3());
	}
	request.base.batch_id++;
	request.base.key.locator.relNumber++;
	local_scan_rel_number = request.base.key.locator.relNumber;
	multiple_barriers = true;
	space_identity.key.locator = request.base.key.locator;
	enqueue_shared_v3(&request);
	expecting_error = true;
	PG_TRY();
	{
		fixture_peer_drain();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(flush_count, flushes);
	UT_ASSERT_EQ(drop_count, drops);
	UT_ASSERT_EQ(sync_count, syncs);
	cluster_ko_peer_apply_error_v3(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT(storage.peer_fences[0].failure_sqlerrcode != 0);
	UT_ASSERT(!commit_operation(&request));
}

UT_TEST(test_directory_terminal_requires_entered_calls_to_drain_and_marker_retry)
{
	ClusterKoSharedMessageV3 request = ready_operation(peer_fence_fixture(52000));
	RelFileLocator locator = request.base.key.locator;
	ClusterKoDirectoryIoV3 io;

	UT_ASSERT(!cluster_ko_directory_drop_committed_v3(locator));
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(cluster_ko_directory_io_begin_v3(locator, &io));
	UT_ASSERT(!commit_operation(&request));
	UT_ASSERT(cluster_ko_directory_drop_committed_v3(locator));
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(cluster_ko_directory_io_end_v3(&io));
	/* End closes this call only; the original marker consumer certifies it. */
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(commit_operation(&request));
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(locator, &io));
}

UT_TEST(test_directory_terminal_refuses_unknown_identity_and_runtime_scope)
{
	ClusterKoSharedMessageV3 request = ready_operation(peer_fence_fixture(52001));
	RelFileLocator locator = request.base.key.locator, other = locator;
	KoDropIdentity *identity = &storage.drop_identities[0];
	KoDropIdentity saved;

	UT_ASSERT(commit_operation(&request));
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(locator));
	other.relNumber++;
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(other));
	saved = *identity;
	for (unsigned fault = 0; fault < 4; fault++) {
		*identity = saved;
		if (fault == 0) identity->state = KO_DROP_IDENTITY_RESERVED;
		if (fault == 1) identity->state = KO_DROP_IDENTITY_RETIRED + 1;
		if (fault == 2) identity->fence_serial = 0;
		if (fault == 3) identity->request.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
		UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	}
	*identity = saved;
	cluster_interconnect_tier = CLUSTER_IC_TIER_2;
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	cluster_shared_config = false;
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	cluster_shared_config = true;
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(locator));
}


/* Exact identity consumer, with no SPACE read, remote AX probe or DATA IO. */
static ClusterKoSharedMessageV3
pi_identity_peer(ClusterKoDirectoryIoV3 *call, bool ready)
{
	ClusterKoSharedMessageV3 request;
	ClusterKoPeerFenceHandleV3 handle;
	ClusterKoPeerFenceStateV3 state;

	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	CritSectionCount = 0;
	request = peer_fence_fixture(53000);
	local_scan_rel_number = request.base.key.locator.relNumber;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&request, &handle, &state));
	if (ready) {
		UT_ASSERT(fixture_completed_fence_ready(&request, &handle));
		fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
	}
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, call));
	return request;
}

UT_TEST(test_pi_identity_accepts_exact_active_and_ready_peer_only_with_call)
{
	for (int ready = 0; ready < 2; ready++) {
		ClusterKoDirectoryIoV3 call;
		ClusterKoSharedMessageV3 request = pi_identity_peer(&call, ready);
		ClusterKoPiStorageIdentityV3 sample = {0};
		unsigned probes = holder_probe_sends;
		UT_ASSERT(cluster_ko_pi_storage_identity_v3(&request.base.key,
			request.base.incarnation, &call, &sample));
		UT_ASSERT(!sample.origin && sample.slot == 0);
		UT_ASSERT_EQ(sample.serial, storage.peer_fences[0].serial);
		UT_ASSERT_EQ(sample.route_generation, v3_route_generation);
		UT_ASSERT_EQ(memcmp(&sample.request, &request, sizeof(request)), 0);
		UT_ASSERT(cluster_ko_pi_storage_identity_current_v3(&sample));
		UT_ASSERT_EQ(space_reads + maintenance_space_locks + ordinary_space_locks, 0);
		UT_ASSERT_EQ(holder_probe_sends, probes);
		UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
		UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
	}
}

UT_TEST(test_pi_identity_origin_retains_original_slot_after_private_handoff)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(53001);
	ClusterKoCompletionV2 completion = {0};
	ClusterKoDirectoryIoV3 call;
	ClusterKoPiStorageIdentityV3 sample = {0};
	uint64 boots[16], generation;
	int slot;
	request.base.origin_node = 0;
	request.base.peer_node = 1;
	request.base.origin_boot = writer.claim.identity.origin_owner_incarnation;
	request.base.peer_boot = formation.membership.last_admitted_incarnation[1];
	request.ax_holder.node_id = 0;
	UT_ASSERT(ko_shared_members(&request.base, boots, &generation));
	MyBackendType = B_BACKEND;
	slot = ko_shared_reserve_v3(&request, boots, &completion);
	UT_ASSERT(slot >= 0);
	/* The original private handoff deliberately retains this shared slot. */
	storage.contexts[slot].complete = true;
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &call));
	UT_ASSERT(cluster_ko_pi_storage_identity_v3(&request.base.key,
		request.base.incarnation, &call, &sample));
	UT_ASSERT(sample.origin && sample.slot == slot);
	UT_ASSERT_EQ(sample.serial, completion.serial);
	UT_ASSERT(cluster_ko_pi_storage_identity_current_v3(&sample));
	for (unsigned bad = 0; bad < 5; bad++) {
		ClusterKoSharedContext saved = storage.contexts[slot];
		if (bad == 0) storage.contexts[slot].origin_detached = true;
		if (bad == 1) storage.contexts[slot].ax_release_requested = true;
		if (bad == 2) storage.contexts[slot].ax_failed = true;
		if (bad == 3) storage.contexts[slot].drop_identity_serial++;
		if (bad == 4) storage.contexts[slot].request_failed_peers = 2;
		UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
		storage.contexts[slot] = saved;
	}
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
}

UT_TEST(test_pi_identity_refuses_unknown_namespace_and_ambiguous_operation)
{
	for (unsigned bad = 0; bad < 8; bad++) {
		ClusterKoDirectoryIoV3 call;
		ClusterKoSharedMessageV3 request = pi_identity_peer(&call, false);
		ClusterKoPiStorageIdentityV3 sample, sentinel;
		memset(&sample, 0xa5, sizeof(sample));
		sentinel = sample;
		if (bad == 0) request.base.key.system_identifier++;
		if (bad == 1) request.base.key.database_incarnation++;
		if (bad == 2) request.base.key.storage_uuid[0]++;
		if (bad == 3) request.base.key.locator.relNumber++;
		if (bad == 4) request.base.incarnation[0]++;
		if (bad == 5) storage.peer_fences[0].state = CLUSTER_KO_PEER_FENCE_EMPTY;
		if (bad == 6) storage.peer_fences[1] = storage.peer_fences[0];
		if (bad == 7) storage.peer_fences[0].request.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
		UT_ASSERT(!cluster_ko_pi_storage_identity_v3(&request.base.key,
			request.base.incarnation, &call, &sample));
		UT_ASSERT_EQ(memcmp(&sample, &sentinel, sizeof(sample)), 0);
		UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	}
}

UT_TEST(test_pi_identity_rechecks_original_role_route_lifecycle_and_call)
{
	for (unsigned bad = 0; bad < 21; bad++) {
		ClusterKoDirectoryIoV3 call;
		ClusterKoSharedMessageV3 request = pi_identity_peer(&call, false);
		ClusterKoPiStorageIdentityV3 sample = {0};
		UT_ASSERT(cluster_ko_pi_storage_identity_v3(&request.base.key,
			request.base.incarnation, &call, &sample));
		switch (bad) {
		case 0: storage.peer_fences[0].serial++; break;
		case 1: storage.peer_fences[0].peer_route_generation++; break;
		case 2: storage.peer_fences[0].request.ax_holder.procno++; break;
		case 3: storage.peer_fences[0].request.space_mutation_token++; break;
		case 4: storage.peer_fences[0].request.base.batch_id++; break;
		case 5: storage.peer_fences[0].state = CLUSTER_KO_PEER_FENCE_COMMITTED; break;
		case 6: storage.peer_fences[0].failure_sqlerrcode = ERRCODE_INTERNAL_ERROR; break;
		case 7: v3_route_generation++; break;
		case 8: current_epoch++; break;
		case 9: writer.claim.identity.origin_owner_incarnation++; break;
		case 10: marker_capable = false; break;
		case 11: cluster_interconnect_tier = CLUSTER_IC_TIER_2; break;
		case 12: fixture_recovering = true; break;
		case 13: MyBackendType = B_BACKEND; break;
		case 14: CurrentResourceOwner = NULL; break;
		case 15: CritSectionCount = 1; break;
		case 16: sample.call.serial++; break;
		case 17: sample.call.pid++; break;
		case 18: storage.directory_io[call.slot].locator.relNumber++; break;
		case 19: pg_atomic_fetch_add_u64(&storage.peer_admission_revision, 1); break;
		case 20: sample.request.full_fork_mask ^= 1; break;
		}
		UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
		CritSectionCount = 0;
		UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	}
}

UT_TEST(test_pi_identity_marker_closes_receipt_window_without_ending_call)
{
	ClusterKoDirectoryIoV3 call;
	ClusterKoSharedMessageV3 request = pi_identity_peer(&call, true);
	ClusterKoPiStorageIdentityV3 sample = {0};
	UT_ASSERT(cluster_ko_pi_storage_identity_v3(&request.base.key,
		request.base.incarnation, &call, &sample));
	MyBackendType = B_SINVAL_BCAST;
	UT_ASSERT(!commit_operation(&request));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
	UT_ASSERT(storage.directory_io[call.slot].serial == call.serial);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	MyBackendType = B_SINVAL_BCAST;
	UT_ASSERT(commit_operation(&request));
}

UT_TEST(test_pi_identity_follows_exact_native_commit_before_publication)
{
	ClusterKoCompletionV2 *owner = v3_native_structure_owner(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoCompletionV2 *claimed = NULL;
	ClusterKoSharedMessageV3 request = owner->barrier_request_v3;
	ClusterKoNativeContinuation *work;
	ClusterKoDirectoryIoV3 call;
	ClusterKoPiStorageIdentityV3 sample = {0};
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE, &claimed));
	UT_ASSERT(claimed == owner);
	v3_structure_record(true, space_identity.operation, &terminal, wal);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &terminal, wal, sizeof(wal)));
	native_commit_active = true;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(owner->native_handed_off && storage.contexts[owner->slot].drop_identity == 0);
	work = dsa_get_address(ko_native_area, owner->native_unready);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_directory_io_begin_v3(request.base.key.locator, &call));
	UT_ASSERT(cluster_ko_pi_storage_identity_v3(&request.base.key, request.base.incarnation, &call, &sample));
	UT_ASSERT(cluster_ko_pi_storage_identity_current_v3(&sample));
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterKoNativeContinuation saved = *work;
		if (fault == 0) work->recovery_required = true;
		if (fault == 1) work->buffers_released = true;
		if (fault == 2) work->context.serial++;
		if (fault == 3) work->context.pid++;
		if (fault == 4) work->context.request_v3.base.batch_id++;
		if (fault == 5) work->context.drop_identity_serial++;
		if (fault == 6) work->context.request_done_peers = 0;
		if (fault == 7) work->context.peer_boots[1]++;
		UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
		*work = saved;
	}
	MyBackendType = B_BACKEND;
	cluster_ko_shared_native_publication_v3();
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!cluster_ko_pi_storage_identity_current_v3(&sample));
	UT_ASSERT_EQ(storage.directory_io[call.slot].serial, call.serial);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	MyBackendType = B_BACKEND;
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(request.base.key.locator));
}

int
main(void)
{
	UT_PLAN(19);
	printf(
		"# KO shared=%zu peer_fence=%zu drop_identity=%zu raw_io=%zu; identities=%u raw_calls=%u\n",
		sizeof(ClusterKoShared), sizeof(ClusterKoPeerFenceV3), sizeof(KoDropIdentity),
		sizeof(KoDirectoryIo), KO_DROP_IDENTITY_CAPACITY, CLUSTER_KO_SHARED_CAPACITY);
	UT_RUN(test_drop_identity_survives_two_full_active_slot_reclamations);
	UT_RUN(test_truncate_keeps_original_reclamation_semantics);
	UT_RUN(test_entered_raw_read_delays_marker_and_error_releases_only_call);
	UT_RUN(test_scope_capacity_identity_and_stop_do_not_invent_completion);
	UT_RUN(test_marker_blocks_new_calls_and_cannot_gc_entered_call);
	UT_RUN(test_fresh_grant_cannot_erase_committed_drop_exclusion);
	UT_RUN(test_rollback_releases_identity_reservation_without_retiring_locator);
	UT_RUN(test_terminal_capacity_refuses_before_physical_work_not_during_commit);
	UT_RUN(test_scn_probe_marker_waits_for_normal_error_and_refusal_exit);
	UT_RUN(test_refresh_call_covers_storage_and_original_content_copy);
	UT_RUN(test_extension_metadata_probe_cannot_outlive_committed_drop);
	UT_RUN(test_directory_terminal_requires_entered_calls_to_drain_and_marker_retry);
	UT_RUN(test_directory_terminal_refuses_unknown_identity_and_runtime_scope);
	UT_RUN(test_pi_identity_accepts_exact_active_and_ready_peer_only_with_call);
	UT_RUN(test_pi_identity_origin_retains_original_slot_after_private_handoff);
	UT_RUN(test_pi_identity_refuses_unknown_namespace_and_ambiguous_operation);
	UT_RUN(test_pi_identity_rechecks_original_role_route_lifecycle_and_call);
	UT_RUN(test_pi_identity_marker_closes_receipt_window_without_ending_call);
	UT_RUN(test_pi_identity_follows_exact_native_commit_before_publication);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
