/* An unissued resident PI read must retire only its own native reservation.
 * Execute the original ordinary SHARE and cleanup; OS/process events are
 * controlled leaves. No peer decision or current image is manufactured.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_resident_lock_fixture.inc"

static void up_start(void)
{
	rl_start(2, 2);
	pg_atomic_fetch_and_u32(&request_buf.state, ~BM_VALID);
}
static void up_read(bool wal)
{
	ClusterPcmOwnSnapshot before;
	ClusterPcmLocalPiSnapshotV1 pi;
	PGIOAlignedBlock bytes;
	uint32 state;
	up_start();
	if (wal) {
		rl_pi = local_pi_setup(make_tag(850));
		rl_preserve_pi();
	}
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&request_buf, &before), CLUSTER_PCM_OWN_OK);
	state = pg_atomic_read_u32(&request_buf.state);
	memcpy(bytes.data, disk_shared.data, BLCKSZ);
	UT_ASSERT(!rl_run(NULL));
	UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(complete_broadcasts, 0); UT_ASSERT_EQ(rl_share_locks, 0);
	UT_ASSERT_EQ(ordinary_leaves, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), before.generation);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), before.reservation_token + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT(memcmp(bytes.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	if (wal) {
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_base.tag, &pi));
		UT_ASSERT(memcmp(&pi, &rl_pi_before, sizeof(pi)) == 0);
	}
	complete_idle();
}
UT_TEST(test_nonvalid_PI_unissued_read_retires_only_its_reservation) { up_read(false); }
UT_TEST(test_nonvalid_PI_unissued_read_keeps_original_WAL_and_bytes) { up_read(true); }

static void up_guard(unsigned field)
{
	ClusterBufmgrFirstFill read = {0};
	ClusterPcmOwnEntry before;
	ResourceOwner original;
	BufferTag tag;
	uint8 type;
	uint32 state;
	bool covered = false;
	volatile bool panicked = false;
	up_start();
	read.buf = &request_buf; read.owner = CurrentResourceOwner; read.resident = true;
	read.backend_id = MyBackendId; read.backend_generation = MyProc->cluster_grd_generation;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_begin_grant_reservation_wait(&request_buf,
		PCM_LOCK_MODE_S, &read.base, &read.token, &covered, NULL), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(!covered); read.pending = true;
	/* A different native/process owner or producer must never be adopted by
	 * the local cleanup just because no wire request was seen. */
	switch (field) {
		case 0: pg_atomic_fetch_add_u64(&request_own.generation, 1); break;
		case 1: pg_atomic_fetch_add_u64(&request_own.reservation_token, 1); break;
		case 2: request_buf.tag.blockNum++; break;
		case 3: request_buf.buffer_type = BUF_TYPE_CURRENT; break;
		case 4: pg_atomic_fetch_or_u32(&request_own.flags, PCM_OWN_FLAG_REVOKING); break;
		case 5: pg_atomic_write_u64(&request_own.writer_activation_token, 8); break;
		case 6: pg_atomic_write_u64(&request_own.resource_x_activation_generation, 8); break;
		case 7: pg_atomic_write_u64(&request_own.delivery_attempt, 8); break;
		case 8: pg_atomic_write_u64(&request_own.write_io_generation, 1); break;
		case 9: pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS); break;
		case 10: pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR); break;
		case 11: pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY); break;
		case 12: CurrentResourceOwner = NULL; break;
		case 13: MyProc->cluster_grd_generation++; break;
		case 14: MyProc->pid++; break;
		case 15: request_pins = 0; break;
		case 16: read.base.reservation_token = read.token; break;
		case 17: read.base.reservation_token = UINT64_MAX; break;
		case 18: read.base.flags = PCM_OWN_FLAG_GRANT_PENDING; break;
		case 19:
			read.base.generation = UINT64_MAX - 1;
			pg_atomic_write_u64(&request_own.generation, read.base.generation);
			break;
	}
	before = request_own; tag = request_buf.tag; type = request_buf.buffer_type;
	state = pg_atomic_read_u32(&request_buf.state); original = read.owner;
	PG_TRY(); { cluster_bufmgr_first_fill_cleanup(&request_buf, &read); }
	PG_CATCH(); { panicked = true; } PG_END_TRY();
	UT_ASSERT(panicked); UT_ASSERT_EQ(ordinary_panics, 1);
	UT_ASSERT(read.pending); UT_ASSERT(!read.unissued_quiet);
	UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
	UT_ASSERT(BufferTagsEqual(&tag, &request_buf.tag)); UT_ASSERT_EQ(request_buf.buffer_type, type);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
	CurrentResourceOwner = original; request_pins = 1;
	printf("# unissued PI guard=%u retained=1 REQUEST=0 IO=0\n", field);
	complete_idle();
}
UT_TEST(test_nonvalid_PI_cleanup_rejects_changed_native_and_process_owners)
{
	for (unsigned field = 0; field < 20; field++) up_guard(field);
}
UT_TEST(test_nonvalid_PI_foreign_write_wait_cancel_preserves_original_IO)
{
	ClusterPcmOwnEntry before; uint32 state;
	up_start();
	pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&request_own.write_io_generation, 1);
	before = request_own; state = pg_atomic_read_u32(&request_buf.state);
	rl_cancel_wait = true;
	UT_ASSERT(!rl_run(NULL)); UT_ASSERT_EQ(rl_waits, 1);
	UT_ASSERT_EQ(ordinary_panics, 0); UT_ASSERT_EQ(ordinary_sends, 0);
	request_native_unchanged(&before, state);
	UT_ASSERT_EQ(complete_io_calls, 0); complete_idle();
}
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(4);
	UT_RUN(test_nonvalid_PI_unissued_read_retires_only_its_reservation);
	UT_RUN(test_nonvalid_PI_unissued_read_keeps_original_WAL_and_bytes);
	UT_RUN(test_nonvalid_PI_cleanup_rejects_changed_native_and_process_owners);
	UT_RUN(test_nonvalid_PI_foreign_write_wait_cancel_preserves_original_IO);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
