/* Actual committed SPACE producer; explicit KO and provider boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
int space_storage_fixture_main(void);
#define main space_storage_fixture_main
#include "test_cluster_space_storage.c"
#undef main
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "test_cluster_space_close_policy.inc"
#include "test_cluster_space_drop_state.inc"

/* Only the parent lacks this symbol. The candidate's actual strong producer
 * replaces this runtime refusal; provider counters prove which body ran. */
__attribute__((weak, noinline)) bool
cluster_space_drop_complete_v4(const ClusterSpaceDropState *s,
 const ClusterKoCompletionV2 *c, const ClusterKoDirectoryIoV3 *i,
 ClusterPageWalBindingV1 *t, void *w)
{
 (void)s; (void)c; (void)i; (void)t; (void)w;
 return false;
}

static PGIOAlignedBlock disk[2];
static unsigned red_disk_input;
static unsigned opens, closes, reads, writes, syncs, fence_checks, adg_checks;
static unsigned fault, fail_at;
static unsigned close_failures;
static bool scope_current;
static ClusterKoDirectoryIoV3 scope = { .slot = 2, .pid = 123, .serial = 7 };
static ClusterSharedFsHandle *handle = (ClusterSharedFsHandle *)&disk;

bool
cluster_ko_native_space_current_v4(const ClusterKoCompletionV2 *owner,
	const ClusterKoDirectoryIoV3 *io, const ClusterSpaceIdentity *expected)
{
	UT_ASSERT(owner == (ClusterKoCompletionV2 *)&ko_handle);
	UT_ASSERT(memcmp(io, &scope, sizeof(scope)) == 0);
	UT_ASSERT_EQ(expected->state, CLUSTER_SPACE_IDENTITY_LIVE);
	UT_ASSERT(RelFileLocatorEquals(expected->key.locator, locator));
	UT_ASSERT_EQ(locked | pinned | CritSectionCount, 0);
	return scope_current;
}
void
cluster_write_fence_reject_if_fenced(const char *op)
{
	(void)op;
	fence_checks++;
	if (fault == 1) pg_re_throw();
}
void
cluster_mrp_standby_shared_write_gate(const char *op)
{
	(void)op;
	adg_checks++;
	if (fault == 2) pg_re_throw();
}
void
cluster_shared_fs_open_existing(RelFileLocator tag, ForkNumber fork,
	ClusterSharedFsHandle **out)
{
	UT_ASSERT(fence_checks && adg_checks && scope_current);
	UT_ASSERT(RelFileLocatorEquals(tag, locator));
	UT_ASSERT_EQ(fork, SPACE_FORKNUM);
	opens++;
	if (fault == 3) pg_re_throw();
	*out = handle;
}
BlockNumber
cluster_shared_fs_nblocks(ClusterSharedFsHandle *h)
{
	UT_ASSERT(h == handle);
	return fault == 4 ? 1 : 2;
}
int
cluster_shared_fs_read(ClusterSharedFsHandle *h, BlockNumber block, char *out)
{
	UT_ASSERT(h == handle && block < 2);
	reads++;
	if (fault == 5 && reads == fail_at) pg_re_throw();
	memcpy(out, disk[block].data, BLCKSZ);
	if (fault == 6 && reads == fail_at) out[BLCKSZ-1] ^= 1;
	if (fault == 7 && reads == fail_at) return BLCKSZ-1;
	if (fault == 8 && reads == fail_at) scope_current = false;
	if (fault == 9 && reads == fail_at) ref.timeline++;
	return BLCKSZ;
}
int
cluster_shared_fs_write(ClusterSharedFsHandle *h, BlockNumber block, const char *in)
{
	UT_ASSERT(h == handle && block < 2 && scope_current);
	UT_ASSERT(fence_checks >= 2 && adg_checks >= 2);
	writes++;
	if (fault == 10 && writes == fail_at) pg_re_throw();
	memcpy(disk[block].data, in, BLCKSZ);
	if (fault == 11 && writes == fail_at) return BLCKSZ-1;
	return BLCKSZ;
}
void
cluster_shared_fs_immedsync(ClusterSharedFsHandle *h)
{
	UT_ASSERT(h == handle && writes == 2 && scope_current);
	syncs++;
	if (fault == 12) pg_re_throw();
	if (fault == 13) scope_current = false;
}
void
cluster_shared_fs_close(ClusterSharedFsHandle *h)
{
	UT_ASSERT(h == handle);
	closes++;
	if (fault == 14) pg_re_throw();
	/* FileClose's non-temporary close failure uses this real native policy.
	 * The provider/syscall remains a fixture; LOG deliberately returns. */
	if (fault == 15) {
		close_failures++;
		elog(data_sync_elevel(LOG), "fixture could not close SPACE file");
	}
}

static ClusterSpaceDropState *
setup_committed(Relation *relation, bool missing)
{
	ClusterSpaceDropState *state;
	PGIOAlignedBlock older[2];

	*relation = native_truncate_relation();
	memcpy(older[0].data, pages[0].data, BLCKSZ);
	memcpy(older[1].data, pages[1].data, BLCKSZ);
	if (red_disk_input == 2) {
		ClusterSpaceReservation reservation;
		uint64 token;

		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ,
			SPACE_FORKNUM, 1, &ko_key, &reservation, &token));
		reservation.next_block++;
		token = rf_page_mutation_token_next();
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, token,
			pages[1].data, BLCKSZ));
	}
	/* A qualified writer/source fixture. These fields are not produced here. */
	ref.timeline = 1;
	ref.claim.identity.thread_claim_created_at = 1;
	ref.claim.identity.origin_owner_incarnation = 1;
	ref.claim.identity.root_lineage_seq = 1;
	ref.claim.identity.authority_uuid[0] = 1;
	ref.claim.max_config_generation = 1;
	ref.claim.claim_sha256[0] = 1;
	drop_owner = committed_drop_mode = true;
	commit_attribution_missing = missing;
	state = prepare_original_drop(&locator, 1);
	UT_ASSERT(state != NULL);
	if (state == NULL) abort();
	readback_checksums = true;
	for (unsigned i=0; i<2; i++) {
		if (red_disk_input == 1)
			memset(disk[i].data, 0, BLCKSZ);
		else if (red_disk_input == 2)
			memcpy(disk[i].data, older[i].data, BLCKSZ);
		else
			memcpy(disk[i].data, pages[i].data, BLCKSZ);
		if (red_disk_input != 1)
			((PageHeader)disk[i].data)->pd_checksum = pg_checksum_page(disk[i].data, i);
	}
	START_CRIT_SECTION();
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	emit_drop_commit(state, &locator, 1);
	XLogFlush(UINT64_C(0x10000300));
	cluster_space_drop_defer_v4(state, UINT64_C(0x10000300));
	MyProc->delayChkptFlags = 0;
	END_CRIT_SECTION();
	opens=closes=reads=writes=syncs=fence_checks=adg_checks=0;
	close_failures=0;
	fault=fail_at=0;
	scope_current=true;
	return state;
}

static bool
finish(ClusterSpaceDropState *state, ClusterPageWalBindingV1 *terminal, uint8 *wal)
{
	return cluster_space_drop_complete_v4(state, (ClusterKoCompletionV2 *)&ko_handle,
			&scope, terminal, wal);
}

UT_TEST(test_final_space_uses_original_commit_without_buffer_current)
{
	Relation rel;
	ClusterSpaceDropState *state = setup_committed(&rel, false);
	ClusterPageWalBindingV1 terminal = {0};
	ClusterSpaceStructureChange change = {0};
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
	unsigned original_reads=buffer_reads, original_wal=wal_calls;
	/* Last insertion may have moved after COMMIT; use the saved native fact. */
	commit_attribution_missing=true;
	UT_ASSERT(finish(state, &terminal, wal));
	UT_ASSERT_EQ(opens,1); UT_ASSERT_EQ(closes,1);
	UT_ASSERT_EQ(writes,2); UT_ASSERT_EQ(syncs,1); UT_ASSERT_EQ(reads,6);
	UT_ASSERT_EQ(buffer_reads,original_reads); UT_ASSERT_EQ(wal_calls,original_wal);
	UT_ASSERT_EQ(dirty_calls+capture_calls+space_flushes+unlink_calls,0);
	UT_ASSERT(cluster_page_wal_binding_shape_v1(&terminal));
	UT_ASSERT_EQ(terminal.flags,CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	UT_ASSERT_EQ(terminal.record_end,UINT64_C(0x10000300));
	UT_ASSERT_EQ(terminal.record_crc,7);
	UT_ASSERT(cluster_space_structure_wal_decode(wal,sizeof(wal),&change));
	UT_ASSERT_EQ(change.identity.action,CLUSTER_SPACE_WAL_TOMBSTONE);
	for(unsigned i=0;i<2;i++) UT_ASSERT_EQ(PageGetLSN(disk[i].data),terminal.record_end);
	cluster_space_drop_finish(state); FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_final_space_refuses_missing_or_changed_commit_before_io)
{
	for(unsigned n=0;n<3;n++) {
		Relation rel;
		ClusterSpaceDropState *state=setup_committed(&rel,n==0);
		ClusterPageWalBindingV1 out, saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], saved_wal[sizeof(wal)];
		memset(&out,0x55,sizeof(out)); saved=out;
		memset(wal,0x66,sizeof(wal)); memcpy(saved_wal,wal,sizeof(wal));
		if(n==1) ref.timeline++;
		if(n==2) scope_current=false;
		UT_ASSERT(!finish(state,&out,wal));
		UT_ASSERT_EQ(opens+writes+syncs,0);
		UT_ASSERT(memcmp(&out,&saved,sizeof(out))==0);
		UT_ASSERT(memcmp(wal,saved_wal,sizeof(wal))==0);
		cluster_space_drop_finish(state); FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_error_short_corrupt_or_drift_keeps_outputs_unpublished)
{
	for(unsigned f=1;f<=14;f++) for(unsigned at=1;at<=6;at++) {
		Relation rel;
		ClusterSpaceDropState *state=setup_committed(&rel,false);
		ClusterPageWalBindingV1 out,saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES],saved_wal[sizeof(wal)];
		volatile bool caught=false,ok=false;
		fault=f;fail_at=at;
		if((f==10||f==11)&&at>2) { cluster_space_drop_finish(state);FreeFakeRelcacheEntry(rel);continue; }
		memset(&out,0x55,sizeof(out));saved=out;
		memset(wal,0x66,sizeof(wal));memcpy(saved_wal,wal,sizeof(wal));
		expecting_error=true;
		PG_TRY(); {ok=finish(state,&out,wal);} PG_CATCH(); {caught=true;} PG_END_TRY();
		expecting_error=false;
		UT_ASSERT(!ok); UT_ASSERT(caught || memcmp(&out,&saved,sizeof(out))==0);
		UT_ASSERT(memcmp(&out,&saved,sizeof(out))==0);
		UT_ASSERT(memcmp(wal,saved_wal,sizeof(wal))==0);
		UT_ASSERT_EQ(pinned|locked,0);
		UT_ASSERT_EQ(unlink_calls+dirty_calls+capture_calls,0);
		if(opens && f!=3) UT_ASSERT_EQ(closes,1);
		cluster_space_drop_finish(state);FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_rejects_wrong_typed_identity_before_writes)
{
	for(unsigned n=0;n<2;n++) {
		Relation rel; ClusterSpaceDropState *state=setup_committed(&rel,false);
		ClusterPageWalBindingV1 out; uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
		disk[n].data[BLCKSZ-1]^=1;
		UT_ASSERT(!finish(state,&out,wal)); UT_ASSERT_EQ(writes+syncs,0);
		cluster_space_drop_finish(state);FreeFakeRelcacheEntry(rel);
	}
}
UT_TEST(test_final_space_rejects_valid_foreign_or_unrelated_typed_predecessor)
{
	for (unsigned n = 0; n < 4; n++) {
		Relation rel;
		ClusterSpaceDropState *state = setup_committed(&rel, false);
		ClusterPageWalBindingV1 out = {0};
		ClusterSpaceIdentity identity;
		ClusterSpaceReservation reservation;
		uint64 token;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
		unsigned block = n < 2 ? 0 : 1;

		if (block == 0) {
			UT_ASSERT(cluster_space_identity_page_decode(disk[0].data, BLCKSZ,
				SPACE_FORKNUM, 0, &ko_key, &identity, &token));
			if (n == 0) identity.incarnation[0] ^= 1;
			else identity.sequence++;
			UT_ASSERT(cluster_space_identity_page_encode(&identity, token, disk[0].data, BLCKSZ));
		} else {
			UT_ASSERT(cluster_space_reservation_page_decode(disk[1].data, BLCKSZ,
				SPACE_FORKNUM, 1, &ko_key, &reservation, &token));
			if (n == 2) reservation.next_block++;
			else token++;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, token, disk[1].data, BLCKSZ));
		}
		((PageHeader)disk[block].data)->pd_checksum = pg_checksum_page(disk[block].data, block);
		UT_ASSERT(!finish(state, &out, wal));
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT_EQ(closes, 1);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_refuses_retry_configuration_before_provider_io)
{
	for (unsigned failed_close = 0; failed_close < 2; failed_close++) {
		Relation rel;
		ClusterSpaceDropState *state = setup_committed(&rel, false);
		ClusterPageWalBindingV1 out, saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], saved_wal[sizeof(wal)];

		data_sync_retry = true;
		UT_ASSERT_EQ(data_sync_elevel(LOG), LOG);
		fault = failed_close ? 15 : 0;
		memset(&out, 0x55, sizeof(out)); saved = out;
		memset(wal, 0x66, sizeof(wal)); memcpy(saved_wal, wal, sizeof(wal));
		UT_ASSERT(!finish(state, &out, wal));
		UT_ASSERT_EQ(opens + reads + writes + syncs + closes, 0);
		UT_ASSERT_EQ(close_failures, 0);
		UT_ASSERT_EQ(reported_level, LOG);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(wal, saved_wal, sizeof(wal)) == 0);
		UT_ASSERT_EQ(pinned | locked, 0);
		UT_ASSERT_EQ(dirty_calls + capture_calls + unlink_calls, 0);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
	data_sync_retry = false;
}

UT_TEST(test_final_space_default_close_failure_preserves_native_panic_policy)
{
	Relation rel;
	ClusterSpaceDropState *state = setup_committed(&rel, false);
	ClusterPageWalBindingV1 out, saved;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], saved_wal[sizeof(wal)];
	volatile bool caught = false, ok = false;

	UT_ASSERT(!data_sync_retry);
	UT_ASSERT_EQ(data_sync_elevel(LOG), PANIC);
	fault = 15;
	memset(&out, 0x55, sizeof(out)); saved = out;
	memset(wal, 0x66, sizeof(wal)); memcpy(saved_wal, wal, sizeof(wal));
	expecting_error = true;
	PG_TRY(); { ok = finish(state, &out, wal); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	/* errfinish is a fixture longjmp, not an installed-server PANIC run. */
	UT_ASSERT(caught && !ok);
	UT_ASSERT_EQ(reported_level, PANIC);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(close_failures, 1);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(memcmp(wal, saved_wal, sizeof(wal)) == 0);
	cluster_space_drop_finish(state);
	FreeFakeRelcacheEntry(rel);
}

static void
check_unwritten_disk(unsigned input)
{
	Relation rel;
	ClusterSpaceDropState *state;
	ClusterPageWalBindingV1 terminal = {0};
	ClusterSpaceStructureChange change;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
	unsigned original_reads, original_wal;
	bool ok;

	red_disk_input = input;
	state = setup_committed(&rel, false);
	red_disk_input = 0;
	original_reads = buffer_reads;
	original_wal = wal_calls;
	ok = finish(state, &terminal, wal);
	UT_ASSERT(ok);
	UT_ASSERT_EQ(buffer_reads, original_reads);
	UT_ASSERT_EQ(wal_calls, original_wal);
	UT_ASSERT_EQ(dirty_calls + capture_calls + space_flushes + unlink_calls, 0);
	if (ok) {
		UT_ASSERT(cluster_page_wal_binding_shape_v1(&terminal));
		UT_ASSERT_EQ(terminal.record_end, UINT64_C(0x10000300));
		UT_ASSERT(cluster_space_structure_wal_decode(wal, sizeof(wal), &change));
		UT_ASSERT_EQ(change.identity.result.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
		UT_ASSERT_EQ(writes, 2);
		UT_ASSERT_EQ(syncs, 1);
		for (unsigned i = 0; i < 2; i++)
			UT_ASSERT_EQ(PageGetLSN(disk[i].data), terminal.record_end);
	}
	cluster_space_drop_finish(state);
	FreeFakeRelcacheEntry(rel);
}
UT_TEST(test_final_space_accepts_original_prepare_with_zero_disk)
{
	check_unwritten_disk(1);
}
UT_TEST(test_final_space_accepts_original_prepare_with_older_disk)
{
	check_unwritten_disk(2);
}

UT_TEST(test_final_space_rejects_changed_retained_images_or_record_binding)
{
	for (unsigned n = 0; n < 10; n++) {
		Relation rel;
		ClusterSpaceDropState *state = setup_committed(&rel, false);
		ClusterSpaceTruncateState *entry = state->entries[0];
		ClusterPageWalBindingV1 out, saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], saved_wal[sizeof(wal)];
		ClusterSpaceStructureChange change;

		UT_ASSERT(cluster_space_structure_wal_decode(entry->wal, sizeof(entry->wal), &change));
		switch (n) {
		case 0: entry->result[0].data[BLCKSZ - 1] ^= 1; break;
		case 1: entry->result[1].data[BLCKSZ - 1] ^= 1; break;
		case 2: memcpy(entry->result[0].data, pages[0].data, BLCKSZ); break;
		case 3: memcpy(entry->result[1].data, pages[1].data, BLCKSZ); break;
		case 4:
			change.identity.result.sequence++;
			UT_ASSERT(cluster_space_identity_page_encode(&change.identity.result,
				change.identity.result_token, entry->result[0].data, BLCKSZ));
			break;
		case 5:
			change.reservation.result.next_block++;
			UT_ASSERT(cluster_space_reservation_page_encode(&change.reservation.result,
				change.reservation.result_token, entry->result[1].data, BLCKSZ));
			break;
		case 6: entry->token++; break;
		case 7: state->wal[sizeof(uint32)] ^= 1; break;
		case 8: entry->wal[0] ^= 1; break;
		case 9: state->commit_source.timeline++; break;
		}
		memset(&out, 0x55, sizeof(out)); saved = out;
		memset(wal, 0x66, sizeof(wal)); memcpy(saved_wal, wal, sizeof(wal));
		UT_ASSERT(!finish(state, &out, wal));
		UT_ASSERT_EQ(opens + writes + syncs, 0);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(wal, saved_wal, sizeof(wal)) == 0);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_accepts_partly_unwritten_original_pair)
{
	for (unsigned block = 0; block < 2; block++) {
		Relation rel;
		ClusterSpaceDropState *state = setup_committed(&rel, false);
		ClusterPageWalBindingV1 terminal = {0};
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};

		memset(disk[block].data, 0, BLCKSZ);
		UT_ASSERT(finish(state, &terminal, wal));
		UT_ASSERT_EQ(writes, 2);
		UT_ASSERT_EQ(syncs, 1);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_rejects_nonzero_new_page_and_conflicting_stale_reservation)
{
	for (unsigned n = 0; n < 4; n++) {
		Relation rel;
		ClusterSpaceDropState *state;
		ClusterPageWalBindingV1 out;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterSpaceStructureChange change;
		ClusterSpaceReservation reservation;
		uint64 token;
		unsigned block = n < 2 ? n : 1;

		red_disk_input = n >= 2 ? 2 : 0;
		state = setup_committed(&rel, false);
		red_disk_input = 0;
		if (n < 2)
			((PageHeader)disk[block].data)->pd_upper = 0;
		else {
			UT_ASSERT(cluster_space_structure_wal_decode(state->entries[0]->wal,
				sizeof(state->entries[0]->wal), &change));
			reservation = change.reservation.before;
			/* A lower counter with the same token names different bytes;
			 * a foreign identity is never a stale image of this object. */
			if (reservation.next_block > 0)
				reservation.next_block--;
			if (n == 3)
				reservation.identity.incarnation[0] ^= 1;
			token = change.reservation.before_token;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, token,
				disk[1].data, BLCKSZ));
		}
		if (n >= 2)
			((PageHeader)disk[block].data)->pd_checksum = pg_checksum_page(disk[block].data, block);
		UT_ASSERT(!finish(state, &out, wal));
		UT_ASSERT_EQ(writes + syncs, 0);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_final_space_publishes_retained_bytes_after_cache_reuse)
{
	Relation rel;
	ClusterSpaceDropState *state = setup_committed(&rel, false);
	ClusterSpaceTruncateState *entry = state->entries[0];
	ClusterPageWalBindingV1 terminal = {0};
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
	PGIOAlignedBlock expected;
	unsigned original_reads = buffer_reads, original_wal = wal_calls;

	for (unsigned i = 0; i < 2; i++) {
		memset(pages[i].data, 0x5a, BLCKSZ);
		memset(disk[i].data, 0, BLCKSZ);
	}
	UT_ASSERT(finish(state, &terminal, wal));
	for (unsigned i = 0; i < 2; i++) {
		memcpy(expected.data, entry->result[i].data, BLCKSZ);
		PageSetLSN(expected.data, state->commit_lsn);
		((PageHeader)expected.data)->pd_block_scn = entry->token;
		((PageHeader)expected.data)->pd_checksum = pg_checksum_page(expected.data, i);
		UT_ASSERT(memcmp(expected.data, disk[i].data, BLCKSZ) == 0);
	}
	UT_ASSERT_EQ(buffer_reads, original_reads);
	UT_ASSERT_EQ(wal_calls, original_wal);
	UT_ASSERT_EQ(dirty_calls + capture_calls + space_flushes + unlink_calls, 0);
	cluster_space_drop_finish(state);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_final_space_accepts_same_commit_postimage_or_split_publication)
{
	for (unsigned n = 0; n < 3; n++) {
		Relation rel;
		ClusterSpaceDropState *state = setup_committed(&rel, false);
		ClusterSpaceTruncateState *entry = state->entries[0];
		ClusterPageWalBindingV1 terminal = {0};
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
		unsigned mask = n == 0 ? 3 : 1u << (n - 1);

		for (unsigned i = 0; i < 2; i++)
			if ((mask & (1u << i)) != 0) {
				memcpy(disk[i].data, entry->result[i].data, BLCKSZ);
				PageSetLSN(disk[i].data, state->commit_lsn);
				((PageHeader)disk[i].data)->pd_block_scn = entry->token;
				((PageHeader)disk[i].data)->pd_checksum = pg_checksum_page(disk[i].data, i);
			}
		UT_ASSERT(finish(state, &terminal, wal));
		UT_ASSERT_EQ(writes, 2);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(closes, 1);
		cluster_space_drop_finish(state);
		FreeFakeRelcacheEntry(rel);
	}
}
int main(void)
{
	UT_PLAN(14);
	UT_RUN(test_final_space_publishes_retained_bytes_after_cache_reuse);
	UT_RUN(test_final_space_accepts_same_commit_postimage_or_split_publication);
	UT_RUN(test_final_space_rejects_changed_retained_images_or_record_binding);
	UT_RUN(test_final_space_accepts_partly_unwritten_original_pair);
	UT_RUN(test_final_space_rejects_nonzero_new_page_and_conflicting_stale_reservation);
	UT_RUN(test_final_space_accepts_original_prepare_with_zero_disk);
	UT_RUN(test_final_space_accepts_original_prepare_with_older_disk);
	UT_RUN(test_final_space_uses_original_commit_without_buffer_current);
	UT_RUN(test_final_space_refuses_missing_or_changed_commit_before_io);
	UT_RUN(test_final_space_error_short_corrupt_or_drift_keeps_outputs_unpublished);
	UT_RUN(test_final_space_rejects_wrong_typed_identity_before_writes);
	UT_RUN(test_final_space_rejects_valid_foreign_or_unrelated_typed_predecessor);
	UT_RUN(test_final_space_refuses_retry_configuration_before_provider_io);
	UT_RUN(test_final_space_default_close_failure_preserves_native_panic_policy);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
