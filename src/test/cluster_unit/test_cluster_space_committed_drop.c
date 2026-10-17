/* Actual committed SPACE producer; explicit KO and provider boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
int space_storage_fixture_main(void);
#define main space_storage_fixture_main
#include "test_cluster_space_storage.c"
#undef main
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/storage/cluster_shared_fs.h"

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
static unsigned opens, closes, reads, writes, syncs, fence_checks, adg_checks;
static unsigned fault, fail_at;
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
}

static ClusterSpaceDropState *
setup_committed(Relation *relation, bool missing)
{
	ClusterSpaceDropState *state;
	*relation = native_truncate_relation();
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
		memcpy(disk[i].data, pages[i].data, BLCKSZ);
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

int main(void)
{
	UT_PLAN(5);
	UT_RUN(test_final_space_uses_original_commit_without_buffer_current);
	UT_RUN(test_final_space_refuses_missing_or_changed_commit_before_io);
	UT_RUN(test_final_space_error_short_corrupt_or_drift_keeps_outputs_unpublished);
	UT_RUN(test_final_space_rejects_wrong_typed_identity_before_writes);
	UT_RUN(test_final_space_rejects_valid_foreign_or_unrelated_typed_predecessor);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
