/* Exact native cache gate; mapping storage, native eviction and current
 * authority leaves are controlled. The production predicates are extracted
 * unchanged from bufmgr.c. No I/O or alternate first/release producer.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_buffer_ctrc.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true, IsUnderPostmaster = true;
int MyProcPid = 111, NBuffers = 2;
BackendType MyBackendType = B_STARTUP;
volatile uint32 CritSectionCount, InterruptHoldoffCount, QueryCancelHoldoffCount;
volatile sig_atomic_t ShutdownRequestPending;
static bool recovering, current, scope_current, page_current, mapping, locked, refuse_lock;
static bool retire_success, drift_after_release;
static unsigned current_calls, evictions, first_observations, mapping_calls;
static int mapping_id;
static ClusterPageWalFirstResultV1 first_result;
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static PGAlignedBlock blocks[2];
char *BufferBlocks = (char *)blocks;
static LWLockPadded map_locks[BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS];
LWLockPadded *MainLWLockArray = map_locks;
static ClusterPcmOwnEntry own_array[2];
ClusterPcmOwnEntry *ClusterPcmOwnArray = own_array;
static ClusterPcmOwnSnapshot own;
static ClusterSpaceIdentity space;
static RfPageVersionV1 before;
static PGAlignedBlock image;
static const struct ClusterRecoveryStartV1 *const start = (const void *)256;
static const struct ClusterColdCtrcWorkV1 *const work = (const void *)512;
static const RfPageCtrcAuthorityV1 *const authority = (const void *)768;
static struct ClusterWalInputsV1 *const scope = (void *)1024;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s %s:%d\n", condition, file, line); abort();
}
bool RecoveryInProgress(void) { return recovering; }
bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *s,
	const struct ClusterColdCtrcWorkV1 *w)
{
	UT_ASSERT(!locked); current_calls++;
	return current && s == start && w == work;
}
struct ClusterWalInputsV1 *
ClusterColdRecoveryCtrcScope(const struct ClusterRecoveryStartV1 *s,
	const struct ClusterColdCtrcWorkV1 *w)
{
	UT_ASSERT(!locked); return s == start && w == work && scope_current ? scope : NULL;
}
bool
rf_page_authority_ctrc_replays_scope_nowait_v1(const RfPageCtrcAuthorityV1 *a,
	const struct ClusterWalInputsV1 *s)
{
	UT_ASSERT(!locked); return a == authority && s == scope && scope_current;
}
bool
rf_page_authority_ctrc_replays_covers_nowait_v1(const RfPageCtrcAuthorityV1 *a,
	const RfPageIdentityV1 *id, const RfPageVersionV1 *v)
{
	UT_ASSERT(!locked);
	return a == authority && page_current && id->system_identifier == space.key.system_identifier
		&& memcmp(id->storage_uuid, space.key.storage_uuid, 16) == 0
		&& RelFileLocatorEquals(id->locator, space.key.locator)
		&& id->forknum == MAIN_FORKNUM && id->blockno == 7
		&& memcmp(v, &before, sizeof(before)) == 0;
}
uint32 BufTableHashCode(BufferTag *tag) { return tag->blockNum; }
int
BufTableLookup(BufferTag *tag, uint32 hash)
{
	UT_ASSERT(locked && hash == 7 && tag->blockNum == 7);
	return mapping ? mapping_id : -1;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufMappingPartitionLock(7) && mode == LW_EXCLUSIVE && !locked);
	mapping_calls++;
	if (refuse_lock) return false;
	locked = true; HOLD_INTERRUPTS(); return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(locked && lock == BufMappingPartitionLock(7)); locked = false;
	RESUME_INTERRUPTS();
	if (drift_after_release) current = false;
}
uint32
LockBufHdr(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT(locked && !(state & BM_LOCKED));
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}
static void
cluster_pcm_own_snapshot_locked(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(locked && (pg_atomic_read_u32(&buf->state) & BM_LOCKED)); *out = own;
}
ClusterPageWalFirstResultV1
cluster_page_wal_first_observe_locked_v1(BufferDesc *buf, ClusterPageWalRefV1 *out)
{
	UT_ASSERT(locked && (pg_atomic_read_u32(&buf->state) & BM_LOCKED));
	first_observations++; memset(out, 0, sizeof(*out)); return first_result;
}
static bool
InvalidateBufferCommitLocked(BufferDesc *buf, BufferTag *tag, uint32 hash,
	LWLock *partition, uint32 state)
{
	UT_ASSERT(locked && mapping && BufferTagsEqual(tag, &buf->tag) && hash == 7);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(state), 0);
	UT_ASSERT(!(state & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_IN_PROGRESS | BM_IO_ERROR)));
	UT_ASSERT_EQ(first_result, CLUSTER_PAGE_WAL_FIRST_ABSENT);
	UT_ASSERT(buf->buffer_type == BUF_TYPE_CURRENT && own.pcm_state == PCM_STATE_N);
	evictions++;
	if (retire_success) { mapping = false; ClearBufferTag(&buf->tag); state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK); }
	UnlockBufHdr(buf, state); LWLockRelease(partition); return retire_success;
}

#define BufHdrGetBlock(buf) ((Block)(BufferBlocks + (Size)(buf)->buf_id * BLCKSZ))
#include "test_cluster_ctrc_page_cache.inc"
#undef BufHdrGetBlock

static void
fixture(void)
{
	BufferDesc *buf = &descriptors[0].bufferdesc;
	memset(descriptors, 0, sizeof(descriptors)); memset(&space, 0, sizeof(space));
	memset(&before, 0, sizeof(before)); memset(&own, 0, sizeof(own));
	cluster_enabled = cluster_shared_config = IsUnderPostmaster = true;
	MyBackendType = B_STARTUP; MyProcPid = 111;
	CritSectionCount = InterruptHoldoffCount = QueryCancelHoldoffCount = 0;
	ShutdownRequestPending = 0; recovering = false;
	current = scope_current = page_current = mapping = retire_success = true;
	locked = refuse_lock = drift_after_release = false; mapping_id = 0;
	current_calls = evictions = first_observations = mapping_calls = 0;
	first_result = CLUSTER_PAGE_WAL_FIRST_ABSENT;
	pg_atomic_init_u64(&own_array[0].delivery_attempt, 0);
	space.state = CLUSTER_SPACE_IDENTITY_LIVE; space.key.system_identifier = 123;
	space.key.storage_uuid[0] = 1; space.key.locator = (RelFileLocator){1663, 5, 16392};
	space.incarnation[0] = before.segment_incarnation[0] = 2; before.mutation_token = 42;
	InitBufferTag(&buf->tag, &space.key.locator, MAIN_FORKNUM, 7);
	buf->buf_id = 0; buf->buffer_type = BUF_TYPE_CURRENT; buf->pcm_state = PCM_STATE_N;
	buf->pi_buf_id = buf->cr_chain_head = buf->cr_chain_next = INVALID_BUFFER_ID;
	pg_atomic_init_u32(&buf->state, BM_TAG_VALID | BM_VALID | BM_PERMANENT);
	memset(image.data, 0, BLCKSZ);
	PageSetPageSizeAndVersion(image.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	((PageHeader)image.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)image.data)->pd_upper = ((PageHeader)image.data)->pd_special = BLCKSZ;
	((PageHeader)image.data)->pd_block_scn = 42;
	PageSetLSNPreserveOrigin(image.data, 0x12340); UT_ASSERT(PageSetLSNOrigin(image.data, 1));
	memcpy(blocks[0].data, image.data, BLCKSZ);
	own.tag = buf->tag; own.generation = 1; own.pcm_state = PCM_STATE_N; own.buffer_type = BUF_TYPE_CURRENT;
}
static ClusterCtrcPageCacheResultV1
probe(const char *expected)
{
	ClusterCtrcPageCacheResultV1 result = cluster_bufmgr_ctrc_page_cache_v1(
		start, work, authority, &space, 7, &before, expected);
	UT_ASSERT(!locked); UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT(!(pg_atomic_read_u32(&descriptors[0].bufferdesc.state) & BM_LOCKED));
	return result;
}
UT_TEST(test_ctrc_cache_absence_observation_never_evicts)
{
	fixture(); mapping = false;
	UT_ASSERT_EQ(probe(NULL), CLUSTER_CTRC_PAGE_CACHE_ABSENT);
	UT_ASSERT_EQ(evictions + first_observations, 0); UT_ASSERT(current_calls >= 2);
	fixture(); UT_ASSERT_EQ(probe(NULL), CLUSTER_CTRC_PAGE_CACHE_WAIT);
	UT_ASSERT(mapping); UT_ASSERT_EQ(evictions + first_observations, 0);
}
UT_TEST(test_ctrc_cache_exact_clean_native_predecessor_uses_original_eviction)
{
	fixture(); ((PageHeader)blocks[0].data)->pd_checksum = 1234;
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_ABSENT);
	UT_ASSERT(!mapping); UT_ASSERT_EQ(evictions, 1); UT_ASSERT_EQ(first_observations, 1);
	UT_ASSERT(current_calls >= 2);
}
UT_TEST(test_ctrc_cache_busy_pin_dirty_io_or_first_never_retires)
{
	const uint32 busy[] = {BUF_REFCOUNT_ONE, BM_DIRTY, BM_JUST_DIRTIED,
		BM_CHECKPOINT_NEEDED, BM_IO_IN_PROGRESS, BM_PIN_COUNT_WAITER};
	for (unsigned i = 0; i < lengthof(busy); i++) {
		fixture(); pg_atomic_write_u32(&descriptors[0].bufferdesc.state,
			pg_atomic_read_u32(&descriptors[0].bufferdesc.state) | busy[i]);
		UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
		UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 0);
	}
	for (unsigned i = 0; i < 3; i++) {
		fixture(); first_result = i == 0 ? CLUSTER_PAGE_WAL_FIRST_PRESENT
			: i == 1 ? CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED : CLUSTER_PAGE_WAL_FIRST_INVALID;
		UT_ASSERT_EQ(probe(image.data), i == 2 ? CLUSTER_CTRC_PAGE_CACHE_REFUSED : CLUSTER_CTRC_PAGE_CACHE_WAIT);
		UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 0);
	}
	fixture(); refuse_lock = true; UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
	UT_ASSERT_EQ(evictions + first_observations, 0);
}
UT_TEST(test_ctrc_cache_active_pcm_pi_cr_cf_and_delivery_keep_native_owner)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		BufferDesc *buf;
		fixture(); buf = &descriptors[0].bufferdesc;
		switch (fault) {
		case 0: own.pcm_state = PCM_STATE_X; break;
		case 1: own.flags = PCM_OWN_FLAG_GRANT_PENDING; own.reservation_token = 1; break;
		case 2: own.writer_activation_token = 1; break;
		case 3: own.resource_x_activation_generation = 1; break;
		case 4: buf->buffer_type = BUF_TYPE_PI; break;
		case 5: buf->pi_flags = 1; break;
		case 6: buf->pi_buf_id = 1; break;
		case 7: buf->cr_chain_head = 1; break;
		case 8: buf->cr_chain_next = 1; break;
		case 9: buf->cf_state = CF_STATE_CONVERTING; break;
		case 10: pg_atomic_write_u64(&own_array[0].delivery_attempt, 1); break;
		}
		UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
		UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 0);
	}
}
UT_TEST(test_ctrc_cache_wrong_bytes_version_source_or_mapping_refuse)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		fixture();
		switch (fault) {
		case 0: blocks[0].data[BLCKSZ - 1]++; break;
		case 1: PageSetLSNPreserveOrigin(blocks[0].data, 0x12348); break;
		case 2: ((PageHeader)blocks[0].data)->pd_block_scn++; break;
		case 3: UT_ASSERT(PageSetLSNOrigin(blocks[0].data, 0)); break;
		case 4: descriptors[0].bufferdesc.tag.blockNum++; break;
		case 5: descriptors[0].bufferdesc.buf_id++; break;
		case 6: mapping_id = 2; break;
		case 7: own.generation = UINT64_MAX; break;
		case 8: pg_atomic_write_u32(&descriptors[0].bufferdesc.state, BM_TAG_VALID | BM_IO_ERROR); break;
		}
		UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_REFUSED);
		UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 0);
	}
}
UT_TEST(test_ctrc_cache_original_current_before_and_after_native_locks)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		fixture();
		switch (fault) {
		case 0: current = false; break;
		case 1: scope_current = false; break;
		case 2: page_current = false; break;
		case 3: MyBackendType = B_CHECKPOINTER; break;
		case 4: recovering = true; break;
		case 5: CritSectionCount = 1; break;
		case 6: ShutdownRequestPending = 1; break;
		case 7: before.segment_incarnation[0]++; break;
		}
		UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_REFUSED);
		UT_ASSERT_EQ(mapping_calls + evictions, 0);
	}
	fixture(); drift_after_release = true;
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_REFUSED);
	UT_ASSERT(!mapping); UT_ASSERT_EQ(evictions, 1);
}
UT_TEST(test_ctrc_cache_failed_native_eviction_does_not_claim_absence)
{
	fixture(); retire_success = false;
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
	UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 1);
}
int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_ctrc_cache_absence_observation_never_evicts);
	UT_RUN(test_ctrc_cache_exact_clean_native_predecessor_uses_original_eviction);
	UT_RUN(test_ctrc_cache_busy_pin_dirty_io_or_first_never_retires);
	UT_RUN(test_ctrc_cache_active_pcm_pi_cr_cf_and_delivery_keep_native_owner);
	UT_RUN(test_ctrc_cache_wrong_bytes_version_source_or_mapping_refuse);
	UT_RUN(test_ctrc_cache_original_current_before_and_after_native_locks);
	UT_RUN(test_ctrc_cache_failed_native_eviction_does_not_claim_absence);
	UT_DONE(); return ut_failed_count != 0;
}
