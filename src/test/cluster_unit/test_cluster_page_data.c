/* Native resident DATA owner + full FlushBuffer body; actual file I/O and
 * checksum. Mapping/PCM grants, WAL flush and BufferIO are explicit fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

#include "access/xlog.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_page_data.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/storage/cluster_smgr.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "utils/resowner.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile uint32 CritSectionCount, InterruptHoldoffCount;
int cluster_node_id = 0, NBuffers = 2, NLocBuffer;
bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog = true;
bool cluster_smart_fusion, cluster_past_image;
ResourceOwner CurrentResourceOwner = (void *)1;
BufferUsage pgBufferUsage;
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static PGAlignedBlock pages[2];
char *BufferBlocks = (char *)pages;
Block *LocalBufferBlockPointers;
static ClusterPcmOwnEntry own[2];
ClusterPcmOwnEntry *ClusterPcmOwnArray = own;
static LWLock mapping;
static unsigned pins[2];
static bool locks[2], resident[2], busy[2], selected, sf_blocked, bad_checksum, bad_bytes;
static bool stale_sync, redirty_sync, checksums = true;
static bool zero_disk, late_fence;
static int throw_at;
static unsigned writes, syncs, reads, wal_flushes, aborts;
static XLogRecPtr local_insert_end;
static FILE *file;
static SMgrRelationData relation;
static ClusterPageDataTargetV1 target;
static ClusterWalSourceRef writer;
static ClusterPageWalBindingV1 page_sources[2];
static bool source_capture;
static ClusterSpaceIdentity identity;
static bool cluster_pcm_x_finish_retain_flush_active;
static bool cluster_pcm_x_finish_retain_flush_io_active;
static bool cluster_pcm_x_finish_retain_flush_error_context_pushed;
static ErrorContextCallback *cluster_pcm_x_finish_retain_flush_error_context_previous;
#ifdef ENABLE_INJECTION
static bool cluster_pcm_x_finish_retain_flush_fault_active;
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_injection_should_skip(name) false
#endif

void
ExceptionalCondition(const char *a, const char *b, int c)
{
	fprintf(stderr, "%s %s:%d\n", a, b, c);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
static void
fault(int at)
{
	if (throw_at == at) {
		InterruptHoldoffCount = 0;
		pg_re_throw();
	}
}
void *
palloc(Size size)
{
	void *p = malloc(size);
	if (!p)
		abort();
	return p;
}
void
pfree(void *p)
{
	free(p);
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool cluster_recmerge_window_active;
int cluster_pcm_grd_max_entries = 100;
static ClusterConf conf = { .node_count = 2 };
ClusterConf *ClusterConfShmem = &conf;
int
cluster_smgr_which_for(RelFileLocator r, BackendId b)
{
	return 1;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (!selected)
		return false;
	*out = writer;
	return true;
}
bool
cluster_control_root_identity_equal(const ClusterControlRootIdentity *a,
									const ClusterControlRootIdentity *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}
uint32
BufTableHashCode(BufferTag *tag)
{
	return tag->forkNum;
}
int
BufTableLookup(BufferTag *tag, uint32 hash)
{
	for (int i = 0; i < 2; i++)
		if (resident[i] && BufferTagsEqual(&descriptors[i].bufferdesc.tag, tag))
			return i;
	return -1;
}
#undef BufMappingPartitionLock
#define BufMappingPartitionLock(hash) (&mapping)
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &mapping);
	HOLD_INTERRUPTS();
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			if (busy[i])
				return false;
			UT_ASSERT(pins[i] && !locks[i]);
			if (i == 1)
				UT_ASSERT(locks[0]);
			locks[i] = true;
			HOLD_INTERRUPTS();
			return true;
		}
	abort();
}
void
LWLockRelease(LWLock *lock)
{
	/* Match the native release contract even in a release build. */
	UT_ASSERT(InterruptHoldoffCount > 0);
	RESUME_INTERRUPTS();
	if (lock == &mapping)
		return;
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			UT_ASSERT(locks[i]);
			locks[i] = false;
			return;
		}
	abort();
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc))
			return locks[i];
	return false;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return LWLockHeldByMe(lock) && (mode != LW_EXCLUSIVE || source_capture);
}
uint32
LockBufHdr(BufferDesc *buf)
{
	return pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED) | BM_LOCKED;
}
static void
cluster_bufmgr_pin_for_gcs_locked(BufferDesc *buf, uint32 state)
{
	pins[buf->buf_id]++;
	UnlockBufHdr(buf, state);
}
static void
cluster_bufmgr_unpin_for_gcs(BufferDesc *buf)
{
	UT_ASSERT(!locks[buf->buf_id] && pins[buf->buf_id]);
	pins[buf->buf_id]--;
}
static bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	return true;
}
static bool
cluster_bufmgr_pcm_current_image_locked(BufferDesc *buf, uint32 state)
{
	return cluster_pcm_x_current_image_shape(buf->pcm_state, buf->buffer_type,
											 (state & BM_VALID) != 0);
}
static bool
cluster_bufmgr_pcm_x_retained_image_locked(BufferDesc *buf, uint32 state)
{
	return buf->buffer_type == BUF_TYPE_PI && (state & BM_VALID);
}
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->tag = buf->tag;
	out->generation = cluster_pcm_own_gen_get(buf->buf_id);
	out->reservation_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	out->flags = cluster_pcm_own_flags_get(buf->buf_id);
	out->writer_activation_token = cluster_pcm_own_writer_activation_token_get(buf->buf_id);
	out->resource_x_activation_generation
		= cluster_pcm_own_resource_x_activation_generation_get(buf->buf_id);
	out->semantic_buf_state
		= pg_atomic_read_u32(&buf->state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK;
	out->pcm_state = buf->pcm_state;
	out->buffer_type = buf->buffer_type;
	return CLUSTER_PCM_OWN_OK;
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *buf)
{
	return buf->buf_id == 1 && locks[1] && source_capture;
}

Size
mul_size(Size a, Size b)
{
	return a * b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT_EQ(size, sizeof(page_sources));
	*found = false;
	return page_sources;
}
bool
cluster_space_recovery_flush_permitted_v1(const ClusterSpaceRecoveryBatchV1 *b, Buffer buf)
{
	return false;
}
static bool
StartBufferIO(BufferDesc *buf, bool input)
{
	if (!(pg_atomic_read_u32(&buf->state) & BM_DIRTY))
		return false;
	UT_ASSERT(!(pg_atomic_read_u32(&buf->state) & BM_IO_IN_PROGRESS));
	pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS);
	return true;
}
static void
TerminateBufferIO(BufferDesc *buf, bool clear, uint32 flags)
{
	uint32 state = pg_atomic_read_u32(&buf->state) & ~BM_IO_IN_PROGRESS;
	if (clear)
		state &= ~(BM_DIRTY | BM_JUST_DIRTIED | BM_IO_ERROR);
	pg_atomic_write_u32(&buf->state, state | flags);
}
void
AbortBufferIO(Buffer buf)
{
	aborts++;
	TerminateBufferIO(GetBufferDescriptor(buf - 1), false, BM_IO_ERROR);
}
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return local_insert_end;
}
void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 0x200);
	wal_flushes++;
	fault(1);
}
bool
cluster_sf_dep_buffer_flush_blocked(BufferDesc *buf)
{
	return sf_blocked;
}
void
cluster_gcs_block_pi_write_note(BufferTag tag, uint64 scn)
{
	abort();
}
bool
DataChecksumsEnabled(void)
{
	return checksums;
}
void
PageSetChecksumInplace(Page page, BlockNumber block)
{
	if (checksums)
		((PageHeader)page)->pd_checksum = pg_checksum_page(page, block);
}
char *
PageSetChecksumCopy(Page page, BlockNumber block)
{
	static PGAlignedBlock copy;
	memcpy(copy.data, page, BLCKSZ);
	PageSetChecksumInplace(copy.data, block);
	return copy.data;
}
SMgrRelation
smgropen(RelFileLocator r, BackendId b)
{
	UT_ASSERT(RelFileLocatorEquals(r, target.identity.locator));
	return &relation;
}
void
smgrwrite(SMgrRelation r, ForkNumber f, BlockNumber b, const void *data, bool skip)
{
	UT_ASSERT(locks[0] && locks[1] && wal_flushes);
	UT_ASSERT_EQ(f, target.identity.forknum);
	UT_ASSERT_EQ(b, target.identity.blockno);
	fault(2);
	writes++;
	UT_ASSERT_EQ(pwrite(fileno(file), data, BLCKSZ, 0), BLCKSZ);
}
void
smgrimmedsync(SMgrRelation r, ForkNumber f)
{
	UT_ASSERT(locks[0] && locks[1]);
	fault(3);
	UT_ASSERT_EQ(fsync(fileno(file)), 0);
	syncs++;
	if (stale_sync)
		writer.claim.identity.origin_owner_incarnation++;
	if (redirty_sync)
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	if (late_fence)
		pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
}
void
smgrread(SMgrRelation r, ForkNumber f, BlockNumber b, void *out)
{
	UT_ASSERT(locks[0] && locks[1] && syncs);
	fault(4);
	reads++;
	UT_ASSERT_EQ(pread(fileno(file), out, BLCKSZ, 0), BLCKSZ);
	if (zero_disk) {
		memset(out, 0, BLCKSZ);
		return;
	}
	if (bad_checksum)
		((PageHeader)out)->pd_checksum ^= 1;
	if (bad_bytes) {
		((char *)out)[500] ^= 1;
		PageSetChecksumInplace(out, b);
	}
}
static void
shared_buffer_write_error_callback(void *arg)
{
	abort();
}
#define BufHdrGetBlock(buf) ((Block)(BufferBlocks + (Size)(buf)->buf_id * BLCKSZ))
#define BufferGetLSN(buf) PageGetLSN(BufHdrGetBlock(buf))
#define BufferIsPinned(buf) (pins[(buf) - 1] != 0)
#define pgstat_prepare_io_time() ((instr_time){ 0 })
#define pgstat_count_io_op_time(a, b, c, d, e) ((void)(d))
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		InterruptHoldoffCount = 0;                                                                 \
		pg_re_throw();                                                                             \
	} while (0)
#include "test_cluster_space_recovery_flush.inc"
#include "test_cluster_page_data.inc"

static void
bind_native_source(void)
{
	RfPageVersionEdgeEntryV1 e = { 0 };
	e.page_class = RF_PAGE_CLASS_ORDINARY;
	e.result_kind = RF_PAGE_STATE_PRESENT;
	memcpy(e.result_incarnation, identity.incarnation, 16);
	source_capture = locks[1] = true;
	HOLD_INTERRUPTS();
	UT_ASSERT(cluster_page_wal_capture_native_v1(2, &e, 80, 0x100, 0x200, 0x9192, RM_HEAP_ID, 0));
	RESUME_INTERRUPTS();
	source_capture = locks[1] = false;
}

static void
reset(void)
{
	PageHeader p = (PageHeader)pages[1].data;
	local_insert_end = 0x500;
	cluster_node_id = 0;
	if (file)
		fclose(file);
	file = tmpfile();
	UT_ASSERT(file != NULL);
	memset(pages, 0, sizeof(pages));
	memset(descriptors, 0, sizeof(descriptors));
	memset(own, 0, sizeof(own));
	memset(&writer, 0, sizeof(writer));
	memset(&target, 0, sizeof(target));
	memset(&identity, 0, sizeof(identity));
	target.database_incarnation = 3;
	target.identity.system_identifier = 17;
	target.identity.storage_uuid[0] = 1;
	target.identity.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 9, 18000 };
	target.identity.forknum = MAIN_FORKNUM;
	target.identity.blockno = 7;
	target.version.segment_incarnation[0] = 12;
	target.version.mutation_token = 80;
	identity.key.system_identifier = 17;
	identity.key.database_incarnation = 3;
	identity.key.storage_uuid[0] = 1;
	identity.key.locator = target.identity.locator;
	identity.incarnation[0] = 12;
	identity.sequence = 1;
	identity.operation = 1;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = p->pd_special = BLCKSZ;
	p->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	p->pd_block_scn = 80;
	PageSetLSNPreserveOrigin(pages[1].data, 0x200);
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 0));
	writer.claim.identity.system_identifier = 17;
	writer.claim.identity.storage_uuid[0] = 1;
	writer.claim.identity.authority_uuid[0] = 8;
	writer.claim.identity.origin_thread_id = 1;
	writer.claim.identity.thread_claim_created_at = 100;
	writer.claim.identity.origin_owner_incarnation = 9;
	writer.claim.identity.root_lineage_seq = 1;
	writer.claim.database_incarnation = 3;
	writer.claim.max_config_generation = 1;
	writer.claim.claim_sha256[0] = 6;
	writer.timeline = 1;
	for (int i = 0; i < 2; i++) {
		BufferDesc *buf = &descriptors[i].bufferdesc;
		buf->buf_id = i;
		buf->pcm_state = i ? PCM_STATE_X : PCM_STATE_S;
		buf->buffer_type = i ? BUF_TYPE_XCUR : BUF_TYPE_SCUR;
		InitBufferTag(&buf->tag, &target.identity.locator, i ? MAIN_FORKNUM : SPACE_FORKNUM,
					  i ? 7 : 0);
		pg_atomic_init_u32(&buf->state,
						   BM_VALID | BM_TAG_VALID | BM_PERMANENT | (i ? BM_DIRTY : 0));
		pg_atomic_init_u64(&own[i].generation, 2);
		pins[i] = 0;
		locks[i] = busy[i] = false;
		resident[i] = true;
	}
	relation.smgr_rlocator.locator = target.identity.locator;
	selected = true;
	sf_blocked = bad_checksum = bad_bytes = stale_sync = redirty_sync = false;
	zero_disk = late_fence = false;
	checksums = true;
	throw_at = 0;
	writes = syncs = reads = wal_flushes = aborts = 0;
	cluster_smart_fusion = cluster_past_image = false;
	error_context_stack = NULL;
	cluster_page_wal_shmem_init();
	bind_native_source();
}
static void
clean(void)
{
	for (int i = 0; i < 2; i++) {
		UT_ASSERT_EQ(pins[i], 0);
		UT_ASSERT(!locks[i]);
		UT_ASSERT(!(pg_atomic_read_u32(&descriptors[i].bufferdesc.state)
					& (BM_IO_IN_PROGRESS | BM_LOCKED)));
	}
	UT_ASSERT(error_context_stack == NULL);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}
static void
success_and_old_completion(void)
{
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPageDataTargetV1 out;
	PGAlignedBlock disk;
	reset();
	UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &receipt));
	if (receipt == NULL)
		return;
	UT_ASSERT(receipt != NULL);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(pread(fileno(file), disk.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(((PageHeader)disk.data)->pd_checksum, pg_checksum_page(disk.data, 7));
	UT_ASSERT_EQ(((PageHeader)disk.data)->pd_block_scn, 80);
	clean();
	((PageHeader)pages[1].data)->pd_block_scn = 19;
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	UT_ASSERT(cluster_page_data_receipt_read_v1(receipt, &out));
	UT_ASSERT_EQ(out.version.mutation_token, 80);
	UT_ASSERT_EQ(out.database_incarnation, 3);
	UT_ASSERT(memcmp(out.version.segment_incarnation, target.version.segment_incarnation, 16) == 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	cluster_page_data_receipt_free_v1(&receipt);
	UT_ASSERT(receipt == NULL);
}
static void
identity_refusals(void)
{
	for (int c = 0; c < 8; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		switch (c) {
		case 0:
			target.database_incarnation++;
			break;
		case 1:
			target.version.segment_incarnation[15]++;
			break;
		case 2:
			target.version.mutation_token++;
			break;
		case 3:
			target.identity.storage_uuid[15]++;
			break;
		case 4:
			target.identity.forknum = FSM_FORKNUM;
			break;
		case 5:
			selected = false;
			break;
		case 6:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
			break;
		case 7:
			((PageHeader)pages[1].data)->pd_special = 0;
			break;
		}
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
		clean();
	}
}
static void
authority_refusals(void)
{
	for (int c = 0; c < 9; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		switch (c) {
		case 0:
			resident[0] = false;
			break;
		case 1:
			resident[1] = false;
			break;
		case 2:
			busy[0] = true;
			break;
		case 3:
			busy[1] = true;
			break;
		case 4:
			descriptors[1].bufferdesc.buffer_type = BUF_TYPE_PI;
			break;
		case 5:
			pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
			break;
		case 6:
			pg_atomic_write_u64(&own[1].writer_activation_token, 1);
			break;
		case 7:
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_IO_ERROR);
			break;
		case 8:
			descriptors[1].bufferdesc.pcm_state = PCM_STATE_S;
			break;
		}
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
		clean();
	}
}
static void
explicit_wal_origin(void)
{
	for (int c = 0; c < 4; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c < 2) {
			UT_ASSERT(PageSetLSNOrigin(pages[1].data, 1));
			PageSetLSNPreserveOrigin(pages[1].data, c ? 0x800 : 0x100);
		} else if (c == 2)
			PageSetLSNPreserveOrigin(pages[1].data, 0x800);
		else
			PageClearLSNOrigin(pages[1].data);
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes + syncs + wal_flushes, 0);
		clean();
	}
}
static void
io_failure_cleanup(void)
{
	for (int at = 1; at <= 4; at++) {
		ClusterPageDataReceiptV1 *r = NULL;
		volatile bool caught = false;
		reset();
		throw_at = at;
		PG_TRY();
		{
			(void)cluster_bufmgr_write_page_data_v1(&target, &r);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(aborts, at <= 2 ? 1 : 0);
		clean();
	}
}
static void
completion_refusals(void)
{
	for (int c = 0; c < 7; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c == 0)
			bad_checksum = true;
		if (c == 1)
			bad_bytes = true;
		if (c == 2)
			stale_sync = true;
		if (c == 3)
			redirty_sync = true;
		if (c == 4)
			cluster_smart_fusion = sf_blocked = true;
		if (c == 5)
			zero_disk = true;
		if (c == 6)
			late_fence = true;
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		clean();
		if (c == 3 || c == 4)
			UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	}
}
static void
vm_and_clean_completion(void)
{
	for (int enabled = 0; enabled <= 1; enabled++) {
		ClusterPageDataReceiptV1 *r = NULL;
		ClusterPageDataTargetV1 out;
		reset();
		checksums = enabled;
		target.identity.forknum = VISIBILITYMAP_FORKNUM;
		descriptors[1].bufferdesc.tag.forkNum = VISIBILITYMAP_FORKNUM;
		bind_native_source();
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		if (!r)
			continue;
		UT_ASSERT(cluster_page_data_receipt_read_v1(r, &out));
		UT_ASSERT_EQ(out.identity.forknum, VISIBILITYMAP_FORKNUM);
		cluster_page_data_receipt_free_v1(&r);
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(syncs, 2);
		UT_ASSERT_EQ(reads, 2);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}
static void
new_claim_never_flushes_old_coordinate(void)
{
	for (int c = 0; c < 3; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c == 0)
			writer.claim.identity.origin_owner_incarnation++;
		if (c == 1)
			writer.claim.claim_sha256[15]++;
		if (c == 2)
			memset(page_sources, 0, sizeof(page_sources));
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + wal_flushes, 0);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}

static void
foreign_certified_image_uses_original_wal(void)
{
	for (int c = 0; c < 2; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		ClusterPageWalBindingV1 certified, prepared;
		reset();
		UT_ASSERT(cluster_page_wal_flush_source_v1(&page_sources[1], &certified));
		UT_ASSERT_EQ(wal_flushes, 1);
		writer.claim.identity.origin_thread_id = 2;
		writer.claim.identity.origin_node_id = cluster_node_id = 1;
		writer.claim.identity.origin_owner_incarnation++;
		writer.claim.claim_sha256[0]++;
		local_insert_end = c ? 0x100 : 0x900;
		/* Same page, uncertified old source cannot use the receiver's WAL. */
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes + syncs, 0);
		source_capture = locks[1] = true;
		HOLD_INTERRUPTS();
		UT_ASSERT(cluster_page_wal_prepare_install_v1(2, &certified, pages[1].data, &prepared));
		UT_ASSERT(cluster_page_wal_publish_install_v1(2, &prepared));
		RESUME_INTERRUPTS();
		source_capture = locks[1] = false;
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r != NULL);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}
int
main(void)
{
	UT_PLAN(9);
	UT_RUN(success_and_old_completion);
	UT_RUN(identity_refusals);
	UT_RUN(authority_refusals);
	UT_RUN(explicit_wal_origin);
	UT_RUN(io_failure_cleanup);
	UT_RUN(completion_refusals);
	UT_RUN(vm_and_clean_completion);
	UT_RUN(new_claim_never_flushes_old_coordinate);
	UT_RUN(foreign_certified_image_uses_original_wal);
	if (file)
		fclose(file);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
