/* Actual native insertion/registration/reset and resident source binding.
 * WAL allocation, shared-memory allocation, PCM and content locks are fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_space_reservation.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_page_anchor_cache.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "port/pg_crc32c.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static BufferDescPadded desc, many_descriptors[258];
BufferDescPadded *BufferDescriptors = &desc;
static ClusterPcmOwnEntry own_entry;
ClusterPcmOwnEntry *ClusterPcmOwnArray = &own_entry;
static PGAlignedBlock page, private_page, many_pages[258];
char *BufferBlocks = page.data;
Block *LocalBufferBlockPointers;
int NBuffers = 1, NLocBuffer, cluster_node_id = 0;
bool cluster_enabled = true, cluster_shared_config = true;
static bool locked, exclusive, permitted, selected;
static ClusterSpaceIdentity space;
static ClusterWalSourceRef writer;
static void *shared_memory;
static Size shared_bytes;
static bool attach_existing;
static const ClusterShmemRegion *registered_region;
static RfPageVersionEdgeEntryV1 edge;
static unsigned allocations, insert_calls, assemble_calls;
static bool retry_insert;
static bool consistency_check;
static unsigned flush_calls;
static bool flush_changes_source;
static XLogRecPtr insert_end;
static sigjmp_buf native_error_target;
static bool catch_native_errors, native_panicked;
XLogRecPtr ProcLastRecPtr, XactLastRecEnd;
static XLogRecPtr last_insert_record_end;
ProcessingMode Mode = NormalProcessing;

#include "test_cluster_pcm_checksum_owner.inc"
#include "test_cluster_page_wal_image.inc"
#include "test_cluster_page_data_scn.inc"

/* Real pre-grant caller; ownership checks are controlled boundaries so all
 * of its early exits must release the actual source-pool preparation. */
static unsigned pregrant_variant, pregrant_snapshots, pregrant_failclosed;
static ClusterPcmOwnResult
pregrant_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	pregrant_snapshots++;
	return ((pregrant_variant == 1 && pregrant_snapshots == 1)
			|| (pregrant_variant == 4 && pregrant_snapshots == 2))
			   ? CLUSTER_PCM_OWN_STALE
			   : CLUSTER_PCM_OWN_OK;
}
#define cluster_bufmgr_pcm_own_snapshot pregrant_snapshot
#define gcs_block_pcm_x_reserved_image_write_exact(...) (pregrant_variant != 2)
#define cluster_pcm_own_classify_live_flags(...) CLUSTER_PCM_OWN_CORRUPT
#define cluster_bufmgr_pcm_own_publish_installed_x_image(...)                                      \
	(pregrant_variant == 3 ? CLUSTER_PCM_OWN_CORRUPT : CLUSTER_PCM_OWN_OK)
#define gcs_block_resource_x_fail_closed_current() (pregrant_failclosed++)
#define gcs_block_note_install_copy() ((void)0)
#include "test_cluster_page_wal_pregrant.inc"
#undef cluster_bufmgr_pcm_own_snapshot
#undef gcs_block_pcm_x_reserved_image_write_exact
#undef cluster_pcm_own_classify_live_flags
#undef cluster_bufmgr_pcm_own_publish_installed_x_image
#undef gcs_block_resource_x_fail_closed_current
#undef gcs_block_note_install_copy

bool
errstart(int level, const char *domain)
{
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}

/* Only fields used by the extracted native functions. Assembly is the
 * explicit boundary; native product compilation retains its complete type. */
typedef struct {
	bool in_use, force_fpi_applied, apply_image_emitted, page_anchor_key_registered;
	uint8 flags;
	Buffer page_version_buffer;
	RfPageAnchorCacheKeyV1 page_anchor_key;
	RelFileLocator rlocator;
	ForkNumber forkno;
	BlockNumber block;
	Page page;
	uint32 rdata_len;
	XLogRecData *rdata_head, *rdata_tail;
} registered_buffer;
static registered_buffer buffers[2];
static registered_buffer *registered_buffers = buffers;
static int max_registered_buffers = 2, max_registered_block_id, num_rdatas;
static XLogRecData *mainrdata_head, *mainrdata_last;
static uint64 mainrdata_len;
static uint8 curinsert_flags;
static bool begininsert_called, page_version_edge_registered;
static uint64 registered_page_version_result_token;
static uint8 registered_page_version_entry_count;
static RfPageVersionEdgeEntryV1 registered_page_version_entries[XLR_PAGE_VERSION_EDGE_MAX_ENTRIES];
static XLogRecord record;
static char *hdr_scratch = (char *)&record;

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
Size
mul_size(Size a, Size b)
{
	return a * b;
}
Size
add_size(Size a, Size b)
{
	return a + b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (attach_existing) {
		UT_ASSERT_EQ(size, shared_bytes);
		*found = true;
		return shared_memory;
	}
	free(shared_memory);
	shared_memory = calloc(1, size);
	UT_ASSERT(shared_memory != NULL);
	shared_bytes = size;
	allocations++;
	*found = false;
	return shared_memory;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	registered_region = region;
}
bool
RecoveryInProgress(void)
{
	return false;
}
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return insert_end;
}
void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 0x200);
	flush_calls++;
	if (flush_changes_source)
		writer.claim.claim_sha256[1]++;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (!selected)
		return false;
	*out = writer;
	return true;
}
int
cluster_smgr_which_for(RelFileLocator loc, BackendId b)
{
	return 1;
}
bool
LWLockHeldByMe(LWLock *l)
{
	return locked;
}
bool
LWLockHeldByMeInMode(LWLock *l, LWLockMode m)
{
	return locked && (m != LW_EXCLUSIVE || exclusive);
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *buf)
{
	return permitted;
}
void
BufferGetTag(Buffer b, RelFileLocator *r, ForkNumber *f, BlockNumber *n)
{
	UT_ASSERT_EQ(b, 1);
	*r = space.key.locator;
	*f = desc.bufferdesc.tag.forkNum;
	*n = 7;
}
bool
rf_page_anchor_cache_record_v1(const RfPageAnchorCacheKeyV1 *key, bool a, bool b)
{
	return true;
}
void
GetFullPageWriteInfo(XLogRecPtr *redo, bool *write)
{
	*redo = 0x80;
	*write = true;
}
static XLogRecData *
XLogRecordAssemble(RmgrId rm, uint8 info, XLogRecPtr redo, bool fpw, XLogRecPtr *lsn, int *fpi,
				   bool *top)
{
	static XLogRecData data;
	assemble_calls++;
	*lsn = 0;
	*fpi = 0;
	*top = false;
	data.data = hdr_scratch;
	record.xl_rmid = rm;
	record.xl_info = info | (consistency_check ? XLR_CHECK_CONSISTENCY : 0);
	data.len = sizeof(record);
	data.next = NULL;
	return &data;
}
XLogRecPtr
XLogInsertRecord(XLogRecData *data, XLogRecPtr fpw, uint8 flags, int fpi, bool top)
{
	insert_calls++;
	if (retry_insert && insert_calls == 1)
		return InvalidXLogRecPtr;
	ProcLastRecPtr = 0x120;
	record.xl_crc = 0x9192;
	return XactLastRecEnd = 0x200;
}
static void
pg_attribute_noreturn() native_error(void)
{
	native_panicked = true;
	if (catch_native_errors)
		siglongjmp(native_error_target, 1);
	abort();
}
#undef elog
#define elog(level, ...) native_error()
#include "test_cluster_page_wal_insert.inc"

static void
reset(void)
{
	PageHeader h = (PageHeader)page.data;
	NBuffers = 1;
	BufferDescriptors = &desc;
	BufferBlocks = page.data;
	memset(&desc, 0, sizeof(desc));
	memset(&page, 0, sizeof(page));
	memset(&space, 0, sizeof(space));
	memset(&writer, 0, sizeof(writer));
	memset(&edge, 0, sizeof(edge));
	memset(buffers, 0, sizeof(buffers));
	space.key.system_identifier = 17;
	space.key.database_incarnation = 3;
	space.key.storage_uuid[0] = 4;
	space.key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 9, 18000 };
	space.incarnation[0] = 6;
	space.sequence = 1;
	space.operation = 1;
	space.state = CLUSTER_SPACE_IDENTITY_LIVE;
	writer.claim.identity.system_identifier = 17;
	writer.claim.identity.storage_uuid[0] = 4;
	writer.claim.identity.authority_uuid[0] = 10;
	writer.claim.identity.origin_thread_id = 1;
	writer.claim.identity.thread_claim_created_at = 99;
	writer.claim.identity.origin_owner_incarnation = 7;
	writer.claim.identity.root_lineage_seq = 1;
	writer.claim.database_incarnation = 3;
	writer.claim.max_config_generation = 4;
	writer.claim.claim_sha256[0] = 13;
	writer.timeline = 1;
	desc.bufferdesc.buf_id = 0;
	InitBufferTag(&desc.bufferdesc.tag, &space.key.locator, MAIN_FORKNUM, 7);
	pg_atomic_init_u32(&desc.bufferdesc.state, BM_VALID | BM_TAG_VALID | BM_PERMANENT | BM_DIRTY);
	h->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	h->pd_lower = SizeOfPageHeaderData;
	h->pd_upper = h->pd_special = BLCKSZ;
	h->pd_block_scn = 80;
	PageSetLSNPreserveOrigin(page.data, 0x100);
	UT_ASSERT(PageSetLSNOrigin(page.data, 0));
	edge.page_class = RF_PAGE_CLASS_ORDINARY;
	edge.before_kind = RF_PAGE_STATE_PRESENT;
	edge.result_kind = RF_PAGE_STATE_PRESENT;
	edge.result_incarnation[0] = 6;
	edge.before.segment_incarnation[0] = 6;
	edge.before.mutation_token = 100;
	locked = exclusive = permitted = selected = true;
	cluster_enabled = cluster_shared_config = true;
	insert_calls = assemble_calls = allocations = 0;
	retry_insert = false;
	consistency_check = false;
	flush_calls = 0;
	flush_changes_source = false;
	insert_end = 0x500;
	catch_native_errors = native_panicked = false;
	XLogResetInsertion();
	cluster_page_wal_shmem_init();
	begininsert_called = true;
	page_version_edge_registered = true;
	registered_page_version_result_token = 80;
	registered_page_version_entry_count = 1;
	registered_page_version_entries[0] = edge;
}
static bool
capture(void)
{
	return cluster_page_wal_capture_native_v1(1, &edge, 80, 0x120, 0x200, 0x9192, RM_HEAP_ID, 0)
		   == CLUSTER_PAGE_WAL_CAPTURED;
}
static ClusterPageWalBindingV1
current_binding(void)
{
	ClusterPageWalBindingV1 result = { 0 };
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &result));
	return result;
}
static void
output_snapshot_retains_exact_failed_output_binding(void)
{
	for (int variant = 0; variant < 9; variant++) {
		ClusterPageWalBindingV1 original, observed;
		uint32 before;

		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		original = current_binding();
		pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_ERROR);
		switch (variant) {
		case 1:
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_IN_PROGRESS);
			break;
		case 2:
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_DIRTY);
			break;
		case 3:
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_VALID);
			break;
		case 4:
			locked = false;
			break;
		case 5:
			((PageHeader)page.data)->pd_block_scn++;
			break;
		case 6:
			PageSetLSNPreserveOrigin(page.data, 0x201);
			break;
		case 7:
			UT_ASSERT(cluster_page_wal_forget_v1(1));
			break;
		case 8:
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_TAG_VALID);
			break;
		}
		before = pg_atomic_read_u32(&desc.bufferdesc.state);
		UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &observed));
		UT_ASSERT_EQ(cluster_page_wal_output_snapshot_v1(1, &observed), variant < 2);
		if (variant < 2)
			UT_ASSERT(cluster_page_wal_same_mutation_v1(&original, &observed));
		UT_ASSERT_EQ(pg_atomic_read_u32(&desc.bufferdesc.state), before);
	}
}

static void
resident_binding_memory_budget(void)
{
	Size one, many;
	NBuffers = 1;
	one = cluster_page_wal_shmem_size();
	NBuffers = 16384;
	many = cluster_page_wal_shmem_size();
	printf("# Resident WAL bytes per buffer: %zu; 16384-buffer region: %zu bytes\n",
		   (many - one) / 16383, many);
	/* Latest binding 48B + first record 48B + its atomic LSN 8B (R-A22). */
	UT_ASSERT_EQ(many - one, (Size)104 * 16383);
	UT_ASSERT(many <= (Size)104 * NBuffers + 65536);
	NBuffers = 1;
}

static void
failed_output_distinguishes_empty_latest_from_invalid_binding(void)
{
	for (int variant = 0; variant < 8; variant++) {
		ClusterPageWalBindingV1 observed;
		uint32 before;
		reset();
		if (variant == 1 || variant == 2 || variant == 3) {
			UT_ASSERT(capture());
			PageSetLSNPreserveOrigin(page.data, 0x200);
		}
		if (variant == 2)
			((PageHeader)page.data)->pd_block_scn++;
		if (variant == 3)
			UT_ASSERT(cluster_page_wal_forget_v1(1));
		if (variant == 4)
			locked = false;
		if (variant == 5)
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_VALID);
		if (variant == 6)
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_DIRTY);
		if (variant == 7)
			exclusive = false; /* the normal output owner holds content SHARE */
		pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_ERROR);
		before = pg_atomic_read_u32(&desc.bufferdesc.state);
		UT_ASSERT_EQ(cluster_page_wal_output_binding_absent_v1(1),
					 variant == 0 || variant == 3 || variant == 7);
		if (variant == 2)
			UT_ASSERT(!cluster_page_wal_output_snapshot_v1(1, &observed));
		UT_ASSERT(!cluster_page_wal_output_binding_absent_v1(0));
		UT_ASSERT(!cluster_page_wal_output_binding_absent_v1(NBuffers + 1));
		UT_ASSERT_EQ(pg_atomic_read_u32(&desc.bufferdesc.state), before);
	}
}
static void
native_insert_source(void)
{
	ClusterPageWalBindingV1 out;
	reset();
	retry_insert = true;
	consistency_check = true;
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(insert_calls, 2);
	UT_ASSERT_EQ(assemble_calls, 2);
	UT_ASSERT(!begininsert_called && !page_version_edge_registered);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(cluster_page_wal_read_v1(1, &space, &out));
	if (!cluster_page_wal_read_v1(1, &space, &out))
		return;
	UT_ASSERT_EQ(out.record_start, 0x120);
	UT_ASSERT_EQ(out.record_end, 0x200);
	UT_ASSERT_EQ(out.record_crc, 0x9192);
	UT_ASSERT_EQ(out.rmid, RM_HEAP_ID);
	UT_ASSERT_EQ(out.info, XLR_CHECK_CONSISTENCY);
	UT_ASSERT_EQ(out.version.mutation_token, 80);
	UT_ASSERT_EQ(out.source.claim.identity.origin_owner_incarnation, 7);
	UT_ASSERT_EQ(out.source.claim.max_config_generation, 4);
	UT_ASSERT_EQ(out.source.claim.claim_sha256[0], 13);
	UT_ASSERT_EQ(allocations, 1);
}
static void
exact_generation_survives_new_writer(void)
{
	ClusterPageWalBindingV1 out;
	reset();
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	writer.claim.identity.origin_owner_incarnation++;
	writer.claim.claim_sha256[15]++;
	UT_ASSERT(cluster_page_wal_read_v1(1, &space, &out));
	if (!cluster_page_wal_read_v1(1, &space, &out))
		return;
	UT_ASSERT_EQ(out.source.claim.identity.origin_owner_incarnation, 7);
	UT_ASSERT_EQ(out.source.claim.claim_sha256[15], 0);
}
static void
native_capture_refusal_clears_old_binding(void)
{
	ClusterPageWalBindingV1 out;
	reset();
	UT_ASSERT(capture());
	selected = false;
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	catch_native_errors = true;
	if (sigsetjmp(native_error_target, 1) == 0)
		UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	catch_native_errors = false;
	UT_ASSERT(!native_panicked);
	UT_ASSERT_EQ(insert_calls, 1);
	UT_ASSERT(!begininsert_called && !page_version_edge_registered);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
}
static void
native_capture_requires_clear_owner(void)
{
	void *before;
	reset();
	UT_ASSERT(capture());
	before = malloc(shared_bytes);
	memcpy(before, shared_memory, shared_bytes);
	exclusive = false;
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	catch_native_errors = true;
	if (sigsetjmp(native_error_target, 1) == 0)
		(void)XLogInsert(RM_HEAP_ID, 0);
	catch_native_errors = false;
	UT_ASSERT(native_panicked);
	UT_ASSERT(memcmp(shared_memory, before, shared_bytes) == 0);
	free(before);
}
static void
stale_page_or_space_never_reads(void)
{
	for (int i = 0; i < 8; i++) {
		ClusterPageWalBindingV1 out, before;
		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		memset(&out, 0xa5, sizeof(out));
		before = out;
		if (i == 0)
			((PageHeader)page.data)->pd_block_scn = 19;
		if (i == 1)
			space.incarnation[15]++;
		if (i == 2)
			space.key.database_incarnation++;
		if (i == 3)
			PageSetLSNPreserveOrigin(page.data, 0x201);
		if (i == 4)
			PageSetLSNOrigin(page.data, 1);
		if (i == 5)
			desc.bufferdesc.tag.relNumber++;
		if (i == 6)
			locked = false;
		if (i == 7)
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_VALID);
		UT_ASSERT(!cluster_page_wal_read_v1(1, &space, &out));
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	}
}

static void
native_capture_invariant_failure_is_not_attribution_loss(void)
{
	for (int variant = 0; variant < 4; variant++) {
		reset();
		UT_ASSERT(capture());
		if (variant % 2 == 0)
			permitted = false;
		else
			((PageHeader)page.data)->pd_block_scn = 19;
		if (variant >= 2)
			selected = false; /* missing source cannot hide a broken page owner */
		XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
		catch_native_errors = true;
		if (sigsetjmp(native_error_target, 1) == 0)
			(void)XLogInsert(RM_HEAP_ID, 0);
		catch_native_errors = false;
		UT_ASSERT(native_panicked);
		UT_ASSERT_EQ(insert_calls, 1);
	}
}
static void
capture_requires_original_owner(void)
{
	for (int i = 0; i < 6; i++) {
		ClusterPageWalBindingV1 out;
		reset();
		if (i == 0)
			exclusive = false;
		if (i == 1)
			permitted = false;
		if (i == 2)
			selected = false;
		if (i == 3)
			((PageHeader)page.data)->pd_block_scn = 19;
		if (i == 4)
			edge.result_incarnation[0] = 0;
		if (i == 5)
			edge.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
		UT_ASSERT(!capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
	}
}
static void
private_page_and_nonshared_skip(void)
{
	ClusterPageWalBindingV1 out;
	reset();
	memcpy(private_page.data, page.data, BLCKSZ);
	XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
	reset();
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT_EQ(current_binding().version.mutation_token, 80);
	/* Reuse the same native registration slot for a private build page. */
	begininsert_called = page_version_edge_registered = true;
	registered_page_version_result_token = 19;
	registered_page_version_entry_count = 1;
	registered_page_version_entries[0] = edge;
	((PageHeader)private_page.data)->pd_block_scn = 19;
	XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(current_binding().version.mutation_token, 80);
	reset();
	cluster_shared_config = false;
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
}
static void
global_catalog_identity(void)
{
	ClusterPageWalBindingV1 out;
	reset();
	space.key.locator.spcOid = GLOBALTABLESPACE_OID;
	space.key.locator.dbOid = InvalidOid;
	InitBufferTag(&desc.bufferdesc.tag, &space.key.locator, MAIN_FORKNUM, 7);
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(cluster_page_wal_read_v1(1, &space, &out));
	if (!cluster_page_wal_read_v1(1, &space, &out))
		return;
	UT_ASSERT(rf_page_identity_valid_v1(&out.identity));
	out.identity.locator.dbOid = 9;
	UT_ASSERT(!rf_page_identity_valid_v1(&out.identity));
	out.identity.locator.dbOid = InvalidOid;
	out.identity.locator.spcOid = DEFAULTTABLESPACE_OID;
	UT_ASSERT(!rf_page_identity_valid_v1(&out.identity));
}
static void
carrier_install_keeps_original_generation(void)
{
	ClusterPageWalBindingV1 carrier, out;
	ClusterPageWalInstallV1 prepared = { 0 };
	bool ready;
	ResourceXDecodedFrame block = { 0 }, status, image;
	ClusterPcmOwnSnapshot revoking = { 0 };
	reset();
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &carrier));
	if (!cluster_page_wal_snapshot_v1(1, &carrier))
		return;
	UT_ASSERT(cluster_page_wal_flush_source_v1(&carrier, &carrier));
	UT_ASSERT_EQ(flush_calls, 1);
	block.kind = RESOURCE_X_WIRE_BLOCK_TO_N;
	block.common.logical_assertion.resource = desc.bufferdesc.tag;
	block.common.logical_assertion.requester_node = 1;
	block.common.base_authority_generation = 1;
	block.common.resource_formation = 2;
	block.common.master_session_incarnation = 3;
	block.common.assertion_sequence = 4;
	block.common.sender_connection_generation = 5;
	revoking.generation = 7;
	revoking.reservation_token = 8;
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = PCM_STATE_X;
	UT_ASSERT(gcs_block_pcm_x_resource_x_build_source_frames(
		&block, &revoking, page.data, 0x200, 80, &carrier, 11, 12, PCM_STATE_X, &status, &image));
	UT_ASSERT_EQ(image.body.image_envelope.image_flags, RESOURCE_X_IMAGE_HAS_WAL);
	if (image.body.image_envelope.image_flags != RESOURCE_X_IMAGE_HAS_WAL)
		return;
	UT_ASSERT_EQ(status.body.blocked_to_n.source_proof_crc32c, image.common.semantic_crc32c);
	UT_ASSERT_EQ(memcmp(&carrier, &image.body.image_envelope.page_wal, sizeof(carrier)), 0);
	carrier = image.body.image_envelope.page_wal;
	writer.claim.identity.origin_thread_id = 2;
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_owner_incarnation = 12;
	writer.claim.claim_sha256[0] = 99;
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	ready = cluster_page_wal_prepare_install_v1(1, &carrier, page.data, &prepared);
	UT_ASSERT(ready);
	if (!ready)
		return;
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &prepared));
	cluster_page_wal_release_install_v1(&prepared);
	UT_ASSERT(cluster_page_wal_read_v1(1, &space, &out));
	UT_ASSERT_EQ(out.source.claim.identity.origin_thread_id, 1);
	UT_ASSERT_EQ(out.source.claim.claim_sha256[0], 13);
	/* Same transfer without a new mutation preserves the original source. */
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &out));
	UT_ASSERT_EQ(memcmp(&out, &carrier, sizeof(out)), 0);
	space.incarnation[1]++;
	UT_ASSERT(!cluster_page_wal_read_v1(1, &space, &out));
}
static void
carrier_preflight_refuses_without_mutation(void)
{
	for (int i = 0; i < 9; i++) {
		ClusterPageWalBindingV1 carrier;
		ClusterPageWalInstallV1 prepared;
		void *before;
		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		UT_ASSERT(cluster_page_wal_snapshot_v1(1, &carrier));
		if (!cluster_page_wal_snapshot_v1(1, &carrier))
			return;
		before = malloc(shared_bytes);
		memcpy(before, shared_memory, shared_bytes);
		memset(&prepared, 0x59, sizeof(prepared));
		if (i == 0)
			writer.claim.database_incarnation++;
		if (i == 1)
			writer.claim.identity.storage_uuid[1]++;
		if (i == 2)
			writer.claim.identity.system_identifier++;
		if (i == 3)
			carrier.identity.blockno++;
		if (i == 4)
			carrier.record_end++;
		if (i == 5)
			exclusive = false;
		if (i == 6)
			selected = false;
		if (i == 7)
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_ERROR);
		if (i == 8) {
			writer.claim.identity.origin_thread_id = 2;
			writer.claim.identity.origin_node_id = 1;
		}
		UT_ASSERT(!cluster_page_wal_prepare_install_v1(1, &carrier, page.data, &prepared));
		UT_ASSERT_EQ(((uint8 *)&prepared)[0], 0x59);
		UT_ASSERT_EQ(memcmp(shared_memory, before, shared_bytes), 0);
		free(before);
	}
}
static void
unattributed_carrier_clears_old_source(void)
{
	ClusterPageWalBindingV1 zero = { 0 }, out;
	ClusterPageWalInstallV1 prepared = { 0 };
	bool ready;
	reset();
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	ready = cluster_page_wal_prepare_install_v1(1, &zero, page.data, &prepared);
	UT_ASSERT(ready);
	if (!ready)
		return;
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &prepared));
	cluster_page_wal_release_install_v1(&prepared);
	UT_ASSERT(!cluster_page_wal_read_v1(1, &space, &out));
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &out));
}
static void
flush_certification_is_source_exact(void)
{
	ClusterPageWalBindingV1 certified, copied, original;
	ClusterPageWalInstallV1 prepared = { 0 };
	reset();
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	original = current_binding();
	UT_ASSERT(cluster_page_wal_flush_source_v1(&original, &certified));
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(certified.flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	UT_ASSERT_EQ(current_binding().flags, 0);
	UT_ASSERT_EQ(flush_calls, 1);
	writer.claim.identity.origin_thread_id = 2;
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_owner_incarnation++;
	insert_end = 0x10;
	UT_ASSERT(cluster_page_wal_flush_source_v1(&certified, &copied));
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT(cluster_page_wal_same_mutation_v1(&copied, &original));
	UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &copied, page.data, &prepared));
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &prepared));
	cluster_page_wal_release_install_v1(&prepared);
	UT_ASSERT_EQ(current_binding().flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	UT_ASSERT(capture());
	UT_ASSERT(PageSetLSNOrigin(page.data, 1));
	UT_ASSERT_EQ(current_binding().flags, 0);
}

static void
installed_carrier_matches_explicit_absence_not_snapshot_failure(void)
{
	for (int absent = 0; absent < 2; absent++)
		for (int variant = 0; variant < 10; variant++) {
			ClusterPageWalBindingV1 expected, zero = { 0 };
			void *before;
			uint32 state;

			reset();
			UT_ASSERT(capture());
			PageSetLSNPreserveOrigin(page.data, 0x200);
			expected = current_binding();
			if (absent) {
				UT_ASSERT(cluster_page_wal_forget_v1(1));
				expected = zero;
			}
			switch (variant) {
			case 1:
				locked = false;
				break;
			case 2:
				exclusive = false;
				break;
			case 3:
				pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_ERROR);
				break;
			case 4:
				pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_IN_PROGRESS);
				break;
			case 5:
				pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_VALID);
				break;
			case 6:
				pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_TAG_VALID);
				break;
			case 7:
				expected.source.claim.claim_sha256[0]++;
				break;
			case 8:
				expected.flags ^= CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
				break;
			case 9:
				if (absent)
					UT_ASSERT(capture());
				else
					UT_ASSERT(cluster_page_wal_forget_v1(1));
				break;
			}
			before = malloc(shared_bytes);
			UT_ASSERT(before != NULL);
			if (before == NULL)
				return;
			memcpy(before, shared_memory, shared_bytes);
			state = pg_atomic_read_u32(&desc.bufferdesc.state);
			UT_ASSERT_EQ(cluster_page_wal_install_matches_v1(1, &expected), variant == 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&desc.bufferdesc.state), state);
			UT_ASSERT_EQ(memcmp(before, shared_memory, shared_bytes), 0);
			free(before);
		}
	{
		ClusterPageWalBindingV1 zero = { 0 }, observed;
		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		((PageHeader)page.data)->pd_block_scn++;
		UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &observed));
		UT_ASSERT(!cluster_page_wal_install_matches_v1(1, &zero));
		UT_ASSERT(!cluster_page_wal_install_matches_v1(0, &zero));
		UT_ASSERT(!cluster_page_wal_install_matches_v1(2, &zero));
		UT_ASSERT(!cluster_page_wal_install_matches_v1(1, NULL));
	}
}
static void
flush_refusal_never_certifies(void)
{
	for (int i = 0; i < 7; i++) {
		ClusterPageWalBindingV1 out, binding;
		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		binding = current_binding();
		if (i == 0)
			writer.claim.identity.origin_owner_incarnation++;
		if (i == 1)
			writer.claim.claim_sha256[1]++;
		if (i == 2)
			writer.timeline++;
		if (i == 3)
			insert_end = 0x100;
		if (i == 4)
			selected = false;
		if (i == 5)
			flush_changes_source = true;
		if (i == 6) {
			binding.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
			writer.claim.database_incarnation++;
		}
		memset(&out, 0x59, sizeof(out));
		UT_ASSERT(!cluster_page_wal_flush_source_v1(&binding, &out));
		UT_ASSERT_EQ(((uint8 *)&out)[0], 0x59);
		UT_ASSERT_EQ(flush_calls, i == 5 ? 1 : 0);
	}
}

static void
reset_many(void)
{
	reset();
	for (int i = 0; i < lengthof(many_pages); i++) {
		many_pages[i] = page;
		many_descriptors[i] = desc;
		many_descriptors[i].bufferdesc.buf_id = i;
		many_descriptors[i].bufferdesc.tag.blockNum = 7 + i;
		PageSetLSNPreserveOrigin(many_pages[i].data, 0x200);
	}
	NBuffers = lengthof(many_pages);
	BufferDescriptors = many_descriptors;
	BufferBlocks = many_pages[0].data;
	cluster_page_wal_shmem_init();
}

static ClusterPageWalCaptureResultV1
capture_many(int index, uint32 generation)
{
	writer.claim.identity.origin_owner_incarnation = generation;
	memcpy(writer.claim.claim_sha256 + 1, &generation, sizeof(generation));
	return cluster_page_wal_capture_native_v1(index + 1, &edge, 80, 0x120, 0x200, 0x9192,
											  RM_HEAP_ID, 0);
}

static void
shared_claim_and_descriptor_reuse_do_not_alias(void)
{
	ClusterPageWalBindingV1 first, other;
	unsigned before_allocations;
	reset_many();
	before_allocations = allocations;
	/* More buffers than source slots still consume one shared claim. */
	for (int i = 0; i < NBuffers; i++)
		UT_ASSERT_EQ(capture_many(i, 7), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &first));
	UT_ASSERT(cluster_page_wal_snapshot_v1(2, &other));
	UT_ASSERT_EQ(memcmp(&first.source, &other.source, sizeof(first.source)), 0);
	locked = exclusive = false;
	pg_atomic_fetch_or_u32(&many_descriptors[0].bufferdesc.state, BM_LOCKED);
	cluster_page_wal_reset_reuse_locked(&many_descriptors[0].bufferdesc);
	many_descriptors[0].bufferdesc.tag.relNumber++;
	pg_atomic_fetch_and_u32(&many_descriptors[0].bufferdesc.state, ~BM_LOCKED);
	locked = exclusive = true;
	/* Even identical old token/LSN bytes must not revive another tag's source. */
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &first));
	UT_ASSERT(cluster_page_wal_snapshot_v1(2, &other));
	UT_ASSERT_EQ(other.source.claim.identity.origin_owner_incarnation, 7);
	UT_ASSERT_EQ(capture_many(0, 9), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &first));
	UT_ASSERT_EQ(first.source.claim.identity.origin_owner_incarnation, 9);
	UT_ASSERT(cluster_page_wal_snapshot_v1(2, &other));
	UT_ASSERT_EQ(other.source.claim.identity.origin_owner_incarnation, 7);
	UT_ASSERT_EQ(allocations, before_allocations);
}

/* R-A22: forget keeps a first record, which holds its own source reference;
 * end it as its producer would after handing it over. */
static void
drop_first(int index)
{
	BufferDesc *buf = &many_descriptors[index].bufferdesc;
	ClusterPageWalRefV1 first;
	pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(buf, &first),
				 CLUSTER_PAGE_WAL_FIRST_PRESENT);
	UT_ASSERT(cluster_page_wal_first_handover_locked_v1(buf, &first));
	pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
}

static void
bounded_claim_pool_and_t2_reservation_release(void)
{
	ClusterPageWalBindingV1 carrier, observed;
	ClusterPageWalInstallV1 prepared = { 0 };
	unsigned before_allocations;
	reset_many();
	before_allocations = allocations;
	for (int i = 0; i < 256; i++)
		UT_ASSERT_EQ(capture_many(i, i + 1), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT_EQ(capture_many(256, 500), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	UT_ASSERT(!cluster_page_wal_snapshot_v1(257, &observed));
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &carrier));
	carrier.identity.blockno = many_descriptors[257].bufferdesc.tag.blockNum;
	carrier.source = writer;
	carrier.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	UT_ASSERT(!cluster_page_wal_prepare_install_v1(258, &carrier, many_pages[257].data, &prepared));
	UT_ASSERT_EQ(prepared.source_slot, 0);
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	drop_first(0);
	UT_ASSERT(cluster_page_wal_prepare_install_v1(258, &carrier, many_pages[257].data, &prepared));
	/* Preparation pins its source before any page or authority mutation. */
	UT_ASSERT_EQ(capture_many(256, 501), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	cluster_page_wal_release_install_v1(&prepared); /* failed/duplicate T2 */
	UT_ASSERT_EQ(capture_many(256, 501), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_forget_v1(257));
	/* Its first insertion lacked a source. A later attributed insertion
	 * cannot make it transferable; only ending the residency releases it. */
	pg_atomic_fetch_or_u32(&many_descriptors[256].bufferdesc.state, BM_LOCKED);
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(&many_descriptors[256].bufferdesc,
														  &(ClusterPageWalRefV1){ 0 }),
				 CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
	cluster_page_wal_reset_reuse_locked(&many_descriptors[256].bufferdesc);
	pg_atomic_fetch_and_u32(&many_descriptors[256].bufferdesc.state, ~BM_LOCKED);
	UT_ASSERT(cluster_page_wal_prepare_install_v1(258, &carrier, many_pages[257].data, &prepared));
	UT_ASSERT(cluster_page_wal_publish_install_v1(258, &prepared));
	UT_ASSERT_EQ(prepared.source_slot, 0);
	UT_ASSERT(!cluster_page_wal_publish_install_v1(258, &prepared));
	cluster_page_wal_release_install_v1(&prepared);
	UT_ASSERT_EQ(capture_many(256, 501), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(258, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &carrier, sizeof(carrier)), 0);
	UT_ASSERT(cluster_page_wal_forget_v1(258));
	UT_ASSERT_EQ(capture_many(256, 501), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(2, &observed));
	UT_ASSERT_EQ(observed.source.claim.identity.origin_owner_incarnation, 2);
	UT_ASSERT_EQ(allocations, before_allocations);
}

static void
pregrant_owner_releases_preparation_on_all_outcomes(void)
{
	for (pregrant_variant = 0; pregrant_variant < 5; pregrant_variant++) {
		ClusterPcmOwnSnapshot base = { 0 };
		ResourceXCurrentImage image = { 0 };
		ClusterPcmOwnResult result;
		reset_many();
		for (int i = 0; i < 255; i++)
			UT_ASSERT_EQ(capture_many(i, i + 1), CLUSTER_PAGE_WAL_CAPTURED);
		UT_ASSERT(cluster_page_wal_snapshot_v1(1, &image.page_wal));
		image.page_wal.identity.blockno = many_descriptors[257].bufferdesc.tag.blockNum;
		image.page_wal.source.claim.identity.origin_owner_incarnation = 800;
		image.page_wal.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
		image.page_bytes = many_pages[257].data;
		image.image_length = BLCKSZ;
		image.page_lsn = 0x200;
		image.page_scn = 80;
		image.page_checksum = cluster_gcs_block_compute_checksum(image.page_bytes);
		pregrant_snapshots = pregrant_failclosed = 0;
		result = gcs_block_pcm_x_resource_x_install_target_image_exact(
			&many_descriptors[257].bufferdesc, &base, 9, &image);
		UT_ASSERT_EQ(result, pregrant_variant == 0	 ? CLUSTER_PCM_OWN_OK
							 : pregrant_variant == 1 ? CLUSTER_PCM_OWN_STALE
													 : CLUSTER_PCM_OWN_CORRUPT);
		UT_ASSERT_EQ(pregrant_failclosed, pregrant_variant >= 3 ? 1 : 0);
		UT_ASSERT_EQ(capture_many(256, 900), CLUSTER_PAGE_WAL_CAPTURED);
	}
}
/* ---- D S09 R-A22: first own record since the page was last clean ---- */

/* The page moves to version token under a record [start, end); like a
 * native caller, set the page LSN after the capture. */
static bool
capture_at(uint64 token, XLogRecPtr start, XLogRecPtr end)
{
	bool captured;
	((PageHeader)page.data)->pd_block_scn = token;
	edge.before.mutation_token = token - 1;
	captured
		= cluster_page_wal_capture_native_v1(1, &edge, token, start, end, 0x9192, RM_HEAP_ID, 0)
		  == CLUSTER_PAGE_WAL_CAPTURED;
	PageSetLSNPreserveOrigin(page.data, end);
	return captured;
}

static ClusterPageWalDirtyFloorV1
writer_floor(void)
{
	ClusterPageWalDirtyFloorV1 floor;
	UT_ASSERT(cluster_page_wal_dirty_floor_v1(&writer, &floor));
	return floor;
}

static ClusterPageWalFirstResultV1
observe_first(ClusterPageWalRefV1 *out)
{
	ClusterPageWalFirstResultV1 result;
	pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED);
	result = cluster_page_wal_first_observe_locked_v1(&desc.bufferdesc, out);
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
	return result;
}

/* The write owner's clear, with the page made clean by the write. */
static bool
clear_written(const ClusterPageWalRefV1 *observed, uint64 written, uint32 extra_state)
{
	bool cleared;
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~(BM_DIRTY | BM_JUST_DIRTIED));
	pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED | extra_state);
	cleared = cluster_page_wal_first_clear_written_locked_v1(&desc.bufferdesc, observed, written);
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~(BM_LOCKED | extra_state));
	return cleared;
}

/* A successful WAL insertion without source attribution still owes redo. */
UT_TEST(first_unavailable_source_is_an_unattributed_obligation)
{
	for (unsigned vm = 0; vm < 2; vm++) {
		ClusterPageWalRefV1 first;
		reset();
		desc.bufferdesc.tag.forkNum = vm ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM;
		selected = false;
		UT_ASSERT_EQ(
			cluster_page_wal_capture_native_v1(1, &edge, 80, 0x120, 0x200, 0x9192, RM_HEAP_ID, 0),
			CLUSTER_PAGE_WAL_UNATTRIBUTED);
		UT_ASSERT(cluster_page_wal_forget_v1(1)); /* Original native refusal cleanup. */
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
		UT_ASSERT_EQ(writer_floor().dirty, 1);
		UT_ASSERT_EQ(writer_floor().unattributed, 1);
		selected = true;
		UT_ASSERT(capture_at(81, 0x220, 0x300));
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
		UT_ASSERT_EQ(writer_floor().unattributed, 1); /* Later attribution cannot erase it. */
	}
}

UT_TEST(first_full_pool_is_an_unattributed_obligation)
{
	ClusterPageWalRefV1 first;
	BufferDesc *buf;
	reset_many();
	for (int i = 0; i < 256; i++)
		UT_ASSERT_EQ(capture_many(i, i + 1), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT_EQ(capture_many(256, 500), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	buf = &many_descriptors[256].bufferdesc;
	pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(buf, &first),
				 CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
	pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
	UT_ASSERT_EQ(writer_floor().dirty, 257);
	UT_ASSERT_EQ(writer_floor().unattributed, 1);
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	drop_first(0);
	UT_ASSERT_EQ(capture_many(256, 501), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT_EQ(writer_floor().unattributed, 1);
	pg_atomic_fetch_and_u32(&buf->state, ~(BM_DIRTY | BM_JUST_DIRTIED));
	pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
	UT_ASSERT(cluster_page_wal_first_clear_written_locked_v1(buf, &first, 80));
	pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
	UT_ASSERT_EQ(writer_floor().unattributed, 0);
}

UT_TEST(unattributed_first_ends_only_with_its_exact_clean_write)
{
	for (unsigned variant = 0; variant < 9; variant++) {
		ClusterPageWalRefV1 first, supplied;
		uint32 bad_state = variant == 1	  ? BM_DIRTY
						   : variant == 2 ? BM_JUST_DIRTIED
						   : variant == 3 ? BM_IO_IN_PROGRESS
						   : variant == 4 ? BM_IO_ERROR
										  : 0;

		reset();
		selected = false;
		UT_ASSERT_EQ(
			cluster_page_wal_capture_native_v1(1, &edge, 80, 0x120, 0x200, 0x9192, RM_HEAP_ID, 0),
			CLUSTER_PAGE_WAL_UNATTRIBUTED);
		UT_ASSERT(cluster_page_wal_forget_v1(1));
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
		UT_ASSERT_EQ(first.source_flags, 0);
		UT_ASSERT_EQ(first.start, 0x120);
		UT_ASSERT_EQ(first.token, 80);
		supplied = first;
		if (variant == 6)
			supplied.start++;
		if (variant == 7)
			memset(&supplied, 0, sizeof(supplied));
		if (variant == 8) {
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED);
			cluster_page_wal_reset_reuse_locked(&desc.bufferdesc);
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
			edge.result_incarnation[0]++;
			UT_ASSERT_EQ(cluster_page_wal_capture_native_v1(1, &edge, 80, 0x120, 0x200, 0x9192,
															RM_HEAP_ID, 0),
						 CLUSTER_PAGE_WAL_UNATTRIBUTED);
		}
		UT_ASSERT_EQ(clear_written(&supplied, variant == 5 ? 79 : 80, bad_state), variant == 0);
		UT_ASSERT_EQ(writer_floor().unattributed, variant == 0 ? 0 : 1);
		if (variant == 0) {
			UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_ABSENT);
			selected = true;
			UT_ASSERT(capture_at(81, 0x220, 0x300));
			UT_ASSERT_EQ(writer_floor().floor, 0x220);
		}
	}
}

UT_TEST(later_capture_failure_keeps_the_earlier_first_record)
{
	for (unsigned full = 0; full < 2; full++) {
		ClusterPageWalRefV1 before, after;
		ClusterPageWalDirtyFloorV1 floor;
		ClusterWalSourceRef original;
		BufferDesc *buf;
		reset_many();
		UT_ASSERT_EQ(capture_many(0, 1), CLUSTER_PAGE_WAL_CAPTURED);
		original = writer;
		buf = &many_descriptors[0].bufferdesc;
		pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
		UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(buf, &before),
					 CLUSTER_PAGE_WAL_FIRST_PRESENT);
		pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
		if (full)
			for (int i = 1; i < 256; i++)
				UT_ASSERT_EQ(capture_many(i, i + 1), CLUSTER_PAGE_WAL_CAPTURED);
		else
			selected = false;
		writer.claim.identity.origin_owner_incarnation = 999;
		UT_ASSERT_EQ(
			cluster_page_wal_capture_native_v1(1, &edge, 80, 0x220, 0x300, 0x9192, RM_HEAP_ID, 0),
			CLUSTER_PAGE_WAL_UNATTRIBUTED);
		UT_ASSERT(cluster_page_wal_forget_v1(1));
		pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
		UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(buf, &after),
					 CLUSTER_PAGE_WAL_FIRST_PRESENT);
		pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT(cluster_page_wal_dirty_floor_v1(&original, &floor));
		UT_ASSERT_EQ(floor.floor, 0x120);
	}
}

UT_TEST(first_record_write_coverage_uses_scn_total_order)
{
	for (unsigned newer = 0; newer < 2; newer++) {
		ClusterPageWalRefV1 first;
		SCN original = scn_encode(newer ? 1 : 0, 100);
		SCN written = scn_encode(newer ? 0 : 1, newer ? 101 : 99);
		reset();
		UT_ASSERT(capture_at(original, 0x120, 0x200));
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_PRESENT);
		UT_ASSERT_EQ(clear_written(&first, written, 0), newer != 0);
		UT_ASSERT_EQ(writer_floor().dirty, newer ? 0 : 1);
	}
}

/* r1 then r2 without a write: the first record (and the floor) stays r1. */
static void
first_record_is_first_capture_since_clean(void)
{
	ClusterPageWalRefV1 first;
	ClusterPageWalDirtyFloorV1 floor;
	reset();
	floor = writer_floor();
	UT_ASSERT_EQ(floor.dirty, 0);
	UT_ASSERT(capture_at(80, 0x120, 0x200));
	UT_ASSERT(capture_at(81, 0x220, 0x300));
	floor = writer_floor();
	UT_ASSERT_EQ(floor.floor, 0x120);
	UT_ASSERT_EQ(floor.dirty, 1);
	UT_ASSERT_EQ(floor.foreign + floor.unattributed, 0);
	UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_PRESENT);
	UT_ASSERT_EQ(first.token, 80);
	UT_ASSERT_EQ(first.start, 0x120);
	/* Its flush is certified separately, never copied from the latest. */
	UT_ASSERT_EQ(first.source_flags & 0x8000, 0);
	/* Without the header lock nothing is observed. */
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(&desc.bufferdesc, &first),
				 CLUSTER_PAGE_WAL_FIRST_INVALID);
}

/* Only a write that made the page clean, of a version covering the first
 * record, with the slot unchanged, clears it; the next capture sets it. */
static void
first_record_clears_only_after_its_clean_write(void)
{
	for (int variant = 0; variant < 6; variant++) {
		ClusterPageWalRefV1 first, changed;
		reset();
		UT_ASSERT(capture_at(80, 0x120, 0x200));
		UT_ASSERT(capture_at(81, 0x220, 0x300));
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_PRESENT);
		changed = first;
		changed.start++;
		UT_ASSERT_EQ(clear_written(variant == 4 ? &changed : &first, variant == 3 ? 79 : 81,
								   variant == 1	  ? BM_DIRTY
								   : variant == 2 ? BM_JUST_DIRTIED
								   : variant == 5 ? BM_IO_IN_PROGRESS
												  : 0),
					 variant == 0);
		UT_ASSERT_EQ(writer_floor().floor, variant == 0 ? 0 : 0x120);
		if (variant == 0) {
			UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_ABSENT);
			UT_ASSERT(capture_at(82, 0x320, 0x400));
			UT_ASSERT_EQ(writer_floor().floor, 0x320);
		}
		if (ut_current_failed)
			printf("# clear variant %d\n", variant);
	}
}

/* forget and an install keep an unhanded first record; reuse ends it. */
static void
first_record_survives_forget_and_install_not_reuse(void)
{
	ClusterPageWalInstallV1 prepared = { 0 };
	ClusterPageWalBindingV1 zero = { 0 };
	reset();
	UT_ASSERT(capture_at(80, 0x120, 0x200));
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	UT_ASSERT_EQ(writer_floor().floor, 0x120);
	UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &zero, page.data, &prepared));
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &prepared));
	UT_ASSERT_EQ(writer_floor().floor, 0x120);
	pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED);
	cluster_page_wal_reset_reuse_locked(&desc.bufferdesc);
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
	UT_ASSERT_EQ(writer_floor().dirty, 0);
	UT_ASSERT_EQ(observe_first(&(ClusterPageWalRefV1){ 0 }), CLUSTER_PAGE_WAL_FIRST_ABSENT);
}

/* A retained first record outlives the descriptor; handover clears it. */
static void
first_record_retain_and_handover(void)
{
	ClusterPageWalRefV1 retained = { 0 }, observed;
	ClusterPageWalBindingV1 read;
	reset();
	UT_ASSERT(capture_at(80, 0x120, 0x200));
	pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED);
	UT_ASSERT_EQ(cluster_page_wal_first_retain_locked_v1(&desc.bufferdesc, &retained),
				 CLUSTER_PAGE_WAL_FIRST_PRESENT);
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(&desc.bufferdesc, &observed),
				 CLUSTER_PAGE_WAL_FIRST_PRESENT);
	observed.start++;
	UT_ASSERT(!cluster_page_wal_first_handover_locked_v1(&desc.bufferdesc, &observed));
	observed.start--;
	UT_ASSERT(cluster_page_wal_first_handover_locked_v1(&desc.bufferdesc, &observed));
	UT_ASSERT(!cluster_page_wal_first_handover_locked_v1(&desc.bufferdesc, &observed));
	cluster_page_wal_reset_reuse_locked(&desc.bufferdesc);
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
	UT_ASSERT_EQ(writer_floor().dirty, 0);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&retained, space.key.locator, MAIN_FORKNUM, 7, &read));
	UT_ASSERT_EQ(read.record_start, 0x120);
	UT_ASSERT_EQ(read.source.claim.identity.origin_owner_incarnation, 7);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&retained));
}

/* Another writer's first record is foreign to this source's floor. */
static void
first_record_of_another_writer_is_foreign(void)
{
	ClusterWalSourceRef original;
	ClusterPageWalDirtyFloorV1 floor;
	reset();
	original = writer;
	UT_ASSERT(capture_at(80, 0x120, 0x200));
	writer.claim.identity.origin_owner_incarnation++;
	writer.claim.claim_sha256[15]++;
	UT_ASSERT(capture_at(81, 0x220, 0x300));
	floor = writer_floor();
	UT_ASSERT_EQ(floor.floor, 0);
	UT_ASSERT_EQ(floor.foreign, 1);
	UT_ASSERT(cluster_page_wal_dirty_floor_v1(&original, &floor));
	UT_ASSERT_EQ(floor.floor, 0x120);
	UT_ASSERT_EQ(floor.foreign, 0);
	writer = original;
}

static void
pi_snapshot_requires_frozen_header_owner(void)
{
	for (int c = 0; c < 9; c++) {
		ClusterPageWalBindingV1 out, untouched;
		reset();
		UT_ASSERT(capture());
		PageSetLSNPreserveOrigin(page.data, 0x200);
		desc.bufferdesc.buffer_type = BUF_TYPE_PI;
		desc.bufferdesc.pcm_state = PCM_STATE_N;
		pg_atomic_write_u32(&desc.bufferdesc.state, BM_LOCKED | BM_TAG_VALID | BM_PERMANENT);
		locked = exclusive = false;
		memset(&untouched, 0xa5, sizeof(untouched));
		out = untouched;
		if (c == 1)
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
		if (c == 2)
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_VALID);
		if (c == 3)
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_IN_PROGRESS);
		if (c == 4)
			pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_DIRTY);
		if (c == 5)
			desc.bufferdesc.buffer_type = BUF_TYPE_SCUR;
		if (c == 6)
			desc.bufferdesc.pcm_state = PCM_STATE_X;
		if (c == 7)
			((PageHeader)page.data)->pd_block_scn++;
		if (c == 8)
			pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_PERMANENT);
		UT_ASSERT_EQ(cluster_page_wal_pi_snapshot_locked_v1(&desc.bufferdesc, &out), c == 0);
		if (c == 0) {
			UT_ASSERT_EQ(out.version.mutation_token, 80);
			UT_ASSERT_EQ(out.record_end, 0x200);
		} else
			UT_ASSERT_EQ(memcmp(&out, &untouched, sizeof(out)), 0);
	}
}

static void
eviction_snapshot_requires_exact_clean_revoke(void)
{
	for (int pins = 0; pins <= 1; pins++) {
		for (int variant = 0; variant < 10; variant++) {
			ClusterPcmOwnSnapshot fence = { 0 };
			ClusterPageWalBindingV1 out, untouched;
			reset();
			UT_ASSERT(capture());
			PageSetLSNPreserveOrigin(page.data, 0x200);
			memset(&own_entry, 0, sizeof(own_entry));
			pg_atomic_init_u64(&own_entry.generation, 7);
			pg_atomic_init_u64(&own_entry.reservation_token, 9);
			pg_atomic_init_u32(&own_entry.flags, PCM_OWN_FLAG_REVOKING);
			desc.bufferdesc.pcm_state = PCM_STATE_X;
			desc.bufferdesc.buffer_type = BUF_TYPE_XCUR;
			pg_atomic_write_u32(&desc.bufferdesc.state, BM_LOCKED | BM_VALID | BM_TAG_VALID
															| BM_PERMANENT
															| pins * BUF_REFCOUNT_ONE);
			fence.tag = desc.bufferdesc.tag;
			fence.generation = 7;
			fence.reservation_token = 9;
			fence.flags = PCM_OWN_FLAG_REVOKING;
			fence.pcm_state = PCM_STATE_X;
			fence.buffer_type = BUF_TYPE_XCUR;
			if (variant == 1)
				pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_DIRTY);
			if (variant == 2)
				pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_IO_IN_PROGRESS);
			if (variant == 3)
				pg_atomic_fetch_add_u32(&desc.bufferdesc.state, BUF_REFCOUNT_ONE);
			if (variant == 4)
				pg_atomic_write_u64(&own_entry.generation, 8);
			if (variant == 5)
				pg_atomic_write_u64(&own_entry.writer_activation_token, 1);
			if (variant == 6)
				fence.tag.blockNum++;
			if (variant == 7)
				((PageHeader)page.data)->pd_block_scn++;
			if (variant == 8)
				pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
			if (variant == 9)
				cluster_page_wal_reset_reuse_locked(&desc.bufferdesc);
			memset(&untouched, 0xa5, sizeof(untouched));
			out = untouched;
			UT_ASSERT_EQ(
				cluster_page_wal_eviction_snapshot_locked_v1(&desc.bufferdesc, &fence, pins, &out),
				variant == 0   ? CLUSTER_PAGE_WAL_CAPTURED
				: variant == 9 ? CLUSTER_PAGE_WAL_UNATTRIBUTED
							   : CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
			if (variant == 0) {
				UT_ASSERT_EQ(out.record_end, 0x200);
				UT_ASSERT_EQ(out.version.mutation_token, 80);
			} else
				UT_ASSERT_EQ(memcmp(&out, &untouched, sizeof(out)), 0);
		}
	}
}

static void
detached_reference_survives_descriptor_reuse(void)
{
	ClusterPageWalRefV1 ref = { 0 }, zero = { 0 };
	ClusterPageWalBindingV1 binding, out;
	BufferTag original;
	reset();
	UT_ASSERT(capture());
	PageSetLSNPreserveOrigin(page.data, 0x200);
	binding = current_binding();
	original = desc.bufferdesc.tag;
	UT_ASSERT_EQ(sizeof(ref), 48);
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &ref));
	UT_ASSERT(!cluster_page_wal_ref_retain_v1(&binding, &ref));
	pg_atomic_fetch_or_u32(&desc.bufferdesc.state, BM_LOCKED);
	cluster_page_wal_reset_reuse_locked(&desc.bufferdesc);
	pg_atomic_fetch_and_u32(&desc.bufferdesc.state, ~BM_LOCKED);
	desc.bufferdesc.tag.blockNum++;
	writer.claim.identity.origin_owner_incarnation++;
	writer.claim.claim_sha256[15]++;
	UT_ASSERT(capture());
	UT_ASSERT(cluster_page_wal_ref_read_v1(&ref, BufTagGetRelFileLocator(&original),
										   original.forkNum, original.blockNum, &out));
	UT_ASSERT(cluster_page_wal_same_mutation_v1(&binding, &out));
	UT_ASSERT_EQ(out.flags, 0); /* retaining never certifies a native flush */
	UT_ASSERT(cluster_page_wal_ref_release_v1(&ref));
	UT_ASSERT_EQ(memcmp(&ref, &zero, sizeof(ref)), 0);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&ref));
	UT_ASSERT(!cluster_page_wal_ref_read_v1(
		&ref, binding.identity.locator, binding.identity.forknum, binding.identity.blockno, &out));
	UT_ASSERT_EQ(current_binding().source.claim.identity.origin_owner_incarnation, 8);
}

static void
detached_reference_rejects_invalid_and_full_pool_without_changes(void)
{
	ClusterPageWalRefV1 ref = { 0 }, zero = { 0 };
	ClusterPageWalBindingV1 original, bad, out, untouched;
	reset_many();
	for (int i = 0; i < 256; i++)
		UT_ASSERT_EQ(capture_many(i, i + 1), CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &original));
	bad = original;
	bad.source.claim.identity.origin_owner_incarnation = 999;
	UT_ASSERT(!cluster_page_wal_ref_retain_v1(&bad, &ref));
	UT_ASSERT_EQ(memcmp(&ref, &zero, sizeof(ref)), 0);
	bad = original;
	bad.record_end = bad.record_start;
	UT_ASSERT(!cluster_page_wal_ref_retain_v1(&bad, &ref));
	UT_ASSERT_EQ(memcmp(&ref, &zero, sizeof(ref)), 0);
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&original, &ref));
	pg_atomic_fetch_or_u32(&many_descriptors[0].bufferdesc.state, BM_LOCKED);
	cluster_page_wal_reset_reuse_locked(&many_descriptors[0].bufferdesc);
	pg_atomic_fetch_and_u32(&many_descriptors[0].bufferdesc.state, ~BM_LOCKED);
	UT_ASSERT_EQ(capture_many(257, 999), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	memset(&untouched, 0xa5, sizeof(untouched));
	out = untouched;
	UT_ASSERT(!cluster_page_wal_ref_read_v1(&ref, original.identity.locator, FSM_FORKNUM,
											original.identity.blockno, &out));
	UT_ASSERT_EQ(memcmp(&out, &untouched, sizeof(out)), 0);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&ref));
	UT_ASSERT_EQ(capture_many(257, 999), CLUSTER_PAGE_WAL_CAPTURED);
}

static void
cold_failure_latch_survives_process_attach_and_local_cleanup(void)
{
	void *old;
	pid_t child;
	int status;
	reset();
	UT_ASSERT(cluster_page_wal_cold_redo_write_allowed_v1());
	old = shared_memory;
	shared_memory = mmap(NULL, shared_bytes, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
	UT_ASSERT(shared_memory != MAP_FAILED);
	if (shared_memory == MAP_FAILED)
		abort();
	memcpy(shared_memory, old, shared_bytes);
	free(old);
	attach_existing = true;
	cluster_page_wal_shmem_init();
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		cluster_page_wal_shmem_init();
		cluster_page_wal_cold_redo_fail_v1();
		_exit(cluster_page_wal_cold_redo_write_allowed_v1() ? 1 : 0);
	}
	if (child < 0)
		abort();
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	UT_ASSERT(!cluster_page_wal_cold_redo_write_allowed_v1());
	cluster_page_wal_shmem_init(); /* A new attach must not clear the failure. */
	UT_ASSERT(!cluster_page_wal_cold_redo_write_allowed_v1());
	munmap(shared_memory, shared_bytes);
	shared_memory = NULL;
	attach_existing = false;
	reset(); /* Only a newly created shared-memory region can clear it. */
	UT_ASSERT(cluster_page_wal_cold_redo_write_allowed_v1());
}

static void
space_insert(unsigned block, RmgrId rmid, uint8 info, bool tombstone)
{
	ClusterSpaceReservation reservation = { 0 };
	reset();
	page_version_edge_registered = false;
	max_registered_block_id = 0;
	InitBufferTag(&desc.bufferdesc.tag, &space.key.locator, SPACE_FORKNUM, block);
	if (tombstone)
		space.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	reservation.identity = space;
	reservation.next_block = 16;
	if (block == 0)
		UT_ASSERT(cluster_space_identity_page_encode(&space, 82, page.data, BLCKSZ));
	else
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 82, page.data, BLCKSZ));
	retry_insert = true;
	UT_ASSERT_EQ(XLogInsert(rmid, info), 0x200);
	PageSetLSNPreserveOrigin(page.data, 0x200);
	UT_ASSERT(PageSetLSNOrigin(page.data, 0));
}

UT_TEST(space_native_record_and_both_component_sources)
{
	for (unsigned block = 0; block < 2; block++) {
		for (unsigned action = 0; action < 2; action++) {
			ClusterPageWalBindingV1 value;
			ClusterPageWalRefV1 retained = { 0 };
			space_insert(block, action ? RM_XACT_ID : RM_SMGR_ID,
						 action ? XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO
								: XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE,
						 action != 0);
			UT_ASSERT_EQ(assemble_calls, 2);
			UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
						 CLUSTER_PAGE_WAL_CAPTURED);
			UT_ASSERT(cluster_page_wal_snapshot_v1(1, &value));
			UT_ASSERT_EQ(value.identity.forknum, SPACE_FORKNUM);
			UT_ASSERT_EQ(value.identity.blockno, block);
			UT_ASSERT_EQ(value.record_start, 0x120);
			UT_ASSERT_EQ(value.record_end, 0x200);
			UT_ASSERT_EQ(value.record_crc, 0x9192);
			UT_ASSERT_EQ(value.rmid, action ? RM_XACT_ID : RM_SMGR_ID);
			UT_ASSERT_EQ(value.version.mutation_token, 82);
			UT_ASSERT_EQ(value.flags, 0); /* Insertion is not a flush. */
			UT_ASSERT(memcmp(value.version.segment_incarnation, space.incarnation, 16) == 0);
			UT_ASSERT(cluster_page_wal_ref_retain_v1(&value, &retained));
			UT_ASSERT(cluster_page_wal_forget_v1(1));
			UT_ASSERT(cluster_page_wal_ref_read_v1(&retained, space.key.locator, SPACE_FORKNUM,
												   block, &value));
			UT_ASSERT(cluster_page_wal_ref_release_v1(&retained));
		}
	}
}

UT_TEST(space_advance_is_only_block_one_of_live_identity)
{
	for (unsigned block = 0; block < 2; block++) {
		space_insert(block, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, false);
		UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
					 block ? CLUSTER_PAGE_WAL_CAPTURED : CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
	}
	space_insert(1, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, true);
	UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
				 CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
}

UT_TEST(native_last_record_expires_on_construction_reset_and_other_insert)
{
	XLogRecord value, before;
	XLogRecPtr start = 123;
	space_insert(0, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, false);
	UT_ASSERT(XLogGetLastInsertRecord(0x200, &start, &value));
	UT_ASSERT_EQ(start, 0x120);
	UT_ASSERT_EQ(value.xl_crc, 0x9192);
	before = value;
	start = 123;
	UT_ASSERT(!XLogGetLastInsertRecord(0x201, &start, &value));
	UT_ASSERT_EQ(start, 123);
	UT_ASSERT(memcmp(&before, &value, sizeof(value)) == 0);
	begininsert_called = true;
	UT_ASSERT(!XLogGetLastInsertRecord(0x200, &start, &value));
	XLogResetInsertion();
	UT_ASSERT(!XLogGetLastInsertRecord(0x200, &start, &value));
	space_insert(0, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, false);
	ProcLastRecPtr = XactLastRecEnd = 0x300;
	UT_ASSERT(!XLogGetLastInsertRecord(0x200, &start, &value));
}

UT_TEST(private_record_resident_publication_uses_native_last_insert)
{
	for (unsigned unavailable = 0; unavailable < 2; unavailable++) {
		ClusterPageWalRefV1 first;
		ClusterPageWalBindingV1 latest = { 0 };
		reset();
		memcpy(private_page.data, page.data, BLCKSZ);
		XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data,
						  REGBUF_STANDARD);
		UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_ABSENT);
		memcpy(page.data, private_page.data, BLCKSZ);
		PageSetLSNPreserveOrigin(page.data, 0x200);
		selected = !unavailable;
		UT_ASSERT_EQ(cluster_page_wal_capture_published_v1(1, &edge, 80, 0x200),
					 unavailable ? CLUSTER_PAGE_WAL_UNATTRIBUTED : CLUSTER_PAGE_WAL_CAPTURED);
		if (unavailable)
			UT_ASSERT(cluster_page_wal_forget_v1(1));
		UT_ASSERT_EQ(observe_first(&first), unavailable ? CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED
														: CLUSTER_PAGE_WAL_FIRST_PRESENT);
		UT_ASSERT_EQ(first.start, 0x120);
		UT_ASSERT_EQ(first.end, 0x200);
		UT_ASSERT_EQ(first.crc, 0x9192);
		UT_ASSERT_EQ(cluster_page_wal_snapshot_v1(1, &latest), !unavailable);
		UT_ASSERT_EQ(allocations, 1); /* no allocation in publication */
		UT_ASSERT_EQ(flush_calls, 0); /* attribution is not durability */
	}
}

UT_TEST(private_record_publication_rejects_expired_record_or_changed_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterPageWalRefV1 first;
		reset();
		memcpy(private_page.data, page.data, BLCKSZ);
		XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data,
						  REGBUF_STANDARD);
		UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
		PageSetLSNPreserveOrigin(page.data, 0x200);
		switch (fault) {
		case 0:
			begininsert_called = true;
			break;
		case 1:
			XLogResetInsertion();
			break;
		case 2:
			ProcLastRecPtr = XactLastRecEnd = 0x300;
			break;
		case 3:
			PageSetLSNPreserveOrigin(page.data, 0x199);
			break;
		case 4:
			((PageHeader)page.data)->pd_block_scn++;
			break;
		case 5:
			exclusive = false;
			break;
		case 6:
			permitted = false;
			break;
		case 7:
			break; /* caller supplies a different record end */
		}
		UT_ASSERT_EQ(
			cluster_page_wal_capture_published_v1(1, &edge, 80, fault == 7 ? 0x201 : 0x200),
			CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
		UT_ASSERT_EQ(observe_first(&first), CLUSTER_PAGE_WAL_FIRST_ABSENT);
		UT_ASSERT_EQ(allocations, 1);
		UT_ASSERT_EQ(flush_calls, 0);
	}
}

UT_TEST(space_capture_rejects_wrong_record_key_page_and_owner)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterSpaceIdentityKey key;
		space_insert(0, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, false);
		key = space.key;
		switch (fault) {
		case 0:
			record.xl_rmid = RM_XLOG_ID;
			break;
		case 1:
			desc.bufferdesc.tag.blockNum = 2;
			break;
		case 2:
			key.locator.spcOid++;
			break;
		case 3:
			writer.claim.database_incarnation++;
			break;
		case 4:
			writer.claim.identity.storage_uuid[1]++;
			break;
		case 5:
			permitted = false;
			break;
		case 6:
			exclusive = false;
			break;
		case 7:
			PageSetLSNPreserveOrigin(page.data, 0x199);
			break;
		case 8:
			((PageHeader)page.data)->pd_block_scn = 0;
			break;
		case 9:
			UT_ASSERT(PageSetLSNOrigin(page.data, 1));
			break;
		}
		UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &key, 0x200),
					 CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
	}
	space_insert(0, RM_XACT_ID, XLOG_XACT_COMMIT, false);
	UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
				 CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
	space_insert(0, RM_XACT_ID, XLOG_XACT_ABORT, true);
	UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
				 CLUSTER_PAGE_WAL_INVARIANT_BROKEN);
}

UT_TEST(space_carrier_retains_flushed_original_source_after_transfer)
{
	ClusterPageWalBindingV1 native, received;
	ClusterPageWalInstallV1 install = { 0 };
	space_insert(1, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, false);
	UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
				 CLUSTER_PAGE_WAL_CAPTURED);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &native));
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_thread_id = 2;
	UT_ASSERT(!cluster_page_wal_prepare_install_v1(1, &native, page.data, &install));
	native.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &native, page.data, &install));
	UT_ASSERT(cluster_page_wal_publish_install_v1(1, &install));
	UT_ASSERT_EQ(install.source_slot, 0);
	UT_ASSERT(cluster_page_wal_snapshot_v1(1, &received));
	UT_ASSERT(memcmp(&native, &received, sizeof(native)) == 0);
	UT_ASSERT_EQ(flush_calls, 0); /* Receiver never flushes foreign numeric LSN. */
}

UT_TEST(space_capture_unavailable_source_is_not_mutation_failure)
{
	ClusterPageWalBindingV1 value;
	space_insert(1, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, false);
	selected = false;
	UT_ASSERT_EQ(cluster_page_wal_capture_space_v1(1, &space.key, 0x200),
				 CLUSTER_PAGE_WAL_UNATTRIBUTED);
	UT_ASSERT(cluster_page_wal_forget_v1(1));
	UT_ASSERT(!cluster_page_wal_snapshot_v1(1, &value));
	UT_ASSERT_EQ(writer_floor().dirty, 1);
	UT_ASSERT_EQ(writer_floor().unattributed, 1);
}

int
main(void)
{
	UT_PLAN(44);
	UT_RUN(installed_carrier_matches_explicit_absence_not_snapshot_failure);
	UT_RUN(output_snapshot_retains_exact_failed_output_binding);
	UT_RUN(failed_output_distinguishes_empty_latest_from_invalid_binding);
	UT_RUN(private_record_resident_publication_uses_native_last_insert);
	UT_RUN(private_record_publication_rejects_expired_record_or_changed_owner);
	UT_RUN(space_native_record_and_both_component_sources);
	UT_RUN(space_advance_is_only_block_one_of_live_identity);
	UT_RUN(native_last_record_expires_on_construction_reset_and_other_insert);
	UT_RUN(space_capture_rejects_wrong_record_key_page_and_owner);
	UT_RUN(space_capture_unavailable_source_is_not_mutation_failure);
	UT_RUN(space_carrier_retains_flushed_original_source_after_transfer);
	UT_RUN(resident_binding_memory_budget);
	printf("# Complete private/wire carrier: %zu bytes\n", sizeof(ClusterPageWalBindingV1));
	cluster_page_wal_shmem_register();
	UT_ASSERT(registered_region != NULL);
	UT_RUN(native_insert_source);
	UT_RUN(exact_generation_survives_new_writer);
	UT_RUN(native_capture_refusal_clears_old_binding);
	UT_RUN(native_capture_requires_clear_owner);
	UT_RUN(native_capture_invariant_failure_is_not_attribution_loss);
	UT_RUN(stale_page_or_space_never_reads);
	UT_RUN(capture_requires_original_owner);
	UT_RUN(private_page_and_nonshared_skip);
	UT_RUN(global_catalog_identity);
	UT_RUN(carrier_install_keeps_original_generation);
	UT_RUN(carrier_preflight_refuses_without_mutation);
	UT_RUN(unattributed_carrier_clears_old_source);
	UT_RUN(flush_certification_is_source_exact);
	UT_RUN(flush_refusal_never_certifies);
	UT_RUN(shared_claim_and_descriptor_reuse_do_not_alias);
	UT_RUN(bounded_claim_pool_and_t2_reservation_release);
	UT_RUN(pregrant_owner_releases_preparation_on_all_outcomes);
	UT_RUN(first_record_is_first_capture_since_clean);
	UT_RUN(first_record_clears_only_after_its_clean_write);
	UT_RUN(first_record_survives_forget_and_install_not_reuse);
	UT_RUN(first_record_retain_and_handover);
	UT_RUN(first_record_of_another_writer_is_foreign);
	UT_RUN(first_unavailable_source_is_an_unattributed_obligation);
	UT_RUN(first_full_pool_is_an_unattributed_obligation);
	UT_RUN(unattributed_first_ends_only_with_its_exact_clean_write);
	UT_RUN(later_capture_failure_keeps_the_earlier_first_record);
	UT_RUN(first_record_write_coverage_uses_scn_total_order);
	UT_RUN(pi_snapshot_requires_frozen_header_owner);
	UT_RUN(eviction_snapshot_requires_exact_clean_revoke);
	UT_RUN(detached_reference_survives_descriptor_reuse);
	UT_RUN(detached_reference_rejects_invalid_and_full_pool_without_changes);
	UT_RUN(cold_failure_latch_survives_process_attach_and_local_cleanup);
	free(shared_memory);
	UT_DONE();
	return ut_failed_count != 0;
}
