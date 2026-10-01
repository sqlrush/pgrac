/* Actual native insertion/registration/reset and resident source binding.
 * WAL allocation, shared-memory allocation, PCM and content locks are fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_anchor_cache.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static BufferDescPadded desc;
BufferDescPadded *BufferDescriptors = &desc;
static PGAlignedBlock page, private_page;
char *BufferBlocks = page.data;
Block *LocalBufferBlockPointers;
int NBuffers = 1, NLocBuffer, cluster_node_id = 0;
bool cluster_enabled = true, cluster_shared_config = true;
static bool locked, exclusive, permitted, selected;
static ClusterSpaceIdentity space;
static ClusterWalSourceRef writer;
static ClusterPageWalBindingV1 shared_binding;
static const ClusterShmemRegion *registered_region;
static RfPageVersionEdgeEntryV1 edge;
static unsigned allocations, insert_calls, assemble_calls;
static bool retry_insert;
static bool consistency_check;
XLogRecPtr ProcLastRecPtr;
ProcessingMode Mode = NormalProcessing;

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
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT_EQ(size, sizeof(shared_binding));
	allocations++;
	*found = false;
	return &shared_binding;
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
	return 0x200;
}
#undef elog
#define elog(level, ...) abort()
#include "test_cluster_page_wal_insert.inc"

static void
reset(void)
{
	PageHeader h = (PageHeader)page.data;
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
	return cluster_page_wal_capture_native_v1(1, &edge, 80, 0x120, 0x200, 0x9192, RM_HEAP_ID, 0);
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
capture_requires_original_owner(void)
{
	for (int i = 0; i < 6; i++) {
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
		UT_ASSERT_EQ(shared_binding.record_end, 0);
	}
}
static void
private_page_and_nonshared_skip(void)
{
	reset();
	memcpy(private_page.data, page.data, BLCKSZ);
	XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(shared_binding.record_end, 0);
	reset();
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(shared_binding.version.mutation_token, 80);
	/* Reuse the same native registration slot for a private build page. */
	begininsert_called = page_version_edge_registered = true;
	registered_page_version_result_token = 19;
	registered_page_version_entry_count = 1;
	registered_page_version_entries[0] = edge;
	((PageHeader)private_page.data)->pd_block_scn = 19;
	XLogRegisterBlock(0, &space.key.locator, MAIN_FORKNUM, 7, private_page.data, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(shared_binding.version.mutation_token, 80);
	reset();
	cluster_shared_config = false;
	XLogRegisterBuffer(0, 1, REGBUF_STANDARD);
	UT_ASSERT_EQ(XLogInsert(RM_HEAP_ID, 0), 0x200);
	UT_ASSERT_EQ(shared_binding.record_end, 0);
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
int
main(void)
{
	UT_PLAN(6);
	printf("# Native WAL binding: %zu bytes per buffer\n", sizeof(ClusterPageWalBindingV1));
	cluster_page_wal_shmem_register();
	UT_ASSERT(registered_region != NULL);
	UT_RUN(native_insert_source);
	UT_RUN(exact_generation_survives_new_writer);
	UT_RUN(stale_page_or_space_never_reads);
	UT_RUN(capture_requires_original_owner);
	UT_RUN(private_page_and_nonshared_skip);
	UT_RUN(global_catalog_identity);
	UT_DONE();
	return ut_failed_count != 0;
}
