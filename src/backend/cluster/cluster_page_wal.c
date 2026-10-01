/* Exact native WAL source binding, protected by the original content lock.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "access/xlog.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"

static ClusterPageWalBindingV1 *bindings;

static bool
page_wal_same_source(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return memcmp(&a->claim.identity, &b->claim.identity, sizeof(a->claim.identity)) == 0
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && a->claim.max_config_generation == b->claim.max_config_generation
		   && memcmp(a->claim.claim_sha256, b->claim.claim_sha256, 32) == 0
		   && a->timeline == b->timeline;
}

bool
cluster_page_wal_flush_source_v1(const ClusterPageWalBindingV1 *binding,
								 ClusterPageWalBindingV1 *certified)
{
	ClusterWalSourceRef current, after;
	ClusterPageWalBindingV1 result;
	if (certified == NULL || !cluster_enabled || !cluster_shared_config || RecoveryInProgress()
		|| !cluster_page_wal_binding_shape_v1(binding)
		|| !rf_page_identity_valid_v1(&binding->identity)
		|| !rf_page_version_present_v1(&binding->version)
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| current.claim.identity.system_identifier != binding->identity.system_identifier
		|| current.claim.database_incarnation != binding->source.claim.database_incarnation
		|| memcmp(current.claim.identity.storage_uuid, binding->identity.storage_uuid, 16) != 0)
		return false;
	result = *binding;
	if ((binding->flags & CLUSTER_PAGE_WAL_NATIVE_FLUSHED) == 0) {
		if (!page_wal_same_source(&binding->source, &current)
			|| binding->record_end > GetXLogInsertRecPtr())
			return false;
		XLogFlush(binding->record_end);
		if (!cluster_wal_thread_current_v2_ref(&after) || !page_wal_same_source(&current, &after))
			return false;
		result.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	}
	*certified = result;
	return true;
}

bool
cluster_page_wal_same_mutation_v1(const ClusterPageWalBindingV1 *a,
								  const ClusterPageWalBindingV1 *b)
{
	ClusterPageWalBindingV1 left, right;
	if (a == NULL || b == NULL)
		return false;
	left = *a;
	right = *b;
	left.flags = right.flags = 0;
	return memcmp(&left, &right, sizeof(left)) == 0;
}

bool
cluster_page_wal_snapshot_v1(Buffer buffer, ClusterPageWalBindingV1 *out)
{
	BufferDesc *buf;
	uint32 state;
	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || out == NULL)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0
		|| !cluster_page_wal_binding_matches_v1(
			&bindings[buffer - 1], BufTagGetRelFileLocator(&buf->tag), buf->tag.forkNum,
			buf->tag.blockNum, BufferGetPage(buffer)))
		return false;
	*out = bindings[buffer - 1];
	return true;
}
bool
cluster_page_wal_prepare_install_v1(Buffer buffer, const ClusterPageWalBindingV1 *carrier,
									Page image, ClusterPageWalBindingV1 *prepared)
{
	static const ClusterPageWalBindingV1 zero = { 0 };
	ClusterWalSourceRef current;
	BufferDesc *buf;
	uint32 state;
	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || carrier == NULL || image == NULL
		|| prepared == NULL || !cluster_enabled || !cluster_shared_config)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID)) != (BM_VALID | BM_TAG_VALID)
		|| (state & (BM_IO_ERROR | BM_IO_IN_PROGRESS)) != 0)
		return false;
	if (memcmp(carrier, &zero, sizeof(zero)) != 0) {
		if ((state & BM_PERMANENT) == 0
			|| !cluster_page_wal_binding_matches_v1(carrier, BufTagGetRelFileLocator(&buf->tag),
													buf->tag.forkNum, buf->tag.blockNum, image)
			|| !cluster_wal_thread_current_v2_ref(&current)
			|| current.claim.identity.system_identifier != carrier->identity.system_identifier
			|| memcmp(current.claim.identity.storage_uuid, carrier->identity.storage_uuid, 16) != 0
			|| current.claim.database_incarnation != carrier->source.claim.database_incarnation)
			return false;
	}
	*prepared = *carrier;
	return true;
}
bool
cluster_page_wal_publish_install_v1(Buffer buffer, const ClusterPageWalBindingV1 *prepared)
{
	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || prepared == NULL
		|| !LWLockHeldByMeInMode(BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)),
								 LW_EXCLUSIVE))
		return false;
	bindings[buffer - 1] = *prepared;
	return true;
}

Size
cluster_page_wal_shmem_size(void)
{
	return mul_size((Size)NBuffers, sizeof(ClusterPageWalBindingV1));
}

void
cluster_page_wal_shmem_init(void)
{
	bool found;
	bindings = ShmemInitStruct("pgrac page WAL binding", cluster_page_wal_shmem_size(), &found);
	if (!found)
		memset(bindings, 0, cluster_page_wal_shmem_size());
}

static const ClusterShmemRegion page_wal_region = {
	.name = "pgrac page WAL binding",
	.size_fn = cluster_page_wal_shmem_size,
	.init_fn = cluster_page_wal_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_page_wal",
	.reserved_flags = 0,
};

void
cluster_page_wal_shmem_register(void)
{
	cluster_shmem_register_region(&page_wal_region);
}

bool
cluster_page_wal_capture_native_v1(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
								   uint64 result_token, XLogRecPtr start, XLogRecPtr end,
								   uint32 crc, uint8 rmid, uint8 info)
{
	ClusterPageWalBindingV1 value = { 0 };
	BufferDesc *buf;
	Page page;
	uint32 state;

	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || edge == NULL || !cluster_enabled
		|| !cluster_shared_config || RecoveryInProgress() || result_token == 0
		|| XLogRecPtrIsInvalid(start) || start >= end || edge->page_class != RF_PAGE_CLASS_ORDINARY
		|| edge->result_kind != RF_PAGE_STATE_PRESENT)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)
		|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(buf))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0
		|| (buf->tag.forkNum != MAIN_FORKNUM && buf->tag.forkNum != VISIBILITYMAP_FORKNUM)
		|| cluster_smgr_which_for(BufTagGetRelFileLocator(&buf->tag), InvalidBackendId) != 1
		|| !cluster_wal_thread_current_v2_ref(&value.source)
		|| value.source.claim.database_incarnation == 0 || value.source.timeline == 0
		|| value.source.claim.identity.origin_thread_id == 0
		|| value.source.claim.identity.origin_thread_id > PGRAC_PAGE_LSN_ORIGIN_MAX + 1)
		return false;
	page = BufferGetPage(buffer);
	value.identity.system_identifier = value.source.claim.identity.system_identifier;
	memcpy(value.identity.storage_uuid, value.source.claim.identity.storage_uuid, 16);
	value.identity.locator = BufTagGetRelFileLocator(&buf->tag);
	value.identity.forknum = buf->tag.forkNum;
	value.identity.blockno = buf->tag.blockNum;
	memcpy(value.version.segment_incarnation, edge->result_incarnation, 16);
	value.version.mutation_token = result_token;
	if (!rf_page_identity_valid_v1(&value.identity) || !rf_page_version_present_v1(&value.version)
		|| PageIsNew(page) || ((PageHeader)page)->pd_block_scn != result_token)
		return false;
	value.record_start = start;
	value.record_end = end;
	value.record_crc = crc;
	value.rmid = rmid;
	value.info = info;
	/* This exact successful record owns the new version. Native callers set
	 * PageLSN after XLogInsert returns, still under this content lock. */
	bindings[buffer - 1] = value;
	return true;
}

bool
cluster_page_wal_read_v1(Buffer buffer, const ClusterSpaceIdentity *identity,
						 ClusterPageWalBindingV1 *out)
{
	const ClusterPageWalBindingV1 *value;
	BufferDesc *buf;
	Page page;
	int origin;
	uint32 state;

	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || identity == NULL || out == NULL
		|| identity->state != CLUSTER_SPACE_IDENTITY_LIVE)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0)
		return false;
	value = &bindings[buffer - 1];
	page = BufferGetPage(buffer);
	if (value->record_start == InvalidXLogRecPtr || value->record_start >= value->record_end
		|| (value->flags & ~CLUSTER_PAGE_WAL_NATIVE_FLUSHED) != 0
		|| !rf_page_identity_valid_v1(&value->identity)
		|| !rf_page_version_present_v1(&value->version)
		|| !RelFileLocatorEquals(value->identity.locator, BufTagGetRelFileLocator(&buf->tag))
		|| value->identity.forknum != buf->tag.forkNum
		|| value->identity.blockno != buf->tag.blockNum
		|| value->identity.system_identifier != identity->key.system_identifier
		|| value->source.claim.database_incarnation != identity->key.database_incarnation
		|| memcmp(value->identity.storage_uuid, identity->key.storage_uuid, 16) != 0
		|| !RelFileLocatorEquals(value->identity.locator, identity->key.locator)
		|| memcmp(value->version.segment_incarnation, identity->incarnation, 16) != 0
		|| PageIsNew(page) || ((PageHeader)page)->pd_block_scn != value->version.mutation_token
		|| PageGetLSN(page) != value->record_end || !PageGetLSNOrigin(page, &origin)
		|| origin != value->source.claim.identity.origin_thread_id - 1)
		return false;
	*out = *value;
	return true;
}
