/* Exact native WAL source binding, protected by the original content lock.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_space_reservation.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#define PAGE_WAL_SOURCE_SLOTS 256
#define PAGE_WAL_SOURCE_MASK UINT16_C(0x7fff)
#define PAGE_WAL_SLOT_FLUSHED UINT16_C(0x8000)

typedef ClusterPageWalRefV1 PageWalSlot;
StaticAssertDecl(sizeof(PageWalSlot) == 48, "page WAL resident slot budget");

typedef struct PageWalSource {
	ClusterWalSourceRef source;
	uint32 references;
} PageWalSource;

/*
 * Per buffer: the latest binding (slots), the first own capture since the
 * page was last clean (firsts) and that capture's start LSN (first_lsn).  The
 * first slot and its LSN are written only under content-X (capture) or under
 * the header lock with content SHARE after a write made the page clean
 * (clear); first_lsn is published after its slot and cleared before it, so a
 * lock-free reader can attribute it by re-reading the LSN.
 */
typedef struct PageWalShared {
	slock_t source_lock;
	pg_atomic_uint32 cold_redo_failed;
	PageWalSource sources[PAGE_WAL_SOURCE_SLOTS];
	PageWalSlot slots[FLEXIBLE_ARRAY_MEMBER];
	/* PageWalSlot firsts[NBuffers]; pg_atomic_uint64 first_lsn[NBuffers]; */
} PageWalShared;

static PageWalShared *page_wal_shared;
#define bindings (page_wal_shared == NULL ? NULL : page_wal_shared->slots)
#define firsts (page_wal_shared->slots + NBuffers)
#define first_lsns ((pg_atomic_uint64 *)(firsts + NBuffers))

static bool
page_wal_same_source(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return memcmp(&a->claim.identity, &b->claim.identity, sizeof(a->claim.identity)) == 0
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && a->claim.max_config_generation == b->claim.max_config_generation
		   && memcmp(a->claim.claim_sha256, b->claim.claim_sha256, 32) == 0
		   && a->timeline == b->timeline;
}

/* Only this bounded local pool lock is taken under the original content or
 * descriptor reuse lock. Never allocate, do I/O, or acquire a buffer/GCS lock
 * while holding it. A slot/T2 reference keeps its full source immutable. */
static uint16
page_wal_source_acquire(const ClusterWalSourceRef *source)
{
	uint16 chosen = 0, unused = 0;
	SpinLockAcquire(&page_wal_shared->source_lock);
	for (uint16 i = 1; i <= PAGE_WAL_SOURCE_SLOTS; i++) {
		PageWalSource *entry = &page_wal_shared->sources[i - 1];
		if (entry->references == 0) {
			if (unused == 0)
				unused = i;
		} else if (page_wal_same_source(source, &entry->source)) {
			if (entry->references != UINT32_MAX) {
				entry->references++;
				chosen = i;
			}
			goto done;
		}
	}
	if (unused != 0) {
		PageWalSource *entry = &page_wal_shared->sources[unused - 1];
		entry->source = *source;
		entry->references = 1;
		chosen = unused;
	}
done:
	SpinLockRelease(&page_wal_shared->source_lock);
	return chosen;
}

static bool
page_wal_source_release(uint16 index)
{
	bool valid;
	if (index == 0)
		return true;
	if (page_wal_shared == NULL || index > PAGE_WAL_SOURCE_SLOTS)
		return false;
	SpinLockAcquire(&page_wal_shared->source_lock);
	valid = page_wal_shared->sources[index - 1].references != 0;
	if (valid)
		page_wal_shared->sources[index - 1].references--;
	SpinLockRelease(&page_wal_shared->source_lock);
	return valid;
}

/* One more reference on a source the caller already references. */
static bool
page_wal_source_retain_index(uint16 index)
{
	bool retained;
	if (index == 0 || index > PAGE_WAL_SOURCE_SLOTS)
		return false;
	SpinLockAcquire(&page_wal_shared->source_lock);
	retained = page_wal_shared->sources[index - 1].references != 0
			   && page_wal_shared->sources[index - 1].references != UINT32_MAX;
	if (retained)
		page_wal_shared->sources[index - 1].references++;
	SpinLockRelease(&page_wal_shared->source_lock);
	return retained;
}

static void
page_wal_slot_encode(PageWalSlot *slot, const ClusterPageWalBindingV1 *value, uint16 source)
{
	memcpy(slot->incarnation, value->version.segment_incarnation, 16);
	slot->token = value->version.mutation_token;
	slot->start = value->record_start;
	slot->end = value->record_end;
	slot->crc = value->record_crc;
	slot->source_flags = source;
	if (value->flags & CLUSTER_PAGE_WAL_NATIVE_FLUSHED)
		slot->source_flags |= PAGE_WAL_SLOT_FLUSHED;
	slot->rmid = value->rmid;
	slot->info = value->info;
}

static bool
page_wal_expand_ref(const PageWalSlot *slot, RelFileLocator locator, ForkNumber forknum,
					BlockNumber blockno, ClusterPageWalBindingV1 *value)
{
	uint16 source = slot->source_flags & PAGE_WAL_SOURCE_MASK;
	if (source == 0 || source > PAGE_WAL_SOURCE_SLOTS)
		return false;
	memset(value, 0, sizeof(*value));
	/* The caller protects its owned reference; another owner may change the
	 * pool refcount but cannot replace this source while we retain it. */
	value->source = page_wal_shared->sources[source - 1].source;
	value->identity.system_identifier = value->source.claim.identity.system_identifier;
	memcpy(value->identity.storage_uuid, value->source.claim.identity.storage_uuid, 16);
	value->identity.locator = locator;
	value->identity.forknum = forknum;
	value->identity.blockno = blockno;
	memcpy(value->version.segment_incarnation, slot->incarnation, 16);
	value->version.mutation_token = slot->token;
	value->record_start = slot->start;
	value->record_end = slot->end;
	value->record_crc = slot->crc;
	value->rmid = slot->rmid;
	value->info = slot->info;
	value->flags
		= (slot->source_flags & PAGE_WAL_SLOT_FLUSHED) ? CLUSTER_PAGE_WAL_NATIVE_FLUSHED : 0;
	return true;
}

static bool
page_wal_expand(BufferDesc *buf, ClusterPageWalBindingV1 *value)
{
	return page_wal_expand_ref(&bindings[buf->buf_id], BufTagGetRelFileLocator(&buf->tag),
							   buf->tag.forkNum, buf->tag.blockNum, value);
}

bool
cluster_page_wal_ref_retain_v1(const ClusterPageWalBindingV1 *binding, ClusterPageWalRefV1 *out)
{
	static const ClusterPageWalRefV1 zero = { 0 };
	uint16 source;
	if (out == NULL || page_wal_shared == NULL || memcmp(out, &zero, sizeof(*out)) != 0
		|| !cluster_page_wal_binding_shape_v1(binding)
		|| !rf_page_identity_valid_v1(&binding->identity))
		return false;
	source = page_wal_source_acquire(&binding->source);
	if (source == 0)
		return false;
	page_wal_slot_encode(out, binding, source);
	return true;
}

bool
cluster_page_wal_ref_read_v1(const ClusterPageWalRefV1 *ref, RelFileLocator locator,
							 ForkNumber forknum, BlockNumber blockno, ClusterPageWalBindingV1 *out)
{
	ClusterPageWalBindingV1 value;
	if (out == NULL || ref == NULL || page_wal_shared == NULL
		|| !page_wal_expand_ref(ref, locator, forknum, blockno, &value)
		|| !cluster_page_wal_binding_shape_v1(&value)
		|| !rf_page_identity_valid_v1(&value.identity))
		return false;
	*out = value;
	return true;
}

bool
cluster_page_wal_ref_release_v1(ClusterPageWalRefV1 *ref)
{
	if (ref == NULL || !page_wal_source_release(ref->source_flags & PAGE_WAL_SOURCE_MASK))
		return false;
	memset(ref, 0, sizeof(*ref));
	return true;
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
	ClusterPageWalBindingV1 value;
	uint32 state;
	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || out == NULL)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0 || !page_wal_expand(buf, &value)
		|| !cluster_page_wal_binding_matches_v1(&value, BufTagGetRelFileLocator(&buf->tag),
												buf->tag.forkNum, buf->tag.blockNum,
												BufferGetPage(buffer)))
		return false;
	*out = value;
	return true;
}
bool
cluster_page_wal_pi_snapshot_locked_v1(BufferDesc *buf, ClusterPageWalBindingV1 *out)
{
	ClusterPageWalBindingV1 value;
	uint32 state;
	if (bindings == NULL || buf == NULL || buf->buf_id < 0 || buf->buf_id >= NBuffers
		|| out == NULL)
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_LOCKED | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_LOCKED | BM_TAG_VALID | BM_PERMANENT)
		|| (state
			& (BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR
			   | BM_IO_IN_PROGRESS))
			   != 0
		|| buf->buffer_type != BUF_TYPE_PI || buf->pcm_state != PCM_STATE_N
		|| !page_wal_expand(buf, &value)
		|| !cluster_page_wal_binding_matches_v1(&value, BufTagGetRelFileLocator(&buf->tag),
												buf->tag.forkNum, buf->tag.blockNum,
												BufferGetPage(BufferDescriptorGetBuffer(buf))))
		return false;
	/* PI conversion retains this descriptor's claim reference. With its
	 * header locked and no input I/O, neither reuse nor reread can release
	 * that reference or alter the frozen bytes during this projection. */
	*out = value;
	return true;
}

ClusterPageWalCaptureResultV1
cluster_page_wal_eviction_snapshot_locked_v1(BufferDesc *buf, const ClusterPcmOwnSnapshot *fence,
											 uint32 caller_pins, ClusterPageWalBindingV1 *out)
{
	ClusterPageWalBindingV1 value;
	uint32 state;
	if (buf == NULL || fence == NULL || out == NULL || buf->buf_id < 0 || buf->buf_id >= NBuffers
		|| caller_pins > 1)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_LOCKED | BM_VALID | BM_TAG_VALID)) != (BM_LOCKED | BM_VALID | BM_TAG_VALID)
		|| (state & (BM_DIRTY | BM_IO_ERROR | BM_IO_IN_PROGRESS)) != 0
		|| BUF_STATE_GET_REFCOUNT(state) != caller_pins || !BufferTagsEqual(&buf->tag, &fence->tag)
		|| buf->pcm_state != PCM_STATE_X || fence->pcm_state != PCM_STATE_X
		|| buf->buffer_type != fence->buffer_type || fence->flags != PCM_OWN_FLAG_REVOKING
		|| fence->reservation_token == 0 || fence->reservation_token == UINT64_MAX
		|| fence->writer_activation_token != 0 || fence->resource_x_activation_generation != 0
		|| fence->generation == 0 || fence->generation == UINT64_MAX
		|| cluster_pcm_own_gen_get(buf->buf_id) != fence->generation
		|| cluster_pcm_own_flags_get(buf->buf_id) != fence->flags
		|| cluster_pcm_own_reservation_token_get(buf->buf_id) != fence->reservation_token
		|| cluster_pcm_own_writer_activation_token_get(buf->buf_id) != 0
		|| cluster_pcm_own_resource_x_activation_generation_get(buf->buf_id) != 0)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (bindings == NULL || bindings[buf->buf_id].source_flags == 0)
		return CLUSTER_PAGE_WAL_UNATTRIBUTED;
	if ((state & BM_PERMANENT) == 0 || !page_wal_expand(buf, &value)
		|| !cluster_page_wal_binding_matches_v1(&value, BufTagGetRelFileLocator(&buf->tag),
												buf->tag.forkNum, buf->tag.blockNum,
												BufferGetPage(BufferDescriptorGetBuffer(buf))))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	*out = value;
	return CLUSTER_PAGE_WAL_CAPTURED;
}

bool
cluster_page_wal_prepare_install_v1(Buffer buffer, const ClusterPageWalBindingV1 *carrier,
									Page image, ClusterPageWalInstallV1 *prepared)
{
	static const ClusterPageWalBindingV1 zero = { 0 };
	ClusterPageWalInstallV1 result = { 0 };
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
			|| current.claim.database_incarnation != carrier->source.claim.database_incarnation
			|| ((carrier->flags & CLUSTER_PAGE_WAL_NATIVE_FLUSHED) == 0
				&& !page_wal_same_source(&current, &carrier->source)))
			return false;
		result.source_slot = page_wal_source_acquire(&carrier->source);
		if (result.source_slot == 0)
			return false;
	}
	result.binding = *carrier;
	*prepared = result;
	return true;
}
bool
cluster_page_wal_publish_install_v1(Buffer buffer, ClusterPageWalInstallV1 *prepared)
{
	static const ClusterPageWalBindingV1 zero = { 0 };
	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || prepared == NULL
		|| !LWLockHeldByMeInMode(BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)),
								 LW_EXCLUSIVE))
		return false;
	if (memcmp(&prepared->binding, &zero, sizeof(zero)) != 0
		&& (prepared->source_slot == 0 || prepared->source_slot > PAGE_WAL_SOURCE_SLOTS
			|| !page_wal_same_source(&prepared->binding.source,
									 &page_wal_shared->sources[prepared->source_slot - 1].source)))
		return false;
	if (!page_wal_source_release(bindings[buffer - 1].source_flags & PAGE_WAL_SOURCE_MASK))
		return false;
	page_wal_slot_encode(&bindings[buffer - 1], &prepared->binding, prepared->source_slot);
	prepared->source_slot = 0;
	return true;
}

void
cluster_page_wal_release_install_v1(ClusterPageWalInstallV1 *prepared)
{
	if (prepared != NULL) {
		if (!page_wal_source_release(prepared->source_slot))
			elog(PANIC, "page WAL preparation lost its source reference");
		prepared->source_slot = 0;
	}
}

bool
cluster_page_wal_forget_v1(Buffer buffer)
{
	ClusterPageWalInstallV1 zero = { 0 };
	return cluster_page_wal_publish_install_v1(buffer, &zero);
}

void
cluster_page_wal_reset_reuse_locked(BufferDesc *buf)
{
	if (bindings == NULL)
		return;
	if (buf == NULL || buf->buf_id < 0 || buf->buf_id >= NBuffers
		|| (pg_atomic_read_u32(&buf->state) & BM_LOCKED) == 0
		|| !page_wal_source_release(bindings[buf->buf_id].source_flags & PAGE_WAL_SOURCE_MASK)
		|| !page_wal_source_release(firsts[buf->buf_id].source_flags & PAGE_WAL_SOURCE_MASK))
		elog(PANIC, "page WAL descriptor reuse lost its source owner");
	memset(&bindings[buf->buf_id], 0, sizeof(PageWalSlot));
	/* The old residency ends; its first record leaves with it. */
	pg_atomic_write_u64(&first_lsns[buf->buf_id], 0);
	pg_write_barrier();
	memset(&firsts[buf->buf_id], 0, sizeof(PageWalSlot));
}

Size
cluster_page_wal_shmem_size(void)
{
	StaticAssertStmt(sizeof(PageWalSlot) % sizeof(pg_atomic_uint64) == 0,
					 "first-record LSNs follow the slot arrays aligned");
	return add_size(offsetof(PageWalShared, slots),
					mul_size((Size)NBuffers, 2 * sizeof(PageWalSlot) + sizeof(pg_atomic_uint64)));
}

void
cluster_page_wal_shmem_init(void)
{
	bool found;
	page_wal_shared
		= ShmemInitStruct("pgrac page WAL binding", cluster_page_wal_shmem_size(), &found);
	if (!found) {
		memset(page_wal_shared, 0, cluster_page_wal_shmem_size());
		SpinLockInit(&page_wal_shared->source_lock);
		pg_atomic_init_u32(&page_wal_shared->cold_redo_failed, 0);
		for (int i = 0; i < NBuffers; i++)
			pg_atomic_init_u64(&first_lsns[i], 0);
	}
}

bool
cluster_page_wal_cold_redo_write_allowed_v1(void)
{
	return page_wal_shared != NULL && pg_atomic_read_u32(&page_wal_shared->cold_redo_failed) == 0;
}

void
cluster_page_wal_cold_redo_fail_v1(void)
{
	if (page_wal_shared != NULL)
		pg_atomic_write_u32(&page_wal_shared->cold_redo_failed, 1);
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

/*
 * Under content-X, after the capture of a record [start, end): the first own
 * capture since the page was last clean becomes its first record.  Later
 * captures leave it.  It never copies the latest flags: its own flush is
 * certified separately.  Without a reference the LSN is still published,
 * unattributed, so the floor cannot pass it.
 */
static void
page_wal_first_capture(int buf_id, const ClusterPageWalBindingV1 *value, uint16 source)
{
	ClusterPageWalBindingV1 first = *value;

	if (firsts[buf_id].source_flags != 0 || pg_atomic_read_u64(&first_lsns[buf_id]) != 0)
		return;
	first.flags = 0;
	if (page_wal_source_retain_index(source))
		page_wal_slot_encode(&firsts[buf_id], &first, source);
	pg_write_barrier();
	pg_atomic_write_u64(&first_lsns[buf_id], value->record_start);
}

/*
 * A record this capture cannot attribute (no current writer, or no source
 * slot) still made the page dirty.  Publish its LSN as an unattributed first
 * record unless the page already has one, so the floor cannot pass it; an
 * existing first record is never raised.
 */
static void
page_wal_first_mark_unattributed(int buf_id, XLogRecPtr start)
{
	if (firsts[buf_id].source_flags != 0 || pg_atomic_read_u64(&first_lsns[buf_id]) != 0)
		return;
	pg_atomic_write_u64(&first_lsns[buf_id], start);
}

static ClusterPageWalCaptureResultV1
page_wal_capture_native(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
						uint64 result_token, XLogRecPtr start, XLogRecPtr end,
						uint32 crc, uint8 rmid, uint8 info,
						const ClusterSpaceIdentityKey *space_key)
{
	ClusterPageWalBindingV1 value = { 0 };
	BufferDesc *buf;
	Page page;
	uint32 state;
	uint16 old_source, source;

	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || edge == NULL || !cluster_enabled
		|| !cluster_shared_config || RecoveryInProgress() || result_token == 0
		|| XLogRecPtrIsInvalid(start) || start >= end
		|| edge->page_class != (space_key == NULL ? RF_PAGE_CLASS_ORDINARY : RF_PAGE_CLASS_ROUTED_SPACE)
		|| edge->result_kind != RF_PAGE_STATE_PRESENT)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)
		|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(buf))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0
		|| (space_key == NULL
			? (buf->tag.forkNum != MAIN_FORKNUM && buf->tag.forkNum != VISIBILITYMAP_FORKNUM)
			: (buf->tag.forkNum != SPACE_FORKNUM || buf->tag.blockNum >= 2))
		|| cluster_smgr_which_for(BufTagGetRelFileLocator(&buf->tag), InvalidBackendId) != 1)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	page = BufferGetPage(buffer);
	memcpy(value.version.segment_incarnation, edge->result_incarnation, 16);
	value.version.mutation_token = result_token;
	if (!rf_page_version_present_v1(&value.version) || PageIsNew(page)
		|| ((PageHeader)page)->pd_block_scn != result_token)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (!cluster_wal_thread_current_v2_ref(&value.source)) {
		page_wal_first_mark_unattributed(buffer - 1, start);
		return CLUSTER_PAGE_WAL_UNATTRIBUTED;
	}
	if (value.source.claim.database_incarnation == 0 || value.source.timeline == 0
		|| value.source.claim.identity.origin_thread_id == 0
		|| value.source.claim.identity.origin_thread_id > PGRAC_PAGE_LSN_ORIGIN_MAX + 1)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (space_key != NULL
		&& (space_key->system_identifier != value.source.claim.identity.system_identifier
			|| space_key->database_incarnation != value.source.claim.database_incarnation
			|| memcmp(space_key->storage_uuid, value.source.claim.identity.storage_uuid, 16) != 0
			|| !RelFileLocatorEquals(space_key->locator, BufTagGetRelFileLocator(&buf->tag))))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (space_key != NULL) {
		int origin;
		if (!PageGetLSNOrigin(page, &origin)
			|| origin != value.source.claim.identity.origin_thread_id - 1)
			return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	}
	value.identity.system_identifier = value.source.claim.identity.system_identifier;
	memcpy(value.identity.storage_uuid, value.source.claim.identity.storage_uuid, 16);
	value.identity.locator = BufTagGetRelFileLocator(&buf->tag);
	value.identity.forknum = buf->tag.forkNum;
	value.identity.blockno = buf->tag.blockNum;
	if (!rf_page_identity_valid_v1(&value.identity))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	value.record_start = start;
	value.record_end = end;
	value.record_crc = crc;
	value.rmid = rmid;
	value.info = info;
	/* This exact successful record owns the new version. Native callers set
	 * PageLSN after XLogInsert returns, still under this content lock. */
	old_source = bindings[buffer - 1].source_flags & PAGE_WAL_SOURCE_MASK;
	if (old_source > PAGE_WAL_SOURCE_SLOTS)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (old_source != 0
		&& page_wal_same_source(&page_wal_shared->sources[old_source - 1].source, &value.source))
		source = old_source; /* ordinary same-writer hot path needs no pool lock */
	else {
		source = page_wal_source_acquire(&value.source);
		if (source == 0) {
			page_wal_first_mark_unattributed(buffer - 1, start);
			return CLUSTER_PAGE_WAL_UNATTRIBUTED;
		}
		if (!page_wal_source_release(old_source)) {
			(void)page_wal_source_release(source);
			return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
		}
	}
	page_wal_slot_encode(&bindings[buffer - 1], &value, source);
	page_wal_first_capture(buffer - 1, &value, source);
	return CLUSTER_PAGE_WAL_CAPTURED;
}

ClusterPageWalCaptureResultV1
cluster_page_wal_capture_native_v1(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
								 uint64 result_token, XLogRecPtr start, XLogRecPtr end,
								 uint32 crc, uint8 rmid, uint8 info)
{
	return page_wal_capture_native(buffer, edge, result_token, start, end, crc, rmid, info, NULL);
}

ClusterPageWalCaptureResultV1
cluster_page_wal_capture_space_v1(Buffer buffer, const ClusterSpaceIdentityKey *key,
								XLogRecPtr end)
{
	RfPageVersionEdgeEntryV1 edge = { 0 };
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation;
	XLogRecord record;
	XLogRecPtr start;
	BufferDesc *buf;
	Page page;
	uint64 token;
	uint8 op;

	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || key == NULL
		|| !cluster_enabled || !cluster_shared_config || RecoveryInProgress())
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)
		|| buf->tag.forkNum != SPACE_FORKNUM || buf->tag.blockNum >= 2
		|| !RelFileLocatorEquals(key->locator, BufTagGetRelFileLocator(&buf->tag))
		|| !XLogGetLastInsertRecord(end, &start, &record))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	op = record.xl_info & ~XLR_INFO_MASK;
	if (!((record.xl_rmid == RM_SMGR_ID
			&& (op == XLOG_SMGR_SPACE_IDENTITY
				|| (op == XLOG_SMGR_SPACE_RESERVATION && buf->tag.blockNum == 1)))
		  || (record.xl_rmid == RM_XACT_ID
			  && (record.xl_info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT)))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	page = BufferGetPage(buffer);
	if (PageGetLSN(page) != end)
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	if (buf->tag.blockNum == 0) {
		if (!cluster_space_identity_page_decode(page, BLCKSZ, SPACE_FORKNUM, 0,
				key, &identity, &token))
			return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	} else {
		if (!cluster_space_reservation_page_decode(page, BLCKSZ, SPACE_FORKNUM, 1,
				key, &reservation, &token))
			return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
		identity = reservation.identity;
	}
	if ((record.xl_rmid == RM_XACT_ID)
		!= (identity.state == CLUSTER_SPACE_IDENTITY_TOMBSTONED))
		return CLUSTER_PAGE_WAL_INVARIANT_BROKEN;
	edge.page_class = RF_PAGE_CLASS_ROUTED_SPACE;
	edge.result_kind = RF_PAGE_STATE_PRESENT;
	memcpy(edge.result_incarnation, identity.incarnation, 16);
	return page_wal_capture_native(buffer, &edge, token, start, end, record.xl_crc,
								   record.xl_rmid, record.xl_info, key);
}

bool
cluster_page_wal_read_v1(Buffer buffer, const ClusterSpaceIdentity *identity,
						 ClusterPageWalBindingV1 *out)
{
	ClusterPageWalBindingV1 binding;
	const ClusterPageWalBindingV1 *value = &binding;
	BufferDesc *buf;
	Page page;
	int origin;
	uint32 state;

	if (bindings == NULL || buffer <= 0 || buffer > NBuffers || identity == NULL || out == NULL)
		return false;
	buf = GetBufferDescriptor(buffer - 1);
	if (!LWLockHeldByMe(BufferDescriptorGetContentLock(buf))
		|| (buf->tag.forkNum != SPACE_FORKNUM && identity->state != CLUSTER_SPACE_IDENTITY_LIVE))
		return false;
	state = pg_atomic_read_u32(&buf->state);
	if ((state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT))
			!= (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
		|| (state & BM_IO_ERROR) != 0 || !page_wal_expand(buf, &binding))
		return false;
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
	if (buf->tag.forkNum == SPACE_FORKNUM) {
		ClusterSpaceIdentity decoded;
		uint64 token;

		if (buf->tag.blockNum == 0) {
			if (!cluster_space_identity_page_decode(page, BLCKSZ, SPACE_FORKNUM, 0, &identity->key,
													&decoded, &token))
				return false;
		} else {
			ClusterSpaceReservation reservation;
			if (!cluster_space_reservation_page_decode(page, BLCKSZ, SPACE_FORKNUM,
													   buf->tag.blockNum, &identity->key,
													   &reservation, &token))
				return false;
			decoded = reservation.identity;
		}
		/* The reservation belongs to this exact structural identity. A
		 * matching BufferTag, LSN or incarnation alone is insufficient. */
		if (memcmp(decoded.incarnation, identity->incarnation, 16) != 0
			|| decoded.sequence != identity->sequence || decoded.operation != identity->operation
			|| decoded.state != identity->state || token != value->version.mutation_token)
			return false;
	}
	*out = *value;
	return true;
}

/* Whether the header-locked descriptor has a first record (attributed or not). */
static bool
page_wal_first_present_locked(BufferDesc *buf)
{
	return bindings != NULL && buf != NULL && buf->buf_id >= 0 && buf->buf_id < NBuffers
		   && (pg_atomic_read_u32(&buf->state) & BM_LOCKED) != 0
		   && pg_atomic_read_u64(&first_lsns[buf->buf_id]) != 0;
}

ClusterPageWalFirstResultV1
cluster_page_wal_first_observe_locked_v1(BufferDesc *buf, ClusterPageWalRefV1 *out)
{
	if (out == NULL)
		return CLUSTER_PAGE_WAL_FIRST_INVALID;
	memset(out, 0, sizeof(*out));
	if (bindings == NULL || buf == NULL || buf->buf_id < 0 || buf->buf_id >= NBuffers
		|| (pg_atomic_read_u32(&buf->state) & BM_LOCKED) == 0)
		return CLUSTER_PAGE_WAL_FIRST_INVALID;
	if (!page_wal_first_present_locked(buf))
		return CLUSTER_PAGE_WAL_FIRST_ABSENT;
	if (firsts[buf->buf_id].source_flags == 0) {
		/* Only the LSN of an unattributed first record is known. */
		out->start = pg_atomic_read_u64(&first_lsns[buf->buf_id]);
		return CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED;
	}
	*out = firsts[buf->buf_id];
	return CLUSTER_PAGE_WAL_FIRST_PRESENT;
}

ClusterPageWalFirstResultV1
cluster_page_wal_first_retain_locked_v1(BufferDesc *buf, ClusterPageWalRefV1 *out)
{
	ClusterPageWalFirstResultV1 result = cluster_page_wal_first_observe_locked_v1(buf, out);

	if (result != CLUSTER_PAGE_WAL_FIRST_PRESENT)
		return result;
	if (!page_wal_source_retain_index(out->source_flags & PAGE_WAL_SOURCE_MASK)) {
		memset(out, 0, sizeof(*out));
		return CLUSTER_PAGE_WAL_FIRST_INVALID;
	}
	return CLUSTER_PAGE_WAL_FIRST_PRESENT;
}

bool
cluster_page_wal_first_clear_written_locked_v1(BufferDesc *buf, const ClusterPageWalRefV1 *observed,
											   uint64 written_token)
{
	uint32 state;
	bool unattributed;
	if (observed == NULL || !page_wal_first_present_locked(buf))
		return false;
	/* An unattributed first record is known by its LSN alone; it was
	 * observed before this write began, so the write includes it. */
	unattributed = observed->source_flags == 0;
	if (unattributed ? firsts[buf->buf_id].source_flags != 0 || observed->start == InvalidXLogRecPtr
						   || pg_atomic_read_u64(&first_lsns[buf->buf_id]) != observed->start
					 : memcmp(&firsts[buf->buf_id], observed, sizeof(*observed)) != 0)
		return false;
	state = pg_atomic_read_u32(&buf->state);
	/* A page still (or again) dirty, invalid, or written below its first
	 * record (in SCN total order) keeps it; a conservative keep only delays
	 * the floor. */
	if ((state & (BM_VALID | BM_TAG_VALID)) != (BM_VALID | BM_TAG_VALID)
		|| (state & (BM_DIRTY | BM_JUST_DIRTIED | BM_IO_IN_PROGRESS | BM_IO_ERROR)) != 0
		|| (!unattributed && scn_total_cmp(written_token, observed->token) < 0))
		return false;
	pg_atomic_write_u64(&first_lsns[buf->buf_id], 0);
	pg_write_barrier();
	if (!unattributed
		&& !page_wal_source_release(firsts[buf->buf_id].source_flags & PAGE_WAL_SOURCE_MASK))
		elog(PANIC, "page WAL first record lost its source owner");
	memset(&firsts[buf->buf_id], 0, sizeof(PageWalSlot));
	return true;
}

bool
cluster_page_wal_first_handover_locked_v1(BufferDesc *buf, const ClusterPageWalRefV1 *observed)
{
	if (observed == NULL || !page_wal_first_present_locked(buf)
		|| memcmp(&firsts[buf->buf_id], observed, sizeof(*observed)) != 0
		|| observed->source_flags == 0)
		return false;
	pg_atomic_write_u64(&first_lsns[buf->buf_id], 0);
	pg_write_barrier();
	if (!page_wal_source_release(firsts[buf->buf_id].source_flags & PAGE_WAL_SOURCE_MASK))
		elog(PANIC, "page WAL first record lost its source owner");
	memset(&firsts[buf->buf_id], 0, sizeof(PageWalSlot));
	return true;
}

/*
 * Lock-free: for every buffer with a first record, attribute it by reading
 * its slot between two reads of its LSN (published after, cleared before the
 * slot), then the referenced source, which cannot change while referenced.
 * A buffer whose first record changes three times in a row, or carries none,
 * is unattributed: the caller must keep its lower.
 */
bool
cluster_page_wal_dirty_floor_v1(const ClusterWalSourceRef *source, ClusterPageWalDirtyFloorV1 *out)
{
	ClusterPageWalDirtyFloorV1 value = { 0 };

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (source == NULL || page_wal_shared == NULL)
		return false;
	for (int i = 0; i < NBuffers; i++) {
		bool attributed = false, own = false;
		XLogRecPtr lsn = 0;

		for (int attempt = 0; attempt < 3; attempt++) {
			uint16 index;
			ClusterWalSourceRef owner;

			lsn = pg_atomic_read_u64(&first_lsns[i]);
			if (lsn == 0)
				break;
			pg_read_barrier();
			index = firsts[i].source_flags & PAGE_WAL_SOURCE_MASK;
			if (index != 0 && index <= PAGE_WAL_SOURCE_SLOTS)
				owner = page_wal_shared->sources[index - 1].source;
			pg_read_barrier();
			if (pg_atomic_read_u64(&first_lsns[i]) != lsn)
				continue;
			if (index != 0 && index <= PAGE_WAL_SOURCE_SLOTS) {
				attributed = true;
				own = page_wal_same_source(&owner, source);
			}
			break;
		}
		if (lsn == 0)
			continue;
		value.dirty++;
		if (!attributed)
			value.unattributed++;
		else if (!own)
			value.foreign++;
		else if (value.floor == InvalidXLogRecPtr || lsn < value.floor)
			value.floor = lsn;
	}
	*out = value;
	return true;
}
