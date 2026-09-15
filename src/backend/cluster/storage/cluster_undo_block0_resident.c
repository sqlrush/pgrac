/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0_resident.c
 *	  Sparse resident frame authority for undo block zero.
 *
 *	  Block-zero metadata is direct-indexed by logical owner/segment, while
 *	  BLCKSZ payload frames are drawn from a separate B=D bank.  This layer
 *	  supplies only local pin/content serialization.  A caller still needs the
 *	  generation-independent cluster-current guard before invoking it.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_block0_resident.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_semantic_activation.h"
#include "lib/ilist.h"
#include "cluster/cluster_clean_leave.h"
#include "port/atomics.h"
#include "storage/bufpage.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#define CLUSTER_UNDO_BLOCK0_FRAME_INVALID UINT32_MAX
#define CLUSTER_UNDO_BLOCK0_META_STRIDE 128

typedef struct ClusterUndoBlock0SlotData {
	LWLock content_lock;
	pg_atomic_uint32 state;
	pg_atomic_uint32 pincount;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot resolved_root;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0AuthorityProof proof;
	uint32 frame_index;
	XLogRecPtr last_wal_lsn;
} ClusterUndoBlock0SlotData;

typedef union ClusterUndoBlock0Slot {
	ClusterUndoBlock0SlotData data;
	char padding[CLUSTER_UNDO_BLOCK0_META_STRIDE];
} ClusterUndoBlock0Slot;

typedef struct ClusterUndoBlock0Ctl {
	LWLock frame_lock;
	uint32 frame_count;
	uint32 free_count;
	uint8 reserved[104];
} ClusterUndoBlock0Ctl;

StaticAssertDecl(sizeof(ClusterUndoBlock0SlotData) <= CLUSTER_UNDO_BLOCK0_META_STRIDE,
				 "block-zero metadata must fit the fixed 128-byte stride");
StaticAssertDecl(sizeof(ClusterUndoBlock0Slot) == CLUSTER_UNDO_BLOCK0_META_STRIDE,
				 "block-zero metadata stride must remain 128 bytes");
StaticAssertDecl(sizeof(ClusterUndoBlock0Ctl) == 128,
				 "block-zero control header must remain 128 bytes");

static ClusterUndoBlock0Ctl *Block0Ctl = NULL;
static ClusterUndoBlock0Slot *Block0Slots = NULL;
static uint32 *Block0FreeFrames = NULL;
static char *Block0Frames = NULL;

typedef enum ClusterUndoBlock0OwnedKind {
	CLUSTER_UNDO_BLOCK0_OWNED_FRAME = 0,
	CLUSTER_UNDO_BLOCK0_OWNED_RESERVED_PIN,
	CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN,
	CLUSTER_UNDO_BLOCK0_OWNED_FILL,
	CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY,
	CLUSTER_UNDO_BLOCK0_OWNED_PROVISION
} ClusterUndoBlock0OwnedKind;

typedef struct ClusterUndoBlock0OwnedResource {
	dlist_node node;
	ResourceOwner owner;
	const void *handle;
	uint32 slot;
	uint32 frame_index;
	ClusterUndoBlock0OwnedKind kind;
	char *temp_path;
} ClusterUndoBlock0OwnedResource;

static dlist_head Block0OwnedResources = DLIST_STATIC_INIT(Block0OwnedResources);

bool
cluster_undo_block0_backend_has_resources(void)
{
	return !dlist_is_empty(&Block0OwnedResources);
}
static bool Block0ResourceCallbackRegistered = false;

#define BLOCK0_FRAME_DATA(i) (Block0Frames + ((Size)(i)) * BLCKSZ)

static bool block0_authority_proof_valid(const ClusterUndoBlock0LogicalKey *logical,
											 const ClusterUndoBlock0AuthorityProof *proof);
static bool block0_authority_proof_matches(const ClusterUndoBlock0AuthorityProof *observed,
											   const ClusterUndoBlock0AuthorityProof *expected);
static bool block0_page_identity(const char *page, const ClusterUndoBlock0LogicalKey *logical,
								 ClusterUndoBlock0Generation *generation);
static void block0_pin_clear(ClusterUndoBlock0Pin *pin);
static void block0_drop_reservation(ClusterUndoBlock0SlotData *meta,
										ClusterUndoBlock0Pin *pin);
static void block0_abort_pin(ClusterUndoBlock0Pin *pin);
static void block0_recovery_guard_clear(ClusterUndoBlock0RecoveryGuard *guard);
static void block0_resource_release_callback(ResourceReleasePhase phase, bool isCommit,
										 bool isTopLevel, void *arg);
static ClusterUndoBlock0OwnedResource *block0_resource_prepare(const void *handle,
											ClusterUndoBlock0OwnedKind kind);
static void block0_resource_link(ClusterUndoBlock0OwnedResource *resource);
static ClusterUndoBlock0OwnedResource *block0_resource_find(
	const void *handle, ClusterUndoBlock0OwnedKind kind);
static void block0_resource_free(ClusterUndoBlock0OwnedResource *resource);
static void block0_resource_forget(ClusterUndoBlock0OwnedResource *resource);


static void
block0_resource_ensure_callback(void)
{
	if (!Block0ResourceCallbackRegistered) {
		RegisterResourceReleaseCallback(block0_resource_release_callback, NULL);
		Block0ResourceCallbackRegistered = true;
	}
}


static ClusterUndoBlock0OwnedResource *
block0_resource_prepare(const void *handle, ClusterUndoBlock0OwnedKind kind)
{
	ClusterUndoBlock0OwnedResource *resource;

	if (CurrentResourceOwner == NULL || handle == NULL)
		return NULL;
	block0_resource_ensure_callback();
	resource = MemoryContextAlloc(TopMemoryContext, sizeof(*resource));
	memset(resource, 0, sizeof(*resource));
	resource->owner = CurrentResourceOwner;
	resource->handle = handle;
	resource->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
	resource->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	resource->kind = kind;
	return resource;
}


static void
block0_resource_link(ClusterUndoBlock0OwnedResource *resource)
{
	Assert(resource != NULL);
	dlist_push_tail(&Block0OwnedResources, &resource->node);
}


static ClusterUndoBlock0OwnedResource *
block0_resource_find(const void *handle, ClusterUndoBlock0OwnedKind kind)
{
	dlist_iter iter;

	dlist_foreach(iter, &Block0OwnedResources)
	{
		ClusterUndoBlock0OwnedResource *resource
			= dlist_container(ClusterUndoBlock0OwnedResource, node, iter.cur);

		if (resource->handle == handle && resource->kind == kind)
			return resource;
	}
	return NULL;
}


static void
block0_resource_free(ClusterUndoBlock0OwnedResource *resource)
{
	if (resource == NULL)
		return;
	if (resource->temp_path != NULL)
		pfree(resource->temp_path);
	pfree(resource);
}


static void
block0_resource_forget(ClusterUndoBlock0OwnedResource *resource)
{
	if (resource == NULL)
		return;
	dlist_delete(&resource->node);
	block0_resource_free(resource);
}


static void
block0_resource_return_frame(uint32 frame_index)
{
	if (Block0Ctl == NULL || frame_index >= Block0Ctl->frame_count)
		return;
	LWLockAcquire(&Block0Ctl->frame_lock, LW_EXCLUSIVE);
	if (Block0Ctl->free_count < Block0Ctl->frame_count)
		Block0FreeFrames[Block0Ctl->free_count++] = frame_index;
	LWLockRelease(&Block0Ctl->frame_lock);
}


static void
block0_resource_release_callback(ResourceReleasePhase phase, bool isCommit,
									 bool isTopLevel, void *arg)
{
	dlist_mutable_iter iter;

	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	dlist_foreach_modify(iter, &Block0OwnedResources)
	{
		ClusterUndoBlock0OwnedResource *resource
			= dlist_container(ClusterUndoBlock0OwnedResource, node, iter.cur);
		ClusterUndoBlock0SlotData *meta = NULL;
		bool return_frame = false;

		if (resource->owner != CurrentResourceOwner)
			continue;
		if (Block0Ctl != NULL && resource->slot < CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
			meta = &Block0Slots[resource->slot].data;
		switch (resource->kind) {
		case CLUSTER_UNDO_BLOCK0_OWNED_FRAME:
			return_frame = true;
			break;
		case CLUSTER_UNDO_BLOCK0_OWNED_RESERVED_PIN:
			if (meta != NULL && pg_atomic_read_u32(&meta->pincount) > 0)
				pg_atomic_fetch_sub_u32(&meta->pincount, 1);
			break;
		case CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN:
			if (meta != NULL) {
				LWLockRelease(&meta->content_lock);
				if (pg_atomic_read_u32(&meta->pincount) > 0)
					pg_atomic_fetch_sub_u32(&meta->pincount, 1);
			}
			break;
		case CLUSTER_UNDO_BLOCK0_OWNED_FILL:
			if (meta != NULL) {
				LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
				if (pg_atomic_read_u32(&meta->state) == CLUSTER_UNDO_BLOCK0_SLOT_FILLING
					&& meta->frame_index == resource->frame_index) {
					meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
					pg_atomic_write_u32(&meta->pincount, 0);
					pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
					return_frame = true;
				}
				LWLockRelease(&meta->content_lock);
			}
			break;
		case CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY:
			if (meta != NULL)
				LWLockRelease(&meta->content_lock);
			break;
		case CLUSTER_UNDO_BLOCK0_OWNED_PROVISION:
			if (meta != NULL) {
				if (resource->temp_path != NULL)
					(void)cluster_undo_smgr_provision_temp_cleanup(
						meta->resolved_root.intent, meta->logical.segment_id,
						meta->logical.owner_instance, resource->temp_path);
				if (pg_atomic_read_u32(&meta->state)
					== CLUSTER_UNDO_BLOCK0_SLOT_FILLING
					&& meta->frame_index == resource->frame_index) {
					meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
					pg_atomic_write_u32(&meta->pincount, 0);
					pg_atomic_write_u32(&meta->state,
								CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
					return_frame = true;
				}
				LWLockRelease(&meta->content_lock);
			}
			break;
		}
		dlist_delete(&resource->node);
		if (return_frame)
			block0_resource_return_frame(resource->frame_index);
		block0_resource_free(resource);
	}
}


Size
cluster_undo_block0_shmem_size(uint32 frame_count)
{
	Size sz;

	if (frame_count == 0)
		return 0;

	sz = MAXALIGN(sizeof(ClusterUndoBlock0Ctl));
	sz = add_size(sz, MAXALIGN(mul_size((Size)CLUSTER_UNDO_BLOCK0_SLOT_COUNT,
										 sizeof(ClusterUndoBlock0Slot))));
	sz = add_size(sz, MAXALIGN(mul_size((Size)frame_count, sizeof(uint32))));
	sz = add_size(sz, mul_size((Size)frame_count, (Size)BLCKSZ));
	return sz;
}


bool
cluster_undo_block0_shmem_init_region(void *address, Size size, uint32 frame_count, bool found)
{
	Size expected;
	char *cursor;

	expected = cluster_undo_block0_shmem_size(frame_count);
	if (address == NULL || expected == 0 || size != expected) {
		Block0Ctl = NULL;
		Block0Slots = NULL;
		Block0FreeFrames = NULL;
		Block0Frames = NULL;
		return false;
	}

	cursor = (char *)address;
	Block0Ctl = (ClusterUndoBlock0Ctl *)cursor;
	cursor += MAXALIGN(sizeof(ClusterUndoBlock0Ctl));
	Block0Slots = (ClusterUndoBlock0Slot *)cursor;
	cursor += MAXALIGN(mul_size((Size)CLUSTER_UNDO_BLOCK0_SLOT_COUNT,
							 sizeof(ClusterUndoBlock0Slot)));
	Block0FreeFrames = (uint32 *)cursor;
	cursor += MAXALIGN(mul_size((Size)frame_count, sizeof(uint32)));
	Block0Frames = cursor;

	if (found) {
		if (Block0Ctl->frame_count != frame_count
			|| Block0Ctl->free_count > Block0Ctl->frame_count) {
			cluster_undo_block0_shmem_detach();
			return false;
		}
		return true;
	}

	memset(address, 0, size);
	LWLockInitialize(&Block0Ctl->frame_lock, LWTRANCHE_CLUSTER_UNDO_BUF);
	Block0Ctl->frame_count = frame_count;
	Block0Ctl->free_count = frame_count;
	for (uint32 i = 0; i < frame_count; i++)
		Block0FreeFrames[i] = frame_count - i - 1;
	for (uint32 i = 0; i < CLUSTER_UNDO_BLOCK0_SLOT_COUNT; i++) {
		ClusterUndoBlock0SlotData *slot = &Block0Slots[i].data;

		LWLockInitialize(&slot->content_lock, LWTRANCHE_CLUSTER_UNDO_BUF);
		pg_atomic_init_u32(&slot->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
		pg_atomic_init_u32(&slot->pincount, 0);
		slot->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		slot->last_wal_lsn = InvalidXLogRecPtr;
	}
	return true;
}


void
cluster_undo_block0_shmem_detach(void)
{
	Block0Ctl = NULL;
	Block0Slots = NULL;
	Block0FreeFrames = NULL;
	Block0Frames = NULL;
}


ClusterUndoBlock0Result
cluster_undo_block0_frame_reserve_batch(uint32 count, ClusterUndoBlock0FrameToken *tokens)
{
	dlist_head prepared = DLIST_STATIC_INIT(prepared);
	dlist_mutable_iter iter;
	uint32 i;

	if (count == 0 || tokens == NULL)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	for (i = 0; i < count; i++) {
		tokens[i].frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		tokens[i].owned = false;
	}
	if (Block0Ctl == NULL || CurrentResourceOwner == NULL)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	for (i = 0; i < count; i++) {
		ClusterUndoBlock0OwnedResource *resource;

		resource = block0_resource_prepare(&tokens[i], CLUSTER_UNDO_BLOCK0_OWNED_FRAME);
		Assert(resource != NULL);
		dlist_push_tail(&prepared, &resource->node);
	}

	LWLockAcquire(&Block0Ctl->frame_lock, LW_EXCLUSIVE);
	if (count > Block0Ctl->free_count) {
		LWLockRelease(&Block0Ctl->frame_lock);
		dlist_foreach_modify(iter, &prepared)
		{
			ClusterUndoBlock0OwnedResource *resource
				= dlist_container(ClusterUndoBlock0OwnedResource, node, iter.cur);

			dlist_delete(&resource->node);
			block0_resource_free(resource);
		}
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	}
	for (i = 0; i < count; i++) {
		tokens[i].frame_index = Block0FreeFrames[--Block0Ctl->free_count];
		tokens[i].owned = true;
	}
	LWLockRelease(&Block0Ctl->frame_lock);
	dlist_foreach_modify(iter, &prepared)
	{
		ClusterUndoBlock0OwnedResource *resource
			= dlist_container(ClusterUndoBlock0OwnedResource, node, iter.cur);
		const ClusterUndoBlock0FrameToken *token
			= (const ClusterUndoBlock0FrameToken *)resource->handle;

		resource->frame_index = token->frame_index;
		dlist_delete(&resource->node);
		block0_resource_link(resource);
	}
	return CLUSTER_UNDO_BLOCK0_OK;
}


void
cluster_undo_block0_frame_release(ClusterUndoBlock0FrameToken *token)
{
	ClusterUndoBlock0OwnedResource *owned;

	if (token == NULL || !token->owned)
		return;
	owned = block0_resource_find(token, CLUSTER_UNDO_BLOCK0_OWNED_FRAME);
	if (Block0Ctl == NULL || token->frame_index >= Block0Ctl->frame_count
		|| owned == NULL || owned->frame_index != token->frame_index) {
		token->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		token->owned = false;
		if (Block0Ctl == NULL)
			block0_resource_forget(owned);
		return;
	}

	LWLockAcquire(&Block0Ctl->frame_lock, LW_EXCLUSIVE);
	Assert(Block0Ctl->free_count < Block0Ctl->frame_count);
	Block0FreeFrames[Block0Ctl->free_count++] = token->frame_index;
	LWLockRelease(&Block0Ctl->frame_lock);
	token->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	token->owned = false;
	block0_resource_forget(owned);
}


static bool
block0_authority_proof_valid(const ClusterUndoBlock0LogicalKey *logical,
							 const ClusterUndoBlock0AuthorityProof *proof)
{
	if (logical == NULL || proof == NULL || proof->owner_instance != logical->owner_instance)
		return false;
	if (proof->kind != CLUSTER_UNDO_BLOCK0_LIVE_OWNER
		&& proof->kind != CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER
		&& proof->kind != CLUSTER_UNDO_BLOCK0_STARTUP_REDO)
		return false;
	return proof->cluster_epoch_present;
}


static bool
block0_authority_proof_matches(const ClusterUndoBlock0AuthorityProof *observed,
							   const ClusterUndoBlock0AuthorityProof *expected)
{
	return observed != NULL && expected != NULL && observed->kind == expected->kind
		   && observed->owner_instance == expected->owner_instance
		   && observed->cluster_epoch_present == expected->cluster_epoch_present
		   && observed->cluster_epoch == expected->cluster_epoch
		   && observed->recovery_generation == expected->recovery_generation;
}


static bool
block0_page_identity(const char *page, const ClusterUndoBlock0LogicalKey *logical,
					 ClusterUndoBlock0Generation *generation)
{
	PageHeader ph;
	const UndoSegmentHeaderData *hdr;

	if (page == NULL || logical == NULL || generation == NULL)
		return false;
	ph = (PageHeader)page;
	hdr = (const UndoSegmentHeaderData *)page;
	if ((ph->pd_flags & PD_UNDO_SEG_HEADER) == 0
		|| PageGetPageSize((Page)page) != BLCKSZ
		|| PageGetPageLayoutVersion((Page)page) != PG_PAGE_LAYOUT_VERSION
		|| hdr->segment_id != logical->segment_id
		|| hdr->segment_size_bytes != UNDO_SEGMENT_SIZE_BYTES
		|| hdr->owner_instance != logical->owner_instance
		|| hdr->tt_slots_count != TT_SLOTS_PER_SEGMENT)
		return false;

	switch (hdr->segment_state) {
	case SEGMENT_ALLOCATED:
	case SEGMENT_ACTIVE:
	case SEGMENT_COMMITTED:
	case SEGMENT_RECYCLABLE:
		break;
	default:
		return false;
	}
	generation->known = true;
	generation->value = hdr->wrap_count;
	return true;
}


static void
block0_pin_clear(ClusterUndoBlock0Pin *pin)
{
	if (pin != NULL) {
		memset(pin, 0, sizeof(*pin));
		pin->slot = -1;
	}
}


static void
block0_drop_reservation(ClusterUndoBlock0SlotData *meta, ClusterUndoBlock0Pin *pin)
{
	Assert(pg_atomic_read_u32(&meta->pincount) > 0);
	pg_atomic_fetch_sub_u32(&meta->pincount, 1);
	block0_pin_clear(pin);
}

/* ereport(ERROR) resets InterruptHoldoffCount before PG_FINALLY/PG_CATCH.
 * Restore the one holdoff consumed by LWLockRelease so exact pin cleanup can
 * release its content lock without underflowing the process counter. */
static void
block0_release_content_lock(ClusterUndoBlock0SlotData *meta)
{
	if (InterruptHoldoffCount == 0)
		HOLD_INTERRUPTS();
	LWLockRelease(&meta->content_lock);
}


static void
block0_abort_pin(ClusterUndoBlock0Pin *pin)
{
	ClusterUndoBlock0OwnedResource *owned;
	ClusterUndoBlock0SlotData *meta;

	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0
		|| pin->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		return;
	meta = &Block0Slots[pin->slot].data;
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN);
	if (owned != NULL) {
		block0_release_content_lock(meta);
		if (pg_atomic_read_u32(&meta->pincount) > 0)
			pg_atomic_fetch_sub_u32(&meta->pincount, 1);
		block0_resource_forget(owned);
		block0_pin_clear(pin);
		return;
	}
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_RESERVED_PIN);
	if (owned != NULL) {
		if (pg_atomic_read_u32(&meta->pincount) > 0)
			pg_atomic_fetch_sub_u32(&meta->pincount, 1);
		block0_resource_forget(owned);
		block0_pin_clear(pin);
	}
}


static void
block0_recovery_guard_clear(ClusterUndoBlock0RecoveryGuard *guard)
{
	if (guard != NULL) {
		memset(guard, 0, sizeof(*guard));
		guard->slot = -1;
	}
}


/* The normal loader catches I/O ERROR before Startup's outer ResourceOwner
 * teardown. Restore only its exact unfinished FILL to the still-owned token,
 * so that outer closed-pass cleanup can return the frame and loaded prefix.
 * Other recovery/runtime callers retain their existing ResourceOwner path. */
static bool
block0_normal_start_read(ClusterUndoBlock0SlotData *meta,
						 const ClusterUndoBlock0LogicalKey *logical,
						 const ClusterUndoBlock0ResolvedRoot *root,
						 const ClusterUndoBlock0AuthorityProof *proof,
						 ClusterUndoBlock0FrameToken *token, char *frame)
{
	volatile bool read_ok = false;
	if (!AmStartupProcess() || !cluster_semantic_normal_start_closed()
		|| !cluster_undo_block0_current_startup_fenced_owned())
		return cluster_undo_smgr_read_block(root->intent, logical->segment_id,
											logical->owner_instance, 0, frame);
	PG_TRY();
	{
		read_ok = cluster_undo_smgr_read_block(root->intent, logical->segment_id,
											   logical->owner_instance, 0, frame);
	}
	PG_CATCH();
	{
		ClusterUndoBlock0OwnedResource *owned
			= block0_resource_find(token, CLUSTER_UNDO_BLOCK0_OWNED_FILL);
		LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
		if (owned != NULL && owned->owner == CurrentResourceOwner && token->owned
			&& owned->frame_index == token->frame_index && meta->frame_index == token->frame_index
			&& pg_atomic_read_u32(&meta->state) == CLUSTER_UNDO_BLOCK0_SLOT_FILLING
			&& pg_atomic_read_u32(&meta->pincount) == 1 && XLogRecPtrIsInvalid(meta->last_wal_lsn)
			&& meta->logical.segment_id == logical->segment_id
			&& meta->logical.owner_instance == logical->owner_instance
			&& cluster_undo_block0_root_matches(&meta->resolved_root, root)
			&& block0_authority_proof_matches(&meta->proof, proof)) {
			meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
			pg_atomic_write_u32(&meta->pincount, 0);
			pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
			owned->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
			owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FRAME;
		}
		LWLockRelease(&meta->content_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return read_ok;
}

ClusterUndoBlock0Result
cluster_undo_block0_admit_runtime(const ClusterUndoBlock0LogicalKey *logical,
							  const ClusterUndoBlock0ResolvedRoot *root,
							  const ClusterUndoBlock0AuthorityProof *proof,
							  ClusterUndoBlock0FrameToken *token, ClusterUndoBlock0Pin *pin,
							  char **page)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0OwnedResource *owned;
	uint32 slotno;
	char *frame;

	block0_pin_clear(pin);
	if (page != NULL)
		*page = NULL;
	if (Block0Ctl == NULL || root == NULL || token == NULL || !token->owned || pin == NULL
		|| page == NULL || token->frame_index >= Block0Ctl->frame_count)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	if (cluster_undo_block0_logical_slot(logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(root))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (!block0_authority_proof_valid(logical, proof))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (root->intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	owned = block0_resource_find(token, CLUSTER_UNDO_BLOCK0_OWNED_FRAME);

	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&meta->state) != CLUSTER_UNDO_BLOCK0_SLOT_EMPTY
		|| pg_atomic_read_u32(&meta->pincount) != 0) {
		LWLockRelease(&meta->content_lock);
		return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	}
	meta->logical = *logical;
	meta->resolved_root = *root;
	meta->proof = *proof;
	meta->frame_index = token->frame_index;
	pg_atomic_write_u32(&meta->pincount, 1);
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_FILLING);
	frame = BLOCK0_FRAME_DATA(meta->frame_index);
	if (owned != NULL) {
		owned->slot = slotno;
		owned->frame_index = token->frame_index;
		owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FILL;
	}
	LWLockRelease(&meta->content_lock);

	if (!block0_normal_start_read(meta, logical, root, proof, token, frame)) {
		LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
		meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		pg_atomic_write_u32(&meta->pincount, 0);
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
		LWLockRelease(&meta->content_lock);
		if (owned != NULL) {
			owned->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
			owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FRAME;
		}
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
	if (!block0_page_identity(frame, logical, &generation)) {
		LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
		meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		pg_atomic_write_u32(&meta->pincount, 0);
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
		LWLockRelease(&meta->content_lock);
		if (owned != NULL) {
			owned->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
			owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FRAME;
		}
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}
	if (generation.value == UINT32_MAX) {
		LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
		meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		pg_atomic_write_u32(&meta->pincount, 0);
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
		LWLockRelease(&meta->content_lock);
		if (owned != NULL) {
			owned->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
			owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FRAME;
		}
		return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	}

	LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
	meta->generation = generation;
	meta->last_wal_lsn = InvalidXLogRecPtr;
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN);
	token->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	token->owned = false;
	pin->slot = (int)slotno;
	pin->logical = *logical;
	pin->resolved_root = *root;
	pin->observed_generation = generation;
	pin->mode = CLUSTER_UNDO_BLOCK0_EXCLUSIVE;
	pin->proof = *proof;
	*page = frame;
	if (owned != NULL) {
		owned->handle = pin;
		owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN;
		owned->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	}
	return CLUSTER_UNDO_BLOCK0_OK;
}


static void
block0_provision_restore_token(ClusterUndoBlock0SlotData *meta,
							 ClusterUndoBlock0OwnedResource *owned,
							 ClusterUndoBlock0FrameToken *token)
{
	meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	pg_atomic_write_u32(&meta->pincount, 0);
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
	LWLockRelease(&meta->content_lock);
	if (owned != NULL) {
		owned->slot = CLUSTER_UNDO_BLOCK0_SLOT_COUNT;
		owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_FRAME;
		if (owned->temp_path != NULL) {
			pfree(owned->temp_path);
			owned->temp_path = NULL;
		}
	}
	Assert(token != NULL && token->owned);
}


ClusterUndoBlock0Result
cluster_undo_block0_provision_begin(const ClusterUndoBlock0LogicalKey *logical,
									const ClusterUndoBlock0ResolvedRoot *target_root,
									const ClusterUndoBlock0AuthorityProof *proof,
									ClusterUndoBlock0FrameToken *token,
									ClusterUndoBlock0Pin *pin, char **unpublished_page,
									bool *creator)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0OwnedResource *owned;
	ClusterUndoSmgrFinalState final_state;
	uint32 slotno;
	char *frame;

	block0_pin_clear(pin);
	if (unpublished_page != NULL)
		*unpublished_page = NULL;
	if (creator != NULL)
		*creator = false;
	if (Block0Ctl == NULL || target_root == NULL || token == NULL || !token->owned
		|| pin == NULL || unpublished_page == NULL || creator == NULL
		|| token->frame_index >= Block0Ctl->frame_count)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	if (cluster_undo_block0_logical_slot(logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(target_root))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (!block0_authority_proof_valid(logical, proof))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (proof->kind != CLUSTER_UNDO_BLOCK0_LIVE_OWNER)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (target_root->intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	owned = block0_resource_find(token, CLUSTER_UNDO_BLOCK0_OWNED_FRAME);
	if (owned == NULL)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;

	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&meta->state) != CLUSTER_UNDO_BLOCK0_SLOT_EMPTY
		|| pg_atomic_read_u32(&meta->pincount) != 0) {
		LWLockRelease(&meta->content_lock);
		return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	}
	meta->logical = *logical;
	meta->resolved_root = *target_root;
	meta->proof = *proof;
	meta->frame_index = token->frame_index;
	meta->last_wal_lsn = InvalidXLogRecPtr;
	pg_atomic_write_u32(&meta->pincount, 1);
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_FILLING);
	frame = BLOCK0_FRAME_DATA(meta->frame_index);
	owned->slot = slotno;
	owned->frame_index = token->frame_index;
	owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_PROVISION;
	Assert(owned->temp_path == NULL);

	final_state = cluster_undo_smgr_probe_segment(target_root->intent, logical->segment_id,
										 logical->owner_instance, frame);
	if (final_state != CLUSTER_UNDO_SMGR_FINAL_ABSENT
		&& final_state != CLUSTER_UNDO_SMGR_FINAL_EXACT
		&& final_state != CLUSTER_UNDO_SMGR_FINAL_INVALID
		&& final_state != CLUSTER_UNDO_SMGR_FINAL_IO_ERROR) {
		block0_provision_restore_token(meta, owned, token);
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_INVALID) {
		block0_provision_restore_token(meta, owned, token);
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_IO_ERROR) {
		block0_provision_restore_token(meta, owned, token);
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_ABSENT) {
		owned->temp_path = MemoryContextAlloc(TopMemoryContext, MAXPGPATH);
		owned->temp_path[0] = '\0';
		if (!cluster_undo_smgr_provision_temp_create(target_root->intent,
				logical->segment_id, logical->owner_instance, owned->temp_path)) {
			block0_provision_restore_token(meta, owned, token);
			return CLUSTER_UNDO_BLOCK0_IO_ERROR;
		}
	}
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_EXACT) {
		if (!block0_page_identity(frame, logical, &generation)) {
			block0_provision_restore_token(meta, owned, token);
			return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
		}
		if (generation.value == UINT32_MAX) {
			block0_provision_restore_token(meta, owned, token);
			return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
		}
		meta->generation = generation;
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN);
	}

	token->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	token->owned = false;
	pin->slot = (int)slotno;
	pin->logical = *logical;
	pin->resolved_root = *target_root;
	pin->observed_generation = final_state == CLUSTER_UNDO_SMGR_FINAL_EXACT
									 ? generation
									 : (ClusterUndoBlock0Generation){ .known = false, .value = 0 };
	pin->mode = CLUSTER_UNDO_BLOCK0_EXCLUSIVE;
	pin->proof = *proof;
	*unpublished_page = frame;
	*creator = final_state == CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	owned->handle = pin;
	if (!*creator) {
		owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN;
		owned->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	}
	return CLUSTER_UNDO_BLOCK0_OK;
}


void
cluster_undo_block0_provision_publish(ClusterUndoBlock0Pin *pin, XLogRecPtr init_lsn)
{
	ClusterUndoBlock0OwnedResource *owned;
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation generation;
	ClusterUndoSmgrPublishResult publish_result;
	char *frame;

	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0
		|| pin->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT || XLogRecPtrIsInvalid(init_lsn))
		ereport(ERROR, (errmsg("invalid undo block-zero provision publisher")));
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_PROVISION);
	if (owned == NULL || owned->temp_path == NULL || owned->temp_path[0] == '\0')
		ereport(ERROR, (errmsg("undo block-zero provision publisher has no private temp")));
	meta = &Block0Slots[pin->slot].data;
	if (pg_atomic_read_u32(&meta->state) != CLUSTER_UNDO_BLOCK0_SLOT_FILLING
		|| meta->frame_index != owned->frame_index)
		ereport(ERROR, (errmsg("undo block-zero provision publisher lost filling authority")));
	frame = BLOCK0_FRAME_DATA(meta->frame_index);
	if (!block0_page_identity(frame, &meta->logical, &generation)
		|| generation.value == UINT32_MAX)
		ereport(ERROR, (errmsg("invalid undo block-zero provision image")));

	XLogFlush(init_lsn);
	publish_result = cluster_undo_smgr_provision_temp_publish(
		meta->resolved_root.intent, meta->logical.segment_id, meta->logical.owner_instance,
		owned->temp_path, frame);
	if (publish_result == CLUSTER_UNDO_SMGR_PUBLISH_EXISTS) {
		if (cluster_undo_smgr_probe_segment(meta->resolved_root.intent,
				meta->logical.segment_id, meta->logical.owner_instance, frame)
			!= CLUSTER_UNDO_SMGR_FINAL_EXACT
			|| !block0_page_identity(frame, &meta->logical, &generation)
			|| generation.value == UINT32_MAX)
			ereport(ERROR, (errmsg("undo block-zero provision winner is not exact")));
	} else if (publish_result != CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED)
		ereport(ERROR, (errmsg("could not publish undo block-zero provision image")));

	meta->generation = generation;
	meta->last_wal_lsn = InvalidXLogRecPtr;
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN);
	pin->observed_generation = generation;
	owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN;
	owned->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
	pfree(owned->temp_path);
	owned->temp_path = NULL;
}


void
cluster_undo_block0_provision_abort(ClusterUndoBlock0Pin *pin)
{
	ClusterUndoBlock0OwnedResource *owned;
	ClusterUndoBlock0SlotData *meta;
	uint32 frame_index;

	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0
		|| pin->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		return;
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_PROVISION);
	if (owned == NULL)
		return;
	meta = &Block0Slots[pin->slot].data;
	frame_index = owned->frame_index;
	if (owned->temp_path != NULL && owned->temp_path[0] != '\0')
		(void)cluster_undo_smgr_provision_temp_cleanup(meta->resolved_root.intent,
			meta->logical.segment_id, meta->logical.owner_instance, owned->temp_path);
	if (pg_atomic_read_u32(&meta->state) == CLUSTER_UNDO_BLOCK0_SLOT_FILLING
		&& meta->frame_index == frame_index) {
		meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		pg_atomic_write_u32(&meta->pincount, 0);
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
	}
	LWLockRelease(&meta->content_lock);
	block0_resource_return_frame(frame_index);
	block0_resource_forget(owned);
	block0_pin_clear(pin);
}


ClusterUndoBlock0Result
cluster_undo_block0_reserve(const ClusterUndoBlock0LogicalKey *logical,
							const ClusterUndoBlock0ResolvedRoot *expected_root,
							const ClusterUndoBlock0AuthorityProof *proof,
							ClusterUndoBlock0Pin *pin)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0OwnedResource *owned;
	uint32 state;
	uint32 slotno;

	block0_pin_clear(pin);
	if (Block0Ctl == NULL)
		return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	if (pin == NULL || expected_root == NULL
		|| cluster_undo_block0_logical_slot(logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(expected_root))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (!block0_authority_proof_valid(logical, proof))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	owned = block0_resource_prepare(pin, CLUSTER_UNDO_BLOCK0_OWNED_RESERVED_PIN);

	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
	state = pg_atomic_read_u32(&meta->state);
	if (state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
		&& state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY) {
		LWLockRelease(&meta->content_lock);
		if (owned != NULL)
			pfree(owned);
		return state == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
				   ? CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH
				   : CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	}
	if (meta->logical.segment_id != logical->segment_id
		|| meta->logical.owner_instance != logical->owner_instance
		|| !cluster_undo_block0_root_matches(&meta->resolved_root, expected_root)) {
		LWLockRelease(&meta->content_lock);
		if (owned != NULL)
			pfree(owned);
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}
	if (!block0_authority_proof_matches(&meta->proof, proof)) {
		LWLockRelease(&meta->content_lock);
		if (owned != NULL)
			pfree(owned);
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	}
	pg_atomic_fetch_add_u32(&meta->pincount, 1);
	pin->slot = (int)slotno;
	pin->logical = *logical;
	pin->resolved_root = meta->resolved_root;
	pin->observed_generation = meta->generation;
	pin->proof = *proof;
	if (owned != NULL) {
		owned->slot = slotno;
		block0_resource_link(owned);
	}
	LWLockRelease(&meta->content_lock);
	return CLUSTER_UNDO_BLOCK0_OK;
}


ClusterUndoBlock0Result
cluster_undo_block0_lock_content(ClusterUndoBlock0Pin *pin,
								 const ClusterUndoBlock0Generation *expected,
								 ClusterUndoBlock0Mode mode, char **page)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0OwnedResource *owned;
	ClusterUndoBlock0Result result;
	LWLockMode lockmode;
	uint32 state;

	if (page != NULL)
		*page = NULL;
	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0
		|| pin->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	meta = &Block0Slots[pin->slot].data;
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_RESERVED_PIN);
	if (page == NULL
		|| (mode != CLUSTER_UNDO_BLOCK0_SHARED && mode != CLUSTER_UNDO_BLOCK0_EXCLUSIVE)) {
		block0_drop_reservation(meta, pin);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}
	lockmode = mode == CLUSTER_UNDO_BLOCK0_EXCLUSIVE ? LW_EXCLUSIVE : LW_SHARED;
	LWLockAcquire(&meta->content_lock, lockmode);
	state = pg_atomic_read_u32(&meta->state);
	if (state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
		&& state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY)
		result = state == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
					 ? CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH
					 : CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	else if (meta->logical.segment_id != pin->logical.segment_id
		|| meta->logical.owner_instance != pin->logical.owner_instance
		|| !cluster_undo_block0_root_matches(&meta->resolved_root, &pin->resolved_root))
		result = CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	else if (!block0_authority_proof_matches(&meta->proof, &pin->proof))
		result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	else if (expected != NULL
			 && !cluster_undo_block0_generation_matches(&meta->generation, expected))
		result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	else
		result = CLUSTER_UNDO_BLOCK0_OK;

	if (result != CLUSTER_UNDO_BLOCK0_OK) {
		LWLockRelease(&meta->content_lock);
		block0_drop_reservation(meta, pin);
		block0_resource_forget(owned);
		return result;
	}
	pin->observed_generation = meta->generation;
	pin->mode = mode;
	*page = BLOCK0_FRAME_DATA(meta->frame_index);
	if (owned != NULL)
		owned->kind = CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN;
	return CLUSTER_UNDO_BLOCK0_OK;
}


ClusterUndoBlock0Result
cluster_undo_block0_pin(const ClusterUndoBlock0LogicalKey *logical,
						const ClusterUndoBlock0ResolvedRoot *expected_root,
						const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Mode mode,
						const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Pin *pin,
						char **page)
{
	ClusterUndoBlock0Result result;

	result = cluster_undo_block0_reserve(logical, expected_root, proof, pin);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	PG_TRY();
	{
		result = cluster_undo_block0_lock_content(pin, expected, mode, page);
	}
	PG_CATCH();
	{
		block0_abort_pin(pin);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}


ClusterUndoBlock0Result
cluster_undo_block0_copy_resident(const ClusterUndoBlock0LogicalKey *logical,
								  const ClusterUndoBlock0ResolvedRoot *expected_root,
								  const ClusterUndoBlock0Generation *expected,
								  const ClusterUndoBlock0AuthorityProof *proof,
								  char private_page[BLCKSZ],
								  ClusterUndoBlock0Generation *observed_generation)
{
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Result result;
	char *page;

	if (private_page == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = cluster_undo_block0_pin(logical, expected_root, expected, CLUSTER_UNDO_BLOCK0_SHARED,
								 proof, &pin, &page);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	memcpy(private_page, page, BLCKSZ);
	if (observed_generation != NULL)
		*observed_generation = pin.observed_generation;
	cluster_undo_block0_unpin(&pin);
	return CLUSTER_UNDO_BLOCK0_OK;
}


/*
 * Closed-lane diagnostic/private-recovery copy.  This is deliberately not a
 * resident fill and must never be used by a live origin serve path: it takes
 * the same direct logical content lock only to serialize the disk snapshot
 * against local block-zero modifiers, validates the complete page privately,
 * then releases every local resource before publishing bytes to the caller.
 */
ClusterUndoBlock0Result
cluster_undo_block0_copy_readonly(const ClusterUndoBlock0LogicalKey *logical,
								  const ClusterUndoBlock0ResolvedRoot *read_root,
								  const ClusterUndoBlock0Generation *expected,
								  const ClusterUndoBlock0AuthorityProof *proof,
								  char private_page[BLCKSZ])
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation observed;
	ClusterUndoBlock0OwnedResource *owned;
	char image[BLCKSZ];
	uint32 slotno;

	if (private_page == NULL || expected == NULL || read_root == NULL
		|| cluster_undo_block0_logical_slot(logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(read_root))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (Block0Ctl == NULL)
		return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	if (!block0_authority_proof_valid(logical, proof)
		|| (proof->kind != CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER
			&& proof->kind != CLUSTER_UNDO_BLOCK0_STARTUP_REDO))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;

	owned = block0_resource_prepare(image, CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY);
	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_SHARED);
	if (owned != NULL) {
		owned->slot = slotno;
		block0_resource_link(owned);
	}
	if (!cluster_undo_smgr_read_block(read_root->intent, logical->segment_id,
									  logical->owner_instance, 0, image)) {
		LWLockRelease(&meta->content_lock);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
	if (!block0_page_identity(image, logical, &observed)) {
		LWLockRelease(&meta->content_lock);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}
	if (!cluster_undo_block0_generation_matches(&observed, expected)) {
		LWLockRelease(&meta->content_lock);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	}
	memcpy(private_page, image, BLCKSZ);
	LWLockRelease(&meta->content_lock);
	block0_resource_forget(owned);
	return CLUSTER_UNDO_BLOCK0_OK;
}


ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *expected_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	ClusterUndoBlock0Generation *observed_generation)
{
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Result result;
	char *page;

	if (observed_generation == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = cluster_undo_block0_pin(logical, expected_root, NULL, CLUSTER_UNDO_BLOCK0_SHARED,
								 proof, &pin, &page);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	*observed_generation = pin.observed_generation;
	cluster_undo_block0_unpin(&pin);
	return CLUSTER_UNDO_BLOCK0_OK;
}


ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation_conditional(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *expected_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	ClusterUndoBlock0Generation *observed_generation)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_OK;
	uint32 slotno;
	uint32 state;

	if (observed_generation == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	memset(observed_generation, 0, sizeof(*observed_generation));
	if (Block0Ctl == NULL
		|| cluster_undo_block0_logical_slot(logical, &slotno)
			!= CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(expected_root)
		|| !block0_authority_proof_valid(logical, proof))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

	meta = &Block0Slots[slotno].data;
	if (!LWLockConditionalAcquire(&meta->content_lock, LW_SHARED))
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	state = pg_atomic_read_u32(&meta->state);
	if (state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
		&& state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY)
		result = state == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
			? CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH
			: CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	else if (meta->logical.segment_id != logical->segment_id
		|| meta->logical.owner_instance != logical->owner_instance
		|| !cluster_undo_block0_root_matches(
			&meta->resolved_root, expected_root))
		result = CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	else if (!block0_authority_proof_matches(&meta->proof, proof))
		result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	else
		*observed_generation = meta->generation;
	LWLockRelease(&meta->content_lock);
	return result;
}


ClusterUndoBlock0Result
cluster_undo_block0_prove_strict_empty(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0AuthorityProof *proof)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Result result;
	uint32 slotno;

	if (cluster_undo_block0_logical_slot(logical, &slotno)
		!= CLUSTER_UNDO_BLOCK0_OK)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (!block0_authority_proof_valid(logical, proof)
		|| proof->kind != CLUSTER_UNDO_BLOCK0_LIVE_OWNER
		|| proof->recovery_generation != 0)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (Block0Ctl == NULL)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;

	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_SHARED);
	if (pg_atomic_read_u32(&meta->state) == CLUSTER_UNDO_BLOCK0_SLOT_EMPTY
		&& pg_atomic_read_u32(&meta->pincount) == 0
		&& meta->frame_index == CLUSTER_UNDO_BLOCK0_FRAME_INVALID
		&& XLogRecPtrIsInvalid(meta->last_wal_lsn))
		result = CLUSTER_UNDO_BLOCK0_OK;
	else
		result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	LWLockRelease(&meta->content_lock);

	return result;
}


ClusterUndoBlock0Result
cluster_undo_block0_recovery_private_begin(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *redo_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	bool allow_absent,
	ClusterUndoBlock0RecoveryGuard *guard,
	char private_page[BLCKSZ],
	bool *exists)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0OwnedResource *owned;
	char image[BLCKSZ];
	uint32 slotno;
	uint32 state;

	block0_recovery_guard_clear(guard);
	if (exists != NULL)
		*exists = false;
	if (Block0Ctl == NULL)
		return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	if (guard == NULL || private_page == NULL || exists == NULL || redo_root == NULL
		|| cluster_undo_block0_logical_slot(logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_root_valid(redo_root))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (!block0_authority_proof_valid(logical, proof)
		|| (proof->kind != CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER
			&& proof->kind != CLUSTER_UNDO_BLOCK0_STARTUP_REDO)
		|| redo_root->intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	owned = block0_resource_prepare(guard, CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY);

	meta = &Block0Slots[slotno].data;
	LWLockAcquire(&meta->content_lock, LW_EXCLUSIVE);
	state = pg_atomic_read_u32(&meta->state);
	if (state != CLUSTER_UNDO_BLOCK0_SLOT_EMPTY
		|| pg_atomic_read_u32(&meta->pincount) != 0) {
		LWLockRelease(&meta->content_lock);
		if (owned != NULL)
			pfree(owned);
		return state == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
				   ? CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH
				   : CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	}
	if (owned != NULL) {
		owned->slot = slotno;
		block0_resource_link(owned);
	}

	/*
	 * The current smgr read seam reports absent, short read and I/O failure as
	 * one false value.  Until it grows a typed result, allow_absent cannot be
	 * admitted without turning a real I/O failure into a false no-op.
	 */
	(void)allow_absent;
	if (!cluster_undo_smgr_read_block(redo_root->intent, logical->segment_id,
									  logical->owner_instance, 0, image)) {
		LWLockRelease(&meta->content_lock);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
	if (!block0_page_identity(image, logical, &generation)) {
		LWLockRelease(&meta->content_lock);
		block0_resource_forget(owned);
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	}

	guard->slot = (int)slotno;
	guard->logical = *logical;
	guard->resolved_root = *redo_root;
	guard->proof = *proof;
	guard->content_x_held = true;
	memcpy(private_page, image, BLCKSZ);
	*exists = true;
	return CLUSTER_UNDO_BLOCK0_OK;
}


void
cluster_undo_block0_recovery_private_finish(ClusterUndoBlock0RecoveryGuard *guard,
											const char *successor_page,
											XLogRecPtr replay_lsn,
											bool write_image,
											bool fsync_parent)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation successor_generation;
	ClusterUndoBlock0OwnedResource *owned;
	uint32 slotno;

	if (Block0Ctl == NULL || guard == NULL || !guard->content_x_held || guard->slot < 0
		|| guard->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT
		|| XLogRecPtrIsInvalid(replay_lsn)
		|| cluster_undo_block0_logical_slot(&guard->logical, &slotno)
			   != CLUSTER_UNDO_BLOCK0_OK
		|| slotno != (uint32)guard->slot
		|| !cluster_undo_block0_root_valid(&guard->resolved_root)
		|| !block0_authority_proof_valid(&guard->logical, &guard->proof)
		|| (guard->proof.kind != CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER
			&& guard->proof.kind != CLUSTER_UNDO_BLOCK0_STARTUP_REDO)
		|| guard->resolved_root.intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0)
		ereport(PANIC, (errmsg("invalid undo block-zero recovery-private finish guard")));

	meta = &Block0Slots[slotno].data;
	owned = block0_resource_find(guard, CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY);
	if (pg_atomic_read_u32(&meta->state) != CLUSTER_UNDO_BLOCK0_SLOT_EMPTY
		|| pg_atomic_read_u32(&meta->pincount) != 0)
		ereport(PANIC,
				(errmsg("undo block-zero recovery-private finish found resident state")));

	if (write_image) {
		if (successor_page == NULL
			|| !block0_page_identity(successor_page, &guard->logical,
									 &successor_generation))
			ereport(PANIC,
					(errmsg("invalid undo block-zero recovery-private successor image")));
		if (fsync_parent)
			ereport(PANIC,
					(errmsg("undo block-zero recovery parent-directory fsync is unavailable")));
		if (!cluster_undo_smgr_write_block(guard->resolved_root.intent,
									  guard->logical.segment_id,
									  guard->logical.owner_instance, 0,
									  successor_page, true))
			ereport(PANIC, (errmsg("could not durably apply recovery-private block zero")));
	}

	LWLockRelease(&meta->content_lock);
	block0_resource_forget(owned);
	block0_recovery_guard_clear(guard);
}


void
cluster_undo_block0_recovery_private_abort(ClusterUndoBlock0RecoveryGuard *guard)
{
	ClusterUndoBlock0OwnedResource *owned;

	if (guard == NULL)
		return;
	owned = block0_resource_find(guard, CLUSTER_UNDO_BLOCK0_OWNED_RECOVERY);
	if (Block0Ctl != NULL && guard->content_x_held && guard->slot >= 0
		&& guard->slot < CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		LWLockRelease(&Block0Slots[guard->slot].data.content_lock);
	block0_resource_forget(owned);
	block0_recovery_guard_clear(guard);
}


/*
 * Recheck the caller-owned complete post-replay census against every direct
 * logical slot.  The ordered direct-slot walk makes duplicates, omissions and
 * extra resident bindings fail closed without allocating a shadow bitmap.
 * This proves only local clean residency; ROOT/SIDE/PAGE and post-job/I/O
 * closure remain separate READY prerequisites.
 */
bool
cluster_undo_block0_verify_clean_census(
	const ClusterUndoBlock0ResidentCensusItem *items, uint32 count)
{
	uint32 item_index = 0;
	uint32 slotno;

	if (Block0Ctl == NULL || (count > 0 && items == NULL)
		|| count > CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		return false;

	for (slotno = 0; slotno < CLUSTER_UNDO_BLOCK0_SLOT_COUNT; slotno++) {
		ClusterUndoBlock0SlotData *meta = &Block0Slots[slotno].data;
		uint32 state;
		bool matched = false;

		LWLockAcquire(&meta->content_lock, LW_SHARED);
		state = pg_atomic_read_u32(&meta->state);
		if (item_index < count) {
			const ClusterUndoBlock0ResidentCensusItem *item
				= &items[item_index];
			uint32 expected_slot;

			if (cluster_undo_block0_logical_slot(&item->logical, &expected_slot)
					== CLUSTER_UNDO_BLOCK0_OK
				&& expected_slot == slotno
				&& state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
				&& pg_atomic_read_u32(&meta->pincount) == 0
				&& meta->frame_index < Block0Ctl->frame_count
				&& XLogRecPtrIsInvalid(meta->last_wal_lsn)
				&& meta->logical.segment_id == item->logical.segment_id
				&& meta->logical.owner_instance == item->logical.owner_instance
				&& cluster_undo_block0_root_matches(
					&meta->resolved_root, &item->resolved_root)
				&& cluster_undo_block0_generation_matches(
					&meta->generation, &item->generation)
				&& item->generation.known
				&& block0_authority_proof_matches(&meta->proof, &item->proof))
				matched = true;
		}

		if (matched)
			item_index++;
		else if (state != CLUSTER_UNDO_BLOCK0_SLOT_EMPTY || pg_atomic_read_u32(&meta->pincount) != 0
				 || meta->frame_index != CLUSTER_UNDO_BLOCK0_FRAME_INVALID
				 || !XLogRecPtrIsInvalid(meta->last_wal_lsn)) {
			LWLockRelease(&meta->content_lock);
			return false;
		}
		LWLockRelease(&meta->content_lock);
	}

	return item_index == count;
}

bool
cluster_undo_block0_normal_start_empty(void)
{
	bool empty;
	if (!AmStartupProcess() || Block0Ctl == NULL || !cluster_semantic_normal_start_closed()
		|| !cluster_undo_block0_current_startup_fenced_owned()
		|| cluster_undo_block0_backend_has_resources())
		return false;
	LWLockAcquire(&Block0Ctl->frame_lock, LW_SHARED);
	empty = Block0Ctl->free_count == Block0Ctl->frame_count;
	LWLockRelease(&Block0Ctl->frame_lock);
	return empty && cluster_undo_block0_verify_clean_census(NULL, 0)
		   && cluster_semantic_normal_start_closed();
}

/* The one Startup executor saves this exact item and frame before admission.
 * A failed pass may return only its own clean, unpinned prefix, while every
 * serving path remains closed. No live caller can evict a resident binding. */
bool
cluster_undo_block0_normal_start_discard(const ClusterUndoBlock0ResidentCensusItem *item,
										 uint32 frame_index)
{
	ClusterUndoBlock0SlotData *meta;
	uint32 slotno, i;
	bool exact;
	if (!AmStartupProcess() || Block0Ctl == NULL || item == NULL
		|| !cluster_semantic_normal_start_closed()
		|| !cluster_undo_block0_current_startup_fenced_owned() || cluster_node_id < 0
		|| cluster_node_id >= UNDO_OWNER_INSTANCE_MAX
		|| item->logical.owner_instance != cluster_node_id + 1
		|| item->resolved_root.intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED || !item->generation.known
		|| item->generation.value == UINT32_MAX
		|| item->proof.kind != CLUSTER_UNDO_BLOCK0_LIVE_OWNER
		|| item->proof.owner_instance != item->logical.owner_instance
		|| !item->proof.cluster_epoch_present || item->proof.cluster_epoch != 0
		|| item->proof.recovery_generation != 0 || frame_index >= Block0Ctl->frame_count
		|| cluster_undo_block0_logical_slot(&item->logical, &slotno) != CLUSTER_UNDO_BLOCK0_OK)
		return false;
	meta = &Block0Slots[slotno].data;
	/* In particular, never wait on a pin/content lock we do not own. */
	if (pg_atomic_read_u32(&meta->pincount) != 0
		|| !LWLockConditionalAcquire(&meta->content_lock, LW_EXCLUSIVE))
		return false;
	exact = pg_atomic_read_u32(&meta->state) == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
			&& pg_atomic_read_u32(&meta->pincount) == 0 && meta->frame_index == frame_index
			&& XLogRecPtrIsInvalid(meta->last_wal_lsn)
			&& meta->logical.segment_id == item->logical.segment_id
			&& meta->logical.owner_instance == item->logical.owner_instance
			&& cluster_undo_block0_root_matches(&meta->resolved_root, &item->resolved_root)
			&& cluster_undo_block0_generation_matches(&meta->generation, &item->generation)
			&& block0_authority_proof_matches(&meta->proof, &item->proof);
	if (!exact) {
		LWLockRelease(&meta->content_lock);
		return false;
	}
	LWLockAcquire(&Block0Ctl->frame_lock, LW_EXCLUSIVE);
	exact
		= Block0Ctl->free_count < Block0Ctl->frame_count && cluster_semantic_normal_start_closed();
	for (i = 0; exact && i < Block0Ctl->free_count; i++)
		if (Block0FreeFrames[i] == frame_index)
			exact = false;
	if (exact) {
		memset(&meta->logical, 0, sizeof(meta->logical));
		memset(&meta->resolved_root, 0, sizeof(meta->resolved_root));
		memset(&meta->generation, 0, sizeof(meta->generation));
		memset(&meta->proof, 0, sizeof(meta->proof));
		meta->frame_index = CLUSTER_UNDO_BLOCK0_FRAME_INVALID;
		meta->last_wal_lsn = InvalidXLogRecPtr;
		pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_EMPTY);
		Block0FreeFrames[Block0Ctl->free_count++] = frame_index;
	}
	LWLockRelease(&Block0Ctl->frame_lock);
	LWLockRelease(&meta->content_lock);
	return exact;
}

typedef struct Block0StopObservation {
	ClusterNormalStopPollResult result;
	uint32 segment;
	int tt_slot;
	const char *reason;
} Block0StopObservation;

static void
block0_normal_stop_note(Block0StopObservation *observation, ClusterNormalStopPollResult result,
						uint32 segment, int tt_slot, const char *reason)
{
	if (result == CLUSTER_NORMAL_STOP_READY || observation->result == CLUSTER_NORMAL_STOP_INVALID
		|| (observation->result == CLUSTER_NORMAL_STOP_PENDING
			&& result == CLUSTER_NORMAL_STOP_PENDING))
		return;
	observation->result = result;
	observation->segment = segment;
	observation->tt_slot = tt_slot;
	observation->reason = reason;
}

/*
 * Observe every original resident owner, including TT slots in rolled-away
 * segments no longer represented by the bounded TT allocator.  A valid
 * resident binding is not evicted by normal runtime; only unfinished fill /
 * provision cleanup returns its frame.  Startup's complete namespace census
 * is nevertheless a SEPARATE prerequisite: an empty resident slot says
 * nothing about an unopened old file.  This routine grants no read/mutation
 * authority and performs no I/O, publication, pin release or TT transition.
 *
 * Do not queue behind a content-X owner: the coordinator must leave its
 * original completion path running.  Pool and slot locks are never nested.
 * Non-atomic pool accounting is only a conservative pending observation;
 * stability still requires the controller's producer and service seals.
 */
ClusterNormalStopPollResult
cluster_undo_block0_normal_stop_poll(
	bool post_checkpoint, const uint8 root_descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	uint64 expected_epoch, uint32 *segment_out, int *tt_slot_out, const char **reason_out)
{
	Block0StopObservation observation
		= { CLUSTER_NORMAL_STOP_INVALID, 0, -1, "UNDO_BLOCK0_STOP_STATE_INVALID" };
	ClusterUndoRootDescriptorV1 descriptor;
	uint32 frame_count;
	uint32 free_before;
	uint32 resident_count = 0;
	uint32 slotno;

	if (!IsUnderPostmaster
		|| (MyBackendType != B_CHECKPOINTER && !(post_checkpoint && MyBackendType == B_LMON))
		|| Block0Ctl == NULL || Block0Slots == NULL || Block0Frames == NULL
		|| Block0FreeFrames == NULL || root_descriptor == NULL
		|| cluster_undo_root_descriptor_decode(root_descriptor, GetSystemIdentifier(), &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED)
		goto done;
	/* This process must first return its own original ResourceOwner handles;
	 * in particular it may itself hold a slot's content-X lock. */
	if (cluster_undo_block0_backend_has_resources()) {
		observation.result = CLUSTER_NORMAL_STOP_PENDING;
		observation.reason = "UNDO_BLOCK0_PRIVATE_OWNER_PENDING";
		goto done;
	}
	LWLockAcquire(&Block0Ctl->frame_lock, LW_SHARED);
	frame_count = Block0Ctl->frame_count;
	free_before = Block0Ctl->free_count;
	LWLockRelease(&Block0Ctl->frame_lock);
	if (frame_count == 0 || free_before > frame_count)
		goto done;
	observation.result = CLUSTER_NORMAL_STOP_READY;
	observation.reason = "NONE";
	for (slotno = 0; slotno < CLUSTER_UNDO_BLOCK0_SLOT_COUNT; slotno++) {
		ClusterUndoBlock0SlotData *meta = &Block0Slots[slotno].data;
		ClusterUndoBlock0Generation page_generation;
		ClusterUndoBlock0ResolvedRoot expected_root;
		const UndoSegmentHeaderData *header;
		uint32 logical_slot;
		uint32 state;
		uint32 pins;
		int i;

		if (!LWLockConditionalAcquire(&meta->content_lock, LW_SHARED)) {
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, -1,
									"UNDO_BLOCK0_CONTENT_BUSY");
			continue;
		}
		state = pg_atomic_read_u32(&meta->state);
		pins = pg_atomic_read_u32(&meta->pincount);
		if (state == CLUSTER_UNDO_BLOCK0_SLOT_EMPTY) {
			/* Failed fill may retain identity fields, but never frame/pin/I/O. */
			if (pins != 0 || meta->frame_index != CLUSTER_UNDO_BLOCK0_FRAME_INVALID
				|| !XLogRecPtrIsInvalid(meta->last_wal_lsn))
				block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_INVALID, slotno + 1, -1,
										"UNDO_BLOCK0_EMPTY_HAS_OWNER");
			goto next_slot;
		}
		if (state > CLUSTER_UNDO_BLOCK0_SLOT_RETIRING || meta->frame_index >= frame_count
			|| cluster_undo_block0_logical_slot(&meta->logical, &logical_slot)
				   != CLUSTER_UNDO_BLOCK0_OK
			|| logical_slot != slotno
			|| !cluster_undo_root_descriptor_resolve(&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
													 meta->logical.owner_instance,
													 meta->logical.segment_id, &expected_root)
			|| !cluster_undo_block0_root_matches(&meta->resolved_root, &expected_root)
			|| !block0_authority_proof_valid(&meta->logical, &meta->proof)
			|| meta->proof.kind != CLUSTER_UNDO_BLOCK0_LIVE_OWNER
			|| meta->proof.cluster_epoch != expected_epoch
			|| meta->proof.recovery_generation != 0) {
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_INVALID, slotno + 1, -1,
									"UNDO_BLOCK0_RESIDENT_IDENTITY_INVALID");
			goto next_slot;
		}
		resident_count++;
		if (state == CLUSTER_UNDO_BLOCK0_SLOT_FILLING
			|| state == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING) {
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, -1,
									"UNDO_BLOCK0_PUBLICATION_PENDING");
			goto next_slot;
		}
		header = (const UndoSegmentHeaderData *)BLOCK0_FRAME_DATA(meta->frame_index);
		if (!meta->generation.known || meta->generation.value == UINT32_MAX
			|| !block0_page_identity((const char *)header, &meta->logical, &page_generation)
			|| !cluster_undo_block0_generation_matches(&page_generation, &meta->generation)
			|| (state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN)
				   != XLogRecPtrIsInvalid(meta->last_wal_lsn)) {
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_INVALID, slotno + 1, -1,
									"UNDO_BLOCK0_RESIDENT_IMAGE_INVALID");
			goto next_slot;
		}
		if (pins != 0)
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, -1,
									"UNDO_BLOCK0_PIN_PENDING");
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *tt = &header->tt_slots[i];
			const TTSlot zero = { 0 };
			bool valid;

			if (tt->status == TT_SLOT_UNUSED)
				valid = memcmp(tt, &zero, sizeof(zero)) == 0;
			else
				valid = TransactionIdIsNormal(tt->xid) && tt->wrap != TT_WRAP_INVALID
						&& tt->status <= TT_SLOT_RECYCLABLE
						&& (tt->flags & ~TT_SLOT_FLAGS_KNOWN) == 0
						&& ((tt->status == TT_SLOT_COMMITTED && SCN_VALID(tt->commit_scn))
							|| (tt->status != TT_SLOT_COMMITTED && tt->commit_scn == InvalidScn))
						&& (tt->status != TT_SLOT_ACTIVE || tt->flags == TT_FLAGS_RESERVED);
			if (!valid)
				block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_INVALID, slotno + 1, i,
										"UNDO_BLOCK0_TT_SHAPE_INVALID");
			else if (tt->status == TT_SLOT_ACTIVE)
				block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, i,
										"UNDO_BLOCK0_TT_ACTIVE");
			else if (!UBA_is_invalid(tt->first_undo_block))
				block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, i,
										"UNDO_BLOCK0_TT_UNDO_PENDING");
		}
		if (post_checkpoint && state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY)
			block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, slotno + 1, -1,
									"UNDO_BLOCK0_DURABILITY_PENDING");
	next_slot:
		LWLockRelease(&meta->content_lock);
		if (observation.result == CLUSTER_NORMAL_STOP_INVALID)
			break;
	}
	LWLockAcquire(&Block0Ctl->frame_lock, LW_SHARED);
	if (Block0Ctl->frame_count != frame_count || Block0Ctl->free_count > frame_count)
		block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_INVALID, 0, -1,
								"UNDO_BLOCK0_FRAME_POOL_INVALID");
	else if (Block0Ctl->free_count != free_before
			 || (uint64)free_before + resident_count != frame_count)
		block0_normal_stop_note(&observation, CLUSTER_NORMAL_STOP_PENDING, 0, -1,
								"UNDO_BLOCK0_FRAME_OWNER_PENDING");
	LWLockRelease(&Block0Ctl->frame_lock);
done:
	if (segment_out != NULL)
		*segment_out = observation.segment;
	if (tt_slot_out != NULL)
		*tt_slot_out = observation.tt_slot;
	if (reason_out != NULL)
		*reason_out = observation.reason;
	return observation.result;
}


void
cluster_undo_block0_mark_wal_dirty(ClusterUndoBlock0Pin *pin, XLogRecPtr wal_lsn)
{
	ClusterUndoBlock0SlotData *meta;
	uint32 state;

	Assert(Block0Ctl != NULL);
	Assert(pin != NULL && pin->slot >= 0
		   && pin->slot < CLUSTER_UNDO_BLOCK0_SLOT_COUNT);
	Assert(pin->mode == CLUSTER_UNDO_BLOCK0_EXCLUSIVE);
	Assert(!XLogRecPtrIsInvalid(wal_lsn));
	meta = &Block0Slots[pin->slot].data;
	state = pg_atomic_read_u32(&meta->state);
	Assert(state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
		   || state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY);
	if (XLogRecPtrIsInvalid(meta->last_wal_lsn) || wal_lsn > meta->last_wal_lsn)
		meta->last_wal_lsn = wal_lsn;
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY);
}


void
cluster_undo_block0_flush_sync(ClusterUndoBlock0Pin *pin, const char *successor_page,
								   XLogRecPtr required_wal_lsn, bool fsync_parent)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0Generation successor_generation;
	XLogRecPtr flush_lsn;
	uint32 state;

	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0
		|| pin->slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT
		|| pin->mode != CLUSTER_UNDO_BLOCK0_EXCLUSIVE || successor_page == NULL)
		ereport(ERROR, (errmsg("invalid undo block-zero synchronous flush owner")));
	if (fsync_parent)
		ereport(ERROR, (errmsg("undo block-zero parent-directory fsync is unavailable")));

	meta = &Block0Slots[pin->slot].data;
	if (!block0_page_identity(successor_page, &meta->logical,
							  &successor_generation))
		ereport(ERROR,
				(errmsg("invalid undo block-zero synchronous flush successor image")));
	state = pg_atomic_read_u32(&meta->state);
	if (state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
		&& state != CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY)
		ereport(ERROR, (errmsg("undo block-zero synchronous flush found invalid resident state")));
	if (XLogRecPtrIsInvalid(required_wal_lsn)
		&& state == CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY)
		ereport(ERROR, (errmsg("undo block-zero dirty flush requires a valid WAL LSN")));

	flush_lsn = required_wal_lsn;
	if (!XLogRecPtrIsInvalid(meta->last_wal_lsn)
		&& (XLogRecPtrIsInvalid(flush_lsn) || meta->last_wal_lsn > flush_lsn))
		flush_lsn = meta->last_wal_lsn;
	if (!XLogRecPtrIsInvalid(flush_lsn))
		XLogFlush(flush_lsn);
	if (!cluster_undo_smgr_write_block(meta->resolved_root.intent, meta->logical.segment_id,
									  meta->logical.owner_instance, 0, successor_page, true))
		ereport(ERROR, (errmsg("could not flush undo block zero")));

	memcpy(BLOCK0_FRAME_DATA(meta->frame_index), successor_page, BLCKSZ);
	meta->generation = successor_generation;
	pin->observed_generation = successor_generation;
	meta->last_wal_lsn = InvalidXLogRecPtr;
	pg_atomic_write_u32(&meta->state, CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN);
}


void
cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin)
{
	ClusterUndoBlock0SlotData *meta;
	ClusterUndoBlock0OwnedResource *owned;

	if (Block0Ctl == NULL || pin == NULL || pin->slot < 0)
		return;
	owned = block0_resource_find(pin, CLUSTER_UNDO_BLOCK0_OWNED_LOCKED_PIN);
	meta = &Block0Slots[pin->slot].data;
	block0_release_content_lock(meta);
	pg_atomic_fetch_sub_u32(&meta->pincount, 1);
	block0_resource_forget(owned);
	block0_pin_clear(pin);
}
