/*-------------------------------------------------------------------------
 *
 * cluster_pcm_x_convert.c
 *	  PCM-X checked five-pool external shared-memory substrate.
 *
 *	  The region is self-describing and uses offsets and slot indexes only.
 *	  Creation initializes six allocators over five logical pools because the
 *	  local membership pool has fixed wait and holder partitions.  Attach
 *	  validates the complete immutable layout before publishing the local
 *	  region pointer.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pcm_x_convert.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Exported symbols use the
 *	  cluster_pcm_x prefix.  Queue state transitions and message dispatch are
 *	  intentionally outside this substrate.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "port/pg_crc32c.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_shmem.h"


/* Process-local pointer published only after create/attach validation. */
PcmXShmemHeader *ClusterPcmXConvertShmem = NULL;


StaticAssertDecl(offsetof(PcmXSlotHeader, next_free) == 0, "PCM-X common free link must be first");
StaticAssertDecl(offsetof(PcmXMasterTagSlot, slot) == 0,
				 "PCM-X master-tag slot header must be first");
StaticAssertDecl(offsetof(PcmXMasterTicketSlot, slot) == 0,
				 "PCM-X master-ticket slot header must be first");
StaticAssertDecl(offsetof(PcmXBlockerSlot, slot) == 0, "PCM-X blocker slot header must be first");
StaticAssertDecl(offsetof(PcmXLocalTagSlot, slot) == 0,
				 "PCM-X local-tag slot header must be first");
StaticAssertDecl(offsetof(PcmXLocalMembershipSlot, slot) == 0,
				 "PCM-X local-membership slot header must be first");
StaticAssertDecl((PCM_X_LOCK_PARTITIONS & (PCM_X_LOCK_PARTITIONS - 1)) == 0,
				 "PCM-X lock partition count must be a power of two");
StaticAssertDecl(PCM_X_RUNTIME_SHUTTING_DOWN < PCM_X_RUNTIME_GATE_STATE_MASK,
				 "PCM-X runtime states must fit in the packed gate");


/* Private fail-closed phase used to serialize session publication. */
#define PCM_X_RUNTIME_GATE_ACTIVATING 3U
#define PCM_X_RUNTIME_GATE_EXHAUSTED 4U
#define PCM_X_SLOT_GENERATION_READ_RETRIES 16
#define PCM_X_STATE_BIT(state) (UINT32_C(1) << (state))
#define PCM_X_MASTER_TICKET_DOMAIN_STATES                                                          \
	(PCM_X_STATE_BIT(PCM_XT_ADMITTING) | PCM_X_STATE_BIT(PCM_XT_QUEUED)                            \
	 | PCM_X_STATE_BIT(PCM_XT_ACTIVE_PROBE) | PCM_X_STATE_BIT(PCM_XT_ACTIVE_TRANSFER)              \
	 | PCM_X_STATE_BIT(PCM_XT_COMPLETE) | PCM_X_STATE_BIT(PCM_XT_CANCELLED)                        \
	 | PCM_X_STATE_BIT(PCM_XT_RETIRE_CREDIT) | PCM_X_STATE_BIT(PCM_XT_RECOVERY_BLOCKED))
#define PCM_X_LOCAL_MEMBER_DOMAIN_STATES                                                           \
	(PCM_X_STATE_BIT(PCM_XL_JOINED_NONWAITABLE) | PCM_X_STATE_BIT(PCM_XL_WAITABLE_FOLLOWER)        \
	 | PCM_X_STATE_BIT(PCM_XL_NODE_LEADER) | PCM_X_STATE_BIT(PCM_XL_REMOTE_WAIT)                   \
	 | PCM_X_STATE_BIT(PCM_XL_CONTENT_ACTIVE) | PCM_X_STATE_BIT(PCM_XL_GRANTED)                    \
	 | PCM_X_STATE_BIT(PCM_XL_CANCELLED) | PCM_X_STATE_BIT(PCM_XL_RECOVERY_BLOCKED))
#define PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES                                                        \
	(PCM_X_STATE_BIT(PCM_XL_HOLDER_ACQUIRING) | PCM_X_STATE_BIT(PCM_XL_HOLDER_ACTIVE)              \
	 | PCM_X_STATE_BIT(PCM_XL_HOLDER_RELEASING))
StaticAssertDecl(PCM_X_RUNTIME_GATE_ACTIVATING > PCM_X_RUNTIME_SHUTTING_DOWN,
				 "PCM-X activating phase must not overlap public runtime states");
StaticAssertDecl(PCM_X_RUNTIME_GATE_EXHAUSTED > PCM_X_RUNTIME_SHUTTING_DOWN,
				 "PCM-X exhausted phase must not overlap public runtime states");
StaticAssertDecl(PCM_X_RUNTIME_GATE_ACTIVATING <= PCM_X_RUNTIME_GATE_STATE_MASK,
				 "PCM-X activating phase must fit in the packed gate");
StaticAssertDecl(PCM_X_RUNTIME_GATE_EXHAUSTED <= PCM_X_RUNTIME_GATE_STATE_MASK,
				 "PCM-X exhausted phase must fit in the packed gate");


static Size pcm_x_maxalign(Size value);
static Size pcm_x_append_array(Size *cursor, Size capacity, Size element_size);
static void pcm_x_set_pool(PcmXShmemLayout *layout, PcmXPoolKind kind, Size capacity,
						   Size slot_size, bool has_directory, Size *cursor);
static void pcm_x_compute_runtime_layout(PcmXShmemLayout *layout);
static void pcm_x_init_free_list(char *base, Size slots_offset, Size slot_size,
								 Size first_slot_index, Size capacity,
								 PcmXAllocatorState *allocator);
static void pcm_x_init_allocators(PcmXShmemHeader *header);
static void pcm_x_init_stats(PcmXStats *stats);
static void pcm_x_stats_increment(pg_atomic_uint64 *counter);
static bool pcm_x_stats_depth_increment(PcmXShmemHeader *header);
static bool pcm_x_stats_depth_decrement(PcmXShmemHeader *header);
static bool pcm_x_master_terminal_leg_is_clear(const PcmXReliableLegState *leg);
static bool pcm_x_master_blocker_stage_metadata_valid(const PcmXMasterTicketSlot *ticket);
static void pcm_x_master_blocker_snapshot_clear(PcmXMasterBlockerSnapshot *snapshot);
static bool pcm_x_ticket_ref_is_zero(const PcmXTicketRef *ref);
static bool pcm_x_local_terminal_ref_valid(const PcmXTicketRef *ref);
static bool pcm_x_local_admission_ref_valid(const PcmXTicketRef *ref,
											const PcmXWaitIdentity *identity);
static bool pcm_x_local_terminal_unlinked_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
												 PcmXLocalMembershipSlot *terminal,
												 PcmXSlotRef terminal_ref);


static inline uint32
pcm_x_runtime_gate_pack(uint32 generation, uint32 state)
{
	Assert(generation <= PCM_X_RUNTIME_GATE_GENERATION_MAX);
	Assert(state <= PCM_X_RUNTIME_GATE_STATE_MASK);
	return (generation << PCM_X_RUNTIME_GATE_STATE_BITS) | state;
}

static inline uint32
pcm_x_runtime_gate_generation(uint32 gate)
{
	return gate >> PCM_X_RUNTIME_GATE_STATE_BITS;
}

static inline uint32
pcm_x_runtime_gate_state(uint32 gate)
{
	return gate & PCM_X_RUNTIME_GATE_STATE_MASK;
}


/* Align only after a checked addition proves MAXALIGN cannot wrap. */
static Size
pcm_x_maxalign(Size value)
{
	(void)add_size(value, MAXIMUM_ALIGNOF - 1);
	return MAXALIGN(value);
}

/* Append one aligned fixed-record array and return its region offset. */
static Size
pcm_x_append_array(Size *cursor, Size capacity, Size element_size)
{
	Size offset;
	Size bytes;

	offset = pcm_x_maxalign(*cursor);
	bytes = mul_size(capacity, element_size);
	*cursor = add_size(offset, bytes);
	return offset;
}

/* Populate one logical pool and append its optional bounded directory. */
static void
pcm_x_set_pool(PcmXShmemLayout *layout, PcmXPoolKind kind, Size capacity, Size slot_size,
			   bool has_directory, Size *cursor)
{
	PcmXPoolLayout *pool = &layout->pools[kind];

	pool->slots_offset = pcm_x_append_array(cursor, capacity, slot_size);
	pool->slot_size = slot_size;
	pool->capacity = capacity;
	if (has_directory) {
		pool->directory_capacity = mul_size(capacity, 2);
		pool->directory_offset
			= pcm_x_append_array(cursor, pool->directory_capacity, sizeof(PcmXDirectoryEntry));
	}
}


/*
 * cluster_pcm_x_layout_compute -- Compute the complete immutable layout.
 *
 * All capacity arithmetic and every byte/offset step uses PostgreSQL's
 * checked shared-memory helpers.  An overflow raises ERROR before the
 * resulting offset can be used.
 *
 * Inputs:
 *	max_backends: configured backend process count.
 *	auxiliary_procs: fixed auxiliary process count.
 *	n_buffers: shared buffer count.
 *	max_wait_edges: configured WFG edge bound.
 *	layout: writable output object.
 *
 * Returns:
 *	Nothing.  On success, layout contains the complete region description.
 *
 * Side Effects:
 *	Raises ERROR for a null output or checked-arithmetic overflow.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_pcm_x_layout_compute(Size max_backends, Size auxiliary_procs, Size n_buffers,
							 Size max_wait_edges, PcmXShmemLayout *layout)
{
	Size cursor;
	Size blocker_capacity;
	Size local_tag_capacity;
	Size membership_capacity;
	Size master_ticket_directory_capacity;
	PcmXPoolLayout *membership;

	if (layout == NULL)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("PCM-X shared memory layout output is null"),
						errhint("Pass a writable PcmXShmemLayout object.")));

	memset(layout, 0, sizeof(*layout));
	layout->magic = PCM_X_SHMEM_MAGIC;
	layout->version = PCM_X_SHMEM_LAYOUT_VERSION;
	layout->pool_count = PCM_X_POOL_COUNT;
	layout->allocator_count = PCM_X_ALLOC_COUNT;
	layout->lock_partition_count = PCM_X_LOCK_PARTITIONS;
	layout->lwlock_count = PCM_X_LWLOCK_COUNT;
	layout->max_backends = max_backends;
	layout->auxiliary_procs = auxiliary_procs;
	layout->n_buffers = n_buffers;
	layout->max_wait_edges = max_wait_edges;
	layout->process_capacity = add_size(max_backends, auxiliary_procs);
	layout->locks_per_process = Min(n_buffers, (Size)LWLOCK_MAX_HELD_BY_PROC);
	layout->active_holder_capacity = mul_size(layout->process_capacity, layout->locks_per_process);
	layout->holder_tag_capacity = Min(n_buffers, layout->active_holder_capacity);
	layout->node_ticket_capacity
		= mul_size(layout->process_capacity, (Size)PCM_X_PROTOCOL_NODE_LIMIT);
	master_ticket_directory_capacity = mul_size(layout->node_ticket_capacity, 2);

	blocker_capacity = mul_size(max_wait_edges, 2);
	local_tag_capacity = add_size(layout->node_ticket_capacity, layout->holder_tag_capacity);
	membership_capacity = add_size(layout->process_capacity, layout->active_holder_capacity);

	layout->header_size = pcm_x_maxalign(sizeof(PcmXShmemHeader));
	cursor = layout->header_size;
	pcm_x_set_pool(layout, PCM_X_POOL_MASTER_TAG, layout->node_ticket_capacity,
				   sizeof(PcmXMasterTagSlot), true, &cursor);
	pcm_x_set_pool(layout, PCM_X_POOL_MASTER_TICKET, layout->node_ticket_capacity,
				   sizeof(PcmXMasterTicketSlot), false, &cursor);
	layout->master_ticket_directories.prehandle_capacity = master_ticket_directory_capacity;
	layout->master_ticket_directories.prehandle_offset = pcm_x_append_array(
		&cursor, layout->master_ticket_directories.prehandle_capacity, sizeof(PcmXDirectoryEntry));
	layout->master_ticket_directories.handle_capacity = master_ticket_directory_capacity;
	layout->master_ticket_directories.handle_offset = pcm_x_append_array(
		&cursor, layout->master_ticket_directories.handle_capacity, sizeof(PcmXDirectoryEntry));
	layout->master_ticket_directories.retire_capacity = master_ticket_directory_capacity;
	layout->master_ticket_directories.retire_offset = pcm_x_append_array(
		&cursor, layout->master_ticket_directories.retire_capacity, sizeof(PcmXDirectoryEntry));
	pcm_x_set_pool(layout, PCM_X_POOL_BLOCKER, blocker_capacity, sizeof(PcmXBlockerSlot), false,
				   &cursor);
	pcm_x_set_pool(layout, PCM_X_POOL_LOCAL_TAG, local_tag_capacity, sizeof(PcmXLocalTagSlot), true,
				   &cursor);
	pcm_x_set_pool(layout, PCM_X_POOL_LOCAL_MEMBERSHIP, membership_capacity,
				   sizeof(PcmXLocalMembershipSlot), false, &cursor);

	membership = &layout->pools[PCM_X_POOL_LOCAL_MEMBERSHIP];
	layout->local_wait.first_slot_index = 0;
	layout->local_wait.slots_offset = membership->slots_offset;
	layout->local_wait.capacity = layout->process_capacity;
	layout->local_wait.directory_capacity = mul_size(layout->local_wait.capacity, 2);
	layout->local_wait.directory_offset = pcm_x_append_array(
		&cursor, layout->local_wait.directory_capacity, sizeof(PcmXDirectoryEntry));

	layout->local_holder.first_slot_index = layout->process_capacity;
	layout->local_holder.slots_offset = add_size(
		membership->slots_offset, mul_size(layout->local_wait.capacity, membership->slot_size));
	layout->local_holder.capacity = layout->active_holder_capacity;
	layout->local_holder.directory_capacity = mul_size(layout->local_holder.capacity, 2);
	layout->local_holder.directory_offset = pcm_x_append_array(
		&cursor, layout->local_holder.directory_capacity, sizeof(PcmXDirectoryEntry));
	layout->total_size = pcm_x_maxalign(cursor);
}


/*
 * cluster_pcm_x_validate_attach -- Validate an existing region description.
 *
 * Inputs:
 *	header: existing shared-memory header.
 *	expected: layout computed from this process's runtime configuration.
 *
 * Returns:
 *	PCM_X_ATTACH_OK for an exact match, or a specific mismatch result.
 *
 * Side Effects:
 *	None.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
PcmXAttachResult
cluster_pcm_x_validate_attach(const PcmXShmemHeader *header, const PcmXShmemLayout *expected)
{
	if (header == NULL || expected == NULL)
		return PCM_X_ATTACH_NULL;
	if (header->layout.magic != expected->magic)
		return PCM_X_ATTACH_MAGIC_MISMATCH;
	if (header->layout.version != expected->version)
		return PCM_X_ATTACH_VERSION_MISMATCH;
	if (memcmp(&header->layout, expected, sizeof(*expected)) != 0)
		return PCM_X_ATTACH_LAYOUT_MISMATCH;
	return PCM_X_ATTACH_OK;
}


/*
 * cluster_pcm_x_generation_next -- Advance a bounded generation counter.
 *
 * Inputs:
 *	current: current generation; zero is valid.
 *	next: writable output for the successor.
 *
 * Returns:
 *	True after storing the successor.  False for a null output or UINT64_MAX;
 *	in that case, caller storage is unchanged.
 *
 * Side Effects:
 *	None beyond writing *next on success.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
bool
cluster_pcm_x_generation_next(uint64 current, uint64 *next)
{
	if (next == NULL || current == UINT64_MAX)
		return false;
	*next = current + 1;
	return true;
}


static bool
pcm_x_slot_generation_read(const PcmXSlotHeader *slot, uint64 *generation_out)
{
	uint32 after;
	uint32 before;
	uint64 generation;
	int retry;

	if (slot == NULL || generation_out == NULL)
		return false;
	*generation_out = 0;
	for (retry = 0; retry < PCM_X_SLOT_GENERATION_READ_RETRIES; retry++) {
		before = pg_atomic_read_u32((volatile pg_atomic_uint32 *)&slot->generation_change_seq);
		if ((before & 1U) != 0)
			continue;
		pg_read_barrier();
		generation = ((uint64)slot->slot_generation_hi << 32) | (uint64)slot->slot_generation_lo;
		pg_read_barrier();
		after = pg_atomic_read_u32((volatile pg_atomic_uint32 *)&slot->generation_change_seq);
		if (before == after && (after & 1U) == 0) {
			*generation_out = generation;
			return true;
		}
	}
	return false;
}


/* Caller holds the allocator lock for this slot. */
static void
pcm_x_slot_generation_write(PcmXSlotHeader *slot, uint64 generation)
{
	uint32 odd;

	Assert(ClusterPcmXConvertShmem != NULL);
	Assert(LWLockHeldByMeInMode(&ClusterPcmXConvertShmem->allocator_lock.lock, LW_EXCLUSIVE));
	odd = pg_atomic_read_u32(&slot->generation_change_seq) | 1U;
	pg_atomic_write_u32(&slot->generation_change_seq, odd);
	pg_write_barrier();
	slot->slot_generation_lo = (uint32)generation;
	slot->slot_generation_hi = (uint32)(generation >> 32);
	pg_write_barrier();
	pg_atomic_write_u32(&slot->generation_change_seq, odd + 1U);
}


static uint32
pcm_x_slot_state_read(const PcmXSlotHeader *slot)
{
	return pg_atomic_read_u32((volatile pg_atomic_uint32 *)&slot->state_flags)
		   & PCM_X_SLOT_STATE_MASK;
}


static void
pcm_x_slot_state_write(PcmXSlotHeader *slot, uint32 state)
{
	uint32 current;
	uint32 desired;

	if (slot == NULL || (state & ~PCM_X_SLOT_STATE_MASK) != 0)
		return;
	current = pg_atomic_read_u32(&slot->state_flags);
	do {
		desired = (current & PCM_X_SLOT_FLAGS_MASK) | state;
	} while (!pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired));
}


static void
pcm_x_slot_state_flags_write(PcmXSlotHeader *slot, uint32 state, uint32 flags)
{
	uint32 flag_value_mask = PCM_X_SLOT_FLAGS_MASK >> PCM_X_SLOT_FLAGS_SHIFT;

	if (slot == NULL || (state & ~PCM_X_SLOT_STATE_MASK) != 0 || (flags & ~flag_value_mask) != 0)
		return;
	pg_atomic_write_u32(&slot->state_flags, state | (flags << PCM_X_SLOT_FLAGS_SHIFT));
}


static uint32
pcm_x_slot_flags_read(const PcmXSlotHeader *slot)
{
	if (slot == NULL)
		return 0;
	return pg_atomic_read_u32((volatile pg_atomic_uint32 *)&slot->state_flags)
		   >> PCM_X_SLOT_FLAGS_SHIFT;
}


static bool
pcm_x_slot_flags_compare_exchange(PcmXSlotHeader *slot, uint32 *expected_flags,
								  uint32 desired_flags)
{
	uint32 current;
	uint32 current_flags;
	uint32 desired;
	uint32 flag_value_mask = PCM_X_SLOT_FLAGS_MASK >> PCM_X_SLOT_FLAGS_SHIFT;

	if (slot == NULL || expected_flags == NULL || (desired_flags & ~flag_value_mask) != 0)
		return false;
	current = pg_atomic_read_u32(&slot->state_flags);
	for (;;) {
		current_flags = current >> PCM_X_SLOT_FLAGS_SHIFT;
		if (current_flags != *expected_flags) {
			*expected_flags = current_flags;
			return false;
		}
		desired = (current & PCM_X_SLOT_STATE_MASK) | (desired_flags << PCM_X_SLOT_FLAGS_SHIFT);
		if (pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired))
			return true;
	}
}


static void
pcm_x_slot_flags_write(PcmXSlotHeader *slot, uint32 flags)
{
	uint32 current;
	uint32 desired;
	uint32 flag_value_mask = PCM_X_SLOT_FLAGS_MASK >> PCM_X_SLOT_FLAGS_SHIFT;

	if (slot == NULL || (flags & ~flag_value_mask) != 0)
		return;
	current = pg_atomic_read_u32(&slot->state_flags);
	do {
		desired = (current & PCM_X_SLOT_STATE_MASK) | (flags << PCM_X_SLOT_FLAGS_SHIFT);
	} while (!pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired));
}


static uint32
pcm_x_slot_flags_fetch_or(PcmXSlotHeader *slot, uint32 flags)
{
	uint32 current;
	uint32 desired;
	uint32 old_flags;
	uint32 flag_value_mask = PCM_X_SLOT_FLAGS_MASK >> PCM_X_SLOT_FLAGS_SHIFT;

	if (slot == NULL || (flags & ~flag_value_mask) != 0)
		return 0;
	current = pg_atomic_read_u32(&slot->state_flags);
	for (;;) {
		old_flags = current >> PCM_X_SLOT_FLAGS_SHIFT;
		desired
			= (current & PCM_X_SLOT_STATE_MASK) | ((old_flags | flags) << PCM_X_SLOT_FLAGS_SHIFT);
		if (pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired))
			return old_flags;
	}
}


static uint32
pcm_x_slot_flags_fetch_and(PcmXSlotHeader *slot, uint32 flags)
{
	uint32 current;
	uint32 desired;
	uint32 old_flags;
	uint32 flag_value_mask = PCM_X_SLOT_FLAGS_MASK >> PCM_X_SLOT_FLAGS_SHIFT;

	if (slot == NULL)
		return 0;
	flags &= flag_value_mask;
	current = pg_atomic_read_u32(&slot->state_flags);
	for (;;) {
		old_flags = current >> PCM_X_SLOT_FLAGS_SHIFT;
		desired
			= (current & PCM_X_SLOT_STATE_MASK) | ((old_flags & flags) << PCM_X_SLOT_FLAGS_SHIFT);
		if (pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired))
			return old_flags;
	}
}


/*
 * Make one FREE slot non-visible under the sole allocator lock.
 *
 * The checked generation bump happens before a caller can publish a directory
 * entry.  Exhaustion is itself persisted in the slot and removes the slot from
 * reuse; a stale generation can therefore never wrap to zero.
 */
static bool
pcm_x_slot_generation_reserve_locked(PcmXSlotHeader *slot)
{
	uint64 current;
	uint64 next;

	if (slot == NULL || pcm_x_slot_state_read(slot) != PCM_X_SLOT_FREE
		|| pcm_x_slot_flags_read(slot) != 0)
		return false;
	if (!pcm_x_slot_generation_read(slot, &current))
		return false;
	if (!cluster_pcm_x_generation_next(current, &next)) {
		slot->next_free = PCM_X_INVALID_SLOT_INDEX;
		pg_write_barrier();
		pcm_x_slot_state_write(slot, PCM_X_SLOT_GENERATION_EXHAUSTED);
		return false;
	}

	slot->next_free = PCM_X_INVALID_SLOT_INDEX;
	pcm_x_slot_generation_write(slot, next);
	pg_write_barrier();
	pcm_x_slot_state_write(slot, PCM_X_SLOT_RESERVED_NONVISIBLE);
	return true;
}


typedef struct PcmXAllocatorView {
	Size slots_offset;
	Size slot_size;
	Size first_slot_index;
	Size capacity;
	PcmXAllocatorState *state;
} PcmXAllocatorView;

typedef struct PcmXDirectoryView {
	PcmXDirectoryEntry *entries;
	Size capacity;
	PcmXAllocatorKind allocator_kind;
} PcmXDirectoryView;

typedef enum PcmXDirectoryMatch {
	PCM_X_DIRECTORY_MATCH_EXACT = 0,
	PCM_X_DIRECTORY_MATCH_DIFFERENT,
	PCM_X_DIRECTORY_MATCH_STALE,
	PCM_X_DIRECTORY_MATCH_CORRUPT
} PcmXDirectoryMatch;


static void pcm_x_runtime_fail_closed_impl(const char *site_file, int site_line);
static PcmXSlotHeader *pcm_x_domain_slot(PcmXAllocatorKind kind, PcmXSlotRef ref,
										 const BufferTag *expected_tag, uint32 allowed_state_mask);
static void pcm_x_local_gate_acquire_guarded(LWLock *lock, LWLockMode mode,
											 PcmXLocalTagSlot *tag_slot);

/* Capture each internal fail-closed arm (file:line) while keeping the many
 * zero-argument call sites in this file textually unchanged. */
#define pcm_x_runtime_fail_closed() pcm_x_runtime_fail_closed_impl(__FILE__, __LINE__)


/* Required runtime precondition: never nest allocator under a tag domain. */
static bool
pcm_x_allocator_entry_unlocked(const PcmXShmemHeader *header)
{
	return !LWLockHeldByMe((LWLock *)&header->allocator_lock.lock)
		   && !LWLockAnyHeldByMe((LWLock *)&header->master_locks[0].lock, PCM_X_LOCK_PARTITIONS,
								 sizeof(LWLockPadded))
		   && !LWLockAnyHeldByMe((LWLock *)&header->local_locks[0].lock, PCM_X_LOCK_PARTITIONS,
								 sizeof(LWLockPadded));
}


static bool
pcm_x_allocator_view(PcmXAllocatorKind kind, PcmXAllocatorView *view)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	const PcmXPoolLayout *pool;

	if (header == NULL || view == NULL || kind < 0 || kind >= PCM_X_ALLOC_COUNT)
		return false;
	memset(view, 0, sizeof(*view));
	view->state = &header->allocator[kind];

	switch (kind) {
	case PCM_X_ALLOC_MASTER_TAG:
		pool = &header->layout.pools[PCM_X_POOL_MASTER_TAG];
		break;
	case PCM_X_ALLOC_MASTER_TICKET:
		pool = &header->layout.pools[PCM_X_POOL_MASTER_TICKET];
		break;
	case PCM_X_ALLOC_BLOCKER:
		pool = &header->layout.pools[PCM_X_POOL_BLOCKER];
		break;
	case PCM_X_ALLOC_LOCAL_TAG:
		pool = &header->layout.pools[PCM_X_POOL_LOCAL_TAG];
		break;
	case PCM_X_ALLOC_LOCAL_WAIT:
		view->slots_offset = header->layout.local_wait.slots_offset;
		view->slot_size = sizeof(PcmXLocalMembershipSlot);
		view->first_slot_index = header->layout.local_wait.first_slot_index;
		view->capacity = header->layout.local_wait.capacity;
		return true;
	case PCM_X_ALLOC_LOCAL_HOLDER:
		view->slots_offset = header->layout.local_holder.slots_offset;
		view->slot_size = sizeof(PcmXLocalMembershipSlot);
		view->first_slot_index = header->layout.local_holder.first_slot_index;
		view->capacity = header->layout.local_holder.capacity;
		return true;
	case PCM_X_ALLOC_COUNT:
		return false;
	}

	view->slots_offset = pool->slots_offset;
	view->slot_size = pool->slot_size;
	view->first_slot_index = 0;
	view->capacity = pool->capacity;
	return true;
}


static PcmXSlotHeader *
pcm_x_allocator_slot(const PcmXAllocatorView *view, Size slot_index)
{
	Size local_index;

	if (view == NULL || slot_index < view->first_slot_index)
		return NULL;
	local_index = slot_index - view->first_slot_index;
	if (local_index >= view->capacity)
		return NULL;
	return (PcmXSlotHeader *)((char *)ClusterPcmXConvertShmem + view->slots_offset
							  + local_index * view->slot_size);
}


/* Validate the O(1) allocator conservation facts before mutating its free list. */
static bool
pcm_x_allocator_state_valid_locked(const PcmXAllocatorView *view)
{
	PcmXSlotHeader *head;
	Size available;

	if (view == NULL || view->state == NULL || view->slot_size < sizeof(PcmXSlotHeader)
		|| view->state->generation_exhausted > view->capacity)
		return false;
	available = view->capacity - view->state->generation_exhausted;
	if (view->state->used > available || view->state->high_water > view->capacity
		|| view->state->high_water < view->state->used)
		return false;
	if (view->state->free_head == PCM_X_INVALID_SLOT_INDEX)
		return view->state->used == available;
	if (view->state->used >= available)
		return false;
	head = pcm_x_allocator_slot(view, view->state->free_head);
	return head != NULL && pcm_x_slot_state_read(head) == PCM_X_SLOT_FREE;
}


/* Reserve from one fixed allocator while the caller owns allocator_lock. */
static PcmXAllocatorResult
pcm_x_allocator_reserve_locked(PcmXAllocatorKind kind, PcmXSlotRef *ref_out,
							   PcmXSlotHeader **slot_out)
{
	PcmXAllocatorView view;
	PcmXAllocatorResult result = PCM_X_ALLOC_NO_CAPACITY;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	Size scanned = 0;

	if (header == NULL || ref_out == NULL || slot_out == NULL
		|| !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !pcm_x_allocator_view(kind, &view))
		return PCM_X_ALLOC_INVALID;
	ref_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	ref_out->slot_generation = 0;
	*slot_out = NULL;
	if (!pcm_x_allocator_state_valid_locked(&view))
		return PCM_X_ALLOC_CORRUPT;

	while (view.state->free_head != PCM_X_INVALID_SLOT_INDEX && scanned < view.capacity) {
		PcmXSlotHeader *slot;
		PcmXSlotHeader *successor = NULL;
		Size slot_index = view.state->free_head;
		Size saved_next;
		Size free_slots;

		slot = pcm_x_allocator_slot(&view, slot_index);
		if (slot == NULL || pcm_x_slot_state_read(slot) != PCM_X_SLOT_FREE)
			return PCM_X_ALLOC_CORRUPT;
		saved_next = slot->next_free;
		if (saved_next != PCM_X_INVALID_SLOT_INDEX)
			successor = pcm_x_allocator_slot(&view, saved_next);
		if (saved_next == slot_index
			|| (saved_next != PCM_X_INVALID_SLOT_INDEX
				&& (successor == NULL || pcm_x_slot_state_read(successor) != PCM_X_SLOT_FREE)))
			return PCM_X_ALLOC_CORRUPT;
		free_slots = view.capacity - view.state->generation_exhausted - view.state->used;
		if ((saved_next == PCM_X_INVALID_SLOT_INDEX) != (free_slots == 1))
			return PCM_X_ALLOC_CORRUPT;

		view.state->free_head = saved_next;
		scanned++;
		if (!pcm_x_slot_generation_reserve_locked(slot)) {
			if (pcm_x_slot_state_read(slot) != PCM_X_SLOT_GENERATION_EXHAUSTED
				|| view.state->generation_exhausted >= view.capacity)
				return PCM_X_ALLOC_CORRUPT;
			view.state->generation_exhausted++;
			if (!pcm_x_allocator_state_valid_locked(&view))
				return PCM_X_ALLOC_CORRUPT;
			continue;
		}

		view.state->used++;
		view.state->high_water = Max(view.state->high_water, view.state->used);
		if (!pcm_x_allocator_state_valid_locked(&view))
			return PCM_X_ALLOC_CORRUPT;
		ref_out->slot_index = slot_index;
		if (!pcm_x_slot_generation_read(slot, &ref_out->slot_generation))
			return PCM_X_ALLOC_CORRUPT;
		*slot_out = slot;
		return PCM_X_ALLOC_OK;
	}
	if (scanned == view.capacity && view.state->free_head != PCM_X_INVALID_SLOT_INDEX)
		result = PCM_X_ALLOC_CORRUPT;
	if (result == PCM_X_ALLOC_NO_CAPACITY && !pcm_x_allocator_state_valid_locked(&view))
		result = PCM_X_ALLOC_CORRUPT;
	return result;
}


/* Release one RESERVED/DETACHING slot while the caller owns allocator_lock. */
static PcmXAllocatorResult
pcm_x_allocator_release_locked(PcmXAllocatorKind kind, PcmXSlotRef expected, uint32 expected_state)
{
	PcmXAllocatorView view;
	PcmXAllocatorResult result = PCM_X_ALLOC_OK;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *slot;
	Size old_head;
	uint64 generation;

	if (header == NULL
		|| (expected_state != PCM_X_SLOT_RESERVED_NONVISIBLE
			&& expected_state != PCM_X_SLOT_DETACHING)
		|| !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !pcm_x_allocator_view(kind, &view))
		return PCM_X_ALLOC_INVALID;
	if (!pcm_x_allocator_state_valid_locked(&view))
		return PCM_X_ALLOC_CORRUPT;
	slot = pcm_x_allocator_slot(&view, expected.slot_index);
	if (slot == NULL)
		return PCM_X_ALLOC_INVALID;
	if (!pcm_x_slot_generation_read(slot, &generation))
		return PCM_X_ALLOC_CORRUPT;
	if (generation != expected.slot_generation)
		return PCM_X_ALLOC_STALE_REF;
	if (pcm_x_slot_state_read(slot) != expected_state)
		return PCM_X_ALLOC_BAD_STATE;
	if (view.state->used == 0)
		return PCM_X_ALLOC_CORRUPT;

	old_head = view.state->free_head;
	if (old_head != PCM_X_INVALID_SLOT_INDEX
		&& (pcm_x_allocator_slot(&view, old_head) == NULL
			|| pcm_x_slot_state_read(pcm_x_allocator_slot(&view, old_head)) != PCM_X_SLOT_FREE))
		return PCM_X_ALLOC_CORRUPT;
	memset((char *)slot + sizeof(PcmXSlotHeader), 0, view.slot_size - sizeof(PcmXSlotHeader));
	slot->next_free = old_head;
	pcm_x_slot_generation_write(slot, generation);
	pg_write_barrier();
	pcm_x_slot_state_flags_write(slot, PCM_X_SLOT_FREE, 0);
	view.state->free_head = expected.slot_index;
	view.state->used--;
	if (!pcm_x_allocator_state_valid_locked(&view))
		result = PCM_X_ALLOC_CORRUPT;
	return result;
}


static PcmXSlotHeader *
pcm_x_slot_ref_resolve_locked(PcmXAllocatorKind kind, PcmXSlotRef expected)
{
	PcmXAllocatorView view;
	PcmXSlotHeader *slot;
	uint64 generation;

	if (!pcm_x_allocator_view(kind, &view))
		return NULL;
	slot = pcm_x_allocator_slot(&view, expected.slot_index);
	if (slot == NULL)
		return NULL;
	if (!pcm_x_slot_generation_read(slot, &generation)) {
		pcm_x_runtime_fail_closed();
		return NULL;
	}
	if (generation != expected.slot_generation || pcm_x_slot_state_read(slot) == PCM_X_SLOT_FREE
		|| pcm_x_slot_state_read(slot) == PCM_X_SLOT_GENERATION_EXHAUSTED)
		return NULL;
	return slot;
}


static void
pcm_x_init_stats(PcmXStats *stats)
{
	pg_atomic_init_u64(&stats->enqueue_count, 0);
	pg_atomic_init_u64(&stats->admit_count, 0);
	pg_atomic_init_u64(&stats->confirm_count, 0);
	pg_atomic_init_u64(&stats->promotion_count, 0);
	pg_atomic_init_u64(&stats->transfer_count, 0);
	pg_atomic_init_u64(&stats->complete_count, 0);
	pg_atomic_init_u64(&stats->cancel_count, 0);
	pg_atomic_init_u64(&stats->revoke_count, 0);
	pg_atomic_init_u64(&stats->coalesced_count, 0);
	pg_atomic_init_u64(&stats->wait_count, 0);
	pg_atomic_init_u64(&stats->full_count, 0);
	pg_atomic_init_u64(&stats->stale_count, 0);
	pg_atomic_init_u64(&stats->miss_count, 0);
	pg_atomic_init_u64(&stats->recovery_blocked_count, 0);
	pg_atomic_init_u64(&stats->activating_reset_count, 0);
	pg_atomic_init_u64(&stats->depth, 0);
	pg_atomic_init_u64(&stats->depth_high_water, 0);
	pg_atomic_init_u64(&stats->own_begin_count, 0);
	pg_atomic_init_u64(&stats->own_commit_count, 0);
	pg_atomic_init_u64(&stats->own_abort_count, 0);
	pg_atomic_init_u64(&stats->own_busy_count, 0);
	pg_atomic_init_u64(&stats->own_corrupt_count, 0);
	pg_atomic_init_u64(&stats->barrier_unwind_count, 0);
}


static void
pcm_x_stats_increment(pg_atomic_uint64 *counter)
{
	if (counter != NULL)
		(void)pg_atomic_fetch_add_u64(counter, 1);
}


static bool
pcm_x_stats_depth_increment(PcmXShmemHeader *header)
{
	uint64 depth;
	uint64 observed;

	if (header == NULL)
		return false;
	observed = pg_atomic_read_u64(&header->stats.depth);
	for (;;) {
		uint64 expected;

		if (observed == UINT64_MAX)
			return false;
		expected = observed;
		if (pg_atomic_compare_exchange_u64(&header->stats.depth, &expected, observed + 1))
			break;
		observed = expected;
	}
	depth = observed + 1;
	observed = pg_atomic_read_u64(&header->stats.depth_high_water);
	while (depth > observed) {
		uint64 expected = observed;

		if (pg_atomic_compare_exchange_u64(&header->stats.depth_high_water, &expected, depth))
			break;
		observed = expected;
	}
	return true;
}


static bool
pcm_x_stats_depth_decrement(PcmXShmemHeader *header)
{
	uint64 observed;

	if (header == NULL)
		return false;
	observed = pg_atomic_read_u64(&header->stats.depth);
	while (observed != 0) {
		uint64 expected = observed;

		if (pg_atomic_compare_exchange_u64(&header->stats.depth, &expected, observed - 1))
			return true;
		observed = expected;
	}
	return false;
}


bool
cluster_pcm_x_stats_snapshot(PcmXStatsSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	bool allocator_lock_held;
	int i;

	if (snapshot_out == NULL)
		return false;
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (header == NULL)
		return false;

	allocator_lock_held = LWLockHeldByMe(&header->allocator_lock.lock);
	if (!allocator_lock_held)
		LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	snapshot_out->enqueue_count = pg_atomic_read_u64(&header->stats.enqueue_count);
	snapshot_out->admit_count = pg_atomic_read_u64(&header->stats.admit_count);
	snapshot_out->confirm_count = pg_atomic_read_u64(&header->stats.confirm_count);
	snapshot_out->promotion_count = pg_atomic_read_u64(&header->stats.promotion_count);
	snapshot_out->transfer_count = pg_atomic_read_u64(&header->stats.transfer_count);
	snapshot_out->complete_count = pg_atomic_read_u64(&header->stats.complete_count);
	snapshot_out->cancel_count = pg_atomic_read_u64(&header->stats.cancel_count);
	snapshot_out->revoke_count = pg_atomic_read_u64(&header->stats.revoke_count);
	snapshot_out->coalesced_count = pg_atomic_read_u64(&header->stats.coalesced_count);
	snapshot_out->wait_count = pg_atomic_read_u64(&header->stats.wait_count);
	snapshot_out->full_count = pg_atomic_read_u64(&header->stats.full_count);
	snapshot_out->stale_count = pg_atomic_read_u64(&header->stats.stale_count);
	snapshot_out->miss_count = pg_atomic_read_u64(&header->stats.miss_count);
	snapshot_out->recovery_blocked_count
		= pg_atomic_read_u64(&header->stats.recovery_blocked_count);
	snapshot_out->activating_reset_count
		= pg_atomic_read_u64(&header->stats.activating_reset_count);
	snapshot_out->depth = pg_atomic_read_u64(&header->stats.depth);
	snapshot_out->depth_high_water = pg_atomic_read_u64(&header->stats.depth_high_water);
	snapshot_out->own_begin_count = pg_atomic_read_u64(&header->stats.own_begin_count);
	snapshot_out->own_commit_count = pg_atomic_read_u64(&header->stats.own_commit_count);
	snapshot_out->own_abort_count = pg_atomic_read_u64(&header->stats.own_abort_count);
	snapshot_out->own_busy_count = pg_atomic_read_u64(&header->stats.own_busy_count);
	snapshot_out->own_corrupt_count = pg_atomic_read_u64(&header->stats.own_corrupt_count);
	snapshot_out->barrier_unwind_count = pg_atomic_read_u64(&header->stats.barrier_unwind_count);
	snapshot_out->active_tags = (uint64)header->allocator[PCM_X_ALLOC_MASTER_TAG].used;
	snapshot_out->live_tickets = (uint64)header->allocator[PCM_X_ALLOC_MASTER_TICKET].used;
	snapshot_out->local_retire_gate = (uint64)pg_atomic_read_u32(&header->local_retire_gate);
	for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
		uint64 marker = header->peer_frontiers[i].local_retire_in_progress_ticket_id;

		if (marker == 0)
			continue;
		if (snapshot_out->local_retire_marker_count == 0)
			snapshot_out->local_retire_marker_ticket_id = marker;
		snapshot_out->local_retire_marker_count++;
	}
	for (i = 0; i < PCM_X_ALLOC_COUNT; i++)
		snapshot_out->live_slots += (uint64)header->allocator[i].used;
	if (!allocator_lock_held)
		LWLockRelease(&header->allocator_lock.lock);
	return true;
}


void
cluster_pcm_x_stats_note_enqueue(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.enqueue_count);
}


void
cluster_pcm_x_stats_note_wait(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.wait_count);
}


void
cluster_pcm_x_stats_note_queue_result(PcmXQueueResult result)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header == NULL)
		return;
	switch (result) {
	case PCM_X_QUEUE_NO_CAPACITY:
		pcm_x_stats_increment(&header->stats.full_count);
		break;
	case PCM_X_QUEUE_STALE:
		pcm_x_stats_increment(&header->stats.stale_count);
		break;
	case PCM_X_QUEUE_NOT_FOUND:
		pcm_x_stats_increment(&header->stats.miss_count);
		break;
	default:
		break;
	}
}


void
cluster_pcm_x_stats_note_own_begin(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.own_begin_count);
}


void
cluster_pcm_x_stats_note_own_commit(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.own_commit_count);
}


void
cluster_pcm_x_stats_note_own_abort(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.own_abort_count);
}


void
cluster_pcm_x_stats_note_own_busy(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.own_busy_count);
}


void
cluster_pcm_x_stats_note_own_corrupt(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.own_corrupt_count);
}


/* t/400 L3 item 3: a BARRIER_CLOSED refusal consumed by a barrier-aware
 * LockBuffer caller (unwound in place of a client ERROR). */
void
cluster_pcm_x_stats_note_barrier_unwind(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header != NULL)
		pcm_x_stats_increment(&header->stats.barrier_unwind_count);
}


static void
pcm_x_runtime_fail_closed_impl(const char *site_file, int site_line)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	bool transitioned;

	transitioned
		= cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	if (!transitioned)
		transitioned = cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_SHUTTING_DOWN,
														PCM_X_RUNTIME_RECOVERY_BLOCKED);
	if (transitioned && header != NULL) {
		const char *base;

		/* Record the fusing arm.  Single writer: only the CAS winner above
		 * reaches this block, and a second fuse requires a full reactivation
		 * in between, so writes are serialized. */
		base = strrchr(site_file, '/');
		base = (base != NULL) ? base + 1 : site_file;
		snprintf(header->fail_closed_site, sizeof(header->fail_closed_site), "%s:%d", base,
				 site_line);
		pcm_x_stats_increment(&header->stats.recovery_blocked_count);

		/* At most one log line per ACTIVE generation (single CAS winner), so
		 * this cannot flood.  Skip inside critical sections; the shmem site
		 * above stays observable through pg_cluster_state either way. */
		if (CritSectionCount == 0)
			ereport(LOG,
					(errmsg("cluster PCM-X runtime fail-closed (recovery blocked) at %s:%d", base,
							site_line),
					 errhint("Writer conversions on this node stay fail-closed until the PCM-X "
							 "runtime is reformed.")));
	}
}


void
cluster_pcm_x_runtime_fail_closed_at(const char *site_file, int site_line)
{
	pcm_x_runtime_fail_closed_impl(site_file, site_line);
}


bool
cluster_pcm_x_runtime_fail_closed_site(char *buf, Size buflen)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (buf == NULL || buflen == 0)
		return false;
	buf[0] = '\0';
	if (header == NULL || header->fail_closed_site[0] == '\0')
		return false;
	strlcpy(buf, header->fail_closed_site, Min(buflen, sizeof(header->fail_closed_site)));
	return true;
}


/* Map an FIFO index to a printable value: PCM_X_INVALID_SLOT_INDEX -> -1. */
static long
pcm_x_debug_index(Size index)
{
	return index == PCM_X_INVALID_SLOT_INDEX ? -1L : (long)index;
}


/* Diagnostic iterator over live master tag slots.  Coherence contract: the
 * candidate is identified by a racy pre-read, then revalidated and copied
 * under the matching master partition shared lock (the same lock every
 * in-place tag/ticket field mutation holds), so the printed combination is a
 * state that actually existed.  Diagnostic surface only — never used for
 * protocol decisions. */
bool
cluster_pcm_x_master_tag_debug_next(Size *cursor_io, Size *index_out, char *buf, Size buflen)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorView view;
	PcmXMasterTagSlot copy;
	Size i;

	if (header == NULL || cursor_io == NULL || index_out == NULL || buf == NULL || buflen == 0)
		return false;
	if (!pcm_x_allocator_entry_unlocked(header)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TAG, &view))
		return false;
	for (i = *cursor_io; i < view.capacity; i++) {
		PcmXMasterTagSlot *raw = (PcmXMasterTagSlot *)pcm_x_allocator_slot(&view, i);
		PcmXMasterTagSlot *locked;
		PcmXSlotRef tag_ref;
		BufferTag tag;
		uint32 partition;

		if (raw == NULL || pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE
			|| !pcm_x_slot_generation_read(&raw->slot, &tag_ref.slot_generation))
			continue;
		tag_ref.slot_index = i;
		tag = raw->tag;
		pg_read_barrier();
		partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&tag));
		LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
		locked = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref, &tag,
														PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
		if (locked == NULL) {
			LWLockRelease(&header->master_locks[partition].lock);
			continue;
		}
		memcpy(&copy, locked, sizeof(copy));
		LWLockRelease(&header->master_locks[partition].lock);
		snprintf(buf, buflen,
				 "rel=%u blk=%u head=%ld tail=%ld active=%ld queued=0x%x qseq=" UINT64_FORMAT
				 " outstanding=%zu",
				 copy.tag.relNumber, copy.tag.blockNum, pcm_x_debug_index(copy.head_index),
				 pcm_x_debug_index(copy.tail_index), pcm_x_debug_index(copy.active_index),
				 pg_atomic_read_u32(&copy.queued_node_bitmap), copy.queue_state_sequence,
				 copy.outstanding_ticket_count);
		*index_out = i;
		*cursor_io = i + 1;
		return true;
	}
	*cursor_io = view.capacity;
	return false;
}


/* Diagnostic iterator over occupied master ticket slots; same partition-lock
 * coherence contract as the tag iterator. */
bool
cluster_pcm_x_master_ticket_debug_next(Size *cursor_io, Size *index_out, char *buf, Size buflen)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorView view;
	PcmXMasterTicketSlot copy;
	Size i;

	if (header == NULL || cursor_io == NULL || index_out == NULL || buf == NULL || buflen == 0)
		return false;
	if (!pcm_x_allocator_entry_unlocked(header)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &view))
		return false;
	for (i = *cursor_io; i < view.capacity; i++) {
		PcmXMasterTicketSlot *raw = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&view, i);
		PcmXMasterTicketSlot *locked;
		PcmXSlotRef ticket_ref;
		BufferTag tag;
		uint32 partition;
		uint32 state;

		if (raw == NULL)
			continue;
		state = pcm_x_slot_state_read(&raw->slot);
		if (state == PCM_XT_FREE || state == PCM_XT_RESERVED_NONVISIBLE)
			continue;
		if (!pcm_x_slot_generation_read(&raw->slot, &ticket_ref.slot_generation))
			continue;
		ticket_ref.slot_index = i;
		tag = raw->ref.identity.tag;
		pg_read_barrier();
		partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&tag));
		LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
		locked = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
														   &tag, PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (locked == NULL) {
			LWLockRelease(&header->master_locks[partition].lock);
			continue;
		}
		memcpy(&copy, locked, sizeof(copy));
		LWLockRelease(&header->master_locks[partition].lock);
		snprintf(buf, buflen,
				 "state=%u flags=0x%x node=%d ticket=" UINT64_FORMAT " grant=" UINT64_FORMAT
				 " tomb=0x%llx involved=0x%x drained=0x%x retire_acked=0x%x pending_s=0x%x"
				 " acked_s=0x%x leg_op=%u leg_phase=%u leg_flags=0x%x leg_retry=%u leg_node=%d"
				 " leg_deadline=" UINT64_FORMAT,
				 pcm_x_slot_state_read(&copy.slot), pcm_x_slot_flags_read(&copy.slot),
				 copy.ref.identity.node_id, copy.ref.handle.ticket_id, copy.ref.grant_generation,
				 (unsigned long long)copy.reliable.response_tombstone_mask,
				 copy.involved_nodes_bitmap, copy.drained_nodes_bitmap,
				 copy.retire_acked_nodes_bitmap, copy.pending_s_holders_bitmap,
				 copy.acked_s_holders_bitmap, copy.reliable.pending_opcode, copy.reliable.phase,
				 copy.reliable.flags, copy.reliable.retry_count,
				 copy.reliable.expected_responder_node, copy.reliable.retry_deadline_ms);
		*index_out = i;
		*cursor_io = i + 1;
		return true;
	}
	*cursor_io = view.capacity;
	return false;
}


/* Diagnostic iterator over live local tag slots; partition-lock coherent copy
 * like the master iterators, but under the local lock partition. */
bool
cluster_pcm_x_local_tag_debug_next(Size *cursor_io, Size *index_out, char *buf, Size buflen)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorView view;
	PcmXLocalTagSlot copy;
	Size i;

	if (header == NULL || cursor_io == NULL || index_out == NULL || buf == NULL || buflen == 0)
		return false;
	if (!pcm_x_allocator_entry_unlocked(header)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &view))
		return false;
	for (i = *cursor_io; i < view.capacity; i++) {
		PcmXLocalTagSlot *raw = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&view, i);
		PcmXLocalTagSlot *locked;
		PcmXSlotRef tag_ref;
		BufferTag tag;
		uint32 partition;

		if (raw == NULL || pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE
			|| !pcm_x_slot_generation_read(&raw->slot, &tag_ref.slot_generation))
			continue;
		tag_ref.slot_index = i;
		tag = raw->tag;
		pg_read_barrier();
		partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&tag));
		pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_SHARED, NULL);
		locked = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, &tag,
													   PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
		if (locked == NULL) {
			LWLockRelease(&header->local_locks[partition].lock);
			continue;
		}
		memcpy(&copy, locked, sizeof(copy));
		LWLockRelease(&header->local_locks[partition].lock);
		snprintf(buf, buflen,
				 "rel=%u blk=%u flags=0x%x round=%u master=%d ref_node=%d ref_ticket=" UINT64_FORMAT
				 " ref_grant=" UINT64_FORMAT " holder_ticket=" UINT64_FORMAT
				 " blocker_ticket=" UINT64_FORMAT " drain_gen=" UINT64_FORMAT
				 " holder_drain_gen=" UINT64_FORMAT
				 " members=%zu head=%ld leader=%ld active_writer=%ld",
				 copy.tag.relNumber, copy.tag.blockNum, pcm_x_slot_flags_read(&copy.slot),
				 copy.local_round, copy.master_node, copy.ref.identity.node_id,
				 copy.ref.handle.ticket_id, copy.ref.grant_generation,
				 copy.holder_ref.handle.ticket_id, copy.blocker_snapshot_ref.handle.ticket_id,
				 copy.terminal_drain_generation, copy.holder_terminal_drain_generation,
				 copy.membership_count, pcm_x_debug_index(copy.head_index),
				 pcm_x_debug_index(copy.leader_index), pcm_x_debug_index(copy.active_writer_index));
		*index_out = i;
		*cursor_io = i + 1;
		return true;
	}
	*cursor_io = view.capacity;
	return false;
}


void
cluster_pcm_x_terminal_note(uint32 op, uint32 result, uint64 ticket_id)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header == NULL)
		return;
	if (header->terminal_note_op == op && header->terminal_note_result == result
		&& header->terminal_note_ticket == ticket_id)
		header->terminal_note_count++;
	else {
		header->terminal_note_op = op;
		header->terminal_note_result = result;
		header->terminal_note_ticket = ticket_id;
		header->terminal_note_count = 1;
	}
}


bool
cluster_pcm_x_terminal_note_read(uint32 *op_out, uint32 *result_out, uint64 *ticket_out,
								 uint32 *count_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header == NULL || header->terminal_note_count == 0)
		return false;
	if (op_out != NULL)
		*op_out = header->terminal_note_op;
	if (result_out != NULL)
		*result_out = header->terminal_note_result;
	if (ticket_out != NULL)
		*ticket_out = header->terminal_note_ticket;
	if (count_out != NULL)
		*count_out = header->terminal_note_count;
	return true;
}


/* Reserve one slot under the sole allocator authority. */
PcmXAllocatorResult
cluster_pcm_x_allocator_reserve(PcmXAllocatorKind kind, PcmXSlotRef *ref_out,
								PcmXSlotHeader **slot_out)
{
	PcmXAllocatorView view;
	PcmXAllocatorResult result = PCM_X_ALLOC_NO_CAPACITY;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	Size scanned = 0;

	if (ref_out == NULL || slot_out == NULL || header == NULL || !pcm_x_allocator_view(kind, &view))
		return PCM_X_ALLOC_INVALID;
	ref_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	ref_out->slot_generation = 0;
	*slot_out = NULL;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_ALLOC_INVALID;
	}

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_allocator_state_valid_locked(&view)) {
		result = PCM_X_ALLOC_CORRUPT;
		goto done;
	}
	while (view.state->free_head != PCM_X_INVALID_SLOT_INDEX && scanned < view.capacity) {
		PcmXSlotHeader *slot;
		PcmXSlotHeader *successor = NULL;
		Size slot_index = view.state->free_head;
		Size saved_next;
		Size free_slots;

		slot = pcm_x_allocator_slot(&view, slot_index);
		if (slot == NULL || pcm_x_slot_state_read(slot) != PCM_X_SLOT_FREE) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		saved_next = slot->next_free;
		if (saved_next != PCM_X_INVALID_SLOT_INDEX)
			successor = pcm_x_allocator_slot(&view, saved_next);
		if (saved_next == slot_index
			|| (saved_next != PCM_X_INVALID_SLOT_INDEX
				&& (successor == NULL || pcm_x_slot_state_read(successor) != PCM_X_SLOT_FREE))) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		free_slots = view.capacity - view.state->generation_exhausted - view.state->used;
		if ((saved_next == PCM_X_INVALID_SLOT_INDEX) != (free_slots == 1)) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}

		/* Save and unlink the successor before reserve clears next_free. */
		view.state->free_head = saved_next;
		scanned++;
		if (!pcm_x_slot_generation_reserve_locked(slot)) {
			if (pcm_x_slot_state_read(slot) != PCM_X_SLOT_GENERATION_EXHAUSTED
				|| view.state->generation_exhausted >= view.capacity) {
				result = PCM_X_ALLOC_CORRUPT;
				break;
			}
			view.state->generation_exhausted++;
			if (!pcm_x_allocator_state_valid_locked(&view)) {
				result = PCM_X_ALLOC_CORRUPT;
				break;
			}
			continue;
		}

		view.state->used++;
		view.state->high_water = Max(view.state->high_water, view.state->used);
		if (!pcm_x_allocator_state_valid_locked(&view)) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		ref_out->slot_index = slot_index;
		if (!pcm_x_slot_generation_read(slot, &ref_out->slot_generation)) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		*slot_out = slot;
		result = PCM_X_ALLOC_OK;
		break;
	}
	if (result == PCM_X_ALLOC_NO_CAPACITY && scanned == view.capacity
		&& view.state->free_head != PCM_X_INVALID_SLOT_INDEX)
		result = PCM_X_ALLOC_CORRUPT;
	if (result == PCM_X_ALLOC_NO_CAPACITY && !pcm_x_allocator_state_valid_locked(&view))
		result = PCM_X_ALLOC_CORRUPT;

done:
	LWLockRelease(&header->allocator_lock.lock);

	if (result == PCM_X_ALLOC_CORRUPT) {
		ref_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
		ref_out->slot_generation = 0;
		*slot_out = NULL;
		pcm_x_runtime_fail_closed();
	}
	return result;
}


/* Release an unpublished rollback or a quiesced DETACHING slot by exact ref. */
PcmXAllocatorResult
cluster_pcm_x_allocator_release_exact(PcmXAllocatorKind kind, PcmXSlotRef expected,
									  uint32 expected_state)
{
	PcmXAllocatorView view;
	PcmXAllocatorResult result = PCM_X_ALLOC_OK;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *slot;
	Size old_head;
	uint64 generation;

	if (header == NULL
		|| (expected_state != PCM_X_SLOT_RESERVED_NONVISIBLE
			&& expected_state != PCM_X_SLOT_DETACHING)
		|| !pcm_x_allocator_view(kind, &view))
		return PCM_X_ALLOC_INVALID;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_ALLOC_INVALID;
	}

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_allocator_state_valid_locked(&view)) {
		result = PCM_X_ALLOC_CORRUPT;
		goto done;
	}
	slot = pcm_x_allocator_slot(&view, expected.slot_index);
	if (slot == NULL)
		result = PCM_X_ALLOC_INVALID;
	else if (!pcm_x_slot_generation_read(slot, &generation))
		result = PCM_X_ALLOC_CORRUPT;
	else if (generation != expected.slot_generation)
		result = PCM_X_ALLOC_STALE_REF;
	else if (pcm_x_slot_state_read(slot) != expected_state)
		result = PCM_X_ALLOC_BAD_STATE;
	else if (view.state->used == 0)
		result = PCM_X_ALLOC_CORRUPT;
	else {
		old_head = view.state->free_head;
		if (old_head != PCM_X_INVALID_SLOT_INDEX
			&& (pcm_x_allocator_slot(&view, old_head) == NULL
				|| pcm_x_slot_state_read(pcm_x_allocator_slot(&view, old_head)) != PCM_X_SLOT_FREE))
			result = PCM_X_ALLOC_CORRUPT;
		else {
			memset((char *)slot + sizeof(PcmXSlotHeader), 0,
				   view.slot_size - sizeof(PcmXSlotHeader));
			slot->next_free = old_head;
			pcm_x_slot_generation_write(slot, generation);
			pg_write_barrier();
			pcm_x_slot_state_flags_write(slot, PCM_X_SLOT_FREE, 0);
			view.state->free_head = expected.slot_index;
			view.state->used--;
			if (!pcm_x_allocator_state_valid_locked(&view))
				result = PCM_X_ALLOC_CORRUPT;
		}
	}

done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_ALLOC_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/*
 * Revalidate a directory ref only after the caller owns its tag-domain lock.
 *
 * The allocator deliberately does not nest that lock.  A delayed lookup can
 * therefore overlap FREE->RESERVED reuse.  The monotonic generation counter
 * brackets the complete lifecycle-state and BufferTag snapshot; a torn
 * multi-field read or slot reuse cannot escape as one incarnation.  Once
 * accepted, DETACHING must take the same domain lock before the allocator is
 * allowed to reclaim the slot.
 */
static const BufferTag *
pcm_x_slot_tag(PcmXAllocatorKind kind, const PcmXSlotHeader *slot)
{
	switch (kind) {
	case PCM_X_ALLOC_MASTER_TAG:
		return &((const PcmXMasterTagSlot *)slot)->tag;
	case PCM_X_ALLOC_MASTER_TICKET:
		return &((const PcmXMasterTicketSlot *)slot)->ref.identity.tag;
	case PCM_X_ALLOC_LOCAL_TAG:
		return &((const PcmXLocalTagSlot *)slot)->tag;
	case PCM_X_ALLOC_LOCAL_WAIT:
	case PCM_X_ALLOC_LOCAL_HOLDER:
		return &((const PcmXLocalMembershipSlot *)slot)->identity.tag;
	case PCM_X_ALLOC_BLOCKER:
	case PCM_X_ALLOC_COUNT:
		return NULL;
	}
	return NULL;
}


PcmXSlotHeader *
cluster_pcm_x_slot_ref_revalidate(PcmXAllocatorKind kind, PcmXSlotRef expected,
								  uint32 expected_state, const BufferTag *expected_tag,
								  uint32 tag_hash)
{
	PcmXAllocatorView view;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *slot;
	const BufferTag *slot_tag;
	BufferTag tag_snapshot;
	uint64 generation1;
	uint64 generation2;
	uint32 state1;
	uint32 state2;
	uint32 authoritative_hash;
	uint32 partition;

	/* Blockers have no independent tag; revalidate them through their owner ticket. */
	if (header == NULL || expected_tag == NULL || kind == PCM_X_ALLOC_BLOCKER
		|| !pcm_x_allocator_view(kind, &view) || expected_state == PCM_X_SLOT_FREE
		|| expected_state == PCM_X_SLOT_RESERVED_NONVISIBLE
		|| expected_state == PCM_X_SLOT_GENERATION_EXHAUSTED)
		return NULL;
	authoritative_hash = cluster_pcm_x_tag_hash(expected_tag);
	if (tag_hash != authoritative_hash) {
		pcm_x_runtime_fail_closed();
		return NULL;
	}
	partition = cluster_pcm_x_lock_partition(authoritative_hash);
	if (LWLockHeldByMe(&header->allocator_lock.lock)
		|| ((kind == PCM_X_ALLOC_MASTER_TAG || kind == PCM_X_ALLOC_MASTER_TICKET)
			&& !LWLockHeldByMe(&header->master_locks[partition].lock))
		|| (kind >= PCM_X_ALLOC_LOCAL_TAG
			&& !LWLockHeldByMe(&header->local_locks[partition].lock))) {
		pcm_x_runtime_fail_closed();
		return NULL;
	}

	slot = pcm_x_allocator_slot(&view, expected.slot_index);
	if (slot == NULL)
		return NULL;
	/* Allocator reuse does not nest the domain lock.  An unstable seqlock is a
	 * legal overlap here; the caller retries from its exact directory ref. */
	if (!pcm_x_slot_generation_read(slot, &generation1))
		return NULL;
	if (generation1 != expected.slot_generation)
		return NULL;
	pg_read_barrier();
	state1 = pcm_x_slot_state_read(slot);
	if (state1 != expected_state)
		return NULL;
	pg_read_barrier();
	slot_tag = pcm_x_slot_tag(kind, slot);
	if (slot_tag == NULL)
		return NULL;
	tag_snapshot = *slot_tag;
	pg_read_barrier();
	state2 = pcm_x_slot_state_read(slot);
	pg_read_barrier();
	if (!pcm_x_slot_generation_read(slot, &generation2))
		return NULL;
	if (state1 != state2 || generation1 != generation2)
		return NULL;
	if (!BufferTagsEqual(&tag_snapshot, expected_tag))
		return NULL;
	return slot;
}


static bool
pcm_x_directory_view(PcmXDirectoryKind kind, PcmXDirectoryView *view)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	Size offset;

	if (header == NULL || view == NULL || kind < 0 || kind >= PCM_X_DIR_COUNT)
		return false;
	memset(view, 0, sizeof(*view));

	switch (kind) {
	case PCM_X_DIR_MASTER_TAG:
		offset = header->layout.pools[PCM_X_POOL_MASTER_TAG].directory_offset;
		view->capacity = header->layout.pools[PCM_X_POOL_MASTER_TAG].directory_capacity;
		view->allocator_kind = PCM_X_ALLOC_MASTER_TAG;
		break;
	case PCM_X_DIR_MASTER_TICKET_PREHANDLE:
		offset = header->layout.master_ticket_directories.prehandle_offset;
		view->capacity = header->layout.master_ticket_directories.prehandle_capacity;
		view->allocator_kind = PCM_X_ALLOC_MASTER_TICKET;
		break;
	case PCM_X_DIR_MASTER_TICKET_HANDLE:
		offset = header->layout.master_ticket_directories.handle_offset;
		view->capacity = header->layout.master_ticket_directories.handle_capacity;
		view->allocator_kind = PCM_X_ALLOC_MASTER_TICKET;
		break;
	case PCM_X_DIR_MASTER_TICKET_RETIRE:
		offset = header->layout.master_ticket_directories.retire_offset;
		view->capacity = header->layout.master_ticket_directories.retire_capacity;
		view->allocator_kind = PCM_X_ALLOC_MASTER_TICKET;
		break;
	case PCM_X_DIR_LOCAL_TAG:
		offset = header->layout.pools[PCM_X_POOL_LOCAL_TAG].directory_offset;
		view->capacity = header->layout.pools[PCM_X_POOL_LOCAL_TAG].directory_capacity;
		view->allocator_kind = PCM_X_ALLOC_LOCAL_TAG;
		break;
	case PCM_X_DIR_LOCAL_WAIT:
		offset = header->layout.local_wait.directory_offset;
		view->capacity = header->layout.local_wait.directory_capacity;
		view->allocator_kind = PCM_X_ALLOC_LOCAL_WAIT;
		break;
	case PCM_X_DIR_LOCAL_HOLDER:
		offset = header->layout.local_holder.directory_offset;
		view->capacity = header->layout.local_holder.directory_capacity;
		view->allocator_kind = PCM_X_ALLOC_LOCAL_HOLDER;
		break;
	case PCM_X_DIR_COUNT:
		return false;
	}
	if (view->capacity == 0)
		return false;
	view->entries = (PcmXDirectoryEntry *)((char *)header + offset);
	return true;
}


static uint64
pcm_x_hash_scalar(uint64 hash, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		hash ^= (uint8)(value >> (i * 8));
		hash *= UINT64CONST(1099511628211);
	}
	return hash;
}


static uint64
pcm_x_hash_tag(uint64 hash, const BufferTag *tag)
{
	hash = pcm_x_hash_scalar(hash, (uint64)tag->spcOid);
	hash = pcm_x_hash_scalar(hash, (uint64)tag->dbOid);
	hash = pcm_x_hash_scalar(hash, (uint64)tag->relNumber);
	hash = pcm_x_hash_scalar(hash, (uint64)(uint32)tag->forkNum);
	return pcm_x_hash_scalar(hash, (uint64)tag->blockNum);
}


/* The sole hash authority for every PCM-X master/local lock partition. */
uint32
cluster_pcm_x_tag_hash(const BufferTag *tag)
{
	uint64 hash = UINT64CONST(1469598103934665603);

	if (tag == NULL)
		return 0;
	hash = pcm_x_hash_tag(hash, tag);
	return (uint32)(hash ^ (hash >> 32));
}


static uint64
pcm_x_hash_wait_identity(uint64 hash, const PcmXWaitIdentity *identity)
{
	hash = pcm_x_hash_tag(hash, &identity->tag);
	hash = pcm_x_hash_scalar(hash, (uint64)(uint32)identity->node_id);
	hash = pcm_x_hash_scalar(hash, (uint64)identity->procno);
	hash = pcm_x_hash_scalar(hash, (uint64)identity->xid);
	hash = pcm_x_hash_scalar(hash, identity->cluster_epoch);
	hash = pcm_x_hash_scalar(hash, identity->request_id);
	hash = pcm_x_hash_scalar(hash, identity->wait_seq);
	return pcm_x_hash_scalar(hash, identity->base_own_generation);
}


/* Hash fields canonically; structure padding is never part of directory identity. */
bool
cluster_pcm_x_directory_key_hash(PcmXDirectoryKind kind, const void *key, uint64 *hash_out)
{
	uint64 hash = UINT64CONST(1469598103934665603);

	if (key == NULL || hash_out == NULL || kind < 0 || kind >= PCM_X_DIR_COUNT)
		return false;
	hash = pcm_x_hash_scalar(hash, (uint64)kind);

	switch (kind) {
	case PCM_X_DIR_MASTER_TAG:
	case PCM_X_DIR_LOCAL_TAG:
		hash = pcm_x_hash_tag(hash, (const BufferTag *)key);
		break;
	case PCM_X_DIR_MASTER_TICKET_PREHANDLE: {
		const PcmXPrehandleKey *prehandle = (const PcmXPrehandleKey *)key;

		hash = pcm_x_hash_scalar(hash, prehandle->sender_session_incarnation);
		hash = pcm_x_hash_scalar(hash, prehandle->prehandle_sequence);
		break;
	}
	case PCM_X_DIR_MASTER_TICKET_HANDLE: {
		const PcmXTicketHandle *handle = (const PcmXTicketHandle *)key;

		hash = pcm_x_hash_scalar(hash, handle->ticket_id);
		hash = pcm_x_hash_scalar(hash, handle->queue_generation);
		break;
	}
	case PCM_X_DIR_MASTER_TICKET_RETIRE: {
		const PcmXRetirePayload *retire = (const PcmXRetirePayload *)key;

		hash = pcm_x_hash_scalar(hash, retire->cluster_epoch);
		hash = pcm_x_hash_scalar(hash, retire->master_session_incarnation);
		hash = pcm_x_hash_scalar(hash, retire->retire_through_ticket_id);
		break;
	}
	case PCM_X_DIR_LOCAL_WAIT: {
		const PcmXWaitIdentity *identity = (const PcmXWaitIdentity *)key;

		hash = pcm_x_hash_wait_identity(hash, identity);
		break;
	}
	case PCM_X_DIR_LOCAL_HOLDER: {
		const PcmXLocalHolderKey *holder = (const PcmXLocalHolderKey *)key;

		hash = pcm_x_hash_wait_identity(hash, &holder->identity);
		hash = pcm_x_hash_scalar(hash, (uint64)(uint32)holder->buffer_id);
		break;
	}
	case PCM_X_DIR_COUNT:
		return false;
	}

	*hash_out = hash;
	return true;
}


static bool
pcm_x_wait_identity_equal(const PcmXWaitIdentity *left, const PcmXWaitIdentity *right)
{
	return BufferTagsEqual(&left->tag, &right->tag) && left->node_id == right->node_id
		   && left->procno == right->procno && left->xid == right->xid
		   && left->cluster_epoch == right->cluster_epoch && left->request_id == right->request_id
		   && left->wait_seq == right->wait_seq
		   && left->base_own_generation == right->base_own_generation;
}


static void
pcm_x_retire_key_from_ticket(const PcmXMasterTicketSlot *ticket, PcmXRetirePayload *key_out)
{
	memset(key_out, 0, sizeof(*key_out));
	key_out->cluster_epoch = ticket->ref.identity.cluster_epoch;
	key_out->master_session_incarnation = ticket->master_session_incarnation;
	key_out->retire_through_ticket_id = ticket->ref.handle.ticket_id;
}


static bool
pcm_x_retire_key_equal(const PcmXRetirePayload *left, const PcmXRetirePayload *right)
{
	return left->cluster_epoch == right->cluster_epoch
		   && left->master_session_incarnation == right->master_session_incarnation
		   && left->retire_through_ticket_id == right->retire_through_ticket_id;
}


static bool
pcm_x_directory_key_matches_slot(PcmXDirectoryKind kind, const void *key,
								 const PcmXSlotHeader *slot)
{
	switch (kind) {
	case PCM_X_DIR_MASTER_TAG:
		return BufferTagsEqual((const BufferTag *)key, &((const PcmXMasterTagSlot *)slot)->tag);
	case PCM_X_DIR_MASTER_TICKET_PREHANDLE: {
		const PcmXPrehandleKey *left = (const PcmXPrehandleKey *)key;
		const PcmXPrehandleKey *right = &((const PcmXMasterTicketSlot *)slot)->prehandle;

		return left->sender_session_incarnation == right->sender_session_incarnation
			   && left->prehandle_sequence == right->prehandle_sequence;
	}
	case PCM_X_DIR_MASTER_TICKET_HANDLE: {
		const PcmXTicketHandle *left = (const PcmXTicketHandle *)key;
		const PcmXTicketHandle *right = &((const PcmXMasterTicketSlot *)slot)->ref.handle;

		return left->ticket_id == right->ticket_id
			   && left->queue_generation == right->queue_generation;
	}
	case PCM_X_DIR_MASTER_TICKET_RETIRE: {
		const PcmXRetirePayload *retire = (const PcmXRetirePayload *)key;
		const PcmXMasterTicketSlot *ticket = (const PcmXMasterTicketSlot *)slot;
		PcmXRetirePayload resident;

		pcm_x_retire_key_from_ticket(ticket, &resident);
		return pcm_x_retire_key_equal(retire, &resident);
	}
	case PCM_X_DIR_LOCAL_TAG:
		return BufferTagsEqual((const BufferTag *)key, &((const PcmXLocalTagSlot *)slot)->tag);
	case PCM_X_DIR_LOCAL_WAIT:
		return pcm_x_wait_identity_equal((const PcmXWaitIdentity *)key,
										 &((const PcmXLocalMembershipSlot *)slot)->identity);
	case PCM_X_DIR_LOCAL_HOLDER: {
		const PcmXLocalHolderKey *holder = (const PcmXLocalHolderKey *)key;
		const PcmXLocalMembershipSlot *membership = (const PcmXLocalMembershipSlot *)slot;

		return holder->buffer_id == membership->buffer_id
			   && pcm_x_wait_identity_equal(&holder->identity, &membership->identity);
	}
	case PCM_X_DIR_COUNT:
		return false;
	}
	return false;
}


static bool
pcm_x_directory_slot_key_hash(PcmXDirectoryKind kind, const PcmXSlotHeader *slot, uint64 *hash_out)
{
	switch (kind) {
	case PCM_X_DIR_MASTER_TAG:
		return cluster_pcm_x_directory_key_hash(kind, &((const PcmXMasterTagSlot *)slot)->tag,
												hash_out);
	case PCM_X_DIR_MASTER_TICKET_PREHANDLE:
		return cluster_pcm_x_directory_key_hash(
			kind, &((const PcmXMasterTicketSlot *)slot)->prehandle, hash_out);
	case PCM_X_DIR_MASTER_TICKET_HANDLE:
		return cluster_pcm_x_directory_key_hash(
			kind, &((const PcmXMasterTicketSlot *)slot)->ref.handle, hash_out);
	case PCM_X_DIR_MASTER_TICKET_RETIRE: {
		PcmXRetirePayload retire;

		pcm_x_retire_key_from_ticket((const PcmXMasterTicketSlot *)slot, &retire);
		return cluster_pcm_x_directory_key_hash(kind, &retire, hash_out);
	}
	case PCM_X_DIR_LOCAL_TAG:
		return cluster_pcm_x_directory_key_hash(kind, &((const PcmXLocalTagSlot *)slot)->tag,
												hash_out);
	case PCM_X_DIR_LOCAL_WAIT:
		return cluster_pcm_x_directory_key_hash(
			kind, &((const PcmXLocalMembershipSlot *)slot)->identity, hash_out);
	case PCM_X_DIR_LOCAL_HOLDER: {
		const PcmXLocalMembershipSlot *membership = (const PcmXLocalMembershipSlot *)slot;
		PcmXLocalHolderKey holder;

		memset(&holder, 0, sizeof(holder));
		holder.identity = membership->identity;
		holder.buffer_id = membership->buffer_id;
		return cluster_pcm_x_directory_key_hash(kind, &holder, hash_out);
	}
	case PCM_X_DIR_COUNT:
		return false;
	}
	return false;
}


static PcmXDirectoryMatch
pcm_x_directory_entry_match_locked(PcmXDirectoryKind kind, const PcmXDirectoryView *directory,
								   const PcmXDirectoryEntry *entry, const void *key)
{
	PcmXAllocatorView allocator;
	PcmXSlotHeader *slot;
	uint64 slot_generation;
	uint64 resident_key_hash;
	uint32 slot_state;

	if (!pcm_x_allocator_view(directory->allocator_kind, &allocator))
		return PCM_X_DIRECTORY_MATCH_CORRUPT;
	slot = pcm_x_allocator_slot(&allocator, entry->slot_index);
	if (slot == NULL)
		return PCM_X_DIRECTORY_MATCH_CORRUPT;
	slot_state = pcm_x_slot_state_read(slot);
	if (!pcm_x_slot_generation_read(slot, &slot_generation))
		return PCM_X_DIRECTORY_MATCH_CORRUPT;
	if (slot_generation != entry->slot_generation || slot_state == PCM_X_SLOT_FREE
		|| slot_state == PCM_X_SLOT_GENERATION_EXHAUSTED)
		return PCM_X_DIRECTORY_MATCH_STALE;
	/* The entry hash is also the immutable-key checksum for its resident slot. */
	if (!pcm_x_directory_slot_key_hash(kind, slot, &resident_key_hash)
		|| resident_key_hash != entry->key_hash)
		return PCM_X_DIRECTORY_MATCH_CORRUPT;
	return pcm_x_directory_key_matches_slot(kind, key, slot) ? PCM_X_DIRECTORY_MATCH_EXACT
															 : PCM_X_DIRECTORY_MATCH_DIFFERENT;
}


static void
pcm_x_directory_entry_publish(PcmXDirectoryEntry *entry, uint64 key_hash, PcmXSlotRef ref)
{
	entry->key_hash = key_hash;
	entry->slot_index = ref.slot_index;
	entry->slot_generation = ref.slot_generation;
	entry->reserved = 0;
	pg_write_barrier();
	entry->state = PCM_X_DIRECTORY_OCCUPIED;
}


static void
pcm_x_directory_entry_clear(PcmXDirectoryEntry *entry)
{
	entry->key_hash = 0;
	entry->slot_index = 0;
	entry->slot_generation = 0;
	entry->reserved = 0;
	pg_write_barrier();
	entry->state = PCM_X_DIRECTORY_EMPTY;
}


static Size
pcm_x_directory_distance(Size home, Size position, Size capacity)
{
	return position >= home ? position - home : capacity - home + position;
}


static PcmXDirectoryResult
pcm_x_directory_backshift_delete_locked(PcmXDirectoryView *directory, Size hole)
{
	Size scan = (hole + 1) % directory->capacity;
	Size steps;

	for (steps = 0; steps < directory->capacity; steps++) {
		PcmXDirectoryEntry *entry = &directory->entries[scan];

		if (entry->state == PCM_X_DIRECTORY_EMPTY) {
			pcm_x_directory_entry_clear(&directory->entries[hole]);
			return PCM_X_DIRECTORY_OK;
		}
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_DIRECTORY_CORRUPT;
		if (pcm_x_directory_distance(entry->key_hash % directory->capacity, hole,
									 directory->capacity)
			< pcm_x_directory_distance(entry->key_hash % directory->capacity, scan,
									   directory->capacity)) {
			PcmXSlotRef ref = { entry->slot_index, entry->slot_generation };

			pcm_x_directory_entry_publish(&directory->entries[hole], entry->key_hash, ref);
			hole = scan;
		}
		scan = (scan + 1) % directory->capacity;
	}
	return PCM_X_DIRECTORY_CORRUPT;
}


static PcmXDirectoryResult
pcm_x_directory_find_locked(PcmXDirectoryKind kind, const void *key, PcmXSlotRef *found_out)
{
	PcmXDirectoryView directory;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || found_out == NULL || !LWLockHeldByMe(&header->allocator_lock.lock)
		|| !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	found_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	found_out->slot_generation = 0;
	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			return PCM_X_DIRECTORY_NOT_FOUND;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_DIRECTORY_CORRUPT;
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			found_out->slot_index = entry->slot_index;
			found_out->slot_generation = entry->slot_generation;
			return PCM_X_DIRECTORY_OK;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE)
			return PCM_X_DIRECTORY_STALE_REF;
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT)
			return PCM_X_DIRECTORY_CORRUPT;
	}
	return PCM_X_DIRECTORY_NOT_FOUND;
}


static PcmXDirectoryResult
pcm_x_directory_insert_locked(PcmXDirectoryKind kind, const void *key, PcmXSlotRef candidate,
							  PcmXSlotRef *existing_out)
{
	PcmXDirectoryView directory;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *candidate_slot;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || existing_out == NULL
		|| !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	existing_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	existing_out->slot_generation = 0;
	candidate_slot = pcm_x_slot_ref_resolve_locked(directory.allocator_kind, candidate);
	if (candidate_slot == NULL)
		return PCM_X_DIRECTORY_STALE_REF;
	if (!pcm_x_directory_key_matches_slot(kind, key, candidate_slot))
		return PCM_X_DIRECTORY_INVALID;

	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY) {
			pcm_x_directory_entry_publish(entry, key_hash, candidate);
			return PCM_X_DIRECTORY_OK;
		}
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_DIRECTORY_CORRUPT;
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			existing_out->slot_index = entry->slot_index;
			existing_out->slot_generation = entry->slot_generation;
			return PCM_X_DIRECTORY_EXISTS;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE)
			return PCM_X_DIRECTORY_STALE_REF;
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT)
			return PCM_X_DIRECTORY_CORRUPT;
	}
	return PCM_X_DIRECTORY_FULL;
}


/*
 * Publish the target key of one gated local-leader rekey while the resident
 * membership still carries its old identity.  The admission gate is the
 * cross-lock transaction marker; only the later local-lock phase may replace
 * resident_identity.  This helper therefore validates the exact old resident
 * instead of applying the ordinary directory checksum prerequisite to the
 * target key.
 */
static PcmXDirectoryResult
pcm_x_directory_insert_rekey_target_locked(const PcmXWaitIdentity *target_identity,
										   const PcmXWaitIdentity *resident_identity,
										   PcmXSlotRef candidate, PcmXSlotRef *existing_out)
{
	PcmXDirectoryView directory;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalMembershipSlot *candidate_slot;
	PcmXWaitIdentity expected_target;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || target_identity == NULL || resident_identity == NULL
		|| existing_out == NULL || !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_WAIT, target_identity, &key_hash)
		|| !pcm_x_directory_view(PCM_X_DIR_LOCAL_WAIT, &directory))
		return PCM_X_DIRECTORY_INVALID;
	existing_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	existing_out->slot_generation = 0;
	expected_target = *resident_identity;
	expected_target.base_own_generation = target_identity->base_own_generation;
	if (!pcm_x_wait_identity_equal(&expected_target, target_identity))
		return PCM_X_DIRECTORY_INVALID;
	candidate_slot = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(
		PCM_X_ALLOC_LOCAL_WAIT, candidate);
	if (candidate_slot == NULL)
		return PCM_X_DIRECTORY_STALE_REF;
	if (candidate_slot->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| !pcm_x_wait_identity_equal(&candidate_slot->identity, resident_identity))
		return PCM_X_DIRECTORY_INVALID;

	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY) {
			pcm_x_directory_entry_publish(entry, key_hash, candidate);
			return PCM_X_DIRECTORY_OK;
		}
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_DIRECTORY_CORRUPT;
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(PCM_X_DIR_LOCAL_WAIT, &directory, entry,
												   target_identity);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			existing_out->slot_index = entry->slot_index;
			existing_out->slot_generation = entry->slot_generation;
			return PCM_X_DIRECTORY_EXISTS;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE)
			return PCM_X_DIRECTORY_STALE_REF;
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT)
			return PCM_X_DIRECTORY_CORRUPT;
	}
	return PCM_X_DIRECTORY_FULL;
}


static PcmXDirectoryResult
pcm_x_directory_delete_exact_locked(PcmXDirectoryKind kind, const void *key, PcmXSlotRef expected)
{
	PcmXDirectoryView directory;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		Size position = (bucket + step) % directory.capacity;
		PcmXDirectoryEntry *entry = &directory.entries[position];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			return PCM_X_DIRECTORY_NOT_FOUND;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_DIRECTORY_CORRUPT;
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			if (entry->slot_index != expected.slot_index
				|| entry->slot_generation != expected.slot_generation)
				return PCM_X_DIRECTORY_STALE_REF;
			return pcm_x_directory_backshift_delete_locked(&directory, position);
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE)
			return PCM_X_DIRECTORY_STALE_REF;
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT)
			return PCM_X_DIRECTORY_CORRUPT;
	}
	return PCM_X_DIRECTORY_NOT_FOUND;
}


PcmXDirectoryResult
cluster_pcm_x_directory_find(PcmXDirectoryKind kind, const void *key, PcmXSlotRef *found_out)
{
	PcmXDirectoryView directory;
	PcmXDirectoryResult result = PCM_X_DIRECTORY_NOT_FOUND;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (found_out == NULL || header == NULL
		|| !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	found_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	found_out->slot_generation = 0;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_DIRECTORY_INVALID;
	}
	bucket = key_hash % directory.capacity;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			break;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			found_out->slot_index = entry->slot_index;
			found_out->slot_generation = entry->slot_generation;
			result = PCM_X_DIRECTORY_OK;
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE) {
			result = PCM_X_DIRECTORY_STALE_REF;
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_DIRECTORY_STALE_REF || result == PCM_X_DIRECTORY_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXDirectoryResult
cluster_pcm_x_directory_insert(PcmXDirectoryKind kind, const void *key, PcmXSlotRef candidate,
							   PcmXSlotRef *existing_out)
{
	PcmXDirectoryView directory;
	PcmXDirectoryResult result = PCM_X_DIRECTORY_FULL;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *candidate_slot;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (existing_out == NULL || header == NULL
		|| !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	existing_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	existing_out->slot_generation = 0;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_DIRECTORY_INVALID;
	}
	bucket = key_hash % directory.capacity;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	candidate_slot = pcm_x_slot_ref_resolve_locked(directory.allocator_kind, candidate);
	if (candidate_slot == NULL) {
		result = PCM_X_DIRECTORY_STALE_REF;
		goto done;
	}
	if (!pcm_x_directory_key_matches_slot(kind, key, candidate_slot)) {
		result = PCM_X_DIRECTORY_INVALID;
		goto done;
	}

	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY) {
			pcm_x_directory_entry_publish(entry, key_hash, candidate);
			result = PCM_X_DIRECTORY_OK;
			break;
		}
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			existing_out->slot_index = entry->slot_index;
			existing_out->slot_generation = entry->slot_generation;
			result = PCM_X_DIRECTORY_EXISTS;
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE) {
			result = PCM_X_DIRECTORY_STALE_REF;
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
	}

done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_DIRECTORY_STALE_REF || result == PCM_X_DIRECTORY_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXDirectoryResult
cluster_pcm_x_directory_delete_exact(PcmXDirectoryKind kind, const void *key, PcmXSlotRef expected)
{
	PcmXDirectoryView directory;
	PcmXDirectoryResult result = PCM_X_DIRECTORY_NOT_FOUND;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || !cluster_pcm_x_directory_key_hash(kind, key, &key_hash)
		|| !pcm_x_directory_view(kind, &directory))
		return PCM_X_DIRECTORY_INVALID;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_DIRECTORY_INVALID;
	}
	bucket = key_hash % directory.capacity;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	for (step = 0; step < directory.capacity; step++) {
		Size position = (bucket + step) % directory.capacity;
		PcmXDirectoryEntry *entry = &directory.entries[position];
		PcmXDirectoryMatch match;

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			break;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
		if (entry->key_hash != key_hash)
			continue;
		match = pcm_x_directory_entry_match_locked(kind, &directory, entry, key);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			if (entry->slot_index != expected.slot_index
				|| entry->slot_generation != expected.slot_generation)
				result = PCM_X_DIRECTORY_STALE_REF;
			else
				result = pcm_x_directory_backshift_delete_locked(&directory, position);
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE) {
			result = PCM_X_DIRECTORY_STALE_REF;
			break;
		}
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT) {
			result = PCM_X_DIRECTORY_CORRUPT;
			break;
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_DIRECTORY_STALE_REF || result == PCM_X_DIRECTORY_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Return the master/local partition selected by a stable BufferTag hash. */
uint32
cluster_pcm_x_lock_partition(uint32 tag_hash)
{
	return tag_hash & (PCM_X_LOCK_PARTITIONS - 1);
}


/* Frozen application-message successor table for live and terminal ticket slots. */
static bool
pcm_x_master_event_successor_state(PcmXMasterTicketState current, PcmXMasterEvent event)
{
	switch (event) {
	case PCM_X_EVENT_ADMIT_CONFIRM:
		return current >= PCM_XT_QUEUED && current <= PCM_XT_RETIRE_CREDIT;
	case PCM_X_EVENT_COMMIT_COMPLETE:
		return current == PCM_XT_COMPLETE || current == PCM_XT_RETIRE_CREDIT;
	case PCM_X_EVENT_CANCEL_EXACT:
		return current == PCM_XT_CANCELLED || current == PCM_XT_RETIRE_CREDIT;
	default:
		return false;
	}
}


static uint64
pcm_x_master_event_tombstone(PcmXMasterEvent event)
{
	switch (event) {
	case PCM_X_EVENT_ADMIT_CONFIRM:
		return PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM;
	case PCM_X_EVENT_COMMIT_COMPLETE:
		return PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
	case PCM_X_EVENT_CANCEL_EXACT:
		return PCM_X_RESPONSE_TOMBSTONE_CANCEL;
	default:
		return 0;
	}
}


static bool
pcm_x_master_event_replay_exact(PcmXMasterTicketState current, PcmXMasterEvent event,
								uint64 response_tombstones)
{
	uint64 required = pcm_x_master_event_tombstone(event);

	return required != 0 && pcm_x_master_event_successor_state(current, event)
		   && (response_tombstones & required) != 0;
}


/*
 * cluster_pcm_x_master_step -- Pure authority for master ticket transitions.
 *
 * Callers must separately prove exact slot/ref identity.  The EXACT guard is
 * mandatory for every transition after local reserve.  Missing positive
 * gates leave the state unchanged; post-authorization cancel is converted to
 * RECOVERY_BLOCKED instead of guessing rollback.
 */
PcmXStepResult
cluster_pcm_x_master_step(PcmXMasterTicketState current, PcmXMasterEvent event, uint32 guards,
						  PcmXMasterTicketState *next)
{
	uint64 response_tombstones = 0;

	if (next == NULL)
		return PCM_X_STEP_BLOCKED;
	*next = current;

	if ((guards & PCM_X_G_EXACT) == 0)
		return PCM_X_STEP_STALE;
	if ((guards & PCM_X_G_CONFIRM_TOMBSTONE) != 0)
		response_tombstones |= PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM;
	if ((guards & PCM_X_G_COMPLETE_TOMBSTONE) != 0)
		response_tombstones |= PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
	if ((guards & PCM_X_G_CANCEL_TOMBSTONE) != 0)
		response_tombstones |= PCM_X_RESPONSE_TOMBSTONE_CANCEL;

	if (current == PCM_XT_RECOVERY_BLOCKED && event == PCM_X_EVENT_FAULT)
		return PCM_X_STEP_DUPLICATE;
	if (event == PCM_X_EVENT_FAULT && current != PCM_XT_FREE
		&& current != PCM_XT_GENERATION_EXHAUSTED) {
		*next = PCM_XT_RECOVERY_BLOCKED;
		return PCM_X_STEP_APPLIED;
	}

	if (event == PCM_X_EVENT_ADMIT_CONFIRM && current != PCM_XT_ADMITTING) {
		if (pcm_x_master_event_replay_exact(current, event, response_tombstones))
			return PCM_X_STEP_DUPLICATE;
		return PCM_X_STEP_BLOCKED;
	}
	if ((event == PCM_X_EVENT_COMMIT_COMPLETE || event == PCM_X_EVENT_CANCEL_EXACT)
		&& pcm_x_master_event_replay_exact(current, event, response_tombstones))
		return PCM_X_STEP_DUPLICATE;
	if (event == PCM_X_EVENT_DRAIN_EXACT && current == PCM_XT_RETIRE_CREDIT)
		return PCM_X_STEP_DUPLICATE;

	switch (event) {
	case PCM_X_EVENT_PUBLISH_ADMIT:
		if (current == PCM_XT_RESERVED_NONVISIBLE) {
			*next = PCM_XT_ADMITTING;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_ADMIT_CONFIRM:
		if (current == PCM_XT_ADMITTING && (guards & PCM_X_G_WFG_READY) != 0) {
			*next = PCM_XT_QUEUED;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_PROMOTE_HEAD:
		if (current == PCM_XT_QUEUED && (guards & PCM_X_G_HEAD) != 0) {
			*next = PCM_XT_ACTIVE_PROBE;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_BLOCKERS_COMMITTED:
		if (current == PCM_XT_ACTIVE_PROBE && (guards & PCM_X_G_WFG_READY) != 0
			&& (guards & PCM_X_G_REVERSIBLE) != 0) {
			*next = PCM_XT_ACTIVE_TRANSFER;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_COMMIT_COMPLETE:
		if (current == PCM_XT_ACTIVE_TRANSFER) {
			*next = PCM_XT_COMPLETE;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_CANCEL_EXACT:
		if ((current == PCM_XT_RESERVED_NONVISIBLE || current == PCM_XT_ADMITTING
			 || current == PCM_XT_QUEUED || current == PCM_XT_ACTIVE_PROBE)
			&& (guards & PCM_X_G_REVERSIBLE) != 0) {
			*next = PCM_XT_CANCELLED;
			return PCM_X_STEP_APPLIED;
		}
		if (current == PCM_XT_ACTIVE_TRANSFER) {
			*next = PCM_XT_RECOVERY_BLOCKED;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_DRAIN_EXACT:
		if ((current == PCM_XT_COMPLETE || current == PCM_XT_CANCELLED)
			&& (guards & PCM_X_G_DRAINED) != 0) {
			*next = PCM_XT_RETIRE_CREDIT;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_RETIRE_ACK_EXACT:
		if (current == PCM_XT_RETIRE_CREDIT && (guards & PCM_X_G_RETIRED) != 0) {
			*next = PCM_XT_DETACHING;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_DETACH_EXACT:
		if (current == PCM_XT_DETACHING) {
			*next = PCM_XT_FREE;
			return PCM_X_STEP_APPLIED;
		}
		break;
	case PCM_X_EVENT_FAULT:
		break;
	}

	return PCM_X_STEP_BLOCKED;
}


/*
 * Sample the cross-partition gate together with its master-session authority.
 *
 * pg_atomic_read_u32() has no barrier semantics in this tree.  An ACTIVE
 * observation therefore needs an explicit read barrier before consuming the
 * session published by the activating CAS, followed by a second barrier and
 * state read.  Any transition during the sample fails closed instead of
 * returning a session detached from the ACTIVE incarnation that published it.
 */
PcmXRuntimeSnapshot
cluster_pcm_x_runtime_snapshot(void)
{
	PcmXRuntimeSnapshot snapshot = { 0 };
	uint32 gate1;
	uint32 gate2;
	uint32 rebase_wire_active;
	uint32 state1;
	uint64 session;

	if (ClusterPcmXConvertShmem == NULL)
		return snapshot;

	gate1 = pg_atomic_read_u32(&ClusterPcmXConvertShmem->runtime_gate);
	state1 = pcm_x_runtime_gate_state(gate1);
	snapshot.gate_generation = pcm_x_runtime_gate_generation(gate1);
	if (state1 != PCM_X_RUNTIME_ACTIVE) {
		if (state1 <= PCM_X_RUNTIME_SHUTTING_DOWN)
			snapshot.state = (PcmXRuntimeState)state1;
		return snapshot;
	}

	pg_read_barrier();
	session = ClusterPcmXConvertShmem->master_session_incarnation;
	rebase_wire_active = ClusterPcmXConvertShmem->rebase_wire_active;
	pg_read_barrier();
	gate2 = pg_atomic_read_u32(&ClusterPcmXConvertShmem->runtime_gate);
	if (gate1 != gate2 || session == 0)
		return snapshot;

	snapshot.state = PCM_X_RUNTIME_ACTIVE;
	snapshot.master_session_incarnation = session;
	snapshot.rebase_wire_active = rebase_wire_active != 0;
	return snapshot;
}


void
cluster_pcm_x_runtime_set_rebase_wire_active(bool active)
{
	if (ClusterPcmXConvertShmem == NULL)
		return;
	ClusterPcmXConvertShmem->rebase_wire_active = active ? 1 : 0;
}


/*
 * Revalidate one fail-stop acting token at the final owner lock.
 *
 * The second exact snapshot is the operation's authority linearization point:
 * a close observed first returns NOT_READY with no visible mutation; a close
 * that follows is ordered after this already-authorized operation.  Composite
 * admit/join operations take this point under allocator_lock before publishing
 * any directory entry, then must finish their domain phase or leave evidence.
 */
static bool
pcm_x_runtime_token_exact(const PcmXRuntimeSnapshot *start, uint64 expected_master_session)
{
	PcmXRuntimeSnapshot current;

	if (start == NULL || start->state != PCM_X_RUNTIME_ACTIVE
		|| start->master_session_incarnation == 0 || start->gate_generation == 0)
		return false;
	current = cluster_pcm_x_runtime_snapshot();
	return current.state == PCM_X_RUNTIME_ACTIVE
		   && current.gate_generation == start->gate_generation
		   && current.master_session_incarnation == start->master_session_incarnation
		   && (expected_master_session == 0
			   || expected_master_session == start->master_session_incarnation);
}


/*
 * Claim a blocked gate, install a strictly newer session, then publish ACTIVE.
 *
 * The private ACTIVATING phase prevents concurrent publishers from racing the
 * fenced session store.  Its generation is already the next public generation,
 * so the final ACTIVATING-to-ACTIVE CAS does not consume another value.  A
 * process failure after the claim leaves the gate in a private phase that all
 * readers normalize to RECOVERY_BLOCKED.
 */
bool
cluster_pcm_x_runtime_activate_bound(uint64 master_session_incarnation,
									 const PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT])
{
	uint32 active_gate;
	uint32 activation_retry_generation;
	uint32 claimed_gate;
	uint64 current_session;
	uint32 expected_gate;
	uint32 generation;
	int i;

	if (ClusterPcmXConvertShmem == NULL || master_session_incarnation == 0)
		return false;
	if (bindings != NULL) {
		for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
			/* CLUSTER_EPOCH_INITIAL (zero) is a live, wire-visible epoch.
			 * A binding is absent only when its peer session is absent. */
			if (bindings[i].peer_session_incarnation == 0 && bindings[i].cluster_epoch != 0)
				return false;
		}
	}

	expected_gate = pg_atomic_read_u32(&ClusterPcmXConvertShmem->runtime_gate);
	if (pcm_x_runtime_gate_state(expected_gate) != PCM_X_RUNTIME_RECOVERY_BLOCKED)
		return false;
	generation = pcm_x_runtime_gate_generation(expected_gate);
	if (generation >= PCM_X_RUNTIME_GATE_GENERATION_MAX - 1)
		return false;
	current_session = ClusterPcmXConvertShmem->master_session_incarnation;
	activation_retry_generation
		= pg_atomic_read_u32(&ClusterPcmXConvertShmem->activation_retry_generation);
	/*
	 * A normal ACTIVE-to-BLOCKED transition is permanently fail-stop in the
	 * steady-state core.  Re-entry is authorized only for pristine startup or
	 * by the exact marker published after recovering a stranded ACTIVATING CAS.
	 */
	if (!((generation == 0 && current_session == 0 && activation_retry_generation == 0)
		  || (generation != 0 && activation_retry_generation == generation)))
		return false;
	if (master_session_incarnation <= current_session)
		return false;

	claimed_gate = pcm_x_runtime_gate_pack(generation + 1, PCM_X_RUNTIME_GATE_ACTIVATING);
	if (!pg_atomic_compare_exchange_u32(&ClusterPcmXConvertShmem->runtime_gate, &expected_gate,
										claimed_gate))
		return false;

	for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
		PcmXOutboundTargetFrontier *outbound = &ClusterPcmXConvertShmem->outbound_targets[i];
		uint64 peer_epoch = bindings == NULL ? 0 : bindings[i].cluster_epoch;
		uint64 peer_session = bindings == NULL ? 0 : bindings[i].peer_session_incarnation;

		ClusterPcmXConvertShmem->peer_frontiers[i].cluster_epoch = peer_epoch;
		ClusterPcmXConvertShmem->peer_frontiers[i].sender_session_incarnation = peer_session;
		ClusterPcmXConvertShmem->peer_frontiers[i].next_expected_prehandle_sequence = 1;
		ClusterPcmXConvertShmem->peer_frontiers[i].retired_prehandle_sequence = 0;
		pg_atomic_write_u32(&outbound->mint_gate, 0);
		outbound->flags = PCM_X_OUTBOUND_TARGET_INITIALIZED
						  | (peer_session == 0 ? 0 : PCM_X_OUTBOUND_TARGET_BOUND);
		outbound->cluster_epoch = peer_epoch;
		outbound->target_session_incarnation = peer_session;
		outbound->next_prehandle_sequence = 1;
	}
	ClusterPcmXConvertShmem->master_session_incarnation = master_session_incarnation;
	pg_atomic_write_u32(&ClusterPcmXConvertShmem->activation_retry_generation, 0);
	pg_write_barrier();
	active_gate = pcm_x_runtime_gate_pack(generation + 1, PCM_X_RUNTIME_ACTIVE);
	if (!pg_atomic_compare_exchange_u32(&ClusterPcmXConvertShmem->runtime_gate, &claimed_gate,
										active_gate))
		return false;
	return true;
}


bool
cluster_pcm_x_runtime_activate(uint64 master_session_incarnation)
{
	return cluster_pcm_x_runtime_activate_bound(master_session_incarnation, NULL);
}


/*
 * Revalidate one already-published peer binding without extending authority.
 *
 * The caller enters without allocator or tag-domain locks.  allocator_lock
 * makes the two frontiers one immutable identity sample, while the runtime
 * token binds that sample to the ACTIVE gate observed before lock acquisition.
 * An empty frontier is a mismatch: only activation/recovery may publish it.
 */
PcmXQueueResult
cluster_pcm_x_runtime_peer_binding_revalidate_exact(int32 peer_node, uint64 cluster_epoch,
													uint64 peer_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXPeerFrontier *frontier;
	PcmXOutboundTargetFrontier *outbound;
	PcmXRuntimeSnapshot runtime;
	PcmXQueueResult result;
	bool fail_closed = false;

	if (header == NULL || peer_node < 0 || peer_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| peer_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (!pcm_x_allocator_entry_unlocked(header)) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_INVALID;
	}

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto done;
	}
	frontier = &header->peer_frontiers[peer_node];
	outbound = &header->outbound_targets[peer_node];
	if (frontier->cluster_epoch == cluster_epoch
		&& frontier->sender_session_incarnation == peer_session
		&& frontier->next_expected_prehandle_sequence != 0
		&& frontier->retired_prehandle_sequence < frontier->next_expected_prehandle_sequence
		&& outbound->flags == (PCM_X_OUTBOUND_TARGET_INITIALIZED | PCM_X_OUTBOUND_TARGET_BOUND)
		&& outbound->cluster_epoch == cluster_epoch
		&& outbound->target_session_incarnation == peer_session
		&& outbound->next_prehandle_sequence != 0)
		result = PCM_X_QUEUE_OK;
	else {
		result = PCM_X_QUEUE_STALE;
		fail_closed = true;
	}

done:
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


/*
 * Recovery-only escape from a publisher that died after claiming ACTIVATING.
 *
 * The caller must already have external proof that the activating publisher is
 * gone.  The exact packed generation prevents an old recovery action from
 * closing a later activation.  The fenced session value remains monotonic; a
 * subsequent activation must publish a strictly newer session.
 */
bool
cluster_pcm_x_runtime_reset_activating(uint32 expected_gate_generation)
{
	uint32 blocked_gate;
	uint32 expected_gate;

	if (ClusterPcmXConvertShmem == NULL || expected_gate_generation == 0
		|| expected_gate_generation > PCM_X_RUNTIME_GATE_GENERATION_MAX)
		return false;
	expected_gate
		= pcm_x_runtime_gate_pack(expected_gate_generation, PCM_X_RUNTIME_GATE_ACTIVATING);
	blocked_gate
		= pcm_x_runtime_gate_pack(expected_gate_generation, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	if (!pg_atomic_compare_exchange_u32(&ClusterPcmXConvertShmem->runtime_gate, &expected_gate,
										blocked_gate))
		return false;
	/* A crash before this store only leaves a safely non-restartable gate. */
	pg_write_barrier();
	pg_atomic_write_u32(&ClusterPcmXConvertShmem->activation_retry_generation,
						expected_gate_generation);
	pcm_x_stats_increment(&ClusterPcmXConvertShmem->stats.activating_reset_count);
	return true;
}


/* Publish one legal non-activation transition and advance the ABA generation. */
bool
cluster_pcm_x_runtime_transition(PcmXRuntimeState expected, PcmXRuntimeState desired)
{
	uint32 desired_gate;
	uint32 expected_gate;
	uint32 exhausted_gate;
	uint32 generation;

	if (ClusterPcmXConvertShmem == NULL || expected == desired
		|| expected > PCM_X_RUNTIME_SHUTTING_DOWN || desired > PCM_X_RUNTIME_SHUTTING_DOWN
		|| desired == PCM_X_RUNTIME_ACTIVE)
		return false;
	if (!((expected == PCM_X_RUNTIME_ACTIVE && desired == PCM_X_RUNTIME_RECOVERY_BLOCKED)
		  || (expected == PCM_X_RUNTIME_ACTIVE && desired == PCM_X_RUNTIME_SHUTTING_DOWN)
		  || (expected == PCM_X_RUNTIME_SHUTTING_DOWN
			  && desired == PCM_X_RUNTIME_RECOVERY_BLOCKED)))
		return false;

	expected_gate = pg_atomic_read_u32(&ClusterPcmXConvertShmem->runtime_gate);
	if (pcm_x_runtime_gate_state(expected_gate) != (uint32)expected)
		return false;
	generation = pcm_x_runtime_gate_generation(expected_gate);
	if (generation == PCM_X_RUNTIME_GATE_GENERATION_MAX
		|| (desired == PCM_X_RUNTIME_SHUTTING_DOWN
			&& generation >= PCM_X_RUNTIME_GATE_GENERATION_MAX - 1)) {
		/* Never leave an ACTIVE gate open merely because its version is spent. */
		exhausted_gate = pcm_x_runtime_gate_pack(
			generation == PCM_X_RUNTIME_GATE_GENERATION_MAX ? generation : generation + 1,
			PCM_X_RUNTIME_GATE_EXHAUSTED);
		(void)pg_atomic_compare_exchange_u32(&ClusterPcmXConvertShmem->runtime_gate, &expected_gate,
											 exhausted_gate);
		return false;
	}
	desired_gate = pcm_x_runtime_gate_pack(generation + 1, (uint32)desired);

	return pg_atomic_compare_exchange_u32(&ClusterPcmXConvertShmem->runtime_gate, &expected_gate,
										  desired_gate);
}


/* Only the short PREPARE-to-COMMIT transfer window may reserve ownership. */
bool
cluster_pcm_x_ownership_reservation_allowed(PcmXMasterTicketState state)
{
	return state == PCM_XT_ACTIVE_TRANSFER;
}


/* Terminal tickets detach from the hot queue only after every node drains. */
bool
cluster_pcm_x_ticket_drain_ready(const PcmXMasterTicketSlot *ticket)
{
	uint32 state;

	if (ticket == NULL || ticket->involved_nodes_bitmap == 0)
		return false;
	state = pcm_x_slot_state_read(&ticket->slot);
	if (state != PCM_XT_COMPLETE && state != PCM_XT_CANCELLED)
		return false;
	return ticket->drained_nodes_bitmap == ticket->involved_nodes_bitmap;
}


/* RETIRE_CREDIT is reusable only after every involved peer application-ACKs. */
bool
cluster_pcm_x_ticket_retire_ready(const PcmXMasterTicketSlot *ticket)
{
	if (ticket == NULL || ticket->involved_nodes_bitmap == 0
		|| pcm_x_slot_state_read(&ticket->slot) != PCM_XT_RETIRE_CREDIT)
		return false;
	return ticket->drained_nodes_bitmap == ticket->involved_nodes_bitmap
		   && ticket->retire_acked_nodes_bitmap == ticket->involved_nodes_bitmap;
}


static bool
pcm_x_prehandle_equal(const PcmXPrehandleKey *left, const PcmXPrehandleKey *right)
{
	return left->sender_session_incarnation == right->sender_session_incarnation
		   && left->prehandle_sequence == right->prehandle_sequence;
}


static bool
pcm_x_ticket_handle_equal(const PcmXTicketHandle *left, const PcmXTicketHandle *right)
{
	return left->ticket_id == right->ticket_id && left->queue_generation == right->queue_generation;
}


static bool
pcm_x_ticket_locator_equal(const PcmXTicketRef *left, const PcmXTicketRef *right)
{
	return pcm_x_wait_identity_equal(&left->identity, &right->identity)
		   && pcm_x_ticket_handle_equal(&left->handle, &right->handle);
}


static bool
pcm_x_ticket_ref_equal(const PcmXTicketRef *left, const PcmXTicketRef *right)
{
	return pcm_x_ticket_locator_equal(left, right)
		   && left->grant_generation == right->grant_generation;
}


static bool
pcm_x_image_token_equal(const PcmXImageToken *left, const PcmXImageToken *right)
{
	return left != NULL && right != NULL && left->image_id == right->image_id
		   && left->source_own_generation == right->source_own_generation
		   && left->page_scn == right->page_scn && left->page_lsn == right->page_lsn
		   && left->source_node == right->source_node
		   && left->page_checksum == right->page_checksum;
}


static bool
pcm_x_image_token_valid(const PcmXImageToken *image)
{
	return image != NULL && image->source_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && cluster_pcm_x_image_id_decode(image->image_id, NULL, NULL);
}


static bool
pcm_x_image_id_master_exact(uint64 image_id, int32 expected_master_node)
{
	int32 encoded_master_node;

	return expected_master_node >= 0 && expected_master_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && cluster_pcm_x_image_id_decode(image_id, &encoded_master_node, NULL)
		   && encoded_master_node == expected_master_node;
}


/* Admission-phase wire references are immutable and always carry grant 0. */
static bool
pcm_x_ticket_admission_ref_equal(const PcmXTicketRef *current, const PcmXTicketRef *admission)
{
	return admission->grant_generation == 0 && pcm_x_ticket_locator_equal(current, admission);
}


static bool
pcm_x_wait_identity_valid(const PcmXWaitIdentity *identity)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	return header != NULL && identity != NULL && identity->node_id >= 0
		   && identity->node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && identity->procno < header->layout.process_capacity && identity->request_id != 0
		   && identity->wait_seq != 0;
}


static void
pcm_x_master_admission_clear(PcmXMasterAdmission *admission)
{
	memset(admission, 0, sizeof(*admission));
	admission->tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	admission->ticket_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


static void
pcm_x_local_handle_clear(PcmXLocalHandle *handle)
{
	if (handle == NULL)
		return;
	memset(handle, 0, sizeof(*handle));
	handle->tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	handle->membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


/* The exact commit proof runs against the effective grant base: the one-shot
 * pre-reservation rebase when an interleaved revoke consumed the enqueue-time
 * base on this node, the immutable identity base otherwise. */
static inline uint64
pcm_x_local_effective_grant_base(const PcmXLocalTagSlot *tag_slot)
{
	return tag_slot->grant_base_own_generation != 0 ? tag_slot->grant_base_own_generation
													: tag_slot->ref.identity.base_own_generation;
}


static void
pcm_x_local_follower_wfg_snapshot_clear(PcmXLocalFollowerWfgSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->waiter.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->waiter.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->blocker_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->leader_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->active_writer_index = PCM_X_INVALID_SLOT_INDEX;
}


static void
pcm_x_local_writer_claim_clear(PcmXLocalWriterClaim *claim)
{
	if (claim == NULL)
		return;
	memset(claim, 0, sizeof(*claim));
	claim->writer.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	claim->writer.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	claim->active_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


static void
pcm_x_master_admission_from_ticket(PcmXMasterAdmission *admission, PcmXSlotRef ticket_ref,
								   const PcmXMasterTicketSlot *ticket)
{
	pcm_x_master_admission_clear(admission);
	/* grant_generation is mutable under the master-domain lock.  Admission
	 * replay copies only the immutable locator while holding allocator_lock. */
	admission->ref.identity = ticket->ref.identity;
	admission->ref.handle = ticket->ref.handle;
	admission->ref.grant_generation = 0;
	admission->prehandle = ticket->prehandle;
	admission->tag_slot.slot_index = ticket->tag_slot_index;
	admission->tag_slot.slot_generation = ticket->tag_slot_generation;
	admission->ticket_slot = ticket_ref;
	admission->master_session_incarnation = ticket->master_session_incarnation;
	admission->admission_sequence = ticket->admission_sequence;
}


static void
pcm_x_local_handle_from_member(PcmXLocalHandle *handle, PcmXSlotRef membership_ref,
							   const PcmXLocalMembershipSlot *member, PcmXLocalRole role)
{
	pcm_x_local_handle_clear(handle);
	handle->identity = member->identity;
	handle->tag_slot.slot_index = member->tag_slot_index;
	handle->tag_slot.slot_generation = member->tag_slot_generation;
	handle->membership_slot = membership_ref;
	handle->local_sequence = member->local_sequence;
	handle->local_round = member->admitted_round;
	handle->role = (uint16)role;
}


static PcmXQueueResult
pcm_x_queue_result_from_allocator(PcmXAllocatorResult result)
{
	switch (result) {
	case PCM_X_ALLOC_OK:
		return PCM_X_QUEUE_OK;
	case PCM_X_ALLOC_NO_CAPACITY:
		return PCM_X_QUEUE_NO_CAPACITY;
	case PCM_X_ALLOC_STALE_REF:
		return PCM_X_QUEUE_STALE;
	case PCM_X_ALLOC_BAD_STATE:
		return PCM_X_QUEUE_BAD_STATE;
	case PCM_X_ALLOC_CORRUPT:
		return PCM_X_QUEUE_CORRUPT;
	case PCM_X_ALLOC_INVALID:
		return PCM_X_QUEUE_INVALID;
	}
	return PCM_X_QUEUE_CORRUPT;
}


static PcmXQueueResult
pcm_x_queue_result_from_directory(PcmXDirectoryResult result)
{
	switch (result) {
	case PCM_X_DIRECTORY_OK:
		return PCM_X_QUEUE_OK;
	case PCM_X_DIRECTORY_NOT_FOUND:
		return PCM_X_QUEUE_NOT_FOUND;
	case PCM_X_DIRECTORY_EXISTS:
		return PCM_X_QUEUE_DUPLICATE;
	case PCM_X_DIRECTORY_FULL:
		return PCM_X_QUEUE_NO_CAPACITY;
	case PCM_X_DIRECTORY_STALE_REF:
		/*
		 * Internal lookups hold the sole allocator lock.  An occupied entry
		 * cannot legally outlive or change its resident slot there; this is
		 * persistent directory corruption, not an old wire request.
		 */
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	case PCM_X_DIRECTORY_CORRUPT:
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	case PCM_X_DIRECTORY_INVALID:
		return PCM_X_QUEUE_INVALID;
	}
	return PCM_X_QUEUE_CORRUPT;
}


static PcmXSlotHeader *
pcm_x_domain_slot(PcmXAllocatorKind kind, PcmXSlotRef ref, const BufferTag *expected_tag,
				  uint32 allowed_state_mask)
{
	PcmXAllocatorView view;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXSlotHeader *slot;
	const BufferTag *slot_tag;
	BufferTag tag_snapshot;
	uint64 generation1;
	uint64 generation2;
	uint32 partition;
	uint32 state1;
	uint32 state2;

	if (header == NULL || expected_tag == NULL || allowed_state_mask == 0
		|| !pcm_x_allocator_view(kind, &view))
		return NULL;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(expected_tag));
	if (LWLockHeldByMe(&header->allocator_lock.lock)
		|| ((kind == PCM_X_ALLOC_MASTER_TAG || kind == PCM_X_ALLOC_MASTER_TICKET)
			&& !LWLockHeldByMe(&header->master_locks[partition].lock))
		|| (kind >= PCM_X_ALLOC_LOCAL_TAG
			&& !LWLockHeldByMe(&header->local_locks[partition].lock))) {
		pcm_x_runtime_fail_closed();
		return NULL;
	}
	slot = pcm_x_allocator_slot(&view, ref.slot_index);
	if (slot == NULL)
		return NULL;
	/* FREE/reuse can overlap this domain snapshot under the allocator lock. */
	if (!pcm_x_slot_generation_read(slot, &generation1))
		return NULL;
	if (generation1 != ref.slot_generation)
		return NULL;
	pg_read_barrier();
	state1 = pcm_x_slot_state_read(slot);
	if (state1 >= 32 || (allowed_state_mask & (UINT32_C(1) << state1)) == 0)
		return NULL;
	pg_read_barrier();
	slot_tag = pcm_x_slot_tag(kind, slot);
	if (slot_tag == NULL)
		return NULL;
	tag_snapshot = *slot_tag;
	pg_read_barrier();
	state2 = pcm_x_slot_state_read(slot);
	pg_read_barrier();
	if (!pcm_x_slot_generation_read(slot, &generation2))
		return NULL;
	if (state1 != state2 || generation1 != generation2
		|| !BufferTagsEqual(&tag_snapshot, expected_tag))
		return NULL;
	return slot;
}


static bool
pcm_x_master_stage_rollback_locked(bool new_tag, bool tag_directory_published,
								   bool prehandle_published, bool handle_published,
								   const BufferTag *tag, PcmXSlotRef tag_ref,
								   PcmXMasterTicketSlot *ticket, PcmXSlotRef ticket_ref)
{
	bool ok = true;

	if (handle_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &ticket->ref.handle,
											   ticket_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (prehandle_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
											   &ticket->prehandle, ticket_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (ticket != NULL
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
										  PCM_X_SLOT_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	if (tag_directory_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TAG, tag, tag_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (new_tag
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_MASTER_TAG, tag_ref,
										  PCM_X_SLOT_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	return ok;
}


PcmXQueueResult
cluster_pcm_x_peer_bind_ack_publish(int32 peer_node, uint64 cluster_epoch, uint64 peer_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXPeerFrontier *frontier;
	PcmXOutboundTargetFrontier *outbound;
	PcmXRuntimeSnapshot runtime;
	PcmXQueueResult result;
	uint32 expected_gate = 0;
	bool fail_closed = false;
	bool gate_claimed = false;

	if (header == NULL || peer_node < 0 || peer_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| peer_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto done;
	}
	frontier = &header->peer_frontiers[peer_node];
	outbound = &header->outbound_targets[peer_node];
	if (!pg_atomic_compare_exchange_u32(&outbound->mint_gate, &expected_gate, 1)) {
		result = PCM_X_QUEUE_BUSY;
		goto done;
	}
	gate_claimed = true;
	if ((outbound->flags & ~PCM_X_OUTBOUND_TARGET_KNOWN_FLAGS) != 0
		|| (outbound->flags & PCM_X_OUTBOUND_TARGET_INITIALIZED) == 0
		|| outbound->next_prehandle_sequence == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto done;
	}
	if (frontier->cluster_epoch == 0 && frontier->sender_session_incarnation == 0
		&& frontier->next_expected_prehandle_sequence == 1
		&& frontier->retired_prehandle_sequence == 0
		&& outbound->flags == PCM_X_OUTBOUND_TARGET_INITIALIZED && outbound->cluster_epoch == 0
		&& outbound->target_session_incarnation == 0 && outbound->next_prehandle_sequence == 1) {
		frontier->cluster_epoch = cluster_epoch;
		frontier->sender_session_incarnation = peer_session;
		outbound->cluster_epoch = cluster_epoch;
		outbound->target_session_incarnation = peer_session;
		outbound->flags |= PCM_X_OUTBOUND_TARGET_BOUND;
		result = PCM_X_QUEUE_OK;
	} else if (frontier->cluster_epoch == cluster_epoch
			   && frontier->sender_session_incarnation == peer_session
			   && frontier->next_expected_prehandle_sequence != 0
			   && frontier->retired_prehandle_sequence < frontier->next_expected_prehandle_sequence
			   && outbound->flags
					  == (PCM_X_OUTBOUND_TARGET_INITIALIZED | PCM_X_OUTBOUND_TARGET_BOUND)
			   && outbound->cluster_epoch == cluster_epoch
			   && outbound->target_session_incarnation == peer_session
			   && outbound->next_prehandle_sequence != 0) {
		result = PCM_X_QUEUE_DUPLICATE;
	} else {
		result = PCM_X_QUEUE_STALE;
		fail_closed = true;
	}

done:
	if (gate_claimed) {
		pg_write_barrier();
		pg_atomic_write_u32(&outbound->mint_gate, 0);
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_master_gate_release_exact(PcmXMasterTagSlot *tag_slot, uint32 claimed_value)
{
	uint32 expected = claimed_value;

	pg_write_barrier();
	return pg_atomic_compare_exchange_u32(&tag_slot->admission_gate, &expected, 0);
}


static bool
pcm_x_master_admission_gate_release(PcmXMasterTagSlot *tag_slot)
{
	return pcm_x_master_gate_release_exact(tag_slot, 1);
}


/*
 * Cross one allocator/domain lock-order window without stranding its gate.
 *
 * LWLockAcquire can ERROR before ownership (for example at the per-backend
 * held-lock limit).  Publication before this call cannot be rolled back
 * safely across lock domains, so first make the partial phase explicit in the
 * runtime recovery gate, then release only our exact short gate and rethrow.
 */
static void
pcm_x_master_gate_acquire_guarded(LWLock *lock, LWLockMode mode, PcmXMasterTagSlot *tag_slot,
								  uint32 claimed_value)
{
	PG_TRY();
	{
		LWLockAcquire(lock, mode);
	}
	PG_CATCH();
	{
		pcm_x_runtime_fail_closed();
		if (LWLockHeldByMe(lock))
			LWLockRelease(lock);
		if (claimed_value != 0 && tag_slot != NULL)
			(void)pcm_x_master_gate_release_exact(tag_slot, claimed_value);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static PcmXQueueResult
pcm_x_resident_tag_state_result(const PcmXSlotHeader *slot)
{
	uint32 state;

	if (slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	state = pcm_x_slot_state_read(slot);
	if (state == PCM_X_TAG_LIVE)
		return PCM_X_QUEUE_OK;
	if (state == PCM_X_TAG_RESERVED_NONVISIBLE || state == PCM_X_TAG_DETACHING)
		return PCM_X_QUEUE_BUSY;
	if (state == PCM_X_TAG_RECOVERY_BLOCKED)
		return PCM_X_QUEUE_NOT_READY;
	return PCM_X_QUEUE_CORRUPT;
}


static PcmXQueueResult
pcm_x_master_busy_for_node(PcmXSlotRef tag_ref, const BufferTag *tag, int32 node_id,
						   PcmXMasterAdmission *admission_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXMasterTagSlot *tag_slot;
	Size current;
	uint32 partition;
	uint32 admission_gate;
	uint32 queued_node_bitmap;
	Size visited = 0;
	bool scan_corrupt = false;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref, tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| !BufferTagsEqual(&tag_slot->tag, tag)) {
		LWLockRelease(&header->master_locks[partition].lock);
		return PCM_X_QUEUE_BUSY;
	}
	current = tag_slot->head_index;
	while (current != PCM_X_INVALID_SLOT_INDEX && visited < PCM_X_PROTOCOL_NODE_LIMIT) {
		PcmXAllocatorView view;
		PcmXMasterTicketSlot *ticket;
		PcmXSlotRef ticket_ref;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &view)) {
			scan_corrupt = true;
			break;
		}
		ticket = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&view, current);
		if (ticket == NULL) {
			scan_corrupt = true;
			break;
		}
		ticket_ref.slot_index = current;
		if (!pcm_x_slot_generation_read(&ticket->slot, &ticket_ref.slot_generation)) {
			scan_corrupt = true;
			break;
		}
		ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
														   tag, PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (ticket == NULL || ticket->tag_slot_index != tag_ref.slot_index
			|| ticket->tag_slot_generation != tag_ref.slot_generation) {
			scan_corrupt = true;
			break;
		}
		if (ticket->ref.identity.node_id == node_id) {
			pcm_x_master_admission_from_ticket(admission_out, ticket_ref, ticket);
			admission_out->flags = PCM_X_ADMIT_F_NODE_COALESCED;
			if (ticket->prev_index == PCM_X_INVALID_SLOT_INDEX)
				admission_out->flags |= PCM_X_ADMIT_F_QUEUE_HEAD;
			LWLockRelease(&header->master_locks[partition].lock);
			return PCM_X_QUEUE_BUSY;
		}
		current = ticket->next_index;
		visited++;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		scan_corrupt = true;
	queued_node_bitmap = pg_atomic_read_u32(&tag_slot->queued_node_bitmap);
	admission_gate = pg_atomic_read_u32(&tag_slot->admission_gate);
	LWLockRelease(&header->master_locks[partition].lock);
	/*
	 * A same-node bit observed under allocator_lock can legitimately disappear
	 * before this scan when the old ticket cancels.  Conversely, a replacement
	 * publisher can own admission_gate and republish the bit before its domain
	 * link becomes visible.  Only a stable bit with no publisher and no linked
	 * ticket is contradictory authority.
	 */
	if (!scan_corrupt
		&& ((queued_node_bitmap & (UINT32_C(1) << node_id)) == 0 || admission_gate != 0))
		return PCM_X_QUEUE_BUSY;
	pcm_x_runtime_fail_closed();
	return PCM_X_QUEUE_CORRUPT;
}


/* Atomically publish one master tag/ticket/directory admission, then link it. */
static PcmXQueueResult
pcm_x_master_admit_begin_impl(const PcmXEnqueuePayload *request, PcmXMasterAdmission *admission_out,
							  bool cancel_first)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXMasterTagSlot *tag_slot = NULL;
	PcmXMasterTicketSlot *ticket = NULL;
	PcmXSlotHeader *raw_slot;
	PcmXSlotRef tag_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef ticket_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef found;
	PcmXSlotRef existing;
	PcmXDirectoryResult directory_result;
	PcmXAllocatorResult allocator_result;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	uint64 ticket_id;
	uint64 admission_sequence;
	uint32 node_bit;
	uint32 partition;
	uint32 expected_gate;
	bool new_tag = false;
	bool tag_directory_published = false;
	bool prehandle_published = false;
	bool gate_claimed = false;
	bool fail_closed = false;

	if (admission_out != NULL)
		pcm_x_master_admission_clear(admission_out);
	if (header == NULL || request == NULL || admission_out == NULL
		|| !pcm_x_wait_identity_valid(&request->identity)
		|| request->prehandle.sender_session_incarnation == 0
		|| request->prehandle.prehandle_sequence == 0)
		return PCM_X_QUEUE_INVALID;
	if (request->prehandle.prehandle_sequence == UINT64_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	/*
	 * This queue primitive consumes an already authenticated sender context.
	 * The production wire adapter must complete BIND before invoking it.
	 */
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	node_bit = UINT32_C(1) << request->identity.node_id;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	frontier = &header->peer_frontiers[request->identity.node_id];
	if (frontier->cluster_epoch == 0 && frontier->sender_session_incarnation == 0
		&& frontier->next_expected_prehandle_sequence == 1
		&& frontier->retired_prehandle_sequence == 0) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	if (frontier->cluster_epoch != request->identity.cluster_epoch
		|| frontier->sender_session_incarnation != request->prehandle.sender_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto allocator_done;
	}
	if (frontier->next_expected_prehandle_sequence == 0
		|| frontier->retired_prehandle_sequence >= frontier->next_expected_prehandle_sequence) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto allocator_done;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
												   &request->prehandle, &found);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																	   found);
		if (ticket == NULL || !pcm_x_prehandle_equal(&ticket->prehandle, &request->prehandle)
			|| !pcm_x_wait_identity_equal(&ticket->ref.identity, &request->identity)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		pcm_x_master_admission_from_ticket(admission_out, found, ticket);
		admission_out->flags = PCM_X_ADMIT_F_EXACT_REPLAY;
		result = pcm_x_slot_state_read(&ticket->slot) == PCM_X_SLOT_RESERVED_NONVISIBLE
					 ? PCM_X_QUEUE_BUSY
					 : PCM_X_QUEUE_DUPLICATE;
		goto allocator_done;
	}
	if (directory_result == PCM_X_DIRECTORY_STALE_REF
		|| directory_result == PCM_X_DIRECTORY_CORRUPT) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto allocator_done;
	}
	if (directory_result != PCM_X_DIRECTORY_NOT_FOUND) {
		result = pcm_x_queue_result_from_directory(directory_result);
		goto allocator_done;
	}

	if (request->prehandle.prehandle_sequence <= frontier->retired_prehandle_sequence) {
		result = PCM_X_QUEUE_RETIRED;
		goto allocator_done;
	}
	if (request->prehandle.prehandle_sequence < frontier->next_expected_prehandle_sequence) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto allocator_done;
	}
	if (request->prehandle.prehandle_sequence > frontier->next_expected_prehandle_sequence) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	if (frontier->next_expected_prehandle_sequence == UINT64_MAX || header->next_ticket_id == 0
		|| header->next_ticket_id > PCM_X_MASTER_TICKET_ID_MAX) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		goto allocator_done;
	}

	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TAG, &request->identity.tag, &tag_ref);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		tag_slot
			= (PcmXMasterTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TAG, tag_ref);
		result = pcm_x_resident_tag_state_result(tag_slot == NULL ? NULL : &tag_slot->slot);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		if (!BufferTagsEqual(&tag_slot->tag, &request->identity.tag)
			|| tag_slot->cluster_epoch != request->identity.cluster_epoch) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		expected_gate = 0;
		if (!pg_atomic_compare_exchange_u32(&tag_slot->admission_gate, &expected_gate, 1)) {
			result = PCM_X_QUEUE_BUSY;
			goto allocator_done;
		}
		gate_claimed = true;
		pg_read_barrier();
		if ((pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & node_bit) != 0) {
			if (!pcm_x_master_admission_gate_release(tag_slot)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto allocator_done;
			}
			gate_claimed = false;
			result = PCM_X_QUEUE_BUSY;
			goto allocator_busy;
		}
		if (tag_slot->next_admission_sequence == 0
			|| tag_slot->next_admission_sequence == UINT64_MAX) {
			result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
			goto allocator_done;
		}
		admission_sequence = tag_slot->next_admission_sequence;
	} else if (directory_result == PCM_X_DIRECTORY_NOT_FOUND) {
		allocator_result
			= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_MASTER_TAG, &tag_ref, &raw_slot);
		if (allocator_result != PCM_X_ALLOC_OK) {
			result = pcm_x_queue_result_from_allocator(allocator_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		new_tag = true;
		tag_slot = (PcmXMasterTagSlot *)raw_slot;
		tag_slot->tag = request->identity.tag;
		tag_slot->cluster_epoch = request->identity.cluster_epoch;
		tag_slot->next_admission_sequence = 1;
		tag_slot->queue_state_sequence = 0;
		tag_slot->head_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->tail_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->active_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->outstanding_ticket_count = 0;
		pg_atomic_init_u32(&tag_slot->queued_node_bitmap, 0);
		pg_atomic_init_u32(&tag_slot->admission_gate, 1);
		gate_claimed = true;
		admission_sequence = 1;
	} else {
		result = pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto allocator_done;
	}

	ticket_id = header->next_ticket_id;
	allocator_result
		= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_MASTER_TICKET, &ticket_ref, &raw_slot);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		if (new_tag
			&& !pcm_x_master_stage_rollback_locked(
				true, false, false, false, &request->identity.tag, tag_ref, NULL, ticket_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	ticket = (PcmXMasterTicketSlot *)raw_slot;
	ticket->ref.identity = request->identity;
	ticket->ref.handle.ticket_id = ticket_id;
	ticket->ref.handle.queue_generation = ticket_ref.slot_generation;
	ticket->ref.grant_generation = 0;
	ticket->prehandle = request->prehandle;
	ticket->master_session_incarnation = runtime.master_session_incarnation;
	ticket->admission_sequence = admission_sequence;
	ticket->tag_slot_index = tag_ref.slot_index;
	ticket->tag_slot_generation = tag_ref.slot_generation;
	ticket->next_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->prev_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->blocker_head_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->blocker_stage_head_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->blocker_stage_source_session = 0;
	ticket->blocker_set_source_session = 0;
	ticket->blocker_stage_source_node = -1;
	ticket->blocker_set_source_node = -1;
	ticket->involved_nodes_bitmap = node_bit;
	ticket->grant_base_own_generation = 0;

	if (new_tag) {
		directory_result = pcm_x_directory_insert_locked(
			PCM_X_DIR_MASTER_TAG, &request->identity.tag, tag_ref, &existing);
		if (directory_result != PCM_X_DIRECTORY_OK) {
			result = directory_result == PCM_X_DIRECTORY_EXISTS
						 ? PCM_X_QUEUE_CORRUPT
						 : pcm_x_queue_result_from_directory(directory_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
			if (!pcm_x_master_stage_rollback_locked(true, false, false, false,
													&request->identity.tag, tag_ref, ticket,
													ticket_ref)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
			}
			goto allocator_done;
		}
		tag_directory_published = true;
	}
	directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
													 &ticket->prehandle, ticket_ref, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = directory_result == PCM_X_DIRECTORY_EXISTS
					 ? PCM_X_QUEUE_CORRUPT
					 : pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		if (!pcm_x_master_stage_rollback_locked(new_tag, tag_directory_published, false, false,
												&request->identity.tag, tag_ref, ticket,
												ticket_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	prehandle_published = true;
	directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_MASTER_TICKET_HANDLE,
													 &ticket->ref.handle, ticket_ref, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = directory_result == PCM_X_DIRECTORY_EXISTS
					 ? PCM_X_QUEUE_CORRUPT
					 : pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		if (!pcm_x_master_stage_rollback_locked(new_tag, tag_directory_published,
												prehandle_published, false, &request->identity.tag,
												tag_ref, ticket, ticket_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	if (!cancel_first)
		(void)pg_atomic_fetch_or_u32(&tag_slot->queued_node_bitmap, node_bit);
	tag_slot->next_admission_sequence = admission_sequence + 1;
	header->next_ticket_id = ticket_id + 1;
	frontier->next_expected_prehandle_sequence = request->prehandle.prehandle_sequence + 1;

allocator_done:
	if (result != PCM_X_QUEUE_OK && gate_claimed && !new_tag) {
		if (fail_closed) {
			/*
			 * A post-claim allocator invariant failed.  Normalize internal
			 * stale references to corruption and retain the gate as recovery
			 * evidence; all retryable outcomes take the release arm below.
			 */
			result = PCM_X_QUEUE_CORRUPT;
		} else {
			if (!pcm_x_master_admission_gate_release(tag_slot)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
			}
			gate_claimed = false;
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&request->identity.tag));
	pcm_x_master_gate_acquire_guarded(&header->master_locks[partition].lock, LW_EXCLUSIVE, tag_slot,
									  1);
	/*
	 * Allocator publication is now durable.  Every domain-phase invariant
	 * failure below deliberately leaves admission_gate claimed and moves the
	 * runtime to RECOVERY_BLOCKED; only the successful link releases it.
	 */
	{
		PcmXAllocatorView tag_view;
		PcmXMasterTagSlot *raw_tag;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TAG, &tag_view)) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		raw_tag = (PcmXMasterTagSlot *)pcm_x_allocator_slot(&tag_view, tag_ref.slot_index);
		if (raw_tag == NULL || pg_atomic_read_u32(&raw_tag->admission_gate) != 1) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		pg_read_barrier();
	}
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &request->identity.tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &request->identity.tag,
													   PCM_X_STATE_BIT(PCM_XT_RESERVED_NONVISIBLE));
	if (tag_slot == NULL || ticket == NULL
		|| pcm_x_slot_state_read(&ticket->slot) != PCM_XT_RESERVED_NONVISIBLE
		|| (new_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_RESERVED_NONVISIBLE)
		|| (!new_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE)
		|| !BufferTagsEqual(&tag_slot->tag, &request->identity.tag)
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation
		|| pg_atomic_read_u32(&tag_slot->admission_gate) != 1) {
		LWLockRelease(&header->master_locks[partition].lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	pg_read_barrier();
	if ((!cancel_first && (pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & node_bit) == 0)
		|| pg_atomic_read_u32(&tag_slot->admission_gate) != 1) {
		LWLockRelease(&header->master_locks[partition].lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (cancel_first) {
		/* PREHANDLE_CANCEL may overtake ENQUEUE.  Publish the formal terminal
		 * while admission_gate still excludes every competing admission: no
		 * observer can ever see this ticket as ADMITTING or hot-linked. */
		if (tag_slot->outstanding_ticket_count == SIZE_MAX) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_COUNTER_EXHAUSTED;
		}
		tag_slot->outstanding_ticket_count++;
		ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_CANCEL;
		pg_write_barrier();
		pcm_x_slot_state_write(&ticket->slot, PCM_XT_CANCELLED);
		if (new_tag) {
			pg_write_barrier();
			pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
		}
		pg_write_barrier();
		if (!pcm_x_master_admission_gate_release(tag_slot)) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		pcm_x_master_admission_from_ticket(admission_out, ticket_ref, ticket);
		pcm_x_stats_increment(&header->stats.admit_count);
		pcm_x_stats_increment(&header->stats.cancel_count);
		LWLockRelease(&header->master_locks[partition].lock);
		return PCM_X_QUEUE_OK;
	}
	if (tag_slot->queue_state_sequence == UINT64_MAX
		|| tag_slot->outstanding_ticket_count == SIZE_MAX) {
		/* Publication cannot be rolled back across lock domains; fence recovery. */
		LWLockRelease(&header->master_locks[partition].lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	}
	if (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		tag_slot->head_index = ticket_ref.slot_index;
	} else {
		PcmXAllocatorView ticket_view;
		PcmXMasterTicketSlot *tail;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &ticket_view)) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		tail = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, tag_slot->tail_index);
		if (tail == NULL || tail->next_index != PCM_X_INVALID_SLOT_INDEX
			|| tail->tag_slot_index != tag_ref.slot_index
			|| tail->tag_slot_generation != tag_ref.slot_generation) {
			LWLockRelease(&header->master_locks[partition].lock);
			pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		ticket->prev_index = tag_slot->tail_index;
		tail->next_index = ticket_ref.slot_index;
	}
	tag_slot->tail_index = ticket_ref.slot_index;
	tag_slot->queue_state_sequence++;
	tag_slot->outstanding_ticket_count++;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_ADMITTING);
	if (new_tag) {
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
	}
	pg_write_barrier();
	if (!pcm_x_master_admission_gate_release(tag_slot)) {
		LWLockRelease(&header->master_locks[partition].lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	pcm_x_master_admission_from_ticket(admission_out, ticket_ref, ticket);
	if (ticket->prev_index == PCM_X_INVALID_SLOT_INDEX)
		admission_out->flags |= PCM_X_ADMIT_F_QUEUE_HEAD;
	pcm_x_stats_increment(&header->stats.admit_count);
	LWLockRelease(&header->master_locks[partition].lock);
	return PCM_X_QUEUE_OK;

allocator_busy:
	LWLockRelease(&header->allocator_lock.lock);
	return pcm_x_master_busy_for_node(tag_ref, &request->identity.tag, request->identity.node_id,
									  admission_out);
}


PcmXQueueResult
cluster_pcm_x_master_admit_begin(const PcmXEnqueuePayload *request,
								 PcmXMasterAdmission *admission_out)
{
	return pcm_x_master_admit_begin_impl(request, admission_out, false);
}


static PcmXQueueResult
pcm_x_master_ticket_lookup_locked(const PcmXTicketRef *ref, PcmXSlotRef *ticket_ref_out,
								  PcmXMasterTicketSlot **ticket_out)
{
	PcmXDirectoryResult directory_result;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;

	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle, &ticket_ref);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																   ticket_ref);
	if (ticket == NULL) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	/* grant_generation is published under the master-domain lock.  Locator
	 * lookup under allocator_lock uses immutable identity+handle only; every
	 * caller rechecks the full ref after acquiring the domain lock. */
	if (!pcm_x_ticket_locator_equal(&ticket->ref, ref))
		return PCM_X_QUEUE_STALE;
	*ticket_ref_out = ticket_ref;
	*ticket_out = ticket;
	return PCM_X_QUEUE_OK;
}


/* Locate only admission-phase messages; later phases keep full grant exactness. */
static PcmXQueueResult
pcm_x_master_admission_ticket_lookup_locked(const PcmXTicketRef *ref, PcmXSlotRef *ticket_ref_out,
											PcmXMasterTicketSlot **ticket_out)
{
	PcmXDirectoryResult directory_result;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;

	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle, &ticket_ref);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																   ticket_ref);
	if (ticket == NULL) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (!pcm_x_ticket_admission_ref_equal(&ticket->ref, ref))
		return PCM_X_QUEUE_STALE;
	*ticket_ref_out = ticket_ref;
	*ticket_out = ticket;
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
pcm_x_master_tag_lookup_locked(const BufferTag *tag, PcmXSlotRef *tag_ref_out,
							   PcmXMasterTagSlot **tag_out)
{
	PcmXDirectoryResult directory_result;
	PcmXMasterTagSlot *tag_slot;
	PcmXSlotRef tag_ref;

	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TAG, tag, &tag_ref);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TAG, tag_ref);
	if (tag_slot == NULL) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (!BufferTagsEqual(&tag_slot->tag, tag))
		return PCM_X_QUEUE_STALE;
	*tag_ref_out = tag_ref;
	*tag_out = tag_slot;
	return PCM_X_QUEUE_OK;
}


static void
pcm_x_master_wfg_snapshot_clear(PcmXMasterWfgSnapshot *snapshot)
{
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->token.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->token.ticket_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->token.predecessor_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	snapshot->token.active_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


static bool
pcm_x_slot_ref_equal(PcmXSlotRef left, PcmXSlotRef right)
{
	return left.slot_index == right.slot_index && left.slot_generation == right.slot_generation;
}


static bool
pcm_x_lmd_vertex_identity_equal(const ClusterLmdVertex *left, const ClusterLmdVertex *right)
{
	return left->node_id == right->node_id && left->procno == right->procno
		   && left->cluster_epoch == right->cluster_epoch && left->request_id == right->request_id;
}


static void
pcm_x_master_ticket_vertex(const PcmXMasterTicketSlot *ticket, ClusterLmdVertex *vertex)
{
	memset(vertex, 0, sizeof(*vertex));
	vertex->node_id = ticket->ref.identity.node_id;
	vertex->procno = ticket->ref.identity.procno;
	vertex->cluster_epoch = ticket->ref.identity.cluster_epoch;
	vertex->request_id = ticket->ref.identity.request_id;
	vertex->xid = ticket->ref.identity.xid;
	vertex->wait_seq = ticket->ref.identity.wait_seq;
}


static bool
pcm_x_master_wfg_token_equal(const PcmXMasterWfgToken *left, const PcmXMasterWfgToken *right)
{
	return pcm_x_ticket_ref_equal(&left->ticket, &right->ticket)
		   && pcm_x_slot_ref_equal(left->tag_slot, right->tag_slot)
		   && pcm_x_slot_ref_equal(left->ticket_slot, right->ticket_slot)
		   && pcm_x_slot_ref_equal(left->predecessor_slot, right->predecessor_slot)
		   && pcm_x_slot_ref_equal(left->active_slot, right->active_slot)
		   && left->queue_state_sequence == right->queue_state_sequence;
}


/*
 * Capture only master-queue blockers while the caller holds the tag partition
 * lock.  This function does not call LMD: the graph replace happens after the
 * public snapshot API has released every PCM queue lock.
 */
static PcmXQueueResult
pcm_x_master_wfg_capture_locked(PcmXRuntimeSnapshot runtime, const PcmXTicketRef *ref,
								PcmXSlotRef tag_ref, PcmXSlotRef ticket_ref,
								PcmXMasterWfgSnapshot *snapshot)
{
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *active = NULL;
	PcmXMasterTicketSlot *predecessor = NULL;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView ticket_view;
	PcmXSlotRef active_ref;
	PcmXSlotRef predecessor_ref;
	uint32 active_state;
	uint32 state;

	pcm_x_master_wfg_snapshot_clear(snapshot);
	active_ref.slot_index = PCM_X_INVALID_SLOT_INDEX;
	active_ref.slot_generation = 0;
	predecessor_ref.slot_index = PCM_X_INVALID_SLOT_INDEX;
	predecessor_ref.slot_generation = 0;
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref)
		|| pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| tag_slot->cluster_epoch != ref->identity.cluster_epoch
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation)
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		return PCM_X_QUEUE_NOT_READY;
	state = pcm_x_slot_state_read(&ticket->slot);
	if (state != PCM_XT_ADMITTING) {
		if (ticket->graph_generation != 0
			&& pcm_x_master_event_replay_exact((PcmXMasterTicketState)state,
											   PCM_X_EVENT_ADMIT_CONFIRM,
											   ticket->reliable.response_tombstone_mask))
			return PCM_X_QUEUE_DUPLICATE;
		return PCM_X_QUEUE_BAD_STATE;
	}
	if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &ticket_view))
		return PCM_X_QUEUE_CORRUPT;

	if (ticket->prev_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->head_index != ticket_ref.slot_index)
			return PCM_X_QUEUE_CORRUPT;
	} else {
		if (ticket->prev_index == ticket_ref.slot_index
			|| tag_slot->head_index == ticket_ref.slot_index)
			return PCM_X_QUEUE_CORRUPT;
		predecessor
			= (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, ticket->prev_index);
		if (predecessor == NULL
			|| !pcm_x_slot_generation_read(&predecessor->slot, &predecessor_ref.slot_generation))
			return PCM_X_QUEUE_CORRUPT;
		predecessor_ref.slot_index = ticket->prev_index;
		predecessor = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET,
																predecessor_ref, &ref->identity.tag,
																PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (predecessor == NULL || predecessor->next_index != ticket_ref.slot_index
			|| predecessor->tag_slot_index != tag_ref.slot_index
			|| predecessor->tag_slot_generation != tag_ref.slot_generation
			|| (pcm_x_slot_state_read(&predecessor->slot) != PCM_XT_ADMITTING
				&& pcm_x_slot_state_read(&predecessor->slot) != PCM_XT_QUEUED
				&& pcm_x_slot_state_read(&predecessor->slot) != PCM_XT_ACTIVE_PROBE
				&& pcm_x_slot_state_read(&predecessor->slot) != PCM_XT_ACTIVE_TRANSFER))
			return PCM_X_QUEUE_CORRUPT;
	}

	if (tag_slot->active_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->active_slot_generation != 0)
			return PCM_X_QUEUE_CORRUPT;
	} else {
		if (tag_slot->active_index == ticket_ref.slot_index
			|| tag_slot->active_index != tag_slot->head_index)
			return PCM_X_QUEUE_CORRUPT;
		active_ref.slot_index = tag_slot->active_index;
		active_ref.slot_generation = tag_slot->active_slot_generation;
		active = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, active_ref,
														   &ref->identity.tag,
														   PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (active == NULL || active->tag_slot_index != tag_ref.slot_index
			|| active->tag_slot_generation != tag_ref.slot_generation)
			return PCM_X_QUEUE_CORRUPT;
		active_state = pcm_x_slot_state_read(&active->slot);
		if (active_state != PCM_XT_ACTIVE_PROBE && active_state != PCM_XT_ACTIVE_TRANSFER)
			return PCM_X_QUEUE_CORRUPT;
	}

	snapshot->token.ticket = ticket->ref;
	snapshot->token.tag_slot = tag_ref;
	snapshot->token.ticket_slot = ticket_ref;
	snapshot->token.predecessor_slot = predecessor_ref;
	snapshot->token.active_slot = active_ref;
	snapshot->token.queue_state_sequence = tag_slot->queue_state_sequence;
	if (active != NULL)
		pcm_x_master_ticket_vertex(active, &snapshot->blockers[snapshot->blocker_count++]);
	if (predecessor != NULL) {
		ClusterLmdVertex predecessor_vertex;

		pcm_x_master_ticket_vertex(predecessor, &predecessor_vertex);
		if (snapshot->blocker_count == 0
			|| !pcm_x_lmd_vertex_identity_equal(&snapshot->blockers[0], &predecessor_vertex))
			snapshot->blockers[snapshot->blocker_count++] = predecessor_vertex;
	}
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_master_admit_wfg_snapshot_exact(const PcmXTicketRef *ref,
											  PcmXMasterWfgSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef requested_ref;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (ref != NULL)
		requested_ref = *ref;
	if (snapshot_out != NULL)
		pcm_x_master_wfg_snapshot_clear(snapshot_out);
	if (header == NULL || ref == NULL || snapshot_out == NULL || requested_ref.grant_generation != 0
		|| !pcm_x_wait_identity_valid(&requested_ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_admission_ticket_lookup_locked(&requested_ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&requested_ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	result = pcm_x_master_wfg_capture_locked(runtime, &requested_ref, tag_ref, ticket_ref,
											 snapshot_out);
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_master_admit_confirm_transition_locked(PcmXShmemHeader *header, PcmXMasterTicketSlot *ticket,
											 uint64 graph_generation)
{
	if (!pcm_x_stats_depth_increment(header))
		return PCM_X_QUEUE_CORRUPT;
	ticket->graph_generation = graph_generation;
	ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_QUEUED);
	pcm_x_stats_increment(&header->stats.confirm_count);
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_master_admit_confirm_revalidate_exact(const PcmXMasterWfgToken *token,
													uint64 graph_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXMasterWfgSnapshot current;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;
	uint32 state;

	if (header == NULL || token == NULL || graph_generation == 0
		|| token->ticket.grant_generation != 0
		|| !pcm_x_wait_identity_valid(&token->ticket.identity)
		|| token->tag_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| token->ticket_slot.slot_index == PCM_X_INVALID_SLOT_INDEX)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_admission_ticket_lookup_locked(&token->ticket, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
		if (!pcm_x_slot_ref_equal(ticket_ref, token->ticket_slot)
			|| !pcm_x_slot_ref_equal(tag_ref, token->tag_slot))
			result = PCM_X_QUEUE_STALE;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&token->ticket.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &token->ticket.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, &token->ticket)) {
		result = PCM_X_QUEUE_STALE;
		goto done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	if (state != PCM_XT_ADMITTING) {
		if (pcm_x_master_event_replay_exact((PcmXMasterTicketState)state, PCM_X_EVENT_ADMIT_CONFIRM,
											ticket->reliable.response_tombstone_mask)
			&& ticket->graph_generation == graph_generation)
			result = PCM_X_QUEUE_DUPLICATE;
		else
			result = PCM_X_QUEUE_BAD_STATE;
		goto done;
	}
	result
		= pcm_x_master_wfg_capture_locked(runtime, &token->ticket, tag_ref, ticket_ref, &current);
	if (result == PCM_X_QUEUE_OK && !pcm_x_master_wfg_token_equal(&current.token, token))
		result = PCM_X_QUEUE_STALE;
	if (result == PCM_X_QUEUE_OK)
		result = pcm_x_master_admit_confirm_transition_locked(header, ticket, graph_generation);

done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_admit_confirm_exact(const PcmXTicketRef *ref, uint64 graph_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || ref == NULL || graph_generation == 0 || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_admission_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) == PCM_XT_ADMITTING)
		result = pcm_x_master_admit_confirm_transition_locked(header, ticket, graph_generation);
	else if (pcm_x_master_event_replay_exact(
				 (PcmXMasterTicketState)pcm_x_slot_state_read(&ticket->slot),
				 PCM_X_EVENT_ADMIT_CONFIRM, ticket->reliable.response_tombstone_mask)
			 && ticket->graph_generation == graph_generation)
		result = PCM_X_QUEUE_DUPLICATE;
	else
		result = PCM_X_QUEUE_BAD_STATE;
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_promote_head_exact(const BufferTag *tag, uint64 cluster_epoch,
										PcmXTicketRef *active_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView ticket_view;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;
	uint32 state;
	Size ticket_index;

	if (active_out != NULL)
		memset(active_out, 0, sizeof(*active_out));
	if (header == NULL || tag == NULL || active_out == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_tag_lookup_locked(tag, &tag_ref, &tag_slot);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref, tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| !BufferTagsEqual(&tag_slot->tag, tag) || tag_slot->cluster_epoch != cluster_epoch) {
		result = PCM_X_QUEUE_STALE;
		goto done;
	}
	if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &ticket_view)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto done;
	}
	if (tag_slot->active_index != PCM_X_INVALID_SLOT_INDEX) {
		ticket = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, tag_slot->active_index);
		if (ticket == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			goto done;
		}
		ticket_ref.slot_index = tag_slot->active_index;
		ticket_ref.slot_generation = tag_slot->active_slot_generation;
		ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
														   tag, PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (ticket == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			goto done;
		}
		state = pcm_x_slot_state_read(&ticket->slot);
		if (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED) {
			result = PCM_X_QUEUE_NOT_READY;
			goto done;
		}
		if (state != PCM_XT_ACTIVE_PROBE && state != PCM_XT_ACTIVE_TRANSFER) {
			result = PCM_X_QUEUE_CORRUPT;
			goto done;
		}
		*active_out = ticket->ref;
		result = PCM_X_QUEUE_BUSY;
		goto done;
	}
	ticket_index = tag_slot->head_index;
	if (ticket_index == PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_NOT_FOUND;
		goto done;
	}
	ticket = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, ticket_index);
	if (ticket == NULL) {
		result = PCM_X_QUEUE_CORRUPT;
		goto done;
	}
	ticket_ref.slot_index = ticket_index;
	if (!pcm_x_slot_generation_read(&ticket->slot, &ticket_ref.slot_generation)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto done;
	}
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref, tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation) {
		result = PCM_X_QUEUE_CORRUPT;
		goto done;
	}
	if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_QUEUED) {
		result = PCM_X_QUEUE_NOT_READY;
		goto done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto done;
	}
	if (!pcm_x_stats_depth_decrement(header)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto done;
	}
	tag_slot->active_index = ticket_index;
	tag_slot->active_slot_generation = ticket_ref.slot_generation;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_ACTIVE_PROBE);
	*active_out = ticket->ref;
	pcm_x_stats_increment(&header->stats.promotion_count);
	result = PCM_X_QUEUE_OK;

done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_master_pending_x_flags_valid(uint32 flags)
{
	const uint32 terminal = PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING
							| PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED;

	if ((flags & ~PCM_X_MASTER_TICKET_F_PENDING_X_KNOWN) != 0)
		return false;
	if ((flags & terminal) != 0 && (flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) == 0)
		return false;
	return (flags & terminal) != terminal;
}


static PcmXQueueResult
pcm_x_master_pending_x_claim_exact_impl(const PcmXTicketRef *ref, bool claim, bool *claimed_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 flags;
	uint32 partition;
	uint32 state;

	if (claimed_out != NULL)
		*claimed_out = false;
	if (header == NULL || ref == NULL || (claim && claimed_out != NULL)
		|| (!claim && claimed_out == NULL) || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_admission_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else {
		state = pcm_x_slot_state_read(&ticket->slot);
		if (state != PCM_XT_ACTIVE_PROBE) {
			if (!claim
				&& (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED
					|| state == PCM_XT_RETIRE_CREDIT)) {
				if (ticket->next_index != PCM_X_INVALID_SLOT_INDEX
					|| ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
					|| tag_slot->active_index == ticket_ref.slot_index
					|| tag_slot->head_index == ticket_ref.slot_index
					|| tag_slot->tail_index == ticket_ref.slot_index)
					result = PCM_X_QUEUE_CORRUPT;
				else
					result = PCM_X_QUEUE_NOT_READY;
			} else
				result = PCM_X_QUEUE_BAD_STATE;
		} else if (tag_slot->active_index != ticket_ref.slot_index
				   || tag_slot->active_slot_generation != ticket_ref.slot_generation)
			result = PCM_X_QUEUE_BAD_STATE;
		else if (!pcm_x_master_pending_x_flags_valid(
					 (flags = pcm_x_slot_flags_read(&ticket->slot))))
			result = PCM_X_QUEUE_CORRUPT;
		else if (!claim) {
			if ((flags
				 & (PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING
					| PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED))
				!= 0)
				result = PCM_X_QUEUE_NOT_READY;
			else {
				*claimed_out = (flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0;
				result = PCM_X_QUEUE_OK;
			}
		} else if ((flags
					& (PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING
					   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED))
				   != 0)
			result = PCM_X_QUEUE_NOT_READY;
		else if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0)
			result = PCM_X_QUEUE_DUPLICATE;
		else {
			pcm_x_slot_flags_write(&ticket->slot, PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED);
			result = PCM_X_QUEUE_OK;
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_pending_x_claim_state_exact(const PcmXTicketRef *ref, bool *claimed_out)
{
	return pcm_x_master_pending_x_claim_exact_impl(ref, false, claimed_out);
}


PcmXQueueResult
cluster_pcm_x_master_pending_x_claim_exact(const PcmXTicketRef *ref)
{
	return pcm_x_master_pending_x_claim_exact_impl(ref, true, NULL);
}


static PcmXQueueResult
pcm_x_master_pending_x_claim_required(const PcmXMasterTicketSlot *ticket)
{
	uint32 flags;

	if (ticket == NULL)
		return PCM_X_QUEUE_CORRUPT;
	flags = pcm_x_slot_flags_read(&ticket->slot);
	if (!pcm_x_master_pending_x_flags_valid(flags))
		return PCM_X_QUEUE_CORRUPT;
	if ((flags
		 & (PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING
			| PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED))
		!= 0)
		return PCM_X_QUEUE_NOT_READY;
	return flags == PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED ? PCM_X_QUEUE_OK
															: PCM_X_QUEUE_BAD_STATE;
}


static PcmXQueueResult
pcm_x_master_drive_candidate_exact(PcmXShmemHeader *header, PcmXSlotRef tag_ref,
								   const BufferTag *tag, BufferTag *tag_out,
								   uint64 *cluster_epoch_out)
{
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	uint32 partition;
	uint32 ticket_state;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref, tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || tag_slot->active_index == PCM_X_INVALID_SLOT_INDEX) {
		LWLockRelease(&header->master_locks[partition].lock);
		return PCM_X_QUEUE_NOT_FOUND;
	}
	ticket_ref.slot_index = tag_slot->active_index;
	ticket_ref.slot_generation = tag_slot->active_slot_generation;
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref, tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL) {
		LWLockRelease(&header->master_locks[partition].lock);
		return PCM_X_QUEUE_CORRUPT;
	}
	ticket_state = pcm_x_slot_state_read(&ticket->slot);
	if (ticket_state != PCM_XT_ACTIVE_PROBE && ticket_state != PCM_XT_ACTIVE_TRANSFER) {
		LWLockRelease(&header->master_locks[partition].lock);
		return ticket_state == PCM_XT_COMPLETE || ticket_state == PCM_XT_CANCELLED
				   ? PCM_X_QUEUE_NOT_FOUND
				   : PCM_X_QUEUE_CORRUPT;
	}
	*tag_out = tag_slot->tag;
	*cluster_epoch_out = tag_slot->cluster_epoch;
	LWLockRelease(&header->master_locks[partition].lock);
	return PCM_X_QUEUE_OK;
}


/* Inspect at most scan_budget master-tag slots from a caller-owned cursor.
 * Formation retry therefore makes bounded progress without adding a shared
 * cursor to the fixed shmem header. */
PcmXQueueResult
cluster_pcm_x_master_drive_work_next(Size *cursor_io, Size scan_budget, BufferTag *tag_out,
									 uint64 *cluster_epoch_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXAllocatorView view;
	PcmXMasterTagSlot *raw;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	BufferTag tag;
	Size budget;
	Size i;
	Size index;
	Size start;
	uint64 generation_after;

	if (tag_out != NULL)
		memset(tag_out, 0, sizeof(*tag_out));
	if (cluster_epoch_out != NULL)
		*cluster_epoch_out = 0;
	if (header == NULL || cursor_io == NULL || scan_budget == 0 || tag_out == NULL
		|| cluster_epoch_out == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (!pcm_x_allocator_entry_unlocked(header)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TAG, &view) || view.capacity == 0)
		return PCM_X_QUEUE_INVALID;

	start = *cursor_io % view.capacity;
	budget = Min(scan_budget, view.capacity);
	for (i = 0; i < budget; i++) {
		index = (start + i) % view.capacity;
		*cursor_io = (index + 1) % view.capacity;
		raw = (PcmXMasterTagSlot *)pcm_x_allocator_slot(&view, index);
		if (raw == NULL || pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE
			|| !pcm_x_slot_generation_read(&raw->slot, &tag_ref.slot_generation))
			continue;
		tag_ref.slot_index = index;
		tag = raw->tag;
		pg_read_barrier();
		if (!pcm_x_slot_generation_read(&raw->slot, &generation_after)
			|| generation_after != tag_ref.slot_generation
			|| pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE)
			continue;

		/* Formation retries only work that has crossed the ADMIT_CONFIRM_ACK
		 * outbound admission boundary.  A queued-only head is first promoted by
		 * the event path after that ACK is durably staged; otherwise a full
		 * outbound ring could let type 49 overtake an unstaged type 44. */
		result
			= pcm_x_master_drive_candidate_exact(header, tag_ref, &tag, tag_out, cluster_epoch_out);
		if (result == PCM_X_QUEUE_CORRUPT) {
			pcm_x_runtime_fail_closed();
			return result;
		}
		if (result == PCM_X_QUEUE_OK) {
			if (!pcm_x_runtime_token_exact(&runtime, 0)) {
				memset(tag_out, 0, sizeof(*tag_out));
				*cluster_epoch_out = 0;
				return PCM_X_QUEUE_NOT_READY;
			}
			return PCM_X_QUEUE_OK;
		}
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0))
		return PCM_X_QUEUE_NOT_READY;
	return PCM_X_QUEUE_NOT_FOUND;
}


static void
pcm_x_master_drive_snapshot_clear(PcmXMasterDriveSnapshot *snapshot)
{
	if (snapshot != NULL)
		memset(snapshot, 0, sizeof(*snapshot));
}


static bool
pcm_x_master_drive_leg_valid(const PcmXReliableLegState *leg)
{
	if (leg == NULL || leg->flags != 0 || leg->reserved != 0)
		return false;
	if (leg->pending_opcode == 0)
		return leg->retry_deadline_ms == 0 && leg->expected_responder_session == 0
			   && leg->retry_count == 0 && leg->expected_responder_node == 0 && leg->phase == 0;
	return leg->phase == leg->pending_opcode && leg->expected_responder_session != 0
		   && leg->expected_responder_node >= 0
		   && leg->expected_responder_node < PCM_X_PROTOCOL_NODE_LIMIT;
}


static PcmXQueueResult
pcm_x_master_drive_capture_locked(PcmXRuntimeSnapshot runtime, PcmXMasterTagSlot *tag_slot,
								  PcmXSlotRef tag_ref, PcmXMasterTicketSlot *ticket,
								  PcmXSlotRef ticket_ref, PcmXMasterDriveSnapshot *snapshot)
{
	uint32 state;
	uint32 ticket_flags;

	pcm_x_master_drive_snapshot_clear(snapshot);
	if (tag_slot == NULL || ticket == NULL || snapshot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	state = pcm_x_slot_state_read(&ticket->slot);
	if (pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| !BufferTagsEqual(&tag_slot->tag, &ticket->ref.identity.tag)
		|| tag_slot->cluster_epoch != ticket->ref.identity.cluster_epoch
		|| tag_slot->active_index != ticket_ref.slot_index
		|| tag_slot->active_slot_generation != ticket_ref.slot_generation
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation)
		return PCM_X_QUEUE_CORRUPT;
	/* COMPLETE/CANCELLED legitimately remain the tag's active locator until
	 * exact drain/retire detaches them.  They are not master-drive work and
	 * must not turn an unrelated retry/ACK lookup into RECOVERY_BLOCKED. */
	if (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED)
		return PCM_X_QUEUE_NOT_READY;
	if (state != PCM_XT_ACTIVE_PROBE && state != PCM_XT_ACTIVE_TRANSFER)
		return PCM_X_QUEUE_CORRUPT;
	if ((state == PCM_XT_ACTIVE_PROBE && ticket->ref.grant_generation != 0)
		|| (state == PCM_XT_ACTIVE_TRANSFER && ticket->ref.grant_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		return PCM_X_QUEUE_NOT_READY;
	ticket_flags = pcm_x_slot_flags_read(&ticket->slot);
	if (!pcm_x_master_pending_x_flags_valid(ticket_flags))
		return PCM_X_QUEUE_CORRUPT;
	if ((state == PCM_XT_ACTIVE_TRANSFER && ticket_flags != PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED)
		|| (state == PCM_XT_ACTIVE_PROBE
			&& (ticket_flags & PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING) != 0))
		return PCM_X_QUEUE_CORRUPT;
	if ((ticket->acked_s_holders_bitmap & ~ticket->pending_s_holders_bitmap) != 0
		|| !pcm_x_master_drive_leg_valid(&ticket->reliable)
		|| !pcm_x_master_blocker_stage_metadata_valid(ticket))
		return PCM_X_QUEUE_CORRUPT;

	snapshot->ref = ticket->ref;
	snapshot->image = ticket->image;
	snapshot->master_session_incarnation = ticket->master_session_incarnation;
	snapshot->blocker_set_generation = ticket->blocker_set_generation;
	snapshot->blocker_stage_set_generation = ticket->blocker_stage_set_generation;
	snapshot->graph_generation = ticket->graph_generation;
	snapshot->state_sequence = ticket->reliable.state_sequence;
	snapshot->retry_deadline_ms = ticket->reliable.retry_deadline_ms;
	snapshot->response_tombstone_mask = ticket->reliable.response_tombstone_mask;
	snapshot->expected_responder_session = ticket->reliable.expected_responder_session;
	snapshot->pending_s_holders_bitmap = ticket->pending_s_holders_bitmap;
	snapshot->acked_s_holders_bitmap = ticket->acked_s_holders_bitmap;
	snapshot->involved_nodes_bitmap = ticket->involved_nodes_bitmap;
	snapshot->blocker_count = ticket->blocker_count;
	snapshot->retry_count = ticket->reliable.retry_count;
	snapshot->last_responder_node = ticket->reliable.last_responder_node;
	snapshot->expected_responder_node = ticket->reliable.expected_responder_node;
	snapshot->ticket_state = (uint16)state;
	snapshot->pending_opcode = ticket->reliable.pending_opcode;
	snapshot->last_response_opcode = ticket->reliable.last_response_opcode;
	snapshot->phase = ticket->reliable.phase;
	/* PcmXReliableLegState flags/reserved are validated zero above.  Reuse the
	 * process-local tail for master-ticket control flags so bitmap revalidate
	 * and the tag-sharded driver both observe CANCEL_REQUESTED exactly. */
	snapshot->flags = (uint16)ticket_flags;
	snapshot->reserved = ticket->reliable.reserved;
	return PCM_X_QUEUE_OK;
}


static bool
pcm_x_master_drive_snapshot_base_equal(const PcmXMasterDriveSnapshot *left,
									   const PcmXMasterDriveSnapshot *right)
{
	return left != NULL && right != NULL && pcm_x_ticket_ref_equal(&left->ref, &right->ref)
		   && pcm_x_image_token_equal(&left->image, &right->image)
		   && left->master_session_incarnation == right->master_session_incarnation
		   && left->blocker_set_generation == right->blocker_set_generation
		   && left->blocker_stage_set_generation == right->blocker_stage_set_generation
		   && left->graph_generation == right->graph_generation
		   && left->state_sequence == right->state_sequence
		   && left->retry_deadline_ms == right->retry_deadline_ms
		   && left->response_tombstone_mask == right->response_tombstone_mask
		   && left->expected_responder_session == right->expected_responder_session
		   && left->blocker_count == right->blocker_count && left->retry_count == right->retry_count
		   && left->last_responder_node == right->last_responder_node
		   && left->expected_responder_node == right->expected_responder_node
		   && left->ticket_state == right->ticket_state
		   && left->pending_opcode == right->pending_opcode
		   && left->last_response_opcode == right->last_response_opcode
		   && left->phase == right->phase && left->flags == right->flags
		   && left->reserved == right->reserved;
}


static bool
pcm_x_master_drive_snapshot_equal(const PcmXMasterDriveSnapshot *left,
								  const PcmXMasterDriveSnapshot *right)
{
	return pcm_x_master_drive_snapshot_base_equal(left, right)
		   && left->pending_s_holders_bitmap == right->pending_s_holders_bitmap
		   && left->acked_s_holders_bitmap == right->acked_s_holders_bitmap
		   && left->involved_nodes_bitmap == right->involved_nodes_bitmap;
}


PcmXQueueResult
cluster_pcm_x_master_drive_snapshot_exact(const BufferTag *tag, uint64 cluster_epoch,
										  PcmXMasterDriveSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	pcm_x_master_drive_snapshot_clear(snapshot_out);
	if (header == NULL || tag == NULL || snapshot_out == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_tag_lookup_locked(tag, &tag_ref, &tag_slot);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref, tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || !BufferTagsEqual(&tag_slot->tag, tag)
		|| tag_slot->cluster_epoch != cluster_epoch) {
		result = PCM_X_QUEUE_STALE;
		goto snapshot_done;
	}
	if (tag_slot->active_index == PCM_X_INVALID_SLOT_INDEX) {
		result
			= tag_slot->active_slot_generation == 0 ? PCM_X_QUEUE_NOT_READY : PCM_X_QUEUE_CORRUPT;
		goto snapshot_done;
	}
	ticket_ref.slot_index = tag_slot->active_index;
	ticket_ref.slot_generation = tag_slot->active_slot_generation;
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref, tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	result = pcm_x_master_drive_capture_locked(runtime, tag_slot, tag_ref, ticket, ticket_ref,
											   snapshot_out);

snapshot_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_drive_bitmap_replace_exact(const PcmXMasterDriveSnapshot *expected,
												uint32 pending_s_holders_bitmap,
												uint32 acked_s_holders_bitmap,
												PcmXMasterDriveSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXMasterDriveSnapshot current;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	pcm_x_master_drive_snapshot_clear(snapshot_out);
	if (header == NULL || expected == NULL || snapshot_out == NULL
		|| (acked_s_holders_bitmap & ~pending_s_holders_bitmap) != 0
		|| (expected->acked_s_holders_bitmap & ~expected->pending_s_holders_bitmap) != 0
		|| (expected->ticket_state != PCM_XT_ACTIVE_PROBE
			&& expected->ticket_state != PCM_XT_ACTIVE_TRANSFER)
		|| expected->reserved != 0 || !pcm_x_wait_identity_valid(&expected->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&expected->ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&expected->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref,
													  &expected->ref.identity.tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &expected->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	result = pcm_x_master_drive_capture_locked(runtime, tag_slot, tag_ref, ticket, ticket_ref,
											   &current);
	if (result != PCM_X_QUEUE_OK)
		goto replace_done;
	*snapshot_out = current;
	if (pcm_x_master_drive_snapshot_base_equal(&current, expected)
		&& current.pending_s_holders_bitmap == pending_s_holders_bitmap
		&& current.acked_s_holders_bitmap == acked_s_holders_bitmap
		&& current.involved_nodes_bitmap == expected->involved_nodes_bitmap) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto replace_done;
	}
	if (!pcm_x_master_drive_snapshot_equal(&current, expected)) {
		result = PCM_X_QUEUE_STALE;
		goto replace_done;
	}

	ticket->pending_s_holders_bitmap = pending_s_holders_bitmap;
	ticket->acked_s_holders_bitmap = acked_s_holders_bitmap;
	pg_write_barrier();
	result = pcm_x_master_drive_capture_locked(runtime, tag_slot, tag_ref, ticket, ticket_ref,
											   snapshot_out);

replace_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


typedef struct PcmXBlockerRead {
	PcmXSlotRef slot_ref;
	ClusterLmdVertex blocker;
	Size next_index;
} PcmXBlockerRead;


static bool
pcm_x_lmd_vertex_metadata_equal(const ClusterLmdVertex *left, const ClusterLmdVertex *right)
{
	return left->xid == right->xid && left->local_start_ts_ms == right->local_start_ts_ms
		   && left->wait_seq == right->wait_seq;
}


static bool
pcm_x_lmd_vertex_equal(const ClusterLmdVertex *left, const ClusterLmdVertex *right)
{
	return pcm_x_lmd_vertex_identity_equal(left, right)
		   && pcm_x_lmd_vertex_metadata_equal(left, right);
}


static int
pcm_x_lmd_vertex_identity_compare(const ClusterLmdVertex *left, const ClusterLmdVertex *right)
{
	if (left->node_id != right->node_id)
		return left->node_id < right->node_id ? -1 : 1;
	if (left->procno != right->procno)
		return left->procno < right->procno ? -1 : 1;
	if (left->cluster_epoch != right->cluster_epoch)
		return left->cluster_epoch < right->cluster_epoch ? -1 : 1;
	if (left->request_id != right->request_id)
		return left->request_id < right->request_id ? -1 : 1;
	return 0;
}


static bool
pcm_x_lmd_vertex_valid(const ClusterLmdVertex *vertex)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	return header != NULL && vertex != NULL && vertex->node_id >= 0
		   && vertex->node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && (vertex->request_id != 0
			   || (vertex->procno != CLUSTER_LMD_TX_HOLDER_PROCNO && vertex->wait_seq != 0
				   && TransactionIdIsValid(vertex->xid)))
		   && (vertex->procno == CLUSTER_LMD_TX_HOLDER_PROCNO
			   || vertex->procno < header->layout.process_capacity);
}


/* ClusterLmdVertex has an ABI padding word before local_start_ts_ms.  Never
 * include that indeterminate byte range in the canonical blocker-set CRC. */
static void
pcm_x_blocker_crc32c_update(pg_crc32c *crc, const ClusterLmdVertex *blocker)
{
	COMP_CRC32C(*crc, &blocker->node_id, sizeof(blocker->node_id));
	COMP_CRC32C(*crc, &blocker->procno, sizeof(blocker->procno));
	COMP_CRC32C(*crc, &blocker->cluster_epoch, sizeof(blocker->cluster_epoch));
	COMP_CRC32C(*crc, &blocker->request_id, sizeof(blocker->request_id));
	COMP_CRC32C(*crc, &blocker->xid, sizeof(blocker->xid));
	COMP_CRC32C(*crc, &blocker->local_start_ts_ms, sizeof(blocker->local_start_ts_ms));
	COMP_CRC32C(*crc, &blocker->wait_seq, sizeof(blocker->wait_seq));
}


uint32
cluster_pcm_x_blocker_set_crc32c(const ClusterLmdVertex *blockers, Size nblockers)
{
	pg_crc32c crc;
	Size i;

	if (nblockers != 0 && blockers == NULL)
		return 0;
	INIT_CRC32C(crc);
	for (i = 0; i < nblockers; i++)
		pcm_x_blocker_crc32c_update(&crc, &blockers[i]);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


/* Validate and count canonical identities without allocating process memory. */
static PcmXQueueResult
pcm_x_blocker_input_validate(const PcmXTicketRef *ref, const ClusterLmdVertex *blockers,
							 Size nblockers, Size *unique_count_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	Size i;
	Size j;
	Size unique_count = 0;

	if (unique_count_out != NULL)
		*unique_count_out = 0;
	if (header == NULL || ref == NULL || unique_count_out == NULL
		|| (nblockers > 0 && blockers == NULL) || nblockers > UINT32_MAX)
		return PCM_X_QUEUE_INVALID;
	/* The pool is double-buffered (2 * max_wait_edges); one published set
	 * may consume at most one half so a full replacement can reserve first. */
	if (nblockers > header->layout.max_wait_edges)
		return PCM_X_QUEUE_NO_CAPACITY;

	for (i = 0; i < nblockers; i++) {
		bool first_identity = true;

		if (!pcm_x_lmd_vertex_valid(&blockers[i]))
			return PCM_X_QUEUE_INVALID;
		if (blockers[i].node_id == ref->identity.node_id
			&& blockers[i].procno == ref->identity.procno
			&& blockers[i].cluster_epoch == ref->identity.cluster_epoch
			&& blockers[i].request_id == ref->identity.request_id)
			return PCM_X_QUEUE_CORRUPT;
		for (j = 0; j < i; j++) {
			if (!pcm_x_lmd_vertex_identity_equal(&blockers[j], &blockers[i]))
				continue;
			if (!pcm_x_lmd_vertex_metadata_equal(&blockers[j], &blockers[i]))
				return PCM_X_QUEUE_CORRUPT;
			first_identity = false;
			break;
		}
		if (first_identity)
			unique_count++;
	}
	*unique_count_out = unique_count;
	return PCM_X_QUEUE_OK;
}


static const ClusterLmdVertex *
pcm_x_blocker_next_canonical(const ClusterLmdVertex *blockers, Size nblockers,
							 const ClusterLmdVertex *previous)
{
	const ClusterLmdVertex *candidate = NULL;
	Size i;
	Size j;

	for (i = 0; i < nblockers; i++) {
		bool first_identity = true;

		for (j = 0; j < i; j++) {
			if (pcm_x_lmd_vertex_identity_equal(&blockers[j], &blockers[i])) {
				first_identity = false;
				break;
			}
		}
		if (!first_identity
			|| (previous != NULL && pcm_x_lmd_vertex_identity_compare(previous, &blockers[i]) >= 0))
			continue;
		if (candidate == NULL || pcm_x_lmd_vertex_identity_compare(&blockers[i], candidate) < 0)
			candidate = &blockers[i];
	}
	return candidate;
}


/* Read one blocker only when its complete owner tuple is stable and exact. */
static bool
pcm_x_blocker_read_exact(const PcmXAllocatorView *view, Size slot_index,
						 const PcmXTicketHandle *owner_handle, Size owner_slot_index,
						 uint64 owner_slot_generation, uint64 set_generation,
						 uint32 expected_chunk_no, uint32 expected_state, PcmXBlockerRead *read_out)
{
	PcmXBlockerSlot *slot;
	PcmXTicketHandle handle;
	ClusterLmdVertex blocker;
	uint64 generation1;
	uint64 generation2;
	uint64 copied_owner_generation;
	uint64 copied_set_generation;
	Size copied_next_index;
	Size copied_owner_index;
	uint32 copied_chunk_no;
	uint32 copied_direction;
	uint32 state1;
	uint32 state2;

	if (view == NULL || owner_handle == NULL || read_out == NULL)
		return false;
	slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(view, slot_index);
	if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &generation1) || generation1 == 0)
		return false;
	pg_read_barrier();
	state1 = pcm_x_slot_state_read(&slot->slot);
	if (state1 != expected_state)
		return false;
	pg_read_barrier();
	handle = slot->handle;
	blocker = slot->blocker;
	copied_owner_generation = slot->owner_slot_generation;
	copied_set_generation = slot->set_generation;
	copied_next_index = slot->next_index;
	copied_owner_index = slot->owner_slot_index;
	copied_chunk_no = slot->chunk_no;
	copied_direction = slot->direction;
	pg_read_barrier();
	state2 = pcm_x_slot_state_read(&slot->slot);
	pg_read_barrier();
	if (!pcm_x_slot_generation_read(&slot->slot, &generation2) || generation1 != generation2
		|| state1 != state2)
		return false;
	if (!pcm_x_ticket_handle_equal(&handle, owner_handle)
		|| owner_handle->queue_generation != owner_slot_generation
		|| copied_owner_generation != owner_slot_generation
		|| copied_owner_index != owner_slot_index || copied_set_generation != set_generation
		|| copied_chunk_no != expected_chunk_no
		|| copied_direction != PCM_X_BLOCKER_DIRECTION_MASTER
		|| (copied_next_index != PCM_X_INVALID_SLOT_INDEX && copied_next_index >= view->capacity)
		|| !pcm_x_lmd_vertex_valid(&blocker))
		return false;
	read_out->slot_ref.slot_index = slot_index;
	read_out->slot_ref.slot_generation = generation1;
	read_out->blocker = blocker;
	read_out->next_index = copied_next_index;
	return true;
}


/* Validate a complete canonical chain before any caller mutates or copies it. */
static bool
pcm_x_blocker_chain_validate(const PcmXAllocatorView *view, Size head_index, uint32 count,
							 const PcmXTicketHandle *owner_handle, Size owner_slot_index,
							 uint64 owner_slot_generation, uint64 set_generation,
							 uint32 expected_state, const PcmXMasterBlockerEntry *expected_entries,
							 Size expected_count, PcmXMasterBlockerEntry *entries_out)
{
	ClusterLmdVertex previous;
	Size current_index = head_index;
	uint32 i;
	bool have_previous = false;

	if (view == NULL || owner_handle == NULL || count > view->capacity
		|| (expected_entries != NULL && expected_count != count))
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX;
	if (head_index == PCM_X_INVALID_SLOT_INDEX)
		return false;

	for (i = 0; i < count; i++) {
		PcmXBlockerRead read;

		if (!pcm_x_blocker_read_exact(view, current_index, owner_handle, owner_slot_index,
									  owner_slot_generation, set_generation, i, expected_state,
									  &read))
			return false;
		if (have_previous && pcm_x_lmd_vertex_identity_compare(&previous, &read.blocker) >= 0)
			return false;
		if (expected_entries != NULL) {
			const PcmXMasterBlockerEntry *expected = &expected_entries[i];

			if (expected->reserved != 0 || expected->chunk_no != i
				|| expected->slot_ref.slot_index != read.slot_ref.slot_index
				|| expected->slot_ref.slot_generation != read.slot_ref.slot_generation
				|| !pcm_x_lmd_vertex_equal(&expected->blocker, &read.blocker))
				return false;
		}
		if (entries_out != NULL) {
			entries_out[i].slot_ref = read.slot_ref;
			entries_out[i].blocker = read.blocker;
			entries_out[i].chunk_no = i;
			entries_out[i].reserved = 0;
		}
		previous = read.blocker;
		have_previous = true;
		current_index = read.next_index;
	}
	return current_index == PCM_X_INVALID_SLOT_INDEX;
}


/* Caller already validated this chain while holding its owner domain lock. */
static void
pcm_x_blocker_chain_state_write(const PcmXAllocatorView *view, Size head_index, uint32 count,
								uint32 state)
{
	Size current_index = head_index;
	uint32 i;

	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(view, current_index);

		current_index = slot->next_index;
		pcm_x_slot_state_write(&slot->slot, state);
	}
}


/* Capacity is proved before reserve so ordinary saturation mutates no slot. */
static PcmXAllocatorResult
pcm_x_blocker_capacity_preflight_locked(Size needed)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorView view;
	Size available;
	Size current_index;
	Size reservable = 0;
	Size scanned = 0;

	if (header == NULL || !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view))
		return PCM_X_ALLOC_INVALID;
	if (!pcm_x_allocator_state_valid_locked(&view))
		return PCM_X_ALLOC_CORRUPT;
	available = view.capacity - view.state->generation_exhausted - view.state->used;
	current_index = view.state->free_head;
	while (current_index != PCM_X_INVALID_SLOT_INDEX) {
		PcmXSlotHeader *slot;
		uint64 generation;

		if (scanned >= view.capacity)
			return PCM_X_ALLOC_CORRUPT;
		slot = pcm_x_allocator_slot(&view, current_index);
		if (slot == NULL || pcm_x_slot_state_read(slot) != PCM_X_SLOT_FREE
			|| pcm_x_slot_flags_read(slot) != 0 || !pcm_x_slot_generation_read(slot, &generation)
			|| slot->next_free == current_index)
			return PCM_X_ALLOC_CORRUPT;
		if (slot->next_free != PCM_X_INVALID_SLOT_INDEX
			&& pcm_x_allocator_slot(&view, slot->next_free) == NULL)
			return PCM_X_ALLOC_CORRUPT;
		if (generation != UINT64_MAX)
			reservable++;
		current_index = slot->next_free;
		scanned++;
	}
	if (scanned != available)
		return PCM_X_ALLOC_CORRUPT;
	return reservable >= needed ? PCM_X_ALLOC_OK : PCM_X_ALLOC_NO_CAPACITY;
}


/* Caller owns allocator_lock EXCLUSIVE; validation precedes the first free. */
static bool
pcm_x_blocker_chain_release_locked_exact(Size head_index, uint32 count,
										 const PcmXTicketHandle *owner_handle,
										 Size owner_slot_index, uint64 owner_slot_generation,
										 uint64 set_generation, uint32 expected_state)
{
	PcmXAllocatorView view;
	Size current_index = head_index;
	uint32 i;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view)
		|| !pcm_x_blocker_chain_validate(&view, head_index, count, owner_handle, owner_slot_index,
										 owner_slot_generation, set_generation, expected_state,
										 NULL, 0, NULL))
		return false;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current_index);
		PcmXSlotRef ref;
		Size next_index = slot->next_index;

		ref.slot_index = current_index;
		if (!pcm_x_slot_generation_read(&slot->slot, &ref.slot_generation)
			|| pcm_x_allocator_release_locked(PCM_X_ALLOC_BLOCKER, ref, expected_state)
				   != PCM_X_ALLOC_OK)
			return false;
		current_index = next_index;
	}
	return current_index == PCM_X_INVALID_SLOT_INDEX;
}


static bool
pcm_x_blocker_chain_release_exact(Size head_index, uint32 count,
								  const PcmXTicketHandle *owner_handle, Size owner_slot_index,
								  uint64 owner_slot_generation, uint64 set_generation,
								  uint32 expected_state)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	bool released;

	if (header == NULL || !pcm_x_allocator_entry_unlocked(header))
		return false;
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	released = pcm_x_blocker_chain_release_locked_exact(head_index, count, owner_handle,
														owner_slot_index, owner_slot_generation,
														set_generation, expected_state);
	LWLockRelease(&header->allocator_lock.lock);
	return released;
}


static PcmXAllocatorResult
pcm_x_blocker_chain_reserve_locked(const PcmXTicketRef *ref, PcmXSlotRef owner_ref,
								   uint64 set_generation, const ClusterLmdVertex *blockers,
								   Size nblockers, Size unique_count, Size *head_index_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorResult result;
	PcmXBlockerSlot *previous_slot = NULL;
	const ClusterLmdVertex *previous = NULL;
	Size head_index = PCM_X_INVALID_SLOT_INDEX;
	Size reserved_count = 0;

	if (head_index_out != NULL)
		*head_index_out = PCM_X_INVALID_SLOT_INDEX;
	if (header == NULL || ref == NULL || head_index_out == NULL
		|| !LWLockHeldByMeInMode(&header->allocator_lock.lock, LW_EXCLUSIVE))
		return PCM_X_ALLOC_INVALID;
	result = pcm_x_blocker_capacity_preflight_locked(unique_count);
	if (result != PCM_X_ALLOC_OK)
		return result;

	while (reserved_count < unique_count) {
		const ClusterLmdVertex *candidate
			= pcm_x_blocker_next_canonical(blockers, nblockers, previous);
		PcmXSlotHeader *raw_slot;
		PcmXBlockerSlot *slot;
		PcmXSlotRef slot_ref;

		if (candidate == NULL) {
			result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		result = pcm_x_allocator_reserve_locked(PCM_X_ALLOC_BLOCKER, &slot_ref, &raw_slot);
		if (result != PCM_X_ALLOC_OK) {
			/* Full was ruled out before the first mutation. */
			if (result == PCM_X_ALLOC_NO_CAPACITY)
				result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		slot = (PcmXBlockerSlot *)raw_slot;
		slot->handle = ref->handle;
		slot->blocker = *candidate;
		slot->owner_slot_generation = owner_ref.slot_generation;
		slot->set_generation = set_generation;
		slot->next_index = PCM_X_INVALID_SLOT_INDEX;
		slot->owner_slot_index = owner_ref.slot_index;
		slot->chunk_no = (uint32)reserved_count;
		slot->direction = PCM_X_BLOCKER_DIRECTION_MASTER;
		if (previous_slot == NULL)
			head_index = slot_ref.slot_index;
		else
			previous_slot->next_index = slot_ref.slot_index;
		previous_slot = slot;
		previous = candidate;
		reserved_count++;
	}
	if (result != PCM_X_ALLOC_OK) {
		if (!pcm_x_blocker_chain_release_locked_exact(
				head_index, (uint32)reserved_count, &ref->handle, owner_ref.slot_index,
				owner_ref.slot_generation, set_generation, PCM_XB_RESERVED_NONVISIBLE))
			result = PCM_X_ALLOC_CORRUPT;
		return result;
	}
	*head_index_out = head_index;
	return PCM_X_ALLOC_OK;
}


/* Inbound BLOCKER_SET staging is separate from the currently published set.
 * The old graph and its exact blocker chain remain authoritative until COMMIT
 * atomically swaps a complete canonical replacement into the ticket. */
static bool
pcm_x_master_blocker_stage_is_clear(const PcmXMasterTicketSlot *ticket)
{
	return ticket != NULL && ticket->blocker_stage_head_index == PCM_X_INVALID_SLOT_INDEX
		   && ticket->blocker_stage_set_generation == 0 && ticket->blocker_stage_count == 0
		   && ticket->blocker_stage_crc32c == 0 && ticket->blocker_stage_next_chunk == 0
		   && ticket->blocker_stage_source_session == 0 && ticket->blocker_stage_source_node == -1;
}


static void
pcm_x_master_blocker_stage_clear(PcmXMasterTicketSlot *ticket)
{
	if (ticket == NULL)
		return;
	ticket->blocker_stage_head_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->blocker_stage_set_generation = 0;
	ticket->blocker_stage_count = 0;
	ticket->blocker_stage_crc32c = 0;
	ticket->blocker_stage_next_chunk = 0;
	ticket->blocker_stage_source_session = 0;
	ticket->blocker_stage_source_node = -1;
}


static bool
pcm_x_master_blocker_stage_metadata_valid(const PcmXMasterTicketSlot *ticket)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	if (header == NULL || ticket == NULL)
		return false;
	if (ticket->blocker_stage_set_generation == 0)
		return pcm_x_master_blocker_stage_is_clear(ticket);
	return ticket->blocker_stage_set_generation != UINT64_MAX
		   && ticket->blocker_stage_count <= header->layout.max_wait_edges
		   && ticket->blocker_stage_next_chunk <= ticket->blocker_stage_count
		   && ticket->blocker_stage_source_session != 0 && ticket->blocker_stage_source_node >= 0
		   && ticket->blocker_stage_source_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && ((ticket->blocker_stage_count == 0
				&& ticket->blocker_stage_head_index == PCM_X_INVALID_SLOT_INDEX)
			   || (ticket->blocker_stage_count != 0
				   && ticket->blocker_stage_head_index != PCM_X_INVALID_SLOT_INDEX));
}


static bool
pcm_x_master_blocker_published_source_valid(const PcmXMasterTicketSlot *ticket)
{
	if (ticket == NULL)
		return false;
	if (ticket->blocker_set_generation == 0)
		return ticket->blocker_set_source_node == -1 && ticket->blocker_set_source_session == 0;
	return (ticket->blocker_set_source_node == -1 && ticket->blocker_set_source_session == 0)
		   || (ticket->blocker_set_source_node >= 0
			   && ticket->blocker_set_source_node < PCM_X_PROTOCOL_NODE_LIMIT
			   && ticket->blocker_set_source_session != 0);
}


static bool
pcm_x_master_blocker_published_source_exact(const PcmXMasterTicketSlot *ticket, int32 source_node,
											uint64 source_session)
{
	return ticket != NULL && ticket->blocker_set_generation != 0
		   && ticket->blocker_set_source_node == source_node
		   && ticket->blocker_set_source_session == source_session;
}


static bool
pcm_x_master_blocker_ack_replay_exact(const PcmXMasterTicketSlot *ticket, int32 source_node,
									  uint64 source_session)
{
	const PcmXReliableLegState *leg;

	if (ticket == NULL || !pcm_x_master_blocker_stage_is_clear(ticket)
		|| ticket->graph_generation == 0
		|| !pcm_x_master_blocker_published_source_exact(ticket, source_node, source_session))
		return false;
	leg = &ticket->reliable;
	return leg->retry_deadline_ms == 0 && leg->expected_responder_session == 0
		   && leg->retry_count == 0 && leg->expected_responder_node == 0 && leg->pending_opcode == 0
		   && leg->last_response_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT && leg->phase == 0
		   && leg->flags == 0 && leg->reserved == 0;
}


static bool
pcm_x_master_probe_source_exact(const PcmXMasterTicketSlot *ticket, int32 source_node,
								uint64 source_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	const PcmXPeerFrontier *frontier;

	if (header == NULL || ticket == NULL || source_node < 0
		|| source_node >= PCM_X_PROTOCOL_NODE_LIMIT || source_session == 0)
		return false;
	frontier = &header->peer_frontiers[source_node];
	return ticket->reliable.pending_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
		   && ticket->reliable.phase == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
		   && ticket->reliable.expected_responder_node == source_node
		   && ticket->reliable.expected_responder_session == source_session
		   && frontier->cluster_epoch == ticket->ref.identity.cluster_epoch
		   && frontier->sender_session_incarnation == source_session;
}


static bool
pcm_x_blocker_header_valid(const PcmXBlockerSetHeaderPayload *header_payload)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	return header != NULL && header_payload != NULL && header_payload->set_generation != 0
		   && header_payload->set_generation != UINT64_MAX
		   && header_payload->nblockers <= header->layout.max_wait_edges
		   && header_payload->ref.grant_generation == 0
		   && pcm_x_wait_identity_valid(&header_payload->ref.identity);
}


static bool
pcm_x_blocker_edge_locator_equal(const PcmXMasterTicketSlot *ticket,
								 const PcmXBlockerChunkPayload *edge)
{
	return ticket != NULL && edge != NULL && BufferTagsEqual(&ticket->ref.identity.tag, &edge->tag)
		   && ticket->ref.identity.node_id == edge->requester_node
		   && ticket->ref.identity.procno == edge->requester_procno
		   && ticket->ref.identity.cluster_epoch == edge->cluster_epoch
		   && ticket->ref.identity.request_id == edge->request_id
		   && pcm_x_ticket_handle_equal(&ticket->ref.handle, &edge->handle)
		   && edge->grant_generation == 0;
}


static bool
pcm_x_blocker_stage_slot_exact(const PcmXAllocatorView *view, Size slot_index,
							   const PcmXMasterTicketSlot *ticket, PcmXSlotRef ticket_ref,
							   uint64 set_generation, uint32 chunk_no, uint32 expected_state,
							   PcmXBlockerSlot **slot_out, PcmXSlotRef *slot_ref_out)
{
	PcmXBlockerSlot *slot;
	uint64 slot_generation;

	if (view == NULL || ticket == NULL || slot_out == NULL || slot_ref_out == NULL)
		return false;
	slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(view, slot_index);
	if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &slot_generation)
		|| slot_generation == 0 || pcm_x_slot_state_read(&slot->slot) != expected_state
		|| !pcm_x_ticket_handle_equal(&slot->handle, &ticket->ref.handle)
		|| slot->owner_slot_generation != ticket_ref.slot_generation
		|| slot->owner_slot_index != ticket_ref.slot_index || slot->set_generation != set_generation
		|| slot->chunk_no != chunk_no || slot->direction != PCM_X_BLOCKER_DIRECTION_MASTER
		|| (slot->next_index != PCM_X_INVALID_SLOT_INDEX && slot->next_index >= view->capacity))
		return false;
	slot_ref_out->slot_index = slot_index;
	slot_ref_out->slot_generation = slot_generation;
	*slot_out = slot;
	return true;
}


/* Resolve one exact staging chunk by its bounded owner chain, never by a pool scan. */
static bool
pcm_x_blocker_stage_nth_exact(const PcmXMasterTicketSlot *ticket, PcmXSlotRef ticket_ref,
							  uint32 target_chunk, PcmXBlockerSlot **slot_out,
							  PcmXSlotRef *slot_ref_out)
{
	PcmXAllocatorView view;
	PcmXBlockerSlot *slot = NULL;
	PcmXSlotRef slot_ref;
	Size current;
	uint32 i;

	if (ticket == NULL || target_chunk >= ticket->blocker_stage_count
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view))
		return false;
	current = ticket->blocker_stage_head_index;
	for (i = 0; i <= target_chunk; i++) {
		uint32 expected_state
			= i < ticket->blocker_stage_next_chunk ? PCM_XB_LIVE : PCM_XB_RESERVED_NONVISIBLE;

		if (!pcm_x_blocker_stage_slot_exact(&view, current, ticket, ticket_ref,
											ticket->blocker_stage_set_generation, i, expected_state,
											&slot, &slot_ref))
			return false;
		if (i != target_chunk && slot->next_index == PCM_X_INVALID_SLOT_INDEX)
			return false;
		current = slot->next_index;
	}
	*slot_out = slot;
	*slot_ref_out = slot_ref;
	return true;
}


static bool
pcm_x_blocker_stage_chain_validate(const PcmXMasterTicketSlot *ticket, PcmXSlotRef ticket_ref,
								   PcmXMasterBlockerEntry *entries_out, pg_crc32c *crc_out)
{
	PcmXAllocatorView view;
	ClusterLmdVertex previous;
	pg_crc32c crc;
	Size current;
	uint32 i;
	bool have_previous = false;

	if (ticket == NULL || ticket->blocker_stage_next_chunk != ticket->blocker_stage_count
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view))
		return false;
	INIT_CRC32C(crc);
	current = ticket->blocker_stage_head_index;
	for (i = 0; i < ticket->blocker_stage_count; i++) {
		PcmXBlockerSlot *slot;
		PcmXSlotRef slot_ref;

		if (!pcm_x_blocker_stage_slot_exact(&view, current, ticket, ticket_ref,
											ticket->blocker_stage_set_generation, i, PCM_XB_LIVE,
											&slot, &slot_ref)
			|| !pcm_x_lmd_vertex_valid(&slot->blocker)
			|| (slot->blocker.node_id == ticket->ref.identity.node_id
				&& slot->blocker.procno == ticket->ref.identity.procno
				&& slot->blocker.cluster_epoch == ticket->ref.identity.cluster_epoch
				&& slot->blocker.request_id == ticket->ref.identity.request_id)
			|| (have_previous && pcm_x_lmd_vertex_identity_compare(&previous, &slot->blocker) >= 0))
			return false;
		pcm_x_blocker_crc32c_update(&crc, &slot->blocker);
		if (entries_out != NULL) {
			entries_out[i].slot_ref = slot_ref;
			entries_out[i].blocker = slot->blocker;
			entries_out[i].chunk_no = i;
			entries_out[i].reserved = 0;
		}
		previous = slot->blocker;
		have_previous = true;
		current = slot->next_index;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		return false;
	FIN_CRC32C(crc);
	if (crc_out != NULL)
		*crc_out = crc;
	return true;
}


/* Caller owns allocator_lock EXCLUSIVE; every slot is still nonvisible. */
static bool
pcm_x_blocker_stage_chain_release_reserved_locked(Size head_index, uint32 count,
												  const PcmXTicketHandle *handle,
												  PcmXSlotRef ticket_ref, uint64 set_generation)
{
	PcmXAllocatorView view;
	Size current = head_index;
	uint32 i;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view))
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot;
		PcmXSlotRef slot_ref;
		Size next;

		if (handle == NULL || current == PCM_X_INVALID_SLOT_INDEX)
			return false;
		slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &slot_ref.slot_generation)
			|| pcm_x_slot_state_read(&slot->slot) != PCM_XB_RESERVED_NONVISIBLE
			|| !pcm_x_ticket_handle_equal(&slot->handle, handle)
			|| slot->owner_slot_index != ticket_ref.slot_index
			|| slot->owner_slot_generation != ticket_ref.slot_generation
			|| slot->set_generation != set_generation || slot->chunk_no != i
			|| slot->direction != PCM_X_BLOCKER_DIRECTION_MASTER)
			return false;
		next = slot->next_index;
		slot_ref.slot_index = current;
		if (pcm_x_allocator_release_locked(PCM_X_ALLOC_BLOCKER, slot_ref,
										   PCM_XB_RESERVED_NONVISIBLE)
			!= PCM_X_ALLOC_OK)
			return false;
		current = next;
	}
	return current == PCM_X_INVALID_SLOT_INDEX;
}


/* Caller owns allocator_lock EXCLUSIVE.  An aborted stage can contain a LIVE
 * prefix and an unfilled RESERVED suffix; after the owner lock marks the
 * whole chain DETACHING, validate every owner/link before releasing any slot.
 * Unfilled suffix vertices are intentionally not interpreted. */
static bool
pcm_x_blocker_stage_chain_release_detaching_locked(Size head_index, uint32 count,
												   const PcmXTicketHandle *handle,
												   PcmXSlotRef ticket_ref, uint64 set_generation)
{
	PcmXAllocatorView view;
	Size current = head_index;
	uint32 i;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view) || handle == NULL
		|| count > view.capacity)
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot;
		uint64 slot_generation;

		if (current == PCM_X_INVALID_SLOT_INDEX)
			return false;
		slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &slot_generation)
			|| pcm_x_slot_state_read(&slot->slot) != PCM_XB_DETACHING
			|| !pcm_x_ticket_handle_equal(&slot->handle, handle)
			|| slot->owner_slot_index != ticket_ref.slot_index
			|| slot->owner_slot_generation != ticket_ref.slot_generation
			|| slot->set_generation != set_generation || slot->chunk_no != i
			|| slot->direction != PCM_X_BLOCKER_DIRECTION_MASTER
			|| (slot->next_index != PCM_X_INVALID_SLOT_INDEX && slot->next_index >= view.capacity))
			return false;
		current = slot->next_index;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		return false;
	current = head_index;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		PcmXSlotRef slot_ref;
		Size next = slot->next_index;

		slot_ref.slot_index = current;
		if (!pcm_x_slot_generation_read(&slot->slot, &slot_ref.slot_generation)
			|| pcm_x_allocator_release_locked(PCM_X_ALLOC_BLOCKER, slot_ref, PCM_XB_DETACHING)
				   != PCM_X_ALLOC_OK)
			return false;
		current = next;
	}
	return current == PCM_X_INVALID_SLOT_INDEX;
}


static PcmXAllocatorResult
pcm_x_blocker_stage_chain_reserve_locked(const PcmXTicketRef *ref, PcmXSlotRef ticket_ref,
										 uint64 set_generation, uint32 count, Size *head_index_out)
{
	PcmXAllocatorResult result;
	PcmXBlockerSlot *previous = NULL;
	Size head_index = PCM_X_INVALID_SLOT_INDEX;
	uint32 reserved = 0;

	if (ref == NULL || head_index_out == NULL)
		return PCM_X_ALLOC_INVALID;
	*head_index_out = PCM_X_INVALID_SLOT_INDEX;
	result = pcm_x_blocker_capacity_preflight_locked(count);
	if (result != PCM_X_ALLOC_OK)
		return result;
	while (reserved < count) {
		PcmXSlotHeader *raw_slot;
		PcmXBlockerSlot *slot;
		PcmXSlotRef slot_ref;

		result = pcm_x_allocator_reserve_locked(PCM_X_ALLOC_BLOCKER, &slot_ref, &raw_slot);
		if (result != PCM_X_ALLOC_OK) {
			if (result == PCM_X_ALLOC_NO_CAPACITY)
				result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		slot = (PcmXBlockerSlot *)raw_slot;
		slot->handle = ref->handle;
		slot->owner_slot_generation = ticket_ref.slot_generation;
		slot->set_generation = set_generation;
		slot->next_index = PCM_X_INVALID_SLOT_INDEX;
		slot->owner_slot_index = ticket_ref.slot_index;
		slot->chunk_no = reserved;
		slot->direction = PCM_X_BLOCKER_DIRECTION_MASTER;
		if (previous == NULL)
			head_index = slot_ref.slot_index;
		else
			previous->next_index = slot_ref.slot_index;
		previous = slot;
		reserved++;
	}
	if (result != PCM_X_ALLOC_OK) {
		if (!pcm_x_blocker_stage_chain_release_reserved_locked(head_index, reserved, &ref->handle,
															   ticket_ref, set_generation))
			result = PCM_X_ALLOC_CORRUPT;
		return result;
	}
	*head_index_out = head_index;
	return PCM_X_ALLOC_OK;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_probe_arm_exact(const PcmXTicketRef *ref, int32 responder_node,
											 uint64 responder_session, PcmXPhasePayload *probe_out,
											 PcmXMasterProbeToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXPeerFrontier *frontier;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint64 next_sequence;
	uint32 flags;
	uint32 partition;

	if (probe_out != NULL)
		memset(probe_out, 0, sizeof(*probe_out));
	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (header == NULL || ref == NULL || probe_out == NULL || token_out == NULL
		|| ref->grant_generation != 0 || responder_node < 0
		|| responder_node >= PCM_X_PROTOCOL_NODE_LIMIT || responder_session == 0
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_PROBE)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_master_pending_x_flags_valid((flags = pcm_x_slot_flags_read(&ticket->slot))))
		result = PCM_X_QUEUE_CORRUPT;
	else if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING) != 0)
		result = PCM_X_QUEUE_CORRUPT;
	else {
		frontier = &header->peer_frontiers[responder_node];
		if (frontier->cluster_epoch != ref->identity.cluster_epoch
			|| frontier->sender_session_incarnation != responder_session)
			result = PCM_X_QUEUE_STALE;
		else if (!pcm_x_master_terminal_leg_is_clear(&ticket->reliable)) {
			if (pcm_x_master_probe_source_exact(ticket, responder_node, responder_session)) {
				token_out->ref = ticket->ref;
				token_out->state_sequence = ticket->reliable.state_sequence;
				token_out->expected_responder_session = responder_session;
				token_out->expected_responder_node = responder_node;
				result = PCM_X_QUEUE_DUPLICATE;
			} else
				result = PCM_X_QUEUE_BUSY;
		} else if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED) != 0) {
			/* An exact replay of the already durable leg is allowed above.  Once
			 * that leg clears, cancellation owns forward progress and no later
			 * holder may be added to the terminal participant set. */
			result = PCM_X_QUEUE_NOT_READY;
		} else if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &next_sequence))
			result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		else {
			ticket->reliable.state_sequence = next_sequence;
			ticket->reliable.retry_deadline_ms = 0;
			ticket->reliable.expected_responder_session = responder_session;
			ticket->reliable.retry_count = 0;
			ticket->reliable.expected_responder_node = responder_node;
			ticket->reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK;
			ticket->reliable.phase = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK;
			ticket->reliable.flags = 0;
			ticket->involved_nodes_bitmap |= UINT32_C(1) << responder_node;
			pg_write_barrier();
			token_out->ref = ticket->ref;
			token_out->state_sequence = next_sequence;
			token_out->expected_responder_session = responder_session;
			token_out->expected_responder_node = responder_node;
			result = PCM_X_QUEUE_OK;
		}
	}
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		probe_out->ref = ticket->ref;
		/* The complete [88,96) generation remains zero by construction. */
		Assert(probe_out->reason == 0 && probe_out->phase == 0 && probe_out->flags == 0);
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Caller owns the exact ticket's master-domain lock. */
static PcmXQueueResult
pcm_x_master_blocker_stage_begin_precheck_locked(const PcmXMasterTicketSlot *ticket,
												 PcmXSlotRef ticket_ref,
												 const PcmXBlockerSetHeaderPayload *begin,
												 int32 source_node, uint64 source_session)
{
	PcmXAllocatorView blocker_view;
	uint64 current_generation;
	uint32 state;
	bool current_probe_source;
	bool published_source;

	if (ticket == NULL || begin == NULL || !pcm_x_master_blocker_stage_metadata_valid(ticket)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view))
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_ticket_admission_ref_equal(&ticket->ref, &begin->ref))
		return PCM_X_QUEUE_STALE;
	current_generation = ticket->blocker_set_generation;
	if ((current_generation == 0
		 && (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX || ticket->blocker_count != 0
			 || ticket->blocker_set_crc32c != 0
			 || !pcm_x_master_blocker_published_source_valid(ticket)))
		|| (current_generation != 0
			&& (!pcm_x_blocker_chain_validate(&blocker_view, ticket->blocker_head_index,
											  ticket->blocker_count, &ticket->ref.handle,
											  ticket_ref.slot_index, ticket_ref.slot_generation,
											  current_generation, PCM_XB_LIVE, NULL, 0, NULL)
				|| !pcm_x_master_blocker_published_source_valid(ticket))))
		return PCM_X_QUEUE_CORRUPT;
	state = pcm_x_slot_state_read(&ticket->slot);
	if (state != PCM_XT_ACTIVE_PROBE) {
		if (state != PCM_XT_ACTIVE_TRANSFER && state != PCM_XT_COMPLETE && state != PCM_XT_CANCELLED
			&& state != PCM_XT_RETIRE_CREDIT)
			return PCM_X_QUEUE_BAD_STATE;
		if (!pcm_x_master_blocker_stage_is_clear(ticket))
			return PCM_X_QUEUE_CORRUPT;
		return begin->set_generation == current_generation
					   && begin->nblockers == ticket->blocker_count
					   && begin->set_crc32c == ticket->blocker_set_crc32c
					   && pcm_x_master_blocker_published_source_exact(ticket, source_node,
																	  source_session)
				   ? PCM_X_QUEUE_DUPLICATE
				   : PCM_X_QUEUE_STALE;
	}
	current_probe_source = pcm_x_master_probe_source_exact(ticket, source_node, source_session);
	if (!current_probe_source
		&& !pcm_x_master_blocker_ack_replay_exact(ticket, source_node, source_session))
		return PCM_X_QUEUE_STALE;
	if (ticket->blocker_stage_set_generation != 0) {
		if (begin->set_generation == ticket->blocker_stage_set_generation
			&& begin->nblockers == ticket->blocker_stage_count
			&& begin->set_crc32c == ticket->blocker_stage_crc32c
			&& source_node == ticket->blocker_stage_source_node
			&& source_session == ticket->blocker_stage_source_session)
			return PCM_X_QUEUE_DUPLICATE;
		return begin->set_generation <= ticket->blocker_stage_set_generation ? PCM_X_QUEUE_STALE
																			 : PCM_X_QUEUE_BUSY;
	}
	published_source
		= pcm_x_master_blocker_published_source_exact(ticket, source_node, source_session);
	if (begin->set_generation == current_generation && published_source) {
		return begin->nblockers == ticket->blocker_count
					   && begin->set_crc32c == ticket->blocker_set_crc32c
				   ? PCM_X_QUEUE_DUPLICATE
				   : PCM_X_QUEUE_STALE;
	}
	/* set_generation belongs to the holder's local tag, so another exact
	 * holder may legitimately publish the same (or a lower) numeric value.
	 * The authenticated source tuple completes its namespace.  Before
	 * replacing a previous holder's set, require that exact set to have
	 * reached the graph-commit boundary. */
	if (current_probe_source && !published_source) {
		if (current_generation != 0 && ticket->graph_generation == 0)
			return PCM_X_QUEUE_NOT_READY;
		return PCM_X_QUEUE_OK;
	}
	/* A non-current source can only be replaying the last published set. */
	if (!current_probe_source)
		return PCM_X_QUEUE_STALE;
	if (begin->set_generation < current_generation)
		return PCM_X_QUEUE_STALE;
	if (current_generation == UINT64_MAX || begin->set_generation == UINT64_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	if (current_generation != 0 && ticket->graph_generation == 0)
		return PCM_X_QUEUE_NOT_READY;
	return begin->set_generation == current_generation + 1 ? PCM_X_QUEUE_OK : PCM_X_QUEUE_NOT_READY;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_stage_begin_exact(const PcmXBlockerSetHeaderPayload *begin,
											   int32 authenticated_source_node,
											   uint64 authenticated_source_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	PcmXAllocatorResult allocator_result;
	Size staged_head = PCM_X_INVALID_SLOT_INDEX;
	uint32 partition;
	bool release_staged = false;

	if (!pcm_x_blocker_header_valid(begin) || authenticated_source_node < 0
		|| authenticated_source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_source_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&begin->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&begin->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &begin->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = ticket == NULL ? PCM_X_QUEUE_STALE : PCM_X_QUEUE_NOT_READY;
	else
		result = pcm_x_master_blocker_stage_begin_precheck_locked(
			ticket, ticket_ref, begin, authenticated_source_node, authenticated_source_session);
	LWLockRelease(&header->master_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
			pcm_x_runtime_fail_closed();
		return result;
	}

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	allocator_result = pcm_x_blocker_stage_chain_reserve_locked(
		&begin->ref, ticket_ref, begin->set_generation, begin->nblockers, &staged_head);
	LWLockRelease(&header->allocator_lock.lock);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &begin->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = ticket == NULL ? PCM_X_QUEUE_STALE : PCM_X_QUEUE_NOT_READY;
	else
		result = pcm_x_master_blocker_stage_begin_precheck_locked(
			ticket, ticket_ref, begin, authenticated_source_node, authenticated_source_session);
	if (result == PCM_X_QUEUE_OK) {
		ticket->blocker_stage_head_index = staged_head;
		ticket->blocker_stage_set_generation = begin->set_generation;
		ticket->blocker_stage_count = begin->nblockers;
		ticket->blocker_stage_crc32c = begin->set_crc32c;
		ticket->blocker_stage_next_chunk = 0;
		ticket->blocker_stage_source_session = authenticated_source_session;
		ticket->blocker_stage_source_node = authenticated_source_node;
		pg_write_barrier();
	} else
		release_staged = true;
	LWLockRelease(&header->master_locks[partition].lock);

	if (release_staged) {
		LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
		if (!pcm_x_blocker_stage_chain_release_reserved_locked(staged_head, begin->nblockers,
															   &begin->ref.handle, ticket_ref,
															   begin->set_generation))
			result = PCM_X_QUEUE_CORRUPT;
		LWLockRelease(&header->allocator_lock.lock);
	}
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_master_blocker_published_edge_replay(const PcmXMasterTicketSlot *ticket,
										   PcmXSlotRef ticket_ref,
										   const PcmXBlockerChunkPayload *edge, int32 source_node,
										   uint64 source_session)
{
	PcmXAllocatorView view;
	PcmXBlockerRead read;
	Size current;
	uint32 i;

	if (ticket == NULL || edge == NULL || edge->set_generation != ticket->blocker_set_generation
		|| !pcm_x_master_blocker_published_source_exact(ticket, source_node, source_session)
		|| edge->chunk_no >= ticket->blocker_count)
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view)
		|| !pcm_x_blocker_chain_validate(&view, ticket->blocker_head_index, ticket->blocker_count,
										 &ticket->ref.handle, ticket_ref.slot_index,
										 ticket_ref.slot_generation, ticket->blocker_set_generation,
										 PCM_XB_LIVE, NULL, 0, NULL))
		return PCM_X_QUEUE_CORRUPT;
	current = ticket->blocker_head_index;
	for (i = 0; i <= edge->chunk_no; i++) {
		if (!pcm_x_blocker_read_exact(&view, current, &ticket->ref.handle, ticket_ref.slot_index,
									  ticket_ref.slot_generation, ticket->blocker_set_generation, i,
									  PCM_XB_LIVE, &read))
			return PCM_X_QUEUE_CORRUPT;
		current = read.next_index;
	}
	return pcm_x_lmd_vertex_equal(&read.blocker, &edge->blocker) ? PCM_X_QUEUE_DUPLICATE
																 : PCM_X_QUEUE_STALE;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_stage_edge_exact(const PcmXBlockerChunkPayload *edge,
											  int32 authenticated_source_node,
											  uint64 authenticated_source_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXBlockerSlot *slot;
	PcmXBlockerSlot *previous;
	PcmXSlotRef found;
	PcmXSlotRef slot_ref;
	PcmXSlotRef previous_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 partition;
	uint32 state;

	if (header == NULL || edge == NULL || authenticated_source_node < 0
		|| authenticated_source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_source_session == 0 || edge->set_generation == 0
		|| edge->set_generation == UINT64_MAX || edge->grant_generation != 0
		|| edge->requester_node < 0 || edge->requester_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| edge->requester_procno >= header->layout.process_capacity || edge->request_id == 0
		|| edge->handle.ticket_id == 0 || edge->handle.queue_generation == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &edge->handle, &found);
	if (directory_result == PCM_X_DIRECTORY_OK)
		ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																	   found);
	else
		ticket = NULL;
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	if (ticket == NULL) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&edge->tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, found, &edge->tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_blocker_edge_locator_equal(ticket, edge))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (!pcm_x_master_blocker_stage_metadata_valid(ticket))
		result = PCM_X_QUEUE_CORRUPT;
	else {
		state = pcm_x_slot_state_read(&ticket->slot);
		if (state != PCM_XT_ACTIVE_PROBE) {
			if (state != PCM_XT_ACTIVE_TRANSFER && state != PCM_XT_COMPLETE
				&& state != PCM_XT_CANCELLED && state != PCM_XT_RETIRE_CREDIT)
				result = PCM_X_QUEUE_BAD_STATE;
			else if (!pcm_x_master_blocker_stage_is_clear(ticket))
				result = PCM_X_QUEUE_CORRUPT;
			else
				result = pcm_x_master_blocker_published_edge_replay(
					ticket, found, edge, authenticated_source_node, authenticated_source_session);
		} else if (!pcm_x_master_probe_source_exact(ticket, authenticated_source_node,
													authenticated_source_session)
				   && !pcm_x_master_blocker_ack_replay_exact(ticket, authenticated_source_node,
															 authenticated_source_session))
			result = PCM_X_QUEUE_STALE;
		else if (ticket->blocker_stage_set_generation == 0)
			result = pcm_x_master_blocker_published_edge_replay(
				ticket, found, edge, authenticated_source_node, authenticated_source_session);
		else if (edge->set_generation != ticket->blocker_stage_set_generation
				 || authenticated_source_node != ticket->blocker_stage_source_node
				 || authenticated_source_session != ticket->blocker_stage_source_session)
			result = PCM_X_QUEUE_STALE;
		else if (edge->chunk_no >= ticket->blocker_stage_count)
			result = PCM_X_QUEUE_STALE;
		else if (edge->chunk_no > ticket->blocker_stage_next_chunk)
			result = PCM_X_QUEUE_NOT_READY;
		else if (!pcm_x_lmd_vertex_valid(&edge->blocker)
				 || edge->blocker.node_id != authenticated_source_node
				 || edge->blocker.cluster_epoch != ticket->ref.identity.cluster_epoch
				 || (edge->blocker.node_id == ticket->ref.identity.node_id
					 && edge->blocker.procno == ticket->ref.identity.procno
					 && edge->blocker.request_id == ticket->ref.identity.request_id))
			result = PCM_X_QUEUE_INVALID;
		else if (!pcm_x_blocker_stage_nth_exact(ticket, found, edge->chunk_no, &slot, &slot_ref))
			result = PCM_X_QUEUE_CORRUPT;
		else if (edge->chunk_no < ticket->blocker_stage_next_chunk)
			result = pcm_x_lmd_vertex_equal(&slot->blocker, &edge->blocker) ? PCM_X_QUEUE_DUPLICATE
																			: PCM_X_QUEUE_STALE;
		else {
			result = PCM_X_QUEUE_OK;
			if (edge->chunk_no > 0) {
				if (!pcm_x_blocker_stage_nth_exact(ticket, found, edge->chunk_no - 1, &previous,
												   &previous_ref))
					result = PCM_X_QUEUE_CORRUPT;
				else if (pcm_x_lmd_vertex_identity_compare(&previous->blocker, &edge->blocker) >= 0)
					result = PCM_X_QUEUE_INVALID;
			}
			if (result == PCM_X_QUEUE_OK) {
				slot->blocker = edge->blocker;
				pg_write_barrier();
				pcm_x_slot_state_write(&slot->slot, PCM_XB_LIVE);
				ticket->blocker_stage_next_chunk++;
			}
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_stage_commit_exact(const PcmXBlockerSetHeaderPayload *commit,
												int32 authenticated_source_node,
												uint64 authenticated_source_session,
												PcmXMasterBlockerEntry *entries_out,
												Size entry_capacity,
												PcmXMasterBlockerSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	PcmXTicketHandle owner_handle;
	pg_crc32c actual_crc;
	Size old_head = PCM_X_INVALID_SLOT_INDEX;
	uint64 old_generation = 0;
	uint32 old_count = 0;
	uint32 partition;
	uint32 state;
	bool release_old = false;

	pcm_x_master_blocker_snapshot_clear(snapshot_out);
	if (!pcm_x_blocker_header_valid(commit) || snapshot_out == NULL || authenticated_source_node < 0
		|| authenticated_source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_source_session == 0 || (entry_capacity > 0 && entries_out == NULL))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&commit->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&commit->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &commit->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, &commit->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (!pcm_x_master_blocker_stage_metadata_valid(ticket)
			 || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view))
		result = PCM_X_QUEUE_CORRUPT;
	else {
		state = pcm_x_slot_state_read(&ticket->slot);
		if (state != PCM_XT_ACTIVE_PROBE) {
			if (state != PCM_XT_ACTIVE_TRANSFER && state != PCM_XT_COMPLETE
				&& state != PCM_XT_CANCELLED && state != PCM_XT_RETIRE_CREDIT)
				result = PCM_X_QUEUE_BAD_STATE;
			else if (!pcm_x_master_blocker_stage_is_clear(ticket)
					 || !pcm_x_master_blocker_published_source_valid(ticket))
				result = PCM_X_QUEUE_CORRUPT;
			else if (commit->set_generation != ticket->blocker_set_generation
					 || commit->nblockers != ticket->blocker_count
					 || commit->set_crc32c != ticket->blocker_set_crc32c
					 || !pcm_x_master_blocker_published_source_exact(
						 ticket, authenticated_source_node, authenticated_source_session))
				result = PCM_X_QUEUE_STALE;
			else if (ticket->graph_generation == 0)
				result = PCM_X_QUEUE_CORRUPT;
			else if (ticket->blocker_count > entry_capacity)
				result = PCM_X_QUEUE_NO_CAPACITY;
			else if (!pcm_x_blocker_chain_validate(
						 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
						 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
						 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, entries_out))
				result = PCM_X_QUEUE_CORRUPT;
			else {
				snapshot_out->ref = ticket->ref;
				snapshot_out->set_generation = ticket->blocker_set_generation;
				snapshot_out->graph_generation = ticket->graph_generation;
				snapshot_out->blocker_count = ticket->blocker_count;
				snapshot_out->set_crc32c = ticket->blocker_set_crc32c;
				result = PCM_X_QUEUE_DUPLICATE;
			}
		} else if (!pcm_x_master_probe_source_exact(ticket, authenticated_source_node,
													authenticated_source_session)
				   && !pcm_x_master_blocker_ack_replay_exact(ticket, authenticated_source_node,
															 authenticated_source_session))
			result = PCM_X_QUEUE_STALE;
		else if (ticket->blocker_stage_set_generation == 0) {
			if (commit->set_generation != ticket->blocker_set_generation
				|| commit->nblockers != ticket->blocker_count
				|| commit->set_crc32c != ticket->blocker_set_crc32c
				|| !pcm_x_master_blocker_published_source_exact(ticket, authenticated_source_node,
																authenticated_source_session))
				result = PCM_X_QUEUE_STALE;
			else if (ticket->blocker_count > entry_capacity)
				result = PCM_X_QUEUE_NO_CAPACITY;
			else if (!pcm_x_blocker_chain_validate(
						 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
						 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
						 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, entries_out))
				result = PCM_X_QUEUE_CORRUPT;
			else {
				snapshot_out->ref = ticket->ref;
				snapshot_out->set_generation = ticket->blocker_set_generation;
				snapshot_out->graph_generation = ticket->graph_generation;
				snapshot_out->blocker_count = ticket->blocker_count;
				snapshot_out->set_crc32c = ticket->blocker_set_crc32c;
				result = PCM_X_QUEUE_DUPLICATE;
			}
		} else if (commit->set_generation != ticket->blocker_stage_set_generation
				   || commit->nblockers != ticket->blocker_stage_count
				   || commit->set_crc32c != ticket->blocker_stage_crc32c
				   || authenticated_source_node != ticket->blocker_stage_source_node
				   || authenticated_source_session != ticket->blocker_stage_source_session)
			result = PCM_X_QUEUE_STALE;
		else if (ticket->blocker_stage_next_chunk != ticket->blocker_stage_count)
			result = PCM_X_QUEUE_NOT_READY;
		else if (ticket->blocker_stage_count > entry_capacity)
			result = PCM_X_QUEUE_NO_CAPACITY;
		else if (!pcm_x_blocker_stage_chain_validate(ticket, ticket_ref, entries_out, &actual_crc))
			result = PCM_X_QUEUE_CORRUPT;
		else if ((uint32)actual_crc != ticket->blocker_stage_crc32c)
			result = PCM_X_QUEUE_STALE;
		else if ((ticket->blocker_set_generation == 0
				  && (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX
					  || ticket->blocker_count != 0 || ticket->blocker_set_crc32c != 0
					  || !pcm_x_master_blocker_published_source_valid(ticket)))
				 || (ticket->blocker_set_generation != 0
					 && (!pcm_x_master_blocker_published_source_valid(ticket)
						 || !pcm_x_blocker_chain_validate(
							 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
							 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
							 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL))))
			result = PCM_X_QUEUE_CORRUPT;
		else {
			owner_handle = ticket->ref.handle;
			old_head = ticket->blocker_head_index;
			old_count = ticket->blocker_count;
			old_generation = ticket->blocker_set_generation;
			pcm_x_blocker_chain_state_write(&blocker_view, old_head, old_count, PCM_XB_DETACHING);
			ticket->blocker_head_index = ticket->blocker_stage_head_index;
			ticket->blocker_set_generation = ticket->blocker_stage_set_generation;
			ticket->blocker_count = ticket->blocker_stage_count;
			ticket->blocker_set_crc32c = ticket->blocker_stage_crc32c;
			ticket->blocker_set_source_session = ticket->blocker_stage_source_session;
			ticket->blocker_set_source_node = ticket->blocker_stage_source_node;
			ticket->graph_generation = 0;
			pcm_x_master_blocker_stage_clear(ticket);
			pg_write_barrier();
			snapshot_out->ref = ticket->ref;
			snapshot_out->set_generation = ticket->blocker_set_generation;
			snapshot_out->graph_generation = 0;
			snapshot_out->blocker_count = ticket->blocker_count;
			snapshot_out->set_crc32c = ticket->blocker_set_crc32c;
			release_old = true;
			result = PCM_X_QUEUE_OK;
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);

	if (release_old
		&& !pcm_x_blocker_chain_release_exact(old_head, old_count, &owner_handle,
											  ticket_ref.slot_index, ticket_ref.slot_generation,
											  old_generation, PCM_XB_DETACHING))
		result = PCM_X_QUEUE_CORRUPT;
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_stage_abort_exact(const PcmXTicketRef *ref, uint64 set_generation,
											   int32 authenticated_source_node,
											   uint64 authenticated_source_session,
											   uint64 expected_state_sequence)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXBlockerSlot *slot = NULL;
	PcmXSlotRef slot_ref;
	PcmXSlotRef ticket_ref;
	PcmXTicketHandle owner_handle;
	PcmXQueueResult result;
	Size staged_head = PCM_X_INVALID_SLOT_INDEX;
	uint64 staged_generation = 0;
	uint64 next_sequence;
	uint32 staged_count = 0;
	uint32 partition;
	uint32 i;
	bool release_stage = false;

	if (header == NULL || ref == NULL || set_generation == 0 || set_generation == UINT64_MAX
		|| expected_state_sequence == 0 || ref->grant_generation != 0
		|| authenticated_source_node < 0 || authenticated_source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_source_session == 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_PROBE)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_master_blocker_stage_metadata_valid(ticket))
		result = PCM_X_QUEUE_CORRUPT;
	else if (ticket->reliable.state_sequence != expected_state_sequence
			 || !pcm_x_master_probe_source_exact(ticket, authenticated_source_node,
												 authenticated_source_session)
			 || ticket->blocker_stage_set_generation != set_generation
			 || ticket->blocker_stage_source_node != authenticated_source_node
			 || ticket->blocker_stage_source_session != authenticated_source_session)
		result = PCM_X_QUEUE_STALE;
	else if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &next_sequence))
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
	else {
		for (i = 0; i < ticket->blocker_stage_count; i++) {
			if (!pcm_x_blocker_stage_nth_exact(ticket, ticket_ref, i, &slot, &slot_ref)) {
				result = PCM_X_QUEUE_CORRUPT;
				break;
			}
		}
		if (result == PCM_X_QUEUE_OK && ticket->blocker_stage_count != 0
			&& (slot == NULL || slot->next_index != PCM_X_INVALID_SLOT_INDEX))
			result = PCM_X_QUEUE_CORRUPT;
		if (result == PCM_X_QUEUE_OK) {
			staged_head = ticket->blocker_stage_head_index;
			staged_count = ticket->blocker_stage_count;
			staged_generation = ticket->blocker_stage_set_generation;
			owner_handle = ticket->ref.handle;
			if (staged_count != 0) {
				PcmXAllocatorView blocker_view;

				if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view))
					result = PCM_X_QUEUE_CORRUPT;
				else
					pcm_x_blocker_chain_state_write(&blocker_view, staged_head, staged_count,
													PCM_XB_DETACHING);
			}
			if (result == PCM_X_QUEUE_OK) {
				pcm_x_master_blocker_stage_clear(ticket);
				ticket->reliable.state_sequence = next_sequence;
				pg_write_barrier();
				release_stage = true;
			}
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);

	if (release_stage) {
		LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
		if (!pcm_x_blocker_stage_chain_release_detaching_locked(
				staged_head, staged_count, &owner_handle, ticket_ref, staged_generation))
			result = PCM_X_QUEUE_CORRUPT;
		LWLockRelease(&header->allocator_lock.lock);
	}
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Caller owns the exact tag's master-domain lock. */
static PcmXQueueResult
pcm_x_master_blocker_replace_precheck_locked(const PcmXMasterTicketSlot *ticket,
											 PcmXSlotRef ticket_ref, uint64 requested_generation,
											 uint32 requested_crc32c)
{
	PcmXAllocatorView blocker_view;
	uint64 current_generation;
	uint32 state;

	if (ticket == NULL || !pcm_x_master_blocker_stage_metadata_valid(ticket)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view))
		return PCM_X_QUEUE_CORRUPT;
	if (ticket->blocker_stage_set_generation != 0)
		return PCM_X_QUEUE_BUSY;
	current_generation = ticket->blocker_set_generation;
	if ((current_generation == 0
		 && (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX || ticket->blocker_count != 0
			 || ticket->blocker_set_crc32c != 0
			 || !pcm_x_master_blocker_published_source_valid(ticket)))
		|| (current_generation != 0
			&& (!pcm_x_blocker_chain_validate(&blocker_view, ticket->blocker_head_index,
											  ticket->blocker_count, &ticket->ref.handle,
											  ticket_ref.slot_index, ticket_ref.slot_generation,
											  current_generation, PCM_XB_LIVE, NULL, 0, NULL)
				|| !pcm_x_master_blocker_published_source_valid(ticket))))
		return PCM_X_QUEUE_CORRUPT;
	if (requested_generation == UINT64_MAX || current_generation == UINT64_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	if (requested_generation < current_generation)
		return PCM_X_QUEUE_STALE;
	if (requested_generation == current_generation)
		return requested_crc32c == ticket->blocker_set_crc32c ? PCM_X_QUEUE_DUPLICATE
															  : PCM_X_QUEUE_CORRUPT;
	if (requested_generation != current_generation + 1)
		return PCM_X_QUEUE_NOT_READY;
	state = pcm_x_slot_state_read(&ticket->slot);
	return state == PCM_XT_ACTIVE_PROBE ? PCM_X_QUEUE_OK : PCM_X_QUEUE_BAD_STATE;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_set_replace_exact(const PcmXTicketRef *ref, uint64 set_generation,
											   const ClusterLmdVertex *blockers, Size nblockers,
											   uint32 set_crc32c)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef ticket_ref;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	PcmXAllocatorResult allocator_result;
	PcmXTicketHandle owner_handle;
	Size unique_count;
	Size new_head_index = PCM_X_INVALID_SLOT_INDEX;
	Size old_head_index = PCM_X_INVALID_SLOT_INDEX;
	uint64 old_set_generation = 0;
	uint32 old_count = 0;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || ref == NULL || set_generation == 0 || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	result = pcm_x_blocker_input_validate(ref, blockers, nblockers, &unique_count);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}
	if (set_generation == UINT64_MAX) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	}
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else
		result = pcm_x_master_blocker_replace_precheck_locked(ticket, ticket_ref, set_generation,
															  set_crc32c);
	LWLockRelease(&header->master_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
			pcm_x_runtime_fail_closed();
		return result;
	}

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	allocator_result = pcm_x_blocker_chain_reserve_locked(ref, ticket_ref, set_generation, blockers,
														  nblockers, unique_count, &new_head_index);
	LWLockRelease(&header->allocator_lock.lock);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else
		result = pcm_x_master_blocker_replace_precheck_locked(ticket, ticket_ref, set_generation,
															  set_crc32c);
	if (result == PCM_X_QUEUE_OK) {
		if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			|| !pcm_x_blocker_chain_validate(&blocker_view, new_head_index, (uint32)unique_count,
											 &ref->handle, ticket_ref.slot_index,
											 ticket_ref.slot_generation, set_generation,
											 PCM_XB_RESERVED_NONVISIBLE, NULL, 0, NULL))
			result = PCM_X_QUEUE_CORRUPT;
		else {
			owner_handle = ticket->ref.handle;
			old_head_index = ticket->blocker_head_index;
			old_count = ticket->blocker_count;
			old_set_generation = ticket->blocker_set_generation;
			pcm_x_blocker_chain_state_write(&blocker_view, old_head_index, old_count,
											PCM_XB_DETACHING);
			pcm_x_blocker_chain_state_write(&blocker_view, new_head_index, (uint32)unique_count,
											PCM_XB_LIVE);
			/* A newly published blocker set has no matching WFG commit yet. */
			ticket->graph_generation = 0;
			pg_write_barrier();
			ticket->blocker_head_index = new_head_index;
			ticket->blocker_count = (uint32)unique_count;
			ticket->blocker_set_crc32c = set_crc32c;
			ticket->blocker_set_generation = set_generation;
			ticket->blocker_set_source_session = 0;
			ticket->blocker_set_source_node = -1;
			result = PCM_X_QUEUE_OK;
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);

	if (result != PCM_X_QUEUE_OK) {
		if (!pcm_x_blocker_chain_release_exact(new_head_index, (uint32)unique_count, &ref->handle,
											   ticket_ref.slot_index, ticket_ref.slot_generation,
											   set_generation, PCM_XB_RESERVED_NONVISIBLE))
			result = PCM_X_QUEUE_CORRUPT;
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED;
	} else if (!pcm_x_blocker_chain_release_exact(old_head_index, old_count, &owner_handle,
												  ticket_ref.slot_index, ticket_ref.slot_generation,
												  old_set_generation, PCM_XB_DETACHING)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static void
pcm_x_master_blocker_snapshot_clear(PcmXMasterBlockerSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	memset(snapshot, 0, sizeof(*snapshot));
}


PcmXQueueResult
cluster_pcm_x_master_blocker_snapshot_exact(const PcmXTicketRef *ref,
											PcmXMasterBlockerEntry *entries_out,
											Size entry_capacity,
											PcmXMasterBlockerSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	pcm_x_master_blocker_snapshot_clear(snapshot_out);
	if (header == NULL || ref == NULL || snapshot_out == NULL
		|| (entry_capacity > 0 && entries_out == NULL)
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (ticket->blocker_set_generation == 0)
		result = PCM_X_QUEUE_NOT_READY;
	else if (ticket->blocker_set_generation == UINT64_MAX
			 || !pcm_x_master_blocker_published_source_valid(ticket)
			 || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			 || !pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL))
		result = PCM_X_QUEUE_CORRUPT;
	else if (ticket->blocker_count > entry_capacity)
		result = PCM_X_QUEUE_NO_CAPACITY;
	else if (!pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, entries_out))
		result = PCM_X_QUEUE_CORRUPT;
	else {
		snapshot_out->ref = ticket->ref;
		snapshot_out->set_generation = ticket->blocker_set_generation;
		snapshot_out->graph_generation = ticket->graph_generation;
		snapshot_out->blocker_count = ticket->blocker_count;
		snapshot_out->set_crc32c = ticket->blocker_set_crc32c;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_snapshot_revalidate_exact(const PcmXMasterBlockerSnapshot *snapshot,
													   const PcmXMasterBlockerEntry *entries,
													   Size entry_count)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || snapshot == NULL || snapshot->set_generation == 0
		|| snapshot->set_generation == UINT64_MAX || entry_count != snapshot->blocker_count
		|| (entry_count > 0 && entries == NULL)
		|| !pcm_x_wait_identity_valid(&snapshot->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&snapshot->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&snapshot->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &snapshot->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &snapshot->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (ticket->blocker_set_generation != snapshot->set_generation
			 || ticket->graph_generation != snapshot->graph_generation)
		result = PCM_X_QUEUE_STALE;
	else if (ticket->blocker_count != snapshot->blocker_count
			 || ticket->blocker_set_crc32c != snapshot->set_crc32c
			 || !pcm_x_master_blocker_published_source_valid(ticket)
			 || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			 || !pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL))
		result = PCM_X_QUEUE_CORRUPT;
	else if (!pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, entries, entry_count, NULL))
		result = PCM_X_QUEUE_STALE;
	else
		result = PCM_X_QUEUE_OK;
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_graph_commit_exact(const PcmXMasterBlockerSnapshot *snapshot,
												const PcmXMasterBlockerEntry *entries,
												Size entry_count, uint64 graph_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || snapshot == NULL || snapshot->set_generation == 0
		|| snapshot->set_generation == UINT64_MAX || graph_generation == 0
		|| snapshot->ref.grant_generation != 0 || entry_count != snapshot->blocker_count
		|| (entry_count > 0 && entries == NULL)
		|| !pcm_x_wait_identity_valid(&snapshot->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&snapshot->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	/*
	 * The caller performs the LMD batch replace before entering here.  This
	 * lock never nests the graph lock: it only revalidates the immutable copy
	 * token and publishes the graph generation against that exact blocker set.
	 */
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&snapshot->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &snapshot->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &snapshot->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (ticket->blocker_set_generation != snapshot->set_generation)
		result = PCM_X_QUEUE_STALE;
	else if (ticket->blocker_count != snapshot->blocker_count
			 || ticket->blocker_set_crc32c != snapshot->set_crc32c
			 || !pcm_x_master_blocker_published_source_valid(ticket)
			 || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			 || !pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL))
		result = PCM_X_QUEUE_CORRUPT;
	else if (!pcm_x_blocker_chain_validate(
				 &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				 &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				 ticket->blocker_set_generation, PCM_XB_LIVE, entries, entry_count, NULL))
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_PROBE)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (ticket->graph_generation == graph_generation)
		result = PCM_X_QUEUE_DUPLICATE;
	else if (ticket->graph_generation != 0 && graph_generation < ticket->graph_generation)
		result = PCM_X_QUEUE_STALE;
	else {
		ticket->graph_generation = graph_generation;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_master_unlink_ticket_locked(PcmXMasterTagSlot *tag_slot, PcmXSlotRef tag_ref,
								  PcmXMasterTicketSlot *ticket, PcmXSlotRef ticket_ref)
{
	PcmXAllocatorView ticket_view;
	PcmXMasterTicketSlot *next = NULL;
	PcmXMasterTicketSlot *previous = NULL;
	bool active;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &ticket_view))
		return false;
	if (ticket->prev_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->head_index != ticket_ref.slot_index)
			return false;
	} else {
		if (ticket->prev_index == ticket_ref.slot_index
			|| tag_slot->head_index == ticket_ref.slot_index)
			return false;
		previous = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, ticket->prev_index);
		if (previous == NULL || previous->next_index != ticket_ref.slot_index
			|| previous->tag_slot_index != tag_ref.slot_index
			|| previous->tag_slot_generation != tag_ref.slot_generation)
			return false;
	}
	if (ticket->next_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->tail_index != ticket_ref.slot_index)
			return false;
	} else {
		if (ticket->next_index == ticket_ref.slot_index || ticket->next_index == ticket->prev_index
			|| tag_slot->tail_index == ticket_ref.slot_index)
			return false;
		next = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&ticket_view, ticket->next_index);
		if (next == NULL || next->prev_index != ticket_ref.slot_index
			|| next->tag_slot_index != tag_ref.slot_index
			|| next->tag_slot_generation != tag_ref.slot_generation)
			return false;
	}
	active = tag_slot->active_index == ticket_ref.slot_index;
	if (active && tag_slot->active_slot_generation != ticket_ref.slot_generation)
		return false;

	/* Every structural check above is read-only.  Preserve the original FIFO
	 * as recovery evidence if any backlink or active locator is corrupt. */
	if (previous == NULL)
		tag_slot->head_index = ticket->next_index;
	else
		previous->next_index = ticket->next_index;
	if (next == NULL)
		tag_slot->tail_index = ticket->prev_index;
	else
		next->prev_index = ticket->prev_index;
	if (active) {
		tag_slot->active_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->active_slot_generation = 0;
	}
	ticket->next_index = PCM_X_INVALID_SLOT_INDEX;
	ticket->prev_index = PCM_X_INVALID_SLOT_INDEX;
	return true;
}


static bool
pcm_x_transfer_leg_is_clear(const PcmXReliableLegState *leg)
{
	return leg != NULL && leg->retry_deadline_ms == 0 && leg->expected_responder_session == 0
		   && leg->retry_count == 0 && leg->expected_responder_node == 0 && leg->pending_opcode == 0
		   && leg->phase == 0 && leg->flags == 0 && leg->reserved == 0;
}


static bool
pcm_x_transfer_leg_is_pristine(const PcmXReliableLegState *leg)
{
	return pcm_x_transfer_leg_is_clear(leg) && leg->state_sequence == 0
		   && leg->response_tombstone_mask == 0 && leg->last_responder_node == 0
		   && leg->last_response_opcode == 0;
}


static bool
pcm_x_transfer_leg_exact(const PcmXReliableLegState *leg, uint16 opcode, int32 responder_node,
						 uint64 responder_session)
{
	return leg != NULL && opcode != 0 && responder_node >= 0
		   && responder_node < PCM_X_PROTOCOL_NODE_LIMIT && responder_session != 0
		   && leg->pending_opcode == opcode && leg->phase == opcode && leg->flags == 0
		   && leg->reserved == 0 && leg->expected_responder_node == responder_node
		   && leg->expected_responder_session == responder_session;
}


static bool
pcm_x_transfer_peer_exact(const PcmXMasterTicketSlot *ticket, int32 responder_node,
						  uint64 responder_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	const PcmXPeerFrontier *frontier;

	if (header == NULL || ticket == NULL || responder_node < 0
		|| responder_node >= PCM_X_PROTOCOL_NODE_LIMIT || responder_session == 0)
		return false;
	frontier = &header->peer_frontiers[responder_node];
	return frontier->cluster_epoch == ticket->ref.identity.cluster_epoch
		   && frontier->sender_session_incarnation == responder_session;
}


static void
pcm_x_transfer_leg_clear(PcmXReliableLegState *leg, uint16 response_opcode,
						 int32 authenticated_node)
{
	leg->retry_deadline_ms = 0;
	leg->expected_responder_session = 0;
	leg->retry_count = 0;
	leg->last_responder_node = (uint32)authenticated_node;
	leg->expected_responder_node = 0;
	leg->pending_opcode = 0;
	leg->last_response_opcode = response_opcode;
	leg->phase = 0;
	leg->flags = 0;
	leg->reserved = 0;
}


static PcmXQueueResult
pcm_x_transfer_leg_arm_locked(PcmXMasterTicketSlot *ticket, uint16 opcode, int32 responder_node,
							  uint64 responder_session)
{
	uint64 next_sequence;

	if (ticket == NULL || opcode == 0
		|| !pcm_x_transfer_peer_exact(ticket, responder_node, responder_session))
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_transfer_leg_is_clear(&ticket->reliable))
		return pcm_x_transfer_leg_exact(&ticket->reliable, opcode, responder_node,
										responder_session)
				   ? PCM_X_QUEUE_DUPLICATE
				   : PCM_X_QUEUE_BUSY;
	if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &next_sequence))
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	ticket->reliable.state_sequence = next_sequence;
	ticket->reliable.retry_deadline_ms = 0;
	ticket->reliable.expected_responder_session = responder_session;
	ticket->reliable.retry_count = 0;
	ticket->reliable.expected_responder_node = responder_node;
	ticket->reliable.pending_opcode = opcode;
	ticket->reliable.phase = opcode;
	ticket->reliable.flags = 0;
	ticket->reliable.reserved = 0;
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
pcm_x_transfer_leg_advance_locked(PcmXMasterTicketSlot *ticket, uint16 expected_opcode,
								  int32 authenticated_node, uint64 authenticated_session,
								  uint16 response_opcode, uint16 next_opcode, int32 next_node,
								  uint64 next_session)
{
	uint64 next_sequence = 0;

	if (ticket == NULL
		|| !pcm_x_transfer_leg_exact(&ticket->reliable, expected_opcode, authenticated_node,
									 authenticated_session))
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_transfer_peer_exact(ticket, authenticated_node, authenticated_session))
		return PCM_X_QUEUE_STALE;
	if (next_opcode != 0) {
		if (!pcm_x_transfer_peer_exact(ticket, next_node, next_session))
			return PCM_X_QUEUE_STALE;
		if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &next_sequence))
			return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	}
	pcm_x_transfer_leg_clear(&ticket->reliable, response_opcode, authenticated_node);
	if (next_opcode != 0) {
		ticket->reliable.state_sequence = next_sequence;
		ticket->reliable.expected_responder_session = next_session;
		ticket->reliable.expected_responder_node = next_node;
		ticket->reliable.pending_opcode = next_opcode;
		ticket->reliable.phase = next_opcode;
	}
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
pcm_x_image_id_mint(uint64 *image_id_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXQueueResult result;
	uint64 raw_sequence;

	if (image_id_out != NULL)
		*image_id_out = 0;
	if (header == NULL || image_id_out == NULL)
		return PCM_X_QUEUE_INVALID;
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	raw_sequence = header->next_image_id;
	if (raw_sequence == 0 || raw_sequence > PCM_X_IMAGE_ID_SEQ_MASK)
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
	else if (!cluster_pcm_x_image_id_encode(cluster_node_id, raw_sequence, image_id_out))
		result = PCM_X_QUEUE_CORRUPT;
	else {
		header->next_image_id = raw_sequence + 1;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->allocator_lock.lock);
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_blocker_probe_complete_exact(const PcmXTicketRef *ref, uint64 set_generation,
												  int32 authenticated_source_node,
												  uint64 authenticated_source_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || ref == NULL || set_generation == 0 || set_generation == UINT64_MAX
		|| ref->grant_generation != 0 || authenticated_source_node < 0
		|| authenticated_source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_source_session == 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_PROBE)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_master_blocker_stage_metadata_valid(ticket)
			 || !pcm_x_master_blocker_stage_is_clear(ticket))
		result = PCM_X_QUEUE_CORRUPT;
	else if (ticket->blocker_set_generation != set_generation || ticket->graph_generation == 0
			 || !pcm_x_master_blocker_published_source_exact(ticket, authenticated_source_node,
															 authenticated_source_session))
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_transfer_leg_is_clear(&ticket->reliable))
		result = ticket->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_NOT_READY;
	else if (!pcm_x_master_probe_source_exact(ticket, authenticated_source_node,
											  authenticated_source_session))
		result = PCM_X_QUEUE_STALE;
	else {
		pcm_x_transfer_leg_clear(&ticket->reliable, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT,
								 authenticated_source_node);
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_begin_transfer_exact(const PcmXTicketRef *ref, uint64 graph_generation,
										  PcmXTicketRef *transfer_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint64 grant_generation = 0;
	uint32 partition;
	uint32 state;

	if (transfer_out != NULL)
		memset(transfer_out, 0, sizeof(*transfer_out));
	if (header == NULL || ref == NULL || transfer_out == NULL || graph_generation == 0
		|| ref->grant_generation != 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref)
		|| tag_slot->active_index != ticket_ref.slot_index
		|| tag_slot->active_slot_generation != ticket_ref.slot_generation) {
		result = PCM_X_QUEUE_STALE;
		goto begin_transfer_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto begin_transfer_done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	result = pcm_x_master_pending_x_claim_required(ticket);
	if (result != PCM_X_QUEUE_OK)
		goto begin_transfer_done;
	if (state == PCM_XT_ACTIVE_TRANSFER) {
		if (ticket->graph_generation == graph_generation && ticket->ref.grant_generation != 0) {
			*transfer_out = ticket->ref;
			result = PCM_X_QUEUE_DUPLICATE;
		} else
			result = PCM_X_QUEUE_STALE;
		goto begin_transfer_done;
	}
	if (state != PCM_XT_ACTIVE_PROBE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto begin_transfer_done;
	}
	if (!pcm_x_master_blocker_stage_metadata_valid(ticket)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto begin_transfer_done;
	}
	if (ticket->blocker_stage_set_generation != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto begin_transfer_done;
	}
	if (ticket->blocker_set_generation == 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto begin_transfer_done;
	}
	{
		PcmXAllocatorView blocker_view;

		if (ticket->blocker_set_generation == UINT64_MAX
			|| !pcm_x_master_blocker_published_source_valid(ticket)
			|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			|| !pcm_x_blocker_chain_validate(
				&blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				&ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto begin_transfer_done;
		}
	}
	if (ticket->blocker_count != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto begin_transfer_done;
	}
	if (ticket->graph_generation != graph_generation || ticket->ref.grant_generation != 0) {
		result = PCM_X_QUEUE_STALE;
		goto begin_transfer_done;
	}
	if (!pcm_x_transfer_leg_is_clear(&ticket->reliable)
		|| ticket->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto begin_transfer_done;
	}
	/* grant_generation belongs to the durable master ticket, not to an outer
	 * caller.  Mint it from the ticket's checked reliable sequence while the
	 * exact tag partition is locked, then persist the same value in both the
	 * full wire reference and the sequence that fences every later leg. */
	if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &grant_generation)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		goto begin_transfer_done;
	}
	ticket->reliable.state_sequence = grant_generation;
	ticket->ref.grant_generation = grant_generation;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_ACTIVE_TRANSFER);
	*transfer_out = ticket->ref;
	pcm_x_stats_increment(&header->stats.transfer_count);
	result = PCM_X_QUEUE_OK;

begin_transfer_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_revoke_arm_exact(const PcmXTicketRef *ref, int32 responder_node,
									  uint64 responder_session, PcmXRevokePayload *revoke_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint64 image_id = 0;
	uint32 partition;
	bool mint_image = false;

	if (revoke_out != NULL)
		memset(revoke_out, 0, sizeof(*revoke_out));
	if (header == NULL || ref == NULL || revoke_out == NULL || ref->grant_generation == 0
		|| responder_node < 0 || responder_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| responder_session == 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (ticket->blocker_count != 0 || ticket->graph_generation == 0
			 || !pcm_x_master_blocker_stage_is_clear(ticket))
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_transfer_leg_is_clear(&ticket->reliable)) {
		if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_REVOKE, responder_node,
									 responder_session)
			&& ticket->image.image_id != 0 && ticket->image.source_node == (uint32)responder_node) {
			/* Diagnostic: the transfer driver re-arms this exact REVOKE leg on
			 * every pass; count it so leg_retry exposes a live retry pump. */
			if (ticket->reliable.retry_count != PG_UINT32_MAX)
				ticket->reliable.retry_count++;
			result = PCM_X_QUEUE_DUPLICATE;
		} else
			result = PCM_X_QUEUE_BUSY;
	} else if (ticket->image.image_id != 0) {
		result = ticket->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_IMAGE_READY
					 ? PCM_X_QUEUE_NOT_READY
					 : PCM_X_QUEUE_CORRUPT;
	} else if (!pcm_x_transfer_peer_exact(ticket, responder_node, responder_session))
		result = PCM_X_QUEUE_STALE;
	else {
		result = PCM_X_QUEUE_OK;
		mint_image = true;
	}
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		if (!mint_image) {
			revoke_out->ref = ticket->ref;
			revoke_out->image_id = ticket->image.image_id;
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (!mint_image)
		goto revoke_done;

	result = pcm_x_image_id_mint(&image_id);
	if (result != PCM_X_QUEUE_OK)
		goto revoke_done;
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (ticket->blocker_count != 0 || ticket->graph_generation == 0
			 || !pcm_x_master_blocker_stage_is_clear(ticket))
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_transfer_leg_is_clear(&ticket->reliable)) {
		if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_REVOKE, responder_node,
									 responder_session)
			&& ticket->image.image_id != 0 && ticket->image.source_node == (uint32)responder_node) {
			/* Diagnostic: the transfer driver re-arms this exact REVOKE leg on
			 * every pass; count it so leg_retry exposes a live retry pump. */
			if (ticket->reliable.retry_count != PG_UINT32_MAX)
				ticket->reliable.retry_count++;
			result = PCM_X_QUEUE_DUPLICATE;
		} else
			result = PCM_X_QUEUE_BUSY;
	} else if (ticket->image.image_id != 0)
		result = PCM_X_QUEUE_CORRUPT;
	else {
		result = pcm_x_transfer_leg_arm_locked(ticket, PGRAC_IC_MSG_PCM_X_REVOKE, responder_node,
											   responder_session);
		if (result == PCM_X_QUEUE_OK) {
			ticket->image.image_id = image_id;
			ticket->image.source_node = (uint32)responder_node;
			ticket->involved_nodes_bitmap |= UINT32_C(1) << responder_node;
			pg_write_barrier();
		}
	}
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		revoke_out->ref = ticket->ref;
		revoke_out->image_id = ticket->image.image_id;
	}
	LWLockRelease(&header->master_locks[partition].lock);

revoke_done:
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_image_ready_exact(const PcmXGrantPayload *image_ready,
									   int32 authenticated_node, uint64 authenticated_session,
									   PcmXGrantPayload *prepare_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint64 requester_session;
	int32 requester_node;
	uint32 partition;

	if (prepare_out != NULL)
		memset(prepare_out, 0, sizeof(*prepare_out));
	if (header == NULL || image_ready == NULL || prepare_out == NULL
		|| image_ready->ref.grant_generation == 0 || !pcm_x_image_token_valid(&image_ready->image)
		|| !pcm_x_image_id_master_exact(image_ready->image.image_id, cluster_node_id)
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0
		|| image_ready->image.source_node != (uint32)authenticated_node
		|| !pcm_x_wait_identity_valid(&image_ready->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&image_ready->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&image_ready->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &image_ready->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	requester_node = image_ready->ref.identity.node_id;
	/* Admission fixed the requester session in the prehandle.  A frontier
	 * change cannot retarget an already admitted ticket to a new backend. */
	requester_session = ticket == NULL ? 0 : ticket->prehandle.sender_session_incarnation;
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &image_ready->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (ticket->image.image_id != image_ready->image.image_id
			 || ticket->image.source_node != image_ready->image.source_node)
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT,
									  requester_node, requester_session))
		result = pcm_x_image_token_equal(&ticket->image, &image_ready->image)
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_STALE;
	else if (!pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_REVOKE,
									   authenticated_node, authenticated_session))
		result = PCM_X_QUEUE_STALE;
	else {
		result = pcm_x_transfer_leg_advance_locked(
			ticket, PGRAC_IC_MSG_PCM_X_REVOKE, authenticated_node, authenticated_session,
			PGRAC_IC_MSG_PCM_X_IMAGE_READY, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT, requester_node,
			requester_session);
		if (result == PCM_X_QUEUE_OK) {
			ticket->image = image_ready->image;
			ticket->involved_nodes_bitmap |= UINT32_C(1) << requester_node;
			pg_write_barrier();
		}
	}
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		prepare_out->ref = ticket->ref;
		prepare_out->image = ticket->image;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


/* The exact commit proof runs against the effective grant base: the one-shot
 * INSTALL_READY rebase when an interleaved revoke consumed the enqueue-time
 * base on the requester node, the immutable identity base otherwise. */
static inline uint64
pcm_x_master_ticket_effective_grant_base(const PcmXMasterTicketSlot *ticket)
{
	return ticket->grant_base_own_generation != 0 ? ticket->grant_base_own_generation
												  : ticket->ref.identity.base_own_generation;
}


PcmXQueueResult
cluster_pcm_x_master_install_ready_exact(const PcmXInstallReadyPayload *install_ready,
										 uint64 rebased_own_generation, int32 authenticated_node,
										 uint64 authenticated_session, PcmXPhasePayload *commit_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (commit_out != NULL)
		memset(commit_out, 0, sizeof(*commit_out));
	if (header == NULL || install_ready == NULL || commit_out == NULL
		|| install_ready->ref.grant_generation == 0 || install_ready->image_id == 0
		|| install_ready->result != PCM_X_QUEUE_OK
		|| install_ready->phase != PGRAC_IC_MSG_PCM_X_INSTALL_READY || install_ready->flags != 0
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0 || install_ready->ref.identity.node_id != authenticated_node
		|| !pcm_x_wait_identity_valid(&install_ready->ref.identity)
		|| rebased_own_generation == UINT64_MAX)
		return PCM_X_QUEUE_INVALID;
	/* A nonzero rebase must name a strictly newer effective grant base than
	 * the enqueue-time identity base it supersedes. */
	if (rebased_own_generation != 0
		&& rebased_own_generation <= install_ready->ref.identity.base_own_generation)
		return PCM_X_QUEUE_STALE;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&install_ready->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&install_ready->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &install_ready->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &install_ready->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (ticket->image.image_id != install_ready->image_id)
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_COMMIT_X,
									  authenticated_node, authenticated_session))
		/* Replay after COMMIT_X armed: the published grant base is frozen; a
		 * different value is evidence divergence, never contention. */
		result = ticket->grant_base_own_generation == rebased_own_generation ? PCM_X_QUEUE_DUPLICATE
																			 : PCM_X_QUEUE_CORRUPT;
	else {
		result = pcm_x_transfer_leg_advance_locked(
			ticket, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT, authenticated_node, authenticated_session,
			PGRAC_IC_MSG_PCM_X_INSTALL_READY, PGRAC_IC_MSG_PCM_X_COMMIT_X, authenticated_node,
			authenticated_session);
		if (result == PCM_X_QUEUE_OK && rebased_own_generation != 0) {
			/* One-shot publish, atomic with arming COMMIT_X under this lock:
			 * a committed COMMIT_X leg implies the grant base is persisted. */
			ticket->grant_base_own_generation = rebased_own_generation;
			pg_write_barrier();
		}
	}
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		commit_out->ref = ticket->ref;
		commit_out->phase = PGRAC_IC_MSG_PCM_X_COMMIT_X;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_final_ack_prepare_exact(const PcmXFinalAckPayload *final_ack,
											 int32 authenticated_node, uint64 authenticated_session,
											 PcmXMasterFinalAckToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult claim_result;
	PcmXQueueResult result;
	uint64 expected_generation;
	uint32 partition;

	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (header == NULL || final_ack == NULL || token_out == NULL
		|| final_ack->ref.grant_generation == 0 || final_ack->image_id == 0
		|| final_ack->committed_own_generation == 0 || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| final_ack->ref.identity.node_id != authenticated_node
		|| !pcm_x_wait_identity_valid(&final_ack->ref.identity))
		return PCM_X_QUEUE_INVALID;
	/* Monotonic ingress floor only: the exact "+1" proof runs under the ticket
	 * lock against the effective grant base, which a published INSTALL_READY
	 * rebase may have moved past the enqueue-time identity base. */
	if (!cluster_pcm_x_generation_next(final_ack->ref.identity.base_own_generation,
									   &expected_generation))
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	if (final_ack->committed_own_generation < expected_generation)
		return PCM_X_QUEUE_STALE;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&final_ack->ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&final_ack->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &final_ack->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &final_ack->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if ((claim_result = pcm_x_master_pending_x_claim_required(ticket)) != PCM_X_QUEUE_OK)
		result = claim_result;
	else if (ticket->image.image_id != final_ack->image_id
			 || !pcm_x_image_token_valid(&ticket->image))
		result = PCM_X_QUEUE_STALE;
	else if (!cluster_pcm_x_generation_next(pcm_x_master_ticket_effective_grant_base(ticket),
											&expected_generation)
			 || final_ack->committed_own_generation != expected_generation)
		/* FINAL_ACK proves exactly one ownership round under this grant. */
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
									  authenticated_node, authenticated_session))
		result = pcm_x_transfer_peer_exact(ticket, authenticated_node, authenticated_session)
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_STALE;
	else if (!pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_COMMIT_X,
									   authenticated_node, authenticated_session)
			 || !pcm_x_transfer_peer_exact(ticket, authenticated_node, authenticated_session))
		result = PCM_X_QUEUE_STALE;
	else
		result = PCM_X_QUEUE_OK;
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		token_out->final_ack = *final_ack;
		token_out->image = ticket->image;
		token_out->master_session_incarnation = ticket->master_session_incarnation;
		token_out->state_sequence = ticket->reliable.state_sequence;
		token_out->authenticated_session = authenticated_session;
		token_out->authenticated_node = authenticated_node;
		token_out->reserved = 0;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_final_ack_finalize_exact(const PcmXMasterFinalAckToken *token,
											  PcmXPhasePayload *final_commit_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult claim_result;
	PcmXQueueResult result;
	uint64 expected_generation;
	uint64 next_state_sequence = 0;
	uint32 partition;

	if (final_commit_out != NULL)
		memset(final_commit_out, 0, sizeof(*final_commit_out));
	if (header == NULL || token == NULL || final_commit_out == NULL || token->reserved != 0
		|| token->master_session_incarnation == 0 || token->state_sequence == 0
		|| token->authenticated_node < 0 || token->authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| token->authenticated_session == 0 || token->final_ack.ref.grant_generation == 0
		|| token->final_ack.image_id == 0 || token->final_ack.committed_own_generation == 0
		|| token->final_ack.ref.identity.node_id != token->authenticated_node
		|| !pcm_x_wait_identity_valid(&token->final_ack.ref.identity)
		|| !pcm_x_image_token_valid(&token->image)
		|| token->image.image_id != token->final_ack.image_id)
		return PCM_X_QUEUE_INVALID;
	/* Monotonic floor only; the exact "+1" proof re-runs under the ticket lock
	 * against the effective grant base. */
	if (!cluster_pcm_x_generation_next(token->final_ack.ref.identity.base_own_generation,
									   &expected_generation))
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	if (token->final_ack.committed_own_generation < expected_generation)
		return PCM_X_QUEUE_STALE;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0
		|| !pcm_x_runtime_token_exact(&runtime, token->master_session_incarnation))
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&token->final_ack.ref, &ticket_ref, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&token->final_ack.ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &token->final_ack.ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &token->final_ack.ref))
		result = PCM_X_QUEUE_STALE;
	else if (ticket->master_session_incarnation != token->master_session_incarnation
			 || !pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if ((claim_result = pcm_x_master_pending_x_claim_required(ticket)) != PCM_X_QUEUE_OK)
		result = claim_result;
	else if (!pcm_x_image_token_equal(&ticket->image, &token->image)
			 || ticket->image.image_id != token->final_ack.image_id
			 || !pcm_x_transfer_peer_exact(ticket, token->authenticated_node,
										   token->authenticated_session))
		result = PCM_X_QUEUE_STALE;
	else if (!cluster_pcm_x_generation_next(pcm_x_master_ticket_effective_grant_base(ticket),
											&expected_generation)
			 || token->final_ack.committed_own_generation != expected_generation)
		result = PCM_X_QUEUE_STALE;
	else if (pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
									  token->authenticated_node, token->authenticated_session)) {
		if (ticket->reliable.state_sequence == token->state_sequence)
			result = PCM_X_QUEUE_DUPLICATE;
		else if (cluster_pcm_x_generation_next(token->state_sequence, &next_state_sequence)
				 && ticket->reliable.state_sequence == next_state_sequence)
			result = PCM_X_QUEUE_DUPLICATE;
		else
			result = PCM_X_QUEUE_STALE;
	} else if (ticket->reliable.state_sequence != token->state_sequence
			   || !pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_COMMIT_X,
											token->authenticated_node,
											token->authenticated_session))
		result = PCM_X_QUEUE_STALE;
	else
		result = pcm_x_transfer_leg_advance_locked(
			ticket, PGRAC_IC_MSG_PCM_X_COMMIT_X, token->authenticated_node,
			token->authenticated_session, PGRAC_IC_MSG_PCM_X_FINAL_ACK,
			PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK, token->authenticated_node,
			token->authenticated_session);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		final_commit_out->ref = ticket->ref;
		final_commit_out->phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_final_ack_exact(const PcmXFinalAckPayload *final_ack, int32 authenticated_node,
									 uint64 authenticated_session,
									 PcmXPhasePayload *final_commit_out)
{
	PcmXMasterFinalAckToken token;
	PcmXQueueResult result;

	if (final_commit_out != NULL)
		memset(final_commit_out, 0, sizeof(*final_commit_out));
	if (final_commit_out == NULL)
		return PCM_X_QUEUE_INVALID;
	result = cluster_pcm_x_master_final_ack_prepare_exact(final_ack, authenticated_node,
														  authenticated_session, &token);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	return cluster_pcm_x_master_final_ack_finalize_exact(&token, final_commit_out);
}


PcmXQueueResult
cluster_pcm_x_master_final_confirm_exact(const PcmXPhasePayload *final_confirm,
										 int32 authenticated_node, uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 flags;
	uint32 node_bit;
	uint32 partition;

	if (header == NULL || final_confirm == NULL || final_confirm->ref.grant_generation == 0
		|| final_confirm->reason != 0 || final_confirm->phase != PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM
		|| final_confirm->flags != 0 || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| final_confirm->ref.identity.node_id != authenticated_node
		|| !pcm_x_wait_identity_valid(&final_confirm->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	node_bit = UINT32_C(1) << final_confirm->ref.identity.node_id;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&final_confirm->ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&final_confirm->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TAG, tag_ref,
													  &final_confirm->ref.identity.tag,
													  PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &final_confirm->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL
		|| !pcm_x_ticket_ref_equal(&ticket->ref, &final_confirm->ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (((flags = pcm_x_slot_flags_read(&ticket->slot))
			  & ~PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED)
			 != 0)
		result = PCM_X_QUEUE_CORRUPT;
	else if (pcm_x_slot_state_read(&ticket->slot) == PCM_XT_COMPLETE
			 && pcm_x_transfer_leg_is_clear(&ticket->reliable)
			 && ticket->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM)
		result = PCM_X_QUEUE_DUPLICATE;
	else if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_ACTIVE_TRANSFER)
		result = PCM_X_QUEUE_BAD_STATE;
	else if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) == 0)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_transfer_leg_exact(&ticket->reliable, PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
									   authenticated_node, authenticated_session)
			 || !pcm_x_transfer_peer_exact(ticket, authenticated_node, authenticated_session))
		result = PCM_X_QUEUE_STALE;
	else if (tag_slot->active_index != ticket_ref.slot_index
			 || tag_slot->active_slot_generation != ticket_ref.slot_generation
			 || tag_slot->head_index != ticket_ref.slot_index
			 || ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
			 || (pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & node_bit) == 0)
		result = PCM_X_QUEUE_CORRUPT;
	else if (tag_slot->queue_state_sequence == UINT64_MAX)
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
	else if (!pcm_x_master_unlink_ticket_locked(tag_slot, tag_ref, ticket, ticket_ref))
		result = PCM_X_QUEUE_CORRUPT;
	else {
		pcm_x_transfer_leg_clear(&ticket->reliable, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM,
								 authenticated_node);
		tag_slot->queue_state_sequence++;
		(void)pg_atomic_fetch_and_u32(&tag_slot->queued_node_bitmap, ~node_bit);
		ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
		if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0)
			pcm_x_slot_flags_write(&ticket->slot, 0);
		pg_write_barrier();
		pcm_x_slot_state_write(&ticket->slot, PCM_XT_COMPLETE);
		pcm_x_stats_increment(&header->stats.complete_count);
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_complete_exact(const PcmXTicketRef *ref)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 node_bit;
	uint32 partition;
	uint32 state;
	uint32 flags;

	if (header == NULL || ref == NULL || ref->grant_generation == 0
		|| !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	node_bit = UINT32_C(1) << ref->identity.node_id;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)) {
		result = PCM_X_QUEUE_STALE;
		goto complete_done;
	}
	if (runtime.state != PCM_X_RUNTIME_ACTIVE
		|| !pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto complete_done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	flags = pcm_x_slot_flags_read(&ticket->slot);
	if ((flags & ~PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto complete_done;
	}
	/* This legacy completion primitive has no GRD handoff token.  Once the
	 * ticket owns pending-X it must complete through FINAL confirmation. */
	if ((flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto complete_done;
	}
	if (pcm_x_master_event_replay_exact((PcmXMasterTicketState)state, PCM_X_EVENT_COMMIT_COMPLETE,
										ticket->reliable.response_tombstone_mask)) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto complete_done;
	}
	if (pcm_x_master_event_successor_state((PcmXMasterTicketState)state,
										   PCM_X_EVENT_COMMIT_COMPLETE)) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto complete_done;
	}
	if (state != PCM_XT_ACTIVE_TRANSFER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto complete_done;
	}
	/*
	 * RECOVERY_BLOCKED retains ACTIVE_TRANSFER evidence.  grant_generation is
	 * assigned before FINAL_ACK and is therefore not positive completion proof.
	 */
	if (tag_slot->active_index != ticket_ref.slot_index
		|| tag_slot->active_slot_generation != ticket_ref.slot_generation
		|| tag_slot->head_index != ticket_ref.slot_index
		|| ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| (pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & node_bit) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto complete_done;
	}
	if (tag_slot->queue_state_sequence == UINT64_MAX) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		goto complete_done;
	}
	if (!pcm_x_master_unlink_ticket_locked(tag_slot, tag_ref, ticket, ticket_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto complete_done;
	}
	tag_slot->queue_state_sequence++;
	(void)pg_atomic_fetch_and_u32(&tag_slot->queued_node_bitmap, ~node_bit);
	ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_COMPLETE);
	pcm_x_stats_increment(&header->stats.complete_count);
	result = PCM_X_QUEUE_OK;

complete_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_master_cancel_exact_impl(const PcmXTicketRef *ref, bool reversible_only,
							   bool pending_x_release, PcmXMasterPendingXReleaseToken *release_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	uint32 node_bit;
	uint32 partition;
	uint32 state;
	uint32 flags;
	uint64 release_sequence = 0;
	bool pending_x_claimed = false;
	bool recovery_blocked = false;

	if (release_out != NULL)
		memset(release_out, 0, sizeof(*release_out));
	if (header == NULL || ref == NULL || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity) || (pending_x_release && release_out == NULL)
		|| (!pending_x_release && release_out != NULL))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	node_bit = UINT32_C(1) << ref->identity.node_id;
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	result = pcm_x_master_admission_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result != PCM_X_QUEUE_OK) {
		LWLockRelease(&header->allocator_lock.lock);
		return result;
	}
	tag_ref.slot_index = ticket->tag_slot_index;
	tag_ref.slot_generation = ticket->tag_slot_generation;
	LWLockRelease(&header->allocator_lock.lock);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_admission_ref_equal(&ticket->ref, ref)
		|| pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE) {
		result = PCM_X_QUEUE_STALE;
		goto cancel_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto cancel_done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	flags = pcm_x_slot_flags_read(&ticket->slot);
	if (!pcm_x_master_pending_x_flags_valid(flags)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	pending_x_claimed = (flags & PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) != 0;
	if (pending_x_release && state == PCM_XT_CANCELLED
		&& flags
			   == (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
				   | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING)
		&& ticket->ref.grant_generation == 0
		&& pcm_x_master_event_replay_exact((PcmXMasterTicketState)state, PCM_X_EVENT_CANCEL_EXACT,
										   ticket->reliable.response_tombstone_mask)
		&& pcm_x_transfer_leg_is_clear(&ticket->reliable)) {
		PcmXAllocatorView blocker_view;

		/* External WFG/GRD release is irreversible.  A replay token is safe only
		 * when the tombstone is already structurally detached and no partial
		 * blocker stage can still publish under it. */
		if (ticket->reliable.state_sequence == 0 || ticket->graph_generation == 0
			|| !pcm_x_master_blocker_stage_metadata_valid(ticket)
			|| !pcm_x_master_blocker_stage_is_clear(ticket)
			|| !pcm_x_master_blocker_published_source_valid(ticket)
			|| (ticket->blocker_set_generation == 0
				&& (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX
					|| ticket->blocker_count != 0 || ticket->blocker_set_crc32c != 0))
			|| (ticket->blocker_set_generation != 0
				&& (ticket->blocker_set_generation == UINT64_MAX
					|| ticket->reliable.last_response_opcode
						   != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT
					|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
					|| !pcm_x_blocker_chain_validate(
						&blocker_view, ticket->blocker_head_index, ticket->blocker_count,
						&ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
						ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL)))
			|| ticket->next_index != PCM_X_INVALID_SLOT_INDEX
			|| ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->active_index == ticket_ref.slot_index
			|| tag_slot->head_index == ticket_ref.slot_index
			|| tag_slot->tail_index == ticket_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			goto cancel_done;
		}
		release_out->ref = ticket->ref;
		release_out->master_session_incarnation = ticket->master_session_incarnation;
		release_out->state_sequence = ticket->reliable.state_sequence;
		result = PCM_X_QUEUE_DUPLICATE;
		goto cancel_done;
	}
	if (pending_x_release && flags != 0
		&& (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED
			|| state == PCM_XT_RETIRE_CREDIT)) {
		/* A terminal ticket with pending external authority must have matched
		 * the complete replay proof above.  Never downgrade a torn token/leg to
		 * an ordinary duplicate, which would skip GRD release forever. */
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	if (state == PCM_XT_ACTIVE_TRANSFER) {
		if (ticket->ref.grant_generation == 0 || flags != PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED) {
			result = PCM_X_QUEUE_CORRUPT;
			goto cancel_done;
		}
		/* A cancellation that loses the single master-lock classification race
		 * to ACTIVE_TRANSFER has crossed the irreversible grant boundary.  The
		 * FINAL protocol owns completion; a late CANCEL is retryable, not global
		 * corruption. */
		if (pending_x_release) {
			result = PCM_X_QUEUE_BUSY;
			goto cancel_done;
		}
		pg_write_barrier();
		pcm_x_slot_state_write(&ticket->slot, PCM_XT_RECOVERY_BLOCKED);
		recovery_blocked = true;
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	/* Cancellation cannot silently discard the ticket-exact proof for an
	 * externally retained GRD barrier.  Such an ACTIVE_PROBE is driven to
	 * FINAL or released through the explicit two-phase gate choreography. */
	if (pending_x_release && pending_x_claimed) {
		if (state != PCM_XT_ACTIVE_PROBE
			|| (flags != PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
				&& flags
					   != (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
						   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED))) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto cancel_done;
		}
	} else if (!pending_x_release && pending_x_claimed) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	if (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED || state == PCM_XT_RETIRE_CREDIT) {
		if (ticket->ref.grant_generation == 0
			&& pcm_x_master_event_replay_exact((PcmXMasterTicketState)state,
											   PCM_X_EVENT_CANCEL_EXACT,
											   ticket->reliable.response_tombstone_mask))
			result = PCM_X_QUEUE_DUPLICATE;
		else
			result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	if (ticket->ref.grant_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	/* Once promotion has exposed ACTIVE_PROBE, wire messages 57-60 defer to the
	 * blocker/transfer exit protocol.  The legacy internal primitive retains
	 * its explicit ACTIVE_PROBE cleanup authority for existing recovery tests. */
	if (reversible_only && !pending_x_release && state == PCM_XT_ACTIVE_PROBE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	if (state != PCM_XT_ADMITTING && state != PCM_XT_QUEUED && state != PCM_XT_ACTIVE_PROBE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	if (!pcm_x_master_blocker_stage_metadata_valid(ticket)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	if (ticket->blocker_stage_set_generation != 0) {
		if (pending_x_release && pending_x_claimed) {
			pcm_x_slot_flags_write(&ticket->slot,
								   PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
									   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED);
			result = PCM_X_QUEUE_NOT_READY;
		} else
			result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_done;
	}
	if ((state == PCM_XT_ACTIVE_PROBE
		 && (tag_slot->active_index != ticket_ref.slot_index
			 || tag_slot->active_slot_generation != ticket_ref.slot_generation))
		|| (state != PCM_XT_ACTIVE_PROBE && tag_slot->active_index == ticket_ref.slot_index)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	if (state == PCM_XT_ACTIVE_PROBE) {
		PcmXAllocatorView blocker_view;

		if (!pending_x_release
			&& (ticket->blocker_set_generation == 0 || ticket->graph_generation == 0)) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto cancel_done;
		}
		if (ticket->blocker_set_generation == 0) {
			if (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX || ticket->blocker_count != 0
				|| ticket->blocker_set_crc32c != 0
				|| !pcm_x_master_blocker_published_source_valid(ticket)) {
				result = PCM_X_QUEUE_CORRUPT;
				goto cancel_done;
			}
		} else if (ticket->blocker_set_generation == UINT64_MAX
				   || !pcm_x_master_blocker_published_source_valid(ticket)
				   || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
				   || !pcm_x_blocker_chain_validate(
					   &blocker_view, ticket->blocker_head_index, ticket->blocker_count,
					   &ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
					   ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto cancel_done;
		}
		if (!pending_x_release && ticket->blocker_count != 0) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto cancel_done;
		}
		if (pending_x_release && pending_x_claimed
			&& (ticket->graph_generation == 0 || !pcm_x_transfer_leg_is_clear(&ticket->reliable)
				|| (ticket->blocker_set_generation != 0
					&& ticket->reliable.last_response_opcode
						   != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT))) {
			pcm_x_slot_flags_write(&ticket->slot,
								   PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
									   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED);
			result = PCM_X_QUEUE_NOT_READY;
			goto cancel_done;
		}
	}
	if (tag_slot->queue_state_sequence == UINT64_MAX) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		goto cancel_done;
	}
	if ((pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & node_bit) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	if (pending_x_release && pending_x_claimed
		&& !cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &release_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		goto cancel_done;
	}
	if (!pcm_x_master_unlink_ticket_locked(tag_slot, tag_ref, ticket, ticket_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	if (state == PCM_XT_QUEUED && !pcm_x_stats_depth_decrement(header)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_done;
	}
	tag_slot->queue_state_sequence++;
	(void)pg_atomic_fetch_and_u32(&tag_slot->queued_node_bitmap, ~node_bit);
	if (pending_x_release && pending_x_claimed)
		ticket->reliable.state_sequence = release_sequence;
	ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_CANCEL;
	if (pending_x_release && pending_x_claimed)
		pcm_x_slot_flags_write(&ticket->slot,
							   PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
								   | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_CANCELLED);
	pcm_x_stats_increment(&header->stats.cancel_count);
	if (pending_x_release && pending_x_claimed) {
		release_out->ref = ticket->ref;
		release_out->master_session_incarnation = ticket->master_session_incarnation;
		release_out->state_sequence = ticket->reliable.state_sequence;
	}
	result = PCM_X_QUEUE_OK;

cancel_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED
			|| recovery_blocked)
			pcm_x_runtime_fail_closed();
		return result;
	}
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_master_cancel_exact(const PcmXTicketRef *ref)
{
	return pcm_x_master_cancel_exact_impl(ref, false, false, NULL);
}


PcmXQueueResult
cluster_pcm_x_master_cancel_reversible_exact(const PcmXTicketRef *ref)
{
	return pcm_x_master_cancel_exact_impl(ref, true, false, NULL);
}


PcmXQueueResult
cluster_pcm_x_master_pending_x_cancel_prepare_exact(const PcmXTicketRef *ref,
													PcmXMasterPendingXReleaseToken *token_out)
{
	return pcm_x_master_cancel_exact_impl(ref, true, true, token_out);
}


/* A release worker can be preempted after another worker finalizes the same
 * token and terminal retirement advances.  Revalidate the two states that no
 * longer belong to the live master domain without nesting allocator/master
 * locks: a contiguous retired frontier, or the exact immutable DETACHING
 * tombstone while allocator reclamation is excluded by the shared lock. */
static PcmXQueueResult
pcm_x_master_pending_x_release_post_terminal_exact(const PcmXMasterPendingXReleaseToken *token)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 flags;
	uint32 state;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	if (header->fully_retired_ticket_id != UINT64_MAX
		&& token->ref.handle.ticket_id <= header->fully_retired_ticket_id) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto replay_done;
	}
	result = pcm_x_master_ticket_lookup_locked(&token->ref, &ticket_ref, &ticket);
	if (result != PCM_X_QUEUE_OK)
		goto replay_done;
	state = pcm_x_slot_state_read(&ticket->slot);
	if (state != PCM_XT_DETACHING) {
		result = PCM_X_QUEUE_NOT_READY;
		goto replay_done;
	}
	flags = pcm_x_slot_flags_read(&ticket->slot);
	if (!pcm_x_ticket_ref_equal(&ticket->ref, &token->ref)
		|| ticket->master_session_incarnation != token->master_session_incarnation)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_master_pending_x_flags_valid(flags) || flags != 0
			 || ticket->next_index != PCM_X_INVALID_SLOT_INDEX
			 || ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
			 || (ticket->reliable.response_tombstone_mask & PCM_X_RESPONSE_TOMBSTONE_CANCEL) == 0
			 || (ticket->reliable.response_tombstone_mask & PCM_X_RESPONSE_TOMBSTONE_COMPLETE) != 0)
		result = PCM_X_QUEUE_CORRUPT;
	else
		result = PCM_X_QUEUE_DUPLICATE;

replay_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_pending_x_cancel_finalize_exact(const PcmXMasterPendingXReleaseToken *token)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	PcmXQueueResult replay_result;
	uint32 flags;
	uint32 partition;
	uint32 state;

	if (header == NULL || token == NULL || token->master_session_incarnation == 0
		|| token->state_sequence == 0 || token->ref.grant_generation != 0
		|| !pcm_x_wait_identity_valid(&token->ref.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (runtime.master_session_incarnation != token->master_session_incarnation)
		return PCM_X_QUEUE_STALE;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(&token->ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		replay_result = pcm_x_master_pending_x_release_post_terminal_exact(token);
		if (replay_result == PCM_X_QUEUE_DUPLICATE || replay_result == PCM_X_QUEUE_CORRUPT)
			return replay_result;
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&token->ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &token->ref.identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &token->ref.identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, &token->ref))
		result = PCM_X_QUEUE_STALE;
	else if (ticket->master_session_incarnation != token->master_session_incarnation)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (!pcm_x_master_pending_x_flags_valid((flags = pcm_x_slot_flags_read(&ticket->slot))))
		result = PCM_X_QUEUE_CORRUPT;
	else if ((state = pcm_x_slot_state_read(&ticket->slot)) != PCM_XT_CANCELLED
			 && state != PCM_XT_RETIRE_CREDIT)
		result = PCM_X_QUEUE_BAD_STATE;
	else if (!pcm_x_master_event_replay_exact((PcmXMasterTicketState)state,
											  PCM_X_EVENT_CANCEL_EXACT,
											  ticket->reliable.response_tombstone_mask))
		result = PCM_X_QUEUE_STALE;
	else if (ticket->next_index != PCM_X_INVALID_SLOT_INDEX
			 || ticket->prev_index != PCM_X_INVALID_SLOT_INDEX
			 || tag_slot->active_index == ticket_ref.slot_index
			 || tag_slot->head_index == ticket_ref.slot_index
			 || tag_slot->tail_index == ticket_ref.slot_index)
		result = PCM_X_QUEUE_CORRUPT;
	else if (flags == 0)
		result = PCM_X_QUEUE_DUPLICATE;
	else if (state != PCM_XT_CANCELLED)
		result = PCM_X_QUEUE_CORRUPT;
	else if (flags
			 != (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
				 | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING))
		result = PCM_X_QUEUE_CORRUPT;
	else if (ticket->reliable.state_sequence != token->state_sequence
			 || !pcm_x_transfer_leg_is_clear(&ticket->reliable)
			 || (ticket->blocker_set_generation != 0
				 && ticket->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT))
		result = PCM_X_QUEUE_STALE;
	else {
		pcm_x_slot_flags_write(&ticket->slot, 0);
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_STALE || result == PCM_X_QUEUE_BAD_STATE) {
		replay_result = pcm_x_master_pending_x_release_post_terminal_exact(token);
		if (replay_result == PCM_X_QUEUE_DUPLICATE || replay_result == PCM_X_QUEUE_CORRUPT)
			result = replay_result;
	}
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Resolve a cancellation sent before ADMIT_ACK by the immutable prehandle.
 * The directory slot generation is copied into cancelled_out before dropping
 * allocator_lock; the full-ref cancel below therefore cannot touch a reused
 * ticket even if terminal retirement races this adapter. */
PcmXQueueResult
cluster_pcm_x_master_prehandle_cancel_exact(const PcmXPrehandleCancelPayload *request,
											PcmXMasterAdmission *cancelled_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload staged_request;
	PcmXSlotRef ticket_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	bool fail_closed = false;
	bool cancel_first = false;

	if (cancelled_out != NULL)
		pcm_x_master_admission_clear(cancelled_out);
	if (header == NULL || request == NULL || cancelled_out == NULL
		|| !pcm_x_wait_identity_valid(&request->identity)
		|| request->prehandle.sender_session_incarnation == 0
		|| request->prehandle.prehandle_sequence == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto prehandle_lookup_done;
	}
	frontier = &header->peer_frontiers[request->identity.node_id];
	if (frontier->cluster_epoch != request->identity.cluster_epoch
		|| frontier->sender_session_incarnation != request->prehandle.sender_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto prehandle_lookup_done;
	}
	if (frontier->next_expected_prehandle_sequence == 0
		|| frontier->retired_prehandle_sequence >= frontier->next_expected_prehandle_sequence) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto prehandle_lookup_done;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
												   &request->prehandle, &ticket_ref);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND) {
		if (request->prehandle.prehandle_sequence <= frontier->retired_prehandle_sequence)
			result = PCM_X_QUEUE_RETIRED;
		else if (request->prehandle.prehandle_sequence
				 < frontier->next_expected_prehandle_sequence) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else if (request->prehandle.prehandle_sequence
				   == frontier->next_expected_prehandle_sequence) {
			/* CANCEL may overtake its ENQUEUE transport.  Consume the frontier
			 * below through the normal composite admission path, then publish a
			 * formal CANCELLED tombstone before acknowledging application. */
			result = PCM_X_QUEUE_OK;
			cancel_first = true;
		} else
			result = PCM_X_QUEUE_NOT_READY;
		goto prehandle_lookup_done;
	}
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto prehandle_lookup_done;
	}
	ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																   ticket_ref);
	if (ticket == NULL || !pcm_x_prehandle_equal(&ticket->prehandle, &request->prehandle)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto prehandle_lookup_done;
	}
	if (!pcm_x_wait_identity_equal(&ticket->ref.identity, &request->identity)) {
		/* One prehandle is an immutable application idempotency key.  Resolving
		 * it to another identity is internal namespace corruption, not an old
		 * generation that may be ignored. */
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto prehandle_lookup_done;
	}
	pcm_x_master_admission_from_ticket(cancelled_out, ticket_ref, ticket);
	result = PCM_X_QUEUE_OK;

prehandle_lookup_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed) {
		pcm_x_runtime_fail_closed();
		return result;
	}
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (cancel_first) {
		memset(&staged_request, 0, sizeof(staged_request));
		staged_request.identity = request->identity;
		staged_request.prehandle = request->prehandle;
		result = pcm_x_master_admit_begin_impl(&staged_request, cancelled_out, true);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
			return result;
		if (result == PCM_X_QUEUE_OK)
			return PCM_X_QUEUE_OK;
	}

	result = cluster_pcm_x_master_cancel_reversible_exact(&cancelled_out->ref);
	if (result == PCM_X_QUEUE_DUPLICATE)
		cancelled_out->flags = PCM_X_ADMIT_F_EXACT_REPLAY;
	return result;
}


static PcmXQueueResult
pcm_x_master_terminal_lookup(const PcmXTicketRef *ref, PcmXSlotRef *ticket_ref_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXMasterTicketSlot *ticket;
	PcmXQueueResult result;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, ticket_ref_out, &ticket);
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Prove that one terminal slot carries exactly one outcome authority. */
static PcmXQueueResult
pcm_x_master_terminal_outcome_exact(const PcmXMasterTicketSlot *ticket, uint32 state)
{
	const uint64 allowed = PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM
						   | PCM_X_RESPONSE_TOMBSTONE_COMPLETE | PCM_X_RESPONSE_TOMBSTONE_CANCEL;
	uint64 outcome;
	uint64 expected;
	uint32 flags;

	if (ticket == NULL)
		return PCM_X_QUEUE_CORRUPT;
	flags = pcm_x_slot_flags_read(&ticket->slot);
	if (!pcm_x_master_pending_x_flags_valid(flags))
		return PCM_X_QUEUE_CORRUPT;
	if ((ticket->reliable.response_tombstone_mask & ~allowed) != 0)
		return PCM_X_QUEUE_CORRUPT;
	outcome = ticket->reliable.response_tombstone_mask
			  & (PCM_X_RESPONSE_TOMBSTONE_COMPLETE | PCM_X_RESPONSE_TOMBSTONE_CANCEL);
	if (state == PCM_XT_COMPLETE)
		expected = PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
	else if (state == PCM_XT_CANCELLED)
		expected = PCM_X_RESPONSE_TOMBSTONE_CANCEL;
	else if (state == PCM_XT_RETIRE_CREDIT) {
		if (outcome != PCM_X_RESPONSE_TOMBSTONE_COMPLETE
			&& outcome != PCM_X_RESPONSE_TOMBSTONE_CANCEL)
			return PCM_X_QUEUE_CORRUPT;
		expected = outcome;
	} else
		return PCM_X_QUEUE_BAD_STATE;
	if (outcome != expected)
		return PCM_X_QUEUE_CORRUPT;
	if ((expected == PCM_X_RESPONSE_TOMBSTONE_COMPLETE && ticket->ref.grant_generation == 0)
		|| (expected == PCM_X_RESPONSE_TOMBSTONE_CANCEL && ticket->ref.grant_generation != 0))
		return PCM_X_QUEUE_CORRUPT;
	if (flags
		== (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
			| PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING))
		return PCM_X_QUEUE_NOT_READY;
	if (flags != 0)
		return PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_master_cancel_ack_snapshot_exact(const PcmXTicketRef *ref,
											   PcmXMasterAdmission *cancelled_out,
											   bool *prehandle_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 partition;
	uint32 state;
	bool prehandle;

	if (cancelled_out != NULL)
		pcm_x_master_admission_clear(cancelled_out);
	if (prehandle_out != NULL)
		*prehandle_out = false;
	if (header == NULL || ref == NULL || cancelled_out == NULL || prehandle_out == NULL
		|| ref->grant_generation != 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = ticket->tag_slot_index;
		tag_ref.slot_generation = ticket->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TICKET, ticket_ref, &ref->identity.tag,
		PCM_X_STATE_BIT(PCM_XT_CANCELLED) | PCM_X_STATE_BIT(PCM_XT_RETIRE_CREDIT));
	if (tag_slot == NULL || ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if ((state = pcm_x_slot_state_read(&ticket->slot)) != PCM_XT_CANCELLED
			 && state != PCM_XT_RETIRE_CREDIT)
		result = PCM_X_QUEUE_BAD_STATE;
	else if ((result = pcm_x_master_terminal_outcome_exact(ticket, state)) == PCM_X_QUEUE_OK) {
		prehandle
			= (ticket->reliable.response_tombstone_mask & PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM)
			  == 0;
		if (ticket->prehandle.sender_session_incarnation == 0
			|| ticket->prehandle.prehandle_sequence == 0)
			result = PCM_X_QUEUE_CORRUPT;
		else {
			pcm_x_master_admission_from_ticket(cancelled_out, ticket_ref, ticket);
			*prehandle_out = prehandle;
		}
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_master_terminal_leg_is_clear(const PcmXReliableLegState *leg)
{
	return leg != NULL && leg->pending_opcode == PCM_X_TERMINAL_LEG_NONE && leg->phase == 0
		   && leg->expected_responder_node == 0 && leg->expected_responder_session == 0;
}


/* Terminal progress is monotone within the fixed participant set. */
static bool
pcm_x_master_terminal_bitmaps_valid(const PcmXMasterTicketSlot *ticket, uint32 state)
{
	uint32 involved;

	if (ticket == NULL)
		return false;
	involved = ticket->involved_nodes_bitmap;
	if (involved == 0 || (ticket->drained_nodes_bitmap & ~involved) != 0
		|| (ticket->retire_acked_nodes_bitmap & ~involved) != 0)
		return false;
	if (state == PCM_XT_COMPLETE || state == PCM_XT_CANCELLED)
		return ticket->retire_acked_nodes_bitmap == 0;
	if (state == PCM_XT_RETIRE_CREDIT)
		return ticket->drained_nodes_bitmap == involved;
	return false;
}


PcmXQueueResult
cluster_pcm_x_master_terminal_leg_arm_exact(const PcmXTicketRef *ref, PcmXTerminalLegKind kind,
											int32 responder_node, uint64 responder_session,
											PcmXTerminalLegToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXMasterTicketSlot *ticket;
	PcmXDirectoryResult directory_result;
	PcmXRetirePayload retire_key;
	PcmXSlotRef existing;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint64 next_sequence;
	uint32 responder_bit;
	uint32 partition;
	uint32 state;
	bool fail_closed = false;

	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (header == NULL || ref == NULL || token_out == NULL
		|| (kind != PCM_X_TERMINAL_LEG_DRAIN && kind != PCM_X_TERMINAL_LEG_RETIRE)
		|| responder_node < 0 || responder_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| responder_session == 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (kind == PCM_X_TERMINAL_LEG_DRAIN) {
		result = pcm_x_master_terminal_lookup(ref, &ticket_ref);
		if (result != PCM_X_QUEUE_OK)
			return result;
	} else {
		/* Publish the durable wire-key authority before arming a RETIRE leg. */
		LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
		result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
		if (result == PCM_X_QUEUE_OK
			&& pcm_x_slot_state_read(&ticket->slot) != PCM_XT_RETIRE_CREDIT)
			result = PCM_X_QUEUE_NOT_READY;
		if (result == PCM_X_QUEUE_OK) {
			/* RETIRE_CREDIT is published only after grant_generation and the
			 * terminal outcome are stable.  No earlier state may expose a key. */
			pg_read_barrier();
			pcm_x_retire_key_from_ticket(ticket, &retire_key);
			directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_MASTER_TICKET_RETIRE,
															 &retire_key, ticket_ref, &existing);
			if (directory_result == PCM_X_DIRECTORY_EXISTS) {
				result = existing.slot_index == ticket_ref.slot_index
								 && existing.slot_generation == ticket_ref.slot_generation
							 ? PCM_X_QUEUE_OK
							 : PCM_X_QUEUE_CORRUPT;
				fail_closed = result == PCM_X_QUEUE_CORRUPT;
			} else if (directory_result != PCM_X_DIRECTORY_OK) {
				result = pcm_x_queue_result_from_directory(directory_result);
				fail_closed = result == PCM_X_QUEUE_CORRUPT;
			}
		}
		LWLockRelease(&header->allocator_lock.lock);
		if (result != PCM_X_QUEUE_OK) {
			if (fail_closed)
				pcm_x_runtime_fail_closed();
			return result;
		}
	}
	responder_bit = UINT32_C(1) << responder_node;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)) {
		result = PCM_X_QUEUE_STALE;
		goto arm_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto arm_done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	result = pcm_x_master_terminal_outcome_exact(ticket, state);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto arm_done;
	}
	if (!pcm_x_master_terminal_bitmaps_valid(ticket, state)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto arm_done;
	}
	if ((ticket->involved_nodes_bitmap & responder_bit) == 0) {
		result = PCM_X_QUEUE_STALE;
		goto arm_done;
	}
	frontier = &header->peer_frontiers[responder_node];
	if (frontier->cluster_epoch != ref->identity.cluster_epoch
		|| frontier->sender_session_incarnation != responder_session) {
		result = PCM_X_QUEUE_STALE;
		goto arm_done;
	}
	/* The retry driver scans the fixed participant bitmap from node zero on
	 * every pass.  Once an earlier responder has ACKed, a later responder may
	 * own the one reliable leg.  Classify the completed responder before the
	 * generic cross-responder BUSY check, otherwise that later leg permanently
	 * hides itself behind the first completed node. */
	if ((kind == PCM_X_TERMINAL_LEG_DRAIN
		 && (state == PCM_XT_RETIRE_CREDIT || (ticket->drained_nodes_bitmap & responder_bit) != 0))
		|| (kind == PCM_X_TERMINAL_LEG_RETIRE && state == PCM_XT_RETIRE_CREDIT
			&& (ticket->retire_acked_nodes_bitmap & responder_bit) != 0)) {
		/* A same-phase leg cannot remain armed after its responder bit commits.
		 * Do not let the retry classification conceal that structural split. */
		if (!pcm_x_master_terminal_leg_is_clear(&ticket->reliable)
			&& ticket->reliable.pending_opcode == (uint16)kind
			&& ticket->reliable.phase == (uint16)kind
			&& ticket->reliable.expected_responder_node == responder_node) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result = PCM_X_QUEUE_NOT_READY;
		goto arm_done;
	}
	if ((kind == PCM_X_TERMINAL_LEG_DRAIN && state == PCM_XT_RETIRE_CREDIT)
		|| (kind == PCM_X_TERMINAL_LEG_RETIRE && state != PCM_XT_RETIRE_CREDIT)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto arm_done;
	}
	if (!pcm_x_master_terminal_leg_is_clear(&ticket->reliable)) {
		if (ticket->reliable.pending_opcode == (uint16)kind
			&& ticket->reliable.phase == (uint16)kind
			&& ticket->reliable.expected_responder_node == responder_node
			&& ticket->reliable.expected_responder_session == responder_session
			&& ticket->reliable.state_sequence != 0
			&& (kind != PCM_X_TERMINAL_LEG_DRAIN || ticket->drain_generation != 0)) {
			token_out->state_sequence = ticket->reliable.state_sequence;
			token_out->expected_responder_session = responder_session;
			token_out->drain_generation
				= kind == PCM_X_TERMINAL_LEG_DRAIN ? ticket->drain_generation : 0;
			token_out->expected_responder_node = responder_node;
			token_out->kind = (uint16)kind;
			/* Diagnostic: an exact re-arm is the retry pump re-staging this
			 * leg.  The count reaches the per-slot dump as leg_retry, so a
			 * frozen wedge separates a dead pump from a refusing responder. */
			if (ticket->reliable.retry_count != PG_UINT32_MAX)
				ticket->reliable.retry_count++;
			result = PCM_X_QUEUE_DUPLICATE;
		} else
			result = PCM_X_QUEUE_BUSY;
		goto arm_done;
	}
	if (!cluster_pcm_x_generation_next(ticket->reliable.state_sequence, &next_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto arm_done;
	}
	if (kind == PCM_X_TERMINAL_LEG_DRAIN && ticket->drain_generation == 0)
		ticket->drain_generation = next_sequence;
	ticket->reliable.state_sequence = next_sequence;
	ticket->reliable.retry_deadline_ms = 0;
	ticket->reliable.expected_responder_session = responder_session;
	ticket->reliable.retry_count = 0;
	ticket->reliable.expected_responder_node = responder_node;
	ticket->reliable.pending_opcode = (uint16)kind;
	ticket->reliable.phase = (uint16)kind;
	ticket->reliable.flags = 0;
	pg_write_barrier();
	token_out->state_sequence = next_sequence;
	token_out->expected_responder_session = responder_session;
	token_out->drain_generation = kind == PCM_X_TERMINAL_LEG_DRAIN ? ticket->drain_generation : 0;
	token_out->expected_responder_node = responder_node;
	token_out->kind = (uint16)kind;
	result = PCM_X_QUEUE_OK;

arm_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_master_terminal_leg_ack_resolved(const PcmXTicketRef *ref, PcmXTerminalLegKind actual_kind,
									   int32 authenticated_node, uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXQueueResult result;
	uint32 responder_bit;
	uint32 partition;
	uint32 state;
	bool fail_closed = false;

	if (header == NULL || ref == NULL
		|| (actual_kind != PCM_X_TERMINAL_LEG_DRAIN && actual_kind != PCM_X_TERMINAL_LEG_RETIRE)
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0 || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_master_terminal_lookup(ref, &ticket_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	responder_bit = UINT32_C(1) << authenticated_node;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_MASTER_TICKET_DOMAIN_STATES);
	if (ticket == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}
	state = pcm_x_slot_state_read(&ticket->slot);
	result = pcm_x_master_terminal_outcome_exact(ticket, state);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto ack_done;
	}
	if (!pcm_x_master_terminal_bitmaps_valid(ticket, state)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto ack_done;
	}
	if ((ticket->involved_nodes_bitmap & responder_bit) == 0) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	frontier = &header->peer_frontiers[authenticated_node];
	if (frontier->cluster_epoch != ref->identity.cluster_epoch
		|| frontier->sender_session_incarnation != authenticated_session) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	if (actual_kind == PCM_X_TERMINAL_LEG_DRAIN && ticket->drain_generation == 0) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}
	if ((actual_kind == PCM_X_TERMINAL_LEG_DRAIN ? ticket->drained_nodes_bitmap
												 : ticket->retire_acked_nodes_bitmap)
		& responder_bit) {
		/* A closed exact key replays without touching another node's pending leg. */
		result = PCM_X_QUEUE_DUPLICATE;
		goto ack_done;
	}
	if (pcm_x_master_terminal_leg_is_clear(&ticket->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}
	if (ticket->reliable.pending_opcode != (uint16)actual_kind
		|| ticket->reliable.phase != (uint16)actual_kind
		|| ticket->reliable.expected_responder_node != authenticated_node
		|| ticket->reliable.expected_responder_session != authenticated_session) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	if ((actual_kind == PCM_X_TERMINAL_LEG_DRAIN && state != PCM_XT_COMPLETE
		 && state != PCM_XT_CANCELLED && state != PCM_XT_RETIRE_CREDIT)
		|| (actual_kind == PCM_X_TERMINAL_LEG_RETIRE && state != PCM_XT_RETIRE_CREDIT)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}

	/* Clear the durable pending leg before publishing its bitmap outcome. */
	ticket->reliable.retry_deadline_ms = 0;
	ticket->reliable.expected_responder_session = 0;
	ticket->reliable.retry_count = 0;
	ticket->reliable.last_responder_node = (uint32)authenticated_node;
	ticket->reliable.expected_responder_node = 0;
	ticket->reliable.pending_opcode = PCM_X_TERMINAL_LEG_NONE;
	ticket->reliable.last_response_opcode = (uint16)actual_kind;
	ticket->reliable.phase = 0;
	ticket->reliable.flags = 0;
	pg_write_barrier();
	if (actual_kind == PCM_X_TERMINAL_LEG_DRAIN) {
		ticket->drained_nodes_bitmap |= responder_bit;
		if (state != PCM_XT_RETIRE_CREDIT && cluster_pcm_x_ticket_drain_ready(ticket)) {
			pg_write_barrier();
			pcm_x_slot_state_write(&ticket->slot, PCM_XT_RETIRE_CREDIT);
		}
	} else
		ticket->retire_acked_nodes_bitmap |= responder_bit;
	result = PCM_X_QUEUE_OK;

ack_done:
	LWLockRelease(&header->master_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_terminal_leg_ack_exact(const PcmXTicketRef *ref,
											PcmXTerminalLegKind actual_kind,
											int32 authenticated_node, uint64 authenticated_session)
{
	if (actual_kind != PCM_X_TERMINAL_LEG_DRAIN)
		return PCM_X_QUEUE_INVALID;
	return pcm_x_master_terminal_leg_ack_resolved(ref, actual_kind, authenticated_node,
												  authenticated_session);
}


static PcmXQueueResult
pcm_x_master_retire_ack_common(const PcmXRetirePayload *ack, int32 authenticated_node,
							   uint64 authenticated_session, PcmXTicketRef *ref_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXDirectoryResult directory_result;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	PcmXTicketRef ref;
	PcmXQueueResult result;
	uint32 partition;

	if (ref_out != NULL)
		memset(ref_out, 0, sizeof(*ref_out));
	if (header == NULL || ack == NULL || ack->master_session_incarnation == 0
		|| ack->retire_through_ticket_id == 0 || ack->sender_node < 0
		|| ack->sender_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| ack->flags != 0)
		return PCM_X_QUEUE_INVALID;
	if (ack->sender_node != authenticated_node)
		return PCM_X_QUEUE_STALE;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_RETIRE, ack, &ticket_ref);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																	   ticket_ref);
		if (ticket == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
		} else if (!pcm_x_directory_key_matches_slot(PCM_X_DIR_MASTER_TICKET_RETIRE, ack,
													 &ticket->slot)) {
			result = PCM_X_QUEUE_CORRUPT;
		} else {
			/* Only immutable locator fields are readable under allocator_lock. */
			memset(&ref, 0, sizeof(ref));
			ref.identity = ticket->ref.identity;
			ref.handle = ticket->ref.handle;
			result = PCM_X_QUEUE_OK;
		}
	} else if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		result = PCM_X_QUEUE_STALE;
	else
		result = pcm_x_queue_result_from_directory(directory_result);
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	/* grant_generation is owned by the master domain.  RETIRE_CREDIT proves
	 * that its preceding publication is complete before we copy the full ref. */
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref.identity.tag,
													   PCM_X_STATE_BIT(PCM_XT_RETIRE_CREDIT));
	if (ticket == NULL || !pcm_x_ticket_locator_equal(&ticket->ref, &ref))
		result = PCM_X_QUEUE_STALE;
	else {
		pg_read_barrier();
		ref.grant_generation = ticket->ref.grant_generation;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->master_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		return result;
	result = pcm_x_master_terminal_leg_ack_resolved(&ref, PCM_X_TERMINAL_LEG_RETIRE,
													authenticated_node, authenticated_session);
	if ((result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) && ref_out != NULL)
		*ref_out = ref;
	return result;
}


PcmXQueueResult
cluster_pcm_x_master_retire_ack_exact(const PcmXRetirePayload *ack, int32 authenticated_node,
									  uint64 authenticated_session)
{
	return pcm_x_master_retire_ack_common(ack, authenticated_node, authenticated_session, NULL);
}


PcmXQueueResult
cluster_pcm_x_master_retire_ack_resolve_exact(const PcmXRetirePayload *ack,
											  int32 authenticated_node,
											  uint64 authenticated_session, PcmXTicketRef *ref_out)
{
	if (ref_out == NULL)
		return PCM_X_QUEUE_INVALID;
	return pcm_x_master_retire_ack_common(ack, authenticated_node, authenticated_session, ref_out);
}


/* Return the oldest terminal ticket so the LMON cold-path retry driver cannot
 * starve the global contiguous retirement frontier behind a newer slot. */
PcmXQueueResult
cluster_pcm_x_master_terminal_work_next(PcmXTicketRef *ref_out)
{
	return cluster_pcm_x_master_terminal_work_next_after(0, ref_out);
}


/* Floor-scoped scan for the retry driver's fairness loop: the oldest ticket
 * keeps frontier priority, but its stuck RETIRE preflight may be waiting for
 * a younger ticket's DRAIN evidence (cross-lane holder interlock), so the
 * driver walks every terminal ticket in ticket-id order per pass. */
PcmXQueueResult
cluster_pcm_x_master_terminal_work_next_after(uint64 after_ticket_id, PcmXTicketRef *ref_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXAllocatorView view;
	PcmXMasterTicketSlot *raw;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	BufferTag tag;
	uint64 generation_after;
	uint64 best_ticket_id = UINT64_MAX;
	Size i;
	bool fail_closed = false;

	if (ref_out != NULL)
		memset(ref_out, 0, sizeof(*ref_out));
	if (header == NULL || ref_out == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (!pcm_x_allocator_entry_unlocked(header)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &view))
		return PCM_X_QUEUE_INVALID;

	for (i = 0; i < view.capacity; i++) {
		uint32 partition;
		uint32 flags;
		uint32 state;
		PcmXQueueResult result;
		bool release_work;

		raw = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&view, i);
		if (raw == NULL)
			continue;
		state = pcm_x_slot_state_read(&raw->slot);
		if (state != PCM_XT_COMPLETE && state != PCM_XT_CANCELLED && state != PCM_XT_RETIRE_CREDIT)
			continue;
		if (!pcm_x_slot_generation_read(&raw->slot, &ticket_ref.slot_generation))
			continue;
		ticket_ref.slot_index = i;
		tag = raw->ref.identity.tag;
		pg_read_barrier();
		if (!pcm_x_slot_generation_read(&raw->slot, &generation_after)
			|| generation_after != ticket_ref.slot_generation
			|| pcm_x_slot_state_read(&raw->slot) != state)
			continue;

		partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&tag));
		LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
		ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(
			PCM_X_ALLOC_MASTER_TICKET, ticket_ref, &tag,
			PCM_X_STATE_BIT(PCM_XT_COMPLETE) | PCM_X_STATE_BIT(PCM_XT_CANCELLED)
				| PCM_X_STATE_BIT(PCM_XT_RETIRE_CREDIT));
		if (ticket == NULL) {
			LWLockRelease(&header->master_locks[partition].lock);
			continue;
		}
		state = pcm_x_slot_state_read(&ticket->slot);
		result = pcm_x_master_terminal_outcome_exact(ticket, state);
		flags = pcm_x_slot_flags_read(&ticket->slot);
		release_work = result == PCM_X_QUEUE_NOT_READY && state == PCM_XT_CANCELLED
					   && flags
							  == (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
								  | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
		if ((result != PCM_X_QUEUE_OK && !release_work)
			|| !pcm_x_master_terminal_bitmaps_valid(ticket, state)
			|| ticket->ref.handle.ticket_id == 0) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT
						  || !pcm_x_master_terminal_bitmaps_valid(ticket, state)
						  || ticket->ref.handle.ticket_id == 0;
			LWLockRelease(&header->master_locks[partition].lock);
			if (fail_closed)
				break;
			continue;
		}
		if (ticket->ref.handle.ticket_id > after_ticket_id
			&& ticket->ref.handle.ticket_id < best_ticket_id) {
			best_ticket_id = ticket->ref.handle.ticket_id;
			*ref_out = ticket->ref;
		}
		LWLockRelease(&header->master_locks[partition].lock);
	}
	if (fail_closed) {
		memset(ref_out, 0, sizeof(*ref_out));
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		memset(ref_out, 0, sizeof(*ref_out));
		return PCM_X_QUEUE_NOT_READY;
	}
	return best_ticket_id == UINT64_MAX ? PCM_X_QUEUE_NOT_FOUND : PCM_X_QUEUE_OK;
}


/* Prove that a terminal ticket is no longer reachable from the hot FIFO. */
static bool
pcm_x_master_terminal_unlinked_locked(PcmXMasterTagSlot *tag_slot, PcmXSlotRef tag_ref,
									  PcmXMasterTicketSlot *terminal, PcmXSlotRef terminal_ref)
{
	PcmXAllocatorView view;
	Size current;
	Size previous = PCM_X_INVALID_SLOT_INDEX;
	Size visited = 0;

	if (tag_slot == NULL || terminal == NULL
		|| !pcm_x_allocator_view(PCM_X_ALLOC_MASTER_TICKET, &view)
		|| terminal->next_index != PCM_X_INVALID_SLOT_INDEX
		|| terminal->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->head_index == terminal_ref.slot_index
		|| tag_slot->tail_index == terminal_ref.slot_index
		|| tag_slot->active_index == terminal_ref.slot_index)
		return false;

	current = tag_slot->head_index;
	while (current != PCM_X_INVALID_SLOT_INDEX) {
		PcmXMasterTicketSlot *linked;
		PcmXSlotRef linked_ref;

		if (current == terminal_ref.slot_index || visited >= PCM_X_PROTOCOL_NODE_LIMIT)
			return false;
		linked = (PcmXMasterTicketSlot *)pcm_x_allocator_slot(&view, current);
		if (linked == NULL)
			return false;
		linked_ref.slot_index = current;
		if (!pcm_x_slot_generation_read(&linked->slot, &linked_ref.slot_generation))
			return false;
		linked = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, linked_ref,
														   &tag_slot->tag,
														   PCM_X_MASTER_TICKET_DOMAIN_STATES);
		if (linked == NULL || linked->tag_slot_index != tag_ref.slot_index
			|| linked->tag_slot_generation != tag_ref.slot_generation
			|| linked->prev_index != previous)
			return false;
		previous = current;
		current = linked->next_index;
		visited++;
	}
	if (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
		return tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX;
	return tag_slot->tail_index == previous;
}


PcmXQueueResult
cluster_pcm_x_master_detach_terminal_exact(const PcmXTicketRef *ref)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef found;
	PcmXSlotRef tag_ref;
	PcmXSlotRef ticket_ref;
	PcmXPrehandleKey prehandle;
	PcmXRetirePayload retire_key;
	PcmXTicketHandle blocker_owner;
	PcmXQueueResult result;
	Size blocker_head = PCM_X_INVALID_SLOT_INDEX;
	uint64 blocker_generation = 0;
	uint32 blocker_count = 0;
	uint32 expected_gate;
	uint32 partition;
	bool detach_tag = false;
	bool hot_queue_empty;
	bool release_blockers = false;

	if (header == NULL || ref == NULL || !pcm_x_wait_identity_valid(&ref->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	/* First prove consecutive retirement and capture immutable slot identities. */
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	result = pcm_x_master_ticket_lookup_locked(ref, &ticket_ref, &ticket);
	if (result != PCM_X_QUEUE_OK)
		goto allocator_preflight_done;
	if (pcm_x_slot_state_read(&ticket->slot) != PCM_XT_RETIRE_CREDIT) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_preflight_done;
	}
	if (header->fully_retired_ticket_id == UINT64_MAX
		|| ref->handle.ticket_id != header->fully_retired_ticket_id + 1) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_preflight_done;
	}
	prehandle = ticket->prehandle;
	frontier = &header->peer_frontiers[ref->identity.node_id];
	if (frontier->cluster_epoch != ref->identity.cluster_epoch
		|| frontier->sender_session_incarnation != prehandle.sender_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto allocator_preflight_done;
	}
	if (frontier->retired_prehandle_sequence == UINT64_MAX
		|| prehandle.prehandle_sequence != frontier->retired_prehandle_sequence + 1) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_preflight_done;
	}
	tag_ref.slot_index = ticket->tag_slot_index;
	tag_ref.slot_generation = ticket->tag_slot_generation;
	result = PCM_X_QUEUE_OK;

allocator_preflight_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	/* Quiesce under the tag domain before allocator-owned reclamation. */
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	ticket = (PcmXMasterTicketSlot *)pcm_x_domain_slot(PCM_X_ALLOC_MASTER_TICKET, ticket_ref,
													   &ref->identity.tag,
													   PCM_X_STATE_BIT(PCM_XT_RETIRE_CREDIT));
	tag_slot = (PcmXMasterTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_MASTER_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (ticket == NULL || tag_slot == NULL || !pcm_x_ticket_ref_equal(&ticket->ref, ref)
		|| ticket->tag_slot_index != tag_ref.slot_index
		|| ticket->tag_slot_generation != tag_ref.slot_generation) {
		result = PCM_X_QUEUE_STALE;
		goto domain_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, ticket->master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto domain_done;
	}
	result = pcm_x_master_terminal_outcome_exact(ticket, pcm_x_slot_state_read(&ticket->slot));
	if (result != PCM_X_QUEUE_OK)
		goto domain_done;
	if (!pcm_x_master_terminal_leg_is_clear(&ticket->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto domain_done;
	}
	if (!pcm_x_master_blocker_stage_metadata_valid(ticket)
		|| !pcm_x_master_blocker_stage_is_clear(ticket)
		|| !pcm_x_master_blocker_published_source_valid(ticket)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (ticket->blocker_set_generation == 0) {
		if (ticket->blocker_head_index != PCM_X_INVALID_SLOT_INDEX || ticket->blocker_count != 0
			|| ticket->blocker_set_crc32c != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	} else {
		PcmXAllocatorView blocker_view;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			|| !pcm_x_blocker_chain_validate(
				&blocker_view, ticket->blocker_head_index, ticket->blocker_count,
				&ticket->ref.handle, ticket_ref.slot_index, ticket_ref.slot_generation,
				ticket->blocker_set_generation, PCM_XB_LIVE, NULL, 0, NULL)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	}
	if (ticket->involved_nodes_bitmap == 0
		|| ticket->drained_nodes_bitmap != ticket->involved_nodes_bitmap
		|| (ticket->retire_acked_nodes_bitmap & ~ticket->involved_nodes_bitmap) != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (!cluster_pcm_x_ticket_retire_ready(ticket)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto domain_done;
	}
	if (!pcm_x_master_terminal_unlinked_locked(tag_slot, tag_ref, ticket, ticket_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (tag_slot->outstanding_ticket_count == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (tag_slot->outstanding_ticket_count == 1) {
		/* First exclude an allocator-stage admission, then inspect hot state. */
		expected_gate = 0;
		if (!pg_atomic_compare_exchange_u32(&tag_slot->admission_gate, &expected_gate, 2)) {
			result = PCM_X_QUEUE_BUSY;
			goto domain_done;
		}
		pg_read_barrier();
		hot_queue_empty = tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX
						  && tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX
						  && tag_slot->active_index == PCM_X_INVALID_SLOT_INDEX
						  && pg_atomic_read_u32(&tag_slot->queued_node_bitmap) == 0;
		if (tag_slot->outstanding_ticket_count != 1 || !hot_queue_empty) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
		detach_tag = true;
	}
	if (ticket->blocker_count != 0) {
		PcmXAllocatorView blocker_view;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		blocker_owner = ticket->ref.handle;
		blocker_head = ticket->blocker_head_index;
		blocker_count = ticket->blocker_count;
		blocker_generation = ticket->blocker_set_generation;
		pcm_x_blocker_chain_state_write(&blocker_view, blocker_head, blocker_count,
										PCM_XB_DETACHING);
		release_blockers = true;
	}
	tag_slot->outstanding_ticket_count--;
	pg_write_barrier();
	pcm_x_slot_state_write(&ticket->slot, PCM_XT_DETACHING);
	result = PCM_X_QUEUE_OK;

domain_done:
	/* A post-CAS jump reaches here only for CORRUPT and retains gate value 2. */
	LWLockRelease(&header->master_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	/* Advance replay authority first, then remove exact indexes and free slots. */
	pcm_x_master_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, tag_slot,
									  detach_tag ? 2 : 0);
	ticket = (PcmXMasterTicketSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TICKET,
																   ticket_ref);
	tag_slot = (PcmXMasterTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_MASTER_TAG, tag_ref);
	frontier = &header->peer_frontiers[ref->identity.node_id];
	if (ticket == NULL || tag_slot == NULL
		|| pcm_x_slot_state_read(&ticket->slot) != PCM_XT_DETACHING
		|| (detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_DETACHING)
		|| (!detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE)
		|| (detach_tag && tag_slot->outstanding_ticket_count != 0)
		|| !pcm_x_ticket_ref_equal(&ticket->ref, ref)
		|| !pcm_x_prehandle_equal(&ticket->prehandle, &prehandle)
		|| header->fully_retired_ticket_id == UINT64_MAX
		|| ref->handle.ticket_id != header->fully_retired_ticket_id + 1
		|| frontier->cluster_epoch != ref->identity.cluster_epoch
		|| frontier->sender_session_incarnation != prehandle.sender_session_incarnation
		|| frontier->retired_prehandle_sequence == UINT64_MAX
		|| prehandle.prehandle_sequence != frontier->retired_prehandle_sequence + 1) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_detach_done;
	}
	if (pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &prehandle, &found)
			!= PCM_X_DIRECTORY_OK
		|| found.slot_index != ticket_ref.slot_index
		|| found.slot_generation != ticket_ref.slot_generation
		|| pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle, &found)
			   != PCM_X_DIRECTORY_OK
		|| found.slot_index != ticket_ref.slot_index
		|| found.slot_generation != ticket_ref.slot_generation
		|| (detach_tag
			&& (pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TAG, &ref->identity.tag, &found)
					!= PCM_X_DIRECTORY_OK
				|| found.slot_index != tag_ref.slot_index
				|| found.slot_generation != tag_ref.slot_generation))) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_detach_done;
	}
	pcm_x_retire_key_from_ticket(ticket, &retire_key);
	if (pcm_x_directory_find_locked(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire_key, &found)
			!= PCM_X_DIRECTORY_OK
		|| found.slot_index != ticket_ref.slot_index
		|| found.slot_generation != ticket_ref.slot_generation) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_detach_done;
	}
	/* Preserve the allocator's validate-before-first-free discipline: every
	 * terminal directory locator must be exact before reclaiming a retained
	 * published blocker chain. */
	if (release_blockers
		&& !pcm_x_blocker_chain_release_locked_exact(
			blocker_head, blocker_count, &blocker_owner, ticket_ref.slot_index,
			ticket_ref.slot_generation, blocker_generation, PCM_XB_DETACHING)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_detach_done;
	}
	header->fully_retired_ticket_id = ref->handle.ticket_id;
	frontier->retired_prehandle_sequence = prehandle.prehandle_sequence;
	pg_write_barrier();
	if (pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &prehandle,
											ticket_ref)
			!= PCM_X_DIRECTORY_OK
		|| pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle,
											   ticket_ref)
			   != PCM_X_DIRECTORY_OK
		|| pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire_key,
											   ticket_ref)
			   != PCM_X_DIRECTORY_OK
		|| (detach_tag
			&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_MASTER_TAG, &ref->identity.tag,
												   tag_ref)
				   != PCM_X_DIRECTORY_OK)
		|| pcm_x_allocator_release_locked(PCM_X_ALLOC_MASTER_TICKET, ticket_ref, PCM_XT_DETACHING)
			   != PCM_X_ALLOC_OK
		|| (detach_tag
			&& pcm_x_allocator_release_locked(PCM_X_ALLOC_MASTER_TAG, tag_ref, PCM_X_TAG_DETACHING)
				   != PCM_X_ALLOC_OK)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_detach_done;
	}
	result = PCM_X_QUEUE_OK;

allocator_detach_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool pcm_x_local_gate_release(PcmXLocalTagSlot *tag_slot);


static PcmXQueueResult
pcm_x_local_gate_try_acquire_mode(PcmXLocalTagSlot *tag_slot, bool retire_protocol)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint32 retire_gate;
	uint32 desired;
	uint32 flags;

	if (header == NULL || tag_slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	/* RETIRE publishes this global gate while holding allocator_lock.  Ordinary
	 * tag-only mutators check on both sides of their tag CAS: if they win first,
	 * RETIRE's quiescence scan sees the tag gate; if RETIRE wins between checks,
	 * they release without touching domain state. */
	retire_gate = pg_atomic_read_u32(&header->local_retire_gate);
	if ((!retire_protocol && retire_gate != 0)
		|| (retire_protocol
			&& (tag_slot->master_node < 0 || retire_gate != (uint32)tag_slot->master_node + 1)))
		return retire_protocol && tag_slot->master_node < 0 ? PCM_X_QUEUE_CORRUPT
															: PCM_X_QUEUE_BUSY;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	for (;;) {
		if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0)
			return PCM_X_QUEUE_BUSY;
		desired = flags | PCM_X_LOCAL_TAG_F_ADMISSION_GATE;
		if (pcm_x_slot_flags_compare_exchange(&tag_slot->slot, &flags, desired)) {
			pg_read_barrier();
			retire_gate = pg_atomic_read_u32(&header->local_retire_gate);
			if ((!retire_protocol && retire_gate != 0)
				|| (retire_protocol && retire_gate != (uint32)tag_slot->master_node + 1)) {
				if (!pcm_x_local_gate_release(tag_slot))
					return PCM_X_QUEUE_CORRUPT;
				return PCM_X_QUEUE_BUSY;
			}
			return PCM_X_QUEUE_OK;
		}
	}
}


static PcmXQueueResult
pcm_x_local_gate_try_acquire(PcmXLocalTagSlot *tag_slot)
{
	return pcm_x_local_gate_try_acquire_mode(tag_slot, false);
}


static PcmXQueueResult
pcm_x_local_retire_gate_try_acquire(PcmXLocalTagSlot *tag_slot)
{
	return pcm_x_local_gate_try_acquire_mode(tag_slot, true);
}


static bool
pcm_x_local_gate_release(PcmXLocalTagSlot *tag_slot)
{
	uint32 desired;
	uint32 flags;

	pg_write_barrier();
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	for (;;) {
		if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0)
			return false;
		desired = flags & ~PCM_X_LOCAL_TAG_F_ADMISSION_GATE;
		if (pcm_x_slot_flags_compare_exchange(&tag_slot->slot, &flags, desired))
			return true;
	}
}


/* RETIRE scans and detaches more than one local tag under a monotonic peer
 * watermark.  Cross-lock admissions publish ADMISSION_GATE while moving from
 * allocator_lock to a tag lock; serialize those handoffs with the whole
 * retirement episode so a legal register/join window can never be mistaken
 * for post-preflight corruption.  Caller holds allocator_lock. */
static PcmXQueueResult
pcm_x_local_retire_episode_state_locked(PcmXShmemHeader *header)
{
	uint32 retire_gate;
	int marker_node = -1;
	int marker_count = 0;
	int i;

	Assert(header != NULL);
	Assert(LWLockHeldByMe(&header->allocator_lock.lock));
	retire_gate = pg_atomic_read_u32(&header->local_retire_gate);
	for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
		if (header->peer_frontiers[i].local_retire_in_progress_ticket_id != 0) {
			marker_node = i;
			marker_count++;
		}
	}
	if (retire_gate == 0 && marker_count == 0)
		return PCM_X_QUEUE_OK;
	if (retire_gate == 0 || retire_gate > PCM_X_PROTOCOL_NODE_LIMIT || marker_count != 1
		|| marker_node != (int)retire_gate - 1
		|| header->peer_frontiers[marker_node].local_retire_in_progress_ticket_id
			   <= header->peer_frontiers[marker_node].local_retired_ticket_id)
		return PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_BUSY;
}


/* Order every allocator<->tag handoff against RETIRE without nesting LWLocks.
 * The caller holds allocator_lock; claiming the atomic tag gate before that
 * lock is released means RETIRE must observe either this gate or its own
 * already-published episode marker. */
static PcmXQueueResult
pcm_x_local_handoff_gate_claim_locked(PcmXShmemHeader *header, PcmXLocalTagSlot *tag_slot,
									  bool retire_protocol, PcmXQueueResult contention_result)
{
	PcmXQueueResult result;

	Assert(header != NULL);
	Assert(tag_slot != NULL);
	Assert(LWLockHeldByMe(&header->allocator_lock.lock));
	Assert(contention_result == PCM_X_QUEUE_BUSY || contention_result == PCM_X_QUEUE_GATE_RETRY);
	if (!retire_protocol) {
		result = pcm_x_local_retire_episode_state_locked(header);
		if (result != PCM_X_QUEUE_OK)
			return result == PCM_X_QUEUE_BUSY ? contention_result : result;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	return result == PCM_X_QUEUE_BUSY ? contention_result : result;
}


/* Prove that no admission was already between allocator publication and its
 * tag-domain commit before claiming a RETIRE episode.  Once the marker is
 * published, register/join refuse to create a new such window. */
static PcmXQueueResult
pcm_x_local_admission_handoffs_quiescent_locked(PcmXShmemHeader *header)
{
	PcmXAllocatorView view;
	PcmXLocalTagSlot *tag_slot;
	Size i;
	uint32 flags;
	uint32 state;

	Assert(header != NULL);
	Assert(LWLockHeldByMe(&header->allocator_lock.lock));
	if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &view))
		return PCM_X_QUEUE_CORRUPT;
	for (i = 0; i < view.capacity; i++) {
		tag_slot = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&view, i);
		if (tag_slot == NULL)
			return PCM_X_QUEUE_CORRUPT;
		state = pcm_x_slot_state_read(&tag_slot->slot);
		flags = pcm_x_slot_flags_read(&tag_slot->slot);
		if (state == PCM_X_SLOT_FREE || state == PCM_X_SLOT_GENERATION_EXHAUSTED) {
			if (flags != 0)
				return PCM_X_QUEUE_CORRUPT;
			continue;
		}
		if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0)
			return PCM_X_QUEUE_BUSY;
	}
	return PCM_X_QUEUE_OK;
}


/* Local-domain twin of pcm_x_master_gate_acquire_guarded(). */
static void
pcm_x_local_gate_acquire_guarded(LWLock *lock, LWLockMode mode, PcmXLocalTagSlot *tag_slot)
{
	PG_TRY();
	{
		LWLockAcquire(lock, mode);
	}
	PG_CATCH();
	{
		pcm_x_runtime_fail_closed();
		if (LWLockHeldByMe(lock))
			LWLockRelease(lock);
		if (tag_slot != NULL)
			(void)pcm_x_local_gate_release(tag_slot);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * The final rekey phase starts after the target directory key is durable.
 * An acquire ERROR there must retain ADMISSION_GATE as recovery evidence;
 * releasing it would expose the intentional target-key/old-resident window.
 */
static void
pcm_x_local_gate_acquire_guarded_retain_gate(LWLock *lock, LWLockMode mode)
{
	PG_TRY();
	{
		LWLockAcquire(lock, mode);
	}
	PG_CATCH();
	{
		pcm_x_runtime_fail_closed();
		if (LWLockHeldByMe(lock))
			LWLockRelease(lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static bool
pcm_x_local_stage_rollback_locked(bool new_tag, bool tag_directory_published,
								  bool member_directory_published, const BufferTag *tag,
								  const PcmXWaitIdentity *identity, PcmXSlotRef tag_ref,
								  PcmXLocalMembershipSlot *member, PcmXSlotRef member_ref)
{
	bool ok = true;

	if (member_directory_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_WAIT, identity, member_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (member != NULL
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
										  PCM_XL_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	if (tag_directory_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, tag, tag_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (new_tag
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
										  PCM_X_TAG_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	return ok;
}


static PcmXQueueResult
pcm_x_local_member_lookup_locked(const PcmXWaitIdentity *identity, PcmXSlotRef *member_ref_out,
								 PcmXLocalMembershipSlot **member_out)
{
	PcmXDirectoryView directory;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXDirectoryMatch match;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || identity == NULL || member_ref_out == NULL || member_out == NULL
		|| !LWLockHeldByMe(&header->allocator_lock.lock)
		|| !cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_WAIT, identity, &key_hash)
		|| !pcm_x_directory_view(PCM_X_DIR_LOCAL_WAIT, &directory))
		return PCM_X_QUEUE_INVALID;
	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			return PCM_X_QUEUE_NOT_FOUND;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return pcm_x_queue_result_from_directory(PCM_X_DIRECTORY_CORRUPT);
		if (entry->key_hash != key_hash)
			continue;
		member_ref.slot_index = entry->slot_index;
		member_ref.slot_generation = entry->slot_generation;
		member = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_WAIT,
																		  member_ref);
		if (member != NULL) {
			tag_ref.slot_index = member->tag_slot_index;
			tag_ref.slot_generation = member->tag_slot_generation;
			tag_slot
				= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
			/* Do not read the resident identity while the local-lock rekey
			 * writer may be replacing it.  A hash collision is conservatively
			 * retryable until the gate clears and exact matching resumes.  The
			 * release side publishes identity before clearing the gate; pair its
			 * write barrier here before dereferencing the resident checksum. */
			if (tag_slot != NULL) {
				if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE)
					!= 0)
					return PCM_X_QUEUE_BUSY;
				pg_read_barrier();
			}
		}
		match
			= pcm_x_directory_entry_match_locked(PCM_X_DIR_LOCAL_WAIT, &directory, entry, identity);
		if (match == PCM_X_DIRECTORY_MATCH_EXACT) {
			if (member == NULL)
				return pcm_x_queue_result_from_directory(PCM_X_DIRECTORY_STALE_REF);
			*member_ref_out = member_ref;
			*member_out = member;
			return PCM_X_QUEUE_OK;
		}
		if (match == PCM_X_DIRECTORY_MATCH_STALE)
			return pcm_x_queue_result_from_directory(PCM_X_DIRECTORY_STALE_REF);
		if (match == PCM_X_DIRECTORY_MATCH_CORRUPT)
			return pcm_x_queue_result_from_directory(PCM_X_DIRECTORY_CORRUPT);
	}
	return PCM_X_QUEUE_NOT_FOUND;
}


static bool
pcm_x_local_handle_exact(const PcmXLocalHandle *handle, PcmXSlotRef member_ref,
						 const PcmXLocalMembershipSlot *member, const PcmXLocalTagSlot *tag_slot)
{
	uint32 round;
	uint32 flags;

	if (tag_slot == NULL)
		return false;
	round = member->admitted_round;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0) {
		if (member->local_sequence <= tag_slot->cutoff_sequence) {
			if (round != tag_slot->local_round)
				return false;
		} else if (tag_slot->local_round == UINT32_MAX || round != tag_slot->local_round + 1)
			return false;
	} else if (round != tag_slot->local_round)
		return false;
	return pcm_x_wait_identity_equal(&handle->identity, &member->identity)
		   && handle->tag_slot.slot_index == member->tag_slot_index
		   && handle->tag_slot.slot_generation == member->tag_slot_generation
		   && handle->membership_slot.slot_index == member_ref.slot_index
		   && handle->membership_slot.slot_generation == member_ref.slot_generation
		   && handle->local_sequence == member->local_sequence && handle->local_round == round
		   && handle->role == member->role;
}


/* Allocator cleanup normally reuses the execution-round validator above.  A
 * final terminal RETIRE is the one exception: its local-lock transaction has
 * already advanced the tag to the next round while the old member remains
 * DETACHING until allocator cleanup.  Revalidate only the immutable locator
 * facts for that exact tombstone; applying current-round semantics to it would
 * misclassify every successful close as corruption. */
static bool
pcm_x_local_detaching_handle_exact(const PcmXLocalHandle *handle, PcmXSlotRef member_ref,
								   const PcmXLocalMembershipSlot *member,
								   const PcmXLocalTagSlot *tag_slot)
{
	return handle != NULL && member != NULL && tag_slot != NULL
		   && BufferTagsEqual(&handle->identity.tag, &tag_slot->tag)
		   && handle->identity.cluster_epoch == tag_slot->cluster_epoch
		   && pcm_x_wait_identity_equal(&handle->identity, &member->identity)
		   && handle->tag_slot.slot_index == member->tag_slot_index
		   && handle->tag_slot.slot_generation == member->tag_slot_generation
		   && handle->membership_slot.slot_index == member_ref.slot_index
		   && handle->membership_slot.slot_generation == member_ref.slot_generation
		   && handle->local_sequence == member->local_sequence
		   && handle->local_round == member->admitted_round && handle->role == member->role;
}


/* A revoke barrier freezes one immutable cohort; it does not stop that
 * cohort from draining in FIFO order.  Only post-cutoff members wait for
 * exact round retirement. */
static PcmXQueueResult
pcm_x_local_execution_round_locked(const PcmXLocalTagSlot *tag_slot,
								   const PcmXLocalMembershipSlot *member)
{
	uint32 flags = pcm_x_slot_flags_read(&tag_slot->slot);

	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0)
		return member->admitted_round == tag_slot->local_round ? PCM_X_QUEUE_OK
															   : PCM_X_QUEUE_CORRUPT;
	if (tag_slot->local_round == UINT32_MAX
		|| (tag_slot->cutoff_sequence == 0 && tag_slot->closed_round_member_count != 0))
		return PCM_X_QUEUE_CORRUPT;
	if (member->admitted_round == tag_slot->local_round)
		return member->local_sequence <= tag_slot->cutoff_sequence ? PCM_X_QUEUE_OK
																   : PCM_X_QUEUE_CORRUPT;
	if (member->admitted_round == tag_slot->local_round + 1)
		return member->local_sequence > tag_slot->cutoff_sequence ? PCM_X_QUEUE_BARRIER_CLOSED
																  : PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_CORRUPT;
}


static void
pcm_x_local_holder_handle_clear(PcmXLocalHolderHandle *handle)
{
	if (handle == NULL)
		return;
	memset(handle, 0, sizeof(*handle));
	handle->tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	handle->holder_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


static void
pcm_x_local_holder_snapshot_clear(PcmXLocalHolderSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
}


static bool
pcm_x_local_holder_key_valid(const PcmXLocalHolderKey *key)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;

	return header != NULL && key != NULL && key->reserved == 0
		   && pcm_x_wait_identity_valid(&key->identity) && key->buffer_id >= 0
		   && (Size)key->buffer_id < header->layout.n_buffers;
}


static bool
pcm_x_local_holder_key_equal(const PcmXLocalHolderKey *left, const PcmXLocalHolderKey *right)
{
	return left != NULL && right != NULL && left->reserved == 0 && right->reserved == 0
		   && left->buffer_id == right->buffer_id
		   && pcm_x_wait_identity_equal(&left->identity, &right->identity);
}


static bool
pcm_x_local_holder_state_is_occupancy(uint32 state)
{
	return state < 32 && (PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES & PCM_X_STATE_BIT(state)) != 0;
}


static void
pcm_x_local_holder_handle_from_member(PcmXLocalHolderHandle *handle, PcmXSlotRef tag_ref,
									  PcmXSlotRef holder_ref, const PcmXLocalMembershipSlot *member)
{
	pcm_x_local_holder_handle_clear(handle);
	handle->key.identity = member->identity;
	handle->key.buffer_id = member->buffer_id;
	handle->tag_slot = tag_ref;
	handle->holder_slot = holder_ref;
}


static bool
pcm_x_local_holder_handle_exact(const PcmXLocalHolderHandle *handle, PcmXSlotRef tag_ref,
								PcmXSlotRef holder_ref, const PcmXLocalMembershipSlot *member)
{
	PcmXLocalHolderKey resident;

	if (handle == NULL || member == NULL || handle->tag_slot.slot_index != tag_ref.slot_index
		|| handle->tag_slot.slot_generation != tag_ref.slot_generation
		|| handle->holder_slot.slot_index != holder_ref.slot_index
		|| handle->holder_slot.slot_generation != holder_ref.slot_generation
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| member->partition != PCM_X_LOCAL_PARTITION_HOLDER)
		return false;
	memset(&resident, 0, sizeof(resident));
	resident.identity = member->identity;
	resident.buffer_id = member->buffer_id;
	return pcm_x_local_holder_key_equal(&handle->key, &resident);
}


static void
pcm_x_local_tag_init_common(PcmXLocalTagSlot *tag_slot, const BufferTag *tag, uint64 cluster_epoch,
							int32 master_node, uint64 master_session_incarnation)
{
	tag_slot->tag = *tag;
	tag_slot->master_node = master_node;
	tag_slot->cluster_epoch = cluster_epoch;
	tag_slot->master_session_incarnation = master_session_incarnation;
	tag_slot->next_sequence = 1;
	tag_slot->writer_claim_generation = 0;
	tag_slot->head_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->tail_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->active_writer_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->active_holder_head_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->blocker_snapshot_head_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->blocker_snapshot_count = 0;
	tag_slot->blocker_snapshot_crc32c = 0;
	tag_slot->blocker_snapshot_next_chunk = 0;
	tag_slot->local_round = 1;
	tag_slot->membership_count = 0;
	tag_slot->closed_round_member_count = 0;
	tag_slot->terminal_drain_generation = 0;
	tag_slot->committed_own_generation = 0;
	tag_slot->grant_base_own_generation = 0;
	memset(&tag_slot->holder_ref, 0, sizeof(tag_slot->holder_ref));
	memset(&tag_slot->holder_image, 0, sizeof(tag_slot->holder_image));
	memset(&tag_slot->holder_reliable, 0, sizeof(tag_slot->holder_reliable));
	tag_slot->holder_terminal_drain_generation = 0;
	memset(&tag_slot->blocker_snapshot_ref, 0, sizeof(tag_slot->blocker_snapshot_ref));
	memset(&tag_slot->blocker_snapshot_reliable, 0, sizeof(tag_slot->blocker_snapshot_reliable));
	pcm_x_slot_flags_write(&tag_slot->slot, PCM_X_LOCAL_TAG_F_ADMISSION_GATE);
}


static bool
pcm_x_local_holder_stage_rollback_locked(bool new_tag, bool tag_directory_published,
										 const PcmXLocalHolderKey *key, PcmXSlotRef tag_ref,
										 PcmXLocalMembershipSlot *holder, PcmXSlotRef holder_ref)
{
	bool ok = true;

	if (holder != NULL
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_HOLDER, holder_ref,
										  PCM_X_SLOT_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	if (tag_directory_published
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, &key->identity.tag, tag_ref)
			   != PCM_X_DIRECTORY_OK)
		ok = false;
	if (new_tag
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
										  PCM_X_TAG_RESERVED_NONVISIBLE)
			   != PCM_X_ALLOC_OK)
		ok = false;
	return ok;
}


static PcmXLocalMembershipSlot *
pcm_x_local_holder_slot_resolve(const BufferTag *tag, PcmXSlotRef tag_ref, Size holder_index,
								PcmXSlotRef *holder_ref_out)
{
	PcmXAllocatorView holder_view;
	PcmXLocalMembershipSlot *holder;
	uint64 generation;

	if (holder_ref_out == NULL || !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_HOLDER, &holder_view))
		return NULL;
	holder = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&holder_view, holder_index);
	if (holder == NULL || !pcm_x_slot_generation_read(&holder->slot, &generation))
		return NULL;
	holder_ref_out->slot_index = holder_index;
	holder_ref_out->slot_generation = generation;
	holder = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_HOLDER, *holder_ref_out,
														  tag, PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES);
	if (holder == NULL || holder->partition != PCM_X_LOCAL_PARTITION_HOLDER
		|| holder->tag_slot_index != tag_ref.slot_index
		|| holder->tag_slot_generation != tag_ref.slot_generation || holder->buffer_id < 0
		|| (Size)holder->buffer_id >= ClusterPcmXConvertShmem->layout.n_buffers
		|| !pcm_x_wait_identity_valid(&holder->identity))
		return NULL;
	return holder;
}


/* Resolve one linked holder under the BufferTag local-domain lock. */
static PcmXLocalMembershipSlot *
pcm_x_local_holder_link_resolve(const BufferTag *tag, PcmXSlotRef tag_ref, Size holder_index,
								Size expected_previous, PcmXSlotRef *holder_ref_out)
{
	PcmXLocalMembershipSlot *holder;

	holder = pcm_x_local_holder_slot_resolve(tag, tag_ref, holder_index, holder_ref_out);
	if (holder == NULL || holder->prev_index != expected_previous)
		return NULL;
	return holder;
}


static bool
pcm_x_local_writer_claim_well_formed(const PcmXLocalWriterClaim *claim)
{
	return claim != NULL && claim->flags == 0 && claim->writer.flags == 0
		   && claim->active_slot.slot_index != PCM_X_INVALID_SLOT_INDEX
		   && claim->active_slot.slot_generation != 0 && claim->claim_generation != 0
		   && claim->local_round != 0 && claim->role == claim->writer.role
		   && (claim->role == PCM_X_LOCAL_ROLE_NODE_LEADER
			   || claim->role == PCM_X_LOCAL_ROLE_FOLLOWER)
		   && pcm_x_wait_identity_valid(&claim->writer.identity);
}


static bool
pcm_x_local_writer_holder_key_bound(const PcmXLocalHolderKey *key,
									const PcmXLocalWriterClaim *claim)
{
	return BufferTagsEqual(&key->identity.tag, &claim->writer.identity.tag)
		   && key->identity.node_id == claim->writer.identity.node_id
		   && key->identity.procno == claim->writer.identity.procno
		   && key->identity.xid == claim->writer.identity.xid
		   && key->identity.cluster_epoch == claim->writer.identity.cluster_epoch;
}


static PcmXQueueResult
pcm_x_local_writer_holder_authority_check(const PcmXLocalHolderKey *key,
										  const PcmXLocalWriterClaim *claim, PcmXSlotRef tag_ref,
										  PcmXLocalTagSlot *tag_slot, PcmXSlotRef member_ref,
										  PcmXLocalMembershipSlot *member, PcmXSlotRef leader_ref,
										  PcmXLocalMembershipSlot *leader,
										  uint64 *committed_own_generation_out)
{
	PcmXQueueResult result;
	uint64 expected_generation;
	uint32 leader_state;
	uint32 member_state;
	uint32 tag_flags;

	*committed_own_generation_out = 0;
	if (!pcm_x_local_writer_holder_key_bound(key, claim))
		return PCM_X_QUEUE_STALE;
	if (member == NULL || !pcm_x_local_handle_exact(&claim->writer, member_ref, member, tag_slot))
		return PCM_X_QUEUE_STALE;
	if (claim->writer.tag_slot.slot_index != tag_ref.slot_index
		|| claim->writer.tag_slot.slot_generation != tag_ref.slot_generation
		|| claim->writer.membership_slot.slot_index != member_ref.slot_index
		|| claim->writer.membership_slot.slot_generation != member_ref.slot_generation
		|| claim->claim_generation != tag_slot->writer_claim_generation
		|| tag_slot->active_writer_index != member_ref.slot_index
		|| tag_slot->active_writer_slot_generation != member_ref.slot_generation
		|| claim->local_round != member->admitted_round || claim->role != member->role
		|| (pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0)
		return PCM_X_QUEUE_STALE;
	result = pcm_x_local_execution_round_locked(tag_slot, member);
	if (result != PCM_X_QUEUE_OK)
		return result == PCM_X_QUEUE_CORRUPT ? result : PCM_X_QUEUE_STALE;
	tag_flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((tag_flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (leader == NULL || leader->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| leader->tag_slot_index != tag_ref.slot_index
		|| leader->tag_slot_generation != tag_ref.slot_generation
		|| leader->admitted_round != tag_slot->local_round
		|| tag_slot->head_index != leader_ref.slot_index)
		return PCM_X_QUEUE_CORRUPT;
	member_state = pcm_x_slot_state_read(&member->slot);
	if ((member->role == PCM_X_LOCAL_ROLE_NODE_LEADER
		 && (member_ref.slot_index != leader_ref.slot_index
			 || member_ref.slot_generation != leader_ref.slot_generation))
		|| (member->role == PCM_X_LOCAL_ROLE_FOLLOWER
			&& (member_state != PCM_XL_JOINED_NONWAITABLE || member->graph_generation != 0)))
		return PCM_X_QUEUE_STALE;
	leader_state = pcm_x_slot_state_read(&leader->slot);
	if (leader_state != PCM_XL_GRANTED)
		return PCM_X_QUEUE_NOT_READY;
	if (member->role == PCM_X_LOCAL_ROLE_NODE_LEADER && member_state != PCM_XL_GRANTED)
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->ref.handle.ticket_id == 0 || tag_slot->ref.handle.queue_generation == 0
		|| tag_slot->ref.grant_generation == 0
		|| !pcm_x_wait_identity_equal(&tag_slot->ref.identity, &leader->identity)
		|| tag_slot->committed_own_generation == 0
		|| !cluster_pcm_x_generation_next(pcm_x_local_effective_grant_base(tag_slot),
										  &expected_generation)
		|| tag_slot->committed_own_generation != expected_generation)
		return PCM_X_QUEUE_CORRUPT;
	if (key->identity.base_own_generation != tag_slot->committed_own_generation)
		return PCM_X_QUEUE_STALE;
	*committed_own_generation_out = tag_slot->committed_own_generation;
	return PCM_X_QUEUE_OK;
}


/* Validate the active writer while allocator ownership prevents slot reuse. */
static PcmXQueueResult
pcm_x_local_writer_holder_authority_allocator_locked(const PcmXLocalHolderKey *key,
													 const PcmXLocalWriterClaim *claim,
													 PcmXSlotRef tag_ref,
													 PcmXLocalTagSlot *tag_slot,
													 uint64 *committed_own_generation_out)
{
	PcmXLocalMembershipSlot *leader;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef leader_ref;
	PcmXSlotRef member_ref = claim->active_slot;

	*committed_own_generation_out = 0;
	if ((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->leader_slot_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	leader_ref.slot_index = tag_slot->leader_index;
	leader_ref.slot_generation = tag_slot->leader_slot_generation;
	member = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_WAIT,
																	  member_ref);
	leader = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_WAIT,
																	  leader_ref);
	return pcm_x_local_writer_holder_authority_check(key, claim, tag_ref, tag_slot, member_ref,
													 member, leader_ref, leader,
													 committed_own_generation_out);
}


/* Revalidate the same proof in the local-domain link transaction. */
static PcmXQueueResult
pcm_x_local_writer_holder_authority_domain_locked(const PcmXLocalHolderKey *key,
												  const PcmXLocalWriterClaim *claim,
												  PcmXSlotRef tag_ref, PcmXLocalTagSlot *tag_slot,
												  uint64 *committed_own_generation_out)
{
	PcmXLocalMembershipSlot *leader;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef leader_ref;
	PcmXSlotRef member_ref = claim->active_slot;

	*committed_own_generation_out = 0;
	if ((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->leader_slot_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	leader_ref.slot_index = tag_slot->leader_index;
	leader_ref.slot_generation = tag_slot->leader_slot_generation;
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &key->identity.tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	leader = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, leader_ref, &key->identity.tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	return pcm_x_local_writer_holder_authority_check(key, claim, tag_ref, tag_slot, member_ref,
													 member, leader_ref, leader,
													 committed_own_generation_out);
}


/*
 * Prepare one prospective content-lock holder before taking the content lock.
 * Allocator, directory, and local-domain publication all complete here; the
 * linked ACQUIRING slot is already barrier occupancy before this returns.
 */
static PcmXQueueResult
pcm_x_local_holder_register_common(const PcmXLocalHolderKey *key,
								   const PcmXLocalWriterClaim *writer_claim,
								   PcmXLocalHolderHandle *handle_out,
								   uint64 *committed_own_generation_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot = NULL;
	PcmXLocalMembershipSlot *holder = NULL;
	PcmXSlotHeader *raw_slot;
	PcmXSlotRef tag_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef holder_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef existing;
	PcmXDirectoryResult directory_result;
	PcmXAllocatorResult allocator_result;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	uint64 committed_own_generation = 0;
	uint64 next_holder_generation;
	uint32 partition;
	bool new_tag = false;
	bool writer_authorized = false;
	bool gate_claimed = false;
	bool tag_directory_published = false;
	bool fail_closed = false;

	pcm_x_local_holder_handle_clear(handle_out);
	if (committed_own_generation_out != NULL)
		*committed_own_generation_out = 0;
	if (header == NULL || handle_out == NULL || !pcm_x_local_holder_key_valid(key)
		|| (writer_claim != NULL
			&& (committed_own_generation_out == NULL
				|| !pcm_x_local_writer_claim_well_formed(writer_claim))))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	if (writer_claim != NULL) {
		directory_result
			= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &key->identity.tag, &tag_ref);
		if (directory_result != PCM_X_DIRECTORY_OK) {
			result = directory_result == PCM_X_DIRECTORY_NOT_FOUND
						 ? PCM_X_QUEUE_STALE
						 : pcm_x_queue_result_from_directory(directory_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		tag_slot
			= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
		result = pcm_x_resident_tag_state_result(tag_slot == NULL ? NULL : &tag_slot->slot);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		if (!BufferTagsEqual(&tag_slot->tag, &key->identity.tag)
			|| tag_slot->cluster_epoch != key->identity.cluster_epoch) {
			result = PCM_X_QUEUE_STALE;
			goto allocator_done;
		}
		result = pcm_x_local_gate_try_acquire(tag_slot);
		if (result != PCM_X_QUEUE_OK) {
			if (result == PCM_X_QUEUE_BUSY)
				result = PCM_X_QUEUE_GATE_RETRY;
			else
				fail_closed = true;
			goto allocator_done;
		}
		gate_claimed = true;
		result = pcm_x_local_writer_holder_authority_allocator_locked(
			key, writer_claim, tag_ref, tag_slot, &committed_own_generation);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		writer_authorized = true;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_HOLDER, key, &holder_ref);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		uint32 holder_state;

		holder = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_HOLDER,
																		  holder_ref);
		if (holder == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		holder_state = pcm_x_slot_state_read(&holder->slot);
		if (holder_state == PCM_X_SLOT_RESERVED_NONVISIBLE || holder_state == PCM_XL_DETACHING) {
			result = PCM_X_QUEUE_GATE_RETRY;
			goto allocator_done;
		}
		tag_ref.slot_index = holder->tag_slot_index;
		tag_ref.slot_generation = holder->tag_slot_generation;
		if (!pcm_x_local_holder_state_is_occupancy(holder_state)
			|| !pcm_x_local_holder_handle_exact(
				&(PcmXLocalHolderHandle){ .key = *key,
										  .tag_slot = tag_ref,
										  .holder_slot = holder_ref },
				tag_ref, holder_ref, holder)
			|| pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &key->identity.tag, &existing)
				   != PCM_X_DIRECTORY_OK
			|| existing.slot_index != tag_ref.slot_index
			|| existing.slot_generation != tag_ref.slot_generation) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		pcm_x_local_holder_handle_from_member(handle_out, tag_ref, holder_ref, holder);
		if (writer_authorized)
			*committed_own_generation_out = committed_own_generation;
		result = PCM_X_QUEUE_DUPLICATE;
		goto allocator_done;
	}
	if (directory_result != PCM_X_DIRECTORY_NOT_FOUND) {
		result = pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto allocator_done;
	}
	result = pcm_x_local_retire_episode_state_locked(header);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_BUSY)
			result = PCM_X_QUEUE_GATE_RETRY;
		else
			fail_closed = true;
		goto allocator_done;
	}

	if (!writer_authorized) {
		directory_result
			= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &key->identity.tag, &tag_ref);
		if (directory_result == PCM_X_DIRECTORY_OK) {
			tag_slot
				= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
			result = pcm_x_resident_tag_state_result(tag_slot == NULL ? NULL : &tag_slot->slot);
			if (result != PCM_X_QUEUE_OK) {
				fail_closed = result == PCM_X_QUEUE_CORRUPT;
				goto allocator_done;
			}
			if (!BufferTagsEqual(&tag_slot->tag, &key->identity.tag)
				|| tag_slot->cluster_epoch != key->identity.cluster_epoch) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto allocator_done;
			}
			result = pcm_x_local_gate_try_acquire(tag_slot);
			if (result != PCM_X_QUEUE_OK) {
				if (result == PCM_X_QUEUE_BUSY)
					result = PCM_X_QUEUE_GATE_RETRY;
				else
					fail_closed = true;
				goto allocator_done;
			}
			gate_claimed = true;
			if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0) {
				result = PCM_X_QUEUE_BARRIER_CLOSED;
				goto allocator_done;
			}
		} else if (directory_result == PCM_X_DIRECTORY_NOT_FOUND) {
			allocator_result
				= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_LOCAL_TAG, &tag_ref, &raw_slot);
			if (allocator_result != PCM_X_ALLOC_OK) {
				result = pcm_x_queue_result_from_allocator(allocator_result);
				fail_closed = result == PCM_X_QUEUE_CORRUPT;
				goto allocator_done;
			}
			new_tag = true;
			gate_claimed = true;
			tag_slot = (PcmXLocalTagSlot *)raw_slot;
			pcm_x_local_tag_init_common(tag_slot, &key->identity.tag, key->identity.cluster_epoch,
										-1, 0);
		} else {
			result = pcm_x_queue_result_from_directory(directory_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
	}
	if (!cluster_pcm_x_generation_next(tag_slot->holder_set_generation, &next_holder_generation)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto allocator_done;
	}

	allocator_result
		= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_LOCAL_HOLDER, &holder_ref, &raw_slot);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		if (new_tag
			&& !pcm_x_local_holder_stage_rollback_locked(true, false, key, tag_ref, NULL,
														 holder_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	holder = (PcmXLocalMembershipSlot *)raw_slot;
	holder->identity = key->identity;
	holder->tag_slot_index = tag_ref.slot_index;
	holder->tag_slot_generation = tag_ref.slot_generation;
	holder->next_index = PCM_X_INVALID_SLOT_INDEX;
	holder->prev_index = PCM_X_INVALID_SLOT_INDEX;
	holder->buffer_id = key->buffer_id;
	holder->role = PCM_X_LOCAL_ROLE_NONE;
	holder->partition = PCM_X_LOCAL_PARTITION_HOLDER;

	if (new_tag) {
		directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_TAG, &key->identity.tag,
														 tag_ref, &existing);
		if (directory_result != PCM_X_DIRECTORY_OK) {
			result = directory_result == PCM_X_DIRECTORY_EXISTS
						 ? PCM_X_QUEUE_CORRUPT
						 : pcm_x_queue_result_from_directory(directory_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			if (!pcm_x_local_holder_stage_rollback_locked(true, false, key, tag_ref, holder,
														  holder_ref)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
			}
			goto allocator_done;
		}
		tag_directory_published = true;
	}
	directory_result
		= pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_HOLDER, key, holder_ref, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = directory_result == PCM_X_DIRECTORY_EXISTS
					 ? PCM_X_QUEUE_CORRUPT
					 : pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		if (!pcm_x_local_holder_stage_rollback_locked(new_tag, tag_directory_published, key,
													  tag_ref, holder, holder_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	result = PCM_X_QUEUE_OK;

allocator_done:
	if (result != PCM_X_QUEUE_OK && gate_claimed && !new_tag) {
		if (fail_closed) {
			result = PCM_X_QUEUE_CORRUPT;
		} else if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&key->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, tag_slot);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &key->identity.tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	holder = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_HOLDER, holder_ref, &key->identity.tag,
		PCM_X_STATE_BIT(PCM_X_SLOT_RESERVED_NONVISIBLE));
	if (tag_slot == NULL || holder == NULL
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| (!writer_authorized
			&& (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0)
		|| holder->partition != PCM_X_LOCAL_PARTITION_HOLDER
		|| holder->tag_slot_index != tag_ref.slot_index
		|| holder->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_local_holder_key_equal(
			key,
			&(PcmXLocalHolderKey){ .identity = holder->identity, .buffer_id = holder->buffer_id })
		|| tag_slot->holder_set_generation == UINT64_MAX) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (writer_authorized) {
		uint64 revalidated_generation = 0;

		result = pcm_x_local_writer_holder_authority_domain_locked(
			key, writer_claim, tag_ref, tag_slot, &revalidated_generation);
		if (result != PCM_X_QUEUE_OK || revalidated_generation != committed_own_generation) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	}
	if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX) {
		PcmXLocalMembershipSlot *head;
		PcmXSlotRef head_ref;

		head = pcm_x_local_holder_link_resolve(&key->identity.tag, tag_ref,
											   tag_slot->active_holder_head_index,
											   PCM_X_INVALID_SLOT_INDEX, &head_ref);
		if (head == NULL || head_ref.slot_index == holder_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		holder->next_index = head_ref.slot_index;
		head->prev_index = holder_ref.slot_index;
	}
	tag_slot->active_holder_head_index = holder_ref.slot_index;
	tag_slot->holder_set_generation = next_holder_generation;
	pg_write_barrier();
	pcm_x_slot_state_write(&holder->slot, PCM_XL_HOLDER_ACQUIRING);
	if (new_tag)
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
	pcm_x_local_holder_handle_from_member(handle_out, tag_ref, holder_ref, holder);
	if (writer_authorized)
		*committed_own_generation_out = committed_own_generation;
	if (!pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	result = PCM_X_QUEUE_OK;

domain_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_register(const PcmXLocalHolderKey *key,
									PcmXLocalHolderHandle *handle_out)
{
	return pcm_x_local_holder_register_common(key, NULL, handle_out, NULL);
}


PcmXQueueResult
cluster_pcm_x_local_writer_holder_register_exact(const PcmXLocalHolderKey *key,
												 const PcmXLocalWriterClaim *claim,
												 PcmXLocalHolderHandle *handle_out,
												 uint64 *committed_own_generation_out)
{
	return pcm_x_local_holder_register_common(key, claim, handle_out, committed_own_generation_out);
}


/*
 * Content-window W1.  All addressed fields are immutable between ACQUIRING
 * publication and post-RELEASING unlink.  The generation seqlock fences slot
 * reuse; the state CAS is the only shared mutation here.  In particular this
 * path must never acquire a PCM-X LWLock or promote a retry to fail-closed.
 */
static PcmXQueueResult
pcm_x_local_holder_transition_exact(const PcmXLocalHolderHandle *handle, uint32 expected_state,
									uint32 desired_state)
{
	PcmXAllocatorView holder_view;
	PcmXLocalMembershipSlot *holder;
	uint64 generation;
	uint32 current;
	uint32 desired;
	uint32 state;

	if (ClusterPcmXConvertShmem == NULL || handle == NULL
		|| !pcm_x_local_holder_key_valid(&handle->key)
		|| handle->tag_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| handle->holder_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_HOLDER, &holder_view))
		return PCM_X_QUEUE_INVALID;
	holder = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&holder_view,
															 handle->holder_slot.slot_index);
	if (holder == NULL)
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_slot_generation_read(&holder->slot, &generation))
		return PCM_X_QUEUE_BUSY;
	if (generation != handle->holder_slot.slot_generation)
		return PCM_X_QUEUE_STALE;
	state = pcm_x_slot_state_read(&holder->slot);
	if (state != expected_state && state != desired_state)
		return PCM_X_QUEUE_BAD_STATE;
	pg_read_barrier();
	if (!pcm_x_local_holder_handle_exact(handle, handle->tag_slot, handle->holder_slot, holder))
		return PCM_X_QUEUE_CORRUPT;
	pg_read_barrier();
	if (!pcm_x_slot_generation_read(&holder->slot, &generation))
		return PCM_X_QUEUE_BUSY;
	if (generation != handle->holder_slot.slot_generation)
		return PCM_X_QUEUE_STALE;

	current = pg_atomic_read_u32(&holder->slot.state_flags);
	for (;;) {
		state = current & PCM_X_SLOT_STATE_MASK;
		if (state == desired_state)
			return PCM_X_QUEUE_DUPLICATE;
		if (state != expected_state)
			return PCM_X_QUEUE_BAD_STATE;
		desired = (current & PCM_X_SLOT_FLAGS_MASK) | desired_state;
		pg_write_barrier();
		if (pg_atomic_compare_exchange_u32(&holder->slot.state_flags, &current, desired))
			return PCM_X_QUEUE_OK;
	}
}


PcmXQueueResult
cluster_pcm_x_local_holder_activate_exact(const PcmXLocalHolderHandle *handle)
{
	return pcm_x_local_holder_transition_exact(handle, PCM_XL_HOLDER_ACQUIRING,
											   PCM_XL_HOLDER_ACTIVE);
}


PcmXQueueResult
cluster_pcm_x_local_holder_mark_releasing_exact(const PcmXLocalHolderHandle *handle)
{
	return pcm_x_local_holder_transition_exact(handle, PCM_XL_HOLDER_ACTIVE,
											   PCM_XL_HOLDER_RELEASING);
}


static PcmXQueueResult
pcm_x_local_holder_claim_detaching(PcmXLocalMembershipSlot *holder, uint32 allowed_holder_states)
{
	uint32 current;
	uint32 desired;
	uint32 holder_state;

	if (holder == NULL || allowed_holder_states == 0
		|| (allowed_holder_states & ~PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES) != 0)
		return PCM_X_QUEUE_INVALID;
	current = pg_atomic_read_u32(&holder->slot.state_flags);
	for (;;) {
		holder_state = current & PCM_X_SLOT_STATE_MASK;
		if (!pcm_x_local_holder_state_is_occupancy(holder_state)
			|| (allowed_holder_states & PCM_X_STATE_BIT(holder_state)) == 0)
			return pcm_x_local_holder_state_is_occupancy(holder_state) ? PCM_X_QUEUE_BAD_STATE
																	   : PCM_X_QUEUE_CORRUPT;
		desired = (current & PCM_X_SLOT_FLAGS_MASK) | PCM_XL_DETACHING;
		pg_write_barrier();
		if (pg_atomic_compare_exchange_u32(&holder->slot.state_flags, &current, desired))
			return PCM_X_QUEUE_OK;
	}
}


static PcmXQueueResult
pcm_x_local_holder_detach_exact(const PcmXLocalHolderHandle *handle, uint32 allowed_holder_states)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *gated_tag = NULL;
	PcmXLocalMembershipSlot *holder;
	PcmXLocalMembershipSlot *previous = NULL;
	PcmXLocalMembershipSlot *next = NULL;
	PcmXImageToken zero_image;
	PcmXSlotRef found;
	PcmXSlotRef tag_ref;
	PcmXSlotRef holder_ref;
	PcmXSlotRef previous_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef next_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint64 next_holder_generation;
	uint32 flags;
	uint32 holder_state;
	uint32 partition;
	bool detach_tag = false;
	bool gate_claimed = false;
	bool last_holder;

	memset(&zero_image, 0, sizeof(zero_image));

	if (allowed_holder_states == 0
		|| (allowed_holder_states & ~PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES) != 0 || header == NULL
		|| handle == NULL || !pcm_x_local_holder_key_valid(&handle->key)
		|| handle->tag_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| handle->holder_slot.slot_index == PCM_X_INVALID_SLOT_INDEX)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	/* The exact runtime token is the authority linearization point for both
	 * normal unregister and exceptional owner cleanup.  A fenced runtime must
	 * preserve the directory, holder slot, and tag links byte-for-byte for
	 * deferred recovery; a close ordered after this point may let the already
	 * authorized cross-lock detach finish or retain DETACHING evidence. */
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		LWLockRelease(&header->allocator_lock.lock);
		return PCM_X_QUEUE_NOT_READY;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_HOLDER, &handle->key, &found);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND) {
		LWLockRelease(&header->allocator_lock.lock);
		return PCM_X_QUEUE_NOT_FOUND;
	}
	if (directory_result != PCM_X_DIRECTORY_OK) {
		LWLockRelease(&header->allocator_lock.lock);
		return pcm_x_queue_result_from_directory(directory_result);
	}
	if (found.slot_index != handle->holder_slot.slot_index
		|| found.slot_generation != handle->holder_slot.slot_generation) {
		LWLockRelease(&header->allocator_lock.lock);
		return PCM_X_QUEUE_STALE;
	}
	holder_ref = found;
	holder = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_HOLDER,
																	  holder_ref);
	if (holder == NULL) {
		LWLockRelease(&header->allocator_lock.lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	holder_state = pcm_x_slot_state_read(&holder->slot);
	if (holder_state == PCM_XL_DETACHING) {
		LWLockRelease(&header->allocator_lock.lock);
		return PCM_X_QUEUE_BUSY;
	}
	tag_ref.slot_index = holder->tag_slot_index;
	tag_ref.slot_generation = holder->tag_slot_generation;
	if (!pcm_x_local_holder_handle_exact(handle, tag_ref, holder_ref, holder)
		|| pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &handle->key.identity.tag, &found)
			   != PCM_X_DIRECTORY_OK
		|| found.slot_index != tag_ref.slot_index
		|| found.slot_generation != tag_ref.slot_generation) {
		LWLockRelease(&header->allocator_lock.lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (!pcm_x_local_holder_state_is_occupancy(holder_state)
		|| (allowed_holder_states & PCM_X_STATE_BIT(holder_state)) == 0) {
		LWLockRelease(&header->allocator_lock.lock);
		if (pcm_x_local_holder_state_is_occupancy(holder_state))
			return PCM_X_QUEUE_BAD_STATE;
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| !BufferTagsEqual(&tag_slot->tag, &handle->key.identity.tag)
		|| tag_slot->cluster_epoch != handle->key.identity.cluster_epoch) {
		LWLockRelease(&header->allocator_lock.lock);
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	result = pcm_x_local_handoff_gate_claim_locked(header, tag_slot, false, PCM_X_QUEUE_GATE_RETRY);
	if (result != PCM_X_QUEUE_OK) {
		LWLockRelease(&header->allocator_lock.lock);
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}
	gate_claimed = true;
	gated_tag = tag_slot;
	LWLockRelease(&header->allocator_lock.lock);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&handle->key.identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, gated_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &handle->key.identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	holder = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_HOLDER, holder_ref,
														  &handle->key.identity.tag,
														  PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES);
	if (tag_slot == NULL || holder == NULL
		|| !pcm_x_local_holder_handle_exact(handle, tag_ref, holder_ref, holder)) {
		result = PCM_X_QUEUE_STALE;
		goto domain_done;
	}
	holder_state = pcm_x_slot_state_read(&holder->slot);
	if (!pcm_x_local_holder_state_is_occupancy(holder_state)
		|| (allowed_holder_states & PCM_X_STATE_BIT(holder_state)) == 0) {
		result = pcm_x_local_holder_state_is_occupancy(holder_state) ? PCM_X_QUEUE_BAD_STATE
																	 : PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (!cluster_pcm_x_generation_next(tag_slot->holder_set_generation, &next_holder_generation)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (holder->prev_index == holder_ref.slot_index || holder->next_index == holder_ref.slot_index
		|| (holder->prev_index != PCM_X_INVALID_SLOT_INDEX
			&& holder->prev_index == holder->next_index)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (holder->prev_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->active_holder_head_index != holder_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	} else {
		previous = pcm_x_local_holder_slot_resolve(&handle->key.identity.tag, tag_ref,
												   holder->prev_index, &previous_ref);
		if (previous == NULL || previous->next_index != holder_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	}
	if (holder->next_index != PCM_X_INVALID_SLOT_INDEX) {
		next
			= pcm_x_local_holder_link_resolve(&handle->key.identity.tag, tag_ref,
											  holder->next_index, holder_ref.slot_index, &next_ref);
		if (next == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
	}
	last_holder = holder->prev_index == PCM_X_INVALID_SLOT_INDEX
				  && holder->next_index == PCM_X_INVALID_SLOT_INDEX;
	if (last_holder && tag_slot->membership_count == 0) {
		flags = pcm_x_slot_flags_read(&tag_slot->slot);
		if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->closed_round_member_count != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		/* A holder may leave after PROBE has frozen the tag.  Reclaim only a
		 * completely pristine holder-only tag; otherwise preserve the durable
		 * barrier/transfer evidence for exact ACK, DRAIN, and RETIRE. */
		detach_tag
			= (flags
			   & (PCM_X_LOCAL_TAG_F_REVOKE_BARRIER | PCM_X_LOCAL_TAG_F_TERMINAL_MASK
				  | PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK))
				  == 0
			  && pcm_x_ticket_ref_is_zero(&tag_slot->ref)
			  && pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
			  && pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
			  && pcm_x_transfer_leg_is_clear(&tag_slot->reliable)
			  && pcm_x_image_token_equal(&tag_slot->holder_image, &zero_image)
			  && pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable)
			  && tag_slot->holder_terminal_drain_generation == 0
			  && tag_slot->blocker_snapshot_head_index == PCM_X_INVALID_SLOT_INDEX
			  && tag_slot->blocker_snapshot_count == 0 && tag_slot->blocker_snapshot_crc32c == 0
			  && tag_slot->blocker_snapshot_next_chunk == 0 && tag_slot->blocker_set_generation == 0
			  && pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable);
	}
	result = pcm_x_local_holder_claim_detaching(holder, allowed_holder_states);
	if (result != PCM_X_QUEUE_OK)
		goto domain_done;
	if (previous == NULL)
		tag_slot->active_holder_head_index = holder->next_index;
	else
		previous->next_index = holder->next_index;
	if (next != NULL)
		next->prev_index = holder->prev_index;
	tag_slot->holder_set_generation = next_holder_generation;
	if (detach_tag)
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
	holder->next_index = PCM_X_INVALID_SLOT_INDEX;
	holder->prev_index = PCM_X_INVALID_SLOT_INDEX;
	result = PCM_X_QUEUE_OK;

domain_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result != PCM_X_QUEUE_CORRUPT && gate_claimed) {
			if (!pcm_x_local_gate_release(gated_tag))
				result = PCM_X_QUEUE_CORRUPT;
			else
				gate_claimed = false;
		}
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, tag_slot);
	holder = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_HOLDER,
																	  holder_ref);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (holder == NULL || tag_slot == NULL
		|| pcm_x_slot_state_read(&holder->slot) != PCM_XL_DETACHING
		|| (detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_DETACHING)
		|| (!detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE)
		|| !pcm_x_local_holder_handle_exact(handle, tag_ref, holder_ref, holder)
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_HOLDER, &handle->key, &found)
			   != PCM_X_DIRECTORY_OK
		|| found.slot_index != holder_ref.slot_index
		|| found.slot_generation != holder_ref.slot_generation
		|| (detach_tag
			&& (pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &handle->key.identity.tag, &found)
					!= PCM_X_DIRECTORY_OK
				|| found.slot_index != tag_ref.slot_index
				|| found.slot_generation != tag_ref.slot_generation))) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_cleanup_done;
	}
	if (pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_HOLDER, &handle->key, holder_ref)
			!= PCM_X_DIRECTORY_OK
		|| (detach_tag
			&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, &handle->key.identity.tag,
												   tag_ref)
				   != PCM_X_DIRECTORY_OK)
		|| pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_HOLDER, holder_ref, PCM_XL_DETACHING)
			   != PCM_X_ALLOC_OK
		|| (detach_tag
			&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref, PCM_X_TAG_DETACHING)
				   != PCM_X_ALLOC_OK)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_cleanup_done;
	}
	if (!detach_tag && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto allocator_cleanup_done;
	}
	result = PCM_X_QUEUE_OK;

allocator_cleanup_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}

/* Error cleanup may run beneath a PG_CATCH.  Such a caller must first copy
 * and flush its original ErrorData (LockBuffer does this) so this wrapper's
 * FlushErrorState can discard only a nested detach failure.  Exact detach may
 * take its LWLocks once; on acquisition ERROR preserve every published
 * identity, close the runtime, and return CORRUPT without a second throw. */
static PcmXQueueResult
pcm_x_local_holder_cleanup_detach_exact(const PcmXLocalHolderHandle *handle,
										uint32 allowed_holder_states)
{
	volatile PcmXQueueResult result = PCM_X_QUEUE_CORRUPT;

	PG_TRY();
	{
		result = pcm_x_local_holder_detach_exact(handle, allowed_holder_states);
	}
	PG_CATCH();
	{
		FlushErrorState();
		pcm_x_runtime_fail_closed();
		result = PCM_X_QUEUE_CORRUPT;
	}
	PG_END_TRY();
	return (PcmXQueueResult)result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_abort_acquiring_exact(const PcmXLocalHolderHandle *handle)
{
	return pcm_x_local_holder_cleanup_detach_exact(handle,
												   PCM_X_STATE_BIT(PCM_XL_HOLDER_ACQUIRING));
}


PcmXQueueResult
cluster_pcm_x_local_holder_exceptional_detach_exact(const PcmXLocalHolderHandle *handle,
													LWLock *content_lock)
{
	/* This is a production guard, not an assertion: ACTIVE evidence may be
	 * removed only after error cleanup has actually released the content
	 * LWLock.  The saved exact handle, not the current BufferDesc mirror,
	 * provides tag/generation/slot ABA protection. */
	if (content_lock == NULL || LWLockHeldByMe(content_lock))
		return PCM_X_QUEUE_BAD_STATE;
	return pcm_x_local_holder_cleanup_detach_exact(handle, PCM_X_LOCAL_HOLDER_OCCUPANCY_STATES);
}


PcmXQueueResult
cluster_pcm_x_local_holder_unregister_exact(const PcmXLocalHolderHandle *handle)
{
	return pcm_x_local_holder_detach_exact(handle, PCM_X_STATE_BIT(PCM_XL_HOLDER_RELEASING));
}


static int
pcm_x_local_holder_handle_compare(const void *left_ptr, const void *right_ptr)
{
	const PcmXLocalHolderHandle *left = (const PcmXLocalHolderHandle *)left_ptr;
	const PcmXLocalHolderHandle *right = (const PcmXLocalHolderHandle *)right_ptr;

#define PCM_X_COMPARE_FIELD(field)                                                                 \
	do {                                                                                           \
		if (left->key.identity.field < right->key.identity.field)                                  \
			return -1;                                                                             \
		if (left->key.identity.field > right->key.identity.field)                                  \
			return 1;                                                                              \
	} while (0)
	PCM_X_COMPARE_FIELD(node_id);
	PCM_X_COMPARE_FIELD(procno);
	PCM_X_COMPARE_FIELD(cluster_epoch);
	PCM_X_COMPARE_FIELD(request_id);
	PCM_X_COMPARE_FIELD(wait_seq);
	PCM_X_COMPARE_FIELD(xid);
	PCM_X_COMPARE_FIELD(base_own_generation);
#undef PCM_X_COMPARE_FIELD
	if (left->key.buffer_id < right->key.buffer_id)
		return -1;
	if (left->key.buffer_id > right->key.buffer_id)
		return 1;
	if (left->holder_slot.slot_index < right->holder_slot.slot_index)
		return -1;
	if (left->holder_slot.slot_index > right->holder_slot.slot_index)
		return 1;
	if (left->holder_slot.slot_generation < right->holder_slot.slot_generation)
		return -1;
	if (left->holder_slot.slot_generation > right->holder_slot.slot_generation)
		return 1;
	return 0;
}


/* Caller holds the BufferTag local-domain lock.  The count is established
 * before any output byte is written, so NO_CAPACITY is a byte-stable
 * count-first result. */
static PcmXQueueResult
pcm_x_local_holder_snapshot_copy_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
										PcmXLocalHolderHandle *holders_out, Size holder_capacity,
										PcmXLocalHolderSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalMembershipSlot *holder;
	PcmXSlotRef holder_ref;
	Size count = 0;
	Size current;
	Size previous;
	Size i;

	if (header == NULL || tag_slot == NULL || snapshot_out == NULL
		|| (holder_capacity != 0 && holders_out == NULL))
		return PCM_X_QUEUE_INVALID;
	current = tag_slot->active_holder_head_index;
	previous = PCM_X_INVALID_SLOT_INDEX;
	while (current != PCM_X_INVALID_SLOT_INDEX && count < header->layout.local_holder.capacity) {
		holder = pcm_x_local_holder_link_resolve(&tag_slot->tag, tag_ref, current, previous,
												 &holder_ref);
		if (holder == NULL || holder->next_index == current)
			return PCM_X_QUEUE_CORRUPT;
		previous = current;
		current = holder->next_index;
		count++;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX || (count != 0 && tag_slot->holder_set_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	snapshot_out->tag_slot = tag_ref;
	snapshot_out->holder_set_generation = tag_slot->holder_set_generation;
	snapshot_out->holder_count = count;
	if (count > holder_capacity)
		return PCM_X_QUEUE_NO_CAPACITY;

	current = tag_slot->active_holder_head_index;
	previous = PCM_X_INVALID_SLOT_INDEX;
	for (i = 0; i < count; i++) {
		holder = pcm_x_local_holder_link_resolve(&tag_slot->tag, tag_ref, current, previous,
												 &holder_ref);
		if (holder == NULL)
			return PCM_X_QUEUE_CORRUPT;
		pcm_x_local_holder_handle_from_member(&holders_out[i], tag_ref, holder_ref, holder);
		previous = current;
		current = holder->next_index;
	}
	if (count > 1)
		qsort(holders_out, count, sizeof(*holders_out), pcm_x_local_holder_handle_compare);
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_local_holder_snapshot(const BufferTag *tag, PcmXLocalHolderHandle *holders_out,
									Size holder_capacity, PcmXLocalHolderSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	uint32 partition;

	pcm_x_local_holder_snapshot_clear(snapshot_out);
	if (header == NULL || tag == NULL || snapshot_out == NULL
		|| (holder_capacity != 0 && holders_out == NULL))
		return PCM_X_QUEUE_INVALID;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_OK;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto done;
	}
	result = pcm_x_local_holder_snapshot_copy_locked(tag_slot, tag_ref, holders_out,
													 holder_capacity, snapshot_out);

done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_snapshot_revalidate(const BufferTag *tag,
											   const PcmXLocalHolderSnapshot *snapshot)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef found;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || tag == NULL || snapshot == NULL)
		return PCM_X_QUEUE_INVALID;
	if (snapshot->holder_count > header->layout.local_holder.capacity
		|| (snapshot->holder_count != 0 && snapshot->holder_set_generation == 0))
		return PCM_X_QUEUE_INVALID;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &found);
	LWLockRelease(&header->allocator_lock.lock);
	if (snapshot->tag_slot.slot_index == PCM_X_INVALID_SLOT_INDEX) {
		if (snapshot->tag_slot.slot_generation != 0 || snapshot->holder_set_generation != 0
			|| snapshot->holder_count != 0)
			return PCM_X_QUEUE_INVALID;
		if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
			return PCM_X_QUEUE_OK;
		if (directory_result == PCM_X_DIRECTORY_OK)
			return PCM_X_QUEUE_STALE;
		return pcm_x_queue_result_from_directory(directory_result);
	}
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	if (found.slot_index != snapshot->tag_slot.slot_index
		|| found.slot_generation != snapshot->tag_slot.slot_generation)
		return PCM_X_QUEUE_STALE;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, found, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL)
		result = PCM_X_QUEUE_STALE;
	else if (tag_slot->holder_set_generation != snapshot->holder_set_generation)
		result = PCM_X_QUEUE_STALE;
	else
		result = PCM_X_QUEUE_OK;
	LWLockRelease(&header->local_locks[partition].lock);
	return result;
}


/* Reserve a tag-only holder participant without publishing transfer state
 * under the allocator lock.  Both an existing and a new tag return with the
 * admission gate claimed until the caller publishes under the BufferTag
 * domain lock. */
static PcmXQueueResult
pcm_x_local_holder_transfer_tag_prepare(const PcmXTicketRef *ref, int32 master_node,
										uint64 master_session, PcmXLocalTagSlot **prepared_tag_out,
										PcmXSlotRef *tag_ref_out, bool *new_tag_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot = NULL;
	PcmXSlotHeader *raw_slot;
	PcmXSlotRef existing;
	PcmXDirectoryResult directory_result;
	PcmXAllocatorResult allocator_result;
	PcmXQueueResult result;

	*prepared_tag_out = NULL;
	*tag_ref_out = (PcmXSlotRef){ PCM_X_INVALID_SLOT_INDEX, 0 };
	*new_tag_out = false;
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	result = pcm_x_local_retire_episode_state_locked(header);
	if (result != PCM_X_QUEUE_OK) {
		goto prepare_done;
	}
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, tag_ref_out);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG,
																	 *tag_ref_out);
		result = pcm_x_resident_tag_state_result(tag_slot == NULL ? NULL : &tag_slot->slot);
		if (result == PCM_X_QUEUE_OK
			&& (!BufferTagsEqual(&tag_slot->tag, &ref->identity.tag)
				|| tag_slot->cluster_epoch != ref->identity.cluster_epoch))
			result = PCM_X_QUEUE_CORRUPT;
		if (result == PCM_X_QUEUE_OK)
			result
				= pcm_x_local_handoff_gate_claim_locked(header, tag_slot, false, PCM_X_QUEUE_BUSY);
		goto prepare_done;
	}
	if (directory_result != PCM_X_DIRECTORY_NOT_FOUND) {
		result = pcm_x_queue_result_from_directory(directory_result);
		goto prepare_done;
	}
	allocator_result
		= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref_out, &raw_slot);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		goto prepare_done;
	}
	tag_slot = (PcmXLocalTagSlot *)raw_slot;
	pcm_x_local_tag_init_common(tag_slot, &ref->identity.tag, ref->identity.cluster_epoch,
								master_node, master_session);
	directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag,
													 *tag_ref_out, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = directory_result == PCM_X_DIRECTORY_EXISTS
					 ? PCM_X_QUEUE_CORRUPT
					 : pcm_x_queue_result_from_directory(directory_result);
		if (pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, *tag_ref_out,
										   PCM_X_TAG_RESERVED_NONVISIBLE)
			!= PCM_X_ALLOC_OK)
			result = PCM_X_QUEUE_CORRUPT;
		*tag_ref_out = (PcmXSlotRef){ PCM_X_INVALID_SLOT_INDEX, 0 };
		goto prepare_done;
	}
	*new_tag_out = true;
	result = PCM_X_QUEUE_OK;

prepare_done:
	if (result == PCM_X_QUEUE_OK)
		*prepared_tag_out = tag_slot;
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_local_holder_transfer_tag_abort_new(const BufferTag *tag, PcmXSlotRef tag_ref,
										  PcmXLocalTagSlot *prepared_tag)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef found;
	bool ok = false;

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, prepared_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (pcm_x_local_retire_episode_state_locked(header) == PCM_X_QUEUE_OK && tag_slot != NULL
		&& pcm_x_slot_state_read(&tag_slot->slot) == PCM_X_TAG_RESERVED_NONVISIBLE
		&& (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0
		&& BufferTagsEqual(&tag_slot->tag, tag)
		&& pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &found) == PCM_X_DIRECTORY_OK
		&& found.slot_index == tag_ref.slot_index
		&& found.slot_generation == tag_ref.slot_generation
		&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, tag, tag_ref)
			   == PCM_X_DIRECTORY_OK
		&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
										  PCM_X_TAG_RESERVED_NONVISIBLE)
			   == PCM_X_ALLOC_OK)
		ok = true;
	LWLockRelease(&header->allocator_lock.lock);
	return ok;
}


static bool
pcm_x_local_holder_transfer_peer_exact(const PcmXLocalTagSlot *tag_slot, int32 master_node,
									   uint64 master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	const PcmXPeerFrontier *frontier;

	if (header == NULL || tag_slot == NULL || master_node < 0
		|| master_node >= PCM_X_PROTOCOL_NODE_LIMIT || master_session == 0
		|| tag_slot->master_node != master_node
		|| tag_slot->master_session_incarnation != master_session)
		return false;
	frontier = &header->peer_frontiers[master_node];
	return frontier->cluster_epoch == tag_slot->cluster_epoch
		   && frontier->sender_session_incarnation == master_session;
}


static void
pcm_x_local_cutoff_fill(const PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
						PcmXLocalCutoff *cutoff_out)
{
	memset(cutoff_out, 0, sizeof(*cutoff_out));
	cutoff_out->tag_slot = tag_ref;
	cutoff_out->cutoff_sequence = tag_slot->cutoff_sequence;
	cutoff_out->master_session_incarnation = tag_slot->master_session_incarnation;
	cutoff_out->closed_round = tag_slot->local_round;
	cutoff_out->next_round = tag_slot->local_round + 1;
	cutoff_out->master_node = tag_slot->master_node;
}


/*
 * Close the current local writer round under the tag admission gate.  The
 * membership-free case is intentional: a holder-only (or freshly reserved)
 * tag still needs the same barrier so a writer arriving after PROBE cannot be
 * mistaken for a member of the sampled round.
 */
static PcmXQueueResult
pcm_x_local_freeze_round_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
								PcmXLocalCutoff *cutoff_out)
{
	PcmXLocalMembershipSlot *tail;
	PcmXAllocatorView member_view;
	uint32 flags;

	if (tag_slot == NULL || cutoff_out == NULL)
		return PCM_X_QUEUE_INVALID;
	memset(cutoff_out, 0, sizeof(*cutoff_out));
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0) {
		if (tag_slot->local_round == UINT32_MAX
			|| tag_slot->closed_round_member_count > tag_slot->membership_count
			|| tag_slot->cutoff_sequence >= tag_slot->next_sequence
			|| (tag_slot->cutoff_sequence == 0 && tag_slot->closed_round_member_count != 0))
			return PCM_X_QUEUE_CORRUPT;
		pcm_x_local_cutoff_fill(tag_slot, tag_ref, cutoff_out);
		return PCM_X_QUEUE_DUPLICATE;
	}
	if (tag_slot->local_round == UINT32_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	if (tag_slot->closed_round_member_count != 0 || tag_slot->next_sequence == 0)
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->membership_count == 0) {
		if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX)
			return PCM_X_QUEUE_CORRUPT;
	} else {
		if (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX
			|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view))
			return PCM_X_QUEUE_CORRUPT;
		tail = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, tag_slot->tail_index);
		if (tail == NULL || tail->tag_slot_index != tag_ref.slot_index
			|| tail->tag_slot_generation != tag_ref.slot_generation || tail->local_sequence == 0
			|| tail->local_sequence >= tag_slot->next_sequence)
			return PCM_X_QUEUE_CORRUPT;
	}

	/* Sequence allocation is monotonic.  Cancelled/detached identities leave
	 * legal holes but remain members of the round closed at this high-water. */
	tag_slot->cutoff_sequence = tag_slot->next_sequence - 1;
	tag_slot->closed_round_member_count = tag_slot->membership_count;
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_REVOKE_BARRIER);
	pcm_x_local_cutoff_fill(tag_slot, tag_ref, cutoff_out);
	return PCM_X_QUEUE_OK;
}


static bool
pcm_x_local_blocker_freeze_reservation_exact(const PcmXLocalTagSlot *tag_slot)
{
	return tag_slot != NULL && !pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
		   && tag_slot->blocker_set_generation == 0
		   && pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
		   && tag_slot->blocker_snapshot_reliable.state_sequence == 0
		   && tag_slot->blocker_snapshot_reliable.response_tombstone_mask == 0
		   && tag_slot->blocker_snapshot_reliable.last_responder_node == 0
		   && tag_slot->blocker_snapshot_reliable.last_response_opcode == 0
		   && tag_slot->blocker_snapshot_head_index == PCM_X_INVALID_SLOT_INDEX
		   && tag_slot->blocker_snapshot_count == 0 && tag_slot->blocker_snapshot_crc32c == 0
		   && tag_slot->blocker_snapshot_next_chunk == 0;
}


PcmXQueueResult
cluster_pcm_x_local_probe_freeze_snapshot_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session,
	PcmXLocalHolderHandle *holders_out, Size holder_capacity, PcmXLocalHolderSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *prepared_tag;
	PcmXSlotRef tag_ref;
	PcmXLocalCutoff cutoff;
	PcmXQueueResult result;
	PcmXQueueResult freeze_result;
	uint32 flags;
	uint32 partition;
	bool new_tag;
	bool gate_claimed = false;
	bool durable_freeze = false;
	bool fail_closed = false;

	pcm_x_local_holder_snapshot_clear(snapshot_out);
	if (header == NULL || ref == NULL || snapshot_out == NULL
		|| (holder_capacity != 0 && holders_out == NULL) || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity) || ref->handle.ticket_id == 0
		|| ref->handle.queue_generation == 0 || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_holder_transfer_tag_prepare(ref, authenticated_master_node,
													 authenticated_master_session, &prepared_tag,
													 &tag_ref, &new_tag);
	if (result != PCM_X_QUEUE_OK)
		return result;
	gate_claimed = true;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE,
									 prepared_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		PcmXAllocatorView tag_view;
		PcmXLocalTagSlot *raw_tag = NULL;
		uint64 generation;

		/* A writer-in-progress generation is never normalized to "no tag".
		 * It is a retryable closed decision; every other impossible locator is
		 * stale evidence. */
		if (pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &tag_view))
			raw_tag = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&tag_view, tag_ref.slot_index);
		result = raw_tag != NULL && !pcm_x_slot_generation_read(&raw_tag->slot, &generation)
					 ? PCM_X_QUEUE_BUSY
					 : PCM_X_QUEUE_STALE;
		tag_slot = prepared_tag;
		goto freeze_release_gate;
	}
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto freeze_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto freeze_release_gate;
	}
	if (tag_slot->master_node == -1 && tag_slot->master_session_incarnation == 0) {
		if (tag_slot->membership_count != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto freeze_release_gate;
		}
		tag_slot->master_node = authenticated_master_node;
		tag_slot->master_session_incarnation = authenticated_master_session;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto freeze_release_gate;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if (!new_tag && (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0
		&& tag_slot->membership_count != 0
		&& (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX)) {
		/* cancel_exact unlinks before terminal detach decrements membership.
		 * Wait for that evidence owner instead of treating the legal window as a
		 * broken FIFO or publishing a blocker reservation that cannot be frozen. */
		if (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX
			&& tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX
			&& tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
			&& tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX
			&& tag_slot->closed_round_member_count == 0) {
			result = PCM_X_QUEUE_NOT_READY;
			goto freeze_release_gate;
		}
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto freeze_release_gate;
	}

	if (!pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, ref)) {
			result = PCM_X_QUEUE_BUSY;
			goto freeze_release_gate;
		}
		if (!pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
			if (tag_slot->blocker_set_generation == 0
				|| !pcm_x_transfer_leg_exact(
					&tag_slot->blocker_snapshot_reliable, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT,
					authenticated_master_node, authenticated_master_session)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto freeze_release_gate;
			}
			/* The previous set still awaits its exact ACK.  The caller must
			 * replay it, not resample a possibly different holder set. */
			result = PCM_X_QUEUE_DUPLICATE;
			goto freeze_release_gate;
		}
		if (tag_slot->blocker_set_generation == 0) {
			if (!pcm_x_local_blocker_freeze_reservation_exact(tag_slot)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto freeze_release_gate;
			}
		} else if (tag_slot->blocker_set_generation == UINT64_MAX
				   || tag_slot->blocker_snapshot_reliable.state_sequence == 0
				   || tag_slot->blocker_snapshot_reliable.last_response_opcode
						  != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
				   || tag_slot->blocker_snapshot_reliable.last_responder_node
						  != (uint32)authenticated_master_node
				   || tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
				   || tag_slot->blocker_snapshot_count != 0
				   || tag_slot->blocker_snapshot_crc32c != 0
				   || tag_slot->blocker_snapshot_next_chunk != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto freeze_release_gate;
		}
	} else {
		if (tag_slot->blocker_set_generation != 0
			|| !pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
			|| tag_slot->blocker_snapshot_reliable.state_sequence != 0
			|| tag_slot->blocker_snapshot_reliable.response_tombstone_mask != 0
			|| tag_slot->blocker_snapshot_reliable.last_responder_node != 0
			|| tag_slot->blocker_snapshot_reliable.last_response_opcode != 0
			|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
			|| tag_slot->blocker_snapshot_next_chunk != 0
			|| !pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)) {
			result = PCM_X_QUEUE_BUSY;
			goto freeze_release_gate;
		}
		tag_slot->blocker_snapshot_ref = *ref;
	}

	freeze_result = pcm_x_local_freeze_round_locked(tag_slot, tag_ref, &cutoff);
	if (freeze_result != PCM_X_QUEUE_OK && freeze_result != PCM_X_QUEUE_DUPLICATE) {
		result = freeze_result;
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto freeze_release_gate;
	}
	if (new_tag)
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
	durable_freeze = true;
	result = pcm_x_local_holder_snapshot_copy_locked(tag_slot, tag_ref, holders_out,
													 holder_capacity, snapshot_out);
	if (result == PCM_X_QUEUE_CORRUPT)
		fail_closed = true;

freeze_release_gate:
	if (gate_claimed && !fail_closed && (!new_tag || durable_freeze)) {
		if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
	}
freeze_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (!durable_freeze && new_tag && !fail_closed
		&& !pcm_x_local_holder_transfer_tag_abort_new(&ref->identity.tag, tag_ref, prepared_tag)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


typedef struct PcmXHeldContentLockSnapshot {
	BufferTag tags[LWLOCK_MAX_HELD_BY_PROC];
	Size count;
	bool overflow;
} PcmXHeldContentLockSnapshot;


/* ForEachLWLockHeldByMe callback: reverse-map and copy only.  Acquiring a
 * PCM local lock here would invert content_lock -> PCM and deadlock against
 * holder registration/freeze, so all shared-state checks happen later. */
static void
pcm_x_collect_held_content_lock(LWLock *lock, LWLockMode mode, void *context)
{
	PcmXHeldContentLockSnapshot *snapshot = (PcmXHeldContentLockSnapshot *)context;
	BufferDesc *buf;
	Size i;

	(void)mode;
	if (snapshot == NULL || snapshot->overflow)
		return;
	buf = BufferDescriptorFromContentLock(lock);
	if (buf == NULL)
		return;
	for (i = 0; i < snapshot->count; i++) {
		if (BufferTagsEqual(&snapshot->tags[i], &buf->tag))
			return;
	}
	if (snapshot->count >= lengthof(snapshot->tags)) {
		snapshot->overflow = true;
		return;
	}
	snapshot->tags[snapshot->count++] = buf->tag;
}


/* Check one copied tag after ForEachLWLockHeldByMe has returned. */
static PcmXQueueResult
pcm_x_local_nested_wait_guard_find_locked(const BufferTag *tag, PcmXSlotRef *tag_ref_out)
{
	PcmXDirectoryView directory;
	PcmXAllocatorView allocator;
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	uint64 key_hash;
	Size bucket;
	Size step;

	if (header == NULL || tag == NULL || tag_ref_out == NULL
		|| !LWLockHeldByMe(&header->allocator_lock.lock)
		|| !cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_TAG, tag, &key_hash)
		|| !pcm_x_directory_view(PCM_X_DIR_LOCAL_TAG, &directory)
		|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &allocator))
		return PCM_X_QUEUE_INVALID;
	tag_ref_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	tag_ref_out->slot_generation = 0;
	bucket = key_hash % directory.capacity;
	for (step = 0; step < directory.capacity; step++) {
		PcmXDirectoryEntry *entry = &directory.entries[(bucket + step) % directory.capacity];
		PcmXLocalTagSlot *slot;
		uint64 generation;
		uint64 resident_key_hash;
		uint32 state;

		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			return PCM_X_QUEUE_NOT_FOUND;
		if (entry->state != PCM_X_DIRECTORY_OCCUPIED)
			return PCM_X_QUEUE_CORRUPT;
		if (entry->key_hash != key_hash)
			continue;
		slot = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&allocator, entry->slot_index);
		if (slot == NULL)
			return PCM_X_QUEUE_CORRUPT;
		/* Unlike the generic directory lookup, the pre-sleep guard must
		 * preserve an in-progress generation write as BUSY.  Treating it as
		 * NOT_FOUND would open the exact freeze-vs-sleep race this API closes. */
		if (!pcm_x_slot_generation_read(&slot->slot, &generation))
			return PCM_X_QUEUE_BUSY;
		state = pcm_x_slot_state_read(&slot->slot);
		if (generation != entry->slot_generation || state == PCM_X_SLOT_FREE
			|| state == PCM_X_SLOT_GENERATION_EXHAUSTED
			|| !pcm_x_directory_slot_key_hash(PCM_X_DIR_LOCAL_TAG, &slot->slot, &resident_key_hash)
			|| resident_key_hash != entry->key_hash)
			return PCM_X_QUEUE_CORRUPT;
		if (!BufferTagsEqual(&slot->tag, tag))
			continue;
		tag_ref_out->slot_index = entry->slot_index;
		tag_ref_out->slot_generation = entry->slot_generation;
		return PCM_X_QUEUE_OK;
	}
	return PCM_X_QUEUE_NOT_FOUND;
}


static PcmXQueueResult
pcm_x_local_nested_wait_guard_tag(const BufferTag *tag)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (header == NULL || tag == NULL)
		return PCM_X_QUEUE_OK;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_nested_wait_guard_find_locked(tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_NOT_FOUND)
		return PCM_X_QUEUE_OK;
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}
	/* PCM runtime formation is irrelevant until an exact local-tag entry
	 * proves that this held content lock participates in the protocol. */
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		PcmXAllocatorView tag_view;
		PcmXLocalTagSlot *raw_tag = NULL;
		uint64 generation;

		if (pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &tag_view))
			raw_tag = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&tag_view, tag_ref.slot_index);
		/* A generation writer in progress is retryable BUSY.  A completed
		 * locator mismatch is stale evidence, but is still a closed result. */
		result = raw_tag != NULL && !pcm_x_slot_generation_read(&raw_tag->slot, &generation)
					 ? PCM_X_QUEUE_BUSY
					 : PCM_X_QUEUE_STALE;
	} else if (!pcm_x_runtime_token_exact(&runtime, 0))
		result = PCM_X_QUEUE_NOT_READY;
	else if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0)
		result = PCM_X_QUEUE_BARRIER_CLOSED;
	else
		result = PCM_X_QUEUE_OK;
	LWLockRelease(&header->local_locks[partition].lock);
	return result;
}


/* This backend's most recent nested-guard refusal: the held content-lock tag
 * that closed the guard and the refusing result.  Diagnostic only. */
static BufferTag pcm_x_nested_guard_block_tag;
static int pcm_x_nested_guard_block_result = PCM_X_QUEUE_OK;

bool
cluster_pcm_x_nested_wait_guard_last_block(BufferTag *tag_out, int *result_out)
{
	if (pcm_x_nested_guard_block_result == PCM_X_QUEUE_OK)
		return false;
	if (tag_out != NULL)
		*tag_out = pcm_x_nested_guard_block_tag;
	if (result_out != NULL)
		*result_out = pcm_x_nested_guard_block_result;
	return true;
}

PcmXQueueResult
cluster_pcm_x_nested_wait_guard_before_block(void)
{
	PcmXHeldContentLockSnapshot snapshot;
	PcmXQueueResult result;
	Size i;

	memset(&snapshot, 0, sizeof(snapshot));
	ForEachLWLockHeldByMe(pcm_x_collect_held_content_lock, &snapshot);
	if (snapshot.overflow) {
		pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	for (i = 0; i < snapshot.count; i++) {
		result = pcm_x_local_nested_wait_guard_tag(&snapshot.tags[i]);
		if (result != PCM_X_QUEUE_OK) {
			pcm_x_nested_guard_block_tag = snapshot.tags[i];
			pcm_x_nested_guard_block_result = (int)result;
			return result;
		}
	}
	return PCM_X_QUEUE_OK;
}


static bool
pcm_x_local_blocker_chain_validate(const PcmXAllocatorView *view, Size head_index, uint32 count,
								   const PcmXTicketRef *ref, PcmXSlotRef tag_ref,
								   uint64 set_generation, uint32 expected_state,
								   uint32 expected_crc32c, PcmXBlockerChunkPayload *edges_out)
{
	ClusterLmdVertex previous;
	pg_crc32c crc;
	Size current = head_index;
	uint32 i;
	bool have_previous = false;

	if (view == NULL || ref == NULL || count > view->capacity)
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX && expected_crc32c == 0;
	if (head_index == PCM_X_INVALID_SLOT_INDEX)
		return false;
	INIT_CRC32C(crc);
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot;
		uint64 generation;

		slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(view, current);
		if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &generation) || generation == 0
			|| pcm_x_slot_state_read(&slot->slot) != expected_state
			|| !pcm_x_ticket_handle_equal(&slot->handle, &ref->handle)
			|| slot->owner_slot_index != tag_ref.slot_index
			|| slot->owner_slot_generation != tag_ref.slot_generation
			|| slot->set_generation != set_generation || slot->chunk_no != i
			|| slot->direction != PCM_X_BLOCKER_DIRECTION_LOCAL
			|| !pcm_x_lmd_vertex_valid(&slot->blocker)
			|| (have_previous && pcm_x_lmd_vertex_identity_compare(&previous, &slot->blocker) >= 0)
			|| (slot->next_index != PCM_X_INVALID_SLOT_INDEX && slot->next_index >= view->capacity))
			return false;
		pcm_x_blocker_crc32c_update(&crc, &slot->blocker);
		if (edges_out != NULL) {
			PcmXBlockerChunkPayload *edge = &edges_out[i];

			memset(edge, 0, sizeof(*edge));
			edge->tag = ref->identity.tag;
			edge->requester_node = ref->identity.node_id;
			edge->requester_procno = ref->identity.procno;
			edge->chunk_no = i;
			edge->cluster_epoch = ref->identity.cluster_epoch;
			edge->request_id = ref->identity.request_id;
			edge->handle = ref->handle;
			edge->grant_generation = ref->grant_generation;
			edge->set_generation = set_generation;
			edge->blocker = slot->blocker;
		}
		previous = slot->blocker;
		have_previous = true;
		current = slot->next_index;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		return false;
	FIN_CRC32C(crc);
	return (uint32)crc == expected_crc32c;
}


static bool
pcm_x_local_blocker_chain_release_locked(Size head_index, uint32 count, const PcmXTicketRef *ref,
										 PcmXSlotRef tag_ref, uint64 set_generation,
										 uint32 expected_state)
{
	PcmXAllocatorView view;
	Size current = head_index;
	uint32 i;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view) || ref == NULL || count > view.capacity)
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot;
		uint64 generation;

		slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &generation)
			|| pcm_x_slot_state_read(&slot->slot) != expected_state
			|| !pcm_x_ticket_handle_equal(&slot->handle, &ref->handle)
			|| slot->owner_slot_index != tag_ref.slot_index
			|| slot->owner_slot_generation != tag_ref.slot_generation
			|| slot->set_generation != set_generation || slot->chunk_no != i
			|| slot->direction != PCM_X_BLOCKER_DIRECTION_LOCAL
			|| (slot->next_index != PCM_X_INVALID_SLOT_INDEX && slot->next_index >= view.capacity))
			return false;
		current = slot->next_index;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		return false;
	current = head_index;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		PcmXSlotRef slot_ref;
		Size next = slot->next_index;

		slot_ref.slot_index = current;
		if (!pcm_x_slot_generation_read(&slot->slot, &slot_ref.slot_generation)
			|| pcm_x_allocator_release_locked(PCM_X_ALLOC_BLOCKER, slot_ref, expected_state)
				   != PCM_X_ALLOC_OK)
			return false;
		current = next;
	}
	return true;
}


static PcmXAllocatorResult
pcm_x_local_blocker_chain_reserve_locked(const PcmXTicketRef *ref, PcmXSlotRef tag_ref,
										 uint64 set_generation, const ClusterLmdVertex *blockers,
										 Size blocker_count, Size *head_index_out)
{
	PcmXAllocatorResult result;
	PcmXBlockerSlot *previous = NULL;
	Size head = PCM_X_INVALID_SLOT_INDEX;
	Size i;

	*head_index_out = PCM_X_INVALID_SLOT_INDEX;
	result = pcm_x_blocker_capacity_preflight_locked(blocker_count);
	if (result != PCM_X_ALLOC_OK)
		return result;
	for (i = 0; i < blocker_count; i++) {
		PcmXSlotHeader *raw_slot;
		PcmXBlockerSlot *slot;
		PcmXSlotRef slot_ref;

		result = pcm_x_allocator_reserve_locked(PCM_X_ALLOC_BLOCKER, &slot_ref, &raw_slot);
		if (result != PCM_X_ALLOC_OK) {
			if (result == PCM_X_ALLOC_NO_CAPACITY)
				result = PCM_X_ALLOC_CORRUPT;
			break;
		}
		slot = (PcmXBlockerSlot *)raw_slot;
		slot->handle = ref->handle;
		slot->blocker = blockers[i];
		slot->owner_slot_generation = tag_ref.slot_generation;
		slot->set_generation = set_generation;
		slot->next_index = PCM_X_INVALID_SLOT_INDEX;
		slot->owner_slot_index = tag_ref.slot_index;
		slot->chunk_no = (uint32)i;
		slot->direction = PCM_X_BLOCKER_DIRECTION_LOCAL;
		if (previous == NULL)
			head = slot_ref.slot_index;
		else
			previous->next_index = slot_ref.slot_index;
		previous = slot;
	}
	if (result != PCM_X_ALLOC_OK) {
		if (!pcm_x_local_blocker_chain_release_locked(head, (uint32)i, ref, tag_ref, set_generation,
													  PCM_XB_RESERVED_NONVISIBLE))
			result = PCM_X_ALLOC_CORRUPT;
		return result;
	}
	*head_index_out = head;
	return PCM_X_ALLOC_OK;
}


/* Reserved blocker slots are private to one in-flight arm.  The allocator
 * lock publishes only their RESERVED lifecycle; after the exact local-tag
 * lock mints a durable set generation, relabel the whole private chain in two
 * passes so corruption can never leave a partially relabeled chain. */
static bool
pcm_x_local_blocker_chain_relabel_reserved(Size head_index, uint32 count, const PcmXTicketRef *ref,
										   PcmXSlotRef tag_ref, uint64 old_generation,
										   uint64 new_generation)
{
	PcmXAllocatorView view;
	Size current = head_index;
	uint32 i;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &view) || ref == NULL || new_generation == 0
		|| new_generation == UINT64_MAX || count > view.capacity)
		return false;
	if (count == 0)
		return head_index == PCM_X_INVALID_SLOT_INDEX;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);
		uint64 slot_generation;

		if (slot == NULL || !pcm_x_slot_generation_read(&slot->slot, &slot_generation)
			|| slot_generation == 0
			|| pcm_x_slot_state_read(&slot->slot) != PCM_XB_RESERVED_NONVISIBLE
			|| !pcm_x_ticket_handle_equal(&slot->handle, &ref->handle)
			|| slot->owner_slot_index != tag_ref.slot_index
			|| slot->owner_slot_generation != tag_ref.slot_generation
			|| slot->set_generation != old_generation || slot->chunk_no != i
			|| slot->direction != PCM_X_BLOCKER_DIRECTION_LOCAL
			|| (slot->next_index != PCM_X_INVALID_SLOT_INDEX && slot->next_index >= view.capacity))
			return false;
		current = slot->next_index;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX)
		return false;
	current = head_index;
	for (i = 0; i < count; i++) {
		PcmXBlockerSlot *slot = (PcmXBlockerSlot *)pcm_x_allocator_slot(&view, current);

		slot->set_generation = new_generation;
		current = slot->next_index;
	}
	return true;
}


static bool
pcm_x_local_holder_snapshot_exact_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
										 const PcmXLocalHolderSnapshot *snapshot,
										 const PcmXLocalHolderHandle *holders, Size holder_count)
{
	PcmXLocalMembershipSlot *holder;
	PcmXSlotRef holder_ref;
	Size current;
	Size previous;
	Size active_count = 0;
	Size i;

	if (tag_slot == NULL || snapshot == NULL || snapshot->holder_count != holder_count
		|| snapshot->tag_slot.slot_index != tag_ref.slot_index
		|| snapshot->tag_slot.slot_generation != tag_ref.slot_generation
		|| snapshot->holder_set_generation != tag_slot->holder_set_generation)
		return false;
	current = tag_slot->active_holder_head_index;
	previous = PCM_X_INVALID_SLOT_INDEX;
	while (current != PCM_X_INVALID_SLOT_INDEX
		   && active_count < ClusterPcmXConvertShmem->layout.local_holder.capacity) {
		holder = pcm_x_local_holder_link_resolve(&tag_slot->tag, tag_ref, current, previous,
												 &holder_ref);
		if (holder == NULL || holder->next_index == current)
			return false;
		previous = current;
		current = holder->next_index;
		active_count++;
	}
	if (current != PCM_X_INVALID_SLOT_INDEX || active_count != holder_count)
		return false;
	for (i = 0; i < holder_count; i++) {
		if (i != 0 && pcm_x_local_holder_handle_compare(&holders[i - 1], &holders[i]) >= 0)
			return false;
		holder = pcm_x_local_holder_slot_resolve(&tag_slot->tag, tag_ref,
												 holders[i].holder_slot.slot_index, &holder_ref);
		if (holder == NULL
			|| !pcm_x_local_holder_handle_exact(&holders[i], tag_ref, holder_ref, holder))
			return false;
	}
	return true;
}


/* The holder registry is a pin/barrier fingerprint, not a WFG edge list.
 * Outer code may publish only live nested waits observed for exact registered
 * holder processes.  One process can own at most one current nested wait, and
 * passive holders legitimately produce an empty blocker set. */
static bool
pcm_x_local_filtered_blockers_valid(const PcmXTicketRef *ref, const PcmXLocalHolderHandle *holders,
									Size holder_count, const ClusterLmdVertex *blockers,
									Size blocker_count, uint32 *set_crc32c_out)
{
	ClusterLmdVertex previous;
	pg_crc32c crc;
	Size i;

	if (ref == NULL || set_crc32c_out == NULL || blocker_count > holder_count
		|| (holder_count != 0 && holders == NULL) || (blocker_count != 0 && blockers == NULL))
		return false;
	*set_crc32c_out = 0;
	if (blocker_count == 0)
		return true;
	INIT_CRC32C(crc);
	for (i = 0; i < blocker_count; i++) {
		const ClusterLmdVertex *blocker = &blockers[i];
		Size holder_index;
		bool bound = false;

		if (!pcm_x_lmd_vertex_valid(blocker) || blocker->node_id != cluster_node_id
			|| (blocker->node_id == ref->identity.node_id && blocker->procno == ref->identity.procno
				&& blocker->cluster_epoch == ref->identity.cluster_epoch
				&& blocker->request_id == ref->identity.request_id)
			|| (i != 0 && pcm_x_lmd_vertex_identity_compare(&previous, blocker) >= 0)
			|| (i != 0 && previous.node_id == blocker->node_id
				&& previous.procno == blocker->procno))
			return false;
		for (holder_index = 0; holder_index < holder_count; holder_index++) {
			const PcmXWaitIdentity *holder = &holders[holder_index].key.identity;

			if (holder->node_id == blocker->node_id && holder->procno == blocker->procno
				&& holder->cluster_epoch == blocker->cluster_epoch) {
				bound = true;
				break;
			}
		}
		if (!bound)
			return false;
		pcm_x_blocker_crc32c_update(&crc, blocker);
		previous = *blocker;
	}
	FIN_CRC32C(crc);
	*set_crc32c_out = (uint32)crc;
	return true;
}


static bool
pcm_x_local_blocker_ack_tombstone_exact(const PcmXLocalTagSlot *tag_slot,
										int32 authenticated_master_node)
{
	return tag_slot != NULL && authenticated_master_node >= 0
		   && authenticated_master_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && tag_slot->blocker_set_generation != 0
		   && tag_slot->blocker_set_generation != UINT64_MAX
		   && pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
		   && tag_slot->blocker_snapshot_reliable.state_sequence != 0
		   && tag_slot->blocker_snapshot_reliable.last_response_opcode
				  == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
		   && tag_slot->blocker_snapshot_reliable.last_responder_node
				  == (uint32)authenticated_master_node
		   && tag_slot->blocker_snapshot_head_index == PCM_X_INVALID_SLOT_INDEX
		   && tag_slot->blocker_snapshot_count == 0 && tag_slot->blocker_snapshot_crc32c == 0
		   && tag_slot->blocker_snapshot_next_chunk == 0;
}


PcmXQueueResult
cluster_pcm_x_local_blocker_snapshot_arm_exact(
	const PcmXTicketRef *ref, int32 authenticated_master_node, uint64 authenticated_master_session,
	const PcmXLocalHolderSnapshot *holder_snapshot, const PcmXLocalHolderHandle *holders,
	Size holder_count, const ClusterLmdVertex *blockers, Size blocker_count,
	PcmXLocalBlockerSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *prepared_tag;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	PcmXAllocatorResult allocator_result;
	Size reserved_head = PCM_X_INVALID_SLOT_INDEX;
	uint64 reserved_generation = 0;
	uint64 set_generation = 0;
	uint64 next_sequence;
	uint32 set_crc32c = 0;
	uint32 partition;
	bool new_tag;
	bool gate_claimed = false;
	bool chain_published = false;
	bool fail_closed = false;

	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (header == NULL || ref == NULL || holder_snapshot == NULL || snapshot_out == NULL
		|| ref->grant_generation != 0 || !pcm_x_wait_identity_valid(&ref->identity)
		|| holder_count != holder_snapshot->holder_count || (holder_count != 0 && holders == NULL)
		|| holder_count > header->layout.local_holder.capacity || blocker_count > holder_count
		|| blocker_count > header->layout.max_wait_edges || blocker_count > UINT32_MAX
		|| (blocker_count != 0 && blockers == NULL) || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (!pcm_x_local_filtered_blockers_valid(ref, holders, holder_count, blockers, blocker_count,
											 &set_crc32c))
		return PCM_X_QUEUE_INVALID;
	result = pcm_x_local_holder_transfer_tag_prepare(ref, authenticated_master_node,
													 authenticated_master_session, &prepared_tag,
													 &tag_ref, &new_tag);
	if (result != PCM_X_QUEUE_OK)
		return result;
	gate_claimed = true;

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, prepared_tag);
	allocator_result = pcm_x_local_blocker_chain_reserve_locked(
		ref, tag_ref, reserved_generation, blockers, blocker_count, &reserved_head);
	LWLockRelease(&header->allocator_lock.lock);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto arm_cleanup_new_tag;
	}
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE,
									 prepared_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		tag_slot = prepared_tag;
		goto arm_release_gate;
	}
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto arm_domain_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_STALE;
		goto arm_release_gate;
	}
	if (tag_slot->master_node == -1 && tag_slot->master_session_incarnation == 0) {
		tag_slot->master_node = authenticated_master_node;
		tag_slot->master_session_incarnation = authenticated_master_session;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto arm_release_gate;
	}
	if (new_tag) {
		if (holder_count != 0 || blocker_count != 0
			|| holder_snapshot->tag_slot.slot_index != PCM_X_INVALID_SLOT_INDEX
			|| holder_snapshot->tag_slot.slot_generation != 0
			|| holder_snapshot->holder_set_generation != 0) {
			result = PCM_X_QUEUE_STALE;
			goto arm_release_gate;
		}
	} else if (!pcm_x_local_holder_snapshot_exact_locked(tag_slot, tag_ref, holder_snapshot,
														 holders, holder_count)) {
		result = PCM_X_QUEUE_STALE;
		goto arm_release_gate;
	}
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, ref)) {
			result = PCM_X_QUEUE_BUSY;
			goto arm_release_gate;
		}
		if (!pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
			if (tag_slot->blocker_set_generation != 0
				&& tag_slot->blocker_set_generation != UINT64_MAX
				&& tag_slot->blocker_snapshot_count == blocker_count
				&& tag_slot->blocker_snapshot_crc32c == set_crc32c
				&& pcm_x_transfer_leg_exact(&tag_slot->blocker_snapshot_reliable,
											PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT,
											authenticated_master_node, authenticated_master_session)
				&& pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
				&& pcm_x_local_blocker_chain_validate(
					&blocker_view, tag_slot->blocker_snapshot_head_index,
					tag_slot->blocker_snapshot_count, ref, tag_ref,
					tag_slot->blocker_set_generation, PCM_XB_LIVE, set_crc32c, NULL)) {
				result = PCM_X_QUEUE_DUPLICATE;
				goto arm_output;
			}
			result = PCM_X_QUEUE_BUSY;
			goto arm_release_gate;
		}
		/* A type-48 freeze reserves this ref with generation zero.  After an
		 * exact ACK, the same ref remains as a tombstone and a later PROBE
		 * advances N -> N+1; neither case conflicts with the new arm.  The
		 * reservation must otherwise be byte-exact so torn or stale state cannot
		 * be normalized into a new blocker generation. */
		if (tag_slot->blocker_set_generation == 0) {
			if (!pcm_x_local_blocker_freeze_reservation_exact(tag_slot)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto arm_domain_done;
			}
		} else if (!pcm_x_local_blocker_ack_tombstone_exact(tag_slot, authenticated_master_node)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto arm_domain_done;
		}
	}
	if (!cluster_pcm_x_generation_next(tag_slot->blocker_set_generation, &set_generation)
		|| set_generation == UINT64_MAX) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto arm_domain_done;
	}
	if (!pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
		|| !cluster_pcm_x_generation_next(tag_slot->blocker_snapshot_reliable.state_sequence,
										  &next_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto arm_domain_done;
	}
	if (!pcm_x_local_blocker_chain_relabel_reserved(reserved_head, (uint32)blocker_count, ref,
													tag_ref, reserved_generation, set_generation)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto arm_domain_done;
	}
	reserved_generation = set_generation;
	if (!pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
		|| !pcm_x_local_blocker_chain_validate(&blocker_view, reserved_head, (uint32)blocker_count,
											   ref, tag_ref, set_generation,
											   PCM_XB_RESERVED_NONVISIBLE, set_crc32c, NULL)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto arm_domain_done;
	}
	pcm_x_blocker_chain_state_write(&blocker_view, reserved_head, (uint32)blocker_count,
									PCM_XB_LIVE);
	tag_slot->blocker_snapshot_ref = *ref;
	tag_slot->blocker_set_generation = set_generation;
	tag_slot->blocker_snapshot_head_index = reserved_head;
	tag_slot->blocker_snapshot_count = (uint32)blocker_count;
	tag_slot->blocker_snapshot_crc32c = set_crc32c;
	tag_slot->blocker_snapshot_next_chunk = (uint32)blocker_count;
	tag_slot->blocker_snapshot_reliable.state_sequence = next_sequence;
	tag_slot->blocker_snapshot_reliable.retry_deadline_ms = 0;
	tag_slot->blocker_snapshot_reliable.expected_responder_session = authenticated_master_session;
	tag_slot->blocker_snapshot_reliable.retry_count = 0;
	tag_slot->blocker_snapshot_reliable.last_responder_node = 0;
	tag_slot->blocker_snapshot_reliable.expected_responder_node = authenticated_master_node;
	tag_slot->blocker_snapshot_reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT;
	tag_slot->blocker_snapshot_reliable.last_response_opcode = 0;
	tag_slot->blocker_snapshot_reliable.phase = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT;
	tag_slot->blocker_snapshot_reliable.flags = 0;
	tag_slot->blocker_snapshot_reliable.reserved = 0;
	if (new_tag)
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
	chain_published = true;
	result = PCM_X_QUEUE_OK;

arm_output:
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		snapshot_out->ref = tag_slot->blocker_snapshot_ref;
		snapshot_out->set_generation = tag_slot->blocker_set_generation;
		snapshot_out->reliable_state_sequence = tag_slot->blocker_snapshot_reliable.state_sequence;
		snapshot_out->blocker_count = tag_slot->blocker_snapshot_count;
		snapshot_out->set_crc32c = tag_slot->blocker_snapshot_crc32c;
	}
arm_release_gate:
	if (gate_claimed && !fail_closed && (!new_tag || chain_published)) {
		if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
	}
arm_domain_done:
	LWLockRelease(&header->local_locks[partition].lock);

	if (!chain_published) {
		pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE,
										 gate_claimed ? (tag_slot != NULL ? tag_slot : prepared_tag)
													  : NULL);
		if (!pcm_x_local_blocker_chain_release_locked(reserved_head, (uint32)blocker_count, ref,
													  tag_ref, reserved_generation,
													  PCM_XB_RESERVED_NONVISIBLE)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		LWLockRelease(&header->allocator_lock.lock);
	}
arm_cleanup_new_tag:
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE && !new_tag && gate_claimed
		&& !fail_closed) {
		if (!pcm_x_local_gate_release(prepared_tag)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
	}
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE && new_tag && !fail_closed
		&& !pcm_x_local_holder_transfer_tag_abort_new(&ref->identity.tag, tag_ref, prepared_tag)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_blocker_snapshot_lookup_exact(const PcmXTicketRef *ref,
												  int32 authenticated_master_node,
												  uint64 authenticated_master_session,
												  PcmXLocalBlockerSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (header == NULL || ref == NULL || snapshot_out == NULL || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity) || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || !pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_STALE;
		goto lookup_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto lookup_done;
	}
	if (pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		if (tag_slot->blocker_set_generation != 0
			|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
			|| tag_slot->blocker_snapshot_next_chunk != 0
			|| !pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result = PCM_X_QUEUE_NOT_FOUND;
		goto lookup_done;
	}
	if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, ref)) {
		result = PCM_X_QUEUE_BUSY;
		goto lookup_done;
	}
	if (pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
		if (pcm_x_local_blocker_freeze_reservation_exact(tag_slot)
			|| pcm_x_local_blocker_ack_tombstone_exact(tag_slot, authenticated_master_node))
			result = PCM_X_QUEUE_NOT_FOUND;
		else {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto lookup_done;
	}
	if (tag_slot->blocker_set_generation == 0 || tag_slot->blocker_set_generation == UINT64_MAX
		|| tag_slot->blocker_snapshot_reliable.state_sequence == 0
		|| tag_slot->blocker_snapshot_count > header->layout.max_wait_edges
		|| tag_slot->blocker_snapshot_next_chunk != tag_slot->blocker_snapshot_count) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto lookup_done;
	}
	if (!pcm_x_transfer_leg_exact(&tag_slot->blocker_snapshot_reliable,
								  PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, authenticated_master_node,
								  authenticated_master_session)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto lookup_done;
	}
	snapshot_out->ref = tag_slot->blocker_snapshot_ref;
	snapshot_out->set_generation = tag_slot->blocker_set_generation;
	snapshot_out->reliable_state_sequence = tag_slot->blocker_snapshot_reliable.state_sequence;
	snapshot_out->blocker_count = tag_slot->blocker_snapshot_count;
	snapshot_out->set_crc32c = tag_slot->blocker_snapshot_crc32c;
	result = PCM_X_QUEUE_OK;

lookup_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_blocker_snapshot_copy_exact(const PcmXLocalBlockerSnapshot *snapshot,
												PcmXBlockerSetHeaderPayload *header_out,
												PcmXBlockerChunkPayload *edges_out,
												Size edge_capacity)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (header_out != NULL)
		memset(header_out, 0, sizeof(*header_out));
	if (header == NULL || snapshot == NULL || header_out == NULL
		|| (snapshot->blocker_count != 0 && edges_out == NULL)
		|| snapshot->ref.grant_generation != 0
		|| !pcm_x_wait_identity_valid(&snapshot->ref.identity) || snapshot->set_generation == 0
		|| snapshot->set_generation == UINT64_MAX || snapshot->reliable_state_sequence == 0
		|| snapshot->blocker_count > header->layout.max_wait_edges)
		return PCM_X_QUEUE_INVALID;
	if (snapshot->blocker_count > edge_capacity)
		return PCM_X_QUEUE_NO_CAPACITY;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &snapshot->ref.identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&snapshot->ref.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
													 &snapshot->ref.identity.tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || !pcm_x_runtime_token_exact(&runtime, 0))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, &snapshot->ref)
			 || tag_slot->blocker_set_generation != snapshot->set_generation
			 || tag_slot->blocker_snapshot_reliable.state_sequence
					!= snapshot->reliable_state_sequence
			 || tag_slot->blocker_snapshot_count != snapshot->blocker_count
			 || tag_slot->blocker_snapshot_crc32c != snapshot->set_crc32c)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, tag_slot->master_node,
													 tag_slot->master_session_incarnation))
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_transfer_leg_exact(&tag_slot->blocker_snapshot_reliable,
									   PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, tag_slot->master_node,
									   tag_slot->master_session_incarnation))
		result = PCM_X_QUEUE_NOT_READY;
	else if (tag_slot->blocker_snapshot_next_chunk != tag_slot->blocker_snapshot_count
			 || !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
			 || !pcm_x_local_blocker_chain_validate(
				 &blocker_view, tag_slot->blocker_snapshot_head_index,
				 tag_slot->blocker_snapshot_count, &tag_slot->blocker_snapshot_ref, tag_ref,
				 tag_slot->blocker_set_generation, PCM_XB_LIVE, tag_slot->blocker_snapshot_crc32c,
				 edges_out)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	} else {
		header_out->ref = tag_slot->blocker_snapshot_ref;
		header_out->set_generation = tag_slot->blocker_set_generation;
		header_out->nblockers = tag_slot->blocker_snapshot_count;
		header_out->set_crc32c = tag_slot->blocker_snapshot_crc32c;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_blocker_ack_exact(const PcmXTicketRef *ref, uint64 set_generation,
									  int32 authenticated_master_node,
									  uint64 authenticated_master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXAllocatorView blocker_view;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	Size detached_head = PCM_X_INVALID_SLOT_INDEX;
	uint32 detached_count = 0;
	uint32 partition;
	bool gate_claimed = false;
	bool release_chain = false;
	bool fail_closed = false;

	if (header == NULL || ref == NULL || ref->grant_generation != 0
		|| !pcm_x_wait_identity_valid(&ref->identity) || set_generation == 0
		|| set_generation == UINT64_MAX || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto ack_done;
	}
	gate_claimed = true;
	if (!pcm_x_runtime_token_exact(&runtime, 0)
		|| !pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												   authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto ack_release_gate;
	}
	if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, ref)
		|| tag_slot->blocker_set_generation != set_generation) {
		result = PCM_X_QUEUE_STALE;
		goto ack_release_gate;
	}
	if (pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
		if (tag_slot->blocker_snapshot_reliable.state_sequence == 0
			|| tag_slot->blocker_snapshot_reliable.last_response_opcode
				   != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
			|| tag_slot->blocker_snapshot_reliable.last_responder_node
				   != (uint32)authenticated_master_node
			|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
			|| tag_slot->blocker_snapshot_next_chunk != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result = PCM_X_QUEUE_DUPLICATE;
		goto ack_release_gate;
	}
	if (!pcm_x_transfer_leg_exact(&tag_slot->blocker_snapshot_reliable,
								  PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, authenticated_master_node,
								  authenticated_master_session)
		|| tag_slot->blocker_snapshot_next_chunk != tag_slot->blocker_snapshot_count
		|| !pcm_x_allocator_view(PCM_X_ALLOC_BLOCKER, &blocker_view)
		|| !pcm_x_local_blocker_chain_validate(
			&blocker_view, tag_slot->blocker_snapshot_head_index, tag_slot->blocker_snapshot_count,
			ref, tag_ref, set_generation, PCM_XB_LIVE, tag_slot->blocker_snapshot_crc32c, NULL)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto ack_release_gate;
	}
	detached_head = tag_slot->blocker_snapshot_head_index;
	detached_count = tag_slot->blocker_snapshot_count;
	pcm_x_blocker_chain_state_write(&blocker_view, detached_head, detached_count, PCM_XB_DETACHING);
	tag_slot->blocker_snapshot_head_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->blocker_snapshot_count = 0;
	tag_slot->blocker_snapshot_crc32c = 0;
	tag_slot->blocker_snapshot_next_chunk = 0;
	pcm_x_transfer_leg_clear(&tag_slot->blocker_snapshot_reliable,
							 PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK, authenticated_master_node);
	release_chain = true;
	result = PCM_X_QUEUE_OK;

ack_release_gate:
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
ack_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (release_chain) {
		pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, NULL);
		if (!pcm_x_local_blocker_chain_release_locked(detached_head, detached_count, ref, tag_ref,
													  set_generation, PCM_XB_DETACHING)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		LWLockRelease(&header->allocator_lock.lock);
	}
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_revoke_apply_exact(const PcmXRevokePayload *revoke,
											  int32 authenticated_master_node,
											  uint64 authenticated_master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *prepared_tag;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	uint32 flags;
	uint32 partition;
	bool new_tag;
	bool gate_claimed = false;
	bool tag_published = false;
	bool fail_closed = false;

	if (header == NULL || revoke == NULL
		|| !pcm_x_image_id_master_exact(revoke->image_id, authenticated_master_node)
		|| revoke->ref.grant_generation == 0 || !pcm_x_wait_identity_valid(&revoke->ref.identity)
		|| authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (header->peer_frontiers[authenticated_master_node].cluster_epoch
			!= revoke->ref.identity.cluster_epoch
		|| header->peer_frontiers[authenticated_master_node].sender_session_incarnation
			   != authenticated_master_session)
		return PCM_X_QUEUE_STALE;
	result = pcm_x_local_holder_transfer_tag_prepare(&revoke->ref, authenticated_master_node,
													 authenticated_master_session, &prepared_tag,
													 &tag_ref, &new_tag);
	if (result != PCM_X_QUEUE_OK)
		return result;
	gate_claimed = true;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&revoke->ref.identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE,
									 prepared_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &revoke->ref.identity.tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		tag_slot = prepared_tag;
		goto revoke_release_gate;
	}
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto revoke_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto revoke_release_gate;
	}
	if (tag_slot->master_node == -1 && tag_slot->master_session_incarnation == 0) {
		tag_slot->master_node = authenticated_master_node;
		tag_slot->master_session_incarnation = authenticated_master_session;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto revoke_release_gate;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0) {
		result = pcm_x_ticket_ref_equal(&tag_slot->holder_ref, &revoke->ref) ? PCM_X_QUEUE_DUPLICATE
																			 : PCM_X_QUEUE_BUSY;
		goto revoke_release_gate;
	}
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)) {
		if (!pcm_x_ticket_ref_equal(&tag_slot->holder_ref, &revoke->ref)
			|| tag_slot->holder_image.image_id != revoke->image_id) {
			result = PCM_X_QUEUE_STALE;
			goto revoke_release_gate;
		}
		if ((!pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)
			 && !pcm_x_transfer_leg_exact(&tag_slot->holder_reliable,
										  PGRAC_IC_MSG_PCM_X_IMAGE_READY, authenticated_master_node,
										  authenticated_master_session))
			|| (pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)
				&& tag_slot->holder_reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_REVOKE)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto revoke_release_gate;
		}
		result = PCM_X_QUEUE_DUPLICATE;
		goto revoke_release_gate;
	}
	/* Type 49 is the fail-closed boundary that replaces the legacy type-48
	 * unconditional ACK.  The same ticket locator's immutable blocker set must
	 * have received an exact type-48 ACK and reclaimed every blocker slot before
	 * holder revoke state can become visible.  A premature 49 is retryable; an
	 * inconsistent tombstone is corruption. */
	if (pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto revoke_release_gate;
	}
	if (!pcm_x_ticket_locator_equal(&tag_slot->blocker_snapshot_ref, &revoke->ref)) {
		result = PCM_X_QUEUE_STALE;
		goto revoke_release_gate;
	}
	if (!pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto revoke_release_gate;
	}
	if (tag_slot->blocker_set_generation == 0
		|| tag_slot->blocker_snapshot_reliable.state_sequence == 0
		|| tag_slot->blocker_snapshot_reliable.last_response_opcode
			   != PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
		|| tag_slot->blocker_snapshot_reliable.last_responder_node
			   != (uint32)authenticated_master_node
		|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
		|| tag_slot->blocker_snapshot_next_chunk != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto revoke_release_gate;
	}
	if (!pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)
		|| tag_slot->holder_terminal_drain_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto revoke_release_gate;
	}
	tag_slot->holder_ref = revoke->ref;
	memset(&tag_slot->holder_image, 0, sizeof(tag_slot->holder_image));
	tag_slot->holder_image.image_id = revoke->image_id;
	pcm_x_transfer_leg_clear(&tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_REVOKE,
							 authenticated_master_node);
	memset(&tag_slot->blocker_snapshot_ref, 0, sizeof(tag_slot->blocker_snapshot_ref));
	tag_slot->blocker_set_generation = 0;
	memset(&tag_slot->blocker_snapshot_reliable, 0, sizeof(tag_slot->blocker_snapshot_reliable));
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_REVOKE_BARRIER);
	if (new_tag) {
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
		tag_published = true;
	}
	result = PCM_X_QUEUE_OK;

revoke_release_gate:
	if (gate_claimed && !fail_closed && (!new_tag || tag_published)) {
		if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
	}
revoke_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE && new_tag && !fail_closed
		&& !pcm_x_local_holder_transfer_tag_abort_new(&revoke->ref.identity.tag, tag_ref,
													  prepared_tag)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_progress_exact(const BufferTag *tag,
										  PcmXLocalHolderProgress *progress_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (progress_out != NULL)
		memset(progress_out, 0, sizeof(*progress_out));
	if (header == NULL || tag == NULL || progress_out == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, 0))
		result = PCM_X_QUEUE_NOT_READY;
	else if (pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref))
		result = PCM_X_QUEUE_NOT_FOUND;
	else if (!pcm_x_local_terminal_ref_valid(&tag_slot->holder_ref)
			 || tag_slot->holder_ref.grant_generation == 0 || tag_slot->holder_image.image_id == 0
			 || !pcm_x_local_holder_transfer_peer_exact(tag_slot, tag_slot->master_node,
														tag_slot->master_session_incarnation)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	} else {
		progress_out->ref = tag_slot->holder_ref;
		progress_out->image = tag_slot->holder_image;
		progress_out->reliable_state_sequence = tag_slot->holder_reliable.state_sequence;
		progress_out->pending_opcode = tag_slot->holder_reliable.pending_opcode;
		progress_out->last_response_opcode = tag_slot->holder_reliable.last_response_opcode;
		progress_out->phase = tag_slot->holder_reliable.phase;
		/* Expose terminal holder state, not the reliable-leg reserved flags.
		 * Consumers need this positive evidence after DRAIN clears the leg. */
		progress_out->flags = (uint16)(pcm_x_slot_flags_read(&tag_slot->slot)
									   & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
		progress_out->master_session_incarnation = tag_slot->master_session_incarnation;
		progress_out->master_node = tag_slot->master_node;
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		memset(progress_out, 0, sizeof(*progress_out));
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_queue_invalidate_authorize_exact(const BufferTag *tag, uint64 cluster_epoch,
													 uint64 request_id, int32 requester_node,
													 int32 authenticated_master_node,
													 uint64 authenticated_master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 flags;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || tag == NULL || request_id == 0 || requester_node < 0
		|| requester_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (header->peer_frontiers[authenticated_master_node].cluster_epoch != cluster_epoch
		|| header->peer_frontiers[authenticated_master_node].sender_session_incarnation
			   != authenticated_master_session)
		return PCM_X_QUEUE_STALE;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL)
		result = PCM_X_QUEUE_STALE;
	else if (!pcm_x_runtime_token_exact(&runtime, 0))
		result = PCM_X_QUEUE_NOT_READY;
	else if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
													 authenticated_master_session))
		result = PCM_X_QUEUE_STALE;
	else {
		flags = pcm_x_slot_flags_read(&tag_slot->slot);
		if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0
			|| (flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0)
			result = PCM_X_QUEUE_NOT_READY;
		else if (!pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref))
			result = PCM_X_QUEUE_NOT_READY;
		else if (pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref))
			result = PCM_X_QUEUE_NOT_READY;
		else if (tag_slot->blocker_snapshot_ref.grant_generation != 0
				 || !pcm_x_local_admission_ref_valid(&tag_slot->blocker_snapshot_ref,
													 &tag_slot->blocker_snapshot_ref.identity)
				 || !BufferTagsEqual(tag, &tag_slot->blocker_snapshot_ref.identity.tag)
				 || tag_slot->blocker_snapshot_ref.identity.cluster_epoch != cluster_epoch
				 || tag_slot->blocker_snapshot_ref.identity.request_id != request_id
				 || tag_slot->blocker_snapshot_ref.identity.node_id != requester_node)
			result = PCM_X_QUEUE_STALE;
		else if (!pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
				 || tag_slot->blocker_set_generation == 0)
			result = PCM_X_QUEUE_NOT_READY;
		else if (tag_slot->blocker_set_generation == UINT64_MAX
				 || tag_slot->blocker_snapshot_reliable.state_sequence == 0
				 || tag_slot->blocker_snapshot_reliable.last_response_opcode
						!= PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK
				 || tag_slot->blocker_snapshot_reliable.last_responder_node
						!= (uint32)authenticated_master_node
				 || tag_slot->blocker_snapshot_reliable.response_tombstone_mask != 0
				 || tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
				 || tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
				 || tag_slot->blocker_snapshot_next_chunk != 0
				 || (flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0
				 || tag_slot->holder_terminal_drain_generation != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_holder_image_ready_arm_exact(const PcmXGrantPayload *image_ready,
												 int32 authenticated_master_node,
												 uint64 authenticated_master_session,
												 PcmXGrantPayload *replay_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint64 next_sequence;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;

	if (replay_out != NULL)
		memset(replay_out, 0, sizeof(*replay_out));
	if (header == NULL || image_ready == NULL || replay_out == NULL
		|| image_ready->ref.grant_generation == 0
		|| !pcm_x_wait_identity_valid(&image_ready->ref.identity)
		|| !pcm_x_image_token_valid(&image_ready->image)
		|| !pcm_x_image_id_master_exact(image_ready->image.image_id, authenticated_master_node)
		|| authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG,
												   &image_ready->ref.identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);
	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&image_ready->ref.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
													 &image_ready->ref.identity.tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto ready_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto ready_done;
	}
	gate_claimed = true;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ready_release_gate;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)
		|| !pcm_x_ticket_ref_equal(&tag_slot->holder_ref, &image_ready->ref)
		|| tag_slot->holder_image.image_id != image_ready->image.image_id) {
		result = PCM_X_QUEUE_STALE;
		goto ready_release_gate;
	}
	if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ready_release_gate;
	}
	if (!pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)) {
		if (!pcm_x_transfer_leg_exact(&tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_IMAGE_READY,
									  authenticated_master_node, authenticated_master_session)) {
			result = PCM_X_QUEUE_BUSY;
			goto ready_release_gate;
		}
		result = pcm_x_image_token_equal(&tag_slot->holder_image, &image_ready->image)
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_STALE;
		goto ready_output;
	}
	if (tag_slot->holder_reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_REVOKE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto ready_release_gate;
	}
	if (!cluster_pcm_x_generation_next(tag_slot->holder_reliable.state_sequence, &next_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto ready_release_gate;
	}
	tag_slot->holder_image = image_ready->image;
	tag_slot->holder_reliable.state_sequence = next_sequence;
	tag_slot->holder_reliable.expected_responder_session = authenticated_master_session;
	tag_slot->holder_reliable.expected_responder_node = authenticated_master_node;
	tag_slot->holder_reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	tag_slot->holder_reliable.phase = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	tag_slot->holder_reliable.flags = 0;
	tag_slot->holder_reliable.reserved = 0;
	result = PCM_X_QUEUE_OK;

ready_output:
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		replay_out->ref = tag_slot->holder_ref;
		replay_out->image = tag_slot->holder_image;
	}
ready_release_gate:
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
ready_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_join_begin(const PcmXWaitIdentity *identity, int32 master_node,
							   uint64 master_session_incarnation, PcmXLocalHandle *handle_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXLocalTagSlot *tag_slot = NULL;
	PcmXLocalMembershipSlot *member = NULL;
	PcmXSlotHeader *raw_slot;
	PcmXSlotRef tag_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef member_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef existing;
	PcmXDirectoryResult directory_result;
	PcmXAllocatorResult allocator_result;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	uint64 local_sequence = 0;
	uint32 flags;
	uint32 local_round;
	uint32 partition;
	bool new_tag = false;
	bool gate_claimed = false;
	bool tag_directory_published = false;
	bool member_directory_published = false;
	bool fail_closed = false;
	bool adopt_holder_only_tag = false;

	pcm_x_local_handle_clear(handle_out);
	if (header == NULL || handle_out == NULL || !pcm_x_wait_identity_valid(identity)
		|| master_node < 0 || master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| master_session_incarnation == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	if (!pcm_x_runtime_token_exact(&runtime, runtime.master_session_incarnation)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	frontier = &header->peer_frontiers[master_node];
	if (frontier->cluster_epoch == 0 && frontier->sender_session_incarnation == 0
		&& frontier->next_expected_prehandle_sequence == 1
		&& frontier->retired_prehandle_sequence == 0) {
		result = PCM_X_QUEUE_NOT_READY;
		goto allocator_done;
	}
	if (frontier->cluster_epoch != identity->cluster_epoch
		|| frontier->sender_session_incarnation != master_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto allocator_done;
	}
	result = pcm_x_local_member_lookup_locked(identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = member->tag_slot_index;
		tag_ref.slot_generation = member->tag_slot_generation;
		goto allocator_duplicate;
	}
	if (result != PCM_X_QUEUE_NOT_FOUND) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto allocator_done;
	}
	result = pcm_x_local_retire_episode_state_locked(header);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto allocator_done;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &identity->tag, &tag_ref);
	if (directory_result == PCM_X_DIRECTORY_OK) {
		tag_slot
			= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
		result = pcm_x_resident_tag_state_result(tag_slot == NULL ? NULL : &tag_slot->slot);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		if (!BufferTagsEqual(&tag_slot->tag, &identity->tag)
			|| tag_slot->cluster_epoch != identity->cluster_epoch) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		result = pcm_x_local_gate_try_acquire(tag_slot);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		gate_claimed = true;
		if (tag_slot->master_node == -1 && tag_slot->master_session_incarnation == 0)
			adopt_holder_only_tag = true;
		else if (tag_slot->master_node != master_node
				 || tag_slot->master_session_incarnation != master_session_incarnation) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		if (adopt_holder_only_tag
			&& (tag_slot->active_holder_head_index == PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->membership_count != 0
				|| tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->blocker_snapshot_count != 0 || tag_slot->next_sequence != 1
				|| tag_slot->reliable.pending_opcode != 0 || tag_slot->reliable.phase != 0
				|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER)
					   != 0)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto allocator_done;
		}
		flags = pcm_x_slot_flags_read(&tag_slot->slot);
		if (tag_slot->next_sequence == 0 || tag_slot->next_sequence == UINT64_MAX
			|| tag_slot->membership_count == SIZE_MAX
			|| (((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0)
				&& tag_slot->local_round == UINT32_MAX)) {
			result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
			goto allocator_done;
		}
		local_sequence = tag_slot->next_sequence;
	} else if (directory_result == PCM_X_DIRECTORY_NOT_FOUND) {
		allocator_result
			= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_LOCAL_TAG, &tag_ref, &raw_slot);
		if (allocator_result != PCM_X_ALLOC_OK) {
			result = pcm_x_queue_result_from_allocator(allocator_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto allocator_done;
		}
		new_tag = true;
		gate_claimed = true;
		tag_slot = (PcmXLocalTagSlot *)raw_slot;
		pcm_x_local_tag_init_common(tag_slot, &identity->tag, identity->cluster_epoch, master_node,
									master_session_incarnation);
		local_sequence = 1;
	} else {
		result = pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto allocator_done;
	}

	allocator_result
		= pcm_x_allocator_reserve_locked(PCM_X_ALLOC_LOCAL_WAIT, &member_ref, &raw_slot);
	if (allocator_result != PCM_X_ALLOC_OK) {
		result = pcm_x_queue_result_from_allocator(allocator_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		if (new_tag
			&& !pcm_x_local_stage_rollback_locked(true, false, false, &identity->tag, identity,
												  tag_ref, NULL, member_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	member = (PcmXLocalMembershipSlot *)raw_slot;
	member->identity = *identity;
	member->local_sequence = local_sequence;
	member->tag_slot_index = tag_ref.slot_index;
	member->tag_slot_generation = tag_ref.slot_generation;
	member->next_index = PCM_X_INVALID_SLOT_INDEX;
	member->prev_index = PCM_X_INVALID_SLOT_INDEX;
	member->buffer_id = -1;
	member->partition = PCM_X_LOCAL_PARTITION_WAIT;

	if (new_tag) {
		directory_result = pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_TAG, &identity->tag,
														 tag_ref, &existing);
		if (directory_result != PCM_X_DIRECTORY_OK) {
			result = directory_result == PCM_X_DIRECTORY_EXISTS
						 ? PCM_X_QUEUE_CORRUPT
						 : pcm_x_queue_result_from_directory(directory_result);
			fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
			if (!pcm_x_local_stage_rollback_locked(true, false, false, &identity->tag, identity,
												   tag_ref, member, member_ref)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
			}
			goto allocator_done;
		}
		tag_directory_published = true;
	}
	directory_result
		= pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_WAIT, identity, member_ref, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		result = directory_result == PCM_X_DIRECTORY_EXISTS
					 ? PCM_X_QUEUE_CORRUPT
					 : pcm_x_queue_result_from_directory(directory_result);
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		if (!pcm_x_local_stage_rollback_locked(new_tag, tag_directory_published, false,
											   &identity->tag, identity, tag_ref, member,
											   member_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		}
		goto allocator_done;
	}
	member_directory_published = true;
	(void)member_directory_published;
	tag_slot->next_sequence = local_sequence + 1;
	result = PCM_X_QUEUE_OK;

allocator_done:
	if (result != PCM_X_QUEUE_OK && gate_claimed && !new_tag) {
		if (fail_closed) {
			/*
			 * A post-claim allocator invariant failed.  Keep the gate for
			 * recovery and report CORRUPT; retryable outcomes release below.
			 */
			result = PCM_X_QUEUE_CORRUPT;
		} else {
			if (!pcm_x_local_gate_release(tag_slot)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
			}
			gate_claimed = false;
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity->tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, tag_slot);
	{
		PcmXAllocatorView tag_view;
		PcmXLocalTagSlot *raw_tag;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &tag_view)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		raw_tag = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&tag_view, tag_ref.slot_index);
		if (raw_tag == NULL
			|| (pcm_x_slot_flags_read(&raw_tag->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		pg_read_barrier();
	}
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &identity->tag,
		PCM_X_STATE_BIT(new_tag ? PCM_X_TAG_RESERVED_NONVISIBLE : PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &identity->tag,
		PCM_X_STATE_BIT(PCM_XL_RESERVED_NONVISIBLE));
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_wait_identity_equal(&member->identity, identity)
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (tag_slot->membership_count == SIZE_MAX) {
		/* The allocator phase checked this while holding the same gate. */
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (adopt_holder_only_tag) {
		if (tag_slot->master_node != -1 || tag_slot->master_session_incarnation != 0
			|| tag_slot->membership_count != 0 || tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		tag_slot->master_node = master_node;
		tag_slot->master_session_incarnation = master_session_incarnation;
	} else if (tag_slot->master_node != master_node
			   || tag_slot->master_session_incarnation != master_session_incarnation) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		tag_slot->head_index = member_ref.slot_index;
	} else {
		PcmXAllocatorView member_view;
		PcmXLocalMembershipSlot *tail;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		tail = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, tag_slot->tail_index);
		if (tail == NULL || tail->next_index != PCM_X_INVALID_SLOT_INDEX
			|| tail->tag_slot_index != tag_ref.slot_index
			|| tail->tag_slot_generation != tag_ref.slot_generation) {
			result = PCM_X_QUEUE_CORRUPT;
			goto domain_done;
		}
		member->prev_index = tag_slot->tail_index;
		tail->next_index = member_ref.slot_index;
	}
	tag_slot->tail_index = member_ref.slot_index;
	tag_slot->membership_count++;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	local_round = tag_slot->local_round;
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0)
		local_round++;
	member->admitted_round = local_round;
	/* A closed round has no leader by construction.  New arrivals belong to
	 * next_round and must remain non-waitable followers until exact round
	 * retirement reopens the tag and promotes the oldest survivor. */
	if (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
		&& (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
		tag_slot->leader_index = member_ref.slot_index;
		tag_slot->leader_slot_generation = member_ref.slot_generation;
		member->role = PCM_X_LOCAL_ROLE_NODE_LEADER;
		pcm_x_slot_state_write(&member->slot, PCM_XL_NODE_LEADER);
	} else {
		member->role = PCM_X_LOCAL_ROLE_FOLLOWER;
		pcm_x_slot_state_write(&member->slot, PCM_XL_JOINED_NONWAITABLE);
	}
	if (new_tag) {
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_LIVE);
	}
	pcm_x_local_handle_from_member(handle_out, member_ref, member, (PcmXLocalRole)member->role);
	if (!pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto domain_done;
	}
	if (member->role == PCM_X_LOCAL_ROLE_FOLLOWER)
		pcm_x_stats_increment(&header->stats.coalesced_count);
	result = PCM_X_QUEUE_OK;

domain_done:
	/* Any post-publication jump here is CORRUPT and retains the gate. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_runtime_fail_closed();
	return result;

allocator_duplicate:
	LWLockRelease(&header->allocator_lock.lock);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity->tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &identity->tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, &identity->tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (member != NULL && tag_slot != NULL
		&& pcm_x_wait_identity_equal(&member->identity, identity)) {
		pcm_x_local_handle_from_member(handle_out, member_ref, member, (PcmXLocalRole)member->role);
		result = PCM_X_QUEUE_DUPLICATE;
	} else {
		PcmXAllocatorView member_view;
		PcmXAllocatorView tag_view;
		PcmXLocalMembershipSlot *raw_member;
		PcmXLocalTagSlot *raw_tag;
		uint64 member_generation1;
		uint64 member_generation2;
		uint64 tag_generation;

		if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view)
			|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &tag_view)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto duplicate_done;
		}
		raw_member
			= (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, member_ref.slot_index);
		raw_tag = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&tag_view, tag_ref.slot_index);
		if (raw_member != NULL
			&& !pcm_x_slot_generation_read(&raw_member->slot, &member_generation1)) {
			result = PCM_X_QUEUE_STALE;
			goto duplicate_done;
		}
		if (raw_tag != NULL && !pcm_x_slot_generation_read(&raw_tag->slot, &tag_generation)) {
			result = PCM_X_QUEUE_STALE;
			goto duplicate_done;
		}
		if (raw_member != NULL && raw_tag != NULL
			&& member_generation1 == member_ref.slot_generation
			&& pcm_x_slot_state_read(&raw_member->slot) == PCM_XL_RESERVED_NONVISIBLE
			&& tag_generation == tag_ref.slot_generation
			&& (pcm_x_slot_flags_read(&raw_tag->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0) {
			pg_read_barrier();
			if (!pcm_x_slot_generation_read(&raw_member->slot, &member_generation2)) {
				result = PCM_X_QUEUE_STALE;
				goto duplicate_done;
			}
			result
				= member_generation2 == member_ref.slot_generation
						  && pcm_x_slot_state_read(&raw_member->slot) == PCM_XL_RESERVED_NONVISIBLE
						  && (pcm_x_slot_flags_read(&raw_tag->slot)
							  & PCM_X_LOCAL_TAG_F_ADMISSION_GATE)
								 != 0
					  ? PCM_X_QUEUE_BUSY
					  : PCM_X_QUEUE_STALE;
		} else
			result = PCM_X_QUEUE_STALE;
	}

duplicate_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Resolve one already-published local membership without admitting a new one. */
PcmXQueueResult
cluster_pcm_x_local_lookup_exact(const PcmXWaitIdentity *identity, PcmXLocalHandle *handle_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 member_state;
	uint32 partition;
	bool fail_closed = false;

	pcm_x_local_handle_clear(handle_out);
	if (header == NULL || identity == NULL || handle_out == NULL
		|| !pcm_x_wait_identity_valid(identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = member->tag_slot_index;
		tag_ref.slot_generation = member->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity->tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, &identity->tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &identity->tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto lookup_done;
	}
	/* The directory may already carry a rekey target while this resident still
	 * carries the old identity.  Check the cross-lock marker before reading any
	 * identity field. */
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0) {
		result = PCM_X_QUEUE_BUSY;
		goto lookup_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if (!pcm_x_wait_identity_equal(&member->identity, identity)
		|| member->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| !BufferTagsEqual(&tag_slot->tag, &identity->tag)
		|| tag_slot->cluster_epoch != identity->cluster_epoch || tag_slot->master_node < 0
		|| tag_slot->master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| tag_slot->master_session_incarnation == 0 || tag_slot->membership_count == 0
		|| (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
			&& member->role != PCM_X_LOCAL_ROLE_FOLLOWER)
		|| (member->role == PCM_X_LOCAL_ROLE_NODE_LEADER && member_state != PCM_XL_NODE_LEADER
			&& member_state != PCM_XL_REMOTE_WAIT && member_state != PCM_XL_CONTENT_ACTIVE
			&& member_state != PCM_XL_GRANTED)
		|| (member->role == PCM_X_LOCAL_ROLE_FOLLOWER && member_state != PCM_XL_JOINED_NONWAITABLE
			&& member_state != PCM_XL_WAITABLE_FOLLOWER && member_state != PCM_XL_CANCELLED)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto lookup_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto lookup_done;
	}
	pcm_x_local_handle_from_member(handle_out, member_ref, member, (PcmXLocalRole)member->role);
	result = PCM_X_QUEUE_OK;

lookup_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static void
pcm_x_local_reliable_token_clear(PcmXLocalReliableToken *token)
{
	if (token != NULL)
		memset(token, 0, sizeof(*token));
}


static bool
pcm_x_local_reliable_leg_is_clear(const PcmXReliableLegState *leg)
{
	return leg != NULL && leg->retry_deadline_ms == 0 && leg->expected_responder_session == 0
		   && leg->retry_count == 0 && leg->expected_responder_node == 0 && leg->pending_opcode == 0
		   && leg->phase == 0 && leg->flags == 0 && leg->reserved == 0;
}


static bool
pcm_x_ticket_ref_is_zero(const PcmXTicketRef *ref)
{
	PcmXTicketRef zero;

	memset(&zero, 0, sizeof(zero));
	return ref != NULL && pcm_x_ticket_ref_equal(ref, &zero);
}


static bool
pcm_x_local_admission_ref_valid(const PcmXTicketRef *ref, const PcmXWaitIdentity *identity)
{
	return ref != NULL && identity != NULL && ref->handle.ticket_id != 0
		   && ref->handle.queue_generation != 0 && ref->grant_generation == 0
		   && pcm_x_wait_identity_equal(&ref->identity, identity);
}


/* Terminal refs keep the exact locator and the final non-sentinel grant. */
static bool
pcm_x_local_terminal_ref_valid(const PcmXTicketRef *ref)
{
	return ref != NULL && pcm_x_wait_identity_valid(&ref->identity) && ref->handle.ticket_id != 0
		   && ref->handle.queue_generation != 0 && ref->grant_generation != UINT64_MAX;
}


static PcmXQueueResult
pcm_x_local_refs_lookup(const PcmXLocalHandle *leader, PcmXSlotRef *tag_ref_out,
						PcmXSlotRef *member_ref_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(&leader->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref_out->slot_index = member->tag_slot_index;
		tag_ref_out->slot_generation = member->tag_slot_generation;
		*member_ref_out = member_ref;
	}
	LWLockRelease(&header->allocator_lock.lock);
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_progress_exact(const PcmXLocalHandle *handle, PcmXLocalProgress *progress_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 member_state;
	uint32 partition;
	bool fail_closed = false;

	if (progress_out != NULL)
		memset(progress_out, 0, sizeof(*progress_out));
	if (header == NULL || handle == NULL || progress_out == NULL || handle->flags != 0
		|| !pcm_x_wait_identity_valid(&handle->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(handle, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&handle->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &handle->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &handle->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto progress_done;
	}
	/* See cluster_pcm_x_local_lookup_exact(): never dereference a resident
	 * identity while the gated rekey writer owns the local-lock phase. */
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0) {
		result = PCM_X_QUEUE_BUSY;
		goto progress_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if (!pcm_x_wait_identity_equal(&member->identity, &handle->identity)
		|| member->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| !BufferTagsEqual(&tag_slot->tag, &handle->identity.tag)
		|| tag_slot->cluster_epoch != handle->identity.cluster_epoch || tag_slot->master_node < 0
		|| tag_slot->master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| tag_slot->master_session_incarnation == 0 || tag_slot->membership_count == 0
		|| (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
			&& member->role != PCM_X_LOCAL_ROLE_FOLLOWER)
		|| (member->role == PCM_X_LOCAL_ROLE_NODE_LEADER && member_state != PCM_XL_NODE_LEADER
			&& member_state != PCM_XL_REMOTE_WAIT && member_state != PCM_XL_CONTENT_ACTIVE
			&& member_state != PCM_XL_GRANTED)
		|| (member->role == PCM_X_LOCAL_ROLE_FOLLOWER && member_state != PCM_XL_JOINED_NONWAITABLE
			&& member_state != PCM_XL_WAITABLE_FOLLOWER && member_state != PCM_XL_CANCELLED)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto progress_done;
	}
	if (!pcm_x_local_handle_exact(handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto progress_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto progress_done;
	}

	progress_out->identity = member->identity;
	progress_out->ref = tag_slot->ref;
	progress_out->image = tag_slot->image;
	progress_out->local_sequence = member->local_sequence;
	progress_out->reliable_state_sequence = tag_slot->reliable.state_sequence;
	progress_out->local_round = member->admitted_round;
	progress_out->member_state = member_state;
	progress_out->role = member->role;
	progress_out->pending_opcode = tag_slot->reliable.pending_opcode;
	progress_out->last_response_opcode = tag_slot->reliable.last_response_opcode;
	progress_out->phase = tag_slot->reliable.phase;
	progress_out->master_session_incarnation = tag_slot->master_session_incarnation;
	progress_out->master_node = tag_slot->master_node;
	progress_out->grant_base_own_generation = tag_slot->grant_base_own_generation;
	result = PCM_X_QUEUE_OK;

progress_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		memset(progress_out, 0, sizeof(*progress_out));
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_follower_wfg_snapshot_exact(const PcmXLocalHandle *follower,
												PcmXLocalFollowerWfgSnapshot *snapshot_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *blocker;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef blocker_ref;
	PcmXQueueResult result;
	uint32 member_state;
	uint32 partition;
	bool fail_closed = false;

	pcm_x_local_follower_wfg_snapshot_clear(snapshot_out);
	if (header == NULL || follower == NULL || snapshot_out == NULL || follower->flags != 0
		|| follower->role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| !pcm_x_wait_identity_valid(&follower->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(follower, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&follower->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &follower->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &follower->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(follower, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto snapshot_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if (member->role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| (member_state != PCM_XL_JOINED_NONWAITABLE && member_state != PCM_XL_WAITABLE_FOLLOWER)
		|| (pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto snapshot_done;
	}
	if ((member_state == PCM_XL_JOINED_NONWAITABLE) != (member->graph_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto snapshot_done;
	}
	result = pcm_x_local_execution_round_locked(tag_slot, member);
	/* Next-round writers cannot claim yet, but they are real waiters and must
	 * remain visible to the cross-node WFG while the frozen cohort drains. */
	if (result == PCM_X_QUEUE_CORRUPT) {
		fail_closed = true;
		goto snapshot_done;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->active_writer_slot_generation == 0)
		|| ((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->leader_slot_generation == 0))) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	/* A holder-only freeze has no local writer blocker.  The same empty-leader
	 * window is reachable after a closed-round leader drains and before exact
	 * round retirement promotes the oldest late writer.  Keep the backend
	 * behind the barrier without inventing a WFG edge or halting the runtime. */
	if (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX) {
		if (result == PCM_X_QUEUE_BARRIER_CLOSED
			&& tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX) {
			result = PCM_X_QUEUE_BARRIER_CLOSED;
			goto snapshot_done;
		}
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	blocker_ref.slot_index = tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
								 ? tag_slot->active_writer_index
								 : tag_slot->leader_index;
	blocker_ref.slot_generation = tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
									  ? tag_slot->active_writer_slot_generation
									  : tag_slot->leader_slot_generation;
	if (blocker_ref.slot_index == member_ref.slot_index) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	blocker = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, blocker_ref,
														   &follower->identity.tag,
														   PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (blocker == NULL || blocker->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| blocker->tag_slot_index != tag_ref.slot_index
		|| blocker->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_wait_identity_valid(&blocker->identity)
		|| blocker->local_sequence >= member->local_sequence) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	/* An empty active-writer slot is a transient scheduling window after
	 * completion.  Do not publish a new edge to the completed node leader;
	 * the caller retries until the next FIFO writer is active. */
	if ((pcm_x_slot_flags_read(&blocker->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_BUSY;
		goto snapshot_done;
	}

	snapshot_out->waiter = *follower;
	snapshot_out->blocker = blocker->identity;
	snapshot_out->blocker_slot = blocker_ref;
	snapshot_out->waiter_graph_generation = member->graph_generation;
	snapshot_out->leader_index = tag_slot->leader_index;
	snapshot_out->active_writer_index = tag_slot->active_writer_index;
	snapshot_out->leader_slot_generation = tag_slot->leader_slot_generation;
	snapshot_out->active_writer_slot_generation = tag_slot->active_writer_slot_generation;
	snapshot_out->holder_set_generation = tag_slot->holder_set_generation;
	snapshot_out->local_round = member->admitted_round;
	result = PCM_X_QUEUE_OK;

snapshot_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_local_follower_wfg_snapshot_clear(snapshot_out);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_follower_wfg_commit_exact(const PcmXLocalFollowerWfgSnapshot *snapshot,
											  uint64 graph_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *blocker;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef expected_blocker;
	PcmXQueueResult result;
	uint32 member_state;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;

	if (header == NULL || snapshot == NULL || graph_generation == 0 || snapshot->reserved != 0
		|| snapshot->waiter.flags != 0 || snapshot->waiter.role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| snapshot->local_round == 0 || !pcm_x_wait_identity_valid(&snapshot->waiter.identity)
		|| !pcm_x_wait_identity_valid(&snapshot->blocker)
		|| snapshot->blocker_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| snapshot->blocker_slot.slot_generation == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(&snapshot->waiter, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition
		= cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&snapshot->waiter.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
													 &snapshot->waiter.identity.tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &snapshot->waiter.identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(&snapshot->waiter, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto commit_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto commit_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto commit_done;
	}
	gate_claimed = true;
	result = pcm_x_local_execution_round_locked(tag_slot, member);
	/* Barrier-closed next-round writers still publish exact WFG edges; only
	 * execution/claim is held until round retirement. */
	if (result == PCM_X_QUEUE_CORRUPT) {
		fail_closed = true;
		goto commit_release_gate;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if (member->role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| (member_state != PCM_XL_JOINED_NONWAITABLE && member_state != PCM_XL_WAITABLE_FOLLOWER)
		|| (pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto commit_release_gate;
	}
	if ((member_state == PCM_XL_JOINED_NONWAITABLE) != (member->graph_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto commit_done;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->active_writer_slot_generation == 0)
		|| tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->leader_slot_generation == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto commit_done;
	}
	expected_blocker.slot_index = tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
									  ? tag_slot->active_writer_index
									  : tag_slot->leader_index;
	expected_blocker.slot_generation = tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
										   ? tag_slot->active_writer_slot_generation
										   : tag_slot->leader_slot_generation;
	if (snapshot->local_round != member->admitted_round
		|| snapshot->leader_index != tag_slot->leader_index
		|| snapshot->active_writer_index != tag_slot->active_writer_index
		|| snapshot->leader_slot_generation != tag_slot->leader_slot_generation
		|| snapshot->active_writer_slot_generation != tag_slot->active_writer_slot_generation
		|| snapshot->holder_set_generation != tag_slot->holder_set_generation
		|| snapshot->blocker_slot.slot_index != expected_blocker.slot_index
		|| snapshot->blocker_slot.slot_generation != expected_blocker.slot_generation) {
		result = PCM_X_QUEUE_STALE;
		goto commit_release_gate;
	}
	blocker = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, expected_blocker,
														   &snapshot->waiter.identity.tag,
														   PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (blocker == NULL || blocker->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| blocker->tag_slot_index != tag_ref.slot_index
		|| blocker->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_wait_identity_equal(&blocker->identity, &snapshot->blocker)
		|| blocker->local_sequence >= member->local_sequence) {
		result = PCM_X_QUEUE_STALE;
		goto commit_release_gate;
	}
	/* Claim/release can ABA active_writer back to INVALID while leaving the
	 * same leader locator.  COMPLETE closes that semantic ABA: the external
	 * graph must be removed and replaced, never committed against old proof. */
	if ((pcm_x_slot_flags_read(&blocker->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_STALE;
		goto commit_release_gate;
	}
	if (member->graph_generation == graph_generation) {
		result = member_state == PCM_XL_WAITABLE_FOLLOWER ? PCM_X_QUEUE_DUPLICATE
														  : PCM_X_QUEUE_CORRUPT;
		if (result == PCM_X_QUEUE_CORRUPT)
			fail_closed = true;
		goto commit_release_gate;
	}
	if (snapshot->waiter_graph_generation != member->graph_generation) {
		result = PCM_X_QUEUE_STALE;
		goto commit_release_gate;
	}
	member->graph_generation = graph_generation;
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_WAITABLE_FOLLOWER);
	result = PCM_X_QUEUE_OK;

commit_release_gate:
	if (!fail_closed && gate_claimed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
commit_done:
	/* Structural failures retain the gate as recovery evidence. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_follower_wfg_clear_exact(const PcmXLocalHandle *follower,
											 uint64 graph_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 member_state;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;

	if (header == NULL || follower == NULL || graph_generation == 0 || follower->flags != 0
		|| follower->role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| !pcm_x_wait_identity_valid(&follower->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(follower, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&follower->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &follower->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &follower->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(follower, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto clear_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto clear_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto clear_done;
	}
	gate_claimed = true;
	if (member->role != PCM_X_LOCAL_ROLE_FOLLOWER
		|| (pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto clear_release_gate;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if (member_state == PCM_XL_JOINED_NONWAITABLE) {
		if (member->graph_generation != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto clear_done;
		}
		result = PCM_X_QUEUE_DUPLICATE;
		goto clear_release_gate;
	}
	if (member_state != PCM_XL_WAITABLE_FOLLOWER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto clear_release_gate;
	}
	if (member->graph_generation == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto clear_done;
	}
	if (member->graph_generation != graph_generation) {
		result = PCM_X_QUEUE_STALE;
		goto clear_release_gate;
	}
	member->graph_generation = 0;
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_JOINED_NONWAITABLE);
	result = PCM_X_QUEUE_OK;

clear_release_gate:
	if (!fail_closed && gate_claimed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
clear_done:
	/* Structural failures retain the gate as recovery evidence. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_follower_chain_ready(const PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
								 Size first_index, Size expected_prev_index,
								 uint64 expected_prev_sequence, PcmXLocalMembershipSlot **first_out,
								 PcmXSlotRef *first_ref_out, bool *first_promote_out);


/* A promoted successor is directory-visible before terminal image/ticket
 * evidence is retired.  Timed waiter wakeups must not let that successor
 * claim content authority, refreeze the round, or mint a new ENQUEUE while
 * either cleanup lane is still durable. */
static PcmXQueueResult
pcm_x_local_terminal_cleanup_fence(const PcmXLocalTagSlot *tag_slot)
{
	uint32 flags;
	uint32 holder_flags;
	uint32 writer_flags;

	if (tag_slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	writer_flags = flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK;
	holder_flags = flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	if ((writer_flags == 0 && tag_slot->terminal_drain_generation != 0)
		|| (writer_flags != 0 && writer_flags != PCM_X_LOCAL_TAG_F_TERMINAL_READY
			&& writer_flags != PCM_X_LOCAL_TAG_F_TERMINAL_MASK)
		|| (writer_flags == PCM_X_LOCAL_TAG_F_TERMINAL_READY
			&& tag_slot->terminal_drain_generation != 0)
		|| (writer_flags == PCM_X_LOCAL_TAG_F_TERMINAL_MASK
			&& (tag_slot->terminal_drain_generation == 0
				|| tag_slot->terminal_drain_generation == UINT64_MAX))
		|| (holder_flags == 0) != (tag_slot->holder_terminal_drain_generation == 0)
		|| (holder_flags != 0
			&& (holder_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
				|| tag_slot->holder_terminal_drain_generation == 0
				|| tag_slot->holder_terminal_drain_generation == UINT64_MAX)))
		return PCM_X_QUEUE_CORRUPT;
	return writer_flags != 0 || holder_flags != 0 ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_OK;
}


/*
 * Move one promoted local leader to the ownership generation sampled by its
 * caller immediately before the first ENQUEUE.
 *
 * The tag admission gate is the cross-lock transaction marker.  It freezes
 * ENQUEUE, cancellation, writer claims, and terminal cleanup while the local
 * lock proves that the leader has no graph or outbound authority.  The
 * allocator phase then replaces only the immutable directory key.  While its
 * target key names the still-old resident identity, lookup/progress observe
 * the gate and return BUSY without reading that identity.  A final local-lock
 * phase commits the resident identity and then opens the gate.  If target-key
 * publication fails, the old key is restored without ever changing resident
 * bytes.
 */
PcmXQueueResult
cluster_pcm_x_local_leader_rekey_generation_exact(const PcmXLocalHandle *leader,
												  uint64 base_own_generation,
												  PcmXLocalHandle *rekeyed_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXWaitIdentity target_identity;
	PcmXWaitIdentity old_identity;
	PcmXLocalHandle resident_handle;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *gated_tag = NULL;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef found;
	PcmXSlotRef existing;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	PcmXImageToken zero_image;
	PcmXTicketHandle zero_handle;
	uint32 partition;
	uint32 flags;
	uint32 member_flags;
	uint32 state;
	bool fail_closed = false;
	bool gate_claimed = false;
	bool replay_target = false;
	bool migrate = false;

	pcm_x_local_handle_clear(rekeyed_out);
	if (header == NULL || leader == NULL || rekeyed_out == NULL || leader->flags != 0
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	target_identity = leader->identity;
	target_identity.base_own_generation = base_own_generation;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	/* Resolve either the original key or an already-committed target key.  The
	 * latter makes a lost-return replay idempotent without reviving the old
	 * directory key. */
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	result = pcm_x_local_member_lookup_locked(&leader->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_NOT_FOUND
		&& !pcm_x_wait_identity_equal(&leader->identity, &target_identity)) {
		result = pcm_x_local_member_lookup_locked(&target_identity, &member_ref, &member);
		if (result == PCM_X_QUEUE_OK)
			replay_target = true;
	}
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto rekey_allocator_lookup_done;
	}
	if (member_ref.slot_index != leader->membership_slot.slot_index
		|| member_ref.slot_generation != leader->membership_slot.slot_generation) {
		result = replay_target ? PCM_X_QUEUE_CORRUPT : PCM_X_QUEUE_STALE;
		fail_closed = replay_target;
		goto rekey_allocator_lookup_done;
	}
	tag_ref.slot_index = member->tag_slot_index;
	tag_ref.slot_generation = member->tag_slot_generation;
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	resident_handle = *leader;
	if (replay_target)
		resident_handle.identity = target_identity;
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| tag_ref.slot_index != leader->tag_slot.slot_index
		|| tag_ref.slot_generation != leader->tag_slot.slot_generation
		|| member->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| !BufferTagsEqual(&tag_slot->tag, &leader->identity.tag)
		|| tag_slot->cluster_epoch != leader->identity.cluster_epoch
		|| !pcm_x_local_handle_exact(&resident_handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto rekey_allocator_lookup_done;
	}
	if (!replay_target && !pcm_x_wait_identity_equal(&leader->identity, &target_identity)) {
		directory_result
			= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_WAIT, &target_identity, &found);
		if (directory_result == PCM_X_DIRECTORY_OK) {
			/* request_id/wait_seq are immutable; another slot owning the target
			 * generation is an identity alias, never ordinary contention. */
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto rekey_allocator_lookup_done;
		}
		if (directory_result != PCM_X_DIRECTORY_NOT_FOUND) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto rekey_allocator_lookup_done;
		}
	}
	result = pcm_x_local_handoff_gate_claim_locked(header, tag_slot, false, PCM_X_QUEUE_BUSY);
	if (result == PCM_X_QUEUE_OK) {
		gate_claimed = true;
		gated_tag = tag_slot;
	} else if (result == PCM_X_QUEUE_CORRUPT)
		fail_closed = true;

rekey_allocator_lookup_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (fail_closed) {
			pcm_x_runtime_fail_closed();
		}
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, gated_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &leader->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &leader->identity.tag,
														  PCM_X_STATE_BIT(PCM_XL_NODE_LEADER));
	resident_handle = *leader;
	if (replay_target)
		resident_handle.identity = target_identity;
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(&resident_handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto rekey_local_release_gate;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto rekey_local_release_gate;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| (tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			   != (tag_slot->active_writer_slot_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state != PCM_XL_NODE_LEADER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto rekey_local_release_gate;
	}
	member_flags = pcm_x_slot_flags_read(&member->slot);
	if (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation
		|| tag_slot->head_index != member_ref.slot_index
		|| member->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| (member_flags & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_done;
	}
	if (member->graph_generation != 0
		|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_BUSY;
		goto rekey_local_release_gate;
	}
	result = pcm_x_local_terminal_cleanup_fence(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto rekey_local_release_gate;
	}
	memset(&zero_handle, 0, sizeof(zero_handle));
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->ref)
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| tag_slot->prehandle.sender_session_incarnation != 0
		|| tag_slot->prehandle.prehandle_sequence != 0
		|| !pcm_x_ticket_handle_equal(&member->handle, &zero_handle)) {
		result = PCM_X_QUEUE_BUSY;
		goto rekey_local_release_gate;
	}
	memset(&zero_image, 0, sizeof(zero_image));
	if (!pcm_x_image_token_equal(&tag_slot->image, &zero_image)
		|| tag_slot->committed_own_generation != 0 || tag_slot->grant_base_own_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_done;
	}
	if (replay_target || pcm_x_wait_identity_equal(&member->identity, &target_identity)) {
		pcm_x_local_handle_from_member(rekeyed_out, member_ref, member,
									   PCM_X_LOCAL_ROLE_NODE_LEADER);
		result = PCM_X_QUEUE_DUPLICATE;
		goto rekey_local_release_gate;
	}
	if (base_own_generation < member->identity.base_own_generation) {
		result = PCM_X_QUEUE_STALE;
		goto rekey_local_release_gate;
	}
	old_identity = member->identity;
	migrate = true;
	result = PCM_X_QUEUE_OK;
	goto rekey_local_done;

rekey_local_release_gate:
	if (!fail_closed && gate_claimed) {
		if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
	}
rekey_local_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed) {
		pcm_x_runtime_fail_closed();
		return result;
	}
	if (!migrate)
		return result;

	/* The gate remains set across the local-to-allocator handoff.  No local
	 * operation can mint authority while the immutable directory key moves. */
	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, gated_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	member = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_WAIT,
																	  member_ref);
	if (tag_slot == NULL || member == NULL
		|| pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
		|| pcm_x_slot_state_read(&member->slot) != PCM_XL_NODE_LEADER
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| !pcm_x_wait_identity_equal(&member->identity, &old_identity)
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_allocator_commit_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		if (!pcm_x_local_gate_release(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			gate_claimed = false;
		goto rekey_allocator_commit_done;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_WAIT, &old_identity, &found);
	if (directory_result != PCM_X_DIRECTORY_OK || found.slot_index != member_ref.slot_index
		|| found.slot_generation != member_ref.slot_generation) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_allocator_commit_done;
	}
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_WAIT, &target_identity, &found);
	if (directory_result != PCM_X_DIRECTORY_NOT_FOUND) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_allocator_commit_done;
	}
	if (pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_WAIT, &old_identity, member_ref)
		!= PCM_X_DIRECTORY_OK) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_allocator_commit_done;
	}
	directory_result = pcm_x_directory_insert_rekey_target_locked(&target_identity, &old_identity,
																  member_ref, &existing);
	if (directory_result != PCM_X_DIRECTORY_OK) {
		/* Resident bytes never changed.  Republish exactly the old key under
		 * this same allocator lock before closing the runtime. */
		if (pcm_x_directory_insert_locked(PCM_X_DIR_LOCAL_WAIT, &old_identity, member_ref,
										  &existing)
			!= PCM_X_DIRECTORY_OK) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto rekey_allocator_commit_done;
		}
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_allocator_commit_done;
	}
	result = PCM_X_QUEUE_OK;

rekey_allocator_commit_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		pcm_x_local_handle_clear(rekeyed_out);
		if (fail_closed) {
			pcm_x_runtime_fail_closed();
		}
		return result;
	}

	/* The directory now names target_identity while the resident still names
	 * old_identity.  Only this local-lock phase may close that gated window.
	 * Any acquire or validation failure retains the gate and blocks recovery. */
	pcm_x_local_gate_acquire_guarded_retain_gate(&header->local_locks[partition].lock,
												 LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &old_identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &old_identity.tag, PCM_X_STATE_BIT(PCM_XL_NODE_LEADER));
	resident_handle = *leader;
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(&resident_handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_commit_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		fail_closed = true;
		goto rekey_local_commit_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	member_flags = pcm_x_slot_flags_read(&member->slot);
	state = pcm_x_slot_state_read(&member->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| (tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			   != (tag_slot->active_writer_slot_generation == 0)
		|| state != PCM_XL_NODE_LEADER || member->partition != PCM_X_LOCAL_PARTITION_WAIT
		|| member->role != PCM_X_LOCAL_ROLE_NODE_LEADER || tag_slot->membership_count == 0
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation
		|| tag_slot->head_index != member_ref.slot_index
		|| member->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| (member_flags & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0
		|| member->graph_generation != 0
		|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
		|| member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_commit_done;
	}
	if (pcm_x_local_terminal_cleanup_fence(tag_slot) != PCM_X_QUEUE_OK
		|| !pcm_x_ticket_ref_is_zero(&tag_slot->ref)
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| tag_slot->prehandle.sender_session_incarnation != 0
		|| tag_slot->prehandle.prehandle_sequence != 0
		|| !pcm_x_ticket_handle_equal(&member->handle, &zero_handle)
		|| !pcm_x_image_token_equal(&tag_slot->image, &zero_image)
		|| tag_slot->committed_own_generation != 0
		|| !pcm_x_wait_identity_equal(&member->identity, &old_identity)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_commit_done;
	}

	member->identity = target_identity;
	pcm_x_local_handle_from_member(rekeyed_out, member_ref, member, PCM_X_LOCAL_ROLE_NODE_LEADER);
	if (!pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rekey_local_commit_done;
	}
	gate_claimed = false;
	result = PCM_X_QUEUE_OK;

rekey_local_commit_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_local_handle_clear(rekeyed_out);
	if (fail_closed) {
		pcm_x_runtime_fail_closed();
	}
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_writer_claim_exact(const PcmXLocalHandle *writer,
									   PcmXLocalWriterClaim *claim_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *leader;
	PcmXLocalMembershipSlot *candidate;
	PcmXAllocatorView member_view;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef leader_ref;
	PcmXQueueResult result;
	uint32 state;
	uint32 partition;
	Size candidate_index;
	Size previous_index = PCM_X_INVALID_SLOT_INDEX;
	Size visited = 0;
	uint64 previous_sequence = 0;
	uint64 next_claim_generation;
	bool gate_claimed = false;
	bool fail_closed = false;

	pcm_x_local_writer_claim_clear(claim_out);
	if (header == NULL || writer == NULL || claim_out == NULL || writer->flags != 0
		|| (writer->role != PCM_X_LOCAL_ROLE_NODE_LEADER
			&& writer->role != PCM_X_LOCAL_ROLE_FOLLOWER)
		|| !pcm_x_wait_identity_valid(&writer->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(writer, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&writer->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &writer->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &writer->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(writer, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto claim_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto claim_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto claim_done;
	}
	gate_claimed = true;
	result = pcm_x_local_terminal_cleanup_fence(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto claim_release_gate;
	}
	result = pcm_x_local_execution_round_locked(tag_slot, member);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			fail_closed = true;
		goto claim_release_gate;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->active_writer_slot_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto claim_done;
	}
	if (tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_BUSY;
		goto claim_release_gate;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if ((pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto claim_release_gate;
	}
	if (member->role == PCM_X_LOCAL_ROLE_NODE_LEADER) {
		if (tag_slot->leader_index != member_ref.slot_index
			|| tag_slot->leader_slot_generation != member_ref.slot_generation
			|| tag_slot->head_index != member_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto claim_done;
		}
		if (state != PCM_XL_NODE_LEADER && state != PCM_XL_REMOTE_WAIT
			&& state != PCM_XL_CONTENT_ACTIVE && state != PCM_XL_GRANTED) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto claim_release_gate;
		}
	} else {
		if (state != PCM_XL_JOINED_NONWAITABLE || member->graph_generation != 0) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto claim_release_gate;
		}
		if (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->leader_slot_generation == 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto claim_done;
		}
		leader_ref.slot_index = tag_slot->leader_index;
		leader_ref.slot_generation = tag_slot->leader_slot_generation;
		leader = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, leader_ref,
															  &writer->identity.tag,
															  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
		if (leader == NULL || leader->role != PCM_X_LOCAL_ROLE_NODE_LEADER
			|| leader->local_sequence >= member->local_sequence) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto claim_done;
		}
		if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto claim_done;
		}
		candidate_index = tag_slot->head_index;
		while (candidate_index != PCM_X_INVALID_SLOT_INDEX) {
			PcmXSlotRef candidate_ref;
			uint32 candidate_flags;

			if (visited++ >= tag_slot->membership_count || visited > member_view.capacity) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto claim_done;
			}
			candidate
				= (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, candidate_index);
			if (candidate == NULL
				|| !pcm_x_slot_generation_read(&candidate->slot, &candidate_ref.slot_generation)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto claim_done;
			}
			candidate_ref.slot_index = candidate_index;
			candidate = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
				PCM_X_ALLOC_LOCAL_WAIT, candidate_ref, &writer->identity.tag,
				PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
			if (candidate == NULL || candidate->tag_slot_index != tag_ref.slot_index
				|| candidate->tag_slot_generation != tag_ref.slot_generation
				|| candidate->prev_index != previous_index
				|| candidate->admitted_round != member->admitted_round
				|| candidate->local_sequence <= previous_sequence
				|| candidate->local_sequence > member->local_sequence) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto claim_done;
			}
			if (candidate_index == member_ref.slot_index)
				break;
			state = pcm_x_slot_state_read(&candidate->slot);
			candidate_flags = pcm_x_slot_flags_read(&candidate->slot);
			if (candidate_index == tag_slot->leader_index) {
				if (candidate->role != PCM_X_LOCAL_ROLE_NODE_LEADER
					|| (state != PCM_XL_NODE_LEADER && state != PCM_XL_REMOTE_WAIT
						&& state != PCM_XL_CONTENT_ACTIVE && state != PCM_XL_GRANTED)) {
					result = PCM_X_QUEUE_CORRUPT;
					fail_closed = true;
					goto claim_done;
				}
			} else if (candidate->role != PCM_X_LOCAL_ROLE_FOLLOWER
					   || (state != PCM_XL_JOINED_NONWAITABLE && state != PCM_XL_WAITABLE_FOLLOWER)
					   || ((state == PCM_XL_JOINED_NONWAITABLE)
						   != (candidate->graph_generation == 0))) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto claim_done;
			}
			if ((candidate_flags & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) == 0) {
				result = PCM_X_QUEUE_BUSY;
				goto claim_release_gate;
			}
			if (candidate_index != tag_slot->leader_index
				&& (state != PCM_XL_JOINED_NONWAITABLE || candidate->graph_generation != 0)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto claim_done;
			}
			previous_index = candidate_index;
			previous_sequence = candidate->local_sequence;
			candidate_index = candidate->next_index;
		}
		if (candidate_index != member_ref.slot_index) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto claim_done;
		}
	}
	if (!cluster_pcm_x_generation_next(tag_slot->writer_claim_generation, &next_claim_generation)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto claim_done;
	}
	tag_slot->writer_claim_generation = next_claim_generation;
	tag_slot->active_writer_index = member_ref.slot_index;
	tag_slot->active_writer_slot_generation = member_ref.slot_generation;
	pg_write_barrier();
	claim_out->writer = *writer;
	claim_out->active_slot = member_ref;
	claim_out->claim_generation = next_claim_generation;
	claim_out->local_round = member->admitted_round;
	claim_out->role = member->role;
	/* A' rebase: every same-round claim inherits the published effective
	 * grant base under this partition lock; zero (no rebase) keeps the
	 * enqueue-time identity math.  Without this a FIFO follower would verify
	 * the rebased X against its stale base+1 and fail closed. */
	claim_out->grant_base_own_generation = tag_slot->grant_base_own_generation;
	result = PCM_X_QUEUE_OK;

claim_release_gate:
	if (!fail_closed && gate_claimed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
claim_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		pcm_x_local_writer_claim_clear(claim_out);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_writer_claim_finish_exact(const PcmXLocalWriterClaim *claim, bool complete,
									  PcmXWaitIdentity *successor_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXAllocatorView member_view;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *successor;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef successor_ref;
	PcmXWaitIdentity successor_identity;
	PcmXQueueResult result;
	uint32 successor_flags;
	uint32 successor_state;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;
	bool have_successor = false;

	if (successor_out != NULL)
		memset(successor_out, 0, sizeof(*successor_out));
	memset(&successor_identity, 0, sizeof(successor_identity));

	if (header == NULL || claim == NULL || claim->flags != 0 || claim->writer.flags != 0
		|| claim->active_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| claim->active_slot.slot_generation == 0 || claim->claim_generation == 0
		|| claim->local_round == 0 || claim->role != claim->writer.role
		|| !pcm_x_wait_identity_valid(&claim->writer.identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(&claim->writer, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&claim->writer.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref,
													 &claim->writer.identity.tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &claim->writer.identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(&claim->writer, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto release_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto release_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto release_done;
	}
	gate_claimed = true;
	if (claim->active_slot.slot_index != member_ref.slot_index
		|| claim->active_slot.slot_generation != member_ref.slot_generation
		|| claim->claim_generation != tag_slot->writer_claim_generation
		|| tag_slot->active_writer_index != member_ref.slot_index
		|| tag_slot->active_writer_slot_generation != member_ref.slot_generation
		|| claim->local_round != member->admitted_round || claim->role != member->role) {
		result = PCM_X_QUEUE_STALE;
		goto release_gate;
	}
	if ((pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto release_done;
	}
	/* Collect only the generation-exact direct successor in this closed local
	 * round.  Validation precedes WRITER_COMPLETE, so a corrupt link cannot
	 * publish completion or a wake hint.  A next-round follower remains asleep
	 * until the RETIRE watermark promotes that round. */
	if (complete && successor_out != NULL) {
		if (member->next_index == PCM_X_INVALID_SLOT_INDEX) {
			if (tag_slot->tail_index != member_ref.slot_index) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
		} else {
			if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
			successor_ref.slot_index = member->next_index;
			successor = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view,
																		successor_ref.slot_index);
			if (successor == NULL
				|| !pcm_x_slot_generation_read(&successor->slot, &successor_ref.slot_generation)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
			successor = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
				PCM_X_ALLOC_LOCAL_WAIT, successor_ref, &claim->writer.identity.tag,
				PCM_X_STATE_BIT(PCM_XL_JOINED_NONWAITABLE)
					| PCM_X_STATE_BIT(PCM_XL_WAITABLE_FOLLOWER));
			if (successor == NULL || successor->tag_slot_index != tag_ref.slot_index
				|| successor->tag_slot_generation != tag_ref.slot_generation
				|| successor->prev_index != member_ref.slot_index
				|| successor->local_sequence <= member->local_sequence
				|| successor->local_sequence >= tag_slot->next_sequence
				|| successor->role != PCM_X_LOCAL_ROLE_FOLLOWER
				|| !pcm_x_wait_identity_valid(&successor->identity)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
			successor_flags = pcm_x_slot_flags_read(&successor->slot);
			successor_state = pcm_x_slot_state_read(&successor->slot);
			if ((successor_flags & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0
				|| (successor_state == PCM_XL_JOINED_NONWAITABLE
					&& successor->graph_generation != 0)
				|| (successor_state == PCM_XL_WAITABLE_FOLLOWER
					&& successor->graph_generation == 0)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
			if (successor->admitted_round == claim->local_round) {
				successor_identity = successor->identity;
				have_successor = true;
			} else if (claim->local_round == UINT32_MAX
					   || successor->admitted_round != claim->local_round + 1
					   || (pcm_x_slot_flags_read(&tag_slot->slot)
						   & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER)
							  == 0
					   || member->local_sequence > tag_slot->cutoff_sequence
					   || successor->local_sequence <= tag_slot->cutoff_sequence) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto release_done;
			}
		}
	}
	if (complete)
		(void)pcm_x_slot_flags_fetch_or(&member->slot, PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE);
	pg_write_barrier();
	tag_slot->active_writer_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->active_writer_slot_generation = 0;
	result = PCM_X_QUEUE_OK;

release_gate:
	if (!fail_closed && gate_claimed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
release_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result == PCM_X_QUEUE_OK && complete && successor_out != NULL && have_successor)
		*successor_out = successor_identity;
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_writer_claim_release_collect_exact(const PcmXLocalWriterClaim *claim,
													   PcmXWaitIdentity *successor_out)
{
	if (successor_out == NULL)
		return PCM_X_QUEUE_INVALID;
	return pcm_x_local_writer_claim_finish_exact(claim, true, successor_out);
}


PcmXQueueResult
cluster_pcm_x_local_writer_claim_release_exact(const PcmXLocalWriterClaim *claim)
{
	return pcm_x_local_writer_claim_finish_exact(claim, true, NULL);
}


PcmXQueueResult
cluster_pcm_x_local_writer_claim_abort_exact(const PcmXLocalWriterClaim *claim)
{
	return pcm_x_local_writer_claim_finish_exact(claim, false, NULL);
}


static PcmXQueueResult
pcm_x_local_leader_slots_in_states_exact(const PcmXLocalHandle *leader, PcmXSlotRef tag_ref,
										 PcmXSlotRef member_ref, uint32 allowed_states,
										 PcmXLocalTagSlot **tag_slot_out,
										 PcmXLocalMembershipSlot **member_out)
{
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	uint32 state;

	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &leader->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &leader->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(leader, member_ref, member, tag_slot))
		return PCM_X_QUEUE_STALE;
	state = pcm_x_slot_state_read(&member->slot);
	if ((allowed_states & PCM_X_STATE_BIT(state)) == 0)
		return PCM_X_QUEUE_BAD_STATE;
	if (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation
		|| tag_slot->master_node < 0 || tag_slot->master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| tag_slot->cluster_epoch != leader->identity.cluster_epoch
		|| tag_slot->master_session_incarnation == 0)
		return PCM_X_QUEUE_CORRUPT;
	*tag_slot_out = tag_slot;
	*member_out = member;
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
pcm_x_local_leader_slots_exact(const PcmXLocalHandle *leader, PcmXSlotRef tag_ref,
							   PcmXSlotRef member_ref, PcmXLocalTagSlot **tag_slot_out,
							   PcmXLocalMembershipSlot **member_out)
{
	return pcm_x_local_leader_slots_in_states_exact(leader, tag_ref, member_ref,
													PCM_X_STATE_BIT(PCM_XL_NODE_LEADER)
														| PCM_X_STATE_BIT(PCM_XL_REMOTE_WAIT),
													tag_slot_out, member_out);
}


/* Reliable transfer ledgers remain live after the queue leader progresses
 * beyond REMOTE_WAIT.  Snapshot/retry must therefore accept the two
 * application phases as well; admission/cancel callers keep the narrower
 * helper above and cannot accidentally operate on a granted writer. */
static PcmXQueueResult
pcm_x_local_reliable_slots_exact(const PcmXLocalHandle *leader, PcmXSlotRef tag_ref,
								 PcmXSlotRef member_ref, PcmXLocalTagSlot **tag_slot_out,
								 PcmXLocalMembershipSlot **member_out)
{
	return pcm_x_local_leader_slots_in_states_exact(
		leader, tag_ref, member_ref,
		PCM_X_STATE_BIT(PCM_XL_NODE_LEADER) | PCM_X_STATE_BIT(PCM_XL_REMOTE_WAIT)
			| PCM_X_STATE_BIT(PCM_XL_CONTENT_ACTIVE) | PCM_X_STATE_BIT(PCM_XL_GRANTED),
		tag_slot_out, member_out);
}


static PcmXQueueResult
pcm_x_local_outbound_authority_exact(const PcmXLocalTagSlot *tag_slot,
									 PcmXOutboundTargetFrontier **outbound_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXOutboundTargetFrontier *outbound;
	PcmXPeerFrontier *peer;

	if (tag_slot == NULL || tag_slot->master_node < 0
		|| tag_slot->master_node >= PCM_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_QUEUE_CORRUPT;
	outbound = &header->outbound_targets[tag_slot->master_node];
	peer = &header->peer_frontiers[tag_slot->master_node];
	if ((outbound->flags & ~PCM_X_OUTBOUND_TARGET_KNOWN_FLAGS) != 0
		|| (outbound->flags & PCM_X_OUTBOUND_TARGET_INITIALIZED) == 0
		|| outbound->next_prehandle_sequence == 0 || peer->next_expected_prehandle_sequence == 0
		|| peer->retired_prehandle_sequence >= peer->next_expected_prehandle_sequence)
		return PCM_X_QUEUE_CORRUPT;
	if (outbound->flags != (PCM_X_OUTBOUND_TARGET_INITIALIZED | PCM_X_OUTBOUND_TARGET_BOUND)
		|| outbound->cluster_epoch != tag_slot->cluster_epoch
		|| outbound->target_session_incarnation != tag_slot->master_session_incarnation
		|| peer->cluster_epoch != tag_slot->cluster_epoch
		|| peer->sender_session_incarnation != tag_slot->master_session_incarnation)
		return PCM_X_QUEUE_STALE;
	*outbound_out = outbound;
	return PCM_X_QUEUE_OK;
}


static bool
pcm_x_local_reliable_leg_valid(const PcmXLocalTagSlot *tag_slot,
							   const PcmXLocalMembershipSlot *member)
{
	const PcmXReliableLegState *leg;
	uint32 member_state;

	if (tag_slot == NULL || member == NULL)
		return false;
	leg = &tag_slot->reliable;
	if (leg->state_sequence == 0 || leg->expected_responder_session == 0
		|| leg->expected_responder_node != tag_slot->master_node
		|| leg->expected_responder_session != tag_slot->master_session_incarnation
		|| tag_slot->prehandle.sender_session_incarnation == 0
		|| tag_slot->prehandle.prehandle_sequence == 0 || leg->flags != 0 || leg->reserved != 0)
		return false;
	member_state = pcm_x_slot_state_read(&member->slot);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_ENQUEUE)
		return leg->phase == PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE
			   && member_state == PCM_XL_NODE_LEADER && pcm_x_ticket_ref_is_zero(&tag_slot->ref);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM)
		return leg->phase == PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM
			   && member_state == PCM_XL_NODE_LEADER
			   && pcm_x_local_admission_ref_valid(&tag_slot->ref, &member->identity);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL)
		return leg->phase == PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL
			   && member_state == PCM_XL_NODE_LEADER && pcm_x_ticket_ref_is_zero(&tag_slot->ref);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_CANCEL)
		return leg->phase == PCM_X_LOCAL_RELIABLE_PHASE_CANCEL && member_state == PCM_XL_REMOTE_WAIT
			   && pcm_x_local_admission_ref_valid(&tag_slot->ref, &member->identity);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY)
		return leg->phase == PGRAC_IC_MSG_PCM_X_INSTALL_READY
			   && member_state == PCM_XL_CONTENT_ACTIVE && tag_slot->ref.grant_generation != 0
			   && pcm_x_image_token_valid(&tag_slot->image);
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK)
		return leg->phase == PGRAC_IC_MSG_PCM_X_FINAL_ACK && member_state == PCM_XL_CONTENT_ACTIVE
			   && tag_slot->ref.grant_generation != 0 && pcm_x_image_token_valid(&tag_slot->image)
			   && tag_slot->committed_own_generation != 0;
	if (leg->pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM)
		return leg->phase == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM && member_state == PCM_XL_GRANTED
			   && tag_slot->ref.grant_generation != 0 && pcm_x_image_token_valid(&tag_slot->image)
			   && tag_slot->committed_own_generation != 0;
	return false;
}


static void
pcm_x_local_reliable_token_from_slot(PcmXLocalReliableToken *token,
									 const PcmXLocalTagSlot *tag_slot,
									 const PcmXLocalMembershipSlot *member)
{
	pcm_x_local_reliable_token_clear(token);
	token->identity = member->identity;
	token->prehandle = tag_slot->prehandle;
	token->ref = tag_slot->ref;
	token->state_sequence = tag_slot->reliable.state_sequence;
	token->retry_deadline_ms = tag_slot->reliable.retry_deadline_ms;
	token->expected_responder_session = tag_slot->reliable.expected_responder_session;
	token->retry_count = tag_slot->reliable.retry_count;
	token->expected_responder_node = tag_slot->reliable.expected_responder_node;
	token->pending_opcode = tag_slot->reliable.pending_opcode;
	token->phase = tag_slot->reliable.phase;
	token->flags = tag_slot->reliable.flags;
}


static bool
pcm_x_local_reliable_token_exact(const PcmXLocalReliableToken *expected,
								 const PcmXLocalTagSlot *tag_slot,
								 const PcmXLocalMembershipSlot *member)
{
	return expected != NULL && expected->reserved == 0
		   && pcm_x_wait_identity_equal(&expected->identity, &member->identity)
		   && pcm_x_prehandle_equal(&expected->prehandle, &tag_slot->prehandle)
		   && pcm_x_ticket_ref_equal(&expected->ref, &tag_slot->ref)
		   && expected->state_sequence == tag_slot->reliable.state_sequence
		   && expected->expected_responder_session == tag_slot->reliable.expected_responder_session
		   && expected->expected_responder_node == tag_slot->reliable.expected_responder_node
		   && expected->pending_opcode == tag_slot->reliable.pending_opcode
		   && expected->phase == tag_slot->reliable.phase
		   && expected->flags == tag_slot->reliable.flags;
}


static bool
pcm_x_local_response_matches(uint16 pending_opcode, uint16 response_opcode)
{
	return (pending_opcode == PGRAC_IC_MSG_PCM_X_ENQUEUE
			&& response_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_ACK)
		   || (pending_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM
			   && response_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK)
		   || (pending_opcode == PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL
			   && response_opcode == PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK)
		   || (pending_opcode == PGRAC_IC_MSG_PCM_X_CANCEL
			   && response_opcode == PGRAC_IC_MSG_PCM_X_CANCEL_ACK)
		   || (pending_opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY
			   && response_opcode == PGRAC_IC_MSG_PCM_X_COMMIT_X)
		   || (pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK
			   && response_opcode == PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK);
}


static void
pcm_x_local_reliable_clear(PcmXReliableLegState *leg, uint16 response_opcode,
						   int32 authenticated_node)
{
	leg->retry_deadline_ms = 0;
	leg->expected_responder_session = 0;
	leg->retry_count = 0;
	leg->last_responder_node = (uint32)authenticated_node;
	leg->expected_responder_node = 0;
	leg->pending_opcode = 0;
	leg->last_response_opcode = response_opcode;
	leg->phase = 0;
	leg->flags = 0;
	leg->reserved = 0;
}


PcmXQueueResult
cluster_pcm_x_local_enqueue_arm_exact(const PcmXLocalHandle *leader,
									  PcmXEnqueuePayload *payload_out,
									  PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound = NULL;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	PcmXPrehandleKey prehandle;
	uint64 next_state_sequence;
	uint32 expected_gate = 0;
	uint32 partition;
	bool fail_closed = false;
	bool mint_gate_claimed = false;
	bool tag_gate_claimed = false;

	if (payload_out != NULL)
		memset(payload_out, 0, sizeof(*payload_out));
	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || payload_out == NULL || token_out == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto enqueue_done;
	if (pcm_x_slot_state_read(&member->slot) != PCM_XL_NODE_LEADER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto enqueue_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto enqueue_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto enqueue_done;
	}
	tag_gate_claimed = true;
	result = pcm_x_local_terminal_cleanup_fence(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto enqueue_done;
	}
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto enqueue_done;
		}
		if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_ENQUEUE) {
			result = PCM_X_QUEUE_BUSY;
			goto enqueue_done;
		}
		payload_out->identity = member->identity;
		payload_out->prehandle = tag_slot->prehandle;
		pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
		result = PCM_X_QUEUE_DUPLICATE;
		goto enqueue_done;
	}
	if (tag_slot->prehandle.sender_session_incarnation != 0
		|| tag_slot->prehandle.prehandle_sequence != 0
		|| !pcm_x_ticket_ref_is_zero(&tag_slot->ref)) {
		if ((tag_slot->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK
			 && tag_slot->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_CANCEL_ACK)
			|| tag_slot->prehandle.sender_session_incarnation == 0
			|| tag_slot->prehandle.prehandle_sequence == 0
			|| !pcm_x_local_admission_ref_valid(&tag_slot->ref, &tag_slot->ref.identity)
			|| pcm_x_wait_identity_equal(&tag_slot->ref.identity, &member->identity)) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto enqueue_done;
		}
		/* Retire the preceding leader's duplicate-ACK tombstone immediately
		 * before minting this successor's new prehandle. */
		memset(&tag_slot->prehandle, 0, sizeof(tag_slot->prehandle));
		memset(&tag_slot->ref, 0, sizeof(tag_slot->ref));
	}
	outbound = &header->outbound_targets[tag_slot->master_node];
	if (!pg_atomic_compare_exchange_u32(&outbound->mint_gate, &expected_gate, 1)) {
		result = PCM_X_QUEUE_BUSY;
		goto enqueue_done;
	}
	mint_gate_claimed = true;
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto enqueue_done;
	}
	if (outbound->next_prehandle_sequence == UINT64_MAX
		|| !cluster_pcm_x_generation_next(tag_slot->reliable.state_sequence,
										  &next_state_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto enqueue_done;
	}
	prehandle.sender_session_incarnation = runtime.master_session_incarnation;
	prehandle.prehandle_sequence = outbound->next_prehandle_sequence;
	tag_slot->prehandle = prehandle;
	tag_slot->reliable.state_sequence = next_state_sequence;
	tag_slot->reliable.retry_deadline_ms = 0;
	tag_slot->reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->reliable.retry_count = 0;
	tag_slot->reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_ENQUEUE;
	tag_slot->reliable.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	tag_slot->reliable.flags = 0;
	tag_slot->reliable.reserved = 0;
	/* The durable local resend authority precedes the no-hole counter mint. */
	pg_write_barrier();
	outbound->next_prehandle_sequence = prehandle.prehandle_sequence + 1;
	payload_out->identity = member->identity;
	payload_out->prehandle = prehandle;
	pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	result = PCM_X_QUEUE_OK;

enqueue_done:
	if (mint_gate_claimed) {
		pg_write_barrier();
		pg_atomic_write_u32(&outbound->mint_gate, 0);
	}
	if (tag_gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_reliable_snapshot_exact(const PcmXLocalHandle *leader,
											PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || token_out == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	result = pcm_x_local_reliable_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto snapshot_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto snapshot_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto snapshot_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto snapshot_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	result = PCM_X_QUEUE_OK;

snapshot_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_reliable_retry_exact(const PcmXLocalHandle *leader,
										 const PcmXLocalReliableToken *expected,
										 uint64 retry_deadline_ms,
										 PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || expected == NULL || token_out == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_reliable_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto retry_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto retry_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto retry_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto retry_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto retry_done;
	}
	if (!pcm_x_local_reliable_token_exact(expected, tag_slot, member)) {
		result = PCM_X_QUEUE_STALE;
		goto retry_done;
	}
	if (tag_slot->reliable.retry_count != PG_UINT32_MAX)
		tag_slot->reliable.retry_count++;
	tag_slot->reliable.retry_deadline_ms = retry_deadline_ms;
	pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	result = PCM_X_QUEUE_OK;

retry_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_duplicate_response_exact(const PcmXLocalHandle *leader, const PcmXTicketRef *actual_ref,
									 const PcmXPrehandleKey *actual_prehandle,
									 uint16 response_opcode, uint32 expected_member_state,
									 int32 authenticated_node, uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto duplicate_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto duplicate_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto duplicate_done;
	}
	if (pcm_x_slot_state_read(&member->slot) != expected_member_state
		|| tag_slot->reliable.last_response_opcode != response_opcode) {
		result = PCM_X_QUEUE_NOT_READY;
		goto duplicate_done;
	}
	if (authenticated_node != tag_slot->master_node
		|| authenticated_session != tag_slot->master_session_incarnation
		|| !pcm_x_ticket_ref_equal(actual_ref, &tag_slot->ref)
		|| (actual_prehandle != NULL
			&& !pcm_x_prehandle_equal(actual_prehandle, &tag_slot->prehandle))) {
		result = PCM_X_QUEUE_STALE;
		goto duplicate_done;
	}
	result = PCM_X_QUEUE_DUPLICATE;

duplicate_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_reliable_ack_exact(const PcmXLocalHandle *leader,
									   const PcmXLocalReliableToken *expected,
									   const PcmXTicketRef *actual_ref,
									   const PcmXPrehandleKey *actual_prehandle,
									   uint16 response_opcode, int32 authenticated_node,
									   uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || leader == NULL || expected == NULL || actual_ref == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity) || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| (response_opcode != PGRAC_IC_MSG_PCM_X_ADMIT_ACK
			&& response_opcode != PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto ack_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto ack_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto ack_done;
	}
	if (!pcm_x_local_reliable_token_exact(expected, tag_slot, member)
		|| !pcm_x_local_response_matches(tag_slot->reliable.pending_opcode, response_opcode)
		|| authenticated_node != tag_slot->reliable.expected_responder_node
		|| authenticated_session != tag_slot->reliable.expected_responder_session) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	if (tag_slot->reliable.pending_opcode == PGRAC_IC_MSG_PCM_X_ENQUEUE) {
		if (actual_prehandle == NULL
			|| !pcm_x_prehandle_equal(actual_prehandle, &tag_slot->prehandle)
			|| !pcm_x_local_admission_ref_valid(actual_ref, &member->identity)) {
			result = PCM_X_QUEUE_STALE;
			goto ack_done;
		}
		tag_slot->ref = *actual_ref;
		member->handle = actual_ref->handle;
	} else if (!pcm_x_ticket_ref_equal(actual_ref, &tag_slot->ref)
			   || (actual_prehandle != NULL
				   && !pcm_x_prehandle_equal(actual_prehandle, &tag_slot->prehandle))) {
		result = PCM_X_QUEUE_STALE;
		goto ack_done;
	}
	pg_write_barrier();
	pcm_x_local_reliable_clear(&tag_slot->reliable, response_opcode, authenticated_node);
	result = PCM_X_QUEUE_OK;

ack_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_apply_admit_ack_exact(const PcmXLocalHandle *leader,
										  const PcmXAdmitAckPayload *ack, int32 authenticated_node,
										  uint64 authenticated_session)
{
	const uint16 known_flags = (uint16)(PCM_X_ADMIT_F_EXACT_REPLAY | PCM_X_ADMIT_F_NODE_COALESCED
										| PCM_X_ADMIT_F_QUEUE_HEAD);
	PcmXLocalReliableToken token;
	PcmXQueueResult result;
	PcmXQueueResult duplicate_result;

	if (ClusterPcmXConvertShmem == NULL || leader == NULL || ack == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity) || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| (ack->result != PCM_X_QUEUE_OK && ack->result != PCM_X_QUEUE_DUPLICATE)
		|| ack->phase != PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE || (ack->flags & ~known_flags) != 0
		|| ack->prehandle.sender_session_incarnation == 0 || ack->prehandle.prehandle_sequence == 0
		|| !pcm_x_wait_identity_valid(&ack->ref.identity) || ack->ref.handle.ticket_id == 0
		|| ack->ref.handle.queue_generation == 0 || ack->ref.grant_generation != 0)
		return PCM_X_QUEUE_INVALID;
	result = cluster_pcm_x_local_reliable_snapshot_exact(leader, &token);
	if (result == PCM_X_QUEUE_OK && token.pending_opcode == PGRAC_IC_MSG_PCM_X_ENQUEUE) {
		if (!pcm_x_prehandle_equal(&token.prehandle, &ack->prehandle)
			|| !pcm_x_wait_identity_equal(&token.identity, &ack->ref.identity))
			return PCM_X_QUEUE_STALE;
		result = cluster_pcm_x_local_reliable_ack_exact(leader, &token, &ack->ref, &ack->prehandle,
														PGRAC_IC_MSG_PCM_X_ADMIT_ACK,
														authenticated_node, authenticated_session);
		if (result == PCM_X_QUEUE_OK)
			return result;
		if (result != PCM_X_QUEUE_NOT_READY && result != PCM_X_QUEUE_STALE)
			return result;
		duplicate_result = pcm_x_local_duplicate_response_exact(
			leader, &ack->ref, &ack->prehandle, PGRAC_IC_MSG_PCM_X_ADMIT_ACK, PCM_XL_NODE_LEADER,
			authenticated_node, authenticated_session);
		return duplicate_result == PCM_X_QUEUE_DUPLICATE ? duplicate_result : result;
	}
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_NOT_READY)
		return result;
	return pcm_x_local_duplicate_response_exact(leader, &ack->ref, &ack->prehandle,
												PGRAC_IC_MSG_PCM_X_ADMIT_ACK, PCM_XL_NODE_LEADER,
												authenticated_node, authenticated_session);
}


PcmXQueueResult
cluster_pcm_x_local_admit_confirm_arm_exact(const PcmXLocalHandle *leader,
											PcmXPhasePayload *payload_out,
											PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint64 next_state_sequence;
	uint32 partition;
	bool fail_closed = false;

	if (payload_out != NULL)
		memset(payload_out, 0, sizeof(*payload_out));
	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || payload_out == NULL || token_out == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto confirm_arm_done;
	if (pcm_x_slot_state_read(&member->slot) != PCM_XL_NODE_LEADER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto confirm_arm_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto confirm_arm_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto confirm_arm_done;
	}
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto confirm_arm_done;
		}
		if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM) {
			result = PCM_X_QUEUE_BUSY;
			goto confirm_arm_done;
		}
		payload_out->ref = tag_slot->ref;
		payload_out->phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
		pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
		result = PCM_X_QUEUE_DUPLICATE;
		goto confirm_arm_done;
	}
	if (!pcm_x_local_admission_ref_valid(&tag_slot->ref, &member->identity)
		|| tag_slot->prehandle.sender_session_incarnation == 0
		|| tag_slot->prehandle.prehandle_sequence == 0) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto confirm_arm_done;
	}
	if (!cluster_pcm_x_generation_next(tag_slot->reliable.state_sequence, &next_state_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto confirm_arm_done;
	}
	tag_slot->reliable.state_sequence = next_state_sequence;
	tag_slot->reliable.retry_deadline_ms = 0;
	tag_slot->reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->reliable.retry_count = 0;
	tag_slot->reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM;
	tag_slot->reliable.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
	tag_slot->reliable.flags = 0;
	tag_slot->reliable.reserved = 0;
	pg_write_barrier();
	payload_out->ref = tag_slot->ref;
	payload_out->phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
	pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	result = PCM_X_QUEUE_OK;

confirm_arm_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_admit_confirm_ack_exact(const PcmXLocalHandle *leader,
											const PcmXPhasePayload *ack, int32 authenticated_node,
											uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 state;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || leader == NULL || ack == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity) || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| ack->reason != 0 || ack->phase != PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM
		|| ack->flags != 0 || !pcm_x_wait_identity_valid(&ack->ref.identity)
		|| ack->ref.handle.ticket_id == 0 || ack->ref.handle.queue_generation == 0
		|| ack->ref.grant_generation != 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto confirm_ack_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto confirm_ack_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto confirm_ack_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state == PCM_XL_REMOTE_WAIT) {
		if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
			&& tag_slot->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK
			&& authenticated_node == tag_slot->master_node
			&& authenticated_session == tag_slot->master_session_incarnation
			&& pcm_x_ticket_ref_equal(&ack->ref, &tag_slot->ref))
			result = PCM_X_QUEUE_DUPLICATE;
		else
			result = PCM_X_QUEUE_STALE;
		goto confirm_ack_done;
	}
	if (state != PCM_XL_NODE_LEADER) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto confirm_ack_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto confirm_ack_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto confirm_ack_done;
	}
	if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM
		|| tag_slot->reliable.phase != PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM
		|| authenticated_node != tag_slot->reliable.expected_responder_node
		|| authenticated_session != tag_slot->reliable.expected_responder_session
		|| !pcm_x_ticket_ref_equal(&ack->ref, &tag_slot->ref)) {
		result = PCM_X_QUEUE_STALE;
		goto confirm_ack_done;
	}
	pcm_x_local_reliable_clear(&tag_slot->reliable, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK,
							   authenticated_node);
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_REMOTE_WAIT);
	result = PCM_X_QUEUE_OK;

confirm_ack_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_transfer_slots_exact(const PcmXLocalHandle *leader, PcmXSlotRef tag_ref,
								 PcmXSlotRef member_ref, PcmXLocalTagSlot **tag_slot_out,
								 PcmXLocalMembershipSlot **member_out)
{
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	uint32 state;

	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &leader->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &leader->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(leader, member_ref, member, tag_slot))
		return PCM_X_QUEUE_STALE;
	state = pcm_x_slot_state_read(&member->slot);
	if (state != PCM_XL_REMOTE_WAIT && state != PCM_XL_CONTENT_ACTIVE && state != PCM_XL_GRANTED)
		return PCM_X_QUEUE_BAD_STATE;
	if (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation
		|| tag_slot->master_node < 0 || tag_slot->master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| tag_slot->cluster_epoch != leader->identity.cluster_epoch
		|| tag_slot->master_session_incarnation == 0)
		return PCM_X_QUEUE_CORRUPT;
	*tag_slot_out = tag_slot;
	*member_out = member;
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_local_prepare_grant_exact(const PcmXLocalHandle *leader,
										const PcmXGrantPayload *prepare, int32 authenticated_node,
										uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 state;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || leader == NULL || prepare == NULL || prepare->ref.grant_generation == 0
		|| !pcm_x_image_token_valid(&prepare->image)
		|| !pcm_x_image_id_master_exact(prepare->image.image_id, authenticated_node)
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0 || !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_transfer_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto prepare_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto prepare_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto prepare_done;
	}
	/* Outbound authority proves the peer frontier; bind the DATA source to it. */
	if (authenticated_node != tag_slot->master_node
		|| authenticated_session != tag_slot->master_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto prepare_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state == PCM_XL_REMOTE_WAIT || state == PCM_XL_CONTENT_ACTIVE || state == PCM_XL_GRANTED) {
		result
			= pcm_x_ticket_ref_equal(&tag_slot->ref, &prepare->ref)
					  && pcm_x_image_token_equal(&tag_slot->image, &prepare->image)
					  && (tag_slot->reliable.last_response_opcode
							  == PGRAC_IC_MSG_PCM_X_PREPARE_GRANT
						  || tag_slot->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_COMMIT_X
						  || tag_slot->reliable.last_response_opcode
								 == PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK)
				  ? PCM_X_QUEUE_DUPLICATE
				  : PCM_X_QUEUE_STALE;
		if (result == PCM_X_QUEUE_DUPLICATE
			|| tag_slot->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_PREPARE_GRANT)
			goto prepare_done;
	}
	if (state != PCM_XL_REMOTE_WAIT) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto prepare_done;
	}
	if (!pcm_x_ticket_locator_equal(&tag_slot->ref, &prepare->ref)
		|| !pcm_x_wait_identity_equal(&prepare->ref.identity, &member->identity)) {
		result = PCM_X_QUEUE_STALE;
		goto prepare_done;
	}
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto prepare_done;
		}
		/* The master may cross ACTIVE_PROBE->ACTIVE_TRANSFER after the local
		 * requester has armed CANCEL.  An exact PREPARE_GRANT is the durable
		 * proof that transfer won that race; consume only that CANCEL leg so
		 * neither side waits forever on the other terminal path. */
		if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_CANCEL
			|| tag_slot->reliable.phase != PCM_X_LOCAL_RELIABLE_PHASE_CANCEL) {
			result = PCM_X_QUEUE_BUSY;
			goto prepare_done;
		}
		pcm_x_local_reliable_clear(&tag_slot->reliable, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT,
								   authenticated_node);
	} else if (tag_slot->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto prepare_done;
	}
	tag_slot->ref = prepare->ref;
	tag_slot->image = prepare->image;
	tag_slot->committed_own_generation = 0;
	tag_slot->grant_base_own_generation = 0;
	tag_slot->reliable.last_responder_node = (uint32)authenticated_node;
	tag_slot->reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_PREPARE_GRANT;
	pg_write_barrier();
	result = PCM_X_QUEUE_OK;

prepare_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_transfer_arm_exact(const PcmXLocalHandle *leader, uint16 opcode,
							   const PcmXTicketRef *expected_ref,
							   const PcmXImageToken *expected_image,
							   uint64 committed_own_generation,
							   PcmXInstallReadyPayload *install_ready_out,
							   PcmXFinalAckPayload *final_ack_out,
							   PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint64 expected_generation;
	uint64 next_sequence;
	uint16 expected_response;
	uint32 partition;
	bool fail_closed = false;

	if (install_ready_out != NULL)
		memset(install_ready_out, 0, sizeof(*install_ready_out));
	if (final_ack_out != NULL)
		memset(final_ack_out, 0, sizeof(*final_ack_out));
	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || token_out == NULL
		|| (opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY
			&& (expected_ref == NULL || expected_image == NULL || install_ready_out == NULL
				|| final_ack_out != NULL || committed_own_generation != 0
				|| expected_ref->grant_generation == 0 || !pcm_x_image_token_valid(expected_image)))
		|| (opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK
			&& (expected_ref != NULL || expected_image != NULL || final_ack_out == NULL
				|| install_ready_out != NULL || committed_own_generation == 0))
		|| (opcode != PGRAC_IC_MSG_PCM_X_INSTALL_READY && opcode != PGRAC_IC_MSG_PCM_X_FINAL_ACK)
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	if (opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK) {
		/* Monotonic floor only; the exact "+1" proof re-runs under the local
		 * lock against the effective grant base. */
		if (!cluster_pcm_x_generation_next(leader->identity.base_own_generation,
										   &expected_generation))
			return PCM_X_QUEUE_COUNTER_EXHAUSTED;
		if (committed_own_generation < expected_generation)
			return PCM_X_QUEUE_STALE;
	}
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_transfer_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto arm_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto arm_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto arm_done;
	}
	if (tag_slot->ref.grant_generation == 0 || !pcm_x_image_token_valid(&tag_slot->image)) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto arm_done;
	}
	if (opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY) {
		if (!pcm_x_ticket_ref_equal(&tag_slot->ref, expected_ref)
			|| !pcm_x_image_token_equal(&tag_slot->image, expected_image)) {
			result = PCM_X_QUEUE_STALE;
			goto arm_done;
		}
		if (pcm_x_slot_state_read(&member->slot) != PCM_XL_REMOTE_WAIT
			&& pcm_x_slot_state_read(&member->slot) != PCM_XL_CONTENT_ACTIVE) {
			result = PCM_X_QUEUE_BAD_STATE;
			goto arm_done;
		}
	} else if (pcm_x_slot_state_read(&member->slot) != PCM_XL_CONTENT_ACTIVE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto arm_done;
	}
	expected_response = opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY
							? PGRAC_IC_MSG_PCM_X_PREPARE_GRANT
							: PGRAC_IC_MSG_PCM_X_COMMIT_X;
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto arm_done;
		}
		result = tag_slot->reliable.pending_opcode == opcode && tag_slot->reliable.phase == opcode
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_BUSY;
		if (result == PCM_X_QUEUE_DUPLICATE && opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK
			&& tag_slot->committed_own_generation != committed_own_generation)
			result = PCM_X_QUEUE_STALE;
		goto arm_output;
	}
	if (opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY
		&& pcm_x_slot_state_read(&member->slot) != PCM_XL_REMOTE_WAIT) {
		result = PCM_X_QUEUE_NOT_READY;
		goto arm_done;
	}
	if (tag_slot->reliable.last_response_opcode != expected_response) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto arm_done;
	}
	if (opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK) {
		if (!cluster_pcm_x_generation_next(pcm_x_local_effective_grant_base(tag_slot),
										   &expected_generation)) {
			result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
			fail_closed = true;
			goto arm_done;
		}
		/* FINAL_ACK proves exactly one ownership round under this grant. */
		if (committed_own_generation != expected_generation) {
			result = PCM_X_QUEUE_STALE;
			goto arm_done;
		}
	}
	if (!cluster_pcm_x_generation_next(tag_slot->reliable.state_sequence, &next_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto arm_done;
	}
	tag_slot->reliable.state_sequence = next_sequence;
	tag_slot->reliable.retry_deadline_ms = 0;
	tag_slot->reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->reliable.retry_count = 0;
	tag_slot->reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->reliable.pending_opcode = opcode;
	tag_slot->reliable.phase = opcode;
	tag_slot->reliable.flags = 0;
	tag_slot->reliable.reserved = 0;
	if (opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK)
		tag_slot->committed_own_generation = committed_own_generation;
	pg_write_barrier();
	if (opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY)
		pcm_x_slot_state_write(&member->slot, PCM_XL_CONTENT_ACTIVE);
	result = PCM_X_QUEUE_OK;

arm_output:
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		if (opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY) {
			install_ready_out->ref = tag_slot->ref;
			install_ready_out->image_id = tag_slot->image.image_id;
			install_ready_out->result = PCM_X_QUEUE_OK;
			install_ready_out->phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;
			install_ready_out->rebased_own_generation = tag_slot->grant_base_own_generation;
		} else {
			final_ack_out->ref = tag_slot->ref;
			final_ack_out->image_id = tag_slot->image.image_id;
			final_ack_out->committed_own_generation = tag_slot->committed_own_generation;
		}
		pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	}

arm_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


/*
 * One-shot pre-reservation publication of the effective grant base for a
 * prepared transfer whose enqueue-time base was consumed by an interleaved
 * revoke.  Runs strictly between PREPARE_GRANT apply and the INSTALL_READY
 * arm: the reliable leg must be clear with PREPARE_GRANT as its last
 * response.  A same-value replay is DUPLICATE; a different second value is
 * evidence divergence and fails the runtime closed.  The immutable identity
 * (directory key) is never touched.
 */
PcmXQueueResult
cluster_pcm_x_local_grant_rebase_publish_exact(const PcmXLocalHandle *leader,
											   uint64 rebased_own_generation)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || leader == NULL || rebased_own_generation == 0
		|| rebased_own_generation == UINT64_MAX || !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	if (rebased_own_generation <= leader->identity.base_own_generation)
		return PCM_X_QUEUE_STALE;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_transfer_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto rebase_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto rebase_done;
	}
	/* Only a prepared, not-yet-armed transfer may move its grant base. */
	if (tag_slot->ref.grant_generation == 0 || !pcm_x_image_token_valid(&tag_slot->image)
		|| pcm_x_slot_state_read(&member->slot) != PCM_XL_REMOTE_WAIT
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| tag_slot->reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_PREPARE_GRANT) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto rebase_done;
	}
	if (tag_slot->grant_base_own_generation == rebased_own_generation) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto rebase_done;
	}
	if (tag_slot->grant_base_own_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto rebase_done;
	}
	tag_slot->grant_base_own_generation = rebased_own_generation;
	pg_write_barrier();
	result = PCM_X_QUEUE_OK;

rebase_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_install_ready_arm_exact(const PcmXLocalHandle *leader,
											const PcmXTicketRef *expected_ref,
											const PcmXImageToken *expected_image,
											PcmXInstallReadyPayload *install_ready_out,
											PcmXLocalReliableToken *token_out)
{
	return pcm_x_local_transfer_arm_exact(leader, PGRAC_IC_MSG_PCM_X_INSTALL_READY, expected_ref,
										  expected_image, 0, install_ready_out, NULL, token_out);
}


PcmXQueueResult
cluster_pcm_x_local_commit_x_exact(const PcmXLocalHandle *leader, const PcmXPhasePayload *commit,
								   int32 authenticated_node, uint64 authenticated_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 partition;
	bool fail_closed = false;

	if (header == NULL || leader == NULL || commit == NULL || commit->ref.grant_generation == 0
		|| commit->reason != 0 || commit->phase != PGRAC_IC_MSG_PCM_X_COMMIT_X || commit->flags != 0
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0 || !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_transfer_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto commit_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto commit_done;
	}
	if (authenticated_node != tag_slot->master_node
		|| authenticated_session != tag_slot->master_session_incarnation
		|| !pcm_x_ticket_ref_equal(&tag_slot->ref, &commit->ref)) {
		result = PCM_X_QUEUE_STALE;
		goto commit_done;
	}
	if (pcm_x_slot_state_read(&member->slot) != PCM_XL_CONTENT_ACTIVE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto commit_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = tag_slot->reliable.last_response_opcode == PGRAC_IC_MSG_PCM_X_COMMIT_X
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_NOT_READY;
		goto commit_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto commit_done;
	}
	if (tag_slot->reliable.pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto commit_done;
	}
	if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_INSTALL_READY
		|| tag_slot->reliable.phase != PGRAC_IC_MSG_PCM_X_INSTALL_READY
		|| !pcm_x_local_response_matches(tag_slot->reliable.pending_opcode,
										 PGRAC_IC_MSG_PCM_X_COMMIT_X)) {
		result = PCM_X_QUEUE_STALE;
		goto commit_done;
	}
	pcm_x_local_reliable_clear(&tag_slot->reliable, PGRAC_IC_MSG_PCM_X_COMMIT_X,
							   authenticated_node);
	result = PCM_X_QUEUE_OK;

commit_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_final_ack_arm_exact(const PcmXLocalHandle *leader,
										uint64 committed_own_generation,
										PcmXFinalAckPayload *final_ack_out,
										PcmXLocalReliableToken *token_out)
{
	return pcm_x_local_transfer_arm_exact(leader, PGRAC_IC_MSG_PCM_X_FINAL_ACK, NULL, NULL,
										  committed_own_generation, NULL, final_ack_out, token_out);
}


PcmXQueueResult
cluster_pcm_x_local_final_commit_ack_exact(const PcmXLocalHandle *leader,
										   const PcmXPhasePayload *final_commit,
										   int32 authenticated_node, uint64 authenticated_session,
										   PcmXPhasePayload *final_confirm_out,
										   PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint64 next_sequence;
	uint32 state;
	uint32 partition;
	bool fail_closed = false;

	if (final_confirm_out != NULL)
		memset(final_confirm_out, 0, sizeof(*final_confirm_out));
	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || final_commit == NULL || final_confirm_out == NULL
		|| token_out == NULL || final_commit->ref.grant_generation == 0 || final_commit->reason != 0
		|| final_commit->phase != PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK || final_commit->flags != 0
		|| authenticated_node < 0 || authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_session == 0 || !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_transfer_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto final_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto final_done;
	}
	if (authenticated_node != tag_slot->master_node
		|| authenticated_session != tag_slot->master_session_incarnation
		|| !pcm_x_ticket_ref_equal(&tag_slot->ref, &final_commit->ref)) {
		result = PCM_X_QUEUE_STALE;
		goto final_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state == PCM_XL_GRANTED) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto final_done;
		}
		result = tag_slot->reliable.pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM
						 && tag_slot->reliable.phase == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM
						 && tag_slot->reliable.last_response_opcode
								== PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_STALE;
		goto final_output;
	}
	if (state != PCM_XL_CONTENT_ACTIVE) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto final_done;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto final_done;
	}
	if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto final_done;
	}
	if (tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_FINAL_ACK
		|| tag_slot->reliable.phase != PGRAC_IC_MSG_PCM_X_FINAL_ACK
		|| !pcm_x_local_response_matches(tag_slot->reliable.pending_opcode,
										 PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK)) {
		result = PCM_X_QUEUE_STALE;
		goto final_done;
	}
	if (!cluster_pcm_x_generation_next(tag_slot->reliable.state_sequence, &next_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto final_done;
	}
	tag_slot->reliable.state_sequence = next_sequence;
	tag_slot->reliable.retry_deadline_ms = 0;
	tag_slot->reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->reliable.retry_count = 0;
	tag_slot->reliable.last_responder_node = (uint32)authenticated_node;
	tag_slot->reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
	tag_slot->reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	tag_slot->reliable.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
	tag_slot->reliable.flags = 0;
	tag_slot->reliable.reserved = 0;
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_GRANTED);
	result = PCM_X_QUEUE_OK;

final_output:
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		final_confirm_out->ref = tag_slot->ref;
		final_confirm_out->phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
		pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);
	}

final_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_begin_revoke_cutoff_exact(const PcmXLocalHandle *leader,
											  PcmXLocalCutoff *cutoff_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef member_ref;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	uint32 partition;

	if (cutoff_out != NULL)
		memset(cutoff_out, 0, sizeof(*cutoff_out));
	if (header == NULL || leader == NULL || cutoff_out == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(&leader->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = member->tag_slot_index;
		tag_ref.slot_generation = member->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &leader->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &leader->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(leader, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto cutoff_done;
	}
	if (pcm_x_slot_state_read(&member->slot) != PCM_XL_NODE_LEADER
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cutoff_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto cutoff_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, tag_slot->master_node,
												tag_slot->master_session_incarnation)) {
		result = PCM_X_QUEUE_STALE;
		goto cutoff_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		goto cutoff_done;
	}
	result = pcm_x_local_terminal_cleanup_fence(tag_slot);
	if (result == PCM_X_QUEUE_OK)
		result = pcm_x_local_freeze_round_locked(tag_slot, tag_ref, cutoff_out);
	if (result == PCM_X_QUEUE_CORRUPT)
		goto cutoff_gate_error;
	if (!pcm_x_local_gate_release(tag_slot))
		result = PCM_X_QUEUE_CORRUPT;
	if (result == PCM_X_QUEUE_OK)
		pcm_x_stats_increment(&header->stats.revoke_count);
	goto cutoff_done;

cutoff_gate_error:
	/* Preserve the gate as evidence once a linked queue invariant is broken. */
cutoff_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


static PcmXQueueResult
pcm_x_local_blocker_lane_retire_state(const PcmXLocalTagSlot *tag_slot)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	bool empty_head;

	if (header == NULL || tag_slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	if (pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		if (tag_slot->blocker_set_generation != 0
			|| !pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable)
			|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
			|| tag_slot->blocker_snapshot_next_chunk != 0)
			return PCM_X_QUEUE_CORRUPT;
		return PCM_X_QUEUE_OK;
	}
	if (!pcm_x_wait_identity_valid(&tag_slot->blocker_snapshot_ref.identity)
		|| !pcm_x_local_admission_ref_valid(&tag_slot->blocker_snapshot_ref,
											&tag_slot->blocker_snapshot_ref.identity)
		|| !BufferTagsEqual(&tag_slot->tag, &tag_slot->blocker_snapshot_ref.identity.tag)
		|| tag_slot->blocker_snapshot_ref.identity.cluster_epoch != tag_slot->cluster_epoch
		|| tag_slot->blocker_snapshot_reliable.response_tombstone_mask != 0)
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->blocker_set_generation == 0)
		return pcm_x_local_blocker_freeze_reservation_exact(tag_slot) ? PCM_X_QUEUE_NOT_READY
																	  : PCM_X_QUEUE_CORRUPT;
	if (tag_slot->blocker_set_generation == UINT64_MAX)
		return PCM_X_QUEUE_CORRUPT;
	if (pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable))
		return pcm_x_local_blocker_ack_tombstone_exact(tag_slot, tag_slot->master_node)
				   ? PCM_X_QUEUE_NOT_READY
				   : PCM_X_QUEUE_CORRUPT;
	empty_head = tag_slot->blocker_snapshot_head_index == PCM_X_INVALID_SLOT_INDEX;
	if (tag_slot->blocker_snapshot_reliable.state_sequence == 0
		|| tag_slot->blocker_snapshot_count > header->layout.max_wait_edges
		|| tag_slot->blocker_snapshot_next_chunk != tag_slot->blocker_snapshot_count
		|| (empty_head != (tag_slot->blocker_snapshot_count == 0))
		|| !pcm_x_transfer_leg_exact(&tag_slot->blocker_snapshot_reliable,
									 PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, tag_slot->master_node,
									 tag_slot->master_session_incarnation))
		return PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_NOT_READY;
}


static bool
pcm_x_local_blocker_lane_is_pristine(const PcmXLocalTagSlot *tag_slot)
{
	return tag_slot != NULL && pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
		   && tag_slot->blocker_set_generation == 0
		   && tag_slot->blocker_snapshot_head_index == PCM_X_INVALID_SLOT_INDEX
		   && tag_slot->blocker_snapshot_count == 0 && tag_slot->blocker_snapshot_crc32c == 0
		   && tag_slot->blocker_snapshot_next_chunk == 0
		   && pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable);
}


/* A local participant has exactly one external terminal authority: holder_ref
 * for the selected image source, or blocker_snapshot_ref for a non-source
 * holder.  Locator mismatch must never mask the structurally impossible
 * dual-lane shape as STALE. */
static bool
pcm_x_local_external_terminal_lanes_collide(const PcmXLocalTagSlot *tag_slot)
{
	return tag_slot != NULL && !pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
		   && !pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref);
}


/* A closed writer round cannot reopen while the holder lane can still mutate
 * or lose its source page.  For the S3 core we use the stronger steady-state
 * boundary: exact holder DRAIN, after immutable image publication and before
 * terminal RETIRE.  A pure writer round has no holder/blocker evidence and is
 * immediately eligible once its own closed cohort is empty. */
static PcmXQueueResult
pcm_x_local_holder_lane_retire_state(const PcmXLocalTagSlot *tag_slot, uint32 flags)
{
	PcmXImageToken zero_image;
	PcmXQueueResult blocker_state;
	bool blocker_terminal;
	uint32 holder_flags;

	if (tag_slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	holder_flags = flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	memset(&zero_image, 0, sizeof(zero_image));
	blocker_terminal = pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
					   && !pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
					   && (tag_slot->blocker_snapshot_ref.grant_generation != 0 || holder_flags != 0
						   || tag_slot->holder_terminal_drain_generation != 0);

	/* A non-source S-holder never owns an image-transfer lane.  Its exact
	 * DRAIN upgrades the immutable type-48 admission locator in place to the
	 * final grant ref and consumes the blocker ACK tombstone.  This is the
	 * external-participant terminal form; it is mutually exclusive with the
	 * source holder_ref form and adds no parallel pin lifecycle. */
	if (blocker_terminal) {
		if ((tag_slot->blocker_snapshot_ref.grant_generation == 0
			 && !pcm_x_local_admission_ref_valid(&tag_slot->blocker_snapshot_ref,
												 &tag_slot->blocker_snapshot_ref.identity))
			|| (tag_slot->blocker_snapshot_ref.grant_generation != 0
				&& !pcm_x_local_terminal_ref_valid(&tag_slot->blocker_snapshot_ref))
			|| tag_slot->blocker_snapshot_ref.grant_generation == UINT64_MAX
			|| !BufferTagsEqual(&tag_slot->tag, &tag_slot->blocker_snapshot_ref.identity.tag)
			|| tag_slot->blocker_snapshot_ref.identity.cluster_epoch != tag_slot->cluster_epoch
			|| tag_slot->blocker_set_generation != 0
			|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
			|| tag_slot->blocker_snapshot_next_chunk != 0
			|| !pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable)
			|| !pcm_x_image_token_equal(&tag_slot->holder_image, &zero_image)
			|| !pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable)
			|| holder_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
			|| tag_slot->holder_terminal_drain_generation == 0
			|| tag_slot->holder_terminal_drain_generation == UINT64_MAX)
			return PCM_X_QUEUE_CORRUPT;
		return PCM_X_QUEUE_OK;
	}

	blocker_state = pcm_x_local_blocker_lane_retire_state(tag_slot);

	if (pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)) {
		if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX)
			return PCM_X_QUEUE_NOT_READY;
		if (holder_flags != 0 || tag_slot->holder_terminal_drain_generation != 0
			|| !pcm_x_image_token_equal(&tag_slot->holder_image, &zero_image)
			|| !pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable))
			return PCM_X_QUEUE_CORRUPT;
		return blocker_state;
	}
	/* REVOKE consumes and clears the blocker snapshot before publishing a
	 * holder ref; both lanes alive at once is not a retryable phase. */
	if (blocker_state != PCM_X_QUEUE_OK)
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_local_terminal_ref_valid(&tag_slot->holder_ref)
		|| tag_slot->holder_ref.grant_generation == 0
		|| !BufferTagsEqual(&tag_slot->tag, &tag_slot->holder_ref.identity.tag)
		|| tag_slot->holder_ref.identity.cluster_epoch != tag_slot->cluster_epoch
		|| !pcm_x_image_token_valid(&tag_slot->holder_image)
		|| !pcm_x_image_id_master_exact(tag_slot->holder_image.image_id, tag_slot->master_node))
		return PCM_X_QUEUE_CORRUPT;
	if (holder_flags == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (holder_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
		|| tag_slot->holder_terminal_drain_generation == 0
		|| tag_slot->holder_terminal_drain_generation == UINT64_MAX
		|| !pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)
		|| tag_slot->holder_reliable.state_sequence == 0
		|| tag_slot->holder_reliable.response_tombstone_mask != 0
		|| tag_slot->holder_reliable.last_responder_node != (uint32)tag_slot->master_node
		|| tag_slot->holder_reliable.last_response_opcode != PGRAC_IC_MSG_PCM_X_DRAIN_POLL)
		return PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_OK;
}


static const PcmXTicketRef *
pcm_x_local_external_terminal_ref(const PcmXLocalTagSlot *tag_slot)
{
	uint32 holder_flags;

	if (tag_slot == NULL)
		return NULL;
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref))
		return pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref) ? &tag_slot->holder_ref
																		 : NULL;
	holder_flags = pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
		&& (tag_slot->blocker_snapshot_ref.grant_generation != 0 || holder_flags != 0
			|| tag_slot->holder_terminal_drain_generation != 0))
		return &tag_slot->blocker_snapshot_ref;
	return NULL;
}


static bool pcm_x_local_terminal_pending_clear(const PcmXLocalTagSlot *tag_slot);


/* A requester may also be an exact external participant: either the source
 * holder or a non-source S-holder recorded by the blocker ACK tombstone.  One
 * DRAIN then closes both terminal legs for the same ticket.  This is the only
 * case in which external evidence may be consumed while the writer-round
 * barrier remains closed. */
static PcmXQueueResult
pcm_x_local_same_ref_dual_retire_state(const PcmXLocalTagSlot *tag_slot, uint32 flags,
									   bool *dual_out, bool *cancel_dual_out)
{
	const PcmXTicketRef *external_ref;
	PcmXImageToken zero_image;
	bool source_holder;
	uint32 holder_flags;
	uint32 writer_flags;

	if (tag_slot == NULL || dual_out == NULL || cancel_dual_out == NULL)
		return PCM_X_QUEUE_CORRUPT;
	*dual_out = false;
	*cancel_dual_out = false;
	writer_flags = flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK;
	holder_flags = flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	if (writer_flags == 0 || holder_flags == 0)
		return PCM_X_QUEUE_OK;
	source_holder = !pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref);
	external_ref = source_holder ? &tag_slot->holder_ref : &tag_slot->blocker_snapshot_ref;
	memset(&zero_image, 0, sizeof(zero_image));
	if (writer_flags != PCM_X_LOCAL_TAG_F_TERMINAL_MASK
		|| holder_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK || tag_slot->membership_count == 0
		|| pcm_x_ticket_ref_is_zero(external_ref)
		|| !pcm_x_ticket_ref_equal(&tag_slot->ref, external_ref)
		|| tag_slot->terminal_drain_generation == 0
		|| tag_slot->terminal_drain_generation == UINT64_MAX
		|| tag_slot->terminal_drain_generation != tag_slot->holder_terminal_drain_generation
		|| !pcm_x_local_terminal_pending_clear(tag_slot)
		|| pcm_x_local_holder_lane_retire_state(tag_slot, flags) != PCM_X_QUEUE_OK)
		return PCM_X_QUEUE_CORRUPT;
	if (external_ref->grant_generation == 0) {
		if (source_holder || tag_slot->ref.grant_generation != 0
			|| (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0
			|| tag_slot->closed_round_member_count == 0
			|| tag_slot->closed_round_member_count > tag_slot->membership_count
			|| !pcm_x_image_token_equal(&tag_slot->image, &zero_image)
			|| !pcm_x_image_token_equal(&tag_slot->holder_image, &zero_image))
			return PCM_X_QUEUE_CORRUPT;
		*dual_out = true;
		*cancel_dual_out = true;
		return PCM_X_QUEUE_OK;
	}
	/* A successful grant keeps the original, stronger transfer-round proof.
	 * Cancellation never enters this arm merely because it shares a locator. */
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0 || tag_slot->closed_round_member_count != 1
		|| !pcm_x_image_token_valid(&tag_slot->image)
		|| (source_holder && !pcm_x_image_token_equal(&tag_slot->image, &tag_slot->holder_image))
		|| (!source_holder
			&& tag_slot->image.source_node == (uint32)tag_slot->ref.identity.node_id))
		return PCM_X_QUEUE_CORRUPT;
	*dual_out = true;
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_local_current_cutoff_snapshot_exact(const BufferTag *tag, uint64 cluster_epoch,
												  int32 master_node,
												  uint64 master_session_incarnation,
												  PcmXLocalCutoff *cutoff_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	uint32 flags;
	uint32 partition;
	bool fail_closed = false;

	if (cutoff_out != NULL)
		memset(cutoff_out, 0, sizeof(*cutoff_out));
	if (header == NULL || tag == NULL || cutoff_out == NULL || master_node < 0
		|| master_node >= PCM_X_PROTOCOL_NODE_LIMIT || master_session_incarnation == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_NOT_FOUND;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_SHARED);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || tag_slot->cluster_epoch != cluster_epoch
		|| tag_slot->master_node != master_node
		|| tag_slot->master_session_incarnation != master_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto snapshot_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, master_node,
												master_session_incarnation)) {
		result = PCM_X_QUEUE_STALE;
		goto snapshot_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto snapshot_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0) {
		result = PCM_X_QUEUE_BUSY;
		goto snapshot_done;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
		result = PCM_X_QUEUE_NOT_READY;
		goto snapshot_done;
	}
	if (tag_slot->local_round == 0 || tag_slot->local_round == UINT32_MAX
		|| tag_slot->cutoff_sequence >= tag_slot->next_sequence
		|| (tag_slot->cutoff_sequence == 0 && tag_slot->closed_round_member_count != 0)
		|| tag_slot->closed_round_member_count > tag_slot->membership_count
		|| ((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->leader_slot_generation == 0))
		|| ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->active_writer_slot_generation == 0))) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto snapshot_done;
	}
	pcm_x_local_cutoff_fill(tag_slot, tag_ref, cutoff_out);
	result = PCM_X_QUEUE_OK;

snapshot_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK)
		memset(cutoff_out, 0, sizeof(*cutoff_out));
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_retire_round_exact(const BufferTag *tag, uint64 cluster_epoch,
									   const PcmXLocalCutoff *cutoff,
									   PcmXLocalHandle *next_leader_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *gated_tag = NULL;
	PcmXLocalMembershipSlot *candidate = NULL;
	PcmXSlotRef tag_ref;
	PcmXSlotRef found;
	PcmXSlotRef candidate_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	PcmXQueueResult holder_state;
	PcmXAllocatorView member_view;
	uint32 candidate_state;
	uint32 flags;
	uint32 partition;
	bool candidate_promote = false;
	bool detach_tag = false;
	bool fail_closed = false;
	bool gate_claimed = false;

	pcm_x_local_handle_clear(next_leader_out);
	if (header == NULL || tag == NULL || cutoff == NULL || next_leader_out == NULL
		|| cutoff->closed_round == 0 || cutoff->closed_round == UINT32_MAX
		|| cutoff->next_round != cutoff->closed_round + 1 || cutoff->master_node < 0
		|| cutoff->master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| cutoff->master_session_incarnation == 0 || cutoff->reserved != 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result = pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &tag_ref);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		result = PCM_X_QUEUE_NOT_FOUND;
	else if (directory_result != PCM_X_DIRECTORY_OK)
		result = pcm_x_queue_result_from_directory(directory_result);
	else if (tag_ref.slot_index != cutoff->tag_slot.slot_index
			 || tag_ref.slot_generation != cutoff->tag_slot.slot_generation)
		result = PCM_X_QUEUE_STALE;
	else {
		tag_slot
			= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
		if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE)
			result = PCM_X_QUEUE_STALE;
		else if (!BufferTagsEqual(&tag_slot->tag, tag)
				 || tag_slot->cluster_epoch != cluster_epoch) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result
				= pcm_x_local_handoff_gate_claim_locked(header, tag_slot, false, PCM_X_QUEUE_BUSY);
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			fail_closed = true;
		if (fail_closed)
			pcm_x_runtime_fail_closed();
		return result;
	}
	gate_claimed = true;
	gated_tag = tag_slot;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, gated_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL || tag_slot->cluster_epoch != cluster_epoch
		|| tag_slot->cutoff_sequence != cutoff->cutoff_sequence
		|| tag_slot->master_node != cutoff->master_node
		|| tag_slot->master_session_incarnation != cutoff->master_session_incarnation) {
		result = PCM_X_QUEUE_STALE;
		goto retire_round_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, cutoff->master_node,
												cutoff->master_session_incarnation)) {
		result = PCM_X_QUEUE_STALE;
		goto retire_round_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto retire_round_done;
	}
	if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto retire_round_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if (((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
		 != (tag_slot->leader_slot_generation == 0))
		|| ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->active_writer_slot_generation == 0))
		|| ((tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX))
		|| (tag_slot->membership_count == 0 && tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX)
		|| ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0
			&& tag_slot->cutoff_sequence >= tag_slot->next_sequence)
		|| tag_slot->closed_round_member_count > tag_slot->membership_count) {
		result = PCM_X_QUEUE_CORRUPT;
		goto retire_round_done;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
		if (tag_slot->local_round != cutoff->next_round) {
			result = PCM_X_QUEUE_STALE;
			goto retire_round_gate_release;
		}
		if (tag_slot->closed_round_member_count != 0
			|| ((tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
				!= (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX))
			|| (tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
				&& tag_slot->leader_index != tag_slot->head_index)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto retire_round_done;
		}
		if (tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX) {
			if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view)) {
				result = PCM_X_QUEUE_CORRUPT;
				goto retire_round_done;
			}
			candidate = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view,
																		tag_slot->leader_index);
			if (candidate == NULL) {
				result = PCM_X_QUEUE_CORRUPT;
				goto retire_round_done;
			}
			candidate_ref.slot_index = tag_slot->leader_index;
			candidate_ref.slot_generation = tag_slot->leader_slot_generation;
			candidate = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
				PCM_X_ALLOC_LOCAL_WAIT, candidate_ref, tag,
				PCM_X_STATE_BIT(PCM_XL_NODE_LEADER) | PCM_X_STATE_BIT(PCM_XL_REMOTE_WAIT)
					| PCM_X_STATE_BIT(PCM_XL_CONTENT_ACTIVE) | PCM_X_STATE_BIT(PCM_XL_GRANTED));
			candidate_state
				= candidate == NULL ? PCM_X_SLOT_FREE : pcm_x_slot_state_read(&candidate->slot);
			if (candidate == NULL || candidate->tag_slot_index != tag_ref.slot_index
				|| candidate->tag_slot_generation != tag_ref.slot_generation
				|| candidate->admitted_round != cutoff->next_round
				|| candidate->local_sequence <= cutoff->cutoff_sequence
				|| candidate->identity.cluster_epoch != cluster_epoch
				|| candidate->role != PCM_X_LOCAL_ROLE_NODE_LEADER
				|| candidate->graph_generation != 0) {
				result = PCM_X_QUEUE_CORRUPT;
				goto retire_round_done;
			}
			/* A replay can race the successor's normal ENQUEUE/GRANT progress.
			 * Its exact resident identity remains valid, but only a still-idle
			 * NODE_LEADER needs another wake token. */
			if (candidate_state == PCM_XL_NODE_LEADER
				&& (pcm_x_slot_flags_read(&candidate->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE)
					   == 0)
				pcm_x_local_handle_from_member(next_leader_out, candidate_ref, candidate,
											   PCM_X_LOCAL_ROLE_NODE_LEADER);
		}
		result = PCM_X_QUEUE_DUPLICATE;
		goto retire_round_gate_release;
	}
	if (tag_slot->local_round != cutoff->closed_round
		|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| tag_slot->closed_round_member_count != 0) {
		result = PCM_X_QUEUE_NOT_READY;
		goto retire_round_gate_release;
	}
	holder_state = pcm_x_local_holder_lane_retire_state(tag_slot, flags);
	if (holder_state != PCM_X_QUEUE_OK) {
		result = holder_state;
		if (result == PCM_X_QUEUE_CORRUPT)
			goto retire_round_done;
		goto retire_round_gate_release;
	}
	if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX) {
		result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, tag_slot->head_index,
												  PCM_X_INVALID_SLOT_INDEX, 0, &candidate,
												  &candidate_ref, &candidate_promote);
		if (result == PCM_X_QUEUE_BUSY) {
			result = PCM_X_QUEUE_NOT_READY;
			goto retire_round_gate_release;
		}
		if (result != PCM_X_QUEUE_OK || candidate_promote) {
			result = PCM_X_QUEUE_CORRUPT;
			goto retire_round_done;
		}
	}
	tag_slot->local_round = cutoff->next_round;
	(void)pcm_x_slot_flags_fetch_and(&tag_slot->slot, ~PCM_X_LOCAL_TAG_F_REVOKE_BARRIER);
	if (candidate != NULL) {
		tag_slot->leader_index = candidate_ref.slot_index;
		tag_slot->leader_slot_generation = candidate_ref.slot_generation;
		candidate->role = PCM_X_LOCAL_ROLE_NODE_LEADER;
		pg_write_barrier();
		pcm_x_slot_state_write(&candidate->slot, PCM_XL_NODE_LEADER);
		pcm_x_local_handle_from_member(next_leader_out, candidate_ref, candidate,
									   PCM_X_LOCAL_ROLE_NODE_LEADER);
	}
	result = PCM_X_QUEUE_OK;
	/* An empty pure-writer tag has no later backend that could reclaim it.
	 * Keep the admission gate across the local-to-allocator lock handoff and
	 * remove it here.  Holder/terminal evidence keeps the tag live for its own
	 * exact RETIRE path. */
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if (candidate == NULL && tag_slot->membership_count == 0
		&& tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX
		&& tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX
		&& tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
		&& tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX
		&& tag_slot->active_holder_head_index == PCM_X_INVALID_SLOT_INDEX
		&& pcm_x_ticket_ref_is_zero(&tag_slot->ref)
		&& pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
		&& pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
		&& pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		&& pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable)
		&& pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable)
		&& (flags & (PCM_X_LOCAL_TAG_F_TERMINAL_MASK | PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK))
			   == 0) {
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
		detach_tag = true;
		goto retire_round_done;
	}

retire_round_gate_release:
	if (!pcm_x_local_gate_release(tag_slot))
		result = PCM_X_QUEUE_CORRUPT;
	else
		gate_claimed = false;
	goto retire_round_done;

retire_round_done:
	/* Any post-acquire direct jump here is CORRUPT and retains the gate. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_CORRUPT && gate_claimed) {
		if (!pcm_x_local_gate_release(gated_tag))
			result = PCM_X_QUEUE_CORRUPT;
		else
			gate_claimed = false;
	}
	if (result == PCM_X_QUEUE_CORRUPT) {
		pcm_x_runtime_fail_closed();
		return result;
	}
	if (result != PCM_X_QUEUE_OK || !detach_tag)
		return result;

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, tag_slot);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_DETACHING
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0
		|| pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, tag, &found) != PCM_X_DIRECTORY_OK
		|| found.slot_index != tag_ref.slot_index
		|| found.slot_generation != tag_ref.slot_generation
		|| pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, tag, tag_ref)
			   != PCM_X_DIRECTORY_OK
		|| pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref, PCM_X_TAG_DETACHING)
			   != PCM_X_ALLOC_OK) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	} else
		result = PCM_X_QUEUE_OK;
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


static bool
pcm_x_local_unlink_member_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
								 PcmXLocalMembershipSlot *member, PcmXSlotRef member_ref)
{
	PcmXAllocatorView member_view;
	PcmXLocalMembershipSlot *next = NULL;
	PcmXLocalMembershipSlot *previous = NULL;

	if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view))
		return false;
	if (member->prev_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->head_index != member_ref.slot_index)
			return false;
	} else {
		if (member->prev_index == member_ref.slot_index
			|| tag_slot->head_index == member_ref.slot_index)
			return false;
		previous
			= (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, member->prev_index);
		if (previous == NULL || previous->next_index != member_ref.slot_index
			|| previous->tag_slot_index != tag_ref.slot_index
			|| previous->tag_slot_generation != tag_ref.slot_generation)
			return false;
	}
	if (member->next_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->tail_index != member_ref.slot_index)
			return false;
	} else {
		if (member->next_index == member_ref.slot_index || member->next_index == member->prev_index
			|| tag_slot->tail_index == member_ref.slot_index)
			return false;
		next = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, member->next_index);
		if (next == NULL || next->prev_index != member_ref.slot_index
			|| next->tag_slot_index != tag_ref.slot_index
			|| next->tag_slot_generation != tag_ref.slot_generation)
			return false;
	}

	/* Validate both directions before changing either one. */
	if (previous == NULL)
		tag_slot->head_index = member->next_index;
	else
		previous->next_index = member->next_index;
	if (next == NULL)
		tag_slot->tail_index = member->prev_index;
	else
		next->prev_index = member->prev_index;
	member->next_index = PCM_X_INVALID_SLOT_INDEX;
	member->prev_index = PCM_X_INVALID_SLOT_INDEX;
	return true;
}


/* Arm either cancel wire leg without dropping the currently authoritative
 * local leader.  PREHANDLE_CANCEL replaces the older ENQUEUE resend authority;
 * full CANCEL starts only after ADMIT_CONFIRM_ACK made the leader REMOTE_WAIT. */
static PcmXQueueResult
pcm_x_local_cancel_arm_common(const PcmXLocalHandle *leader, bool prehandle_cancel,
							  PcmXPrehandleCancelPayload *prehandle_out,
							  PcmXPhasePayload *phase_out, PcmXLocalReliableToken *token_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint64 next_state_sequence;
	uint16 desired_opcode;
	uint16 desired_phase;
	uint32 member_state;
	uint32 partition;
	bool fail_closed = false;

	if (prehandle_out != NULL)
		memset(prehandle_out, 0, sizeof(*prehandle_out));
	if (phase_out != NULL)
		memset(phase_out, 0, sizeof(*phase_out));
	pcm_x_local_reliable_token_clear(token_out);
	if (header == NULL || leader == NULL || token_out == NULL
		|| (prehandle_cancel && (prehandle_out == NULL || phase_out != NULL))
		|| (!prehandle_cancel && (phase_out == NULL || prehandle_out != NULL))
		|| !pcm_x_wait_identity_valid(&leader->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	result = pcm_x_local_leader_slots_exact(leader, tag_ref, member_ref, &tag_slot, &member);
	if (result != PCM_X_QUEUE_OK)
		goto cancel_arm_done;
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto cancel_arm_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto cancel_arm_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	desired_opcode
		= prehandle_cancel ? PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL : PGRAC_IC_MSG_PCM_X_CANCEL;
	desired_phase = prehandle_cancel ? PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL
									 : PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	if ((prehandle_cancel && member_state != PCM_XL_NODE_LEADER)
		|| (!prehandle_cancel && member_state != PCM_XL_REMOTE_WAIT)) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_arm_done;
	}
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto cancel_arm_done;
		}
		if (tag_slot->reliable.pending_opcode == desired_opcode
			&& tag_slot->reliable.phase == desired_phase) {
			result = PCM_X_QUEUE_DUPLICATE;
			goto cancel_arm_output;
		}
		if (!prehandle_cancel || tag_slot->reliable.pending_opcode != PGRAC_IC_MSG_PCM_X_ENQUEUE
			|| tag_slot->reliable.phase != PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE) {
			result = PCM_X_QUEUE_BUSY;
			goto cancel_arm_done;
		}
	} else if (prehandle_cancel) {
		/* A prehandle exists only while ENQUEUE itself remains application-pending. */
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_arm_done;
	}
	if ((prehandle_cancel && !pcm_x_ticket_ref_is_zero(&tag_slot->ref))
		|| (!prehandle_cancel
			&& !pcm_x_local_admission_ref_valid(&tag_slot->ref, &member->identity))) {
		result = PCM_X_QUEUE_BAD_STATE;
		goto cancel_arm_done;
	}
	if (!cluster_pcm_x_generation_next(tag_slot->reliable.state_sequence, &next_state_sequence)) {
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		fail_closed = true;
		goto cancel_arm_done;
	}
	tag_slot->reliable.state_sequence = next_state_sequence;
	tag_slot->reliable.retry_deadline_ms = 0;
	tag_slot->reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->reliable.retry_count = 0;
	tag_slot->reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->reliable.pending_opcode = desired_opcode;
	tag_slot->reliable.phase = desired_phase;
	tag_slot->reliable.flags = 0;
	tag_slot->reliable.reserved = 0;
	pg_write_barrier();
	result = PCM_X_QUEUE_OK;

cancel_arm_output:
	if (prehandle_cancel) {
		prehandle_out->identity = member->identity;
		prehandle_out->prehandle = tag_slot->prehandle;
	} else {
		phase_out->ref = tag_slot->ref;
		phase_out->phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	}
	pcm_x_local_reliable_token_from_slot(token_out, tag_slot, member);

cancel_arm_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_prehandle_cancel_arm_exact(const PcmXLocalHandle *leader,
											   PcmXPrehandleCancelPayload *payload_out,
											   PcmXLocalReliableToken *token_out)
{
	return pcm_x_local_cancel_arm_common(leader, true, payload_out, NULL, token_out);
}


PcmXQueueResult
cluster_pcm_x_local_cancel_arm_exact(const PcmXLocalHandle *leader, PcmXPhasePayload *payload_out,
									 PcmXLocalReliableToken *token_out)
{
	return pcm_x_local_cancel_arm_common(leader, false, NULL, payload_out, token_out);
}


static PcmXQueueResult pcm_x_local_cancel_successor_state(const PcmXLocalTagSlot *tag_slot,
														  const PcmXLocalMembershipSlot *candidate,
														  bool *promote_out);


static PcmXQueueResult
pcm_x_local_cancel_current_leader_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
										 PcmXLocalHandle *leader_out)
{
	PcmXAllocatorView member_view;
	PcmXLocalMembershipSlot *candidate;
	PcmXSlotRef candidate_ref;
	PcmXQueueResult result;
	bool promote;

	pcm_x_local_handle_clear(leader_out);
	if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view))
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX) {
		if (tag_slot->leader_slot_generation != 0)
			return PCM_X_QUEUE_CORRUPT;
		if (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
			return tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX ? PCM_X_QUEUE_OK
																	: PCM_X_QUEUE_CORRUPT;
		candidate_ref.slot_index = tag_slot->head_index;
		candidate = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view,
																	candidate_ref.slot_index);
		if (candidate == NULL || candidate->tag_slot_index != tag_ref.slot_index
			|| candidate->tag_slot_generation != tag_ref.slot_generation
			|| candidate->prev_index != PCM_X_INVALID_SLOT_INDEX
			|| candidate->role != PCM_X_LOCAL_ROLE_FOLLOWER
			|| !pcm_x_slot_generation_read(&candidate->slot, &candidate_ref.slot_generation))
			return PCM_X_QUEUE_CORRUPT;
		candidate = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
			PCM_X_ALLOC_LOCAL_WAIT, candidate_ref, &tag_slot->tag,
			PCM_X_STATE_BIT(PCM_XL_JOINED_NONWAITABLE) | PCM_X_STATE_BIT(PCM_XL_WAITABLE_FOLLOWER));
		if (candidate == NULL)
			return PCM_X_QUEUE_CORRUPT;
		result = pcm_x_local_cancel_successor_state(tag_slot, candidate, &promote);
		if (result != PCM_X_QUEUE_OK)
			return result;
		return promote ? PCM_X_QUEUE_CORRUPT : PCM_X_QUEUE_OK;
	}
	candidate
		= (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, tag_slot->leader_index);
	if (candidate == NULL || candidate->tag_slot_index != tag_ref.slot_index
		|| candidate->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_slot_generation_read(&candidate->slot, &candidate_ref.slot_generation))
		return PCM_X_QUEUE_CORRUPT;
	candidate_ref.slot_index = tag_slot->leader_index;
	candidate = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, candidate_ref, &tag_slot->tag, PCM_X_STATE_BIT(PCM_XL_NODE_LEADER));
	if (candidate == NULL || candidate->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| tag_slot->leader_slot_generation != candidate_ref.slot_generation)
		return PCM_X_QUEUE_CORRUPT;
	if ((pcm_x_slot_flags_read(&candidate->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0)
		return PCM_X_QUEUE_NOT_READY;
	if (leader_out != NULL)
		pcm_x_local_handle_from_member(leader_out, candidate_ref, candidate,
									   PCM_X_LOCAL_ROLE_NODE_LEADER);
	return PCM_X_QUEUE_OK;
}


/* Classify the linked successor before an interrupt path unlinks a leader.
 * An externally published follower edge must be removed exactly before role
 * rotation.  A post-cutoff follower remains in the next round and is never
 * promoted while the revoke barrier is closed. */
static PcmXQueueResult
pcm_x_local_cancel_successor_state(const PcmXLocalTagSlot *tag_slot,
								   const PcmXLocalMembershipSlot *candidate, bool *promote_out)
{
	uint32 candidate_state;
	uint32 flags;

	if (tag_slot == NULL || candidate == NULL || promote_out == NULL)
		return PCM_X_QUEUE_CORRUPT;
	*promote_out = false;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if (candidate->local_sequence == 0 || candidate->local_sequence >= tag_slot->next_sequence
		|| candidate->identity.cluster_epoch != tag_slot->cluster_epoch)
		return PCM_X_QUEUE_CORRUPT;
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0) {
		if (tag_slot->local_round == UINT32_MAX
			|| tag_slot->cutoff_sequence >= tag_slot->next_sequence
			|| tag_slot->closed_round_member_count > tag_slot->membership_count)
			return PCM_X_QUEUE_CORRUPT;
		if (candidate->local_sequence <= tag_slot->cutoff_sequence) {
			if (candidate->admitted_round != tag_slot->local_round)
				return PCM_X_QUEUE_CORRUPT;
			*promote_out = true;
		} else if (candidate->admitted_round != tag_slot->local_round + 1)
			return PCM_X_QUEUE_CORRUPT;
	} else {
		if (candidate->admitted_round != tag_slot->local_round)
			return PCM_X_QUEUE_CORRUPT;
		*promote_out = true;
	}
	if ((pcm_x_slot_flags_read(&candidate->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0)
		return *promote_out ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT;
	candidate_state = pcm_x_slot_state_read(&candidate->slot);
	if (candidate_state == PCM_XL_WAITABLE_FOLLOWER)
		return candidate->graph_generation != 0 ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT;
	if (candidate_state != PCM_XL_JOINED_NONWAITABLE || candidate->graph_generation != 0)
		return PCM_X_QUEUE_CORRUPT;
	return PCM_X_QUEUE_OK;
}


/* Prove that every linked follower has retired its external WFG edge before
 * a leader identity can disappear or change roles.  Checking only the head
 * would leave later followers pointing at a terminal identity. */
static PcmXQueueResult
pcm_x_local_follower_chain_ready(const PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
								 Size first_index, Size expected_prev_index,
								 uint64 expected_prev_sequence, PcmXLocalMembershipSlot **first_out,
								 PcmXSlotRef *first_ref_out, bool *first_promote_out)
{
	PcmXAllocatorView member_view;
	PcmXLocalMembershipSlot *candidate;
	PcmXSlotRef candidate_ref;
	PcmXQueueResult result;
	Size current = first_index;
	Size previous = expected_prev_index;
	uint64 previous_sequence = expected_prev_sequence;
	Size visited = 0;
	bool promote;

	if (tag_slot == NULL || first_out == NULL || first_ref_out == NULL || first_promote_out == NULL
		|| !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &member_view))
		return PCM_X_QUEUE_CORRUPT;
	*first_out = NULL;
	first_ref_out->slot_index = PCM_X_INVALID_SLOT_INDEX;
	first_ref_out->slot_generation = 0;
	*first_promote_out = false;
	while (current != PCM_X_INVALID_SLOT_INDEX) {
		candidate = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&member_view, current);
		if (candidate == NULL || visited >= tag_slot->membership_count
			|| visited >= member_view.capacity || candidate->tag_slot_index != tag_ref.slot_index
			|| candidate->tag_slot_generation != tag_ref.slot_generation
			|| candidate->prev_index != previous || candidate->local_sequence <= previous_sequence
			|| candidate->role != PCM_X_LOCAL_ROLE_FOLLOWER
			|| !pcm_x_slot_generation_read(&candidate->slot, &candidate_ref.slot_generation))
			return PCM_X_QUEUE_CORRUPT;
		candidate_ref.slot_index = current;
		candidate = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
			PCM_X_ALLOC_LOCAL_WAIT, candidate_ref, &tag_slot->tag,
			PCM_X_STATE_BIT(PCM_XL_JOINED_NONWAITABLE) | PCM_X_STATE_BIT(PCM_XL_WAITABLE_FOLLOWER));
		if (candidate == NULL)
			return PCM_X_QUEUE_CORRUPT;
		result = pcm_x_local_cancel_successor_state(tag_slot, candidate, &promote);
		if (result != PCM_X_QUEUE_OK)
			return result;
		if (visited == 0) {
			*first_out = candidate;
			*first_ref_out = candidate_ref;
			*first_promote_out = promote;
		}
		previous = current;
		previous_sequence = candidate->local_sequence;
		current = candidate->next_index;
		visited++;
	}
	return tag_slot->tail_index == previous ? PCM_X_QUEUE_OK : PCM_X_QUEUE_CORRUPT;
}


/* Apply a cancel ACK and rotate the local leader under one local-domain lock.
 * The reliable leg remains intact until all queue/candidate invariants have
 * been validated, so transport delivery can never masquerade as application
 * completion. */
static PcmXQueueResult
pcm_x_local_cancel_ack_common(const PcmXLocalHandle *leader, const PcmXTicketRef *actual_ref,
							  const PcmXPrehandleKey *actual_prehandle, uint16 response_opcode,
							  int32 authenticated_node, uint64 authenticated_session,
							  PcmXLocalHandle *new_leader_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXOutboundTargetFrontier *outbound;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *candidate = NULL;
	PcmXImageToken zero_image = { 0 };
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef candidate_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXQueueResult result;
	uint16 pending_opcode;
	uint16 pending_phase;
	uint32 expected_state;
	uint32 state;
	uint32 flags;
	uint32 partition;
	bool fail_closed = false;
	bool gate_claimed = false;
	bool promote_candidate = false;

	pcm_x_local_handle_clear(new_leader_out);
	if (header == NULL || leader == NULL || actual_ref == NULL
		|| !pcm_x_wait_identity_valid(&leader->identity) || authenticated_node < 0
		|| authenticated_node >= PCM_X_PROTOCOL_NODE_LIMIT || authenticated_session == 0
		|| !pcm_x_local_admission_ref_valid(actual_ref, &leader->identity))
		return PCM_X_QUEUE_INVALID;
	if (response_opcode == PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK) {
		if (actual_prehandle == NULL || actual_prehandle->sender_session_incarnation == 0
			|| actual_prehandle->prehandle_sequence == 0)
			return PCM_X_QUEUE_INVALID;
		pending_opcode = PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL;
		pending_phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
		expected_state = PCM_XL_NODE_LEADER;
	} else if (response_opcode == PGRAC_IC_MSG_PCM_X_CANCEL_ACK) {
		if (actual_prehandle != NULL)
			return PCM_X_QUEUE_INVALID;
		pending_opcode = PGRAC_IC_MSG_PCM_X_CANCEL;
		pending_phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
		expected_state = PCM_XL_REMOTE_WAIT;
	} else
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	result = pcm_x_local_refs_lookup(leader, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&leader->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &leader->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &leader->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(leader, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto cancel_ack_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto cancel_ack_done;
	}
	result = pcm_x_local_outbound_authority_exact(tag_slot, &outbound);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_STALE;
		goto cancel_ack_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state == PCM_XL_CANCELLED) {
		if (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER || tag_slot->reliable.pending_opcode != 0
			|| tag_slot->reliable.last_response_opcode != response_opcode
			|| authenticated_node != tag_slot->master_node
			|| authenticated_session != tag_slot->master_session_incarnation
			|| !pcm_x_ticket_handle_equal(&member->handle, &actual_ref->handle)
			|| !pcm_x_ticket_ref_equal(&tag_slot->ref, actual_ref)
			|| (actual_prehandle != NULL
				&& !pcm_x_prehandle_equal(&tag_slot->prehandle, actual_prehandle))) {
			result = PCM_X_QUEUE_STALE;
			goto cancel_ack_done;
		}
		result = pcm_x_local_cancel_current_leader_locked(tag_slot, tag_ref, new_leader_out);
		if (result == PCM_X_QUEUE_OK)
			result = PCM_X_QUEUE_DUPLICATE;
		else
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto cancel_ack_done;
	}
	if (state != expected_state || member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| tag_slot->leader_index != member_ref.slot_index
		|| tag_slot->leader_slot_generation != member_ref.slot_generation
		|| tag_slot->head_index != member_ref.slot_index) {
		result = state == expected_state ? PCM_X_QUEUE_CORRUPT : PCM_X_QUEUE_BAD_STATE;
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto cancel_ack_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto cancel_ack_done;
	}
	gate_claimed = true;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->active_writer_slot_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	if (tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_BUSY;
		goto cancel_ack_release_gate;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0) {
		result = pcm_x_local_execution_round_locked(tag_slot, member);
		if (result != PCM_X_QUEUE_OK || tag_slot->closed_round_member_count == 0
			|| tag_slot->closed_round_member_count > tag_slot->membership_count
			|| tag_slot->cutoff_sequence >= tag_slot->next_sequence) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto cancel_ack_release_gate;
		}
	} else if (tag_slot->closed_round_member_count != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	if (!pcm_x_image_token_equal(&tag_slot->image, &zero_image)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	if (member->graph_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	if (pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| !pcm_x_local_reliable_leg_valid(tag_slot, member)) {
		result = pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable) ? PCM_X_QUEUE_NOT_READY
																		: PCM_X_QUEUE_CORRUPT;
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto cancel_ack_release_gate;
	}
	if (tag_slot->reliable.pending_opcode != pending_opcode
		|| tag_slot->reliable.phase != pending_phase
		|| !pcm_x_local_response_matches(pending_opcode, response_opcode)
		|| authenticated_node != tag_slot->reliable.expected_responder_node
		|| authenticated_session != tag_slot->reliable.expected_responder_session
		|| (actual_prehandle != NULL
			&& !pcm_x_prehandle_equal(actual_prehandle, &tag_slot->prehandle))
		|| (pending_opcode == PGRAC_IC_MSG_PCM_X_CANCEL
			&& !pcm_x_ticket_ref_equal(actual_ref, &tag_slot->ref))) {
		result = PCM_X_QUEUE_STALE;
		goto cancel_ack_release_gate;
	}
	result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, member->next_index,
											  member_ref.slot_index, member->local_sequence,
											  &candidate, &candidate_ref, &promote_candidate);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto cancel_ack_release_gate;
	}
	if (!pcm_x_local_unlink_member_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto cancel_ack_release_gate;
	}
	tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->leader_slot_generation = 0;
	member->handle = actual_ref->handle;
	member->graph_generation = 0;
	if (pending_opcode == PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL)
		tag_slot->ref = *actual_ref;
	pcm_x_local_reliable_clear(&tag_slot->reliable, response_opcode, authenticated_node);
	/* Keep the exact old prehandle/ref as a bounded duplicate-ACK tombstone.
	 * The successor clears it under this same local-domain lock before minting
	 * its own ENQUEUE authority. */
	if (candidate != NULL && promote_candidate) {
		tag_slot->leader_index = candidate_ref.slot_index;
		tag_slot->leader_slot_generation = candidate_ref.slot_generation;
		candidate->role = PCM_X_LOCAL_ROLE_NODE_LEADER;
		pg_write_barrier();
		pcm_x_slot_state_write(&candidate->slot, PCM_XL_NODE_LEADER);
		if (new_leader_out != NULL)
			pcm_x_local_handle_from_member(new_leader_out, candidate_ref, candidate,
										   PCM_X_LOCAL_ROLE_NODE_LEADER);
	}
	tag_slot->terminal_drain_generation = 0;
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_TERMINAL_READY);
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_CANCELLED);
	result = PCM_X_QUEUE_OK;

cancel_ack_release_gate:
	/* Retain a claimed gate as recovery evidence after an invariant failure;
	 * retryable/application mismatches release it for the durable resend. */
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}

cancel_ack_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_prehandle_cancel_ack_exact(const PcmXLocalHandle *leader,
											   const PcmXAdmitAckPayload *ack,
											   int32 authenticated_node,
											   uint64 authenticated_session,
											   PcmXLocalHandle *new_leader_out)
{
	if (ack == NULL || ack->result != PCM_X_QUEUE_OK
		|| ack->phase != PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL || ack->flags != 0)
		return PCM_X_QUEUE_INVALID;
	return pcm_x_local_cancel_ack_common(leader, &ack->ref, &ack->prehandle,
										 PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK,
										 authenticated_node, authenticated_session, new_leader_out);
}


PcmXQueueResult
cluster_pcm_x_local_cancel_ack_exact(const PcmXLocalHandle *leader, const PcmXPhasePayload *ack,
									 int32 authenticated_node, uint64 authenticated_session,
									 PcmXLocalHandle *new_leader_out)
{
	if (ack == NULL || ack->reason != 0 || ack->phase != PCM_X_LOCAL_RELIABLE_PHASE_CANCEL
		|| ack->flags != 0)
		return PCM_X_QUEUE_INVALID;
	return pcm_x_local_cancel_ack_common(leader, &ack->ref, NULL, PGRAC_IC_MSG_PCM_X_CANCEL_ACK,
										 authenticated_node, authenticated_session, new_leader_out);
}


static PcmXQueueResult
pcm_x_local_terminal_refs_lookup(const PcmXTicketRef *ref, PcmXSlotRef *tag_ref_out,
								 PcmXSlotRef *member_ref_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(&ref->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref_out->slot_index = member->tag_slot_index;
		tag_ref_out->slot_generation = member->tag_slot_generation;
		*member_ref_out = member_ref;
	}
	LWLockRelease(&header->allocator_lock.lock);
	return result == PCM_X_QUEUE_NOT_FOUND ? PCM_X_QUEUE_STALE : result;
}


static bool
pcm_x_local_terminal_pending_clear(const PcmXLocalTagSlot *tag_slot)
{
	return tag_slot != NULL && pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		   && tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX;
}


PcmXQueueResult
cluster_pcm_x_local_terminal_publish_exact(const PcmXTicketRef *ref,
										   int32 authenticated_master_node,
										   uint64 authenticated_master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	uint32 flags;
	uint32 member_state;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;

	if (header == NULL || !pcm_x_local_terminal_ref_valid(ref) || authenticated_master_node < 0
		|| authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	frontier = &header->peer_frontiers[authenticated_master_node];
	if (frontier->cluster_epoch != ref->identity.cluster_epoch
		|| frontier->sender_session_incarnation != authenticated_master_session)
		return PCM_X_QUEUE_STALE;
	result = pcm_x_local_terminal_refs_lookup(ref, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &ref->identity.tag,
		PCM_X_STATE_BIT(PCM_XL_CANCELLED) | PCM_X_STATE_BIT(PCM_XL_GRANTED));
	if (tag_slot == NULL || member == NULL || member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_wait_identity_equal(&member->identity, &ref->identity)) {
		result = PCM_X_QUEUE_STALE;
		goto publish_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto publish_done;
	}
	if (tag_slot->master_node != authenticated_master_node
		|| tag_slot->master_session_incarnation != authenticated_master_session
		|| tag_slot->cluster_epoch != ref->identity.cluster_epoch
		|| !pcm_x_ticket_handle_equal(&member->handle, &ref->handle)) {
		result = PCM_X_QUEUE_STALE;
		goto publish_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto publish_done;
	}
	gate_claimed = true;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) != 0
		&& (flags & PCM_X_LOCAL_TAG_F_TERMINAL_READY) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto publish_done;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_READY) != 0) {
		if (((flags & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) != 0)
			!= (tag_slot->terminal_drain_generation != 0)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else
			result = pcm_x_ticket_ref_equal(&tag_slot->ref, ref) ? PCM_X_QUEUE_DUPLICATE
																 : PCM_X_QUEUE_STALE;
		goto publish_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if ((member_state != PCM_XL_CANCELLED && member_state != PCM_XL_GRANTED)
		|| !pcm_x_local_terminal_pending_clear(tag_slot)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto publish_done;
	}
	if (member->next_index != PCM_X_INVALID_SLOT_INDEX
		|| member->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->head_index == member_ref.slot_index
		|| tag_slot->tail_index == member_ref.slot_index
		|| tag_slot->leader_index == member_ref.slot_index) {
		result = PCM_X_QUEUE_NOT_READY;
		goto publish_done;
	}
	if (!pcm_x_local_terminal_unlinked_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto publish_done;
	}
	if (!pcm_x_ticket_locator_equal(&tag_slot->ref, ref)) {
		result = PCM_X_QUEUE_STALE;
		goto publish_done;
	}
	tag_slot->ref = *ref;
	tag_slot->terminal_drain_generation = 0;
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_TERMINAL_READY);
	result = PCM_X_QUEUE_OK;

publish_done:
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


typedef struct PcmXLocalRoundClosePlan {
	PcmXLocalMembershipSlot *candidate;
	PcmXSlotRef candidate_ref;
	bool close_round;
} PcmXLocalRoundClosePlan;

/* A holder-only PROBE freezes an empty current cohort; writers joining later
 * are linked as next-round followers.  Validate that whole shape before the
 * first terminal mutation, then apply an infallible close under the same local
 * lock.  A terminal replay uses the same validator to finish a close stranded
 * after the terminal evidence was published. */
static PcmXQueueResult
pcm_x_local_empty_frozen_round_plan_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
										   PcmXLocalRoundClosePlan *plan)
{
	PcmXQueueResult result;
	uint32 flags;
	bool candidate_promote = false;

	if (tag_slot == NULL || plan == NULL)
		return PCM_X_QUEUE_CORRUPT;
	memset(plan, 0, sizeof(*plan));
	plan->candidate_ref.slot_index = PCM_X_INVALID_SLOT_INDEX;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	/* Every first holder DRAIN closes beneath the revoke fence, including the
	 * case where a writer cohort is still terminal and therefore no round is
	 * advanced here.  Check these common facts before the nonempty fast path;
	 * otherwise a torn barrier/drain can be overwritten as a successful first
	 * arrival merely because closed_round_member_count is nonzero. */
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0
		|| (flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0
		|| tag_slot->holder_terminal_drain_generation != 0 || tag_slot->local_round == 0
		|| tag_slot->local_round == UINT32_MAX || tag_slot->next_sequence == 0
		|| tag_slot->cutoff_sequence >= tag_slot->next_sequence
		|| tag_slot->closed_round_member_count > tag_slot->membership_count
		|| (tag_slot->closed_round_member_count != 0 && tag_slot->cutoff_sequence == 0))
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->closed_round_member_count != 0)
		return PCM_X_QUEUE_OK;
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != 0
		|| ((tag_slot->membership_count == 0) != (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX))
		|| ((tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX))
		|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->leader_slot_generation != 0
		|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->active_writer_slot_generation != 0
		|| tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		|| !pcm_x_ticket_ref_is_zero(&tag_slot->ref))
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX) {
		result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, tag_slot->head_index,
												  PCM_X_INVALID_SLOT_INDEX, 0, &plan->candidate,
												  &plan->candidate_ref, &candidate_promote);
		if (result != PCM_X_QUEUE_OK)
			return result == PCM_X_QUEUE_BUSY ? PCM_X_QUEUE_NOT_READY : result;
		if (plan->candidate == NULL || candidate_promote)
			return PCM_X_QUEUE_CORRUPT;
	}
	plan->close_round = true;
	return PCM_X_QUEUE_OK;
}


static void
pcm_x_local_empty_frozen_round_apply_locked(PcmXLocalTagSlot *tag_slot,
											const PcmXLocalRoundClosePlan *plan)
{
	if (tag_slot == NULL || plan == NULL || !plan->close_round)
		return;
	tag_slot->local_round++;
	pg_write_barrier();
	(void)pcm_x_slot_flags_fetch_and(&tag_slot->slot, ~PCM_X_LOCAL_TAG_F_REVOKE_BARRIER);
	if (plan->candidate != NULL) {
		tag_slot->leader_index = plan->candidate_ref.slot_index;
		tag_slot->leader_slot_generation = plan->candidate_ref.slot_generation;
		plan->candidate->role = PCM_X_LOCAL_ROLE_NODE_LEADER;
		pg_write_barrier();
		pcm_x_slot_state_write(&plan->candidate->slot, PCM_XL_NODE_LEADER);
	}
}


/* Project the state immediately after RETIRE removes the final member of a
 * closed writer cohort.  The terminal member is already unlinked by DRAIN, so
 * the remaining FIFO, if any, must consist entirely of next-round followers.
 * This is a read-only plan: every retryable/structural result precedes the
 * terminal/ref/count mutation performed by detach_terminal_common. */
static PcmXQueueResult
pcm_x_local_final_member_round_plan_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
										   PcmXLocalRoundClosePlan *plan)
{
	PcmXQueueResult holder_state;
	PcmXQueueResult result;
	uint32 flags;
	bool candidate_promote = false;

	if (tag_slot == NULL || plan == NULL)
		return PCM_X_QUEUE_CORRUPT;
	memset(plan, 0, sizeof(*plan));
	plan->candidate_ref.slot_index = PCM_X_INVALID_SLOT_INDEX;
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0 || tag_slot->closed_round_member_count != 1
		|| tag_slot->membership_count == 0 || tag_slot->local_round == 0
		|| tag_slot->local_round == UINT32_MAX || tag_slot->cutoff_sequence == 0
		|| tag_slot->next_sequence == 0 || tag_slot->cutoff_sequence >= tag_slot->next_sequence
		|| ((tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
			!= (tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX))
		|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->leader_slot_generation != 0
		|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->active_writer_slot_generation != 0
		|| tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX
		|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable))
		return PCM_X_QUEUE_CORRUPT;
	holder_state = pcm_x_local_holder_lane_retire_state(tag_slot, flags);
	if (holder_state != PCM_X_QUEUE_OK)
		return holder_state;
	if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX) {
		result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, tag_slot->head_index,
												  PCM_X_INVALID_SLOT_INDEX, 0, &plan->candidate,
												  &plan->candidate_ref, &candidate_promote);
		if (result != PCM_X_QUEUE_OK)
			return result == PCM_X_QUEUE_BUSY ? PCM_X_QUEUE_NOT_READY : result;
		if (plan->candidate == NULL || candidate_promote)
			return PCM_X_QUEUE_CORRUPT;
	}
	plan->close_round = true;
	return PCM_X_QUEUE_OK;
}


/* Close the terminal leg for an S-holder node that was not selected as the
 * image source.  Type-48 leaves a generation-zero admission locator plus an
 * exact ACK tombstone.  The authenticated DRAIN carries the first final grant
 * generation this node can know, so capture it exactly and make all replays
 * generation- and drain-exact.  Caller holds the local tag lock. */
static PcmXQueueResult
pcm_x_local_blocker_participant_drain_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
											 const PcmXDrainPollPayload *poll,
											 int32 authenticated_master_node)
{
	PcmXImageToken zero_image;
	PcmXLocalRoundClosePlan round_plan;
	PcmXQueueResult blocker_state;
	PcmXQueueResult holder_state;
	uint32 holder_flags;

	if (tag_slot == NULL || poll == NULL)
		return PCM_X_QUEUE_CORRUPT;
	if (pcm_x_local_external_terminal_lanes_collide(tag_slot))
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
		|| pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref))
		return PCM_X_QUEUE_STALE;
	if (!pcm_x_ticket_locator_equal(&tag_slot->blocker_snapshot_ref, &poll->ref))
		return PCM_X_QUEUE_STALE;

	holder_flags = pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	memset(&zero_image, 0, sizeof(zero_image));
	if (tag_slot->blocker_snapshot_ref.grant_generation != 0 || holder_flags != 0
		|| tag_slot->holder_terminal_drain_generation != 0) {
		/* Classify the resident terminal shape before the incoming replay.  A
		 * wrong poll must not hide torn resident flags/drain evidence as STALE. */
		holder_state = pcm_x_local_holder_lane_retire_state(tag_slot,
															pcm_x_slot_flags_read(&tag_slot->slot));
		if (holder_state != PCM_X_QUEUE_OK)
			return holder_state;
		if (!pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, &poll->ref)
			|| tag_slot->holder_terminal_drain_generation != poll->drain_generation)
			return PCM_X_QUEUE_STALE;
		return PCM_X_QUEUE_DUPLICATE;
	}
	if (poll->ref.grant_generation == 0
		&& !pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, &poll->ref))
		return PCM_X_QUEUE_STALE;
	if (poll->ref.grant_generation == 0
		&& (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0)
		return PCM_X_QUEUE_CORRUPT;

	if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX)
		return PCM_X_QUEUE_NOT_READY;
	if (holder_flags != 0 || tag_slot->holder_terminal_drain_generation != 0
		|| !pcm_x_image_token_equal(&tag_slot->holder_image, &zero_image)
		|| !pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable))
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_local_admission_ref_valid(&tag_slot->blocker_snapshot_ref,
										 &tag_slot->blocker_snapshot_ref.identity)
		|| !BufferTagsEqual(&tag_slot->tag, &tag_slot->blocker_snapshot_ref.identity.tag)
		|| tag_slot->blocker_snapshot_ref.identity.cluster_epoch != tag_slot->cluster_epoch)
		return PCM_X_QUEUE_CORRUPT;
	if (!pcm_x_local_blocker_ack_tombstone_exact(tag_slot, authenticated_master_node)
		|| tag_slot->blocker_snapshot_reliable.response_tombstone_mask != 0) {
		blocker_state = pcm_x_local_blocker_lane_retire_state(tag_slot);
		return blocker_state == PCM_X_QUEUE_CORRUPT ? blocker_state : PCM_X_QUEUE_NOT_READY;
	}
	blocker_state = pcm_x_local_empty_frozen_round_plan_locked(tag_slot, tag_ref, &round_plan);
	if (blocker_state != PCM_X_QUEUE_OK)
		return blocker_state;

	tag_slot->blocker_snapshot_ref = poll->ref;
	tag_slot->blocker_set_generation = 0;
	memset(&tag_slot->blocker_snapshot_reliable, 0, sizeof(tag_slot->blocker_snapshot_reliable));
	tag_slot->holder_terminal_drain_generation = poll->drain_generation;
	pg_write_barrier();
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	pcm_x_local_empty_frozen_round_apply_locked(tag_slot, &round_plan);
	return PCM_X_QUEUE_OK;
}


/* A holder node may have no local writer membership at all.  Its type-50
 * resend ledger therefore closes directly through the local-tag directory.
 * The exact DRAIN_POLL is positive proof that the master consumed IMAGE_READY;
 * transport send success never reaches this function. */
static PcmXQueueResult
pcm_x_local_holder_drain_poll_exact(const PcmXDrainPollPayload *poll,
									int32 authenticated_master_node,
									uint64 authenticated_master_session)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalRoundClosePlan round_plan;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef tag_ref;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	PcmXQueueResult round_state;
	uint32 flags;
	uint32 holder_terminal_flags;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;

	runtime = cluster_pcm_x_runtime_snapshot();
	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &poll->ref.identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&poll->ref.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &poll->ref.identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto holder_drain_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto holder_drain_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto holder_drain_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_drain_done;
	}
	gate_claimed = true;
	if (pcm_x_local_external_terminal_lanes_collide(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto holder_drain_done;
	}
	if (pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)) {
		result = pcm_x_local_blocker_participant_drain_locked(tag_slot, tag_ref, poll,
															  authenticated_master_node);
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_drain_done;
	}
	if (!pcm_x_ticket_ref_equal(&tag_slot->holder_ref, &poll->ref)) {
		result = PCM_X_QUEUE_STALE;
		goto holder_drain_done;
	}
	/* REVOKE publishes holder_ref and consumes the blocker lane under this same
	 * tag lock.  Seeing both is structural corruption, never a transient wait. */
	if (!pcm_x_local_blocker_lane_is_pristine(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto holder_drain_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	holder_terminal_flags = flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	if ((holder_terminal_flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_DRAINED) != 0
		&& (holder_terminal_flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_READY) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto holder_drain_done;
	}
	if (holder_terminal_flags != 0) {
		if (holder_terminal_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
			|| tag_slot->holder_terminal_drain_generation == 0
			|| !pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
		} else if (tag_slot->holder_terminal_drain_generation != poll->drain_generation)
			result = PCM_X_QUEUE_STALE;
		else
			result = PCM_X_QUEUE_DUPLICATE;
		goto holder_drain_done;
	}
	if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->blocker_snapshot_count != 0 || tag_slot->blocker_snapshot_crc32c != 0
		|| tag_slot->blocker_snapshot_next_chunk != 0
		|| !pcm_x_transfer_leg_is_clear(&tag_slot->blocker_snapshot_reliable)
		|| !pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto holder_drain_done;
	}
	if (!pcm_x_transfer_leg_exact(&tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_IMAGE_READY,
								  authenticated_master_node, authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto holder_drain_done;
	}
	round_state = pcm_x_local_empty_frozen_round_plan_locked(tag_slot, tag_ref, &round_plan);
	if (round_state != PCM_X_QUEUE_OK) {
		result = round_state;
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_drain_done;
	}
	pcm_x_transfer_leg_clear(&tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_DRAIN_POLL,
							 authenticated_master_node);
	tag_slot->holder_terminal_drain_generation = poll->drain_generation;
	pg_write_barrier();
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	pcm_x_local_empty_frozen_round_apply_locked(tag_slot, &round_plan);
	result = PCM_X_QUEUE_OK;

holder_drain_done:
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_drain_poll_exact(const PcmXDrainPollPayload *poll,
									 int32 authenticated_master_node,
									 uint64 authenticated_master_session)
{
	return cluster_pcm_x_local_drain_poll_certificate_exact(poll, authenticated_master_node,
															authenticated_master_session, NULL);
}


/* First-consumption variant: capture the completion certificate under the
 * same tag lock that publishes TERMINAL_DRAINED, so the caller can release
 * the self-source immutable record against the exact protocol ledger. */
PcmXQueueResult
cluster_pcm_x_local_drain_poll_certificate_exact(const PcmXDrainPollPayload *poll,
												 int32 authenticated_master_node,
												 uint64 authenticated_master_session,
												 PcmXLocalDrainCertificate *certificate_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *candidate;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXSlotRef candidate_ref;
	PcmXQueueResult external_result;
	PcmXQueueResult result;
	uint32 flags;
	uint32 member_state;
	uint32 partition;
	bool gate_claimed = false;
	bool fail_closed = false;
	bool promote_candidate;

	if (certificate_out != NULL)
		memset(certificate_out, 0, sizeof(*certificate_out));
	if (header == NULL || poll == NULL || !pcm_x_local_terminal_ref_valid(&poll->ref)
		|| poll->drain_generation == 0 || poll->drain_generation == UINT64_MAX
		|| authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	frontier = &header->peer_frontiers[authenticated_master_node];
	if (frontier->cluster_epoch != poll->ref.identity.cluster_epoch
		|| frontier->sender_session_incarnation != authenticated_master_session)
		return PCM_X_QUEUE_STALE;
	result = pcm_x_local_terminal_refs_lookup(&poll->ref, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_STALE)
			return pcm_x_local_holder_drain_poll_exact(poll, authenticated_master_node,
													   authenticated_master_session);
		return result;
	}

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&poll->ref.identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &poll->ref.identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &poll->ref.identity.tag,
		PCM_X_STATE_BIT(PCM_XL_CANCELLED) | PCM_X_STATE_BIT(PCM_XL_GRANTED));
	if (tag_slot == NULL || member == NULL || member->tag_slot_index != tag_ref.slot_index
		|| member->tag_slot_generation != tag_ref.slot_generation
		|| !pcm_x_wait_identity_equal(&member->identity, &poll->ref.identity)) {
		result = PCM_X_QUEUE_STALE;
		goto drain_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto drain_done;
	}
	if (tag_slot->master_node != authenticated_master_node
		|| tag_slot->master_session_incarnation != authenticated_master_session
		|| tag_slot->cluster_epoch != poll->ref.identity.cluster_epoch
		|| !pcm_x_ticket_ref_equal(&tag_slot->ref, &poll->ref)
		|| !pcm_x_ticket_handle_equal(&member->handle, &poll->ref.handle)) {
		result = PCM_X_QUEUE_STALE;
		goto drain_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto drain_done;
	}
	gate_claimed = true;
	if (pcm_x_local_external_terminal_lanes_collide(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto drain_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) != 0
		&& (flags & PCM_X_LOCAL_TAG_F_TERMINAL_READY) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto drain_done;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->active_writer_slot_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto drain_done;
	}
	/* FINAL_CONFIRM and backend unlock are independent arrivals.  DRAIN must
	 * never unlink the leader or clear a claim owned by a backend that still
	 * has content authority; the master retry closes the terminal leg after
	 * exact claim release. */
	if (tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
		result = PCM_X_QUEUE_NOT_READY;
		goto drain_done;
	}
	member_state = pcm_x_slot_state_read(&member->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_READY) == 0) {
		if (member_state != PCM_XL_GRANTED || (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
			result = PCM_X_QUEUE_NOT_READY;
			goto drain_done;
		}
		if (member->admitted_round != tag_slot->local_round
			|| member->local_sequence > tag_slot->cutoff_sequence
			|| tag_slot->closed_round_member_count == 0) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto drain_done;
		}
		if ((pcm_x_slot_flags_read(&member->slot) & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) == 0
			|| tag_slot->closed_round_member_count != 1) {
			result = PCM_X_QUEUE_NOT_READY;
			goto drain_done;
		}
		if (!pcm_x_local_reliable_leg_valid(tag_slot, member)
			|| !pcm_x_transfer_leg_exact(&tag_slot->reliable, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM,
										 authenticated_master_node, authenticated_master_session)
			|| tag_slot->leader_index != member_ref.slot_index
			|| tag_slot->leader_slot_generation != member_ref.slot_generation
			|| member->role != PCM_X_LOCAL_ROLE_NODE_LEADER) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto drain_done;
		}
		result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, member->next_index,
												  member_ref.slot_index, member->local_sequence,
												  &candidate, &candidate_ref, &promote_candidate);
		if (result != PCM_X_QUEUE_OK) {
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			if (!fail_closed)
				goto drain_release_gate;
			goto drain_done;
		}
		if (!pcm_x_local_unlink_member_locked(tag_slot, tag_ref, member, member_ref)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto drain_done;
		}
		tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->leader_slot_generation = 0;
		pcm_x_transfer_leg_clear(&tag_slot->reliable, PGRAC_IC_MSG_PCM_X_DRAIN_POLL,
								 authenticated_master_node);
		(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_TERMINAL_READY);
		flags |= PCM_X_LOCAL_TAG_F_TERMINAL_READY;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) != 0) {
		result = tag_slot->terminal_drain_generation == poll->drain_generation
					 ? PCM_X_QUEUE_DUPLICATE
					 : PCM_X_QUEUE_STALE;
		goto drain_release_gate;
	}
	if ((member_state != PCM_XL_CANCELLED && member_state != PCM_XL_GRANTED)
		|| !pcm_x_local_terminal_pending_clear(tag_slot)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto drain_release_gate;
	}
	if (member->next_index != PCM_X_INVALID_SLOT_INDEX
		|| member->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->head_index == member_ref.slot_index
		|| tag_slot->tail_index == member_ref.slot_index
		|| tag_slot->leader_index == member_ref.slot_index) {
		result = PCM_X_QUEUE_NOT_READY;
		goto drain_done;
	}
	if (!pcm_x_local_terminal_unlinked_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto drain_done;
	}
	/* The requester node may simultaneously be either the source S-holder or
	 * a non-source holder participant.  Consume both durable legs under this
	 * one tag lock before publishing either drain. */
	if (pcm_x_ticket_ref_equal(&tag_slot->holder_ref, &poll->ref)) {
		uint32 holder_flags = flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;

		if (!pcm_x_local_blocker_lane_is_pristine(tag_slot)) {
			result = PCM_X_QUEUE_CORRUPT;
			fail_closed = true;
			goto drain_done;
		}
		if (holder_flags != 0) {
			if (holder_flags != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
				|| tag_slot->holder_terminal_drain_generation != poll->drain_generation
				|| !pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)) {
				result = PCM_X_QUEUE_CORRUPT;
				fail_closed = true;
				goto drain_done;
			}
		} else if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX
				   || !pcm_x_transfer_leg_exact(
					   &tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_IMAGE_READY,
					   authenticated_master_node, authenticated_master_session)) {
			result = PCM_X_QUEUE_NOT_READY;
			goto drain_release_gate;
		} else {
			pcm_x_transfer_leg_clear(&tag_slot->holder_reliable, PGRAC_IC_MSG_PCM_X_DRAIN_POLL,
									 authenticated_master_node);
			tag_slot->holder_terminal_drain_generation = poll->drain_generation;
			(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot,
											PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
		}
	} else if (!pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
			   && pcm_x_ticket_locator_equal(&tag_slot->blocker_snapshot_ref, &poll->ref)) {
		external_result = pcm_x_local_blocker_participant_drain_locked(tag_slot, tag_ref, poll,
																	   authenticated_master_node);
		if (external_result != PCM_X_QUEUE_OK && external_result != PCM_X_QUEUE_DUPLICATE) {
			result = external_result;
			fail_closed = result == PCM_X_QUEUE_CORRUPT;
			goto drain_release_gate;
		}
	}
	tag_slot->terminal_drain_generation = poll->drain_generation;
	pg_write_barrier();
	(void)pcm_x_slot_flags_fetch_or(&tag_slot->slot, PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED);
	/* Completion certificate: the writer-round ledger is captured under this
	 * same lock, before RETIRE can clear it, so the caller can verify the
	 * self-source release against the exact protocol evidence instead of the
	 * live descriptor (which may legitimately have moved on or been evicted). */
	if (certificate_out != NULL) {
		certificate_out->ref = tag_slot->ref;
		certificate_out->image = tag_slot->image;
		certificate_out->committed_own_generation = tag_slot->committed_own_generation;
		certificate_out->master_session_incarnation = tag_slot->master_session_incarnation;
		certificate_out->master_node = tag_slot->master_node;
		certificate_out->valid = true;
	}
	result = PCM_X_QUEUE_OK;

drain_release_gate:
drain_done:
	if (gate_claimed && !fail_closed && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	LWLockRelease(&header->local_locks[partition].lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_cancel_exact(const PcmXLocalHandle *handle, PcmXLocalHandle *new_leader_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *candidate = NULL;
	PcmXSlotRef member_ref;
	PcmXSlotRef candidate_ref = { PCM_X_INVALID_SLOT_INDEX, 0 };
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	uint32 partition;
	uint32 state;
	bool promote_candidate = false;

	pcm_x_local_handle_clear(new_leader_out);
	if (header == NULL || handle == NULL || !pcm_x_wait_identity_valid(&handle->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(&handle->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = member->tag_slot_index;
		tag_ref.slot_generation = member->tag_slot_generation;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK)
		return result;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&handle->identity.tag));
	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &handle->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_WAIT, member_ref,
														  &handle->identity.tag,
														  PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto cancel_local_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto cancel_local_done;
	}
	result = pcm_x_local_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		goto cancel_local_done;
	}
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->active_writer_slot_generation == 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	/* Cancellation may unlink or promote queue members.  Serialize it after
	 * exact active-claim completion so no claim token can retain a dangling
	 * slot or a role changed underneath it. */
	if (tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX) {
		result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	state = pcm_x_slot_state_read(&member->slot);
	if (state == PCM_XL_CANCELLED) {
		result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_DUPLICATE : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if (state != PCM_XL_NODE_LEADER && state != PCM_XL_JOINED_NONWAITABLE
		&& state != PCM_XL_WAITABLE_FOLLOWER) {
		result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_BAD_STATE : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if ((state == PCM_XL_NODE_LEADER
		 && (member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
			 || tag_slot->leader_index != member_ref.slot_index
			 || tag_slot->leader_slot_generation != member_ref.slot_generation))
		|| (state != PCM_XL_NODE_LEADER
			&& (member->role != PCM_X_LOCAL_ROLE_FOLLOWER
				|| tag_slot->leader_index == member_ref.slot_index))) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if (state == PCM_XL_WAITABLE_FOLLOWER && member->graph_generation != 0) {
		result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if ((state == PCM_XL_JOINED_NONWAITABLE || state == PCM_XL_NODE_LEADER)
		&& member->graph_generation != 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	/* Follower cancellation is local-only.  Once a leader has minted any
	 * remote authority, it must keep the tag/member and durable resend leg
	 * until an exact 58/60 application ACK performs the atomic rotation. */
	if (state == PCM_XL_NODE_LEADER
		&& (tag_slot->prehandle.sender_session_incarnation != 0
			|| tag_slot->prehandle.prehandle_sequence != 0
			|| !pcm_x_ticket_ref_is_zero(&tag_slot->ref)
			|| !pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable))) {
		result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	/* A follower can have been an active blocker and then abort its claim
	 * without acquiring a persistent COMPLETE marker.  Conservatively drain
	 * every later edge before any linked identity disappears; this also keeps
	 * cancellation sound across that former-active window. */
	result = pcm_x_local_follower_chain_ready(tag_slot, tag_ref, member->next_index,
											  member_ref.slot_index, member->local_sequence,
											  &candidate, &candidate_ref, &promote_candidate);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_BUSY)
			result = pcm_x_local_gate_release(tag_slot) ? result : PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if (!pcm_x_local_unlink_member_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto cancel_local_done;
	}
	if (state == PCM_XL_NODE_LEADER) {
		tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
		tag_slot->leader_slot_generation = 0;
		if (candidate != NULL && promote_candidate) {
			tag_slot->leader_index = candidate_ref.slot_index;
			tag_slot->leader_slot_generation = candidate_ref.slot_generation;
			candidate->role = PCM_X_LOCAL_ROLE_NODE_LEADER;
			pg_write_barrier();
			pcm_x_slot_state_write(&candidate->slot, PCM_XL_NODE_LEADER);
			if (new_leader_out != NULL)
				pcm_x_local_handle_from_member(new_leader_out, candidate_ref, candidate,
											   PCM_X_LOCAL_ROLE_NODE_LEADER);
		}
	}
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_CANCELLED);
	result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_OK : PCM_X_QUEUE_CORRUPT;

cancel_local_done:
	/* Any post-acquire direct jump here is CORRUPT and retains the gate. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	return result;
}


/* Prove that a terminal membership is no longer reachable from the local FIFO. */
static bool
pcm_x_local_terminal_unlinked_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
									 PcmXLocalMembershipSlot *terminal, PcmXSlotRef terminal_ref)
{
	PcmXAllocatorView view;
	Size current;
	Size previous = PCM_X_INVALID_SLOT_INDEX;
	Size visited = 0;

	if (tag_slot == NULL || terminal == NULL || !pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_WAIT, &view)
		|| terminal->next_index != PCM_X_INVALID_SLOT_INDEX
		|| terminal->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| tag_slot->head_index == terminal_ref.slot_index
		|| tag_slot->tail_index == terminal_ref.slot_index
		|| tag_slot->leader_index == terminal_ref.slot_index
		|| tag_slot->active_writer_index == terminal_ref.slot_index)
		return false;

	current = tag_slot->head_index;
	while (current != PCM_X_INVALID_SLOT_INDEX) {
		PcmXLocalMembershipSlot *linked;
		PcmXSlotRef linked_ref;

		if (current == terminal_ref.slot_index || visited >= tag_slot->membership_count
			|| visited >= view.capacity)
			return false;
		linked = (PcmXLocalMembershipSlot *)pcm_x_allocator_slot(&view, current);
		if (linked == NULL)
			return false;
		linked_ref.slot_index = current;
		if (!pcm_x_slot_generation_read(&linked->slot, &linked_ref.slot_generation))
			return false;
		linked = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
			PCM_X_ALLOC_LOCAL_WAIT, linked_ref, &tag_slot->tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
		if (linked == NULL || linked->tag_slot_index != tag_ref.slot_index
			|| linked->tag_slot_generation != tag_ref.slot_generation
			|| linked->prev_index != previous)
			return false;
		previous = current;
		current = linked->next_index;
		visited++;
	}
	if (tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX)
		return tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX;
	return tag_slot->tail_index == previous;
}


/* Return an open, empty writer queue to the holder-only form.  The holder
 * chain and its generation remain authoritative; only queue-session state is
 * cleared so a later writer can adopt the same tag under the admission gate. */
static void
pcm_x_local_reset_holder_only_queue(PcmXLocalTagSlot *tag_slot)
{
	tag_slot->master_node = -1;
	tag_slot->master_session_incarnation = 0;
	tag_slot->next_sequence = 1;
	tag_slot->cutoff_sequence = 0;
	tag_slot->blocker_set_generation = 0;
	memset(&tag_slot->prehandle, 0, sizeof(tag_slot->prehandle));
	memset(&tag_slot->ref, 0, sizeof(tag_slot->ref));
	memset(&tag_slot->image, 0, sizeof(tag_slot->image));
	memset(&tag_slot->reliable, 0, sizeof(tag_slot->reliable));
	tag_slot->leader_slot_generation = 0;
	tag_slot->active_writer_slot_generation = 0;
	tag_slot->closed_round_member_count = 0;
	tag_slot->committed_own_generation = 0;
	tag_slot->grant_base_own_generation = 0;
}


static PcmXQueueResult pcm_x_local_ready_leader_wake_locked(PcmXLocalTagSlot *tag_slot,
															PcmXSlotRef tag_ref,
															PcmXWaitIdentity *identity_out,
															PcmXLocalHandle *handle_out,
															bool *candidate_out);


static PcmXQueueResult
pcm_x_local_detach_terminal_common(const PcmXLocalHandle *handle, bool retire_protocol,
								   PcmXLocalHandle *promoted_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalRoundClosePlan round_plan;
	PcmXLocalHandle promoted;
	PcmXLocalHandle ready_leader;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot *gated_tag = NULL;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot *promoted_member = NULL;
	PcmXSlotRef found;
	PcmXSlotRef member_ref;
	PcmXSlotRef tag_ref;
	PcmXQueueResult result;
	PcmXWaitIdentity ready_identity;
	uint32 flags;
	uint32 partition;
	bool cancel_same_ref_dual = false;
	bool detach_after_close = false;
	bool detach_tag = false;
	bool gate_claimed = false;
	bool same_ref_dual = false;
	bool ready_leader_candidate = false;
	bool target_in_closed_round;
	bool close_round = false;

	pcm_x_local_handle_clear(promoted_out);
	pcm_x_local_handle_clear(&promoted);
	pcm_x_local_handle_clear(&ready_leader);
	memset(&round_plan, 0, sizeof(round_plan));
	memset(&ready_identity, 0, sizeof(ready_identity));
	if (header == NULL || handle == NULL || !pcm_x_wait_identity_valid(&handle->identity))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;
	if (retire_protocol)
		pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_SHARED, NULL);
	else
		LWLockAcquire(&header->allocator_lock.lock, LW_SHARED);
	result = pcm_x_local_member_lookup_locked(&handle->identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK) {
		tag_ref.slot_index = member->tag_slot_index;
		tag_ref.slot_generation = member->tag_slot_generation;
		if (!retire_protocol) {
			tag_slot
				= (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
			if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE
				|| !BufferTagsEqual(&tag_slot->tag, &handle->identity.tag)
				|| tag_slot->cluster_epoch != handle->identity.cluster_epoch)
				result = PCM_X_QUEUE_CORRUPT;
			else
				result = pcm_x_local_handoff_gate_claim_locked(header, tag_slot, false,
															   PCM_X_QUEUE_BUSY);
		}
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}
	gate_claimed = !retire_protocol;
	if (gate_claimed)
		gated_tag = tag_slot;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&handle->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE,
									 retire_protocol ? NULL : gated_tag);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &handle->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &handle->identity.tag,
		PCM_X_STATE_BIT(PCM_XL_CANCELLED)
			| (retire_protocol ? PCM_X_STATE_BIT(PCM_XL_GRANTED) : 0));
	if (tag_slot == NULL || member == NULL
		|| !pcm_x_local_handle_exact(handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_STALE;
		goto detach_local_domain_done;
	}
	if (!pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto detach_local_domain_done;
	}
	if (retire_protocol) {
		result = pcm_x_local_retire_gate_try_acquire(tag_slot);
		if (result != PCM_X_QUEUE_OK)
			goto detach_local_domain_done;
	}
	if (retire_protocol) {
		gate_claimed = true;
		gated_tag = tag_slot;
	} else if ((pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_domain_done;
	}
	if (tag_slot->membership_count == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_domain_done;
	}
	if (!pcm_x_local_terminal_unlinked_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_domain_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != 0) {
		uint32 terminal_flags = flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK;

		if (!retire_protocol) {
			result = PCM_X_QUEUE_NOT_READY;
			goto detach_local_domain_done;
		}
		if (terminal_flags == PCM_X_LOCAL_TAG_F_TERMINAL_READY) {
			result = PCM_X_QUEUE_NOT_READY;
			goto detach_local_domain_done;
		}
		if (terminal_flags != PCM_X_LOCAL_TAG_F_TERMINAL_MASK
			|| tag_slot->terminal_drain_generation == 0
			|| !pcm_x_local_terminal_ref_valid(&tag_slot->ref)
			|| !pcm_x_wait_identity_equal(&tag_slot->ref.identity, &handle->identity)
			|| !pcm_x_ticket_handle_equal(&tag_slot->ref.handle, &member->handle)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto detach_local_domain_done;
		}
	} else if (retire_protocol) {
		result = PCM_X_QUEUE_STALE;
		goto detach_local_domain_done;
	}
	if (retire_protocol) {
		result = pcm_x_local_same_ref_dual_retire_state(tag_slot, flags, &same_ref_dual,
														&cancel_same_ref_dual);
		if (result != PCM_X_QUEUE_OK)
			goto detach_local_domain_done;
		if (cancel_same_ref_dual) {
			/* Unlike a successful grant, cancellation has no positive generation
			 * that a holder-only detach can validate later.  Prove the exact
			 * writer tombstone is CANCELLED, then consume both same-ref terminal
			 * legs atomically under this local lock. */
			if (!same_ref_dual || pcm_x_slot_state_read(&member->slot) != PCM_XL_CANCELLED
				|| tag_slot->ref.grant_generation != 0
				|| !pcm_x_ticket_ref_equal(&tag_slot->blocker_snapshot_ref, &tag_slot->ref)) {
				result = PCM_X_QUEUE_CORRUPT;
				goto detach_local_domain_done;
			}
		}
	}
	target_in_closed_round = (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0
							 && member->admitted_round == tag_slot->local_round
							 && member->local_sequence <= tag_slot->cutoff_sequence;
	if ((cancel_same_ref_dual && !target_in_closed_round)
		|| (target_in_closed_round && tag_slot->closed_round_member_count == 0)
		|| tag_slot->closed_round_member_count > tag_slot->membership_count
		|| (((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0)
			&& tag_slot->closed_round_member_count != 0)
		|| (tag_slot->membership_count == 1
			&& tag_slot->closed_round_member_count != (target_in_closed_round ? 1 : 0))) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_domain_done;
	}
	if (retire_protocol && target_in_closed_round && tag_slot->closed_round_member_count == 1) {
		result = pcm_x_local_final_member_round_plan_locked(tag_slot, tag_ref, &round_plan);
		if (result != PCM_X_QUEUE_OK)
			goto detach_local_domain_done;
		close_round = true;
	}
	if (tag_slot->membership_count == 1) {
		if (tag_slot->head_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX
			|| tag_slot->reliable.pending_opcode != 0 || tag_slot->reliable.phase != 0) {
			result = PCM_X_QUEUE_CORRUPT;
			goto detach_local_domain_done;
		}
		/* Writer membership and holder transfer are independent lifetimes on
		 * the same tag.  Neither returning to holder-only form nor deleting the
		 * tag may erase the master binding while an exact holder ticket, send
		 * leg, or terminal-drain outcome remains authoritative. */
		if (!pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
			|| !pcm_x_transfer_leg_is_clear(&tag_slot->holder_reliable)
			|| ((flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0 && !cancel_same_ref_dual)) {
			result = PCM_X_QUEUE_NOT_READY;
			goto detach_local_domain_done;
		}
		if (tag_slot->active_holder_head_index != PCM_X_INVALID_SLOT_INDEX) {
			if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0
				|| tag_slot->blocker_snapshot_head_index != PCM_X_INVALID_SLOT_INDEX
				|| tag_slot->blocker_snapshot_count != 0
				|| tag_slot->closed_round_member_count != 0) {
				result = PCM_X_QUEUE_NOT_READY;
				goto detach_local_domain_done;
			}
			pcm_x_local_reset_holder_only_queue(tag_slot);
		} else if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
			pg_write_barrier();
			pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
			detach_tag = true;
		}
		if (close_round) {
			if (round_plan.candidate != NULL) {
				result = PCM_X_QUEUE_CORRUPT;
				goto detach_local_domain_done;
			}
			detach_after_close = true;
		}
	}
	/* All retryable and structural checks precede the first mutation.  A
	 * cancellation dual terminal is one local transaction: holder evidence,
	 * writer terminal, and membership either all survive or all advance. */
	if (cancel_same_ref_dual) {
		memset(&tag_slot->blocker_snapshot_ref, 0, sizeof(tag_slot->blocker_snapshot_ref));
		tag_slot->blocker_set_generation = 0;
		memset(&tag_slot->blocker_snapshot_reliable, 0,
			   sizeof(tag_slot->blocker_snapshot_reliable));
		tag_slot->holder_terminal_drain_generation = 0;
		pg_write_barrier();
		(void)pcm_x_slot_flags_fetch_and(&tag_slot->slot, ~PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	}
	if (target_in_closed_round) {
		tag_slot->closed_round_member_count--;
	}
	if (retire_protocol) {
		uint64 reliable_state_sequence = tag_slot->reliable.state_sequence;

		memset(&tag_slot->prehandle, 0, sizeof(tag_slot->prehandle));
		memset(&tag_slot->ref, 0, sizeof(tag_slot->ref));
		/* image and committed_own_generation are writer-round evidence.  They
		 * must disappear in the same local transaction as the exact terminal
		 * ref; otherwise the promoted next-round leader inherits a stale grant
		 * and its generation rekey correctly classifies the tag as corrupt. */
		memset(&tag_slot->image, 0, sizeof(tag_slot->image));
		tag_slot->committed_own_generation = 0;
		tag_slot->grant_base_own_generation = 0;
		/* DRAIN closes the old round by leaving its response opcode as a replay
		 * tombstone.  RETIRE consumes that tombstone with the membership: a
		 * promoted successor must see a pristine application leg or the requester
		 * cannot mint its first ENQUEUE.  Keep only the monotonic state sequence so
		 * a new round cannot recreate an old reliable token. */
		memset(&tag_slot->reliable, 0, sizeof(tag_slot->reliable));
		tag_slot->reliable.state_sequence = reliable_state_sequence;
		tag_slot->terminal_drain_generation = 0;
		(void)pcm_x_slot_flags_fetch_and(&tag_slot->slot, ~PCM_X_LOCAL_TAG_F_TERMINAL_MASK);
	}
	tag_slot->membership_count--;
	if (close_round) {
		pcm_x_local_empty_frozen_round_apply_locked(tag_slot, &round_plan);
		if (round_plan.candidate != NULL)
			pcm_x_local_handle_from_member(&promoted, round_plan.candidate_ref,
										   round_plan.candidate, PCM_X_LOCAL_ROLE_NODE_LEADER);
	}
	/* Cancellation ACK may have promoted a same-round successor before DRAIN,
	 * while the terminal fence still made that first wake unusable.  Once this
	 * RETIRE mutation removes the last local terminal lane, report either that
	 * already-promoted leader or the final-member promotion above through the
	 * same exact output.  An independent holder terminal keeps the fence and
	 * therefore deliberately suppresses the wake until its own RETIRE. */
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if (retire_protocol
		&& (flags & (PCM_X_LOCAL_TAG_F_TERMINAL_MASK | PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK))
			   == 0) {
		result = pcm_x_local_ready_leader_wake_locked(tag_slot, tag_ref, &ready_identity,
													  &ready_leader, &ready_leader_candidate);
		if (result != PCM_X_QUEUE_OK)
			goto detach_local_domain_done;
		if (ready_leader_candidate) {
			if (pcm_x_wait_identity_valid(&promoted.identity)
				&& (!pcm_x_wait_identity_equal(&promoted.identity, &ready_identity)
					|| promoted.membership_slot.slot_index
						   != ready_leader.membership_slot.slot_index
					|| promoted.membership_slot.slot_generation
						   != ready_leader.membership_slot.slot_generation)) {
				result = PCM_X_QUEUE_CORRUPT;
				goto detach_local_domain_done;
			}
			promoted = ready_leader;
		}
	}
	if (detach_after_close) {
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
		detach_tag = true;
	}
	pg_write_barrier();
	pcm_x_slot_state_write(&member->slot, PCM_XL_DETACHING);
	result = PCM_X_QUEUE_OK;

detach_local_domain_done:
	/* Any post-acquire error reaching here is CORRUPT and retains the gate. */
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK) {
		if (result != PCM_X_QUEUE_CORRUPT && gate_claimed) {
			if (!pcm_x_local_gate_release(gated_tag))
				result = PCM_X_QUEUE_CORRUPT;
			else
				gate_claimed = false;
		}
		if (result == PCM_X_QUEUE_CORRUPT)
			pcm_x_runtime_fail_closed();
		return result;
	}

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, gated_tag);
	member = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_WAIT,
																	  member_ref);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (close_round && round_plan.candidate != NULL)
		promoted_member = (PcmXLocalMembershipSlot *)pcm_x_slot_ref_resolve_locked(
			PCM_X_ALLOC_LOCAL_WAIT, round_plan.candidate_ref);
	if (member == NULL || tag_slot == NULL
		|| pcm_x_slot_state_read(&member->slot) != PCM_XL_DETACHING
		|| (detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_DETACHING)
		|| (!detach_tag && pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_LIVE)
		|| !(close_round ? pcm_x_local_detaching_handle_exact(handle, member_ref, member, tag_slot)
						 : pcm_x_local_handle_exact(handle, member_ref, member, tag_slot))
		|| (close_round && round_plan.candidate != NULL
			&& (promoted_member == NULL
				|| pcm_x_slot_state_read(&promoted_member->slot) != PCM_XL_NODE_LEADER
				|| promoted_member->role != PCM_X_LOCAL_ROLE_NODE_LEADER
				|| promoted_member->tag_slot_index != tag_ref.slot_index
				|| promoted_member->tag_slot_generation != tag_ref.slot_generation
				|| promoted_member->admitted_round != tag_slot->local_round
				|| promoted_member->local_sequence != promoted.local_sequence
				|| !pcm_x_wait_identity_equal(&promoted_member->identity, &promoted.identity)))
		|| (pcm_x_slot_flags_read(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) == 0) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_allocator_done;
	}
	if (pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_WAIT, &handle->identity, &found)
			!= PCM_X_DIRECTORY_OK
		|| found.slot_index != member_ref.slot_index
		|| found.slot_generation != member_ref.slot_generation
		|| (detach_tag
			&& (pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &handle->identity.tag, &found)
					!= PCM_X_DIRECTORY_OK
				|| found.slot_index != tag_ref.slot_index
				|| found.slot_generation != tag_ref.slot_generation))) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_allocator_done;
	}
	if (pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_WAIT, &handle->identity, member_ref)
			!= PCM_X_DIRECTORY_OK
		|| (detach_tag
			&& pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, &handle->identity.tag,
												   tag_ref)
				   != PCM_X_DIRECTORY_OK)
		|| pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_WAIT, member_ref, PCM_XL_DETACHING)
			   != PCM_X_ALLOC_OK
		|| (detach_tag
			&& pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref, PCM_X_TAG_DETACHING)
				   != PCM_X_ALLOC_OK)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_allocator_done;
	}
	if (!detach_tag && !pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto detach_local_allocator_done;
	}
	result = PCM_X_QUEUE_OK;

detach_local_allocator_done:
	/* Allocator-phase failures are CORRUPT and retain any surviving gate. */
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		pcm_x_runtime_fail_closed();
	if (result == PCM_X_QUEUE_OK && promoted_out != NULL)
		*promoted_out = promoted;
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_detach_terminal_exact(const PcmXLocalHandle *handle)
{
	return pcm_x_local_detach_terminal_common(handle, false, NULL);
}


/*
 * RETIRE is deliberately a bounded cold-path scan over the existing local-tag
 * pool.  A live tag's BufferTag is immutable until allocator-owned release;
 * generation/state are sampled before taking its partition lock and then
 * revalidated exactly under that lock.  No allocator/domain locks are nested.
 */
static PcmXQueueResult
pcm_x_local_ready_leader_wake_locked(PcmXLocalTagSlot *tag_slot, PcmXSlotRef tag_ref,
									 PcmXWaitIdentity *identity_out, PcmXLocalHandle *handle_out,
									 bool *candidate_out)
{
	PcmXLocalMembershipSlot *leader;
	PcmXSlotRef leader_ref;
	uint32 member_flags;
	uint32 member_state;

	if (identity_out == NULL || candidate_out == NULL)
		return PCM_X_QUEUE_CORRUPT;
	memset(identity_out, 0, sizeof(*identity_out));
	pcm_x_local_handle_clear(handle_out);
	*candidate_out = false;
	if (tag_slot == NULL)
		return PCM_X_QUEUE_CORRUPT;
	if ((tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->leader_slot_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX)
		return PCM_X_QUEUE_OK;
	if ((tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX)
		!= (tag_slot->active_writer_slot_generation == 0))
		return PCM_X_QUEUE_CORRUPT;
	if (tag_slot->active_writer_index != PCM_X_INVALID_SLOT_INDEX)
		return PCM_X_QUEUE_OK;
	leader_ref.slot_index = tag_slot->leader_index;
	leader_ref.slot_generation = tag_slot->leader_slot_generation;
	leader = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, leader_ref, &tag_slot->tag, PCM_X_LOCAL_MEMBER_DOMAIN_STATES);
	if (leader == NULL || tag_slot->membership_count == 0
		|| tag_slot->head_index != leader_ref.slot_index
		|| leader->tag_slot_index != tag_ref.slot_index
		|| leader->tag_slot_generation != tag_ref.slot_generation
		|| leader->prev_index != PCM_X_INVALID_SLOT_INDEX
		|| leader->role != PCM_X_LOCAL_ROLE_NODE_LEADER
		|| leader->admitted_round != tag_slot->local_round || leader->graph_generation != 0
		|| !BufferTagsEqual(&leader->identity.tag, &tag_slot->tag)
		|| leader->identity.cluster_epoch != tag_slot->cluster_epoch)
		return PCM_X_QUEUE_CORRUPT;
	member_flags = pcm_x_slot_flags_read(&leader->slot);
	if ((member_flags & PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE) != 0)
		return PCM_X_QUEUE_CORRUPT;
	member_state = pcm_x_slot_state_read(&leader->slot);
	if (!pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)) {
		if (!pcm_x_local_reliable_leg_valid(tag_slot, leader))
			return PCM_X_QUEUE_CORRUPT;
		return PCM_X_QUEUE_OK;
	}
	if (member_state != PCM_XL_NODE_LEADER)
		return PCM_X_QUEUE_OK;
	*identity_out = leader->identity;
	if (handle_out != NULL)
		pcm_x_local_handle_from_member(handle_out, leader_ref, leader,
									   PCM_X_LOCAL_ROLE_NODE_LEADER);
	*candidate_out = true;
	return PCM_X_QUEUE_OK;
}


/*
 * A fully-drained cross-lane holder/blocker terminal (no writer-terminal
 * companion, so it is not a same-ref dual) belongs to an already-revoked older
 * cohort: its S lane has finished DRAIN and it carries a completed grant.  The
 * newer writer's REVOKE_BARRIER guards that writer's own revoke round, not this
 * older holder lane's terminal cleanup, so the older lane may retire itself
 * while every writer ref / round / barrier / FIFO field is preserved.  Retiring
 * it also clears HOLDER_TERMINAL_MASK, which lets the empty-frozen-round path
 * finally drop the REVOKE_BARRIER.  Cancel-duals (grant_generation == 0) are
 * routed through the same-ref-dual path and are excluded here.
 *
 * The exemption is strictly cross-lane: a writer-lane ref that aliases the
 * external ref (or merely collides with its master-unique ticket id) cannot
 * be an older cohort and keeps the pre-exemption NOT_READY verdict.  There is
 * deliberately no ticket-order requirement beyond that: a cancelled
 * predecessor leader's duplicate-ACK tombstone legally lingers in
 * tag_slot->ref with an OLDER ticket until the successor's ENQUEUE arm
 * retires it, and refusing retirement there could wedge the empty-frozen
 * path permanently if the successor exits before arming.
 *
 * Precondition: the caller MUST have validated
 * pcm_x_local_holder_lane_retire_state() == OK before consulting this helper.
 * The test here does not itself re-prove the DRAIN evidence
 * (holder_terminal_drain_generation, a clear reliable leg, a DRAIN_POLL last
 * response); both call sites run that check immediately above the guard.
 */
static inline bool
pcm_x_local_cross_lane_holder_terminal_retirable(const PcmXLocalTagSlot *tag_slot, uint32 flags,
												 const PcmXTicketRef *external_ref)
{
	if (tag_slot == NULL || external_ref == NULL)
		return false;
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != 0
		|| (flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK)
			   != PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK
		|| external_ref->grant_generation == 0)
		return false;
	/* Per-session ticket ids are master-unique (monotonic allocator, exhaustion
	 * fails closed), so id equality alone is complete for the alias class.  Do
	 * not "simplify" this to full-ref equality: that would stop refusing an id
	 * collision under a distinct identity and REDUCE fail-closed coverage. */
	if (!pcm_x_ticket_ref_is_zero(&tag_slot->ref)
		&& external_ref->handle.ticket_id == tag_slot->ref.handle.ticket_id)
		return false;
	return true;
}


static PcmXQueueResult
pcm_x_local_retire_candidate_at(Size slot_index, const PcmXRetirePayload *request,
								int32 authenticated_master_node,
								uint64 authenticated_master_session, PcmXTicketRef *ref_out,
								PcmXTicketRef *writer_ref_out, bool *writer_candidate_out,
								bool *holder_candidate_out, bool *candidate_out,
								bool *contains_watermark_out, PcmXWaitIdentity *holder_wake_out,
								bool *holder_wake_candidate_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXAllocatorView view;
	PcmXLocalTagSlot *raw;
	PcmXLocalTagSlot *tag_slot;
	const PcmXTicketRef *external_ref;
	PcmXSlotRef tag_ref;
	BufferTag tag;
	uint64 generation_after;
	uint32 flags;
	uint32 partition;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	PcmXQueueResult holder_state;
	bool cancel_same_ref_dual = false;
	bool same_ref_dual = false;

	*candidate_out = false;
	*writer_candidate_out = false;
	*holder_candidate_out = false;
	*contains_watermark_out = false;
	*holder_wake_candidate_out = false;
	memset(ref_out, 0, sizeof(*ref_out));
	memset(writer_ref_out, 0, sizeof(*writer_ref_out));
	memset(holder_wake_out, 0, sizeof(*holder_wake_out));
	if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &view) || slot_index >= view.capacity)
		return PCM_X_QUEUE_INVALID;
	raw = (PcmXLocalTagSlot *)pcm_x_allocator_slot(&view, slot_index);
	if (raw == NULL || pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE)
		return PCM_X_QUEUE_OK;
	if (!pcm_x_slot_generation_read(&raw->slot, &tag_ref.slot_generation))
		return PCM_X_QUEUE_BUSY;
	tag_ref.slot_index = slot_index;
	tag = raw->tag;
	pg_read_barrier();
	if (!pcm_x_slot_generation_read(&raw->slot, &generation_after)
		|| generation_after != tag_ref.slot_generation
		|| pcm_x_slot_state_read(&raw->slot) != PCM_X_TAG_LIVE)
		return PCM_X_QUEUE_BUSY;

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_SHARED, NULL);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(PCM_X_ALLOC_LOCAL_TAG, tag_ref, &tag,
													 PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	if (tag_slot == NULL) {
		result = PCM_X_QUEUE_BUSY;
		goto candidate_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & (PCM_X_LOCAL_TAG_F_TERMINAL_MASK | PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK)) == 0)
		goto candidate_done;
	if (tag_slot->master_node != authenticated_master_node)
		goto candidate_done;
	if (tag_slot->cluster_epoch != request->cluster_epoch
		|| tag_slot->master_session_incarnation != authenticated_master_session) {
		result = PCM_X_QUEUE_STALE;
		goto candidate_done;
	}
	result = pcm_x_local_same_ref_dual_retire_state(tag_slot, flags, &same_ref_dual,
													&cancel_same_ref_dual);
	if (result != PCM_X_QUEUE_OK)
		goto candidate_done;
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != 0) {
		holder_state = pcm_x_local_holder_lane_retire_state(tag_slot, flags);
		if (holder_state != PCM_X_QUEUE_OK) {
			result = holder_state;
			goto candidate_done;
		}
		if (!pcm_x_local_terminal_ref_valid(&tag_slot->ref)
			|| !BufferTagsEqual(&tag_slot->tag, &tag_slot->ref.identity.tag)) {
			result = PCM_X_QUEUE_CORRUPT;
			goto candidate_done;
		}
		if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != PCM_X_LOCAL_TAG_F_TERMINAL_MASK
			|| tag_slot->terminal_drain_generation == 0
			|| !pcm_x_local_terminal_pending_clear(tag_slot)) {
			result = PCM_X_QUEUE_NOT_READY;
			goto candidate_done;
		}
		if (tag_slot->ref.handle.ticket_id == request->retire_through_ticket_id)
			*contains_watermark_out = true;
		if (tag_slot->ref.handle.ticket_id <= request->retire_through_ticket_id) {
			*writer_ref_out = tag_slot->ref;
			*writer_candidate_out = true;
			*ref_out = tag_slot->ref;
			*candidate_out = true;
		}
	}
	if ((flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0) {
		holder_state = pcm_x_local_holder_lane_retire_state(tag_slot, flags);
		if (holder_state != PCM_X_QUEUE_OK) {
			result = holder_state;
			goto candidate_done;
		}
		external_ref = pcm_x_local_external_terminal_ref(tag_slot);
		if (external_ref == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			goto candidate_done;
		}
		if (external_ref->handle.ticket_id == request->retire_through_ticket_id)
			*contains_watermark_out = true;
		if (external_ref->handle.ticket_id <= request->retire_through_ticket_id) {
			if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0 && !same_ref_dual
				&& !pcm_x_local_cross_lane_holder_terminal_retirable(tag_slot, flags,
																	 external_ref)) {
				result = PCM_X_QUEUE_NOT_READY;
				goto candidate_done;
			}
			/* A generation-zero dual terminal must validate and consume the
			 * CANCELLED membership before its blocker evidence disappears.  Keep
			 * the writer candidate selected above; detach_terminal_common closes
			 * both lanes under the same local lock. */
			if (cancel_same_ref_dual) {
				if (!*candidate_out || !pcm_x_ticket_ref_equal(ref_out, external_ref))
					result = PCM_X_QUEUE_CORRUPT;
				goto candidate_done;
			}
			if (!same_ref_dual && *candidate_out)
				goto candidate_done;
			*ref_out = *external_ref;
			*holder_candidate_out = true;
			*candidate_out = true;
		}
	}
	if (result == PCM_X_QUEUE_OK && *holder_candidate_out)
		result = pcm_x_local_ready_leader_wake_locked(tag_slot, tag_ref, holder_wake_out, NULL,
													  holder_wake_candidate_out);

candidate_done:
	LWLockRelease(&header->local_locks[partition].lock);
	return result;
}


static PcmXQueueResult
pcm_x_local_holder_detach_terminal_exact(const PcmXTicketRef *ref, int32 authenticated_master_node,
										 uint64 authenticated_master_session,
										 PcmXWaitIdentity *ready_leader_out,
										 bool *ready_leader_candidate_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalTagSlot *tag_slot;
	const PcmXTicketRef *external_ref;
	PcmXSlotRef tag_ref;
	PcmXSlotRef found;
	PcmXDirectoryResult directory_result;
	PcmXQueueResult result;
	PcmXQueueResult holder_state;
	PcmXWaitIdentity ready_leader;
	uint32 flags;
	uint32 partition;
	bool detach_tag = false;
	bool fail_closed = false;
	bool blocker_terminal = false;
	bool cancel_same_ref_dual = false;
	bool same_ref_dual = false;
	bool ready_leader_candidate = false;

	if (ready_leader_out == NULL || ready_leader_candidate_out == NULL)
		return PCM_X_QUEUE_INVALID;
	memset(ready_leader_out, 0, sizeof(*ready_leader_out));
	*ready_leader_candidate_out = false;
	memset(&ready_leader, 0, sizeof(ready_leader));

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_SHARED, NULL);
	directory_result
		= pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, &tag_ref);
	LWLockRelease(&header->allocator_lock.lock);
	if (directory_result == PCM_X_DIRECTORY_NOT_FOUND)
		return PCM_X_QUEUE_STALE;
	if (directory_result != PCM_X_DIRECTORY_OK)
		return pcm_x_queue_result_from_directory(directory_result);

	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&ref->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_EXCLUSIVE, NULL);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &ref->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	external_ref = pcm_x_local_external_terminal_ref(tag_slot);
	if (tag_slot == NULL || external_ref == NULL || !pcm_x_ticket_ref_equal(external_ref, ref)) {
		result = PCM_X_QUEUE_STALE;
		goto holder_detach_domain_done;
	}
	blocker_terminal = external_ref == &tag_slot->blocker_snapshot_ref;
	result = pcm_x_local_retire_gate_try_acquire(tag_slot);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_detach_domain_done;
	}
	if (!pcm_x_local_holder_transfer_peer_exact(tag_slot, authenticated_master_node,
												authenticated_master_session)) {
		result = PCM_X_QUEUE_STALE;
		goto holder_detach_release_gate;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	result = pcm_x_local_same_ref_dual_retire_state(tag_slot, flags, &same_ref_dual,
													&cancel_same_ref_dual);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_detach_domain_done;
	}
	if (cancel_same_ref_dual) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto holder_detach_domain_done;
	}
	holder_state = pcm_x_local_holder_lane_retire_state(tag_slot, flags);
	if (holder_state != PCM_X_QUEUE_OK) {
		result = holder_state;
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		if (!fail_closed)
			goto holder_detach_release_gate;
		goto holder_detach_domain_done;
	}
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0 && !same_ref_dual
		&& !pcm_x_local_cross_lane_holder_terminal_retirable(tag_slot, flags, external_ref)) {
		result = PCM_X_QUEUE_NOT_READY;
		goto holder_detach_release_gate;
	}
	result = pcm_x_local_ready_leader_wake_locked(tag_slot, tag_ref, &ready_leader, NULL,
												  &ready_leader_candidate);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto holder_detach_domain_done;
	}
	if (blocker_terminal) {
		memset(&tag_slot->blocker_snapshot_ref, 0, sizeof(tag_slot->blocker_snapshot_ref));
		tag_slot->blocker_set_generation = 0;
		memset(&tag_slot->blocker_snapshot_reliable, 0,
			   sizeof(tag_slot->blocker_snapshot_reliable));
	} else {
		memset(&tag_slot->holder_ref, 0, sizeof(tag_slot->holder_ref));
		memset(&tag_slot->holder_image, 0, sizeof(tag_slot->holder_image));
		memset(&tag_slot->holder_reliable, 0, sizeof(tag_slot->holder_reliable));
	}
	tag_slot->holder_terminal_drain_generation = 0;
	(void)pcm_x_slot_flags_fetch_and(&tag_slot->slot, ~PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	detach_tag
		= tag_slot->membership_count == 0 && tag_slot->closed_round_member_count == 0
		  && tag_slot->active_holder_head_index == PCM_X_INVALID_SLOT_INDEX
		  && tag_slot->head_index == PCM_X_INVALID_SLOT_INDEX
		  && tag_slot->tail_index == PCM_X_INVALID_SLOT_INDEX
		  && tag_slot->leader_index == PCM_X_INVALID_SLOT_INDEX
		  && tag_slot->active_writer_index == PCM_X_INVALID_SLOT_INDEX
		  && pcm_x_ticket_ref_is_zero(&tag_slot->ref)
		  && pcm_x_ticket_ref_is_zero(&tag_slot->holder_ref)
		  && pcm_x_ticket_ref_is_zero(&tag_slot->blocker_snapshot_ref)
		  && pcm_x_local_reliable_leg_is_clear(&tag_slot->reliable)
		  && pcm_x_transfer_leg_is_pristine(&tag_slot->holder_reliable)
		  && pcm_x_transfer_leg_is_pristine(&tag_slot->blocker_snapshot_reliable)
		  && (flags & (PCM_X_LOCAL_TAG_F_REVOKE_BARRIER | PCM_X_LOCAL_TAG_F_TERMINAL_MASK)) == 0;
	if (detach_tag) {
		pg_write_barrier();
		pcm_x_slot_state_write(&tag_slot->slot, PCM_X_TAG_DETACHING);
		result = PCM_X_QUEUE_OK;
		goto holder_detach_domain_done;
	}
	result = pcm_x_local_gate_release(tag_slot) ? PCM_X_QUEUE_OK : PCM_X_QUEUE_CORRUPT;
	if (result == PCM_X_QUEUE_CORRUPT)
		fail_closed = true;
	goto holder_detach_domain_done;

holder_detach_release_gate:
	if (!pcm_x_local_gate_release(tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
holder_detach_domain_done:
	LWLockRelease(&header->local_locks[partition].lock);
	if (result != PCM_X_QUEUE_OK || !detach_tag) {
		if (fail_closed)
			pcm_x_runtime_fail_closed();
		if (result == PCM_X_QUEUE_OK && ready_leader_candidate) {
			*ready_leader_out = ready_leader;
			*ready_leader_candidate_out = true;
		}
		return result;
	}

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, tag_slot);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_slot_ref_resolve_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref);
	if (tag_slot == NULL || pcm_x_slot_state_read(&tag_slot->slot) != PCM_X_TAG_DETACHING
		|| pcm_x_directory_find_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, &found)
			   != PCM_X_DIRECTORY_OK
		|| found.slot_index != tag_ref.slot_index
		|| found.slot_generation != tag_ref.slot_generation
		|| pcm_x_directory_delete_exact_locked(PCM_X_DIR_LOCAL_TAG, &ref->identity.tag, tag_ref)
			   != PCM_X_DIRECTORY_OK
		|| pcm_x_allocator_release_locked(PCM_X_ALLOC_LOCAL_TAG, tag_ref, PCM_X_TAG_DETACHING)
			   != PCM_X_ALLOC_OK) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	} else
		result = PCM_X_QUEUE_OK;
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	if (result == PCM_X_QUEUE_OK && ready_leader_candidate) {
		*ready_leader_out = ready_leader;
		*ready_leader_candidate_out = true;
	}
	return result;
}


static PcmXQueueResult
pcm_x_local_terminal_handle_lookup(const PcmXWaitIdentity *identity, PcmXLocalHandle *handle_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;

	pcm_x_local_handle_clear(handle_out);
	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_SHARED, NULL);
	result = pcm_x_local_member_lookup_locked(identity, &member_ref, &member);
	if (result == PCM_X_QUEUE_OK)
		pcm_x_local_handle_from_member(handle_out, member_ref, member, (PcmXLocalRole)member->role);
	LWLockRelease(&header->allocator_lock.lock);
	return result == PCM_X_QUEUE_NOT_FOUND ? PCM_X_QUEUE_STALE : result;
}


/* RETIRE's first pass must prove every projected final-member round close
 * before any older tag is reclaimed.  In particular a late follower can
 * still own an external WFG edge; that is ordinary NOT_READY, not a reason to
 * apply earlier tags and then turn the second-pass surprise into corruption. */
static PcmXQueueResult
pcm_x_local_final_member_round_preflight_exact(const PcmXLocalHandle *handle,
											   PcmXWaitIdentity *projected_wake_out,
											   bool *projected_wake_candidate_out)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXLocalRoundClosePlan plan;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXSlotRef tag_ref;
	PcmXSlotRef member_ref;
	PcmXQueueResult result;
	PcmXWaitIdentity existing_wake;
	uint32 flags;
	uint32 partition;
	bool cancel_same_ref_dual = false;
	bool existing_wake_candidate = false;
	bool same_ref_dual = false;
	bool target_in_closed_round;

	if (projected_wake_out == NULL || projected_wake_candidate_out == NULL)
		return PCM_X_QUEUE_INVALID;
	memset(projected_wake_out, 0, sizeof(*projected_wake_out));
	memset(&existing_wake, 0, sizeof(existing_wake));
	*projected_wake_candidate_out = false;
	if (header == NULL || handle == NULL || !pcm_x_wait_identity_valid(&handle->identity))
		return PCM_X_QUEUE_INVALID;
	result = pcm_x_local_refs_lookup(handle, &tag_ref, &member_ref);
	if (result != PCM_X_QUEUE_OK)
		return result == PCM_X_QUEUE_NOT_FOUND ? PCM_X_QUEUE_STALE : result;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&handle->identity.tag));
	pcm_x_local_gate_acquire_guarded(&header->local_locks[partition].lock, LW_SHARED, NULL);
	tag_slot = (PcmXLocalTagSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_TAG, tag_ref, &handle->identity.tag, PCM_X_STATE_BIT(PCM_X_TAG_LIVE));
	member = (PcmXLocalMembershipSlot *)pcm_x_domain_slot(
		PCM_X_ALLOC_LOCAL_WAIT, member_ref, &handle->identity.tag,
		PCM_X_STATE_BIT(PCM_XL_CANCELLED) | PCM_X_STATE_BIT(PCM_XL_GRANTED));
	if (tag_slot == NULL || member == NULL) {
		result = PCM_X_QUEUE_STALE;
		goto preflight_done;
	}
	if (!pcm_x_local_handle_exact(handle, member_ref, member, tag_slot)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto preflight_done;
	}
	flags = pcm_x_slot_flags_read(&tag_slot->slot);
	if ((flags & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) != PCM_X_LOCAL_TAG_F_TERMINAL_MASK
		|| tag_slot->terminal_drain_generation == 0
		|| !pcm_x_wait_identity_equal(&tag_slot->ref.identity, &member->identity)
		|| !pcm_x_ticket_handle_equal(&tag_slot->ref.handle, &member->handle)
		|| !pcm_x_local_terminal_unlinked_locked(tag_slot, tag_ref, member, member_ref)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto preflight_done;
	}
	result = pcm_x_local_same_ref_dual_retire_state(tag_slot, flags, &same_ref_dual,
													&cancel_same_ref_dual);
	if (result != PCM_X_QUEUE_OK)
		goto preflight_done;
	if (cancel_same_ref_dual
		&& (!same_ref_dual || pcm_x_slot_state_read(&member->slot) != PCM_XL_CANCELLED)) {
		result = PCM_X_QUEUE_CORRUPT;
		goto preflight_done;
	}
	/* A cancel ACK may already have rotated leader_index while its terminal
	 * fence kept the successor asleep.  Project that exact leader only when
	 * this writer RETIRE also clears every holder terminal lane; otherwise the
	 * independent holder RETIRE remains the sole future wake authority. */
	if ((flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) == 0 || same_ref_dual) {
		result = pcm_x_local_ready_leader_wake_locked(tag_slot, tag_ref, &existing_wake, NULL,
													  &existing_wake_candidate);
		if (result != PCM_X_QUEUE_OK)
			goto preflight_done;
		if (existing_wake_candidate) {
			*projected_wake_out = existing_wake;
			*projected_wake_candidate_out = true;
		}
	}
	target_in_closed_round = (flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0
							 && member->admitted_round == tag_slot->local_round
							 && member->local_sequence <= tag_slot->cutoff_sequence;
	if ((flags & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) == 0) {
		result = tag_slot->closed_round_member_count == 0 ? PCM_X_QUEUE_OK : PCM_X_QUEUE_CORRUPT;
		goto preflight_done;
	}
	if (!target_in_closed_round || tag_slot->closed_round_member_count == 0
		|| tag_slot->closed_round_member_count > tag_slot->membership_count) {
		result = PCM_X_QUEUE_CORRUPT;
		goto preflight_done;
	}
	if (tag_slot->closed_round_member_count != 1) {
		result = PCM_X_QUEUE_OK;
		goto preflight_done;
	}
	result = pcm_x_local_final_member_round_plan_locked(tag_slot, tag_ref, &plan);
	if (result == PCM_X_QUEUE_OK && plan.candidate != NULL) {
		if (*projected_wake_candidate_out) {
			result = PCM_X_QUEUE_CORRUPT;
			goto preflight_done;
		}
		*projected_wake_out = plan.candidate->identity;
		*projected_wake_candidate_out = true;
	}

preflight_done:
	LWLockRelease(&header->local_locks[partition].lock);
	return result;
}


static PcmXQueueResult
pcm_x_local_wake_project(PcmXLocalWakeBatch *wake_batch, const PcmXWaitIdentity *identity,
						 Size *projected_count)
{
	Size i;

	if (projected_count == NULL || identity == NULL || !pcm_x_wait_identity_valid(identity))
		return PCM_X_QUEUE_CORRUPT;
	if (wake_batch == NULL)
		return PCM_X_QUEUE_OK;
	for (i = 0; i < *projected_count; i++) {
		if (pcm_x_wait_identity_equal(&wake_batch->items[i], identity))
			return PCM_X_QUEUE_CORRUPT;
	}
	if (*projected_count >= wake_batch->capacity)
		return PCM_X_QUEUE_NO_CAPACITY;
	wake_batch->items[*projected_count] = *identity;
	(*projected_count)++;
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
pcm_x_local_wake_actual_match(const PcmXLocalWakeBatch *wake_batch,
							  const PcmXWaitIdentity *identity, Size projected_count,
							  Size *actual_count)
{
	if (actual_count == NULL || identity == NULL || !pcm_x_wait_identity_valid(identity))
		return PCM_X_QUEUE_CORRUPT;
	if (wake_batch == NULL)
		return PCM_X_QUEUE_OK;
	if (*actual_count >= projected_count
		|| !pcm_x_wait_identity_equal(&wake_batch->items[*actual_count], identity))
		return PCM_X_QUEUE_CORRUPT;
	(*actual_count)++;
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_pcm_x_local_retire_up_to_collect_exact(const PcmXRetirePayload *request,
											   int32 authenticated_master_node,
											   uint64 authenticated_master_session,
											   PcmXLocalWakeBatch *wake_batch)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXRuntimeSnapshot runtime;
	PcmXPeerFrontier *frontier;
	PcmXAllocatorView view;
	PcmXTicketRef candidate_ref;
	PcmXTicketRef writer_candidate_ref;
	PcmXLocalHandle handle;
	PcmXLocalHandle promoted;
	PcmXWaitIdentity holder_wake;
	PcmXWaitIdentity writer_wake;
	PcmXQueueResult result = PCM_X_QUEUE_OK;
	Size actual_wake_count = 0;
	Size i;
	Size projected_wake_count = 0;
	uint32 expected_retire_gate;
	bool candidate;
	bool contains_watermark;
	bool holder_candidate;
	bool holder_wake_candidate;
	bool writer_candidate;
	bool writer_wake_candidate;
	bool found_exact = false;
	bool fail_closed = false;

	if (wake_batch != NULL)
		wake_batch->count = 0;
	if (header == NULL || request == NULL || request->master_session_incarnation == 0
		|| request->retire_through_ticket_id == 0 || request->sender_node < 0
		|| request->sender_node >= PCM_X_PROTOCOL_NODE_LIMIT || request->flags != 0
		|| authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0
		|| (wake_batch != NULL && wake_batch->capacity != 0 && wake_batch->items == NULL))
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return PCM_X_QUEUE_NOT_READY;

	/* Claim one exact monotonic retirement episode before any local evidence is
	 * removed.  A stranded nonzero marker is intentionally fail-closed evidence. */
	LWLockAcquire(&header->allocator_lock.lock, LW_EXCLUSIVE);
	frontier = &header->peer_frontiers[authenticated_master_node];
	if (request->master_session_incarnation != authenticated_master_session
		|| frontier->cluster_epoch != request->cluster_epoch
		|| frontier->sender_session_incarnation != authenticated_master_session) {
		result = PCM_X_QUEUE_STALE;
		goto claim_done;
	}
	if (request->retire_through_ticket_id <= frontier->local_retired_ticket_id) {
		result = PCM_X_QUEUE_DUPLICATE;
		goto claim_done;
	}
	result = pcm_x_local_retire_episode_state_locked(header);
	if (result != PCM_X_QUEUE_OK) {
		fail_closed = result == PCM_X_QUEUE_CORRUPT;
		goto claim_done;
	}
	expected_retire_gate = 0;
	if (!pg_atomic_compare_exchange_u32(&header->local_retire_gate, &expected_retire_gate,
										(uint32)authenticated_master_node + 1)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto claim_done;
	}
	pg_write_barrier();
	frontier->local_retire_in_progress_ticket_id = request->retire_through_ticket_id;
	result = pcm_x_local_admission_handoffs_quiescent_locked(header);
	if (result != PCM_X_QUEUE_OK) {
		if (result == PCM_X_QUEUE_BUSY) {
			frontier->local_retire_in_progress_ticket_id = 0;
			pg_write_barrier();
			pg_atomic_write_u32(&header->local_retire_gate, 0);
		} else
			fail_closed = true;
		goto claim_done;
	}
	result = PCM_X_QUEUE_OK;

claim_done:
	LWLockRelease(&header->allocator_lock.lock);
	if (result != PCM_X_QUEUE_OK) {
		if (fail_closed)
			pcm_x_runtime_fail_closed();
		return result;
	}
	if (!pcm_x_allocator_view(PCM_X_ALLOC_LOCAL_TAG, &view)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto retire_failed;
	}

	/* First pass is read-only: an undrained lower/equal ticket must not cause
	 * partial reclamation of any older terminal evidence. */
	for (i = 0; i < view.capacity; i++) {
		result = pcm_x_local_retire_candidate_at(
			i, request, authenticated_master_node, authenticated_master_session, &candidate_ref,
			&writer_candidate_ref, &writer_candidate, &holder_candidate, &candidate,
			&contains_watermark, &holder_wake, &holder_wake_candidate);
		if (result != PCM_X_QUEUE_OK)
			goto retire_preflight_failed;
		if (holder_wake_candidate) {
			result = pcm_x_local_wake_project(wake_batch, &holder_wake, &projected_wake_count);
			if (result != PCM_X_QUEUE_OK)
				goto retire_preflight_failed;
		}
		if (writer_candidate) {
			result = pcm_x_local_terminal_handle_lookup(&writer_candidate_ref.identity, &handle);
			if (result == PCM_X_QUEUE_OK)
				result = pcm_x_local_final_member_round_preflight_exact(&handle, &writer_wake,
																		&writer_wake_candidate);
			if (result != PCM_X_QUEUE_OK)
				goto retire_preflight_failed;
			if (writer_wake_candidate) {
				result = pcm_x_local_wake_project(wake_batch, &writer_wake, &projected_wake_count);
				if (result != PCM_X_QUEUE_OK)
					goto retire_preflight_failed;
			}
		}
		if (contains_watermark)
			found_exact = true;
	}
	if (!found_exact) {
		result = PCM_X_QUEUE_STALE;
		goto retire_preflight_failed;
	}

	/* Second pass revalidates each exact terminal membership while detaching.
	 * Any post-first-detach surprise retains the in-progress marker and blocks
	 * the runtime; retrying a partially applied watermark would be ambiguous. */
	for (i = 0; i < view.capacity; i++) {
		do {
			result = pcm_x_local_retire_candidate_at(
				i, request, authenticated_master_node, authenticated_master_session, &candidate_ref,
				&writer_candidate_ref, &writer_candidate, &holder_candidate, &candidate,
				&contains_watermark, &holder_wake, &holder_wake_candidate);
			if (result != PCM_X_QUEUE_OK) {
				fail_closed = true;
				result = PCM_X_QUEUE_CORRUPT;
				goto retire_failed;
			}
			if (!candidate)
				break;
			memset(&promoted, 0, sizeof(promoted));
			memset(&writer_wake, 0, sizeof(writer_wake));
			writer_wake_candidate = false;
			if (holder_candidate)
				result = pcm_x_local_holder_detach_terminal_exact(
					&candidate_ref, authenticated_master_node, authenticated_master_session,
					&writer_wake, &writer_wake_candidate);
			else {
				result = pcm_x_local_terminal_handle_lookup(&candidate_ref.identity, &handle);
				if (result == PCM_X_QUEUE_OK)
					result = pcm_x_local_detach_terminal_common(&handle, true, &promoted);
				if (result == PCM_X_QUEUE_OK && pcm_x_wait_identity_valid(&promoted.identity)) {
					writer_wake = promoted.identity;
					writer_wake_candidate = true;
				}
			}
			if (result != PCM_X_QUEUE_OK) {
				fail_closed = true;
				result = PCM_X_QUEUE_CORRUPT;
				goto retire_failed;
			}
			if (writer_wake_candidate) {
				result = pcm_x_local_wake_actual_match(wake_batch, &writer_wake,
													   projected_wake_count, &actual_wake_count);
				if (result != PCM_X_QUEUE_OK) {
					fail_closed = true;
					result = PCM_X_QUEUE_CORRUPT;
					goto retire_failed;
				}
			}
		} while (candidate);
	}
	if (wake_batch != NULL && actual_wake_count != projected_wake_count) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
		goto retire_failed;
	}

	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, NULL);
	frontier = &header->peer_frontiers[authenticated_master_node];
	if (pcm_x_local_retire_episode_state_locked(header) != PCM_X_QUEUE_BUSY
		|| frontier->cluster_epoch != request->cluster_epoch
		|| frontier->sender_session_incarnation != authenticated_master_session
		|| frontier->local_retire_in_progress_ticket_id != request->retire_through_ticket_id
		|| frontier->local_retired_ticket_id >= request->retire_through_ticket_id
		|| pg_atomic_read_u32(&header->local_retire_gate) != (uint32)authenticated_master_node + 1
		|| !pcm_x_runtime_token_exact(&runtime, 0)) {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	} else {
		frontier->local_retired_ticket_id = request->retire_through_ticket_id;
		pg_write_barrier();
		frontier->local_retire_in_progress_ticket_id = 0;
		pg_write_barrier();
		pg_atomic_write_u32(&header->local_retire_gate, 0);
		result = PCM_X_QUEUE_OK;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	if (result == PCM_X_QUEUE_OK && wake_batch != NULL) {
		pg_write_barrier();
		wake_batch->count = projected_wake_count;
	}
	return result;

retire_preflight_failed:
	/* No tag was mutated, so a retryable/stale preflight can release the claim.
	 * Structural corruption retains both exact episode witnesses. */
	if (result == PCM_X_QUEUE_CORRUPT) {
		fail_closed = true;
		goto retire_failed;
	}
	pcm_x_local_gate_acquire_guarded(&header->allocator_lock.lock, LW_EXCLUSIVE, NULL);
	frontier = &header->peer_frontiers[authenticated_master_node];
	if (pcm_x_local_retire_episode_state_locked(header) == PCM_X_QUEUE_BUSY
		&& frontier->local_retire_in_progress_ticket_id == request->retire_through_ticket_id
		&& pg_atomic_read_u32(&header->local_retire_gate)
			   == (uint32)authenticated_master_node + 1) {
		frontier->local_retire_in_progress_ticket_id = 0;
		pg_write_barrier();
		pg_atomic_write_u32(&header->local_retire_gate, 0);
	} else {
		result = PCM_X_QUEUE_CORRUPT;
		fail_closed = true;
	}
	LWLockRelease(&header->allocator_lock.lock);
	if (result == PCM_X_QUEUE_CORRUPT)
		fail_closed = true;
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;

retire_failed:
	/* Keep the in-progress marker as the durable ambiguity witness. */
	if (fail_closed)
		pcm_x_runtime_fail_closed();
	return result;
}


PcmXQueueResult
cluster_pcm_x_local_retire_up_to_exact(const PcmXRetirePayload *request,
									   int32 authenticated_master_node,
									   uint64 authenticated_master_session)
{
	return cluster_pcm_x_local_retire_up_to_collect_exact(request, authenticated_master_node,
														  authenticated_master_session, NULL);
}


/* Validate signed runtime inputs before converting them to Size. */
static void
pcm_x_compute_runtime_layout(PcmXShmemLayout *layout)
{
	if (MaxBackends <= 0 || NBuffers <= 0 || cluster_lmd_max_wait_edges <= 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid PCM-X shared memory capacity inputs"),
						errhint("Use positive MaxBackends, shared_buffers, and "
								"cluster.lmd_max_wait_edges values.")));

	cluster_pcm_x_layout_compute((Size)MaxBackends, (Size)NUM_AUXILIARY_PROCS, (Size)NBuffers,
								 (Size)cluster_lmd_max_wait_edges, layout);
}


/*
 * cluster_pcm_x_convert_shmem_size -- Return the checked runtime region size.
 *
 * Inputs:
 *	None.  Capacity inputs come from postmaster configuration.
 *
 * Returns:
 *	The exact byte size required by the PCM-X conversion region.
 *
 * Side Effects:
 *	Raises ERROR for invalid capacity inputs or arithmetic overflow.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
Size
cluster_pcm_x_convert_shmem_size(void)
{
	PcmXShmemLayout layout;

	pcm_x_compute_runtime_layout(&layout);
	return layout.total_size;
}


/* Initialize one index-based free list without storing process pointers. */
static void
pcm_x_init_free_list(char *base, Size slots_offset, Size slot_size, Size first_slot_index,
					 Size capacity, PcmXAllocatorState *allocator)
{
	Size i;

	allocator->free_head = capacity == 0 ? PCM_X_INVALID_SLOT_INDEX : first_slot_index;
	allocator->used = 0;
	allocator->high_water = 0;
	allocator->generation_exhausted = 0;
	for (i = 0; i < capacity;) {
		Size local_next = add_size(i, 1);
		Size next = local_next < capacity ? add_size(first_slot_index, local_next)
										  : PCM_X_INVALID_SLOT_INDEX;
		Size offset = add_size(slots_offset, mul_size(i, slot_size));
		PcmXSlotHeader *slot = (PcmXSlotHeader *)(base + offset);

		slot->next_free = next;
		pg_atomic_init_u32(&slot->generation_change_seq, 0);
		slot->slot_generation_lo = 0;
		slot->slot_generation_hi = 0;
		pg_atomic_init_u32(&slot->state_flags, PCM_X_SLOT_FREE);
		i = local_next;
	}
}


/* Build all pool allocators, keeping membership wait/holder lists disjoint. */
static void
pcm_x_init_allocators(PcmXShmemHeader *header)
{
	PcmXShmemLayout *layout = &header->layout;
	char *base = (char *)header;

	pcm_x_init_free_list(base, layout->pools[PCM_X_POOL_MASTER_TAG].slots_offset,
						 layout->pools[PCM_X_POOL_MASTER_TAG].slot_size, 0,
						 layout->pools[PCM_X_POOL_MASTER_TAG].capacity,
						 &header->allocator[PCM_X_ALLOC_MASTER_TAG]);
	pcm_x_init_free_list(base, layout->pools[PCM_X_POOL_MASTER_TICKET].slots_offset,
						 layout->pools[PCM_X_POOL_MASTER_TICKET].slot_size, 0,
						 layout->pools[PCM_X_POOL_MASTER_TICKET].capacity,
						 &header->allocator[PCM_X_ALLOC_MASTER_TICKET]);
	pcm_x_init_free_list(base, layout->pools[PCM_X_POOL_BLOCKER].slots_offset,
						 layout->pools[PCM_X_POOL_BLOCKER].slot_size, 0,
						 layout->pools[PCM_X_POOL_BLOCKER].capacity,
						 &header->allocator[PCM_X_ALLOC_BLOCKER]);
	pcm_x_init_free_list(base, layout->pools[PCM_X_POOL_LOCAL_TAG].slots_offset,
						 layout->pools[PCM_X_POOL_LOCAL_TAG].slot_size, 0,
						 layout->pools[PCM_X_POOL_LOCAL_TAG].capacity,
						 &header->allocator[PCM_X_ALLOC_LOCAL_TAG]);
	pcm_x_init_free_list(base, layout->local_wait.slots_offset, sizeof(PcmXLocalMembershipSlot),
						 layout->local_wait.first_slot_index, layout->local_wait.capacity,
						 &header->allocator[PCM_X_ALLOC_LOCAL_WAIT]);
	pcm_x_init_free_list(base, layout->local_holder.slots_offset, sizeof(PcmXLocalMembershipSlot),
						 layout->local_holder.first_slot_index, layout->local_holder.capacity,
						 &header->allocator[PCM_X_ALLOC_LOCAL_HOLDER]);
}


/*
 * cluster_pcm_x_convert_shmem_init -- Create or attach the conversion region.
 *
 * Inputs:
 *	None.  The expected layout comes from postmaster configuration.
 *
 * Returns:
 *	Nothing.  On success, ClusterPcmXConvertShmem is published locally.
 *
 * Side Effects:
 *	Creates and initializes the shared region on first use.  An attach
 *	preserves mutable allocator state.  A ProcGlobal or layout mismatch is
 *	fatal, and the process-local pointer remains unpublished.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_pcm_x_convert_shmem_init(void)
{
	PcmXShmemLayout expected;
	PcmXShmemHeader *header;
	PcmXAttachResult attach_result;
	bool found;

	ClusterPcmXConvertShmem = NULL;
	pcm_x_compute_runtime_layout(&expected);
	if (ProcGlobal == NULL || (Size)ProcGlobal->allProcCount != expected.process_capacity)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("PCM-X process capacity does not match ProcGlobal"),
				 errhint("Restart the postmaster with a consistent backend process layout.")));
	Assert((Size)ProcGlobal->allProcCount == expected.process_capacity);

	header = ShmemInitStruct(PCM_X_SHMEM_REGION_NAME, expected.total_size, &found);
	if (found) {
		attach_result = cluster_pcm_x_validate_attach(header, &expected);
		if (attach_result != PCM_X_ATTACH_OK)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("PCM-X shared memory layout is incompatible (validation result %d)",
							(int)attach_result),
					 errhint("Restart the postmaster with one binary and one PCM-X capacity "
							 "configuration.")));
	} else {
		int i;

		memset(header, 0, expected.total_size);
		header->layout = expected;
		header->next_ticket_id = 1;
		header->next_image_id = 1;
		header->master_session_incarnation = 0;
		for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
			header->peer_frontiers[i].next_expected_prehandle_sequence = 1;
			pg_atomic_init_u32(&header->outbound_targets[i].mint_gate, 0);
		}
		pg_atomic_init_u32(&header->runtime_gate,
						   pcm_x_runtime_gate_pack(0, PCM_X_RUNTIME_RECOVERY_BLOCKED));
		pg_atomic_init_u32(&header->activation_retry_generation, 0);
		pg_atomic_init_u32(&header->local_retire_gate, 0);
		pcm_x_init_stats(&header->stats);
		pcm_x_init_allocators(header);
		LWLockInitialize(&header->allocator_lock.lock, LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR);
		for (i = 0; i < PCM_X_LOCK_PARTITIONS; i++) {
			LWLockInitialize(&header->master_locks[i].lock, LWTRANCHE_CLUSTER_PCM_X_MASTER);
			LWLockInitialize(&header->local_locks[i].lock, LWTRANCHE_CLUSTER_PCM_X_LOCAL);
		}
	}
	ClusterPcmXConvertShmem = header;
}


static const ClusterShmemRegion pcm_x_convert_region = {
	.name = PCM_X_SHMEM_REGION_NAME,
	.size_fn = cluster_pcm_x_convert_shmem_size,
	.init_fn = cluster_pcm_x_convert_shmem_init,
	.lwlock_count = PCM_X_LWLOCK_COUNT,
	.owner_subsys = "pcm_x_convert",
	.reserved_flags = 0,
};


/*
 * cluster_pcm_x_convert_shmem_register -- Register the one external region.
 *
 * Inputs:
 *	None.
 *
 * Returns:
 *	Nothing.
 *
 * Side Effects:
 *	Adds one immutable descriptor to the cluster shared-memory registry.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_pcm_x_convert_shmem_register(void)
{
	cluster_shmem_register_region(&pcm_x_convert_region);
}
