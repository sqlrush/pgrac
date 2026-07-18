/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_x_convert.c
 *	  PCM-X wire ABI and five-pool external shared-memory substrate tests.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pcm_x_convert.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It links cluster_pcm_x_convert.o
 *	  standalone and supplies checked-arithmetic, shmem, and error stubs.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <pthread.h>
#include <setjmp.h>

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_shmem.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
FlushErrorState(void)
{}

int MaxBackends = 4;
int NBuffers = 8;
int cluster_lmd_max_wait_edges = 3;
int cluster_node_id = 0;

static BufferDescPadded fake_buffer_descriptors[8];
BufferDescPadded *BufferDescriptors = fake_buffer_descriptors;

static PROC_HDR fake_proc_global;
PROC_HDR *ProcGlobal = &fake_proc_global;

static union {
	uint64 force_align;
	char data[4194304];
} fake_shmem;
static bool fake_shmem_found;
static Size fake_shmem_size;
static const ClusterShmemRegion *registered_region;
static int registered_region_count;

static sigjmp_buf test_jump;
static bool arithmetic_jump_armed;
static bool ereport_jump_armed;
static int arithmetic_overflow_count;
static int ereport_count;
static Size arithmetic_overflow_left;
static Size arithmetic_overflow_right;
static int lwlock_init_count;
static int allocator_lock_init_count;
static int master_lock_init_count;
static int local_lock_init_count;
static LWLock *held_lwlocks[16];
static LWLockMode held_lwlock_modes[16];
static int held_lwlock_count;
static int max_held_lwlock_count;
static int allocator_lock_acquire_count;
static int allocator_lock_shared_count;
static int allocator_lock_exclusive_count;
static int domain_lock_acquire_count;
static int lwlock_release_count;
static int coalesce_interlock_mode;
static PcmXMasterTagSlot *coalesce_interlock_tag;
static PcmXMasterTicketSlot *coalesce_interlock_ticket;
static bool detach_interlock_armed;
static PcmXMasterTagSlot *detach_interlock_tag;
static PcmXMasterTicketSlot *detach_interlock_ticket;
static bool grant_interlock_armed;
static LWLock *grant_interlock_lock;
static PcmXMasterTicketSlot *grant_interlock_ticket;
static uint64 grant_interlock_generation;
static LWLock *local_generation_interlock_lock;
static PcmXSlotHeader *local_generation_interlock_slot;
static LWLock *holder_abort_interlock_lock;
static PcmXLocalMembershipSlot *holder_abort_interlock_target;
static int holder_abort_interlock_match;
static PcmXLocalTagSlot *local_retire_release_interlock_tag;
static PcmXPeerFrontier *local_retire_release_interlock_frontier;
static uint64 local_retire_release_interlock_ticket_id;
static uint32 local_retire_release_interlock_gate_owner;
static bool local_retire_release_interlock_observed_gate;
static bool local_retire_release_interlock_claimed;
static PcmXLocalTagSlot *local_abort_acquire_interlock_tag;
static PcmXPeerFrontier *local_abort_acquire_interlock_frontier;
static uint64 local_abort_acquire_interlock_ticket_id;
static uint32 local_abort_acquire_interlock_gate_owner;
static int local_abort_acquire_interlock_match;
static bool local_abort_acquire_interlock_observed_gate;
static bool local_abort_acquire_interlock_claimed;
static LWLock *local_transfer_peer_drift_lock;
static PcmXPeerFrontier *local_transfer_peer_drift_frontier;
static bool staged_prehandle_insert_exists_armed;
static PcmXPrehandleKey staged_prehandle_insert_exists_key;
static bool local_rekey_insert_failure_armed;
static int local_rekey_insert_failure_match;
static int local_rekey_insert_failure_phase;
static PcmXLocalMembershipSlot *local_rekey_insert_failure_member;
static PcmXWaitIdentity local_rekey_insert_failure_old_identity;
static PcmXWaitIdentity local_rekey_insert_failure_target_identity;
static PcmXDirectoryEntry *local_rekey_insert_failure_poisoned_entry;
static bool local_rekey_insert_failure_observed_old_identity;
static LWLock *lwlock_acquire_error_lock;
static LWLockMode lwlock_acquire_error_mode;
static int lwlock_acquire_error_match;
static bool iterating_held_lwlocks;
static int lock_acquire_during_iteration_count;

static void maybe_publish_staged_prehandle_insert_exists(void);
static void maybe_inject_local_rekey_insert_failure(void);
static uint64 test_slot_generation(PcmXSlotHeader *slot);
static uint32 test_slot_state(PcmXSlotHeader *slot);
static void test_set_slot_state(PcmXSlotHeader *slot, uint32 state);
static uint32 test_slot_flags(PcmXSlotHeader *slot);
static bool ticket_refs_equal(const PcmXTicketRef *left, const PcmXTicketRef *right);
static void init_active_pcm_x(uint64 master_session_incarnation);
static void bind_local_master(int32 master_node, uint64 cluster_epoch, uint64 master_session);

static void
test_image_id_domain_is_canonical_and_bounded(void)
{
	uint64 id0;
	uint64 id31;
	uint64 idmax;
	uint64 sequence;
	int32 master_node;

	UT_ASSERT(cluster_pcm_x_image_id_encode(0, 1, &id0));
	UT_ASSERT(cluster_pcm_x_image_id_encode(31, 9, &id31));
	UT_ASSERT(cluster_pcm_x_image_id_encode(31, PCM_X_IMAGE_ID_SEQ_MASK, &idmax));
	UT_ASSERT_EQ(id0, UINT64CONST(0xc000000000000001));
	UT_ASSERT_EQ(id31, UINT64CONST(0xdf00000000000009));
	UT_ASSERT_EQ(idmax, UINT64CONST(0xdfffffffffffffff));

	UT_ASSERT(cluster_pcm_x_image_id_decode(id0, &master_node, &sequence));
	UT_ASSERT_EQ(master_node, 0);
	UT_ASSERT_EQ(sequence, 1);
	UT_ASSERT(cluster_pcm_x_image_id_decode(id31, &master_node, &sequence));
	UT_ASSERT_EQ(master_node, 31);
	UT_ASSERT_EQ(sequence, 9);
	UT_ASSERT(cluster_pcm_x_image_id_decode(idmax, &master_node, &sequence));
	UT_ASSERT_EQ(master_node, 31);
	UT_ASSERT_EQ(sequence, PCM_X_IMAGE_ID_SEQ_MASK);

	UT_ASSERT(!cluster_pcm_x_image_id_encode(-1, 1, &id0));
	UT_ASSERT(!cluster_pcm_x_image_id_encode(PCM_X_PROTOCOL_NODE_LIMIT, 1, &id0));
	UT_ASSERT(!cluster_pcm_x_image_id_encode(0, 0, &id0));
	UT_ASSERT(!cluster_pcm_x_image_id_encode(0, PCM_X_IMAGE_ID_SEQ_MASK + 1, &id0));
	UT_ASSERT(!cluster_pcm_x_image_id_encode(0, 1, NULL));
	UT_ASSERT(
		!cluster_pcm_x_image_id_decode(UINT64CONST(0xe000000000000001), &master_node, &sequence));
	UT_ASSERT(
		!cluster_pcm_x_image_id_decode(UINT64CONST(0xf000000000000001), &master_node, &sequence));
	UT_ASSERT(
		!cluster_pcm_x_image_id_decode(UINT64CONST(0x8000000000000001), &master_node, &sequence));
	UT_ASSERT(!cluster_pcm_x_image_id_decode(PCM_X_IMAGE_ID_DOMAIN, &master_node, &sequence));
}


void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

Size
add_size(Size left, Size right)
{
	if (left > SIZE_MAX - right) {
		arithmetic_overflow_count++;
		if (arithmetic_jump_armed)
			siglongjmp(test_jump, 1);
		abort();
	}
	return left + right;
}

Size
mul_size(Size left, Size right)
{
	if (left != 0 && right > SIZE_MAX / left) {
		arithmetic_overflow_count++;
		arithmetic_overflow_left = left;
		arithmetic_overflow_right = right;
		if (arithmetic_jump_armed)
			siglongjmp(test_jump, 1);
		abort();
	}
	return left * right;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found_ptr)
{
	UT_ASSERT_STR_EQ(name, PCM_X_SHMEM_REGION_NAME);
	UT_ASSERT(size <= sizeof(fake_shmem.data));
	fake_shmem_size = size;
	*found_ptr = fake_shmem_found;
	fake_shmem_found = true;
	return fake_shmem.data;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	registered_region = region;
	registered_region_count++;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	ereport_count++;
	if (ereport_jump_armed)
		siglongjmp(test_jump, 1);
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	lock->tranche = (uint16)tranche_id;
	lwlock_init_count++;
	if (tranche_id == LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR)
		allocator_lock_init_count++;
	else if (tranche_id == LWTRANCHE_CLUSTER_PCM_X_MASTER)
		master_lock_init_count++;
	else if (tranche_id == LWTRANCHE_CLUSTER_PCM_X_LOCAL)
		local_lock_init_count++;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT_NOT_NULL(lock);
	if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR && mode == LW_EXCLUSIVE
		&& local_abort_acquire_interlock_tag != NULL
		&& --local_abort_acquire_interlock_match == 0) {
		uint32 flags = test_slot_flags(&local_abort_acquire_interlock_tag->slot);

		UT_ASSERT_NOT_NULL(local_abort_acquire_interlock_frontier);
		local_abort_acquire_interlock_observed_gate
			= (flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0;
		if (!local_abort_acquire_interlock_observed_gate) {
			pg_atomic_write_u32(&ClusterPcmXConvertShmem->local_retire_gate,
								local_abort_acquire_interlock_gate_owner);
			local_abort_acquire_interlock_frontier->local_retire_in_progress_ticket_id
				= local_abort_acquire_interlock_ticket_id;
			local_abort_acquire_interlock_claimed = true;
		}
		local_abort_acquire_interlock_tag = NULL;
	}
	if (iterating_held_lwlocks)
		lock_acquire_during_iteration_count++;
	if (lock == lwlock_acquire_error_lock && mode == lwlock_acquire_error_mode
		&& --lwlock_acquire_error_match == 0) {
		lwlock_acquire_error_lock = NULL;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	UT_ASSERT(held_lwlock_count < (int)lengthof(held_lwlocks));
	held_lwlocks[held_lwlock_count] = lock;
	held_lwlock_modes[held_lwlock_count] = mode;
	held_lwlock_count++;
	if (held_lwlock_count > max_held_lwlock_count)
		max_held_lwlock_count = held_lwlock_count;
	if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR) {
		allocator_lock_acquire_count++;
		if (mode == LW_SHARED)
			allocator_lock_shared_count++;
		else if (mode == LW_EXCLUSIVE)
			allocator_lock_exclusive_count++;
	} else if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_MASTER
			   || lock->tranche == LWTRANCHE_CLUSTER_PCM_X_LOCAL) {
		domain_lock_acquire_count++;
		if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_MASTER && coalesce_interlock_mode != 0) {
			int mode = coalesce_interlock_mode;

			coalesce_interlock_mode = 0;
			UT_ASSERT_NOT_NULL(coalesce_interlock_tag);
			UT_ASSERT_NOT_NULL(coalesce_interlock_ticket);
			coalesce_interlock_tag->head_index = PCM_X_INVALID_SLOT_INDEX;
			coalesce_interlock_tag->tail_index = PCM_X_INVALID_SLOT_INDEX;
			coalesce_interlock_tag->active_index = PCM_X_INVALID_SLOT_INDEX;
			coalesce_interlock_ticket->next_index = PCM_X_INVALID_SLOT_INDEX;
			coalesce_interlock_ticket->prev_index = PCM_X_INVALID_SLOT_INDEX;
			test_set_slot_state(&coalesce_interlock_ticket->slot, PCM_XT_CANCELLED);
			pg_atomic_write_u32(&coalesce_interlock_tag->queued_node_bitmap,
								mode == 1 ? 0 : UINT32_C(1));
			pg_atomic_write_u32(&coalesce_interlock_tag->admission_gate, mode == 2 ? 1 : 0);
		}
		if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_MASTER && detach_interlock_armed) {
			detach_interlock_armed = false;
			UT_ASSERT_NOT_NULL(detach_interlock_tag);
			UT_ASSERT_NOT_NULL(detach_interlock_ticket);
			UT_ASSERT_EQ(detach_interlock_tag->outstanding_ticket_count, 1);
			detach_interlock_tag->outstanding_ticket_count = 0;
			pg_atomic_write_u32(&detach_interlock_tag->admission_gate, 2);
			test_set_slot_state(&detach_interlock_tag->slot, PCM_X_TAG_DETACHING);
			test_set_slot_state(&detach_interlock_ticket->slot, PCM_XT_DETACHING);
		}
		if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_MASTER && grant_interlock_armed
			&& lock == grant_interlock_lock && mode == LW_EXCLUSIVE) {
			grant_interlock_armed = false;
			UT_ASSERT_NOT_NULL(grant_interlock_ticket);
			grant_interlock_ticket->ref.grant_generation = grant_interlock_generation;
		}
		if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_LOCAL
			&& lock == local_generation_interlock_lock && mode == LW_SHARED
			&& local_generation_interlock_slot != NULL) {
			PcmXSlotHeader *slot = local_generation_interlock_slot;

			local_generation_interlock_slot = NULL;
			pg_atomic_write_u32(&slot->generation_change_seq,
								pg_atomic_read_u32(&slot->generation_change_seq) | 1U);
		}
		if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_LOCAL && lock == local_transfer_peer_drift_lock
			&& mode == LW_EXCLUSIVE && local_transfer_peer_drift_frontier != NULL) {
			local_transfer_peer_drift_lock = NULL;
			local_transfer_peer_drift_frontier->sender_session_incarnation++;
			local_transfer_peer_drift_frontier = NULL;
		}
	}
	return false;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(held_lwlock_count > 0);
	UT_ASSERT_EQ(held_lwlocks[held_lwlock_count - 1], lock);
	held_lwlock_count--;
	lwlock_release_count++;
	if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR
		&& local_retire_release_interlock_tag != NULL) {
		uint32 flags = test_slot_flags(&local_retire_release_interlock_tag->slot);

		UT_ASSERT_NOT_NULL(local_retire_release_interlock_frontier);
		local_retire_release_interlock_observed_gate
			= (flags & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0;
		if (!local_retire_release_interlock_observed_gate) {
			pg_atomic_write_u32(&ClusterPcmXConvertShmem->local_retire_gate,
								local_retire_release_interlock_gate_owner);
			local_retire_release_interlock_frontier->local_retire_in_progress_ticket_id
				= local_retire_release_interlock_ticket_id;
			local_retire_release_interlock_claimed = true;
		}
		local_retire_release_interlock_tag = NULL;
	}
}

bool
LWLockHeldByMe(LWLock *lock)
{
	int i;

	if (lock == holder_abort_interlock_lock && holder_abort_interlock_target != NULL
		&& --holder_abort_interlock_match == 0) {
		PcmXLocalMembershipSlot *target = holder_abort_interlock_target;

		holder_abort_interlock_lock = NULL;
		holder_abort_interlock_target = NULL;
		test_set_slot_state(&target->slot, PCM_XL_HOLDER_ACTIVE);
	}
	for (i = 0; i < held_lwlock_count; i++)
		if (held_lwlocks[i] == lock)
			return true;
	return false;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	int i;

	for (i = 0; i < held_lwlock_count; i++) {
		if (held_lwlocks[i] == lock && held_lwlock_modes[i] == mode) {
			if (lock->tranche == LWTRANCHE_CLUSTER_PCM_X_ALLOCATOR && mode == LW_EXCLUSIVE) {
				maybe_publish_staged_prehandle_insert_exists();
				maybe_inject_local_rekey_insert_failure();
			}
			return true;
		}
	}
	return false;
}

bool
LWLockAnyHeldByMe(LWLock *lock, int nlocks, size_t stride)
{
	char *begin = (char *)lock;
	char *end = begin + nlocks * stride;
	int i;

	for (i = 0; i < held_lwlock_count; i++) {
		char *held = (char *)held_lwlocks[i];

		if (held >= begin && held < end && (held - begin) % stride == 0)
			return true;
	}
	return false;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	int i;

	UT_ASSERT(!iterating_held_lwlocks);
	iterating_held_lwlocks = true;
	for (i = 0; i < held_lwlock_count; i++)
		callback(held_lwlocks[i], held_lwlock_modes[i], context);
	iterating_held_lwlocks = false;
}


#define UT_EXPECT_ARITHMETIC_OVERFLOW(statement)                                                   \
	do {                                                                                           \
		if (sigsetjmp(test_jump, 1) == 0) {                                                        \
			arithmetic_jump_armed = true;                                                          \
			statement;                                                                             \
			arithmetic_jump_armed = false;                                                         \
			UT_ASSERT(false);                                                                      \
		} else {                                                                                   \
			arithmetic_jump_armed = false;                                                         \
			UT_ASSERT(arithmetic_overflow_count > 0);                                              \
		}                                                                                          \
	} while (0)

#define UT_EXPECT_EREPORT(statement)                                                               \
	do {                                                                                           \
		if (sigsetjmp(test_jump, 1) == 0) {                                                        \
			ereport_jump_armed = true;                                                             \
			statement;                                                                             \
			ereport_jump_armed = false;                                                            \
			UT_ASSERT(false);                                                                      \
		} else {                                                                                   \
			ereport_jump_armed = false;                                                            \
			UT_ASSERT(ereport_count > 0);                                                          \
		}                                                                                          \
	} while (0)

#define UT_EXPECT_PG_EXCEPTION(statement)                                                          \
	do {                                                                                           \
		volatile bool caught_ = false;                                                             \
		PG_TRY();                                                                                  \
		{                                                                                          \
			statement;                                                                             \
		}                                                                                          \
		PG_CATCH();                                                                                \
		{                                                                                          \
			caught_ = true;                                                                        \
		}                                                                                          \
		PG_END_TRY();                                                                              \
		UT_ASSERT(caught_);                                                                        \
	} while (0)


static void
reset_fake_shmem(void)
{
	int i;

	memset(&fake_shmem, 0, sizeof(fake_shmem));
	memset(&fake_proc_global, 0, sizeof(fake_proc_global));
	memset(fake_buffer_descriptors, 0, sizeof(fake_buffer_descriptors));
	for (i = 0; i < NBuffers; i++)
		fake_buffer_descriptors[i].bufferdesc.buf_id = i;
	fake_proc_global.allProcCount = (uint32)(MaxBackends + NUM_AUXILIARY_PROCS);
	fake_shmem_found = false;
	fake_shmem_size = 0;
	ClusterPcmXConvertShmem = NULL;
	registered_region = NULL;
	registered_region_count = 0;
	arithmetic_jump_armed = false;
	ereport_jump_armed = false;
	arithmetic_overflow_count = 0;
	ereport_count = 0;
	arithmetic_overflow_left = 0;
	arithmetic_overflow_right = 0;
	lwlock_init_count = 0;
	allocator_lock_init_count = 0;
	master_lock_init_count = 0;
	local_lock_init_count = 0;
	memset(held_lwlocks, 0, sizeof(held_lwlocks));
	memset(held_lwlock_modes, 0, sizeof(held_lwlock_modes));
	held_lwlock_count = 0;
	max_held_lwlock_count = 0;
	allocator_lock_acquire_count = 0;
	allocator_lock_shared_count = 0;
	allocator_lock_exclusive_count = 0;
	domain_lock_acquire_count = 0;
	lwlock_release_count = 0;
	coalesce_interlock_mode = 0;
	coalesce_interlock_tag = NULL;
	coalesce_interlock_ticket = NULL;
	detach_interlock_armed = false;
	detach_interlock_tag = NULL;
	detach_interlock_ticket = NULL;
	grant_interlock_armed = false;
	grant_interlock_lock = NULL;
	grant_interlock_ticket = NULL;
	grant_interlock_generation = 0;
	local_generation_interlock_lock = NULL;
	local_generation_interlock_slot = NULL;
	holder_abort_interlock_lock = NULL;
	holder_abort_interlock_target = NULL;
	holder_abort_interlock_match = 0;
	local_retire_release_interlock_tag = NULL;
	local_retire_release_interlock_frontier = NULL;
	local_retire_release_interlock_ticket_id = 0;
	local_retire_release_interlock_gate_owner = 0;
	local_retire_release_interlock_observed_gate = false;
	local_retire_release_interlock_claimed = false;
	local_abort_acquire_interlock_tag = NULL;
	local_abort_acquire_interlock_frontier = NULL;
	local_abort_acquire_interlock_ticket_id = 0;
	local_abort_acquire_interlock_gate_owner = 0;
	local_abort_acquire_interlock_match = 0;
	local_abort_acquire_interlock_observed_gate = false;
	local_abort_acquire_interlock_claimed = false;
	local_transfer_peer_drift_lock = NULL;
	local_transfer_peer_drift_frontier = NULL;
	cluster_node_id = 0;
	staged_prehandle_insert_exists_armed = false;
	memset(&staged_prehandle_insert_exists_key, 0, sizeof(staged_prehandle_insert_exists_key));
	local_rekey_insert_failure_armed = false;
	local_rekey_insert_failure_match = 0;
	local_rekey_insert_failure_phase = 0;
	local_rekey_insert_failure_member = NULL;
	memset(&local_rekey_insert_failure_old_identity, 0,
		   sizeof(local_rekey_insert_failure_old_identity));
	memset(&local_rekey_insert_failure_target_identity, 0,
		   sizeof(local_rekey_insert_failure_target_identity));
	local_rekey_insert_failure_poisoned_entry = NULL;
	local_rekey_insert_failure_observed_old_identity = false;
	lwlock_acquire_error_lock = NULL;
	lwlock_acquire_error_mode = LW_SHARED;
	lwlock_acquire_error_match = 0;
	iterating_held_lwlocks = false;
	lock_acquire_during_iteration_count = 0;
}

static void
arm_lwlock_acquire_error(LWLock *lock, LWLockMode mode, int match)
{
	UT_ASSERT_NOT_NULL(lock);
	UT_ASSERT(match > 0);
	lwlock_acquire_error_lock = lock;
	lwlock_acquire_error_mode = mode;
	lwlock_acquire_error_match = match;
}

static void
compute_runtime_layout(PcmXShmemLayout *layout)
{
	cluster_pcm_x_layout_compute((Size)MaxBackends, (Size)NUM_AUXILIARY_PROCS, (Size)NBuffers,
								 (Size)cluster_lmd_max_wait_edges, layout);
}

static PcmXLocalMembershipSlot *
membership_slots(PcmXShmemHeader *header)
{
	return (PcmXLocalMembershipSlot
				*)((char *)header + header->layout.pools[PCM_X_POOL_LOCAL_MEMBERSHIP].slots_offset);
}

static PcmXMasterTagSlot *
master_tag_slots(PcmXShmemHeader *header)
{
	return (PcmXMasterTagSlot *)((char *)header
								 + header->layout.pools[PCM_X_POOL_MASTER_TAG].slots_offset);
}

static PcmXMasterTicketSlot *
master_ticket_slots(PcmXShmemHeader *header)
{
	return (PcmXMasterTicketSlot *)((char *)header
									+ header->layout.pools[PCM_X_POOL_MASTER_TICKET].slots_offset);
}

static PcmXBlockerSlot *
blocker_slots(PcmXShmemHeader *header)
{
	return (PcmXBlockerSlot *)((char *)header
							   + header->layout.pools[PCM_X_POOL_BLOCKER].slots_offset);
}

static PcmXLocalTagSlot *
local_tag_slots(PcmXShmemHeader *header)
{
	return (PcmXLocalTagSlot *)((char *)header
								+ header->layout.pools[PCM_X_POOL_LOCAL_TAG].slots_offset);
}

static PcmXSlotHeader *
pool_slot_header(PcmXShmemHeader *header, PcmXPoolKind kind, Size index)
{
	const PcmXPoolLayout *pool = &header->layout.pools[kind];

	UT_ASSERT(index < pool->capacity);
	return (PcmXSlotHeader *)((char *)header + pool->slots_offset + index * pool->slot_size);
}

static PcmXDirectoryEntry *
directory_entries(PcmXShmemHeader *header, PcmXDirectoryKind kind, Size *capacity)
{
	Size offset;

	switch (kind) {
	case PCM_X_DIR_MASTER_TAG:
		offset = header->layout.pools[PCM_X_POOL_MASTER_TAG].directory_offset;
		*capacity = header->layout.pools[PCM_X_POOL_MASTER_TAG].directory_capacity;
		break;
	case PCM_X_DIR_MASTER_TICKET_PREHANDLE:
		offset = header->layout.master_ticket_directories.prehandle_offset;
		*capacity = header->layout.master_ticket_directories.prehandle_capacity;
		break;
	case PCM_X_DIR_MASTER_TICKET_HANDLE:
		offset = header->layout.master_ticket_directories.handle_offset;
		*capacity = header->layout.master_ticket_directories.handle_capacity;
		break;
	case PCM_X_DIR_MASTER_TICKET_RETIRE:
		offset = header->layout.master_ticket_directories.retire_offset;
		*capacity = header->layout.master_ticket_directories.retire_capacity;
		break;
	case PCM_X_DIR_LOCAL_TAG:
		offset = header->layout.pools[PCM_X_POOL_LOCAL_TAG].directory_offset;
		*capacity = header->layout.pools[PCM_X_POOL_LOCAL_TAG].directory_capacity;
		break;
	case PCM_X_DIR_LOCAL_WAIT:
		offset = header->layout.local_wait.directory_offset;
		*capacity = header->layout.local_wait.directory_capacity;
		break;
	case PCM_X_DIR_LOCAL_HOLDER:
		offset = header->layout.local_holder.directory_offset;
		*capacity = header->layout.local_holder.directory_capacity;
		break;
	default:
		UT_ASSERT(false);
		offset = 0;
		*capacity = 0;
		break;
	}
	return (PcmXDirectoryEntry *)((char *)header + offset);
}


/*
 * Inject the otherwise-impossible same-lock race: the admission preflight saw
 * NOT_FOUND, but its staged publish later sees an exact resident entry.
 */
static void
maybe_publish_staged_prehandle_insert_exists(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXDirectoryEntry *entries;
	PcmXMasterTicketSlot *tickets;
	uint64 key_hash;
	Size capacity;
	Size i;
	Size step;

	if (!staged_prehandle_insert_exists_armed || header == NULL)
		return;
	tickets = master_ticket_slots(header);
	for (i = 0; i < header->layout.pools[PCM_X_POOL_MASTER_TICKET].capacity; i++) {
		PcmXMasterTicketSlot *ticket = &tickets[i];

		if (test_slot_state(&ticket->slot) != PCM_X_SLOT_RESERVED_NONVISIBLE
			|| ticket->prehandle.sender_session_incarnation
				   != staged_prehandle_insert_exists_key.sender_session_incarnation
			|| ticket->prehandle.prehandle_sequence
				   != staged_prehandle_insert_exists_key.prehandle_sequence)
			continue;
		UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
												   &staged_prehandle_insert_exists_key, &key_hash));
		entries = directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &capacity);
		for (step = 0; step < capacity; step++) {
			PcmXDirectoryEntry *entry = &entries[((key_hash % capacity) + step) % capacity];

			if (entry->state != PCM_X_DIRECTORY_EMPTY)
				continue;
			entry->key_hash = key_hash;
			entry->slot_index = i;
			entry->slot_generation = test_slot_generation(&ticket->slot);
			entry->reserved = 0;
			pg_write_barrier();
			entry->state = PCM_X_DIRECTORY_OCCUPIED;
			staged_prehandle_insert_exists_armed = false;
			return;
		}
		return;
	}
}


/*
 * Fail the rekey target-key publication after the old key has been removed.
 * The next allocator-X assertion belongs to rollback publication and clears
 * the temporary invalid entry before the old key is scanned.  This also
 * records whether production kept the resident identity immutable throughout
 * the allocator-only phase.
 */
static void
maybe_inject_local_rekey_insert_failure(void)
{
	PcmXShmemHeader *header = ClusterPcmXConvertShmem;
	PcmXDirectoryEntry *entries;
	uint64 key_hash;
	Size capacity;
	Size step;

	if (!local_rekey_insert_failure_armed || header == NULL)
		return;
	if (local_rekey_insert_failure_phase == 1) {
		UT_ASSERT_NOT_NULL(local_rekey_insert_failure_poisoned_entry);
		local_rekey_insert_failure_poisoned_entry->key_hash = 0;
		local_rekey_insert_failure_poisoned_entry->slot_index = 0;
		local_rekey_insert_failure_poisoned_entry->slot_generation = 0;
		local_rekey_insert_failure_poisoned_entry->reserved = 0;
		pg_write_barrier();
		local_rekey_insert_failure_poisoned_entry->state = PCM_X_DIRECTORY_EMPTY;
		local_rekey_insert_failure_poisoned_entry = NULL;
		local_rekey_insert_failure_phase = 2;
		local_rekey_insert_failure_armed = false;
		return;
	}
	UT_ASSERT(local_rekey_insert_failure_match > 0);
	if (--local_rekey_insert_failure_match != 0)
		return;

	UT_ASSERT_NOT_NULL(local_rekey_insert_failure_member);
	local_rekey_insert_failure_observed_old_identity
		= memcmp(&local_rekey_insert_failure_member->identity,
				 &local_rekey_insert_failure_old_identity,
				 sizeof(local_rekey_insert_failure_old_identity))
		  == 0;
	UT_ASSERT(cluster_pcm_x_directory_key_hash(
		PCM_X_DIR_LOCAL_WAIT, &local_rekey_insert_failure_target_identity, &key_hash));
	entries = directory_entries(header, PCM_X_DIR_LOCAL_WAIT, &capacity);
	for (step = 0; step < capacity; step++) {
		PcmXDirectoryEntry *entry = &entries[((key_hash % capacity) + step) % capacity];

		if (entry->state != PCM_X_DIRECTORY_EMPTY)
			continue;
		entry->key_hash = key_hash;
		entry->slot_index = local_rekey_insert_failure_member - membership_slots(header);
		entry->slot_generation = test_slot_generation(&local_rekey_insert_failure_member->slot);
		entry->reserved = 0;
		pg_write_barrier();
		entry->state = UINT32_MAX;
		local_rekey_insert_failure_poisoned_entry = entry;
		local_rekey_insert_failure_phase = 1;
		return;
	}
	UT_ASSERT(false);
}

static BufferTag
make_tag(BlockNumber block)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1;
	tag.dbOid = 2;
	tag.relNumber = 3;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = block;
	return tag;
}

static PcmXWaitIdentity
make_wait_identity(BlockNumber block, int32 node_id, uint32 procno, uint64 request_id)
{
	PcmXWaitIdentity identity;

	memset(&identity, 0, sizeof(identity));
	identity.tag = make_tag(block);
	identity.node_id = node_id;
	identity.procno = procno;
	identity.xid = (TransactionId)(100 + procno);
	identity.cluster_epoch = UINT64_C(9);
	identity.request_id = request_id;
	identity.wait_seq = request_id + UINT64_C(1000);
	/* Generation zero is deliberately valid for the first ownership tuple. */
	identity.base_own_generation = 0;
	return identity;
}


static void
test_publish_raw_local_wait_mapping(PcmXShmemHeader *header, const PcmXWaitIdentity *identity,
									PcmXSlotRef ref)
{
	PcmXDirectoryEntry *entries;
	uint64 key_hash;
	Size capacity;
	Size step;

	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_NOT_NULL(identity);
	UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_WAIT, identity, &key_hash));
	entries = directory_entries(header, PCM_X_DIR_LOCAL_WAIT, &capacity);
	for (step = 0; step < capacity; step++) {
		PcmXDirectoryEntry *entry = &entries[((key_hash % capacity) + step) % capacity];

		UT_ASSERT(entry->state == PCM_X_DIRECTORY_EMPTY
				  || entry->state == PCM_X_DIRECTORY_OCCUPIED);
		if (entry->state != PCM_X_DIRECTORY_EMPTY)
			continue;
		entry->key_hash = key_hash;
		entry->slot_index = ref.slot_index;
		entry->slot_generation = ref.slot_generation;
		entry->reserved = 0;
		pg_write_barrier();
		entry->state = PCM_X_DIRECTORY_OCCUPIED;
		return;
	}
	UT_ASSERT(false);
}


static bool
test_raw_local_wait_mapping_exists(PcmXShmemHeader *header, const PcmXWaitIdentity *identity,
								   PcmXSlotRef expected)
{
	PcmXDirectoryEntry *entries;
	uint64 key_hash;
	Size capacity;
	Size step;

	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_NOT_NULL(identity);
	UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_WAIT, identity, &key_hash));
	entries = directory_entries(header, PCM_X_DIR_LOCAL_WAIT, &capacity);
	for (step = 0; step < capacity; step++) {
		PcmXDirectoryEntry *entry = &entries[((key_hash % capacity) + step) % capacity];

		UT_ASSERT(entry->state == PCM_X_DIRECTORY_EMPTY
				  || entry->state == PCM_X_DIRECTORY_OCCUPIED);
		if (entry->state == PCM_X_DIRECTORY_EMPTY)
			return false;
		if (entry->key_hash == key_hash && entry->slot_index == expected.slot_index
			&& entry->slot_generation == expected.slot_generation)
			return true;
	}
	return false;
}


static void
prepare_promoted_rekey_fixture(BlockNumber block, uint64 master_session, uint64 base_own_generation,
							   PcmXLocalHandle *promoted_out)
{
	PcmXLocalHandle handles[2];
	int i;

	init_active_pcm_x(master_session);
	bind_local_master(1, UINT64_C(9), master_session);
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity = make_wait_identity(
			block, 0, (uint32)(26 + i), UINT64_C(20000) + (uint64)block * 10 + (uint64)i);

		identity.base_own_generation = base_own_generation;
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], promoted_out), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(promoted_out->role, PCM_X_LOCAL_ROLE_NODE_LEADER);
}

static PcmXLocalHolderKey
make_local_holder_key(BlockNumber block, int32 node_id, uint32 procno, uint64 request_id,
					  int32 buffer_id)
{
	PcmXLocalHolderKey key;

	memset(&key, 0, sizeof(key));
	key.identity = make_wait_identity(block, node_id, procno, request_id);
	key.buffer_id = buffer_id;
	return key;
}

static PcmXQueueResult
register_active_local_holder(const PcmXLocalHolderKey *key, PcmXLocalHolderHandle *handle)
{
	PcmXQueueResult activate_result;
	PcmXQueueResult result;

	result = cluster_pcm_x_local_holder_register(key, handle);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	activate_result = cluster_pcm_x_local_holder_activate_exact(handle);
	UT_ASSERT_EQ(activate_result,
				 result == PCM_X_QUEUE_OK ? PCM_X_QUEUE_OK : PCM_X_QUEUE_DUPLICATE);
	return result;
}

static PcmXQueueResult
release_active_local_holder(const PcmXLocalHolderHandle *handle)
{
	PcmXQueueResult result;

	result = cluster_pcm_x_local_holder_mark_releasing_exact(handle);
	if (result != PCM_X_QUEUE_OK)
		return result;
	return cluster_pcm_x_local_holder_unregister_exact(handle);
}

static void
arm_local_retire_release_interlock(PcmXLocalTagSlot *tag_slot, int32 master_node, uint64 ticket_id)
{
	UT_ASSERT_NOT_NULL(ClusterPcmXConvertShmem);
	UT_ASSERT_NOT_NULL(tag_slot);
	UT_ASSERT(master_node >= 0 && master_node < PCM_X_PROTOCOL_NODE_LIMIT);
	UT_ASSERT(ticket_id != 0);
	UT_ASSERT_NULL(local_retire_release_interlock_tag);
	local_retire_release_interlock_tag = tag_slot;
	local_retire_release_interlock_frontier = &ClusterPcmXConvertShmem->peer_frontiers[master_node];
	local_retire_release_interlock_ticket_id = ticket_id;
	local_retire_release_interlock_gate_owner = (uint32)master_node + 1;
	local_retire_release_interlock_observed_gate = false;
	local_retire_release_interlock_claimed = false;
}

static void
arm_local_abort_acquire_interlock(PcmXLocalTagSlot *tag_slot, int32 master_node, uint64 ticket_id,
								  int allocator_acquire_match)
{
	UT_ASSERT_NOT_NULL(ClusterPcmXConvertShmem);
	UT_ASSERT_NOT_NULL(tag_slot);
	UT_ASSERT(master_node >= 0 && master_node < PCM_X_PROTOCOL_NODE_LIMIT);
	UT_ASSERT(ticket_id != 0);
	UT_ASSERT(allocator_acquire_match > 0);
	UT_ASSERT_NULL(local_abort_acquire_interlock_tag);
	local_abort_acquire_interlock_tag = tag_slot;
	local_abort_acquire_interlock_frontier = &ClusterPcmXConvertShmem->peer_frontiers[master_node];
	local_abort_acquire_interlock_ticket_id = ticket_id;
	local_abort_acquire_interlock_gate_owner = (uint32)master_node + 1;
	local_abort_acquire_interlock_match = allocator_acquire_match;
	local_abort_acquire_interlock_observed_gate = false;
	local_abort_acquire_interlock_claimed = false;
}

/* Model one independently live holder-transfer lane on an existing local
 * tag.  The tests using this helper deliberately isolate the final-writer
 * detach interlock from the holder wire state machine: these exact persistent
 * fields are the authority that detach must preserve until holder drain. */
static void
seed_live_local_holder_transfer(PcmXLocalTagSlot *tag_slot, uint32 procno, uint64 request_id,
								uint64 ticket_id)
{
	PcmXWaitIdentity identity;

	UT_ASSERT_NOT_NULL(tag_slot);
	identity = make_wait_identity(tag_slot->tag.blockNum, 2, procno, request_id);
	UT_ASSERT(memcmp(&identity.tag, &tag_slot->tag, sizeof(identity.tag)) == 0);
	memset(&tag_slot->holder_ref, 0, sizeof(tag_slot->holder_ref));
	tag_slot->holder_ref.identity = identity;
	tag_slot->holder_ref.handle.ticket_id = ticket_id;
	tag_slot->holder_ref.handle.queue_generation = UINT64_C(1);
	tag_slot->holder_ref.grant_generation = UINT64_C(1);
	memset(&tag_slot->holder_reliable, 0, sizeof(tag_slot->holder_reliable));
	tag_slot->holder_reliable.state_sequence = UINT64_C(1);
	tag_slot->holder_reliable.expected_responder_session = tag_slot->master_session_incarnation;
	tag_slot->holder_reliable.expected_responder_node = tag_slot->master_node;
	tag_slot->holder_reliable.pending_opcode = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	tag_slot->holder_reliable.phase = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
}

/* Build the exact steady-state boundary consumed by local round retirement:
 * the closed writer cohort is gone, one next-round writer remains, and an
 * independently published holder image has reached exact DRAIN.  Individual
 * tests corrupt one proof field at a time after this helper returns. */
static PcmXLocalTagSlot *
prepare_retire_ready_round_with_holder(uint64 master_session, BlockNumber block,
									   PcmXLocalCutoff *cutoff_out, PcmXLocalHandle *late_out)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	identity = make_wait_identity(block, 0, 2, master_session + UINT64_C(100));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, cutoff_out),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(block, 0, 3, master_session + UINT64_C(101));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, late_out),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);

	tag_slot = &local_tag_slots(header)[late_out->tag_slot.slot_index];
	memset(&tag_slot->holder_ref, 0, sizeof(tag_slot->holder_ref));
	tag_slot->holder_ref.identity = leader.identity;
	tag_slot->holder_ref.handle.ticket_id = master_session + UINT64_C(200);
	tag_slot->holder_ref.handle.queue_generation = UINT64_C(1);
	tag_slot->holder_ref.grant_generation = UINT64_C(1);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, master_session + UINT64_C(300),
											&tag_slot->holder_image.image_id));
	tag_slot->holder_image.source_own_generation = UINT64_C(1);
	tag_slot->holder_image.page_scn = UINT64_C(2);
	tag_slot->holder_image.page_lsn = UINT64_C(3);
	tag_slot->holder_image.source_node = 0;
	tag_slot->holder_image.page_checksum = UINT32_C(4);
	memset(&tag_slot->holder_reliable, 0, sizeof(tag_slot->holder_reliable));
	tag_slot->holder_reliable.state_sequence = UINT64_C(1);
	tag_slot->holder_reliable.last_responder_node = 1;
	tag_slot->holder_reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_DRAIN_POLL;
	tag_slot->holder_terminal_drain_generation = UINT64_C(1);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						pg_atomic_read_u32(&tag_slot->slot.state_flags)
							| (PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK << PCM_X_SLOT_FLAGS_SHIFT));
	return tag_slot;
}

static void
clear_local_holder_transfer(PcmXLocalTagSlot *tag_slot)
{
	UT_ASSERT_NOT_NULL(tag_slot);
	memset(&tag_slot->holder_ref, 0, sizeof(tag_slot->holder_ref));
	memset(&tag_slot->holder_reliable, 0, sizeof(tag_slot->holder_reliable));
}

static PcmXEnqueuePayload
make_enqueue(PcmXWaitIdentity identity, uint64 sender_session, uint64 prehandle_sequence)
{
	PcmXEnqueuePayload request;

	memset(&request, 0, sizeof(request));
	request.identity = identity;
	request.prehandle.sender_session_incarnation = sender_session;
	request.prehandle.prehandle_sequence = prehandle_sequence;
	return request;
}

static void
bind_enqueue_peer(const PcmXEnqueuePayload *request)
{
	PcmXQueueResult result;

	result = cluster_pcm_x_peer_bind_ack_publish(request->identity.node_id,
												 request->identity.cluster_epoch,
												 request->prehandle.sender_session_incarnation);
	UT_ASSERT(result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE);
}

static void
bind_local_master(int32 master_node, uint64 cluster_epoch, uint64 master_session)
{
	PcmXQueueResult result;

	result = cluster_pcm_x_peer_bind_ack_publish(master_node, cluster_epoch, master_session);
	UT_ASSERT(result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE);
}

static ClusterLmdVertex
make_blocker(int32 node_id, uint32 procno, uint64 request_id)
{
	ClusterLmdVertex blocker;

	memset(&blocker, 0, sizeof(blocker));
	blocker.node_id = node_id;
	blocker.procno = procno;
	blocker.cluster_epoch = UINT64_C(9);
	blocker.request_id = request_id;
	blocker.xid = (TransactionId)(200 + procno);
	blocker.local_start_ts_ms = (int64)(request_id + UINT64_C(500));
	blocker.wait_seq = request_id + UINT64_C(1000);
	return blocker;
}

static bool
blockers_equal(const ClusterLmdVertex *left, const ClusterLmdVertex *right)
{
	return left->node_id == right->node_id && left->procno == right->procno
		   && left->cluster_epoch == right->cluster_epoch && left->request_id == right->request_id
		   && left->xid == right->xid && left->local_start_ts_ms == right->local_start_ts_ms
		   && left->wait_seq == right->wait_seq;
}

static uint32
blocker_set_crc32c(const ClusterLmdVertex *blockers, Size nblockers)
{
	return cluster_pcm_x_blocker_set_crc32c(blockers, nblockers);
}

typedef struct TestLocalParticipantTransfer {
	PcmXLocalHandle leader;
	PcmXLocalCutoff cutoff;
	PcmXGrantPayload prepare;
	PcmXDrainPollPayload poll;
	uint64 master_session;
} TestLocalParticipantTransfer;

/* Drive one requester that also held a local S pin to the exact pre-DRAIN
 * boundary, while a different node remains the immutable image source. */
static void
prepare_local_non_source_participant_transfer(BlockNumber block, uint64 master_session,
											  TestLocalParticipantTransfer *fixture)
{
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalWriterClaim writer_claim;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload admit_confirm;
	PcmXInstallReadyPayload install_ready;
	PcmXPhasePayload commit;
	PcmXFinalAckPayload final_ack;
	PcmXPhasePayload final_commit;
	PcmXPhasePayload final_confirm;
	PcmXLocalReliableToken token;

	UT_ASSERT_NOT_NULL(fixture);
	memset(fixture, 0, sizeof(*fixture));
	fixture->master_session = master_session;
	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(block, 0, 2, master_session + UINT64_C(1), 3);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	identity = make_wait_identity(block, 0, 3, master_session + UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &fixture->leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&fixture->leader, &writer_claim),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&fixture->leader, &fixture->cutoff),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&fixture->leader, &enqueue, &token),
				 PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = identity;
	admit_ack.ref.handle.ticket_id = master_session + UINT64_C(100);
	admit_ack.ref.handle.queue_generation = UINT64_C(1);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_apply_admit_ack_exact(&fixture->leader, &admit_ack, 1, master_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_arm_exact(&fixture->leader, &admit_confirm, &token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&fixture->leader, &admit_confirm, 1,
															 master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&admit_ack.ref, 1, master_session,
																 &holder_copy, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&admit_ack.ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(
					 &admit_ack.ref, blocker_snapshot.set_generation, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);

	fixture->prepare.ref = admit_ack.ref;
	fixture->prepare.ref.grant_generation = UINT64_C(1);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, master_session + UINT64_C(200),
											&fixture->prepare.image.image_id));
	fixture->prepare.image.source_own_generation = UINT64_C(9);
	fixture->prepare.image.page_scn = UINT64_C(10);
	fixture->prepare.image.page_lsn = UINT64_C(11);
	fixture->prepare.image.source_node = 2;
	fixture->prepare.image.page_checksum = UINT32_C(12);
	UT_ASSERT_EQ(cluster_pcm_x_local_prepare_grant_exact(&fixture->leader, &fixture->prepare, 1,
														 master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_install_ready_arm_exact(
					 &fixture->leader, &fixture->prepare.ref, &fixture->prepare.image,
					 &install_ready, &token),
				 PCM_X_QUEUE_OK);
	memset(&commit, 0, sizeof(commit));
	commit.ref = fixture->prepare.ref;
	commit.phase = PGRAC_IC_MSG_PCM_X_COMMIT_X;
	UT_ASSERT_EQ(cluster_pcm_x_local_commit_x_exact(&fixture->leader, &commit, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_final_ack_arm_exact(&fixture->leader, 1, &final_ack, &token),
				 PCM_X_QUEUE_OK);
	memset(&final_commit, 0, sizeof(final_commit));
	final_commit.ref = fixture->prepare.ref;
	final_commit.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	UT_ASSERT_EQ(cluster_pcm_x_local_final_commit_ack_exact(&fixture->leader, &final_commit, 1,
															master_session, &final_confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&writer_claim), PCM_X_QUEUE_OK);
	fixture->poll.ref = fixture->prepare.ref;
	fixture->poll.drain_generation = UINT64_C(1);
}

static PcmXBlockerSetHeaderPayload
make_blocker_header(const PcmXTicketRef *ref, uint64 set_generation,
					const ClusterLmdVertex *blockers, Size nblockers)
{
	PcmXBlockerSetHeaderPayload header;

	memset(&header, 0, sizeof(header));
	header.ref = *ref;
	header.set_generation = set_generation;
	header.nblockers = (uint32)nblockers;
	header.set_crc32c = blocker_set_crc32c(blockers, nblockers);
	return header;
}

static PcmXBlockerChunkPayload
make_blocker_chunk(const PcmXTicketRef *ref, uint64 set_generation, uint32 chunk_no,
				   ClusterLmdVertex blocker)
{
	PcmXBlockerChunkPayload chunk;

	memset(&chunk, 0, sizeof(chunk));
	chunk.tag = ref->identity.tag;
	chunk.requester_node = ref->identity.node_id;
	chunk.requester_procno = ref->identity.procno;
	chunk.chunk_no = chunk_no;
	chunk.cluster_epoch = ref->identity.cluster_epoch;
	chunk.request_id = ref->identity.request_id;
	chunk.handle = ref->handle;
	chunk.grant_generation = ref->grant_generation;
	chunk.set_generation = set_generation;
	chunk.blocker = blocker;
	return chunk;
}

static void
admit_active_probe(BlockNumber block, int32 node_id, uint32 procno, uint64 request_id,
				   uint64 sender_session, uint64 prehandle_sequence,
				   PcmXMasterAdmission *admission_out)
{
	PcmXEnqueuePayload request = make_enqueue(
		make_wait_identity(block, node_id, procno, request_id), sender_session, prehandle_sequence);
	PcmXTicketRef active;

	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, admission_out), PCM_X_QUEUE_OK);
	UT_ASSERT_NOT_NULL(admission_out);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_admit_confirm_exact(&admission_out->ref, UINT64_C(9000) + request_id),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&active, &admission_out->ref));
}

UT_TEST(test_master_pending_x_claim_is_active_ticket_exact)
{
	PcmXMasterAdmission admission;
	PcmXTicketRef stale;
	bool claimed = true;

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(805, 0, 7, UINT64_C(80006), UINT64_C(8106), 1, &admission);

	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_state_exact(&admission.ref, &claimed),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(!claimed);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_state_exact(&admission.ref, &claimed),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(claimed);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_DUPLICATE);

	stale = admission.ref;
	stale.identity.wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_state_exact(&stale, &claimed),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&stale), PCM_X_QUEUE_STALE);

	/* External GRD ownership must be released before an active claim can be
	 * cancelled; the engine must not silently discard that proof bit. */
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
}

UT_TEST(test_master_pending_x_claim_state_classifies_cancel_progress_as_not_ready)
{
	PcmXMasterAdmission admission;
	PcmXMasterPendingXReleaseToken release;
	PcmXMasterProbeToken probe;
	PcmXPhasePayload payload;
	bool claimed = true;

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(812, 0, 13, UINT64_C(80012), UINT64_C(8112), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	bind_local_master(1, admission.ref.identity.cluster_epoch, UINT64_C(8212));
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 1, UINT64_C(8212),
															  &payload, &probe),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_state_exact(&admission.ref, &claimed),
				 PCM_X_QUEUE_NOT_READY);

	init_active_pcm_x(UINT64_C(78));
	admit_active_probe(813, 0, 14, UINT64_C(80013), UINT64_C(8113), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_state_exact(&admission.ref, &claimed),
				 PCM_X_QUEUE_NOT_READY);
}

static void
arm_blocker_probe(const PcmXTicketRef *ref, int32 source_node, uint64 source_session)
{
	PcmXPhasePayload probe;
	PcmXMasterProbeToken token;

	bind_local_master(source_node, ref->identity.cluster_epoch, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(ref, source_node, source_session,
															  &probe, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&probe.ref, ref));
	UT_ASSERT_EQ(probe.reason, 0);
	UT_ASSERT_EQ(probe.phase, 0);
	UT_ASSERT_EQ(probe.flags, 0);
	UT_ASSERT(ticket_refs_equal(&token.ref, ref));
	UT_ASSERT_EQ(token.expected_responder_node, source_node);
	UT_ASSERT_EQ(token.expected_responder_session, source_session);
}

static void
commit_empty_blocker_graph(const PcmXTicketRef *ref, uint64 graph_generation)
{
	PcmXMasterBlockerSnapshot snapshot;
	PcmXMasterTicketSlot *ticket;
	PcmXSlotRef ticket_ref;
	uint32 set_crc32c = (uint32)(graph_generation ^ (graph_generation >> 32)) | UINT32_C(1);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(ref, 1, NULL, 0, set_crc32c),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(ref, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, graph_generation),
		PCM_X_QUEUE_OK);
	/* This legacy white-box fixture bypasses the 45-48 wire exchange.  Mark
	 * the same durable ACK boundary that production reaches via
	 * blocker_probe_complete_exact before beginning transfer. */
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle, &ticket_ref),
		PCM_X_DIRECTORY_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[ticket_ref.slot_index];
	ticket->reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT;
}

/* Production reserves the GRD cookie before this engine transition.  Most
 * state-machine tests intentionally mock only the engine, so centralize that
 * prerequisite instead of weakening the production begin gate. */
static PcmXQueueResult
test_master_begin_transfer_claimed_exact(const PcmXTicketRef *ref, uint64 graph_generation,
										 PcmXTicketRef *transfer_out)
{
	PcmXQueueResult claim_result = cluster_pcm_x_master_pending_x_claim_exact(ref);

	if (claim_result == PCM_X_QUEUE_CORRUPT || claim_result == PCM_X_QUEUE_COUNTER_EXHAUSTED
		|| claim_result == PCM_X_QUEUE_NO_CAPACITY)
		return claim_result;
	return cluster_pcm_x_master_begin_transfer_exact(ref, graph_generation, transfer_out);
}

static PcmXQueueResult
test_master_begin_transfer_unclaimed_exact(const PcmXTicketRef *ref, uint64 graph_generation,
										   PcmXTicketRef *transfer_out)
{
	return cluster_pcm_x_master_begin_transfer_exact(ref, graph_generation, transfer_out);
}

static bool
test_ticket_locator_equal(const PcmXTicketRef *left, const PcmXTicketRef *right)
{
	return BufferTagsEqual(&left->identity.tag, &right->identity.tag)
		   && left->identity.node_id == right->identity.node_id
		   && left->identity.procno == right->identity.procno
		   && left->identity.xid == right->identity.xid
		   && left->identity.cluster_epoch == right->identity.cluster_epoch
		   && left->identity.request_id == right->identity.request_id
		   && left->identity.wait_seq == right->identity.wait_seq
		   && left->identity.base_own_generation == right->identity.base_own_generation
		   && left->handle.ticket_id == right->handle.ticket_id
		   && left->handle.queue_generation == right->handle.queue_generation;
}

/* complete_exact is a legacy engine-only test primitive with no GRD handoff.
 * Strip the mocked claim only for those legacy tests; production queue code
 * has no caller and must complete through FINAL_CONFIRM. */
static PcmXQueueResult
test_master_complete_legacy_exact(const PcmXTicketRef *ref)
{
	PcmXSlotRef ticket_ref;

	if (ref != NULL && ClusterPcmXConvertShmem != NULL
		&& cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &ref->handle, &ticket_ref)
			   == PCM_X_DIRECTORY_OK) {
		PcmXMasterTicketSlot *ticket
			= &master_ticket_slots(ClusterPcmXConvertShmem)[ticket_ref.slot_index];

		if (cluster_pcm_x_runtime_snapshot().state == PCM_X_RUNTIME_ACTIVE
			&& (ticket_refs_equal(&ticket->ref, ref)
				|| (grant_interlock_armed && test_ticket_locator_equal(&ticket->ref, ref)))
			&& test_slot_state(&ticket->slot) == PCM_XT_ACTIVE_TRANSFER) {
			uint32 packed = pg_atomic_read_u32(&ticket->slot.state_flags);

			packed &= ~(PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED << PCM_X_SLOT_FLAGS_SHIFT);
			pg_atomic_write_u32(&ticket->slot.state_flags, packed);
		}
	}
	return cluster_pcm_x_master_complete_exact(ref);
}

#define cluster_pcm_x_master_begin_transfer_exact test_master_begin_transfer_claimed_exact
#define cluster_pcm_x_master_complete_exact test_master_complete_legacy_exact

static Size
directory_occupied_count(PcmXShmemHeader *header, PcmXDirectoryKind kind)
{
	PcmXDirectoryEntry *entries;
	Size capacity;
	Size count = 0;
	Size i;

	entries = directory_entries(header, kind, &capacity);
	for (i = 0; i < capacity; i++)
		if (entries[i].state == PCM_X_DIRECTORY_OCCUPIED)
			count++;
	return count;
}

static void
init_active_pcm_x(uint64 master_session_incarnation)
{
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate(master_session_incarnation));
}

static bool
ticket_refs_equal(const PcmXTicketRef *left, const PcmXTicketRef *right)
{
	return memcmp(left, right, sizeof(*left)) == 0;
}

static bool
vertex_matches_ticket(const ClusterLmdVertex *vertex, const PcmXTicketRef *ticket)
{
	return vertex->node_id == ticket->identity.node_id && vertex->procno == ticket->identity.procno
		   && vertex->cluster_epoch == ticket->identity.cluster_epoch
		   && vertex->request_id == ticket->identity.request_id
		   && vertex->xid == ticket->identity.xid && vertex->local_start_ts_ms == 0
		   && vertex->wait_seq == ticket->identity.wait_seq;
}

static void
assert_master_queue_baseline(PcmXShmemHeader *header)
{
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TAG), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_HANDLE), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_RETIRE), 0);
}

static void
assert_local_queue_baseline(PcmXShmemHeader *header)
{
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), 0);
}

static void
arm_and_ack_master_terminal_leg(const PcmXTicketRef *ref, PcmXTerminalLegKind kind,
								int32 responder_node)
{
	PcmXRetirePayload retire;
	PcmXTerminalLegToken token;
	uint64 responder_session;

	UT_ASSERT_NOT_NULL(ClusterPcmXConvertShmem);
	UT_ASSERT(responder_node >= 0 && responder_node < PCM_X_PROTOCOL_NODE_LIMIT);
	responder_session
		= ClusterPcmXConvertShmem->peer_frontiers[responder_node].sender_session_incarnation;
	UT_ASSERT(responder_session != 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(ref, kind, responder_node,
															 responder_session, &token),
				 PCM_X_QUEUE_OK);
	if (kind == PCM_X_TERMINAL_LEG_DRAIN)
		UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(ref, kind, responder_node,
																 responder_session),
					 PCM_X_QUEUE_OK);
	else {
		memset(&retire, 0, sizeof(retire));
		retire.cluster_epoch = ref->identity.cluster_epoch;
		retire.master_session_incarnation = ClusterPcmXConvertShmem->master_session_incarnation;
		retire.retire_through_ticket_id = ref->handle.ticket_id;
		retire.sender_node = responder_node;
		UT_ASSERT_EQ(
			cluster_pcm_x_master_retire_ack_exact(&retire, responder_node, responder_session),
			PCM_X_QUEUE_OK);
	}
}

static PcmXRetirePayload
make_retire_ack(const PcmXTicketRef *ref, int32 sender_node)
{
	PcmXRetirePayload retire;

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = ref->identity.cluster_epoch;
	retire.master_session_incarnation = ClusterPcmXConvertShmem->master_session_incarnation;
	retire.retire_through_ticket_id = ref->handle.ticket_id;
	retire.sender_node = sender_node;
	return retire;
}

static void
drain_retire_and_detach_master(const PcmXMasterAdmission *admission)
{
	arm_and_ack_master_terminal_leg(&admission->ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission->ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&admission->ref, PCM_X_TERMINAL_LEG_RETIRE,
									admission->ref.identity.node_id);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission->ref), PCM_X_QUEUE_OK);
}

UT_TEST(test_master_pending_x_cancel_release_is_two_phase_and_replay_exact)
{
	PcmXMasterAdmission admission;
	PcmXBlockerSetHeaderPayload blocker_commit;
	PcmXMasterBlockerSnapshot blocker_snapshot;
	PcmXMasterPendingXReleaseToken release;
	PcmXMasterPendingXReleaseToken replay;
	PcmXMasterPendingXReleaseToken stale;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXTerminalLegToken terminal;
	PcmXTicketRef work;

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(806, 0, 8, UINT64_C(80007), UINT64_C(8107), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_blocker_probe(&admission.ref, 1, UINT64_C(8207));
	blocker_commit = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 1, UINT64_C(8207)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 1, UINT64_C(8207),
																 NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&blocker_snapshot, NULL, 0, UINT64_C(9807)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 1, UINT64_C(8207)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_CANCELLED);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot),
				 PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
					 | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
	UT_ASSERT(ticket_refs_equal(&release.ref, &admission.ref));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&release, &replay, sizeof(release)) == 0);
	/* The cold-path scanner must surface RELEASE_PENDING so cleanup survives
	 * the cancelling backend's death; DRAIN itself remains gated below. */
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_work_next(&work), PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&work, &admission.ref));
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, admission.ref.identity.node_id,
					 UINT64_C(8107), &terminal),
				 PCM_X_QUEUE_NOT_READY);

	stale = release;
	stale.master_session_incarnation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&stale), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot),
				 PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
					 | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot), 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0, UINT64_C(8107), &terminal),
				 PCM_X_QUEUE_OK);
	before = *ticket;
	/* A concurrent release worker may arrive after another worker finalized
	 * and already armed DRAIN.  The old release token is now an exact replay;
	 * it must not compare its obsolete sequence against the terminal leg. */
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0, UINT64_C(8107)),
				 PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	before = *ticket;
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	assert_master_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_master_pending_x_cancel_before_first_probe_releases_immediately)
{
	PcmXMasterAdmission admission;
	PcmXMasterDriveSnapshot initial;
	PcmXMasterDriveSnapshot seeded;
	PcmXMasterPendingXReleaseToken release;
	PcmXMasterTicketSlot *ticket;
	const uint32 unarmed_holders = (UINT32_C(1) << 1) | (UINT32_C(1) << 2);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(809, 0, 11, UINT64_C(80010), UINT64_C(8110), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &initial),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_drive_bitmap_replace_exact(&initial, unarmed_holders, 0, &seeded),
		PCM_X_QUEUE_OK);
	/* A holder becomes a terminal participant only after its exact PROBE leg
	 * is durable, not merely because it appeared in the authority bitmap. */
	UT_ASSERT_EQ(seeded.involved_nodes_bitmap, UINT32_C(1) << 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_CANCELLED);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot),
				 PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
					 | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
	UT_ASSERT(release.state_sequence != 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_OK);
	assert_master_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_master_cancel_ack_snapshot_distinguishes_prehandle_and_confirmed_cancel)
{
	PcmXMasterAdmission admission;
	PcmXMasterAdmission replay;
	PcmXMasterPendingXReleaseToken release;
	PcmXEnqueuePayload request;
	bool prehandle = false;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(816, 0, 16, UINT64_C(80016)), UINT64_C(8116), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_cancel_ack_snapshot_exact(&admission.ref, &replay, &prehandle),
		PCM_X_QUEUE_OK);
	UT_ASSERT(prehandle);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &admission.ref));
	UT_ASSERT(memcmp(&replay.prehandle, &admission.prehandle, sizeof(replay.prehandle)) == 0);

	init_active_pcm_x(UINT64_C(78));
	admit_active_probe(817, 0, 17, UINT64_C(80017), UINT64_C(8117), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release), PCM_X_QUEUE_OK);
	prehandle = true;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_cancel_ack_snapshot_exact(&admission.ref, &replay, &prehandle),
		PCM_X_QUEUE_OK);
	UT_ASSERT(!prehandle);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &admission.ref));
}

UT_TEST(test_master_pending_x_cancel_prepare_replay_validates_unlinked_tombstone)
{
	PcmXMasterAdmission admission;
	PcmXMasterPendingXReleaseToken release;
	PcmXMasterPendingXReleaseToken replay;
	PcmXMasterTicketSlot *ticket;

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(810, 0, 12, UINT64_C(80011), UINT64_C(8111), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	ticket->next_index = admission.ticket_slot.slot_index;
	memset(&replay, 0x7f, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(replay.master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(78));
	admit_active_probe(811, 0, 13, UINT64_C(80012), UINT64_C(8112), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&admission.ref, UINT64_C(9812));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT(ticket->blocker_set_generation != 0);
	ticket->reliable.last_response_opcode = 0;
	memset(&replay, 0x7f, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(replay.master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(81));
	admit_active_probe(818, 0, 18, UINT64_C(80018), UINT64_C(8118), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	ticket->graph_generation = 0;
	memset(&replay, 0x7f, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(replay.master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(79));
	admit_active_probe(814, 0, 14, UINT64_C(80014), UINT64_C(8114), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	ticket->reliable.state_sequence = 0;
	memset(&replay, 0x7f, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(replay.master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(80));
	admit_active_probe(815, 0, 15, UINT64_C(80015), UINT64_C(8115), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	ticket->reliable.expected_responder_session = UINT64_C(999);
	memset(&replay, 0x7f, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &replay),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(replay.master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_pending_x_cancel_intent_survives_partial_probe_and_nonempty_graph)
{
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entry;
	PcmXMasterBlockerSnapshot blocker_snapshot;
	PcmXMasterDriveSnapshot drive_snapshot;
	PcmXMasterPendingXReleaseToken release;
	PcmXBlockerSetHeaderPayload blocker_commit;
	PcmXBlockerChunkPayload edge;
	PcmXMasterProbeToken probe_replay;
	PcmXPhasePayload probe_payload;
	PcmXMasterTicketSlot *ticket;
	ClusterLmdVertex blocker;
	const uint64 source_session = UINT64_C(8217);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(807, 0, 9, UINT64_C(80008), UINT64_C(8108), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_blocker_probe(&admission.ref, 1, source_session);
	blocker = make_blocker(1, 10, UINT64_C(8308));
	blocker_commit = make_blocker_header(&admission.ref, 1, &blocker, 1);
	edge = make_blocker_chunk(&admission.ref, 1, 0, blocker);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 1, source_session),
				 PCM_X_QUEUE_OK);

	/* Cancellation is durable while the holder's reliable PROBE leg finishes;
	 * it must neither enter transfer nor discard a partial staged set. */
	memset(&release, 0x7f, sizeof(release));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(release.master_session_incarnation, 0);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ACTIVE_PROBE);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot),
				 PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
					 | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(&admission.ref.identity.tag,
														   admission.ref.identity.cluster_epoch,
														   &drive_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(drive_snapshot.flags, PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
										   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 1, source_session,
															  &probe_payload, &probe_replay),
				 PCM_X_QUEUE_DUPLICATE);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge, 1, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 1, source_session,
																 &entry, 1, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_graph_commit_exact(&blocker_snapshot, &entry, 1,
																 UINT64_C(9808)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 1, source_session),
		PCM_X_QUEUE_OK);
	/* Once the in-flight PROBE has settled, durable cancellation freezes the
	 * holder fanout.  Re-arming here would starve the release forever. */
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 1, source_session,
															  &probe_payload, &probe_replay),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_CANCELLED);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot),
				 PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
					 | PCM_X_MASTER_TICKET_F_PENDING_X_RELEASE_PENDING);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_finalize_exact(&release), PCM_X_QUEUE_OK);
	/* Type-49 can be admitted to the outbound FIFO and then lost before the
	 * holder consumes it.  Its BEGIN/EDGE/COMMIT retry must still reproduce
	 * the exact ACK from the cancellation tombstone. */
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 1, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge, 1, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 1, source_session,
																 &entry, 1, &blocker_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 1, source_session),
		PCM_X_QUEUE_BAD_STATE);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 1, source_session,
																 &entry, 1, &blocker_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	assert_master_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_master_pending_x_cancel_loses_cleanly_to_active_transfer)
{
	PcmXMasterAdmission admission;
	PcmXMasterPendingXReleaseToken release;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef transfer;

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(808, 0, 10, UINT64_C(80009), UINT64_C(8109), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&admission.ref, UINT64_C(9809));
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9809), &transfer),
		PCM_X_QUEUE_OK);
	memset(&release, 0x7f, sizeof(release));
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_cancel_prepare_exact(&admission.ref, &release),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(release.master_session_incarnation, 0);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ACTIVE_TRANSFER);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot), PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

static bool
slot_refs_equal(PcmXSlotRef left, PcmXSlotRef right)
{
	return left.slot_index == right.slot_index && left.slot_generation == right.slot_generation;
}

static uint64
test_slot_generation(PcmXSlotHeader *slot)
{
	uint32 after;
	uint32 before;
	uint64 generation = 0;
	int retry;

	for (retry = 0; retry < 16; retry++) {
		before = pg_atomic_read_u32(&slot->generation_change_seq);
		if ((before & 1U) != 0)
			continue;
		pg_read_barrier();
		generation = ((uint64)slot->slot_generation_hi << 32) | slot->slot_generation_lo;
		pg_read_barrier();
		after = pg_atomic_read_u32(&slot->generation_change_seq);
		if (before == after && (after & 1U) == 0)
			return generation;
	}
	UT_ASSERT(false);
	return 0;
}

static void
test_set_slot_generation(PcmXSlotHeader *slot, uint64 generation)
{
	uint32 odd = pg_atomic_read_u32(&slot->generation_change_seq) | 1U;

	pg_atomic_write_u32(&slot->generation_change_seq, odd);
	pg_write_barrier();
	slot->slot_generation_lo = (uint32)generation;
	slot->slot_generation_hi = (uint32)(generation >> 32);
	pg_write_barrier();
	pg_atomic_write_u32(&slot->generation_change_seq, odd + 1U);
}

static uint32
test_slot_state(PcmXSlotHeader *slot)
{
	return pg_atomic_read_u32(&slot->state_flags) & PCM_X_SLOT_STATE_MASK;
}

static void
test_set_slot_state(PcmXSlotHeader *slot, uint32 state)
{
	uint32 current = pg_atomic_read_u32(&slot->state_flags);
	uint32 desired;

	do {
		desired = (current & PCM_X_SLOT_FLAGS_MASK) | state;
	} while (!pg_atomic_compare_exchange_u32(&slot->state_flags, &current, desired));
}

static uint32
test_slot_flags(PcmXSlotHeader *slot)
{
	return pg_atomic_read_u32(&slot->state_flags) >> PCM_X_SLOT_FLAGS_SHIFT;
}

static PcmXSlotHeader *
reserve_slot(PcmXAllocatorKind kind, PcmXSlotRef *ref)
{
	PcmXSlotHeader *slot = NULL;

	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(kind, ref, &slot), PCM_X_ALLOC_OK);
	UT_ASSERT_NOT_NULL(slot);
	UT_ASSERT_EQ(test_slot_generation(slot), ref->slot_generation);
	UT_ASSERT_EQ(test_slot_state(slot), PCM_X_SLOT_RESERVED_NONVISIBLE);
	return slot;
}

static void
assert_corrupt_reserve_has_no_ref(PcmXAllocatorKind kind)
{
	PcmXSlotRef ref = { 0, UINT64_C(99) };
	PcmXSlotHeader *slot = (PcmXSlotHeader *)(uintptr_t)1;

	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(kind, &ref, &slot), PCM_X_ALLOC_CORRUPT);
	UT_ASSERT_EQ(ref.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ref.slot_generation, 0);
	UT_ASSERT_NULL(slot);
}

static void
find_prehandles_for_bucket(Size capacity, Size bucket, int needed, PcmXPrehandleKey *keys)
{
	uint64 sequence;
	int found = 0;

	for (sequence = 1; sequence <= (uint64)capacity * 1024 && found < needed; sequence++) {
		PcmXPrehandleKey key = { UINT64_C(71), sequence };
		uint64 hash;

		UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, &hash));
		if ((Size)(hash % capacity) == bucket)
			keys[found++] = key;
	}
	UT_ASSERT_EQ(found, needed);
}


UT_TEST(test_wire_abi_sizes_are_exact)
{
	UT_ASSERT_EQ(sizeof(PcmXWaitIdentity), 64);
	UT_ASSERT_EQ(sizeof(PcmXLocalHolderKey), 72);
	UT_ASSERT_EQ(sizeof(PcmXSlotRef), 16);
	UT_ASSERT_EQ(sizeof(PcmXTicketHandle), 16);
	UT_ASSERT_EQ(sizeof(PcmXPrehandleKey), 16);
	UT_ASSERT_EQ(sizeof(PcmXTicketRef), 88);
	UT_ASSERT_EQ(sizeof(PcmXImageToken), 40);
	UT_ASSERT_EQ(sizeof(PcmXEnqueuePayload), 80);
	UT_ASSERT_EQ(sizeof(PcmXPrehandleCancelPayload), 80);
	UT_ASSERT_EQ(sizeof(PcmXAdmitAckPayload), 112);
	UT_ASSERT_EQ(sizeof(PcmXPhasePayload), 96);
	UT_ASSERT_EQ(sizeof(PcmXRevokePayload), 96);
	UT_ASSERT_EQ(sizeof(PcmXGrantPayload), 128);
	UT_ASSERT_EQ(sizeof(PcmXInstallReadyPayload), 104);
	UT_ASSERT_EQ(sizeof(PcmXFinalAckPayload), 104);
	UT_ASSERT_EQ(sizeof(PcmXBlockerSetHeaderPayload), 104);
	UT_ASSERT_EQ(sizeof(PcmXBlockerChunkPayload), 128);
	UT_ASSERT_EQ(sizeof(PcmXRetirePayload), 32);
	UT_ASSERT_EQ(sizeof(PcmXDrainPollPayload), 96);
	UT_ASSERT_EQ(sizeof(PcmXDirectoryEntry), 32);
	UT_ASSERT_EQ(sizeof(PcmXMasterAdmission), 160);
	UT_ASSERT_EQ(sizeof(PcmXLocalHandle), 112);
	UT_ASSERT_EQ(sizeof(PcmXLocalCutoff), 48);
	UT_ASSERT_EQ(sizeof(PcmXLocalHolderHandle), 104);
	UT_ASSERT_EQ(sizeof(PcmXLocalHolderSnapshot), 32);
	UT_ASSERT_EQ(sizeof(PcmXMasterBlockerEntry), 72);
	UT_ASSERT_EQ(sizeof(PcmXMasterBlockerSnapshot), 112);
}

UT_TEST(test_wire_abi_offsets_are_exact)
{
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, base_own_generation), 56);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderKey, identity), 0);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderKey, buffer_id), 64);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderKey, reserved), 68);
	UT_ASSERT_EQ(offsetof(PcmXTicketRef, handle), 64);
	UT_ASSERT_EQ(offsetof(PcmXTicketRef, grant_generation), 80);
	UT_ASSERT_EQ(offsetof(PcmXEnqueuePayload, prehandle), 64);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, prehandle), 88);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, result), 104);
	UT_ASSERT_EQ(offsetof(PcmXGrantPayload, image), 88);
	UT_ASSERT_EQ(offsetof(PcmXBlockerSetHeaderPayload, set_generation), 88);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, blocker), 80);
	UT_ASSERT_EQ(offsetof(PcmXRetirePayload, sender_node), 24);
	UT_ASSERT_EQ(offsetof(PcmXMasterAdmission, ticket_slot), 120);
	UT_ASSERT_EQ(offsetof(PcmXLocalHandle, tag_slot), 64);
	UT_ASSERT_EQ(offsetof(PcmXLocalHandle, local_sequence), 96);
	UT_ASSERT_EQ(offsetof(PcmXLocalCutoff, master_session_incarnation), 24);
	UT_ASSERT_EQ(offsetof(PcmXLocalCutoff, master_node), 40);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderHandle, tag_slot), 72);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderHandle, holder_slot), 88);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderSnapshot, holder_set_generation), 16);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderSnapshot, holder_count), 24);
	UT_ASSERT_EQ(offsetof(PcmXMasterBlockerEntry, blocker), 16);
	UT_ASSERT_EQ(offsetof(PcmXMasterBlockerEntry, chunk_no), 64);
	UT_ASSERT_EQ(offsetof(PcmXMasterBlockerSnapshot, set_generation), 88);
	UT_ASSERT_EQ(offsetof(PcmXMasterBlockerSnapshot, graph_generation), 96);
	UT_ASSERT_EQ(offsetof(PcmXMasterBlockerSnapshot, blocker_count), 104);
	UT_ASSERT_EQ(sizeof(PcmXMasterProbeToken), 112);
	UT_ASSERT_EQ(offsetof(PcmXMasterProbeToken, state_sequence), 88);
	UT_ASSERT_EQ(offsetof(PcmXMasterProbeToken, expected_responder_node), 104);
}

UT_TEST(test_runtime_layout_abi_and_offsets_are_exact)
{
	UT_ASSERT_EQ(PCM_X_SHMEM_LAYOUT_VERSION, 12);
	UT_ASSERT_EQ(PCM_X_LOCK_PARTITIONS, NUM_BUFFER_PARTITIONS);
	UT_ASSERT_EQ(PCM_X_LWLOCK_COUNT, 257);
	UT_ASSERT_EQ(sizeof(PcmXShmemLayout), 440);
	UT_ASSERT_EQ(sizeof(PcmXAllocatorState), 32);
	UT_ASSERT_EQ(sizeof(PcmXStats), 176);
	UT_ASSERT_EQ(sizeof(PcmXStatsSnapshot), 224);
	UT_ASSERT_EQ(sizeof(PcmXSlotHeader), 24);
	UT_ASSERT_EQ(offsetof(PcmXSlotHeader, next_free), 0);
	UT_ASSERT_EQ(offsetof(PcmXSlotHeader, generation_change_seq), 8);
	UT_ASSERT_EQ(offsetof(PcmXSlotHeader, slot_generation_lo), 12);
	UT_ASSERT_EQ(offsetof(PcmXSlotHeader, slot_generation_hi), 16);
	UT_ASSERT_EQ(offsetof(PcmXSlotHeader, state_flags), 20);
	UT_ASSERT_EQ(sizeof(PcmXReliableLegState), 56);
	UT_ASSERT_EQ(sizeof(PcmXLocalProgress), 240);
	UT_ASSERT_EQ(offsetof(PcmXLocalProgress, master_session_incarnation), 224);
	UT_ASSERT_EQ(offsetof(PcmXLocalProgress, master_node), 232);
	UT_ASSERT_EQ(sizeof(PcmXLocalHolderProgress), 160);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderProgress, master_session_incarnation), 144);
	UT_ASSERT_EQ(offsetof(PcmXLocalHolderProgress, master_node), 152);
	UT_ASSERT_EQ(sizeof(PcmXLocalBlockerSnapshot), 112);
	UT_ASSERT_EQ(sizeof(PcmXMasterTagSlot), 120);
	UT_ASSERT_EQ(sizeof(PcmXMasterTicketSlot), 384);
	UT_ASSERT_EQ(sizeof(PcmXBlockerSlot), 128);
	UT_ASSERT_EQ(sizeof(PcmXLocalTagSlot), 752);
	UT_ASSERT_EQ(sizeof(PcmXLocalMembershipSlot), 168);
	UT_ASSERT_EQ(sizeof(PcmXPeerFrontier), 48);
	UT_ASSERT_EQ(sizeof(PcmXPeerBinding), 16);
	UT_ASSERT_EQ(sizeof(PcmXOutboundTargetFrontier), 32);
	UT_ASSERT_EQ(offsetof(PcmXMasterTagSlot, tag), 24);
	UT_ASSERT_EQ(offsetof(PcmXMasterTagSlot, active_index), 96);
	UT_ASSERT_EQ(offsetof(PcmXMasterTagSlot, admission_gate), 104);
	UT_ASSERT_EQ(offsetof(PcmXMasterTagSlot, outstanding_ticket_count), 112);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, ref), 24);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, reliable), 168);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_head_index), 296);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_stage_head_index), 304);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_stage_set_generation), 312);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_stage_source_session), 320);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_set_source_session), 328);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_stage_next_chunk), 352);
	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, blocker_set_source_node), 360);
	UT_ASSERT_EQ(offsetof(PcmXBlockerSlot, blocker), 40);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, ref), 112);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, reliable), 240);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, blocker_snapshot_head_index), 360);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, membership_count), 384);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, closed_round_member_count), 392);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, terminal_drain_generation), 400);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, committed_own_generation), 408);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, holder_ref), 416);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, holder_image), 504);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, holder_reliable), 544);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, holder_terminal_drain_generation), 600);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, blocker_snapshot_ref), 608);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, blocker_snapshot_reliable), 696);
	UT_ASSERT_EQ(offsetof(PcmXLocalMembershipSlot, identity), 24);
	UT_ASSERT_EQ(offsetof(PcmXLocalMembershipSlot, tag_slot_index), 128);
	UT_ASSERT_EQ(offsetof(PcmXLocalMembershipSlot, admitted_round), 160);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, activation_retry_generation), 668);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, local_retire_gate), 672);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, allocator_lock), 768);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, master_locks), 896);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, local_locks), 17280);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, peer_frontiers), 33664);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, stats), 35200);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, outbound_targets), 35376);
	UT_ASSERT_EQ(sizeof(PcmXShmemHeader), 36400);
}

UT_TEST(test_lwlock_held_limit_is_shared_200)
{
	UT_ASSERT_EQ(LWLOCK_MAX_HELD_BY_PROC, 200);
}

UT_TEST(test_default_capacity_formulas_are_exact)
{
	PcmXShmemLayout layout;

	cluster_pcm_x_layout_compute(122, 25, 16384, 1024, &layout);
	UT_ASSERT_EQ(layout.process_capacity, 147);
	UT_ASSERT_EQ(layout.locks_per_process, 200);
	UT_ASSERT_EQ(layout.active_holder_capacity, 29400);
	UT_ASSERT_EQ(layout.holder_tag_capacity, 16384);
	UT_ASSERT_EQ(layout.node_ticket_capacity, 4704);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_MASTER_TAG].capacity, 4704);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_MASTER_TICKET].capacity, 4704);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_BLOCKER].capacity, 2048);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_LOCAL_TAG].capacity, 21088);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_LOCAL_MEMBERSHIP].capacity, 29547);
}

UT_TEST(test_exactly_five_pools_and_bounded_directories)
{
	PcmXShmemLayout layout;

	cluster_pcm_x_layout_compute(122, 25, 16384, 1024, &layout);
	UT_ASSERT_EQ(layout.pool_count, 5);
	UT_ASSERT_EQ(PCM_X_POOL_COUNT, 5);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_MASTER_TAG].directory_capacity,
				 layout.pools[PCM_X_POOL_MASTER_TAG].capacity * 2);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_MASTER_TICKET].directory_capacity, 0);
	UT_ASSERT_EQ(layout.master_ticket_directories.prehandle_capacity,
				 layout.pools[PCM_X_POOL_MASTER_TICKET].capacity * 2);
	UT_ASSERT_EQ(layout.master_ticket_directories.handle_capacity,
				 layout.pools[PCM_X_POOL_MASTER_TICKET].capacity * 2);
	UT_ASSERT_EQ(layout.master_ticket_directories.retire_capacity,
				 layout.pools[PCM_X_POOL_MASTER_TICKET].capacity * 2);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_BLOCKER].directory_capacity, 0);
	UT_ASSERT_EQ(layout.pools[PCM_X_POOL_LOCAL_TAG].directory_capacity,
				 layout.pools[PCM_X_POOL_LOCAL_TAG].capacity * 2);
	UT_ASSERT_EQ(layout.local_wait.directory_capacity, layout.local_wait.capacity * 2);
	UT_ASSERT_EQ(layout.local_holder.directory_capacity, layout.local_holder.capacity * 2);
}

UT_TEST(test_layout_v12_records_transfer_and_terminal_frontiers)
{
	PcmXShmemLayout layout;

	cluster_pcm_x_layout_compute(122, 25, 16384, 1024, &layout);
	UT_ASSERT_EQ(layout.version, 12);
	UT_ASSERT_EQ(layout.lock_partition_count, PCM_X_LOCK_PARTITIONS);
	UT_ASSERT_EQ(layout.lwlock_count, PCM_X_LWLOCK_COUNT);
	UT_ASSERT_EQ(sizeof(PcmXPeerFrontier), 48);
	UT_ASSERT_EQ(sizeof(PcmXOutboundTargetFrontier), 32);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, peer_frontiers), 33664);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, stats), 35200);
	UT_ASSERT_EQ(offsetof(PcmXShmemHeader, outbound_targets), 35376);
}

UT_TEST(test_offsets_are_aligned_ordered_and_bounded)
{
	PcmXShmemLayout layout;
	Size cursor;
	int pool;

	cluster_pcm_x_layout_compute(122, 25, 16384, 1024, &layout);
	cursor = layout.header_size;
	for (pool = 0; pool < PCM_X_POOL_COUNT; pool++) {
		const PcmXPoolLayout *p = &layout.pools[pool];

		UT_ASSERT_EQ(p->slots_offset % MAXIMUM_ALIGNOF, 0);
		UT_ASSERT(p->slots_offset >= cursor);
		cursor = p->slots_offset + p->capacity * p->slot_size;
		if (pool == PCM_X_POOL_MASTER_TICKET) {
			UT_ASSERT_EQ(layout.master_ticket_directories.prehandle_offset % MAXIMUM_ALIGNOF, 0);
			UT_ASSERT(layout.master_ticket_directories.prehandle_offset >= cursor);
			cursor = layout.master_ticket_directories.prehandle_offset
					 + layout.master_ticket_directories.prehandle_capacity
						   * sizeof(PcmXDirectoryEntry);
			UT_ASSERT_EQ(layout.master_ticket_directories.handle_offset % MAXIMUM_ALIGNOF, 0);
			UT_ASSERT(layout.master_ticket_directories.handle_offset >= cursor);
			cursor
				= layout.master_ticket_directories.handle_offset
				  + layout.master_ticket_directories.handle_capacity * sizeof(PcmXDirectoryEntry);
			UT_ASSERT_EQ(layout.master_ticket_directories.retire_offset % MAXIMUM_ALIGNOF, 0);
			UT_ASSERT(layout.master_ticket_directories.retire_offset >= cursor);
			cursor
				= layout.master_ticket_directories.retire_offset
				  + layout.master_ticket_directories.retire_capacity * sizeof(PcmXDirectoryEntry);
		}
		if (p->directory_capacity > 0) {
			UT_ASSERT_EQ(p->directory_offset % MAXIMUM_ALIGNOF, 0);
			UT_ASSERT(p->directory_offset >= cursor);
			cursor = p->directory_offset + p->directory_capacity * sizeof(PcmXDirectoryEntry);
		}
	}
	UT_ASSERT(layout.local_wait.directory_offset >= cursor);
	cursor = layout.local_wait.directory_offset
			 + layout.local_wait.directory_capacity * sizeof(PcmXDirectoryEntry);
	UT_ASSERT(layout.local_holder.directory_offset >= cursor);
	cursor = layout.local_holder.directory_offset
			 + layout.local_holder.directory_capacity * sizeof(PcmXDirectoryEntry);
	UT_ASSERT(layout.total_size >= cursor);
	UT_ASSERT_EQ(layout.total_size % MAXIMUM_ALIGNOF, 0);
}

UT_TEST(test_membership_wait_and_holder_partitions_do_not_overlap)
{
	PcmXShmemLayout layout;
	const PcmXPoolLayout *membership;

	cluster_pcm_x_layout_compute(122, 25, 16384, 1024, &layout);
	membership = &layout.pools[PCM_X_POOL_LOCAL_MEMBERSHIP];
	UT_ASSERT_EQ(layout.local_wait.first_slot_index, 0);
	UT_ASSERT_EQ(layout.local_wait.capacity, layout.process_capacity);
	UT_ASSERT_EQ(layout.local_holder.first_slot_index, layout.process_capacity);
	UT_ASSERT_EQ(layout.local_holder.capacity, layout.active_holder_capacity);
	UT_ASSERT_EQ(layout.local_wait.slots_offset, membership->slots_offset);
	UT_ASSERT_EQ(layout.local_holder.slots_offset,
				 membership->slots_offset
					 + layout.local_holder.first_slot_index * membership->slot_size);
	UT_ASSERT_EQ(layout.local_wait.capacity + layout.local_holder.capacity, membership->capacity);
}

UT_TEST(test_generation_zero_advances_without_being_a_sentinel)
{
	uint64 next = UINT64_MAX;

	UT_ASSERT(cluster_pcm_x_generation_next(0, &next));
	UT_ASSERT_EQ(next, 1);
}

UT_TEST(test_generation_max_never_wraps)
{
	uint64 next = 77;

	UT_ASSERT(!cluster_pcm_x_generation_next(UINT64_MAX, &next));
	UT_ASSERT_EQ(next, 77);
}

UT_TEST(test_slot_generation_seqlock_crosses_u32_rollover)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *free_slot;
	PcmXSlotHeader *reserved_slot;
	PcmXSlotRef ref;
	Size free_index;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	free_index = header->allocator[PCM_X_ALLOC_MASTER_TAG].free_head;
	free_slot = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, free_index);
	test_set_slot_generation(free_slot, UINT64_C(0xffffffff));
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_MASTER_TAG, &ref, &reserved_slot),
				 PCM_X_ALLOC_OK);
	UT_ASSERT(reserved_slot == free_slot);
	UT_ASSERT_EQ(ref.slot_generation, UINT64_C(0x100000000));
	UT_ASSERT_EQ(test_slot_generation(reserved_slot), UINT64_C(0x100000000));
	UT_ASSERT_EQ(pg_atomic_read_u32(&reserved_slot->generation_change_seq) & 1U, 0);
}

typedef struct PcmXGenerationTearRace {
	PcmXSlotHeader *slot;
	pg_atomic_uint32 start;
	pg_atomic_uint32 half_written;
	pg_atomic_uint32 reader_checked;
	pg_atomic_uint32 reader_attempts;
	pg_atomic_uint32 half_write_overlaps;
	pg_atomic_uint32 stop;
} PcmXGenerationTearRace;

static void *
pcm_x_generation_tear_writer(void *arg)
{
	PcmXGenerationTearRace *race = (PcmXGenerationTearRace *)arg;
	uint32 odd;

	while (pg_atomic_read_u32(&race->start) == 0)
		;
	odd = pg_atomic_read_u32(&race->slot->generation_change_seq) | 1U;
	pg_atomic_write_u32(&race->slot->generation_change_seq, odd);
	pg_write_barrier();
	race->slot->slot_generation_lo = UINT32_C(0x44444444);
	pg_write_barrier();
	pg_atomic_write_u32(&race->half_written, 1);
	while (pg_atomic_read_u32(&race->reader_checked) == 0)
		;
	pg_memory_barrier();
	race->slot->slot_generation_hi = UINT32_C(0x33333333);
	pg_write_barrier();
	pg_atomic_write_u32(&race->slot->generation_change_seq, odd + 1U);
	pg_atomic_write_u32(&race->stop, 1);
	return NULL;
}

UT_TEST(test_slot_generation_revalidate_never_accepts_torn_pair)
{
	PcmXGenerationTearRace race;
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload request;
	PcmXSlotRef mixed;
	PcmXSlotHeader *resolved;
	uint64 raw_generation;
	uint32 partition;
	pthread_t writer;
	int rc;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(611, 0, 2, UINT64_C(7611)), UINT64_C(81), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	test_set_slot_generation(&tag_slot->slot, UINT64CONST(0x1111111122222222));
	mixed = admission.tag_slot;
	mixed.slot_generation = UINT64CONST(0x1111111144444444);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&request.identity.tag));

	race.slot = &tag_slot->slot;
	pg_atomic_init_u32(&race.start, 0);
	pg_atomic_init_u32(&race.half_written, 0);
	pg_atomic_init_u32(&race.reader_checked, 0);
	pg_atomic_init_u32(&race.reader_attempts, 0);
	pg_atomic_init_u32(&race.half_write_overlaps, 0);
	pg_atomic_init_u32(&race.stop, 0);
	rc = pthread_create(&writer, NULL, pcm_x_generation_tear_writer, &race);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		return;
	pg_atomic_write_u32(&race.start, 1);
	while (pg_atomic_read_u32(&race.half_written) == 0)
		;
	pg_read_barrier();
	UT_ASSERT((pg_atomic_read_u32(&tag_slot->slot.generation_change_seq) & 1U) != 0);
	raw_generation
		= ((uint64)tag_slot->slot.slot_generation_hi << 32) | tag_slot->slot.slot_generation_lo;
	UT_ASSERT_EQ(raw_generation, mixed.slot_generation);
	pg_atomic_write_u32(&race.half_write_overlaps, 1);
	pg_atomic_write_u32(&race.reader_attempts, 1);
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	resolved = cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, mixed, PCM_X_TAG_LIVE,
												 &request.identity.tag,
												 cluster_pcm_x_tag_hash(&request.identity.tag));
	LWLockRelease(&header->master_locks[partition].lock);
	UT_ASSERT_NULL(resolved);
	UT_ASSERT((pg_atomic_read_u32(&tag_slot->slot.generation_change_seq) & 1U) != 0);
	pg_write_barrier();
	pg_atomic_write_u32(&race.reader_checked, 1);
	rc = pthread_join(writer, NULL);
	UT_ASSERT_EQ(rc, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race.reader_attempts), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race.half_write_overlaps), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race.stop), 1);
	UT_ASSERT_EQ(test_slot_generation(&tag_slot->slot), UINT64CONST(0x3333333344444444));
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_slot_generation_writer_in_progress_is_retryable)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload request;
	PcmXSlotHeader *resolved;
	uint32 partition;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(612, 0, 2, UINT64_C(7612)), UINT64_C(82), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	pg_atomic_write_u32(&tag_slot->slot.generation_change_seq,
						pg_atomic_read_u32(&tag_slot->slot.generation_change_seq) | 1U);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&request.identity.tag));
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	resolved = cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, admission.tag_slot,
												 PCM_X_TAG_LIVE, &request.identity.tag,
												 cluster_pcm_x_tag_hash(&request.identity.tag));
	LWLockRelease(&header->master_locks[partition].lock);
	UT_ASSERT_NULL(resolved);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_allocator_exact_release_reuses_with_next_generation)
{
	PcmXShmemHeader *header;
	PcmXSlotRef first;
	PcmXSlotRef second;
	PcmXSlotHeader *slot;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	slot = reserve_slot(PCM_X_ALLOC_MASTER_TAG, &first);
	UT_ASSERT_EQ(first.slot_index, 0);
	UT_ASSERT_EQ(first.slot_generation, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].high_water, 1);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TAG, first,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].high_water, 1);
	UT_ASSERT_EQ(test_slot_state(slot), PCM_X_SLOT_FREE);
	(void)reserve_slot(PCM_X_ALLOC_MASTER_TAG, &second);
	UT_ASSERT_EQ(second.slot_index, first.slot_index);
	UT_ASSERT_EQ(second.slot_generation, first.slot_generation + 1);
	first.slot_generation--;
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TAG, first,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_STALE_REF);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
}

UT_TEST(test_allocator_generation_max_head_middle_tail_preserve_chain)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *slot;
	PcmXSlotRef ref;
	Size capacity;
	Size i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	slot = pool_slot_header(header, PCM_X_POOL_LOCAL_MEMBERSHIP, 0);
	test_set_slot_generation(slot, UINT64_MAX);
	(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
	UT_ASSERT_EQ(ref.slot_index, 1);
	UT_ASSERT_EQ(test_slot_state(slot), PCM_X_SLOT_GENERATION_EXHAUSTED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].generation_exhausted, 1);

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	test_set_slot_generation(pool_slot_header(header, PCM_X_POOL_LOCAL_MEMBERSHIP, 1), UINT64_MAX);
	(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
	UT_ASSERT_EQ(ref.slot_index, 0);
	(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
	UT_ASSERT_EQ(ref.slot_index, 2);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].generation_exhausted, 1);

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	capacity = header->layout.local_wait.capacity;
	test_set_slot_generation(pool_slot_header(header, PCM_X_POOL_LOCAL_MEMBERSHIP, capacity - 1),
							 UINT64_MAX);
	for (i = 0; i < capacity - 1; i++) {
		(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
		UT_ASSERT_EQ(ref.slot_index, i);
	}
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_LOCAL_WAIT, &ref, &slot),
				 PCM_X_ALLOC_NO_CAPACITY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].free_head, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].generation_exhausted, 1);
}

UT_TEST(test_allocator_lost_free_head_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState *allocator;
	Size capacity;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	allocator = &header->allocator[PCM_X_ALLOC_MASTER_TAG];
	capacity = header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity;
	UT_ASSERT(allocator->used + allocator->generation_exhausted < capacity);
	allocator->free_head = PCM_X_INVALID_SLOT_INDEX;
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
}

UT_TEST(test_allocator_generation_exhausted_overflow_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState *allocator;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	allocator = &header->allocator[PCM_X_ALLOC_MASTER_TAG];
	allocator->generation_exhausted = header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity + 1;
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
}

UT_TEST(test_allocator_live_free_head_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *head;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 0);
	test_set_slot_state(head, PCM_X_SLOT_RESERVED_NONVISIBLE);
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
}

UT_TEST(test_allocator_free_slot_with_packed_flags_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *head;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 0);
	pg_atomic_write_u32(&head->state_flags,
						PCM_X_SLOT_FREE
							| (PCM_X_LOCAL_TAG_F_REVOKE_BARRIER << PCM_X_SLOT_FLAGS_SHIFT));
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
	UT_ASSERT_EQ(test_slot_state(head), PCM_X_SLOT_FREE);
	UT_ASSERT_EQ(test_slot_flags(head), PCM_X_LOCAL_TAG_F_REVOKE_BARRIER);
}

UT_TEST(test_allocator_self_loop_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *head;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 0);
	head->next_free = 0;
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
}

UT_TEST(test_allocator_invalid_next_is_corrupt)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *head;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 0);
	head->next_free = header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity;
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
}

UT_TEST(test_allocator_truncated_free_chain_is_corrupt_before_mutation)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState *allocator;
	PcmXSlotHeader *head;
	PcmXSlotHeader *slot = (PcmXSlotHeader *)(uintptr_t)1;
	PcmXSlotRef ref = { 0, UINT64_C(99) };
	Size free_head_before;
	Size used_before;
	uint64 generation_before;
	uint32 state_before;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	allocator = &header->allocator[PCM_X_ALLOC_MASTER_TAG];
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, allocator->free_head);
	UT_ASSERT(header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity - allocator->used
				  - allocator->generation_exhausted
			  > 1);
	head->next_free = PCM_X_INVALID_SLOT_INDEX;
	free_head_before = allocator->free_head;
	used_before = allocator->used;
	generation_before = test_slot_generation(head);
	state_before = test_slot_state(head);

	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_MASTER_TAG, &ref, &slot),
				 PCM_X_ALLOC_CORRUPT);
	UT_ASSERT_EQ(ref.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ref.slot_generation, 0);
	UT_ASSERT_NULL(slot);
	UT_ASSERT_EQ(allocator->free_head, free_head_before);
	UT_ASSERT_EQ(allocator->used, used_before);
	UT_ASSERT_EQ(test_slot_generation(head), generation_before);
	UT_ASSERT_EQ(test_slot_state(head), state_before);
}

UT_TEST(test_allocator_nonfree_successor_is_corrupt_before_reserve)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState *allocator;
	PcmXSlotHeader *head;
	PcmXSlotHeader *successor;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	allocator = &header->allocator[PCM_X_ALLOC_MASTER_TAG];
	head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 0);
	successor = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 1);
	UT_ASSERT_EQ(head->next_free, 1);
	test_set_slot_state(successor, PCM_X_SLOT_RESERVED_NONVISIBLE);
	assert_corrupt_reserve_has_no_ref(PCM_X_ALLOC_MASTER_TAG);
	UT_ASSERT_EQ(allocator->free_head, 0);
	UT_ASSERT_EQ(allocator->used, 0);
	UT_ASSERT_EQ(test_slot_generation(head), 0);
	UT_ASSERT_EQ(test_slot_state(head), PCM_X_SLOT_FREE);
}

UT_TEST(test_allocator_release_rejects_nonfree_old_head)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState *allocator;
	PcmXSlotHeader *released;
	PcmXSlotHeader *old_head;
	PcmXSlotRef ref;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	allocator = &header->allocator[PCM_X_ALLOC_MASTER_TAG];
	released = reserve_slot(PCM_X_ALLOC_MASTER_TAG, &ref);
	UT_ASSERT_EQ(allocator->free_head, 1);
	old_head = pool_slot_header(header, PCM_X_POOL_MASTER_TAG, 1);
	test_set_slot_state(old_head, PCM_X_SLOT_RESERVED_NONVISIBLE);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TAG, ref,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_CORRUPT);
	UT_ASSERT_EQ(allocator->free_head, 1);
	UT_ASSERT_EQ(allocator->used, 1);
	UT_ASSERT_EQ(test_slot_state(released), PCM_X_SLOT_RESERVED_NONVISIBLE);
}

UT_TEST(test_membership_wait_and_holder_allocators_never_borrow)
{
	PcmXShmemHeader *header;
	PcmXSlotHeader *slot;
	PcmXSlotRef ref;
	Size i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < header->layout.local_wait.capacity; i++)
		(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_LOCAL_WAIT, &ref, &slot),
				 PCM_X_ALLOC_NO_CAPACITY);
	(void)reserve_slot(PCM_X_ALLOC_LOCAL_HOLDER, &ref);
	UT_ASSERT_EQ(ref.slot_index, header->layout.local_holder.first_slot_index);

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < header->layout.local_holder.capacity; i++)
		(void)reserve_slot(PCM_X_ALLOC_LOCAL_HOLDER, &ref);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_LOCAL_HOLDER, &ref, &slot),
				 PCM_X_ALLOC_NO_CAPACITY);
	(void)reserve_slot(PCM_X_ALLOC_LOCAL_WAIT, &ref);
	UT_ASSERT_EQ(ref.slot_index, 0);
}

UT_TEST(test_directory_collision_uses_exact_key_not_hash_bucket)
{
	PcmXShmemHeader *header;
	PcmXPrehandleKey keys[2];
	PcmXSlotRef refs[2];
	PcmXSlotRef found;
	PcmXMasterTicketSlot *tickets[2];
	Size capacity;
	Size ignored;
	int i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	(void)directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &capacity);
	find_prehandles_for_bucket(capacity, 0, 2, keys);
	for (i = 0; i < 2; i++) {
		tickets[i] = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &refs[i]);
		tickets[i]->prehandle = keys[i];
		UT_ASSERT_EQ(cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &keys[i],
													refs[i], &found),
					 PCM_X_DIRECTORY_OK);
	}
	UT_ASSERT(!slot_refs_equal(refs[0], refs[1]));
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(
			cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &keys[i], &found),
			PCM_X_DIRECTORY_OK);
		UT_ASSERT(slot_refs_equal(found, refs[i]));
	}
	(void)directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &ignored);
}

UT_TEST(test_directory_backshift_preserves_wrapped_probe_chain)
{
	PcmXShmemHeader *header;
	PcmXDirectoryEntry *entries;
	PcmXPrehandleKey keys[3];
	PcmXSlotRef refs[3];
	PcmXSlotRef found;
	Size capacity;
	int i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	entries = directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &capacity);
	find_prehandles_for_bucket(capacity, capacity - 1, 3, keys);
	for (i = 0; i < 3; i++) {
		PcmXMasterTicketSlot *ticket
			= (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &refs[i]);

		ticket->prehandle = keys[i];
		UT_ASSERT_EQ(cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &keys[i],
													refs[i], &found),
					 PCM_X_DIRECTORY_OK);
	}
	UT_ASSERT_EQ(entries[capacity - 1].slot_index, refs[0].slot_index);
	UT_ASSERT_EQ(entries[0].slot_index, refs[1].slot_index);
	UT_ASSERT_EQ(entries[1].slot_index, refs[2].slot_index);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &keys[1], refs[1]),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(entries[0].slot_index, refs[2].slot_index);
	UT_ASSERT_EQ(entries[1].state, PCM_X_DIRECTORY_EMPTY);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &keys[2], &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, refs[2]));
}

UT_TEST(test_directory_stale_generation_and_delete_ref_mismatch_fail_closed)
{
	PcmXPrehandleKey key = { UINT64_C(71), UINT64_C(9) };
	PcmXSlotRef first;
	PcmXSlotRef other;
	PcmXSlotRef found;
	PcmXMasterTicketSlot *ticket;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &first);
	ticket->prehandle = key;
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, first, &found),
		PCM_X_DIRECTORY_OK);
	(void)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &other);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, other),
		PCM_X_DIRECTORY_STALE_REF);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, &found),
				 PCM_X_DIRECTORY_OK);
	test_set_slot_generation(&ticket->slot, test_slot_generation(&ticket->slot) + 1);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, &found),
				 PCM_X_DIRECTORY_STALE_REF);
}

UT_TEST(test_directory_full_never_overwrites_existing_entries)
{
	PcmXShmemHeader *header;
	PcmXDirectoryEntry *entries;
	PcmXPrehandleKey first_key = { UINT64_C(71), UINT64_C(1) };
	PcmXPrehandleKey new_key = { UINT64_C(71), UINT64_C(2) };
	PcmXSlotRef first;
	PcmXSlotRef candidate;
	PcmXSlotRef found;
	PcmXMasterTicketSlot *ticket;
	uint64 first_hash;
	Size capacity;
	Size i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	entries = directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &capacity);
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &first);
	ticket->prehandle = first_key;
	UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &first_key,
											   &first_hash));
	for (i = 0; i < capacity; i++) {
		entries[i].key_hash = first_hash;
		entries[i].slot_index = first.slot_index;
		entries[i].slot_generation = first.slot_generation;
		entries[i].state = PCM_X_DIRECTORY_OCCUPIED;
	}
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &candidate);
	ticket->prehandle = new_key;
	UT_ASSERT_EQ(cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &new_key,
												candidate, &found),
				 PCM_X_DIRECTORY_FULL);
	for (i = 0; i < capacity; i++)
		UT_ASSERT_EQ(entries[i].slot_index, first.slot_index);
}

UT_TEST(test_master_ticket_prehandle_and_handle_directories_are_independent)
{
	PcmXPrehandleKey prehandle = { UINT64_C(81), UINT64_C(3) };
	PcmXTicketHandle handle = { UINT64_C(9001), UINT64_C(5) };
	PcmXSlotRef ref;
	PcmXSlotRef found;
	PcmXMasterTicketSlot *ticket;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &ref);
	ticket->prehandle = prehandle;
	ticket->ref.handle = handle;
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &prehandle, ref, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_HANDLE, &handle, ref, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &prehandle, ref),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &prehandle, &found),
		PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &handle, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, ref));
}

UT_TEST(test_local_holder_directory_keeps_backend_identities_independent)
{
	BufferTag tag = make_tag(57);
	PcmXLocalHolderKey keys[2];
	PcmXLocalMembershipSlot *slots[2];
	PcmXSlotRef refs[2];
	PcmXSlotRef found;
	int i;

	memset(keys, 0, sizeof(keys));
	for (i = 0; i < 2; i++) {
		keys[i].identity.tag = tag;
		keys[i].identity.node_id = 2;
		keys[i].identity.procno = 10 + i;
		keys[i].identity.xid = 100 + i;
		keys[i].identity.cluster_epoch = 71;
		keys[i].identity.request_id = 900 + i;
		keys[i].identity.wait_seq = 11 + i;
		keys[i].identity.base_own_generation = 21;
		keys[i].buffer_id = 7;
	}

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	for (i = 0; i < 2; i++) {
		slots[i] = (PcmXLocalMembershipSlot *)reserve_slot(PCM_X_ALLOC_LOCAL_HOLDER, &refs[i]);
		slots[i]->identity = keys[i].identity;
		slots[i]->buffer_id = keys[i].buffer_id;
		UT_ASSERT_EQ(
			cluster_pcm_x_directory_insert(PCM_X_DIR_LOCAL_HOLDER, &keys[i], refs[i], &found),
			PCM_X_DIRECTORY_OK);
	}
	UT_ASSERT(!slot_refs_equal(refs[0], refs[1]));
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_HOLDER, &keys[i], &found),
					 PCM_X_DIRECTORY_OK);
		UT_ASSERT(slot_refs_equal(found, refs[i]));
	}
	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_LOCAL_HOLDER, &keys[0], refs[0]),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_HOLDER, &keys[0], &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_HOLDER, &keys[1], &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, refs[1]));
}

UT_TEST(test_local_holder_registry_returns_canonical_exact_snapshot)
{
	PcmXLocalHolderKey keys[3];
	PcmXLocalHolderHandle handles[3];
	PcmXLocalHolderHandle duplicate;
	PcmXLocalHolderHandle snapshot_holders[3];
	PcmXLocalHolderHandle short_output;
	PcmXLocalHolderHandle short_before;
	PcmXLocalHolderHandle churn_handle;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalHolderSnapshot short_snapshot;
	PcmXLocalHolderKey churn_key;
	int registration_order[3] = { 2, 0, 1 };
	int i;

	keys[0] = make_local_holder_key(758, 0, 11, UINT64_C(71001), 1);
	keys[1] = make_local_holder_key(758, 0, 12, UINT64_C(71002), 2);
	keys[2] = make_local_holder_key(758, 1, 3, UINT64_C(71003), 3);
	init_active_pcm_x(UINT64_C(7201));

	for (i = 0; i < 3; i++)
		UT_ASSERT_EQ(register_active_local_holder(&keys[registration_order[i]],
												  &handles[registration_order[i]]),
					 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(register_active_local_holder(&keys[0], &duplicate), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(slot_refs_equal(duplicate.tag_slot, handles[0].tag_slot));
	UT_ASSERT(slot_refs_equal(duplicate.holder_slot, handles[0].holder_slot));

	memset(snapshot_holders, 0, sizeof(snapshot_holders));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&keys[0].identity.tag, snapshot_holders,
													 lengthof(snapshot_holders), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 3);
	UT_ASSERT_EQ(snapshot.holder_set_generation, 3);
	UT_ASSERT(slot_refs_equal(snapshot.tag_slot, handles[0].tag_slot));
	/* Canonical 4-tuple order is independent of the intrusive-list order. */
	for (i = 0; i < 3; i++) {
		UT_ASSERT(memcmp(&snapshot_holders[i].key, &keys[i], sizeof(keys[i])) == 0);
		UT_ASSERT(slot_refs_equal(snapshot_holders[i].holder_slot, handles[i].holder_slot));
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot_revalidate(&keys[0].identity.tag, &snapshot),
				 PCM_X_QUEUE_OK);
	churn_key = make_local_holder_key(758, 1, 4, UINT64_C(71004), 4);
	UT_ASSERT_EQ(register_active_local_holder(&churn_key, &churn_handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&churn_handle), PCM_X_QUEUE_OK);
	/* Equal cardinality is not equal authority: balanced churn advances the set token. */
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot_revalidate(&keys[0].identity.tag, &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&keys[0].identity.tag, snapshot_holders,
													 lengthof(snapshot_holders), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 3);
	UT_ASSERT_EQ(snapshot.holder_set_generation, 5);

	memset(&short_output, 0xa5, sizeof(short_output));
	short_before = short_output;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&keys[0].identity.tag, &short_output, 1,
													 &short_snapshot),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(short_snapshot.holder_count, 3);
	UT_ASSERT_EQ(short_snapshot.holder_set_generation, 5);
	UT_ASSERT(memcmp(&short_output, &short_before, sizeof(short_output)) == 0);

	/* The first exact removal is from the middle of the intrusive holder list. */
	UT_ASSERT_EQ(release_active_local_holder(&handles[0]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot_revalidate(&keys[0].identity.tag, &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handles[0]), PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(release_active_local_holder(&handles[1]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&handles[2]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&keys[0].identity.tag, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 0);
	UT_ASSERT_EQ(snapshot.tag_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
}

UT_TEST(test_local_holder_stale_handle_cannot_unregister_reused_slot)
{
	PcmXLocalHolderKey key = make_local_holder_key(759, 0, 13, UINT64_C(72001), 4);
	PcmXLocalHolderHandle first;
	PcmXLocalHolderHandle second;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;

	init_active_pcm_x(UINT64_C(7202));
	UT_ASSERT_EQ(register_active_local_holder(&key, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(register_active_local_holder(&key, &second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(second.holder_slot.slot_index, first.holder_slot.slot_index);
	UT_ASSERT(second.holder_slot.slot_generation > first.holder_slot.slot_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&first), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(slot_refs_equal(resident.holder_slot, second.holder_slot));
	UT_ASSERT_EQ(release_active_local_holder(&second), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_capacity_failure_rolls_back_holder_only_tag)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey key = make_local_holder_key(760, 0, 14, UINT64_C(73001), 5);
	PcmXLocalHolderHandle handle;

	init_active_pcm_x(UINT64_C(7203));
	header = ClusterPcmXConvertShmem;
	header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].free_head = PCM_X_INVALID_SLOT_INDEX;
	header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used = header->layout.local_holder.capacity;
	header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].high_water = header->layout.local_holder.capacity;
	UT_ASSERT_EQ(register_active_local_holder(&key, &handle), PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_HOLDER), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
}

UT_TEST(test_local_holder_snapshot_corruption_fails_closed_before_copy)
{
	PcmXLocalHolderKey key = make_local_holder_key(761, 0, 15, UINT64_C(74001), 6);
	PcmXLocalHolderHandle handle;
	PcmXLocalHolderHandle output;
	PcmXLocalHolderHandle before;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalTagSlot *tag_slot;

	init_active_pcm_x(UINT64_C(7204));
	UT_ASSERT_EQ(register_active_local_holder(&key, &handle), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[handle.tag_slot.slot_index];
	tag_slot->active_holder_head_index
		= ClusterPcmXConvertShmem->layout.local_holder.first_slot_index
		  + ClusterPcmXConvertShmem->layout.local_holder.capacity;
	memset(&output, 0x5a, sizeof(output));
	before = output;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &output, 1, &snapshot),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(memcmp(&output, &before, sizeof(output)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_holder_unregister_validates_before_unlinking_corrupt_tag)
{
	PcmXLocalHolderKey key = make_local_holder_key(764, 0, 20, UINT64_C(74002), 6);
	PcmXLocalHolderHandle handle;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *holder;
	Size holder_head;
	Size holder_next;
	Size holder_previous;
	uint64 set_generation;

	init_active_pcm_x(UINT64_C(7207));
	UT_ASSERT_EQ(register_active_local_holder(&key, &handle), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[handle.tag_slot.slot_index];
	holder = &membership_slots(ClusterPcmXConvertShmem)[handle.holder_slot.slot_index];
	/* A holder-only tag cannot claim a local waiter queue head. */
	tag_slot->head_index = 0;
	holder_head = tag_slot->active_holder_head_index;
	set_generation = tag_slot->holder_set_generation;
	holder_next = holder->next_index;
	holder_previous = holder->prev_index;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, holder_head);
	UT_ASSERT_EQ(tag_slot->holder_set_generation, set_generation);
	UT_ASSERT_EQ(holder->next_index, holder_next);
	UT_ASSERT_EQ(holder->prev_index, holder_previous);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_RELEASING);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_HOLDER), 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_holder_only_tag_is_adopted_by_local_queue_without_pool_borrowing)
{
	PcmXLocalHolderKey holder_key = make_local_holder_key(762, 0, 16, UINT64_C(75001), 7);
	PcmXLocalHolderHandle holder;
	PcmXLocalHandle waiter;
	PcmXLocalHandle no_successor;
	PcmXWaitIdentity waiter_identity = make_wait_identity(762, 0, 17, UINT64_C(75002));
	PcmXLocalTagSlot *tag_slot;

	init_active_pcm_x(UINT64_C(7205));
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->master_node, -1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, 0);
	bind_local_master(1, waiter_identity.cluster_epoch, UINT64_C(7601));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&waiter_identity, 1, UINT64_C(7601), &waiter),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(waiter.tag_slot, holder.tag_slot));
	UT_ASSERT_EQ(tag_slot->master_node, 1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, UINT64_C(7601));
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 1);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);

	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&waiter, &no_successor), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&waiter), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
}

UT_TEST(test_holder_only_tag_survives_last_cancelled_waiter_detach)
{
	PcmXLocalHolderKey holder_key = make_local_holder_key(765, 0, 21, UINT64_C(77001), 6);
	PcmXWaitIdentity first_identity = make_wait_identity(765, 0, 22, UINT64_C(77002));
	PcmXWaitIdentity second_identity = make_wait_identity(765, 0, 23, UINT64_C(77003));
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle snapshot_holder;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalHandle first;
	PcmXLocalHandle second;
	PcmXLocalHandle no_successor;
	PcmXLocalTagSlot *tag_slot;

	init_active_pcm_x(UINT64_C(7208));
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, first_identity.cluster_epoch, UINT64_C(7801));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&first_identity, 1, UINT64_C(7801), &first),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&first, &no_successor), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder.tag_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_LIVE);
	UT_ASSERT_EQ(tag_slot->membership_count, 0);
	UT_ASSERT_EQ(tag_slot->master_node, -1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, 0);
	UT_ASSERT_EQ(tag_slot->next_sequence, 1);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 1);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &snapshot_holder, 1,
													 &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(memcmp(&snapshot_holder, &holder, sizeof(holder)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&second_identity, 1, UINT64_C(7801), &second),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(second.tag_slot, holder.tag_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&second, &no_successor), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
}

UT_TEST(test_local_holder_register_refuses_closed_revoke_barrier)
{
	PcmXWaitIdentity leader_identity = make_wait_identity(763, 0, 18, UINT64_C(76001));
	PcmXLocalHolderKey late_holder = make_local_holder_key(763, 0, 19, UINT64_C(76002), 0);
	PcmXLocalHandle leader;
	PcmXLocalHolderHandle rejected;
	PcmXLocalCutoff cutoff;

	init_active_pcm_x(UINT64_C(7206));
	bind_local_master(1, leader_identity.cluster_epoch, UINT64_C(7701));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, UINT64_C(7701), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&late_holder, &rejected),
				 PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_HOLDER), 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_holder_prepare_is_visible_before_content_lock)
{
	PcmXLocalHolderKey key = make_local_holder_key(766, 0, 24, UINT64_C(78001), 7);
	PcmXLocalHolderHandle handle;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalMembershipSlot *holder;

	init_active_pcm_x(UINT64_C(7209));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	holder = &membership_slots(ClusterPcmXConvertShmem)[handle.holder_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACQUIRING);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(slot_refs_equal(resident.holder_slot, handle.holder_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&handle), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_abort_acquiring_unlinks_exact)
{
	PcmXLocalHolderKey key = make_local_holder_key(776, 0, 26, UINT64_C(87001), 7);
	PcmXLocalHolderHandle handle;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalMembershipSlot *holder;

	init_active_pcm_x(UINT64_C(7214));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	holder = &membership_slots(ClusterPcmXConvertShmem)[handle.holder_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACQUIRING);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 0);
	UT_ASSERT_EQ(snapshot.tag_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_HOLDER), 0);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_TAG), 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&handle), PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_holder_abort_acquiring_rejects_live_content_states)
{
	PcmXLocalHolderKey key = make_local_holder_key(777, 0, 27, UINT64_C(87002), 6);
	PcmXLocalHolderHandle handle;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *holder;
	Size holder_head;
	uint64 holder_set_generation;

	init_active_pcm_x(UINT64_C(7215));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[handle.tag_slot.slot_index];
	holder = &membership_slots(ClusterPcmXConvertShmem)[handle.holder_slot.slot_index];
	holder_head = tag_slot->active_holder_head_index;
	holder_set_generation = tag_slot->holder_set_generation;

	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&handle), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACTIVE);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, holder_head);
	UT_ASSERT_EQ(tag_slot->holder_set_generation, holder_set_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(slot_refs_equal(resident.holder_slot, handle.holder_slot));

	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&handle), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_RELEASING);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, holder_head);
	UT_ASSERT_EQ(tag_slot->holder_set_generation, holder_set_generation);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_HOLDER), 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_abort_acquiring_loses_race_to_activate)
{
	PcmXLocalHolderKey existing_key = make_local_holder_key(779, 0, 1, UINT64_C(87004), 4);
	PcmXLocalHolderKey target_key = make_local_holder_key(779, 0, 2, UINT64_C(87005), 3);
	PcmXLocalHolderHandle existing;
	PcmXLocalHolderHandle target;
	PcmXLocalHolderHandle residents[2];
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalMembershipSlot *target_slot;
	PcmXQueueResult result;
	uint32 partition;

	init_active_pcm_x(UINT64_C(7217));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&existing_key, &existing), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&target_key, &target), PCM_X_QUEUE_OK);
	target_slot = &membership_slots(ClusterPcmXConvertShmem)[target.holder_slot.slot_index];
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&target_key.identity.tag));
	/* The third local-domain ownership check resolves target's successor, after
	 * abort has sampled ACQUIRING but before it commits the unlink. */
	holder_abort_interlock_lock = &ClusterPcmXConvertShmem->local_locks[partition].lock;
	holder_abort_interlock_target = target_slot;
	holder_abort_interlock_match = 3;
	result = cluster_pcm_x_local_holder_abort_acquiring_exact(&target);
	UT_ASSERT_EQ(result, PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_NULL(holder_abort_interlock_target);
	UT_ASSERT_EQ(test_slot_state(&target_slot->slot), PCM_XL_HOLDER_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&target_key.identity.tag, residents,
													 lengthof(residents), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 2);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	if (result == PCM_X_QUEUE_BAD_STATE) {
		UT_ASSERT_EQ(release_active_local_holder(&target), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&existing), PCM_X_QUEUE_OK);
	}
}

UT_TEST(test_local_holder_abort_acquiring_rejects_stale_aba_handle)
{
	PcmXLocalHolderKey key = make_local_holder_key(778, 0, 28, UINT64_C(87003), 5);
	PcmXLocalHolderHandle first;
	PcmXLocalHolderHandle second;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;

	init_active_pcm_x(UINT64_C(7216));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(second.holder_slot.slot_index, first.holder_slot.slot_index);
	UT_ASSERT(second.holder_slot.slot_generation > first.holder_slot.slot_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&first), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(slot_refs_equal(resident.holder_slot, second.holder_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_holder_exceptional_detach_requires_released_content_lock)
{
	PcmXLocalHolderKey key = make_local_holder_key(780, 0, 1, UINT64_C(87006), 4);
	PcmXLocalHolderHandle handle;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;
	LWLock *content_lock = BufferDescriptorGetContentLock(&fake_buffer_descriptors[4].bufferdesc);

	init_active_pcm_x(UINT64_C(7218));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	LWLockAcquire(content_lock, LW_SHARED);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&handle, content_lock),
				 PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	LWLockRelease(content_lock);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&handle, content_lock),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 0);
}

UT_TEST(test_local_holder_exceptional_detach_accepts_live_content_states)
{
	PcmXLocalHolderKey active_key = make_local_holder_key(781, 0, 2, UINT64_C(87007), 5);
	PcmXLocalHolderKey releasing_key = make_local_holder_key(782, 0, 3, UINT64_C(87008), 6);
	PcmXLocalHolderHandle active;
	PcmXLocalHolderHandle releasing;
	LWLock *active_lock = BufferDescriptorGetContentLock(&fake_buffer_descriptors[5].bufferdesc);
	LWLock *releasing_lock = BufferDescriptorGetContentLock(&fake_buffer_descriptors[6].bufferdesc);

	init_active_pcm_x(UINT64_C(7219));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&active_key, &active), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&active), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&active, active_lock),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&releasing_key, &releasing), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&releasing), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&releasing), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&releasing, releasing_lock),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_holder_exceptional_detach_rejects_stale_aba_handle)
{
	PcmXLocalHolderKey key = make_local_holder_key(783, 0, 4, UINT64_C(87009), 7);
	PcmXLocalHolderHandle first;
	PcmXLocalHolderHandle second;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;
	LWLock *content_lock = BufferDescriptorGetContentLock(&fake_buffer_descriptors[7].bufferdesc);

	init_active_pcm_x(UINT64_C(7220));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&first, content_lock),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(second.holder_slot.slot_index, first.holder_slot.slot_index);
	UT_ASSERT(second.holder_slot.slot_generation > first.holder_slot.slot_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&first, content_lock),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&key.identity.tag, &resident, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_abort_acquiring_exact(&second), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_cleanup_lock_errors_return_corrupt_without_rethrow)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey acquiring_key = make_local_holder_key(784, 0, 5, UINT64_C(87010), 1);
	PcmXLocalHolderKey active_key = make_local_holder_key(785, 0, 6, UINT64_C(87011), 2);
	PcmXLocalHolderHandle acquiring;
	PcmXLocalHolderHandle active;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *holder;
	LWLock *content_lock = BufferDescriptorGetContentLock(&fake_buffer_descriptors[2].bufferdesc);
	volatile PcmXQueueResult result = PCM_X_QUEUE_INVALID;
	volatile bool caught = false;

	/* Abort cleanup runs inside an outer PG_CATCH.  A lock-acquire exception
	 * must be converted to CORRUPT rather than replacing the original ERROR;
	 * the exact ACQUIRING evidence remains for fail-closed recovery. */
	init_active_pcm_x(UINT64_C(7221));
	header = ClusterPcmXConvertShmem;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&acquiring_key, &acquiring), PCM_X_QUEUE_OK);
	holder = &membership_slots(header)[acquiring.holder_slot.slot_index];
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_SHARED, 1);
	PG_TRY();
	{
		result = cluster_pcm_x_local_holder_abort_acquiring_exact(&acquiring);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(result, PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACQUIRING);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* Exceptional cleanup has the same no-second-error contract for ACTIVE
	 * evidence after the content lock has already been released. */
	init_active_pcm_x(UINT64_C(7222));
	header = ClusterPcmXConvertShmem;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&active_key, &active), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&active), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[active.tag_slot.slot_index];
	holder = &membership_slots(header)[active.holder_slot.slot_index];
	result = PCM_X_QUEUE_INVALID;
	caught = false;
	/* Reach the post-domain handoff: holder/tag are already DETACHING and the
	 * guarded final allocator acquire must release the admission gate before
	 * the cleanup wrapper swallows its secondary ERROR. */
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 1);
	PG_TRY();
	{
		result = cluster_pcm_x_local_holder_exceptional_detach_exact(&active, content_lock);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(result, PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_DETACHING);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_HOLDER), 1);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_holder_content_window_transitions_are_lock_free)
{
	PcmXLocalHolderKey key = make_local_holder_key(767, 0, 25, UINT64_C(79001), 6);
	PcmXLocalHolderHandle handle;
	PcmXLocalMembershipSlot *holder;
	int allocator_before;
	int domain_before;

	init_active_pcm_x(UINT64_C(7210));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	holder = &membership_slots(ClusterPcmXConvertShmem)[handle.holder_slot.slot_index];
	allocator_before = allocator_lock_acquire_count;
	domain_before = domain_lock_acquire_count;
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&handle), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_RELEASING);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&handle), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(allocator_lock_acquire_count, allocator_before);
	UT_ASSERT_EQ(domain_lock_acquire_count, domain_before);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_finish_requires_post_content_releasing_state)
{
	PcmXLocalHolderKey key = make_local_holder_key(768, 0, 26, UINT64_C(80001), 5);
	PcmXLocalHolderHandle handle;

	init_active_pcm_x(UINT64_C(7211));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&handle), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&handle), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_gate_contention_is_retryable_without_fail_closed)
{
	PcmXLocalHolderKey first_key = make_local_holder_key(769, 0, 27, UINT64_C(81001), 4);
	PcmXLocalHolderKey late_key = make_local_holder_key(769, 0, 28, UINT64_C(81002), 3);
	PcmXLocalHolderHandle first;
	PcmXLocalHolderHandle rejected;
	PcmXLocalTagSlot *tag_slot;
	uint32 state_flags;

	init_active_pcm_x(UINT64_C(7212));
	UT_ASSERT_EQ(register_active_local_holder(&first_key, &first), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[first.tag_slot.slot_index];
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&late_key, &rejected), PCM_X_QUEUE_GATE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);
	UT_ASSERT_EQ(directory_occupied_count(ClusterPcmXConvertShmem, PCM_X_DIR_LOCAL_HOLDER), 1);
	pg_atomic_write_u32(&tag_slot->slot.state_flags, state_flags);
	UT_ASSERT_EQ(release_active_local_holder(&first), PCM_X_QUEUE_OK);
}

UT_TEST(test_local_holder_barrier_counts_acquiring_active_and_releasing)
{
	PcmXWaitIdentity leader_identity = make_wait_identity(770, 0, 1, UINT64_C(82001));
	PcmXLocalHolderKey holder_key = make_local_holder_key(770, 0, 2, UINT64_C(82002), 2);
	PcmXLocalHandle leader;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle resident;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalHolderSnapshot empty;
	PcmXLocalCutoff cutoff;
	PcmXLocalMembershipSlot *holder_slot;

	init_active_pcm_x(UINT64_C(7213));
	bind_local_master(1, leader_identity.cluster_epoch, UINT64_C(8201));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, UINT64_C(8201), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&holder_key, &holder), PCM_X_QUEUE_OK);
	holder_slot = &membership_slots(ClusterPcmXConvertShmem)[holder.holder_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_HOLDER_ACQUIRING);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &resident, 1, &snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_snapshot_revalidate(&holder_key.identity.tag, &snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_HOLDER_ACTIVE);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_snapshot_revalidate(&holder_key.identity.tag, &snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_HOLDER_RELEASING);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_snapshot_revalidate(&holder_key.identity.tag, &snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_snapshot_revalidate(&holder_key.identity.tag, &snapshot),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, NULL, 0, &empty),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(empty.holder_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_probe_freeze_is_count_first_exact_for_holder_only_tag)
{
	PcmXLocalHolderKey holder_key = make_local_holder_key(773, 0, 4, UINT64_C(84001), 1);
	PcmXLocalHolderKey late_key = make_local_holder_key(773, 0, 5, UINT64_C(84002), 2);
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle late;
	PcmXLocalHolderHandle copied;
	PcmXLocalHandle writer;
	PcmXLocalHolderSnapshot count_snapshot;
	PcmXLocalHolderSnapshot copied_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalCutoff cutoff;
	PcmXTicketRef probe_ref;
	PcmXTicketRef other_ref;
	PcmXRevokePayload revoke;
	PcmXGrantPayload ready;
	PcmXGrantPayload replay;
	PcmXDrainPollPayload poll;
	PcmXRetirePayload retire;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef found;
	uint64 master_session = UINT64_C(8301);

	init_active_pcm_x(UINT64_C(7215));
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(773, 2, 7, UINT64_C(84003));
	probe_ref.handle.ticket_id = UINT64_C(91);
	probe_ref.handle.queue_generation = UINT64_C(3);

	/* The first count-only call persists the exact freeze even though the
	 * caller has not allocated its bounded copy array yet. */
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &count_snapshot),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(count_snapshot.holder_count, 1);
	UT_ASSERT(slot_refs_equal(count_snapshot.tag_slot, holder.tag_slot));
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder.tag_slot.slot_index];
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT_EQ(tag_slot->cutoff_sequence, 0);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 0);
	UT_ASSERT_EQ(tag_slot->master_node, 1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, master_session);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &probe_ref));
	UT_ASSERT_EQ(tag_slot->blocker_set_generation, 0);

	memset(&copied, 0, sizeof(copied));
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &copied, 1, &copied_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&copied_snapshot, &count_snapshot, sizeof(copied_snapshot)) == 0);
	UT_ASSERT(memcmp(&copied, &holder, sizeof(copied)) == 0);

	other_ref = probe_ref;
	other_ref.handle.ticket_id++;
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&other_ref, 1, master_session,
																 &copied, 1, &copied_snapshot),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&late_key, &late), PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &holder_key.identity.tag, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, holder.tag_slot));
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &probe_ref));
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	/* Re-snapshot after the last local pin leaves, then drive the retained
	 * holder-only lane through its exact wire lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &count_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(count_snapshot.holder_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&count_snapshot, NULL, 0, NULL, 0,
																&blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	memset(&revoke, 0, sizeof(revoke));
	revoke.ref = probe_ref;
	revoke.ref.grant_generation = UINT64_C(1);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, UINT64_C(201), &revoke.image_id));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_OK);
	memset(&ready, 0, sizeof(ready));
	ready.ref = revoke.ref;
	ready.image.image_id = revoke.image_id;
	ready.image.source_own_generation = UINT64_C(61);
	ready.image.page_scn = UINT64_C(62);
	ready.image.page_lsn = UINT64_C(63);
	ready.image.source_node = 0;
	ready.image.page_checksum = UINT32_C(64);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_image_ready_arm_exact(&ready, 1, master_session, &replay),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(
					 &holder_key.identity.tag, holder_key.identity.cluster_epoch, 1, master_session,
					 &cutoff),
				 PCM_X_QUEUE_OK);
	memset(&poll, 0, sizeof(poll));
	poll.ref = revoke.ref;
	poll.drain_generation = UINT64_C(65);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session), PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = holder_key.identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = revoke.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session),
				 PCM_X_QUEUE_OK);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);

	/* A cancelled writer remains as unlinked terminal evidence until detach.
	 * A concurrent holder PROBE must wait without publishing a freeze ref or
	 * fail-closing the runtime. */
	init_active_pcm_x(UINT64_C(7217));
	bind_local_master(1, UINT64_C(9), UINT64_C(8302));
	probe_ref.identity = make_wait_identity(782, 2, 9, UINT64_C(84004));
	probe_ref.handle.ticket_id = UINT64_C(92);
	probe_ref.handle.queue_generation = UINT64_C(4);
	{
		PcmXWaitIdentity writer_identity = make_wait_identity(782, 0, 8, UINT64_C(84005));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&writer_identity, 1, UINT64_C(8302), &writer),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&writer, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, UINT64_C(8302),
																 NULL, 0, &count_snapshot),
				 PCM_X_QUEUE_NOT_READY);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[writer.tag_slot.slot_index];
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &(PcmXTicketRef){ 0 }));
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writer), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_local_queue_invalidate_authority_is_exact_and_read_only)
{
	PcmXLocalHolderKey holder_key = make_local_holder_key(774, 0, 4, UINT64_C(84101), 1);
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXTicketRef probe_ref;
	uint64 master_session = UINT64_C(8302);

	init_active_pcm_x(UINT64_C(7216));
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(774, 2, 7, UINT64_C(84102));
	probe_ref.handle.ticket_id = UINT64_C(92);
	probe_ref.handle.queue_generation = UINT64_C(4);

	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &holder, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	/* Freeze alone is not a drop certificate. */
	UT_ASSERT_EQ(cluster_pcm_x_local_queue_invalidate_authorize_exact(
					 &probe_ref.identity.tag, probe_ref.identity.cluster_epoch,
					 probe_ref.identity.request_id, probe_ref.identity.node_id, 1, master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder, 1, NULL,
																0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_queue_invalidate_authorize_exact(
					 &probe_ref.identity.tag, probe_ref.identity.cluster_epoch,
					 probe_ref.identity.request_id, probe_ref.identity.node_id, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_queue_invalidate_authorize_exact(
					 &probe_ref.identity.tag, probe_ref.identity.cluster_epoch,
					 probe_ref.identity.request_id + 1, probe_ref.identity.node_id, 1,
					 master_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_queue_invalidate_authorize_exact(
					 &probe_ref.identity.tag, probe_ref.identity.cluster_epoch,
					 probe_ref.identity.request_id, probe_ref.identity.node_id + 1, 1,
					 master_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_queue_invalidate_authorize_exact(
					 &probe_ref.identity.tag, probe_ref.identity.cluster_epoch,
					 probe_ref.identity.request_id, probe_ref.identity.node_id, 1,
					 master_session + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_probe_freeze_closes_fifo_round_and_arm_consumes_reservation)
{
	PcmXWaitIdentity leader_identity = make_wait_identity(774, 0, 6, UINT64_C(85001));
	PcmXWaitIdentity late_identity = make_wait_identity(774, 0, 7, UINT64_C(85002));
	PcmXLocalHolderKey holder_key = make_local_holder_key(774, 0, 8, UINT64_C(85003), 3);
	PcmXLocalHandle leader;
	PcmXLocalHandle late;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle copied;
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalBlockerSnapshot empty_snapshot;
	PcmXTicketRef probe_ref;
	PcmXLocalTagSlot *tag_slot;
	ClusterLmdVertex blocker;
	uint64 master_session = UINT64_C(8401);

	init_active_pcm_x(UINT64_C(7216));
	bind_local_master(1, leader_identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(774, 2, 9, UINT64_C(85004));
	probe_ref.handle.ticket_id = UINT64_C(92);
	probe_ref.handle.queue_generation = UINT64_C(4);

	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &copied, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->cutoff_sequence, 1);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 1);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(memcmp(&copied, &holder, sizeof(copied)) == 0);
	/* The production type-48 collector probes for a replay before it samples
	 * PGPROC wait state.  A generation-zero freeze reservation is therefore
	 * an exact "not armed yet" result, never corruption. */
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_lookup_exact(&probe_ref, 1, master_session,
																   &blocker_snapshot),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	/* Writer admission remains legal but belongs to the next round; it can
	 * never enter the frozen round sampled by this PROBE. */
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&late_identity, 1, master_session, &late),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late.local_round, leader.local_round + 1);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 1);

	/* Snapshot publication upgrades the existing exact freeze reservation;
	 * it must not reject it as a conflicting in-flight set. */
	blocker = make_blocker(holder_key.identity.node_id, holder_key.identity.procno,
						   holder_key.identity.request_id);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&snapshot, &copied, 1, &blocker, 1,
																&blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(blocker_snapshot.set_generation, 1);
	UT_ASSERT(ticket_refs_equal(&blocker_snapshot.ref, &probe_ref));
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);

	/* A nonempty set may resolve before transfer.  The exact ACK tombstone
	 * remains the generation authority; same-ticket re-PROBE reuses the
	 * barrier and advances N -> N+1 instead of resetting to one. */
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &copied, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&snapshot, &copied, 1, NULL, 0,
																&empty_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(empty_snapshot.set_generation, 2);
	UT_ASSERT_EQ(empty_snapshot.blocker_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_nested_wait_guard_snapshots_content_locks_then_checks_barriers)
{
	PcmXLocalHolderKey holder_key = make_local_holder_key(775, 0, 10, UINT64_C(86001), 1);
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle copied;
	PcmXLocalHolderSnapshot snapshot;
	PcmXTicketRef probe_ref;
	PcmXLocalTagSlot *tag_slot;
	LWLock unrelated;
	uint64 master_session = UINT64_C(8501);

	init_active_pcm_x(UINT64_C(7217));
	fake_buffer_descriptors[1].bufferdesc.tag = holder_key.identity.tag;
	fake_buffer_descriptors[2].bufferdesc.tag = make_tag(776);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(775, 2, 11, UINT64_C(86002));
	probe_ref.handle.ticket_id = UINT64_C(93);
	probe_ref.handle.queue_generation = UINT64_C(5);

	/* publish-first ordering: before freeze, a held content lock is legal.
	 * The later freeze still enumerates that exact registered holder. */
	held_lwlocks[0] = &fake_buffer_descriptors[1].bufferdesc.content_lock;
	held_lwlock_modes[0] = LW_EXCLUSIVE;
	held_lwlock_count = 1;
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(lock_acquire_during_iteration_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &copied, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.holder_count, 1);
	UT_ASSERT(memcmp(&copied, &holder, sizeof(copied)) == 0);

	/* pcm_lock shares the BufferDesc address range but is not content_lock;
	 * it must not be reverse-mapped or rejected by the frozen tag. */
	held_lwlocks[0] = &fake_buffer_descriptors[1].bufferdesc.pcm_lock;
	held_lwlock_modes[0] = LW_EXCLUSIVE;
	held_lwlock_count = 1;
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_OK);

	/* freeze-first ordering and multiple held locks: collection is bounded on
	 * the stack, ignores unrelated locks/tags, and checks PCM only after the
	 * callback has completed. */
	memset(&unrelated, 0, sizeof(unrelated));
	held_lwlocks[0] = &unrelated;
	held_lwlock_modes[0] = LW_SHARED;
	held_lwlocks[1] = &fake_buffer_descriptors[2].bufferdesc.content_lock;
	held_lwlock_modes[1] = LW_SHARED;
	held_lwlocks[2] = &fake_buffer_descriptors[1].bufferdesc.pcm_lock;
	held_lwlock_modes[2] = LW_EXCLUSIVE;
	held_lwlocks[3] = &fake_buffer_descriptors[1].bufferdesc.content_lock;
	held_lwlock_modes[3] = LW_EXCLUSIVE;
	held_lwlock_count = 4;
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(lock_acquire_during_iteration_count, 0);

	/* A torn tag generation is a closed decision.  It may never be treated as
	 * no resident tag and permit the backend to sleep. */
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder.tag_slot.slot_index];
	pg_atomic_write_u32(&tag_slot->slot.generation_change_seq,
						pg_atomic_read_u32(&tag_slot->slot.generation_change_seq) | 1U);
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	pg_atomic_write_u32(&tag_slot->slot.generation_change_seq,
						pg_atomic_read_u32(&tag_slot->slot.generation_change_seq) + 1U);
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_BARRIER_CLOSED);
	held_lwlock_count = 0;
}

UT_TEST(test_nested_wait_guard_allows_untracked_tag_before_runtime_formation)
{
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	fake_buffer_descriptors[3].bufferdesc.tag = make_tag(778);
	held_lwlocks[0] = &fake_buffer_descriptors[3].bufferdesc.content_lock;
	held_lwlock_modes[0] = LW_EXCLUSIVE;
	held_lwlock_count = 1;

	UT_ASSERT(cluster_pcm_x_runtime_snapshot().state != PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_nested_wait_guard_before_block(), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(lock_acquire_during_iteration_count, 0);
	held_lwlock_count = 0;
}

UT_TEST(test_local_holder_lock_free_transitions_reject_stale_aba_handle)
{
	PcmXLocalHolderKey key = make_local_holder_key(771, 0, 3, UINT64_C(83001), 1);
	PcmXLocalHolderHandle first;
	PcmXLocalHolderHandle second;
	PcmXLocalMembershipSlot *holder;
	int allocator_before;
	int domain_before;

	init_active_pcm_x(UINT64_C(7214));
	UT_ASSERT_EQ(register_active_local_holder(&key, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&key, &second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(second.holder_slot.slot_index, first.holder_slot.slot_index);
	UT_ASSERT(second.holder_slot.slot_generation > first.holder_slot.slot_generation);
	holder = &membership_slots(ClusterPcmXConvertShmem)[second.holder_slot.slot_index];
	allocator_before = allocator_lock_acquire_count;
	domain_before = domain_lock_acquire_count;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&first), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&first), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(test_slot_state(&holder->slot), PCM_XL_HOLDER_ACQUIRING);
	UT_ASSERT_EQ(allocator_lock_acquire_count, allocator_before);
	UT_ASSERT_EQ(domain_lock_acquire_count, domain_before);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&second), PCM_X_QUEUE_OK);
}

UT_TEST(test_directory_delete_then_allocator_reuse_changes_generation)
{
	BufferTag tag = make_tag(44);
	PcmXSlotRef old_ref;
	PcmXSlotRef new_ref;
	PcmXSlotRef found;
	PcmXMasterTagSlot *slot;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	slot = (PcmXMasterTagSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TAG, &old_ref);
	slot->tag = tag;
	UT_ASSERT_EQ(cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TAG, &tag, old_ref, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TAG, &tag, old_ref),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TAG, old_ref,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_OK);
	slot = (PcmXMasterTagSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TAG, &new_ref);
	slot->tag = tag;
	UT_ASSERT_EQ(new_ref.slot_index, old_ref.slot_index);
	UT_ASSERT_EQ(new_ref.slot_generation, old_ref.slot_generation + 1);
	UT_ASSERT_EQ(cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TAG, &tag, new_ref, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TAG, &tag, old_ref),
				 PCM_X_DIRECTORY_STALE_REF);
}

UT_TEST(test_allocator_and_directory_operations_take_only_allocator_lock)
{
	PcmXPrehandleKey key = { UINT64_C(91), UINT64_C(7) };
	PcmXSlotRef ref;
	PcmXSlotRef found;
	PcmXMasterTicketSlot *ticket;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &ref);
	ticket->prehandle = key;
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, ref, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, ref),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TICKET, ref,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_OK);
	UT_ASSERT(allocator_lock_acquire_count > 0);
	UT_ASSERT(allocator_lock_shared_count > 0);
	UT_ASSERT(allocator_lock_exclusive_count > 0);
	UT_ASSERT_EQ(domain_lock_acquire_count, 0);
	UT_ASSERT_EQ(max_held_lwlock_count, 1);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(lwlock_release_count, allocator_lock_acquire_count);
}

UT_TEST(test_allocator_and_directory_reject_domain_lock_nesting)
{
	PcmXShmemHeader *header;
	PcmXPrehandleKey key = { UINT64_C(91), UINT64_C(17) };
	PcmXSlotRef candidate;
	PcmXSlotRef output;
	PcmXSlotHeader *output_slot;
	PcmXMasterTicketSlot *ticket;
	int allocator_acquires_before;
	uint32 partition = 3;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	ticket = (PcmXMasterTicketSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TICKET, &candidate);
	ticket->prehandle = key;
	allocator_acquires_before = allocator_lock_acquire_count;

	LWLockAcquire(&header->master_locks[partition].lock, LW_EXCLUSIVE);
	output.slot_index = 0;
	output.slot_generation = 99;
	output_slot = (PcmXSlotHeader *)(uintptr_t)1;
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_MASTER_TAG, &output, &output_slot),
				 PCM_X_ALLOC_INVALID);
	UT_ASSERT_EQ(output.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(output.slot_generation, 0);
	UT_ASSERT_NULL(output_slot);
	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TICKET, candidate,
													   PCM_X_SLOT_RESERVED_NONVISIBLE),
				 PCM_X_ALLOC_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, &output),
				 PCM_X_DIRECTORY_INVALID);
	UT_ASSERT_EQ(output.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(output.slot_generation, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_insert(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, candidate, &output),
		PCM_X_DIRECTORY_INVALID);
	UT_ASSERT_EQ(output.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(output.slot_generation, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &key, candidate),
		PCM_X_DIRECTORY_INVALID);
	UT_ASSERT_EQ(allocator_lock_acquire_count, allocator_acquires_before);
	UT_ASSERT_EQ(held_lwlock_count, 1);
	LWLockRelease(&header->master_locks[partition].lock);

	LWLockAcquire(&header->local_locks[partition].lock, LW_EXCLUSIVE);
	output.slot_index = 0;
	output.slot_generation = 99;
	output_slot = (PcmXSlotHeader *)(uintptr_t)1;
	UT_ASSERT_EQ(cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_LOCAL_TAG, &output, &output_slot),
				 PCM_X_ALLOC_INVALID);
	UT_ASSERT_EQ(output.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(output.slot_generation, 0);
	UT_ASSERT_NULL(output_slot);
	UT_ASSERT_EQ(allocator_lock_acquire_count, allocator_acquires_before);
	UT_ASSERT_EQ(held_lwlock_count, 1);
	LWLockRelease(&header->local_locks[partition].lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
}

UT_TEST(test_slot_ref_revalidate_requires_exact_domain_tag_state_and_generation)
{
	PcmXShmemHeader *header;
	BufferTag tag = make_tag(88);
	BufferTag candidate_tag;
	BufferTag wrong_tag;
	BufferTag wrong_partition_tag;
	PcmXMasterTagSlot *slot;
	PcmXSlotRef old_ref;
	PcmXSlotRef new_ref;
	PcmXSlotRef stale_ref;
	PcmXSlotHeader *resolved;
	BlockNumber block;
	uint32 candidate_hash;
	uint32 candidate_partition;
	uint32 tag_hash;
	uint32 wrong_tag_hash = 0;
	uint32 wrong_partition_hash = 0;
	uint32 partition;
	uint32 wrong_partition = 0;
	bool found_same_partition = false;
	bool found_wrong_partition = false;

	wrong_tag = tag;
	wrong_partition_tag = tag;
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	slot = (PcmXMasterTagSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TAG, &old_ref);
	slot->tag = tag;
	test_set_slot_state(&slot->slot, PCM_XT_QUEUED);
	tag_hash = cluster_pcm_x_tag_hash(&tag);
	UT_ASSERT_EQ(tag_hash, cluster_pcm_x_tag_hash(&tag));
	partition = cluster_pcm_x_lock_partition(tag_hash);
	for (block = 89; block < 89 + PCM_X_LOCK_PARTITIONS * 32; block++) {
		candidate_tag = make_tag(block);
		candidate_hash = cluster_pcm_x_tag_hash(&candidate_tag);
		candidate_partition = cluster_pcm_x_lock_partition(candidate_hash);
		if (!found_same_partition && candidate_hash != tag_hash
			&& candidate_partition == partition) {
			wrong_tag = candidate_tag;
			wrong_tag_hash = candidate_hash;
			found_same_partition = true;
		}
		if (!found_wrong_partition && candidate_partition != partition) {
			wrong_partition_tag = candidate_tag;
			wrong_partition_hash = candidate_hash;
			wrong_partition = candidate_partition;
			found_wrong_partition = true;
		}
		if (found_same_partition && found_wrong_partition)
			break;
	}
	UT_ASSERT(found_same_partition);
	UT_ASSERT(found_wrong_partition);
	UT_ASSERT(!BufferTagsEqual(&tag, &wrong_tag));
	UT_ASSERT(!BufferTagsEqual(&tag, &wrong_partition_tag));
	UT_ASSERT_NE(tag_hash, wrong_tag_hash);
	UT_ASSERT_NE(tag_hash, wrong_partition_hash);
	UT_ASSERT_EQ(wrong_tag_hash, cluster_pcm_x_tag_hash(&wrong_tag));
	UT_ASSERT_EQ(wrong_partition_hash, cluster_pcm_x_tag_hash(&wrong_partition_tag));
	UT_ASSERT_EQ(cluster_pcm_x_lock_partition(wrong_tag_hash), partition);
	UT_ASSERT_NE(wrong_partition, partition);

	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &tag, tag_hash));
	LWLockAcquire(&header->master_locks[wrong_partition].lock, LW_SHARED);
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &tag, tag_hash));
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &tag, wrong_partition_hash));
	UT_ASSERT_EQ(test_slot_generation(&slot->slot), old_ref.slot_generation);
	UT_ASSERT_EQ(test_slot_state(&slot->slot), PCM_XT_QUEUED);
	UT_ASSERT(BufferTagsEqual(&slot->tag, &tag));
	LWLockRelease(&header->master_locks[wrong_partition].lock);

	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 NULL, tag_hash));
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &wrong_tag, wrong_tag_hash));
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref,
													 PCM_XT_ADMITTING, &tag, tag_hash));
	stale_ref = old_ref;
	stale_ref.slot_generation--;
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, stale_ref,
													 PCM_XT_QUEUED, &tag, tag_hash));
	resolved = cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
												 &tag, tag_hash);
	UT_ASSERT_EQ(resolved, &slot->slot);
	slot->tag = wrong_tag;
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &tag, tag_hash));
	slot->tag = tag;
	resolved = cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
												 &tag, tag_hash);
	UT_ASSERT_EQ(resolved, &slot->slot);
	test_set_slot_state(&slot->slot, PCM_X_SLOT_DETACHING);
	LWLockRelease(&header->master_locks[partition].lock);

	UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_MASTER_TAG, old_ref,
													   PCM_X_SLOT_DETACHING),
				 PCM_X_ALLOC_OK);
	slot = (PcmXMasterTagSlot *)reserve_slot(PCM_X_ALLOC_MASTER_TAG, &new_ref);
	UT_ASSERT_EQ(new_ref.slot_index, old_ref.slot_index);
	UT_ASSERT(new_ref.slot_generation != old_ref.slot_generation);
	slot->tag = tag;
	test_set_slot_state(&slot->slot, PCM_XT_QUEUED);
	LWLockAcquire(&header->master_locks[partition].lock, LW_SHARED);
	UT_ASSERT_NULL(cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, old_ref, PCM_XT_QUEUED,
													 &tag, tag_hash));
	resolved = cluster_pcm_x_slot_ref_revalidate(PCM_X_ALLOC_MASTER_TAG, new_ref, PCM_XT_QUEUED,
												 &tag, tag_hash);
	UT_ASSERT_EQ(resolved, &slot->slot);
	LWLockRelease(&header->master_locks[partition].lock);
}

UT_TEST(test_internal_directory_stale_ref_is_corrupt_and_fails_closed)
{
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXMasterTicketSlot *ticket;

	init_active_pcm_x(UINT64_C(77));
	request
		= make_enqueue(make_wait_identity(740, 0, 9, UINT64_C(50001)), UINT64_C(5101), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	test_set_slot_generation(&ticket->slot, admission.ticket_slot.slot_generation + 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(5201)),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_resident_exact_wire_mismatch_is_stale_without_side_effect)
{
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXTicketRef active;
	PcmXTicketRef stale;
	PcmXTicketRef transfer;

	init_active_pcm_x(UINT64_C(77));
	request
		= make_enqueue(make_wait_identity(741, 0, 9, UINT64_C(50002)), UINT64_C(5102), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	before = *ticket;

	stale = admission.ref;
	stale.identity.request_id++;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&stale), PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(5202)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(5202));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(5202), &transfer),
				 PCM_X_QUEUE_OK);
	before = *ticket;
	stale = transfer;
	stale.grant_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&stale), PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_same_lock_staged_insert_exists_is_corrupt_and_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(742, 0, 9, UINT64_C(50003)), UINT64_C(5103), UINT64_C(1));
	bind_enqueue_peer(&request);
	staged_prehandle_insert_exists_key = request.prehandle;
	staged_prehandle_insert_exists_armed = true;

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(!staged_prehandle_insert_exists_armed);
	UT_ASSERT_EQ(header->next_ticket_id, 1);
	UT_ASSERT_EQ(header->peer_frontiers[request.identity.node_id].next_expected_prehandle_sequence,
				 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_composite_admission_publishes_all_indexes_and_coalesces_node)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission replay;
	PcmXMasterAdmission coalesced;
	PcmXMasterAdmission gated;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXEnqueuePayload follower_request;
	PcmXEnqueuePayload gated_request;
	PcmXSlotRef found;
	uint64 next_ticket_before;
	uint64 next_admission_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(700, 2, 12, UINT64_C(1001)), UINT64_C(501), UINT64_C(1));
	bind_enqueue_peer(&request);
	memset(&first, 0, sizeof(first));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &first), PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(first.ref.handle.ticket_id, 1);
	UT_ASSERT_EQ(first.ref.handle.queue_generation, first.ticket_slot.slot_generation);
	UT_ASSERT_EQ(first.ref.grant_generation, 0);
	UT_ASSERT_EQ(first.master_session_incarnation, 77);
	UT_ASSERT_EQ(first.admission_sequence, 1);
	UT_ASSERT((first.flags & PCM_X_ADMIT_F_QUEUE_HEAD) != 0);
	UT_ASSERT(memcmp(&first.ref.identity, &request.identity, sizeof(request.identity)) == 0);
	UT_ASSERT(memcmp(&first.prehandle, &request.prehandle, sizeof(request.prehandle)) == 0);

	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	ticket = &master_ticket_slots(header)[first.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_LIVE);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ADMITTING);
	UT_ASSERT_EQ(tag_slot->head_index, first.ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->tail_index, first.ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->active_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap), UINT32_C(1) << 2);
	UT_ASSERT_EQ(ticket->tag_slot_index, first.tag_slot.slot_index);
	UT_ASSERT_EQ(ticket->tag_slot_generation, first.tag_slot.slot_generation);
	UT_ASSERT_EQ(ticket->next_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ticket->prev_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ticket->involved_nodes_bitmap, UINT32_C(1) << 2);
	UT_ASSERT_EQ(header->peer_frontiers[2].cluster_epoch, request.identity.cluster_epoch);
	UT_ASSERT_EQ(header->peer_frontiers[2].sender_session_incarnation,
				 request.prehandle.sender_session_incarnation);
	UT_ASSERT_EQ(header->peer_frontiers[2].next_expected_prehandle_sequence, 2);
	UT_ASSERT_EQ(header->peer_frontiers[2].retired_prehandle_sequence, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 0);

	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TAG, &request.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, first.tag_slot));
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &request.prehandle, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, first.ticket_slot));
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &first.ref.handle, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, first.ticket_slot));

	memset(&replay, 0, sizeof(replay));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &replay), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &first.ref));
	UT_ASSERT(slot_refs_equal(replay.tag_slot, first.tag_slot));
	UT_ASSERT(slot_refs_equal(replay.ticket_slot, first.ticket_slot));
	UT_ASSERT((replay.flags & PCM_X_ADMIT_F_EXACT_REPLAY) != 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);

	follower_request
		= make_enqueue(make_wait_identity(700, 2, 13, UINT64_C(1002)), UINT64_C(501), UINT64_C(2));
	memset(&coalesced, 0, sizeof(coalesced));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&follower_request, &coalesced), PCM_X_QUEUE_BUSY);
	UT_ASSERT(ticket_refs_equal(&coalesced.ref, &first.ref));
	UT_ASSERT(slot_refs_equal(coalesced.ticket_slot, first.ticket_slot));
	UT_ASSERT((coalesced.flags & PCM_X_ADMIT_F_NODE_COALESCED) != 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	UT_ASSERT_EQ(header->peer_frontiers[2].next_expected_prehandle_sequence, 2);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
											  &follower_request.prehandle, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);

	gated_request
		= make_enqueue(make_wait_identity(700, 3, 14, UINT64_C(1003)), UINT64_C(502), UINT64_C(1));
	bind_enqueue_peer(&gated_request);
	next_ticket_before = header->next_ticket_id;
	next_admission_before = tag_slot->next_admission_sequence;
	pg_atomic_write_u32(&tag_slot->admission_gate, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&gated_request, &gated), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 1);
	UT_ASSERT_EQ(header->next_ticket_id, next_ticket_before);
	UT_ASSERT_EQ(tag_slot->next_admission_sequence, next_admission_before);
	UT_ASSERT_EQ(header->peer_frontiers[3].next_expected_prehandle_sequence, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	pg_atomic_write_u32(&tag_slot->admission_gate, 0);
}

UT_TEST(test_master_coalesce_scan_distinguishes_legal_publish_races)
{
	int mode;

	for (mode = 1; mode <= 3; mode++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission first;
		PcmXMasterAdmission second;
		PcmXEnqueuePayload first_request;
		PcmXEnqueuePayload second_request;
		PcmXRuntimeSnapshot snapshot;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		first_request = make_enqueue(make_wait_identity(722, 0, 2, UINT64_C(26001)), UINT64_C(2801),
									 UINT64_C(1));
		second_request = make_enqueue(make_wait_identity(722, 0, 3, UINT64_C(26002)),
									  UINT64_C(2801), UINT64_C(2));
		bind_enqueue_peer(&first_request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
		coalesce_interlock_tag = &master_tag_slots(header)[first.tag_slot.slot_index];
		coalesce_interlock_ticket = &master_ticket_slots(header)[first.ticket_slot.slot_index];
		coalesce_interlock_mode = mode;
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second),
					 mode < 3 ? PCM_X_QUEUE_BUSY : PCM_X_QUEUE_CORRUPT);
		UT_ASSERT_EQ(coalesce_interlock_mode, 0);
		UT_ASSERT_EQ(header->next_ticket_id, 2);
		UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 2);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
		snapshot = cluster_pcm_x_runtime_snapshot();
		UT_ASSERT_EQ(snapshot.state,
					 mode < 3 ? PCM_X_RUNTIME_ACTIVE : PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_stale_peer_frontier_is_retryable_without_publication)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXLocalHandle local;
	PcmXEnqueuePayload request;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(725, 0, 2, UINT64_C(29001)), UINT64_C(3001), UINT64_C(1));
	bind_enqueue_peer(&request);
	request.prehandle.sender_session_incarnation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(header->next_ticket_id, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(726, 0, 2, UINT64_C(29002));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(3101));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(3102), &local),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_resident_tag_authority_mismatch_is_corrupt)
{
	int kind;

	for (kind = 0; kind < 2; kind++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission first;
		PcmXMasterAdmission second;
		PcmXMasterTagSlot *tag_slot;
		PcmXEnqueuePayload first_request;
		PcmXEnqueuePayload second_request;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		first_request = make_enqueue(make_wait_identity(727, 0, 2, UINT64_C(29003)), UINT64_C(3201),
									 UINT64_C(1));
		second_request = make_enqueue(make_wait_identity(727, 1, 3, UINT64_C(29004)),
									  UINT64_C(3202), UINT64_C(1));
		bind_enqueue_peer(&first_request);
		bind_enqueue_peer(&second_request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
		tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
		if (kind == 0)
			tag_slot->tag.blockNum++;
		else
			tag_slot->cluster_epoch++;
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second),
					 PCM_X_QUEUE_CORRUPT);
		UT_ASSERT_EQ(header->next_ticket_id, 2);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}

	for (kind = 0; kind < 4; kind++) {
		PcmXShmemHeader *header;
		PcmXLocalHandle first;
		PcmXLocalHandle second;
		PcmXLocalTagSlot *tag_slot;
		PcmXWaitIdentity first_identity;
		PcmXWaitIdentity second_identity;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		first_identity = make_wait_identity(728, 0, 2, UINT64_C(29005));
		second_identity = make_wait_identity(728, 0, 3, UINT64_C(29006));
		bind_local_master(1, first_identity.cluster_epoch, UINT64_C(3301));
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&first_identity, 1, UINT64_C(3301), &first),
					 PCM_X_QUEUE_OK);
		tag_slot = &local_tag_slots(header)[first.tag_slot.slot_index];
		switch (kind) {
		case 0:
			tag_slot->tag.blockNum++;
			break;
		case 1:
			tag_slot->cluster_epoch++;
			break;
		case 2:
			tag_slot->master_node++;
			break;
		default:
			tag_slot->master_session_incarnation++;
			break;
		}
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&second_identity, 1, UINT64_C(3301), &second),
					 PCM_X_QUEUE_CORRUPT);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 1);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 1);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_resident_tag_transitional_states_are_retryable)
{
	static const uint32 states[]
		= { PCM_X_TAG_RESERVED_NONVISIBLE, PCM_X_TAG_DETACHING, PCM_X_TAG_RECOVERY_BLOCKED };
	Size i;

	for (i = 0; i < lengthof(states); i++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission first;
		PcmXMasterAdmission second;
		PcmXMasterTagSlot *tag_slot;
		PcmXEnqueuePayload first_request;
		PcmXEnqueuePayload second_request;
		PcmXQueueResult expected
			= states[i] == PCM_X_TAG_RECOVERY_BLOCKED ? PCM_X_QUEUE_NOT_READY : PCM_X_QUEUE_BUSY;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		first_request = make_enqueue(make_wait_identity(729, 0, 2, UINT64_C(29007)), UINT64_C(3401),
									 UINT64_C(1));
		second_request = make_enqueue(make_wait_identity(729, 1, 3, UINT64_C(29008)),
									  UINT64_C(3402), UINT64_C(1));
		bind_enqueue_peer(&first_request);
		bind_enqueue_peer(&second_request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
		tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
		test_set_slot_state(&tag_slot->slot, states[i]);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second), expected);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	}

	for (i = 0; i < lengthof(states); i++) {
		PcmXShmemHeader *header;
		PcmXLocalHandle first;
		PcmXLocalHandle second;
		PcmXLocalTagSlot *tag_slot;
		PcmXWaitIdentity first_identity;
		PcmXWaitIdentity second_identity;
		PcmXQueueResult expected
			= states[i] == PCM_X_TAG_RECOVERY_BLOCKED ? PCM_X_QUEUE_NOT_READY : PCM_X_QUEUE_BUSY;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		first_identity = make_wait_identity(730, 0, 2, UINT64_C(29009));
		second_identity = make_wait_identity(730, 0, 3, UINT64_C(29010));
		bind_local_master(1, first_identity.cluster_epoch, UINT64_C(3501));
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&first_identity, 1, UINT64_C(3501), &first),
					 PCM_X_QUEUE_OK);
		tag_slot = &local_tag_slots(header)[first.tag_slot.slot_index];
		test_set_slot_state(&tag_slot->slot, states[i]);
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&second_identity, 1, UINT64_C(3501), &second),
					 expected);
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 1);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	}
}

UT_TEST(test_peer_bind_ack_is_exact_and_mismatch_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(-1, 9, 601), PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 9, 0), PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 0, 601), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->peer_frontiers[0].cluster_epoch, 0);
	UT_ASSERT_EQ(header->peer_frontiers[0].sender_session_incarnation, 601);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 1);
	UT_ASSERT_EQ(header->peer_frontiers[0].retired_prehandle_sequence, 0);
	UT_ASSERT_EQ(header->outbound_targets[0].flags,
				 PCM_X_OUTBOUND_TARGET_INITIALIZED | PCM_X_OUTBOUND_TARGET_BOUND);
	UT_ASSERT_EQ(header->outbound_targets[0].cluster_epoch, 0);
	UT_ASSERT_EQ(header->outbound_targets[0].target_session_incarnation, 601);
	UT_ASSERT_EQ(header->outbound_targets[0].next_prehandle_sequence, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->outbound_targets[0].mint_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 0, 601), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 1, 601), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(header->peer_frontiers[0].cluster_epoch, 0);
	UT_ASSERT_EQ(header->peer_frontiers[0].sender_session_incarnation, 601);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_activation_publishes_bound_frontiers_and_counters_before_active)
{
	PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;
	int i;

	memset(bindings, 0, sizeof(bindings));
	bindings[1].cluster_epoch = 0;
	bindings[1].peer_session_incarnation = UINT64_C(601);
	bindings[3].cluster_epoch = 0;
	bindings[3].peer_session_incarnation = UINT64_C(603);
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate_bound(UINT64_C(77), bindings));
	header = ClusterPcmXConvertShmem;
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 77);
	for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
		PcmXOutboundTargetFrontier *outbound = &header->outbound_targets[i];

		UT_ASSERT_EQ(pg_atomic_read_u32(&outbound->mint_gate), 0);
		UT_ASSERT_EQ(outbound->next_prehandle_sequence, 1);
		UT_ASSERT((outbound->flags & PCM_X_OUTBOUND_TARGET_INITIALIZED) != 0);
		UT_ASSERT_EQ(header->peer_frontiers[i].next_expected_prehandle_sequence, 1);
		UT_ASSERT_EQ(header->peer_frontiers[i].retired_prehandle_sequence, 0);
		if (i == 1 || i == 3) {
			UT_ASSERT((outbound->flags & PCM_X_OUTBOUND_TARGET_BOUND) != 0);
			UT_ASSERT_EQ(outbound->cluster_epoch, 0);
			UT_ASSERT_EQ(outbound->target_session_incarnation,
						 i == 1 ? UINT64_C(601) : UINT64_C(603));
			UT_ASSERT_EQ(header->peer_frontiers[i].cluster_epoch, 0);
			UT_ASSERT_EQ(header->peer_frontiers[i].sender_session_incarnation,
						 i == 1 ? UINT64_C(601) : UINT64_C(603));
		} else {
			UT_ASSERT_EQ(outbound->flags, PCM_X_OUTBOUND_TARGET_INITIALIZED);
			UT_ASSERT_EQ(outbound->cluster_epoch, 0);
			UT_ASSERT_EQ(outbound->target_session_incarnation, 0);
			UT_ASSERT_EQ(header->peer_frontiers[i].cluster_epoch, 0);
			UT_ASSERT_EQ(header->peer_frontiers[i].sender_session_incarnation, 0);
		}
	}
	UT_ASSERT_EQ(cluster_pcm_x_runtime_peer_binding_revalidate_exact(1, 0, UINT64_C(601)),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_activation_rejects_partial_binding_before_claiming_gate)
{
	PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXRuntimeSnapshot snapshot;

	memset(bindings, 0, sizeof(bindings));
	bindings[2].cluster_epoch = UINT64_C(19);
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(!cluster_pcm_x_runtime_activate_bound(UINT64_C(77), bindings));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.gate_generation, 0);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->outbound_targets[2].flags, 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->outbound_targets[2].next_prehandle_sequence, 0);
}

UT_TEST(test_runtime_peer_binding_exact_revalidation_is_read_only)
{
	PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXShmemHeader *header;
	PcmXPeerFrontier frontier_before;
	PcmXOutboundTargetFrontier outbound_before;

	memset(bindings, 0, sizeof(bindings));
	bindings[2].cluster_epoch = 0;
	bindings[2].peer_session_incarnation = UINT64_C(602);
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate_bound(UINT64_C(77), bindings));
	header = ClusterPcmXConvertShmem;
	frontier_before = header->peer_frontiers[2];
	outbound_before = header->outbound_targets[2];

	UT_ASSERT_EQ(cluster_pcm_x_runtime_peer_binding_revalidate_exact(2, 0, UINT64_C(602)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(memcmp(&header->peer_frontiers[2], &frontier_before, sizeof(frontier_before)), 0);
	UT_ASSERT_EQ(memcmp(&header->outbound_targets[2], &outbound_before, sizeof(outbound_before)),
				 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(allocator_lock_acquire_count, 1);
	UT_ASSERT_EQ(allocator_lock_shared_count, 1);
	UT_ASSERT_EQ(allocator_lock_exclusive_count, 0);
	UT_ASSERT_EQ(lwlock_release_count, 1);
}

UT_TEST(test_runtime_peer_binding_epoch_drift_fails_closed)
{
	PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerFrontier frontier_before;
	PcmXOutboundTargetFrontier outbound_before;

	memset(bindings, 0, sizeof(bindings));
	bindings[1].cluster_epoch = UINT64_C(19);
	bindings[1].peer_session_incarnation = UINT64_C(601);
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate_bound(UINT64_C(77), bindings));
	frontier_before = ClusterPcmXConvertShmem->peer_frontiers[1];
	outbound_before = ClusterPcmXConvertShmem->outbound_targets[1];

	UT_ASSERT_EQ(
		cluster_pcm_x_runtime_peer_binding_revalidate_exact(1, UINT64_C(20), UINT64_C(601)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(memcmp(&ClusterPcmXConvertShmem->peer_frontiers[1], &frontier_before,
						sizeof(frontier_before)),
				 0);
	UT_ASSERT_EQ(memcmp(&ClusterPcmXConvertShmem->outbound_targets[1], &outbound_before,
						sizeof(outbound_before)),
				 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_runtime_peer_binding_session_drift_fails_closed)
{
	PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT];

	memset(bindings, 0, sizeof(bindings));
	bindings[1].cluster_epoch = UINT64_C(19);
	bindings[1].peer_session_incarnation = UINT64_C(601);
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate_bound(UINT64_C(77), bindings));

	UT_ASSERT_EQ(
		cluster_pcm_x_runtime_peer_binding_revalidate_exact(1, UINT64_C(19), UINT64_C(602)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_runtime_peer_binding_empty_frontier_is_never_late_bound)
{
	PcmXShmemHeader *header;
	PcmXPeerFrontier frontier_before;
	PcmXOutboundTargetFrontier outbound_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	frontier_before = header->peer_frontiers[3];
	outbound_before = header->outbound_targets[3];

	UT_ASSERT_EQ(
		cluster_pcm_x_runtime_peer_binding_revalidate_exact(3, UINT64_C(19), UINT64_C(603)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(memcmp(&header->peer_frontiers[3], &frontier_before, sizeof(frontier_before)), 0);
	UT_ASSERT_EQ(memcmp(&header->outbound_targets[3], &outbound_before, sizeof(outbound_before)),
				 0);
	UT_ASSERT_EQ(header->peer_frontiers[3].cluster_epoch, 0);
	UT_ASSERT_EQ(header->peer_frontiers[3].sender_session_incarnation, 0);
	UT_ASSERT_EQ(header->outbound_targets[3].flags, PCM_X_OUTBOUND_TARGET_INITIALIZED);
	UT_ASSERT_EQ(header->outbound_targets[3].cluster_epoch, 0);
	UT_ASSERT_EQ(header->outbound_targets[3].target_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_runtime_peer_binding_invalid_node_is_rejected_without_closing_runtime)
{
	init_active_pcm_x(UINT64_C(77));
	UT_ASSERT_EQ(
		cluster_pcm_x_runtime_peer_binding_revalidate_exact(-1, UINT64_C(19), UINT64_C(601)),
		PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_peer_binding_revalidate_exact(PCM_X_PROTOCOL_NODE_LIMIT,
																	 UINT64_C(19), UINT64_C(601)),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(allocator_lock_acquire_count, 0);
}

UT_TEST(test_composite_admission_rejects_zero_wrapping_and_out_of_range_identity)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXLocalHandle local;
	PcmXEnqueuePayload request;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), UINT64_C(601), UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_NOT_READY);
	assert_master_queue_baseline(header);
	UT_ASSERT_EQ(header->peer_frontiers[0].cluster_epoch, 0);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 1);
	bind_enqueue_peer(&request);

	request.identity.request_id = 0;
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request
		= make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), UINT64_C(601), UINT64_C(1));
	request.identity.wait_seq = 0;
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request = make_enqueue(make_wait_identity(701, PCM_X_PROTOCOL_NODE_LIMIT, 10, UINT64_C(2001)),
						   UINT64_C(601), UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request = make_enqueue(
		make_wait_identity(701, 0, (uint32)header->layout.process_capacity, UINT64_C(2001)),
		UINT64_C(601), UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request = make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), 0, UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request = make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), UINT64_C(601), 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_INVALID);
	request
		= make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), UINT64_C(601), UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_NOT_READY);
	request
		= make_enqueue(make_wait_identity(701, 0, 10, UINT64_C(2001)), UINT64_C(601), UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	assert_master_queue_baseline(header);

	identity = make_wait_identity(701, 0, 10, UINT64_C(2001));
	identity.request_id = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &local),
				 PCM_X_QUEUE_INVALID);
	identity = make_wait_identity(701, 0, 10, UINT64_C(2001));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &local),
				 PCM_X_QUEUE_NOT_READY);
	assert_local_queue_baseline(header);
	bind_local_master(1, identity.cluster_epoch, UINT64_C(77));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &local),
				 PCM_X_QUEUE_STALE);
	assert_local_queue_baseline(header);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_join_begin(&identity, PCM_X_PROTOCOL_NODE_LIMIT, UINT64_C(77), &local),
		PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, 0, &local), PCM_X_QUEUE_INVALID);
	assert_local_queue_baseline(header);
}

UT_TEST(test_master_composite_capacity_failure_rolls_back_tag_indexes_and_id)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState saved_ticket_allocator;
	PcmXDirectoryEntry *handle_entries;
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXTicketHandle expected_handle = { UINT64_C(1), UINT64_C(1) };
	uint64 expected_hash;
	Size capacity;
	Size directory_capacity;
	Size i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	saved_ticket_allocator = header->allocator[PCM_X_ALLOC_MASTER_TICKET];
	capacity = header->layout.pools[PCM_X_POOL_MASTER_TICKET].capacity;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].free_head = PCM_X_INVALID_SLOT_INDEX;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].used = capacity;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].high_water = capacity;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].generation_exhausted = 0;
	request
		= make_enqueue(make_wait_identity(702, 0, 10, UINT64_C(3001)), UINT64_C(701), UINT64_C(1));
	bind_enqueue_peer(&request);

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(header->next_ticket_id, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TAG), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_HANDLE), 0);
	UT_ASSERT_EQ(header->peer_frontiers[0].cluster_epoch, request.identity.cluster_epoch);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 1);

	header->allocator[PCM_X_ALLOC_MASTER_TICKET] = saved_ticket_allocator;
	handle_entries = directory_entries(header, PCM_X_DIR_MASTER_TICKET_HANDLE, &directory_capacity);
	UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_MASTER_TICKET_HANDLE, &expected_handle,
											   &expected_hash));
	for (i = 0; i < directory_capacity; i++) {
		handle_entries[i].state = PCM_X_DIRECTORY_OCCUPIED;
		handle_entries[i].key_hash = expected_hash ^ UINT64_C(1);
	}
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(header->next_ticket_id, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TAG), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE), 0);
	UT_ASSERT_EQ(header->peer_frontiers[0].cluster_epoch, request.identity.cluster_epoch);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 1);
	memset(handle_entries, 0, directory_capacity * sizeof(*handle_entries));
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_ticket_and_admission_sequence_exhaustion_leave_no_holes)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission rejected;
	PcmXMasterAdmission replay;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload request;
	PcmXEnqueuePayload second_request;
	PcmXSlotRef found;
	uint64 next_ticket_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	header->next_ticket_id = PCM_X_MASTER_TICKET_ID_MAX + 1;
	request
		= make_enqueue(make_wait_identity(703, 0, 10, UINT64_C(4001)), UINT64_C(801), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &rejected),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(header->next_ticket_id, PCM_X_MASTER_TICKET_ID_MAX + 1);
	assert_master_queue_baseline(header);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &first), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	tag_slot->next_admission_sequence = UINT64_MAX;
	next_ticket_before = header->next_ticket_id;
	second_request
		= make_enqueue(make_wait_identity(703, 1, 11, UINT64_C(4002)), UINT64_C(802), UINT64_C(1));
	bind_enqueue_peer(&second_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &rejected),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(header->next_ticket_id, next_ticket_before);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
											  &second_request.prehandle, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&first.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &replay), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &first.ref));
	arm_and_ack_master_terminal_leg(&first.ref, PCM_X_TERMINAL_LEG_DRAIN,
									first.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&first.ref, PCM_X_TERMINAL_LEG_RETIRE,
									first.ref.identity.node_id);
	pg_atomic_write_u32(&tag_slot->admission_gate, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&first.ref), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(test_slot_state(&master_ticket_slots(header)[first.ticket_slot.slot_index].slot),
				 PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE, &first.prehandle, &found),
		PCM_X_DIRECTORY_OK);
	pg_atomic_write_u32(&tag_slot->admission_gate, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&first.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 1);
	UT_ASSERT_EQ(header->peer_frontiers[0].retired_prehandle_sequence, 1);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 2);
	assert_master_queue_baseline(header);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &replay), PCM_X_QUEUE_RETIRED);
	assert_master_queue_baseline(header);
}

UT_TEST(test_missing_live_prehandle_inside_frontier_is_corrupt_not_phantom)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterAdmission replay;
	PcmXEnqueuePayload request;
	PcmXRuntimeSnapshot snapshot;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(710, 0, 14, UINT64_C(11001)), UINT64_C(1201),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 2);
	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
													  &request.prehandle, admission.ticket_slot),
				 PCM_X_DIRECTORY_OK);

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &replay), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(header->next_ticket_id, 2);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	UT_ASSERT_EQ(header->peer_frontiers[0].next_expected_prehandle_sequence, 2);
	UT_ASSERT_EQ(header->peer_frontiers[0].retired_prehandle_sequence, 0);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_confirmed_queue_is_node_fifo_with_one_active)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[3];
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *tickets;
	PcmXTicketRef active;
	int nodes[3] = { 2, 0, 3 };
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 3; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(704, nodes[i], (uint32)(20 + i), UINT64_C(5001) + i),
						   UINT64_C(900) + i, UINT64_C(1));

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(admission[i].admission_sequence, (uint64)i + 1);
		if (i == 0) {
			UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, 0),
						 PCM_X_QUEUE_INVALID);
			UT_ASSERT_EQ(
				test_slot_state(
					&master_ticket_slots(header)[admission[i].ticket_slot.slot_index].slot),
				PCM_XT_ADMITTING);
		}
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(100) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	UT_ASSERT_EQ(tag_slot->head_index, admission[0].ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->tail_index, admission[2].ticket_slot.slot_index);
	UT_ASSERT_EQ(tickets[admission[0].ticket_slot.slot_index].next_index,
				 admission[1].ticket_slot.slot_index);
	UT_ASSERT_EQ(tickets[admission[1].ticket_slot.slot_index].next_index,
				 admission[2].ticket_slot.slot_index);

	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&admission[0].ref.identity.tag,
														 admission[0].ref.identity.cluster_epoch,
														 &active),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&active, &admission[0].ref));
	UT_ASSERT_EQ(tag_slot->active_index, admission[0].ticket_slot.slot_index);
	UT_ASSERT_EQ(test_slot_state(&tickets[tag_slot->active_index].slot), PCM_XT_ACTIVE_PROBE);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&admission[0].ref.identity.tag,
														 admission[0].ref.identity.cluster_epoch,
														 &active),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT(ticket_refs_equal(&active, &admission[0].ref));

	for (i = 0; i < 3; i++) {
		if (i > 0) {
			UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(
							 &admission[i].ref.identity.tag,
							 admission[i].ref.identity.cluster_epoch, &active),
						 PCM_X_QUEUE_OK);
			UT_ASSERT(ticket_refs_equal(&active, &admission[i].ref));
		}
		commit_empty_blocker_graph(&admission[i].ref, UINT64_C(100) + i);
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[i].ref), PCM_X_QUEUE_OK);
		drain_retire_and_detach_master(&admission[i]);
	}
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_wfg_snapshot_head_has_no_blockers_and_confirms_exactly)
{
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot *ticket;
	PcmXMasterWfgSnapshot snapshot;
	PcmXMasterWfgToken token;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(740, 0, 20, UINT64_C(15001)), UINT64_C(1901),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission.ref, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.blocker_count, 0);
	UT_ASSERT(ticket_refs_equal(&snapshot.token.ticket, &admission.ref));
	UT_ASSERT_EQ(snapshot.token.predecessor_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(snapshot.token.active_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	token = snapshot.token;

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_revalidate_exact(&token, UINT64_C(8101)),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_QUEUED);
	UT_ASSERT_EQ(ticket->graph_generation, UINT64_C(8101));
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission.ref, &snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(snapshot.blocker_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_revalidate_exact(&token, UINT64_C(8101)),
				 PCM_X_QUEUE_DUPLICATE);
}

UT_TEST(test_master_wfg_snapshot_second_ticket_names_true_predecessor)
{
	PcmXMasterAdmission admission[2];
	PcmXMasterWfgSnapshot snapshot;
	PcmXEnqueuePayload request;
	int i;

	init_active_pcm_x(UINT64_C(77));
	for (i = 0; i < 2; i++) {
		request = make_enqueue(make_wait_identity(741, i, (uint32)(21 + i), UINT64_C(15101) + i),
							   UINT64_C(1911) + i, UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		if (i == 0) {
			UT_ASSERT_EQ(
				cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission[i].ref, &snapshot),
				PCM_X_QUEUE_OK);
			UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_revalidate_exact(&snapshot.token,
																			 UINT64_C(8111)),
						 PCM_X_QUEUE_OK);
		}
	}

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission[1].ref, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.blocker_count, 1);
	UT_ASSERT(vertex_matches_ticket(&snapshot.blockers[0], &admission[0].ref));
	UT_ASSERT_EQ(snapshot.token.predecessor_slot.slot_index, admission[0].ticket_slot.slot_index);
	UT_ASSERT_EQ(snapshot.token.active_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
}

UT_TEST(test_master_wfg_snapshot_deduplicates_active_predecessor_and_keeps_distinct_pair)
{
	PcmXMasterAdmission admission[3];
	PcmXMasterWfgSnapshot snapshot;
	PcmXEnqueuePayload request;
	PcmXTicketRef active;
	int i;

	init_active_pcm_x(UINT64_C(77));
	for (i = 0; i < 3; i++) {
		request = make_enqueue(make_wait_identity(742, i, (uint32)(23 + i), UINT64_C(15201) + i),
							   UINT64_C(1921) + i, UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		if (i == 0) {
			UT_ASSERT_EQ(
				cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission[i].ref, &snapshot),
				PCM_X_QUEUE_OK);
			UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_revalidate_exact(&snapshot.token,
																			 UINT64_C(8121)),
						 PCM_X_QUEUE_OK);
			UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(
							 &admission[i].ref.identity.tag,
							 admission[i].ref.identity.cluster_epoch, &active),
						 PCM_X_QUEUE_OK);
		} else if (i == 1) {
			UT_ASSERT_EQ(
				cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission[i].ref, &snapshot),
				PCM_X_QUEUE_OK);
			UT_ASSERT_EQ(snapshot.blocker_count, 1);
			UT_ASSERT(vertex_matches_ticket(&snapshot.blockers[0], &admission[0].ref));
			UT_ASSERT_EQ(snapshot.token.active_slot.slot_index,
						 admission[0].ticket_slot.slot_index);
			UT_ASSERT_EQ(snapshot.token.predecessor_slot.slot_index,
						 admission[0].ticket_slot.slot_index);
			UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_revalidate_exact(&snapshot.token,
																			 UINT64_C(8122)),
						 PCM_X_QUEUE_OK);
		}
	}

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission[2].ref, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.blocker_count, 2);
	UT_ASSERT(vertex_matches_ticket(&snapshot.blockers[0], &admission[0].ref));
	UT_ASSERT(vertex_matches_ticket(&snapshot.blockers[1], &admission[1].ref));
}

UT_TEST(test_master_wfg_structural_mutation_invalidates_snapshot_before_confirm)
{
	PcmXMasterAdmission first;
	PcmXMasterAdmission second;
	PcmXMasterTicketSlot *ticket;
	PcmXMasterWfgSnapshot first_snapshot;
	PcmXMasterWfgSnapshot stale_snapshot;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(743, 0, 26, UINT64_C(15301)), UINT64_C(1931),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&first.ref, &first_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_admit_confirm_revalidate_exact(&first_snapshot.token, UINT64_C(8131)),
		PCM_X_QUEUE_OK);

	request = make_enqueue(make_wait_identity(743, 1, 27, UINT64_C(15302)), UINT64_C(1932),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &second), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&second.ref, &stale_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(stale_snapshot.blocker_count, 1);

	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&first.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_admit_confirm_revalidate_exact(&stale_snapshot.token, UINT64_C(8132)),
		PCM_X_QUEUE_STALE);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[second.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ADMITTING);
	UT_ASSERT_EQ(ticket->graph_generation, 0);

	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&second.ref, &stale_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(stale_snapshot.blocker_count, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_admit_confirm_revalidate_exact(&stale_snapshot.token, UINT64_C(8132)),
		PCM_X_QUEUE_OK);
}

UT_TEST(test_master_wfg_snapshot_and_confirm_reject_stale_exact_identity)
{
	PcmXMasterAdmission admission;
	PcmXMasterWfgSnapshot snapshot;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(744, 0, 28, UINT64_C(15401)), UINT64_C(1941),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&admission.ref, &snapshot),
				 PCM_X_QUEUE_OK);

	snapshot.token.ticket.identity.request_id++;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_admit_confirm_revalidate_exact(&snapshot.token, UINT64_C(8141)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_wfg_snapshot_exact(&snapshot.token.ticket, &snapshot),
				 PCM_X_QUEUE_STALE);
}

UT_TEST(test_master_success_transfer_completes_fifo_and_promotes_every_node)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[4];
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *tickets;
	PcmXTicketRef active;
	PcmXTicketRef transfer;
	uint64 queue_state;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 4; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(711, i, (uint32)(10 + i), UINT64_C(12001) + i),
						   UINT64_C(1301) + i, UINT64_C(1));

		request.identity.cluster_epoch = 0;
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(301) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap), UINT32_C(0x0f));
	queue_state = tag_slot->queue_state_sequence;
	UT_ASSERT_EQ(queue_state, 4);
	for (i = 0; i < 4; i++) {
		UT_ASSERT_EQ(
			cluster_pcm_x_master_promote_head_exact(
				&admission[i].ref.identity.tag, admission[i].ref.identity.cluster_epoch, &active),
			PCM_X_QUEUE_OK);
		UT_ASSERT(ticket_refs_equal(&active, &admission[i].ref));
		commit_empty_blocker_graph(&active, UINT64_C(301) + i);
		UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(999), &transfer),
					 PCM_X_QUEUE_STALE);
		UT_ASSERT_EQ(test_slot_state(&tickets[admission[i].ticket_slot.slot_index].slot),
					 PCM_XT_ACTIVE_PROBE);
		UT_ASSERT_EQ(
			cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(301) + i, &transfer),
			PCM_X_QUEUE_OK);
		UT_ASSERT(transfer.grant_generation != 0);
		UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&admission[i].ref, UINT64_C(301) + i,
															   &active),
					 PCM_X_QUEUE_DUPLICATE);
		UT_ASSERT(ticket_refs_equal(&active, &transfer));
		UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_DUPLICATE);
		queue_state++;
		UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state);
		UT_ASSERT_EQ(test_slot_state(&tickets[admission[i].ticket_slot.slot_index].slot),
					 PCM_XT_COMPLETE);
		UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & (UINT32_C(1) << i), 0);
		admission[i].ref = transfer;
		drain_retire_and_detach_master(&admission[i]);
	}
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 4);
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_blocker_replace_canonical_snapshot_and_revalidate)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entries[2];
	PcmXMasterBlockerEntry before_entries[2];
	PcmXMasterBlockerSnapshot snapshot;
	PcmXMasterBlockerSnapshot small_snapshot;
	ClusterLmdVertex b1 = make_blocker(1, 11, UINT64_C(8101));
	ClusterLmdVertex b2 = make_blocker(2, 12, UINT64_C(8102));
	ClusterLmdVertex blockers[3] = { b2, b1, b1 };
	Size used_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(770, 0, 7, UINT64_C(8001), UINT64_C(7001), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 1, blockers, lengthof(blockers), UINT32_C(0x11111111)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);
	UT_ASSERT_EQ(max_held_lwlock_count, 1);

	memset(entries, 0xA5, sizeof(entries));
	memcpy(before_entries, entries, sizeof(entries));
	memset(&small_snapshot, 0xA5, sizeof(small_snapshot));
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, entries, 1, &small_snapshot),
		PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT(memcmp(entries, before_entries, sizeof(entries)) == 0);
	UT_ASSERT_EQ(small_snapshot.blocker_count, 0);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, entries,
															 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&snapshot.ref, &admission.ref));
	UT_ASSERT_EQ(snapshot.set_generation, 1);
	UT_ASSERT_EQ(snapshot.blocker_count, 2);
	UT_ASSERT_EQ(snapshot.set_crc32c, UINT32_C(0x11111111));
	UT_ASSERT(blockers_equal(&entries[0].blocker, &b1));
	UT_ASSERT(blockers_equal(&entries[1].blocker, &b2));
	for (Size i = 0; i < lengthof(entries); i++) {
		UT_ASSERT_EQ(entries[i].chunk_no, i);
		UT_ASSERT_EQ(entries[i].reserved, 0);
		UT_ASSERT_EQ(test_slot_state(&blocker_slots(header)[entries[i].slot_ref.slot_index].slot),
					 2);
		UT_ASSERT_EQ(
			test_slot_generation(&blocker_slots(header)[entries[i].slot_ref.slot_index].slot),
			entries[i].slot_ref.slot_generation);
	}
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&snapshot, entries,
																		lengthof(entries)),
				 PCM_X_QUEUE_OK);
	entries[1].slot_ref.slot_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&snapshot, entries,
																		lengthof(entries)),
				 PCM_X_QUEUE_STALE);
	entries[1].slot_ref.slot_generation--;

	used_before = header->allocator[PCM_X_ALLOC_BLOCKER].used;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, NULL, 0,
																UINT32_C(0x11111111)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, used_before);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 3, blockers, lengthof(blockers), UINT32_C(0x33333333)),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, used_before);
}

UT_TEST(test_master_blocker_replace_grow_shrink_empty_reclaims)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot snapshot;
	ClusterLmdVertex blockers[3]
		= { make_blocker(0, 10, UINT64_C(8201)), make_blocker(1, 11, UINT64_C(8202)),
			make_blocker(2, 12, UINT64_C(8203)) };

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(771, 0, 8, UINT64_C(8002), UINT64_C(7002), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, blockers, 2,
																UINT32_C(0x101)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 2, blockers, 3,
																UINT32_C(0x102)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 3);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, blockers, 3,
																UINT32_C(0x101)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 3);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 3, &blockers[2], 1,
																UINT32_C(0x103)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 4, NULL, 0, UINT32_C(0x104)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.set_generation, 4);
	UT_ASSERT_EQ(snapshot.blocker_count, 0);
	UT_ASSERT_EQ(snapshot.set_crc32c, UINT32_C(0x104));
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&snapshot, NULL, 0),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_master_blocker_capacity_failure_is_byte_stable)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot ticket_before;
	PcmXAllocatorState allocator_before;
	PcmXBlockerSlot pool_before[8];
	PcmXSlotRef fillers[8];
	PcmXSlotHeader *slot;
	ClusterLmdVertex old_blocker = make_blocker(1, 21, UINT64_C(8301));
	ClusterLmdVertex replacement[2]
		= { make_blocker(2, 22, UINT64_C(8302)), make_blocker(3, 23, UINT64_C(8303)) };
	Size filler_count;
	Size pool_capacity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(772, 0, 9, UINT64_C(8003), UINT64_C(7003), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &old_blocker, 1,
																UINT32_C(0x201)),
				 PCM_X_QUEUE_OK);
	pool_capacity = header->layout.pools[PCM_X_POOL_BLOCKER].capacity;
	UT_ASSERT(pool_capacity <= lengthof(fillers));
	for (filler_count = 0; filler_count < pool_capacity - 2; filler_count++)
		UT_ASSERT_EQ(
			cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_BLOCKER, &fillers[filler_count], &slot),
			PCM_X_ALLOC_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, pool_capacity - 1);
	ticket_before = master_ticket_slots(header)[admission.ticket_slot.slot_index];
	allocator_before = header->allocator[PCM_X_ALLOC_BLOCKER];
	memcpy(pool_before, blocker_slots(header), sizeof(PcmXBlockerSlot) * pool_capacity);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 2, replacement, lengthof(replacement), UINT32_C(0x202)),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT(memcmp(&master_ticket_slots(header)[admission.ticket_slot.slot_index], &ticket_before,
					 sizeof(ticket_before))
			  == 0);
	UT_ASSERT(
		memcmp(&header->allocator[PCM_X_ALLOC_BLOCKER], &allocator_before, sizeof(allocator_before))
		== 0);
	UT_ASSERT(memcmp(blocker_slots(header), pool_before, sizeof(PcmXBlockerSlot) * pool_capacity)
			  == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(max_held_lwlock_count, 1);

	for (Size i = 0; i < filler_count; i++)
		UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_BLOCKER, fillers[i],
														   PCM_X_SLOT_RESERVED_NONVISIBLE),
					 PCM_X_ALLOC_OK);
}

UT_TEST(test_master_blocker_generation_and_duplicate_conflicts_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot before;
	ClusterLmdVertex blocker = make_blocker(1, 14, UINT64_C(8401));
	ClusterLmdVertex conflict[2]
		= { make_blocker(2, 15, UINT64_C(8402)), make_blocker(2, 15, UINT64_C(8402)) };
	ClusterLmdVertex too_many[4]
		= { make_blocker(0, 18, UINT64_C(8410)), make_blocker(1, 19, UINT64_C(8411)),
			make_blocker(2, 20, UINT64_C(8412)), make_blocker(3, 21, UINT64_C(8413)) };

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(773, 0, 10, UINT64_C(8004), UINT64_C(7004), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x301)),
				 PCM_X_QUEUE_OK);
	before = master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x301)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&master_ticket_slots(header)[admission.ticket_slot.slot_index], &before,
					 sizeof(before))
			  == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x302)),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&master_ticket_slots(header)[admission.ticket_slot.slot_index], &before,
					 sizeof(before))
			  == 0);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(774, 0, 11, UINT64_C(8005), UINT64_C(7005), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 1, too_many, lengthof(too_many), UINT32_C(0x300)),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 2, &blocker, 1,
																UINT32_C(0x303)),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, UINT64_MAX,
																&blocker, 1, UINT32_C(0x304)),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);

	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(775, 0, 12, UINT64_C(8006), UINT64_C(7006), 1, &admission);
	conflict[1].wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 1, conflict, lengthof(conflict), UINT32_C(0x305)),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
}

UT_TEST(test_master_blocker_snapshot_detects_slot_aba_and_corrupt_link)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry old_entry;
	PcmXMasterBlockerEntry new_entry;
	PcmXMasterBlockerSnapshot old_snapshot;
	PcmXMasterBlockerSnapshot new_snapshot;
	PcmXBlockerSlot *slot;
	ClusterLmdVertex old_blocker = make_blocker(1, 16, UINT64_C(8501));
	ClusterLmdVertex new_blocker = make_blocker(2, 17, UINT64_C(8502));

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(776, 0, 13, UINT64_C(8007), UINT64_C(7007), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &old_blocker, 1,
																UINT32_C(0x401)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &old_entry, 1, &old_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 2, NULL, 0, UINT32_C(0x402)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 3, &new_blocker, 1,
																UINT32_C(0x403)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &new_entry, 1, &new_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(new_entry.slot_ref.slot_index, old_entry.slot_ref.slot_index);
	UT_ASSERT(new_entry.slot_ref.slot_generation > old_entry.slot_ref.slot_generation);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&old_snapshot, &old_entry, 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&new_snapshot, &new_entry, 1),
		PCM_X_QUEUE_OK);

	slot = &blocker_slots(header)[new_entry.slot_ref.slot_index];
	slot->next_index = new_entry.slot_ref.slot_index;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &new_entry, 1, &new_snapshot),
		PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_blocker_snapshot_validates_every_owner_field)
{
	for (int variant = 0; variant < 7; variant++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission admission;
		PcmXMasterBlockerEntry entry;
		PcmXMasterBlockerSnapshot snapshot;
		PcmXMasterTicketSlot *ticket;
		PcmXBlockerSlot *slot;
		ClusterLmdVertex blocker
			= make_blocker(1, (uint32)(5 + variant), UINT64_C(8601) + (uint64)variant);

		init_active_pcm_x(UINT64_C(80) + (uint64)variant);
		header = ClusterPcmXConvertShmem;
		admit_active_probe((BlockNumber)(780 + variant), 0, (uint32)(14 + variant),
						   UINT64_C(8100) + (uint64)variant, UINT64_C(7100) + (uint64)variant, 1,
						   &admission);
		UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
						 &admission.ref, 1, &blocker, 1, UINT32_C(0x501) + (uint32)variant),
					 PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(
			cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &entry, 1, &snapshot),
			PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
		slot = &blocker_slots(header)[entry.slot_ref.slot_index];
		switch (variant) {
		case 0:
			slot->handle.ticket_id++;
			break;
		case 1:
			slot->handle.queue_generation++;
			break;
		case 2:
			slot->owner_slot_generation++;
			break;
		case 3:
			slot->owner_slot_index++;
			break;
		case 4:
			slot->set_generation++;
			break;
		case 5:
			slot->chunk_no++;
			break;
		default:
			slot->direction++;
			break;
		}
		UT_ASSERT_EQ(ticket->blocker_head_index, entry.slot_ref.slot_index);
		UT_ASSERT_EQ(
			cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &entry, 1, &snapshot),
			PCM_X_QUEUE_CORRUPT);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_master_blocker_sets_are_isolated_across_tickets)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission second;
	PcmXMasterBlockerEntry first_entry;
	PcmXMasterBlockerEntry second_entry;
	PcmXMasterBlockerSnapshot first_snapshot;
	PcmXMasterBlockerSnapshot second_snapshot;
	ClusterLmdVertex first_blocker = make_blocker(2, 9, UINT64_C(8701));
	ClusterLmdVertex second_blocker = make_blocker(3, 10, UINT64_C(8702));

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(790, 0, 7, UINT64_C(8201), UINT64_C(7201), 1, &first);
	admit_active_probe(791, 1, 8, UINT64_C(8202), UINT64_C(7202), 1, &second);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&first.ref, 1, &first_blocker, 1,
																UINT32_C(0x601)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&second.ref, 1, &second_blocker, 1,
																UINT32_C(0x602)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&first.ref, &first_entry, 1, &first_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&second.ref, &second_entry, 1,
															 &second_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(first_entry.slot_ref.slot_index != second_entry.slot_ref.slot_index);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);

	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_set_replace_exact(&first.ref, 2, NULL, 0, UINT32_C(0x603)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&first_snapshot, &first_entry, 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&second_snapshot, &second_entry, 1),
		PCM_X_QUEUE_OK);
}

UT_TEST(test_master_blocker_wire_stage_commits_exact_canonical_set)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entries[2];
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload begin;
	PcmXBlockerSetHeaderPayload next;
	PcmXBlockerChunkPayload edge[2];
	ClusterLmdVertex blockers[2];
	const uint64 source_session = UINT64_C(82501);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(791, 0, 11, UINT64_C(8251), UINT64_C(7251), 1, &admission);
	blockers[0] = make_blocker(2, 3, UINT64_C(8751));
	blockers[1] = make_blocker(2, 4, UINT64_C(8752));
	begin = make_blocker_header(&admission.ref, 1, blockers, lengthof(blockers));
	edge[0] = make_blocker_chunk(&admission.ref, 1, 0, blockers[0]);
	edge[1] = make_blocker_chunk(&admission.ref, 1, 1, blockers[1]);
	arm_blocker_probe(&admission.ref, 2, source_session);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, entries,
															 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[0], 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[0], 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[1], 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 2, source_session, entries,
																 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.set_generation, 1);
	UT_ASSERT_EQ(snapshot.blocker_count, 2);
	UT_ASSERT_EQ(snapshot.set_crc32c, begin.set_crc32c);
	UT_ASSERT(blockers_equal(&entries[0].blocker, &blockers[0]));
	UT_ASSERT(blockers_equal(&entries[1].blocker, &blockers[1]));
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 2, source_session, entries,
																 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);
	next = make_blocker_header(&admission.ref, 2, blockers, lengthof(blockers));
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&next, 2, source_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, entries,
																 lengthof(entries), UINT64_C(9251)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&next, 2, source_session),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_master_blocker_generation_is_scoped_by_holder_source)
{
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot first_snapshot;
	PcmXMasterBlockerSnapshot second_snapshot;
	PcmXBlockerSetHeaderPayload first;
	PcmXBlockerSetHeaderPayload second;
	const uint64 first_session = UINT64_C(82511);
	const uint64 second_session = UINT64_C(82512);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(791, 0, 12, UINT64_C(8252), UINT64_C(7252), 1, &admission);

	/* Each holder owns an independent local tag and therefore starts its
	 * blocker-set generation at one.  The authenticated source tuple, not a
	 * ticket-global comparison with the previous holder, namespaces that
	 * generation. */
	arm_blocker_probe(&admission.ref, 2, first_session);
	first = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&first, 2, first_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&first, 2, first_session, NULL, 0,
																 &first_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&first_snapshot, NULL, 0, UINT64_C(92511)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, first_session),
		PCM_X_QUEUE_OK);

	arm_blocker_probe(&admission.ref, 3, second_session);
	second = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&second, 3, second_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&second, 3, second_session, NULL,
																 0, &second_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&second_snapshot, NULL, 0, UINT64_C(92512)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 3, second_session),
		PCM_X_QUEUE_OK);
}

UT_TEST(test_master_blocker_crc_ignores_vertex_abi_padding)
{
	ClusterLmdVertex zero_padding;
	ClusterLmdVertex poisoned_padding;

	memset(&zero_padding, 0, sizeof(zero_padding));
	memset(&poisoned_padding, 0xA5, sizeof(poisoned_padding));
	zero_padding = make_blocker(2, 3, UINT64_C(8750));
	poisoned_padding.node_id = zero_padding.node_id;
	poisoned_padding.procno = zero_padding.procno;
	poisoned_padding.cluster_epoch = zero_padding.cluster_epoch;
	poisoned_padding.request_id = zero_padding.request_id;
	poisoned_padding.xid = zero_padding.xid;
	poisoned_padding.local_start_ts_ms = zero_padding.local_start_ts_ms;
	poisoned_padding.wait_seq = zero_padding.wait_seq;

	UT_ASSERT_EQ(blocker_set_crc32c(&zero_padding, 1), blocker_set_crc32c(&poisoned_padding, 1));
}

UT_TEST(test_master_blocker_wire_stage_crc_rejects_content_mismatch)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entry;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload begin;
	PcmXBlockerChunkPayload edge;
	PcmXBlockerSlot *slot;
	ClusterLmdVertex blocker = make_blocker(2, 3, UINT64_C(8759));
	const uint64 source_session = UINT64_C(82509);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(799, 0, 19, UINT64_C(8259), UINT64_C(7259), 1, &admission);
	begin = make_blocker_header(&admission.ref, 1, &blocker, 1);
	edge = make_blocker_chunk(&admission.ref, 1, 0, blocker);
	arm_blocker_probe(&admission.ref, 2, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge, 2, source_session),
				 PCM_X_QUEUE_OK);
	slot = &blocker_slots(header)[master_ticket_slots(header)[admission.ticket_slot.slot_index]
									  .blocker_stage_head_index];
	slot->blocker.wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 2, source_session, &entry,
																 1, &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		master_ticket_slots(header)[admission.ticket_slot.slot_index].blocker_stage_set_generation,
		1);
}

UT_TEST(test_master_blocker_stage_abort_reclaims_mixed_chain_and_fences_aba)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterProbeToken probe;
	PcmXPhasePayload probe_payload;
	PcmXBlockerSetHeaderPayload begin;
	PcmXBlockerChunkPayload edge;
	ClusterLmdVertex blockers[2];
	PcmXMasterTicketSlot *ticket;
	const uint64 source_session = UINT64_C(82519);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(798, 0, 18, UINT64_C(8258), UINT64_C(7258), 1, &admission);
	bind_local_master(2, admission.ref.identity.cluster_epoch, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 2, source_session,
															  &probe_payload, &probe),
				 PCM_X_QUEUE_OK);
	blockers[0] = make_blocker(2, 3, UINT64_C(8758));
	blockers[1] = make_blocker(2, 4, UINT64_C(8759));
	begin = make_blocker_header(&admission.ref, 1, blockers, 2);
	edge = make_blocker_chunk(&admission.ref, 1, 0, blockers[0]);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 2);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(ticket->blocker_stage_next_chunk, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_abort_exact(
					 &admission.ref, 1, 2, source_session, probe.state_sequence),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(ticket->blocker_stage_set_generation, 0);
	UT_ASSERT_EQ(ticket->blocker_stage_head_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ticket->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK);
	UT_ASSERT(ticket->reliable.state_sequence != probe.state_sequence);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_abort_exact(
					 &admission.ref, 1, 2, source_session, probe.state_sequence),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_master_blocker_probe_arm_is_zero_generation_and_idempotent)
{
	PcmXMasterAdmission admission;
	PcmXMasterProbeToken first;
	PcmXMasterProbeToken replay;
	PcmXPhasePayload first_payload;
	PcmXPhasePayload replay_payload;
	PcmXMasterTicketSlot *ticket;
	const uint64 source_session = UINT64_C(82520);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(799, 0, 19, UINT64_C(8259), UINT64_C(7259), 1, &admission);
	bind_local_master(2, admission.ref.identity.cluster_epoch, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 2, source_session,
															  &first_payload, &first),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(first_payload.reason, 0);
	UT_ASSERT_EQ(first_payload.phase, 0);
	UT_ASSERT_EQ(first_payload.flags, 0);
	UT_ASSERT(ticket_refs_equal(&first_payload.ref, &admission.ref));
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 2, source_session,
															  &replay_payload, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&first_payload, &replay_payload, sizeof(first_payload)) == 0);
	UT_ASSERT_EQ(first.state_sequence, replay.state_sequence);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(ticket->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK);
	UT_ASSERT_EQ(ticket->reliable.phase, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK);
}

UT_TEST(test_master_drive_work_cursor_is_bounded_and_exact)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXTicketRef active;
	BufferTag tag;
	Size cursor;
	uint64 cluster_epoch;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(805, 0, 25, UINT64_C(8302)), UINT64_C(7302), 1);
	request.identity.cluster_epoch = 0;
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(8302)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&admission.ref.identity.tag, 0, &active),
				 PCM_X_QUEUE_OK);
	cursor = admission.tag_slot.slot_index;
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_work_next(&cursor, 1, &tag, &cluster_epoch),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(BufferTagsEqual(&tag, &admission.ref.identity.tag));
	UT_ASSERT_EQ(cluster_epoch, admission.ref.identity.cluster_epoch);
	UT_ASSERT_EQ(cursor, (admission.tag_slot.slot_index + 1)
							 % header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity);

	UT_ASSERT_EQ(cluster_pcm_x_master_drive_work_next(&cursor, 1, &tag, &cluster_epoch),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_epoch, 0);
	UT_ASSERT(memcmp(&tag, &(BufferTag){ 0 }, sizeof(tag)) == 0);
}


UT_TEST(test_master_driver_waits_for_terminal_active_ticket_without_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterDriveSnapshot snapshot;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef active;
	BufferTag tag;
	Size cursor;
	uint64 cluster_epoch;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(807, 0, 27, UINT64_C(8304), UINT64_C(7304), 1, &admission);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	test_set_slot_state(&ticket->slot, PCM_XT_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &snapshot),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	cursor = admission.tag_slot.slot_index;
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_work_next(&cursor, 1, &tag, &cluster_epoch),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_master_drive_snapshot_and_holder_bitmaps_are_exact)
{
	PcmXMasterAdmission admission;
	PcmXMasterDriveSnapshot initial;
	PcmXMasterDriveSnapshot seeded;
	PcmXMasterDriveSnapshot acknowledged;
	PcmXMasterDriveSnapshot observed;
	uint32 holders = (UINT32_C(1) << 1) | (UINT32_C(1) << 2) | (UINT32_C(1) << 3);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(804, 3, 24, UINT64_C(8301), UINT64_C(7301), 1, &admission);

	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &initial),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&initial.ref, &admission.ref));
	UT_ASSERT_EQ(initial.ticket_state, PCM_XT_ACTIVE_PROBE);
	UT_ASSERT_EQ(initial.pending_s_holders_bitmap, 0);
	UT_ASSERT_EQ(initial.acked_s_holders_bitmap, 0);
	UT_ASSERT_EQ(initial.involved_nodes_bitmap, UINT32_C(1) << 3);

	UT_ASSERT_EQ(cluster_pcm_x_master_drive_bitmap_replace_exact(&initial, holders, 0, &seeded),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(seeded.pending_s_holders_bitmap, holders);
	UT_ASSERT_EQ(seeded.acked_s_holders_bitmap, 0);
	UT_ASSERT_EQ(seeded.involved_nodes_bitmap, UINT32_C(1) << 3);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_bitmap_replace_exact(&initial, holders, 0, &observed),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(observed.pending_s_holders_bitmap, holders);

	UT_ASSERT_EQ(cluster_pcm_x_master_drive_bitmap_replace_exact(&seeded, holders, UINT32_C(1) << 2,
																 &acknowledged),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(acknowledged.acked_s_holders_bitmap, UINT32_C(1) << 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_bitmap_replace_exact(&acknowledged, holders,
																 UINT32_C(1) << 4, &observed),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &observed),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(observed.acked_s_holders_bitmap, UINT32_C(1) << 2);
}

UT_TEST(test_master_drive_bitmap_replace_rejects_reliable_leg_drift)
{
	PcmXMasterAdmission admission;
	PcmXMasterDriveSnapshot before;
	PcmXMasterDriveSnapshot after;
	PcmXMasterProbeToken probe;
	PcmXPhasePayload payload;
	const uint64 source_session = UINT64_C(8302);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(805, 0, 25, UINT64_C(8302), UINT64_C(7302), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &before),
				 PCM_X_QUEUE_OK);
	bind_local_master(2, admission.ref.identity.cluster_epoch, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_probe_arm_exact(&admission.ref, 2, source_session,
															  &payload, &probe),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_drive_bitmap_replace_exact(&before, UINT32_C(1) << 2, 0, &after),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &after),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(after.state_sequence != before.state_sequence);
	UT_ASSERT_EQ(after.pending_opcode, PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK);
	UT_ASSERT_EQ(after.involved_nodes_bitmap,
				 (UINT32_C(1) << admission.ref.identity.node_id) | (UINT32_C(1) << 2));
}

UT_TEST(test_master_drive_snapshot_fails_closed_on_bitmap_corruption)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterDriveSnapshot snapshot;
	PcmXMasterTicketSlot *ticket;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(806, 0, 26, UINT64_C(8303), UINT64_C(7303), 1, &admission);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket->pending_s_holders_bitmap = UINT32_C(1) << 1;
	ticket->acked_s_holders_bitmap = UINT32_C(1) << 2;

	UT_ASSERT_EQ(cluster_pcm_x_master_drive_snapshot_exact(
					 &admission.ref.identity.tag, admission.ref.identity.cluster_epoch, &snapshot),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_transfer_wire_49_56_is_generation_exact)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload blocker_commit;
	PcmXTicketRef transfer;
	PcmXRevokePayload revoke;
	PcmXGrantPayload image_ready;
	PcmXGrantPayload prepare;
	PcmXInstallReadyPayload install_ready;
	PcmXPhasePayload commit;
	PcmXFinalAckPayload final_ack;
	PcmXMasterFinalAckToken final_ack_token;
	PcmXMasterFinalAckToken replay_final_ack_token;
	PcmXMasterFinalAckToken stale_final_ack_token;
	PcmXPhasePayload final_commit;
	PcmXPhasePayload final_confirm;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef guessed;
	uint64 expected_image_id;
	const uint64 requester_session = UINT64_C(7269);
	const uint64 source_session = UINT64_C(8269);
	const uint64 graph_generation = UINT64_C(9269);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(800, 0, 20, UINT64_C(8269), requester_session, 1, &admission);
	arm_blocker_probe(&admission.ref, 2, source_session);
	blocker_commit = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, graph_generation),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		test_master_begin_transfer_unclaimed_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_DUPLICATE);
	/* A lost type-48 ACK makes the holder replay the whole zero-edge set.
	 * ACTIVE_PROBE must retain enough exact evidence to replay that ACK. */
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	/* Even with the blocker graph fully committed, irreversible transfer is
	 * forbidden until the ticket carries the external pending-X claim. */
	UT_ASSERT_EQ(
		test_master_begin_transfer_unclaimed_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_BAD_STATE);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	before = *ticket;
	guessed = admission.ref;
	guessed.grant_generation = UINT64_C(9369);
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&guessed, graph_generation, &transfer),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(transfer.grant_generation, ticket->reliable.state_sequence);
	UT_ASSERT(transfer.grant_generation != UINT64_C(9369));
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_DUPLICATE);
	/* A delayed duplicate type-47 COMMIT can arrive after the exact ticket
	 * has already advanced into transfer.  The blocker stage proves this is
	 * the same published set and returns DUPLICATE; probe_complete then sees
	 * the later phase as BAD_STATE.  Re-emitting type 48 already closes this
	 * replay, so the GCS handler must not turn that benign phase race into a
	 * cluster-wide RECOVERY_BLOCKED transition. */
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT(!cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_DUPLICATE,
																		 PCM_X_QUEUE_BAD_STATE));
	UT_ASSERT(!cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_DUPLICATE,
																		 PCM_X_QUEUE_STALE));
	UT_ASSERT(!cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_DUPLICATE,
																		 PCM_X_QUEUE_NOT_READY));
	UT_ASSERT(!cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_DUPLICATE,
																		 PCM_X_QUEUE_BUSY));
	UT_ASSERT(cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_DUPLICATE,
																		PCM_X_QUEUE_CORRUPT));
	UT_ASSERT(cluster_pcm_x_blocker_commit_completion_requires_recovery(PCM_X_QUEUE_OK,
																		PCM_X_QUEUE_BAD_STATE));
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_master_revoke_arm_exact(&transfer, 2, source_session, &revoke),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(cluster_pcm_x_image_id_encode(0, 1, &expected_image_id));
	UT_ASSERT_EQ(revoke.image_id, expected_image_id);
	UT_ASSERT(revoke.image_id != transfer.grant_generation);
	UT_ASSERT_EQ(header->next_image_id, 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_revoke_arm_exact(&transfer, 2, source_session, &revoke),
				 PCM_X_QUEUE_DUPLICATE);

	memset(&image_ready, 0, sizeof(image_ready));
	image_ready.ref = transfer;
	image_ready.image.image_id = revoke.image_id;
	image_ready.image.source_own_generation = UINT64_C(44);
	image_ready.image.page_scn = UINT64_C(55);
	image_ready.image.page_lsn = UINT64_C(66);
	image_ready.image.source_node = 2;
	image_ready.image.page_checksum = UINT32_C(77);
	UT_ASSERT_EQ(cluster_pcm_x_master_image_ready_exact(&image_ready, 2, source_session, &prepare),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&prepare.ref, &transfer));
	UT_ASSERT(memcmp(&prepare.image, &image_ready.image, sizeof(prepare.image)) == 0);
	image_ready.image.image_id++;
	UT_ASSERT_EQ(cluster_pcm_x_master_image_ready_exact(&image_ready, 2, source_session, &prepare),
				 PCM_X_QUEUE_STALE);
	image_ready.image.image_id--;

	memset(&install_ready, 0, sizeof(install_ready));
	install_ready.ref = transfer;
	install_ready.image_id = revoke.image_id;
	install_ready.result = PCM_X_QUEUE_OK;
	install_ready.phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_install_ready_exact(&install_ready, 0, requester_session, &commit),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(commit.phase, PGRAC_IC_MSG_PCM_X_COMMIT_X);

	memset(&final_ack, 0, sizeof(final_ack));
	final_ack.ref = transfer;
	final_ack.image_id = revoke.image_id;
	final_ack.committed_own_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_final_ack_prepare_exact(&final_ack, 0, requester_session,
															  &final_ack_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ticket->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_COMMIT_X);
	replay_final_ack_token = final_ack_token;
	stale_final_ack_token = final_ack_token;
	stale_final_ack_token.state_sequence++;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_final_ack_finalize_exact(&stale_final_ack_token, &final_commit),
		PCM_X_QUEUE_STALE);
	stale_final_ack_token = final_ack_token;
	stale_final_ack_token.image.page_scn++;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_final_ack_finalize_exact(&stale_final_ack_token, &final_commit),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(ticket->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_COMMIT_X);
	UT_ASSERT_EQ(cluster_pcm_x_master_final_ack_finalize_exact(&final_ack_token, &final_commit),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(final_commit.phase, PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_final_ack_finalize_exact(&replay_final_ack_token, &final_commit),
		PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_final_ack_prepare_exact(&final_ack, 0, requester_session,
															  &final_ack_token),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_final_ack_finalize_exact(&final_ack_token, &final_commit),
				 PCM_X_QUEUE_DUPLICATE);
	final_ack.committed_own_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_final_ack_exact(&final_ack, 0, requester_session, &final_commit),
		PCM_X_QUEUE_STALE);

	memset(&final_confirm, 0, sizeof(final_confirm));
	final_confirm.ref = transfer;
	final_confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
	UT_ASSERT_EQ(cluster_pcm_x_master_final_confirm_exact(&final_confirm, 0, requester_session),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_COMPLETE);
	UT_ASSERT_EQ(test_slot_flags(&ticket->slot), 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_final_confirm_exact(&final_confirm, 0, requester_session),
				 PCM_X_QUEUE_DUPLICATE);
}

UT_TEST(test_master_grant_generation_exhaustion_never_wraps)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload blocker_commit;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef transfer;
	const uint64 source_session = UINT64_C(8270);
	const uint64 graph_generation = UINT64_C(9270);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(801, 0, 21, UINT64_C(8270), UINT64_C(7270), 1, &admission);
	arm_blocker_probe(&admission.ref, 2, source_session);
	blocker_commit = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, graph_generation),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket->reliable.state_sequence = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ACTIVE_PROBE);
	UT_ASSERT_EQ(ticket->ref.grant_generation, 0);
	UT_ASSERT_EQ(ticket->reliable.state_sequence, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_image_id_allocator_encodes_node31_and_never_wraps)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload blocker_commit;
	PcmXRevokePayload revoke;
	PcmXTicketRef transfer;
	uint64 expected_image_id;
	const uint64 source_session = UINT64_C(8271);
	const uint64 graph_generation = UINT64_C(9271);

	init_active_pcm_x(UINT64_C(77));
	cluster_node_id = 31;
	header = ClusterPcmXConvertShmem;
	admit_active_probe(802, 0, 22, UINT64_C(8271), UINT64_C(7271), 1, &admission);
	arm_blocker_probe(&admission.ref, 2, source_session);
	blocker_commit = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, graph_generation),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_OK);
	header->next_image_id = PCM_X_IMAGE_ID_SEQ_MASK;
	UT_ASSERT_EQ(cluster_pcm_x_master_revoke_arm_exact(&transfer, 2, source_session, &revoke),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(cluster_pcm_x_image_id_encode(31, PCM_X_IMAGE_ID_SEQ_MASK, &expected_image_id));
	UT_ASSERT_EQ(revoke.image_id, expected_image_id);
	UT_ASSERT_EQ(header->next_image_id, PCM_X_IMAGE_ID_SEQ_MASK + 1);

	/* The first raw value beyond the 56-bit domain is a durable exhausted
	 * marker.  It is neither masked nor wrapped into another wire identity. */
	init_active_pcm_x(UINT64_C(78));
	cluster_node_id = 31;
	header = ClusterPcmXConvertShmem;
	admit_active_probe(803, 0, 23, UINT64_C(8272), UINT64_C(7272), 1, &admission);
	arm_blocker_probe(&admission.ref, 2, source_session);
	blocker_commit = make_blocker_header(&admission.ref, 1, NULL, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&blocker_commit, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&blocker_commit, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, graph_generation),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 1, 2, source_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, graph_generation, &transfer),
		PCM_X_QUEUE_OK);
	header->next_image_id = PCM_X_IMAGE_ID_SEQ_MASK + 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_revoke_arm_exact(&transfer, 2, source_session, &revoke),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(revoke.image_id, 0);
	UT_ASSERT_EQ(header->next_image_id, PCM_X_IMAGE_ID_SEQ_MASK + 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_master_blocker_wire_stage_preserves_old_set_until_commit)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry old_entry;
	PcmXMasterBlockerSnapshot empty_snapshot;
	PcmXMasterBlockerSnapshot old_snapshot;
	PcmXBlockerSetHeaderPayload empty;
	PcmXTicketRef transfer;
	ClusterLmdVertex old_blocker = make_blocker(1, 5, UINT64_C(8761));
	const uint64 source_session = UINT64_C(82601);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(792, 0, 12, UINT64_C(8261), UINT64_C(7261), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(
					 &admission.ref, 1, &old_blocker, 1, blocker_set_crc32c(&old_blocker, 1)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &old_entry, 1, &old_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_graph_commit_exact(&old_snapshot, &old_entry, 1,
																 UINT64_C(9161)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &old_entry, 1, &old_snapshot),
		PCM_X_QUEUE_OK);
	empty = make_blocker_header(&admission.ref, 2, NULL, 0);
	arm_blocker_probe(&admission.ref, 3, source_session);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&empty, 3, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&old_snapshot, &old_entry, 1),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&empty, 3, source_session, NULL, 0,
																 &empty_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&old_snapshot, &old_entry, 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&empty_snapshot, NULL, 0, UINT64_C(9261)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_probe_complete_exact(&admission.ref, 2, 3, source_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9261), &transfer),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&empty, 3, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&empty, 3, source_session, NULL, 0,
																 &empty_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&empty, 3, source_session + 1,
																 NULL, 0, &empty_snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&empty, 3, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&empty, 3, source_session, NULL, 0,
																 &empty_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
}

UT_TEST(test_master_blocker_wire_stage_rejects_reorder_conflict_and_wrong_source)
{
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entries[2];
	PcmXMasterBlockerSnapshot snapshot;
	PcmXBlockerSetHeaderPayload begin;
	PcmXBlockerSetHeaderPayload conflict_begin;
	PcmXBlockerChunkPayload edge[2];
	PcmXBlockerChunkPayload conflict_edge;
	ClusterLmdVertex blockers[2];
	ClusterLmdVertex noncanonical;
	const uint64 source_session = UINT64_C(82701);

	init_active_pcm_x(UINT64_C(77));
	admit_active_probe(793, 0, 13, UINT64_C(8271), UINT64_C(7271), 1, &admission);
	blockers[0] = make_blocker(2, 6, UINT64_C(8771));
	blockers[1] = make_blocker(2, 7, UINT64_C(8772));
	begin = make_blocker_header(&admission.ref, 1, blockers, lengthof(blockers));
	edge[0] = make_blocker_chunk(&admission.ref, 1, 0, blockers[0]);
	edge[1] = make_blocker_chunk(&admission.ref, 1, 1, blockers[1]);
	arm_blocker_probe(&admission.ref, 2, source_session);

	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[0], 3, source_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[1], 2, source_session),
				 PCM_X_QUEUE_NOT_READY);
	conflict_begin = begin;
	conflict_begin.set_crc32c++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&conflict_begin, 2, source_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[0], 2, source_session),
				 PCM_X_QUEUE_OK);
	conflict_begin = begin;
	conflict_begin.nblockers = UINT32_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&conflict_begin, 2, source_session,
																 NULL, 0, &snapshot),
				 PCM_X_QUEUE_INVALID);
	conflict_edge = edge[0];
	conflict_edge.blocker.wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&conflict_edge, 2, source_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 2, source_session, entries,
																 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_NOT_READY);
	noncanonical = make_blocker(1, 8, UINT64_C(8773));
	conflict_edge = make_blocker_chunk(&admission.ref, 1, 1, noncanonical);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&conflict_edge, 2, source_session),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[1], 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 3, source_session, entries,
																 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_commit_exact(&begin, 2, source_session, entries,
																 lengthof(entries), &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&edge[0], 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);
	conflict_edge = edge[0];
	conflict_edge.blocker.wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_edge_exact(&conflict_edge, 2, source_session),
				 PCM_X_QUEUE_STALE);
}

UT_TEST(test_master_blocker_wire_stage_fences_transfer_and_cancel_until_commit)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerSnapshot empty_snapshot;
	PcmXBlockerSetHeaderPayload begin;
	PcmXTicketRef transfer;
	ClusterLmdVertex blocker = make_blocker(2, 9, UINT64_C(8781));
	const uint64 source_session = UINT64_C(82801);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(794, 0, 14, UINT64_C(8281), UINT64_C(7281), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, NULL, 0,
																blocker_set_crc32c(NULL, 0)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, NULL, 0, &empty_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&empty_snapshot, NULL, 0, UINT64_C(9281)),
		PCM_X_QUEUE_OK);
	begin = make_blocker_header(&admission.ref, 2, &blocker, 1);
	arm_blocker_probe(&admission.ref, 2, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);

	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9281), &transfer),
		PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(795, 0, 15, UINT64_C(8282), UINT64_C(7282), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, NULL, 0,
																blocker_set_crc32c(NULL, 0)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, NULL, 0, &empty_snapshot),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&empty_snapshot, NULL, 0, UINT64_C(9282)),
		PCM_X_QUEUE_OK);
	begin = make_blocker_header(&admission.ref, 2, &blocker, 1);
	arm_blocker_probe(&admission.ref, 2, source_session);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_stage_begin_exact(&begin, 2, source_session),
				 PCM_X_QUEUE_DUPLICATE);
}

UT_TEST(test_master_blocker_graph_commit_revalidates_exact_snapshot_and_generation)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entry;
	PcmXMasterBlockerEntry stale_entry;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXMasterBlockerSnapshot stale_snapshot;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	ClusterLmdVertex blocker = make_blocker(1, 1, UINT64_C(8801));

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(792, 0, 8, UINT64_C(8203), UINT64_C(7203), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x701)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &entry, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];

	stale_snapshot = snapshot;
	stale_snapshot.set_generation++;
	before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&stale_snapshot, &entry, 1, UINT64_C(9101)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);

	stale_entry = entry;
	stale_entry.slot_ref.slot_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &stale_entry, 1, UINT64_C(9101)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, 0),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);

	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9101)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ticket->graph_generation, UINT64_C(9101));
	before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9101)),
		PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9100)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9102)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ticket->graph_generation, UINT64_C(9102));
	UT_ASSERT_EQ(max_held_lwlock_count, 1);
}

UT_TEST(test_master_transfer_requires_committed_exact_empty_blocker_set)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entry;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef transfer;
	ClusterLmdVertex blocker = make_blocker(1, 2, UINT64_C(8802));

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(793, 0, 9, UINT64_C(8204), UINT64_C(7204), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x702)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &entry, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9201)),
		PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_master_pending_x_claim_exact(&admission.ref), PCM_X_QUEUE_OK);
	before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9201), &transfer),
		PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);

	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 2, NULL, 0, UINT32_C(0x703)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ticket->graph_generation, 0);
	before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9201), &transfer),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, UINT64_C(9202)),
		PCM_X_QUEUE_OK);
	/* The direct replace fixture bypasses the 45-48 wire ACK. */
	ticket->reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT;
	before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9201), &transfer),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission.ref, UINT64_C(9202), &transfer),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(transfer.grant_generation, ticket->reliable.state_sequence);
}

UT_TEST(test_master_active_probe_cancel_requires_committed_exact_empty_blocker_set)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterBlockerEntry entry;
	PcmXMasterBlockerSnapshot snapshot;
	PcmXMasterBlockerSnapshot stale_snapshot;
	PcmXMasterTagSlot tag_before;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before;
	PcmXMasterTicketSlot *ticket;
	ClusterLmdVertex blocker = make_blocker(1, 3, UINT64_C(8803));

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	admit_active_probe(794, 0, 10, UINT64_C(8205), UINT64_C(7205), 1, &admission);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 1, &blocker, 1,
																UINT32_C(0x704)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, &entry, 1, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, &entry, 1, UINT64_C(9401)),
		PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);

	stale_snapshot = snapshot;
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_set_replace_exact(&admission.ref, 2, NULL, 0, UINT32_C(0x705)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&stale_snapshot, &entry, 1, UINT64_C(9402)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_blocker_snapshot_exact(&admission.ref, NULL, 0, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, NULL, 0, UINT64_C(9402)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(tag_slot->active_index, PCM_X_INVALID_SLOT_INDEX);
}

UT_TEST(test_master_locator_defers_mutable_grant_exactness_to_domain_lock)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXTicketRef active;
	PcmXTicketRef transfer;
	PcmXQueueResult result;
	uint64 grant_generation;
	uint32 partition;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(743, 0, 9, UINT64_C(52001)), UINT64_C(5301), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(401)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(401));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(401), &transfer),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	grant_generation = transfer.grant_generation;

	/* Model a torn allocator-lock observation while the domain writer finishes
	 * publishing grant_generation.  Locator lookup must use immutable fields;
	 * the full ref is accepted only after the master-domain interlock restores
	 * the stable value. */
	ticket->ref.grant_generation = UINT64CONST(0x3333333322222222);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&transfer.identity.tag));
	grant_interlock_lock = &header->master_locks[partition].lock;
	grant_interlock_ticket = ticket;
	grant_interlock_generation = grant_generation;
	grant_interlock_armed = true;
	result = cluster_pcm_x_master_complete_exact(&transfer);
	UT_ASSERT_EQ(result, PCM_X_QUEUE_OK);
	UT_ASSERT(!grant_interlock_armed);
	grant_interlock_armed = false;
	grant_interlock_lock = NULL;
	grant_interlock_ticket = NULL;
}

UT_TEST(test_admission_replay_and_confirm_stay_frozen_after_transfer)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterAdmission replay;
	PcmXEnqueuePayload request;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef active;
	PcmXTicketRef transfer;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(713, 0, 22, UINT64_C(16001)), UINT64_C(1701),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(501)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(501));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(501), &transfer),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(ticket->ref.grant_generation, transfer.grant_generation);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &replay), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(replay.ref.grant_generation, 0);
	UT_ASSERT_EQ(replay.ref.handle.ticket_id, admission.ref.handle.ticket_id);
	UT_ASSERT_EQ(replay.ref.handle.queue_generation, admission.ref.handle.queue_generation);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(501)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ACTIVE_TRANSFER);
	UT_ASSERT_EQ(ticket->ref.grant_generation, transfer.grant_generation);
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_OK);
	admission.ref = transfer;
	drain_retire_and_detach_master(&admission);
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_application_replays_survive_terminal_successors)
{
	static const uint32 confirm_states[]
		= { PCM_XT_QUEUED,	 PCM_XT_ACTIVE_PROBE, PCM_XT_ACTIVE_TRANSFER,
			PCM_XT_COMPLETE, PCM_XT_CANCELLED,	  PCM_XT_RETIRE_CREDIT };
	static const uint32 complete_states[] = { PCM_XT_COMPLETE, PCM_XT_RETIRE_CREDIT };
	static const uint32 cancel_states[] = { PCM_XT_CANCELLED, PCM_XT_RETIRE_CREDIT };
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	Size i;

	for (i = 0; i < lengthof(confirm_states); i++) {
		PcmXMasterTagSlot tag_before;
		PcmXMasterTagSlot *tag_slot;
		PcmXMasterTicketSlot ticket_before;
		PcmXMasterTicketSlot *ticket;

		init_active_pcm_x(UINT64_C(77));
		request = make_enqueue(make_wait_identity(719, 0, 26, UINT64_C(24001)), UINT64_C(2501),
							   UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
		tag_slot = &master_tag_slots(ClusterPcmXConvertShmem)[admission.tag_slot.slot_index];
		ticket->graph_generation = UINT64_C(701);
		if (confirm_states[i] == PCM_XT_ACTIVE_TRANSFER || confirm_states[i] == PCM_XT_COMPLETE)
			ticket->ref.grant_generation = UINT64_C(2701);
		ticket->reliable.response_tombstone_mask = PCM_X_RESPONSE_TOMBSTONE_ADMIT_CONFIRM;
		test_set_slot_state(&ticket->slot, confirm_states[i]);
		ticket_before = *ticket;
		tag_before = *tag_slot;
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(701)),
					 PCM_X_QUEUE_DUPLICATE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
		UT_ASSERT(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(702))
				  != PCM_X_QUEUE_OK);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	}

	for (i = 0; i < lengthof(complete_states); i++) {
		PcmXMasterTagSlot tag_before;
		PcmXMasterTagSlot *tag_slot;
		PcmXMasterTicketSlot ticket_before;
		PcmXMasterTicketSlot *ticket;
		PcmXTicketRef completion_ref;
		PcmXTicketRef stale_ref;

		init_active_pcm_x(UINT64_C(77));
		request = make_enqueue(make_wait_identity(720, 0, 27, UINT64_C(24002)), UINT64_C(2502),
							   UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
		tag_slot = &master_tag_slots(ClusterPcmXConvertShmem)[admission.tag_slot.slot_index];
		ticket->graph_generation = UINT64_C(703);
		ticket->ref.grant_generation = UINT64_C(2702);
		ticket->reliable.response_tombstone_mask = PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
		completion_ref = ticket->ref;
		test_set_slot_state(&ticket->slot, complete_states[i]);
		ticket_before = *ticket;
		tag_before = *tag_slot;
		UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&completion_ref), PCM_X_QUEUE_DUPLICATE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
		stale_ref = completion_ref;
		stale_ref.grant_generation++;
		UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&stale_ref), PCM_X_QUEUE_STALE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	}

	for (i = 0; i < lengthof(cancel_states); i++) {
		PcmXMasterTagSlot tag_before;
		PcmXMasterTagSlot *tag_slot;
		PcmXMasterTicketSlot ticket_before;
		PcmXMasterTicketSlot *ticket;
		PcmXTicketRef stale_ref;

		init_active_pcm_x(UINT64_C(77));
		request = make_enqueue(make_wait_identity(721, 0, 28, UINT64_C(24003)), UINT64_C(2503),
							   UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
		tag_slot = &master_tag_slots(ClusterPcmXConvertShmem)[admission.tag_slot.slot_index];
		ticket->graph_generation = UINT64_C(704);
		ticket->reliable.response_tombstone_mask = PCM_X_RESPONSE_TOMBSTONE_CANCEL;
		test_set_slot_state(&ticket->slot, cancel_states[i]);
		ticket_before = *ticket;
		tag_before = *tag_slot;
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_DUPLICATE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
		stale_ref = admission.ref;
		stale_ref.handle.queue_generation++;
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&stale_ref), PCM_X_QUEUE_STALE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	}

	/* RETIRE_CREDIT alone is not outcome authority. */
	init_active_pcm_x(UINT64_C(77));
	request
		= make_enqueue(make_wait_identity(723, 0, 4, UINT64_C(24004)), UINT64_C(2504), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	{
		PcmXMasterTagSlot tag_before;
		PcmXMasterTagSlot *tag_slot;
		PcmXMasterTicketSlot ticket_before;
		PcmXMasterTicketSlot *ticket;
		PcmXTicketRef cross_complete;

		ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
		tag_slot = &master_tag_slots(ClusterPcmXConvertShmem)[admission.tag_slot.slot_index];
		ticket->ref.grant_generation = UINT64_C(2703);
		ticket->reliable.response_tombstone_mask = PCM_X_RESPONSE_TOMBSTONE_CANCEL;
		test_set_slot_state(&ticket->slot, PCM_XT_RETIRE_CREDIT);
		cross_complete = ticket->ref;
		ticket_before = *ticket;
		tag_before = *tag_slot;
		UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&cross_complete), PCM_X_QUEUE_BAD_STATE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	}

	init_active_pcm_x(UINT64_C(77));
	request
		= make_enqueue(make_wait_identity(724, 0, 5, UINT64_C(24005)), UINT64_C(2505), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	{
		PcmXMasterTagSlot tag_before;
		PcmXMasterTagSlot *tag_slot;
		PcmXMasterTicketSlot ticket_before;
		PcmXMasterTicketSlot *ticket;

		ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
		tag_slot = &master_tag_slots(ClusterPcmXConvertShmem)[admission.tag_slot.slot_index];
		ticket->reliable.response_tombstone_mask = PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
		test_set_slot_state(&ticket->slot, PCM_XT_RETIRE_CREDIT);
		ticket_before = *ticket;
		tag_before = *tag_slot;
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
		UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
		UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	}
}

UT_TEST(test_delayed_pretransfer_cancel_fences_active_transfer)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXMasterTicketSlot *ticket;
	PcmXRuntimeSnapshot snapshot;
	PcmXTicketRef active;
	PcmXTicketRef transfer;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(714, 0, 23, UINT64_C(16002)), UINT64_C(1702),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(502)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(502));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(502), &transfer),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_BAD_STATE);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(ticket->ref.grant_generation, transfer.grant_generation);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_recovery_blocked_runtime_cannot_promote_or_begin_transfer)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterAdmission queued_admission;
	PcmXEnqueuePayload request;
	PcmXEnqueuePayload queued_request;
	PcmXMasterTicketSlot *ticket;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before;
	PcmXMasterTagSlot tag_before;
	PcmXTicketRef active;
	PcmXTicketRef transfer;
	PcmXSlotRef found;
	uint64 queue_state;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(716, 0, 25, UINT64_C(21001)), UINT64_C(2201),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(601)),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	queue_state = tag_slot->queue_state_sequence;
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(
					 request.identity.node_id, request.identity.cluster_epoch,
					 request.prehandle.sender_session_incarnation + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_QUEUED);
	UT_ASSERT_EQ(tag_slot->active_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(717, 0, 25, UINT64_C(21002)), UINT64_C(2202),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(602)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(602));
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	queue_state = tag_slot->queue_state_sequence;
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(
					 request.identity.node_id, request.identity.cluster_epoch,
					 request.prehandle.sender_session_incarnation + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(602), &transfer),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_ACTIVE_PROBE);
	UT_ASSERT_EQ(ticket->ref.grant_generation, 0);
	UT_ASSERT_EQ(tag_slot->active_index, admission.ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(718, 0, 25, UINT64_C(21003)), UINT64_C(2203),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE,
									admission.ref.identity.node_id);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(
					 request.identity.node_id, request.identity.cluster_epoch,
					 request.prehandle.sender_session_incarnation + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &admission.ref.handle, &found),
		PCM_X_DIRECTORY_OK);

	/* A blocked runtime has no FINAL_ACK proof and must retain transfer evidence. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(733, 0, 2, UINT64_C(40001)), UINT64_C(4101), UINT64_C(1));
	queued_request
		= make_enqueue(make_wait_identity(733, 1, 3, UINT64_C(40002)), UINT64_C(4102), UINT64_C(1));
	bind_enqueue_peer(&request);
	bind_enqueue_peer(&queued_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&queued_request, &queued_admission),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(801)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&queued_admission.ref, UINT64_C(802)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(801));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(801), &transfer),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(
					 request.identity.node_id, request.identity.cluster_epoch,
					 request.prehandle.sender_session_incarnation + 1),
				 PCM_X_QUEUE_STALE);
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT_EQ(
		test_slot_state(&master_ticket_slots(header)[admission.ticket_slot.slot_index].slot),
		PCM_XT_ACTIVE_TRANSFER);
	UT_ASSERT_EQ(
		test_slot_state(&master_ticket_slots(header)[queued_admission.ticket_slot.slot_index].slot),
		PCM_XT_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_NOT_READY);
}

UT_TEST(test_recovery_blocked_runtime_refuses_ack_and_local_mutators)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot ticket_before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXRetirePayload retire_ack;
	PcmXTerminalLegToken retire_token;
	PcmXLocalCutoff cutoff;
	PcmXLocalHandle leader;
	PcmXLocalHandle output;
	PcmXLocalMembershipSlot member_before;
	PcmXLocalMembershipSlot *member;
	PcmXLocalTagSlot tag_before;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(734, 0, 2, UINT64_C(43001)), UINT64_C(4401), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket_before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_peer_bind_ack_publish(0, request.identity.cluster_epoch,
											request.prehandle.sender_session_incarnation + 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(901)),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket_before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_peer_bind_ack_publish(0, request.identity.cluster_epoch,
											request.prehandle.sender_session_incarnation + 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, admission.ref.identity.node_id,
					 request.prehandle.sender_session_incarnation),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission.ref.identity.node_id);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, admission.ref.identity.node_id,
					 request.prehandle.sender_session_incarnation, &retire_token),
				 PCM_X_QUEUE_OK);
	retire_ack = make_retire_ack(&admission.ref, admission.ref.identity.node_id);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket_before = *ticket;
	UT_ASSERT_EQ(
		cluster_pcm_x_peer_bind_ack_publish(0, request.identity.cluster_epoch,
											request.prehandle.sender_session_incarnation + 1),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_retire_ack_exact(&retire_ack, admission.ref.identity.node_id,
											  request.prehandle.sender_session_incarnation),
		PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(735, 0, 2, UINT64_C(43002));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(4501));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(4501), &leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	member = &membership_slots(header)[leader.membership_slot.slot_index];
	tag_before = *tag_slot;
	member_before = *member;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(1, identity.cluster_epoch, UINT64_C(4502)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, &output), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(memcmp(member, &member_before, sizeof(*member)) == 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, identity.cluster_epoch, UINT64_C(4501));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(4501), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	member = &membership_slots(header)[leader.membership_slot.slot_index];
	tag_before = *tag_slot;
	member_before = *member;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(1, identity.cluster_epoch, UINT64_C(4502)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &output),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(memcmp(member, &member_before, sizeof(*member)) == 0);
}

UT_TEST(test_master_cancel_is_exact_and_unlinks_middle_without_fifo_damage)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[3];
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *tickets;
	PcmXTerminalLegToken drain_token;
	PcmXTerminalLegToken retire_token;
	PcmXRetirePayload retire_ack0;
	PcmXRetirePayload retire_ack1;
	PcmXTicketRef stale;
	PcmXSlotRef found;
	uint64 queue_state_before;
	uint64 responder_session;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 3; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(705, i, (uint32)(14 + i), UINT64_C(6001) + i),
						   UINT64_C(1001) + i, UINT64_C(1));

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(200) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	queue_state_before = tag_slot->queue_state_sequence;
	stale = admission[1].ref;
	stale.handle.queue_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&stale), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state_before);
	UT_ASSERT_EQ(tag_slot->head_index, admission[0].ticket_slot.slot_index);
	UT_ASSERT_EQ(tickets[admission[0].ticket_slot.slot_index].next_index,
				 admission[1].ticket_slot.slot_index);

	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state_before + 1);
	queue_state_before = tag_slot->queue_state_sequence;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state_before);
	UT_ASSERT_EQ(test_slot_state(&tickets[admission[1].ticket_slot.slot_index].slot),
				 PCM_XT_CANCELLED);
	UT_ASSERT_EQ(tickets[admission[0].ticket_slot.slot_index].next_index,
				 admission[2].ticket_slot.slot_index);
	UT_ASSERT_EQ(tickets[admission[2].ticket_slot.slot_index].prev_index,
				 admission[0].ticket_slot.slot_index);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap) & (UINT32_C(1) << 1), 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[1].ref),
				 PCM_X_QUEUE_NOT_READY);
	responder_session = header->peer_frontiers[1].sender_session_incarnation;
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(&admission[1].ref,
															 PCM_X_TERMINAL_LEG_DRAIN, 1,
															 responder_session, &drain_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(
					 &admission[1].ref, PCM_X_TERMINAL_LEG_DRAIN, 0,
					 header->peer_frontiers[0].sender_session_incarnation),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(
					 &admission[1].ref, PCM_X_TERMINAL_LEG_DRAIN, 1, responder_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&tickets[admission[1].ticket_slot.slot_index].slot),
				 PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(&admission[1].ref,
															 PCM_X_TERMINAL_LEG_RETIRE, 1,
															 responder_session, &retire_token),
				 PCM_X_QUEUE_OK);
	retire_ack0 = make_retire_ack(&admission[1].ref, 0);
	retire_ack1 = make_retire_ack(&admission[1].ref, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(
					 &retire_ack0, 0, header->peer_frontiers[0].sender_session_incarnation),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire_ack1, 1, responder_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[1].ref),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 3);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
											  &admission[1].prehandle, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, admission[1].ticket_slot));

	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[0].ref), PCM_X_QUEUE_OK);
	drain_retire_and_detach_master(&admission[0]);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[1].ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[2].ref), PCM_X_QUEUE_OK);
	drain_retire_and_detach_master(&admission[2]);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 3);
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_prehandle_cancel_replays_exactly_and_never_hits_reused_slot)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission cancelled;
	PcmXMasterAdmission replay;
	PcmXMasterAdmission successor;
	PcmXPrehandleCancelPayload cancel;
	PcmXEnqueuePayload first_request;
	PcmXEnqueuePayload successor_request;
	PcmXMasterTicketSlot *successor_ticket;
	PcmXMasterTicketSlot successor_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	first_request
		= make_enqueue(make_wait_identity(706, 0, 10, UINT64_C(61001)), UINT64_C(6101), 1);
	bind_enqueue_peer(&first_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
	memset(&cancel, 0, sizeof(cancel));
	cancel.identity = first_request.identity;
	cancel.prehandle = first_request.prehandle;

	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&cancel, &cancelled), PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&cancelled.ref, &first.ref));
	UT_ASSERT(memcmp(&cancelled.prehandle, &first.prehandle, sizeof(first.prehandle)) == 0);
	UT_ASSERT_EQ(test_slot_state(&master_ticket_slots(header)[first.ticket_slot.slot_index].slot),
				 PCM_XT_CANCELLED);
	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&cancel, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &cancelled.ref));
	UT_ASSERT(memcmp(&replay.prehandle, &cancelled.prehandle, sizeof(replay.prehandle)) == 0);

	drain_retire_and_detach_master(&cancelled);

	successor_request
		= make_enqueue(make_wait_identity(706, 0, 11, UINT64_C(61002)), UINT64_C(6101), 2);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&successor_request, &successor), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(successor.ticket_slot.slot_index, first.ticket_slot.slot_index);
	UT_ASSERT(successor.ticket_slot.slot_generation != first.ticket_slot.slot_generation);
	successor_ticket = &master_ticket_slots(header)[successor.ticket_slot.slot_index];
	successor_before = *successor_ticket;
	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&cancel, &replay),
				 PCM_X_QUEUE_RETIRED);
	UT_ASSERT(memcmp(successor_ticket, &successor_before, sizeof(*successor_ticket)) == 0);
}

UT_TEST(test_master_prehandle_identity_alias_is_corruption_not_stale_cancel)
{
	PcmXMasterAdmission admission;
	PcmXMasterAdmission output;
	PcmXPrehandleCancelPayload alias;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(709, 0, 14, UINT64_C(64001)), UINT64_C(6401), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	memset(&alias, 0, sizeof(alias));
	alias.identity = request.identity;
	alias.identity.wait_seq++;
	alias.prehandle = request.prehandle;
	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&alias, &output), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(
		test_slot_state(
			&master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index].slot),
		PCM_XT_ADMITTING);
}

UT_TEST(test_master_prehandle_cancel_first_publishes_terminal_tombstone_before_late_enqueue)
{
	PcmXMasterAdmission cancelled;
	PcmXMasterAdmission late_admission;
	PcmXMasterAdmission replay;
	PcmXPrehandleCancelPayload cancel;
	PcmXEnqueuePayload request;
	PcmXMasterTicketSlot *ticket;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(708, 0, 13, UINT64_C(63001)), UINT64_C(6301), 1);
	bind_enqueue_peer(&request);
	memset(&cancel, 0, sizeof(cancel));
	cancel.identity = request.identity;
	cancel.prehandle = request.prehandle;

	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&cancel, &cancelled), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[cancelled.ticket_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_CANCELLED);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->peer_frontiers[0].next_expected_prehandle_sequence, 2);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	UT_ASSERT_EQ(
		master_tag_slots(ClusterPcmXConvertShmem)[cancelled.tag_slot.slot_index].head_index,
		PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(
		pg_atomic_read_u32(&master_tag_slots(ClusterPcmXConvertShmem)[cancelled.tag_slot.slot_index]
								.queued_node_bitmap),
		0);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &late_admission),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&late_admission.ref, &cancelled.ref));
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_CANCELLED);
	UT_ASSERT_EQ(cluster_pcm_x_master_prehandle_cancel_exact(&cancel, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&replay.ref, &cancelled.ref));
}

UT_TEST(test_master_cancel_rejects_active_probe_without_mutation)
{
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef active;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(707, 0, 12, UINT64_C(62001)), UINT64_C(6201), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission.ref, UINT64_C(6202)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(ClusterPcmXConvertShmem)[admission.ticket_slot.slot_index];
	before = *ticket;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_reversible_exact(&active), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_master_unlink_corruption_is_byte_stable_before_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[3];
	PcmXMasterTagSlot tag_before;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before[3];
	PcmXMasterTicketSlot *tickets;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 3; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(748, i, (uint32)(20 + i), UINT64_C(61001) + i),
						   UINT64_C(6201) + i, UINT64_C(1));

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(701) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	/* Corrupt only the successor backlink.  Validation must not first splice
	 * the predecessor out of the queue and erase the original evidence. */
	tickets[admission[2].ticket_slot.slot_index].prev_index = PCM_X_INVALID_SLOT_INDEX;
	tag_before = *tag_slot;
	for (i = 0; i < 3; i++)
		ticket_before[i] = tickets[admission[i].ticket_slot.slot_index];

	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	for (i = 0; i < 3; i++)
		UT_ASSERT(memcmp(&tickets[admission[i].ticket_slot.slot_index], &ticket_before[i],
						 sizeof(ticket_before[i]))
				  == 0);
}

UT_TEST(test_master_cancel_requires_state_exact_active_locator)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[2];
	PcmXMasterTagSlot tag_before;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before[2];
	PcmXMasterTicketSlot *tickets;
	PcmXEnqueuePayload request;
	PcmXTicketRef active;
	int i;

	/* An ACTIVE_PROBE cannot be cancelled after its active locator is lost. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(750, 0, 2, UINT64_C(64001)), UINT64_C(6501), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[0]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[0].ref, UINT64_C(801)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&request.identity.tag,
														 request.identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	tag_slot->active_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->active_slot_generation = 0;
	tag_before = *tag_slot;
	ticket_before[0] = tickets[admission[0].ticket_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&active), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(memcmp(&tickets[admission[0].ticket_slot.slot_index], &ticket_before[0],
					 sizeof(ticket_before[0]))
			  == 0);

	/* Conversely a QUEUED ticket cannot be named by the active locator. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 2; i++) {
		request = make_enqueue(make_wait_identity(751, i, (uint32)(2 + i), UINT64_C(64002) + i),
							   UINT64_C(6502) + i, 1);
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(802) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	tickets = master_ticket_slots(header);
	tag_slot->active_index = admission[1].ticket_slot.slot_index;
	tag_slot->active_slot_generation = admission[1].ticket_slot.slot_generation;
	tag_before = *tag_slot;
	for (i = 0; i < 2; i++)
		ticket_before[i] = tickets[admission[i].ticket_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	for (i = 0; i < 2; i++)
		UT_ASSERT(memcmp(&tickets[admission[i].ticket_slot.slot_index], &ticket_before[i],
						 sizeof(ticket_before[i]))
				  == 0);
}

UT_TEST(test_terminal_ack_requires_prearmed_leg_and_is_byte_stable)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXRetirePayload retire_ack;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(740, 0, 2, UINT64_C(50001)), UINT64_C(5101), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	retire_ack = make_retire_ack(&admission.ref, request.identity.node_id);
	before = *ticket;

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
					 request.prehandle.sender_session_incarnation),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_retire_ack_exact(&retire_ack, request.identity.node_id,
											  request.prehandle.sender_session_incarnation),
		PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
}

UT_TEST(test_terminal_ack_wire_fields_are_exact_and_zero_side_effect)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXRetirePayload retire_ack;
	PcmXTicketRef resolved;
	PcmXTicketRef work;
	PcmXTerminalLegToken drain;
	PcmXTerminalLegToken replay;
	PcmXTerminalLegToken retire;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(741, 0, 2, UINT64_C(50002)), UINT64_C(5102), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_work_next(&work), PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&work, &admission.ref));
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	retire_ack = make_retire_ack(&admission.ref, request.identity.node_id);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
					 request.prehandle.sender_session_incarnation, &drain),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(drain.kind, PCM_X_TERMINAL_LEG_DRAIN);
	UT_ASSERT_EQ(drain.expected_responder_node, request.identity.node_id);
	UT_ASSERT_EQ(drain.expected_responder_session, request.prehandle.sender_session_incarnation);
	UT_ASSERT(drain.state_sequence != 0);
	UT_ASSERT(drain.drain_generation != 0);
	UT_ASSERT_EQ(ticket->drain_generation, drain.drain_generation);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
					 request.prehandle.sender_session_incarnation, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &drain, sizeof(replay)) == 0);
	before = *ticket;

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref, drain.kind, 1,
															 drain.expected_responder_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref, drain.kind,
															 drain.expected_responder_node,
															 drain.expected_responder_session + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire_ack, drain.expected_responder_node,
													   drain.expected_responder_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref, drain.kind,
															 drain.expected_responder_node,
															 drain.expected_responder_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(ticket->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(ticket->reliable.phase, 0);
	UT_ASSERT_EQ(ticket->reliable.expected_responder_node, 0);
	UT_ASSERT_EQ(ticket->reliable.expected_responder_session, 0);

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, request.identity.node_id,
					 request.prehandle.sender_session_incarnation, &retire),
				 PCM_X_QUEUE_OK);
	before = *ticket;
	/* A closed DRAIN key replays without clearing the newly armed RETIRE leg. */
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref, drain.kind,
															 drain.expected_responder_node,
															 drain.expected_responder_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	memset(&resolved, 0, sizeof(resolved));
	UT_ASSERT_EQ(
		cluster_pcm_x_master_retire_ack_resolve_exact(&retire_ack, retire.expected_responder_node,
													  retire.expected_responder_session, &resolved),
		PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&resolved, &admission.ref));
	before = *ticket;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire_ack, retire.expected_responder_node,
													   retire.expected_responder_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
}

UT_TEST(test_retire_ack_uses_persistent_exact_key_index)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXRetirePayload retire0;
	PcmXRetirePayload retire1;
	PcmXRetirePayload stale;
	PcmXSlotRef found;
	PcmXTerminalLegToken token;
	uint64 session0;
	uint64 session1 = UINT64_C(6102);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(745, 0, 2, UINT64_C(50006)), UINT64_C(5106), UINT64_C(1));
	bind_enqueue_peer(&request);
	session0 = request.prehandle.sender_session_incarnation;
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(1, request.identity.cluster_epoch, session1),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket->involved_nodes_bitmap |= UINT32_C(1) << 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	retire0 = make_retire_ack(&admission.ref, 0);
	retire1 = make_retire_ack(&admission.ref, 1);
	before = *ticket;

	/* A byte-exact ACK has no authority until arm publishes its shmem key. */
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire0, 0, session0), PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire0, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0, session0, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire0, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, admission.ticket_slot));
	before = *ticket;

	stale = retire0;
	stale.cluster_epoch++;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&stale, 0, session0), PCM_X_QUEUE_STALE);
	stale = retire0;
	stale.master_session_incarnation++;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&stale, 0, session0), PCM_X_QUEUE_STALE);
	stale = retire0;
	stale.retire_through_ticket_id++;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&stale, 0, session0), PCM_X_QUEUE_STALE);
	stale = retire0;
	stale.sender_node = 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&stale, 1, session1), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire0, 1, session1), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire0, 0, session0 + 1),
				 PCM_X_QUEUE_STALE);
	stale = retire0;
	stale.flags = 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&stale, 0, session0), PCM_X_QUEUE_INVALID);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire0, 0, session0), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1, session1, &token),
				 PCM_X_QUEUE_OK);
	before = *ticket;
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire0, 0, session0),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire1, 1, session1), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ticket->retire_acked_nodes_bitmap, ticket->involved_nodes_bitmap);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire0, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire1, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
}

UT_TEST(test_early_retire_arm_does_not_publish_locator)
{
	PcmXMasterAdmission admission;
	PcmXEnqueuePayload request;
	PcmXRetirePayload retire;
	PcmXSlotRef found;
	PcmXTerminalLegToken token;

	init_active_pcm_x(UINT64_C(77));
	request = make_enqueue(make_wait_identity(749, 0, 13, UINT64_C(61001)), UINT64_C(6201), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	retire = make_retire_ack(&admission.ref, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0,
					 request.prehandle.sender_session_incarnation, &token),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_RETIRE, &retire, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_terminal_outcome_mask_corruption_is_fail_closed)
{
	int mode;

	for (mode = 0; mode < 3; mode++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission admission;
		PcmXMasterTicketSlot before;
		PcmXMasterTicketSlot *ticket;
		PcmXEnqueuePayload request;
		PcmXTerminalLegToken token;
		PcmXQueueResult result;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		request = make_enqueue(make_wait_identity(742 + mode, 0, 2, UINT64_C(50003) + mode),
							   UINT64_C(5103) + mode, UINT64_C(1));
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
		if (mode == 0)
			ticket->reliable.response_tombstone_mask &= ~PCM_X_RESPONSE_TOMBSTONE_CANCEL;
		else if (mode == 1)
			ticket->reliable.response_tombstone_mask |= PCM_X_RESPONSE_TOMBSTONE_COMPLETE;
		else
			ticket->reliable.response_tombstone_mask |= UINT64_C(1) << 63;
		before = *ticket;
		if (mode == 0)
			result = cluster_pcm_x_master_terminal_leg_arm_exact(
				&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
				request.prehandle.sender_session_incarnation, &token);
		else
			result = cluster_pcm_x_master_terminal_leg_ack_exact(
				&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
				request.prehandle.sender_session_incarnation);
		UT_ASSERT_EQ(result, PCM_X_QUEUE_CORRUPT);
		UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_terminal_bitmap_superset_is_fail_closed_before_arm_or_ack_mutation)
{
	int mode;

	for (mode = 0; mode < 2; mode++) {
		PcmXShmemHeader *header;
		PcmXMasterAdmission admission;
		PcmXMasterTicketSlot before;
		PcmXMasterTicketSlot *ticket;
		PcmXEnqueuePayload request;
		PcmXTerminalLegToken token;
		PcmXQueueResult result;

		init_active_pcm_x(UINT64_C(77));
		header = ClusterPcmXConvertShmem;
		request = make_enqueue(
			make_wait_identity(752 + mode, 0, (uint32)(2 + mode), UINT64_C(66001) + mode),
			UINT64_C(6701) + mode, 1);
		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
		ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
		if (mode == 1)
			UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
							 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
							 request.prehandle.sender_session_incarnation, &token),
						 PCM_X_QUEUE_OK);
		/* Node 1 never participated.  Neither arming nor consuming a valid
		 * node-0 ACK may let this impossible superset wedge in ACTIVE. */
		ticket->drained_nodes_bitmap = UINT32_C(1) << 1;
		before = *ticket;
		if (mode == 0)
			result = cluster_pcm_x_master_terminal_leg_arm_exact(
				&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
				request.prehandle.sender_session_incarnation, &token);
		else
			result = cluster_pcm_x_master_terminal_leg_ack_exact(
				&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, token.expected_responder_node,
				token.expected_responder_session);
		UT_ASSERT_EQ(result, PCM_X_QUEUE_CORRUPT);
		UT_ASSERT(memcmp(ticket, &before, sizeof(*ticket)) == 0);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_terminal_detach_missing_retire_ack_is_retryable_not_ready)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot tag_before;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	uint64 session1 = UINT64_C(6802);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(754, 0, 2, UINT64_C(66003)), UINT64_C(6801), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(1, request.identity.cluster_epoch, session1),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket->involved_nodes_bitmap |= UINT32_C(1) << 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	UT_ASSERT_EQ(ticket->retire_acked_nodes_bitmap, UINT32_C(1));
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	ticket_before = *ticket;
	tag_before = *tag_slot;

	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);

	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_OK);
	assert_master_queue_baseline(header);
}

UT_TEST(test_terminal_retry_skips_drained_responder_while_next_leg_is_armed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXTerminalLegToken pending;
	PcmXTerminalLegToken replay;
	PcmXTerminalLegToken skipped;
	PcmXRetirePayload retire_ack;
	uint64 session0;
	uint64 session1 = UINT64_C(6804);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(754, 0, 2, UINT64_C(66004)), UINT64_C(6803), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(1, request.identity.cluster_epoch, session1),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	ticket->involved_nodes_bitmap |= UINT32_C(1) << 1;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	session0 = header->peer_frontiers[0].sender_session_incarnation;
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0, session0, &skipped),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref,
															 PCM_X_TERMINAL_LEG_DRAIN, 0, session0),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1, session1, &pending),
				 PCM_X_QUEUE_OK);

	/* The retry driver scans participants from node zero on every pass.  A
	 * completed earlier responder must not hide the exact later leg behind
	 * BUSY, or the later DRAIN can never be resent. */
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 0, session0, &skipped),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, 1, session1, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &pending, sizeof(replay)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref,
															 PCM_X_TERMINAL_LEG_DRAIN, 1, session1),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0, session0, &skipped),
				 PCM_X_QUEUE_OK);
	retire_ack = make_retire_ack(&admission.ref, 0);
	UT_ASSERT_EQ(cluster_pcm_x_master_retire_ack_exact(&retire_ack, 0, session0), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1, session1, &pending),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 0, session0, &skipped),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, 1, session1, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &pending, sizeof(replay)) == 0);
}

UT_TEST(test_terminal_detach_rejects_pending_leg)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot tag_before;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot ticket_before;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXTerminalLegToken drain;
	PcmXTerminalLegToken retire;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(744, 0, 2, UINT64_C(50005)), UINT64_C(5105), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_DRAIN, request.identity.node_id,
					 request.prehandle.sender_session_incarnation, &drain),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_ack_exact(&admission.ref, drain.kind,
															 drain.expected_responder_node,
															 drain.expected_responder_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_terminal_leg_arm_exact(
					 &admission.ref, PCM_X_TERMINAL_LEG_RETIRE, request.identity.node_id,
					 request.prehandle.sender_session_incarnation, &retire),
				 PCM_X_QUEUE_OK);
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	/* Even a corrupt early bitmap cannot bypass an armed application leg. */
	ticket->retire_acked_nodes_bitmap = ticket->involved_nodes_bitmap;
	ticket_before = *ticket;
	tag_before = *tag_slot;
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(ticket, &ticket_before, sizeof(*ticket)) == 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
}

UT_TEST(test_master_terminal_detach_rejects_hot_link_and_drain_corruption)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[2];
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXTicketRef active;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 2; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(745, i, (uint32)(7 + i), UINT64_C(55001) + i),
						   UINT64_C(5601) + i, 1);

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(501) + i),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&admission[0].ref.identity.tag,
														 admission[0].ref.identity.cluster_epoch,
														 &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(501));
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&active), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission[0].ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission[0].ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	ticket = &master_ticket_slots(header)[admission[0].ticket_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 2);
	UT_ASSERT_EQ(tag_slot->head_index, admission[1].ticket_slot.slot_index);
	tag_slot->head_index = admission[0].ticket_slot.slot_index;
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[0].ref),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 2);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	{
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(746, 0, 9, UINT64_C(57001)), UINT64_C(5801), 1);

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[0]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[0].ref, UINT64_C(601)),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(&admission[0].ref.identity.tag,
														 admission[0].ref.identity.cluster_epoch,
														 &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(601));
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&active), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission[0].ref, PCM_X_TERMINAL_LEG_DRAIN, 0);
	arm_and_ack_master_terminal_leg(&admission[0].ref, PCM_X_TERMINAL_LEG_RETIRE, 0);
	ticket = &master_ticket_slots(header)[admission[0].ticket_slot.slot_index];
	ticket->drained_nodes_bitmap = 0;
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[0].ref),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
}

UT_TEST(test_master_terminal_detach_preserves_same_node_successor)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission successor;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload first_request;
	PcmXEnqueuePayload successor_request;
	uint32 node_bit = UINT32_C(1);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	first_request = make_enqueue(make_wait_identity(747, 0, 9, UINT64_C(59001)), UINT64_C(6001), 1);
	successor_request
		= make_enqueue(make_wait_identity(747, 0, 10, UINT64_C(59002)), UINT64_C(6001), 2);
	bind_enqueue_peer(&first_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&first.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&first.ref, PCM_X_TERMINAL_LEG_DRAIN,
									first.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&first.ref, PCM_X_TERMINAL_LEG_RETIRE,
									first.ref.identity.node_id);

	/* The node bitmap identifies the current hot ticket, not every resident
	 * terminal from that node.  A successor may therefore reuse the node bit
	 * while the older ticket waits to detach in retirement order. */
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&successor_request, &successor), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	UT_ASSERT(slot_refs_equal(first.tag_slot, successor.tag_slot));
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 2);
	UT_ASSERT_EQ(tag_slot->head_index, successor.ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->tail_index, successor.ticket_slot.slot_index);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap), node_bit);

	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&first.ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 1);
	UT_ASSERT_EQ(tag_slot->head_index, successor.ticket_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->tail_index, successor.ticket_slot.slot_index);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap), node_bit);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);

	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&successor.ref), PCM_X_QUEUE_OK);
	drain_retire_and_detach_master(&successor);
	assert_master_queue_baseline(header);
}

UT_TEST(test_master_tag_survives_until_every_terminal_ticket_detaches)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission[3];
	PcmXMasterTagSlot *tag_slot;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	for (i = 0; i < 3; i++) {
		PcmXEnqueuePayload request
			= make_enqueue(make_wait_identity(712, i, (uint32)(18 + i), UINT64_C(14001) + i),
						   UINT64_C(1501) + i, UINT64_C(1));

		bind_enqueue_peer(&request);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(401) + i),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &master_tag_slots(header)[admission[0].tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 3);
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[i].ref), PCM_X_QUEUE_OK);
		arm_and_ack_master_terminal_leg(&admission[i].ref, PCM_X_TERMINAL_LEG_DRAIN,
										admission[i].ref.identity.node_id);
		arm_and_ack_master_terminal_leg(&admission[i].ref, PCM_X_TERMINAL_LEG_RETIRE,
										admission[i].ref.identity.node_id);
	}
	UT_ASSERT_EQ(tag_slot->head_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->tail_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->active_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->queued_node_bitmap), 0);
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission[i].ref), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(header->fully_retired_ticket_id, (uint64)i + 1);
		UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, (Size)(2 - i));
		UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TAG].used, i == 2 ? 0 : 1);
	}
	assert_master_queue_baseline(header);
}

UT_TEST(test_last_terminal_detach_waits_for_staged_admission)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXRuntimeSnapshot snapshot;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(715, 0, 24, UINT64_C(19001)), UINT64_C(2001),
						   UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE,
									admission.ref.identity.node_id);
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 1);
	pg_atomic_write_u32(&tag_slot->admission_gate, 1);
	pg_atomic_write_u32(&tag_slot->queued_node_bitmap, UINT32_C(1) << 1);
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 1);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RETIRE_CREDIT);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_master_outstanding_counter_overflow_and_rollback_are_exact)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission first;
	PcmXMasterAdmission second;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload first_request;
	PcmXEnqueuePayload second_request;
	PcmXSlotRef found;
	uint64 queue_state_before;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	first_request
		= make_enqueue(make_wait_identity(731, 0, 2, UINT64_C(36001)), UINT64_C(3701), UINT64_C(1));
	second_request
		= make_enqueue(make_wait_identity(731, 1, 3, UINT64_C(36002)), UINT64_C(3702), UINT64_C(1));
	bind_enqueue_peer(&first_request);
	bind_enqueue_peer(&second_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	queue_state_before = tag_slot->queue_state_sequence;
	tag_slot->outstanding_ticket_count = SIZE_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, SIZE_MAX);
	UT_ASSERT_EQ(tag_slot->queue_state_sequence, queue_state_before);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 2);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
											  &second_request.prehandle, &found),
				 PCM_X_DIRECTORY_OK);
	ticket = &master_ticket_slots(header)[found.slot_index];
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_RESERVED_NONVISIBLE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* Existing-tag allocator exhaustion is retryable and releases its gate. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_enqueue_peer(&first_request);
	bind_enqueue_peer(&second_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].free_head = PCM_X_INVALID_SLOT_INDEX;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].used
		= header->layout.pools[PCM_X_POOL_MASTER_TICKET].capacity;
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].high_water
		= header->layout.pools[PCM_X_POOL_MASTER_TICKET].capacity;
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 1);
	UT_ASSERT_EQ(tag_slot->next_admission_sequence, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 0);
	UT_ASSERT_EQ(header->next_ticket_id, 2);
	UT_ASSERT_EQ(header->peer_frontiers[1].next_expected_prehandle_sequence, 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	/* Existing-tag directory failure rolls its reserved ticket back exactly. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_enqueue_peer(&first_request);
	bind_enqueue_peer(&second_request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&first_request, &first), PCM_X_QUEUE_OK);
	tag_slot = &master_tag_slots(header)[first.tag_slot.slot_index];
	{
		PcmXDirectoryEntry *entries;
		Size capacity;
		Size i;
		uint64 second_hash;

		entries = directory_entries(header, PCM_X_DIR_MASTER_TICKET_PREHANDLE, &capacity);
		UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_MASTER_TICKET_PREHANDLE,
												   &second_request.prehandle, &second_hash));
		for (i = 0; i < capacity; i++) {
			if (entries[i].state == PCM_X_DIRECTORY_OCCUPIED)
				continue;
			entries[i].state = PCM_X_DIRECTORY_OCCUPIED;
			entries[i].key_hash = second_hash ^ UINT64_C(1);
			entries[i].slot_index = first.ticket_slot.slot_index;
			entries[i].slot_generation = first.ticket_slot.slot_generation;
		}
	}
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&second_request, &second),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 1);
	UT_ASSERT_EQ(tag_slot->next_admission_sequence, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 1);
	UT_ASSERT_EQ(header->next_ticket_id, 2);
	UT_ASSERT_EQ(header->peer_frontiers[1].next_expected_prehandle_sequence, 1);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_master_detach_second_caller_is_stale_not_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXMasterTicketSlot *ticket;
	PcmXEnqueuePayload request;
	PcmXSlotRef found;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request
		= make_enqueue(make_wait_identity(732, 0, 2, UINT64_C(38001)), UINT64_C(3901), UINT64_C(1));
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE,
									admission.ref.identity.node_id);
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	ticket = &master_ticket_slots(header)[admission.ticket_slot.slot_index];
	detach_interlock_tag = tag_slot;
	detach_interlock_ticket = ticket;
	detach_interlock_armed = true;
	UT_ASSERT_EQ(cluster_pcm_x_master_detach_terminal_exact(&admission.ref), PCM_X_QUEUE_STALE);
	UT_ASSERT(!detach_interlock_armed);
	UT_ASSERT_EQ(test_slot_state(&ticket->slot), PCM_XT_DETACHING);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(tag_slot->outstanding_ticket_count, 0);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TICKET_HANDLE, &admission.ref.handle, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_composite_join_publishes_one_leader_and_ordered_followers)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[3];
	PcmXLocalHandle duplicate;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *members;
	PcmXSlotRef found;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, 0, UINT64_C(177));
	for (i = 0; i < 3; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(706, 0, (uint32)(17 + i), UINT64_C(7001) + i);

		identity.cluster_epoch = 0;
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &handles[i]),
					 PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(handles[i].local_sequence, (uint64)i + 1);
		UT_ASSERT_EQ(handles[i].local_round, 1);
		UT_ASSERT_EQ(handles[i].role,
					 i == 0 ? PCM_X_LOCAL_ROLE_NODE_LEADER : PCM_X_LOCAL_ROLE_FOLLOWER);
	}
	UT_ASSERT(slot_refs_equal(handles[0].tag_slot, handles[1].tag_slot));
	UT_ASSERT(slot_refs_equal(handles[0].tag_slot, handles[2].tag_slot));
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_LIVE);
	UT_ASSERT_EQ(tag_slot->head_index, handles[0].membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->tail_index, handles[2].membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->leader_index, handles[0].membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->membership_count, 3);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 0);
	UT_ASSERT_EQ(test_slot_state(&members[handles[0].membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
	UT_ASSERT_EQ(test_slot_state(&members[handles[1].membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);
	UT_ASSERT_EQ(members[handles[0].membership_slot.slot_index].next_index,
				 handles[1].membership_slot.slot_index);
	UT_ASSERT_EQ(members[handles[1].membership_slot.slot_index].next_index,
				 handles[2].membership_slot.slot_index);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 3);

	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &handles[0].identity.tag, &found),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, handles[0].tag_slot));
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ(
			cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &handles[i].identity, &found),
			PCM_X_DIRECTORY_OK);
		UT_ASSERT(slot_refs_equal(found, handles[i].membership_slot));
	}

	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&handles[1].identity, 1, UINT64_C(177), &duplicate),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(slot_refs_equal(duplicate.membership_slot, handles[1].membership_slot));
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 3);
}

UT_TEST(test_local_follower_wfg_publish_is_nonwaitable_then_exact)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalFollowerWfgSnapshot snapshot;
	PcmXLocalFollowerWfgSnapshot stale;
	PcmXLocalMembershipSlot *members;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, 0, UINT64_C(177));
	identity = make_wait_identity(806, 0, 17, UINT64_C(80601));
	identity.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(806, 0, 18, UINT64_C(80602));
	identity.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower),
				 PCM_X_QUEUE_OK);
	members = membership_slots(ClusterPcmXConvertShmem);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);

	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&snapshot.waiter, &follower, sizeof(follower)) == 0);
	UT_ASSERT(memcmp(&snapshot.blocker, &leader.identity, sizeof(leader.identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&snapshot, UINT64_C(901)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_WAITABLE_FOLLOWER);
	UT_ASSERT_EQ(members[follower.membership_slot.slot_index].graph_generation, UINT64_C(901));
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&snapshot, UINT64_C(901)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(902)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(901)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);
	UT_ASSERT_EQ(members[follower.membership_slot.slot_index].graph_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(901)),
				 PCM_X_QUEUE_DUPLICATE);

	stale = snapshot;
	stale.leader_slot_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&stale, UINT64_C(902)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(members[follower.membership_slot.slot_index].graph_generation, 0);
}

UT_TEST(test_local_writer_claim_runs_closed_cohort_and_blocks_next_round)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle late;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalWriterClaim claim;
	PcmXLocalWriterClaim released;
	PcmXLocalWriterClaim stale;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity successor;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(807, 0, 19, UINT64_C(80701));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(807, 0, 20, UINT64_C(80702));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(903)),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->active_writer_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->active_writer_slot_generation, leader.membership_slot.slot_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &stale), PCM_X_QUEUE_BUSY);

	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(807, 0, 21, UINT64_C(80703));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &late),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late.local_round, cutoff.next_round);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&late, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(909)),
				 PCM_X_QUEUE_OK);
	released = claim;
	memset(&successor, 0x7f, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor, &follower.identity, sizeof(successor)) == 0);
	UT_ASSERT_EQ(tag_slot->active_writer_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(903)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &claim), PCM_X_QUEUE_OK);
	memset(&successor, 0x7f, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor, &(PcmXWaitIdentity){ 0 }, sizeof(successor)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&late, &claim), PCM_X_QUEUE_BARRIER_CLOSED);
	stale = released;
	stale.claim_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&stale), PCM_X_QUEUE_STALE);
}

UT_TEST(test_local_writer_claim_completion_is_fifo_and_one_shot)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle follower1;
	PcmXLocalHandle follower2;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalWriterClaim aborted;
	PcmXLocalWriterClaim busy;
	PcmXLocalWriterClaim claim;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity successor;
	uint32 state_flags;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(809, 0, 24, UINT64_C(80901));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(809, 0, 25, UINT64_C(80902));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower1),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(809, 0, 26, UINT64_C(80903));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower2),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower1, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(904)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower2, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(905)),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower2, &busy), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	aborted = claim;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_abort_exact(&claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT(claim.claim_generation > aborted.claim_generation);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_abort_exact(&aborted), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower1, &busy), PCM_X_QUEUE_BUSY);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	memset(&successor, 0x7f, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT(memcmp(&successor, &(PcmXWaitIdentity){ 0 }, sizeof(successor)) == 0);
	UT_ASSERT_EQ(tag_slot->active_writer_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(
		test_slot_flags(
			&membership_slots(ClusterPcmXConvertShmem)[leader.membership_slot.slot_index].slot)
			& PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE,
		0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	pg_atomic_write_u32(&tag_slot->slot.state_flags, state_flags);
	memset(&successor, 0, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor, &follower1.identity, sizeof(successor)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower2, UINT64_C(905)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower2, &busy), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower1, UINT64_C(904)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower1, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower2, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&wfg.blocker, &follower1.identity, sizeof(follower1.identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(906)),
				 PCM_X_QUEUE_OK);
	memset(&successor, 0, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor, &follower2.identity, sizeof(successor)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower1, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower1, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower2, UINT64_C(906)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower1, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower1), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower2, &claim), PCM_X_QUEUE_OK);
	memset(&successor, 0x7f, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor, &(PcmXWaitIdentity){ 0 }, sizeof(successor)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower2, &claim), PCM_X_QUEUE_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower2, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower2), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
}

UT_TEST(test_writer_and_holder_owner_exit_retry_preserves_exact_evidence)
{
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHandle leader;
	PcmXLocalWriterClaim claim;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot tag_before;
	PcmXLocalMembershipSlot holder_before;
	PcmXLocalMembershipSlot writer_before;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity successor;
	LWLock content_lock;
	uint32 state_flags;
	Size holder_used_before;
	const uint64 master_session = UINT64_C(8177);

	init_active_pcm_x(UINT64_C(177));
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(811, 0, 27, UINT64_C(81101), 1);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	identity = make_wait_identity(811, 0, 28, UINT64_C(81102));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	holder_used_before = ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used;
	memset(&content_lock, 0, sizeof(content_lock));
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	memset(&successor, 0x7f, sizeof(successor));
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&holder, &content_lock),
				 PCM_X_QUEUE_GATE_RETRY);
	UT_ASSERT_EQ(tag_slot->active_writer_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, holder.holder_slot.slot_index);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used,
				 holder_used_before);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	pg_atomic_write_u32(&tag_slot->slot.state_flags, state_flags);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&holder, &content_lock),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->active_writer_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used,
				 holder_used_before - 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);

	/* A different runtime may never consume either exact ledger.  Both APIs
	 * preserve byte-identical shared evidence for deferred recovery. */
	init_active_pcm_x(UINT64_C(178));
	bind_local_master(1, UINT64_C(9), master_session + 1);
	holder_key = make_local_holder_key(812, 0, 27, UINT64_C(81201), 1);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	identity = make_wait_identity(812, 0, 28, UINT64_C(81202));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 1, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	tag_before = *tag_slot;
	holder_before = membership_slots(ClusterPcmXConvertShmem)[holder.holder_slot.slot_index];
	writer_before = membership_slots(ClusterPcmXConvertShmem)[leader.membership_slot.slot_index];
	cluster_pcm_x_runtime_fail_closed();
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_collect_exact(&claim, &successor),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_exceptional_detach_exact(&holder, &content_lock),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(&tag_before, tag_slot, sizeof(tag_before)) == 0);
	UT_ASSERT(memcmp(&holder_before,
					 &membership_slots(ClusterPcmXConvertShmem)[holder.holder_slot.slot_index],
					 sizeof(holder_before))
			  == 0);
	UT_ASSERT(memcmp(&writer_before,
					 &membership_slots(ClusterPcmXConvertShmem)[leader.membership_slot.slot_index],
					 sizeof(writer_before))
			  == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_wfg_rejects_completed_blocker_semantic_aba)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalFollowerWfgSnapshot snapshot;
	PcmXLocalWriterClaim claim;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(810, 0, 27, UINT64_C(81001));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(810, 0, 28, UINT64_C(81002));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&snapshot, UINT64_C(906)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &snapshot),
				 PCM_X_QUEUE_BUSY);
}

UT_TEST(test_local_cancel_never_unlinks_an_active_writer)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle follower2;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalWriterClaim claim;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(811, 0, 4, UINT64_C(81101));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(811, 0, 5, UINT64_C(81102));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(907)),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(907)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);

	/* ABORT clears active_writer without setting WRITER_COMPLETE.  A later
	 * follower can still hold an exact edge to that former active writer, so
	 * cancellation must retain the slot until that edge is removed. */
	init_active_pcm_x(UINT64_C(78));
	bind_local_master(1, UINT64_C(9), UINT64_C(178));
	identity = make_wait_identity(812, 0, 6, UINT64_C(81201));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(178), &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(812, 0, 7, UINT64_C(81202));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(178), &follower),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(812, 0, 8, UINT64_C(81203));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(178), &follower2),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(917)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower2, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(918)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(917)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower2, UINT64_C(918)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower2, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&wfg.blocker, &follower.identity, sizeof(follower.identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(919)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_abort_exact(&claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower2, UINT64_C(919)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower2, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower2), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_local_closed_round_never_promotes_a_late_joiner_early)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle late1;
	PcmXLocalHandle late2;
	PcmXLocalCutoff cutoff;
	PcmXLocalMembershipSlot *members;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(808, 0, 21, UINT64_C(80801));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(808, 0, 22, UINT64_C(80802));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &late1),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late1.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);

	identity = make_wait_identity(808, 0, 23, UINT64_C(80803));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &late2),
				 PCM_X_QUEUE_OK);
	members = membership_slots(ClusterPcmXConvertShmem);
	UT_ASSERT_EQ(late2.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(test_slot_state(&members[late2.membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);
	UT_ASSERT_EQ(local_tag_slots(ClusterPcmXConvertShmem)[late2.tag_slot.slot_index].leader_index,
				 PCM_X_INVALID_SLOT_INDEX);
}

UT_TEST(test_local_lookup_is_read_only_and_identity_exact)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle joined;
	PcmXLocalHandle found;
	PcmXLocalHandle cleared;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity wrong;
	Size tag_used;
	Size wait_used;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(177));
	identity = make_wait_identity(706, 0, 27, UINT64_C(7027));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &joined),
				 PCM_X_QUEUE_OK);
	tag_used = header->allocator[PCM_X_ALLOC_LOCAL_TAG].used;
	wait_used = header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used;

	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&identity, &found), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&found, &joined, sizeof(found)) == 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, tag_used);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, wait_used);

	wrong = identity;
	wrong.request_id++;
	memset(&found, 0xa5, sizeof(found));
	memset(&cleared, 0, sizeof(cleared));
	cleared.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	cleared.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&wrong, &found), PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT(memcmp(&found, &cleared, sizeof(found)) == 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, tag_used);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, wait_used);
}

UT_TEST(test_local_progress_is_exact_and_exposes_remote_wait_ledger)
{
	PcmXLocalHandle leader;
	PcmXLocalHandle stale;
	PcmXLocalProgress progress;
	PcmXLocalProgress cleared;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload ack;
	PcmXPhasePayload confirm;
	PcmXLocalReliableToken token;

	init_active_pcm_x(UINT64_C(77));
	identity = make_wait_identity(706, 0, 28, UINT64_C(7028));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(177));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(177), &leader),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&progress.identity, &identity, sizeof(identity)) == 0);
	UT_ASSERT_EQ(progress.local_sequence, leader.local_sequence);
	UT_ASSERT_EQ(progress.local_round, leader.local_round);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_NODE_LEADER);
	UT_ASSERT_EQ(progress.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, 0);

	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&ack, 0, sizeof(ack));
	ack.ref.identity = identity;
	ack.ref.handle.ticket_id = UINT64_C(9008);
	ack.ref.handle.queue_generation = UINT64_C(8);
	ack.prehandle = enqueue.prehandle;
	ack.result = PCM_X_QUEUE_OK;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &ack, 1, UINT64_C(177)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, UINT64_C(177)),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&progress.ref, &ack.ref));
	UT_ASSERT_EQ(progress.local_sequence, leader.local_sequence);
	UT_ASSERT_EQ(progress.local_round, leader.local_round);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_REMOTE_WAIT);
	UT_ASSERT_EQ(progress.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT_EQ(progress.reliable_state_sequence, 2);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK);
	UT_ASSERT_EQ(progress.phase, PCM_X_LOCAL_RELIABLE_PHASE_NONE);

	stale = leader;
	stale.local_sequence++;
	memset(&progress, 0xa5, sizeof(progress));
	memset(&cleared, 0, sizeof(cleared));
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&stale, &progress), PCM_X_QUEUE_STALE);
	UT_ASSERT(memcmp(&progress, &cleared, sizeof(progress)) == 0);
}

UT_TEST(test_local_transfer_prepare_commit_and_final_ack_are_exact)
{
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalProgress progress;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload admit_confirm;
	PcmXLocalCutoff cutoff;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXTicketRef probe_ref;
	PcmXGrantPayload prepare;
	PcmXGrantPayload bad_prepare;
	PcmXGrantPayload holder_replay;
	PcmXRevokePayload revoke;
	PcmXInstallReadyPayload install_ready;
	PcmXPhasePayload commit;
	PcmXFinalAckPayload final_ack;
	PcmXPhasePayload final_commit;
	PcmXPhasePayload final_confirm;
	PcmXPhasePayload duplicate_confirm;
	PcmXDrainPollPayload poll;
	PcmXRetirePayload retire;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken retry_token;
	PcmXLocalWriterClaim writer_claim;
	PcmXLocalWriterClaim stale_writer_claim;
	PcmXLocalHolderKey writer_holder_key;
	PcmXLocalHolderKey stale_holder_key;
	PcmXLocalHolderHandle writer_holder;
	PcmXLocalHolderHandle duplicate_holder;
	uint64 committed_own_generation;
	uint64 holder_set_generation;
	Size holder_used;
	const uint64 master_session = UINT64_C(178);

	init_active_pcm_x(UINT64_C(77));
	identity = make_wait_identity(707, 0, 27, UINT64_C(7029));
	bind_local_master(1, identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	identity = make_wait_identity(707, 0, 28, UINT64_C(7030));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&follower, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(908)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &writer_claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	writer_holder_key = make_local_holder_key(707, 0, 27, UINT64_C(7031), 3);
	committed_own_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &writer_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(committed_own_generation, 0);
	UT_ASSERT_EQ(writer_holder.tag_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = leader.identity;
	admit_ack.ref.handle.ticket_id = UINT64_C(9009);
	admit_ack.ref.handle.queue_generation = UINT64_C(9);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &admit_ack, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &admit_confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &admit_confirm, 1, master_session),
		PCM_X_QUEUE_OK);

	memset(&prepare, 0, sizeof(prepare));
	prepare.ref = admit_ack.ref;
	prepare.ref.grant_generation = UINT64_C(99);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, UINT64_C(199), &prepare.image.image_id));
	prepare.image.source_own_generation = UINT64_C(41);
	prepare.image.page_scn = UINT64_C(42);
	prepare.image.page_lsn = UINT64_C(43);
	prepare.image.source_node = 0;
	prepare.image.page_checksum = UINT32_C(44);
	probe_ref = prepare.ref;
	probe_ref.grant_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, NULL, 0, NULL, 0,
																&blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	memset(&revoke, 0, sizeof(revoke));
	revoke.ref = prepare.ref;
	revoke.image_id = prepare.image.image_id;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_image_ready_arm_exact(&prepare, 1, master_session,
																  &holder_replay),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&holder_replay, &prepare, sizeof(prepare)) == 0);
	bad_prepare = prepare;
	bad_prepare.image.image_id = UINT64CONST(0xe0000000000000c7);
	UT_ASSERT_EQ(cluster_pcm_x_local_prepare_grant_exact(&leader, &bad_prepare, 1, master_session),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_local_prepare_grant_exact(&leader, &prepare, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_prepare_grant_exact(&leader, &prepare, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_REMOTE_WAIT);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT);
	UT_ASSERT(ticket_refs_equal(&progress.ref, &prepare.ref));
	UT_ASSERT(memcmp(&progress.image, &prepare.image, sizeof(progress.image)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_local_install_ready_arm_exact(&leader, &prepare.ref, &prepare.image,
															 &install_ready, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(install_ready.image_id, prepare.image.image_id);
	UT_ASSERT_EQ(install_ready.phase, PGRAC_IC_MSG_PCM_X_INSTALL_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_CONTENT_ACTIVE);
	UT_ASSERT_EQ(progress.pending_opcode, PGRAC_IC_MSG_PCM_X_INSTALL_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &retry_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.pending_opcode, PGRAC_IC_MSG_PCM_X_INSTALL_READY);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_reliable_retry_exact(&leader, &token, UINT64_C(1001), &retry_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.retry_count, 1);
	UT_ASSERT_EQ(retry_token.retry_deadline_ms, UINT64_C(1001));
	memset(&commit, 0, sizeof(commit));
	commit.ref = prepare.ref;
	commit.phase = PGRAC_IC_MSG_PCM_X_COMMIT_X;
	UT_ASSERT_EQ(cluster_pcm_x_local_commit_x_exact(&leader, &commit, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_commit_x_exact(&leader, &commit, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);

	UT_ASSERT_EQ(cluster_pcm_x_local_final_ack_arm_exact(&leader, 1, &final_ack, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(final_ack.image_id, prepare.image.image_id);
	UT_ASSERT_EQ(final_ack.committed_own_generation, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &retry_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.pending_opcode, PGRAC_IC_MSG_PCM_X_FINAL_ACK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_reliable_retry_exact(&leader, &token, UINT64_C(1002), &retry_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.retry_count, 1);
	UT_ASSERT_EQ(retry_token.retry_deadline_ms, UINT64_C(1002));
	memset(&final_commit, 0, sizeof(final_commit));
	final_commit.ref = prepare.ref;
	final_commit.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	UT_ASSERT_EQ(cluster_pcm_x_local_final_commit_ack_exact(&leader, &final_commit, 1,
															master_session, &final_confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(final_confirm.phase, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_GRANTED);
	UT_ASSERT_EQ(progress.pending_opcode, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM);
	UT_ASSERT_EQ(progress.phase, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &retry_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.pending_opcode, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_reliable_retry_exact(&leader, &token, UINT64_C(1003), &retry_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.retry_count, 1);
	UT_ASSERT_EQ(retry_token.retry_deadline_ms, UINT64_C(1003));
	UT_ASSERT_EQ(local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index]
					 .committed_own_generation,
				 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_final_commit_ack_exact(
					 &leader, &final_commit, 1, master_session, &duplicate_confirm, &token),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&duplicate_confirm, &final_confirm, sizeof(final_confirm)) == 0);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_TERMINAL_MASK) == 0);
	UT_ASSERT_EQ(tag_slot->leader_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->active_writer_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &prepare.ref));
	UT_ASSERT_EQ(tag_slot->master_node, 1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, master_session);
	UT_ASSERT_EQ(tag_slot->cluster_epoch, identity.cluster_epoch);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->peer_frontiers[1].cluster_epoch, identity.cluster_epoch);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->peer_frontiers[1].sender_session_incarnation,
				 master_session);
	UT_ASSERT_EQ(membership_slots(ClusterPcmXConvertShmem)[leader.membership_slot.slot_index]
					 .handle.ticket_id,
				 prepare.ref.handle.ticket_id);
	UT_ASSERT_EQ(
		test_slot_state(
			&membership_slots(ClusterPcmXConvertShmem)[leader.membership_slot.slot_index].slot),
		PCM_XL_GRANTED);

	writer_holder_key.identity.base_own_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&writer_holder_key, &writer_holder),
				 PCM_X_QUEUE_BARRIER_CLOSED);
	holder_used = ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used;
	holder_set_generation = tag_slot->holder_set_generation;
	stale_writer_claim = writer_claim;
	stale_writer_claim.claim_generation++;
	committed_own_generation = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_writer_holder_register_exact(&writer_holder_key, &stale_writer_claim,
														 &writer_holder, &committed_own_generation),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(committed_own_generation, 0);
	stale_writer_claim = writer_claim;
	stale_writer_claim.local_round++;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_writer_holder_register_exact(&writer_holder_key, &stale_writer_claim,
														 &writer_holder, &committed_own_generation),
		PCM_X_QUEUE_STALE);
	stale_holder_key = writer_holder_key;
	stale_holder_key.identity.base_own_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &stale_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_STALE);
	stale_holder_key.identity.base_own_generation = 2;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &stale_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_STALE);
	stale_holder_key = writer_holder_key;
	stale_holder_key.identity.procno++;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &stale_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_STALE);
	stale_holder_key = writer_holder_key;
	stale_holder_key.identity.xid++;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &stale_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, holder_used);
	UT_ASSERT_EQ(tag_slot->holder_set_generation, holder_set_generation);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &writer_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(committed_own_generation, 1);
	stale_writer_claim = writer_claim;
	stale_writer_claim.claim_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_writer_holder_register_exact(
			&writer_holder_key, &stale_writer_claim, &duplicate_holder, &committed_own_generation),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(&writer_holder_key, &writer_claim,
																  &duplicate_holder,
																  &committed_own_generation),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&duplicate_holder, &writer_holder, sizeof(writer_holder)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&writer_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&writer_holder), PCM_X_QUEUE_OK);

	memset(&poll, 0, sizeof(poll));
	poll.ref = prepare.ref;
	poll.drain_generation = UINT64_C(43);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(tag_slot->leader_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->active_writer_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&writer_claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&follower, UINT64_C(908)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&follower, &writer_claim), PCM_X_QUEUE_OK);
	writer_holder_key = make_local_holder_key(707, 0, 28, UINT64_C(7032), 4);
	writer_holder_key.identity.base_own_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_holder_register_exact(
					 &writer_holder_key, &writer_claim, &writer_holder, &committed_own_generation),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(committed_own_generation, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_activate_exact(&writer_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&writer_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&writer_claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_GRANTED);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_DRAIN_POLL);

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = prepare.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&leader, &progress), PCM_X_QUEUE_NOT_FOUND);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);
}

UT_TEST(test_local_tag_only_holder_transfer_persists_until_exact_drain)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey new_holder_key;
	PcmXLocalHolderHandle new_holder;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXBlockerSetHeaderPayload blocker_header;
	PcmXEnqueuePayload enqueue;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalCutoff cutoff;
	PcmXLocalCutoff promoted_cutoff;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalReliableToken token;
	PcmXLocalWriterClaim writer_claim;
	PcmXTicketRef probe_ref;
	PcmXLocalHolderProgress progress;
	PcmXRevokePayload revoke;
	PcmXRevokePayload bad_revoke;
	PcmXGrantPayload ready;
	PcmXGrantPayload replay;
	PcmXDrainPollPayload poll;
	PcmXRetirePayload retire;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity late_identity;
	PcmXWaitIdentity wake_items[1];
	PcmXLocalWakeBatch wake_batch;
	const uint64 master_session = UINT64_C(179);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(708, 2, 28, UINT64_C(7030));
	bind_local_master(1, identity.cluster_epoch, master_session);
	memset(&revoke, 0, sizeof(revoke));
	revoke.ref.identity = identity;
	revoke.ref.handle.ticket_id = UINT64_C(9010);
	revoke.ref.handle.queue_generation = UINT64_C(10);
	revoke.ref.grant_generation = UINT64_C(100);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, UINT64_C(200), &revoke.image_id));
	probe_ref = revoke.ref;
	probe_ref.grant_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&identity.tag, NULL, 0, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.tag_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, NULL, 0, NULL, 0,
																&blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_copy_exact(&blocker_snapshot, &blocker_header,
																 NULL, 0),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(blocker_header.nblockers, 0);
	UT_ASSERT_EQ(blocker_header.set_crc32c, blocker_set_crc32c(NULL, 0));
	late_identity = make_wait_identity(708, 0, 6, UINT64_C(7031));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&late_identity, 1, master_session, &late),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(late.local_round, 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(
					 &identity.tag, identity.cluster_epoch, 1, master_session, &cutoff),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cutoff.cutoff_sequence, 0);
	UT_ASSERT_EQ(cutoff.closed_round, 1);
	UT_ASSERT_EQ(cutoff.next_round, 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&late, &wfg),
				 PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	bad_revoke = revoke;
	bad_revoke.image_id = UINT64CONST(0xf0000000000000c8);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&bad_revoke, 1, master_session),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	/* The exact blocker ACK tombstone is durable evidence, but it cannot
	 * reopen the writer round before REVOKE/IMAGE_READY/DRAIN completes. */
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_progress_exact(&identity.tag, &progress),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&progress.ref, &revoke.ref));
	UT_ASSERT_EQ(progress.image.image_id, revoke.image_id);
	UT_ASSERT_EQ(progress.master_session_incarnation, master_session);
	UT_ASSERT_EQ(progress.master_node, 1);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_REVOKE);

	memset(&ready, 0, sizeof(ready));
	ready.ref = revoke.ref;
	ready.image.image_id = revoke.image_id;
	ready.image.source_own_generation = 0;
	ready.image.page_scn = UINT64_C(52);
	ready.image.page_lsn = UINT64_C(53);
	ready.image.source_node = 0;
	ready.image.page_checksum = UINT32_C(54);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_image_ready_arm_exact(&ready, 1, master_session, &replay),
		PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&ready, &replay, sizeof(ready)) == 0);
	memset(&replay, 0, sizeof(replay));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_holder_image_ready_arm_exact(&ready, 1, master_session, &replay),
		PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&ready, &replay, sizeof(ready)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_progress_exact(&identity.tag, &progress),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.pending_opcode, PGRAC_IC_MSG_PCM_X_IMAGE_READY);
	UT_ASSERT_EQ(progress.image.source_own_generation, 0);
	UT_ASSERT_EQ(progress.flags, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);

	memset(&poll, 0, sizeof(poll));
	poll.ref = revoke.ref;
	poll.drain_generation = UINT64_C(42);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session + 1),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&late.identity, &next_leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(next_leader.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(next_leader.membership_slot, late.membership_slot));
	/* Promotion is visible for exact lookup, but holder cleanup is not complete
	 * until RETIRE consumes the terminal image lane.  No timed wake or unrelated
	 * latch may let this successor claim, refreeze, or mint ENQUEUE early. */
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&next_leader, &writer_claim),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&next_leader, &promoted_cutoff),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&next_leader, &enqueue, &token),
				 PCM_X_QUEUE_BUSY);
	new_holder_key = make_local_holder_key(708, 0, 3, UINT64_C(7032), 3);
	UT_ASSERT_EQ(register_active_local_holder(&new_holder_key, &new_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_progress_exact(&identity.tag, &progress),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, PGRAC_IC_MSG_PCM_X_DRAIN_POLL);
	UT_ASSERT_EQ(progress.image.source_own_generation, 0);
	UT_ASSERT_EQ(progress.flags, PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = revoke.ref.handle.ticket_id;
	retire.sender_node = 0;
	memset(wake_items, 0, sizeof(wake_items));
	wake_batch.items = wake_items;
	wake_batch.capacity = lengthof(wake_items);
	wake_batch.count = 0;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, master_session, &wake_batch),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(wake_batch.count, 1);
	UT_ASSERT(memcmp(&wake_items[0], &next_leader.identity, sizeof(wake_items[0])) == 0);
	/* A visible wake batch proves the full watermark and global admission gate
	 * committed before the caller can make the successor runnable. */
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 0);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retired_ticket_id,
				 retire.retire_through_ticket_id);
	wake_batch.count = 1;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, master_session, &wake_batch),
		PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(wake_batch.count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_progress_exact(&identity.tag, &progress),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&next_leader, &writer_claim),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_abort_exact(&writer_claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&new_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&next_leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&next_leader), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_non_source_blocker_participant_drains_and_retires_exactly)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_keys[2];
	PcmXLocalHolderKey new_holder_key;
	PcmXLocalHolderHandle holders[2];
	PcmXLocalHolderHandle new_holder;
	PcmXLocalHolderHandle holder_copies[2];
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef probe_ref;
	PcmXDrainPollPayload poll;
	PcmXDrainPollPayload stale_poll;
	PcmXRetirePayload retire;
	PcmXSlotRef found;
	const uint64 master_session = UINT64_C(1801);
	int i;

	/* Model the local evidence left on a non-source participant: two independent
	 * S pins are captured by PROBE, both are later released, and no holder_ref
	 * image-source lane is installed.  DRAIN must therefore close from the exact
	 * type-48 blocker ACK tombstone alone. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	for (i = 0; i < 2; i++) {
		holder_keys[i]
			= make_local_holder_key(7091, 0, (uint32)(21 + i), UINT64_C(71001) + i, 4 + i);
		UT_ASSERT_EQ(register_active_local_holder(&holder_keys[i], &holders[i]), PCM_X_QUEUE_OK);
	}
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7091, 3, 23, UINT64_C(71003));
	probe_ref.handle.ticket_id = UINT64_C(91001);
	probe_ref.handle.queue_generation = UINT64_C(11);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(
					 &probe_ref, 1, master_session, holder_copies, 2, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, holder_copies, 2,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(&probe_ref.identity.tag,
																   probe_ref.identity.cluster_epoch,
																   1, master_session, &cutoff),
				 PCM_X_QUEUE_OK);
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(release_active_local_holder(&holders[i]), PCM_X_QUEUE_OK);

	tag_slot = &local_tag_slots(header)[holder_snapshot.tag_slot.slot_index];
	UT_ASSERT(ticket_refs_equal(&tag_slot->holder_ref, &(PcmXTicketRef){ 0 }));
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &probe_ref));
	UT_ASSERT_EQ(tag_slot->blocker_snapshot_reliable.last_response_opcode,
				 PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK);

	memset(&poll, 0, sizeof(poll));
	poll.ref = probe_ref;
	poll.ref.grant_generation = UINT64_C(101);
	poll.drain_generation = UINT64_C(42);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(ticket_refs_equal(&tag_slot->holder_ref, &(PcmXTicketRef){ 0 }));
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &poll.ref));
	UT_ASSERT_EQ(tag_slot->blocker_set_generation, 0);
	UT_ASSERT_EQ(tag_slot->holder_terminal_drain_generation, poll.drain_generation);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK,
				 PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);

	stale_poll = poll;
	stale_poll.ref.grant_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&stale_poll, 1, master_session),
				 PCM_X_QUEUE_STALE);
	stale_poll = poll;
	stale_poll.drain_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&stale_poll, 1, master_session),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session + 1),
				 PCM_X_QUEUE_STALE);

	new_holder_key = make_local_holder_key(7091, 0, 24, UINT64_C(71004), 6);
	UT_ASSERT_EQ(register_active_local_holder(&new_holder_key, &new_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = probe_ref.identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(release_active_local_holder(&new_holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_cancelled_non_source_participant_gen0_drains_and_retires_exactly)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalCutoff cutoff;
	PcmXTicketRef probe_ref;
	PcmXDrainPollPayload poll;
	PcmXDrainPollPayload stale_poll;
	PcmXRetirePayload retire;
	PcmXSlotRef found;
	const uint64 master_session = UINT64_C(1807);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(7107, 0, 21, UINT64_C(71101), 4);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7107, 3, 23, UINT64_C(71103));
	probe_ref.handle.ticket_id = UINT64_C(91101);
	probe_ref.handle.queue_generation = UINT64_C(12);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 &holder_copy, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(&probe_ref.identity.tag,
																   probe_ref.identity.cluster_epoch,
																   1, master_session, &cutoff),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);

	memset(&poll, 0, sizeof(poll));
	poll.ref = probe_ref;
	poll.drain_generation = UINT64_C(43);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	stale_poll = poll;
	stale_poll.ref.grant_generation = UINT64_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&stale_poll, 1, master_session),
				 PCM_X_QUEUE_STALE);
	stale_poll = poll;
	stale_poll.drain_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&stale_poll, 1, master_session),
				 PCM_X_QUEUE_STALE);

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = probe_ref.identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_cancelled_participant_gen0_drain_requires_frozen_round)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef blocker_ref;
	PcmXDrainPollPayload poll;
	PcmXReliableLegState reliable_before;
	uint64 set_generation;
	uint32 state_flags;
	const uint64 master_session = UINT64_C(1812);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(7112, 0, 21, UINT64_C(71301), 4);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	memset(&blocker_ref, 0, sizeof(blocker_ref));
	blocker_ref.identity = make_wait_identity(7112, 3, 23, UINT64_C(71303));
	blocker_ref.handle.ticket_id = UINT64_C(91112);
	blocker_ref.handle.queue_generation = UINT64_C(14);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&blocker_ref, 1, master_session,
																 &holder_copy, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&blocker_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(
					 &blocker_ref, blocker_snapshot.set_generation, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder_snapshot.tag_slot.slot_index];
	set_generation = tag_slot->blocker_set_generation;
	reliable_before = tag_slot->blocker_snapshot_reliable;
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags
							& ~(PCM_X_LOCAL_TAG_F_REVOKE_BARRIER << PCM_X_SLOT_FLAGS_SHIFT));
	memset(&poll, 0, sizeof(poll));
	poll.ref = blocker_ref;
	poll.drain_generation = UINT64_C(45);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &blocker_ref));
	UT_ASSERT_EQ(tag_slot->blocker_set_generation, set_generation);
	UT_ASSERT(
		memcmp(&tag_slot->blocker_snapshot_reliable, &reliable_before, sizeof(reliable_before))
		== 0);
	UT_ASSERT_EQ(tag_slot->holder_terminal_drain_generation, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_holder_drain_validates_frozen_round_before_terminal_publish)
{
	PcmXLocalHandle leader;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot tag_after;
	PcmXLocalTagSlot tag_before;
	PcmXDrainPollPayload poll;
	PcmXWaitIdentity identity;
	uint32 before_state_flags;
	uint32 state_flags;
	const uint64 master_session = UINT64_C(1813);

	/* A holder DRAIN can arrive while an unrelated local writer cohort is still
	 * closed.  Its first terminal publication must validate the common revoke
	 * barrier even though closed_round_member_count is nonzero. */
	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), master_session);
	identity = make_wait_identity(7113, 0, 21, UINT64_C(71401));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 1);
	seed_live_local_holder_transfer(tag_slot, 22, UINT64_C(71402), UINT64_C(91402));
	memset(&poll, 0, sizeof(poll));
	poll.ref = tag_slot->holder_ref;
	poll.drain_generation = UINT64_C(46);
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags
							& ~(PCM_X_LOCAL_TAG_F_REVOKE_BARRIER << PCM_X_SLOT_FLAGS_SHIFT));
	tag_before = *tag_slot;
	before_state_flags = pg_atomic_read_u32(&tag_before.slot.state_flags);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->slot.state_flags),
				 before_state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	tag_after = *tag_slot;
	pg_atomic_write_u32(&tag_after.slot.state_flags, before_state_flags);
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* A torn first-arrival drain generation is equally structural and must not
	 * be overwritten merely because a writer remains in the frozen cohort. */
	init_active_pcm_x(UINT64_C(78));
	bind_local_master(1, UINT64_C(9), master_session + 1);
	identity = make_wait_identity(7114, 0, 21, UINT64_C(71403));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 1, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	seed_live_local_holder_transfer(tag_slot, 22, UINT64_C(71404), UINT64_C(91404));
	tag_slot->holder_terminal_drain_generation = UINT64_C(45);
	memset(&poll, 0, sizeof(poll));
	poll.ref = tag_slot->holder_ref;
	poll.drain_generation = UINT64_C(46);
	tag_before = *tag_slot;
	before_state_flags = pg_atomic_read_u32(&tag_before.slot.state_flags);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session + 1),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->slot.state_flags),
				 before_state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	tag_after = *tag_slot;
	pg_atomic_write_u32(&tag_after.slot.state_flags, before_state_flags);
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_same_ref_non_source_participant_retires_both_legs)
{
	TestLocalParticipantTransfer fixture;
	PcmXRetirePayload retire;
	PcmXLocalProgress progress;
	PcmXLocalTagSlot *tag_slot;

	prepare_local_non_source_participant_transfer(7092, UINT64_C(1802), &fixture);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&fixture.poll, 1, fixture.master_session),
				 PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&fixture.leader, &progress),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	assert_local_queue_baseline(ClusterPcmXConvertShmem);

	/* A participant form cannot claim that this requester was also the image
	 * source while omitting the source holder_ref lane. */
	prepare_local_non_source_participant_transfer(7093, UINT64_C(1803), &fixture);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[fixture.leader.tag_slot.slot_index];
	tag_slot->image.source_node = (uint32)fixture.poll.ref.identity.node_id;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&fixture.poll, 1, fixture.master_session),
				 PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_local_successful_retire_clears_round_image_before_promoting_successor)
{
	TestLocalParticipantTransfer fixture;
	PcmXLocalHandle late;
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalProgress progress;
	PcmXLocalTagSlot *tag_slot;
	PcmXRetirePayload retire;
	PcmXWaitIdentity late_identity;
	PcmXWaitIdentity wake_items[1];
	PcmXLocalWakeBatch wake_batch;
	PcmXImageToken zero_image;

	prepare_local_non_source_participant_transfer(7098, UINT64_C(1808), &fixture);
	late_identity = make_wait_identity(7098, 0, 4, UINT64_C(71808));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&late_identity, 1, fixture.master_session, &late),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(late.local_round, fixture.cutoff.next_round);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[late.tag_slot.slot_index];
	UT_ASSERT(tag_slot->image.image_id != 0);
	UT_ASSERT(tag_slot->committed_own_generation != 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&fixture.poll, 1, fixture.master_session),
				 PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	memset(wake_items, 0, sizeof(wake_items));
	wake_batch.items = wake_items;
	wake_batch.capacity = lengthof(wake_items);
	wake_batch.count = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, fixture.master_session,
																&wake_batch),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(wake_batch.count, 1);
	UT_ASSERT(memcmp(&wake_items[0], &late.identity, sizeof(wake_items[0])) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&late.identity, &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(promoted.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	memset(&zero_image, 0, sizeof(zero_image));
	UT_ASSERT(memcmp(&tag_slot->image, &zero_image, sizeof(zero_image)) == 0);
	UT_ASSERT_EQ(tag_slot->committed_own_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &promoted, promoted.identity.base_own_generation + 1, &rekeyed),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&rekeyed, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.pending_opcode, 0);
	UT_ASSERT_EQ(progress.last_response_opcode, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

typedef struct TestLocalCancelledDualTransfer {
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;
	PcmXLocalCutoff cutoff;
	PcmXDrainPollPayload poll;
	uint64 master_session;
} TestLocalCancelledDualTransfer;

static void
prepare_local_same_ref_cancelled_participant(BlockNumber block, uint64 master_session,
											 bool with_follower,
											 TestLocalCancelledDualTransfer *fixture)
{
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity follower_identity;
	PcmXWaitIdentity leader_identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload admit_confirm;
	PcmXPhasePayload cancel;
	PcmXLocalReliableToken token;
	PcmXLocalWriterClaim writer_claim;
	PcmXImageToken zero_image = { 0 };

	UT_ASSERT_NOT_NULL(fixture);
	memset(fixture, 0, sizeof(*fixture));
	fixture->master_session = master_session;
	init_active_pcm_x(UINT64_C(77));
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(block, 0, 21, master_session + UINT64_C(1), 4);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	leader_identity = make_wait_identity(block, 0, 23, master_session + UINT64_C(2));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_join_begin(&leader_identity, 1, master_session, &fixture->leader),
		PCM_X_QUEUE_OK);
	if (with_follower) {
		follower_identity = make_wait_identity(block, 0, 24, master_session + UINT64_C(3));
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&follower_identity, 1, master_session,
													&fixture->follower),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&fixture->leader, &writer_claim),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&fixture->leader, &fixture->cutoff),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&fixture->leader, &enqueue, &token),
				 PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = leader_identity;
	admit_ack.ref.handle.ticket_id = master_session + UINT64_C(100);
	admit_ack.ref.handle.queue_generation = UINT64_C(13);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_apply_admit_ack_exact(&fixture->leader, &admit_ack, 1, master_session),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_arm_exact(&fixture->leader, &admit_confirm, &token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&fixture->leader, &admit_confirm, 1,
															 master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&admit_ack.ref, 1, master_session,
																 &holder_copy, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&admit_ack.ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(
					 &admit_ack.ref, blocker_snapshot.set_generation, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_release_exact(&writer_claim), PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_arm_exact(&fixture->leader, &cancel, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_ack_exact(&fixture->leader, &cancel, 1, master_session,
													  with_follower ? &fixture->promoted : NULL),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[fixture->leader.tag_slot.slot_index];
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &cancel.ref));
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &cancel.ref));
	UT_ASSERT(memcmp(&tag_slot->image, &zero_image, sizeof(zero_image)) == 0);

	fixture->poll.ref = cancel.ref;
	fixture->poll.drain_generation = UINT64_C(44);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&fixture->poll, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&fixture->poll, 1, master_session),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_TERMINAL_MASK,
				 PCM_X_LOCAL_TAG_F_TERMINAL_MASK);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK,
				 PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
}

UT_TEST(test_local_same_ref_cancelled_participant_gen0_retires_both_legs)
{
	TestLocalCancelledDualTransfer fixture;
	PcmXShmemHeader *header;
	PcmXLocalProgress progress;
	PcmXDrainPollPayload stale_poll;
	PcmXRetirePayload retire;

	prepare_local_same_ref_cancelled_participant(7108, UINT64_C(1808), false, &fixture);
	header = ClusterPcmXConvertShmem;
	stale_poll = fixture.poll;
	stale_poll.ref.grant_generation = UINT64_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&stale_poll, 1, fixture.master_session),
				 PCM_X_QUEUE_STALE);

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&fixture.leader, &progress),
				 PCM_X_QUEUE_NOT_FOUND);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_final_closed_member_retire_promotes_late_round)
{
	TestLocalCancelledDualTransfer fixture;
	PcmXLocalHandle late;
	PcmXLocalHandle promoted;
	PcmXLocalProgress progress;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot tag_before;
	PcmXLocalMembershipSlot *late_member;
	PcmXLocalMembershipSlot *terminal_member;
	PcmXLocalMembershipSlot late_before;
	PcmXLocalMembershipSlot terminal_before;
	PcmXWaitIdentity late_identity;
	PcmXEnqueuePayload enqueue;
	PcmXLocalReliableToken token;
	PcmXRetirePayload retire;
	PcmXWaitIdentity wake_items[1];
	PcmXLocalWakeBatch wake_batch;

	prepare_local_same_ref_cancelled_participant(7115, UINT64_C(1815), false, &fixture);
	late_identity = make_wait_identity(7115, 0, 25, UINT64_C(71505));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&late_identity, 1, fixture.master_session, &late),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(late.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(late.local_round, fixture.cutoff.next_round);

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[late.tag_slot.slot_index];
	late_member = &membership_slots(ClusterPcmXConvertShmem)[late.membership_slot.slot_index];
	terminal_member
		= &membership_slots(ClusterPcmXConvertShmem)[fixture.leader.membership_slot.slot_index];
	late_member->graph_generation = UINT64_C(71506);
	test_set_slot_state(&late_member->slot, PCM_XL_WAITABLE_FOLLOWER);
	tag_before = *tag_slot;
	late_before = *late_member;
	terminal_before = *terminal_member;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(memcmp(late_member, &late_before, sizeof(*late_member)) == 0);
	UT_ASSERT(memcmp(terminal_member, &terminal_before, sizeof(*terminal_member)) == 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmXConvertShmem->local_retire_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	late_member->graph_generation = 0;
	test_set_slot_state(&late_member->slot, PCM_XL_JOINED_NONWAITABLE);
	/* The first pass must reject an undersized process-local wake batch before
	 * consuming either terminal leg.  A caller can then retry the same exact
	 * watermark with sufficient capacity. */
	tag_before = *tag_slot;
	late_before = *late_member;
	terminal_before = *terminal_member;
	wake_batch.items = NULL;
	wake_batch.capacity = 0;
	wake_batch.count = 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, fixture.master_session,
																&wake_batch),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(wake_batch.count, 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(memcmp(late_member, &late_before, sizeof(*late_member)) == 0);
	UT_ASSERT(memcmp(terminal_member, &terminal_before, sizeof(*terminal_member)) == 0);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmXConvertShmem->local_retire_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	memset(wake_items, 0, sizeof(wake_items));
	wake_batch.items = wake_items;
	wake_batch.capacity = lengthof(wake_items);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, fixture.master_session,
																&wake_batch),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(wake_batch.count, 1);
	UT_ASSERT(memcmp(&wake_items[0], &late.identity, sizeof(wake_items[0])) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmXConvertShmem->local_retire_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&fixture.leader, &progress),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&late.identity, &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(promoted.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, late.membership_slot));
	UT_ASSERT_EQ(tag_slot->local_round, fixture.cutoff.next_round);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&promoted, &enqueue, &token),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_local_cancelled_dual_retires_one_writer_from_multiwriter_round)
{
	TestLocalCancelledDualTransfer fixture;
	PcmXShmemHeader *header;
	PcmXLocalProgress progress;
	PcmXLocalTagSlot *tag_slot;
	PcmXEnqueuePayload successor_enqueue;
	PcmXLocalReliableToken token;
	PcmXRetirePayload retire;
	PcmXWaitIdentity wake_items[1];
	PcmXLocalWakeBatch wake_batch;

	prepare_local_same_ref_cancelled_participant(7111, UINT64_C(1811), true, &fixture);
	header = ClusterPcmXConvertShmem;
	tag_slot = &local_tag_slots(header)[fixture.leader.tag_slot.slot_index];
	UT_ASSERT(slot_refs_equal(fixture.promoted.membership_slot, fixture.follower.membership_slot));
	UT_ASSERT_EQ(tag_slot->membership_count, 2);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 2);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	memset(wake_items, 0, sizeof(wake_items));
	wake_batch.items = wake_items;
	wake_batch.capacity = lengthof(wake_items);
	wake_batch.count = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, fixture.master_session,
																&wake_batch),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(wake_batch.count, 1);
	UT_ASSERT(memcmp(&wake_items[0], &fixture.promoted.identity, sizeof(wake_items[0])) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&fixture.leader, &progress),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&fixture.promoted, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(progress.member_state, PCM_XL_NODE_LEADER);
	UT_ASSERT_EQ(tag_slot->membership_count, 1);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 1);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &(PcmXTicketRef){ 0 }));
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK, 0);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_enqueue_arm_exact(&fixture.promoted, &successor_enqueue, &token),
		PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&successor_enqueue.identity, &fixture.promoted.identity,
					 sizeof(successor_enqueue.identity))
			  == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_cancelled_dual_retire_validates_before_consuming_holder_evidence)
{
	TestLocalCancelledDualTransfer fixture;
	PcmXLocalMembershipSlot *member;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef blocker_ref;
	PcmXRetirePayload retire;
	uint64 holder_drain_generation;

	prepare_local_same_ref_cancelled_participant(7109, UINT64_C(1809), false, &fixture);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[fixture.leader.tag_slot.slot_index];
	member = &membership_slots(ClusterPcmXConvertShmem)[fixture.leader.membership_slot.slot_index];
	blocker_ref = tag_slot->blocker_snapshot_ref;
	holder_drain_generation = tag_slot->holder_terminal_drain_generation;
	test_set_slot_state(&member->slot, PCM_XL_GRANTED);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &blocker_ref));
	UT_ASSERT_EQ(tag_slot->holder_terminal_drain_generation, holder_drain_generation);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK,
				 PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* A torn admitted-round locator is rejected before either terminal leg is
	 * consumed, even though every flag/ref/drain field remains otherwise exact. */
	prepare_local_same_ref_cancelled_participant(7110, UINT64_C(1810), false, &fixture);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[fixture.leader.tag_slot.slot_index];
	member = &membership_slots(ClusterPcmXConvertShmem)[fixture.leader.membership_slot.slot_index];
	blocker_ref = tag_slot->blocker_snapshot_ref;
	holder_drain_generation = tag_slot->holder_terminal_drain_generation;
	member->admitted_round++;
	retire.cluster_epoch = fixture.poll.ref.identity.cluster_epoch;
	retire.master_session_incarnation = fixture.master_session;
	retire.retire_through_ticket_id = fixture.poll.ref.handle.ticket_id;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, fixture.master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(ticket_refs_equal(&tag_slot->blocker_snapshot_ref, &blocker_ref));
	UT_ASSERT_EQ(tag_slot->holder_terminal_drain_generation, holder_drain_generation);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK,
				 PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_source_holder_rejects_parallel_blocker_terminal_lane)
{
	PcmXLocalCutoff cutoff;
	PcmXLocalHandle late;
	PcmXLocalTagSlot *tag_slot;
	PcmXDrainPollPayload poll;
	PcmXTicketRef blocker_ref;
	const uint64 master_session = UINT64_C(1804);

	tag_slot = prepare_retire_ready_round_with_holder(master_session, 7094, &cutoff, &late);
	memset(&poll, 0, sizeof(poll));
	poll.ref = tag_slot->holder_ref;
	poll.drain_generation = tag_slot->holder_terminal_drain_generation;
	tag_slot->blocker_snapshot_ref = tag_slot->holder_ref;
	tag_slot->blocker_snapshot_ref.grant_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* The XOR invariant precedes locator classification.  If the source lane
	 * names A while a corrupt blocker lane names B, B's DRAIN must not hide the
	 * dual-lane shape behind STALE merely because holder_ref != poll.ref. */
	tag_slot = prepare_retire_ready_round_with_holder(master_session + 1, 7106, &cutoff, &late);
	blocker_ref = tag_slot->holder_ref;
	blocker_ref.identity.procno++;
	blocker_ref.identity.request_id++;
	blocker_ref.identity.wait_seq++;
	blocker_ref.handle.ticket_id++;
	blocker_ref.handle.queue_generation++;
	blocker_ref.grant_generation = 0;
	tag_slot->blocker_snapshot_ref = blocker_ref;
	memset(&poll, 0, sizeof(poll));
	poll.ref = blocker_ref;
	poll.ref.grant_generation = tag_slot->holder_ref.grant_generation + 1;
	poll.drain_generation = tag_slot->holder_terminal_drain_generation;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, master_session + 1),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_retire_claim_serializes_admission_handoffs)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderKey blocked_holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle blocked_holder;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle blocked_writer;
	PcmXRetirePayload retire;
	PcmXWaitIdentity writer_identity;
	uint32 state_flags;
	const uint64 master_session = UINT64_C(1805);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(7095, 0, 2, UINT64_C(72001), 3);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = UINT64_C(9);
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = UINT64_C(999);
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	pg_atomic_write_u32(&tag_slot->slot.state_flags, state_flags);

	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(998);
	blocked_holder_key = make_local_holder_key(7096, 0, 3, UINT64_C(72002), 4);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&blocked_holder_key, &blocked_holder),
				 PCM_X_QUEUE_GATE_RETRY);
	writer_identity = make_wait_identity(7097, 0, 3, UINT64_C(72003));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_join_begin(&writer_identity, 1, master_session, &blocked_writer),
		PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}


UT_TEST(test_local_retire_global_gate_blocks_direct_mutators)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalMembershipSlot *member;
	PcmXLocalMembershipSlot member_before;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot tag_before;
	PcmXLocalWriterClaim claim;
	PcmXWaitIdentity identity;
	const uint64 master_session = UINT64_C(1816);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	identity = make_wait_identity(7116, 0, 26, UINT64_C(71601));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	member = &membership_slots(header)[leader.membership_slot.slot_index];
	tag_before = *tag_slot;
	member_before = *member;

	/* RETIRE's global claim wins before this tag-only mutator reaches its CAS.
	 * It must return retryable BUSY without changing any queue/member byte. */
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(93001);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_BUSY);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(tag_before)) == 0);
	UT_ASSERT(memcmp(member, &member_before, sizeof(member_before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	header->peer_frontiers[1].local_retire_in_progress_ticket_id = 0;
	pg_atomic_write_u32(&header->local_retire_gate, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_exact(&leader, &claim), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_writer_claim_abort_exact(&claim), PCM_X_QUEUE_OK);
}


UT_TEST(test_local_retire_global_gate_rejects_orphan_and_mismatched_evidence)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handle;
	PcmXWaitIdentity identity;
	const uint64 master_session = UINT64_C(1817);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	identity = make_wait_identity(7117, 0, 27, UINT64_C(71701));
	pg_atomic_write_u32(&header->local_retire_gate, PCM_X_PROTOCOL_NODE_LIMIT + 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 1);
	identity = make_wait_identity(7118, 0, 20, UINT64_C(71801));
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 1, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 2);
	identity = make_wait_identity(7119, 0, 21, UINT64_C(71901));
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(93002);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 2, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(80));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 3);
	identity = make_wait_identity(7120, 0, 22, UINT64_C(72001));
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(93003);
	header->peer_frontiers[2].local_retire_in_progress_ticket_id = UINT64_C(93004);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 3, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(81));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 4);
	identity = make_wait_identity(7121, 0, 23, UINT64_C(72101));
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[2].local_retire_in_progress_ticket_id = UINT64_C(93005);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 4, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(82));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 5);
	identity = make_wait_identity(7122, 0, 24, UINT64_C(72201));
	header->peer_frontiers[1].local_retired_ticket_id = UINT64_C(93006);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(93006);
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 5, &handle),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_retire_marker_blocks_cross_lock_tag_handoffs)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalHandle writer;
	PcmXLocalHandle next_leader;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef probe_ref;
	PcmXWaitIdentity writer_identity;
	PcmXRetirePayload retire;
	Size holder_used;
	Size tag_used;
	Size wait_used;
	const uint64 master_session = UINT64_C(1806);

	/* Build every external allocator<->tag handoff before letting RETIRE win
	 * the episode marker.  None may enter its domain mutation afterward. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(7098, 0, 2, UINT64_C(72101), 3);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&holder), PCM_X_QUEUE_OK);
	writer_identity = make_wait_identity(7099, 0, 3, UINT64_C(72102));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&writer_identity, 1, master_session, &writer),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&writer, NULL), PCM_X_QUEUE_OK);
	holder_used = header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used;
	wait_used = header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used;
	tag_used = header->allocator[PCM_X_ALLOC_LOCAL_TAG].used;
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(998);

	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&holder), PCM_X_QUEUE_GATE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writer), PCM_X_QUEUE_BUSY);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7100, 2, 4, UINT64_C(72103));
	probe_ref.handle.ticket_id = UINT64_C(91003);
	probe_ref.handle.queue_generation = UINT64_C(12);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &holder_snapshot),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, holder_used);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, wait_used);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, tag_used);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	/* Bufmgr retains this exact RELEASING handle when its bounded wait batch
	 * expires.  Once RETIRE clears the marker, retrying that same handle must
	 * detach normally (no reconstructed identity and no fail-closed state). */
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = 0;
	pg_atomic_write_u32(&header->local_retire_gate, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, holder_used - 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writer), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	/* A locally closed round is another tag->allocator handoff when it becomes
	 * empty.  RETIRE owns the marker, so even a ready round must remain closed. */
	tag_slot
		= prepare_retire_ready_round_with_holder(master_session + 1, 7101, &cutoff, &next_leader);
	header = ClusterPcmXConvertShmem;
	pg_atomic_write_u32(&header->local_retire_gate, 2);
	header->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(999);
	memset(&retire, 0, sizeof(retire));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_round_exact(&tag_slot->tag, UINT64_C(9), &cutoff, &writer),
		PCM_X_QUEUE_BUSY);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT_EQ(tag_slot->local_round, cutoff.closed_round);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_cross_lock_gate_precedes_retire_claim)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalCutoff cutoff;
	PcmXLocalHandle next_leader;
	PcmXLocalHandle writer;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef probe_ref;
	PcmXWaitIdentity writer_identity;
	const uint64 master_session = UINT64_C(1808);

	/* Simulate RETIRE acquiring allocator_lock immediately after the operation
	 * releases it.  The operation must already own ADMISSION_GATE, otherwise the
	 * hook publishes the RETIRE marker and exposes a forbidden overlap. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	holder_key = make_local_holder_key(7102, 0, 2, UINT64_C(72201), 3);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	arm_local_retire_release_interlock(tag_slot, 1, UINT64_C(92001));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_unregister_exact(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_NULL(local_retire_release_interlock_tag);
	UT_ASSERT(local_retire_release_interlock_observed_gate);
	UT_ASSERT(!local_retire_release_interlock_claimed);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 1);
	holder_key = make_local_holder_key(7103, 0, 3, UINT64_C(72202), 4);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7103, 2, 4, UINT64_C(72203));
	probe_ref.handle.ticket_id = UINT64_C(92002);
	probe_ref.handle.queue_generation = UINT64_C(13);
	arm_local_retire_release_interlock(tag_slot, 1, UINT64_C(92003));
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session + 1,
																 &holder_copy, 1, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_NULL(local_retire_release_interlock_tag);
	UT_ASSERT(local_retire_release_interlock_observed_gate);
	UT_ASSERT(!local_retire_release_interlock_claimed);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);

	tag_slot
		= prepare_retire_ready_round_with_holder(master_session + 2, 7104, &cutoff, &next_leader);
	header = ClusterPcmXConvertShmem;
	arm_local_retire_release_interlock(tag_slot, 1, UINT64_C(92004));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_round_exact(&tag_slot->tag, UINT64_C(9), &cutoff, &writer),
		PCM_X_QUEUE_OK);
	UT_ASSERT_NULL(local_retire_release_interlock_tag);
	UT_ASSERT(local_retire_release_interlock_observed_gate);
	UT_ASSERT(!local_retire_release_interlock_claimed);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session + 3);
	writer_identity = make_wait_identity(7105, 0, 5, UINT64_C(72204));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&writer_identity, 1, master_session + 3, &writer),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&writer, NULL), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[writer.tag_slot.slot_index];
	arm_local_retire_release_interlock(tag_slot, 1, UINT64_C(92005));
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writer), PCM_X_QUEUE_OK);
	UT_ASSERT_NULL(local_retire_release_interlock_tag);
	UT_ASSERT(local_retire_release_interlock_observed_gate);
	UT_ASSERT(!local_retire_release_interlock_claimed);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
}

UT_TEST(test_local_new_transfer_abort_keeps_gate_until_allocator_cleanup)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalTagSlot *reserved_tag;
	PcmXTicketRef probe_ref;
	PcmXSlotRef found;
	uint32 partition;
	const uint64 master_session = UINT64_C(1812);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	UT_ASSERT(header->allocator[PCM_X_ALLOC_LOCAL_TAG].free_head != PCM_X_INVALID_SLOT_INDEX);
	reserved_tag = &local_tag_slots(header)[header->allocator[PCM_X_ALLOC_LOCAL_TAG].free_head];
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7106, 2, 6, UINT64_C(72301));
	probe_ref.handle.ticket_id = UINT64_C(92006);
	probe_ref.handle.queue_generation = UINT64_C(14);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&probe_ref.identity.tag));
	local_transfer_peer_drift_lock = &header->local_locks[partition].lock;
	local_transfer_peer_drift_frontier = &header->peer_frontiers[1];
	/* prepare is allocator acquire #1; abort_new is #2.  If the local-domain
	 * peer drift makes publication fail, the reserved tag gate must still be
	 * present when allocator cleanup begins, otherwise RETIRE can claim the
	 * gap and observe a legal cleanup as post-preflight corruption. */
	arm_local_abort_acquire_interlock(reserved_tag, 1, UINT64_C(92007), 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, master_session,
																 NULL, 0, &holder_snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_NULL(local_transfer_peer_drift_lock);
	UT_ASSERT_NULL(local_transfer_peer_drift_frontier);
	UT_ASSERT_NULL(local_abort_acquire_interlock_tag);
	UT_ASSERT(local_abort_acquire_interlock_observed_gate);
	UT_ASSERT(!local_abort_acquire_interlock_claimed);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
}

UT_TEST(test_local_retire_requires_exact_holder_and_blocker_evidence)
{
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalCutoff cutoff;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef probe_ref;
	PcmXWaitIdentity identity;

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(190), 774, &cutoff, &late);
	tag_slot->holder_reliable.state_sequence = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(191), 775, &cutoff, &late);
	tag_slot->holder_reliable.last_responder_node = 2;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(192), 776, &cutoff, &late);
	tag_slot->holder_reliable.response_tombstone_mask = UINT32_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(193), 777, &cutoff, &late);
	tag_slot->holder_ref.identity.cluster_epoch++;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(194), 778, &cutoff, &late);
	tag_slot->holder_image.image_id = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(195), 779, &cutoff, &late);
	tag_slot->holder_terminal_drain_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	/* A nonzero blocker ref is an admission identity, not merely a durable
	 * busy bit.  A stale epoch must halt instead of becoming an endless retry. */
	init_active_pcm_x(UINT64_C(196));
	identity = make_wait_identity(780, 2, 8, UINT64_C(9501));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(196));
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = identity;
	probe_ref.handle.ticket_id = UINT64_C(9601);
	probe_ref.handle.queue_generation = UINT64_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, UINT64_C(196), NULL,
																 0, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(
					 &identity.tag, identity.cluster_epoch, 1, UINT64_C(196), &cutoff),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder_snapshot.tag_slot.slot_index];
	tag_slot->blocker_snapshot_ref.identity.cluster_epoch++;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	init_active_pcm_x(UINT64_C(198));
	identity = make_wait_identity(783, 2, 8, UINT64_C(9502));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(198));
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = identity;
	probe_ref.handle.ticket_id = UINT64_C(9602);
	probe_ref.handle.queue_generation = UINT64_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_probe_freeze_snapshot_exact(&probe_ref, 1, UINT64_C(198), NULL,
																 0, &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(
					 &identity.tag, identity.cluster_epoch, 1, UINT64_C(198), &cutoff),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder_snapshot.tag_slot.slot_index];
	tag_slot->blocker_set_generation = UINT64_C(1);
	tag_slot->blocker_snapshot_reliable.state_sequence = UINT64_C(1);
	tag_slot->blocker_snapshot_reliable.last_responder_node = 1;
	tag_slot->blocker_snapshot_reliable.last_response_opcode = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK;
	tag_slot->blocker_snapshot_reliable.response_tombstone_mask = UINT32_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&identity.tag, identity.cluster_epoch,
														&cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);

	/* Keep one positive leg beside the field-by-field negative matrix. */
	tag_slot = prepare_retire_ready_round_with_holder(UINT64_C(197), 781, &cutoff, &late);
	UT_ASSERT_NOT_NULL(tag_slot);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_local_blocker_allocator_corruption_retains_new_tag_gate_evidence)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalTagSlot *tag_slot;
	PcmXTicketRef probe_ref;
	PcmXSlotHeader *free_head;
	PcmXSlotRef tag_ref;
	Size free_head_index;
	const uint64 master_session = UINT64_C(1809);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(7105, 2, 5, UINT64_C(72301));
	probe_ref.handle.ticket_id = UINT64_C(91301);
	probe_ref.handle.queue_generation = UINT64_C(1);
	memset(&holder_snapshot, 0, sizeof(holder_snapshot));
	holder_snapshot.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;

	/* Even an empty blocker set validates the complete free chain before it
	 * publishes a generation.  Force the canonical self-loop corruption after
	 * transfer-tag preparation has enough capacity to reserve its new tag. */
	free_head_index = header->allocator[PCM_X_ALLOC_BLOCKER].free_head;
	UT_ASSERT(free_head_index != PCM_X_INVALID_SLOT_INDEX);
	free_head = pool_slot_header(header, PCM_X_POOL_BLOCKER, free_head_index);
	free_head->next_free = free_head_index;
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, NULL, 0, NULL, 0,
																&blocker_snapshot),
				 PCM_X_QUEUE_CORRUPT);

	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &tag_ref),
		PCM_X_DIRECTORY_OK);
	tag_slot = &local_tag_slots(header)[tag_ref.slot_index];
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_RESERVED_NONVISIBLE);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
}

UT_TEST(test_local_blocker_snapshot_replays_exact_holder_set_and_gates_revoke)
{
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalBlockerSnapshot duplicate_snapshot;
	PcmXBlockerSetHeaderPayload header;
	PcmXBlockerChunkPayload edge;
	PcmXRevokePayload revoke;
	PcmXTicketRef probe_ref;
	const uint64 master_session = UINT64_C(180);

	init_active_pcm_x(UINT64_C(77));
	holder_key = make_local_holder_key(709, 0, 1, UINT64_C(7031), 2);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &holder_copy, 1,
													 &holder_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder_snapshot.holder_count, 1);
	UT_ASSERT(memcmp(&holder, &holder_copy, sizeof(holder)) == 0);

	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(709, 2, 2, UINT64_C(7032));
	probe_ref.handle.ticket_id = UINT64_C(9011);
	probe_ref.handle.queue_generation = UINT64_C(11);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &duplicate_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&blocker_snapshot, &duplicate_snapshot, sizeof(blocker_snapshot)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_blocker_snapshot_copy_exact(&blocker_snapshot, &header, NULL, 0),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header.nblockers, 0);
	UT_ASSERT_EQ(header.set_crc32c, blocker_snapshot.set_crc32c);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);

	memset(&revoke, 0, sizeof(revoke));
	revoke.ref = probe_ref;
	revoke.ref.grant_generation = UINT64_C(101);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, UINT64_C(201), &revoke.image_id));
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_blocker_snapshot_copy_exact(&blocker_snapshot, &header, &edge, 1),
		PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_revoke_apply_exact(&revoke, 1, master_session),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_local_blocker_snapshot_exact_lookup_replays_when_pool_is_full)
{
	PcmXShmemHeader *header;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalBlockerSnapshot replay_snapshot;
	PcmXBlockerSetHeaderPayload block_header;
	PcmXBlockerChunkPayload replay_edge;
	PcmXTicketRef probe_ref;
	PcmXSlotRef fillers[8];
	PcmXSlotHeader *slot;
	ClusterLmdVertex blocker;
	Size filler_count;
	Size pool_capacity;
	const uint64 master_session = UINT64_C(181);

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	holder_key = make_local_holder_key(710, 0, 1, UINT64_C(7041), 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_register(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &holder_copy, 1,
													 &holder_snapshot),
				 PCM_X_QUEUE_OK);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(710, 2, 2, UINT64_C(7042));
	probe_ref.handle.ticket_id = UINT64_C(9012);
	probe_ref.handle.queue_generation = UINT64_C(12);
	blocker = make_blocker(0, holder_key.identity.procno, UINT64_C(7043));
	blocker.cluster_epoch = holder_key.identity.cluster_epoch;
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																&blocker, 1, &blocker_snapshot),
				 PCM_X_QUEUE_OK);

	pool_capacity = header->layout.pools[PCM_X_POOL_BLOCKER].capacity;
	UT_ASSERT(pool_capacity <= lengthof(fillers));
	for (filler_count = 0; filler_count < pool_capacity - 1; filler_count++)
		UT_ASSERT_EQ(
			cluster_pcm_x_allocator_reserve(PCM_X_ALLOC_BLOCKER, &fillers[filler_count], &slot),
			PCM_X_ALLOC_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, pool_capacity);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_lookup_exact(&probe_ref, 1, master_session,
																   &replay_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&replay_snapshot, &blocker_snapshot, sizeof(blocker_snapshot)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_copy_exact(&replay_snapshot, &block_header,
																 &replay_edge, 1),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&replay_edge.blocker, &blocker, sizeof(blocker)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_lookup_exact(
					 &probe_ref, 1, master_session + 1, &replay_snapshot),
				 PCM_X_QUEUE_STALE);

	for (Size i = 0; i < filler_count; i++)
		UT_ASSERT_EQ(cluster_pcm_x_allocator_release_exact(PCM_X_ALLOC_BLOCKER, fillers[i],
														   PCM_X_SLOT_RESERVED_NONVISIBLE),
					 PCM_X_ALLOC_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_local_blocker_snapshot_accepts_only_holder_bound_nested_waits)
{
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalHolderSnapshot stale_holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXLocalBlockerSnapshot duplicate_snapshot;
	PcmXBlockerSetHeaderPayload block_header;
	PcmXBlockerChunkPayload edge;
	PcmXTicketRef probe_ref;
	ClusterLmdVertex nested_wait;
	ClusterLmdVertex tx_wait;
	ClusterLmdVertex unrelated_wait;
	const uint64 master_session = UINT64_C(181);

	init_active_pcm_x(UINT64_C(77));
	holder_key = make_local_holder_key(710, 0, 1, UINT64_C(7033), 2);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &holder_copy, 1,
													 &holder_snapshot),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[holder.tag_slot.slot_index];
	tag_slot->blocker_set_generation = UINT64_C(41);

	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(710, 2, 2, UINT64_C(7034));
	probe_ref.handle.ticket_id = UINT64_C(9012);
	probe_ref.handle.queue_generation = UINT64_C(12);
	unrelated_wait = make_blocker(0, 3, UINT64_C(7035));
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(
					 &probe_ref, 1, master_session, &holder_snapshot, &holder_copy, 1,
					 &unrelated_wait, 1, &blocker_snapshot),
				 PCM_X_QUEUE_INVALID);
	UT_ASSERT_EQ(tag_slot->blocker_set_generation, UINT64_C(41));
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);

	nested_wait = make_blocker(0, 1, UINT64_C(7036));
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																&nested_wait, 1, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(blocker_snapshot.set_generation, UINT64_C(42));
	UT_ASSERT_EQ(holder_snapshot.holder_set_generation, UINT64_C(1));
	UT_ASSERT_EQ(blocker_snapshot.blocker_count, 1);
	stale_holder_snapshot = holder_snapshot;
	stale_holder_snapshot.holder_set_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(
					 &probe_ref, 1, master_session, &stale_holder_snapshot, &holder_copy, 1,
					 &nested_wait, 1, &duplicate_snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(
					 &probe_ref, 1, master_session, &holder_snapshot, &holder_copy, 1, &nested_wait,
					 1, &duplicate_snapshot),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&blocker_snapshot, &duplicate_snapshot, sizeof(blocker_snapshot)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_blocker_snapshot_copy_exact(&blocker_snapshot, &block_header, &edge, 1),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(block_header.nblockers, 1);
	UT_ASSERT(memcmp(&edge.blocker, &nested_wait, sizeof(nested_wait)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);

	/* A later PROBE for the same ticket may replace the ACKed immutable set
	 * with the next generation.  TX wait vertices legitimately carry a zero
	 * request_id and are exact through xid + wait_seq. */
	tx_wait = nested_wait;
	tx_wait.request_id = 0;
	tx_wait.xid = (TransactionId)UINT32_C(9042);
	tx_wait.wait_seq = UINT64_C(8042);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																&tx_wait, 1, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(blocker_snapshot.set_generation, UINT64_C(43));
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session,
																&holder_snapshot, &holder_copy, 1,
																NULL, 0, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(blocker_snapshot.set_generation, UINT64_C(44));
	UT_ASSERT_EQ(blocker_snapshot.blocker_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_ack_exact(&probe_ref, blocker_snapshot.set_generation,
													   1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(ClusterPcmXConvertShmem->allocator[PCM_X_ALLOC_BLOCKER].used, 0);
}

UT_TEST(test_local_duplicate_generation_overlap_is_stale_not_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle first;
	PcmXLocalHandle duplicate;
	PcmXLocalHandle cleared;
	PcmXLocalTagSlot tag_before;
	PcmXLocalMembershipSlot member_before;
	PcmXLocalMembershipSlot member_after;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity identity;
	Size local_tag_used_before;
	Size local_wait_used_before;
	Size tag_directory_before;
	Size wait_directory_before;
	uint32 odd;
	uint32 partition;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	identity = make_wait_identity(744, 0, 6, UINT64_C(54001));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &first),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[first.tag_slot.slot_index];
	member = &membership_slots(header)[first.membership_slot.slot_index];
	tag_before = *tag_slot;
	member_before = *member;
	local_tag_used_before = header->allocator[PCM_X_ALLOC_LOCAL_TAG].used;
	local_wait_used_before = header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used;
	tag_directory_before = directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG);
	wait_directory_before = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);
	memset(&duplicate, 0xa5, sizeof(duplicate));
	memset(&cleared, 0, sizeof(cleared));
	cleared.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	cleared.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity.tag));
	local_generation_interlock_lock = &header->local_locks[partition].lock;
	local_generation_interlock_slot = &member->slot;
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &duplicate),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_NULL(local_generation_interlock_slot);
	local_generation_interlock_lock = NULL;
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT(memcmp(&duplicate, &cleared, sizeof(duplicate)) == 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, local_tag_used_before);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, local_wait_used_before);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), tag_directory_before);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), wait_directory_before);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	member_after = *member;
	pg_atomic_write_u32(&member_after.slot.generation_change_seq,
						pg_atomic_read_u32(&member_before.slot.generation_change_seq));
	UT_ASSERT(memcmp(&member_after, &member_before, sizeof(member_after)) == 0);
	odd = pg_atomic_read_u32(&member->slot.generation_change_seq);
	UT_ASSERT((odd & 1U) != 0);
	pg_atomic_write_u32(&member->slot.generation_change_seq, odd + 1U);
}

UT_TEST(test_local_join_capacity_and_sequence_failure_roll_back_all_indexes)
{
	PcmXShmemHeader *header;
	PcmXAllocatorState saved_wait_allocator;
	PcmXDirectoryEntry *wait_entries;
	PcmXLocalHandle first;
	PcmXLocalHandle rejected;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;
	PcmXWaitIdentity second_identity;
	PcmXSlotRef found;
	uint64 identity_hash;
	Size capacity;
	Size directory_capacity;
	Size i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	saved_wait_allocator = header->allocator[PCM_X_ALLOC_LOCAL_WAIT];
	capacity = header->layout.local_wait.capacity;
	header->allocator[PCM_X_ALLOC_LOCAL_WAIT].free_head = PCM_X_INVALID_SLOT_INDEX;
	header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used = capacity;
	header->allocator[PCM_X_ALLOC_LOCAL_WAIT].high_water = capacity;
	header->allocator[PCM_X_ALLOC_LOCAL_WAIT].generation_exhausted = 0;
	identity = make_wait_identity(707, 0, 20, UINT64_C(8001));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &rejected),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), 0);
	header->allocator[PCM_X_ALLOC_LOCAL_WAIT] = saved_wait_allocator;
	assert_local_queue_baseline(header);
	wait_entries = directory_entries(header, PCM_X_DIR_LOCAL_WAIT, &directory_capacity);
	UT_ASSERT(cluster_pcm_x_directory_key_hash(PCM_X_DIR_LOCAL_WAIT, &identity, &identity_hash));
	for (i = 0; i < directory_capacity; i++) {
		wait_entries[i].state = PCM_X_DIRECTORY_OCCUPIED;
		wait_entries[i].key_hash = identity_hash ^ UINT64_C(1);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &rejected),
				 PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_TAG), 0);
	memset(wait_entries, 0, directory_capacity * sizeof(*wait_entries));
	assert_local_queue_baseline(header);

	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &first),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[first.tag_slot.slot_index];
	tag_slot->next_sequence = UINT64_MAX;
	second_identity = make_wait_identity(707, 0, 21, UINT64_C(8002));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&second_identity, 1, UINT64_C(77), &rejected),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_TAG].used, 1);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 1);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &second_identity, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&first, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&first), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_unlink_corruption_preserves_fifo_evidence_before_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[3];
	PcmXLocalTagSlot tag_before;
	PcmXLocalTagSlot tag_after;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot member_before[3];
	PcmXLocalMembershipSlot *members;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 3; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(749, 0, (uint32)(24 + i), UINT64_C(63001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	/* Corrupt only the successor backlink.  The fail-closed admission gate is
	 * expected evidence; the FIFO tag fields and all three members are not. */
	members[handles[2].membership_slot.slot_index].prev_index = PCM_X_INVALID_SLOT_INDEX;
	tag_before = *tag_slot;
	for (i = 0; i < 3; i++)
		member_before[i] = members[handles[i].membership_slot.slot_index];

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[1], NULL), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	tag_after = *tag_slot;
	UT_ASSERT_EQ(test_slot_flags(&tag_after.slot),
				 test_slot_flags(&tag_before.slot) | PCM_X_LOCAL_TAG_F_ADMISSION_GATE);
	pg_atomic_write_u32(&tag_after.slot.state_flags,
						pg_atomic_read_u32(&tag_before.slot.state_flags));
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	for (i = 0; i < 3; i++)
		UT_ASSERT(memcmp(&members[handles[i].membership_slot.slot_index], &member_before[i],
						 sizeof(member_before[i]))
				  == 0);

	/* A link-consistent chain with non-monotonic admission sequences is still
	 * FIFO corruption.  It must be rejected before leader rotation can strand
	 * a closed-round member behind a next-round candidate. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	for (i = 0; i < 3; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(750, 0, (uint32)(24 + i), UINT64_C(63011) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	members[handles[1].membership_slot.slot_index].local_sequence = 3;
	members[handles[2].membership_slot.slot_index].local_sequence = 2;
	tag_before = *tag_slot;
	for (i = 0; i < 3; i++)
		member_before[i] = members[handles[i].membership_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], NULL), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	tag_after = *tag_slot;
	UT_ASSERT_EQ(test_slot_flags(&tag_after.slot),
				 test_slot_flags(&tag_before.slot) | PCM_X_LOCAL_TAG_F_ADMISSION_GATE);
	pg_atomic_write_u32(&tag_after.slot.state_flags,
						pg_atomic_read_u32(&tag_before.slot.state_flags));
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	for (i = 0; i < 3; i++)
		UT_ASSERT(memcmp(&members[handles[i].membership_slot.slot_index], &member_before[i],
						 sizeof(member_before[i]))
				  == 0);
}

UT_TEST(test_local_cancel_requires_state_exact_leader_locator)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[2];
	PcmXLocalTagSlot tag_before;
	PcmXLocalTagSlot tag_after;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot member_before[2];
	PcmXLocalMembershipSlot *members;
	int i;

	/* A resident NODE_LEADER must own the exact tag leader locator. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(755, 0, (uint32)(2 + i), UINT64_C(69001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->leader_slot_generation = 0;
	tag_before = *tag_slot;
	for (i = 0; i < 2; i++)
		member_before[i] = members[handles[i].membership_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], NULL), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	tag_after = *tag_slot;
	UT_ASSERT_EQ(test_slot_flags(&tag_after.slot),
				 test_slot_flags(&tag_before.slot) | PCM_X_LOCAL_TAG_F_ADMISSION_GATE);
	pg_atomic_write_u32(&tag_after.slot.state_flags,
						pg_atomic_read_u32(&tag_before.slot.state_flags));
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	for (i = 0; i < 2; i++)
		UT_ASSERT(memcmp(&members[handles[i].membership_slot.slot_index], &member_before[i],
						 sizeof(member_before[i]))
				  == 0);

	/* A resident follower must never be named by the tag leader locator. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(756, 0, (uint32)(2 + i), UINT64_C(69003) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	tag_slot->leader_index = handles[1].membership_slot.slot_index;
	tag_slot->leader_slot_generation = handles[1].membership_slot.slot_generation;
	tag_before = *tag_slot;
	for (i = 0; i < 2; i++)
		member_before[i] = members[handles[i].membership_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[1], NULL), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	tag_after = *tag_slot;
	UT_ASSERT_EQ(test_slot_flags(&tag_after.slot),
				 test_slot_flags(&tag_before.slot) | PCM_X_LOCAL_TAG_F_ADMISSION_GATE);
	pg_atomic_write_u32(&tag_after.slot.state_flags,
						pg_atomic_read_u32(&tag_before.slot.state_flags));
	UT_ASSERT(memcmp(&tag_after, &tag_before, sizeof(tag_after)) == 0);
	for (i = 0; i < 2; i++)
		UT_ASSERT(memcmp(&members[handles[i].membership_slot.slot_index], &member_before[i],
						 sizeof(member_before[i]))
				  == 0);
}

UT_TEST(test_local_cutoff_preserves_cancelled_tail_sequence)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[3];
	PcmXLocalHandle next_leader;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 3; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(740, 0, (uint32)(2 + i), UINT64_C(41001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->next_sequence, 4);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[2], NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->tail_index, handles[1].membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->next_sequence, 4);

	/* The cutoff is the admission high-water, not the latest still-linked
	 * member.  It must include the cancelled terminal identity in this round. */
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&handles[0], &cutoff),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cutoff.cutoff_sequence, 3);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 3);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[1], NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], NULL), PCM_X_QUEUE_OK);
	for (i = 2; i >= 0; i--)
		UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[i]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(&handles[0].identity.tag,
														handles[0].identity.cluster_epoch, &cutoff,
														&next_leader),
				 PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_revoke_cutoff_sends_late_writer_to_next_round)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalHandle duplicate_leader;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalCutoff cutoff;
	PcmXLocalCutoff duplicate;
	PcmXLocalCutoff wrapped;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload admit_confirm;
	PcmXLocalReliableToken reliable_token;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *members;
	PcmXSlotRef found;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 22, UINT64_C(9001));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &leader),
					 PCM_X_QUEUE_OK);
	}
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 23, UINT64_C(9002));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &follower),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cutoff.cutoff_sequence, 2);
	UT_ASSERT_EQ(cutoff.closed_round, 1);
	UT_ASSERT_EQ(cutoff.next_round, 2);
	UT_ASSERT_EQ(cutoff.master_node, 1);
	UT_ASSERT_EQ(cutoff.master_session_incarnation, UINT64_C(77));
	UT_ASSERT_EQ(cutoff.reserved, 0);
	UT_ASSERT(slot_refs_equal(cutoff.tag_slot, leader.tag_slot));
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT_EQ(tag_slot->cutoff_sequence, 2);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 2);

	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &duplicate),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&duplicate, &cutoff, sizeof(cutoff)) == 0);
	memset(&duplicate, 0, sizeof(duplicate));
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(&leader.identity.tag,
																   leader.identity.cluster_epoch, 1,
																   UINT64_C(77), &duplicate),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&duplicate, &cutoff, sizeof(cutoff)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(&leader.identity.tag,
																   leader.identity.cluster_epoch, 1,
																   UINT64_C(78), &duplicate),
				 PCM_X_QUEUE_STALE);
	header->peer_frontiers[1].sender_session_incarnation = UINT64_C(78);
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(&leader.identity.tag,
																   leader.identity.cluster_epoch, 1,
																   UINT64_C(77), &duplicate),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &leader.identity.tag, leader.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER) != 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	header->peer_frontiers[1].sender_session_incarnation = UINT64_C(77);
	wrapped = cutoff;
	wrapped.closed_round = UINT32_MAX;
	wrapped.next_round = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &leader.identity.tag, leader.identity.cluster_epoch, &wrapped, &next_leader),
				 PCM_X_QUEUE_INVALID);

	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 24, UINT64_C(9003));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &late),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(late.local_sequence, 3);
	UT_ASSERT_EQ(late.local_round, 2);
	UT_ASSERT_EQ(late.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&late, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(910)),
				 PCM_X_QUEUE_OK);
	members = membership_slots(header);
	UT_ASSERT_EQ(test_slot_state(&members[late.membership_slot.slot_index].slot),
				 PCM_XL_WAITABLE_FOLLOWER);
	UT_ASSERT_EQ(tag_slot->leader_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->membership_count, 3);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 2);

	/* Conservative cancellation cannot prove whether this follower was a
	 * former active blocker, so neither linked identity disappears while the
	 * late writer retains its exact WFG edge. */
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&late, UINT64_C(910)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&follower, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&late, &wfg),
				 PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&follower), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 0);
	UT_ASSERT_EQ(tag_slot->leader_index, PCM_X_INVALID_SLOT_INDEX);
	/* Model a DRAIN/graph-removal race: retire must wait for exact removal and
	 * never classify an otherwise valid WAITABLE follower as corruption. */
	members[late.membership_slot.slot_index].graph_generation = UINT64_C(910);
	test_set_slot_state(&members[late.membership_slot.slot_index].slot, PCM_XL_WAITABLE_FOLLOWER);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_clear_exact(&late, UINT64_C(910)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(next_leader.local_round, 2);
	UT_ASSERT_EQ(next_leader.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(next_leader.membership_slot, late.membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &duplicate_leader),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(slot_refs_equal(duplicate_leader.membership_slot, next_leader.membership_slot));

	/* A duplicate old-round RETIRE may arrive after the promoted leader has
	 * already entered the next ticket's remote protocol.  That is idempotent
	 * progress, not corruption; no second wake handle is needed. */
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&next_leader, &enqueue, &reliable_token),
				 PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = next_leader.identity;
	admit_ack.ref.handle.ticket_id = UINT64_C(9201);
	admit_ack.ref.handle.queue_generation = UINT64_C(21);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_apply_admit_ack_exact(&next_leader, &admit_ack, 1, UINT64_C(77)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_arm_exact(&next_leader, &admit_confirm, &reliable_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_ack_exact(&next_leader, &admit_confirm, 1, UINT64_C(77)),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[next_leader.membership_slot.slot_index].slot),
				 PCM_XL_REMOTE_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &duplicate_leader),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(duplicate_leader.membership_slot.slot_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);

	/* Cancel unlinks the closed-round member before terminal detach decrements
	 * membership_count.  That evidence-retention window is legitimate and must
	 * remain a retry, not a structural failure. */
	init_active_pcm_x(UINT64_C(82));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(82));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 25, UINT64_C(9004));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(82), &leader),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->head_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->tail_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->membership_count, 1);
	UT_ASSERT_EQ(tag_slot->closed_round_member_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &leader.identity.tag, leader.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &leader.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, leader.tag_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &leader.identity.tag, leader.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);

	/* A writer joining after the last closed member detached must still see the
	 * retained barrier and enter the next round before exact retirement. */
	init_active_pcm_x(UINT64_C(83));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(83));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 25, UINT64_C(9008));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(83), &leader),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 26, UINT64_C(9009));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(83), &late),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(late.local_round, cutoff.next_round);
	UT_ASSERT_EQ(late.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(next_leader.membership_slot, late.membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&next_leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&next_leader), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 25, UINT64_C(9004));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &leader),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	tag_slot->local_round = UINT32_MAX;
	members = membership_slots(header);
	members[leader.membership_slot.slot_index].admitted_round = UINT32_MAX;
	leader.local_round = UINT32_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_REVOKE_BARRIER, 0);
	UT_ASSERT_EQ(tag_slot->cutoff_sequence, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);

	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(79));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 6, UINT64_C(9005));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &leader),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	members = membership_slots(header);
	tag_slot->local_round = UINT32_MAX;
	members[leader.membership_slot.slot_index].admitted_round = UINT32_MAX;
	leader.local_round = UINT32_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);

	init_active_pcm_x(UINT64_C(80));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(80));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 7, UINT64_C(9006));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(80), &leader),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	tag_slot->cutoff_sequence = 0;
	UT_ASSERT(tag_slot->closed_round_member_count > 0);
	members = membership_slots(header);
	members[leader.membership_slot.slot_index].admitted_round = tag_slot->local_round + 1;
	leader.local_round = tag_slot->local_round + 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);

	init_active_pcm_x(UINT64_C(81));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(81));
	{
		PcmXWaitIdentity identity = make_wait_identity(708, 0, 8, UINT64_C(9007));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(81), &leader),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	tag_slot->closed_round_member_count = tag_slot->membership_count + 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
}

UT_TEST(test_local_retire_rejects_candidate_from_wrong_round)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalCutoff cutoff;
	PcmXLocalMembershipSlot *candidate;
	PcmXRuntimeSnapshot snapshot;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	identity = make_wait_identity(709, 0, 26, UINT64_C(9101));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 27, UINT64_C(9102));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	candidate = &membership_slots(header)[late.membership_slot.slot_index];
	candidate->admitted_round = cutoff.closed_round;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&local_tag_slots(header)[late.tag_slot.slot_index].slot)
			   & PCM_X_LOCAL_TAG_F_ADMISSION_GATE)
			  != 0);
}

UT_TEST(test_local_cancel_rejects_waitable_successor_from_wrong_round)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalHandle late;
	PcmXLocalFollowerWfgSnapshot wfg;
	PcmXLocalCutoff cutoff;
	PcmXLocalMembershipSlot *candidate;
	PcmXRuntimeSnapshot snapshot;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9111));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 7, UINT64_C(9112));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_snapshot_exact(&late, &wfg), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_follower_wfg_commit_exact(&wfg, UINT64_C(911)),
				 PCM_X_QUEUE_OK);
	candidate = &membership_slots(header)[late.membership_slot.slot_index];
	candidate->admitted_round = cutoff.closed_round;
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_CORRUPT);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&local_tag_slots(header)[late.tag_slot.slot_index].slot)
			   & PCM_X_LOCAL_TAG_F_ADMISSION_GATE)
			  != 0);
}

UT_TEST(test_local_retire_rejects_closed_head_and_active_generation_corruption)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalCutoff cutoff;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *candidate;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9121));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 7, UINT64_C(9122));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	candidate = &membership_slots(header)[late.membership_slot.slot_index];
	candidate->local_sequence = cutoff.cutoff_sequence;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9123));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 7, UINT64_C(9124));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[late.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->active_writer_index, PCM_X_INVALID_SLOT_INDEX);
	tag_slot->active_writer_slot_generation = UINT64_C(99);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(79));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9125));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 7, UINT64_C(9126));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[late.tag_slot.slot_index];
	UT_ASSERT(tag_slot->membership_count > 0);
	UT_ASSERT(tag_slot->tail_index != PCM_X_INVALID_SLOT_INDEX);
	tag_slot->head_index = PCM_X_INVALID_SLOT_INDEX;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(80));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(80));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9127));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(80), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(709, 0, 7, UINT64_C(9128));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(80), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	candidate = &membership_slots(header)[late.membership_slot.slot_index];
	UT_ASSERT_EQ(candidate->prev_index, PCM_X_INVALID_SLOT_INDEX);
	candidate->prev_index = late.membership_slot.slot_index;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	init_active_pcm_x(UINT64_C(81));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(81));
	identity = make_wait_identity(709, 0, 6, UINT64_C(9129));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(81), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT(tag_slot->leader_index != PCM_X_INVALID_SLOT_INDEX);
	tag_slot->leader_slot_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_current_cutoff_snapshot_exact(
					 &leader.identity.tag, leader.identity.cluster_epoch, 1, UINT64_C(81), &cutoff),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_retire_duplicate_revalidates_leader_tag_linkage)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle leader;
	PcmXLocalHandle late;
	PcmXLocalHandle next_leader;
	PcmXLocalHandle duplicate;
	PcmXLocalCutoff cutoff;
	PcmXLocalProgress progress;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *candidate;
	PcmXRuntimeSnapshot snapshot;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	identity = make_wait_identity(710, 0, 26, UINT64_C(9201));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(710, 0, 27, UINT64_C(9202));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&next_leader, &progress), PCM_X_QUEUE_OK);
	candidate = &membership_slots(header)[late.membership_slot.slot_index];
	candidate->tag_slot_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&local_tag_slots(header)[late.tag_slot.slot_index].slot)
			   & PCM_X_LOCAL_TAG_F_ADMISSION_GATE)
			  != 0);

	/* Barrier-absent replay is valid only after the old cohort count reached
	 * zero.  A stale count must not be hidden behind DUPLICATE. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	identity = make_wait_identity(710, 0, 26, UINT64_C(9203));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(710, 0, 27, UINT64_C(9204));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[late.tag_slot.slot_index];
	tag_slot->closed_round_member_count = 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* A linked open-round FIFO must name its head as the unique leader. */
	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(79));
	identity = make_wait_identity(710, 0, 26, UINT64_C(9205));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	identity = make_wait_identity(710, 0, 27, UINT64_C(9206));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &late), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&leader), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &next_leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[late.tag_slot.slot_index];
	tag_slot->leader_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->leader_slot_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_round_exact(
					 &late.identity.tag, late.identity.cluster_epoch, &cutoff, &duplicate),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_cancel_is_exact_handoffs_leader_and_terminal_detach_restores_baseline)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[3];
	PcmXLocalHandle promoted;
	PcmXLocalHandle stale;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *members;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 3; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(709, 0, (uint32)(26 + i), UINT64_C(10001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	members = membership_slots(header);
	stale = handles[1];
	stale.membership_slot.slot_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&stale, NULL), PCM_X_QUEUE_STALE);
	stale = handles[1];
	stale.local_round++;
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&stale, NULL), PCM_X_QUEUE_STALE);
	stale = handles[1];
	stale.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&stale, NULL), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(members[handles[0].membership_slot.slot_index].next_index,
				 handles[1].membership_slot.slot_index);

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[1], NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[handles[1].membership_slot.slot_index].slot),
				 PCM_XL_CANCELLED);
	UT_ASSERT_EQ(members[handles[0].membership_slot.slot_index].next_index,
				 handles[2].membership_slot.slot_index);
	UT_ASSERT_EQ(members[handles[2].membership_slot.slot_index].prev_index,
				 handles[0].membership_slot.slot_index);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[1]), PCM_X_QUEUE_OK);

	memset(&promoted, 0, sizeof(promoted));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, handles[2].membership_slot));
	UT_ASSERT_EQ(promoted.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT_EQ(tag_slot->leader_index, handles[2].membership_slot.slot_index);
	UT_ASSERT_EQ(test_slot_state(&members[handles[2].membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&promoted, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_promoted_leader_identity_generation_rekey_is_exact)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[2];
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalHandle replay;
	PcmXLocalHandle lookup;
	PcmXLocalProgress progress;
	PcmXLocalMembershipSlot *member;
	PcmXEnqueuePayload enqueue;
	PcmXLocalReliableToken reliable;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity new_identity;
	PcmXSlotRef found;
	Size directory_count;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(812, 0, (uint32)(26 + i), UINT64_C(12011) + i);

		identity.base_own_generation = UINT64_C(4);
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(promoted.role, PCM_X_LOCAL_ROLE_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, handles[1].membership_slot));
	old_identity = promoted.identity;
	new_identity = old_identity;
	new_identity.base_own_generation = UINT64_C(9);
	directory_count = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);

	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &promoted, new_identity.base_own_generation, &rekeyed),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&rekeyed.identity, &new_identity, sizeof(new_identity)) == 0);
	UT_ASSERT(slot_refs_equal(rekeyed.tag_slot, promoted.tag_slot));
	UT_ASSERT(slot_refs_equal(rekeyed.membership_slot, promoted.membership_slot));
	UT_ASSERT_EQ(rekeyed.local_sequence, promoted.local_sequence);
	UT_ASSERT_EQ(rekeyed.local_round, promoted.local_round);
	UT_ASSERT_EQ(rekeyed.role, promoted.role);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), directory_count);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &old_identity, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&old_identity, &lookup), PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &new_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&new_identity, &lookup), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&lookup, &rekeyed, sizeof(lookup)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&rekeyed, &progress), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&progress.identity, &new_identity, sizeof(new_identity)) == 0);
	member = &membership_slots(header)[rekeyed.membership_slot.slot_index];
	UT_ASSERT(memcmp(&member->identity, &new_identity, sizeof(new_identity)) == 0);

	/* A caller that lost the first return value can replay the immutable old
	 * handle and recover the canonical new identity without a second move. */
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &promoted, new_identity.base_own_generation, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &rekeyed, sizeof(replay)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &rekeyed, new_identity.base_own_generation, &replay),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &rekeyed, sizeof(replay)) == 0);

	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&rekeyed, &enqueue, &reliable),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &rekeyed, new_identity.base_own_generation + 1, &replay),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &new_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));
	UT_ASSERT_EQ(test_slot_flags(&local_tag_slots(header)[rekeyed.tag_slot.slot_index].slot)
					 & PCM_X_LOCAL_TAG_F_ADMISSION_GATE,
				 0);
	UT_ASSERT_EQ(max_held_lwlock_count, 1);
}

UT_TEST(test_local_promoted_leader_rekey_rejects_live_evidence_and_key_alias)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[3];
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity alias_identity;
	PcmXSlotRef found;

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	old_identity = make_wait_identity(813, 0, 21, UINT64_C(13012));
	old_identity.base_own_generation = UINT64_C(5);
	alias_identity = old_identity;
	alias_identity.base_own_generation = UINT64_C(6);
	{
		PcmXWaitIdentity first = make_wait_identity(813, 0, 20, UINT64_C(13011));

		first.base_own_generation = UINT64_C(5);
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&first, 1, UINT64_C(78), &handles[0]),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&old_identity, 1, UINT64_C(78), &handles[1]),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&alias_identity, 1, UINT64_C(78), &handles[2]),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_OK);
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	UT_ASSERT(memcmp(&promoted.identity, &old_identity, sizeof(old_identity)) == 0);

	member->graph_generation = UINT64_C(7);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_leader_rekey_generation_exact(&promoted, UINT64_C(7), &rekeyed),
		PCM_X_QUEUE_BUSY);
	member->graph_generation = 0;
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &old_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));

	tag_slot->active_writer_index = promoted.membership_slot.slot_index;
	tag_slot->active_writer_slot_generation = promoted.membership_slot.slot_generation;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_leader_rekey_generation_exact(&promoted, UINT64_C(7), &rekeyed),
		PCM_X_QUEUE_BUSY);
	tag_slot->active_writer_index = PCM_X_INVALID_SLOT_INDEX;
	tag_slot->active_writer_slot_generation = 0;
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);

	/* A target identity owned by another immutable membership is structural
	 * corruption, not transient contention.  The old mapping must survive. */
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &alias_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, handles[2].membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &promoted, alias_identity.base_own_generation, &rekeyed),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &old_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));
}

UT_TEST(test_local_promoted_leader_rekey_allocator_handoff_error_releases_gate)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[2];
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity target_identity;
	PcmXSlotRef found;
	int i;

	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(79));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(814, 0, (uint32)(22 + i), UINT64_C(14011) + i);

		identity.base_own_generation = UINT64_C(8);
		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(79), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	old_identity = promoted.identity;
	target_identity = old_identity;
	target_identity.base_own_generation = UINT64_C(10);

	/* The first allocator acquisition resolves and gates the old key.  Throw
	 * on the second acquisition, before any directory mutation. */
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 2);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_leader_rekey_generation_exact(
		&promoted, target_identity.base_own_generation, &rekeyed));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &old_identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &target_identity, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
}


UT_TEST(test_local_rekey_staged_target_directory_is_retryable_busy)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle promoted;
	PcmXLocalHandle target_handle;
	PcmXLocalHandle lookup;
	PcmXLocalHandle zero_handle;
	PcmXLocalProgress progress;
	PcmXLocalProgress zero_progress;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity target_identity;
	Size directory_count;
	uint32 state_flags;

	prepare_promoted_rekey_fixture(815, UINT64_C(80), UINT64_C(8), &promoted);
	header = ClusterPcmXConvertShmem;
	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	old_identity = promoted.identity;
	target_identity = old_identity;
	target_identity.base_own_generation = UINT64_C(11);
	target_handle = promoted;
	target_handle.identity = target_identity;
	directory_count = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);

	UT_ASSERT_EQ(cluster_pcm_x_directory_delete_exact(PCM_X_DIR_LOCAL_WAIT, &old_identity,
													  promoted.membership_slot),
				 PCM_X_DIRECTORY_OK);
	test_publish_raw_local_wait_mapping(header, &target_identity, promoted.membership_slot);
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags | (PCM_X_LOCAL_TAG_F_ADMISSION_GATE << PCM_X_SLOT_FLAGS_SHIFT));

	memset(&lookup, 0xa5, sizeof(lookup));
	memset(&progress, 0xa5, sizeof(progress));
	memset(&zero_handle, 0, sizeof(zero_handle));
	zero_handle.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	zero_handle.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	memset(&zero_progress, 0, sizeof(zero_progress));
	UT_ASSERT_EQ(cluster_pcm_x_local_lookup_exact(&target_identity, &lookup), PCM_X_QUEUE_BUSY);
	UT_ASSERT(memcmp(&lookup, &zero_handle, sizeof(lookup)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_progress_exact(&target_handle, &progress), PCM_X_QUEUE_BUSY);
	UT_ASSERT(memcmp(&progress, &zero_progress, sizeof(progress)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT(
		test_raw_local_wait_mapping_exists(header, &target_identity, promoted.membership_slot));
	UT_ASSERT(!test_raw_local_wait_mapping_exists(header, &old_identity, promoted.membership_slot));
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), directory_count);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
}


UT_TEST(test_local_rekey_third_local_acquire_error_retains_gate)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalHandle zero_handle;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity target_identity;
	Size directory_count;
	uint32 partition;

	prepare_promoted_rekey_fixture(816, UINT64_C(81), UINT64_C(8), &promoted);
	header = ClusterPcmXConvertShmem;
	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	old_identity = promoted.identity;
	target_identity = old_identity;
	target_identity.base_own_generation = UINT64_C(12);
	directory_count = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&old_identity.tag));
	memset(&rekeyed, 0xa5, sizeof(rekeyed));
	memset(&zero_handle, 0, sizeof(zero_handle));
	zero_handle.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	zero_handle.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;

	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_EXCLUSIVE, 2);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_leader_rekey_generation_exact(
		&promoted, target_identity.base_own_generation, &rekeyed));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT(memcmp(&rekeyed, &zero_handle, sizeof(rekeyed)) == 0);
	UT_ASSERT(!test_raw_local_wait_mapping_exists(header, &old_identity, promoted.membership_slot));
	UT_ASSERT(
		test_raw_local_wait_mapping_exists(header, &target_identity, promoted.membership_slot));
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), directory_count);
}


UT_TEST(test_local_rekey_requires_canonical_fifo_leader)
{
	int scenario;

	for (scenario = 0; scenario < 3; scenario++) {
		PcmXShmemHeader *header;
		PcmXLocalHandle promoted;
		PcmXLocalHandle rekeyed;
		PcmXLocalHandle zero_handle;
		PcmXLocalTagSlot *tag_slot;
		PcmXLocalMembershipSlot *member;
		PcmXWaitIdentity old_identity;
		PcmXWaitIdentity target_identity;
		Size directory_count;
		uint32 state_flags;

		prepare_promoted_rekey_fixture((BlockNumber)(817 + scenario),
									   UINT64_C(82) + (uint64)scenario, UINT64_C(8), &promoted);
		header = ClusterPcmXConvertShmem;
		tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
		member = &membership_slots(header)[promoted.membership_slot.slot_index];
		old_identity = promoted.identity;
		target_identity = old_identity;
		target_identity.base_own_generation = UINT64_C(13) + (uint64)scenario;
		directory_count = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);
		memset(&rekeyed, 0xa5, sizeof(rekeyed));
		memset(&zero_handle, 0, sizeof(zero_handle));
		zero_handle.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
		zero_handle.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;

		switch (scenario) {
		case 0:
			state_flags = pg_atomic_read_u32(&member->slot.state_flags);
			pg_atomic_write_u32(
				&member->slot.state_flags,
				state_flags | (PCM_X_LOCAL_MEMBER_F_WRITER_COMPLETE << PCM_X_SLOT_FLAGS_SHIFT));
			break;
		case 1:
			tag_slot->head_index = PCM_X_INVALID_SLOT_INDEX;
			break;
		case 2:
			member->prev_index = member - membership_slots(header);
			break;
		default:
			UT_ASSERT(false);
		}

		UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
						 &promoted, target_identity.base_own_generation, &rekeyed),
					 PCM_X_QUEUE_CORRUPT);
		UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
		UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
		UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
		UT_ASSERT(memcmp(&rekeyed, &zero_handle, sizeof(rekeyed)) == 0);
		UT_ASSERT(
			test_raw_local_wait_mapping_exists(header, &old_identity, promoted.membership_slot));
		UT_ASSERT(!test_raw_local_wait_mapping_exists(header, &target_identity,
													  promoted.membership_slot));
		UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), directory_count);
	}
}


UT_TEST(test_local_rekey_target_insert_failure_restores_old_key)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle promoted;
	PcmXLocalHandle rekeyed;
	PcmXLocalHandle zero_handle;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXWaitIdentity old_identity;
	PcmXWaitIdentity target_identity;
	Size directory_count;

	prepare_promoted_rekey_fixture(820, UINT64_C(85), UINT64_C(8), &promoted);
	header = ClusterPcmXConvertShmem;
	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	old_identity = promoted.identity;
	target_identity = old_identity;
	target_identity.base_own_generation = UINT64_C(16);
	directory_count = directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT);
	memset(&rekeyed, 0xa5, sizeof(rekeyed));
	memset(&zero_handle, 0, sizeof(zero_handle));
	zero_handle.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	zero_handle.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;

	local_rekey_insert_failure_armed = true;
	local_rekey_insert_failure_match = 2;
	local_rekey_insert_failure_member = member;
	local_rekey_insert_failure_old_identity = old_identity;
	local_rekey_insert_failure_target_identity = target_identity;
	UT_ASSERT_EQ(cluster_pcm_x_local_leader_rekey_generation_exact(
					 &promoted, target_identity.base_own_generation, &rekeyed),
				 PCM_X_QUEUE_CORRUPT);
	UT_ASSERT(!local_rekey_insert_failure_armed);
	UT_ASSERT_EQ(local_rekey_insert_failure_match, 0);
	UT_ASSERT_EQ(local_rekey_insert_failure_phase, 2);
	UT_ASSERT_NULL(local_rekey_insert_failure_poisoned_entry);
	UT_ASSERT(local_rekey_insert_failure_observed_old_identity);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
	UT_ASSERT(memcmp(&member->identity, &old_identity, sizeof(old_identity)) == 0);
	UT_ASSERT(memcmp(&rekeyed, &zero_handle, sizeof(rekeyed)) == 0);
	UT_ASSERT(test_raw_local_wait_mapping_exists(header, &old_identity, promoted.membership_slot));
	UT_ASSERT(
		!test_raw_local_wait_mapping_exists(header, &target_identity, promoted.membership_slot));
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_WAIT), directory_count);
}

UT_TEST(test_last_writer_detach_waits_for_independent_holder_transfer_before_tag_release)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle writers[2];
	PcmXLocalHandle promoted;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXTicketRef holder_ref_before;
	PcmXReliableLegState holder_reliable_before;
	PcmXSlotRef found;
	uint32 state_flags;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(771, 0, (uint32)(26 + i), UINT64_C(11001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &writers[i]),
					 PCM_X_QUEUE_OK);
	}
	memset(&promoted, 0, sizeof(promoted));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&writers[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writers[0]), PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, writers[1].membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&promoted, NULL), PCM_X_QUEUE_OK);

	tag_slot = &local_tag_slots(header)[promoted.tag_slot.slot_index];
	member = &membership_slots(header)[promoted.membership_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->membership_count, 1);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, PCM_X_INVALID_SLOT_INDEX);
	seed_live_local_holder_transfer(tag_slot, 28, UINT64_C(11003), UINT64_C(12001));
	holder_ref_before = tag_slot->holder_ref;
	holder_reliable_before = tag_slot->holder_reliable;

	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_LIVE);
	UT_ASSERT_EQ(test_slot_state(&member->slot), PCM_XL_CANCELLED);
	UT_ASSERT_EQ(tag_slot->membership_count, 1);
	UT_ASSERT_EQ(tag_slot->master_node, 1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, UINT64_C(77));
	UT_ASSERT(memcmp(&tag_slot->holder_ref, &holder_ref_before, sizeof(holder_ref_before)) == 0);
	UT_ASSERT(
		memcmp(&tag_slot->holder_reliable, &holder_reliable_before, sizeof(holder_reliable_before))
		== 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &promoted.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.tag_slot));
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &promoted.identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));

	clear_local_holder_transfer(tag_slot);
	state_flags = pg_atomic_read_u32(&tag_slot->slot.state_flags);
	pg_atomic_write_u32(&tag_slot->slot.state_flags,
						state_flags
							| (PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_READY << PCM_X_SLOT_FLAGS_SHIFT));
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &promoted.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	pg_atomic_write_u32(&tag_slot->slot.state_flags, state_flags);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_OK);
	assert_local_queue_baseline(header);
}

UT_TEST(test_fifo_writer_drain_preserves_concurrent_holder_lane_until_transfer_drain)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle writers[2];
	PcmXLocalHandle promoted;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *holder_slot;
	PcmXTicketRef holder_ref_before;
	PcmXReliableLegState holder_reliable_before;
	PcmXSlotRef found;
	Size holder_head_before;
	int i;

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(178));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(772, 0, (uint32)(26 + i), UINT64_C(12001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(178), &writers[i]),
					 PCM_X_QUEUE_OK);
	}
	holder_key = make_local_holder_key(772, 0, 28, UINT64_C(12003), 2);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[writers[0].tag_slot.slot_index];
	holder_slot = &membership_slots(header)[holder.holder_slot.slot_index];
	holder_head_before = tag_slot->active_holder_head_index;
	UT_ASSERT_EQ(tag_slot->membership_count, 2);
	UT_ASSERT(holder_head_before != PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_HOLDER_ACTIVE);
	seed_live_local_holder_transfer(tag_slot, 29, UINT64_C(12004), UINT64_C(13001));
	holder_ref_before = tag_slot->holder_ref;
	holder_reliable_before = tag_slot->holder_reliable;

	/* Interleave the independent lanes: holder evidence is live before the
	 * two-writer FIFO becomes terminal and drains to its last membership. */
	memset(&promoted, 0, sizeof(promoted));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&writers[0], &promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&writers[0]), PCM_X_QUEUE_OK);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, writers[1].membership_slot));
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&promoted, NULL), PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(tag_slot->active_holder_head_index, holder_head_before);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_HOLDER_ACTIVE);
	UT_ASSERT_EQ(tag_slot->master_node, 1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, UINT64_C(178));
	UT_ASSERT(memcmp(&tag_slot->holder_ref, &holder_ref_before, sizeof(holder_ref_before)) == 0);
	UT_ASSERT(
		memcmp(&tag_slot->holder_reliable, &holder_reliable_before, sizeof(holder_reliable_before))
		== 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &promoted.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.tag_slot));
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &promoted.identity, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, promoted.membership_slot));

	clear_local_holder_transfer(tag_slot);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&promoted), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->master_node, -1);
	UT_ASSERT_EQ(tag_slot->master_session_incarnation, 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_WAIT, &promoted.identity, &found),
				 PCM_X_DIRECTORY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &promoted.identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(release_active_local_holder(&holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(directory_occupied_count(header, PCM_X_DIR_LOCAL_HOLDER), 0);
	assert_local_queue_baseline(header);
}

UT_TEST(test_local_terminal_detach_rejects_hot_link_and_closed_count_corruption)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handles[2];
	PcmXLocalMembershipSlot *member;
	PcmXLocalTagSlot *tag_slot;
	int i;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(77));
	for (i = 0; i < 2; i++) {
		PcmXWaitIdentity identity
			= make_wait_identity(747, 0, (uint32)(10 + i), UINT64_C(59001) + i);

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(77), &handles[i]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	member = &membership_slots(header)[handles[1].membership_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[1], NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->tail_index, handles[0].membership_slot.slot_index);
	tag_slot->tail_index = handles[1].membership_slot.slot_index;
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[1]), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(test_slot_state(&member->slot), PCM_XL_CANCELLED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 2);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), UINT64_C(78));
	{
		PcmXWaitIdentity identity = make_wait_identity(748, 0, 12, UINT64_C(60001));

		UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(78), &handles[0]),
					 PCM_X_QUEUE_OK);
	}
	tag_slot = &local_tag_slots(header)[handles[0].tag_slot.slot_index];
	member = &membership_slots(header)[handles[0].membership_slot.slot_index];
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handles[0], NULL), PCM_X_QUEUE_OK);
	tag_slot->closed_round_member_count = 1;
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handles[0]), PCM_X_QUEUE_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(test_slot_state(&member->slot), PCM_XL_CANCELLED);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 1);
}

UT_TEST(test_local_enqueue_arm_persists_ledger_and_mints_without_holes)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle leader;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload first;
	PcmXEnqueuePayload replay;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken snapshot;
	PcmXLocalReliableToken replay_token;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(760, 0, 2, UINT64_C(70001));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7101));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7101), &leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &first, &token), PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&first.identity, &identity, sizeof(identity)) == 0);
	UT_ASSERT_EQ(first.prehandle.sender_session_incarnation, 77);
	UT_ASSERT_EQ(first.prehandle.prehandle_sequence, 1);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 2);
	UT_ASSERT_EQ(header->outbound_targets[0].next_prehandle_sequence, 1);
	UT_ASSERT_EQ(header->outbound_targets[2].next_prehandle_sequence, 1);
	UT_ASSERT_EQ(tag_slot->prehandle.sender_session_incarnation, 77);
	UT_ASSERT_EQ(tag_slot->prehandle.prehandle_sequence, 1);
	UT_ASSERT_EQ(tag_slot->reliable.state_sequence, 1);
	UT_ASSERT_EQ(tag_slot->reliable.expected_responder_node, 1);
	UT_ASSERT_EQ(tag_slot->reliable.expected_responder_session, 7101);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ENQUEUE);
	UT_ASSERT_EQ(tag_slot->reliable.phase, PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &snapshot), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(snapshot.state_sequence, token.state_sequence);
	UT_ASSERT(memcmp(&snapshot.identity, &identity, sizeof(identity)) == 0);
	UT_ASSERT(memcmp(&snapshot.prehandle, &first.prehandle, sizeof(first.prehandle)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &replay, &replay_token),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(memcmp(&replay, &first, sizeof(first)) == 0);
	UT_ASSERT_EQ(replay_token.state_sequence, token.state_sequence);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 2);
}

UT_TEST(test_local_enqueue_busy_and_counter_exhaustion_leave_no_hole)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle leader;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXLocalReliableToken token;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(761, 0, 2, UINT64_C(70002));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7102));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7102), &leader),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	pg_atomic_write_u32(&header->outbound_targets[1].mint_gate, 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 1);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->prehandle.prehandle_sequence, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	pg_atomic_write_u32(&header->outbound_targets[1].mint_gate, 0);
	header->outbound_targets[1].next_prehandle_sequence = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->outbound_targets[1].mint_gate), 0);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->prehandle.prehandle_sequence, 0);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE) != 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}

UT_TEST(test_local_admit_ack_retry_and_confirm_are_exact)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *member;
	PcmXLocalHandle leader;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload ack;
	PcmXAdmitAckPayload stale_ack;
	PcmXPhasePayload confirm;
	PcmXPhasePayload stale_confirm;
	PcmXLocalReliableToken enqueue_token;
	PcmXLocalReliableToken retry_token;
	PcmXLocalReliableToken stale_token;
	PcmXLocalReliableToken confirm_token;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(762, 0, 2, UINT64_C(70003));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7103));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7103), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &enqueue_token),
				 PCM_X_QUEUE_OK);
	stale_token = enqueue_token;
	stale_token.state_sequence++;
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_retry_exact(&leader, &stale_token, 41, &retry_token),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_reliable_retry_exact(&leader, &enqueue_token, 42, &retry_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.state_sequence, enqueue_token.state_sequence);
	UT_ASSERT_EQ(retry_token.retry_count, 1);
	UT_ASSERT_EQ(retry_token.retry_deadline_ms, 42);
	UT_ASSERT(
		memcmp(&retry_token.prehandle, &enqueue_token.prehandle, sizeof(retry_token.prehandle))
		== 0);

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity = identity;
	ack.ref.handle.ticket_id = UINT64_C(9001);
	ack.ref.handle.queue_generation = UINT64_C(3);
	ack.prehandle = enqueue.prehandle;
	ack.result = PCM_X_QUEUE_OK;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	ack.flags = PCM_X_ADMIT_F_QUEUE_HEAD;
	stale_ack = ack;
	stale_ack.prehandle.prehandle_sequence++;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &stale_ack, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_STALE);
	stale_ack = ack;
	stale_ack.ref.identity.request_id++;
	stale_ack.ref.identity.wait_seq++;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &stale_ack, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_STALE);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ENQUEUE);
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &ack, 1, UINT64_C(7104)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ENQUEUE);
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &ack, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &ack.ref));
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->reliable.last_response_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_ACK);
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &ack, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_DUPLICATE);

	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &confirm_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&confirm.ref, &ack.ref));
	UT_ASSERT_EQ(confirm.reason, 0);
	UT_ASSERT_EQ(confirm.phase, PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM);
	UT_ASSERT_EQ(confirm.flags, 0);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 2);
	UT_ASSERT_EQ(tag_slot->reliable.state_sequence, 2);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM);
	stale_confirm = confirm;
	stale_confirm.ref.handle.ticket_id++;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &stale_confirm, 1, UINT64_C(7103)),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_OK);
	member = &membership_slots(header)[leader.membership_slot.slot_index];
	UT_ASSERT_EQ(test_slot_state(&member->slot), PCM_XL_REMOTE_WAIT);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->reliable.last_response_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, UINT64_C(7103)),
				 PCM_X_QUEUE_DUPLICATE);
}

UT_TEST(test_local_generic_reliable_ack_requires_exact_token_and_keys)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle leader;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload ack;
	PcmXPhasePayload confirm;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken stale_token;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(763, 0, 2, UINT64_C(70004));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7104));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7104), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&ack, 0, sizeof(ack));
	ack.ref.identity = identity;
	ack.ref.handle.ticket_id = UINT64_C(9002);
	ack.ref.handle.queue_generation = UINT64_C(4);
	ack.prehandle = enqueue.prehandle;
	ack.result = PCM_X_QUEUE_OK;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &ack, 1, UINT64_C(7104)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token),
				 PCM_X_QUEUE_OK);
	stale_token = token;
	stale_token.prehandle.prehandle_sequence++;
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_ack_exact(&leader, &stale_token, &confirm.ref, NULL,
														PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK, 1,
														UINT64_C(7104)),
				 PCM_X_QUEUE_STALE);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_ack_exact(&leader, &token, &confirm.ref, NULL,
														PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK, 1,
														UINT64_C(7104)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(test_slot_state(&membership_slots(header)[leader.membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
}

UT_TEST(test_local_pending_enqueue_cancel_keeps_ledger_until_exact_ack_then_rotates_leader)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalTagSlot tag_before;
	PcmXLocalMembershipSlot *members;
	PcmXLocalMembershipSlot leader_before;
	PcmXLocalMembershipSlot follower_before;
	PcmXWaitIdentity leader_identity;
	PcmXWaitIdentity follower_identity;
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;
	PcmXLocalHandle replay_promoted;
	PcmXLocalHandle no_successor;
	PcmXEnqueuePayload enqueue;
	PcmXEnqueuePayload successor_enqueue;
	PcmXPrehandleCancelPayload cancel;
	PcmXAdmitAckPayload ack;
	PcmXAdmitAckPayload stale_ack;
	PcmXDrainPollPayload poll;
	PcmXRetirePayload retire;
	PcmXLocalReliableToken enqueue_token;
	PcmXLocalReliableToken cancel_token;
	PcmXLocalReliableToken retry_token;
	PcmXLocalReliableToken successor_token;
	PcmXWaitIdentity wake_items[1];
	PcmXLocalWakeBatch wake_batch;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	leader_identity = make_wait_identity(766, 0, 12, UINT64_C(71001));
	follower_identity = make_wait_identity(766, 0, 13, UINT64_C(71002));
	bind_local_master(1, leader_identity.cluster_epoch, UINT64_C(7201));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, UINT64_C(7201), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&follower_identity, 1, UINT64_C(7201), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &enqueue_token),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_prehandle_cancel_arm_exact(&leader, &cancel, &cancel_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT(memcmp(&cancel.identity, &leader.identity, sizeof(cancel.identity)) == 0);
	UT_ASSERT(memcmp(&cancel.prehandle, &enqueue.prehandle, sizeof(cancel.prehandle)) == 0);
	UT_ASSERT(cancel_token.state_sequence > enqueue_token.state_sequence);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	members = membership_slots(header);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL);
	UT_ASSERT_EQ(tag_slot->reliable.phase, PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL);
	UT_ASSERT_EQ(tag_slot->leader_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(test_slot_state(&members[leader.membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&leader, &no_successor), PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(tag_slot->leader_index, leader.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_retry_exact(&leader, &cancel_token, 91, &retry_token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry_token.retry_count, 1);
	UT_ASSERT_EQ(retry_token.pending_opcode, PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL);

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity = leader_identity;
	ack.ref.handle.ticket_id = UINT64_C(9301);
	ack.ref.handle.queue_generation = UINT64_C(7);
	ack.prehandle = cancel.prehandle;
	ack.result = PCM_X_QUEUE_OK;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
	stale_ack = ack;
	stale_ack.prehandle.prehandle_sequence++;
	UT_ASSERT_EQ(cluster_pcm_x_local_prehandle_cancel_ack_exact(&leader, &stale_ack, 1,
																UINT64_C(7201), &promoted),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL);

	UT_ASSERT_EQ(
		cluster_pcm_x_local_prehandle_cancel_ack_exact(&leader, &ack, 1, UINT64_C(7201), &promoted),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[leader.membership_slot.slot_index].slot),
				 PCM_XL_CANCELLED);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, follower.membership_slot));
	UT_ASSERT_EQ(tag_slot->leader_index, follower.membership_slot.slot_index);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->reliable.last_response_opcode, PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK);
	UT_ASSERT(memcmp(&tag_slot->prehandle, &ack.prehandle, sizeof(ack.prehandle)) == 0);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &ack.ref));
	UT_ASSERT_EQ(members[leader.membership_slot.slot_index].handle.ticket_id,
				 ack.ref.handle.ticket_id);
	UT_ASSERT_EQ(cluster_pcm_x_local_prehandle_cancel_ack_exact(&leader, &ack, 1, UINT64_C(7201),
																&replay_promoted),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(slot_refs_equal(replay_promoted.membership_slot, promoted.membership_slot));
	UT_ASSERT_EQ(
		cluster_pcm_x_local_enqueue_arm_exact(&promoted, &successor_enqueue, &successor_token),
		PCM_X_QUEUE_BUSY);
	memset(&poll, 0, sizeof(poll));
	poll.ref = ack.ref;
	poll.drain_generation = UINT64_C(1);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7201)), PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = ack.ref.identity.cluster_epoch;
	retire.master_session_incarnation = UINT64_C(7201);
	retire.retire_through_ticket_id = ack.ref.handle.ticket_id;
	retire.sender_node = 0;
	tag_before = *tag_slot;
	leader_before = members[leader.membership_slot.slot_index];
	follower_before = members[follower.membership_slot.slot_index];
	wake_batch.items = NULL;
	wake_batch.capacity = 0;
	wake_batch.count = 1;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, UINT64_C(7201), &wake_batch),
		PCM_X_QUEUE_NO_CAPACITY);
	UT_ASSERT_EQ(wake_batch.count, 0);
	UT_ASSERT(memcmp(tag_slot, &tag_before, sizeof(*tag_slot)) == 0);
	UT_ASSERT(
		memcmp(&members[leader.membership_slot.slot_index], &leader_before, sizeof(leader_before))
		== 0);
	UT_ASSERT(memcmp(&members[follower.membership_slot.slot_index], &follower_before,
					 sizeof(follower_before))
			  == 0);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 0);
	memset(wake_items, 0, sizeof(wake_items));
	wake_batch.items = wake_items;
	wake_batch.capacity = lengthof(wake_items);
	wake_batch.count = 0;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_retire_up_to_collect_exact(&retire, 1, UINT64_C(7201), &wake_batch),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(wake_batch.count, 1);
	UT_ASSERT(memcmp(&wake_items[0], &promoted.identity, sizeof(wake_items[0])) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_enqueue_arm_exact(&promoted, &successor_enqueue, &successor_token),
		PCM_X_QUEUE_OK);
	UT_ASSERT(
		memcmp(&successor_enqueue.identity, &follower_identity, sizeof(successor_enqueue.identity))
		== 0);
	UT_ASSERT_EQ(successor_enqueue.prehandle.prehandle_sequence, 2);
	UT_ASSERT_EQ(tag_slot->ref.handle.ticket_id, 0);
}

UT_TEST(test_local_remote_wait_cancel_is_exact_retriable_and_rotates_only_on_ack)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *members;
	PcmXWaitIdentity leader_identity;
	PcmXWaitIdentity follower_identity;
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload confirm;
	PcmXPhasePayload cancel;
	PcmXPhasePayload stale_ack;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken retry;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	leader_identity = make_wait_identity(767, 0, 14, UINT64_C(72001));
	follower_identity = make_wait_identity(767, 0, 15, UINT64_C(72002));
	bind_local_master(1, leader_identity.cluster_epoch, UINT64_C(7301));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, UINT64_C(7301), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&follower_identity, 1, UINT64_C(7301), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = leader_identity;
	admit_ack.ref.handle.ticket_id = UINT64_C(9401);
	admit_ack.ref.handle.queue_generation = UINT64_C(8);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &admit_ack, 1, UINT64_C(7301)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, UINT64_C(7301)),
				 PCM_X_QUEUE_OK);

	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_arm_exact(&leader, &cancel, &token), PCM_X_QUEUE_OK);
	UT_ASSERT(ticket_refs_equal(&cancel.ref, &admit_ack.ref));
	UT_ASSERT_EQ(cancel.phase, PCM_X_LOCAL_RELIABLE_PHASE_CANCEL);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	members = membership_slots(header);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_CANCEL);
	UT_ASSERT_EQ(test_slot_state(&members[leader.membership_slot.slot_index].slot),
				 PCM_XL_REMOTE_WAIT);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_JOINED_NONWAITABLE);
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_retry_exact(&leader, &token, 101, &retry),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(retry.pending_opcode, PGRAC_IC_MSG_PCM_X_CANCEL);

	stale_ack = cancel;
	stale_ack.ref.handle.queue_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_cancel_ack_exact(&leader, &stale_ack, 1, UINT64_C(7301), &promoted),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_CANCEL);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_cancel_ack_exact(&leader, &cancel, 1, UINT64_C(7301), &promoted),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(test_slot_state(&members[leader.membership_slot.slot_index].slot),
				 PCM_XL_CANCELLED);
	UT_ASSERT_EQ(test_slot_state(&members[follower.membership_slot.slot_index].slot),
				 PCM_XL_NODE_LEADER);
	UT_ASSERT(slot_refs_equal(promoted.membership_slot, follower.membership_slot));
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->reliable.last_response_opcode, PGRAC_IC_MSG_PCM_X_CANCEL_ACK);
}

UT_TEST(test_local_exact_prepare_wins_cancel_vs_transfer_race)
{
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;
	PcmXLocalHandle leader;
	PcmXEnqueuePayload enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload confirm;
	PcmXPhasePayload cancel;
	PcmXGrantPayload prepare;
	PcmXGrantPayload stale_prepare;
	PcmXInstallReadyPayload install_ready;
	PcmXLocalReliableToken token;
	const uint64 master_session = UINT64_C(7311);

	init_active_pcm_x(UINT64_C(77));
	identity = make_wait_identity(769, 0, 18, UINT64_C(73101));
	bind_local_master(1, identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = identity;
	admit_ack.ref.handle.ticket_id = UINT64_C(9411);
	admit_ack.ref.handle.queue_generation = UINT64_C(11);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &admit_ack, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_arm_exact(&leader, &cancel, &token), PCM_X_QUEUE_OK);

	memset(&prepare, 0, sizeof(prepare));
	prepare.ref = admit_ack.ref;
	prepare.ref.grant_generation = UINT64_C(12);
	UT_ASSERT(cluster_pcm_x_image_id_encode(1, UINT64_C(212), &prepare.image.image_id));
	prepare.image.source_own_generation = UINT64_C(13);
	prepare.image.page_scn = UINT64_C(14);
	prepare.image.page_lsn = UINT64_C(15);
	prepare.image.source_node = 2;
	prepare.image.page_checksum = UINT32_C(16);
	tag_slot = &local_tag_slots(ClusterPcmXConvertShmem)[leader.tag_slot.slot_index];
	stale_prepare = prepare;
	stale_prepare.ref.handle.queue_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_x_local_prepare_grant_exact(&leader, &stale_prepare, 1, master_session),
		PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_CANCEL);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &admit_ack.ref));

	UT_ASSERT_EQ(cluster_pcm_x_local_prepare_grant_exact(&leader, &prepare, 1, master_session),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, 0);
	UT_ASSERT_EQ(tag_slot->reliable.last_response_opcode, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &prepare.ref));
	UT_ASSERT(memcmp(&tag_slot->image, &prepare.image, sizeof(prepare.image)) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_install_ready_arm_exact(&leader, &prepare.ref, &prepare.image,
															 &install_ready, &token),
				 PCM_X_QUEUE_OK);
}

UT_TEST(test_local_terminal_drain_retire_fences_successor_and_replays_watermark)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity leader_identity;
	PcmXWaitIdentity follower_identity;
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;
	PcmXEnqueuePayload enqueue;
	PcmXEnqueuePayload successor_enqueue;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload confirm;
	PcmXPhasePayload cancel;
	PcmXDrainPollPayload poll;
	PcmXRetirePayload retire;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken successor_token;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	leader_identity = make_wait_identity(768, 0, 16, UINT64_C(73001));
	follower_identity = make_wait_identity(768, 0, 17, UINT64_C(73002));
	bind_local_master(1, leader_identity.cluster_epoch, UINT64_C(7401));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 1, UINT64_C(7401), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&follower_identity, 1, UINT64_C(7401), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	memset(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref.identity = leader_identity;
	admit_ack.ref.handle.ticket_id = UINT64_C(9501);
	admit_ack.ref.handle.queue_generation = UINT64_C(9);
	admit_ack.prehandle = enqueue.prehandle;
	admit_ack.result = PCM_X_QUEUE_OK;
	admit_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	UT_ASSERT_EQ(cluster_pcm_x_local_apply_admit_ack_exact(&leader, &admit_ack, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_admit_confirm_ack_exact(&leader, &confirm, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_arm_exact(&leader, &cancel, &token), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_cancel_ack_exact(&leader, &cancel, 1, UINT64_C(7401), &promoted),
		PCM_X_QUEUE_OK);

	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_TERMINAL_READY) != 0);
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) == 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_terminal_publish_exact(&cancel.ref, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_enqueue_arm_exact(&promoted, &successor_enqueue, &successor_token),
		PCM_X_QUEUE_BUSY);
	UT_ASSERT(ticket_refs_equal(&tag_slot->ref, &cancel.ref));

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = leader_identity.cluster_epoch;
	retire.master_session_incarnation = UINT64_C(7401);
	retire.retire_through_ticket_id = cancel.ref.handle.ticket_id;
	retire.sender_node = 0;
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_NOT_READY);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retired_ticket_id, 0);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 0);

	memset(&poll, 0, sizeof(poll));
	poll.ref = cancel.ref;
	poll.drain_generation = UINT64_C(41);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7402)), PCM_X_QUEUE_STALE);
	poll.ref.handle.queue_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7401)), PCM_X_QUEUE_STALE);
	poll.ref = cancel.ref;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7401)), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(tag_slot->terminal_drain_generation, UINT64_C(41));
	UT_ASSERT((test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED) != 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_local_terminal_publish_exact(&cancel.ref, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_DUPLICATE);
	poll.drain_generation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_drain_poll_exact(&poll, 1, UINT64_C(7401)), PCM_X_QUEUE_STALE);

	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, UINT64_C(7402)),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retired_ticket_id,
				 retire.retire_through_ticket_id);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 0);
	UT_ASSERT((test_slot_flags(&tag_slot->slot)
			   & (PCM_X_LOCAL_TAG_F_TERMINAL_READY | PCM_X_LOCAL_TAG_F_TERMINAL_DRAINED))
			  == 0);
	UT_ASSERT_EQ(tag_slot->ref.handle.ticket_id, 0);
	UT_ASSERT_EQ(cluster_pcm_x_local_retire_up_to_exact(&retire, 1, UINT64_C(7401)),
				 PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(
		cluster_pcm_x_local_enqueue_arm_exact(&promoted, &successor_enqueue, &successor_token),
		PCM_X_QUEUE_OK);
}

UT_TEST(test_local_reliable_authority_change_fails_closed_without_dropping_ledger)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalHandle leader;
	PcmXWaitIdentity identity;
	PcmXEnqueuePayload enqueue;
	PcmXLocalReliableToken token;
	PcmXLocalReliableToken snapshot;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(764, 0, 2, UINT64_C(70005));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7105));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7105), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	header->outbound_targets[1].target_session_incarnation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ENQUEUE);
	UT_ASSERT_EQ(tag_slot->prehandle.prehandle_sequence, 1);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 2);

	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(765, 0, 2, UINT64_C(70006));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(7106));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(7106), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_enqueue_arm_exact(&leader, &enqueue, &token), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[leader.tag_slot.slot_index];
	header->peer_frontiers[1].sender_session_incarnation++;
	UT_ASSERT_EQ(cluster_pcm_x_local_reliable_snapshot_exact(&leader, &snapshot),
				 PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(tag_slot->reliable.pending_opcode, PGRAC_IC_MSG_PCM_X_ENQUEUE);
	UT_ASSERT_EQ(tag_slot->prehandle.prehandle_sequence, 1);
	UT_ASSERT_EQ(header->outbound_targets[1].next_prehandle_sequence, 2);
}

UT_TEST(test_capacity_addition_overflow_is_checked)
{
	PcmXShmemLayout layout;

	UT_EXPECT_ARITHMETIC_OVERFLOW(cluster_pcm_x_layout_compute(SIZE_MAX, 1, 1, 1, &layout));
}

UT_TEST(test_capacity_multiplication_overflow_is_checked)
{
	PcmXShmemLayout layout;

	UT_EXPECT_ARITHMETIC_OVERFLOW(
		cluster_pcm_x_layout_compute(SIZE_MAX / PCM_X_PROTOCOL_NODE_LIMIT + 1, 0, 1, 1, &layout));
}

UT_TEST(test_ticket_directory_capacity_doubling_overflow_is_checked)
{
	PcmXShmemLayout layout;
	Size p = SIZE_MAX / PCM_X_PROTOCOL_NODE_LIMIT;
	Size c = p * PCM_X_PROTOCOL_NODE_LIMIT;

	UT_ASSERT(c > SIZE_MAX / 2);
	UT_EXPECT_ARITHMETIC_OVERFLOW(cluster_pcm_x_layout_compute(p, 0, 0, 1, &layout));
	UT_ASSERT_EQ(arithmetic_overflow_left, c);
	UT_ASSERT_EQ(arithmetic_overflow_right, 2);
}

UT_TEST(test_fresh_init_builds_independent_free_lists)
{
	PcmXShmemHeader *header;
	PcmXLocalMembershipSlot *slots;
	PcmXLocalMembershipSlot *holder_slots;
	Size p;
	Size h;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	slots = membership_slots(header);
	holder_slots
		= (PcmXLocalMembershipSlot *)((char *)header + header->layout.local_holder.slots_offset);
	p = header->layout.local_wait.capacity;
	h = header->layout.local_holder.capacity;
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].free_head, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].free_head, p);
	UT_ASSERT_EQ(&holder_slots[header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].free_head
							   - header->layout.local_holder.first_slot_index],
				 &slots[p]);
	UT_ASSERT_EQ(slots[0].slot.next_free, 1);
	UT_ASSERT_EQ(slots[p - 1].slot.next_free, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(slots[p].slot.next_free, p + 1);
	UT_ASSERT_EQ(slots[p + h - 1].slot.next_free, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].used, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_WAIT].high_water, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_LOCAL_HOLDER].high_water, 0);
}

UT_TEST(test_fresh_init_is_recovery_blocked_and_initializes_all_locks_once)
{
	PcmXShmemHeader *header;
	int i;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	UT_ASSERT_EQ(header->master_session_incarnation, 0);
	UT_ASSERT_EQ(header->next_ticket_id, 1);
	UT_ASSERT_EQ(header->next_image_id, 1);
	UT_ASSERT_EQ(header->fully_retired_ticket_id, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->runtime_gate), PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(lwlock_init_count, PCM_X_LWLOCK_COUNT);
	UT_ASSERT_EQ(allocator_lock_init_count, 1);
	UT_ASSERT_EQ(master_lock_init_count, PCM_X_LOCK_PARTITIONS);
	UT_ASSERT_EQ(local_lock_init_count, PCM_X_LOCK_PARTITIONS);
	for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
		UT_ASSERT_EQ(header->peer_frontiers[i].cluster_epoch, 0);
		UT_ASSERT_EQ(header->peer_frontiers[i].sender_session_incarnation, 0);
		UT_ASSERT_EQ(header->peer_frontiers[i].next_expected_prehandle_sequence, 1);
		UT_ASSERT_EQ(header->peer_frontiers[i].retired_prehandle_sequence, 0);
	}
}

UT_TEST(test_fresh_init_sets_every_slot_free_at_generation_zero)
{
	PcmXShmemHeader *header;
	int pool;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	for (pool = 0; pool < PCM_X_POOL_COUNT; pool++) {
		Size i;

		for (i = 0; i < header->layout.pools[pool].capacity; i++) {
			PcmXSlotHeader *slot = pool_slot_header(header, (PcmXPoolKind)pool, i);

			UT_ASSERT_EQ(test_slot_generation(slot), 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&slot->generation_change_seq), 0);
			UT_ASSERT_EQ(test_slot_state(slot), PCM_X_SLOT_FREE);
			UT_ASSERT_EQ(test_slot_flags(slot), 0);
		}
	}
}

UT_TEST(test_exec_backend_attach_preserves_mutable_state)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	UT_ASSERT(cluster_pcm_x_runtime_activate(71));
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(3, 9, 44), PCM_X_QUEUE_OK);
	header->allocator[PCM_X_ALLOC_MASTER_TICKET].used = 7;
	header->peer_frontiers[3].next_expected_prehandle_sequence = 8;
	header->peer_frontiers[3].retired_prehandle_sequence = 6;
	header->outbound_targets[3].next_prehandle_sequence = 8;
	cluster_pcm_x_convert_shmem_init();
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(ClusterPcmXConvertShmem, header);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_MASTER_TICKET].used, 7);
	UT_ASSERT_EQ(header->peer_frontiers[3].cluster_epoch, 9);
	UT_ASSERT_EQ(header->peer_frontiers[3].sender_session_incarnation, 44);
	UT_ASSERT_EQ(header->peer_frontiers[3].next_expected_prehandle_sequence, 8);
	UT_ASSERT_EQ(header->peer_frontiers[3].retired_prehandle_sequence, 6);
	UT_ASSERT_EQ(header->outbound_targets[3].cluster_epoch, 9);
	UT_ASSERT_EQ(header->outbound_targets[3].target_session_incarnation, 44);
	UT_ASSERT_EQ(header->outbound_targets[3].next_prehandle_sequence, 8);
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 71);
	UT_ASSERT_EQ(snapshot.gate_generation, 1);
	UT_ASSERT_EQ(lwlock_init_count, PCM_X_LWLOCK_COUNT);
}

UT_TEST(test_runtime_gate_is_atomic_and_session_fenced)
{
	PcmXRuntimeSnapshot snapshot;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 0);
	UT_ASSERT_EQ(snapshot.gate_generation, 0);
	UT_ASSERT(!cluster_pcm_x_runtime_activate(0));
	UT_ASSERT(cluster_pcm_x_runtime_activate(77));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 77);
	UT_ASSERT_EQ(snapshot.gate_generation, 1);
	UT_ASSERT(!cluster_pcm_x_runtime_activate(78));
	UT_ASSERT(
		!cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_RECOVERY_BLOCKED, PCM_X_RUNTIME_ACTIVE));
	UT_ASSERT(
		cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_RECOVERY_BLOCKED));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 0);
	UT_ASSERT_EQ(snapshot.gate_generation, 2);
	/* A normal fail-stop close is not an implicit recovery authority. */
	UT_ASSERT(!cluster_pcm_x_runtime_activate(88));
}

UT_TEST(test_runtime_activating_phase_has_exact_recovery_reset)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;
	uint32 activating_gate;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	header->master_session_incarnation = 77;
	activating_gate = (UINT32_C(3) << PCM_X_RUNTIME_GATE_STATE_BITS) | UINT32_C(3);
	pg_atomic_write_u32(&header->runtime_gate, activating_gate);
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.gate_generation, 3);
	UT_ASSERT(!cluster_pcm_x_runtime_reset_activating(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->runtime_gate), activating_gate);
	UT_ASSERT(cluster_pcm_x_runtime_reset_activating(3));
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->activation_retry_generation), 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->runtime_gate),
				 (UINT32_C(3) << PCM_X_RUNTIME_GATE_STATE_BITS) | PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT(cluster_pcm_x_runtime_activate(78));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 78);
	UT_ASSERT_EQ(snapshot.gate_generation, 4);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->activation_retry_generation), 0);
}

UT_TEST(test_runtime_gate_generation_prevents_active_aba)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot first;
	PcmXRuntimeSnapshot second;
	uint32 activating_gate;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_runtime_activate(77));
	first = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT(
		cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_RECOVERY_BLOCKED));
	UT_ASSERT(!cluster_pcm_x_runtime_activate(77));
	UT_ASSERT(!cluster_pcm_x_runtime_activate(88));

	/* Only an exact ACTIVATING reset authorizes a later-session retry. */
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	header->master_session_incarnation = 77;
	activating_gate = (UINT32_C(3) << PCM_X_RUNTIME_GATE_STATE_BITS) | UINT32_C(3);
	pg_atomic_write_u32(&header->runtime_gate, activating_gate);
	UT_ASSERT(cluster_pcm_x_runtime_reset_activating(3));
	UT_ASSERT(cluster_pcm_x_runtime_activate(88));
	second = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(first.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(second.state, PCM_X_RUNTIME_ACTIVE);
	UT_ASSERT_EQ(first.master_session_incarnation, 77);
	UT_ASSERT_EQ(second.master_session_incarnation, 88);
	UT_ASSERT(first.gate_generation != second.gate_generation);
}

UT_TEST(test_runtime_gate_generation_exhaustion_never_wraps)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;
	uint32 gate;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	gate = ((PCM_X_RUNTIME_GATE_GENERATION_MAX - 2) << PCM_X_RUNTIME_GATE_STATE_BITS)
		   | PCM_X_RUNTIME_RECOVERY_BLOCKED;
	pg_atomic_write_u32(&header->runtime_gate, gate);
	pg_atomic_write_u32(&header->activation_retry_generation,
						PCM_X_RUNTIME_GATE_GENERATION_MAX - 2);
	UT_ASSERT(cluster_pcm_x_runtime_activate(99));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.gate_generation, PCM_X_RUNTIME_GATE_GENERATION_MAX - 1);
	UT_ASSERT(
		cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_RECOVERY_BLOCKED));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.gate_generation, PCM_X_RUNTIME_GATE_GENERATION_MAX);
	UT_ASSERT(!cluster_pcm_x_runtime_activate(100));
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->runtime_gate),
				 ((PCM_X_RUNTIME_GATE_GENERATION_MAX << PCM_X_RUNTIME_GATE_STATE_BITS)
				  | PCM_X_RUNTIME_RECOVERY_BLOCKED));
}

UT_TEST(test_runtime_gate_generation_exhaustion_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXRuntimeSnapshot snapshot;
	uint32 gate;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	gate = ((PCM_X_RUNTIME_GATE_GENERATION_MAX - 1) << PCM_X_RUNTIME_GATE_STATE_BITS)
		   | PCM_X_RUNTIME_ACTIVE;
	header->master_session_incarnation = 99;
	pg_atomic_write_u32(&header->runtime_gate, gate);
	UT_ASSERT(!cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_SHUTTING_DOWN));
	snapshot = cluster_pcm_x_runtime_snapshot();
	UT_ASSERT_EQ(snapshot.state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, 0);
	UT_ASSERT_EQ(snapshot.gate_generation, PCM_X_RUNTIME_GATE_GENERATION_MAX);
}

UT_TEST(test_master_state_machine_requires_exact_positive_gates)
{
	PcmXMasterTicketState next;

	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_ADMITTING, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT, &next),
				 PCM_X_STEP_BLOCKED);
	UT_ASSERT_EQ(next, PCM_XT_ADMITTING);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_ADMITTING, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT | PCM_X_G_WFG_READY, &next),
				 PCM_X_STEP_APPLIED);
	UT_ASSERT_EQ(next, PCM_XT_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_QUEUED, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT | PCM_X_G_WFG_READY, &next),
				 PCM_X_STEP_BLOCKED);
	UT_ASSERT_EQ(next, PCM_XT_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_QUEUED, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT | PCM_X_G_CONFIRM_TOMBSTONE, &next),
				 PCM_X_STEP_DUPLICATE);
	UT_ASSERT_EQ(next, PCM_XT_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_CANCELLED, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT, &next),
				 PCM_X_STEP_BLOCKED);
	UT_ASSERT_EQ(next, PCM_XT_CANCELLED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_CANCELLED, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_EXACT | PCM_X_G_CONFIRM_TOMBSTONE, &next),
				 PCM_X_STEP_DUPLICATE);
	UT_ASSERT_EQ(next, PCM_XT_CANCELLED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_ADMITTING, PCM_X_EVENT_ADMIT_CONFIRM,
										   PCM_X_G_WFG_READY, &next),
				 PCM_X_STEP_STALE);
	UT_ASSERT_EQ(next, PCM_XT_ADMITTING);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_RETIRE_CREDIT, PCM_X_EVENT_COMMIT_COMPLETE,
										   PCM_X_G_EXACT, &next),
				 PCM_X_STEP_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_RETIRE_CREDIT, PCM_X_EVENT_COMMIT_COMPLETE,
										   PCM_X_G_EXACT | PCM_X_G_COMPLETE_TOMBSTONE, &next),
				 PCM_X_STEP_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_RETIRE_CREDIT, PCM_X_EVENT_CANCEL_EXACT,
										   PCM_X_G_EXACT | PCM_X_G_COMPLETE_TOMBSTONE, &next),
				 PCM_X_STEP_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_RETIRE_CREDIT, PCM_X_EVENT_CANCEL_EXACT,
										   PCM_X_G_EXACT | PCM_X_G_CANCEL_TOMBSTONE, &next),
				 PCM_X_STEP_DUPLICATE);
}

UT_TEST(test_cancel_and_ownership_reservation_obey_authorization_boundary)
{
	PcmXMasterTicketState next;

	UT_ASSERT(!cluster_pcm_x_ownership_reservation_allowed(PCM_XT_ADMITTING));
	UT_ASSERT(!cluster_pcm_x_ownership_reservation_allowed(PCM_XT_QUEUED));
	UT_ASSERT(!cluster_pcm_x_ownership_reservation_allowed(PCM_XT_ACTIVE_PROBE));
	UT_ASSERT(cluster_pcm_x_ownership_reservation_allowed(PCM_XT_ACTIVE_TRANSFER));
	UT_ASSERT(!cluster_pcm_x_ownership_reservation_allowed(PCM_XT_COMPLETE));

	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_QUEUED, PCM_X_EVENT_CANCEL_EXACT,
										   PCM_X_G_EXACT | PCM_X_G_REVERSIBLE, &next),
				 PCM_X_STEP_APPLIED);
	UT_ASSERT_EQ(next, PCM_XT_CANCELLED);
	UT_ASSERT_EQ(cluster_pcm_x_master_step(PCM_XT_ACTIVE_TRANSFER, PCM_X_EVENT_CANCEL_EXACT,
										   PCM_X_G_EXACT | PCM_X_G_REVERSIBLE, &next),
				 PCM_X_STEP_APPLIED);
	UT_ASSERT_EQ(next, PCM_XT_RECOVERY_BLOCKED);
}

UT_TEST(test_retire_requires_exact_drain_and_peer_ack_without_time)
{
	PcmXMasterTicketSlot ticket;

	memset(&ticket, 0, sizeof(ticket));
	pg_atomic_init_u32(&ticket.slot.generation_change_seq, 0);
	ticket.slot.slot_generation_lo = 0;
	ticket.slot.slot_generation_hi = 0;
	pg_atomic_init_u32(&ticket.slot.state_flags, PCM_XT_COMPLETE);
	ticket.involved_nodes_bitmap = UINT32_C(0x5);
	ticket.drained_nodes_bitmap = UINT32_C(0x1);
	ticket.reliable.retry_deadline_ms = 0;
	UT_ASSERT(!cluster_pcm_x_ticket_drain_ready(&ticket));
	ticket.reliable.retry_deadline_ms = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_ticket_drain_ready(&ticket));
	ticket.drained_nodes_bitmap = ticket.involved_nodes_bitmap;
	UT_ASSERT(cluster_pcm_x_ticket_drain_ready(&ticket));

	test_set_slot_state(&ticket.slot, PCM_XT_RETIRE_CREDIT);
	ticket.retire_acked_nodes_bitmap = UINT32_C(0x1);
	UT_ASSERT(!cluster_pcm_x_ticket_retire_ready(&ticket));
	ticket.retire_acked_nodes_bitmap = ticket.involved_nodes_bitmap;
	UT_ASSERT(cluster_pcm_x_ticket_retire_ready(&ticket));
	ticket.drained_nodes_bitmap = UINT32_C(0x1);
	UT_ASSERT(!cluster_pcm_x_ticket_retire_ready(&ticket));
}

UT_TEST(test_lock_partition_is_bounded_and_stable)
{
	UT_ASSERT_EQ(cluster_pcm_x_lock_partition(0), 0);
	UT_ASSERT_EQ(cluster_pcm_x_lock_partition(PCM_X_LOCK_PARTITIONS), 0);
	UT_ASSERT_EQ(cluster_pcm_x_lock_partition(PCM_X_LOCK_PARTITIONS + 7), 7);
	UT_ASSERT(cluster_pcm_x_lock_partition(UINT32_MAX) < PCM_X_LOCK_PARTITIONS);
}

UT_TEST(test_attach_validator_rejects_magic_capacity_and_offset_mismatch)
{
	PcmXShmemHeader *header;
	PcmXShmemLayout expected;

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	compute_runtime_layout(&expected);
	UT_ASSERT_EQ(cluster_pcm_x_validate_attach(header, &expected), PCM_X_ATTACH_OK);
	header->layout.magic++;
	UT_ASSERT_EQ(cluster_pcm_x_validate_attach(header, &expected), PCM_X_ATTACH_MAGIC_MISMATCH);
	header->layout.magic = expected.magic;
	header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity++;
	UT_ASSERT_EQ(cluster_pcm_x_validate_attach(header, &expected), PCM_X_ATTACH_LAYOUT_MISMATCH);
	header->layout.pools[PCM_X_POOL_MASTER_TAG].capacity
		= expected.pools[PCM_X_POOL_MASTER_TAG].capacity;
	header->layout.master_ticket_directories.prehandle_offset++;
	UT_ASSERT_EQ(cluster_pcm_x_validate_attach(header, &expected), PCM_X_ATTACH_LAYOUT_MISMATCH);
	header->layout.master_ticket_directories.prehandle_offset
		= expected.master_ticket_directories.prehandle_offset;
	header->layout.local_holder.slots_offset++;
	UT_ASSERT_EQ(cluster_pcm_x_validate_attach(header, &expected), PCM_X_ATTACH_LAYOUT_MISMATCH);
}

UT_TEST(test_exec_backend_init_fails_closed_on_layout_mismatch)
{
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	ClusterPcmXConvertShmem->layout.version++;
	UT_EXPECT_EREPORT(cluster_pcm_x_convert_shmem_init());
	UT_ASSERT_NULL(ClusterPcmXConvertShmem);
}

UT_TEST(test_registration_exposes_one_exact_region)
{
	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_register();
	UT_ASSERT_EQ(registered_region_count, 1);
	UT_ASSERT_NOT_NULL(registered_region);
	UT_ASSERT_STR_EQ(registered_region->name, PCM_X_SHMEM_REGION_NAME);
	UT_ASSERT_STR_EQ(registered_region->owner_subsys, "pcm_x_convert");
	UT_ASSERT_EQ(registered_region->size_fn, cluster_pcm_x_convert_shmem_size);
	UT_ASSERT_EQ(registered_region->init_fn, cluster_pcm_x_convert_shmem_init);
	UT_ASSERT_EQ(registered_region->lwlock_count, PCM_X_LWLOCK_COUNT);
	UT_ASSERT_EQ(registered_region->reserved_flags, 0);
}


UT_TEST(test_stats_initialize_zero_and_narrow_note_apis_are_exact)
{
	PcmXStatsSnapshot stats;
	PcmXStatsSnapshot zero = { 0 };

	reset_fake_shmem();
	memset(&stats, 0xff, sizeof(stats));
	UT_ASSERT(!cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT(memcmp(&stats, &zero, sizeof(stats)) == 0);
	UT_ASSERT(!cluster_pcm_x_stats_snapshot(NULL));

	cluster_pcm_x_convert_shmem_init();
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT(memcmp(&stats, &zero, sizeof(stats)) == 0);

	cluster_pcm_x_stats_note_enqueue();
	cluster_pcm_x_stats_note_wait();
	cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_NO_CAPACITY);
	cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_STALE);
	cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_NOT_FOUND);
	cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_INVALID);
	cluster_pcm_x_stats_note_own_begin();
	cluster_pcm_x_stats_note_own_commit();
	cluster_pcm_x_stats_note_own_abort();
	cluster_pcm_x_stats_note_own_busy();
	cluster_pcm_x_stats_note_own_corrupt();

	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.enqueue_count, 1);
	UT_ASSERT_EQ(stats.wait_count, 1);
	UT_ASSERT_EQ(stats.full_count, 1);
	UT_ASSERT_EQ(stats.stale_count, 1);
	UT_ASSERT_EQ(stats.miss_count, 1);
	UT_ASSERT_EQ(stats.own_begin_count, 1);
	UT_ASSERT_EQ(stats.own_commit_count, 1);
	UT_ASSERT_EQ(stats.own_abort_count, 1);
	UT_ASSERT_EQ(stats.own_busy_count, 1);
	UT_ASSERT_EQ(stats.own_corrupt_count, 1);
	UT_ASSERT_EQ(stats.admit_count, 0);
	UT_ASSERT_EQ(stats.depth, 0);
	UT_ASSERT_EQ(stats.active_tags, 0);
	UT_ASSERT_EQ(stats.live_tickets, 0);
	UT_ASSERT_EQ(stats.live_slots, 0);
	UT_ASSERT_EQ(stats.local_retire_gate, 0);
	UT_ASSERT_EQ(stats.local_retire_marker_count, 0);
	UT_ASSERT_EQ(stats.local_retire_marker_ticket_id, 0);
	pg_atomic_write_u32(&ClusterPcmXConvertShmem->local_retire_gate, 2);
	ClusterPcmXConvertShmem->peer_frontiers[1].local_retire_in_progress_ticket_id = UINT64_C(8711);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.local_retire_gate, 2);
	UT_ASSERT_EQ(stats.local_retire_marker_count, 1);
	UT_ASSERT_EQ(stats.local_retire_marker_ticket_id, UINT64_C(8711));
	ClusterPcmXConvertShmem->peer_frontiers[1].local_retire_in_progress_ticket_id = 0;
	pg_atomic_write_u32(&ClusterPcmXConvertShmem->local_retire_gate, 0);
}


UT_TEST(test_stats_follow_exact_master_and_local_success_transitions)
{
	PcmXMasterAdmission admission[2];
	PcmXTicketRef active;
	PcmXTicketRef transfer;
	PcmXEnqueuePayload request[2];
	PcmXLocalHandle leader;
	PcmXLocalHandle follower;
	PcmXLocalCutoff cutoff;
	PcmXWaitIdentity leader_identity;
	PcmXWaitIdentity follower_identity;
	PcmXStatsSnapshot stats;
	int i;

	init_active_pcm_x(UINT64_C(77));
	for (i = 0; i < 2; i++) {
		request[i] = make_enqueue(make_wait_identity(760, i, (uint32)(2 + i), UINT64_C(70001) + i),
								  UINT64_C(7101) + i, 1);
		bind_enqueue_peer(&request[i]);
		UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request[i], &admission[i]), PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(
			cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(7201) + i),
			PCM_X_QUEUE_OK);
		UT_ASSERT_EQ(
			cluster_pcm_x_master_admit_confirm_exact(&admission[i].ref, UINT64_C(7201) + i),
			PCM_X_QUEUE_DUPLICATE);
	}
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.admit_count, 2);
	UT_ASSERT_EQ(stats.confirm_count, 2);
	UT_ASSERT_EQ(stats.depth, 2);
	UT_ASSERT_EQ(stats.depth_high_water, 2);
	UT_ASSERT_EQ(stats.active_tags, 1);
	UT_ASSERT_EQ(stats.live_tickets, 2);
	UT_ASSERT_EQ(stats.live_slots, 3);

	UT_ASSERT_EQ(cluster_pcm_x_master_promote_head_exact(
					 &request[0].identity.tag, request[0].identity.cluster_epoch, &active),
				 PCM_X_QUEUE_OK);
	commit_empty_blocker_graph(&active, UINT64_C(7201));
	UT_ASSERT_EQ(cluster_pcm_x_master_begin_transfer_exact(&active, UINT64_C(7201), &transfer),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(
		cluster_pcm_x_master_begin_transfer_exact(&admission[0].ref, UINT64_C(7201), &active),
		PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_complete_exact(&transfer), PCM_X_QUEUE_DUPLICATE);
	admission[0].ref = transfer;
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission[1].ref), PCM_X_QUEUE_DUPLICATE);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.promotion_count, 1);
	UT_ASSERT_EQ(stats.transfer_count, 1);
	UT_ASSERT_EQ(stats.complete_count, 1);
	UT_ASSERT_EQ(stats.cancel_count, 1);
	UT_ASSERT_EQ(stats.depth, 0);
	UT_ASSERT_EQ(stats.depth_high_water, 2);

	for (i = 0; i < 2; i++)
		drain_retire_and_detach_master(&admission[i]);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.active_tags, 0);
	UT_ASSERT_EQ(stats.live_tickets, 0);
	UT_ASSERT_EQ(stats.live_slots, 0);

	leader_identity = make_wait_identity(761, 0, 2, UINT64_C(70003));
	follower_identity = make_wait_identity(761, 0, 3, UINT64_C(70004));
	bind_local_master(2, leader_identity.cluster_epoch, UINT64_C(7401));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&leader_identity, 2, UINT64_C(7401), &leader),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&follower_identity, 2, UINT64_C(7401), &follower),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(follower.role, PCM_X_LOCAL_ROLE_FOLLOWER);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&leader, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.coalesced_count, 1);
	UT_ASSERT_EQ(stats.revoke_count, 1);
	UT_ASSERT_EQ(stats.live_slots, 3);
}


UT_TEST(test_stats_count_fail_closed_and_exact_activating_reset_once)
{
	PcmXShmemHeader *header;
	PcmXStatsSnapshot stats;
	uint32 activating_gate;

	init_active_pcm_x(UINT64_C(77));
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 9, 601), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 10, 601), PCM_X_QUEUE_STALE);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.recovery_blocked_count, 1);
	UT_ASSERT_EQ(cluster_pcm_x_peer_bind_ack_publish(0, 10, 601), PCM_X_QUEUE_NOT_READY);
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.recovery_blocked_count, 1);

	reset_fake_shmem();
	cluster_pcm_x_convert_shmem_init();
	header = ClusterPcmXConvertShmem;
	header->master_session_incarnation = 77;
	activating_gate = (UINT32_C(3) << PCM_X_RUNTIME_GATE_STATE_BITS) | UINT32_C(3);
	pg_atomic_write_u32(&header->runtime_gate, activating_gate);
	UT_ASSERT(!cluster_pcm_x_runtime_reset_activating(2));
	UT_ASSERT(cluster_pcm_x_runtime_reset_activating(3));
	UT_ASSERT(!cluster_pcm_x_runtime_reset_activating(3));
	UT_ASSERT(cluster_pcm_x_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.activating_reset_count, 1);
}


UT_TEST(test_master_admit_lock_window_error_releases_gate_and_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload request;
	PcmXSlotRef tag_ref;
	uint32 partition;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(800, 0, 2, UINT64_C(80001)), UINT64_C(8101), 1);
	bind_enqueue_peer(&request);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&request.identity.tag));
	arm_lwlock_acquire_error(&header->master_locks[partition].lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_master_admit_begin(&request, &admission));

	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_MASTER_TAG, &request.identity.tag, &tag_ref),
		PCM_X_DIRECTORY_OK);
	tag_slot = &master_tag_slots(header)[tag_ref.slot_index];
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_local_join_lock_window_error_releases_gate_and_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handle;
	PcmXLocalTagSlot *tag_slot;
	PcmXWaitIdentity identity;
	PcmXSlotRef tag_ref;
	uint32 partition;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(801, 0, 3, UINT64_C(80002));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(8102));
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity.tag));
	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION(
		(void)cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(8102), &handle));

	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &identity.tag, &tag_ref),
				 PCM_X_DIRECTORY_OK);
	tag_slot = &local_tag_slots(header)[tag_ref.slot_index];
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_master_detach_lock_window_error_releases_gate_and_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXMasterAdmission admission;
	PcmXMasterTagSlot *tag_slot;
	PcmXEnqueuePayload request;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	request = make_enqueue(make_wait_identity(802, 0, 4, UINT64_C(80003)), UINT64_C(8103), 1);
	bind_enqueue_peer(&request);
	UT_ASSERT_EQ(cluster_pcm_x_master_admit_begin(&request, &admission), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_master_cancel_exact(&admission.ref), PCM_X_QUEUE_OK);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_DRAIN,
									admission.ref.identity.node_id);
	arm_and_ack_master_terminal_leg(&admission.ref, PCM_X_TERMINAL_LEG_RETIRE,
									admission.ref.identity.node_id);
	tag_slot = &master_tag_slots(header)[admission.tag_slot.slot_index];
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 2);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_master_detach_terminal_exact(&admission.ref));

	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&tag_slot->admission_gate), 0);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_local_detach_lock_window_error_releases_gate_and_fails_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalCutoff cutoff;
	PcmXLocalHandle handle;
	PcmXLocalHandle next_leader;
	PcmXLocalTagSlot *tag_slot;
	PcmXSlotRef found;
	PcmXWaitIdentity identity;

	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(803, 0, 5, UINT64_C(80004));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(8104));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(8104), &handle),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handle, NULL), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[handle.tag_slot.slot_index];
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_detach_terminal_exact(&handle));

	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* retire_round also crosses from the local partition to the allocator
	 * while retaining the admission gate.  An exception in that handoff must
	 * leave the exact DETACHING tag and directory entry as recovery evidence. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(804, 0, 6, UINT64_C(80005));
	bind_local_master(1, identity.cluster_epoch, UINT64_C(8105));
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, UINT64_C(8105), &handle),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_begin_revoke_cutoff_exact(&handle, &cutoff), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_cancel_exact(&handle, NULL), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_detach_terminal_exact(&handle), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[handle.tag_slot.slot_index];
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_retire_round_exact(
		&identity.tag, identity.cluster_epoch, &cutoff, &next_leader));

	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &identity.tag, &found),
				 PCM_X_DIRECTORY_OK);
	UT_ASSERT(slot_refs_equal(found, handle.tag_slot));
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_local_holder_gate_handoffs_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalTagSlot *tag_slot;
	PcmXLocalMembershipSlot *holder_slot;
	PcmXLocalHolderKey holder_key;
	PcmXLocalHolderHandle holder;
	PcmXLocalHolderHandle holder_copy;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXTicketRef probe_ref;
	PcmXSlotRef tag_ref;
	PcmXSlotRef holder_ref;
	ClusterLmdVertex blocker;
	Size blocker_head;
	uint32 partition;
	const uint64 master_session = UINT64_C(8201);

	/* Holder registration publishes RESERVED tag/holder identities before the
	 * local-domain handoff.  An ERROR must release the gate but retain both
	 * exact identities under RECOVERY_BLOCKED. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	holder_key = make_local_holder_key(805, 0, 9, UINT64_C(80501), 3);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&holder_key.identity.tag));
	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_holder_register(&holder_key, &holder));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &holder_key.identity.tag, &tag_ref),
		PCM_X_DIRECTORY_OK);
	UT_ASSERT_EQ(cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_HOLDER, &holder_key, &holder_ref),
				 PCM_X_DIRECTORY_OK);
	tag_slot = &local_tag_slots(header)[tag_ref.slot_index];
	holder_slot = &membership_slots(header)[holder_ref.slot_index];
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_RESERVED_NONVISIBLE);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_X_SLOT_RESERVED_NONVISIBLE);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* Ordinary holder detach has the inverse shape: DETACHING identities are
	 * already durable when it crosses back to the allocator. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	holder_key = make_local_holder_key(806, 0, 10, UINT64_C(80601), 4);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_mark_releasing_exact(&holder), PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	holder_slot = &membership_slots(header)[holder.holder_slot.slot_index];
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_holder_unregister_exact(&holder));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_DETACHING);
	UT_ASSERT_EQ(test_slot_state(&holder_slot->slot), PCM_XL_DETACHING);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* A tag-only PROBE reservation owns the same gate before taking the local
	 * lock; cover the transfer-tag prepare path independently of registration. */
	init_active_pcm_x(UINT64_C(79));
	header = ClusterPcmXConvertShmem;
	bind_local_master(1, UINT64_C(9), master_session);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(807, 2, 11, UINT64_C(80701));
	probe_ref.handle.ticket_id = UINT64_C(8202);
	probe_ref.handle.queue_generation = UINT64_C(1);
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&probe_ref.identity.tag));
	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_probe_freeze_snapshot_exact(
		&probe_ref, 1, master_session, NULL, 0, &holder_snapshot));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(
		cluster_pcm_x_directory_find(PCM_X_DIR_LOCAL_TAG, &probe_ref.identity.tag, &tag_ref),
		PCM_X_DIRECTORY_OK);
	tag_slot = &local_tag_slots(header)[tag_ref.slot_index];
	UT_ASSERT_EQ(test_slot_state(&tag_slot->slot), PCM_X_TAG_RESERVED_NONVISIBLE);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* Existing-tag blocker publication reserves its external chain before the
	 * local lock.  Guard failure must strand that chain only under fail-closed. */
	init_active_pcm_x(UINT64_C(80));
	header = ClusterPcmXConvertShmem;
	holder_key = make_local_holder_key(808, 0, 12, UINT64_C(80801), 5);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session + 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &holder_copy, 1,
													 &holder_snapshot),
				 PCM_X_QUEUE_OK);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(808, 2, 13, UINT64_C(80802));
	probe_ref.handle.ticket_id = UINT64_C(8203);
	probe_ref.handle.queue_generation = UINT64_C(1);
	blocker = make_blocker(0, holder_key.identity.procno, UINT64_C(80803));
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&probe_ref.identity.tag));
	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_blocker_snapshot_arm_exact(
		&probe_ref, 1, master_session + 1, &holder_snapshot, &holder_copy, 1, &blocker, 1,
		&blocker_snapshot));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* ACK first makes the LIVE chain unreachable and DETACHING.  The allocator
	 * cleanup lock is therefore another mandatory fail-closed handoff. */
	init_active_pcm_x(UINT64_C(81));
	header = ClusterPcmXConvertShmem;
	holder_key = make_local_holder_key(809, 0, 14, UINT64_C(80901), 6);
	UT_ASSERT_EQ(register_active_local_holder(&holder_key, &holder), PCM_X_QUEUE_OK);
	bind_local_master(1, holder_key.identity.cluster_epoch, master_session + 2);
	UT_ASSERT_EQ(cluster_pcm_x_local_holder_snapshot(&holder_key.identity.tag, &holder_copy, 1,
													 &holder_snapshot),
				 PCM_X_QUEUE_OK);
	memset(&probe_ref, 0, sizeof(probe_ref));
	probe_ref.identity = make_wait_identity(809, 2, 15, UINT64_C(80902));
	probe_ref.handle.ticket_id = UINT64_C(8204);
	probe_ref.handle.queue_generation = UINT64_C(1);
	blocker = make_blocker(0, holder_key.identity.procno, UINT64_C(80903));
	UT_ASSERT_EQ(cluster_pcm_x_local_blocker_snapshot_arm_exact(&probe_ref, 1, master_session + 2,
																&holder_snapshot, &holder_copy, 1,
																&blocker, 1, &blocker_snapshot),
				 PCM_X_QUEUE_OK);
	tag_slot = &local_tag_slots(header)[holder.tag_slot.slot_index];
	blocker_head = tag_slot->blocker_snapshot_head_index;
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 1);
	UT_EXPECT_PG_EXCEPTION((void)cluster_pcm_x_local_blocker_ack_exact(
		&probe_ref, blocker_snapshot.set_generation, 1, master_session + 2));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(header->allocator[PCM_X_ALLOC_BLOCKER].used, 1);
	UT_ASSERT_EQ(test_slot_state(&blocker_slots(header)[blocker_head].slot), PCM_XB_DETACHING);
	UT_ASSERT_EQ(tag_slot->blocker_snapshot_head_index, PCM_X_INVALID_SLOT_INDEX);
	UT_ASSERT_EQ(tag_slot->blocker_snapshot_count, 0);
	UT_ASSERT_EQ(test_slot_flags(&tag_slot->slot) & PCM_X_LOCAL_TAG_F_ADMISSION_GATE, 0);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


UT_TEST(test_local_retire_episode_lock_errors_fail_closed)
{
	PcmXShmemHeader *header;
	PcmXLocalHandle handle;
	PcmXRetirePayload retire;
	PcmXWaitIdentity identity;
	uint32 partition;
	const uint64 master_session = UINT64_C(8301);

	/* Once the monotonic marker is published, even the read-only candidate
	 * pass is part of an ambiguous retirement episode. */
	init_active_pcm_x(UINT64_C(77));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(810, 0, 16, UINT64_C(81001));
	bind_local_master(1, identity.cluster_epoch, master_session);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session, &handle),
				 PCM_X_QUEUE_OK);
	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = identity.cluster_epoch;
	retire.master_session_incarnation = master_session;
	retire.retire_through_ticket_id = UINT64_C(8302);
	retire.sender_node = 0;
	partition = cluster_pcm_x_lock_partition(cluster_pcm_x_tag_hash(&identity.tag));
	arm_lwlock_acquire_error(&header->local_locks[partition].lock, LW_SHARED, 1);
	UT_EXPECT_PG_EXCEPTION(
		(void)cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id,
				 retire.retire_through_ticket_id);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 2);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);

	/* A retryable preflight normally clears the marker.  If that cleanup lock
	 * itself errors, the marker remains a durable ambiguity witness. */
	init_active_pcm_x(UINT64_C(78));
	header = ClusterPcmXConvertShmem;
	identity = make_wait_identity(811, 0, 17, UINT64_C(81101));
	bind_local_master(1, identity.cluster_epoch, master_session + 1);
	UT_ASSERT_EQ(cluster_pcm_x_local_join_begin(&identity, 1, master_session + 1, &handle),
				 PCM_X_QUEUE_OK);
	retire.master_session_incarnation = master_session + 1;
	retire.retire_through_ticket_id = UINT64_C(8303);
	arm_lwlock_acquire_error(&header->allocator_lock.lock, LW_EXCLUSIVE, 2);
	UT_EXPECT_PG_EXCEPTION(
		(void)cluster_pcm_x_local_retire_up_to_exact(&retire, 1, master_session + 1));
	UT_ASSERT_NULL(lwlock_acquire_error_lock);
	UT_ASSERT_EQ(held_lwlock_count, 0);
	UT_ASSERT_EQ(header->peer_frontiers[1].local_retire_in_progress_ticket_id,
				 retire.retire_through_ticket_id);
	UT_ASSERT_EQ(pg_atomic_read_u32(&header->local_retire_gate), 2);
	UT_ASSERT_EQ(cluster_pcm_x_runtime_snapshot().state, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


int
main(void)
{
	UT_PLAN(246);
	UT_RUN(test_image_id_domain_is_canonical_and_bounded);
	UT_RUN(test_wire_abi_sizes_are_exact);
	UT_RUN(test_wire_abi_offsets_are_exact);
	UT_RUN(test_runtime_layout_abi_and_offsets_are_exact);
	UT_RUN(test_lwlock_held_limit_is_shared_200);
	UT_RUN(test_default_capacity_formulas_are_exact);
	UT_RUN(test_exactly_five_pools_and_bounded_directories);
	UT_RUN(test_layout_v12_records_transfer_and_terminal_frontiers);
	UT_RUN(test_offsets_are_aligned_ordered_and_bounded);
	UT_RUN(test_membership_wait_and_holder_partitions_do_not_overlap);
	UT_RUN(test_generation_zero_advances_without_being_a_sentinel);
	UT_RUN(test_generation_max_never_wraps);
	UT_RUN(test_slot_generation_seqlock_crosses_u32_rollover);
	UT_RUN(test_slot_generation_revalidate_never_accepts_torn_pair);
	UT_RUN(test_slot_generation_writer_in_progress_is_retryable);
	UT_RUN(test_allocator_exact_release_reuses_with_next_generation);
	UT_RUN(test_allocator_generation_max_head_middle_tail_preserve_chain);
	UT_RUN(test_allocator_lost_free_head_is_corrupt);
	UT_RUN(test_allocator_generation_exhausted_overflow_is_corrupt);
	UT_RUN(test_allocator_live_free_head_is_corrupt);
	UT_RUN(test_allocator_free_slot_with_packed_flags_is_corrupt);
	UT_RUN(test_allocator_self_loop_is_corrupt);
	UT_RUN(test_allocator_invalid_next_is_corrupt);
	UT_RUN(test_allocator_truncated_free_chain_is_corrupt_before_mutation);
	UT_RUN(test_allocator_nonfree_successor_is_corrupt_before_reserve);
	UT_RUN(test_allocator_release_rejects_nonfree_old_head);
	UT_RUN(test_membership_wait_and_holder_allocators_never_borrow);
	UT_RUN(test_directory_collision_uses_exact_key_not_hash_bucket);
	UT_RUN(test_directory_backshift_preserves_wrapped_probe_chain);
	UT_RUN(test_directory_stale_generation_and_delete_ref_mismatch_fail_closed);
	UT_RUN(test_directory_full_never_overwrites_existing_entries);
	UT_RUN(test_master_ticket_prehandle_and_handle_directories_are_independent);
	UT_RUN(test_local_holder_directory_keeps_backend_identities_independent);
	UT_RUN(test_local_holder_registry_returns_canonical_exact_snapshot);
	UT_RUN(test_local_holder_stale_handle_cannot_unregister_reused_slot);
	UT_RUN(test_local_holder_capacity_failure_rolls_back_holder_only_tag);
	UT_RUN(test_local_holder_snapshot_corruption_fails_closed_before_copy);
	UT_RUN(test_local_holder_unregister_validates_before_unlinking_corrupt_tag);
	UT_RUN(test_holder_only_tag_is_adopted_by_local_queue_without_pool_borrowing);
	UT_RUN(test_holder_only_tag_survives_last_cancelled_waiter_detach);
	UT_RUN(test_local_holder_register_refuses_closed_revoke_barrier);
	UT_RUN(test_local_holder_prepare_is_visible_before_content_lock);
	UT_RUN(test_local_holder_abort_acquiring_unlinks_exact);
	UT_RUN(test_local_holder_abort_acquiring_rejects_live_content_states);
	UT_RUN(test_local_holder_abort_acquiring_loses_race_to_activate);
	UT_RUN(test_local_holder_abort_acquiring_rejects_stale_aba_handle);
	UT_RUN(test_local_holder_exceptional_detach_requires_released_content_lock);
	UT_RUN(test_local_holder_exceptional_detach_accepts_live_content_states);
	UT_RUN(test_local_holder_exceptional_detach_rejects_stale_aba_handle);
	UT_RUN(test_local_holder_cleanup_lock_errors_return_corrupt_without_rethrow);
	UT_RUN(test_local_holder_content_window_transitions_are_lock_free);
	UT_RUN(test_local_holder_finish_requires_post_content_releasing_state);
	UT_RUN(test_local_holder_gate_contention_is_retryable_without_fail_closed);
	UT_RUN(test_local_holder_barrier_counts_acquiring_active_and_releasing);
	UT_RUN(test_local_probe_freeze_is_count_first_exact_for_holder_only_tag);
	UT_RUN(test_local_queue_invalidate_authority_is_exact_and_read_only);
	UT_RUN(test_local_probe_freeze_closes_fifo_round_and_arm_consumes_reservation);
	UT_RUN(test_nested_wait_guard_snapshots_content_locks_then_checks_barriers);
	UT_RUN(test_nested_wait_guard_allows_untracked_tag_before_runtime_formation);
	UT_RUN(test_local_holder_lock_free_transitions_reject_stale_aba_handle);
	UT_RUN(test_directory_delete_then_allocator_reuse_changes_generation);
	UT_RUN(test_allocator_and_directory_operations_take_only_allocator_lock);
	UT_RUN(test_allocator_and_directory_reject_domain_lock_nesting);
	UT_RUN(test_slot_ref_revalidate_requires_exact_domain_tag_state_and_generation);
	UT_RUN(test_internal_directory_stale_ref_is_corrupt_and_fails_closed);
	UT_RUN(test_resident_exact_wire_mismatch_is_stale_without_side_effect);
	UT_RUN(test_same_lock_staged_insert_exists_is_corrupt_and_fails_closed);
	UT_RUN(test_master_composite_admission_publishes_all_indexes_and_coalesces_node);
	UT_RUN(test_master_coalesce_scan_distinguishes_legal_publish_races);
	UT_RUN(test_stale_peer_frontier_is_retryable_without_publication);
	UT_RUN(test_resident_tag_authority_mismatch_is_corrupt);
	UT_RUN(test_resident_tag_transitional_states_are_retryable);
	UT_RUN(test_peer_bind_ack_is_exact_and_mismatch_fails_closed);
	UT_RUN(test_activation_publishes_bound_frontiers_and_counters_before_active);
	UT_RUN(test_activation_rejects_partial_binding_before_claiming_gate);
	UT_RUN(test_runtime_peer_binding_exact_revalidation_is_read_only);
	UT_RUN(test_runtime_peer_binding_epoch_drift_fails_closed);
	UT_RUN(test_runtime_peer_binding_session_drift_fails_closed);
	UT_RUN(test_runtime_peer_binding_empty_frontier_is_never_late_bound);
	UT_RUN(test_runtime_peer_binding_invalid_node_is_rejected_without_closing_runtime);
	UT_RUN(test_composite_admission_rejects_zero_wrapping_and_out_of_range_identity);
	UT_RUN(test_master_composite_capacity_failure_rolls_back_tag_indexes_and_id);
	UT_RUN(test_master_ticket_and_admission_sequence_exhaustion_leave_no_holes);
	UT_RUN(test_missing_live_prehandle_inside_frontier_is_corrupt_not_phantom);
	UT_RUN(test_master_confirmed_queue_is_node_fifo_with_one_active);
	UT_RUN(test_master_pending_x_claim_is_active_ticket_exact);
	UT_RUN(test_master_pending_x_claim_state_classifies_cancel_progress_as_not_ready);
	UT_RUN(test_master_pending_x_cancel_release_is_two_phase_and_replay_exact);
	UT_RUN(test_master_pending_x_cancel_before_first_probe_releases_immediately);
	UT_RUN(test_master_cancel_ack_snapshot_distinguishes_prehandle_and_confirmed_cancel);
	UT_RUN(test_master_pending_x_cancel_prepare_replay_validates_unlinked_tombstone);
	UT_RUN(test_master_pending_x_cancel_intent_survives_partial_probe_and_nonempty_graph);
	UT_RUN(test_master_pending_x_cancel_loses_cleanly_to_active_transfer);
	UT_RUN(test_master_wfg_snapshot_head_has_no_blockers_and_confirms_exactly);
	UT_RUN(test_master_wfg_snapshot_second_ticket_names_true_predecessor);
	UT_RUN(test_master_wfg_snapshot_deduplicates_active_predecessor_and_keeps_distinct_pair);
	UT_RUN(test_master_wfg_structural_mutation_invalidates_snapshot_before_confirm);
	UT_RUN(test_master_wfg_snapshot_and_confirm_reject_stale_exact_identity);
	UT_RUN(test_master_success_transfer_completes_fifo_and_promotes_every_node);
	UT_RUN(test_master_blocker_replace_canonical_snapshot_and_revalidate);
	UT_RUN(test_master_blocker_replace_grow_shrink_empty_reclaims);
	UT_RUN(test_master_blocker_capacity_failure_is_byte_stable);
	UT_RUN(test_master_blocker_generation_and_duplicate_conflicts_fail_closed);
	UT_RUN(test_master_blocker_snapshot_detects_slot_aba_and_corrupt_link);
	UT_RUN(test_master_blocker_snapshot_validates_every_owner_field);
	UT_RUN(test_master_blocker_sets_are_isolated_across_tickets);
	UT_RUN(test_master_blocker_wire_stage_commits_exact_canonical_set);
	UT_RUN(test_master_blocker_generation_is_scoped_by_holder_source);
	UT_RUN(test_master_blocker_crc_ignores_vertex_abi_padding);
	UT_RUN(test_master_blocker_wire_stage_crc_rejects_content_mismatch);
	UT_RUN(test_master_blocker_stage_abort_reclaims_mixed_chain_and_fences_aba);
	UT_RUN(test_master_blocker_probe_arm_is_zero_generation_and_idempotent);
	UT_RUN(test_master_drive_work_cursor_is_bounded_and_exact);
	UT_RUN(test_master_driver_waits_for_terminal_active_ticket_without_fail_closed);
	UT_RUN(test_master_drive_snapshot_and_holder_bitmaps_are_exact);
	UT_RUN(test_master_drive_bitmap_replace_rejects_reliable_leg_drift);
	UT_RUN(test_master_drive_snapshot_fails_closed_on_bitmap_corruption);
	UT_RUN(test_master_transfer_wire_49_56_is_generation_exact);
	UT_RUN(test_master_grant_generation_exhaustion_never_wraps);
	UT_RUN(test_master_image_id_allocator_encodes_node31_and_never_wraps);
	UT_RUN(test_master_blocker_wire_stage_preserves_old_set_until_commit);
	UT_RUN(test_master_blocker_wire_stage_rejects_reorder_conflict_and_wrong_source);
	UT_RUN(test_master_blocker_wire_stage_fences_transfer_and_cancel_until_commit);
	UT_RUN(test_master_blocker_graph_commit_revalidates_exact_snapshot_and_generation);
	UT_RUN(test_master_transfer_requires_committed_exact_empty_blocker_set);
	UT_RUN(test_master_active_probe_cancel_requires_committed_exact_empty_blocker_set);
	UT_RUN(test_master_locator_defers_mutable_grant_exactness_to_domain_lock);
	UT_RUN(test_admission_replay_and_confirm_stay_frozen_after_transfer);
	UT_RUN(test_master_application_replays_survive_terminal_successors);
	UT_RUN(test_delayed_pretransfer_cancel_fences_active_transfer);
	UT_RUN(test_recovery_blocked_runtime_cannot_promote_or_begin_transfer);
	UT_RUN(test_recovery_blocked_runtime_refuses_ack_and_local_mutators);
	UT_RUN(test_master_cancel_is_exact_and_unlinks_middle_without_fifo_damage);
	UT_RUN(test_master_prehandle_cancel_replays_exactly_and_never_hits_reused_slot);
	UT_RUN(test_master_prehandle_identity_alias_is_corruption_not_stale_cancel);
	UT_RUN(test_master_prehandle_cancel_first_publishes_terminal_tombstone_before_late_enqueue);
	UT_RUN(test_master_cancel_rejects_active_probe_without_mutation);
	UT_RUN(test_master_unlink_corruption_is_byte_stable_before_fail_closed);
	UT_RUN(test_master_cancel_requires_state_exact_active_locator);
	UT_RUN(test_terminal_ack_requires_prearmed_leg_and_is_byte_stable);
	UT_RUN(test_terminal_ack_wire_fields_are_exact_and_zero_side_effect);
	UT_RUN(test_retire_ack_uses_persistent_exact_key_index);
	UT_RUN(test_early_retire_arm_does_not_publish_locator);
	UT_RUN(test_terminal_outcome_mask_corruption_is_fail_closed);
	UT_RUN(test_terminal_bitmap_superset_is_fail_closed_before_arm_or_ack_mutation);
	UT_RUN(test_terminal_detach_missing_retire_ack_is_retryable_not_ready);
	UT_RUN(test_terminal_retry_skips_drained_responder_while_next_leg_is_armed);
	UT_RUN(test_terminal_detach_rejects_pending_leg);
	UT_RUN(test_master_terminal_detach_rejects_hot_link_and_drain_corruption);
	UT_RUN(test_master_terminal_detach_preserves_same_node_successor);
	UT_RUN(test_master_tag_survives_until_every_terminal_ticket_detaches);
	UT_RUN(test_last_terminal_detach_waits_for_staged_admission);
	UT_RUN(test_master_outstanding_counter_overflow_and_rollback_are_exact);
	UT_RUN(test_master_detach_second_caller_is_stale_not_fail_closed);
	UT_RUN(test_local_composite_join_publishes_one_leader_and_ordered_followers);
	UT_RUN(test_local_follower_wfg_publish_is_nonwaitable_then_exact);
	UT_RUN(test_local_writer_claim_runs_closed_cohort_and_blocks_next_round);
	UT_RUN(test_local_writer_claim_completion_is_fifo_and_one_shot);
	UT_RUN(test_writer_and_holder_owner_exit_retry_preserves_exact_evidence);
	UT_RUN(test_local_wfg_rejects_completed_blocker_semantic_aba);
	UT_RUN(test_local_cancel_never_unlinks_an_active_writer);
	UT_RUN(test_local_closed_round_never_promotes_a_late_joiner_early);
	UT_RUN(test_local_lookup_is_read_only_and_identity_exact);
	UT_RUN(test_local_progress_is_exact_and_exposes_remote_wait_ledger);
	UT_RUN(test_local_transfer_prepare_commit_and_final_ack_are_exact);
	UT_RUN(test_local_tag_only_holder_transfer_persists_until_exact_drain);
	UT_RUN(test_local_non_source_blocker_participant_drains_and_retires_exactly);
	UT_RUN(test_local_cancelled_non_source_participant_gen0_drains_and_retires_exactly);
	UT_RUN(test_local_cancelled_participant_gen0_drain_requires_frozen_round);
	UT_RUN(test_local_holder_drain_validates_frozen_round_before_terminal_publish);
	UT_RUN(test_local_same_ref_non_source_participant_retires_both_legs);
	UT_RUN(test_local_successful_retire_clears_round_image_before_promoting_successor);
	UT_RUN(test_local_same_ref_cancelled_participant_gen0_retires_both_legs);
	UT_RUN(test_local_final_closed_member_retire_promotes_late_round);
	UT_RUN(test_local_cancelled_dual_retires_one_writer_from_multiwriter_round);
	UT_RUN(test_local_cancelled_dual_retire_validates_before_consuming_holder_evidence);
	UT_RUN(test_local_source_holder_rejects_parallel_blocker_terminal_lane);
	UT_RUN(test_local_retire_claim_serializes_admission_handoffs);
	UT_RUN(test_local_retire_global_gate_blocks_direct_mutators);
	UT_RUN(test_local_retire_global_gate_rejects_orphan_and_mismatched_evidence);
	UT_RUN(test_local_retire_marker_blocks_cross_lock_tag_handoffs);
	UT_RUN(test_local_cross_lock_gate_precedes_retire_claim);
	UT_RUN(test_local_new_transfer_abort_keeps_gate_until_allocator_cleanup);
	UT_RUN(test_local_retire_requires_exact_holder_and_blocker_evidence);
	UT_RUN(test_local_blocker_allocator_corruption_retains_new_tag_gate_evidence);
	UT_RUN(test_local_blocker_snapshot_replays_exact_holder_set_and_gates_revoke);
	UT_RUN(test_local_blocker_snapshot_exact_lookup_replays_when_pool_is_full);
	UT_RUN(test_local_blocker_snapshot_accepts_only_holder_bound_nested_waits);
	UT_RUN(test_local_duplicate_generation_overlap_is_stale_not_fail_closed);
	UT_RUN(test_local_join_capacity_and_sequence_failure_roll_back_all_indexes);
	UT_RUN(test_local_unlink_corruption_preserves_fifo_evidence_before_fail_closed);
	UT_RUN(test_local_cancel_requires_state_exact_leader_locator);
	UT_RUN(test_local_cutoff_preserves_cancelled_tail_sequence);
	UT_RUN(test_local_revoke_cutoff_sends_late_writer_to_next_round);
	UT_RUN(test_local_retire_rejects_candidate_from_wrong_round);
	UT_RUN(test_local_cancel_rejects_waitable_successor_from_wrong_round);
	UT_RUN(test_local_retire_rejects_closed_head_and_active_generation_corruption);
	UT_RUN(test_local_retire_duplicate_revalidates_leader_tag_linkage);
	UT_RUN(test_local_cancel_is_exact_handoffs_leader_and_terminal_detach_restores_baseline);
	UT_RUN(test_local_promoted_leader_identity_generation_rekey_is_exact);
	UT_RUN(test_local_promoted_leader_rekey_rejects_live_evidence_and_key_alias);
	UT_RUN(test_local_promoted_leader_rekey_allocator_handoff_error_releases_gate);
	UT_RUN(test_local_rekey_staged_target_directory_is_retryable_busy);
	UT_RUN(test_local_rekey_third_local_acquire_error_retains_gate);
	UT_RUN(test_local_rekey_requires_canonical_fifo_leader);
	UT_RUN(test_local_rekey_target_insert_failure_restores_old_key);
	UT_RUN(test_last_writer_detach_waits_for_independent_holder_transfer_before_tag_release);
	UT_RUN(test_fifo_writer_drain_preserves_concurrent_holder_lane_until_transfer_drain);
	UT_RUN(test_local_terminal_detach_rejects_hot_link_and_closed_count_corruption);
	UT_RUN(test_local_enqueue_arm_persists_ledger_and_mints_without_holes);
	UT_RUN(test_local_enqueue_busy_and_counter_exhaustion_leave_no_hole);
	UT_RUN(test_local_admit_ack_retry_and_confirm_are_exact);
	UT_RUN(test_local_generic_reliable_ack_requires_exact_token_and_keys);
	UT_RUN(test_local_pending_enqueue_cancel_keeps_ledger_until_exact_ack_then_rotates_leader);
	UT_RUN(test_local_remote_wait_cancel_is_exact_retriable_and_rotates_only_on_ack);
	UT_RUN(test_local_exact_prepare_wins_cancel_vs_transfer_race);
	UT_RUN(test_local_terminal_drain_retire_fences_successor_and_replays_watermark);
	UT_RUN(test_local_reliable_authority_change_fails_closed_without_dropping_ledger);
	UT_RUN(test_capacity_addition_overflow_is_checked);
	UT_RUN(test_capacity_multiplication_overflow_is_checked);
	UT_RUN(test_ticket_directory_capacity_doubling_overflow_is_checked);
	UT_RUN(test_fresh_init_builds_independent_free_lists);
	UT_RUN(test_fresh_init_is_recovery_blocked_and_initializes_all_locks_once);
	UT_RUN(test_fresh_init_sets_every_slot_free_at_generation_zero);
	UT_RUN(test_exec_backend_attach_preserves_mutable_state);
	UT_RUN(test_runtime_gate_is_atomic_and_session_fenced);
	UT_RUN(test_runtime_activating_phase_has_exact_recovery_reset);
	UT_RUN(test_runtime_gate_generation_prevents_active_aba);
	UT_RUN(test_runtime_gate_generation_exhaustion_never_wraps);
	UT_RUN(test_runtime_gate_generation_exhaustion_fails_closed);
	UT_RUN(test_master_state_machine_requires_exact_positive_gates);
	UT_RUN(test_cancel_and_ownership_reservation_obey_authorization_boundary);
	UT_RUN(test_retire_requires_exact_drain_and_peer_ack_without_time);
	UT_RUN(test_lock_partition_is_bounded_and_stable);
	UT_RUN(test_attach_validator_rejects_magic_capacity_and_offset_mismatch);
	UT_RUN(test_exec_backend_init_fails_closed_on_layout_mismatch);
	UT_RUN(test_registration_exposes_one_exact_region);
	UT_RUN(test_stats_initialize_zero_and_narrow_note_apis_are_exact);
	UT_RUN(test_stats_follow_exact_master_and_local_success_transitions);
	UT_RUN(test_stats_count_fail_closed_and_exact_activating_reset_once);
	UT_RUN(test_master_admit_lock_window_error_releases_gate_and_fails_closed);
	UT_RUN(test_local_join_lock_window_error_releases_gate_and_fails_closed);
	UT_RUN(test_master_detach_lock_window_error_releases_gate_and_fails_closed);
	UT_RUN(test_local_detach_lock_window_error_releases_gate_and_fails_closed);
	UT_RUN(test_local_holder_gate_handoffs_fail_closed);
	UT_RUN(test_local_retire_episode_lock_errors_fail_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
