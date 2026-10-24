/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the native extension allocation prefix; only external buffer,
 * GES, identity-read and durable-grant boundaries are fixtures. */
#include "postgres.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/cluster_pcm_direct_init.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/storage/cluster_smgr.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
bool cluster_shared_config, cluster_relation_extend_lock_enabled = true;
bool cluster_enabled = true;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
static bool owner_expected, owner_live, owner_armed, owner_end_ok = true;
static bool owner_wait_retire, owner_invalid_admission;
static void owner_retire_handle(void);
static unsigned owner_enters, owner_ends, owner_refusals, owner_waits, owner_materialized,
	owner_aborts;

bool
cluster_ko_extend_begin_v3(RelFileLocator locator, ForkNumber fork, uint64 *serial, bool *retry)
{
	UT_ASSERT(!owner_live);
	owner_enters++;
	*serial = 0;
	*retry = false;
	if (owner_invalid_admission)
		return false;
	if (owner_refusals != 0) {
		*retry = true;
		owner_refusals--;
		return false;
	}
	owner_live = true;
	*serial = 42;
	return true;
}
bool
cluster_ko_extend_active_v3(void)
{
	return owner_live;
}
bool
cluster_ko_extend_write_begin_v3(RelFileLocator locator, ForkNumber fork)
{
	UT_ASSERT(owner_live);
	owner_armed = true;
	return true;
}
bool
cluster_ko_extend_write_done_v3(RelFileLocator locator, ForkNumber fork)
{
	UT_ASSERT(owner_live && owner_armed);
	owner_materialized++;
	return true;
}
void
cluster_ko_extend_abort_v3(uint64 serial)
{
	UT_ASSERT_EQ(serial, 42);
	UT_ASSERT(owner_live);
	owner_aborts++;
}
bool
cluster_ko_extend_end_v3(uint64 serial)
{
	UT_ASSERT_EQ(serial, 42);
	UT_ASSERT(owner_live);
	owner_live = false;
	owner_ends++;
	return owner_end_ok;
}
static void
cluster_bufmgr_ko_read_wait(void)
{
	UT_ASSERT(!owner_live);
	owner_waits++;
	if (owner_invalid_admission)
		pg_re_throw();
	if (owner_wait_retire)
		owner_retire_handle();
}

int cluster_node_id, cluster_space_lease_blocks = 8;
static RelationData relation;
static FormData_pg_class form;
static SMgrRelationData storage, reopened;
static ClusterSpaceIdentity identity;
static PGAlignedBlock pages[2];
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static bool hw_held, retired, missing_identity, refuse_hw, refuse_grant, throw_grant;
static unsigned victims, unpins, identity_reads, reservations, legacy_allocations, size_reads;
static bool local_lock;
volatile uint32 InterruptHoldoffCount;

static void
owner_retire_handle(void)
{
	retired = true;
	relation.rd_smgr = NULL;
	memset(&storage, 0xa5, sizeof(storage));
}


void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool
RecoveryInProgress(void)
{
	return false;
}
ClusterExtendEngage
cluster_extend_liveness_engage(bool wait)
{
	return CLUSTER_EXTEND_ENGAGE_NATIVE;
}
bool
cluster_hw_lease_active(void)
{
	return false;
}
int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	return 1;
}
SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(tag, identity.key.locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return retired ? &reopened : &storage;
}
void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	*owner = rel;
}
BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(rel == (retired ? &reopened : &storage));
	size_reads++;
	return 9;
}
bool
cluster_space_relation_read_identity(RelFileLocator tag, ClusterSpaceIdentity *out)
{
	UT_ASSERT(!hw_held && !local_lock && victims == 0);
	UT_ASSERT(RelFileLocatorEquals(tag, identity.key.locator));
	if (owner_expected)
		UT_ASSERT(owner_live);
	identity_reads++;
	if (missing_identity)
		return false;
	*out = identity;
	retired = true;
	relation.rd_smgr = NULL;
	memset(&storage, 0xa5, sizeof(storage));
	return true;
}
bool
cluster_space_relation_get_identity(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT(rel == &relation);
	return cluster_space_relation_read_identity(rel->rd_locator, out);
}
bool
cluster_hw_lock(const ClusterResId *resid, HwLock *lock)
{
	ClusterResId expected;

	cluster_hw_resid_encode(identity.key.locator, MAIN_FORKNUM, &expected);
	UT_ASSERT(memcmp(resid, &expected, sizeof(expected)) == 0);
	UT_ASSERT(!hw_held && !local_lock);
	memset(lock, 0, sizeof(*lock));
	if (refuse_hw)
		return false;
	lock->held = lock->coordinated = hw_held = true;
	return true;
}
void
cluster_hw_unlock(HwLock *lock)
{
	UT_ASSERT(hw_held && lock->held);
	lock->held = hw_held = false;
}
BlockNumber
cluster_space_reserve(const ClusterSpaceIdentity *id, const HwLock *lock, uint32 want,
					  uint32 *granted)
{
	UT_ASSERT(hw_held && lock->held && id->sequence == identity.sequence);
	if (owner_expected)
		UT_ASSERT(owner_live && owner_armed);
	UT_ASSERT_EQ(want, 2);
	UT_ASSERT_EQ(size_reads, 0);
	if (throw_grant)
		pg_re_throw();
	if (refuse_grant) {
		*granted = 0;
		return InvalidBlockNumber;
	}
	reservations++;
	*granted = want;
	return 200;
}
BlockNumber
cluster_hw_allocate(RelFileLocator tag, ForkNumber fork, uint32 want, BlockNumber seed,
					uint32 *granted)
{
	legacy_allocations++;
	*granted = want;
	return 500;
}
static Buffer
fixture_victim(void)
{
	return ++victims;
}
static void
fixture_unlock(void)
{
	UT_ASSERT(local_lock);
	local_lock = false;
}
#define IOContextForStrategy(strategy) IOCONTEXT_NORMAL
#define LimitAdditionalPins(count) ((void)0)
#define GetVictimBuffer(strategy, context) fixture_victim()
#undef BufHdrGetBlock
#define BufHdrGetBlock(desc) ((Block)pages[(desc)->buf_id].data)
#define ResourceOwnerEnlargeBuffers(owner) ((void)0)
#define StrategyFreeBuffer(desc) ((void)0)
#define UnpinBuffer(desc) (unpins++)
#define LockRelationForExtension(rel, mode) (local_lock = true)
#define UnlockRelationForExtension(rel, mode) fixture_unlock()
#undef elog
#undef ereport
#define elog(...) pg_re_throw()
#define ereport(...) pg_re_throw()
#include "test_cluster_space_extend.inc"

static void
reset(void)
{
	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	memset(&storage, 0, sizeof(storage));
	memset(&identity, 0, sizeof(identity));
	identity.key.locator = (RelFileLocator){ 1663, 5, 17000 };
	identity.sequence = 1;
	relation.rd_locator = storage.smgr_rlocator.locator = identity.key.locator;
	storage.smgr_rlocator.backend = InvalidBackendId;
	reopened = storage;
	relation.rd_smgr = &storage;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	form.relkind = RELKIND_RELATION;
	for (int i = 0; i < 2; i++)
		descriptors[i].bufferdesc.buf_id = i;
	cluster_shared_config = true;
	owner_expected = owner_live = owner_armed = owner_wait_retire = owner_invalid_admission = false;
	owner_end_ok = true;
	owner_enters = owner_ends = owner_refusals = owner_waits = owner_materialized = owner_aborts
		= 0;
	hw_held = local_lock = retired = missing_identity = refuse_hw = refuse_grant = throw_grant
		= false;
	victims = unpins = identity_reads = reservations = legacy_allocations = size_reads = 0;
}

static BlockNumber
extend(bool private, ForkNumber fork)
{
	Buffer buffers[2];
	uint32 extended;
	BufferManagerRelation bmr = { .rel = private ? NULL : &relation,
								  .smgr = &storage,
								  .relpersistence = RELPERSISTENCE_PERMANENT };

	return ExtendBufferedRelShared(bmr, fork, NULL, EB_SKIP_EXTENSION_LOCK, 2, InvalidBlockNumber,
								   buffers, &extended, NULL, NULL);
}

static BlockNumber
extend_preheld_identity(ForkNumber fork, const HwLock *lock,
						const ClusterSpaceIdentity *captured)
{
	Buffer buffers[2];
	uint32 extended;
	BufferManagerRelation bmr = { .rel = &relation, .smgr = &storage,
								  .relpersistence = RELPERSISTENCE_PERMANENT };

	return ExtendBufferedRelShared(bmr, fork, NULL, EB_SKIP_EXTENSION_LOCK, 2,
								   InvalidBlockNumber, buffers, &extended, lock, captured);
}

static BlockNumber
extend_preheld(ForkNumber fork, const HwLock *lock)
{
	ClusterSpaceIdentity captured = identity;

	return extend_preheld_identity(fork, lock, &captured);
}

static HwLock
preheld_hw(void)
{
	HwLock lock = { 0 };

	lock.held = lock.coordinated = hw_held = true;
	form.relkind = RELKIND_INDEX;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	lock.req.lockmode = ExclusiveLock;
	lock.req.request_id = 123;
	cluster_hw_resid_encode(identity.key.locator, MAIN_FORKNUM, &lock.req.resid);
	return lock;
}

UT_TEST(test_preheld_hw_is_not_reacquired_or_released_by_extension)
{
	HwLock lock, before;

	reset();
	owner_expected = true;
	lock = preheld_hw();
	memcpy(&before, &lock, sizeof(before));
	UT_ASSERT_EQ(extend_preheld(MAIN_FORKNUM, &lock), 200);
	UT_ASSERT(hw_held);
	UT_ASSERT(memcmp(&lock, &before, sizeof(lock)) == 0);
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(identity_reads, 0); /* Identity was captured before HW. */
	UT_ASSERT_EQ(owner_enters, 1);
	UT_ASSERT_EQ(owner_ends, 1);
	cluster_hw_unlock(&lock); /* Only the enclosing owner releases HW. */
	UT_ASSERT(!hw_held);
}

UT_TEST(test_preheld_hw_survives_extension_error_for_outer_unwind)
{
	HwLock lock, before;
	volatile bool caught = false;

	reset();
	owner_expected = true;
	lock = preheld_hw();
	memcpy(&before, &lock, sizeof(before));
	throw_grant = true;
	PG_TRY();
	{
		(void)extend_preheld(MAIN_FORKNUM, &lock);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(hw_held);
	UT_ASSERT(memcmp(&lock, &before, sizeof(lock)) == 0);
	UT_ASSERT_EQ(owner_aborts, 1);
	cluster_hw_unlock(&lock);
}

UT_TEST(test_preheld_hw_rejects_invalid_handle_and_noncanonical_scope)
{
	for (int variant = 0; variant < 10; variant++) {
		HwLock lock, before;
		volatile bool caught = false;
		ForkNumber fork = MAIN_FORKNUM;
		const ClusterSpaceIdentity *captured = &identity;

		reset();
		lock = preheld_hw();
		if (variant == 0)
			lock.held = false;
		else if (variant == 1)
			lock.coordinated = false;
		else if (variant == 2)
			lock.req.lockmode = ShareLock;
		else if (variant == 3) {
			RelFileLocator other = identity.key.locator;
			other.relNumber++;
			cluster_hw_resid_encode(other, MAIN_FORKNUM, &lock.req.resid);
		}
		else if (variant == 4)
			fork = FSM_FORKNUM;
		else if (variant == 5)
			cluster_shared_config = false;
		else if (variant == 6)
			form.relkind = RELKIND_RELATION;
		else if (variant == 7)
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		else if (variant == 8)
			identity.key.locator.relNumber++;
		else
			captured = NULL;
		memcpy(&before, &lock, sizeof(before));
		PG_TRY();
		{
			(void)extend_preheld_identity(fork, &lock, captured);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(hw_held);
		UT_ASSERT(memcmp(&lock, &before, sizeof(lock)) == 0);
		UT_ASSERT_EQ(victims + reservations + legacy_allocations + size_reads, 0);
	}
}

UT_TEST(test_preheld_hw_reservation_refusal_has_no_native_fallback)
{
	HwLock lock;
	volatile bool caught = false;

	reset();
	lock = preheld_hw();
	refuse_grant = true;
	PG_TRY();
	{
		(void)extend_preheld(MAIN_FORKNUM, &lock);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(hw_held && lock.held);
	UT_ASSERT_EQ(size_reads + legacy_allocations + identity_reads, 0);
	UT_ASSERT_EQ(victims, unpins);
	cluster_hw_unlock(&lock);
}

UT_TEST(test_single_live_and_private_callers_use_canonical_reservations)
{
	for (int variant = 0; variant < 2; variant++) {
		reset();
		UT_ASSERT_EQ(extend(variant != 0, MAIN_FORKNUM), 200);
		UT_ASSERT_EQ(identity_reads, 1);
		UT_ASSERT_EQ(reservations, 1);
		UT_ASSERT_EQ(size_reads + legacy_allocations, 0);
		UT_ASSERT(!hw_held);
	}
}
UT_TEST(test_nonshared_and_auxiliary_keep_native_extension)
{
	for (int variant = 0; variant < 2; variant++) {
		reset();
		if (variant == 0)
			cluster_shared_config = false;
		UT_ASSERT_EQ(extend(false, variant ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM), 9);
		UT_ASSERT_EQ(identity_reads + reservations + legacy_allocations, 0);
		UT_ASSERT_EQ(size_reads, 1);
	}
}
UT_TEST(test_refusals_do_not_fall_back_to_file_size)
{
	for (int variant = 0; variant < 3; variant++) {
		volatile bool caught = false;

		reset();
		missing_identity = variant == 0;
		refuse_hw = variant == 1;
		refuse_grant = variant == 2;
		PG_TRY();
		{
			(void)extend(true, MAIN_FORKNUM);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(reservations + size_reads + legacy_allocations, 0);
		UT_ASSERT_EQ(victims, unpins);
		UT_ASSERT(!hw_held);
	}
}
UT_TEST(test_reservation_error_releases_hw)
{
	volatile bool caught = false;

	reset();
	throw_grant = true;
	PG_TRY();
	{
		(void)extend(false, MAIN_FORKNUM);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!hw_held);
	UT_ASSERT_EQ(reservations + size_reads + legacy_allocations, 0);
}

/* Execute the native post-extension loop too. Transport and kernel I/O
 * remain boundaries; the ERROR lands after native content-X acquisition. */
typedef struct ClusterPcmXWriterLedgerEntry {
	bool present;
} ClusterPcmXWriterLedgerEntry;
static ClusterPcmXWriterLedgerEntry init_ledgers[2];
static unsigned init_locks, init_completed;
static int init_fail_buffer, init_fail_stage;

static void
init_fault(BufferDesc *buf, int stage)
{
	if (buf->buf_id == init_fail_buffer && stage == init_fail_stage) {
		InterruptHoldoffCount = 0; /* PostgreSQL ERROR unwinding */
		pg_re_throw();
	}
}
static bool
cluster_bufmgr_pcm_arm_direct_init(BufferDesc *buf, ClusterPcmDirectInitKind kind,
								   ClusterPcmDirectInitProof *proof, void *context)
{
	UT_ASSERT_EQ(kind, CLUSTER_PCM_DIRECT_INIT_EXTEND);
	return true;
}
static bool
cluster_bufmgr_pcm_gate_direct_init(BufferDesc *buf, ClusterPcmDirectInitKind kind,
									const ClusterPcmDirectInitProof *proof, void *context)
{
	init_ledgers[buf->buf_id].present = true;
	init_fault(buf, 1);
	return true;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			UT_ASSERT(!(init_locks & (1 << i)));
			UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
			init_locks |= 1 << i;
			InterruptHoldoffCount++;
			return true;
		}
	UT_ASSERT(false);
	return false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc))
			return (init_locks & (1 << i)) != 0;
	return false;
}
void
LWLockRelease(LWLock *lock)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			UT_ASSERT(init_locks & (1 << i));
			UT_ASSERT(InterruptHoldoffCount > 0);
			init_locks &= ~(1 << i);
			InterruptHoldoffCount--;
			return;
		}
	UT_ASSERT(false);
}
static void
cluster_bufmgr_pcm_x_writer_activate_target_direct_init(BufferDesc *buf)
{
	UT_ASSERT(init_locks & (1 << buf->buf_id));
	init_fault(buf, 2);
}
static ClusterPcmXWriterLedgerEntry *
pg_attribute_unused() cluster_bufmgr_pcm_x_writer_find(BufferDesc *buf)
{
	return init_ledgers[buf->buf_id].present ? &init_ledgers[buf->buf_id] : NULL;
}
static void
pg_attribute_unused() cluster_bufmgr_pcm_x_writer_clear(ClusterPcmXWriterLedgerEntry *entry)
{
	if (entry != NULL)
		entry->present = false;
}
static void
TerminateBufferIO(BufferDesc *buf, bool dirty, uint32 flags)
{
	UT_ASSERT(!dirty && flags == BM_VALID);
	init_completed++;
}
static void
finish_extension(uint32 flags, uint32 extend_by)
{
	Buffer buffers[2] = { 1, 2 };
	BlockNumber first_block = 0, extend_upto = extend_by;
#include "test_cluster_space_extend_finish.inc"
}
UT_TEST(test_extension_returns_initialized_content_locks)
{
	reset();
	init_locks = init_completed = InterruptHoldoffCount = 0;
	memset(init_ledgers, 0, sizeof(init_ledgers));
	init_fail_buffer = -1;
	finish_extension(EB_LOCK_FIRST | EB_LOCK_TARGET, 2);
	UT_ASSERT_EQ(init_completed, 2);
	UT_ASSERT_EQ(init_locks, 3);
	UT_ASSERT_EQ(InterruptHoldoffCount, 2);
	UT_ASSERT(init_ledgers[0].present && init_ledgers[1].present);
}
UT_TEST(test_extension_error_releases_current_and_earlier_content_locks)
{
	for (int buffer = 0; buffer < 2; buffer++)
		for (int stage = 1; stage <= 2; stage++) {
			volatile bool returned = false, caught = false;
			reset();
			init_locks = init_completed = InterruptHoldoffCount = 0;
			memset(init_ledgers, 0, sizeof(init_ledgers));
			init_fail_buffer = buffer;
			init_fail_stage = stage;
			PG_TRY();
			{
				finish_extension(EB_LOCK_FIRST | EB_LOCK_TARGET, 2);
				returned = true;
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT(caught && !returned);
			UT_ASSERT_EQ(init_completed, buffer);
			UT_ASSERT_EQ(init_locks, 0);
			UT_ASSERT_EQ(InterruptHoldoffCount, 0);
			UT_ASSERT(!init_ledgers[0].present && !init_ledgers[1].present);
		}
}
static unsigned zero_calls, zero_blocks, zero_fail_at;
void
smgrzeroextend(SMgrRelation rel, ForkNumber fork, BlockNumber first, int count, bool skipFsync)
{
	UT_ASSERT(owner_live && owner_armed);
	UT_ASSERT_EQ(first, 200 + zero_blocks);
	zero_calls++;
	if (zero_fail_at == zero_calls)
		pg_re_throw();
	zero_blocks += count;
}
void
cluster_hw_lease_install(RelFileLocator locator, ForkNumber fork, BlockNumber first, uint32 count)
{
	UT_ASSERT_EQ(first + count, 200 + zero_blocks);
}

/* Original post-reservation resize block; allocation and IO are explicit
 * boundaries. A five-block durable grant may not disappear on extend-to's
 * already-satisfied branch. */
static BlockNumber
resize_reserved_range(BlockNumber extend_upto, uint32 *extended_by, bool ko_scoped)
{
	BufferManagerRelation bmr = { .rel = &relation, .smgr = &reopened };
	ForkNumber fork = MAIN_FORKNUM;
	uint32 flags = EB_SKIP_EXTENSION_LOCK, extend_by = 2;
	uint32 hw_lease_tail = 3;
	BlockNumber first_block = 200;
	Buffer buffers[2] = { 1, 2 };
	ClusterHwClass hwc = CLUSTER_HW_GLOBALIZE;

#include "test_cluster_space_extend_resize.inc"
	*extended_by = extend_by;
	return hw_lease_tail;
}

UT_TEST(test_original_resize_keeps_entire_durable_grant_materialized)
{
	uint32 extended;
	reset();
	owner_live = owner_armed = true;
	zero_calls = zero_blocks = 0;
	UT_ASSERT_EQ(resize_reserved_range(199, &extended, true), 200);
	UT_ASSERT_EQ(extended, 0);
	UT_ASSERT_EQ(zero_calls, 1);
	UT_ASSERT_EQ(zero_blocks, 5);
	reset();
	owner_live = owner_armed = true;
	UT_ASSERT_EQ(resize_reserved_range(201, &extended, true), 4);
	UT_ASSERT_EQ(extended, 1);
	/* No change to the legacy, non-v3 extension implementation. */
	zero_calls = zero_blocks = 0;
	UT_ASSERT_EQ(resize_reserved_range(199, &extended, false), 200);
	UT_ASSERT_EQ(zero_calls, 0);
	UT_ASSERT_EQ(resize_reserved_range(201, &extended, false), 3);
	owner_live = false;
}

static void
materialize_original_range(void)
{
	BufferManagerRelation bmr = { .smgr = &reopened };
	ForkNumber fork = MAIN_FORKNUM;
	BlockNumber first_block = 200;
	uint32 extend_by = 2, hw_lease_tail = 3;
	bool ko_scoped = true;

#include "test_cluster_space_extend_zero.inc"
}

static void
handoff_original_range(volatile uint64 *ko_serial)
{
	bool ko_scoped = true;

#include "test_cluster_space_extend_handoff.inc"
}

UT_TEST(test_original_materialization_precedes_handoff_and_tail_error_retains_owner)
{
	for (int fail = 0; fail < 3; fail++) {
		volatile bool caught = false;
		volatile uint64 serial = 42;
		reset();
		owner_live = owner_armed = true;
		zero_calls = zero_blocks = 0;
		zero_fail_at = fail;
		PG_TRY();
		{
			materialize_original_range();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fail != 0);
		UT_ASSERT_EQ(owner_materialized, fail == 0 ? 1 : 0);
		UT_ASSERT_EQ(owner_ends, 0);
		UT_ASSERT(owner_live);
		if (fail == 0) {
			UT_ASSERT_EQ(zero_blocks, 5);
			handoff_original_range(&serial);
			UT_ASSERT_EQ(serial, 0);
			UT_ASSERT_EQ(owner_ends, 1);
			UT_ASSERT(!owner_live);
		}
	}
	zero_fail_at = 0;
	owner_live = false;
}

UT_TEST(test_original_extension_registers_before_identity_and_reservation)
{
	for (int private = 0; private < 2; private ++) {
		reset();
		owner_expected = owner_wait_retire = true;
		owner_refusals = 2;
		UT_ASSERT_EQ(extend(private, MAIN_FORKNUM), 200);
		UT_ASSERT_EQ(owner_enters, 3);
		UT_ASSERT_EQ(owner_waits, 2);
		UT_ASSERT_EQ(owner_ends, 1);
		UT_ASSERT(!owner_live);
	}
}

UT_TEST(test_original_extension_cleanup_keeps_error_and_ends_exact_attempt)
{
	for (int failure = 0; failure < 3; failure++) {
		volatile bool caught = false;
		reset();
		owner_expected = true;
		missing_identity = failure == 0;
		throw_grant = failure == 1;
		owner_end_ok = failure == 0;
		PG_TRY();
		{
			(void)extend(false, MAIN_FORKNUM);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(owner_enters, 1);
		UT_ASSERT_EQ(owner_ends, failure == 2 ? 1 : 0);
		UT_ASSERT_EQ(owner_aborts, failure == 2 ? 0 : 1);
		UT_ASSERT_EQ(owner_live, failure != 2);
		UT_ASSERT(!hw_held);
		/* Shared cleanup is tested through the actual ResourceOwner callback
		 * in test_cluster_ko_stop; this caller must not end it under ERROR. */
	}
}
UT_TEST(test_invalid_extension_owner_does_not_wait_as_if_only_fenced)
{
	volatile bool caught = false;
	reset();
	owner_invalid_admission = true;
	PG_TRY();
	{
		(void)extend(false, MAIN_FORKNUM);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(owner_waits, 0);
	UT_ASSERT_EQ(owner_enters, 1);
	UT_ASSERT_EQ(owner_ends, 0);
	UT_ASSERT_EQ(identity_reads + victims + reservations, 0);
}


int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_preheld_hw_is_not_reacquired_or_released_by_extension);
	UT_RUN(test_preheld_hw_survives_extension_error_for_outer_unwind);
	UT_RUN(test_preheld_hw_rejects_invalid_handle_and_noncanonical_scope);
	UT_RUN(test_preheld_hw_reservation_refusal_has_no_native_fallback);
	UT_RUN(test_single_live_and_private_callers_use_canonical_reservations);
	UT_RUN(test_nonshared_and_auxiliary_keep_native_extension);
	UT_RUN(test_refusals_do_not_fall_back_to_file_size);
	UT_RUN(test_reservation_error_releases_hw);
	UT_RUN(test_extension_returns_initialized_content_locks);
	UT_RUN(test_extension_error_releases_current_and_earlier_content_locks);
	UT_RUN(test_original_resize_keeps_entire_durable_grant_materialized);
	UT_RUN(test_original_materialization_precedes_handoff_and_tail_error_retains_owner);
	UT_RUN(test_original_extension_registers_before_identity_and_reservation);
	UT_RUN(test_original_extension_cleanup_keeps_error_and_ends_exact_attempt);
	UT_RUN(test_invalid_extension_owner_does_not_wait_as_if_only_fenced);
	UT_DONE();
	return ut_failed_count != 0;
}
