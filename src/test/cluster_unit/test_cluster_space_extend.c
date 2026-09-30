/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the native extension allocation prefix; only external buffer,
 * GES, identity-read and durable-grant boundaries are fixtures. */
#include "postgres.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_hw_lease.h"
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

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool RecoveryInProgress(void) { return false; }
ClusterExtendEngage cluster_extend_liveness_engage(bool wait) { return CLUSTER_EXTEND_ENGAGE_NATIVE; }
bool cluster_hw_lease_active(void) { return false; }
int cluster_smgr_which_for(RelFileLocator tag, BackendId backend) { return 1; }
SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(tag, identity.key.locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return retired ? &reopened : &storage;
}
void smgrsetowner(SMgrRelation *owner, SMgrRelation rel) { *owner = rel; }
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
	identity_reads++;
	if (missing_identity) return false;
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
	if (refuse_hw) return false;
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
cluster_space_reserve(const ClusterSpaceIdentity *id, const HwLock *lock, uint32 want, uint32 *granted)
{
	UT_ASSERT(hw_held && lock->held && id->sequence == identity.sequence);
	UT_ASSERT_EQ(want, 2);
	UT_ASSERT_EQ(size_reads, 0);
	if (throw_grant) pg_re_throw();
	if (refuse_grant) { *granted = 0; return InvalidBlockNumber; }
	reservations++;
	*granted = want;
	return 200;
}
BlockNumber
cluster_hw_allocate(RelFileLocator tag, ForkNumber fork, uint32 want, BlockNumber seed, uint32 *granted)
{
	legacy_allocations++;
	*granted = want;
	return 500;
}
static Buffer fixture_victim(void) { return ++victims; }
static void fixture_unlock(void) { UT_ASSERT(local_lock); local_lock = false; }
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
	identity.key.locator = (RelFileLocator){1663, 5, 17000};
	identity.sequence = 1;
	relation.rd_locator = storage.smgr_rlocator.locator = identity.key.locator;
	storage.smgr_rlocator.backend = InvalidBackendId;
	reopened = storage;
	relation.rd_smgr = &storage;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	form.relkind = RELKIND_RELATION;
	for (int i = 0; i < 2; i++) descriptors[i].bufferdesc.buf_id = i;
	cluster_shared_config = true;
	hw_held = local_lock = retired = missing_identity = refuse_hw = refuse_grant = throw_grant = false;
	victims = unpins = identity_reads = reservations = legacy_allocations = size_reads = 0;
}

static BlockNumber
extend(bool private, ForkNumber fork)
{
	Buffer buffers[2];
	uint32 extended;
	BufferManagerRelation bmr = { .rel = private ? NULL : &relation,
		.smgr = &storage, .relpersistence = RELPERSISTENCE_PERMANENT };

	return ExtendBufferedRelShared(bmr, fork, NULL, EB_SKIP_EXTENSION_LOCK, 2,
		InvalidBlockNumber, buffers, &extended);
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
		if (variant == 0) cluster_shared_config = false;
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
		PG_TRY(); { (void)extend(true, MAIN_FORKNUM); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
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
	PG_TRY(); { (void)extend(false, MAIN_FORKNUM); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!hw_held);
	UT_ASSERT_EQ(reservations + size_reads + legacy_allocations, 0);
}
int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_single_live_and_private_callers_use_canonical_reservations);
	UT_RUN(test_nonshared_and_auxiliary_keep_native_extension);
	UT_RUN(test_refusals_do_not_fall_back_to_file_size);
	UT_RUN(test_reservation_error_releases_hw);
	UT_DONE();
	return ut_failed_count != 0;
}
