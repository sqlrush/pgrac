/*-------------------------------------------------------------------------
 * test_cluster_space_lazy_fork.c
 *    Exercise the native optional fork constructor and subsequent handoff.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static RelationData relation;
static FormData_pg_class form;
static SMgrRelationData storage, reopened;
static RelFileLocator locator;
static ForkNumber expected_fork;
static BlockNumber file_blocks;
static bool present, locked, replaced, create_during_lock, replace_during_lock;
static bool refuse_begin, current_owner, live, armed, aborting, retained;
static bool recover, private_storage, lose_on_lock, lose_on_unlock;
static unsigned fail_site;
static unsigned identity_drift;
static unsigned begins, arms, ends, aborts, locks, unlocks, creates, exists_reads;
static unsigned size_reads, extensions, reads, releases, opens;
static bool create_owned;
enum { FAIL_LOCK = 1, FAIL_CREATE, FAIL_UNLOCK };

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
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
	return recover;
}
int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(tag, locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return private_storage ? 0 : 1;
}

/* The original constructor implementation is unchanged. This explicit
 * boundary observes the caller's serial, effect order and error lifetime. */
bool
cluster_ko_create_begin_v3(RelFileLocator tag, uint64 *serial)
{
	UT_ASSERT(RelFileLocatorEquals(tag, locator));
	UT_ASSERT(!live && !locked);
	begins++;
	*serial = 0;
	if (refuse_begin)
		return false;
	*serial = 71;
	live = true;
	return true;
}
bool
cluster_ko_create_write_begin_v3(RelFileLocator tag, uint64 serial)
{
	UT_ASSERT(RelFileLocatorEquals(tag, locator));
	UT_ASSERT_EQ(serial, 71);
	UT_ASSERT(live && locked);
	arms++;
	if (!current_owner || aborting)
		return false;
	armed = true;
	return true;
}
void
cluster_ko_create_abort_v3(uint64 serial)
{
	if (serial == 0)
		return;
	UT_ASSERT_EQ(serial, 71);
	UT_ASSERT(live);
	aborts++;
	aborting = true;
}
bool
cluster_ko_create_end_v3(uint64 serial, bool complete)
{
	UT_ASSERT_EQ(serial, 71);
	UT_ASSERT(complete && live && !locked && !aborting);
	ends++;
	live = false;
	retained = armed && !current_owner;
	return !retained;
}

SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(tag, relation.rd_locator));
	UT_ASSERT_EQ(backend, relation.rd_backend);
	opens++;
	return replaced ? &reopened : &storage;
}
void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	UT_ASSERT(owner == &relation.rd_smgr);
	*owner = rel;
}
bool
smgrexists(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(rel == (replaced ? &reopened : &storage));
	UT_ASSERT_EQ(fork, expected_fork);
	exists_reads++;
	return present;
}
void
smgrcreate(SMgrRelation rel, ForkNumber fork, bool redo)
{
	UT_ASSERT(rel == (replaced ? &reopened : &storage));
	UT_ASSERT_EQ(fork, expected_fork);
	UT_ASSERT(locked && !present);
	creates++;
	create_owned = live && armed && !aborting;
	if (fail_site == FAIL_CREATE)
		pg_re_throw();
	present = true;
}
BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(rel == (replaced ? &reopened : &storage));
	UT_ASSERT_EQ(fork, expected_fork);
	UT_ASSERT(!live && !locked);
	size_reads++;
	return file_blocks;
}
void
LockRelationForExtension(Relation rel, LOCKMODE lockmode)
{
	UT_ASSERT(rel == &relation && lockmode == ExclusiveLock && !locked);
	locks++;
	if (fail_site == FAIL_LOCK)
		pg_re_throw();
	locked = true;
	if (replace_during_lock) {
		replaced = true;
		relation.rd_smgr = NULL;
		memset(&storage, 0xa5, sizeof(storage));
		if (identity_drift == 1)
			relation.rd_locator.relNumber = ++reopened.smgr_rlocator.locator.relNumber;
		if (identity_drift == 2)
			relation.rd_backend = reopened.smgr_rlocator.backend = 9;
	}
	if (create_during_lock)
		present = true;
	if (lose_on_lock)
		current_owner = false;
}
void
UnlockRelationForExtension(Relation rel, LOCKMODE lockmode)
{
	UT_ASSERT(rel == &relation && lockmode == ExclusiveLock && locked);
	locked = false;
	unlocks++;
	if (lose_on_unlock)
		current_owner = false;
	if (fail_site == FAIL_UNLOCK)
		pg_re_throw();
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && !live && !locked);
	releases++;
}

/* The following operations are independent, original consumers. These
 * fixtures only verify that the constructor no longer owns their entry. */
static BlockNumber
ExtendBufferedRelCommon(BufferManagerRelation bmr, ForkNumber fork, BufferAccessStrategy strategy,
						uint32 flags, uint32 count, BlockNumber upto, Buffer *buffers,
						uint32 *extended, const struct HwLock *hw,
						const struct ClusterSpaceIdentity *identity, uint64 ko_serial)
{
	BlockNumber first = file_blocks;
	UT_ASSERT(hw == NULL && identity == NULL && ko_serial == 0);
	UT_ASSERT(!live && !locked && present);
	UT_ASSERT(bmr.smgr == (replaced ? &reopened : &storage));
	UT_ASSERT_EQ(fork, expected_fork);
	UT_ASSERT(count > 0 && count <= 64 && first + count <= upto);
	extensions++;
	for (uint32 i = 0; i < count; i++)
		buffers[i] = first + i + 1;
	*extended = count;
	file_blocks += count;
	return first;
}
static Buffer
ReadBuffer_common(SMgrRelation rel, char persistence, ForkNumber fork, BlockNumber block,
				  ReadBufferMode mode, BufferAccessStrategy strategy, bool *hit)
{
	UT_ASSERT(!live && !locked);
	UT_ASSERT(rel == (replaced ? &reopened : &storage));
	UT_ASSERT_EQ(fork, expected_fork);
	reads++;
	*hit = true;
	return block + 1;
}

#undef elog
#undef ereport
#define elog(...) pg_re_throw()
#define ereport(...) pg_re_throw()
#include "test_cluster_space_lazy_fork.inc"

static void
reset(ForkNumber fork)
{
	UT_ASSERT(PG_exception_stack == NULL);
	memset(&storage, 0, sizeof(storage));
	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	locator = (RelFileLocator){ 1663, 5, 17000 };
	storage.smgr_rlocator.locator = locator;
	storage.smgr_rlocator.backend = InvalidBackendId;
	reopened = storage;
	relation.rd_locator = locator;
	relation.rd_backend = InvalidBackendId;
	relation.rd_smgr = &storage;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	expected_fork = fork;
	file_blocks = 0;
	present = locked = replaced = create_during_lock = replace_during_lock = false;
	refuse_begin = live = armed = aborting = retained = false;
	recover = private_storage = lose_on_lock = lose_on_unlock = false;
	current_owner = true;
	create_owned = false;
	fail_site = 0;
	identity_drift = 0;
	begins = arms = ends = aborts = locks = unlocks = creates = exists_reads = 0;
	size_reads = extensions = reads = releases = opens = 0;
	cluster_enabled = cluster_shared_config = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
}
static Buffer
extend_to(BlockNumber count, uint32 flags)
{
	return ExtendBufferedRelTo(BMR_REL(&relation), expected_fork, NULL, flags, count,
							   RBM_ZERO_AND_LOCK);
}

UT_TEST(test_lazy_fork_owner_covers_both_original_auxiliary_creates)
{
	for (ForkNumber fork = FSM_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		reset(fork);
		UT_ASSERT_EQ(extend_to(3, EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE), 3);
		UT_ASSERT_EQ(begins, 1);
		UT_ASSERT_EQ(arms, 1);
		UT_ASSERT_EQ(ends, 1);
		UT_ASSERT_EQ(aborts, 0);
		UT_ASSERT_EQ(locks, 1);
		UT_ASSERT_EQ(unlocks, 1);
		UT_ASSERT_EQ(creates, 1);
		UT_ASSERT(create_owned && !live && !retained);
		UT_ASSERT_EQ(extensions, 1);
		UT_ASSERT_EQ(releases, 2);
		UT_ASSERT_EQ(reads, 0);
	}
}
UT_TEST(test_lazy_fork_refusal_precedes_lock_create_and_extension)
{
	volatile bool caught = false;
	reset(FSM_FORKNUM);
	refuse_begin = true;
	PG_TRY();
	{
		(void)extend_to(1, EB_CREATE_FORK_IF_NEEDED);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(locks + creates + ends + aborts + size_reads + extensions + reads, 0);
	UT_ASSERT(!live);
}
UT_TEST(test_lazy_fork_owner_loss_after_lock_rejects_create)
{
	volatile bool caught = false;
	reset(FSM_FORKNUM);
	lose_on_lock = true;
	PG_TRY();
	{
		(void)extend_to(1, EB_CREATE_FORK_IF_NEEDED);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(aborts, 1);
	UT_ASSERT_EQ(creates + ends + extensions + reads, 0);
	UT_ASSERT(live && aborting && locked && !armed);
}
UT_TEST(test_lazy_fork_errors_retain_original_owner_without_fake_unlock)
{
	for (unsigned site = FAIL_LOCK; site <= FAIL_UNLOCK; site++) {
		volatile bool caught = false;
		reset(FSM_FORKNUM);
		fail_site = site;
		PG_TRY();
		{
			(void)extend_to(1, EB_CREATE_FORK_IF_NEEDED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(aborts, 1);
		UT_ASSERT_EQ(ends + extensions + reads, 0);
		UT_ASSERT(live && aborting);
		UT_ASSERT_EQ(locked, site == FAIL_CREATE);
		UT_ASSERT_EQ(armed, site != FAIL_LOCK);
		UT_ASSERT_EQ(creates, site == FAIL_LOCK ? 0 : 1);
		UT_ASSERT_EQ(unlocks, site == FAIL_UNLOCK ? 1 : 0);
	}
}
UT_TEST(test_lazy_fork_end_failure_prevents_extension_or_read)
{
	volatile bool caught = false;
	reset(FSM_FORKNUM);
	lose_on_unlock = true;
	PG_TRY();
	{
		(void)extend_to(1, EB_CREATE_FORK_IF_NEEDED);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(creates, 1);
	UT_ASSERT_EQ(ends, 1);
	UT_ASSERT_EQ(aborts + extensions + reads + size_reads, 0);
	UT_ASSERT(!locked && retained && !live);
}
UT_TEST(test_lazy_fork_concurrent_creator_does_not_invent_local_io)
{
	reset(FSM_FORKNUM);
	create_during_lock = true;
	file_blocks = 2;
	UT_ASSERT_EQ(extend_to(1, EB_CREATE_FORK_IF_NEEDED), 1);
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(ends, 1);
	UT_ASSERT_EQ(creates + arms + aborts + extensions, 0);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT(!live && !armed && !retained);
}
UT_TEST(test_lazy_fork_refreshes_native_handle_after_original_lock_wait)
{
	reset(VISIBILITYMAP_FORKNUM);
	replace_during_lock = true;
	UT_ASSERT_EQ(extend_to(1, EB_CREATE_FORK_IF_NEEDED), 1);
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(opens, 1);
	UT_ASSERT_EQ(creates, 1);
	UT_ASSERT_EQ(ends, 1);
	UT_ASSERT(create_owned && relation.rd_smgr == &reopened);
}
UT_TEST(test_lazy_fork_existing_or_unrequested_creation_does_not_register)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset(FSM_FORKNUM);
		present = true;
		file_blocks = 2;
		if (variant == 1)
			storage.smgr_cached_nblocks[FSM_FORKNUM] = 2;
		UT_ASSERT_EQ(extend_to(1, variant == 2 ? 0 : EB_CREATE_FORK_IF_NEEDED), 1);
		UT_ASSERT_EQ(begins + ends + arms + locks + creates + extensions, 0);
		UT_ASSERT_EQ(reads, 1);
	}
}
UT_TEST(test_lazy_fork_reopened_identity_cannot_borrow_old_owner)
{
	for (unsigned drift = 1; drift <= 2; drift++) {
		volatile bool caught = false;
		reset(FSM_FORKNUM);
		replace_during_lock = true;
		identity_drift = drift;
		PG_TRY();
		{
			(void)extend_to(1, EB_CREATE_FORK_IF_NEEDED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(begins, 1);
		UT_ASSERT_EQ(aborts, 1);
		UT_ASSERT_EQ(creates + arms + ends + extensions + reads, 0);
		UT_ASSERT(live && aborting && locked && !armed);
	}
}
UT_TEST(test_lazy_fork_native_private_temp_recovery_paths_unchanged)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		reset(FSM_FORKNUM);
		cluster_shared_config = variant != 0;
		cluster_enabled = variant != 1;
		private_storage = variant == 2;
		recover = variant == 3;
		if (variant == 4) {
			form.relpersistence = RELPERSISTENCE_TEMP;
			relation.rd_backend = storage.smgr_rlocator.backend = 9;
		}
		UT_ASSERT_EQ(extend_to(1, EB_CREATE_FORK_IF_NEEDED), 1);
		UT_ASSERT_EQ(begins + ends + arms + aborts, 0);
		UT_ASSERT_EQ(creates, 1);
		UT_ASSERT_EQ(extensions, 1);
		UT_ASSERT(!create_owned);
	}
}
UT_TEST(test_lazy_fork_preserves_original_bounded_extension_batches)
{
	reset(FSM_FORKNUM);
	UT_ASSERT_EQ(extend_to(65, EB_CREATE_FORK_IF_NEEDED), 65);
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(ends, 1);
	UT_ASSERT_EQ(extensions, 2);
	UT_ASSERT_EQ(releases, 64);
	UT_ASSERT_EQ(reads, 0);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_lazy_fork_owner_covers_both_original_auxiliary_creates);
	UT_RUN(test_lazy_fork_refusal_precedes_lock_create_and_extension);
	UT_RUN(test_lazy_fork_owner_loss_after_lock_rejects_create);
	UT_RUN(test_lazy_fork_errors_retain_original_owner_without_fake_unlock);
	UT_RUN(test_lazy_fork_end_failure_prevents_extension_or_read);
	UT_RUN(test_lazy_fork_concurrent_creator_does_not_invent_local_io);
	UT_RUN(test_lazy_fork_refreshes_native_handle_after_original_lock_wait);
	UT_RUN(test_lazy_fork_existing_or_unrequested_creation_does_not_register);
	UT_RUN(test_lazy_fork_reopened_identity_cannot_borrow_old_owner);
	UT_RUN(test_lazy_fork_native_private_temp_recovery_paths_unchanged);
	UT_RUN(test_lazy_fork_preserves_original_bounded_extension_batches);
	UT_DONE();
	return ut_failed_count != 0;
}
