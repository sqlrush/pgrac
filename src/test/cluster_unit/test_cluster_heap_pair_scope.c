/* Execute the real ordered heap-pair admission boundary with controlled locks. */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int NBuffers = 4;
static BufferDescPadded descriptors[4];
BufferDescPadded *BufferDescriptors = descriptors;
static RelationData relation_data;
static FormData_pg_class relation_class;
static int pins[4];
static bool held[4];
static LWLockMode modes[4];
static LWLock unrelated_lock;
static bool unrelated_held;
static int acquired;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
BufferIsPinnedByMe(Buffer buffer)
{
	return buffer > 0 && buffer <= NBuffers && pins[buffer - 1] > 0;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	int i;

	for (i = 0; i < NBuffers; i++)
		if (held[i])
			callback(BufferDescriptorGetContentLock(GetBufferDescriptor(i)),
					 modes[i], context);
	if (unrelated_held)
		callback(&unrelated_lock, LW_EXCLUSIVE, context);
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 2);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(held[0] && modes[0] == LW_EXCLUSIVE);
	UT_ASSERT(!held[1]);
	UT_ASSERT(pins[0] > 0 && pins[1] > 0);
	held[1] = true;
	modes[1] = LW_EXCLUSIVE;
	acquired++;
}

#include "test_cluster_heap_pair_scope.inc"

static void
fixture(void)
{
	int i;

	memset(descriptors, 0, sizeof(descriptors));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relation_class, 0, sizeof(relation_class));
	memset(held, 0, sizeof(held));
	relation_data.rd_rel = &relation_class;
	relation_class.relkind = RELKIND_RELATION;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	relation_data.rd_locator.spcOid = 1663;
	relation_data.rd_locator.dbOid = 5;
	relation_data.rd_locator.relNumber = 16385;
	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *desc = GetBufferDescriptor(i);

		desc->buf_id = i;
		InitBufferTag(&desc->tag, &relation_data.rd_locator, MAIN_FORKNUM, i);
		pins[i] = 1;
		modes[i] = LW_EXCLUSIVE;
	}
	held[0] = true;
	unrelated_held = false;
	acquired = 0;
}

static void
assert_refused(Relation relation, Buffer first, Buffer second)
{
	bool first_before = held[0];
	bool second_before = held[1];

	UT_ASSERT(!ClusterLockBufferHeapPair(relation, first, second));
	UT_ASSERT_EQ(acquired, 0);
	UT_ASSERT_EQ(held[0], first_before);
	UT_ASSERT_EQ(held[1], second_before);
}

UT_TEST(ordered_heap_current_uses_original_blocking_acquire)
{
	fixture();
	UT_ASSERT(ClusterLockBufferHeapPair(&relation_data, 1, 2));
	UT_ASSERT_EQ(acquired, 1);
	UT_ASSERT(held[0] && held[1]);
}

UT_TEST(heap_backed_relation_kinds_are_admitted)
{
	char kinds[] = {RELKIND_MATVIEW, RELKIND_TOASTVALUE};
	int i;

	for (i = 0; i < lengthof(kinds); i++)
	{
		fixture();
		relation_class.relkind = kinds[i];
		UT_ASSERT(ClusterLockBufferHeapPair(&relation_data, 1, 2));
		UT_ASSERT_EQ(acquired, 1);
	}
}

UT_TEST(vm_and_fsm_remain_outside_nested_acquire)
{
	ForkNumber forks[] = {VISIBILITYMAP_FORKNUM, FSM_FORKNUM};
	int i;

	for (i = 0; i < lengthof(forks); i++)
	{
		fixture();
		GetBufferDescriptor(1)->tag.forkNum = forks[i];
		assert_refused(&relation_data, 1, 2);
		fixture();
		GetBufferDescriptor(0)->tag.forkNum = forks[i];
		assert_refused(&relation_data, 1, 2);
	}
}

UT_TEST(index_and_nonheap_relations_remain_outside)
{
	char kinds[] = {RELKIND_INDEX, RELKIND_SEQUENCE, RELKIND_PARTITIONED_TABLE};
	int i;

	for (i = 0; i < lengthof(kinds); i++)
	{
		fixture();
		relation_class.relkind = kinds[i];
		assert_refused(&relation_data, 1, 2);
	}
}

UT_TEST(both_tags_must_match_exact_relation)
{
	int i;

	for (i = 0; i < 2; i++)
	{
		fixture();
		GetBufferDescriptor(i)->tag.relNumber++;
		assert_refused(&relation_data, 1, 2);
	}
}

UT_TEST(reverse_and_equal_block_order_are_refused)
{
	fixture();
	GetBufferDescriptor(0)->tag.blockNum = 2;
	assert_refused(&relation_data, 1, 2);
	fixture();
	GetBufferDescriptor(1)->tag.blockNum = 0;
	assert_refused(&relation_data, 1, 2);
}

UT_TEST(both_pins_belong_to_caller)
{
	int i;

	for (i = 0; i < 2; i++)
	{
		fixture();
		pins[i] = 0;
		assert_refused(&relation_data, 1, 2);
	}
}

UT_TEST(first_content_lock_must_be_exclusive)
{
	fixture();
	modes[0] = LW_SHARED;
	assert_refused(&relation_data, 1, 2);
	fixture();
	held[0] = false;
	assert_refused(&relation_data, 1, 2);
}

UT_TEST(any_third_content_lock_keeps_original_refusal)
{
	fixture();
	held[2] = true;
	assert_refused(&relation_data, 1, 2);
}

UT_TEST(already_held_second_content_lock_is_refused)
{
	fixture();
	held[1] = true;
	assert_refused(&relation_data, 1, 2);
}

UT_TEST(noncontent_lwlock_does_not_expand_content_scope)
{
	fixture();
	unrelated_held = true;
	UT_ASSERT(ClusterLockBufferHeapPair(&relation_data, 1, 2));
	UT_ASSERT_EQ(acquired, 1);
}

UT_TEST(local_invalid_and_missing_relation_are_refused)
{
	fixture();
	relation_class.relpersistence = RELPERSISTENCE_TEMP;
	assert_refused(&relation_data, 1, 2);
	fixture();
	assert_refused(NULL, 1, 2);
	assert_refused(&relation_data, InvalidBuffer, 2);
	assert_refused(&relation_data, -1, 2);
	assert_refused(&relation_data, 1, NBuffers + 1);
}

UT_TEST(outside_refusal_leaves_release_and_reentry_to_caller)
{
	fixture();
	GetBufferDescriptor(1)->tag.forkNum = VISIBILITYMAP_FORKNUM;
	assert_refused(&relation_data, 1, 2);
	UT_ASSERT(held[0] && pins[0] == 1 && pins[1] == 1);
	/* nothing-held describes only the refused target: the outer caller
	 * still owns first, and must release it before its separate retry. */
	held[0] = false;
	UT_ASSERT(!held[0] && !held[1]);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(ordered_heap_current_uses_original_blocking_acquire);
	UT_RUN(heap_backed_relation_kinds_are_admitted);
	UT_RUN(vm_and_fsm_remain_outside_nested_acquire);
	UT_RUN(index_and_nonheap_relations_remain_outside);
	UT_RUN(both_tags_must_match_exact_relation);
	UT_RUN(reverse_and_equal_block_order_are_refused);
	UT_RUN(both_pins_belong_to_caller);
	UT_RUN(first_content_lock_must_be_exclusive);
	UT_RUN(any_third_content_lock_keeps_original_refusal);
	UT_RUN(already_held_second_content_lock_is_refused);
	UT_RUN(noncontent_lwlock_does_not_expand_content_scope);
	UT_RUN(local_invalid_and_missing_relation_are_refused);
	UT_RUN(outside_refusal_leaves_release_and_reentry_to_caller);
	UT_DONE();
	return ut_failed_count;
}
