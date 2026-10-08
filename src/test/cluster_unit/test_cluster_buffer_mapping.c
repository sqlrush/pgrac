/*-------------------------------------------------------------------------
 *
 * test_cluster_buffer_mapping.c
 *    Current and read-only version mappings in the native buffer hash.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_buffer_mapping.c
 *
 * NOTES
 *    This is a pgrac-original standalone test.  It links the native buffer
 *    mapping implementation with shared allocation and hash storage stubs.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"

#include <setjmp.h>
#include <stdlib.h>

#include "storage/buf_internals.h"
#include "storage/shmem.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int NBuffers = 64;

/* Only hash storage and shared allocation are fixtures. Mapping decisions
 * and generation allocation execute the production buf_table object. */
static union {
	uint64 align;
	char bytes[64];
} entries[32];
static bool used[32];
static Size entry_size;
static bool hash_initialized;
static bool deny_new_entry;
static pg_atomic_uint64 generation_storage;
static bool generation_found;
static jmp_buf error_jump;
static bool expect_error;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errcode(int code pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (expect_error)
		longjmp(error_jump, 1);
	abort();
}

Size
hash_estimate_size(long count, Size size)
{
	return count * size;
}

Size
add_size(Size first, Size second)
{
	return first + second;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long initial pg_attribute_unused(),
			  long maximum pg_attribute_unused(), HASHCTL *info, int flags pg_attribute_unused())
{
	if (!hash_initialized) {
		memset(entries, 0, sizeof(entries));
		memset(used, 0, sizeof(used));
		entry_size = info->entrysize;
		Assert(entry_size <= sizeof(entries[0]));
		hash_initialized = true;
	}
	return (HTAB *)entries;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	Assert(size == sizeof(generation_storage));
	*found = generation_found;
	generation_found = true;
	return &generation_storage;
}

uint32
get_hash_value(HTAB *hash pg_attribute_unused(), const void *key)
{
	const BufferTag *tag = key;
	return tag->blockNum ^ tag->relNumber;
}

void *
hash_search_with_hash_value(HTAB *hash pg_attribute_unused(), const void *key,
							uint32 value pg_attribute_unused(), HASHACTION action, bool *found)
{
	int empty = -1;
	int i;

	for (i = 0; i < lengthof(entries); i++) {
		if (!used[i]) {
			if (empty < 0)
				empty = i;
			continue;
		}
		if (memcmp(entries[i].bytes, key, sizeof(BufferTag)) == 0) {
			if (found)
				*found = true;
			if (action == HASH_REMOVE)
				used[i] = false;
			return entries[i].bytes;
		}
	}
	if (found)
		*found = false;
	if (action != HASH_ENTER && action != HASH_ENTER_NULL)
		return NULL;
	if (empty < 0 || deny_new_entry) {
		if (action == HASH_ENTER)
			abort();
		return NULL;
	}
	used[empty] = true;
	memset(entries[empty].bytes, 0, entry_size);
	memcpy(entries[empty].bytes, key, sizeof(BufferTag));
	return entries[empty].bytes;
}

static BufferTag
reset_mapping(void)
{
	BufferTag tag;
	RelFileLocator locator = { 1663, 5, 16385 };

	hash_initialized = false;
	generation_found = false;
	deny_new_entry = false;
	expect_error = false;
	pg_atomic_init_u64(&generation_storage, 0);
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);
	InitBufferTag(&tag, &locator, MAIN_FORKNUM, 7);
	return tag;
}

UT_TEST(test_native_current_mapping_is_unchanged)
{
	BufferTag tag = reset_mapping();
	BufferTag other = tag;
	uint32 hash = BufTableHashCode(&tag);

	other.blockNum++;
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 3), -1);
	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 4), 3);
	UT_ASSERT_EQ(BufTableLookup(&other, BufTableHashCode(&other)), -1);
	BufTableDelete(&tag, hash);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
}

UT_TEST(test_current_and_cr_are_distinct)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 generation = 0;
	uint64 observed = 0;

	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 3), -1);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT_EQ(head, -1);
	UT_ASSERT(generation > 0);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 3);
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &observed));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT_EQ(observed, generation);
	BufTableDelete(&tag, hash);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &observed));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 5), -1);
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &observed));
	UT_ASSERT_EQ(observed, generation);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 5);
}

UT_TEST(test_cr_only_never_grants_current)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 generation = 0;

	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 9, &head, &generation));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT(BufTableCRReplaceHead(&tag, hash, generation, 9, 8));
	UT_ASSERT(BufTableCRReplaceHead(&tag, hash, generation, 8, -1));
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	head = 55;
	UT_ASSERT(!BufTableCRLookup(&tag, hash, &head, &generation));
	UT_ASSERT_EQ(head, 55);
}

UT_TEST(test_reused_tag_does_not_reuse_generation)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 before = 0;
	uint64 after = 0;

	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &before));
	UT_ASSERT(BufTableCRReplaceHead(&tag, hash, before, 8, -1));
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &after));
	UT_ASSERT(after > before);
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, before, 8, -1));
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &before));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT_EQ(before, after);
}

UT_TEST(test_stale_head_does_not_remove_successor)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 generation = 0;

	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT(BufTableCRInsert(&tag, hash, 9, &head, &generation));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, 8, -1));
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &generation));
	UT_ASSERT_EQ(head, 9);
}

UT_TEST(test_invalid_ids_and_current_alias_leave_mapping_unchanged)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = 55;
	uint64 generation = 777;

	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 3), -1);
	UT_ASSERT(!BufTableCRInsert(&tag, hash, -1, &head, &generation));
	UT_ASSERT(!BufTableCRInsert(&tag, hash, NBuffers, &head, &generation));
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 3, &head, &generation));
	UT_ASSERT_EQ(head, 55);
	UT_ASSERT_EQ(generation, 777);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 3);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, 8, 3));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, 8, NBuffers));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, 0, 8, -1));
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &generation));
	UT_ASSERT_EQ(head, 8);
}

UT_TEST(test_capacity_refusal_has_no_partial_entry_or_outputs)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = 55;
	uint64 generation = 777;

	deny_new_entry = true;
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT_EQ(head, 55);
	UT_ASSERT_EQ(generation, 777);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), 0);
	deny_new_entry = false;
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT_EQ(head, -1);
}

UT_TEST(test_attach_does_not_reset_generation)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 before = 0;
	uint64 after = 0;

	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &before));
	UT_ASSERT(BufTableCRReplaceHead(&tag, hash, before, 8, -1));
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &after));
	UT_ASSERT(after > before);
}

UT_TEST(test_exhaustion_never_wraps_or_publishes_partial_entry)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = 55;
	uint64 generation = 777;
	volatile bool caught = false;

	pg_atomic_write_u64(&generation_storage, UINT64_MAX);
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT_EQ(head, 55);
	UT_ASSERT_EQ(generation, 777);
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), UINT64_MAX);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	expect_error = true;
	if (setjmp(error_jump) == 0)
		(void)BufTableInsert(&tag, hash, 3);
	else
		caught = true;
	expect_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), UINT64_MAX);
}


UT_TEST(test_invalid_arguments_preserve_outputs)
{
	BufferTag tag = reset_mapping();
	BufferTag invalid = tag;
	uint32 hash = BufTableHashCode(&tag);
	int head = 55;
	uint64 generation = 777;

	invalid.blockNum = P_NEW;
	UT_ASSERT(!BufTableCRInsert(NULL, hash, 8, &head, &generation));
	UT_ASSERT(!BufTableCRInsert(&invalid, hash, 8, &head, &generation));
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 8, NULL, &generation));
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 8, &head, NULL));
	UT_ASSERT(!BufTableCRLookup(NULL, hash, &head, &generation));
	UT_ASSERT_EQ(head, 55);
	UT_ASSERT_EQ(generation, 777);
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), 0);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT(!BufTableCRLookup(&tag, hash, NULL, &generation));
	UT_ASSERT(!BufTableCRLookup(&tag, hash, &head, NULL));
	UT_ASSERT(!BufTableCRReplaceHead(NULL, hash, generation, 8, -1));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, -1, -1));
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, 8, -2));
}

UT_TEST(test_cr_only_current_delete_alias_and_duplicate_refuse)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 generation = 0;
	uint64 observed = 0;
	volatile bool caught = false;

	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT(!BufTableCRInsert(&tag, hash, 8, &head, &observed));
	UT_ASSERT_EQ(head, -1);
	UT_ASSERT_EQ(observed, 0);
	UT_ASSERT(!BufTableCRReplaceHead(&tag, hash, generation, 8, 8));
	expect_error = true;
	if (setjmp(error_jump) == 0)
		BufTableDelete(&tag, hash);
	else
		caught = true;
	expect_error = false;
	UT_ASSERT(caught);
	caught = false;
	expect_error = true;
	if (setjmp(error_jump) == 0)
		(void)BufTableInsert(&tag, hash, 8);
	else
		caught = true;
	expect_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), -1);
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &observed));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT_EQ(observed, generation);
}

UT_TEST(test_shared_memory_accounts_for_anchors_and_allocator)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	int head = -2;
	uint64 generation = 0;

	UT_ASSERT_EQ(entry_size, 40);
	UT_ASSERT_EQ(BufTableShmemSize(128), 128 * 40 + MAXALIGN(sizeof(pg_atomic_uint64)));
	UT_ASSERT_EQ(BufTableInsert(&tag, hash, 3), -1);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &generation));
	UT_ASSERT(BufTableCRReplaceHead(&tag, hash, generation, 8, -1));
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 3);
	UT_ASSERT(!BufTableCRLookup(&tag, hash, &head, &generation));
}

UT_TEST(test_scope_nonce_shares_allocator_without_alias_or_wrap)
{
	BufferTag tag = reset_mapping();
	uint32 hash = BufTableHashCode(&tag);
	uint64 first = 0, second = 0, anchor = 0, refused = 777;
	int head = -2;

	UT_ASSERT(!BufTableNewCRScope(NULL));
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), 0);
	UT_ASSERT(BufTableNewCRScope(&first));
	UT_ASSERT(first != 0);
	UT_ASSERT(BufTableCRInsert(&tag, hash, 8, &head, &anchor));
	UT_ASSERT(anchor != first);
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);
	UT_ASSERT(BufTableNewCRScope(&second));
	UT_ASSERT(second > anchor && second != first);
	pg_atomic_write_u64(&generation_storage, PG_UINT64_MAX - 1);
	UT_ASSERT(BufTableNewCRScope(&second));
	UT_ASSERT_EQ(second, PG_UINT64_MAX);
	UT_ASSERT(!BufTableNewCRScope(&refused));
	UT_ASSERT_EQ(refused, 777);
	UT_ASSERT_EQ(pg_atomic_read_u64(&generation_storage), PG_UINT64_MAX);
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &second));
	UT_ASSERT_EQ(head, 8);
	UT_ASSERT_EQ(second, anchor);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_native_current_mapping_is_unchanged);
	UT_RUN(test_current_and_cr_are_distinct);
	UT_RUN(test_cr_only_never_grants_current);
	UT_RUN(test_reused_tag_does_not_reuse_generation);
	UT_RUN(test_stale_head_does_not_remove_successor);
	UT_RUN(test_invalid_ids_and_current_alias_leave_mapping_unchanged);
	UT_RUN(test_capacity_refusal_has_no_partial_entry_or_outputs);
	UT_RUN(test_attach_does_not_reset_generation);
	UT_RUN(test_exhaustion_never_wraps_or_publishes_partial_entry);
	UT_RUN(test_invalid_arguments_preserve_outputs);
	UT_RUN(test_cr_only_current_delete_alias_and_duplicate_refuse);
	UT_RUN(test_shared_memory_accounts_for_anchors_and_allocator);
	UT_RUN(test_scope_nonce_shares_allocator_without_alias_or_wrap);
	UT_DONE();
	return ut_failed_count != 0;
}
