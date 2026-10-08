/*-------------------------------------------------------------------------
 *
 * test_cluster_buffer_mapping_fixture.h
 *    Shared allocation and hash storage fixtures for native buffer tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_buffer_mapping_fixture.h
 *
 * NOTES
 *    This is a pgrac-original standalone test.  It links the native buffer
 *    mapping implementation with shared allocation and hash storage stubs.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_BUFFER_MAPPING_FIXTURE_H
#define TEST_CLUSTER_BUFFER_MAPPING_FIXTURE_H

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

#endif
