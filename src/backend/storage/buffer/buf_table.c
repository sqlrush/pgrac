/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 * PGRAC MODIFICATIONS
 *   Modified by: SqlRush <sqlrush@gmail.com>
 *   Keep current and read-only CR mappings in the native buffer hash.
 *   Spec: spec-8.16-oracle-cache-fusion-buffer-version-and-unified-cache.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
#ifdef USE_PGRAC_CLUSTER
	uint64		anchor_generation;
	int			cr_head;		/* CR chain head, or -1 */
	uint32		reserved_zero;
#endif
} BufferLookupEnt;

static HTAB *SharedBufHash;

#ifdef USE_PGRAC_CLUSTER
StaticAssertDecl(sizeof(BufferLookupEnt) == 40,
				 "buffer version mapping layout changed");

static pg_atomic_uint64 *SharedBufAnchorGeneration;

/* Initialize a new, exclusively locked anchor without reusing a generation. */
static bool
buf_table_init_anchor(BufferLookupEnt *entry)
{
	uint64		generation = pg_atomic_read_u64(SharedBufAnchorGeneration);

	do
	{
		if (generation == PG_UINT64_MAX)
			return false;
	} while (!pg_atomic_compare_exchange_u64(SharedBufAnchorGeneration,
											 &generation, generation + 1));

	entry->id = -1;
	entry->anchor_generation = generation + 1;
	entry->cr_head = -1;
	entry->reserved_zero = 0;
	return true;
}
#endif


/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 *   What changed: Include the shared anchor generation allocator.
 *   Why: Account for all native buffer mapping shared memory.
 */
Size
BufTableShmemSize(int size)
{
	Size		result = hash_estimate_size(size, sizeof(BufferLookupEnt));

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: account for the shared version-anchor allocator. */
	result = add_size(result, MAXALIGN(sizeof(pg_atomic_uint64)));
#endif
	return result;
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 *   What changed: Initialize or attach the shared anchor generation allocator.
 *   Why: Attaching backends must preserve live mapping identities.
 */
void
InitBufTable(int size)
{
	HASHCTL		info;
#ifdef USE_PGRAC_CLUSTER
	bool		found;
#endif

	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: attach must not reset generations belonging to live anchors. */
	SharedBufAnchorGeneration =
		ShmemInitStruct("Shared Buffer Anchor Generation",
						sizeof(pg_atomic_uint64), &found);
	if (!found)
		pg_atomic_init_u64(SharedBufAnchorGeneration, 0);
#endif
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHash, (void *) tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 *   What changed: Fill a CR-only anchor without losing its CR chain.
 *   Why: Read-only versions and current buffers share one tag mapping.
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufferLookupEnt *result;
	bool		found;

#ifdef USE_PGRAC_CLUSTER
	if (buf_id < 0 || buf_id >= NBuffers || tagPtr == NULL ||
		tagPtr->blockNum == P_NEW)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid current buffer hash insertion")));
#endif
	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: preserve the read-only chain when filling a CR-only anchor. */
	if (found)
	{
		if (result->cr_head == buf_id)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("current buffer hash insertion aliases a CR buffer")));
		if (result->id >= 0)
			return result->id;
	}
	else if (!buf_table_init_anchor(result))
	{
		(void) hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
										  HASH_REMOVE, NULL);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("shared buffer anchor generation exhausted"),
				 errhint("Restart the server to reset the buffer mapping generation.")));
	}
#else
	if (found)					/* found something already in the table */
		return result->id;
#endif

	result->id = buf_id;

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 *   What changed: Remove only the current mapping from a nonempty CR anchor.
 *   Why: Current eviction must not orphan read-only versions.
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: current deletion must retain a nonempty CR chain. */
	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
									HASH_FIND, NULL);
	if (result == NULL || result->id < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("current buffer hash mapping is missing")));
	if (result->cr_head >= 0)
	{
		result->id = -1;
		return;
	}
#endif

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted");
}

#ifdef USE_PGRAC_CLUSTER
/*
 * Look up a nonempty CR chain without granting access to the current buffer.
 * Caller holds at least mapping-S.  False leaves both outputs unchanged.
 */
bool
BufTableCRLookup(BufferTag *tagPtr, uint32 hashcode, int *head,
				 uint64 *generation)
{
	BufferLookupEnt *result;

	if (tagPtr == NULL || tagPtr->blockNum == P_NEW ||
		head == NULL || generation == NULL)
		return false;
	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
									HASH_FIND, NULL);
	if (result == NULL || result->cr_head < 0)
		return false;
	*head = result->cr_head;
	*generation = result->anchor_generation;
	return true;
}

/*
 * Prepend a CR buffer, returning the previous chain head and anchor generation.
 * Caller holds mapping-X and validates the descriptor chain before insertion.
 * Capacity/identity refusal leaves the mapping and outputs unchanged.
 */
bool
BufTableCRInsert(BufferTag *tagPtr, uint32 hashcode, int cr_id,
				 int *old_head, uint64 *generation)
{
	BufferLookupEnt *result;
	bool		found;

	if (tagPtr == NULL || tagPtr->blockNum == P_NEW ||
		cr_id < 0 || cr_id >= NBuffers || old_head == NULL || generation == NULL)
		return false;
	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
									HASH_ENTER_NULL, &found);
	if (result == NULL)
		return false;
	if (found)
	{
		if (result->id == cr_id || result->cr_head == cr_id)
			return false;
	}
	else if (!buf_table_init_anchor(result))
	{
		(void) hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
										  HASH_REMOVE, NULL);
		return false;
	}
	*old_head = result->cr_head;
	*generation = result->anchor_generation;
	result->cr_head = cr_id;
	return true;
}

/*
 * Replace a CR chain head under mapping-X after validating its descriptor
 * links.  Both generation and head must still match the caller's observation.
 * Removing the last version deletes a CR-only anchor, never a current mapping.
 */
bool
BufTableCRReplaceHead(BufferTag *tagPtr, uint32 hashcode, uint64 generation,
					  int expected_head, int replacement_head)
{
	BufferLookupEnt *result;

	if (tagPtr == NULL || tagPtr->blockNum == P_NEW || generation == 0 ||
		expected_head < 0 || expected_head >= NBuffers ||
		replacement_head < -1 || replacement_head >= NBuffers ||
		replacement_head == expected_head)
		return false;
	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
									HASH_FIND, NULL);
	if (result == NULL || result->anchor_generation != generation ||
		result->cr_head != expected_head ||
		(replacement_head >= 0 && replacement_head == result->id))
		return false;
	if (replacement_head == -1 && result->id == -1)
		(void) hash_search_with_hash_value(SharedBufHash, tagPtr, hashcode,
										  HASH_REMOVE, NULL);
	else
		result->cr_head = replacement_head;
	return true;
}
#endif
