/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_tt.c
 *    Original cold Startup ownership of raw persistent TT base inputs.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_cold_recovery_tt.c
 *
 * NOTES
 *    PGRAC-original. Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *    This captures bytes, not checkpoint coverage or transaction outcomes.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/fd.h"
#include "utils/memutils.h"

#define COLD_TT_MAX_ORIGINS UNDO_OWNER_INSTANCE_MAX

typedef struct ColdTtInputV1 {
	struct ColdTtInputV1 *next;
	uint8 owner;
	uint32 segment;
	dev_t device;
	ino_t inode;
	off_t size;
	PGAlignedBlock block;
} ColdTtInputV1;

typedef struct ColdTtBlockV1 {
	struct ColdTtBlockV1 *next;
	const ColdTtInputV1 *file;
	uint32 block;
	PGAlignedBlock bytes;
} ColdTtBlockV1;

typedef struct ClusterColdTtBaseV1 {
	bool owners[COLD_TT_MAX_ORIGINS + 1];
	uint64 system_identifier;
	uint64 database_incarnation;
	ColdTtInputV1 *first;
	ColdTtInputV1 *last;
	ColdTtBlockV1 *blocks;
	Size used;
	Size budget;
	uint32 count;
	ClusterColdDetailV1 detail;
	bool captured;
} ClusterColdTtBaseV1;

typedef enum ColdTtReadV1 { COLD_TT_ABSENT, COLD_TT_PRESENT, COLD_TT_UNREADABLE } ColdTtReadV1;

static bool
cold_tt_owners(const ClusterColdTypedV1 *typed, bool *owners)
{
	memset(owners, 0, sizeof(bool) * (COLD_TT_MAX_ORIGINS + 1));
	if (typed == NULL || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS)
		return false;
	for (uint32 i = 0; i < typed->participant_count; i++) {
		uint16 owner = typed->participants[i].thread_id;

		if (owner == 0 || owner > COLD_TT_MAX_ORIGINS)
			return false;
		/* Different WAL generations may name the same physical TT origin. */
		owners[owner] = true;
	}
	return true;
}

static bool
cold_tt_same_file(const struct stat *a, const struct stat *b)
{
	return S_ISREG(a->st_mode) && S_ISREG(b->st_mode) && a->st_dev == b->st_dev
		   && a->st_ino == b->st_ino && a->st_size == b->st_size;
}

/* No create, mutation, scope admission or fsync. The original Startup owns
 * source selection and isolation; a path lookup grants neither. ENOENT is
 * the only absent result. Do not use an observer's optional-file policy. */
static ColdTtReadV1
cold_tt_read(uint8 owner, uint32 segment, ColdTtInputV1 *input)
{
	char path[MAXPGPATH];
	struct stat before, after, named;
	ColdTtReadV1 result = COLD_TT_UNREADABLE;
	int flags = O_RDONLY | PG_BINARY;
	int fd;

#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	if (cluster_shared_fs_undo_path_resolve(owner, segment, path, sizeof(path)) != 0)
		return COLD_TT_UNREADABLE;
	fd = BasicOpenFile(path, flags);
	if (fd < 0)
		return errno == ENOENT ? COLD_TT_ABSENT : COLD_TT_UNREADABLE;
	if (fstat(fd, &before) != 0 || !S_ISREG(before.st_mode)
		|| before.st_size != UNDO_SEGMENT_SIZE_BYTES)
		goto done;
	if (pg_pread(fd, input->block.data, BLCKSZ, 0) != BLCKSZ
		|| !UndoSegmentHeader_identity_matches(input->block.data, segment, owner))
		goto done;
	if (fstat(fd, &after) != 0 || lstat(path, &named) != 0 || !cold_tt_same_file(&before, &after)
		|| !cold_tt_same_file(&after, &named))
		goto done;
	input->owner = owner;
	input->segment = segment;
	input->device = after.st_dev;
	input->inode = after.st_ino;
	input->size = after.st_size;
	result = COLD_TT_PRESENT;
done:
	if (close(fd) != 0)
		result = COLD_TT_UNREADABLE;
	return result;
}

/* The original owner still supplies isolation. Bind the DATA read to the
 * captured file identity and complete block zero, not just a segment number. */
static bool
cold_tt_read_data(const ColdTtInputV1 *input, uint32 block, char *bytes)
{
	char path[MAXPGPATH];
	PGAlignedBlock header;
	struct stat before, after, named;
	int flags = O_RDONLY | PG_BINARY;
	int fd;
	bool result = false;

#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	if (cluster_shared_fs_undo_path_resolve(input->owner, input->segment, path, sizeof(path)) != 0)
		return false;
	fd = BasicOpenFile(path, flags);
	if (fd < 0)
		return false;
	if (fstat(fd, &before) != 0 || !S_ISREG(before.st_mode) || before.st_dev != input->device
		|| before.st_ino != input->inode || before.st_size != input->size)
		goto done;
	if (pg_pread(fd, bytes, BLCKSZ, (off_t)block * BLCKSZ) != BLCKSZ || fstat(fd, &after) != 0
		|| lstat(path, &named) != 0 || !cold_tt_same_file(&before, &after)
		|| !cold_tt_same_file(&after, &named))
		goto done;
	if (pg_pread(fd, header.data, BLCKSZ, 0) != BLCKSZ
		|| memcmp(header.data, input->block.data, BLCKSZ) != 0 || fstat(fd, &after) != 0
		|| lstat(path, &named) != 0 || !cold_tt_same_file(&before, &after)
		|| !cold_tt_same_file(&after, &named))
		goto done;
	result = true;
done:
	if (close(fd) != 0)
		result = false;
	return result;
}

static void
cold_tt_clear(ClusterColdTtBaseV1 *base)
{
	ColdTtInputV1 *input = base->first;
	ColdTtBlockV1 *block = base->blocks;

	while (block != NULL) {
		ColdTtBlockV1 *next = block->next;

		pfree(block);
		block = next;
	}
	base->blocks = NULL;

	while (input != NULL) {
		ColdTtInputV1 *next = input->next;

		pfree(input);
		input = next;
	}
	base->first = base->last = NULL;
	base->count = 0;
	base->used = sizeof(*base);
}

static ClusterColdDetailV1
cold_tt_capture(ClusterColdTypedV1 *typed, Size budget, ClusterColdTtBaseV1 **target)
{
	ClusterColdTtBaseV1 *base;
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	if (!cold_tt_owners(typed, owners))
		return CLUSTER_COLD_PARTICIPANT_INVALID;
	if (*target != NULL)
		return CLUSTER_COLD_STATE;
	if (budget < sizeof(*base))
		return CLUSTER_COLD_CAPACITY;
	base = palloc_extended(sizeof(*base), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (base == NULL)
		return CLUSTER_COLD_OOM;
	*target = base;
	base->used = sizeof(*base);
	base->budget = budget;
	base->detail = CLUSTER_COLD_SOURCE_GAP;
	base->system_identifier = typed->system_identifier;
	base->database_incarnation = typed->observer.database_incarnation;
	memcpy(base->owners, owners, sizeof(owners));
	for (uint16 owner = 1; owner <= COLD_TT_MAX_ORIGINS; owner++) {
		if (!owners[owner])
			continue;
		for (uint32 n = 0; n < CLUSTER_UNDO_SEGS_PER_INSTANCE; n++) {
			uint32 segment = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + n + 1;
			ColdTtInputV1 observed = { 0 };
			ColdTtInputV1 *saved;
			ColdTtReadV1 result = cold_tt_read((uint8)owner, segment, &observed);

			if (result == COLD_TT_ABSENT)
				continue;
			if (result != COLD_TT_PRESENT)
				goto refused;
			if (sizeof(*saved) > base->budget - base->used) {
				base->detail = CLUSTER_COLD_CAPACITY;
				goto refused;
			}
			saved = palloc_extended(sizeof(*saved), MCXT_ALLOC_NO_OOM);
			if (saved == NULL) {
				base->detail = CLUSTER_COLD_OOM;
				goto refused;
			}
			*saved = observed;
			saved->next = NULL;
			if (base->last == NULL)
				base->first = saved;
			else
				base->last->next = saved;
			base->last = saved;
			base->count++;
			base->used += sizeof(*saved);
		}
	}
	base->captured = true;
	base->detail = CLUSTER_COLD_OK;
	return CLUSTER_COLD_OK;
refused:
	cold_tt_clear(base);
	return base->detail;
}

ClusterColdDetailV1
cluster_cold_tt_base_capture_v1(ClusterColdTypedV1 *typed, Size budget)
{
	if (typed == NULL)
		return CLUSTER_COLD_PARTICIPANT_INVALID;
	return cold_tt_capture(typed, budget, &typed->tt_base);
}

static ClusterColdDetailV1
cold_tt_recheck(ClusterColdTypedV1 *typed, ClusterColdTtBaseV1 *base)
{
	ColdTtInputV1 *saved;
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	if (typed == NULL || base == NULL)
		return CLUSTER_COLD_STATE;
	if (!base->captured || base->detail != CLUSTER_COLD_OK)
		return base->detail;
	/* Any interruption or refusal leaves the snapshot unusable. */
	base->detail = CLUSTER_COLD_SOURCE_GAP;
	if (!cold_tt_owners(typed, owners) || memcmp(owners, base->owners, sizeof(owners)) != 0)
		return base->detail;
	saved = base->first;
	for (uint16 owner = 1; owner <= COLD_TT_MAX_ORIGINS; owner++) {
		if (!owners[owner])
			continue;
		for (uint32 n = 0; n < CLUSTER_UNDO_SEGS_PER_INSTANCE; n++) {
			uint32 segment = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + n + 1;
			ColdTtInputV1 observed = { 0 };
			ColdTtReadV1 result = cold_tt_read((uint8)owner, segment, &observed);
			bool expected = saved != NULL && saved->owner == owner && saved->segment == segment;

			if (result == COLD_TT_UNREADABLE || expected != (result == COLD_TT_PRESENT))
				return base->detail;
			if (!expected)
				continue;
			if (saved->device != observed.device || saved->inode != observed.inode
				|| saved->size != observed.size
				|| memcmp(saved->block.data, observed.block.data, BLCKSZ) != 0)
				return base->detail;
			saved = saved->next;
		}
	}
	if (saved != NULL)
		return base->detail;
	for (const ColdTtBlockV1 *block = base->blocks; block != NULL; block = block->next) {
		PGAlignedBlock observed;

		if (!cold_tt_read_data(block->file, block->block, observed.data)
			|| memcmp(block->bytes.data, observed.data, BLCKSZ) != 0)
			return base->detail;
	}
	base->detail = CLUSTER_COLD_OK;
	return CLUSTER_COLD_OK;
}

ClusterColdDetailV1
cluster_cold_tt_base_recheck_v1(ClusterColdTypedV1 *typed)
{
	return cold_tt_recheck(typed, typed == NULL ? NULL : typed->tt_base);
}

static void
cold_tt_destroy(ClusterColdTtBaseV1 **target)
{
	ClusterColdTtBaseV1 *base = *target;

	if (base == NULL)
		return;
	*target = NULL;
	cold_tt_clear(base);
	pfree(base);
}

void
cluster_cold_tt_after_redo_reset_v1(ClusterColdTypedV1 *typed)
{
	if (typed != NULL)
		cold_tt_destroy(&typed->tt_after_redo);
}

void
cluster_cold_tt_base_destroy_v1(ClusterColdTypedV1 *typed)
{
	if (typed == NULL)
		return;
	cluster_cold_tt_after_redo_reset_v1(typed);
	cold_tt_destroy(&typed->tt_base);
}

uint32
cluster_cold_tt_base_count_v1(const ClusterColdTypedV1 *typed)
{
	if (typed == NULL || typed->tt_base == NULL || !typed->tt_base->captured
		|| typed->tt_base->detail != CLUSTER_COLD_OK)
		return 0;
	return typed->tt_base->count;
}

const UndoSegmentHeaderData *
cluster_cold_tt_base_header_v1(const ClusterColdTypedV1 *typed, uint32 index, uint8 *owner,
							   uint32 *segment)
{
	const ColdTtInputV1 *input;

	if (owner == NULL || segment == NULL || index >= cluster_cold_tt_base_count_v1(typed))
		return NULL;
	input = typed->tt_base->first;
	while (index-- > 0)
		input = input->next;
	*owner = input->owner;
	*segment = input->segment;
	return (const UndoSegmentHeaderData *)input->block.data;
}

static bool
cold_tt_namespace_matches(const ClusterColdTypedV1 *typed, const ClusterColdTtBaseV1 *base)
{
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	return typed != NULL && base != NULL && base->captured && base->detail == CLUSTER_COLD_OK
		   && base->system_identifier != 0 && base->database_incarnation != 0
		   && typed->system_identifier == base->system_identifier
		   && typed->observer.database_incarnation == base->database_incarnation
		   && cold_tt_owners(typed, owners) && memcmp(owners, base->owners, sizeof(owners)) == 0;
}

bool
cluster_cold_tt_base_namespace_matches_v1(const ClusterColdTypedV1 *typed, uint64 system_identifier,
										  uint64 database_incarnation)
{
	return typed != NULL && typed->refusal == CLUSTER_COLD_OK
		   && cold_tt_namespace_matches(typed, typed->tt_base)
		   && typed->system_identifier == system_identifier
		   && typed->observer.database_incarnation == database_incarnation;
}

ClusterColdDetailV1
cluster_cold_tt_after_redo_capture_v1(ClusterColdTypedV1 *typed)
{
	const ClusterColdTtBaseV1 *base;

	/* The caller temporarily marks the attempt incomplete while checking all
	 * original sources. Namespace and pre-redo input must remain intact. */
	if (typed == NULL || !cold_tt_namespace_matches(typed, typed->tt_base))
		return CLUSTER_COLD_SOURCE_GAP;
	base = typed->tt_base;
	if (typed->tt_after_redo != NULL)
		return CLUSTER_COLD_STATE;
	if (base->used > base->budget)
		return CLUSTER_COLD_CAPACITY;
	return cold_tt_capture(typed, base->budget - base->used, &typed->tt_after_redo);
}

ClusterColdDetailV1
cluster_cold_tt_after_redo_recheck_v1(ClusterColdTypedV1 *typed)
{
	if (typed == NULL || typed->tt_after_redo == NULL)
		return CLUSTER_COLD_STATE;
	if (!cold_tt_namespace_matches(typed, typed->tt_base)
		|| !cold_tt_namespace_matches(typed, typed->tt_after_redo)) {
		typed->tt_after_redo->detail = CLUSTER_COLD_SOURCE_GAP;
		return CLUSTER_COLD_SOURCE_GAP;
	}
	return cold_tt_recheck(typed, typed->tt_after_redo);
}

uint32
cluster_cold_tt_after_redo_count_v1(const ClusterColdTypedV1 *typed)
{
	if (!cluster_cold_tt_after_redo_namespace_matches_v1(typed))
		return 0;
	return typed->tt_after_redo->count;
}

bool
cluster_cold_tt_after_redo_namespace_matches_v1(const ClusterColdTypedV1 *typed)
{
	return typed != NULL && typed->refusal == CLUSTER_COLD_OK
		   && cold_tt_namespace_matches(typed, typed->tt_base)
		   && cold_tt_namespace_matches(typed, typed->tt_after_redo);
}

const UndoSegmentHeaderData *
cluster_cold_tt_header_find_v1(const ClusterColdTypedV1 *typed, bool after_redo, uint8 owner,
							   uint32 segment, uint32 *index)
{
	const ClusterColdTtBaseV1 *base;
	uint32 i = 0;

	if (index != NULL)
		*index = UINT32_MAX;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK
		|| !cold_tt_namespace_matches(typed, typed->tt_base)
		|| (after_redo && !cluster_cold_tt_after_redo_namespace_matches_v1(typed)))
		return NULL;
	base = after_redo ? typed->tt_after_redo : typed->tt_base;
	for (const ColdTtInputV1 *input = base->first; input != NULL; input = input->next, i++) {
		if (input->owner != owner || input->segment != segment)
			continue;
		if (index != NULL)
			*index = i;
		return (const UndoSegmentHeaderData *)input->block.data;
	}
	return NULL;
}

const UndoSegmentHeaderData *
cluster_cold_tt_after_redo_header_v1(const ClusterColdTypedV1 *typed, uint32 index, uint8 *owner,
									 uint32 *segment)
{
	const ColdTtInputV1 *input;

	if (owner != NULL)
		*owner = 0;
	if (segment != NULL)
		*segment = 0;
	if (owner == NULL || segment == NULL || index >= cluster_cold_tt_after_redo_count_v1(typed))
		return NULL;
	input = typed->tt_after_redo->first;
	while (index-- > 0)
		input = input->next;
	*owner = input->owner;
	*segment = input->segment;
	return (const UndoSegmentHeaderData *)input->block.data;
}

static void
cold_tt_slot_copy(const ColdTtInputV1 *input, uint32 slot_offset, ClusterColdTtSlotInputV1 *out)
{
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)input->block.data;
	const TTSlot *slot = &header->tt_slots[slot_offset];

	out->key.owner_instance = input->owner;
	out->key.segment_id = input->segment;
	out->key.segment_generation = header->wrap_count;
	out->key.slot_offset = slot_offset;
	out->key.xid = slot->xid;
	out->key.slot_wrap = slot->wrap;
	out->slot = *slot;
}

static bool
cold_tt_slot(const ClusterColdTtBaseV1 *base, uint32 count, uint32 segment_index,
			 uint32 slot_offset, ClusterColdTtSlotInputV1 *out)
{
	const ColdTtInputV1 *input;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (slot_offset >= TT_SLOTS_PER_SEGMENT || segment_index >= count)
		return false;
	input = base->first;
	while (segment_index-- > 0)
		input = input->next;
	cold_tt_slot_copy(input, slot_offset, out);
	return true;
}

bool
cluster_cold_tt_base_slot_v1(const ClusterColdTypedV1 *typed, uint32 segment_index,
							 uint32 slot_offset, ClusterColdTtSlotInputV1 *out)
{
	return cold_tt_slot(typed == NULL ? NULL : typed->tt_base, cluster_cold_tt_base_count_v1(typed),
						segment_index, slot_offset, out);
}

bool
cluster_cold_tt_after_redo_slot_v1(const ClusterColdTypedV1 *typed, uint32 segment_index,
								   uint32 slot_offset, ClusterColdTtSlotInputV1 *out)
{
	return cold_tt_slot(typed == NULL ? NULL : typed->tt_after_redo,
						cluster_cold_tt_after_redo_count_v1(typed), segment_index, slot_offset,
						out);
}

bool
cluster_cold_tt_base_match_v1(const ClusterColdTypedV1 *typed,
							  const ClusterColdTtSlotKeyV1 *expected, ClusterColdTtSlotInputV1 *out)
{
	ClusterColdTtSlotKeyV1 key;
	const ColdTtInputV1 *input;

	if (out == NULL)
		return false;
	/* A caller may use its previous input copy's key as the expectation. */
	if (expected != NULL)
		key = *expected;
	memset(out, 0, sizeof(*out));
	if (expected == NULL || key.owner_instance == 0 || key.owner_instance > COLD_TT_MAX_ORIGINS
		|| key.segment_id == 0
		|| (key.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != key.owner_instance
		|| key.slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(key.xid)
		|| key.slot_wrap == TT_WRAP_INVALID || cluster_cold_tt_base_count_v1(typed) == 0)
		return false;
	for (input = typed->tt_base->first; input != NULL; input = input->next) {
		const UndoSegmentHeaderData *header;
		const TTSlot *slot;

		if (input->owner != key.owner_instance || input->segment != key.segment_id)
			continue;
		header = (const UndoSegmentHeaderData *)input->block.data;
		slot = &header->tt_slots[key.slot_offset];
		if (header->wrap_count != key.segment_generation || slot->xid != key.xid
			|| slot->wrap != key.slot_wrap)
			return false;
		cold_tt_slot_copy(input, key.slot_offset, out);
		return true;
	}
	return false;
}

static const ColdTtBlockV1 *
cold_tt_undo_block(ClusterColdTypedV1 *typed, uint32 segment, uint32 blockno, bool fresh)
{
	ClusterColdTtBaseV1 *base;
	const ColdTtInputV1 *file;
	ColdTtBlockV1 *block, *allocated = NULL;
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	if (typed == NULL || typed->tt_base == NULL)
		return NULL;
	base = typed->tt_base;
	if (!base->captured || base->detail != CLUSTER_COLD_OK)
		return NULL;
	base->detail = CLUSTER_COLD_SOURCE_GAP;
	if (typed->refusal != CLUSTER_COLD_OK || !cold_tt_owners(typed, owners)
		|| memcmp(owners, base->owners, sizeof(owners)) != 0 || segment == 0 || blockno == 0
		|| blockno >= UNDO_BLOCKS_PER_SEGMENT)
		return NULL;
	for (file = base->first; file != NULL; file = file->next)
		if (file->segment == segment)
			break;
	if (file == NULL)
		return NULL;
	for (block = base->blocks; block != NULL; block = block->next)
		if (block->file == file && block->block == blockno)
			break;
	if (block == NULL) {
		Size remaining = base->used <= base->budget ? base->budget - base->used : 0;

		/* Later raw UNDO reads share the same budget with the post-redo TT. */
		if (typed->tt_after_redo != NULL)
			remaining = typed->tt_after_redo->used <= remaining
							? remaining - typed->tt_after_redo->used
							: 0;
		if (sizeof(*block) > remaining) {
			base->detail = CLUSTER_COLD_CAPACITY;
			return NULL;
		}
		allocated = palloc_extended(sizeof(*block), MCXT_ALLOC_NO_OOM);
		if (allocated == NULL) {
			base->detail = CLUSTER_COLD_OOM;
			return NULL;
		}
		block = allocated;
		block->file = file;
		block->block = blockno;
		if (!cold_tt_read_data(file, blockno, block->bytes.data))
			goto refused;
	} else if (fresh) {
		PGAlignedBlock observed;

		/* The graph verifies each record in a second read. A cache hit
		 * must retain the original source's full-byte stability check. */
		if (!cold_tt_read_data(file, blockno, observed.data)
			|| memcmp(observed.data, block->bytes.data, BLCKSZ) != 0)
			goto refused;
	}
	if (allocated != NULL) {
		block->next = base->blocks;
		base->blocks = block;
		base->used += sizeof(*block);
	}
	base->detail = CLUSTER_COLD_OK;
	return block;
refused:
	if (allocated != NULL)
		pfree(allocated);
	return NULL;
}

/* Keep raw DATA capture separate from record parsing: a valid redo delta may
 * introduce the requested record into an already retained original block. */
bool
cluster_cold_tt_base_block_v1(ClusterColdTypedV1 *typed, uint32 segment_index, uint32 blockno,
							  PGAlignedBlock *out, uint32 *generation)
{
	const UndoSegmentHeaderData *header;
	const ColdTtBlockV1 *block;
	uint8 owner;
	uint32 segment;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (out == NULL || generation == NULL
		|| (header = cluster_cold_tt_base_header_v1(typed, segment_index, &owner, &segment))
			   == NULL)
		return false;
	if (typed->system_identifier != typed->tt_base->system_identifier
		|| typed->observer.database_incarnation != typed->tt_base->database_incarnation) {
		typed->tt_base->detail = CLUSTER_COLD_SOURCE_GAP;
		return false;
	}
	block = cold_tt_undo_block(typed, segment, blockno, true);
	if (block == NULL)
		return false;
	*out = block->bytes;
	*generation = header->wrap_count;
	return true;
}

static bool
cold_tt_undo_record(ClusterColdTypedV1 *typed, UBA address, ClusterRecoveryUndoRecord *out,
					uint32 *generation, bool fresh)
{
	const ColdTtBlockV1 *block;
	ClusterRecoveryUndoRecord record;
	uint32 segment, blockno;
	uint16 slot, row;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (typed == NULL || out == NULL || generation == NULL || typed->tt_base == NULL)
		return false;
	if (!uba_decode_record(address, &segment, &blockno, &slot, &row)) {
		if (typed->tt_base->detail == CLUSTER_COLD_OK)
			typed->tt_base->detail = CLUSTER_COLD_SOURCE_GAP;
		return false;
	}
	block = cold_tt_undo_block(typed, segment, blockno, fresh);
	if (block == NULL)
		return false;
	if (cluster_recovery_undo_record_read(block->bytes.data, BLCKSZ, segment, blockno,
										  block->file->owner, address, &record)
		!= CLUSTER_RECOVERY_UNDO_ENTRY) {
		typed->tt_base->detail = CLUSTER_COLD_SOURCE_GAP;
		return false;
	}
	*out = record;
	*generation = ((const UndoSegmentHeaderData *)block->file->block.data)->wrap_count;
	return true;
}

bool
cluster_cold_tt_undo_record_v1(ClusterColdTypedV1 *typed, UBA address,
							   ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	return cold_tt_undo_record(typed, address, out, generation, false);
}

ClusterRecoveryGraphResult
cluster_cold_tt_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
							  ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	ClusterColdTypedV1 *typed = arg;
	ClusterColdTtBaseV1 *base;
	ClusterColdTtSlotKeyV1 expected = { 0 };
	ClusterColdTtSlotInputV1 input;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_INVALID;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (typed == NULL || typed->tt_base == NULL)
		return result;
	base = typed->tt_base;
	if (!base->captured || base->detail != CLUSTER_COLD_OK)
		goto refused;
	if (key == NULL || out == NULL || generation == NULL)
		goto refused;
	result = CLUSTER_RECOVERY_GRAPH_IDENTITY;
	if (base->system_identifier == 0 || base->database_incarnation == 0
		|| typed->system_identifier != base->system_identifier
		|| typed->observer.database_incarnation != base->database_incarnation
		|| key->system_identifier != base->system_identifier
		|| key->database_incarnation != base->database_incarnation
		|| key->origin_node_id >= COLD_TT_MAX_ORIGINS)
		goto refused;
	expected.owner_instance = (uint8)(key->origin_node_id + 1);
	expected.segment_id = key->segment_id;
	expected.segment_generation = key->segment_generation;
	expected.slot_offset = key->slot_offset;
	expected.xid = key->xid;
	expected.slot_wrap = key->slot_wrap;
	if (!cluster_cold_tt_base_match_v1(typed, &expected, &input))
		goto refused;
	result = CLUSTER_RECOVERY_GRAPH_INVALID;
	if (!cold_tt_undo_record(typed, address, out, generation, true))
		goto refused;
	result = cluster_recovery_tx_record_check(key, address, out, *generation);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		goto refused;
	return CLUSTER_RECOVERY_GRAPH_RECORD;
refused:
	if (base->detail == CLUSTER_COLD_CAPACITY || base->detail == CLUSTER_COLD_OOM)
		result = CLUSTER_RECOVERY_GRAPH_CAPACITY;
	else if (base->detail == CLUSTER_COLD_OK)
		base->detail = CLUSTER_COLD_SOURCE_GAP;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	return result;
}
