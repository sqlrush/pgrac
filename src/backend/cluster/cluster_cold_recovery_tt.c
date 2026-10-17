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

typedef struct ClusterColdTtBaseV1 {
	bool owners[COLD_TT_MAX_ORIGINS + 1];
	ColdTtInputV1 *first;
	ColdTtInputV1 *last;
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

static void
cold_tt_clear(ClusterColdTtBaseV1 *base)
{
	ColdTtInputV1 *input = base->first;

	while (input != NULL) {
		ColdTtInputV1 *next = input->next;

		pfree(input);
		input = next;
	}
	base->first = base->last = NULL;
	base->count = 0;
	base->used = sizeof(*base);
}

ClusterColdDetailV1
cluster_cold_tt_base_capture_v1(ClusterColdTypedV1 *typed, Size budget)
{
	ClusterColdTtBaseV1 *base;
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	if (!cold_tt_owners(typed, owners))
		return CLUSTER_COLD_PARTICIPANT_INVALID;
	if (typed->tt_base != NULL)
		return CLUSTER_COLD_STATE;
	if (budget < sizeof(*base))
		return CLUSTER_COLD_CAPACITY;
	base = palloc_extended(sizeof(*base), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (base == NULL)
		return CLUSTER_COLD_OOM;
	typed->tt_base = base;
	base->used = sizeof(*base);
	base->budget = budget;
	base->detail = CLUSTER_COLD_SOURCE_GAP;
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
cluster_cold_tt_base_recheck_v1(ClusterColdTypedV1 *typed)
{
	ClusterColdTtBaseV1 *base;
	ColdTtInputV1 *saved;
	bool owners[COLD_TT_MAX_ORIGINS + 1];

	if (typed == NULL || typed->tt_base == NULL)
		return CLUSTER_COLD_STATE;
	base = typed->tt_base;
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
	base->detail = CLUSTER_COLD_OK;
	return CLUSTER_COLD_OK;
}

void
cluster_cold_tt_base_destroy_v1(ClusterColdTypedV1 *typed)
{
	ClusterColdTtBaseV1 *base;

	if (typed == NULL || typed->tt_base == NULL)
		return;
	base = typed->tt_base;
	typed->tt_base = NULL;
	cold_tt_clear(base);
	pfree(base);
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
