/*-------------------------------------------------------------------------
 * cluster_recovery_relmap_scan.c
 *   Native map observations through the original isolated ROOT reader.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_relmap_scan.h"
#include "catalog/pg_tablespace_d.h"
#ifdef USE_PGRAC_CLUSTER

enum RelmapStage { MAP_SELECT, MAP_READ, MAP_MAPPING, MAP_TARGET, MAP_REREAD };
struct ClusterRecoveryRelmapScan {
	ClusterRecoveryRootSet *roots;
	ClusterSpaceIdentityKey key;
	uint8 digest[32];
	uint32 next_root, map_index, ordinal, next_fork;
	const ClusterRecoveryRootEntry *entry;
	RelMapFile image, recheck;
	enum RelmapStage stage;
	ClusterRecoveryRelmapResult status;
	ClusterRecoveryRelmapDiag diag;
};

static ClusterRecoveryRelmapResult
relmap_fail(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapResult result)
{
	if (scan != NULL) {
		if (scan->status > CLUSTER_RECOVERY_RELMAP_DRAINED)
			return scan->status;
		scan->status = result;
		memset(&scan->image, 0, sizeof(scan->image));
		memset(&scan->recheck, 0, sizeof(scan->recheck));
		cluster_recovery_root_set_invalidate(scan->roots);
	}
	return result;
}

static bool
relmap_roots_current(const ClusterRecoveryRelmapScan *scan)
{
	uint8 digest[32];
	return scan != NULL && cluster_recovery_root_set_digest(scan->roots, digest)
		   && memcmp(scan->digest, digest, sizeof(digest)) == 0;
}

ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin(ClusterRecoveryRootSet *roots, uint64 system_identifier,
								   uint64 database_incarnation, const uint8 storage_uuid[16],
								   size_t memory_budget, ClusterRecoveryRelmapScan **out)
{
	ClusterRecoveryRelmapScan *scan;
	ClusterRecoveryRelmapResult result = CLUSTER_RECOVERY_RELMAP_INVALID;
	uint8 digest[32];
	if (out != NULL)
		*out = NULL;
	if (out == NULL
		|| !cluster_recovery_root_set_storage_matches(roots, system_identifier,
													  database_incarnation, storage_uuid)
		|| !cluster_recovery_root_set_digest(roots, digest))
		goto fail;
	result = CLUSTER_RECOVERY_RELMAP_CAPACITY;
	if (memory_budget < sizeof(*scan) + 1024)
		goto fail;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		goto fail;
	scan->roots = roots;
	scan->key.system_identifier = system_identifier;
	scan->key.database_incarnation = database_incarnation;
	memcpy(scan->key.storage_uuid, storage_uuid, 16);
	memcpy(scan->digest, digest, sizeof(digest));
	*out = scan;
	return CLUSTER_RECOVERY_RELMAP_MORE;
fail:
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

static void
relmap_identity(const ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapEntry *out)
{
	out->map_index = scan->map_index;
	out->file_kind = scan->entry->file.kind;
	out->key = scan->key;
	out->key.locator = scan->entry->file.locator;
	out->image = &scan->image;
	out->ordinal = scan->ordinal;
	out->target_index = UINT32_MAX;
	out->forknum = InvalidForkNumber;
}

static ClusterRecoveryRelmapResult
relmap_read(ClusterRecoveryRelmapScan *scan, RelMapFile *map)
{
	ClusterRecoveryRootsResult result
		= cluster_recovery_root_set_read(scan->roots, scan->map_index, 0, map, sizeof(*map));
	if (result == CLUSTER_RECOVERY_ROOTS_BYTES)
		return CLUSTER_RECOVERY_RELMAP_MORE;
	if (result == CLUSTER_RECOVERY_ROOTS_IO)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_IO);
	if (result == CLUSTER_RECOVERY_ROOTS_CAPACITY)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CAPACITY);
	return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
}

static bool
relmap_shape(const RelMapFile *map)
{
	pg_crc32c crc;
	if (map->magic != RELMAPPER_FILEMAGIC || map->num_mappings < 0
		|| map->num_mappings > MAX_MAPPINGS)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, map->crc))
		return false;
	/* Preserve duplicate/conflicting entries for semantic ownership; never
	 * collapse them to a chosen mapping or infer a publication generation. */
	for (int i = 0; i < map->num_mappings; ++i)
		if (!OidIsValid(map->mappings[i].mapoid) || !OidIsValid(map->mappings[i].mapfilenumber))
			return false;
	return true;
}

static ClusterRecoveryRelmapResult
relmap_step(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapEntry *out)
{
	static const ForkNumber forks[]
		= { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, SPACE_FORKNUM };
	ClusterRecoveryRelmapResult result;
	if (scan->stage == MAP_SELECT) {
		uint32 count = cluster_recovery_root_set_count(scan->roots);
		for (unsigned work = 0; work < 128 && scan->next_root < count; ++work) {
			const ClusterRecoveryRootEntry *entry;
			scan->map_index = scan->next_root++;
			entry = cluster_recovery_root_set_entry(scan->roots, scan->map_index);
			if (entry == NULL)
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
			if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELMAP
				&& entry->file.kind != CLUSTER_RECOVERY_FILE_RELMAP_BACKUP) {
				scan->diag.skipped_roots++;
				continue;
			}
			if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0
				|| entry->file.size < sizeof(RelMapFile))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
			if (entry->file.size != sizeof(RelMapFile)
				|| !((entry->file.locator.spcOid == GLOBALTABLESPACE_OID
					  && entry->file.locator.dbOid == InvalidOid)
					 || (entry->file.locator.spcOid == DEFAULTTABLESPACE_OID
						 && OidIsValid(entry->file.locator.dbOid))))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
			scan->entry = entry;
			scan->stage = MAP_READ;
			return CLUSTER_RECOVERY_RELMAP_MORE;
		}
		if (scan->next_root == count)
			scan->status = CLUSTER_RECOVERY_RELMAP_DRAINED;
		return scan->status;
	}
	if (scan->stage == MAP_READ) {
		result = relmap_read(scan, &scan->image);
		if (result != CLUSTER_RECOVERY_RELMAP_MORE)
			return result;
		if (!relmap_shape(&scan->image))
			return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
		scan->ordinal = 0;
		scan->stage = MAP_MAPPING;
		scan->diag.images++;
		relmap_identity(scan, out);
		out->kind = CLUSTER_RECOVERY_RELMAP_IMAGE;
		return CLUSTER_RECOVERY_RELMAP_ENTRY;
	}
	if (scan->stage == MAP_MAPPING && scan->ordinal == (uint32)scan->image.num_mappings)
		scan->stage = MAP_REREAD;
	if (scan->stage == MAP_MAPPING) {
		relmap_identity(scan, out);
		out->kind = CLUSTER_RECOVERY_RELMAP_MAPPING;
		out->mapping = scan->image.mappings[scan->ordinal];
		out->key.locator.relNumber = out->mapping.mapfilenumber;
		scan->next_fork = 0;
		scan->stage = MAP_TARGET;
		scan->diag.mappings++;
		return CLUSTER_RECOVERY_RELMAP_ENTRY;
	}
	if (scan->stage == MAP_TARGET) {
		ClusterRecoveryRootsResult found;
		relmap_identity(scan, out);
		out->kind = CLUSTER_RECOVERY_RELMAP_TARGET;
		out->mapping = scan->image.mappings[scan->ordinal];
		out->key.locator.relNumber = out->mapping.mapfilenumber;
		out->forknum = forks[scan->next_fork];
		found = cluster_recovery_root_set_find_relation(scan->roots, &out->key, out->forknum,
														&out->target_index);
		if (found != CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return relmap_fail(scan, found == CLUSTER_RECOVERY_ROOTS_CAPACITY
										 ? CLUSTER_RECOVERY_RELMAP_CAPACITY
										 : CLUSTER_RECOVERY_RELMAP_CHANGED);
		out->target = cluster_recovery_root_set_entry(scan->roots, out->target_index);
		out->physical
			= out->target != NULL && (out->target->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0;
		out->extent_present
			= out->physical && out->target->file.size / BLCKSZ >= out->target->required_blocks;
		scan->diag.targets++;
		scan->diag.missing_targets += !out->physical;
		scan->diag.short_targets += out->physical && !out->extent_present;
		if (++scan->next_fork == lengthof(forks)) {
			scan->ordinal++;
			scan->stage = MAP_MAPPING;
		}
		return CLUSTER_RECOVERY_RELMAP_ENTRY;
	}
	result = relmap_read(scan, &scan->recheck);
	if (result != CLUSTER_RECOVERY_RELMAP_MORE)
		return result;
	if (memcmp(&scan->image, &scan->recheck, sizeof(scan->image)) != 0)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
	scan->diag.rereads++;
	scan->stage = MAP_SELECT;
	return CLUSTER_RECOVERY_RELMAP_MORE;
}

ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_next(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapEntry *out)
{
	ClusterRecoveryRelmapResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_INVALID);
	if (scan->status > CLUSTER_RECOVERY_RELMAP_DRAINED)
		return scan->status;
	if (!relmap_roots_current(scan))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
	if (scan->status == CLUSTER_RECOVERY_RELMAP_DRAINED)
		return scan->status;
	result = relmap_step(scan, out);
	if (result > CLUSTER_RECOVERY_RELMAP_DRAINED)
		memset(out, 0, sizeof(*out));
	return result;
}

bool
cluster_recovery_relmap_scan_diag(const ClusterRecoveryRelmapScan *scan,
								  ClusterRecoveryRelmapDiag *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || scan == NULL || scan->status > CLUSTER_RECOVERY_RELMAP_DRAINED
		|| !relmap_roots_current(scan))
		return false;
	*out = scan->diag;
	return true;
}
void
cluster_recovery_relmap_scan_destroy(ClusterRecoveryRelmapScan *scan)
{
	free(scan);
}
#endif
