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

typedef struct ManagedRelmapScan {
	ClusterControlRelmapSelectionV2 *selection; /* borrowed, never destroyed */
	ClusterControlRelmapReferenceV2 reference;
	ClusterControlNativeTableInfoV2 table;
	ClusterControlNativeFileBorrowV2 native;
	bool shared_table, release_attempted;
	ClusterRecoveryFileIdentityV2 identity, pair_identity;
	ClusterRelmapV2Image image;
	uint8 copies[2][CLUSTER_RELMAP_FILE_SIZE_V2];
	uint8 reread[CLUSTER_RELMAP_FILE_SIZE_V2];
	uint64 completed_copies;
	bool backup, await_backup, pending;
} ManagedRelmapScan;

enum RelmapStage { MAP_SELECT, MAP_READ, MAP_MAPPING, MAP_TARGET, MAP_REREAD };
struct ClusterRecoveryRelmapScan {
	ClusterRecoveryRootSet *roots;
	ManagedRelmapScan *managed;
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
		if (scan->managed != NULL) {
			memset(&scan->managed->image, 0, sizeof(scan->managed->image));
			memset(scan->managed->copies, 0, sizeof(scan->managed->copies));
			memset(scan->managed->reread, 0, sizeof(scan->managed->reread));
		}
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

/* No CF or I/O: borrowed count is local activity, not table identity. */
static bool
relmap_table_current(const ClusterControlNativeTableInfoV2 *table)
{
	ClusterControlNativeTableInfoV2 now, expected = *table;
	if (!cluster_control_native_files_info_v2(&now))
		return false;
	now.borrowed = expected.borrowed = 0;
	return memcmp(&now, &expected, sizeof(now)) == 0;
}

static void
relmap_native_release(ManagedRelmapScan *m)
{
	if (m->native.private_self == 0)
		return;
	if (m->release_attempted)
		ereport(PANIC, (errmsg("managed relation map scan has an unfinished file borrow release")));
	m->release_attempted = true;
	if (!cluster_control_native_file_release_v2(&m->native))
		ereport(PANIC, (errmsg("cannot release managed relation map scan file borrow")));
	m->release_attempted = false;
}

/* The reference getter checks original continuous CF custody without I/O. */
static bool
relmap_control_current(const ClusterRecoveryRelmapScan *scan, bool reread)
{
	ManagedRelmapScan *m = scan->managed;
	ClusterControlRelmapReferenceV2 now;
	if (m == NULL)
		return true;
	if (m->shared_table)
		return !m->release_attempted && relmap_table_current(&m->table)
			   && (m->native.private_self == 0
				   || cluster_control_native_file_current_v2(&m->native));
	if (reread && cluster_control_root_relmap_selection_recheck_locked_v4(m->selection)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	return cluster_control_root_relmap_selection_reference_v4(m->selection, &now)
		&& memcmp(&m->reference, &now, sizeof(now)) == 0;
}

ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin_managed_v2(ClusterRecoveryRootSet *roots,
	ClusterControlRelmapSelectionV2 *selection, size_t memory_budget,
	ClusterRecoveryRelmapScan **out)
{
	ClusterRecoveryRelmapScan *volatile scan = NULL;
	ClusterRecoveryRelmapResult result = CLUSTER_RECOVERY_RELMAP_INVALID;
	ClusterControlRelmapReferenceV2 reference;
	if (!cluster_recovery_root_set_output_distinct(roots, out, sizeof(*out))
		|| !cluster_control_root_relmap_selection_output_distinct_v4(selection, out, sizeof(*out))
		|| *out != NULL)
		goto fail;
	PG_TRY();
	{
		if (memory_budget < sizeof(ManagedRelmapScan) + sizeof(*scan) + 1024)
			result = CLUSTER_RECOVERY_RELMAP_CAPACITY;
		else if (cluster_control_root_relmap_selection_reference_v4(selection, &reference)
			&& reference.logical_count != 0 && reference.logical_count <= UINT64_MAX / 2) {
			ClusterRecoveryRelmapScan *created = NULL;
			result = cluster_recovery_relmap_scan_begin(roots,
				reference.config.identity.system_identifier,
				reference.config.identity.database_incarnation,
				reference.config.identity.storage_uuid,
				memory_budget - sizeof(ManagedRelmapScan), &created);
			scan = created;
			if (scan != NULL) {
				scan->managed = calloc(1, sizeof(ManagedRelmapScan));
				if (scan->managed == NULL)
					result = CLUSTER_RECOVERY_RELMAP_CAPACITY;
				else {
					scan->managed->selection = selection;
					scan->managed->reference = reference;
					if (!relmap_control_current(scan, true) || !relmap_roots_current(scan))
						result = CLUSTER_RECOVERY_RELMAP_CHANGED;
				}
			}
		}
	}
	PG_CATCH();
	{
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_invalidate(roots);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_RECOVERY_RELMAP_MORE) {
		*out = scan;
		return result;
	}
	cluster_recovery_relmap_scan_destroy(scan);
fail:
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin_native_v2(ClusterRecoveryRootSet *roots,
											 const ClusterControlNativeTableInfoV2 *table,
											 size_t memory_budget, ClusterRecoveryRelmapScan **out)
{
	ClusterRecoveryRelmapScan *volatile scan = NULL;
	ClusterRecoveryRelmapResult result = CLUSTER_RECOVERY_RELMAP_INVALID;
	if (table == NULL || !cluster_recovery_root_set_output_distinct(roots, out, sizeof(*out))
		|| *out != NULL
		|| ((uintptr_t)table <= (uintptr_t)out ? (uintptr_t)out - (uintptr_t)table < sizeof(*table)
											   : (uintptr_t)table - (uintptr_t)out < sizeof(*out)))
		goto fail;
	PG_TRY();
	{
		if (memory_budget < sizeof(ManagedRelmapScan) + sizeof(*scan) + 1024)
			result = CLUSTER_RECOVERY_RELMAP_CAPACITY;
		else if (table->mode == CLUSTER_NATIVE_FILES_MANAGED_V2 && table->table_generation
				 && table->startup_generation && table->control.logical_count
				 && table->control.logical_count <= UINT64_MAX / 2
				 && table->control.logical_count <= table->count && table->count <= table->capacity
				 && relmap_table_current(table)) {
			ClusterRecoveryRelmapScan *created = NULL;
			result = cluster_recovery_relmap_scan_begin(
				roots, table->control.config.identity.system_identifier,
				table->control.config.identity.database_incarnation,
				table->control.config.identity.storage_uuid,
				memory_budget - sizeof(ManagedRelmapScan), &created);
			scan = created;
			if (scan != NULL) {
				scan->managed = calloc(1, sizeof(ManagedRelmapScan));
				if (scan->managed == NULL)
					result = CLUSTER_RECOVERY_RELMAP_CAPACITY;
				else {
					scan->managed->shared_table = true;
					scan->managed->table = *table;
					scan->managed->reference = table->control;
					if (!relmap_control_current(scan, false) || !relmap_roots_current(scan))
						result = CLUSTER_RECOVERY_RELMAP_CHANGED;
				}
			}
		}
	}
	PG_CATCH();
	{
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_invalidate(roots);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_RECOVERY_RELMAP_MORE) {
		*out = scan;
		return result;
	}
	cluster_recovery_relmap_scan_destroy(scan);
fail:
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

static bool
relmap_same_file(const ClusterRecoveryFileIdentityV2 *a, const ClusterRecoveryFileIdentityV2 *b)
{
	uint8 bytes[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	return cluster_recovery_file_identity_encode_v2(a, bytes, sizeof(bytes))
		&& cluster_recovery_file_identity_matches_v2(bytes, sizeof(bytes), b);
}

static ClusterRecoveryRelmapResult
relmap_managed_select(ClusterRecoveryRelmapScan *scan, const ClusterRecoveryRootEntry *entry)
{
	ManagedRelmapScan *m = scan->managed;
	ClusterRecoveryFileIdentityV2 identity;
	bool backup = entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP;
	if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	if (entry->file.size != CLUSTER_RELMAP_FILE_SIZE_V2
		|| !((entry->file.locator.spcOid == GLOBALTABLESPACE_OID
			  && entry->file.locator.dbOid == InvalidOid)
			 || (entry->file.locator.spcOid == DEFAULTTABLESPACE_OID
				 && OidIsValid(entry->file.locator.dbOid))))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
	if (m->shared_table) {
		if (!backup) {
			if (m->await_backup || m->completed_copies / 2 >= m->reference.logical_count)
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
			relmap_native_release(m);
			if (cluster_control_native_file_borrow_by_address_v2(
					CLUSTER_RECOVERY_FILE_RELMAP_V2, entry->file.locator.dbOid,
					entry->file.locator.spcOid, 0, &m->native)
				!= CLUSTER_NATIVE_FILES_OK_V2)
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
		}
		if (!relmap_control_current(scan, false)
			|| m->native.table_generation != m->table.table_generation
			|| m->native.startup_generation != m->table.startup_generation)
			return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
		identity = m->native.expected;
		if (identity.database_oid != entry->file.locator.dbOid
			|| identity.tablespace_oid != entry->file.locator.spcOid || identity.object_id != 0)
			return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
	} else if (!cluster_control_root_relmap_selection_lookup_v4(
				   m->selection, entry->file.locator.dbOid, entry->file.locator.spcOid, 0,
				   &identity))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
	if (identity.system_identifier != scan->key.system_identifier
		|| identity.database_incarnation != scan->key.database_incarnation
		|| memcmp(identity.storage_uuid, scan->key.storage_uuid, 16))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
	/* ROOT paths are unique and sorted: a primary precedes its .bak. Only a
	 * complete pair advances, so equal counts cannot hide missing namespaces. */
	if (backup != m->await_backup || (backup && !relmap_same_file(&identity, &m->pair_identity)))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	m->identity = identity;
	m->backup = backup;
	m->pending = false;
	scan->entry = entry;
	scan->stage = MAP_READ;
	return CLUSTER_RECOVERY_RELMAP_MORE;
}

static ClusterRecoveryRelmapResult
relmap_managed_read(ClusterRecoveryRelmapScan *scan, void *bytes)
{
	ClusterRecoveryRootsResult result;
	if (!relmap_control_current(scan, true) || !relmap_roots_current(scan))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
	result = cluster_recovery_root_set_read(scan->roots, scan->map_index, 0,
		bytes, CLUSTER_RELMAP_FILE_SIZE_V2);
	if (result != CLUSTER_RECOVERY_ROOTS_BYTES)
		return relmap_fail(scan, result == CLUSTER_RECOVERY_ROOTS_IO
			? CLUSTER_RECOVERY_RELMAP_IO : CLUSTER_RECOVERY_RELMAP_CHANGED);
	if (!relmap_control_current(scan, true) || !relmap_roots_current(scan))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
	return CLUSTER_RECOVERY_RELMAP_MORE;
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
	if (scan->managed != NULL) {
		out->file_identity = &scan->managed->identity;
		out->managed_image = &scan->managed->image;
		out->pending = scan->managed->pending;
	}
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
			if (scan->managed != NULL) {
				if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY
					|| entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP)
					return relmap_managed_select(scan, entry);
				if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP
					|| entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_BACKUP)
					return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
			} else if ((entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY
				|| entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP)
				&& entry->file.size == CLUSTER_RELMAP_FILE_SIZE_V2)
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
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
		if (scan->next_root == count) {
			if (scan->managed != NULL && (scan->managed->await_backup
				|| scan->managed->completed_copies != 2 * scan->managed->reference.logical_count))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
			if (!relmap_control_current(scan, true) || !relmap_roots_current(scan))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
			scan->status = CLUSTER_RECOVERY_RELMAP_DRAINED;
		}
		return scan->status;
	}
	if (scan->stage == MAP_READ) {
		if (scan->managed != NULL) {
			ManagedRelmapScan *m = scan->managed;
			ClusterRelmapV2View pair;
			result = relmap_managed_read(scan, m->copies[m->backup]);
			if (result != CLUSTER_RECOVERY_RELMAP_MORE)
				return result;
			if (!cluster_relmap_file_decode_v2(m->copies[m->backup], CLUSTER_RELMAP_FILE_SIZE_V2,
				&m->identity, &m->image))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
			if (m->backup && (cluster_relmap_v2_read_pair(
				m->copies[0] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2, CLUSTER_RELMAP_V2_FILE_SIZE,
				m->copies[1] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2, CLUSTER_RELMAP_V2_FILE_SIZE,
				&m->image.identity, &pair) != CLUSTER_RELMAP_V2_PAIR_VALID || !pair.pending_complete))
				return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CORRUPT);
			scan->image = m->image.committed;
		} else
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
	if (scan->stage == MAP_MAPPING && scan->ordinal == (uint32)scan->image.num_mappings) {
		if (scan->managed != NULL && !scan->managed->pending
			&& scan->managed->image.pending_generation != 0) {
			scan->managed->pending = true;
			scan->image = scan->managed->image.pending;
			scan->ordinal = 0;
			scan->diag.images++;
			relmap_identity(scan, out);
			out->kind = CLUSTER_RECOVERY_RELMAP_IMAGE;
			return CLUSTER_RECOVERY_RELMAP_ENTRY;
		}
		scan->stage = MAP_REREAD;
	}
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
	if (scan->managed != NULL) {
		ManagedRelmapScan *m = scan->managed;
		result = relmap_managed_read(scan, m->reread);
		if (result != CLUSTER_RECOVERY_RELMAP_MORE)
			return result;
		if (memcmp(m->copies[m->backup], m->reread, sizeof(m->reread)))
			return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
		m->completed_copies++;
		m->await_backup = !m->backup;
		if (!m->backup)
			m->pair_identity = m->identity;
		scan->diag.rereads++;
		scan->stage = MAP_SELECT;
		return CLUSTER_RECOVERY_RELMAP_MORE;
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

static bool
relmap_overlap(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

bool
cluster_recovery_relmap_scan_output_distinct_v2(const ClusterRecoveryRelmapScan *scan,
												const void *out, Size length)
{
	return out != NULL && length != 0
		   && (scan == NULL
			   || (cluster_recovery_root_set_output_distinct(scan->roots, out, length)
				   && (scan->managed == NULL || scan->managed->selection == NULL
					   || cluster_control_root_relmap_selection_output_distinct_v4(
						   scan->managed->selection, out, length))
				   && !relmap_overlap(out, length, scan, sizeof(*scan))
				   && !relmap_overlap(out, length, scan->managed, sizeof(ManagedRelmapScan))));
}

ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_next(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapEntry *out)
{
	ClusterRecoveryRelmapResult result = CLUSTER_RECOVERY_RELMAP_INVALID;
	if (!cluster_recovery_relmap_scan_output_distinct_v2(scan, out, sizeof(*out)))
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_INVALID);
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_INVALID);
	if (scan->status > CLUSTER_RECOVERY_RELMAP_DRAINED)
		return scan->status;
	PG_TRY();
	{
		if (!relmap_roots_current(scan) || !relmap_control_current(scan, false))
			result = relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
		else if (scan->status == CLUSTER_RECOVERY_RELMAP_DRAINED)
			result = scan->status;
		else
			result = relmap_step(scan, out);
	}
	PG_CATCH();
	{
		relmap_fail(scan, CLUSTER_RECOVERY_RELMAP_CHANGED);
		memset(out, 0, sizeof(*out));
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result > CLUSTER_RECOVERY_RELMAP_DRAINED)
		memset(out, 0, sizeof(*out));
	return result;
}

bool
cluster_recovery_relmap_scan_diag(const ClusterRecoveryRelmapScan *scan,
								  ClusterRecoveryRelmapDiag *out)
{
	bool current = false;
	if (!cluster_recovery_relmap_scan_output_distinct_v2(scan, out, sizeof(*out))) {
		if (scan != NULL)
			cluster_recovery_root_set_invalidate(scan->roots);
		return false;
	}
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || scan == NULL || scan->status > CLUSTER_RECOVERY_RELMAP_DRAINED)
		return false;
	PG_TRY();
	{
		current = relmap_roots_current(scan) && relmap_control_current(scan, false);
	}
	PG_CATCH();
	{
		cluster_recovery_root_set_invalidate(scan->roots);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!current) {
		cluster_recovery_root_set_invalidate(scan->roots);
		return false;
	}
	*out = scan->diag;
	return true;
}

void
cluster_recovery_relmap_scan_destroy(ClusterRecoveryRelmapScan *scan)
{
	if (scan != NULL && scan->managed != NULL) {
		if (scan->managed->shared_table)
			relmap_native_release(scan->managed);
		free(scan->managed);
	}
	free(scan);
}
#endif
