/*-------------------------------------------------------------------------
 * cluster_recovery_undo_store.c
 *   Cold namespace adapter for TT enumeration and exact UBA graph reads.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_undo_store.h"

#ifdef USE_PGRAC_CLUSTER
#define UNDO_STORE_SEGMENTS (UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_SEGS_PER_INSTANCE)

typedef struct RecoveryUndoFile {
	uint32 root_index;
	bool loaded;
	UndoSegmentHeaderData header;
} RecoveryUndoFile;

struct ClusterRecoveryUndoStore {
	ClusterRecoveryRootSet *roots;
	RecoveryUndoFile *files;
	uint32 by_segment[UNDO_STORE_SEGMENTS + 1]; /* file index + 1 */
	uint32 count, current, rechecked;
	uint16 slot;
	ClusterRecoveryGraphResult status;
	char page[BLCKSZ];
};

static ClusterRecoveryGraphResult
undo_store_fail(ClusterRecoveryUndoStore *store, ClusterRecoveryGraphResult result)
{
	if (store != NULL)
		store->status = result;
	return result;
}

static ClusterRecoveryGraphResult
undo_store_roots_result(ClusterRecoveryUndoStore *store, ClusterRecoveryRootsResult result)
{
	switch (result) {
	case CLUSTER_RECOVERY_ROOTS_CHANGED:
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	case CLUSTER_RECOVERY_ROOTS_CAPACITY:
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CAPACITY);
	case CLUSTER_RECOVERY_ROOTS_UNSAFE:
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	default:
		/* Includes beyond EOF: an exact UBA read cannot treat it as absence. */
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IO);
	}
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_begin(ClusterRecoveryRootSet *roots, size_t memory_budget,
								  ClusterRecoveryUndoStore **out)
{
	ClusterRecoveryUndoStore *store;
	uint32 count = 0, total = cluster_recovery_root_set_count(roots);
	uint8 digest[32];
	size_t fixed = sizeof(*store) + 16 * 1024; /* transient original header parser */

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	for (uint32 i = 0; i < total; i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		if (entry == NULL)
			return CLUSTER_RECOVERY_GRAPH_CHANGED;
		if (entry->file.kind == CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
			count++;
	}
	if (count > UNDO_STORE_SEGMENTS || memory_budget < fixed
		|| count > (memory_budget - fixed) / sizeof(RecoveryUndoFile))
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	store = calloc(1, sizeof(*store));
	if (store == NULL)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	store->files = count == 0 ? NULL : calloc(count, sizeof(*store->files));
	if (count > 0 && store->files == NULL) {
		free(store);
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	}
	store->roots = roots;
	for (uint32 i = 0; i < total; i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		uint32 segment = entry->file.undo_segment;

		if (entry->file.kind != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
			continue;
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0 || segment == 0
			|| segment > UNDO_STORE_SEGMENTS || store->by_segment[segment] != 0
			|| entry->file.size < BLCKSZ || entry->file.size % BLCKSZ != 0) {
			cluster_recovery_undo_store_destroy(store);
			return CLUSTER_RECOVERY_GRAPH_CORRUPT;
		}
		store->files[store->count].root_index = i;
		store->by_segment[segment] = ++store->count;
	}
	*out = store;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_next(ClusterRecoveryUndoStore *store, ClusterRecoveryUndoEntry *out)
{
	RecoveryUndoFile *file;
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (store == NULL || out == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (store->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return store->status;
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (store->slot == TT_SLOTS_PER_SEGMENT) {
		store->slot = 0;
		store->current++;
	}
	if (store->current == store->count)
		return store->status = CLUSTER_RECOVERY_GRAPH_DRAINED;
	file = &store->files[store->current];
	if (!file->loaded) {
		const ClusterRecoveryRootEntry *entry
			= cluster_recovery_root_set_entry(store->roots, file->root_index);
		ClusterRecoveryUndoScan *scan = NULL;
		ClusterRecoveryUndoResult parsed;
		ClusterRecoveryRootsResult read = cluster_recovery_root_set_read(
			store->roots, file->root_index, 0, store->page, BLCKSZ);

		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return undo_store_roots_result(store, read);
		parsed = cluster_recovery_undo_scan_begin(store->page, BLCKSZ, entry->file.undo_segment,
												  entry->file.undo_owner_instance, &scan);
		if (parsed != CLUSTER_RECOVERY_UNDO_MORE)
			return undo_store_fail(store, parsed == CLUSTER_RECOVERY_UNDO_CAPACITY
											  ? CLUSTER_RECOVERY_GRAPH_CAPACITY
											  : CLUSTER_RECOVERY_GRAPH_CORRUPT);
		memcpy(&file->header, cluster_recovery_undo_scan_header(scan), BLCKSZ);
		cluster_recovery_undo_scan_destroy(scan);
		file->loaded = true;
		return CLUSTER_RECOVERY_GRAPH_MORE;
	}
	out->segment_id = file->header.segment_id;
	out->segment_generation = file->header.wrap_count;
	out->owner_instance = file->header.owner_instance;
	out->slot_offset = store->slot;
	out->slot = file->header.tt_slots[store->slot++];
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

const UndoSegmentHeaderData *
cluster_recovery_undo_store_header(const ClusterRecoveryUndoStore *store, uint32 segment)
{
	uint8 digest[32];
	uint32 at;

	if (store == NULL || store->status != CLUSTER_RECOVERY_GRAPH_DRAINED || segment == 0
		|| segment > UNDO_STORE_SEGMENTS || !cluster_recovery_root_set_digest(store->roots, digest))
		return NULL;
	at = store->by_segment[segment];
	return at == 0 ? NULL : &store->files[at - 1].header;
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_read(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
								 ClusterRecoveryUndoRecord *record, uint32 *generation)
{
	ClusterRecoveryUndoStore *store = arg;
	const UndoSegmentHeaderData *canonical, *physical;
	uint32 segment, block;
	uint16 slot, row;
	ClusterRecoveryRootsResult read;
	ClusterRecoveryUndoResult parsed;
	ClusterRecoveryGraphResult checked;

	if (record != NULL)
		memset(record, 0, sizeof(*record));
	if (generation != NULL)
		*generation = 0;
	if (store == NULL || key == NULL || record == NULL || generation == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (store->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return store->status;
	if (store->status != CLUSTER_RECOVERY_GRAPH_DRAINED)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!cluster_recovery_root_set_namespace_matches(store->roots, key->system_identifier,
													 key->database_incarnation)
		|| !uba_decode_record(address, &segment, &block, &slot, &row)
		|| segment > UNDO_STORE_SEGMENTS || key->origin_node_id >= UNDO_OWNER_INSTANCE_MAX
		|| slot != key->slot_offset || key->slot_wrap == TT_WRAP_INVALID)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	canonical = cluster_recovery_undo_store_header(store, key->segment_id);
	physical = cluster_recovery_undo_store_header(store, segment);
	if (canonical == NULL || canonical->wrap_count != key->segment_generation
		|| canonical->owner_instance != key->origin_node_id + 1)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	if (physical == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IO);
	if (physical->owner_instance != canonical->owner_instance)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	read = cluster_recovery_root_set_read(store->roots,
										  store->files[store->by_segment[segment] - 1].root_index,
										  (uint64)block * BLCKSZ, store->page, BLCKSZ);
	if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
		return undo_store_roots_result(store, read);
	parsed = cluster_recovery_undo_record_read(store->page, BLCKSZ, segment, block,
											   physical->owner_instance, address, record);
	if (parsed != CLUSTER_RECOVERY_UNDO_ENTRY) {
		memset(record, 0, sizeof(*record));
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CORRUPT);
	}
	checked = cluster_recovery_tx_record_check(key, address, record, physical->wrap_count);
	if (checked != CLUSTER_RECOVERY_GRAPH_MORE) {
		memset(record, 0, sizeof(*record));
		return undo_store_fail(store, checked);
	}
	*generation = physical->wrap_count;
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_recheck(ClusterRecoveryUndoStore *store)
{
	RecoveryUndoFile *file;
	ClusterRecoveryRootsResult read;
	uint8 digest[32];

	if (store == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (store->status != CLUSTER_RECOVERY_GRAPH_DRAINED)
		return undo_store_fail(store, store->status > CLUSTER_RECOVERY_GRAPH_DRAINED
										  ? store->status
										  : CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (store->rechecked == store->count)
		return CLUSTER_RECOVERY_GRAPH_DRAINED;
	file = &store->files[store->rechecked];
	read = cluster_recovery_root_set_read(store->roots, file->root_index, 0, store->page, BLCKSZ);
	if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
		return undo_store_roots_result(store, read);
	if (memcmp(store->page, &file->header, BLCKSZ) != 0)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	store->rechecked++;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

void
cluster_recovery_undo_store_destroy(ClusterRecoveryUndoStore *store)
{
	if (store != NULL) {
		free(store->files);
		free(store);
	}
}
#endif
