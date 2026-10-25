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
#define UNDO_STORE_TICK 128

typedef struct RecoveryUndoFile {
	uint32 root_index;
	bool loaded;
	UndoSegmentHeaderData header;
} RecoveryUndoFile;

struct ClusterRecoveryUndoStore {
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryReferenceGraph *graph;
	uint64 system_identifier, database_incarnation;
	RecoveryUndoFile *files;
	uint32 by_segment[UNDO_STORE_SEGMENTS + 1]; /* file index + 1 */
	uint32 count, current, rechecked;
	uint32 root_cursor, fill_cursor, files_filled;
	bool indexed, files_ready, started;
	size_t memory_budget;
	uint16 slot;
	ClusterRecoveryGraphResult status;
	char page[BLCKSZ];
};

static ClusterRecoveryGraphResult
undo_store_fail(ClusterRecoveryUndoStore *store, ClusterRecoveryGraphResult result)
{
	if (store != NULL) {
		store->status = result;
		cluster_recovery_reference_graph_invalidate(store->graph);
	}
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
	uint8 digest[32];
	size_t fixed = sizeof(*store) + 16 * 1024; /* transient original header parser */

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (memory_budget < fixed)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	store = calloc(1, sizeof(*store));
	if (store == NULL)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	store->roots = roots;
	store->memory_budget = memory_budget;
	*out = store;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_graph_begin(ClusterRecoveryUndoStore *store, uint64 system_identifier,
										uint64 database_incarnation, size_t graph_memory_budget)
{
	ClusterRecoveryGraphResult result;
	uint8 digest[32];

	if (store == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (store->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return store->status;
	if (store->started || store->graph != NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (!cluster_recovery_root_set_namespace_matches(store->roots, system_identifier,
													 database_incarnation))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	result = cluster_recovery_reference_graph_begin(
		graph_memory_budget, cluster_recovery_undo_store_read, store, &store->graph);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		return undo_store_fail(store, result);
	store->system_identifier = system_identifier;
	store->database_incarnation = database_incarnation;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryReferenceGraph *
cluster_recovery_undo_store_graph(ClusterRecoveryUndoStore *store)
{
	uint8 digest[32];

	if (store == NULL || store->status != CLUSTER_RECOVERY_GRAPH_DRAINED)
		return NULL;
	if (!cluster_recovery_root_set_digest(store->roots, digest)) {
		undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
		return NULL;
	}
	return store->graph;
}

ClusterRecoveryRootSet *
cluster_recovery_undo_store_root_set(ClusterRecoveryUndoStore *store)
{
	return cluster_recovery_undo_store_graph(store) == NULL ? NULL : store->roots;
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
	store->started = true;
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (!store->indexed) {
		uint32 total = cluster_recovery_root_set_count(store->roots);
		unsigned work = 0;

		for (; store->root_cursor < total && work++ < UNDO_STORE_TICK; store->root_cursor++) {
			const ClusterRecoveryRootEntry *entry
				= cluster_recovery_root_set_entry(store->roots, store->root_cursor);
			uint32 segment;

			if (entry == NULL)
				return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
			if (entry->file.kind != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
				continue;
			segment = entry->file.undo_segment;
			if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0 || segment == 0
				|| segment > UNDO_STORE_SEGMENTS || store->by_segment[segment] != 0
				|| entry->file.size < BLCKSZ || entry->file.size % BLCKSZ != 0)
				return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CORRUPT);
			store->by_segment[segment] = store->root_cursor + 1;
			store->count++;
		}
		if (store->root_cursor < total)
			return CLUSTER_RECOVERY_GRAPH_MORE;
		if (store->count > UNDO_STORE_SEGMENTS || store->memory_budget < sizeof(*store) + 16 * 1024
			|| store->count
				   > (store->memory_budget - sizeof(*store) - 16 * 1024) / sizeof(RecoveryUndoFile))
			return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CAPACITY);
		if (store->count > 0) {
			store->files = calloc(store->count, sizeof(*store->files));
			if (store->files == NULL)
				return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CAPACITY);
		}
		store->indexed = true;
	}
	if (!store->files_ready) {
		unsigned work = 0;
		uint32 total = cluster_recovery_root_set_count(store->roots);

		for (; store->fill_cursor < total && work++ < UNDO_STORE_TICK; store->fill_cursor++) {
			const ClusterRecoveryRootEntry *entry
				= cluster_recovery_root_set_entry(store->roots, store->fill_cursor);
			uint32 segment;

			if (entry == NULL)
				return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
			if (entry->file.kind != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
				continue;
			segment = entry->file.undo_segment;
			/* by_segment still contains root index + 1; convert it while
			 * preserving the canonical sorted root order. */
			if (store->by_segment[segment] == 0)
				return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
			store->files[store->files_filled].root_index = store->by_segment[segment] - 1;
			store->by_segment[segment] = ++store->files_filled;
		}
		if (store->fill_cursor < total)
			return CLUSTER_RECOVERY_GRAPH_MORE;
		if (store->files_filled != store->count)
			return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
		store->files_ready = true;
	}
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
	if (store->graph != NULL) {
		const TTSlot *slot = &file->header.tt_slots[store->slot];

		/* The legacy first_undo_block field is the transaction-chain head.
		 * Status/SCN/CTRC flags do not establish that any dependency vanished. */
		if (!UBA_is_invalid(slot->first_undo_block)) {
			ClusterRecoveryTxnKey key = { 0 };
			ClusterRecoveryGraphResult added;

			key.system_identifier = store->system_identifier;
			key.database_incarnation = store->database_incarnation;
			key.segment_id = file->header.segment_id;
			key.segment_generation = file->header.wrap_count;
			key.origin_node_id = file->header.owner_instance - 1;
			key.slot_offset = store->slot;
			key.slot_wrap = slot->wrap;
			key.xid = slot->xid;
			added
				= cluster_recovery_reference_graph_add(store->graph, &key, slot->first_undo_block);
			if (added != CLUSTER_RECOVERY_GRAPH_MORE)
				return undo_store_fail(store, added);
		}
	}
	out->segment_id = file->header.segment_id;
	out->segment_generation = file->header.wrap_count;
	out->owner_instance = file->header.owner_instance;
	out->slot_offset = store->slot;
	out->slot = file->header.tt_slots[store->slot++];
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

ClusterRecoveryGraphResult
cluster_recovery_undo_store_at(ClusterRecoveryUndoStore *store, uint32 index,
							   ClusterRecoveryUndoEntry *out)
{
	uint8 digest[32];
	uint32 file = index / TT_SLOTS_PER_SEGMENT;
	uint16 slot = index % TT_SLOTS_PER_SEGMENT;
	const UndoSegmentHeaderData *header;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (store == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (store->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return store->status;
	if (out == NULL || store->status != CLUSTER_RECOVERY_GRAPH_DRAINED || file > store->count
		|| (file == store->count && slot != 0))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (file == store->count)
		return CLUSTER_RECOVERY_GRAPH_DRAINED;
	if (!store->files[file].loaded)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	header = &store->files[file].header;
	out->segment_id = header->segment_id;
	out->segment_generation = header->wrap_count;
	out->owner_instance = header->owner_instance;
	out->slot_offset = slot;
	out->slot = header->tt_slots[slot];
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
cluster_recovery_undo_store_read_base(ClusterRecoveryUndoStore *store, UBA address,
									TransactionId xid, ClusterRecoveryTxnKey *transaction,
									ClusterRecoveryUndoRecord *record, uint32 *generation)
{
	const UndoSegmentHeaderData *physical, *canonical;
	const TTSlot *binding;
	ClusterRecoveryTxnKey key = { 0 };
	ClusterRecoveryRootsResult read;
	ClusterRecoveryGraphResult result;
	uint8 digest[32];
	uint32 segment, block;
	uint16 slot, row;

	if (transaction != NULL)
		memset(transaction, 0, sizeof(*transaction));
	if (record != NULL)
		memset(record, 0, sizeof(*record));
	if (generation != NULL)
		*generation = 0;
	if (store == NULL || transaction == NULL || record == NULL || generation == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (store->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return store->status;
	if (store->status != CLUSTER_RECOVERY_GRAPH_DRAINED || store->graph == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!cluster_recovery_root_set_digest(store->roots, digest))
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (!TransactionIdIsNormal(xid)
		|| !uba_decode_record(address, &segment, &block, &slot, &row)
		|| segment > UNDO_STORE_SEGMENTS)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	physical = cluster_recovery_undo_store_header(store, segment);
	if (physical == NULL)
		return undo_store_fail(store, CLUSTER_RECOVERY_GRAPH_IO);
	read = cluster_recovery_root_set_read(store->roots,
										  store->files[store->by_segment[segment] - 1].root_index,
										  (uint64)block * BLCKSZ, store->page, BLCKSZ);
	if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
		return undo_store_roots_result(store, read);
	if (cluster_recovery_undo_record_read(store->page, BLCKSZ, segment, block,
										 physical->owner_instance, address, record)
		!= CLUSTER_RECOVERY_UNDO_ENTRY) {
		result = CLUSTER_RECOVERY_GRAPH_CORRUPT;
		goto refuse;
	}
	if (record->header.xid != xid) {
		result = CLUSTER_RECOVERY_GRAPH_IDENTITY;
		goto refuse;
	}
	canonical = cluster_recovery_undo_store_header(store, record->header.tt_slot_segment_id);
	/* A complete matching base is an independent binding. A current segment
	 * number alone, or the local PAGE wrap, is never sufficient. Reused and
	 * legacy identities remain for the later independently seeded WAL forest. */
	if (canonical == NULL || canonical->wrap_count == UINT32_MAX
		|| record->header.tt_wrap_plus1 == 0)
		goto pending;
	binding = &canonical->tt_slots[slot];
	if (binding->status < TT_SLOT_ACTIVE || binding->status > TT_SLOT_ABORTED
		|| binding->xid != xid || binding->wrap == TT_WRAP_INVALID
		|| binding->wrap != record->header.tt_wrap_plus1 - 1)
		goto pending;
	key.system_identifier = store->system_identifier;
	key.database_incarnation = store->database_incarnation;
	key.segment_id = canonical->segment_id;
	key.segment_generation = canonical->wrap_count;
	key.origin_node_id = canonical->owner_instance - 1;
	key.slot_offset = slot;
	key.slot_wrap = binding->wrap;
	key.xid = xid;
	result = cluster_recovery_tx_record_check(&key, address, record, physical->wrap_count);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		goto refuse;
	*transaction = key;
	*generation = physical->wrap_count;
	return CLUSTER_RECOVERY_GRAPH_RECORD;
pending:
	memset(record, 0, sizeof(*record));
	return CLUSTER_RECOVERY_GRAPH_PENDING;
refuse:
	memset(record, 0, sizeof(*record));
	return undo_store_fail(store, result);
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
		cluster_recovery_reference_graph_destroy(store->graph);
		free(store->files);
		free(store);
	}
}
#endif
