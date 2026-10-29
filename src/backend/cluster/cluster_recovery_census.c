/*-------------------------------------------------------------------------
 * cluster_recovery_census.c
 *   Owned observations of the original isolated cold recovery inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_census.h"
#include "common/cryptohash.h"
#include "storage/lwlock.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryCensus {
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeInventory *inventory;
	ClusterRecoveryUndoStore *undo;
	ClusterRecoveryReferenceGraph *graph; /* owned by undo */
	ClusterRecoveryPageRoots *page_roots;
	ClusterRecoveryRelationCensus *relations;
	ClusterRecoveryMultixactScan *mx;
	ClusterRecoverySlruScan *slru;
	ClusterRecoveryRelmapScan *relmap;
	ClusterRecoveryCensusPagesV1 *pages;
	uint32 page_count, page_capacity;
	uint8 pages_sha256[32];
	bool pages_complete;
	ClusterRecoveryCensusReferencesV1 *references;
	uint32 reference_count, reference_capacity;
	char *reference_tuples; /* trailing storage in the same reserved allocation */
	size_t reference_tuple_bytes, reference_tuple_capacity;
	uint8 references_sha256[32];
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusManifest manifest;
	ClusterRecoveryCensusResult status;
	uint32 source_count, target_at, tt_terminal_at;
	uint8 storage_uuid[16];
	uint64 system_identifier, database_incarnation;
	size_t child_budget;
	bool ctrc_namespace;
	int ctrc_namespace_fd; /* borrowed from inputs, never closed here */
};

static void
census_release(ClusterRecoveryCensus *census)
{
	/* Dependents precede their owners. Inventory destruction revokes its
	 * borrowed graph/roots, so both must still be alive here. */
	free(census->references);
	census->references = NULL;
	census->reference_tuples = NULL;
	census->reference_count = census->reference_capacity = 0;
	census->reference_tuple_bytes = census->reference_tuple_capacity = 0;
	memset(census->references_sha256, 0, sizeof(census->references_sha256));
	free(census->pages);
	census->pages = NULL;
	census->page_count = census->page_capacity = 0;
	census->pages_complete = false;
	memset(census->pages_sha256, 0, sizeof(census->pages_sha256));
	cluster_recovery_page_roots_destroy(census->page_roots);
	census->page_roots = NULL;
	cluster_recovery_relation_census_destroy(census->relations);
	census->relations = NULL;
	cluster_recovery_multixact_scan_destroy(census->mx);
	census->mx = NULL;
	cluster_recovery_slru_scan_destroy(census->slru);
	census->slru = NULL;
	cluster_recovery_relmap_scan_destroy(census->relmap);
	census->relmap = NULL;
	cluster_recovery_native_inventory_destroy(census->inventory);
	census->inventory = NULL;
	cluster_recovery_undo_store_destroy(census->undo);
	census->undo = NULL;
	census->graph = NULL;
	memset(&census->manifest, 0, sizeof(census->manifest));
	cluster_recovery_root_set_destroy(census->roots);
	census->roots = NULL;
}

static ClusterRecoveryCensusResult
census_fail(ClusterRecoveryCensus *census, ClusterRecoveryCensusResult result)
{
	census->status = result;
	census_release(census);
	return result;
}

ClusterRecoveryCensusResult
cluster_recovery_census_begin(ClusterWalInputsV1 *inputs, int root_fd, size_t memory_budget,
							  ClusterRecoveryCensus **out)
{
	ClusterRecoveryCensus *census;
	const ClusterWalInputV1 *first;
	uint32 count;
	size_t budget;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || inputs == NULL || root_fd < 0)
		return CLUSTER_RECOVERY_CENSUS_INVALID;
	count = cluster_wal_inputs_count_v1(inputs);
	first = cluster_wal_inputs_at_v1(inputs, 0);
	if (count == 0 || count > CLUSTER_WAL_INPUTS_MAX || first == NULL)
		return CLUSTER_RECOVERY_CENSUS_INVALID;
	/* Seven concurrent reservations: roots, inventory, undo, TX graph,
	 * sequential relation/SLRU/MX scratch, retained PAGE metadata, and
	 * complete relation/history reference metadata with private tuple bytes.
	 * Original consumers enforce each cap.
	 * Divide only after subtraction; never overflow a sum of allocations. */
	if (memory_budget <= sizeof(*census))
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	budget = (memory_budget - sizeof(*census)) / 7;
	if (budget < 32 * BLCKSZ)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	census = calloc(1, sizeof(*census));
	if (census == NULL)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	/* Reserve the complete metadata cap before traversing any input. This is
	 * not another page cache: original bytes are reduced to a full-page hash. */
	census->page_capacity = (uint32)Min((budget - offsetof(ClusterRecoveryCensusPagesV1, entries))
		/ sizeof(ClusterRecoveryCensusPageV1), (size_t)UINT32_MAX);
	census->pages = calloc(1, offsetof(ClusterRecoveryCensusPagesV1, entries)
		+ (size_t)census->page_capacity * sizeof(ClusterRecoveryCensusPageV1));
	if (census->pages == NULL) {
		cluster_recovery_census_destroy(census);
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	}
	/* One fixed cap for both reference metadata and tuple copies. Keep the
	 * original scanner's whole output, never grow outside the caller budget. */
	census->reference_capacity = (uint32)Min(
		(budget - offsetof(ClusterRecoveryCensusReferencesV1, entries))
			/ 2 / sizeof(ClusterRecoveryCensusReferenceV1), (size_t)UINT32_MAX);
	census->references = calloc(1, budget);
	if (census->references == NULL) {
		cluster_recovery_census_destroy(census);
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	}
	census->reference_tuple_capacity = budget
		- offsetof(ClusterRecoveryCensusReferencesV1, entries)
		- (size_t)census->reference_capacity * sizeof(ClusterRecoveryCensusReferenceV1);
	census->reference_tuples = (char *)census->references + budget
		- census->reference_tuple_capacity;
	census->inputs = inputs;
	census->source_count = count;
	census->system_identifier = first->source.claim.identity.system_identifier;
	census->database_incarnation = first->source.claim.database_incarnation;
	memcpy(census->storage_uuid, first->source.claim.identity.storage_uuid, 16);
	census->child_budget = budget;
	census->diag.phase = CLUSTER_RECOVERY_CENSUS_WAL;
	PG_TRY();
	{
		census->diag.input_result = cluster_wal_inputs_revalidate_v1(inputs);
		if (census->diag.input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			census->status = CLUSTER_RECOVERY_CENSUS_REFUSED;
		else {
			census->diag.roots_result = cluster_recovery_root_set_begin(
				root_fd, census->system_identifier, census->database_incarnation,
				first->source.claim.identity.storage_uuid, budget, &census->roots);
			if (census->diag.roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
				census->status = CLUSTER_RECOVERY_CENSUS_REFUSED;
		}
	}
	PG_CATCH();
	{
		cluster_recovery_census_destroy(census);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (census->status != CLUSTER_RECOVERY_CENSUS_MORE) {
		cluster_recovery_census_destroy(census);
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	}
	*out = census;
	return CLUSTER_RECOVERY_CENSUS_MORE;
}

ClusterRecoveryCensusResult
cluster_recovery_census_begin_ctrc(ClusterWalInputsV1 *inputs, size_t memory_budget,
	ClusterRecoveryCensus **out)
{
	ClusterRecoveryCensusResult result;
	int fd = -1;

	if (out == NULL)
		return CLUSTER_RECOVERY_CENSUS_INVALID;
	*out = NULL;
	if (cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	result = cluster_recovery_census_begin(inputs, fd, memory_budget, out);
	if (result == CLUSTER_RECOVERY_CENSUS_MORE) {
		(*out)->ctrc_namespace = true;
		(*out)->ctrc_namespace_fd = fd;
	}
	return result;
}

static ClusterRecoveryCensusResult
census_entry(ClusterRecoveryCensusEntry *out, ClusterRecoveryCensusKind kind)
{
	out->kind = kind;
	return CLUSTER_RECOVERY_CENSUS_ENTRY;
}

/* Two exact bounded candidate lookups for one fully re-read TX node. No
 * late roots or rebased digest: an absent edge remains explicitly absent.
 * Physical file presence/extent does not resolve incarnation or DROP/CREATE. */
static ClusterRecoveryCensusResult
census_target(ClusterRecoveryCensus *census, ClusterRecoveryCensusEntry *out)
{
	ClusterRecoveryCensusDiag *d = &census->diag;
	ClusterRecoveryCensusTarget *target = &out->value.target;
	const UndoRecordHeader *header;

	d->graph_result = cluster_recovery_reference_graph_at(census->graph, census->target_at,
														  &target->transaction);
	if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED) {
		d->phase = CLUSTER_RECOVERY_CENSUS_RELATIONS;
		return CLUSTER_RECOVERY_CENSUS_MORE;
	}
	if (d->graph_result != CLUSTER_RECOVERY_GRAPH_RECORD)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	header = &target->transaction.record->header;
	target->key.system_identifier = target->transaction.transaction.system_identifier;
	target->key.database_incarnation = target->transaction.transaction.database_incarnation;
	memcpy(target->key.storage_uuid, census->storage_uuid, 16);
	target->key.locator = header->target_locator;
	target->forknum = header->target_fork;
	target->blockno = header->target_block;
	target->required_blocks = (uint64)header->target_block + 1;
	d->roots_result = cluster_recovery_root_set_find_relation(census->roots, &target->key,
															  target->forknum, &target->root_index);
	if (d->roots_result != CLUSTER_RECOVERY_ROOTS_OBSERVED)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	d->roots_result = cluster_recovery_root_set_find_relation(census->roots, &target->key,
															  SPACE_FORKNUM, &target->space_index);
	if (d->roots_result != CLUSTER_RECOVERY_ROOTS_OBSERVED)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	target->root = cluster_recovery_root_set_entry(census->roots, target->root_index);
	target->space = cluster_recovery_root_set_entry(census->roots, target->space_index);
	target->target_physical
		= target->root != NULL && (target->root->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0;
	target->target_extent_present
		= target->target_physical && target->root->file.size / BLCKSZ >= target->required_blocks;
	target->space_physical
		= target->space != NULL && (target->space->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0;
	d->tx_targets++;
	d->tx_target_missing += !target->target_physical;
	d->tx_target_short += target->target_physical && !target->target_extent_present;
	d->tx_space_missing += !target->space_physical;
	census->target_at++;
	return census_entry(out, CLUSTER_RECOVERY_CENSUS_TX_TARGET);
}

static bool
census_hash_number(pg_cryptohash_ctx *hash, uint64 value)
{
	uint8 bytes[8];
	for (unsigned i = 0; i < sizeof(bytes); ++i)
		bytes[i] = (uint8)(value >> (8 * i));
	return pg_cryptohash_update(hash, bytes, sizeof(bytes)) == 0;
}

/* Hash private value bytes while the original cursor still owns them.
 * ERROR must release the hash context before the census retires its owners. */
static bool
census_hash_bytes(const void *bytes, size_t length, uint8 digest[32])
{
	pg_cryptohash_ctx *volatile hash = NULL;
	volatile bool ok = false;

	PG_TRY();
	{
		hash = pg_cryptohash_create(PG_SHA256);
		ok = hash != NULL && pg_cryptohash_init(hash) == 0
			&& pg_cryptohash_update(hash, bytes, length) == 0
			&& pg_cryptohash_final(hash, digest, 32) == 0;
	}
	PG_FINALLY();
	{
		if (hash != NULL)
			pg_cryptohash_free(hash);
	}
	PG_END_TRY();
	return ok;
}

static ClusterRecoveryCensusResult
census_remember_page(ClusterRecoveryCensus *census,
	const ClusterRecoveryRelationCensusEntry *entry)
{
	ClusterRecoveryCensusPageV1 *value;

	if (entry->kind != CLUSTER_RECOVERY_RELATIONS_PAGE)
		return CLUSTER_RECOVERY_CENSUS_MORE;
	if (census->pages == NULL || census->pages_complete || entry->page.page == NULL)
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	if (census->page_count == census->page_capacity)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	value = &census->pages->entries[census->page_count];
	value->reservation = entry->page.reservation;
	value->identity_token = entry->page.identity_token;
	value->reservation_token = entry->page.reservation_token;
	value->root_index = entry->root_index;
	value->forknum = entry->page.forknum;
	value->blockno = entry->page.block;
	value->heap = entry->heap;
	if (!census_hash_bytes(entry->page.page, BLCKSZ, value->page_sha256))
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	census->pages->count = ++census->page_count;
	return CLUSTER_RECOVERY_CENSUS_MORE;
}

/* Consume every original relation/history emission in its original order.
 * The page index binds the original physical baseline, including versions
 * reconstructed by the existing inverse reader. No current/old xid filtering. */
static ClusterRecoveryCensusResult
census_remember_reference(ClusterRecoveryCensus *census,
	const ClusterRecoveryRelationCensusEntry *entry)
{
	const ClusterRecoveryHeapEntry *heap = &entry->reference.entry;
	const ClusterRecoveryCensusPageV1 *page;
	ClusterRecoveryCensusReferenceV1 *value;

	if (entry->kind != CLUSTER_RECOVERY_RELATIONS_REFERENCE)
		return CLUSTER_RECOVERY_CENSUS_MORE;
	if (census->references == NULL || census->pages_complete || census->page_count == 0)
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	page = &census->pages->entries[census->page_count - 1];
	if (!page->heap || !entry->heap || page->root_index != entry->root_index
		|| page->forknum != entry->page.forknum || page->blockno != entry->page.block
		|| page->identity_token != entry->page.identity_token
		|| page->reservation_token != entry->page.reservation_token
		|| memcmp(&page->reservation, &entry->page.reservation, sizeof(page->reservation)) != 0
		|| (heap->kind != CLUSTER_RECOVERY_HEAP_ITL && heap->kind != CLUSTER_RECOVERY_HEAP_ITEM)
		|| heap->tuple_length > BLCKSZ
		|| (heap->tuple_length == 0) != (heap->tuple_bytes == NULL))
		return CLUSTER_RECOVERY_CENSUS_REFUSED;
	if (census->reference_count == census->reference_capacity
		|| census->reference_tuple_bytes > census->reference_tuple_capacity
		|| heap->tuple_length > census->reference_tuple_capacity - census->reference_tuple_bytes)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	value = &census->references->entries[census->reference_count];
	value->page_index = census->page_count - 1;
	value->version = entry->reference.version;
	value->entry = *heap;
	value->transaction = entry->reference.transaction;
	if (heap->tuple_length != 0) {
		char *tuple = census->reference_tuples + census->reference_tuple_bytes;

		memcpy(tuple, heap->tuple_bytes, heap->tuple_length);
		value->entry.tuple_bytes = tuple;
		census->reference_tuple_bytes += heap->tuple_length;
	}
	census->references->count = ++census->reference_count;
	return CLUSTER_RECOVERY_CENSUS_MORE;
}

/* Private observation integrity only, not the portable D0 graph transcript.
 * Check pointer/length layout before hashing any borrowed view's raw bytes;
 * no modified tuple pointer is followed. The TX graph retains record custody. */
static bool
census_references_digest(ClusterRecoveryCensus *census, uint8 digest[32])
{
	uint8 parts[64];
	size_t bytes = 0;

	if (census->references == NULL || census->reference_count > census->reference_capacity
		|| census->references->count != census->reference_count
		|| census->reference_tuple_bytes > census->reference_tuple_capacity)
		return false;
	for (uint32 i = 0; i < census->reference_count; i++) {
		const ClusterRecoveryCensusReferenceV1 *value = &census->references->entries[i];
		const ClusterRecoveryHeapEntry *heap = &value->entry;

		if (value->page_index >= census->page_count || heap->tuple_length > BLCKSZ
			|| heap->tuple_length > census->reference_tuple_bytes - bytes
			|| (heap->tuple_length == 0 ? heap->tuple_bytes != NULL
				: heap->tuple_bytes != census->reference_tuples + bytes))
			return false;
		bytes += heap->tuple_length;
	}
	return bytes == census->reference_tuple_bytes
		&& census_hash_bytes(census->references,
			offsetof(ClusterRecoveryCensusReferencesV1, entries)
				+ (size_t)census->reference_count * sizeof(ClusterRecoveryCensusReferenceV1), parts)
		&& census_hash_bytes(census->reference_tuples, bytes, parts + 32)
		&& census_hash_bytes(parts, sizeof(parts), digest);
}

/* Native private storage, not a wire encoding or a completion witness. The
 * separate count bounds every access even if the public readonly view changes. */
static bool
census_pages_digest(ClusterRecoveryCensus *census, uint8 digest[32])
{
	return census->pages != NULL && census->page_count <= census->page_capacity
		&& census->pages->count == census->page_count
		&& census_hash_bytes(census->pages, offsetof(ClusterRecoveryCensusPagesV1, entries)
			+ (size_t)census->page_count * sizeof(ClusterRecoveryCensusPageV1), digest);
}

static bool
census_build_manifest(ClusterRecoveryCensus *census)
{
	static const uint8 domain[] = "PGRAC-E1-CENSUS-OBSERVATION-V1";
	ClusterRecoveryCensusManifest value = { 0 };
	pg_cryptohash_ctx *volatile hash = NULL;
	volatile bool ok = false;

	if (!cluster_recovery_reference_graph_observed(census->graph)
		|| !cluster_recovery_root_set_digest(census->roots, value.roots_sha256)
		|| !cluster_recovery_native_inventory_digest(census->inventory, value.inputs_sha256))
		return false;
	value.encoding_version = 1;
	value.source_count = census->source_count;
	value.root_count = cluster_recovery_root_set_count(census->roots);
	value.tx_record_count = cluster_recovery_reference_graph_count(census->graph);
	value.wal_record_count = census->diag.inventory.records;
	value.native_effect_count = census->diag.effects;
	value.other_wal_record_count = census->diag.inventory.other_records;
	value.unowned_target_record_count = census->diag.inventory.unowned_target_records;
	PG_TRY();
	{
		hash = pg_cryptohash_create(PG_SHA256);
		ok = hash != NULL && pg_cryptohash_init(hash) == 0
			 && pg_cryptohash_update(hash, domain, sizeof(domain)) == 0
			 && census_hash_number(hash, value.encoding_version)
			 && census_hash_number(hash, value.source_count)
			 && census_hash_number(hash, value.root_count)
			 && census_hash_number(hash, value.tx_record_count)
			 && census_hash_number(hash, value.wal_record_count)
			 && census_hash_number(hash, value.native_effect_count)
			 && census_hash_number(hash, value.other_wal_record_count)
			 && census_hash_number(hash, value.unowned_target_record_count)
			 && pg_cryptohash_update(hash, value.inputs_sha256, 32) == 0
			 && pg_cryptohash_update(hash, value.roots_sha256, 32) == 0
			 && pg_cryptohash_final(hash, value.observation_sha256, 32) == 0;
	}
	PG_FINALLY();
	{
		pg_cryptohash_free(hash);
	}
	PG_END_TRY();
	if (ok)
		census->manifest = value;
	return ok;
}

static ClusterRecoveryCensusResult
census_tt_terminal(ClusterRecoveryCensus *census, ClusterRecoveryCensusEntry *out)
{
	ClusterRecoveryCensusDiag *d = &census->diag;
	ClusterRecoveryCensusTTTerminal *value = &out->value.tt_terminal;
	ClusterRecoveryUndoEntry *physical = &value->physical;
	ClusterRecoveryTxnKey key = { 0 };

	d->graph_result
		= cluster_recovery_undo_store_at(census->undo, census->tt_terminal_at, physical);
	if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED) {
		d->phase = CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL;
		return CLUSTER_RECOVERY_CENSUS_MORE;
	}
	if (d->graph_result != CLUSTER_RECOVERY_GRAPH_RECORD)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	value->key_usable = TransactionIdIsNormal(physical->slot.xid)
						&& physical->slot.wrap != TT_WRAP_INVALID
						&& physical->segment_generation != UINT32_MAX;
	if (value->key_usable) {
		key.system_identifier = census->system_identifier;
		key.database_incarnation = census->database_incarnation;
		key.origin_node_id = physical->owner_instance - 1;
		key.segment_id = physical->segment_id;
		key.segment_generation = physical->segment_generation;
		key.slot_offset = physical->slot_offset;
		key.slot_wrap = physical->slot.wrap;
		key.xid = physical->slot.xid;
		d->terminal_result = cluster_recovery_native_inventory_terminal_lookup_step(
			census->inventory, &key, &value->retained, &d->inventory);
		if (d->terminal_result == CLUSTER_RECOVERY_TERMINAL_MORE)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		if (d->terminal_result != CLUSTER_RECOVERY_TERMINAL_ENTRY)
			return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		if (value->retained.exact_records == 0)
			d->tt_terminal_missing++;
		if (value->retained.unresolved_records != 0)
			d->tt_terminal_unresolved++;
		if (value->retained.conflict)
			d->tt_terminal_conflicts++;
		value->raw_terminal_matches
			= value->retained.exact_records != 0 && value->retained.unresolved_records == 0
			  && !value->retained.conflict
			  && ((value->retained.has_commit && physical->slot.status == TT_SLOT_COMMITTED
				   && physical->slot.commit_scn == value->retained.commit_scn)
				  || (value->retained.has_abort && physical->slot.status == TT_SLOT_ABORTED
					  && physical->slot.commit_scn == InvalidScn));
	}
	census->tt_terminal_at++;
	d->tt_terminal_slots++;
	return census_entry(out, CLUSTER_RECOVERY_CENSUS_TT_TERMINAL);
}

static ClusterRecoveryCensusResult
census_step(ClusterRecoveryCensus *census, ClusterRecoveryCensusEntry *out)
{
	ClusterRecoveryCensusDiag *d = &census->diag;

	/* Cheap original scope/owner check, not a substitute for full final
	 * ROOT/claim/pin/WAL revalidation. All intermediate events are provisional. */
	if (cluster_wal_inputs_count_v1(census->inputs) != census->source_count)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	switch (d->phase) {
	case CLUSTER_RECOVERY_CENSUS_WAL:
		d->inventory_result = cluster_recovery_native_inventory_collect_roots(
			census->inputs, census->roots, census->child_budget, &census->inventory, &d->inventory);
		if (d->inventory_result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
			break;
		d->phase = CLUSTER_RECOVERY_CENSUS_ROOTS;
		return CLUSTER_RECOVERY_CENSUS_MORE;
	case CLUSTER_RECOVERY_CENSUS_ROOTS: {
		ClusterRecoveryRootsEvent event;
		d->roots_result = cluster_recovery_root_set_step(census->roots, &event);
		if (d->roots_result == CLUSTER_RECOVERY_ROOTS_OBSERVED)
			d->phase = CLUSTER_RECOVERY_CENSUS_TT;
		if (d->roots_result <= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	}
	case CLUSTER_RECOVERY_CENSUS_TT:
		if (census->undo == NULL) {
			d->graph_result = cluster_recovery_undo_store_begin(census->roots, census->child_budget,
																&census->undo);
			if (d->graph_result != CLUSTER_RECOVERY_GRAPH_MORE)
				break;
			d->graph_result = cluster_recovery_undo_store_graph_begin(
				census->undo, census->system_identifier, census->database_incarnation,
				census->child_budget);
			if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE)
				return CLUSTER_RECOVERY_CENSUS_MORE;
			break;
		}
		d->graph_result = cluster_recovery_undo_store_next(census->undo, &out->value.tt);
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			d->tt_slots++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_TT_SLOT);
		}
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED) {
			census->graph = cluster_recovery_undo_store_graph(census->undo);
			if (census->graph == NULL)
				break;
			d->phase = CLUSTER_RECOVERY_CENSUS_WAL_HEADS;
			return CLUSTER_RECOVERY_CENSUS_MORE;
		}
		break;
	case CLUSTER_RECOVERY_CENSUS_WAL_HEADS:
		d->graph_result = cluster_recovery_native_inventory_seed_graph_step(
			census->inventory, census->graph, &d->inventory);
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED)
			d->phase = CLUSTER_RECOVERY_CENSUS_PAGE_ROOTS;
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE
			|| d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_PAGE_ROOTS:
		if (census->page_roots == NULL)
			d->graph_result = cluster_recovery_page_roots_begin(census->undo, census->child_budget,
																  &census->page_roots);
		else
			d->graph_result = cluster_recovery_page_roots_step(census->page_roots, &d->page_roots);
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED) {
			cluster_recovery_page_roots_destroy(census->page_roots);
			census->page_roots = NULL;
			d->phase = CLUSTER_RECOVERY_CENSUS_TX;
		}
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE
			|| d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_TX:
		d->graph_result = cluster_recovery_reference_graph_step(census->graph, &out->value.tx);
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			d->tx_records++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_TX_RECORD);
		}
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED)
			d->phase = CLUSTER_RECOVERY_CENSUS_TX_TARGETS;
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE
			|| d->graph_result == CLUSTER_RECOVERY_GRAPH_PENDING
			|| d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_TX_TARGETS:
		return census_target(census, out);
	case CLUSTER_RECOVERY_CENSUS_RELATIONS:
		if (census->relations == NULL) {
			d->relation_result = cluster_recovery_relation_census_begin(
				census->roots, census->graph, census->child_budget, &census->relations);
		} else
			d->relation_result
				= cluster_recovery_relation_census_next(census->relations, &out->value.relation);
		if (d->relation_result == CLUSTER_RECOVERY_RELATIONS_ENTRY) {
			ClusterRecoveryCensusResult result = census_remember_page(census, &out->value.relation);
			if (result == CLUSTER_RECOVERY_CENSUS_MORE)
				result = census_remember_reference(census, &out->value.relation);
			if (result != CLUSTER_RECOVERY_CENSUS_MORE)
				return census_fail(census, result);
			d->relation_entries++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_RELATION);
		}
		if (d->relation_result == CLUSTER_RECOVERY_RELATIONS_DRAINED) {
			cluster_recovery_relation_census_destroy(census->relations);
			census->relations = NULL;
			d->phase = CLUSTER_RECOVERY_CENSUS_SLRU_PHYSICAL;
		}
		if (d->relation_result == CLUSTER_RECOVERY_RELATIONS_MORE
			|| d->relation_result == CLUSTER_RECOVERY_RELATIONS_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_SLRU_PHYSICAL:
		if (census->slru == NULL)
			d->slru_result = cluster_recovery_slru_scan_begin(census->roots, census->child_budget,
															  &census->slru);
		else
			d->slru_result = cluster_recovery_slru_scan_next(census->slru, &out->value.slru);
		if (d->slru_result == CLUSTER_RECOVERY_SLRU_ENTRY) {
			d->slru_entries++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_SLRU_SLOT);
		}
		if (d->slru_result == CLUSTER_RECOVERY_SLRU_DRAINED) {
			cluster_recovery_slru_scan_destroy(census->slru);
			census->slru = NULL;
			d->phase = CLUSTER_RECOVERY_CENSUS_MX_PHYSICAL;
		}
		if (d->slru_result == CLUSTER_RECOVERY_SLRU_MORE
			|| d->slru_result == CLUSTER_RECOVERY_SLRU_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_MX_PHYSICAL:
		if (census->mx == NULL)
			d->mx_result = cluster_recovery_multixact_scan_begin(census->roots,
																 census->child_budget, &census->mx);
		else
			d->mx_result = cluster_recovery_multixact_scan_next(census->mx, &out->value.mx);
		if (d->mx_result == CLUSTER_RECOVERY_MX_ENTRY) {
			d->mx_entries++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_MX_SLOT);
		}
		if (d->mx_result == CLUSTER_RECOVERY_MX_DRAINED) {
			cluster_recovery_multixact_scan_destroy(census->mx);
			census->mx = NULL;
			d->phase = CLUSTER_RECOVERY_CENSUS_MX_COMPARE;
		}
		if (d->mx_result == CLUSTER_RECOVERY_MX_MORE || d->mx_result == CLUSTER_RECOVERY_MX_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_MX_COMPARE:
		d->join_result = cluster_recovery_native_inventory_multixact_step(
			census->inventory, census->roots, census->child_budget, &out->value.compare,
			&d->inventory);
		if (d->join_result == CLUSTER_RECOVERY_MX_JOIN_ENTRY) {
			d->mx_comparisons++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_MX_MATCH);
		}
		if (d->join_result == CLUSTER_RECOVERY_MX_JOIN_DRAINED)
			d->phase = CLUSTER_RECOVERY_CENSUS_XACT_COMPARE;
		if (d->join_result == CLUSTER_RECOVERY_MX_JOIN_MORE
			|| d->join_result == CLUSTER_RECOVERY_MX_JOIN_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_XACT_COMPARE:
		d->xact_result = cluster_recovery_native_inventory_xact_step(
			census->inventory, census->roots, census->child_budget, &out->value.xact_compare,
			&d->inventory);
		if (d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_ENTRY) {
			d->xact_comparisons++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_XACT_MATCH);
		}
		if (d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_DRAINED)
			d->phase = CLUSTER_RECOVERY_CENSUS_TERMINALS;
		if (d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_MORE
			|| d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_TERMINALS: {
		ClusterRecoveryCensusTerminal *terminal = &out->value.terminal;
		ClusterRecoveryTxnKey *key = &terminal->terminal.transaction;
		d->terminal_result = cluster_recovery_native_inventory_terminal_step(
			census->inventory, &terminal->terminal, &d->inventory);
		if (d->terminal_result == CLUSTER_RECOVERY_TERMINAL_ENTRY) {
			uint8 digest[32];
			if (!cluster_recovery_root_set_digest(census->roots, digest))
				break;
			if (terminal->terminal.identity == CLUSTER_RECOVERY_TERMINAL_EXACT
				|| terminal->terminal.identity == CLUSTER_RECOVERY_TERMINAL_BOUND) {
				const UndoSegmentHeaderData *header;
				if (key->system_identifier != census->system_identifier
					|| key->database_incarnation != census->database_incarnation
					|| key->slot_offset >= TT_SLOTS_PER_SEGMENT)
					break;
				header = cluster_recovery_undo_store_header(census->undo, key->segment_id);
				if (header == NULL)
					d->terminal_missing++;
				else {
					terminal->physical_present = true;
					terminal->physical_generation = header->wrap_count;
					terminal->physical = header->tt_slots[key->slot_offset];
					terminal->identity_matches = header->owner_instance == key->origin_node_id + 1
												 && header->segment_id == key->segment_id
												 && header->wrap_count == key->segment_generation
												 && terminal->physical.xid == key->xid
												 && terminal->physical.wrap == key->slot_wrap;
					terminal->raw_terminal_matches
						= terminal->identity_matches
						  && terminal->physical.status == terminal->terminal.status
						  && terminal->physical.commit_scn == terminal->terminal.scn;
					if (!terminal->identity_matches)
						d->terminal_identity_mismatches++;
					else if (!terminal->raw_terminal_matches)
						d->terminal_state_mismatches++;
				}
			}
			/* Equality does not discharge the record's effects or lifecycle. */
			d->terminal_entries++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_TERMINAL);
		}
		if (d->terminal_result == CLUSTER_RECOVERY_TERMINAL_DRAINED)
			d->phase = CLUSTER_RECOVERY_CENSUS_TT_TERMINALS;
		if (d->terminal_result == CLUSTER_RECOVERY_TERMINAL_MORE
			|| d->terminal_result == CLUSTER_RECOVERY_TERMINAL_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	}
	case CLUSTER_RECOVERY_CENSUS_TT_TERMINALS:
		return census_tt_terminal(census, out);
	case CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL:
		if (census->relmap == NULL)
			d->relmap_result = cluster_recovery_relmap_scan_begin(
				census->roots, census->system_identifier, census->database_incarnation,
				census->storage_uuid, census->child_budget, &census->relmap);
		else
			d->relmap_result
				= cluster_recovery_relmap_scan_next(census->relmap, &out->value.relmap);
		if (d->relmap_result == CLUSTER_RECOVERY_RELMAP_ENTRY) {
			d->relmap_entries++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_RELMAP);
		}
		if (d->relmap_result == CLUSTER_RECOVERY_RELMAP_DRAINED) {
			cluster_recovery_relmap_scan_destroy(census->relmap);
			census->relmap = NULL;
			d->phase = CLUSTER_RECOVERY_CENSUS_EFFECTS;
		}
		if (d->relmap_result == CLUSTER_RECOVERY_RELMAP_MORE
			|| d->relmap_result == CLUSTER_RECOVERY_RELMAP_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	case CLUSTER_RECOVERY_CENSUS_EFFECTS:
		d->inventory_result
			= cluster_recovery_native_inventory_next(census->inventory, &out->value.effect);
		if (d->inventory_result == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
			d->effects++;
			return census_entry(out, CLUSTER_RECOVERY_CENSUS_NATIVE_EFFECT);
		}
		if (d->inventory_result == CLUSTER_RECOVERY_NATIVE_INVENTORY_END) {
			d->phase = CLUSTER_RECOVERY_CENSUS_FINAL_TT;
			return CLUSTER_RECOVERY_CENSUS_MORE;
		}
		break;
	case CLUSTER_RECOVERY_CENSUS_FINAL_TT:
		d->graph_result = cluster_recovery_undo_store_recheck(census->undo);
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_MORE)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		if (d->graph_result == CLUSTER_RECOVERY_GRAPH_DRAINED) {
			d->roots_result = cluster_recovery_root_set_recheck_begin(census->roots);
			if (d->roots_result == CLUSTER_RECOVERY_ROOTS_MORE) {
				d->phase = CLUSTER_RECOVERY_CENSUS_FINAL_ROOTS;
				return CLUSTER_RECOVERY_CENSUS_MORE;
			}
		}
		break;
	case CLUSTER_RECOVERY_CENSUS_FINAL_ROOTS: {
		ClusterRecoveryRootsEvent event;
		d->roots_result = cluster_recovery_root_set_step(census->roots, &event);
		if (d->roots_result == CLUSTER_RECOVERY_ROOTS_OBSERVED)
			d->phase = CLUSTER_RECOVERY_CENSUS_FINAL_WAL;
		if (d->roots_result <= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
	}
	case CLUSTER_RECOVERY_CENSUS_FINAL_WAL:
		d->inventory_result = cluster_recovery_native_inventory_recheck(
			census->inputs, census->inventory, &d->inventory);
		if (d->inventory_result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
			|| !census_build_manifest(census)
			|| !census_pages_digest(census, census->pages_sha256)
			|| !census_references_digest(census, census->references_sha256))
			break;
		census->pages_complete = true;
		d->phase = CLUSTER_RECOVERY_CENSUS_DONE;
		census->status = CLUSTER_RECOVERY_CENSUS_DRAINED;
		return census->status;
	case CLUSTER_RECOVERY_CENSUS_DONE: {
		uint8 digest[32], inputs_digest[32], pages_digest[32], references_digest[32];
		d->input_result = cluster_wal_inputs_revalidate_v1(census->inputs);
		if (d->input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_recovery_root_set_digest(census->roots, digest)
			|| !cluster_recovery_reference_graph_observed(census->graph)
			|| !cluster_recovery_native_inventory_digest(census->inventory, inputs_digest)
			|| !census->pages_complete || !census_pages_digest(census, pages_digest)
			|| memcmp(pages_digest, census->pages_sha256, 32) != 0
			|| !census_references_digest(census, references_digest)
			|| memcmp(references_digest, census->references_sha256, 32) != 0
			|| census->manifest.encoding_version != 1
			|| census->manifest.source_count != census->source_count
			|| census->manifest.root_count != cluster_recovery_root_set_count(census->roots)
			|| census->manifest.tx_record_count
				   != cluster_recovery_reference_graph_count(census->graph)
			|| memcmp(digest, census->manifest.roots_sha256, 32) != 0
			|| memcmp(inputs_digest, census->manifest.inputs_sha256, 32) != 0)
			break;
		return CLUSTER_RECOVERY_CENSUS_DRAINED;
	}
	}
	return census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
}

ClusterRecoveryCensusResult
cluster_recovery_census_next(ClusterRecoveryCensus *census, ClusterRecoveryCensusEntry *out)
{
	ClusterRecoveryCensusResult result;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (census == NULL)
		return CLUSTER_RECOVERY_CENSUS_INVALID;
	if (census->status > CLUSTER_RECOVERY_CENSUS_DRAINED)
		return census->status;
	if (out == NULL)
		return census_fail(census, CLUSTER_RECOVERY_CENSUS_INVALID);
	PG_TRY();
	{
		int fd = -1;

		if (census->ctrc_namespace
			&& ((census->diag.input_result = cluster_wal_inputs_ctrc_namespace_v1(
				census->inputs, &fd)) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| fd != census->ctrc_namespace_fd))
			result = census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		else
			result = census_step(census, out);
	}
	PG_CATCH();
	{
		memset(out, 0, sizeof(*out));
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result != CLUSTER_RECOVERY_CENSUS_ENTRY)
		memset(out, 0, sizeof(*out));
	return result;
}

bool
cluster_recovery_census_manifest(ClusterRecoveryCensus *census, ClusterRecoveryCensusManifest *out)
{
	ClusterRecoveryCensusEntry entry;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || census == NULL || census->status != CLUSTER_RECOVERY_CENSUS_DRAINED)
		return false;
	/* Use the original DONE path and its ERROR cleanup. Do not turn an
	 * accessor call into implicit progress through a partial observation. */
	if (cluster_recovery_census_next(census, &entry) != CLUSTER_RECOVERY_CENSUS_DRAINED)
		return false;
	*out = census->manifest;
	return true;
}

static void
census_page_lock_held(LWLock *lock, LWLockMode mode, void *arg)
{
	(void)lock; (void)mode;
	*(bool *)arg = true;
}

ClusterRecoveryPagePlanBuildV1
cluster_recovery_census_page_plan_v1(ClusterRecoveryCensus *census, Size memory_budget)
{
	ClusterRecoveryPagePlanBuildV1 result = {NULL, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT};
	ClusterRecoveryCensusManifest before, after;
	RfPageOnlinePlanV1 *volatile plan = NULL;
	bool held = false;

	if (census == NULL || census->status != CLUSTER_RECOVERY_CENSUS_DRAINED
		|| memory_budget == 0 || CritSectionCount != 0 || ShutdownRequestPending)
		return result;
	/* The manifest path itself needs native owner snapshots and may do I/O. */
	ForEachLWLockHeldByMe(census_page_lock_held, &held);
	if (held) {
		result.detail = RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		return result;
	}
	PG_TRY();
	{
		if (cluster_recovery_census_manifest(census, &before)) {
			result = cluster_recovery_native_inventory_page_plan_v1(census->inputs,
				census->inventory, memory_budget);
			plan = result.plan;
			if (result.detail == RF_PAGE_PROOF_DETAIL_OK && plan != NULL
				&& cluster_recovery_census_manifest(census, &after)
				&& memcmp(&before, &after, sizeof(before)) == 0)
				plan = NULL; /* caller receives the independent private graph */
			else {
				RfPageOnlinePlanV1 *discard = plan;
				plan = NULL;
				rf_page_online_plan_destroy_v1(&discard);
				result.plan = NULL;
				if (result.detail == RF_PAGE_PROOF_DETAIL_OK)
					result.detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
				(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
			}
		} else
			result.detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	}
	PG_CATCH();
	{
		RfPageOnlinePlanV1 *discard = plan;
		plan = NULL;
		rf_page_online_plan_destroy_v1(&discard);
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

const ClusterRecoveryCensusPagesV1 *
cluster_recovery_census_pages_v1(ClusterRecoveryCensus *census)
{
	ClusterRecoveryCensusManifest manifest;

	/* The original manifest path owns DONE and ERROR revalidation. Never
	 * advance a partial census or export a prefix after capacity refusal. */
	return cluster_recovery_census_manifest(census, &manifest) ? census->pages : NULL;
}

ClusterRecoveryCensusPageDigestV1
cluster_recovery_census_page_digest_v1(ClusterRecoveryCensus *census, uint32 index)
{
	ClusterRecoveryCensusPageDigestV1 result = {0};
	ClusterRecoveryCensusManifest before, after;
	ClusterRecoveryCensusPageV1 saved;
	PGAlignedBlock page;
	uint8 digest[32];
	bool held = false;
	volatile bool ok = false;

	if (census == NULL || census->status != CLUSTER_RECOVERY_CENSUS_DRAINED
		|| CritSectionCount != 0 || ShutdownRequestPending) return result;
	/* Preflight before manifest callbacks/I/O. DONE has retired the original
	 * sequential relation scratch; its fixed budget covers this one block. */
	ForEachLWLockHeldByMe(census_page_lock_held, &held);
	if (held || index >= census->page_count) return result;
	PG_TRY();
	{
		if (cluster_recovery_census_manifest(census, &before)) {
			saved = census->pages->entries[index];
			ok = cluster_recovery_root_set_read_current_relation(census->roots, saved.root_index,
				(uint64)saved.blockno * BLCKSZ, page.data, BLCKSZ) == CLUSTER_RECOVERY_ROOTS_BYTES
				&& census_hash_bytes(page.data, BLCKSZ, digest)
				&& cluster_recovery_census_manifest(census, &after)
				&& memcmp(&before, &after, sizeof(before)) == 0
				&& memcmp(&saved, &census->pages->entries[index], sizeof(saved)) == 0;
		}
		if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	}
	PG_CATCH();
	{
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ok) { result.observed = true; memcpy(result.sha256, digest, 32); }
	return result;
}

static bool
census_namespace_current(void *arg)
{
	ClusterRecoveryCensus *census = arg;

	HandleStartupProcInterrupts();
	/* Do not release the borrowed root set while its scanner is active. The
	 * enclosing census owns failure/ERROR cleanup after scanner retirement. */
	return census->status == CLUSTER_RECOVERY_CENSUS_DRAINED
		&& cluster_wal_inputs_count_v1(census->inputs) == census->source_count
		&& cluster_wal_inputs_revalidate_v1(census->inputs) == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_recovery_census_namespace_current_v1(ClusterRecoveryCensus *census)
{
	ClusterRecoveryCensusManifest before, after;
	bool held = false;
	volatile bool ok = false;

	if (census == NULL || census->status != CLUSTER_RECOVERY_CENSUS_DRAINED
		|| CritSectionCount != 0 || ShutdownRequestPending) return false;
	ForEachLWLockHeldByMe(census_page_lock_held, &held);
	if (held) return false;
	PG_TRY();
	{
		ok = cluster_recovery_census_manifest(census, &before)
			&& cluster_recovery_root_set_namespace_current_v1(census->roots,
				census_namespace_current, census)
			&& cluster_recovery_census_manifest(census, &after)
			&& memcmp(&before, &after, sizeof(before)) == 0;
		if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	}
	PG_CATCH();
	{
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

const ClusterRecoveryCensusReferencesV1 *
cluster_recovery_census_references_v1(ClusterRecoveryCensus *census)
{
	ClusterRecoveryCensusManifest manifest;

	return cluster_recovery_census_manifest(census, &manifest) ? census->references : NULL;
}

/* The original forest owns every complete record. This accessor neither
 * walks nor qualifies a new graph, and cannot advance a partial census. */
ClusterRecoveryCensusTxRecordV1
cluster_recovery_census_tx_record_v1(ClusterRecoveryCensus *census, uint32 index)
{
	ClusterRecoveryCensusTxRecordV1 out = {0};
	ClusterRecoveryCensusManifest before, after;
	ClusterRecoveryGraphRecord record;
	ClusterRecoveryGraphResult result;
	bool held = false;
	bool ok = false;

	if (census == NULL || census->status != CLUSTER_RECOVERY_CENSUS_DRAINED
		|| CritSectionCount != 0 || ShutdownRequestPending
		|| index > census->manifest.tx_record_count) return out;
	ForEachLWLockHeldByMe(census_page_lock_held, &held);
	if (held) return out;
	PG_TRY();
	{
		if (cluster_recovery_census_manifest(census, &before)) {
			result = cluster_recovery_reference_graph_at(census->graph, index, &record);
			ok = (index == before.tx_record_count
					? result == CLUSTER_RECOVERY_GRAPH_DRAINED && record.record == NULL
					: result == CLUSTER_RECOVERY_GRAPH_RECORD && record.record != NULL)
				&& cluster_recovery_census_manifest(census, &after)
				&& memcmp(&before, &after, sizeof(before)) == 0;
			if (ok) {
				out.observed = true;
				out.count = before.tx_record_count;
				out.value = record;
			}
		}
		if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	}
	PG_CATCH();
	{
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return out;
}

bool
cluster_recovery_census_wal_source_count_v1(ClusterRecoveryCensus *census, uint32 *out)
{
	ClusterRecoveryCensusManifest manifest;
	bool ok = false;

	if (out != NULL) *out = 0;
	if (out == NULL || census == NULL)
		return false;
	PG_TRY();
	{
		if (cluster_recovery_census_manifest(census, &manifest)) {
			ok = cluster_recovery_native_inventory_source_count_v1(census->inventory, out)
				&& *out == manifest.source_count;
			if (!ok) {
				*out = 0;
				(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
			}
		}
	}
	PG_CATCH();
	{
		*out = 0;
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_recovery_census_wal_source_at_v1(ClusterRecoveryCensus *census, uint32 index,
	ClusterRecoveryWalSourceObservationV1 *out)
{
	uint32 count;
	bool ok = false;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_census_wal_source_count_v1(census, &count)
		|| index >= count)
		return false;
	PG_TRY();
	{
		ok = cluster_recovery_native_inventory_source_at_v1(census->inventory, index, out);
		if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	}
	PG_CATCH();
	{
		memset(out, 0, sizeof(*out));
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_recovery_census_page_wal_count_v1(ClusterRecoveryCensus *census, uint32 *out)
{
	ClusterRecoveryCensusManifest manifest;
	bool ok = false;

	if (out != NULL) *out = 0;
	if (out == NULL || census == NULL)
		return false;
	PG_TRY();
	{
		if (cluster_recovery_census_manifest(census, &manifest)) {
			ok = cluster_recovery_native_inventory_page_count_v1(census->inventory, out);
			if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		}
	}
	PG_CATCH();
	{
		*out = 0;
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_recovery_census_page_wal_at_v1(ClusterRecoveryCensus *census, uint32 index,
	ClusterRecoveryPageWalV1 *out)
{
	uint32 count;
	bool ok = false;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_census_page_wal_count_v1(census, &count)
		|| index >= count)
		return false;
	PG_TRY();
	{
		ok = cluster_recovery_native_inventory_page_at_v1(census->inventory, index, out);
		if (!ok) (void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	}
	PG_CATCH();
	{
		memset(out, 0, sizeof(*out));
		(void)census_fail(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_recovery_census_diag(const ClusterRecoveryCensus *census, ClusterRecoveryCensusDiag *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || census == NULL)
		return false;
	*out = census->diag;
	return true;
}

void
cluster_recovery_census_destroy(ClusterRecoveryCensus *census)
{
	if (census != NULL) {
		census_release(census);
		free(census);
	}
}
#endif
