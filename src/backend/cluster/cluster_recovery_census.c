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
	/* Five concurrent reservations: roots, inventory, undo, TX graph, and
	 * sequential relation/SLRU/MX scratch. Original consumers enforce each cap.
	 * Divide only after subtraction; never overflow a sum of allocations. */
	if (memory_budget <= sizeof(*census))
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	budget = (memory_budget - sizeof(*census)) / 5;
	if (budget < 32 * BLCKSZ)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	census = calloc(1, sizeof(*census));
	if (census == NULL)
		return CLUSTER_RECOVERY_CENSUS_CAPACITY;
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
			|| !census_build_manifest(census))
			break;
		d->phase = CLUSTER_RECOVERY_CENSUS_DONE;
		census->status = CLUSTER_RECOVERY_CENSUS_DRAINED;
		return census->status;
	case CLUSTER_RECOVERY_CENSUS_DONE: {
		uint8 digest[32], inputs_digest[32];
		d->input_result = cluster_wal_inputs_revalidate_v1(census->inputs);
		if (d->input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_recovery_root_set_digest(census->roots, digest)
			|| !cluster_recovery_reference_graph_observed(census->graph)
			|| !cluster_recovery_native_inventory_digest(census->inventory, inputs_digest)
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
