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
#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryCensus {
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeInventory *inventory;
	ClusterRecoveryUndoStore *undo;
	ClusterRecoveryReferenceGraph *graph; /* owned by undo */
	ClusterRecoveryRelationCensus *relations;
	ClusterRecoveryMultixactScan *mx;
	ClusterRecoverySlruScan *slru;
	ClusterRecoveryRelmapScan *relmap;
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusResult status;
	uint32 source_count, target_at;
	uint8 storage_uuid[16];
	uint64 system_identifier, database_incarnation;
	size_t child_budget;
};

static void
census_release(ClusterRecoveryCensus *census)
{
	/* Dependents precede their owners. Inventory destruction revokes its
	 * borrowed graph/roots, so both must still be alive here. */
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
			d->phase = CLUSTER_RECOVERY_CENSUS_TX;
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
			d->phase = CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL;
		if (d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_MORE
			|| d->xact_result == CLUSTER_RECOVERY_XACT_JOIN_DRAINED)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		break;
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
		if (d->inventory_result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
			break;
		d->phase = CLUSTER_RECOVERY_CENSUS_DONE;
		census->status = CLUSTER_RECOVERY_CENSUS_DRAINED;
		return census->status;
	case CLUSTER_RECOVERY_CENSUS_DONE: {
		uint8 digest[32];
		d->input_result = cluster_wal_inputs_revalidate_v1(census->inputs);
		if (d->input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_recovery_root_set_digest(census->roots, digest)
			|| !cluster_recovery_reference_graph_observed(census->graph))
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
