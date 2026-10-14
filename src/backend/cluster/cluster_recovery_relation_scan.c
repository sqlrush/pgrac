/*-------------------------------------------------------------------------
 * cluster_recovery_relation_scan.c
 *   SPACE-bound physical relation candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_relation_scan.h"
#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryRelationScan {
	ClusterRecoveryRootSet *roots;
	ClusterSpaceIdentityKey key;
	ClusterSpaceReservation reservation;
	uint64 identity_token, reservation_token;
	uint32 root_index, space_index;
	uint64 blocks, block;
	ForkNumber forknum;
	unsigned stage;
	PGAlignedBlock space[2], current;
	ClusterRecoveryRelationResult status;
};

static ClusterRecoveryRelationResult
relation_fail(ClusterRecoveryRelationScan *scan, ClusterRecoveryRelationResult result)
{
	if (scan != NULL)
		scan->status = result;
	return result;
}

static ClusterRecoveryRelationResult
relation_roots_result(ClusterRecoveryRootsResult result)
{
	switch (result) {
	case CLUSTER_RECOVERY_ROOTS_CHANGED:
		return CLUSTER_RECOVERY_RELATION_CHANGED;
	case CLUSTER_RECOVERY_ROOTS_CAPACITY:
		return CLUSTER_RECOVERY_RELATION_CAPACITY;
	case CLUSTER_RECOVERY_ROOTS_IO:
		return CLUSTER_RECOVERY_RELATION_IO;
	default:
		return CLUSTER_RECOVERY_RELATION_INVALID;
	}
}

ClusterRecoveryRelationResult
cluster_recovery_relation_begin(ClusterRecoveryRootSet *roots, uint32 root_index,
								size_t memory_budget, ClusterRecoveryRelationScan **out)
{
	const ClusterRecoveryRootEntry *entry, *space;
	ClusterSpaceIdentityKey key;
	uint32 space_index;
	ClusterRecoveryRootsResult result;
	ClusterRecoveryRelationScan *scan;

	if (out != NULL)
		*out = NULL;
	if (out == NULL)
		return CLUSTER_RECOVERY_RELATION_INVALID;
	result = cluster_recovery_root_set_relation(roots, root_index, &key, &space_index);
	if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED)
		return relation_roots_result(result);
	entry = cluster_recovery_root_set_entry(roots, root_index);
	space = cluster_recovery_root_set_entry(roots, space_index);
	if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0 || space == NULL
		|| (space->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0 || space->file.size != 2 * BLCKSZ
		|| entry->required_blocks > entry->file.size / BLCKSZ)
		return CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED;
	if (entry->file.size % BLCKSZ != 0 || entry->file.size / BLCKSZ > InvalidBlockNumber)
		return CLUSTER_RECOVERY_RELATION_CORRUPT;
	/* Includes original codec stack scratch and allocator bookkeeping. */
	if (memory_budget < sizeof(*scan) + 2 * BLCKSZ)
		return CLUSTER_RECOVERY_RELATION_CAPACITY;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		return CLUSTER_RECOVERY_RELATION_CAPACITY;
	scan->roots = roots;
	scan->key = key;
	scan->root_index = root_index;
	scan->space_index = space_index;
	scan->blocks = entry->file.size / BLCKSZ;
	scan->forknum = entry->file.forknum;
	*out = scan;
	return CLUSTER_RECOVERY_RELATION_MORE;
}

ClusterRecoveryRelationResult
cluster_recovery_relation_next(ClusterRecoveryRelationScan *scan, ClusterRecoveryRelationPage *out)
{
	ClusterRecoveryRootsResult read;
	ClusterSpaceIdentity identity;
	uint64 token;
	uint8 before[CLUSTER_SPACE_IDENTITY_BYTES], after[CLUSTER_SPACE_IDENTITY_BYTES];
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return relation_fail(scan, CLUSTER_RECOVERY_RELATION_INVALID);
	if (scan->status > CLUSTER_RECOVERY_RELATION_DRAINED)
		return scan->status;
	if (!cluster_recovery_root_set_digest(scan->roots, digest))
		return relation_fail(scan, CLUSTER_RECOVERY_RELATION_CHANGED);
	if (scan->status == CLUSTER_RECOVERY_RELATION_DRAINED)
		return scan->status;
	if (scan->stage < 2) {
		read = cluster_recovery_root_set_read(scan->roots, scan->space_index,
											  (uint64)scan->stage * BLCKSZ,
											  scan->space[scan->stage].data, BLCKSZ);
		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return relation_fail(scan, relation_roots_result(read));
		if (scan->stage == 0) {
			if (!cluster_space_identity_page_decode(scan->space[0].data, BLCKSZ, SPACE_FORKNUM, 0,
													&scan->key, &identity, &token))
				return relation_fail(scan, CLUSTER_RECOVERY_RELATION_CORRUPT);
			scan->reservation.identity = identity;
			scan->identity_token = token;
		} else {
			ClusterSpaceReservation reservation;

			if (!cluster_space_reservation_page_decode(scan->space[1].data, BLCKSZ, SPACE_FORKNUM,
													   1, &scan->key, &reservation, &token))
				return relation_fail(scan, CLUSTER_RECOVERY_RELATION_CORRUPT);
			if (!cluster_space_identity_encode(&scan->reservation.identity, before, sizeof(before))
				|| !cluster_space_identity_encode(&reservation.identity, after, sizeof(after)))
				return relation_fail(scan, CLUSTER_RECOVERY_RELATION_CORRUPT);
			/* A partial structural install must be completed by its typed owner.
             * Tokens are independently versioned; numeric ordering is no proof. */
			if (memcmp(before, after, sizeof(before)) != 0)
				return relation_fail(scan, CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED);
			scan->reservation = reservation;
			scan->reservation_token = token;
		}
		scan->stage++;
		return CLUSTER_RECOVERY_RELATION_MORE;
	}
	if (scan->stage == 2 && scan->block < scan->blocks) {
		read = cluster_recovery_root_set_read(scan->roots, scan->root_index, scan->block * BLCKSZ,
											  scan->current.data, BLCKSZ);
		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return relation_fail(scan, relation_roots_result(read));
		out->reservation = scan->reservation;
		out->identity_token = scan->identity_token;
		out->reservation_token = scan->reservation_token;
		out->forknum = scan->forknum;
		out->block = (BlockNumber)scan->block++;
		out->page = scan->current.data;
		return CLUSTER_RECOVERY_RELATION_PAGE;
	}
	if (scan->stage < 4) {
		unsigned which = scan->stage - 2;

		read = cluster_recovery_root_set_read(scan->roots, scan->space_index,
											  (uint64)which * BLCKSZ, scan->current.data, BLCKSZ);
		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return relation_fail(scan, relation_roots_result(read));
		if (memcmp(scan->current.data, scan->space[which].data, BLCKSZ) != 0)
			return relation_fail(scan, CLUSTER_RECOVERY_RELATION_CHANGED);
		scan->stage++;
		return CLUSTER_RECOVERY_RELATION_MORE;
	}
	scan->status = CLUSTER_RECOVERY_RELATION_DRAINED;
	return scan->status;
}

const ClusterSpaceReservation *
cluster_recovery_relation_identity(const ClusterRecoveryRelationScan *scan)
{
	uint8 digest[32];

	if (scan == NULL || scan->status != CLUSTER_RECOVERY_RELATION_DRAINED
		|| !cluster_recovery_root_set_digest(scan->roots, digest))
		return NULL;
	return &scan->reservation;
}

void
cluster_recovery_relation_destroy(ClusterRecoveryRelationScan *scan)
{
	free(scan);
}
#endif
