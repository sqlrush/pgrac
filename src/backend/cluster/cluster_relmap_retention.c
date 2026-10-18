/* PGRAC: bounded relmap pending retention state core.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "cluster/cluster_relmap_retention.h"

/* Strictly less than half of the raw-XID space, using full-width identities. */
#define RELMAP_XID_HALF UINT64CONST(0x80000000)

static bool
normal_full_xid(uint64 full)
{
	return (uint32) full >= 3;
}

static bool
same_namespace(const ClusterRelmapV2Namespace *a, const ClusterRelmapV2Namespace *b)
{
	return a->system_identifier == b->system_identifier
		&& a->database_incarnation == b->database_incarnation && a->dbid == b->dbid
		&& a->shared == b->shared;
}

static bool
same_key(const ClusterRelmapRetentionKey *a, const ClusterRelmapRetentionKey *b)
{
	return same_namespace(&a->identity, &b->identity) && a->generation == b->generation
		&& a->origin == b->origin && a->top_full_xid == b->top_full_xid
		&& a->membership_epoch == b->membership_epoch
		&& a->admitted_incarnation == b->admitted_incarnation
		&& a->boot_incarnation == b->boot_incarnation;
}

static ClusterRelmapRetentionResult
check_key(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key)
{
	if (key == NULL || key->identity.system_identifier != directory->system_identifier
		|| key->identity.database_incarnation != directory->database_incarnation
		|| key->identity.shared != (key->identity.dbid == InvalidOid)
		|| key->generation < 2 || key->origin != directory->origin
		|| !normal_full_xid(key->top_full_xid) || key->membership_epoch == 0
		|| key->admitted_incarnation == 0 || key->boot_incarnation == 0)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (key->top_full_xid < directory->native_floor
		|| (directory->inflight.serial != 0 && key->top_full_xid < directory->inflight.cutoff))
		return CLUSTER_RELMAP_RETENTION_NATIVE_GONE;
	if (key->top_full_xid > directory->last_issued)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (key->top_full_xid > PG_UINT64_MAX - RELMAP_XID_HALF
		|| directory->last_issued >= key->top_full_xid + RELMAP_XID_HALF)
		return CLUSTER_RELMAP_RETENTION_XID_WINDOW;
	return CLUSTER_RELMAP_RETENTION_OK;
}

static ClusterRelmapRetentionPin *
find_pin(ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key)
{
	int i;

	if (key == NULL)
		return NULL;
	for (i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
		if (directory->pins[i].occupied && same_key(&directory->pins[i].key, key))
			return &directory->pins[i];
	return NULL;
}

static uint64
minimum_pin(const ClusterRelmapRetentionDirectory *directory)
{
	uint64 minimum = PG_UINT64_MAX;
	int i;

	for (i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
		if (directory->pins[i].occupied)
			minimum = Min(minimum, directory->pins[i].key.top_full_xid);
	return minimum;
}

static ClusterRelmapRetentionResult
add_pin(ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	uint64 wal_end, bool restoring)
{
	ClusterRelmapRetentionResult result = check_key(directory, key);
	ClusterRelmapRetentionPin *free_pin = NULL;
	int i;

	if (result != CLUSTER_RELMAP_RETENTION_OK)
		return result;
	for (i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
		ClusterRelmapRetentionPin *pin = &directory->pins[i];

		if (!pin->occupied) {
			if (free_pin == NULL)
				free_pin = pin;
			continue;
		}
		if (!same_namespace(&pin->key.identity, &key->identity))
			continue;
		if (!same_key(&pin->key, key))
			return CLUSTER_RELMAP_RETENTION_CONFLICT;
		if (restoring)
			return pin->may_be_staged && pin->wal_end == wal_end
				? CLUSTER_RELMAP_RETENTION_OK : CLUSTER_RELMAP_RETENTION_CONFLICT;
		return pin->may_be_staged ? CLUSTER_RELMAP_RETENTION_RETAINED
			: CLUSTER_RELMAP_RETENTION_OK;
	}
	if (free_pin == NULL)
		return CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED;
	free_pin->key = *key;
	free_pin->wal_end = wal_end;
	free_pin->may_be_staged = restoring;
	free_pin->occupied = true;
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_build_start(ClusterRelmapRetentionDirectory *directory,
	uint64 system_identifier, uint64 database_incarnation, uint16 origin,
	uint64 generation, uint64 native_floor, uint64 last_issued)
{
	int i;

	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_UNKNOWN
		|| directory->generation != 0 || directory->system_identifier != 0
		|| directory->database_incarnation != 0 || directory->origin != 0
		|| directory->native_floor != 0 || directory->last_issued != 0
		|| directory->truncate_serial != 0 || directory->inflight.generation != 0
		|| directory->inflight.serial != 0 || directory->inflight.cutoff != 0)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	for (i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
		if (directory->pins[i].occupied)
			return CLUSTER_RELMAP_RETENTION_INVALID;
	if (system_identifier == 0 || database_incarnation == 0 || origin >= 128
		|| generation == 0 || !normal_full_xid(native_floor)
		|| !normal_full_xid(last_issued) || native_floor > last_issued
		|| last_issued == PG_UINT64_MAX)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	memset(directory, 0, sizeof(*directory));
	directory->system_identifier = system_identifier;
	directory->database_incarnation = database_incarnation;
	directory->origin = origin;
	directory->generation = generation;
	directory->native_floor = native_floor;
	directory->last_issued = last_issued;
	directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_build_add(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key, uint64 wal_end)
{
	ClusterRelmapRetentionResult result;

	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_BUILDING)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	result = wal_end != 0 ? add_pin(directory, key, wal_end, true)
		: CLUSTER_RELMAP_RETENTION_INVALID;
	/* Incomplete/corrupt/over-capacity enumeration cannot become COMPLETE. */
	if (result != CLUSTER_RELMAP_RETENTION_OK)
		directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	return result;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_build_finish(ClusterRelmapRetentionDirectory *directory)
{
	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_BUILDING)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	directory->state = CLUSTER_RELMAP_RETENTION_COMPLETE;
	return CLUSTER_RELMAP_RETENTION_OK;
}

void
cluster_relmap_retention_invalidate(ClusterRelmapRetentionDirectory *directory)
{
	if (directory != NULL)
		directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_begin(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key)
{
	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	return add_pin(directory, key, 0, false);
}

ClusterRelmapRetentionResult
cluster_relmap_retention_stage_start(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key, uint64 wal_end)
{
	ClusterRelmapRetentionPin *pin;

	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (wal_end == 0)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	pin = find_pin(directory, key);
	if (pin == NULL || (pin->may_be_staged && pin->wal_end != wal_end))
		return CLUSTER_RELMAP_RETENTION_CONFLICT;
	pin->wal_end = wal_end;
	pin->may_be_staged = true;
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_cancel_prepare(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key)
{
	ClusterRelmapRetentionPin *pin;

	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	pin = find_pin(directory, key);
	if (pin == NULL)
		return CLUSTER_RELMAP_RETENTION_CONFLICT;
	if (pin->may_be_staged)
		return CLUSTER_RELMAP_RETENTION_RETAINED;
	memset(pin, 0, sizeof(*pin));
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_truncate_begin(ClusterRelmapRetentionDirectory *directory,
	uint64 requested_cutoff, ClusterRelmapRetentionTruncate *reservation)
{
	uint64 cutoff;

	/* Never zero the live reservation through the output argument. */
	if (reservation == NULL || (directory != NULL && reservation == &directory->inflight))
		return CLUSTER_RELMAP_RETENTION_INVALID;
	memset(reservation, 0, sizeof(*reservation));
	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (directory->inflight.serial != 0)
		return CLUSTER_RELMAP_RETENTION_BUSY;
	if (!normal_full_xid(requested_cutoff) || requested_cutoff < directory->native_floor
		|| requested_cutoff > directory->last_issued + 1)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (directory->truncate_serial == PG_UINT64_MAX)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	cutoff = Min(requested_cutoff, minimum_pin(directory));
	directory->inflight.generation = directory->generation;
	directory->inflight.serial = ++directory->truncate_serial;
	directory->inflight.cutoff = cutoff;
	*reservation = directory->inflight;
	return CLUSTER_RELMAP_RETENTION_OK;
}

static bool
matches_truncate(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionTruncate *reservation)
{
	return reservation != NULL && reservation->serial != 0
		&& reservation->generation == directory->generation
		&& reservation->generation == directory->inflight.generation
		&& reservation->serial == directory->inflight.serial
		&& reservation->cutoff == directory->inflight.cutoff;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_truncate_complete(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionTruncate *reservation)
{
	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (!matches_truncate(directory, reservation))
		return CLUSTER_RELMAP_RETENTION_CONFLICT;
	directory->native_floor = directory->inflight.cutoff;
	memset(&directory->inflight, 0, sizeof(directory->inflight));
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_truncate_uncertain(ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionTruncate *reservation)
{
	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (!matches_truncate(directory, reservation))
		return CLUSTER_RELMAP_RETENTION_CONFLICT;
	cluster_relmap_retention_invalidate(directory);
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_allocation_check(const ClusterRelmapRetentionDirectory *directory,
	uint64 candidate)
{
	uint64 minimum;

	if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (!normal_full_xid(candidate) || candidate == PG_UINT64_MAX
		|| candidate <= directory->last_issued || candidate < directory->native_floor)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	minimum = minimum_pin(directory);
	if (minimum != PG_UINT64_MAX
		&& (minimum > PG_UINT64_MAX - RELMAP_XID_HALF || candidate >= minimum + RELMAP_XID_HALF))
		return CLUSTER_RELMAP_RETENTION_XID_WINDOW;
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_allocation_observe(ClusterRelmapRetentionDirectory *directory,
	uint64 candidate)
{
	ClusterRelmapRetentionResult result = cluster_relmap_retention_allocation_check(directory, candidate);

	if (result == CLUSTER_RELMAP_RETENTION_OK)
		directory->last_issued = candidate;
	return result;
}
