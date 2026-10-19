/*-------------------------------------------------------------------------
 *
 * cluster_relmap_pending.c
 *    Bounded structural pending obligations for relmap DROP/TRUNCATE.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_pending.h"

#ifdef USE_PGRAC_CLUSTER

static bool
pending_kind_valid(ClusterRelmapPendingKind kind)
{
	return kind == CLUSTER_RELMAP_PENDING_DROP ||
		kind == CLUSTER_RELMAP_PENDING_TRUNCATE;
}

static bool
pending_uuid_valid(const uint8 *uuid)
{
	bool nonzero = false;

	for (int i = 0; i < 16; i++)
		if (uuid[i] != 0)
		{
			nonzero = true;
			break;
		}
	return nonzero;
}

static bool
pending_identity_valid(const ClusterSpaceIdentityKey *identity)
{
	return identity != NULL && identity->system_identifier != 0 &&
		identity->database_incarnation != 0 && pending_uuid_valid(identity->storage_uuid) &&
		identity->locator.spcOid != InvalidOid &&
		RelFileNumberIsValid(identity->locator.relNumber);
}

static bool
pending_namespace_equal(const ClusterRelmapPendingSet *set,
							const ClusterSpaceIdentityKey *identity)
{
	return set->has_namespace &&
		set->system_identifier == identity->system_identifier &&
		set->database_incarnation == identity->database_incarnation;
}

static bool
pending_locator_equal(const ClusterSpaceIdentityKey *left,
						  const ClusterSpaceIdentityKey *right)
{
	return RelFileLocatorEquals(left->locator, right->locator);
}

static bool
pending_identity_equal(const ClusterSpaceIdentityKey *left,
						   const ClusterSpaceIdentityKey *right)
{
	return left->system_identifier == right->system_identifier &&
		left->database_incarnation == right->database_incarnation &&
		memcmp(left->storage_uuid, right->storage_uuid, sizeof(left->storage_uuid)) == 0 &&
		pending_locator_equal(left, right);
}

static int
pending_find_exact(const ClusterRelmapPendingSet *set,
					  ClusterRelmapPendingKind kind,
					  const ClusterSpaceIdentityKey *identity)
{
	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
	{
		const ClusterRelmapPendingEntry *entry = &set->entries[i];

		if (entry->occupied && entry->kind == kind &&
			pending_identity_equal(&entry->identity, identity))
			return i;
	}
	return -1;
}

static bool
pending_proofs_complete(const ClusterRelmapPendingEntry *entry)
{
	return entry->main_placeholder && entry->copies_cleared_readback &&
		entry->native_terminal && entry->tt_complete && entry->space_complete;
}

static bool
pending_set_empty(const ClusterRelmapPendingSet *set)
{
	if (set == NULL || set->has_namespace)
		return false;

	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
		if (set->entries[i].occupied)
			return false;
	return true;
}

void
cluster_relmap_pending_init(ClusterRelmapPendingSet *set)
{
	if (set != NULL)
		memset(set, 0, sizeof(*set));
}

ClusterRelmapPendingResult
cluster_relmap_pending_rebuild_from_committed(
	ClusterRelmapPendingSet *set, const ClusterRelmapV2Namespace *expected,
	ClusterRelmapV2PairResult pair_result,
	const ClusterRelmapV2View *view,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count)
{
	ClusterRelmapPendingSet rebuilt;

	if (set == NULL || expected == NULL || view == NULL ||
		expected->system_identifier == 0 || expected->database_incarnation == 0 ||
		expected->shared != (expected->dbid == InvalidOid) ||
		(descriptor_count > 0 && descriptors == NULL))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	if (pair_result != CLUSTER_RELMAP_V2_PAIR_VALID ||
		!view->pending_complete || view->committed_generation == 0)
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	if (!pending_set_empty(set))
		return CLUSTER_RELMAP_PENDING_CONFLICT;
	if (descriptor_count > CLUSTER_RELMAP_PENDING_CAPACITY)
		return CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED;

	cluster_relmap_pending_init(&rebuilt);
	for (size_t i = 0; i < descriptor_count; i++)
	{
		ClusterRelmapPendingResult result;

		if (descriptors[i].identity.system_identifier != expected->system_identifier ||
			descriptors[i].identity.database_incarnation != expected->database_incarnation ||
			descriptors[i].identity.locator.dbOid != expected->dbid ||
			(descriptors[i].identity.locator.spcOid == GLOBALTABLESPACE_OID) !=
			expected->shared)
			return CLUSTER_RELMAP_PENDING_CONFLICT;
		result = cluster_relmap_pending_add(&rebuilt, descriptors[i].kind,
														&descriptors[i].identity, true);
		if (result != CLUSTER_RELMAP_PENDING_OK)
			return result;
	}

	*set = rebuilt;
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_pending_add(ClusterRelmapPendingSet *set,
							ClusterRelmapPendingKind kind,
							const ClusterSpaceIdentityKey *identity,
							bool main_placeholder)
{
	int free_index = -1;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity) ||
		!main_placeholder)
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	if (set->has_namespace && !pending_namespace_equal(set, identity))
		return CLUSTER_RELMAP_PENDING_CONFLICT;

	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
	{
		ClusterRelmapPendingEntry *entry = &set->entries[i];

		if (!entry->occupied)
		{
			if (free_index < 0)
				free_index = i;
			continue;
		}
		/* A physical locator remains reserved across UUID and kind changes. */
		if (pending_locator_equal(&entry->identity, identity))
			return CLUSTER_RELMAP_PENDING_CONFLICT;
	}
	if (free_index < 0)
		return CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED;

	if (!set->has_namespace)
	{
		set->has_namespace = true;
		set->system_identifier = identity->system_identifier;
		set->database_incarnation = identity->database_incarnation;
	}
	memset(&set->entries[free_index], 0, sizeof(set->entries[free_index]));
	set->entries[free_index].identity = *identity;
	set->entries[free_index].kind = kind;
	set->entries[free_index].main_placeholder = true;
	set->entries[free_index].occupied = true;
	return CLUSTER_RELMAP_PENDING_OK;
}

bool
cluster_relmap_pending_contains(const ClusterRelmapPendingSet *set,
								ClusterRelmapPendingKind kind,
								const ClusterSpaceIdentityKey *identity)
{
	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return false;
	return pending_find_exact(set, kind, identity) >= 0;
}

bool
cluster_relmap_pending_rel_number_reserved(const ClusterRelmapPendingSet *set,
											const ClusterSpaceIdentityKey *identity)
{
	if (set == NULL || !pending_identity_valid(identity) ||
		!pending_namespace_equal(set, identity))
		return false;

	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
		if (set->entries[i].occupied &&
			pending_locator_equal(&set->entries[i].identity, identity))
			return true;
	return false;
}

ClusterRelmapPendingResult
cluster_relmap_pending_mark_copies_cleared(ClusterRelmapPendingSet *set,
									ClusterRelmapPendingKind kind,
									const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	index = pending_find_exact(set, kind, identity);
	if (index < 0)
		return CLUSTER_RELMAP_PENDING_NOT_FOUND;
	set->entries[index].copies_cleared_readback = true;
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_pending_mark_native_terminal(ClusterRelmapPendingSet *set,
									ClusterRelmapPendingKind kind,
									const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	index = pending_find_exact(set, kind, identity);
	if (index < 0)
		return CLUSTER_RELMAP_PENDING_NOT_FOUND;
	set->entries[index].native_terminal = true;
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_pending_mark_tt_complete(ClusterRelmapPendingSet *set,
								ClusterRelmapPendingKind kind,
								const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	index = pending_find_exact(set, kind, identity);
	if (index < 0)
		return CLUSTER_RELMAP_PENDING_NOT_FOUND;
	set->entries[index].tt_complete = true;
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_pending_mark_space_complete(ClusterRelmapPendingSet *set,
									ClusterRelmapPendingKind kind,
									const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	index = pending_find_exact(set, kind, identity);
	if (index < 0)
		return CLUSTER_RELMAP_PENDING_NOT_FOUND;
	set->entries[index].space_complete = true;
	return CLUSTER_RELMAP_PENDING_OK;
}

bool
cluster_relmap_pending_can_release(const ClusterRelmapPendingSet *set,
								ClusterRelmapPendingKind kind,
								const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return false;
	index = pending_find_exact(set, kind, identity);
	return index >= 0 && pending_proofs_complete(&set->entries[index]);
}

ClusterRelmapPendingResult
cluster_relmap_pending_release(ClusterRelmapPendingSet *set,
							ClusterRelmapPendingKind kind,
							const ClusterSpaceIdentityKey *identity)
{
	int index;

	if (set == NULL || !pending_kind_valid(kind) || !pending_identity_valid(identity))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	index = pending_find_exact(set, kind, identity);
	if (index < 0)
		return CLUSTER_RELMAP_PENDING_NOT_FOUND;
	if (!pending_proofs_complete(&set->entries[index]))
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	memset(&set->entries[index], 0, sizeof(set->entries[index]));

	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
		if (set->entries[i].occupied)
			return CLUSTER_RELMAP_PENDING_OK;
	set->has_namespace = false;
	set->system_identifier = 0;
	set->database_incarnation = 0;
	return CLUSTER_RELMAP_PENDING_OK;
}

#endif /* USE_PGRAC_CLUSTER */
