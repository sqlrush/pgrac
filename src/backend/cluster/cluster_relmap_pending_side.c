/*-------------------------------------------------------------------------
 * cluster_relmap_pending_side.c
 *    Read-only original SIDE input for relmap structural pending rebuild.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_relmap_pending_side.h"

#ifdef USE_PGRAC_CLUSTER

static ClusterRelmapPendingResult
pending_side_append(const ClusterRelmapV2Namespace *expected,
					const uint8 storage_uuid[16], ClusterRelmapPendingSet *seen,
					ClusterRelmapPendingDescriptor *descriptors, size_t *count,
					size_t capacity, ClusterRelmapPendingKind kind,
					const ClusterSpaceIdentityKey *key)
{
	ClusterRelmapPendingResult result;

	if (key->system_identifier != expected->system_identifier ||
		key->database_incarnation != expected->database_incarnation ||
		memcmp(key->storage_uuid, storage_uuid, 16) != 0 ||
		key->locator.spcOid == InvalidOid ||
		!RelFileNumberIsValid(key->locator.relNumber) ||
		(key->locator.spcOid == GLOBALTABLESPACE_OID) !=
		(key->locator.dbOid == InvalidOid))
		return CLUSTER_RELMAP_PENDING_CONFLICT;
	if (key->locator.dbOid != expected->dbid)
		return CLUSTER_RELMAP_PENDING_OK;
	if (*count == capacity || *count == CLUSTER_RELMAP_PENDING_CAPACITY)
		return CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED;
	/* Reuse the original duplicate/physical locator and bounded-set rules.
	 * This scratch MAIN placeholder is never a file-existence proof. */
	result = cluster_relmap_pending_add(seen, kind, key, true);
	if (result != CLUSTER_RELMAP_PENDING_OK)
		return result;
	descriptors[*count].kind = kind;
	descriptors[*count].identity = *key;
	(*count)++;
	return CLUSTER_RELMAP_PENDING_OK;
}

static ClusterRelmapPendingResult
pending_side_plan(const ClusterRelmapV2Namespace *expected,
				  const uint8 storage_uuid[16], const RfSideOnlinePlanV1 *plan,
				  ClusterRelmapPendingSet *seen,
				  ClusterRelmapPendingDescriptor *descriptors, size_t *count,
				  size_t capacity)
{
	uint32 operation_count = rf_side_online_plan_operation_count_v1(plan);

	if (operation_count == UINT32_MAX)
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	for (uint32 i = 0; i < operation_count; i++)
	{
		RfSideOnlineOperationV1 operation;
		ClusterRelmapPendingKind kind = CLUSTER_RELMAP_PENDING_INVALID;
		uint32 locator_count;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation) ||
			operation.purpose != RF_SIDE_PLAN_RECOVERY)
			return CLUSTER_RELMAP_PENDING_NOT_READY;
		locator_count = rf_side_online_plan_space_contribution_count_v1(plan, i);
		if (locator_count == UINT32_MAX)
			return CLUSTER_RELMAP_PENDING_NOT_READY;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT)
		{
			/* ABORT deletions need their original native CREATE pairing;
			 * PREPARED has its own retained structural obligations. */
			if (operation.xact.kind != RF_SIDE_XACT_COMMIT &&
				operation.xact.kind != RF_SIDE_XACT_ABORT)
				return CLUSTER_RELMAP_PENDING_NOT_READY;
			if (operation.xact.relation_count != locator_count ||
				(operation.xact.kind != RF_SIDE_XACT_COMMIT && locator_count != 0))
				return CLUSTER_RELMAP_PENDING_NOT_READY;
			kind = CLUSTER_RELMAP_PENDING_DROP;
		}
		else if (operation.kind == RF_SIDE_ONLINE_OPERATION_SPACE)
		{
			if (operation.route.normalized_info == XLOG_SMGR_SPACE_IDENTITY)
			{
				ClusterSpaceStructureChange change;

				if (!cluster_space_structure_wal_decode(operation.owned_payload,
						operation.owned_payload_length, &change))
					return CLUSTER_RELMAP_PENDING_NOT_READY;
				if (change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE)
					kind = CLUSTER_RELMAP_PENDING_TRUNCATE;
				else if (change.identity.action != CLUSTER_SPACE_WAL_CREATE)
					return CLUSTER_RELMAP_PENDING_NOT_READY;
			}
			else if (operation.route.normalized_info != XLOG_SMGR_SPACE_RESERVATION)
				return CLUSTER_RELMAP_PENDING_NOT_READY;
		}
		else if (operation.kind <= RF_SIDE_ONLINE_OPERATION_INVALID ||
				 operation.kind > RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT ||
				 locator_count != 0)
			return CLUSTER_RELMAP_PENDING_NOT_READY;

		/* These accessors enumerate retained history and every locator,
		 * unlike the SPACE replay-target subset. No history_only filter. */
		for (uint32 j = 0; j < locator_count; j++)
		{
			RfSideSpaceContributionV1 contribution;
			ClusterRelmapPendingResult result;

			if (!rf_side_online_plan_space_contribution_v1(plan, i, j, &contribution))
				return CLUSTER_RELMAP_PENDING_NOT_READY;
			if (kind == CLUSTER_RELMAP_PENDING_INVALID)
				continue;
			if (contribution.page_mask != 3 ||
				(kind == CLUSTER_RELMAP_PENDING_DROP &&
				 contribution.result.state != CLUSTER_SPACE_IDENTITY_TOMBSTONED))
				return CLUSTER_RELMAP_PENDING_NOT_READY;
			result = pending_side_append(expected, storage_uuid, seen, descriptors,
				count, capacity, kind, &contribution.result.key);
			if (result != CLUSTER_RELMAP_PENDING_OK)
				return result;
		}
	}
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_pending_collect_side(
	const ClusterRelmapV2Namespace *expected, const uint8 storage_uuid[16],
	const ClusterRelmapPendingSideSource *sources, size_t source_count,
	ClusterRelmapPendingDescriptor *out, size_t capacity, size_t *count_out)
{
	ClusterRelmapPendingDescriptor descriptors[CLUSTER_RELMAP_PENDING_CAPACITY];
	ClusterRelmapPendingSet seen;
	size_t count = 0;
	bool uuid_nonzero = false;

	if (expected == NULL || expected->system_identifier == 0 ||
		expected->database_incarnation == 0 ||
		expected->shared != (expected->dbid == InvalidOid) ||
		storage_uuid == NULL || count_out == NULL || (capacity != 0 && out == NULL) ||
		(source_count != 0 && sources == NULL))
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	for (int i = 0; i < 16; i++)
		uuid_nonzero |= storage_uuid[i] != 0;
	if (!uuid_nonzero)
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	if (source_count == 0)
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	if (source_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED;

	/* Validate the whole supplied participant vector before interpreting any
	 * operation. Counts of graph components are not original source proof. */
	for (size_t i = 0; i < source_count; i++)
	{
		const ClusterRelmapPendingSideSource *input = &sources[i];
		const ClusterControlRootIdentity *identity = &input->source.claim.identity;
		uint32 participants = rf_side_online_plan_participant_count_v1(input->plan);
		size_t supplied = 0;

		if (participants == UINT32_MAX || participants == 0 ||
			input->source.claim.database_incarnation != expected->database_incarnation ||
			identity->system_identifier != expected->system_identifier ||
			memcmp(identity->storage_uuid, storage_uuid, 16) != 0 ||
			input->cut.failed_thread != identity->origin_thread_id ||
			input->cut.origin_owner_incarnation != identity->origin_owner_incarnation ||
			input->cut.timeline_id != input->source.timeline ||
			!rf_side_online_plan_recovery_source_matches_v1(input->plan,
				expected->system_identifier, storage_uuid, &input->cut) ||
			!rf_side_online_plan_recovery_claim_matches_v1(input->plan, &input->source))
			return CLUSTER_RELMAP_PENDING_NOT_READY;
		for (size_t j = 0; j < i; j++)
			if (sources[j].cut.failed_thread == input->cut.failed_thread &&
				sources[j].cut.origin_owner_incarnation == input->cut.origin_owner_incarnation)
				return CLUSTER_RELMAP_PENDING_CONFLICT;
		for (size_t j = 0; j < source_count; j++)
			if (sources[j].plan == input->plan)
				supplied++;
		if (supplied != participants)
			return CLUSTER_RELMAP_PENDING_NOT_READY;
	}

	cluster_relmap_pending_init(&seen);
	memset(descriptors, 0, sizeof(descriptors));
	for (size_t i = 0; i < source_count; i++)
	{
		ClusterRelmapPendingResult result;
		bool already_seen = false;

		for (size_t j = 0; j < i; j++)
			already_seen |= sources[j].plan == sources[i].plan;
		if (already_seen)
			continue;
		result = pending_side_plan(expected, storage_uuid, sources[i].plan, &seen,
			descriptors, &count, capacity);
		if (result != CLUSTER_RELMAP_PENDING_OK)
			return result;
	}
	if (count != 0)
		memcpy(out, descriptors, count * sizeof(*out));
	*count_out = count;
	return CLUSTER_RELMAP_PENDING_OK;
}

#endif /* USE_PGRAC_CLUSTER */
