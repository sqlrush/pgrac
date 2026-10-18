/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_space.c
 *	  Pass-2 SPACE installs of typed cold-crash replay.
 *
 *	  A relation's SPACE inputs were proven and ordered by the SPACE owner
 *	  in pass 1.  Pass 2 asks the same owner to bring the relation's SPACE
 *	  pages (and, for a CREATE or TRUNCATE, its files) through one input:
 *	  at each SPACE step, the step's own input, so pages of an incarnation
 *	  are never replayed under a later one; after every stream is drained,
 *	  all of them.  Each input is stamped with the record end of the
 *	  generation that logged it.  A TRUNCATE's shrink is repeated only at
 *	  its own step and only for the forks pass 1 found no later proof of
 *	  (shrink_forks).
 *
 *	  The install itself (physical action, stamping, write, fsync,
 *	  post-read) belongs to the SPACE owner.  The production typed cold
 *	  handshake below advertises this owner only because this file now
 *	  supplies its complete entry; the readiness gate still refuses every
 *	  plan whose owner or inputs are incomplete before any modification.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_space.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_space_recovery.h"

/*
 * This translation unit is the production R-A11 owner.  Keep the capability
 * local to the owner rather than relying on an external build switch: a
 * missing switch would silently advertise a false negative and reject every
 * valid SPACE plan.  All physical work remains behind the existing
 * cluster_space_recovery_cold_relation_install_v1() identity, source,
 * durability and read-back gates.
 */
#define CLUSTER_COLD_SPACE_OWNER_CONSUMER_V1 1

bool
cluster_cold_space_owner_v1(void)
{
#ifdef CLUSTER_COLD_SPACE_OWNER_CONSUMER_V1
	return true;
#else
	return false;
#endif
}

#ifdef CLUSTER_COLD_SPACE_OWNER_CONSUMER_V1
/* The relation's inputs in the owner's order, and who logged each. */
static bool
cold_space_inputs(const ClusterColdTypedV1 *typed, uint32 relation, uint32 count,
				  ClusterSpaceRecoveryInput *inputs, ClusterSpaceColdSourceV1 *sources)
{
	uint32 i;

	for (i = 0; i < count; i++) {
		ClusterColdSpaceInputV1 input;

		if (!cluster_cold_plan_space_input_v1(typed->plan, relation, i, &input)
			|| input.participant >= typed->participant_count)
			return false;
		inputs[i].data = input.payload;
		inputs[i].length = input.payload_length;
		sources[i].origin_thread = typed->participants[input.participant].thread_id;
		sources[i].end_rec_ptr = input.end_rec_ptr;
	}
	return true;
}
#endif

bool
cluster_cold_typed_space_abort_final_v1(const ClusterColdTypedV1 *typed, uint32 relation,
										bool *aborted)
{
	const ClusterColdParticipantV1 *participant;
	const RfSideOnlinePlanV1 *side;
	RfContributorStreamCutV1 cut = { 0 };
	ClusterColdSpaceInputV1 last;
	RelFileLocator locator;
	uint32 count, space = UINT32_MAX, abort, ordinal;

	if (typed == NULL || aborted == NULL || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| !cluster_cold_plan_space_relation_v1(typed->plan, relation, &locator, &count)
		|| count == 0 || !cluster_cold_plan_space_input_v1(typed->plan, relation, count - 1, &last))
		return false;
	if (last.kind == CLUSTER_COLD_SPACE_TRUNCATE) {
		*aborted = false;
		return true;
	}
	if ((last.kind != CLUSTER_COLD_SPACE_CREATE && last.kind != CLUSTER_COLD_SPACE_ADVANCE)
		|| last.participant >= typed->participant_count || last.payload == NULL
		|| last.payload_length == 0 || typed->side_capture_detail[last.participant] != 0
		|| (side = typed->side_plans[last.participant]) == NULL)
		return false;
	participant = &typed->participants[last.participant];
	cut.failed_thread = participant->thread_id;
	cut.timeline_id = participant->timeline;
	cut.origin_owner_incarnation = participant->owner_incarnation;
	cut.scan_begin_inclusive = participant->physical_lower;
	cut.scan_end_exclusive = participant->tail_end;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	if (last.read_rec_ptr < participant->native_redo || XLogRecPtrIsInvalid(last.read_rec_ptr)
		|| last.end_rec_ptr <= last.read_rec_ptr || last.end_rec_ptr > participant->tail_end
		|| !rf_side_online_plan_recovery_source_matches_v1(side, typed->observer.system_identifier,
														   typed->observer.storage_uuid, &cut)
		|| !rf_side_online_plan_replay_start_matches_v1(
			side, participant->thread_id, participant->owner_incarnation,
			typed->observer.database_incarnation, participant->native_redo))
		return false;
	/* Find the exact last cold input in that source's immutable capture.
	 * Payload or locator similarity alone cannot select a deleted result. */
	for (uint32 i = 0; i < rf_side_online_plan_operation_count_v1(side); i++) {
		RfSideOnlineOperationV1 op;

		if (!rf_side_online_plan_operation_v1(side, i, &op))
			return false;
		if (op.kind != RF_SIDE_ONLINE_OPERATION_SPACE
			|| op.identity.record.origin_thread != participant->thread_id
			|| op.identity.record.read_rec_ptr != last.read_rec_ptr
			|| op.identity.record.end_rec_ptr != last.end_rec_ptr
			|| op.owned_payload_length != last.payload_length || op.owned_payload == NULL
			|| memcmp(op.owned_payload, last.payload, last.payload_length) != 0)
			continue;
		if (space != UINT32_MAX || !RelFileLocatorEquals(op.space_key.locator, locator)
			|| op.space_key.system_identifier != typed->observer.system_identifier
			|| op.space_key.database_incarnation != typed->observer.database_incarnation
			|| memcmp(op.space_key.storage_uuid, typed->observer.storage_uuid, 16) != 0)
			return false;
		space = i;
	}
	if (space == UINT32_MAX || !rf_side_online_plan_space_abort_v1(side, space, &abort, &ordinal))
		return false;
	if (abort == UINT32_MAX) {
		*aborted = false;
		return true;
	}
	if (!cluster_space_recovery_cold_abort_result_v1(side, abort, ordinal))
		return false;
	*aborted = true;
	return true;
}

bool
cluster_cold_typed_space_install_v1(const ClusterColdTypedV1 *typed, uint32 relation,
									uint32 through, bool step)
{
#ifdef CLUSTER_COLD_SPACE_OWNER_CONSUMER_V1
	ClusterSpaceIdentityKey key;
	ClusterSpaceRecoveryInput *inputs;
	ClusterSpaceColdSourceV1 *sources;
	ClusterColdSpaceInputV1 at;
	uint32 count = 0;
	bool installed;

	memset(&key, 0, sizeof(key));
	if (typed == NULL
		|| !cluster_cold_plan_space_relation_v1(typed->plan, relation, &key.locator, &count)
		|| through >= count
		|| !cluster_cold_plan_space_input_v1(typed->plan, relation, through, &at))
		return false;
	key.system_identifier = typed->observer.system_identifier;
	key.database_incarnation = typed->observer.database_incarnation;
	memcpy(key.storage_uuid, typed->observer.storage_uuid, 16);
	inputs = (ClusterSpaceRecoveryInput *)palloc_extended((Size)count * sizeof(*inputs),
														  MCXT_ALLOC_HUGE);
	sources = (ClusterSpaceColdSourceV1 *)palloc_extended((Size)count * sizeof(*sources),
														  MCXT_ALLOC_HUGE);
	/* Only at its own step: once later pages are replayed, a shrink would
	 * remove them. */
	installed = cold_space_inputs(typed, relation, count, inputs, sources)
				&& cluster_space_cold_install_v1(&key, inputs, sources, count, through,
												 step ? at.shrink_forks : 0);
	pfree(sources);
	pfree(inputs);
	return installed;
#else
	(void)typed;
	(void)relation;
	(void)through;
	(void)step;
	return false;
#endif
}

#endif /* USE_PGRAC_CLUSTER */
