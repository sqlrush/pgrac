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
 *	  post-read) belongs to the SPACE owner.  Until it provides its cold
 *	  entry this build refuses every install; the readiness gate refuses a
 *	  plan with SPACE inputs before any modification.
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
