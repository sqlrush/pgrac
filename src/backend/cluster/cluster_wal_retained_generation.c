/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_generation.c
 *	  Which older WAL generations the retention census proves deletable
 *	  (S07, R-A20).
 *
 *	  An older generation of a thread (a CLOSED or RECOVERY_COMPLETE record
 *	  kept in the ROOT history) may have its WAL deleted only when no
 *	  obligation needs it and no running instance can still refer to it.
 *	  Nothing tracks PI responsibility per generation, so the proof uses the
 *	  census input only: the generation is CLOSED, its census bound is its
 *	  completion, and every running instance of another thread restarted
 *	  after it closed, so that no buffer page-WAL binding, local PI or
 *	  master PI holder bit -- all in shared memory -- can name it.  This file
 *	  judges; it deletes nothing.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_retained_generation.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retained_cut.h"
#include "cluster_wal_retained_cut_internal.h"

/* The latest older generation of item's thread, or NULL. */
static const ClusterWalInputV1 *
retained_predecessor(ClusterWalInputsV1 *inputs, const ClusterWalInputV1 *item)
{
	const ClusterControlRootIdentity *current = &item->source.claim.identity;
	const ClusterWalInputV1 *best = NULL;
	uint32 count = cluster_wal_inputs_count_v1(inputs);

	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *other = cluster_wal_inputs_at_v1(inputs, i);
		const ClusterControlRootIdentity *id;

		if (other == NULL || other == item || other->current)
			continue;
		id = &other->source.claim.identity;
		if (id->origin_thread_id == current->origin_thread_id
			&& id->origin_owner_incarnation < current->origin_owner_incarnation
			&& (best == NULL
				|| id->origin_owner_incarnation
					   > best->source.claim.identity.origin_owner_incarnation))
			best = other;
	}
	return best;
}

/* Has every running instance restarted since the old generation closed?
 * An OPEN current generation began after its latest predecessor's final
 * ROOT publication; that publication is at or after the old generation's
 * (one publication may close several records, and the old generation's own
 * thread has it or a later one as predecessor).  A stopped or crashed
 * instance holds no memory.  A predecessor without a publication of its own
 * (an initializer terminal) proves nothing. */
static bool
retained_restarted_since(ClusterWalInputsV1 *inputs, const ClusterWalInputV1 *old)
{
	uint32 count = cluster_wal_inputs_count_v1(inputs);

	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		const ClusterWalInputV1 *predecessor;

		if (item == NULL || !item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| item->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
			continue;
		predecessor = retained_predecessor(inputs, item);
		if (predecessor == NULL || predecessor->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| predecessor->checkpoint.root_publish_seq < old->checkpoint.root_publish_seq)
			return false;
	}
	return true;
}

/*
 * R-A20: older generations whose WAL could be deleted, judged on this census
 * input alone.  A generation qualifies only if it is CLOSED (a recovered one
 * can still owe recovered-PI acknowledgements on survivors), its census
 * bound is its completion (no edge, SIDE key or structure change in it is
 * needed by any obligation), and every running instance restarted since it
 * closed (no buffer sidecar, local PI or master holder bit can still
 * reference it).
 * Anything unproven keeps the generation.
 */
void
retained_prunable(RetainedCutWork *work, ClusterWalInputsV1 *inputs, ClusterWalRetainedCutV1 *out)
{
	for (uint32 i = 0; i < work->nsources; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		const RetainedSource *source = &work->sources[i];

		if (item == NULL || item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| item->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			|| source->bound != source->completion || !retained_restarted_since(inputs, item))
			continue;
		if (out->prunable_generations < CLUSTER_WAL_RETAINED_PRUNABLE_MAX)
			out->prunable[out->prunable_generations] = item->checkpoint.identity;
		out->prunable_generations++;
	}
}

#endif /* USE_PGRAC_CLUSTER */
