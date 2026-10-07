/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_generation.c
 *	  Which older WAL generations the retention census no longer needs
 *	  (S07, R-A20).
 *
 *	  An older generation of a thread (a CLOSED or RECOVERY_COMPLETE record
 *	  kept in the ROOT history) is unneeded when it is CLOSED and its census
 *	  bound is its completion: no edge, SIDE key or structure change in it
 *	  serves any obligation.  That is necessary for deleting its WAL, not
 *	  sufficient: a running instance may still hold a page-WAL binding, local
 *	  PI or master holder bit naming it, and a restarted one may read its WAL
 *	  again when it rebuilds such references.  No persistent field proves the
 *	  absence of both -- ROOT publish sequences count per record for
 *	  checkpoints and normal stops, so they do not order closes across
 *	  threads -- hence the list is a readout and nothing here permits a
 *	  deletion.
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

/*
 * R-A20: older generations whose WAL no obligation of this census needs.  A
 * RECOVERY_COMPLETE generation can still owe recovered-PI acknowledgements
 * on survivors, so only a CLOSED one qualifies, and only when its census
 * bound is its completion.
 */
void
retained_unneeded(RetainedCutWork *work, ClusterWalInputsV1 *inputs, ClusterWalRetainedCutV1 *out)
{
	for (uint32 i = 0; i < work->nsources; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		const RetainedSource *source = &work->sources[i];

		if (item == NULL || item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| item->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			|| source->bound != source->completion)
			continue;
		if (out->unneeded_generations < CLUSTER_WAL_RETAINED_UNNEEDED_MAX)
			out->unneeded[out->unneeded_generations] = item->checkpoint.identity;
		out->unneeded_generations++;
	}
}

#endif /* USE_PGRAC_CLUSTER */
