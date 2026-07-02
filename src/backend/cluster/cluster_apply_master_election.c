/*-------------------------------------------------------------------------
 *
 * cluster_apply_master_election.c
 *	  ADG Apply Master election runtime facade.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_apply_master_election.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_apply_master_election.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"

bool
cluster_apply_master_is_self(void)
{
	uint32 owner = cluster_mrp_apply_master_node_id();

	return owner != UINT32_MAX && (int32)owner == cluster_node_id;
}

uint64
cluster_apply_master_current_term(void)
{
	return cluster_mrp_apply_master_term();
}

uint32
cluster_apply_master_current_node_id(void)
{
	return cluster_mrp_apply_master_node_id();
}

bool
cluster_apply_master_term_still_valid(uint64 held_term)
{
	return cluster_mrp_apply_master_term_still_valid(held_term);
}

#endif /* USE_PGRAC_CLUSTER */
