/*-------------------------------------------------------------------------
 *
 * cluster_apply_master_election.h
 *	  ADG Apply Master election runtime facade.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_apply_master_election.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_APPLY_MASTER_ELECTION_H
#define CLUSTER_APPLY_MASTER_ELECTION_H

#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

extern bool cluster_apply_master_is_self(void);
extern uint64 cluster_apply_master_current_term(void);
extern uint32 cluster_apply_master_current_node_id(void);
extern bool cluster_apply_master_term_still_valid(uint64 held_term);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_APPLY_MASTER_ELECTION_H */
