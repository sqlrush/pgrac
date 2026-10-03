/* Original new-database configuration owner, not startup admission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_CONFIG_H
#define CLUSTER_INITDB_CONFIG_H

#include "common/pgrac_initdb_wal.h"

typedef struct ClusterInitdbConfig ClusterInitdbConfig;

/* Only the original successful post-bootstrap shared-base creator calls
 * these. Prepare precedes every shared mutation; create takes that creator's
 * held new global directory. No CF bypass for runtime publication is exposed.
 * Neither call publishes ROOT/PGCB or qualifies the full creation cohort. */
extern ClusterInitdbConfig *cluster_initdb_config_prepare(const PgracInitdbWalContext *context);
extern void cluster_initdb_config_create(ClusterInitdbConfig *config, int global_fd);

#endif
