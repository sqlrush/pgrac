/* Original new-database configuration owner, not startup admission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_CONFIG_H
#define CLUSTER_INITDB_CONFIG_H

#include "common/pgrac_initdb_wal.h"
#include "cluster/cluster_shared_config.h"

typedef struct ClusterInitdbConfig ClusterInitdbConfig;

/* Read/validate an explicit original-creation request before any target
 * exists. No writes and no root/formation/serving authority. Paths 0/1/2
 * are DATA/WAL/UNDO from the same fully validated canonical request. */
extern ClusterInitdbConfig *cluster_initdb_config_preflight(const PgracInitdbConfigContext *source);
extern const ClusterSharedConfigRef *cluster_initdb_config_reference(const ClusterInitdbConfig *config);
extern const char *cluster_initdb_config_path(const ClusterInitdbConfig *config, unsigned index);
extern void cluster_initdb_config_free(ClusterInitdbConfig *config);

/* Only the original successful post-bootstrap shared-base creator calls
 * these. Prepare precedes every shared mutation; create takes that creator's
 * held new global directory. No CF bypass for runtime publication is exposed.
 * Neither call publishes ROOT/PGCB or qualifies the full creation cohort. */
extern ClusterInitdbConfig *cluster_initdb_config_prepare(const PgracInitdbWalContext *context);
extern void cluster_initdb_config_create(ClusterInitdbConfig *config, int global_fd);

#endif
