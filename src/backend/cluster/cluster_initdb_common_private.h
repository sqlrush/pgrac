/* Original cohort common objects; not ROOT/formation publication permission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_COMMON_PRIVATE_H
#define CLUSTER_INITDB_COMMON_PRIVATE_H

#include "catalog/pg_control.h"
#include "cluster/cluster_shared_config.h"

typedef struct ClusterInitdbCommon
{
	uint8 control[PG_CONTROL_FILE_SIZE];
	uint8 control_sha256[32];
	uint8 catalog[512];
	Size catalog_length;
	uint8 catalog_sha256[32];
} ClusterInitdbCommon;

/* Pure representation check. Caller has qualified actual successful native
 * child exits, fsync and physical checkpoint/dir identities for every source.
 * Require the complete original configured cohort and common-field agreement.
 * No cross-namespace MAX or serving/recovery conclusion. Failure clears a
 * distinct out; overlapping inputs/outputs are refused without modification. */
extern bool cluster_initdb_common_build(const ClusterSharedConfigRef *config,
	const ControlFileData *const sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT],
	ClusterInitdbCommon *out);

#endif
