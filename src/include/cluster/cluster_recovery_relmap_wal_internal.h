/*-------------------------------------------------------------------------
 * cluster_recovery_relmap_wal_internal.h
 *    Shared syntax/source checks for owned managed relmap WAL observations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_recovery_relmap_wal_internal.h
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_RELMAP_WAL_INTERNAL_H
#define CLUSTER_RECOVERY_RELMAP_WAL_INTERNAL_H
#ifdef USE_PGRAC_CLUSTER
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_native_scan.h"
#include "common/cluster_relmap_file.h"
#include "utils/relmapper.h"

/* The envelope is an observation, never an independently selected control
 * registration. Creation origin is deliberately independent of the WAL writer.
 * Authentication of the complete source/cut remains with the original owner. */
static inline bool
recovery_relmap_wal_observe_v2(const ClusterRecoveryNativeRecord *record,
							   ClusterRecoveryFileIdentityV2 *file, ClusterRelmapV2Image *image)
{
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image decoded;

	if (record == NULL || record->header.xl_rmid != RM_RELMAP_ID
		|| (record->header.xl_info & ~XLR_INFO_MASK) != XLOG_RELMAP_UPDATE_V2
		|| !cluster_wal_claim_v2_ref_valid(&record->source.claim)
		|| !cluster_relmap_wal_observe_intent_v2(record->payload, record->payload_length, &observed,
												 &decoded)
		|| observed.system_identifier != record->source.claim.identity.system_identifier
		|| observed.database_incarnation != record->source.claim.database_incarnation
		|| memcmp(observed.storage_uuid, record->source.claim.identity.storage_uuid, 16) != 0
		|| observed.redo_timeline != record->source.timeline
		|| decoded.pending_owner.node != record->source.claim.identity.origin_node_id
		|| (observed.tablespace_oid == GLOBALTABLESPACE_OID
				? observed.database_oid != InvalidOid
				: observed.tablespace_oid != DEFAULTTABLESPACE_OID
					  || observed.database_oid == InvalidOid))
		return false;
	*file = observed;
	*image = decoded;
	return true;
}
#endif
#endif
