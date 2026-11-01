/*-------------------------------------------------------------------------
 *
 * cluster_recovery_registry_reservation_format.h
 *     Format registration for identity-only registry reservations.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_recovery_registry_reservation_format.h
 *
 * NOTES
 *     This is a pgrac-original file. Registration does not encode a record,
 *     publish a reservation, or grant file coverage or access to a reader.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_H
#define CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_H

#include "c.h"
#include "catalog/catversion.h"

#define CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1 UINT32_C(0x52535631)
#define CLUSTER_RECOVERY_REGISTRY_WITH_RESERVATIONS_VERSION UINT32_C(3)
#define CLUSTER_RECOVERY_REGISTRY_RESERVATION_RECORD_VERSION_V1 UINT32_C(1)
#define CLUSTER_RECOVERY_REGISTRY_RESERVATION_MIN_CATALOG_VERSION UINT32_C(202611010)

/* The new registry container carries coverage and reservation vectors under
 * one ROOT selection. Existing file identities and the old coverage-only
 * registry codec retain their original format IDs and versions. The caller
 * supplies its original loaded control catalog, never an incoming claim. */
static inline bool
cluster_recovery_registry_reservation_format_registered_v1(uint32 format_id,
														   uint32 registry_version,
														   uint32 reservation_record_version,
														   uint32 control_catalog_version)
{
	return format_id == CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1
		   && registry_version == CLUSTER_RECOVERY_REGISTRY_WITH_RESERVATIONS_VERSION
		   && reservation_record_version == CLUSTER_RECOVERY_REGISTRY_RESERVATION_RECORD_VERSION_V1
		   && control_catalog_version == CATALOG_VERSION_NO
		   && control_catalog_version >= CLUSTER_RECOVERY_REGISTRY_RESERVATION_MIN_CATALOG_VERSION;
}

#endif /* CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_H */
