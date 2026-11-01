/*-------------------------------------------------------------------------
 *
 * cluster_foreign_mx_format.h
 *     Format registration for complete foreign MultiXact operations.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_foreign_mx_format.h
 *
 * NOTES
 *     This is a pgrac-original file. Registration does not advertise a
 *     decoder, activate a format, or replace the operation's authority checks.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_FOREIGN_MX_FORMAT_H
#define CLUSTER_FOREIGN_MX_FORMAT_H

#include "c.h"
#include "catalog/catversion.h"
#include "cluster/cluster_ic.h"

#define CLUSTER_FOREIGN_MX_FORMAT_ID_V1 UINT32_C(0x4D584731)
#define CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1 UINT32_C(1)
#define CLUSTER_FOREIGN_MX_MIN_CATALOG_VERSION UINT32_C(202611010)
/* Independent GCS service selectors. Consumers must establish this family's
 * format/version and negotiated capabilities before dispatch. These values
 * neither extend the old V5 domains nor authorize use of their reserved bytes.
 * Registration alone does not enable either request or reply handling. */
#define CLUSTER_FOREIGN_MX_GCS_RANGE_REQUEST_KIND_V1 ((uint8)13)
#define CLUSTER_FOREIGN_MX_GCS_RANGE_REPLY_STATUS_V1 ((uint8)31)
#define CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES                                                   \
	(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1 | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1                \
	 | PGRAC_IC_HELLO_CAP_MULTIXACT_FULL_V1)

/* control_catalog_version must come from the original loaded control owner,
 * not the incoming message. The normal control-file check still enforces the
 * binary's exact catalog version; the family additionally has a version floor. */
static inline bool
cluster_foreign_mx_format_registered_v1(uint32 format_id, uint32 format_version,
										uint32 control_catalog_version)
{
	return format_id == CLUSTER_FOREIGN_MX_FORMAT_ID_V1
		   && format_version == CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1
		   && control_catalog_version == CATALOG_VERSION_NO
		   && control_catalog_version >= CLUSTER_FOREIGN_MX_MIN_CATALOG_VERSION;
}

#endif /* CLUSTER_FOREIGN_MX_FORMAT_H */
