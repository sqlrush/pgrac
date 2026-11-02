/*-------------------------------------------------------------------------
 *
 * cluster_vm_qualified_format.h
 *     Catalog identity for qualified shared visibility-map publication.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_vm_qualified_format.h
 *
 * NOTES
 *     Registration only. No VM page, WAL or wire layout is changed here.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VM_QUALIFIED_FORMAT_H
#define CLUSTER_VM_QUALIFIED_FORMAT_H

#include "c.h"
#include "catalog/catversion.h"

#define CLUSTER_VM_QUALIFIED_FORMAT_ID_V1 UINT32_C(0x564D5131)
#define CLUSTER_VM_QUALIFIED_FORMAT_VERSION_V1 UINT32_C(1)
#define CLUSTER_VM_QUALIFIED_MIN_CATALOG_VERSION PGRAC_VM_QUALIFIED_CATALOG_VERSION

/* The floor is the independently loaded control-file catalog identity, not
 * a page field, GUC, capability, or a claim supplied by a peer. The original
 * control-file compatibility check remains responsible for the binary's exact
 * catalog match. This family adds no serialized field or separate decoder.
 *
 * Assigning this identity does not qualify any existing VM bit. The catalog
 * may reach the reserved floor only in the same integration batch as the
 * qualified publication guards. Readers still require their original coherent
 * VM S scope, SPACE identity, snapshot admission and freshness checks; writers
 * retain every per-entry publication, WAL, failure and retirement requirement.
 */

#endif /* CLUSTER_VM_QUALIFIED_FORMAT_H */
