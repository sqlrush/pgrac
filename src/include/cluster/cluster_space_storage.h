/*-------------------------------------------------------------------------
 *
 * cluster_space_storage.h
 *    Native relation/WAL owners of persistent SPACE identities.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_space_storage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_STORAGE_H
#define CLUSTER_SPACE_STORAGE_H

#include "access/xlogreader.h"
#include "cluster/cluster_space_identity.h"

/* Caller is the original relation-create owner, after registering native
 * abort-time deletion (or under CREATE DATABASE's whole-directory cleanup).
 * False MUST abort that operation. No legacy/local
 * relation acquires a SPACE identity, and no existing identity is replaced. */
extern bool cluster_space_relation_create(RelFileLocator locator);

/* Original recovery executor only, with its existing isolation and selected
 * WAL inputs. Decode/identity success is not recovery admission. False MUST
 * reject replay; an unsupported structural action is never silently skipped. */
extern bool cluster_space_relation_redo(XLogReaderState *record);

#endif /* CLUSTER_SPACE_STORAGE_H */
