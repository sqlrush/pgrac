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
#include "storage/buf.h"

/* Caller is the original relation-create owner, after registering native
 * abort-time deletion (or under CREATE DATABASE's whole-directory cleanup).
 * False MUST abort that operation. No legacy/local
 * relation acquires a SPACE identity, and no existing identity is replaced. */
extern bool cluster_space_relation_create(RelFileLocator locator);

/* Original relation owner holds its lifecycle lock. Reads exact LIVE state
 * through native buffers; never creates missing storage. Output unchanged
 * on refusal. Caller must not hold a page content lock across this read. */
extern bool cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out);

/* New empty-fork copy owner only. A private source page becomes a new
 * ABSENT->PRESENT version under the destination identity, logged as one
 * FPI+version record. FSM has its explicit rebuildable class. The caller
 * flushes returned WAL before DATA and retains its final native fsync. */
extern bool cluster_space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum,
										BlockNumber block, void *page, XLogRecPtr *lsn);

/* Original new-fork buffer-copy owner only: source content is share-locked,
 * destination is pinned/exclusively content-locked and must be entirely zero
 * after native extension. Captures UNFORMATTED before copying; emits the same
 * FPI+version record and publishes dirty bytes atomically. Native buffer
 * writeback owns WAL-before-DATA. Refusal leaves destination unchanged. */
extern bool cluster_space_copy_buffer_wal(const ClusterSpaceIdentity *identity, const void *source,
										  Buffer destination);

/* Original recovery executor only, with its existing isolation and selected
 * WAL inputs. Decode/identity success is not recovery admission. False MUST
 * reject replay; an unsupported structural action is never silently skipped. */
extern bool cluster_space_relation_redo(XLogReaderState *record);

#endif /* CLUSTER_SPACE_STORAGE_H */
