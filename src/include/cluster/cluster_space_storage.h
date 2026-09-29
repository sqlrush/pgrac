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
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_identity.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/* Caller is the original relation-create owner, after registering native
 * abort-time deletion (or under CREATE DATABASE's whole-directory cleanup).
 * False MUST abort that operation. No legacy/local
 * relation acquires a SPACE identity, and no existing identity is replaced. */
extern bool cluster_space_relation_create(RelFileLocator locator);

/* Original relation owner holds its lifecycle lock. Reads exact LIVE state
 * through native buffers; never creates missing storage. Output unchanged
 * on refusal. Caller must not hold a page content lock across this read. */
extern bool cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out);

/* Recovery executor, under its existing isolation and selected restart input.
 * Uses restart namespace, never current writer/relcache or identity creation.
 * Same output/locking contract as the runtime raw reader; not admission. */
extern bool cluster_space_relation_read_redo_identity(RelFileLocator locator,
													  ClusterSpaceIdentity *out);

/* DML owner holds the original relation lifecycle lock and has consumed native
 * relcache invalidations at its lock/statement boundary. Backend-private
 * relcache adjunct: hits use memory only, misses use the exact reader above.
 * Caller must not hold content locks even on an expected hit. A returned value
 * is a copy, never a cache or SMgr pointer. Cross-node DDL must complete its
 * existing reliable invalidation barrier before releasing lifecycle locks. */
extern bool cluster_space_relation_get_identity(Relation relation, ClusterSpaceIdentity *out);

/* Memory-only single-component capture for native owners whose WAL batch can
 * span relations. Same locked-buffer/identity contract as prepare below, but
 * no token allocation; the owner prepares one token for the complete batch. */
extern bool cluster_space_buffer_version_component(const ClusterSpaceIdentity *identity,
												   Buffer buffer, uint8 block_id, uint16 ordinal,
												   RfPageProducerComponentV1 *component);

/* Capture the original mutation owner's locked, initialized MAIN/VM pages.
 * Identity was read before acquiring content locks; this call performs no
 * storage I/O or buffer acquisition. It validates the whole batch before
 * allocating one result token and never changes page bytes. Block IDs are
 * those used by the native WAL record, in strictly increasing order. Caller
 * retains all pins/content locks through stamp, native mutation and WAL. */
extern bool cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity *identity,
												  const Buffer *buffers, const uint8 *block_ids,
												  uint8 count, RfPageProducerBatchV1 *batch);

/* New empty-fork copy owner only. A private source page becomes a new
 * ABSENT->PRESENT version under the destination identity, logged as one
 * FPI+version record. FSM has its explicit rebuildable class. The caller
 * flushes returned WAL before DATA and retains its final native fsync. */
extern bool cluster_space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum,
										BlockNumber block, void *page, XLogRecPtr *lsn);

/* Native btree bulk-build owner, under the original new-index lifecycle lock.
 * A NULL before denotes a not-yet-extended target; otherwise caller has read
 * the exact previously zero-filled target. Validate all zero bytes before
 * publishing its UNFORMATTED edge. Source/result are private construction
 * bytes; caller flushes returned WAL before DATA and retains final fsync. */
extern bool cluster_space_btree_build_page_wal(const ClusterSpaceIdentity *identity,
											   BlockNumber block, void *page,
											   const void *zero_before, XLogRecPtr *lsn);

/* Original new-fork buffer-copy owner only: source content is share-locked,
 * destination is pinned/exclusively content-locked and must be entirely zero
 * after native extension. Captures UNFORMATTED before copying; emits the same
 * FPI+version record and publishes dirty bytes atomically. Native buffer
 * writeback owns WAL-before-DATA. Refusal leaves destination unchanged. */
extern bool cluster_space_copy_buffer_wal(const ClusterSpaceIdentity *identity, const void *source,
										  Buffer destination);

/* Native heap allocation only. The identity was read before content locks;
 * the locked MAIN target must still be entirely zero. Publish the original
 * heap/ITL layout with its own UNFORMATTED edge before exposing it to callers. */
extern bool cluster_space_init_heap_buffer_wal(const ClusterSpaceIdentity *identity,
											   Buffer destination);

/* Original VM write-pin initializer, with the same exact zero-before and WAL
 * ownership contract. Publishes native VM layout, never heap/ITL layout. */
extern bool cluster_space_init_vm_buffer_wal(const ClusterSpaceIdentity *identity,
											 Buffer destination);

/* Original recovery executor only, with its existing isolation and selected
 * WAL inputs. Decode/identity success is not recovery admission. False MUST
 * reject replay; an unsupported structural action is never silently skipped. */
extern bool cluster_space_relation_redo(XLogReaderState *record);

#endif /* CLUSTER_SPACE_STORAGE_H */
