/*-------------------------------------------------------------------------
 *
 * visibilitymap.h
 *		visibility map interface
 *
 *
 * Portions Copyright (c) 2007-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * PGRAC MODIFICATIONS by SqlRush <sqlrush@gmail.com>
 *    Keep VM read-lock lifetimes separate from bitmap permissions.
 *    Spec: 2026-10-19-pre2-m1-vm-positive-paths.md
 *
 * src/include/access/visibilitymap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VISIBILITYMAP_H
#define VISIBILITYMAP_H

#include "access/visibilitymapdefs.h"
#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"

/* PGRAC: policy only; raw VM bits retain their original diagnostic meaning. */
extern bool cluster_heap_maintenance_restricted(Relation rel);

/* Macros for visibilitymap test */
#define VM_ALL_VISIBLE(r, b, v) \
	((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_VISIBLE) != 0)
#define VM_ALL_FROZEN(r, b, v) \
	((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_FROZEN) != 0)

extern bool visibilitymap_clear(Relation rel, BlockNumber heapBlk,
								Buffer vmbuf, uint8 flags);
/* PGRAC: original heap WAL consumers; never use runtime version initialization. */
struct XLogReaderState;
extern void visibilitymap_clear_redo(struct XLogReaderState *record,
									 RelFileLocator locator, BlockNumber heapBlk, uint8 flags);
/* PGRAC: in-crit variant, map page content lock held by the caller. */
extern bool visibilitymap_clear_locked(Relation rel, BlockNumber heapBlk,
									   Buffer vmbuf, uint8 flags);
extern void visibilitymap_pin(Relation rel, BlockNumber heapBlk,
							  Buffer *vmbuf);
extern bool visibilitymap_pin_recent(Relation rel, BlockNumber heapBlk,
								 Buffer recent_buffer, Buffer *vmbuf);
extern bool visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf);
#ifdef USE_PGRAC_CLUSTER
/* PGRAC: callers copy this identity before acquiring page content locks. */
struct ClusterSpaceIdentity;
/* Both exact native content-X/ACTIVE writer brackets are caller-owned.
 * No I/O, locking, page initialization or general tuple visibility proof.
 * False preserves both pages; true includes an already-qualified no-op. */
extern bool visibilitymap_set_empty(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
								   Buffer vmBuf, const struct ClusterSpaceIdentity *identity);
/* True only after exact shared VM replay; false selects native nonshared redo. */
extern bool visibilitymap_set_versioned_redo(struct XLogReaderState *record,
											RelFileLocator locator, BlockNumber heapBlk,
											uint8 flags);
#endif
extern void visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
							  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
							  uint8 flags
#ifdef USE_PGRAC_CLUSTER
							  , const struct ClusterSpaceIdentity *identity
#endif
							  );
struct ResourceXAuxiliaryAcquireContext;
/* False means not executed; caller releases/requalifies its outer heap proof.
 * InvalidBuffer means the lower handoff already released the old pin. */
extern bool visibilitymap_clear_retry_aware(Relation rel, BlockNumber heapBlk, Buffer *vmbuf,
											uint8 flags,
											struct ResourceXAuxiliaryAcquireContext *context,
											bool *cleared);
extern bool visibilitymap_set_retry_aware(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
										  XLogRecPtr recptr, Buffer *vmbuf,
										  TransactionId cutoff_xid, uint8 flags,
										  struct ResourceXAuxiliaryAcquireContext *context
#ifdef USE_PGRAC_CLUSTER
										  , const struct ClusterSpaceIdentity *identity
#endif
										  );
extern uint8 visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
/* Exact pinned-image observation only: no I/O, repin or grant acquisition.
 * Caller retains its coherent current/content bracket before relying on bits.
 * False clears status. This does not authorize visibility or freeze skips. */
extern bool visibilitymap_get_status_pinned(Relation rel, BlockNumber heapBlk, Buffer vmbuf,
										   uint8 *status);
/* Zero-initialize before first use; do not copy or reuse an active scope.
 * Owns only a shared VM read lock. The map pin remains caller-owned.
 * No snapshot admission, publication proof or visibility/freeze permission. */
typedef struct VisibilityMapReadScope
{
	Buffer locked_buffer;
} VisibilityMapReadScope;

/* Retain the relation lifecycle lock; enter without content locks or a critical
 * section. End before releasing/rebinding the map pin, I/O, waiting or another
 * block acquisition. False clears status; an active scope is left unchanged.
 * Native or missing-map success acquires no read lock. */
extern bool visibilitymap_read_begin(Relation rel, BlockNumber heapBlk, Buffer *vmbuf,
									 VisibilityMapReadScope *scope, uint8 *status);
#ifdef USE_PGRAC_CLUSTER
struct SnapshotData;
/* Same entry/exit and pin rules as read_begin. Shared observation also requires
 * the original retained snapshot throughout the scope. Admission is sampled
 * with SPACE identity before VM acquisition; local checks after acquisition
 * invoke no services and grant no publication or skip permission.
 * Failure unwinds only the new scope, preserving any scope active on entry. */
extern bool visibilitymap_read_snapshot_begin(Relation rel, BlockNumber heapBlk,
											  struct SnapshotData *snapshot, Buffer *vmbuf,
											  VisibilityMapReadScope *scope, uint8 *status);
#endif
/* Idempotent; releases only this scope's read lock, never the caller's pin. */
extern void visibilitymap_read_end(VisibilityMapReadScope *scope);
/* No content locks on entry; relation lifecycle lock retained. False clears
 * status. Success observes bits only, without granting visibility/freeze skips.
 * The returned pin remains caller-owned, with no retained content lock. */
extern bool visibilitymap_get_status_coherent(Relation rel, BlockNumber heapBlk, Buffer *vmbuf,
											  uint8 *status);
extern void visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen);
extern BlockNumber visibilitymap_prepare_truncate(Relation rel,
												  BlockNumber nheapblocks);
#ifdef USE_PGRAC_CLUSTER
/* Read-only cold structural preparation. A surviving tail must already be
 * clear; never initialize or mutate it. Output is unchanged on refusal. */
extern bool visibilitymap_prepare_cold_truncate(Relation rel, BlockNumber nheapblocks,
											  BlockNumber *newnblocks);
#endif

#endif							/* VISIBILITYMAP_H */
