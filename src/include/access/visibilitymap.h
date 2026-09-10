/*-------------------------------------------------------------------------
 *
 * visibilitymap.h
 *		visibility map interface
 *
 *
 * Portions Copyright (c) 2007-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
#include "utils/relcache.h"

/* Macros for visibilitymap test */
#define VM_ALL_VISIBLE(r, b, v) \
	((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_VISIBLE) != 0)
#define VM_ALL_FROZEN(r, b, v) \
	((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_FROZEN) != 0)

extern bool visibilitymap_clear(Relation rel, BlockNumber heapBlk,
								Buffer vmbuf, uint8 flags);
/* PGRAC: in-crit variant, map page content lock held by the caller. */
extern bool visibilitymap_clear_locked(Relation rel, BlockNumber heapBlk,
									   Buffer vmbuf, uint8 flags);
extern void visibilitymap_pin(Relation rel, BlockNumber heapBlk,
							  Buffer *vmbuf);
extern bool visibilitymap_pin_recent(Relation rel, BlockNumber heapBlk,
								 Buffer recent_buffer, Buffer *vmbuf);
extern bool visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf);
extern void visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
							  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
							  uint8 flags);
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
										  struct ResourceXAuxiliaryAcquireContext *context);
extern uint8 visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
extern void visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen);
extern BlockNumber visibilitymap_prepare_truncate(Relation rel,
												  BlockNumber nheapblocks);

#endif							/* VISIBILITYMAP_H */
