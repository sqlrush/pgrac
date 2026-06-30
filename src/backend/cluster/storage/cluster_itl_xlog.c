/*-------------------------------------------------------------------------
 *
 * cluster_itl_xlog.c
 *	  pgrac ITL-finish WAL resource manager redo (RM_CLUSTER_ITL).
 *
 *	  spec-3.26: the ITL xact-finish path (cluster_itl_touch.c) emits one
 *	  XLOG_CLUSTER_ITL_FINISH record per batch, registering each touched heap
 *	  buffer with its own block-local xl_heap_itl_delta_block (v1 deltas
 *	  stamping ACTIVE -> COMMITTED/ABORTED).  This replaces the prior GenericXLog
 *	  whole-page byte-diff, removing the computeRegionDelta CPU on commit.
 *
 *	  redo replays each registered block through the shared slot-stamp helper
 *	  cluster_itl_redo_apply_block_local_delta() (the SAME helper the inline DML
 *	  ITL-delta redo and online block recovery use -- L428, no drift).  htup is
 *	  NULL: a finish record carries no tuple.  FPI blocks are restored wholesale
 *	  by XLogReadBufferForRedo (the image was captured AFTER the stamp, so it
 *	  already holds the COMMITTED slot state).
 *
 *	  desc/identify live in src/backend/access/rmgrdesc/clusteritldesc.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_itl_xlog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-3.26-single-node-write-tax-cpu-closure.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "cluster/cluster_itl.h"
#include "cluster/storage/cluster_itl_xlog.h"

/*
 * cluster_itl_redo_finish -- replay one XLOG_CLUSTER_ITL_FINISH record.
 *
 *	Each registered block carries one xl_heap_itl_delta_block as its block data.
 *	A delta count != the registered blocks, or a missing delta, is a corrupt
 *	record -> PANIC (8.A; never apply partial state).  The shared helper itself
 *	fails closed on a COMMITTED delta with InvalidScn (spec-3.4a D9).
 */
static void
cluster_itl_redo_finish(XLogReaderState *record)
{
	uint8 block_id;

	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		Buffer buffer;
		XLogRedoAction action;

		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		action = XLogReadBufferForRedo(record, block_id, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			Page page = BufferGetPage(buffer);
			Size datalen = 0;
			char *blkdata = XLogRecGetBlockData(record, block_id, &datalen);

			if (blkdata == NULL || datalen == 0)
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("XLOG_CLUSTER_ITL_FINISH redo: block %u carries no ITL delta",
								(unsigned) block_id)));

			/*
			 * Apply the v1 ITL-finish delta via the shared helper.  htup = NULL:
			 * a finish record stamps slot state only and patches no tuple.  The
			 * helper preserves on-page UBA for v1 deltas and skips the ACTIVE-only
			 * recycle-watermark advance, so the replayed special area is
			 * byte-identical to the primary's itl_finish_stamp_page() result
			 * (spec-3.26 byte-equivalence; verified by unit + crash-redo TAP).
			 */
			(void) cluster_itl_redo_apply_block_local_delta(page, NULL, blkdata);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * cluster_itl_redo -- RM_CLUSTER_ITL rm_redo entry point.
 */
void
cluster_itl_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_CLUSTER_ITL_FINISH:
		cluster_itl_redo_finish(record);
		break;
	default:
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_itl_redo: unknown op %u", (unsigned) info)));
	}
}

#endif /* USE_PGRAC_CLUSTER */
