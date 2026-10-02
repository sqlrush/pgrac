/* PGRAC: startup-only consumption of exact cold block decisions.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_COLD_REDO_H
#define CLUSTER_PAGE_COLD_REDO_H

#include "access/xlogutils.h"
#include "cluster/cluster_cold_recovery.h"

/* The original cold driver brackets native redo inside its published step.
 * Completion verifies actual dirty publication under the content lock. This
 * is not recovery authority or a ROOT/retirement completion interface. */
extern void cluster_page_cold_redo_begin_v1(XLogReaderState *record);
extern void cluster_page_cold_redo_end_v1(XLogReaderState *record);
extern void cluster_page_cold_redo_abort_v1(void);
extern bool cluster_page_cold_redo_active_v1;
extern bool cluster_page_cold_redo_read_v1(XLogReaderState *record, uint8 block_id,
										   ReadBufferMode mode, bool cleanup, Buffer *buffer,
										   XLogRedoAction *action);
/* Called by MarkBufferDirty with the original pin and content-X still held. */
extern void cluster_page_cold_redo_dirty_v1(Buffer buffer);

#endif
