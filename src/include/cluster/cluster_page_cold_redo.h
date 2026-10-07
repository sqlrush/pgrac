/* PGRAC: startup-only consumption of exact cold block decisions.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_COLD_REDO_H
#define CLUSTER_PAGE_COLD_REDO_H

#include "access/xlogutils.h"
#include "cluster/cluster_cold_recovery.h"

/* The original cold driver brackets native redo inside its published step:
 * step_enter -> begin -> native apply -> end -> step_leave. begin/end run
 * outside critical sections; end runs after native pins/content locks have
 * been released. On any error, abort clears only this local context. This
 * is not recovery authority or a ROOT/retirement completion interface. */
extern void cluster_page_cold_redo_begin_v1(XLogReaderState *record);
extern void cluster_page_cold_redo_end_v1(XLogReaderState *record);
extern void cluster_page_cold_redo_abort_v1(void);
extern bool cluster_page_cold_redo_active_v1;
extern bool cluster_page_cold_redo_read_v1(XLogReaderState *record, uint8 block_id,
										   ReadBufferMode mode, bool cleanup, Buffer *buffer,
										   XLogRedoAction *action);
/* Same-record VM image completion only; not a durable DATA receipt. */
extern bool cluster_page_cold_redo_vm_image_applied_v1(XLogReaderState *record, uint8 block_id);
/* Called with the original pin and content-X. Never raises; violations are
 * sticky and are reported by end outside the native critical section. */
extern void cluster_page_cold_redo_dirty_v1(Buffer buffer);

#endif
