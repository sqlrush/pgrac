/* Borrowed-FD CLOG observations. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CLOG_STORAGE_H
#define CLUSTER_CLOG_STORAGE_H

#include "access/clog_file.h"

/* The original owner supplies the expected registration and holds the FD,
 * page I/O serialization and file lifetime throughout this read. It must
 * recheck its control/source custody before publishing an observation.
 * This helper never opens, closes, creates, repairs, writes or syncs a file.
 * Missing/legacy/partial target frames or mismatched input are refused, even
 * during recovery. Other frames may be extending under their own I/O locks.
 * Success returns bytes and page identity, not checkpoint or replay authority.
 * Failure preserves both outputs. Input and output ranges must not overlap.
 */
extern bool cluster_clog_storage_read_page_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 uint64 logical_page,
									 ClusterClogPageIdentityV2 *page_out,
									 void *payload_out, Size payload_length);

/* Overwrite an existing, completely validated local frame. The original owner
 * holds file lifetime and page I/O serialization throughout, and supplies one
 * private control-lock snapshot of page identity, bytes and group-LSN maximum
 * (page_lsn). It rechecks original control/source custody before publication.
 *
 * Every call logs a complete frame through native WAL, flushes its actual end,
 * then writes, syncs and reads back the complete frame. This conservatively
 * protects every overwrite without a slot-based or cached redo-point shortcut.
 * WAL contains the captured frame; replay must stamp the actual record end as
 * image_lsn, as this writer does after insertion. No new birth, header creation,
 * foreign/recovery write, fsync-disabled or bootstrap phony WAL is accepted here.
 *
 * The borrowed FD, inputs, preowned checkpoint-delay flag and last_end output
 * are retained on failure. Partial writes/material are never rolled back or
 * repaired; the original owner retains its dirty/error/recovery responsibility.
 * Native WAL errors propagate after restoring only our checkpoint-delay flag.
 * This is not file registration, custody, checkpoint or replay authority.
 */
extern bool cluster_clog_storage_write_page_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 const ClusterClogPageIdentityV2 *page,
									 const void *payload, Size payload_length,
									 XLogRecPtr *last_end);

#endif
