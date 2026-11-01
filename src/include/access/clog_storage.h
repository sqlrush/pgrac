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

#endif
