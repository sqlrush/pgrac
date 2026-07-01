/*-------------------------------------------------------------------------
 *
 * cluster_rfs.c
 *	  ADG Remote File Server (RFS) helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_rfs.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog_internal.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_rfs.h"
#include "cluster/cluster_standby_scn.h"
#include "cluster/cluster_xlog.h"
#include "libpq/pqformat.h"

void
cluster_rfs_observe_received_chunk(const char *buf, Size nbytes, XLogRecPtr recptr,
								   uint16 *last_thread_id)
{
	XLogRecPtr chunk_end;
	XLogRecPtr page_lsn;
	uint32 page_offset;
	uint16 observed_thread_id;

	if (!cluster_mrp_should_start())
		return;
	if (buf == NULL || nbytes == 0 || XLogRecPtrIsInvalid(recptr))
		return;

	observed_thread_id = last_thread_id != NULL ? *last_thread_id : XLP_THREAD_ID_LEGACY;
	chunk_end = recptr + nbytes;
	page_offset = recptr % XLOG_BLCKSZ;
	if (page_offset == 0)
		page_lsn = recptr;
	else
		page_lsn = recptr + (XLOG_BLCKSZ - page_offset);

	while (page_lsn <= chunk_end && chunk_end - page_lsn >= sizeof(XLogPageHeaderData)) {
		Size offset = (Size)(page_lsn - recptr);
		XLogPageHeaderData hdr;

		memcpy(&hdr, buf + offset, sizeof(hdr));
		if (hdr.xlp_magic == XLOG_PAGE_MAGIC
			&& cluster_xlog_validate_page_header(hdr.xlp_thread_id, hdr.xlp_cluster_flags,
												 XLP_THREAD_ID_INVALID))
			observed_thread_id = hdr.xlp_thread_id;
		page_lsn += XLOG_BLCKSZ;
	}

	if (last_thread_id != NULL)
		*last_thread_id = observed_thread_id;
	cluster_standby_scn_mark_received(observed_thread_id, chunk_end);
}

void
cluster_rfs_append_reply_trailer(StringInfo reply_message, uint16 last_thread_id)
{
	if (reply_message == NULL || !cluster_mrp_should_start())
		return;

	pq_sendint(reply_message, CLUSTER_ADG_REPLY_MAGIC, 4);
	pq_sendint16(reply_message, CLUSTER_ADG_REPLY_VERSION);
	pq_sendint16(reply_message, last_thread_id);
	pq_sendint64(reply_message, (uint64)cluster_mrp_standby_consistent_scn());
	pq_sendint64(reply_message, cluster_mrp_apply_master_term());
}

#endif /* USE_PGRAC_CLUSTER */
