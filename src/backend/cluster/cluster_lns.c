/*-------------------------------------------------------------------------
 *
 * cluster_lns.c
 *	  ADG Log Network Server (LNS) helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lns.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_adg.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lns.h"
#include "libpq/pqformat.h"

ClusterDgAckMode
cluster_lns_ack_mode_from_guc(int dg_mode)
{
	switch (dg_mode) {
	case CLUSTER_DG_MODE_SYNC:
		return CLUSTER_DG_ACK_SYNC_ONE;
	case CLUSTER_DG_MODE_MAX_AVAILABILITY:
		return CLUSTER_DG_ACK_MAX_AVAILABILITY;
	case CLUSTER_DG_MODE_ASYNC:
	default:
		return CLUSTER_DG_ACK_ASYNC;
	}
}

const char *
cluster_lns_ack_mode_name(ClusterDgAckMode mode)
{
	switch (mode) {
	case CLUSTER_DG_ACK_ASYNC:
		return "async";
	case CLUSTER_DG_ACK_SYNC_ONE:
		return "sync_one";
	case CLUSTER_DG_ACK_MAX_AVAILABILITY:
		return "max_availability";
	default:
		return "unknown";
	}
}

bool
cluster_lns_commit_ack_satisfied(ClusterDgAckMode mode, bool standby_connected,
								 bool standby_acknowledged)
{
	switch (mode) {
	case CLUSTER_DG_ACK_ASYNC:
		return true;
	case CLUSTER_DG_ACK_SYNC_ONE:
		return standby_connected && standby_acknowledged;
	case CLUSTER_DG_ACK_MAX_AVAILABILITY:
		return !standby_connected || standby_acknowledged;
	default:
		return false;
	}
}

bool
cluster_lns_thread_ack_matches(uint16 target_thread_id, uint16 send_thread_id,
							   uint16 reply_thread_id)
{
	if (target_thread_id == XLP_THREAD_ID_LEGACY)
		return send_thread_id == XLP_THREAD_ID_LEGACY && reply_thread_id == XLP_THREAD_ID_LEGACY;
	if (target_thread_id < XLP_THREAD_ID_FIRST_REAL || target_thread_id > CLUSTER_WAL_THREAD_MAX)
		return false;
	return send_thread_id == target_thread_id && reply_thread_id == target_thread_id;
}

ClusterLnsTrailerStatus
cluster_lns_parse_adg_reply_trailer(StringInfo reply_message, ClusterLnsReplyState *out)
{
	int saved_cursor;
	int remaining;
	uint32 magic;
	uint16 version;
	uint16 thread_id;
	uint64 standby_consistent_scn;
	uint64 apply_master_term;

	if (reply_message == NULL || out == NULL)
		return CLUSTER_LNS_TRAILER_INVALID;

	saved_cursor = reply_message->cursor;
	remaining = reply_message->len - reply_message->cursor;
	if (remaining == 0)
		return CLUSTER_LNS_TRAILER_ABSENT;
	if (remaining != CLUSTER_ADG_REPLY_TRAILER_BYTES)
		return CLUSTER_LNS_TRAILER_INVALID;

	magic = (uint32)pq_getmsgint(reply_message, 4);
	if (magic != CLUSTER_ADG_REPLY_MAGIC) {
		reply_message->cursor = saved_cursor;
		return CLUSTER_LNS_TRAILER_INVALID;
	}

	version = (uint16)pq_getmsgint(reply_message, 2);
	thread_id = (uint16)pq_getmsgint(reply_message, 2);
	standby_consistent_scn = (uint64)pq_getmsgint64(reply_message);
	apply_master_term = (uint64)pq_getmsgint64(reply_message);

	if (!cluster_adg_reply_trailer_valid(magic, version, thread_id, (SCN)standby_consistent_scn,
										 apply_master_term))
		return CLUSTER_LNS_TRAILER_INVALID;

	out->thread_id = thread_id;
	out->reply_version = version;
	out->standby_consistent_scn = (SCN)standby_consistent_scn;
	out->apply_master_term = apply_master_term;
	return CLUSTER_LNS_TRAILER_VALID;
}

#endif /* USE_PGRAC_CLUSTER */
