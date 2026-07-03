/*-------------------------------------------------------------------------
 *
 * cluster_lns.h
 *	  ADG Log Network Server (LNS) helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lns.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LNS_H
#define CLUSTER_LNS_H

#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"
#include "lib/stringinfo.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterDgAckMode {
	CLUSTER_DG_ACK_ASYNC = 0,
	CLUSTER_DG_ACK_SYNC_ONE,
	CLUSTER_DG_ACK_MAX_AVAILABILITY
} ClusterDgAckMode;

typedef enum ClusterLnsTrailerStatus {
	CLUSTER_LNS_TRAILER_ABSENT = 0,
	CLUSTER_LNS_TRAILER_VALID,
	CLUSTER_LNS_TRAILER_INVALID
} ClusterLnsTrailerStatus;

typedef struct ClusterLnsReplyState {
	uint16 thread_id;
	uint16 reply_version;
	SCN standby_consistent_scn;
	uint64 apply_master_term;
} ClusterLnsReplyState;

extern ClusterDgAckMode cluster_lns_ack_mode_from_guc(int dg_mode);
extern const char *cluster_lns_ack_mode_name(ClusterDgAckMode mode);
extern bool cluster_lns_commit_ack_satisfied(ClusterDgAckMode mode, bool standby_connected,
											 bool standby_acknowledged);
extern bool cluster_lns_thread_ack_matches(uint16 target_thread_id, uint16 send_thread_id,
										   uint16 reply_thread_id);
extern ClusterLnsTrailerStatus cluster_lns_parse_adg_reply_trailer(StringInfo reply_message,
																   ClusterLnsReplyState *out);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_LNS_H */
