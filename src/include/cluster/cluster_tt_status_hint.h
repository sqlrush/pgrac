/*-------------------------------------------------------------------------
 *
 * cluster_tt_status_hint.h
 *	  pgrac cross-node Undo TT status hint — wire ABI + emit/receive API.
 *
 *	  spec-3.2 D2 (NEW;Stage 3 第 2 sub-spec).
 *
 *	  This file ships the cross-node propagation layer for spec-3.1
 *	  Undo TT status overlay.  Each commit/abort emit one hint per
 *	  exact TT key (no raw-xid range coalescing — spec-3.2 §0.1 F6
 *	  hard guardrail).  Wire payload embeds the full ClusterTTStatusKey
 *	  (24B) so receiver never reconstructs key from raw xid — direct
 *	  install_local with sender-supplied key.
 *
 *	  spec-3.2 v1.0 FROZEN scope:
 *	    - LMON-mediated emit (L172 family);fire-and-forget no ack
 *	      (commit hot path unaffected)
 *	    - receiver validates msg_version + checksum + epoch + anti-spoof
 *	      (key.origin_node_id == env->source_node_id) + reserved-zero +
 *	      status range
 *	    - forward-compat reject:未识别 msg_version → DROP +
 *	      drop_unknown_version_count
 *	    - 6 counter exposed via pg_cluster_state 'tt_status_hint' category
 *
 *	  HC contracts in this header (HC184-HC187 4 NEW):
 *	    HC184 wire ABI 32B = 8B header + 24B embedded ClusterTTStatusKey;
 *	          no raw-xid rebuild allowed (spec-3.2 §0.1 F2/F3)
 *	    HC185 producer mask LMON only (L172 family)
 *	    HC186 receiver anti-spoof — msg.key.origin_node_id MUST equal
 *	          env->source_node_id (framework-set);防 sender 伪造 origin
 *	    HC187 forward-compat reject — V1 receiver drops V2+ msg
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tt_status_hint.h
 *
 * NOTES
 *	  pgrac-original file.  Producer mask defined here (matches
 *	  spec-2.38/2.39 sinval pattern — producer mask per subsystem
 *	  header, not in ic_envelope.h).  Frontend-safe — depends only
 *	  on cluster_tt_status.h types.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_STATUS_HINT_H
#define CLUSTER_TT_STATUS_HINT_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_tt_status.h" /* ClusterTTStatus + ClusterTTStatusKey */

/* Forward decl from cluster_ic_envelope.h (avoid heavy include in this
 * frontend-safe header). */
struct ClusterICEnvelope;

/*
 * ClusterTTStatusHintVersion -- wire format version.
 *
 * V1:  spec-3.2 v1.0 baseline.  Future V2+ may add fields after
 * embedded key;  receiver in V1 era DROPs unknown-version msgs
 * (HC187 forward-compat reject).
 */
typedef enum ClusterTTStatusHintVersion {
	CLUSTER_TT_STATUS_HINT_V1 = 1
} ClusterTTStatusHintVersion;

/*
 * ClusterTTStatusHintMsg -- 32 bytes wire-stable payload.
 *
 *	HC184: layout MUST stay byte-stable across pgrac versions.
 *
 *	Field layout:
 *	  offset  0,  2B : msg_version (uint16; V1==1)
 *	  offset  2,  2B : status (uint16; ClusterTTStatus enum value)
 *	  offset  4,  2B : flags (uint16; zero in V1)
 *	  offset  6,  2B : _reserved16 (zero on emit)
 *	  offset  8, 24B : key (ClusterTTStatusKey embed — origin_node_id +
 *	                  undo_segment_id + tt_slot_id + cluster_epoch +
 *	                  local_xid + _reserved + _reserved2)
 */
typedef struct ClusterTTStatusHintMsg {
	uint16 msg_version;		/* offset  0, 2B */
	uint16 status;			/* offset  2, 2B */
	uint16 flags;			/* offset  4, 2B */
	uint16 _reserved16;		/* offset  6, 2B */
	ClusterTTStatusKey key; /* offset  8, 24B */
} ClusterTTStatusHintMsg;

StaticAssertDecl(offsetof(ClusterTTStatusHintMsg, key) == 8,
				 "ClusterTTStatusHintMsg.key must start at offset 8 "
				 "(spec-3.2 HC184 wire ABI lock)");
StaticAssertDecl(sizeof(ClusterTTStatusHintMsg) == 32,
				 "ClusterTTStatusHintMsg must be 32 bytes wire-stable "
				 "(spec-3.2 HC184)");

/*
 * Producer mask — LMON only (L172 family;tier1-fd-LMON-exclusive-
 * ownership).  Matches spec-2.38 SINVAL_FANOUT / spec-2.39 SINVAL_ACK
 * pattern.
 */
#define CLUSTER_IC_PRODUCER_TT_STATUS_HINT ((uint32)(1u << B_LMON))

/*
 * Public API.
 *
 * cluster_tt_status_hint_emit:
 *	  enqueue a hint for cross-node propagation.  Caller is D4
 *	  xact.c commit/abort hook (spec-3.1 D5 install path) — caller
 *	  passes the EXACT key it just install_local'd (HC184:no raw-xid
 *	  rebuild).  Fire-and-forget;does not block commit hot path;
 *	  enqueue failure increments drop_invalid_count + WARNING.
 *
 * cluster_tt_status_hint_handle_envelope:
 *	  tier1 receiver dispatcher.  Validates per §3.2 (msg_version +
 *	  checksum + epoch + anti-spoof + reserved-zero + status range),
 *	  then install_local with msg.key directly.
 *
 * cluster_tt_status_hint_drain_outbound:
 *	  LMON drain entry point.  Iterates alive peers (3-gate) and
 *	  fanout each hint via tier1 send.  Only LMON calls this (HC185).
 */
extern void cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status);
extern void cluster_tt_status_hint_handle_envelope(const struct ClusterICEnvelope *env,
												   const void *payload);
extern void cluster_tt_status_hint_drain_outbound(void);
extern void cluster_tt_status_hint_register_msg_type(void);

/* Counters (v0.3 N5:  6 counters; +drop_unknown_version). */
extern uint64 cluster_tt_status_hint_get_emit_count(void);
extern uint64 cluster_tt_status_hint_get_receive_count(void);
extern uint64 cluster_tt_status_hint_get_drop_invalid_count(void);
extern uint64 cluster_tt_status_hint_get_drop_stale_epoch_count(void);
extern uint64 cluster_tt_status_hint_get_drop_unknown_version_count(void);
extern uint64 cluster_tt_status_hint_get_install_count(void);

extern Size cluster_tt_status_hint_shmem_size(void);
extern void cluster_tt_status_hint_shmem_init(void);
extern void cluster_tt_status_hint_shmem_register(void);

#endif /* CLUSTER_TT_STATUS_HINT_H */
