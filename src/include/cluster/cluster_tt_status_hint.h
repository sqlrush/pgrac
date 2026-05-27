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
 * V1:  spec-3.2 v1.0 baseline.  32B payload (header 8 + embedded key 24).
 *      commit_scn NOT carried -> overlay installs InvalidScn -> snapshot
 *      consumer flags UNKNOWN.
 * V2:  spec-3.3 D8.  40B payload (V1 32B + 8B SCN commit_scn @ offset 32).
 *      Real commit_scn flows end-to-end so receiver-side overlay can
 *      satisfy snapshot consumer at COMMITTED visibility (L181 chain).
 *
 * Senders post spec-3.3 emit V2 only. Receivers must accept both V1 and
 * V2 for the upgrade window; V1 install path stores InvalidScn + bumps
 * drop_v1_compat_count + emits WARNING. Unknown-version messages drop
 * (HC187 forward-compat reject).
 */
typedef enum ClusterTTStatusHintVersion {
	CLUSTER_TT_STATUS_HINT_V1 = 1,
	CLUSTER_TT_STATUS_HINT_V2 = 2,
	CLUSTER_TT_STATUS_HINT_V3 = 3, /* spec-3.5 NEW: SUBCOMMITTED + parent_key */
	CLUSTER_TT_STATUS_HINT_V4 = 4  /* spec-3.6 NEW: sidecar — multixact composition */
} ClusterTTStatusHintVersion;

/*
 * ClusterTTStatusHintMsgV1 -- 32 bytes wire-stable payload (spec-3.2).
 *
 *	HC184: layout MUST stay byte-stable across pgrac versions.
 *
 *	Field layout:
 *	  offset  0,  2B : msg_version (uint16; V1==1)
 *	  offset  2,  2B : status (uint16; ClusterTTStatus enum value)
 *	  offset  4,  2B : flags (uint16; zero in V1)
 *	  offset  6,  2B : _reserved16 (zero on emit)
 *	  offset  8, 24B : key (ClusterTTStatusKey embed)
 */
typedef struct ClusterTTStatusHintMsgV1 {
	uint16 msg_version;		/* offset  0, 2B */
	uint16 status;			/* offset  2, 2B */
	uint16 flags;			/* offset  4, 2B */
	uint16 _reserved16;		/* offset  6, 2B */
	ClusterTTStatusKey key; /* offset  8, 24B */
} ClusterTTStatusHintMsgV1;

StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV1, key) == 8,
				 "ClusterTTStatusHintMsgV1.key must start at offset 8 "
				 "(spec-3.2 HC184 wire ABI lock)");
StaticAssertDecl(sizeof(ClusterTTStatusHintMsgV1) == 32,
				 "ClusterTTStatusHintMsgV1 must be 32 bytes wire-stable "
				 "(spec-3.2 HC184)");

/*
 * ClusterTTStatusHintMsgV2 -- 40 bytes wire-stable payload (spec-3.3 D8).
 *
 *	V1 header + key preserved bit-for-bit; commit_scn appended at offset 32.
 *
 *	Field layout:
 *	  offset  0, 32B : V1 base (msg_version=V2, status, flags, _reserved16, key)
 *	  offset 32,  8B : commit_scn (SCN; InvalidScn for ABORTED / IN_PROGRESS;
 *	                   real cluster_scn_advance_for_commit() value for
 *	                   COMMITTED/CLEANED_OUT -- L181 chain)
 */
typedef struct ClusterTTStatusHintMsgV2 {
	uint16 msg_version; /* offset  0, 2B; = CLUSTER_TT_STATUS_HINT_V2 */
	uint16 status;
	uint16 flags;
	uint16 _reserved16;
	ClusterTTStatusKey key; /* offset  8, 24B */
	SCN commit_scn;			/* offset 32, 8B */
} ClusterTTStatusHintMsgV2;

StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV2, key) == 8,
				 "ClusterTTStatusHintMsgV2.key must start at offset 8");
StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV2, commit_scn) == 32,
				 "ClusterTTStatusHintMsgV2.commit_scn must start at offset 32 "
				 "(spec-3.3 D8 wire ABI lock)");
StaticAssertDecl(sizeof(ClusterTTStatusHintMsgV2) == 40,
				 "ClusterTTStatusHintMsgV2 must be 40 bytes wire-stable "
				 "(spec-3.3 D8 / V2 wire ABI lock)");

/*
 * ClusterTTStatusHintMsgV3 -- 64 bytes wire-stable payload (spec-3.5 D3).
 *
 *	V2 layout 0-39 preserved bit-for-bit;parent_key (ClusterTTStatusKey 24B)
 *	appended at offset 40.  This is the strict progressive extend convention
 *	(HC184 V1 key offset 8 + spec-3.3 D8 V2 commit_scn offset 32);see
 *	docs/spec-drafting-lessons.md L203 for the canonical statement.
 *
 *	Field layout:
 *	  offset  0, 40B : V2 base (msg_version=V3, status, flags, _reserved16,
 *	                   key, commit_scn=InvalidScn for SUBCOMMITTED)
 *	  offset 40, 24B : parent_key (ClusterTTStatusKey;zero unless
 *	                   status == SUBCOMMITTED)
 *
 *	V1/V2 receivers MUST NOT decode V3 — version dispatch first; they bump
 *	drop_unknown_version_count + DROP (HC187 forward-compat reject).  Origin
 *	in spec-3.5 has no peer-version table, so it does not downgrade V3 to V2
 *	(V2 cannot carry parent_key without silent-wrong).  The
 *	cluster_tt_hint_v3_downgrade_count counter is reserved for a future
 *	explicit mixed-version negotiation path.  Remote reader cluster exact-ref
 *	miss → 53R97 fail-closed (L199;NOT PG-native fallback).
 */
typedef struct ClusterTTStatusHintMsgV3 {
	uint16 msg_version; /* offset  0, 2B; = CLUSTER_TT_STATUS_HINT_V3 */
	uint16 status;
	uint16 flags;
	uint16 _reserved16;
	ClusterTTStatusKey key;		   /* offset  8, 24B */
	SCN commit_scn;				   /* offset 32, 8B */
	ClusterTTStatusKey parent_key; /* offset 40, 24B (NEW V3) */
} ClusterTTStatusHintMsgV3;

StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV3, msg_version) == 0,
				 "ClusterTTStatusHintMsgV3.msg_version must start at offset 0 "
				 "(L203 progressive extend convention)");
StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV3, key) == 8,
				 "ClusterTTStatusHintMsgV3.key must start at offset 8 "
				 "(HC184 + L203 progressive extend convention)");
StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV3, commit_scn) == 32,
				 "ClusterTTStatusHintMsgV3.commit_scn must start at offset 32 "
				 "(spec-3.3 D8 + L203 progressive extend convention)");
StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV3, parent_key) == 40,
				 "ClusterTTStatusHintMsgV3.parent_key must start at offset 40 "
				 "(spec-3.5 D3 V3 wire ABI lock)");
StaticAssertDecl(sizeof(ClusterTTStatusHintMsgV3) == 64,
				 "ClusterTTStatusHintMsgV3 must be 64 bytes wire-stable "
				 "(spec-3.5 D3 / V3 wire ABI lock)");

/*
 * Legacy typedef preserved for source compatibility with internal callers
 * that still reference ClusterTTStatusHintMsg without a version suffix.
 * Always means the V1 32B layout; new code should pick V1 / V2 / V3 explicitly.
 */
typedef ClusterTTStatusHintMsgV1 ClusterTTStatusHintMsg;

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
extern void cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status,
										SCN commit_scn);

/*
 * cluster_tt_status_hint_emit_subcommitted (spec-3.5 D3 NEW):
 *	  Emit a V3 SUBCOMMITTED hint with parent_key chain pointer.  Used by
 *	  spec-3.5 D7 xact.c CommitSubTransaction hook.  Caller MUST have
 *	  already installed local overlay via cluster_tt_status_install_subcommitted.
 *	  V3-only peers receive correctly;V1/V2 peers DROP (forward-compat).
 *	  Origin skips emit if no peer >= V3 (warn-only counter bump).
 */
extern void cluster_tt_status_hint_emit_subcommitted(const ClusterTTStatusKey *child_key,
													 const ClusterTTStatusKey *parent_key);
extern void cluster_tt_status_hint_handle_envelope(const struct ClusterICEnvelope *env,
												   const void *payload);
extern void cluster_tt_status_hint_drain_outbound(void);
extern void cluster_tt_status_hint_register_msg_type(void);

/* Counters (spec-3.3 D9: 7 counters; +drop_v1_compat). */
extern uint64 cluster_tt_status_hint_get_emit_count(void);
extern uint64 cluster_tt_status_hint_get_receive_count(void);
extern uint64 cluster_tt_status_hint_get_drop_invalid_count(void);
extern uint64 cluster_tt_status_hint_get_drop_stale_epoch_count(void);
extern uint64 cluster_tt_status_hint_get_drop_unknown_version_count(void);
extern uint64 cluster_tt_status_hint_get_install_count(void);
extern uint64 cluster_tt_status_hint_get_drop_v1_compat_count(void);
/* PGRAC (spec-3.5):  V3 downgrade counter (peer < V3 → skip SUBCOMMITTED emit). */
extern uint64 cluster_tt_status_hint_get_v3_downgrade_count(void);

/*
 * PGRAC spec-3.6 D3:  V4 sidecar wire variant.
 *
 *	  Sidecar pattern (L204 NEW lesson):  msg_type = PGRAC_IC_MSG_TT_STATUS_HINT
 *	  reused, msg_version = 4 sub-dispatch to ClusterTTStatusHintMsgV4Header
 *	  + variable members[].  V1/V2/V3 single-key payload byte-for-byte
 *	  unchanged;  V4 carries different data model (MultiXact composition).
 *
 *	  Strict payload length contract:
 *	      payload_length == 24 + member_count × 16
 *	  Receiver mismatch → DROP + drop_invalid_count +1.
 *	  member_count > cluster.multixact_member_overlay_max_members
 *	  → DROP + overlay_overflow_count +1.
 *
 *	  V1/V2/V3 receivers see msg_version=4 → DROP + drop_unknown_version_count
 *	  +1 (HC187 forward-compat reject).
 */
#include "cluster/cluster_multixact.h" /* ClusterMultiXactKey + ClusterMultiXactMember */

typedef struct ClusterTTStatusHintMsgV4Header
{
	uint16 msg_version;		   /* offset  0, 2B = 4 */
	uint16 payload_kind;	   /* offset  2, 2B = 1 for "multixact overlay" */
	uint16 flags;			   /* offset  4, 2B zero */
	uint16 member_count;	   /* offset  6, 2B per-message member count */
	ClusterMultiXactKey key;   /* offset  8, 16B identity */
	/* offset 24:  ClusterMultiXactMember members[member_count] (variable) */
} ClusterTTStatusHintMsgV4Header;

StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV4Header, msg_version) == 0,
				 "V4 msg_version offset 0 (L203 family)");
StaticAssertDecl(offsetof(ClusterTTStatusHintMsgV4Header, key) == 8,
				 "V4 key offset 8 (HC208 wire ABI lock)");
StaticAssertDecl(sizeof(ClusterTTStatusHintMsgV4Header) == 24,
				 "V4 header 24B wire-stable (HC208)");

/*
 * Fixed-size sidecar queue entry for LMON-mediated outbound V4.
 * V2/V3 outbound ring remains byte-for-byte unchanged;  V4 uses a
 * separate queue so variable members[] never pollutes single-key entries.
 */
#define CLUSTER_MULTIXACT_HINT_MAX_MEMBERS 256

typedef struct ClusterMultiXactHintOutboundEntry
{
	ClusterTTStatusHintMsgV4Header header; /* 24B */
	ClusterMultiXactMember members[CLUSTER_MULTIXACT_HINT_MAX_MEMBERS];
} ClusterMultiXactHintOutboundEntry;

StaticAssertDecl(sizeof(ClusterMultiXactHintOutboundEntry) == 4120,
				 "V4 outbound entry = 24 + 256 × 16 (F3/F7)");

/*
 * cluster_tt_status_hint_emit_multixact_overlay (spec-3.6 D4):
 *   Enqueue V4 sidecar emit for cross-node MultiXact member overlay
 *   broadcast.  Used by D5 multixact.c hook end of MultiXactIdCreate /
 *   Expand (local-all-member path).  Caller must have already installed
 *   local overlay via cluster_multixact_member_overlay_install.
 *   Sender member_count > GUC cap → fail-closed (no partial emit).
 */
extern void cluster_tt_status_hint_emit_multixact_overlay(const ClusterMultiXactKey *key,
														  uint16 member_count,
														  const ClusterMultiXactMember *members);

extern uint64 cluster_tt_status_hint_get_v4_drop_unknown_count(void);

extern Size cluster_tt_status_hint_shmem_size(void);
extern void cluster_tt_status_hint_shmem_init(void);
extern void cluster_tt_status_hint_shmem_register(void);

#endif /* CLUSTER_TT_STATUS_HINT_H */
