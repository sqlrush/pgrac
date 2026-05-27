/*-------------------------------------------------------------------------
 *
 * cluster_multixact.h
 *	  pgrac MULTIXACT reader/member-resolution foundation — cross-node
 *	  MultiXact composition overlay + visibility helper.
 *
 *	  spec-3.6 D1 (NEW;Stage 3 第 10 sub-spec).
 *
 *	  Reader-side cluster overlay for remote-composed MultiXact:  each
 *	  ClusterMultiXactKey = (origin_node_id, multixact_id, cluster_epoch)
 *	  maps to a list of ClusterMultiXactMember(xid, status, origin, epoch).
 *	  Reader resolves visibility by combining per-member MultiXactStatus
 *	  with per-member commit/abort/in-progress status via spec-3.2 TT
 *	  framework.  lock-side MultiXactIdCreate/Expand with remote member
 *	  remains spec-3.4d 53R99 fail-closed (推 spec-3.6b/3.7).
 *
 *	  Scope frozen reader-only per Q1 C-lite (spec-3.6 §0/§1.3).
 *
 *	  HC contracts in this header (HC206-HC209 4 NEW):
 *	    HC206 ClusterMultiXactKey wire-stable — sizeof == 16 bytes,
 *	          explicit _pad16 + _reserved padding (mirror HC183 pattern)
 *	    HC207 ClusterMultiXactMember wire-stable — sizeof == 16 bytes,
 *	          explicit _pad8 + _reserved2 padding;status field uint8
 *	          maps to PG MultiXactStatus enum 0-5
 *	    HC208 V4 sidecar wire ABI — msg_version=4 sub-dispatch at
 *	          cluster_tt_status_hint_handle_envelope;V1/V2/V3 receivers
 *	          MUST DROP V4 + drop_unknown_version_count +1
 *	    HC209 overlay HTAB capacity — `cluster.multixact_member_overlay_
 *	          max_entries` GUC default 16384;overflow → reader miss path
 *	          53R9C fail-closed (NOT silent eviction)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.6-multixact-reader-member-resolution.md (v0.3 FROZEN 2026-05-27)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_multixact.h
 *
 * NOTES
 *	  pgrac-original file (spec-3.6 D1 NEW,2026-05-27).  All types use
 *	  the ClusterMultiXact prefix.  All exported functions use the
 *	  cluster_multixact_ prefix.  Companion impl in
 *	  src/backend/cluster/cluster_multixact.c (D2).  Frontend-safe —
 *	  depends only on cluster_tt_status.h types + PG core MultiXactId.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_H
#define CLUSTER_MULTIXACT_H

#include "c.h"
#include "access/transam.h"
#include "access/multixact.h" /* MultiXactId + MultiXactStatus */
#include "cluster/cluster_tt_status.h"
#include "utils/snapshot.h"

/*
 * ClusterMultiXactKey -- exact-key identity for cluster multixact overlay.
 *
 * 16 bytes wire-stable (HC206).  multixact_id is per-node local SLRU
 * offset;  cluster identity requires (origin_node_id, multixact_id,
 * cluster_epoch) tuple — raw MultiXactId lookup forbidden.
 *
 * Field layout (must remain stable across pgrac versions in Stage 3+):
 *   offset  0, 2B : origin_node_id
 *   offset  2, 2B : _pad16 (zero on emit;explicit padding)
 *   offset  4, 4B : multixact_id (PG MultiXactId == uint32)
 *   offset  8, 4B : cluster_epoch
 *   offset 12, 4B : _reserved (zero on emit;future wrap bits)
 */
typedef struct ClusterMultiXactKey {
	uint16 origin_node_id;
	uint16 _pad16;
	MultiXactId multixact_id;
	uint32 cluster_epoch;
	uint32 _reserved;
} ClusterMultiXactKey;

StaticAssertDecl(sizeof(ClusterMultiXactKey) == 16,
				 "ClusterMultiXactKey must be 16 bytes wire-stable (HC206)");

/*
 * ClusterMultiXactMember -- per-member record.
 *
 * 16 bytes wire-stable (HC207).  status field maps to PG MultiXactStatus
 * enum (0=ForKeyShare / 1=ForShare / 2=ForNoKeyUpdate / 3=ForUpdate /
 * 4=NoKeyUpdate / 5=Update).  origin_node_id + epoch identify the
 * member xid's binding for spec-3.2 TT lookup.
 *
 * Field layout:
 *   offset  0, 4B : xid (PG TransactionId)
 *   offset  4, 1B : status (MultiXactStatus 0-5)
 *   offset  5, 1B : _pad8 (zero on emit)
 *   offset  6, 2B : origin_node_id
 *   offset  8, 4B : epoch
 *   offset 12, 4B : _reserved2 (zero on emit)
 */
typedef struct ClusterMultiXactMember {
	TransactionId xid;
	uint8 status;
	uint8 _pad8;
	uint16 origin_node_id;
	uint32 epoch;
	uint32 _reserved2;
} ClusterMultiXactMember;

StaticAssertDecl(sizeof(ClusterMultiXactMember) == 16,
				 "ClusterMultiXactMember must be 16 bytes wire-stable (HC207)");

/*
 * ClusterMultiXactMemberOverlayResult -- reader-side lookup output.
 *
 * Caller allocates buffer with capacity for at least `member_count`
 * ClusterMultiXactMember entries;  cluster_multixact_member_overlay_lookup
 * writes member_count + members[].  If buffer too small returns false +
 * member_count set to actual needed length.
 */
typedef struct ClusterMultiXactMemberOverlayResult {
	bool authoritative;
	uint16 member_count;
	uint16 _pad16;
	TimestampTz generation_ts;
	ClusterMultiXactMember members[FLEXIBLE_ARRAY_MEMBER];
} ClusterMultiXactMemberOverlayResult;

/* ------------------------------------------------------------ */
/* Public API                                                   */
/* ------------------------------------------------------------ */

/*
 * cluster_multixact_member_overlay_install (spec-3.6 D2)
 *
 *   Install or overwrite an overlay entry for `key` with `member_count`
 *   members.  Caller is D5 multixact.c hook (local-all-member emit path)
 *   or D4 V4 wire receiver.  Returns true on success, false on overlay
 *   full / member_count > GUC cap (caller increments overlay_overflow_count).
 */
extern bool cluster_multixact_member_overlay_install(const ClusterMultiXactKey *key,
													 uint16 member_count,
													 const ClusterMultiXactMember *members);

/*
 * cluster_multixact_member_overlay_lookup (spec-3.6 D2)
 *
 *   Look up overlay entry by exact key.  Writes result + copies members
 *   into caller buffer (up to max_members_buf entries).  Returns true on
 *   hit + authoritative=true;  false on miss + bumps overlay_miss_count.
 *   Caller raises 53R9C on miss (per L199 fail-closed).
 */
extern bool cluster_multixact_member_overlay_lookup(const ClusterMultiXactKey *key,
													ClusterMultiXactMemberOverlayResult *out,
													int max_members_buf);

/*
 * cluster_multixact_resolve_visibility (spec-3.6 D2 core helper)
 *
 *   Given a hit overlay result + current snapshot, compute the
 *   visibility decision combining per-member MultiXactStatus
 *   (0-3 lock-only;  4-5 Update/NoKeyUpdate) with per-member commit/
 *   abort/in-progress status (via spec-3.2 cluster_tt_status_lookup_exact).
 *
 *   Truth table (per OBS-1 amend MVCC-accurate):
 *     lock-only ANY state                              → VISIBLE
 *     Update/NoKeyUpdate ABORTED                       → VISIBLE
 *     Update/NoKeyUpdate IN_PROGRESS (authoritative)   → VISIBLE
 *     Update/NoKeyUpdate COMMITTED + scn <= read_scn   → INVISIBLE
 *     Update/NoKeyUpdate COMMITTED + scn > read_scn    → VISIBLE
 *     ANY UNKNOWN / TT miss / overlay miss             → UNKNOWN
 *
 *   UNKNOWN → caller raises 53R9C (per L199 NOT PG-native fallback).
 *   Pure / no syscall / no wait (L177 hot path).
 */
extern ClusterVisibilityDecision
cluster_multixact_resolve_visibility(const ClusterMultiXactMemberOverlayResult *overlay,
									 const Snapshot snap);

/*
 * cluster_multixact_get_member_count (spec-3.6 D2)
 *
 *   Return member_count of overlay entry for `key`, or 0 on miss.
 *   Used by D6 to size lookup buffer.
 */
extern uint16 cluster_multixact_get_member_count(const ClusterMultiXactKey *key);

/*
 * cluster_multixact_purge_epoch (spec-3.6 D2)
 *
 *   Purge all overlay entries with cluster_epoch < obsolete_epoch.
 *   Called by reconfig hook (HC182 pattern from spec-3.1).
 */
extern void cluster_multixact_purge_epoch(uint32 obsolete_epoch);

/*
 * Counter getters (always linked;return 0 in disable-cluster build).
 */
extern uint64 cluster_multixact_get_overlay_install_count(void);
extern uint64 cluster_multixact_get_overlay_lookup_hit_count(void);
extern uint64 cluster_multixact_get_overlay_miss_count(void);
extern uint64 cluster_multixact_get_overlay_overflow_count(void);
extern uint64 cluster_multixact_get_resolve_visibility_count(void);

/*
 * Shmem hooks (defined in cluster_multixact.c when USE_PGRAC_CLUSTER;
 * disable-cluster stubs return 0 / no-op).
 */
extern Size cluster_multixact_shmem_size(void);
extern void cluster_multixact_shmem_init(void);
extern void cluster_multixact_shmem_register(void);

#endif /* CLUSTER_MULTIXACT_H */
