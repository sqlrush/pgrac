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
 *	  maps to a list of ClusterMultiXactMember(xid, status, exact TT key).
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
 *	    HC207 ClusterMultiXactMember wire-stable — sizeof == 24 bytes,
 *	          carries exact TT key fields (origin, undo_segment_id,
 *	          tt_slot_id, epoch, xid);status field uint8 maps to PG
 *	          MultiXactStatus enum 0-5
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
#include "cluster/cluster_semantic_activation.h"
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
 * 24 bytes wire-stable (HC207).  status field maps to PG MultiXactStatus
 * enum (0=ForKeyShare / 1=ForShare / 2=ForNoKeyUpdate / 3=ForUpdate /
 * 4=NoKeyUpdate / 5=Update).  origin_node_id + undo_segment_id +
 * tt_slot_id + epoch + xid are the exact TT key fields for spec-3.2
 * lookup; receivers MUST NOT reconstruct segment/slot from raw xid.
 *
 * Field layout:
 *   offset  0, 4B : xid (PG TransactionId)
 *   offset  4, 1B : status (MultiXactStatus 0-5)
 *   offset  5, 1B : _pad8 (zero on emit)
 *   offset  6, 2B : origin_node_id
 *   offset  8, 2B : undo_segment_id
 *   offset 10, 2B : _pad16 (zero on emit)
 *   offset 12, 4B : tt_slot_id
 *   offset 16, 4B : epoch
 *   offset 20, 4B : _reserved2 (zero on emit)
 */
typedef struct ClusterMultiXactMember {
	TransactionId xid;
	uint8 status;
	uint8 _pad8;
	uint16 origin_node_id;
	uint16 undo_segment_id;
	uint16 _pad16;
	uint32 tt_slot_id;
	uint32 epoch;
	uint32 _reserved2;
} ClusterMultiXactMember;

StaticAssertDecl(sizeof(ClusterMultiXactMember) == 24,
				 "ClusterMultiXactMember must be 24 bytes wire-stable (HC207)");

/*
 * R4 D7 native-composition identity.  The starting offset is the native
 * members-array generation returned by GetMultiXactIdMembersWithOffset();
 * equality is exact and ordered because member order is part of the sampled
 * native composition.  This predicate is pure and accepts only the frozen
 * all-member capacity domain.
 */
static inline bool
cluster_multixact_native_snapshot_equal(MultiXactOffset first_generation, int first_count,
										const MultiXactMember *first,
										MultiXactOffset second_generation, int second_count,
										const MultiXactMember *second)
{
	int i;

	if (first_generation == 0 || second_generation == 0 || first_generation != second_generation
		|| first_count != second_count || first_count < 2 || first_count > 256 || first == NULL
		|| second == NULL)
		return false;

	for (i = 0; i < first_count; i++) {
		if (!TransactionIdIsNormal(first[i].xid) || !TransactionIdIsNormal(second[i].xid)
			|| first[i].status < MultiXactStatusForKeyShare || first[i].status > MaxMultiXactStatus
			|| second[i].status < MultiXactStatusForKeyShare
			|| second[i].status > MaxMultiXactStatus || first[i].xid != second[i].xid
			|| first[i].status != second[i].status)
			return false;
	}
	return true;
}

/*
 * ClusterMultiXactMemberOverlayResult -- reader-side lookup output.
 *
 * Caller allocates buffer with capacity for at least `member_count`
 * ClusterMultiXactMember entries;  cluster_multixact_member_overlay_lookup
 * writes member_count + members[].  If buffer too small returns false +
 * member_count set to actual needed length.
 */
typedef struct ClusterMultiXactMemberOverlayResult {
	/* `authoritative` means exact composition metadata only; it is never
	 * transaction/terminal authority. */
	bool authoritative;
	uint16 member_count;
	uint16 _pad16;
	TimestampTz generation_ts;
	MultiXactOffset member_offset;
	XLogRecPtr source_lsn;
	XLogRecPtr source_end_lsn;
	uint16 member_wraps[256]; /* recovery-local ABA proof; wire v4 stays unchanged */
	ClusterMultiXactMember members[FLEXIBLE_ARRAY_MEMBER];
} ClusterMultiXactMemberOverlayResult;

typedef enum ClusterMultiXactSourceOp {
	CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL = 0,
	CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP = 1,
	CLUSTER_MULTI_SOURCE_RESOLVE_VISIBILITY = 2,
	CLUSTER_MULTI_SOURCE_GET_MEMBER_COUNT = 3,
	CLUSTER_MULTI_SOURCE_REMOTE_XMAX_RESOLVE = 4,
	CLUSTER_MULTI_SOURCE_NOTE_HALFSPACE_REFUSE = 5,
	CLUSTER_MULTI_SOURCE_NOTE_UNDERIVABLE_READ = 6
} ClusterMultiXactSourceOp;

typedef struct ClusterMultiXactSourceRequest {
	const ClusterMultiXactKey *key;
	uint16 member_count;
	const ClusterMultiXactMember *members;
	ClusterMultiXactMemberOverlayResult *overlay_out;
	int max_members_buf;
	const ClusterMultiXactMemberOverlayResult *overlay_in;
	Snapshot snapshot;
	uint16 origin_slot;
	MultiXactId mxid;
} ClusterMultiXactSourceRequest;

typedef struct ClusterMultiXactSourceResult {
	bool bool_value;
	uint16 member_count;
	ClusterVisibilityDecision visibility;
	bool overlay_hit;
} ClusterMultiXactSourceResult;

/* ------------------------------------------------------------ */
/* Public API                                                   */
/* ------------------------------------------------------------ */

/*
 * cluster_multixact_source_dispatch -- admit one dormant source operation.
 *
 *	The typed result is canonical-zero on refusal or generation drift.
 *	Callers may consume overlay_out only when the return value is OK and
 *	the operation's fixed success field is positive.
 */
extern ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op,
								  const ClusterMultiXactSourceRequest *request,
								  ClusterMultiXactSourceResult *result);

/*
 * Resolve the frozen D3-b snapshot-visibility contract under the semantic
 * side that is current for this generation.  An OPEN R4 installation owns
 * the request through TARGET admission; a pre-activation installation may
 * fall back to the legacy SOURCE dispatcher only after an exact
 * TARGET_DISABLED result.  The wire format and visibility truth table are
 * unchanged.
 */
extern ClusterSemanticAdmissionResult
cluster_multixact_remote_xmax_visibility_dispatch(const ClusterMultiXactSourceRequest *request,
												  ClusterMultiXactSourceResult *result);

struct ClusterSideProjectionOperationV1;

/* RF-SIDE retained-redo rebuild path.  These callback-shaped APIs bypass the
 * R4 semantic-serving gate only to mutate/verify non-authoritative projection
 * metadata; they never grant terminal state, readiness or OPEN. */
extern bool cluster_multixact_recovery_projection_apply(
	void *arg, int origin_slot, uint32 cluster_epoch,
	const struct ClusterSideProjectionOperationV1 *operation, const uint8 *owned_payload,
	uint32 owned_payload_length, XLogRecPtr source_lsn, XLogRecPtr source_end_lsn);
extern bool cluster_multixact_recovery_projection_verify(
	void *arg, int origin_slot, uint32 cluster_epoch,
	const struct ClusterSideProjectionOperationV1 *operation, const uint8 *owned_payload,
	uint32 owned_payload_length, XLogRecPtr source_lsn, XLogRecPtr source_end_lsn);

/*
 * spec-7.1 D3-b: one multixact member's origin-SERVED terminal verdict.
 *
 *   Unlike ClusterMultiXactMember (an exact TT key the local resolver looks
 *   up), this carries the terminal state the ORIGIN already resolved for a
 *   foreign multi's member -- there is no local TT to consult (that is why the
 *   overlay missed).  member_status distinguishes updater (4-5) from lock-only
 *   (0-3, ignored for visibility, A2).  verdict/commit_scn/horizon_scn/wrap
 *   mirror one ClusterGcsUndoVerdictPage per updater member; lock-only members
 *   need no verdict.
 */
typedef struct ClusterMultiXactServedMember {
	SCN commit_scn;		 /* COMMITTED_EXACT only, else InvalidScn */
	SCN horizon_scn;	 /* COMMITTED_BELOW_HORIZON bound, else InvalidScn */
	TransactionId xid;	 /* member xid (uint32; not full-xid) */
	uint16 wrap;		 /* COMMITTED_EXACT slot wrap evidence */
	uint8 verdict;		 /* ClusterGcsUndoVerdictKind; 0 = none (lock-only) */
	uint8 member_status; /* MultiXactStatus: updater(4-5) vs lock-only(0-3) */
} ClusterMultiXactServedMember;

/*
 * cluster_multixact_resolve_visibility_served (spec-7.1 D3-b, A1)
 *
 *   Pure combination resolver for a foreign multixact xmax whose members'
 *   terminal states were SERVED by the origin (no local TT lookup).  Mirrors
 *   the local overlay resolver's decision structure verbatim, but
 *   the per-updater-member terminal comes from the served verdict instead of
 *   cluster_tt_status_lookup_exact.  8.A: any updater member without a proven
 *   terminal (verdict 0 / inadmissible below-horizon / unknown) -> UNKNOWN
 *   (caller fail-closes 53R9C); lock-only members never gate visibility.
 *   Pure (no shmem / lock / I/O) so cluster_unit exercises the truth table.
 */
extern ClusterVisibilityDecision
cluster_multixact_resolve_visibility_served(const ClusterMultiXactServedMember *members,
											uint16 member_count, SCN read_scn);

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
extern uint64 cluster_multixact_get_mxid_halfspace_refuse_count(void);
extern uint64 cluster_multixact_get_mxid_underivable_read_count(void);

/*
 * Shmem hooks (defined in cluster_multixact.c when USE_PGRAC_CLUSTER;
 * disable-cluster stubs return 0 / no-op).
 */
extern Size cluster_multixact_shmem_size(void);
extern void cluster_multixact_shmem_init(void);
extern void cluster_multixact_shmem_register(void);

#endif /* CLUSTER_MULTIXACT_H */
