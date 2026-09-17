/*-------------------------------------------------------------------------
 *
 * cluster_side_projection.h
 *	  RF-SIDE D-SIDE-04 — derived-projection producer judgements for
 *	  CLOG / MULTIXACT / COMMIT_TS.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-04 ("CLOG/MULTIXACT/COMMIT_TS projection producer、
 *	  invalidate、rebuild/fail-closed、verification；local store 不作
 *	  authority"), §2.4 derived projection contracts, §5.1
 *	  U-SIDE-08/09/10.
 *
 *	  CONTRACT (spec §2.4 common rules):
 *	    - projection 是可丢失、可失效的 materialized view，不是
 *	      transaction authority;
 *	    - projection 写入可在 recovery path 持久化，但 "durable" 不等于
 *	      "authoritative"：每次 serve 前仍核对 canonical producer
 *	      identity/version/coverage;
 *	    - 若 rebuild source 不能证明在 failed-origin redo 退休后仍
 *	      存在，FND-10 不成立，RF-SIDE 必须拒绝 retire;
 *	    - projection reset 不建立全实例 barrier;
 *	    - projection normal lookup 不得新增同步 network 或 durable I/O；
 *	      不一致时关 scope 并在重建前返回 fail-closed error。
 *
 *	  DELIVERED HERE: the judgement layer — verified() (canonical
 *	  producer + coverage + integrity), rebuildable() (per-kind source
 *	  retention rule), and lookup() (miss/UNKNOWN fails closed — never
 *	  read the local ProcArray/CLOG to guess).  The actual projection
 *	  storage, invalidation triggers and rebuild execution remain the
 *	  production cluster_remote_xact wiring (RED).  No ABI change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_projection.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_PROJECTION_H
#define CLUSTER_SIDE_PROJECTION_H

#include "access/multixact.h"
#include "access/transam.h"
#include "access/xlogdefs.h"

typedef enum ClusterSideProjectionKind {
	CLUSTER_SIDE_PROJECTION_CLOG = 0,
	CLUSTER_SIDE_PROJECTION_MULTIXACT,
	CLUSTER_SIDE_PROJECTION_COMMIT_TS
} ClusterSideProjectionKind;

typedef enum ClusterSideProjectionActionV1 {
	CLUSTER_SIDE_PROJECTION_ACTION_INVALID = 0,
	CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE,
	CLUSTER_SIDE_PROJECTION_ACTION_CREATE,
	CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE
} ClusterSideProjectionActionV1;

/*
 * Immutable, pointer-free description of one retained projection record.
 * MULTIXACT CREATE members remain caller-owned bytes in the enclosing SIDE
 * plan; member_count/member_offset describe their exact native identity.
 */
typedef struct ClusterSideProjectionOperationV1 {
	ClusterSideProjectionKind kind;
	ClusterSideProjectionActionV1 action;
	uint8 normalized_info;
	uint8 reserved9[3];
	int32 page_number;
	Oid oldest_database;
	TransactionId oldest_xid;
	MultiXactId multixact_id;
	MultiXactOffset member_offset;
	uint32 member_count;
	MultiXactId truncate_start_multixact;
	MultiXactId truncate_end_multixact;
	MultiXactOffset truncate_start_member;
	MultiXactOffset truncate_end_member;
} ClusterSideProjectionOperationV1;

typedef enum ClusterSideProjectionApplyResultV1 {
	CLUSTER_SIDE_PROJECTION_APPLY_OK = 0,
	CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED,
	CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED
} ClusterSideProjectionApplyResultV1;

typedef struct ClusterSideProjectionApplyInputV1 {
	const ClusterSideProjectionOperationV1 *operation;
	const uint8 *owned_payload;
	uint32 owned_payload_length;
	uint16 origin_thread;
	bool source_retained;
	uint8 reserved19;
	uint32 cluster_epoch;
	XLogRecPtr source_lsn;
	XLogRecPtr source_end_lsn;
} ClusterSideProjectionApplyInputV1;

typedef bool (*ClusterSideProjectionResetRangeV1)(void *arg, int origin_slot,
												  TransactionId first_xid, uint32 xid_count);
typedef bool (*ClusterSideProjectionRangeEmptyV1)(void *arg, int origin_slot,
												  TransactionId first_xid, uint32 xid_count);
typedef bool (*ClusterSideProjectionTruncateBeforeV1)(void *arg, int origin_slot,
													  TransactionId oldest_xid);
typedef bool (*ClusterSideProjectionMultiOperationV1)(
	void *arg, int origin_slot, uint32 cluster_epoch,
	const ClusterSideProjectionOperationV1 *operation, const uint8 *owned_payload,
	uint32 owned_payload_length, XLogRecPtr source_lsn, XLogRecPtr source_end_lsn);

typedef struct ClusterSideProjectionApplyOpsV1 {
	void *arg;
	ClusterSideProjectionResetRangeV1 reset_remote_xact_range;
	ClusterSideProjectionRangeEmptyV1 remote_xact_range_empty;
	ClusterSideProjectionTruncateBeforeV1 truncate_remote_xact_before;
	ClusterSideProjectionMultiOperationV1 apply_multixact_projection;
	ClusterSideProjectionMultiOperationV1 verify_multixact_projection;
} ClusterSideProjectionApplyOpsV1;

extern ClusterSideProjectionApplyResultV1
cluster_side_projection_target_preflight_v1(const ClusterSideProjectionApplyInputV1 *input);
extern ClusterSideProjectionApplyResultV1
cluster_side_projection_apply_owned_v1(const ClusterSideProjectionApplyInputV1 *input,
									   const ClusterSideProjectionApplyOpsV1 *ops);

/*
 * §2.4 verifier facts for one projection scope.
 */
typedef struct ClusterSideProjectionVerifyInput {
	bool canonical_truth_ok; /* canonical producer (TT/undo + matching
									 * terminal redo) bidirectional match */
	bool coverage_ok;		 /* xid/origin/wrap/terminal coverage exact */
	bool integrity_ok;		 /* checksum/coverage mismatch detector passed */
} ClusterSideProjectionVerifyInput;

/*
 * §2.4 verification: the projection may SERVE only when every fact
 * holds.  A miss or UNKNOWN state fails closed — the caller must never
 * read the local ProcArray/CLOG to guess a result (U-SIDE-08: a local
 * bit cannot override the canonical TT/redo truth).
 */
extern bool cluster_side_projection_verified(ClusterSideProjectionKind kind,
											 const ClusterSideProjectionVerifyInput *in);

/*
 * §2.4 rebuild rule + FND-10 source-retention boundary:
 *   - CLOG rebuilds from the canonical transaction truth (TT/undo +
 *     terminal redo) — `source_retained` is not required;
 *   - MULTIXACT / COMMIT_TS rebuild from the retained failed-origin
 *     redo — `source_retained` IS required (U-SIDE-09/10: a projection
 *     whose rebuild source cannot be proven after retirement is denied
 *     retire, and a missing/unknown timestamp never becomes COMMITTED).
 * `canonical_producer_ok` is the exact producer identity/version/coverage
 * check for the rebuild source.
 */
extern bool cluster_side_projection_rebuildable(ClusterSideProjectionKind kind,
												bool source_retained, bool canonical_producer_ok);

/*
 * §2.4 lookup verdict: a VERIFIED projection answers; anything else is
 * FAIL_CLOSED (scope closed until rebuild; no synchronous network or
 * durable I/O is added by this judgement — it is a pure function).
 */
typedef enum ClusterSideProjectionLookup {
	CLUSTER_SIDE_PROJECTION_LOOKUP_OK = 0,
	CLUSTER_SIDE_PROJECTION_LOOKUP_FAIL_CLOSED
} ClusterSideProjectionLookup;

extern ClusterSideProjectionLookup cluster_side_projection_lookup(bool verified);

#endif /* CLUSTER_SIDE_PROJECTION_H */
